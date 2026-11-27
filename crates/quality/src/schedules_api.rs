//! The schedules the orchestrator fires (decision 0090, rulings 92(a) and
//! 92(c)), with the core's machine token (audience `core`, ruling 97):
//! `GET /internal/quality/v1/schedules` lists every schedule's timing, and
//! `POST /internal/quality/v1/schedules/{id}/fires {fire_at}` answers the
//! chain's steps for that fire. A cut's fire opens the cut as the core's
//! scheduler did and answers `prepare` (with the mapping's columns'
//! fingerprint, so the orchestrator runs it only when they change),
//! `extract` and `evaluate`; a blocked cut answers no step. The answer is
//! kept, so the same fire always gets the same steps.

use axum::extract::{Path, State};
use axum::http::{header::AUTHORIZATION, HeaderMap, StatusCode};
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use sqlx::PgPool;
use uuid::Uuid;

use crate::firing::{open_cut, CutRequest, Outcome};
use crate::mapping::Mapping;
use crate::run_report_api::RunReportServices;
use crate::run_rows_api::{internal, refused, Refusal};
use crate::schedule::{cut_date, ScheduleTrigger, Target};
use crate::schedule_store;

/// The most steps a fire answers (the orchestrator's limit).
pub const MAX_STEPS: usize = 16;

/// A schedule's timing, as the orchestrator fires it.
#[derive(Debug, Clone, Serialize)]
pub struct ScheduleTiming {
    pub id: Uuid,
    pub cron: String,
    pub timezone: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trigger: Option<ScheduleTrigger>,
    pub enabled: bool,
}

#[derive(Debug, Serialize)]
pub struct Schedules {
    pub schedules: Vec<ScheduleTiming>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Fire {
    pub fire_at: DateTime<Utc>,
}

/// A step's setup: run only when its fingerprint under `key` changed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Setup {
    pub key: String,
    pub fingerprint: String,
}

/// One step of a fire's chain.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Step {
    pub kind: String,
    pub parameters: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub setup: Option<Setup>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Fired {
    pub steps: Vec<Step>,
}

#[derive(Clone)]
struct Api {
    pool: PgPool,
    services: RunReportServices,
}

/// The two routes.
pub fn routes(pool: PgPool, services: RunReportServices) -> Router {
    Router::new()
        .route("/internal/quality/v1/schedules", get(list))
        .route("/internal/quality/v1/schedules/{id}/fires", post(fire))
        .with_state(Api { pool, services })
}

fn caller(api: &Api, headers: &HeaderMap) -> Result<(), Refusal> {
    headers
        .get(AUTHORIZATION)
        .and_then(|h| h.to_str().ok())
        .and_then(|h| h.strip_prefix("Bearer "))
        .and_then(|t| api.services.tokens.client(t))
        .map(|_| ())
        .ok_or_else(|| refused(StatusCode::UNAUTHORIZED, "unauthorized"))
}

async fn list(State(api): State<Api>, headers: HeaderMap) -> Result<Json<Schedules>, Refusal> {
    caller(&api, &headers)?;
    let schedules = schedule_store::list(&api.pool)
        .await
        .map_err(internal)?
        .into_iter()
        .map(|s| ScheduleTiming {
            id: s.id,
            cron: s.spec.cron,
            timezone: s.spec.timezone,
            trigger: s.spec.trigger,
            enabled: s.spec.enabled,
        })
        .collect();
    Ok(Json(Schedules { schedules }))
}

/// The fingerprint of a mapping's columns: the lower-case hex SHA-256 of
/// its source table and each field's column and type, in field order.
pub fn columns_fingerprint(mapping: &Mapping) -> String {
    let mut text = String::new();
    if let Some(from) = &mapping.from {
        text.push_str(&format!("{}\t{}\n", from.value.source, from.value.table));
    }
    for (field, f) in &mapping.fields {
        text.push_str(&format!(
            "{field}\t{}\t{:?}\n",
            f.value.column, f.value.kind
        ));
    }
    format!("{:x}", Sha256::digest(text.as_bytes()))
}

async fn fire(
    State(api): State<Api>,
    Path(id): Path<Uuid>,
    headers: HeaderMap,
    Json(f): Json<Fire>,
) -> Result<Json<Fired>, Refusal> {
    caller(&api, &headers)?;
    let schedule = schedule_store::get(&api.pool, id)
        .await
        .map_err(internal)?
        .ok_or_else(|| refused(StatusCode::NOT_FOUND, "no_schedule"))?;
    let kept: Option<sqlx::types::Json<Fired>> = sqlx::query_scalar(
        "SELECT steps FROM quality.schedule_fires WHERE schedule = $1 AND fire_at = $2",
    )
    .bind(id)
    .bind(f.fire_at)
    .fetch_optional(&api.pool)
    .await
    .map_err(internal)?;
    if let Some(kept) = kept {
        return Ok(Json(kept.0));
    }
    let mut tx = api.pool.begin().await.map_err(internal)?;
    let steps = match &schedule.spec.target {
        Target::Cut { dataset, rerun } => {
            let date = cut_date(f.fire_at, &schedule.spec.timezone)
                .map_err(|_| refused(StatusCode::UNPROCESSABLE_ENTITY, "timezone"))?;
            let fingerprint = api.fingerprint(dataset).await?;
            let request = CutRequest {
                dataset,
                date,
                rerun: *rerun,
                schedule: Some(id),
                requested_by: None,
                due_at: f.fire_at,
            };
            match open_cut(&mut tx, &request).await.map_err(internal)? {
                (Outcome::Blocked | Outcome::Skipped, _, _) => Vec::new(),
                (_, _, job) => cut_steps(dataset, &date.to_string(), job, fingerprint),
            }
        }
        Target::Job {
            kind,
            dataset,
            parameters,
        } => {
            let mut p = parameters.clone();
            if let (Some(d), Some(o)) = (dataset, p.as_object_mut()) {
                o.insert("dataset".into(), json!(d));
            }
            vec![Step {
                kind: kind.clone(),
                parameters: p,
                setup: None,
            }]
        }
    };
    let fired = Fired { steps };
    let inserted = sqlx::query(
        "INSERT INTO quality.schedule_fires (schedule, fire_at, steps) VALUES ($1, $2, $3)
         ON CONFLICT DO NOTHING",
    )
    .bind(id)
    .bind(f.fire_at)
    .bind(sqlx::types::Json(&fired))
    .execute(&mut *tx)
    .await
    .map_err(internal)?
    .rows_affected();
    if inserted == 0 {
        // Another ask of the same fire won: its answer is the one.
        drop(tx);
        let kept: sqlx::types::Json<Fired> = sqlx::query_scalar(
            "SELECT steps FROM quality.schedule_fires WHERE schedule = $1 AND fire_at = $2",
        )
        .bind(id)
        .bind(f.fire_at)
        .fetch_one(&api.pool)
        .await
        .map_err(internal)?;
        return Ok(Json(kept.0));
    }
    tx.commit().await.map_err(internal)?;
    tracing::info!(schedule = %id, steps = fired.steps.len(), "schedule fired");
    Ok(Json(fired))
}

/// A cut's chain: its dataset's tables prepared when the mapping's columns
/// changed, then its extraction (the load the core's cut names), then its
/// evaluation.
fn cut_steps(dataset: &str, cut: &str, extract: Option<Uuid>, fingerprint: String) -> Vec<Step> {
    let of_cut = json!({ "dataset": dataset, "cut": cut });
    let mut extract_parameters = of_cut.clone();
    if let (Some(job), Some(o)) = (extract, extract_parameters.as_object_mut()) {
        o.insert("job".into(), json!(job));
    }
    let steps = vec![
        Step {
            kind: "prepare".into(),
            parameters: json!({ "dataset": dataset }),
            setup: Some(Setup {
                key: format!("dataset:{dataset}"),
                fingerprint,
            }),
        },
        Step {
            kind: "extract".into(),
            parameters: extract_parameters,
            setup: None,
        },
        Step {
            kind: "evaluate".into(),
            parameters: of_cut,
            setup: None,
        },
    ];
    debug_assert!(steps.len() <= MAX_STEPS);
    steps
}

impl Api {
    /// The fingerprint of `dataset`'s mapping on the catalog's `main`.
    async fn fingerprint(&self, dataset: &str) -> Result<String, Refusal> {
        let catalog =
            self.services.catalog.as_deref().ok_or_else(|| {
                refused(StatusCode::SERVICE_UNAVAILABLE, "catalog_not_configured")
            })?;
        let snapshot = catalog
            .snapshot(dataset)
            .await
            .map_err(internal)?
            .ok_or_else(|| refused(StatusCode::CONFLICT, "no_mapping"))?;
        let mapping = Mapping::parse(&snapshot.mapping)
            .map_err(|_| refused(StatusCode::CONFLICT, "mapping_invalid"))?;
        Ok(columns_fingerprint(&mapping))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_fingerprint_moves_with_the_columns_only() {
        let a = Mapping::parse(
            "dataset: loans\nsource: loans\nfields:\n  Code: { column: code, type: text }\n",
        )
        .unwrap();
        let b = Mapping::parse(
            "dataset: loans\nsource: loans\nfields:\n  Code: { column: code_2, type: text }\n",
        )
        .unwrap();
        assert_eq!(columns_fingerprint(&a), columns_fingerprint(&a.clone()));
        assert_ne!(columns_fingerprint(&a), columns_fingerprint(&b));
        assert_eq!(columns_fingerprint(&a).len(), 64);
        let steps = cut_steps("loans", "2026-10-31", None, "f".into());
        let kinds: Vec<&str> = steps.iter().map(|s| s.kind.as_str()).collect();
        assert_eq!(kinds, ["prepare", "extract", "evaluate"]);
        assert_eq!(steps[0].setup.as_ref().unwrap().key, "dataset:loans");
    }
}
