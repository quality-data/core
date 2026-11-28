//! An entity looked up by a typed document (decision 0090, rulings 81, 85
//! and 102). A person behind `quality:ViewPersonalData` asks with a reason
//! (`POST /quality/entities/lookup`), audited as a view of personal data
//! before anything runs; an `entity_key` job is queued with the datasets
//! the asker may view, and the document waits sealed for its job runner,
//! which reads it once (`POST /internal/quality/v1/runs/{run}/document`)
//! and fills the bundle's text token with it; Trino hashes it with the
//! findings key. The rows (each dataset's ICD and band, never the hash)
//! come back on the values route and are kept an hour for the asker
//! (`GET /quality/entities/lookup/{job}`). The document is never logged,
//! stored in clear, in a bundle, a report or an audit row.

use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::{header::AUTHORIZATION, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Extension, Json, Router};
use contracts::{AuditEntry, Caller, Change, ViewDetail, ViewReason};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sqlx::PgPool;
use uuid::Uuid;

use crate::refusal::refuse;
use crate::rule_test::PERSONAL;
use crate::run_report_api::RunReportServices;
use crate::run_rows_api::{internal, refused, Refusal, RunRows, ValuesRun};
use crate::source_store::Sealer;

/// The job's kind.
pub const KIND: &str = crate::bundle::ENTITY_KEY;
/// The statement whose rows are the result.
pub const ENTITY_READ: &str = "entity";

/// What a person asks: the document, and why (decision 0069).
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LookupAsk {
    pub document: String,
    #[serde(default)]
    pub purpose: String,
    pub reference: Option<String>,
}

impl std::fmt::Debug for LookupAsk {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LookupAsk")
            .field("purpose", &self.purpose)
            .finish_non_exhaustive()
    }
}

/// One dataset's result for the entity.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EntityResult {
    pub dataset: String,
    pub cut: String,
    pub icd: Option<f64>,
    pub band: Option<String>,
}

#[derive(Clone)]
struct Api {
    pool: PgPool,
    sealer: Arc<dyn Sealer>,
}

/// The routes for people.
pub fn routes(pool: PgPool, sealer: Arc<dyn Sealer>) -> Router {
    Router::new()
        .route("/quality/entities/lookup", post(ask))
        .route("/quality/entities/lookup/{job}", get(read))
        .with_state(Api { pool, sealer })
}

/// The job runner's route for the document.
pub fn internal_routes(pool: PgPool, services: RunReportServices) -> Router {
    Router::new()
        .route("/internal/quality/v1/runs/{run}/document", post(document))
        .with_state((pool, services))
}

/// Whether `document` may be looked up: 1 to 64 characters, none a control.
fn takes(document: &str) -> bool {
    crate::bundle::PlaceholderType::Text.takes(document)
}

async fn ask(
    State(api): State<Api>,
    Extension(caller): Extension<Caller>,
    Json(body): Json<LookupAsk>,
) -> Response {
    if !takes(&body.document) {
        return refuse(StatusCode::UNPROCESSABLE_ENTITY, "document");
    }
    let reason = match ViewReason::parse(&body.purpose, body.reference.as_deref()) {
        Ok(r) => r,
        Err(e) => return refuse(StatusCode::UNPROCESSABLE_ENTITY, e.code()),
    };
    let all: Result<Vec<String>, _> =
        sqlx::query_scalar("SELECT DISTINCT dataset FROM quality.cuts ORDER BY dataset")
            .fetch_all(&api.pool)
            .await;
    let datasets: Vec<String> = match all {
        Ok(all) => all
            .into_iter()
            .filter(|d| caller.can(PERSONAL, &format!("pipeline/{d}")))
            .collect(),
        Err(e) => return unavailable(e),
    };
    if datasets.is_empty() {
        return refuse(StatusCode::FORBIDDEN, "forbidden");
    }
    let sealed = match api.sealer.seal(&body.document) {
        Ok(s) => s,
        Err(_) => return refuse(StatusCode::SERVICE_UNAVAILABLE, "unavailable"),
    };
    let job = Uuid::new_v4();
    let detail = ViewDetail {
        reason,
        job: job.to_string(),
        rows_shown: u32::try_from(datasets.len()).unwrap_or(u32::MAX),
        fields_shown: vec!["icd".into(), "band".into()],
    };
    let entry = AuditEntry {
        action: PERSONAL,
        resource: format!("entity/lookup/{job}"),
        change: Change::Viewed,
    };
    if caller.audit_view(entry, detail).await.is_err() {
        return refuse(StatusCode::SERVICE_UNAVAILABLE, "audit_unavailable");
    }
    let parameters = json!({ "by": caller.account, "datasets": datasets });
    let queued = async {
        let mut tx = api.pool.begin().await?;
        sqlx::query(
            "INSERT INTO quality.jobs (id, kind, parameters, priority, max_attempts)
             VALUES ($1, $2, $3, 10, 1)",
        )
        .bind(job)
        .bind(KIND)
        .bind(&parameters)
        .execute(&mut *tx)
        .await?;
        sqlx::query("INSERT INTO quality.lookup_documents (job, sealed) VALUES ($1, $2)")
            .bind(job)
            .bind(&sealed)
            .execute(&mut *tx)
            .await?;
        tx.commit().await
    };
    match queued.await {
        Ok(()) => (StatusCode::ACCEPTED, Json(json!({ "job": job }))).into_response(),
        Err(e) => unavailable(e),
    }
}

fn unavailable(e: sqlx::Error) -> Response {
    tracing::error!("entity lookup: {e}");
    refuse(StatusCode::SERVICE_UNAVAILABLE, "unavailable")
}

async fn read(
    State(api): State<Api>,
    Extension(caller): Extension<Caller>,
    Path(job): Path<Uuid>,
) -> Response {
    let found: Result<Option<(String, Option<Value>)>, _> = sqlx::query_as(
        "SELECT j.state, l.result FROM quality.jobs j
           LEFT JOIN quality.entity_lookups l ON l.job = j.id
          WHERE j.id = $1 AND j.kind = $2 AND j.parameters->>'by' = $3",
    )
    .bind(job)
    .bind(KIND)
    .bind(&caller.account)
    .fetch_optional(&api.pool)
    .await;
    match found {
        Ok(Some((state, result))) => {
            let state = if result.is_some() {
                "done".into()
            } else {
                state
            };
            Json(json!({ "state": state, "result": result })).into_response()
        }
        Ok(None) => refuse(StatusCode::NOT_FOUND, "not_found"),
        Err(e) => unavailable(e),
    }
}

/// The run's document, once: the sealed row goes as it is read.
async fn document(
    State((pool, services)): State<(PgPool, RunReportServices)>,
    Path(run): Path<String>,
    headers: HeaderMap,
) -> Result<Json<Value>, Refusal> {
    let caller = headers
        .get(AUTHORIZATION)
        .and_then(|h| h.to_str().ok())
        .and_then(|h| h.strip_prefix("Bearer "))
        .and_then(|t| services.tokens.client(t))
        .ok_or_else(|| refused(StatusCode::UNAUTHORIZED, "unauthorized"))?;
    let sealer = services
        .sealer
        .as_deref()
        .ok_or_else(|| refused(StatusCode::SERVICE_UNAVAILABLE, "unavailable"))?;
    let job = Uuid::parse_str(&run).map_err(|_| refused(StatusCode::BAD_REQUEST, "invalid_run"))?;
    let pinned: Option<String> =
        sqlx::query_scalar("SELECT kind FROM quality.job_bundles WHERE run = $1")
            .bind(&run)
            .fetch_optional(&pool)
            .await
            .map_err(internal)?;
    if pinned.as_deref() != Some(KIND) {
        return Err(refused(StatusCode::NOT_FOUND, "no_bundle"));
    }
    // Recorded before it is given, as a source's secrets are (0030).
    crate::job_sources::record(
        services.log.as_deref(),
        job,
        KIND,
        &caller.worker_prefix(),
        &headers,
        &format!("entity/lookup/{job}/document"),
    )
    .await
    .map_err(|status| refused(status, "not_recorded"))?;
    let sealed: Option<String> =
        sqlx::query_scalar("DELETE FROM quality.lookup_documents WHERE job = $1 RETURNING sealed")
            .bind(job)
            .fetch_optional(&pool)
            .await
            .map_err(internal)?;
    let sealed = sealed.ok_or_else(|| refused(StatusCode::GONE, "gone"))?;
    let document = sealer
        .open(&sealed)
        .map_err(|_| internal("the document does not open"))?;
    tracing::info!(run, "lookup document given");
    Ok(Json(json!({ "document": document })))
}

/// The lookup's rows: its result, kept an hour for its asker.
pub(crate) async fn take(pool: &PgPool, run: &ValuesRun, rows: &RunRows) -> Result<(), Refusal> {
    if rows.statement != ENTITY_READ {
        return Err(refused(StatusCode::UNPROCESSABLE_ENTITY, "not_the_entity"));
    }
    let text = |v: &Value| v.as_str().map(str::to_owned);
    let result: Vec<EntityResult> = rows
        .rows
        .iter()
        .filter_map(|r| {
            Some(EntityResult {
                dataset: text(r.first()?)?,
                cut: text(r.get(1)?)?,
                icd: r.get(2)?.as_f64(),
                band: text(r.get(3)?),
            })
        })
        .collect();
    let job =
        Uuid::parse_str(&run.run).map_err(|_| refused(StatusCode::BAD_REQUEST, "invalid_run"))?;
    sqlx::query("DELETE FROM quality.entity_lookups WHERE created_at < now() - interval '1 hour'")
        .execute(pool)
        .await
        .map_err(internal)?;
    sqlx::query(
        "INSERT INTO quality.entity_lookups (job, result) VALUES ($1, $2)
         ON CONFLICT (tenant_id, job) DO UPDATE SET result = EXCLUDED.result",
    )
    .bind(job)
    .bind(sqlx::types::Json(&result))
    .execute(pool)
    .await
    .map_err(internal)?;
    Ok(())
}

/// The datasets a queued lookup may read.
pub async fn datasets_of(
    pool: &PgPool,
    run: &str,
) -> Result<Vec<String>, crate::job_inputs::FactsError> {
    use crate::job_inputs::FactsError;
    let id = Uuid::parse_str(run).map_err(|_| FactsError::Refused("invalid_run"))?;
    let p: Value =
        sqlx::query_scalar("SELECT parameters FROM quality.jobs WHERE id = $1 AND kind = $2")
            .bind(id)
            .bind(KIND)
            .fetch_optional(pool)
            .await
            .map_err(|e| FactsError::Failed(e.to_string()))?
            .ok_or(FactsError::Refused("no_job"))?;
    p.get("datasets")
        .and_then(|d| serde_json::from_value::<Vec<String>>(d.clone()).ok())
        .filter(|d| !d.is_empty())
        .ok_or(FactsError::Refused("no_dataset"))
}
