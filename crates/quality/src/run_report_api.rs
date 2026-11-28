//! A run's report from its job runner (decision 0090, rulings 75 and 76):
//! `POST /internal/quality/v1/runs/{run}/report` with the bundle route's
//! machine token. The core checks the digest against the run's pinned
//! bundle and takes rows only for statements the bundle declares as a
//! `Rows` read, within their columns and row count: counts and names,
//! never values. It keeps the report once, and on an evaluate's (or a
//! backfill's) `done` writes the index from the run's read-back rows and
//! marks the cut done.

use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::{header::AUTHORIZATION, HeaderMap, StatusCode};
use axum::routing::post;
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sqlx::PgPool;

use crate::bundle::{self, Bundle, BundleSigner, Expect};
use crate::worker_api::MachineTokens;

/// The statements whose rows feed the index (quality-jobs' evaluate).
const INDEX_SUMMARY: &str = "index_run_summary";
/// The statement whose rows are a `tables` run's listing.
const LISTING: &str = "listing";

/// What a job runner reports of a run.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RunReport {
    pub digest: String,
    pub state: RunState,
    pub statements: Vec<StatementReport>,
    #[serde(default)]
    pub snapshots: Vec<SnapshotFilled>,
    #[serde(default)]
    pub error_class: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RunState {
    Done,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StatementState {
    Done,
    Skipped,
    Failed,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct StatementReport {
    pub id: String,
    pub state: StatementState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rows: Option<Vec<Vec<Value>>>,
}

/// A placeholder as the runner filled it: a snapshot id or a bigint.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshotFilled {
    pub placeholder: String,
    pub value: String,
}

/// What the route uses besides the database.
#[derive(Clone)]
pub struct RunReportServices {
    /// Machine tokens for the bundle route's audience.
    pub tokens: Arc<dyn MachineTokens>,
    pub signer: Arc<dyn BundleSigner>,
    /// Where a map run's proposal is published, and its rules' texts
    /// (decision 0090, ruling 79); without them a map's profile waits.
    pub drafts: Option<Arc<dyn crate::job_map::Drafts>>,
    pub proposal_alerts: Option<crate::proposal::Alerts>,
    /// The catalog's `main`, whose mappings a schedule's fire fingerprints
    /// (ruling 92(c)).
    pub catalog: Option<Arc<dyn crate::catalog::Catalog>>,
    /// The sources' key, which seals a lookup's document until its job
    /// runner reads it (ruling 102).
    pub sealer: Option<Arc<dyn crate::source_store::Sealer>>,
}

impl std::fmt::Debug for RunReportServices {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RunReportServices").finish_non_exhaustive()
    }
}

#[derive(Clone)]
struct Api {
    pool: PgPool,
    services: RunReportServices,
}

/// `POST /internal/quality/v1/runs/{run}/report`, and the values reads'
/// `POST …/runs/{run}/rows` ([`crate::run_rows_api`]), and the schedules
/// the orchestrator fires ([`crate::schedules_api`]).
pub fn routes(pool: PgPool, services: RunReportServices) -> Router {
    Router::new()
        .route("/internal/quality/v1/runs/{run}/report", post(report))
        .with_state(Api {
            pool: pool.clone(),
            services: services.clone(),
        })
        .merge(crate::run_rows_api::routes(pool.clone(), services.clone()))
        .merge(crate::schedules_api::routes(pool.clone(), services.clone()))
        .merge(crate::entity_lookup_api::internal_routes(pool, services))
}

type Refusal = (StatusCode, Json<Value>);

/// A pinned bundle's row: kind, dataset, cut, digest, the signed bundle
/// and the source a source's job reads.
type PinnedRow = (
    String,
    Option<String>,
    Option<chrono::NaiveDate>,
    String,
    String,
    Option<String>,
);

fn refused(status: StatusCode, error: &str) -> Refusal {
    (status, Json(json!({ "error": error })))
}

fn internal(e: impl std::fmt::Display) -> Refusal {
    tracing::error!("run report: {e}");
    refused(StatusCode::INTERNAL_SERVER_ERROR, "internal")
}

/// Whether `v` is a value of the Trino type `ty` (or null).
pub(crate) fn takes(ty: &str, v: &Value) -> bool {
    use crate::index_rows::ColumnType as T;
    let t = match ty {
        "bigint" => T::Bigint,
        "double" => T::Double,
        "boolean" => T::Boolean,
        "date" => T::Date,
        "array(bigint)" => T::BigintArray,
        _ => T::Varchar,
    };
    t.takes(v)
}

/// Whether `rows` fit what the bundle declared of `id`.
fn rows_fit(b: &Bundle, id: &str, rows: &[Vec<Value>]) -> bool {
    let Some(s) = b.statements.iter().find(|s| s.id == id) else {
        return false;
    };
    match &s.expects {
        Expect::Rows { columns, max_rows } => {
            u64::try_from(rows.len()).is_ok_and(|n| n <= *max_rows)
                && rows.iter().all(|r| {
                    r.len() == columns.len()
                        && r.iter().zip(columns).all(|(v, (_, ty))| takes(ty, v))
                })
        }
        _ => false,
    }
}

async fn report(
    State(api): State<Api>,
    Path(run): Path<String>,
    headers: HeaderMap,
    Json(r): Json<RunReport>,
) -> Result<StatusCode, Refusal> {
    let caller = headers
        .get(AUTHORIZATION)
        .and_then(|h| h.to_str().ok())
        .and_then(|h| h.strip_prefix("Bearer "))
        .and_then(|t| api.services.tokens.client(t))
        .ok_or_else(|| refused(StatusCode::UNAUTHORIZED, "unauthorized"))?;
    let pinned: Option<PinnedRow> = sqlx::query_as(
        "SELECT kind, dataset, cut, digest, bundle, source FROM quality.job_bundles WHERE run = $1",
    )
    .bind(&run)
    .fetch_optional(&api.pool)
    .await
    .map_err(internal)?;
    let (kind, dataset, cut, digest, token, source) =
        pinned.ok_or_else(|| refused(StatusCode::NOT_FOUND, "no_bundle"))?;
    if r.digest != digest {
        return Err(refused(StatusCode::CONFLICT, "digest_mismatch"));
    }
    let b = bundle::verify(api.services.signer.as_ref(), &token, &run, &digest)
        .map_err(|_| internal("the pinned bundle does not verify"))?;
    for s in &r.statements {
        if let Some(rows) = &s.rows {
            if !rows_fit(&b, &s.id, rows) {
                return Err(refused(
                    StatusCode::UNPROCESSABLE_ENTITY,
                    "rows_not_allowed",
                ));
            }
        }
    }
    let state = match r.state {
        RunState::Done => "done",
        RunState::Failed => "failed",
    };
    let mut tx = api.pool.begin().await.map_err(internal)?;
    let kept = sqlx::query(
        "INSERT INTO quality.job_reports (run, state, error_class, report, reported_by)
         VALUES ($1, $2, $3, $4, $5) ON CONFLICT DO NOTHING",
    )
    .bind(&run)
    .bind(state)
    .bind(&r.error_class)
    .bind(sqlx::types::Json(&r))
    .bind(caller.worker_prefix())
    .execute(&mut *tx)
    .await
    .map_err(internal)?
    .rows_affected();
    if kept == 0 {
        return Err(refused(StatusCode::CONFLICT, "already_reported"));
    }
    if kind == crate::console_api::KIND && r.state == RunState::Failed {
        crate::console_run::failed(&api.pool, &run, r.error_class.as_deref()).await?;
    }
    if kind == "tables" && r.state == RunState::Done {
        listed(&mut tx, &run, source.as_deref(), &r).await?;
    }
    let indexed = matches!(kind.as_str(), "evaluate" | "backfill") && r.state == RunState::Done;
    if let (true, Some(dataset), Some(cut)) = (indexed, dataset, cut) {
        index(&mut tx, &run, &dataset, cut, &b, &r).await?;
    }
    tx.commit().await.map_err(internal)?;
    tracing::info!(run, kind, state, "run reported");
    Ok(StatusCode::NO_CONTENT)
}

fn rows_of<'a>(r: &'a RunReport, id: &str) -> Option<&'a Vec<Vec<Value>>> {
    r.statements
        .iter()
        .find(|s| s.id == id && s.state == StatementState::Done)
        .and_then(|s| s.rows.as_ref())
}

/// A `tables` run's listing into `quality.source_tables` (ruling 68).
async fn listed(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    run: &str,
    source: Option<&str>,
    r: &RunReport,
) -> Result<(), Refusal> {
    let job =
        uuid::Uuid::parse_str(run).map_err(|_| refused(StatusCode::BAD_REQUEST, "invalid_run"))?;
    let source = source.ok_or_else(|| refused(StatusCode::UNPROCESSABLE_ENTITY, "no_source"))?;
    let rows = rows_of(r, LISTING)
        .ok_or_else(|| refused(StatusCode::UNPROCESSABLE_ENTITY, "listing_missing"))?;
    let listing = crate::source_tables::listing_from_rows(source, rows);
    let driver: Option<String> =
        sqlx::query_scalar("SELECT driver FROM quality.sources WHERE name = $1")
            .bind(source)
            .fetch_optional(&mut **tx)
            .await
            .map_err(internal)?;
    let Some(driver) = driver else {
        return Ok(());
    };
    if !listing.is_valid(&driver) {
        return Err(refused(StatusCode::UNPROCESSABLE_ENTITY, "listing_invalid"));
    }
    crate::source_tables::store_run(tx, job, &listing)
        .await
        .map_err(internal)
}

/// The index from the run's read-back rows (ruling 76): its summary and
/// its rules' counts replace the cut's, and the cut is done.
async fn index(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    run: &str,
    dataset: &str,
    cut: chrono::NaiveDate,
    b: &Bundle,
    r: &RunReport,
) -> Result<(), Refusal> {
    let job =
        uuid::Uuid::parse_str(run).map_err(|_| refused(StatusCode::BAD_REQUEST, "invalid_run"))?;
    let summary = rows_of(r, INDEX_SUMMARY)
        .and_then(|rows| rows.first())
        .ok_or_else(|| refused(StatusCode::UNPROCESSABLE_ENTITY, "index_rows_missing"))?;
    let int = |v: &Value| v.as_i64().unwrap_or(0).max(0);
    let commit = b.catalog_commit.clone().unwrap_or_default();
    sqlx::query("DELETE FROM quality.run_summary WHERE dataset = $1 AND cut = $2")
        .bind(dataset)
        .bind(cut)
        .execute(&mut **tx)
        .await
        .map_err(internal)?;
    sqlx::query(
        "INSERT INTO quality.run_summary (dataset, cut, job_id, records, duration_ms, rules_commit)
         VALUES ($1, $2, $3, $4, $5, $6)",
    )
    .bind(dataset)
    .bind(cut)
    .bind(job)
    .bind(int(&summary[0]))
    .bind(int(&summary[1]))
    .bind(&commit)
    .execute(&mut **tx)
    .await
    .map_err(internal)?;
    for table in crate::index_rows::INDEX_TABLES {
        let rows = rows_of(r, &table.statement()).map_or(&[][..], Vec::as_slice);
        crate::index_rows::write(tx, table, dataset, cut, rows)
            .await
            .map_err(internal)?;
    }
    sqlx::query("UPDATE quality.cuts SET state = 'done' WHERE dataset = $1 AND cut = $2")
        .bind(dataset)
        .bind(cut)
        .execute(&mut **tx)
        .await
        .map_err(internal)?;
    Ok(())
}
