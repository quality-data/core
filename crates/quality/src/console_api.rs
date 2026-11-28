//! The console (decision 0054): a statement over the lake is
//! queued as a `query` job, which a worker runs on DuckDB with the lake
//! locked down; its result is kept an hour for whoever asked only. Asking
//! needs `quality:QueryData`; values are complete behind
//! `quality:ViewPersonalData` on every dataset, allowed on all and denied on
//! none (decision 0064): a read always answers them masked, and a reveal
//! with a reason answers them whole (decision 0069, [`reveal`]); each
//! statement run and each reveal is audited.

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Extension, Json, Router};
use contracts::{console_column, AuditEntry, Caller, Change, ViewPurpose};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sqlx::PgPool;
use utoipa::{OpenApi, ToSchema};
use uuid::Uuid;

use crate::console::{wrapped, ConsoleResult};
use crate::personal_data;
use crate::refusal::refuse;
use crate::reveal::{self, Refusal, RevealIn, Shown};

/// The job's kind.
pub const KIND: &str = "query";
/// Who may run a statement.
pub const QUERY: &str = "quality:QueryData";

/// `POST /quality/console`, `GET /quality/console/{job}` and
/// `POST …/{job}/reveal`.
pub fn routes(pool: PgPool) -> Router {
    Router::new()
        .route("/quality/console", post(ask))
        .route("/quality/console/{job}", get(read))
        .route("/quality/console/{job}/reveal", post(reveal))
        .with_state(pool)
}

/// The routes' OpenAPI paths and schemas.
#[derive(Debug, OpenApi)]
#[openapi(
    paths(ask, read, reveal),
    components(schemas(ConsoleAsk, ConsoleAsked, ConsoleOut, ConsoleResult, RevealIn, ViewPurpose)),
    tags((name = "console", description = "Statements over the lake")),
)]
pub struct ConsoleDoc;

/// A statement to run: one query over the lake's tables (`lake.<ns>.<table>`).
#[derive(Debug, Deserialize, ToSchema)]
pub struct ConsoleAsk {
    pub sql: String,
}

/// The statement queued: its job.
#[derive(Debug, Serialize, ToSchema)]
pub struct ConsoleAsked {
    pub job: String,
}

/// A statement as its asker sees it: `queued`, `running`, `done` or `failed`.
#[derive(Debug, Serialize, ToSchema)]
pub struct ConsoleOut {
    pub state: String,
    pub error: Option<String>,
    /// Whether the values are masked: always on a read, never on a reveal.
    pub masked: bool,
    /// Whether a reveal would show values: the result has rows or an
    /// engine message and the caller holds the permission that shows them.
    pub revealable: bool,
    pub result: Option<ConsoleResult>,
}

fn internal(e: sqlx::Error) -> Response {
    tracing::error!("console: {e}");
    refuse(StatusCode::SERVICE_UNAVAILABLE, "unavailable")
}

/// The datasets with a cut whose results `caller` may view
/// (`quality:ViewResults` on `pipeline/<dataset>`).
async fn viewable(pool: &PgPool, caller: &Caller) -> Result<Vec<String>, sqlx::Error> {
    let all: Vec<String> =
        sqlx::query_scalar("SELECT DISTINCT dataset FROM quality.cuts ORDER BY dataset")
            .fetch_all(pool)
            .await?;
    Ok(all
        .into_iter()
        .filter(|d| caller.can(crate::chart_api::VIEW, &format!("pipeline/{d}")))
        .collect())
}

/// Queues a statement, audited as run.
#[utoipa::path(post, path = "/quality/console", operation_id = "quality_run_statement",
    tag = "console", request_body = ConsoleAsk,
    responses((status = 202, body = ConsoleAsked), (status = 401), (status = 403),
        (status = 422, description = "`statement`: empty, too long, or more than one"),
        (status = 503)))]
async fn ask(
    State(pool): State<PgPool>,
    Extension(caller): Extension<Caller>,
    Json(ask): Json<ConsoleAsk>,
) -> Response {
    if !caller.can(QUERY, "*") {
        return refuse(StatusCode::FORBIDDEN, "forbidden");
    }
    if wrapped(&ask.sql).is_err() {
        return refuse(StatusCode::UNPROCESSABLE_ENTITY, "statement");
    }
    // Trino reads, for this statement, only the datasets whose results
    // the asker may view (decision 0090, ruling 84).
    let datasets = match viewable(&pool, &caller).await {
        Ok(d) => d,
        Err(e) => return internal(e),
    };
    // Waited for: before the scheduled work, and run once.
    let parameters = json!({ "sql": ask.sql, "by": caller.account, "datasets": datasets });
    let job: Result<Uuid, _> = sqlx::query_scalar(
        "INSERT INTO quality.jobs (kind, parameters, priority, max_attempts)
         VALUES ($1, $2, 10, 1) RETURNING id",
    )
    .bind(KIND)
    .bind(&parameters)
    .fetch_one(&pool)
    .await;
    let job = match job {
        Ok(job) => job,
        Err(e) => return internal(e),
    };
    let entry = AuditEntry {
        action: QUERY,
        resource: format!("lake/console/{job}"),
        change: Change::Ran,
    };
    if let Err(e) = caller.audit(entry).await {
        tracing::error!("console: audit: {e}");
        return refuse(StatusCode::SERVICE_UNAVAILABLE, "audit_unavailable");
    }
    let out = ConsoleAsked {
        job: job.to_string(),
    };
    (StatusCode::ACCEPTED, Json(out)).into_response()
}

#[derive(sqlx::FromRow)]
struct ResultRow {
    state: String,
    error: Option<String>,
    result: Option<serde_json::Value>,
}

/// The caller's statement `job` and its result if kept; `Err` with the
/// answer when not found.
async fn load(
    pool: &PgPool,
    caller: &Caller,
    job: Uuid,
) -> Result<(ResultRow, Option<ConsoleResult>), Refusal> {
    let found: Result<Option<ResultRow>, _> = sqlx::query_as(
        "SELECT j.state, j.error, r.result FROM quality.jobs j
           LEFT JOIN quality.console_results r ON r.job = j.id
          WHERE j.id = $1 AND j.kind = $2 AND j.parameters->>'by' = $3",
    )
    .bind(job)
    .bind(KIND)
    .bind(&caller.account)
    .fetch_optional(pool)
    .await;
    match found {
        Ok(Some(mut row)) => {
            let result = row
                .result
                .take()
                .and_then(|r| serde_json::from_value(r).ok());
            Ok((row, result))
        }
        Ok(None) => Err(Refusal(StatusCode::NOT_FOUND, "not_found")),
        Err(e) => {
            tracing::error!("console: {e}");
            Err(Refusal(StatusCode::SERVICE_UNAVAILABLE, "unavailable"))
        }
    }
}

/// The answer of a read or a reveal; `revealable` when it has rows or an
/// engine message and the caller sees them, once the job is finished.
fn answer(row: ResultRow, result: Option<ConsoleResult>, sees: bool, masked: bool) -> Response {
    let values = result
        .as_ref()
        .is_some_and(|r| !r.rows.is_empty() || r.error.is_some());
    let revealable = sees && values && reveal::finished(&row.state);
    let state = if row.state == "done" && result.is_none() {
        "running".into()
    } else {
        row.state
    };
    Json(ConsoleOut {
        state,
        error: row.error,
        masked,
        revealable,
        result,
    })
    .into_response()
}

/// A statement's state and, when run, its result, always masked;
/// `revealable` when it has rows or an engine message and the caller holds
/// `quality:ViewPersonalData` on every dataset. Writes no audit row.
#[utoipa::path(get, path = "/quality/console/{job}", operation_id = "quality_read_statement",
    tag = "console", params(("job" = String, Path)),
    responses((status = 200, body = ConsoleOut), (status = 401),
        (status = 404, description = "No such statement, or another person's"), (status = 503)))]
async fn read(
    State(pool): State<PgPool>,
    Extension(caller): Extension<Caller>,
    Path(job): Path<Uuid>,
) -> Response {
    match load(&pool, &caller, job).await {
        Ok((row, result)) => {
            let sees = personal_data::on_every_dataset(&caller);
            answer(row, result.map(ConsoleResult::masked), sees, true)
        }
        Err(refused) => refused.into_response(),
    }
}

/// A statement's result with its values and its engine message whole, for
/// a reason: audited first, one row per reveal with the purpose, the
/// reference, the job, the rows and the columns shown, each by its name
/// when a plain identifier and by its position (`#2`) otherwise.
#[utoipa::path(post, path = "/quality/console/{job}/reveal", operation_id = "quality_reveal_statement",
    tag = "console", params(("job" = String, Path)), request_body = RevealIn,
    responses((status = 200, body = ConsoleOut), (status = 401),
        (status = 403, description = "`forbidden`: no `quality:ViewPersonalData` on every dataset"),
        (status = 404, description = "No such statement, another person's, or purged"),
        (status = 409, description = "`not_done`: queued, running, or failed with no result"),
        (status = 422, description = "`purpose` (not a key) or `reference` (not of its form, or none with `other`)"),
        (status = 503, description = "`audit_unavailable`: the view could not be audited, nothing shown")))]
async fn reveal(
    State(pool): State<PgPool>,
    Extension(caller): Extension<Caller>,
    Path(job): Path<Uuid>,
    Json(body): Json<RevealIn>,
) -> Response {
    let (row, result) = match load(&pool, &caller, job).await {
        Ok(found) => found,
        Err(refused) => return refused.into_response(),
    };
    let sees = personal_data::on_every_dataset(&caller);
    let shown = result.as_ref().map(|result| Shown {
        resource: format!("lake/console/{job}"),
        rows: result.rows.len(),
        fields: (result.columns.iter().enumerate())
            .map(|(n, name)| console_column(n + 1, name))
            .collect(),
    });
    if let Err(refused) = reveal::admit(&caller, job, &row.state, sees, &body, shown).await {
        return refused.into_response();
    }
    answer(row, result, sees, false)
}

/// A `query` job's result, kept an hour for its asker; older ones purged.
pub(crate) async fn report(
    pool: &PgPool,
    id: Uuid,
    body: serde_json::Value,
) -> Result<StatusCode, StatusCode> {
    let result: ConsoleResult =
        serde_json::from_value(body).map_err(|_| StatusCode::UNPROCESSABLE_ENTITY)?;
    if !result.is_valid() {
        return Err(StatusCode::UNPROCESSABLE_ENTITY);
    }
    keep_result(pool, id, &result)
        .await
        .map(|()| StatusCode::NO_CONTENT)
        .map_err(|e| {
            tracing::error!("console report: {e}");
            StatusCode::SERVICE_UNAVAILABLE
        })
}

/// Keeps `result` as statement `id`'s for an hour; older ones are purged.
pub(crate) async fn keep_result(
    pool: &PgPool,
    id: Uuid,
    result: &ConsoleResult,
) -> Result<(), sqlx::Error> {
    let value = serde_json::to_value(result).map_err(|e| sqlx::Error::Protocol(e.to_string()))?;
    sqlx::query("DELETE FROM quality.console_results WHERE created_at < now() - interval '1 hour'")
        .execute(pool)
        .await?;
    sqlx::query(
        "INSERT INTO quality.console_results (job, result) VALUES ($1, $2)
         ON CONFLICT (tenant_id, job) DO UPDATE SET result = EXCLUDED.result",
    )
    .bind(id)
    .bind(value)
    .execute(pool)
    .await?;
    Ok(())
}
