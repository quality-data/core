//! A source table's records for people (decision 0049, points 3 to 5): a
//! page asked is queued as a `records` job, which a worker reads through
//! the source's engine; its page is kept an hour for whoever asked only.
//! Asking needs `quality:ViewSources` on `source/<name>`; the values are
//! complete behind `quality:ViewPersonalData` on every dataset that reads
//! the table (decision 0064, [`personal_data`]): a read always answers them
//! masked, and a reveal with a reason answers them whole after its audit
//! row (decision 0069, [`reveal`]).

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Extension, Json, Router};
use contracts::{Caller, ViewPurpose};
use serde::Serialize;
use serde_json::json;
use sqlx::PgPool;
use std::sync::Arc;
use utoipa::{OpenApi, ToSchema};
use uuid::Uuid;

use crate::catalog::Catalog;
use crate::personal_data;
use crate::refusal::refuse;
use crate::reveal::{self, Refusal, RevealIn, Shown};
use crate::source_api::VIEW;
use crate::source_records::{AskError, RecordsAsk, RecordsPage};
use crate::source_tables;

/// The job's kind.
pub const KIND: &str = "records";

/// `POST /quality/sources/{name}/records` and `GET …/records/{job}`; the
/// catalog says which datasets read a table, for who sees its values.
pub fn routes(pool: PgPool, catalog: Option<Arc<dyn Catalog>>) -> Router {
    Router::new()
        .route("/quality/sources/{name}/records", post(ask))
        .route("/quality/sources/{name}/records/{job}", get(read))
        .route("/quality/sources/{name}/records/{job}/reveal", post(reveal))
        .with_state(pool)
        .layer(Extension(Readers(catalog)))
}

/// The catalog the records' reads ask who reads a table.
#[derive(Clone)]
struct Readers(Option<Arc<dyn Catalog>>);

/// The routes' OpenAPI paths and schemas.
#[derive(Debug, OpenApi)]
#[openapi(
    paths(ask, read, reveal),
    components(schemas(RecordsAsk, RecordsAsked, RecordsOut, RecordsPage, RevealIn, ViewPurpose)),
    tags((name = "sources", description = "The sources and the driver catalog")),
)]
pub struct RecordsDoc;

/// The page queued: its job.
#[derive(Debug, Serialize, ToSchema)]
pub struct RecordsAsked {
    pub job: String,
}

/// A page as its asker sees it: `queued`, `running`, `done` or `failed`.
#[derive(Debug, Serialize, ToSchema)]
pub struct RecordsOut {
    pub state: String,
    pub error: Option<String>,
    /// Whether the values are masked: always on a read, never on a reveal.
    pub masked: bool,
    /// Whether a reveal would show values: the page has records and the
    /// caller holds the permission that shows them whole.
    pub revealable: bool,
    pub page: Option<RecordsPage>,
}

fn internal(e: sqlx::Error) -> Response {
    tracing::error!("records: {e}");
    refuse(StatusCode::SERVICE_UNAVAILABLE, "unavailable")
}

/// Why a page is not asked, as the screen's texts name it.
fn refused(ask: &RecordsAsk, listing: &source_tables::ListingView) -> Option<&'static str> {
    match ask.check() {
        Err(AskError::Table) => return Some("table"),
        Err(AskError::NoOrder) => return Some("no_order"),
        Err(AskError::TooFar) => return Some("too_far"),
        Ok(()) => {}
    }
    let (schema, name) = ask.schema_and_table().ok()?;
    let Some(table) = listing
        .tables
        .iter()
        .find(|t| t.schema == schema && t.name == name)
    else {
        return Some("table");
    };
    match &ask.order {
        Some(c) if !table.columns.iter().any(|col| &col.name == c) => Some("order"),
        _ => None,
    }
}

/// Queues the reading of a page of a source table's records, as the last
/// listing of the source names the table and its columns.
#[utoipa::path(post, path = "/quality/sources/{name}/records", operation_id = "quality_ask_records",
    tag = "sources", params(("name" = String, Path)), request_body = RecordsAsk,
    responses((status = 202, body = RecordsAsked), (status = 401), (status = 403),
        (status = 404, description = "No such source"),
        (status = 422, description = "`table` (not in its last listing), `order` (no column of it), \
            `no_order` (the last ones need one) or `too_far`"),
        (status = 503)))]
async fn ask(
    State(pool): State<PgPool>,
    Extension(caller): Extension<Caller>,
    Path(name): Path<String>,
    Json(ask): Json<RecordsAsk>,
) -> Response {
    if !caller.can(VIEW, &format!("source/{name}")) {
        return refuse(StatusCode::FORBIDDEN, "forbidden");
    }
    match crate::source_store::exists(&pool, &name).await {
        Ok(true) => {}
        Ok(false) => return refuse(StatusCode::NOT_FOUND, "not_found"),
        Err(e) => return internal(e),
    }
    let listing = match source_tables::view(&pool, &name).await {
        Ok(listing) => listing,
        Err(e) => return internal(e),
    };
    if let Some(why) = refused(&ask, &listing) {
        return refuse(StatusCode::UNPROCESSABLE_ENTITY, why);
    }
    let mut parameters = serde_json::to_value(&ask).unwrap_or_default();
    parameters["source"] = json!(name);
    parameters["by"] = json!(caller.account);
    match queue(&pool, &parameters).await {
        Ok(job) => {
            let out = RecordsAsked {
                job: job.to_string(),
            };
            (StatusCode::ACCEPTED, Json(out)).into_response()
        }
        Err(e) => internal(e),
    }
}

/// Queues the page asked: waited for, before the scheduled work, and run
/// once.
async fn queue(pool: &PgPool, parameters: &serde_json::Value) -> Result<Uuid, sqlx::Error> {
    sqlx::query_scalar(
        "INSERT INTO quality.jobs (kind, parameters, priority, max_attempts)
         VALUES ($1, $2, 10, 1) RETURNING id",
    )
    .bind(KIND)
    .bind(parameters)
    .fetch_one(pool)
    .await
}

#[derive(sqlx::FromRow)]
struct PageRow {
    state: String,
    error: Option<String>,
    page: Option<serde_json::Value>,
    parameters: serde_json::Value,
}

/// The caller's page `job` of `name`, its page if kept, and whether the
/// caller sees its values; `Err` with the answer when not found.
async fn load(
    pool: &PgPool,
    caller: &Caller,
    catalog: Option<&dyn Catalog>,
    (name, job): (&str, Uuid),
) -> Result<(PageRow, Option<RecordsPage>, bool), Refusal> {
    let found: Result<Option<PageRow>, _> = sqlx::query_as(
        "SELECT j.state, j.error, r.page, j.parameters FROM quality.jobs j
           LEFT JOIN quality.source_records r ON r.job = j.id
          WHERE j.id = $1 AND j.kind = $2 AND j.parameters->>'source' = $3
            AND j.parameters->>'by' = $4",
    )
    .bind(job)
    .bind(KIND)
    .bind(name)
    .bind(&caller.account)
    .fetch_optional(pool)
    .await;
    let mut row = match found {
        Ok(Some(row)) => row,
        Ok(None) => return Err(Refusal(StatusCode::NOT_FOUND, "not_found")),
        Err(e) => {
            tracing::error!("records: {e}");
            return Err(Refusal(StatusCode::SERVICE_UNAVAILABLE, "unavailable"));
        }
    };
    let page: Option<RecordsPage> = row.page.take().and_then(|p| serde_json::from_value(p).ok());
    let table = row.parameters.get("table").and_then(|t| t.as_str());
    let sees = personal_data::sees_table(caller, catalog, name, table.unwrap_or_default()).await;
    Ok((row, page, sees))
}

/// The answer of a read or a reveal.
fn answer(row: PageRow, page: Option<RecordsPage>, masked: bool, revealable: bool) -> Response {
    let state = if row.state == "done" && page.is_none() {
        "running".into()
    } else {
        row.state
    };
    Json(RecordsOut {
        state,
        error: row.error,
        masked,
        revealable,
        page,
    })
    .into_response()
}

/// A page's state and, when read, its records, always masked; `revealable`
/// when it has values and the caller holds `quality:ViewPersonalData` on
/// every dataset that reads the table. Writes no audit row.
#[utoipa::path(get, path = "/quality/sources/{name}/records/{job}", operation_id = "quality_read_records",
    tag = "sources", params(("name" = String, Path), ("job" = String, Path)),
    responses((status = 200, body = RecordsOut), (status = 401),
        (status = 404, description = "No such page, or another person's"), (status = 503)))]
async fn read(
    State(pool): State<PgPool>,
    Extension(caller): Extension<Caller>,
    Extension(Readers(catalog)): Extension<Readers>,
    Path((name, job)): Path<(String, Uuid)>,
) -> Response {
    let (row, page, sees) = match load(&pool, &caller, catalog.as_deref(), (&name, job)).await {
        Ok(found) => found,
        Err(refused) => return refused.into_response(),
    };
    let values = page.as_ref().is_some_and(|p| !p.rows.is_empty());
    let revealable = sees && values && reveal::finished(&row.state);
    answer(row, page.map(RecordsPage::masked), true, revealable)
}

/// A page's records with their values whole, for a reason: audited first,
/// one row per reveal with the purpose, the reference, the job, the rows
/// and the columns shown.
#[utoipa::path(post, path = "/quality/sources/{name}/records/{job}/reveal",
    operation_id = "quality_reveal_records", tag = "sources",
    params(("name" = String, Path), ("job" = String, Path)), request_body = RevealIn,
    responses((status = 200, body = RecordsOut), (status = 401),
        (status = 403, description = "`forbidden`: no `quality:ViewPersonalData` on every dataset that reads the table"),
        (status = 404, description = "No such page, another person's, or purged"),
        (status = 409, description = "`not_done`: queued, running, or failed with no page"),
        (status = 422, description = "`purpose` (not a key) or `reference` (not of its form, or none with `other`)"),
        (status = 503, description = "`audit_unavailable`: the view could not be audited, nothing shown")))]
async fn reveal(
    State(pool): State<PgPool>,
    Extension(caller): Extension<Caller>,
    Extension(Readers(catalog)): Extension<Readers>,
    Path((name, job)): Path<(String, Uuid)>,
    Json(body): Json<RevealIn>,
) -> Response {
    let (row, page, sees) = match load(&pool, &caller, catalog.as_deref(), (&name, job)).await {
        Ok(found) => found,
        Err(refused) => return refused.into_response(),
    };
    let number = row.parameters.get("page").and_then(|p| p.as_u64());
    let shown = page.as_ref().map(|page| Shown {
        resource: format!(
            "source/{name}/{}/page/{}",
            page.table,
            number.unwrap_or_default()
        ),
        rows: page.rows.len(),
        fields: page.columns.clone(),
    });
    if let Err(refused) = reveal::admit(&caller, job, &row.state, sees, &body, shown).await {
        return refused.into_response();
    }
    let revealable = page.as_ref().is_some_and(|p| !p.rows.is_empty());
    answer(row, page, false, revealable)
}

/// A `records` job's page, kept an hour for its asker; older pages purged.
/// A page of another source or table than asked is refused.
pub(crate) async fn report(
    pool: &PgPool,
    id: Uuid,
    parameters: &serde_json::Value,
    body: serde_json::Value,
) -> Result<StatusCode, StatusCode> {
    let page: RecordsPage =
        serde_json::from_value(body).map_err(|_| StatusCode::UNPROCESSABLE_ENTITY)?;
    let asked = |key: &str| parameters.get(key).and_then(|v| v.as_str());
    if !page.is_valid()
        || asked("source") != Some(page.source.as_str())
        || asked("table") != Some(page.table.as_str())
    {
        return Err(StatusCode::UNPROCESSABLE_ENTITY);
    }
    keep_page(pool, id, &page)
        .await
        .map(|()| StatusCode::NO_CONTENT)
        .map_err(|e| {
            tracing::error!("records report: {e}");
            StatusCode::SERVICE_UNAVAILABLE
        })
}

/// Keeps `page` as job `id`'s for an hour; older pages are purged.
pub(crate) async fn keep_page(
    pool: &PgPool,
    id: Uuid,
    page: &RecordsPage,
) -> Result<(), sqlx::Error> {
    let value = serde_json::to_value(page).map_err(|e| sqlx::Error::Protocol(e.to_string()))?;
    sqlx::query("DELETE FROM quality.source_records WHERE created_at < now() - interval '1 hour'")
        .execute(pool)
        .await?;
    sqlx::query(
        "INSERT INTO quality.source_records (job, page) VALUES ($1, $2)
         ON CONFLICT (tenant_id, job) DO UPDATE SET page = EXCLUDED.page",
    )
    .bind(id)
    .bind(value)
    .execute(pool)
    .await?;
    Ok(())
}
