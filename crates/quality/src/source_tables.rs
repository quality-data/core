//! A source's tables (decision 0042): a `tables` job lists them through
//! the source's driver, its catalog only (each table's columns, by name
//! and declared type, in order), never a row, the
//! system's schemas left out; the source keeps its last listing, which a
//! late report of an older job never replaces.

use serde::{Deserialize, Serialize};
use sqlx::{PgPool, Postgres, Transaction};
use utoipa::ToSchema;
use uuid::Uuid;

use crate::firing::queue_job;
use crate::source::{driver, Engine};

/// The job's kind.
pub const KIND: &str = "tables";
/// The most tables a listing keeps; the rest are cut, and it says so.
pub const MAX_TABLES: usize = 10_000;
/// The longest schema, table or column name a listing takes.
const MAX_NAME: usize = 128;

/// One table of a source, as a map job names it (`schema.table`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct SourceTable {
    pub schema: String,
    pub name: String,
    /// In the source's order.
    pub columns: Vec<SourceColumn>,
}

/// A column as the source's catalog declares it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct SourceColumn {
    pub name: String,
    /// As the engine reads the source's type: Trino's `information_schema`
    /// name from a Trino `tables` job (`integer`, `decimal(14,2)`; decision
    /// 0090, ruling 68), DuckDB's from an older listing.
    pub data_type: String,
}

/// What a `tables` job reports.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableListing {
    pub source: String,
    pub tables: Vec<SourceTable>,
    /// More than [`MAX_TABLES`] were there.
    #[serde(default)]
    pub truncated: bool,
}

impl TableListing {
    /// At most [`MAX_TABLES`], each named and its columns too, none of the
    /// system's schemas.
    pub fn is_valid(&self, driver: &str) -> bool {
        let named = |s: &str| !s.is_empty() && s.chars().count() <= MAX_NAME;
        let system = system_schemas(driver);
        self.tables.len() <= MAX_TABLES
            && self.tables.iter().all(|t| {
                named(&t.schema)
                    && named(&t.name)
                    && !system.contains(&t.schema.as_str())
                    && t.columns
                        .iter()
                        .all(|c| named(&c.name) && !c.data_type.is_empty())
            })
    }
}

/// The schemas a driver's own catalog lives in, left out of a listing
/// (decision 0042, point 4; verified by `tools/verify/source_tables.py`).
pub fn system_schemas(driver: &str) -> &'static [&'static str] {
    match driver {
        "postgres" => &["information_schema", "pg_catalog", "pg_toast"],
        "mysql" => &["information_schema", "mysql", "performance_schema", "sys"],
        // The system schemas as first verified (decision 0048); whether DuckDB's
        // extensions show the same is decision 0077's Open 8.
        "sqlserver" => &["information_schema", "sys"],
        "oracle" => &["information_schema"],
        _ => &[],
    }
}

/// Whether a source of this driver can be listed: DuckDB reads it.
pub fn listable(driver_name: &str) -> bool {
    driver(driver_name).is_some_and(|d| matches!(d.engine, Engine::DuckDb { .. }))
}

/// Queues the listing of `source`.
pub async fn queue(tx: &mut Transaction<'_, Postgres>, source: &str) -> Result<Uuid, sqlx::Error> {
    queue_job(tx, KIND, None, &serde_json::json!({ "source": source })).await
}

/// Keeps job `job`'s listing as its source's last, unless a newer job's
/// is there; nothing if the source is gone.
pub(crate) async fn store(
    pool: &PgPool,
    job: Uuid,
    listing: &TableListing,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO quality.source_tables (source, job, asked_at, tables, truncated)
         SELECT s.name, j.id, j.created_at, $3, $4
           FROM quality.jobs j JOIN quality.sources s ON s.name = $1
          WHERE j.id = $2
         ON CONFLICT (tenant_id, source) DO UPDATE
            SET job = EXCLUDED.job, asked_at = EXCLUDED.asked_at, listed_at = now(),
                tables = EXCLUDED.tables, truncated = EXCLUDED.truncated
          WHERE quality.source_tables.asked_at <= EXCLUDED.asked_at",
    )
    .bind(&listing.source)
    .bind(job)
    .bind(sqlx::types::Json(&listing.tables))
    .bind(listing.truncated)
    .execute(pool)
    .await
    .map(|_| ())
}

/// A listing's report, of the source it was asked for: refused (422) if
/// it is another's or does not hold together; dropped if the source was
/// deleted meanwhile, which takes its listing with it.
pub(crate) async fn report(
    pool: &PgPool,
    job: Uuid,
    parameters: &serde_json::Value,
    body: serde_json::Value,
) -> Result<axum::http::StatusCode, axum::http::StatusCode> {
    use axum::http::StatusCode;
    let listing: TableListing =
        serde_json::from_value(body).map_err(|_| StatusCode::UNPROCESSABLE_ENTITY)?;
    if parameters.get("source").and_then(|v| v.as_str()) != Some(listing.source.as_str()) {
        return Err(StatusCode::UNPROCESSABLE_ENTITY);
    }
    let failed = |e: sqlx::Error| {
        tracing::error!("tables report: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    };
    let driver: Option<String> =
        sqlx::query_scalar("SELECT driver FROM quality.sources WHERE name = $1")
            .bind(&listing.source)
            .fetch_optional(pool)
            .await
            .map_err(failed)?;
    let Some(driver) = driver else {
        return Ok(StatusCode::NO_CONTENT);
    };
    if !listing.is_valid(&driver) {
        return Err(StatusCode::UNPROCESSABLE_ENTITY);
    }
    store(pool, job, &listing).await.map_err(failed)?;
    Ok(StatusCode::NO_CONTENT)
}

/// Where a source's listing stands.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ListingState {
    /// Never asked.
    None,
    /// A job is listing it; the tables shown, if any, are the last ones.
    Listing,
    /// Its job failed an attempt and is waiting for, or on, the next one.
    Retrying,
    /// Its last job failed every attempt.
    Failed,
    Listed,
}

/// Why a listing's last attempt failed, from the worker's message (which
/// names the stage, never a value of the source).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ListingReason {
    /// The source refused the connection or its credentials.
    Connect,
    /// The engine failed reading the source's catalog.
    Engine,
    Other,
}

/// The reason in a `tables` job's error (`crates/worker/src/tables.rs`).
pub fn reason(error: &str) -> ListingReason {
    if error.ends_with("refused the job") {
        ListingReason::Connect
    } else if error.contains("the engine failed") {
        ListingReason::Engine
    } else {
        ListingReason::Other
    }
}

/// A source's last listing, and whether a newer one is coming.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
pub struct ListingView {
    pub state: ListingState,
    /// When the tables were read (RFC 3339).
    pub listed_at: Option<String>,
    pub tables: Vec<SourceTable>,
    pub truncated: bool,
    /// While retrying: the attempt coming or running, of `max_attempts`.
    pub attempt: Option<i32>,
    pub max_attempts: Option<i32>,
    /// While retrying or failed: why the last attempt failed.
    pub reason: Option<ListingReason>,
}

/// A listing's latest job.
#[derive(sqlx::FromRow)]
pub(crate) struct Latest {
    id: Uuid,
    state: String,
    attempts: i32,
    max_attempts: i32,
    error: Option<String>,
}

/// The listing kept, and the job that read it.
#[derive(sqlx::FromRow)]
pub(crate) struct Kept {
    job: Uuid,
    listed_at: chrono::DateTime<chrono::Utc>,
    tables: sqlx::types::Json<Vec<SourceTable>>,
    truncated: bool,
}

/// The source's last listing and its latest job's state.
pub async fn view(pool: &PgPool, source: &str) -> Result<ListingView, sqlx::Error> {
    let latest: Option<Latest> = sqlx::query_as(
        "SELECT id, state, attempts, max_attempts, error FROM quality.jobs
          WHERE kind = $1 AND parameters->>'source' = $2
          ORDER BY created_at DESC LIMIT 1",
    )
    .bind(KIND)
    .bind(source)
    .fetch_optional(pool)
    .await?;
    let kept: Option<Kept> = sqlx::query_as(
        "SELECT job, listed_at, tables, truncated FROM quality.source_tables WHERE source = $1",
    )
    .bind(source)
    .fetch_optional(pool)
    .await?;
    Ok(assemble(latest, kept))
}

/// A listing as people see it: the one kept, and where its latest job
/// stands (decision 0042, point 2; the lake's too, 0043).
pub(crate) fn assemble(latest: Option<Latest>, kept: Option<Kept>) -> ListingView {
    let mut out = ListingView {
        state: state(latest.as_ref(), kept.as_ref().map(|k| k.job)),
        listed_at: None,
        tables: Vec::new(),
        truncated: false,
        attempt: None,
        max_attempts: None,
        reason: None,
    };
    if let Some(l) =
        latest.filter(|_| matches!(out.state, ListingState::Retrying | ListingState::Failed))
    {
        out.reason = l.error.as_deref().map(reason);
        if out.state == ListingState::Retrying {
            // A queued job's attempts are the ones that failed; a running
            // one's include the one it is on.
            out.attempt = Some(l.attempts + i32::from(l.state == "queued"));
            out.max_attempts = Some(l.max_attempts);
        }
    }
    if let Some(k) = kept {
        out.listed_at = Some(
            k.listed_at
                .to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        );
        out.tables = k.tables.0;
        out.truncated = k.truncated;
    }
    out
}

/// Where a listing stands, from its latest job and the job of the one kept.
fn state(latest: Option<&Latest>, kept: Option<Uuid>) -> ListingState {
    match (latest, kept) {
        (None, None) => ListingState::None,
        (Some(l), Some(job)) if l.id == job => ListingState::Listed,
        (Some(l), _) if l.state == "failed" => ListingState::Failed,
        (Some(l), _) if l.state == "queued" || l.state == "running" => {
            if l.error.is_some() {
                ListingState::Retrying
            } else {
                ListingState::Listing
            }
        }
        // Done, but its report was not kept (the source was edited
        // meanwhile): what is kept is the last.
        (_, Some(_)) => ListingState::Listed,
        (Some(_), None) => ListingState::Failed,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn table(schema: &str, name: &str) -> SourceTable {
        SourceTable {
            schema: schema.into(),
            name: name.into(),
            columns: vec![SourceColumn {
                name: "id".into(),
                data_type: "INTEGER".into(),
            }],
        }
    }

    #[test]
    fn a_listing_holds_no_system_schema_and_no_empty_name() {
        let listing = |tables| TableListing {
            source: "core".into(),
            tables,
            truncated: false,
        };
        assert!(
            listing(vec![table("public", "clients"), table("loans", "credits")])
                .is_valid("postgres")
        );
        assert!(!listing(vec![table("pg_catalog", "pg_class")]).is_valid("postgres"));
        assert!(!listing(vec![table("sys", "x")]).is_valid("mysql"));
        assert!(!listing(vec![table("public", "")]).is_valid("postgres"));
        let mut unnamed = table("public", "t");
        unnamed.columns[0].name = String::new();
        assert!(!listing(vec![unnamed]).is_valid("postgres"));
        assert!(!listing(vec![table("public", &"x".repeat(129))]).is_valid("postgres"));
        assert!(!listing(vec![table("public", "t"); MAX_TABLES + 1]).is_valid("postgres"));
    }

    #[test]
    fn a_failed_attempt_says_why_and_a_retry_shows_it() {
        assert_eq!(
            reason("tables: the source core refused the job"),
            ListingReason::Connect
        );
        assert_eq!(
            reason("tables: the engine failed while listing"),
            ListingReason::Engine
        );
        assert_eq!(reason("report: 500"), ListingReason::Other);
        let job = |state: &str, error: Option<&str>| Latest {
            id: Uuid::nil(),
            state: state.into(),
            attempts: 1,
            max_attempts: 3,
            error: error.map(Into::into),
        };
        let refused = Some("tables: the source core refused the job");
        assert_eq!(
            state(Some(&job("queued", None)), None),
            ListingState::Listing
        );
        assert_eq!(
            state(Some(&job("queued", refused)), None),
            ListingState::Retrying
        );
        assert_eq!(
            state(Some(&job("running", refused)), None),
            ListingState::Retrying
        );
        assert_eq!(
            state(Some(&job("failed", refused)), None),
            ListingState::Failed
        );
        assert_eq!(
            state(Some(&job("done", None)), Some(Uuid::nil())),
            ListingState::Listed
        );
        assert_eq!(state(None, None), ListingState::None);
    }

    #[test]
    fn duckdb_drivers_are_listed() {
        assert!(listable("postgres") && listable("mysql"));
        // Read through DuckDB's extensions (decision 0077).
        assert!(listable("sqlserver") && listable("oracle"));
        // A driver this installation does not have (decision 0077).
        assert!(!listable("openedge") && !listable("nope"));
    }
}
