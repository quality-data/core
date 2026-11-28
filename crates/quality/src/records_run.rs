//! A `records` job run on Trino (decision 0090, ruling 79): the page
//! asked, from the job a person queued (decision 0049), compiled as one
//! values read of the source; its rows, posted on the values route,
//! become the page kept for its asker, masked on every read as today.

use axum::http::StatusCode;
use serde_json::Value;
use sqlx::PgPool;
use uuid::Uuid;

use crate::bundle::Expect;
use crate::job_inputs::FactsError;
use crate::records_api::{keep_page, KIND};
use crate::run_rows_api::{internal, refused, Refusal, RunRows, ValuesRun};
use crate::source_records::{RecordsAsk, RecordsPage};
use crate::source_tables;

/// The id of the statement whose rows are the page.
pub const PAGE_READ: &str = "page";

/// A queued page as the compile reads it: its source, what was asked, and
/// the table's columns in the source's order, from its last listing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordsJob {
    pub source: String,
    pub ask: RecordsAsk,
    pub columns: Vec<String>,
}

fn failed(e: sqlx::Error) -> FactsError {
    FactsError::Failed(e.to_string())
}

/// The `records` job `run`: refused when there is none, when what it asks
/// no longer checks, or when the source's last listing lacks the table or
/// the column it orders by.
pub async fn job_of(pool: &PgPool, run: &str) -> Result<RecordsJob, FactsError> {
    let id = Uuid::parse_str(run).map_err(|_| FactsError::Refused("invalid_run"))?;
    let parameters: Value =
        sqlx::query_scalar("SELECT parameters FROM quality.jobs WHERE id = $1 AND kind = $2")
            .bind(id)
            .bind(KIND)
            .fetch_optional(pool)
            .await
            .map_err(failed)?
            .ok_or(FactsError::Refused("no_job"))?;
    let source = parameters
        .get("source")
        .and_then(Value::as_str)
        .ok_or(FactsError::Refused("no_source"))?
        .to_owned();
    let ask: RecordsAsk =
        serde_json::from_value(parameters).map_err(|_| FactsError::Refused("page_invalid"))?;
    ask.check()
        .map_err(|_| FactsError::Refused("page_invalid"))?;
    let (schema, name) = ask
        .schema_and_table()
        .map_err(|_| FactsError::Refused("page_invalid"))?;
    let listing = source_tables::view(pool, &source).await.map_err(failed)?;
    let table = listing
        .tables
        .iter()
        .find(|t| t.schema == schema && t.name == name)
        .ok_or(FactsError::Refused("table_not_listed"))?;
    let columns: Vec<String> = table.columns.iter().map(|c| c.name.clone()).collect();
    if ask.order.as_ref().is_some_and(|o| !columns.contains(o)) {
        return Err(FactsError::Refused("page_invalid"));
    }
    Ok(RecordsJob {
        source,
        ask,
        columns,
    })
}

/// A page's rows: its values read's columns, each value text or empty.
pub(crate) async fn take(pool: &PgPool, run: &ValuesRun, rows: &RunRows) -> Result<(), Refusal> {
    let statement = run
        .bundle
        .statements
        .iter()
        .find(|s| s.id == rows.statement && s.id == PAGE_READ);
    let Some(Expect::Values { columns, .. }) = statement.map(|s| &s.expects) else {
        return Err(refused(StatusCode::UNPROCESSABLE_ENTITY, "not_the_page"));
    };
    let job = match job_of(pool, &run.run).await {
        Ok(job) => job,
        Err(FactsError::Refused(why)) => {
            return Err(refused(StatusCode::UNPROCESSABLE_ENTITY, why));
        }
        Err(FactsError::Failed(e)) => return Err(internal(e)),
    };
    let texts = rows
        .rows
        .iter()
        .map(|r| r.iter().map(|v| v.as_str().map(str::to_owned)).collect())
        .collect();
    let names = columns.iter().map(|(n, _)| n.clone()).collect();
    let page = RecordsPage::new(&job.source, &job.ask.table, names, texts);
    let id =
        Uuid::parse_str(&run.run).map_err(|_| refused(StatusCode::BAD_REQUEST, "invalid_run"))?;
    keep_page(pool, id, &page).await.map_err(internal)
}
