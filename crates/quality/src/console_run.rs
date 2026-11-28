//! A console statement run on Trino (decision 0054; decision 0090,
//! rulings 80 and 84): the statement a person queued, wrapped and limited
//! as today, run as `job_console_<run>`, a user Trino lets read only the
//! bronze, silver and gold of the datasets the asker may view; its rows,
//! posted on the values route with the answer's columns, become the
//! result kept for its asker, masked on every read as today.

use axum::http::StatusCode;
use serde_json::Value;
use sqlx::PgPool;
use uuid::Uuid;

use crate::console::{ConsoleResult, MAX_ROWS};
use crate::console_api::{keep_result, KIND};
use crate::job_inputs::FactsError;
use crate::run_rows_api::{internal, refused, Refusal, RunRows, ValuesRun};

/// The id of the statement whose rows are the result.
pub const RESULT_READ: &str = "result";

/// A queued statement as the compile reads it.
#[derive(Clone, PartialEq, Eq)]
pub struct ConsoleJob {
    pub sql: String,
    /// The datasets the asker may view when it was queued.
    pub datasets: Vec<String>,
}

impl std::fmt::Debug for ConsoleJob {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConsoleJob")
            .field("datasets", &self.datasets)
            .finish_non_exhaustive()
    }
}

/// The Trino user's target for run `job`: `r` and its 32 hex digits, a
/// name Trino's user mapping takes (`job_console_r<hex>`).
pub fn console_target(job: Uuid) -> String {
    format!("r{}", job.simple())
}

/// The statement `run` queued.
pub async fn job_of(pool: &PgPool, run: &str) -> Result<ConsoleJob, FactsError> {
    let id = Uuid::parse_str(run).map_err(|_| FactsError::Refused("invalid_run"))?;
    let p: Value =
        sqlx::query_scalar("SELECT parameters FROM quality.jobs WHERE id = $1 AND kind = $2")
            .bind(id)
            .bind(KIND)
            .fetch_optional(pool)
            .await
            .map_err(|e| FactsError::Failed(e.to_string()))?
            .ok_or(FactsError::Refused("no_job"))?;
    let sql = p
        .get("sql")
        .and_then(Value::as_str)
        .ok_or(FactsError::Refused("job_incomplete"))?
        .to_owned();
    let datasets: Vec<String> = p
        .get("datasets")
        .and_then(|d| serde_json::from_value(d.clone()).ok())
        .unwrap_or_default();
    if datasets.is_empty() {
        return Err(FactsError::Refused("no_dataset"));
    }
    Ok(ConsoleJob { sql, datasets })
}

/// The statement's rows: its result, the 201st only saying there are more.
pub(crate) async fn take(pool: &PgPool, run: &ValuesRun, rows: &RunRows) -> Result<(), Refusal> {
    let known = run.bundle.statements.iter().any(|s| s.id == rows.statement);
    let (true, Some(columns)) = (rows.statement == RESULT_READ && known, &rows.columns) else {
        return Err(refused(StatusCode::UNPROCESSABLE_ENTITY, "not_the_result"));
    };
    let mut texts: Vec<Vec<Option<String>>> = rows
        .rows
        .iter()
        .map(|r| r.iter().map(|v| v.as_str().map(str::to_owned)).collect())
        .collect();
    let more = texts.len() > MAX_ROWS;
    texts.truncate(MAX_ROWS);
    let result = ConsoleResult {
        columns: columns.clone(),
        rows: texts,
        more,
        error: None,
    };
    let id =
        Uuid::parse_str(&run.run).map_err(|_| refused(StatusCode::BAD_REQUEST, "invalid_run"))?;
    keep_result(pool, id, &result).await.map_err(internal)
}

/// A statement that failed on Trino: its result says only the error's
/// class the job runner reported, never the engine's message.
pub(crate) async fn failed(pool: &PgPool, run: &str, class: Option<&str>) -> Result<(), Refusal> {
    let id = Uuid::parse_str(run).map_err(|_| refused(StatusCode::BAD_REQUEST, "invalid_run"))?;
    let result = ConsoleResult {
        columns: Vec::new(),
        rows: Vec::new(),
        more: false,
        error: Some(class.unwrap_or("engine").to_owned()),
    };
    keep_result(pool, id, &result).await.map_err(internal)
}
