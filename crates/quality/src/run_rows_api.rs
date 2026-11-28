//! A run's values from its job runner (decision 0090, rulings 79 and 80):
//! `POST /internal/quality/v1/runs/{run}/rows {statement, rows}` with the
//! bundle route's machine token. The core takes rows only for a statement
//! the run's pinned bundle declares as a values read, within its columns
//! and its row limit, and hands them to the run's kind, which masks,
//! stores and audits them as it does today (decisions 0043 and 0069).
//! Nothing here logs, prints or keeps a value: only the run, the kind,
//! the statement and how many rows.

use axum::extract::{Path, State};
use axum::http::{header::AUTHORIZATION, HeaderMap, StatusCode};
use axum::routing::post;
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{json, Value};
use sqlx::PgPool;

use crate::bundle::{self, Bundle, Expect};
use crate::run_report_api::{takes, RunReportServices};

/// What a job runner posts of one values read.
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunRows {
    pub statement: String,
    /// The answer's columns, for a read that declares none (ruling 84).
    #[serde(default)]
    pub columns: Option<Vec<String>>,
    pub rows: Vec<Vec<Value>>,
}

impl std::fmt::Debug for RunRows {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RunRows")
            .field("statement", &self.statement)
            .field("columns", &self.columns.as_ref().map(Vec::len))
            .field("rows", &self.rows.len())
            .finish()
    }
}

/// The run the rows belong to, as its bundle was pinned.
#[derive(Debug, Clone)]
pub(crate) struct ValuesRun {
    pub(crate) run: String,
    pub(crate) kind: String,
    pub(crate) bundle: Bundle,
}

#[derive(Clone)]
struct Api {
    pool: PgPool,
    services: RunReportServices,
}

/// `POST /internal/quality/v1/runs/{run}/rows`.
pub fn routes(pool: PgPool, services: RunReportServices) -> Router {
    Router::new()
        .route("/internal/quality/v1/runs/{run}/rows", post(rows))
        .with_state(Api { pool, services })
}

pub(crate) type Refusal = (StatusCode, Json<Value>);

pub(crate) fn refused(status: StatusCode, error: &str) -> Refusal {
    (status, Json(json!({ "error": error })))
}

pub(crate) fn internal(e: impl std::fmt::Display) -> Refusal {
    tracing::error!("run rows: {e}");
    refused(StatusCode::INTERNAL_SERVER_ERROR, "internal")
}

/// A pinned bundle's row: kind, digest, the signed bundle, and whether the
/// run has reported.
type PinnedRow = (String, String, String, bool);

/// The most columns an answer that declares its own may have.
const MAX_ANSWER_COLUMNS: usize = 1_000;

/// Whether `r` fit the values read it names in `b`: its columns (declared
/// by the bundle, or by the answer for a text read), each value of its
/// type (or null), at most its rows.
fn values_fit(b: &Bundle, r: &RunRows) -> Result<(), &'static str> {
    let s = b
        .statements
        .iter()
        .find(|s| s.id == r.statement)
        .ok_or("no_such_statement")?;
    let (types, max_rows): (Vec<&str>, u64) = match (&s.expects, &r.columns) {
        (Expect::Values { columns, max_rows }, None) => {
            (columns.iter().map(|(_, t)| t.as_str()).collect(), *max_rows)
        }
        (Expect::Text { max_rows }, Some(names))
            if !names.is_empty()
                && names.len() <= MAX_ANSWER_COLUMNS
                && names.iter().all(|n| n.chars().count() <= 128) =>
        {
            (vec!["varchar"; names.len()], *max_rows)
        }
        (Expect::Values { .. } | Expect::Text { .. }, _) => return Err("rows_not_allowed"),
        _ => return Err("not_a_values_read"),
    };
    let rows = &r.rows;
    let fit = u64::try_from(rows.len()).is_ok_and(|n| n <= max_rows)
        && rows.iter().all(|row| {
            row.len() == types.len() && row.iter().zip(&types).all(|(v, ty)| takes(ty, v))
        });
    if fit {
        Ok(())
    } else {
        Err("rows_not_allowed")
    }
}

async fn rows(
    State(api): State<Api>,
    Path(run): Path<String>,
    headers: HeaderMap,
    Json(r): Json<RunRows>,
) -> Result<StatusCode, Refusal> {
    headers
        .get(AUTHORIZATION)
        .and_then(|h| h.to_str().ok())
        .and_then(|h| h.strip_prefix("Bearer "))
        .and_then(|t| api.services.tokens.client(t))
        .ok_or_else(|| refused(StatusCode::UNAUTHORIZED, "unauthorized"))?;
    let pinned: Option<PinnedRow> = sqlx::query_as(
        "SELECT b.kind, b.digest, b.bundle,
                EXISTS (SELECT 1 FROM quality.job_reports r WHERE r.run = b.run)
           FROM quality.job_bundles b WHERE b.run = $1",
    )
    .bind(&run)
    .fetch_optional(&api.pool)
    .await
    .map_err(internal)?;
    let (kind, digest, token, reported) =
        pinned.ok_or_else(|| refused(StatusCode::NOT_FOUND, "no_bundle"))?;
    if reported {
        return Err(refused(StatusCode::CONFLICT, "already_reported"));
    }
    let b = bundle::verify(api.services.signer.as_ref(), &token, &run, &digest)
        .map_err(|_| internal("the pinned bundle does not verify"))?;
    values_fit(&b, &r).map_err(|why| refused(StatusCode::UNPROCESSABLE_ENTITY, why))?;
    let taken = ValuesRun {
        run,
        kind,
        bundle: b,
    };
    hand_over(&api, &taken, &r).await?;
    tracing::info!(
        run = taken.run,
        kind = taken.kind,
        statement = r.statement,
        rows = r.rows.len(),
        "run values taken"
    );
    Ok(StatusCode::NO_CONTENT)
}

/// The rows to the run's kind, which keeps them as it keeps a worker's
/// today. A kind that reads no values takes none.
async fn hand_over(api: &Api, run: &ValuesRun, rows: &RunRows) -> Result<(), Refusal> {
    if run.kind == crate::records_api::KIND {
        return crate::records_run::take(&api.pool, run, rows).await;
    }
    if run.kind == crate::console_api::KIND {
        return crate::console_run::take(&api.pool, run, rows).await;
    }
    if run.kind == crate::test_rule_run::KIND {
        return crate::test_rule_run::take(&api.pool, run, rows).await;
    }
    if run.kind == crate::map_run::KIND {
        return crate::map_run::take(&api.pool, &api.services, run, rows).await;
    }
    tracing::warn!(
        run = run.run,
        kind = run.kind,
        "values for a kind that takes none"
    );
    Err(refused(
        StatusCode::UNPROCESSABLE_ENTITY,
        "kind_takes_no_values",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn debug_names_the_statement_and_counts_the_rows() {
        let r = RunRows {
            statement: "page".into(),
            columns: None,
            rows: vec![vec![json!("Synthetic Name")]],
        };
        let shown = format!("{r:?}");
        assert_eq!(
            shown,
            "RunRows { statement: \"page\", columns: None, rows: 1 }"
        );
    }
}
