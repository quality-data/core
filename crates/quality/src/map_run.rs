//! A `map` job run on Trino (decision 0038; decision 0090, ruling 79): the
//! table asked, from the job a person queued, profiled over a sample as
//! one values read of the source; its row of counts, posted on the values
//! route, becomes the table's profile and the dataset's proposal as a
//! worker's report does.

use axum::http::StatusCode;
use serde_json::Value;
use sqlx::PgPool;
use uuid::Uuid;

use crate::job_inputs::FactsError;
use crate::proposal::{ColumnProfile, Looks, Patterns, TableProfile};
use crate::run_report_api::RunReportServices;
use crate::run_rows_api::{internal, refused, Refusal, RunRows, ValuesRun};
use crate::source_tables::{self, SourceColumn};

/// The job's kind.
pub const KIND: &str = "map";
/// The id of the statement whose row is the profile.
pub const PROFILE_READ: &str = "profile";
/// The sample when the job names none (decision 0038).
pub const SAMPLE: u64 = 10_000;
/// A column's aggregates in the profile's row: empty, distinct, shortest,
/// longest, then one per pattern.
const PER_COLUMN: usize = 4 + 6;

/// A queued map as the compile reads it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MapJob {
    pub dataset: String,
    pub source: String,
    /// The table as the job asked it.
    pub table: String,
    /// Its schema and name in the source's last listing.
    pub schema: String,
    pub name: String,
    /// Its columns in the source's order, with the type Trino lists.
    pub columns: Vec<SourceColumn>,
    pub sample: u64,
    pub patterns: Patterns,
    parameters: Value,
}

fn failed(e: sqlx::Error) -> FactsError {
    FactsError::Failed(e.to_string())
}

/// The `map` job `run`, its table found in its source's last listing: by
/// `schema.table`, or by a name only one schema has.
pub async fn job_of(pool: &PgPool, run: &str) -> Result<MapJob, FactsError> {
    let id = Uuid::parse_str(run).map_err(|_| FactsError::Refused("invalid_run"))?;
    let found: Option<(Option<String>, Value)> =
        sqlx::query_as("SELECT dataset, parameters FROM quality.jobs WHERE id = $1 AND kind = $2")
            .bind(id)
            .bind(KIND)
            .fetch_optional(pool)
            .await
            .map_err(failed)?;
    let (dataset, parameters) = found.ok_or(FactsError::Refused("no_job"))?;
    let dataset = dataset.ok_or(FactsError::Refused("job_incomplete"))?;
    let text = |key: &str| {
        parameters
            .get(key)
            .and_then(Value::as_str)
            .map(str::to_owned)
            .ok_or(FactsError::Refused("job_incomplete"))
    };
    let (source, table) = (text("source")?, text("table")?);
    let listing = source_tables::view(pool, &source).await.map_err(failed)?;
    let mut matching = listing
        .tables
        .iter()
        .filter(|t| match table.split_once('.') {
            Some((schema, name)) => t.schema == schema && t.name == name,
            None => t.name == table,
        });
    let (Some(t), None) = (matching.next(), matching.next()) else {
        return Err(FactsError::Refused("table_not_listed"));
    };
    Ok(MapJob {
        dataset,
        source,
        schema: t.schema.clone(),
        name: t.name.clone(),
        columns: t.columns.clone(),
        table,
        sample: parameters
            .get("sample")
            .and_then(Value::as_u64)
            .unwrap_or(SAMPLE),
        patterns: Patterns::of_job(&parameters),
        parameters,
    })
}

/// The table's profile from the row of counts: nullable as the listing
/// says (ruling 86), nullable when it does not; Trino names no primary
/// key, so none is the key and the person chooses it when mapping.
fn profile(job: &MapJob, row: &[Value]) -> Option<TableProfile> {
    if row.len() != 1 + job.columns.len() * PER_COLUMN {
        return None;
    }
    let n = |i: usize| row[i].as_u64().unwrap_or(0);
    let length = |i: usize| row[i].as_u64().and_then(|l| u32::try_from(l).ok());
    let columns = job
        .columns
        .iter()
        .enumerate()
        .map(|(i, c)| {
            let at = 1 + i * PER_COLUMN;
            ColumnProfile {
                name: c.name.clone(),
                declared_type: c.data_type.clone(),
                nullable: c.nullable.unwrap_or(true),
                primary_key: false,
                empty: n(at),
                distinct: n(at + 1),
                min_length: length(at + 2),
                max_length: length(at + 3),
                looks: Looks {
                    email: n(at + 4),
                    mobile: n(at + 5),
                    document: n(at + 6),
                    date: n(at + 7),
                    number: n(at + 8),
                    flag: n(at + 9),
                },
            }
        })
        .collect();
    Some(TableProfile {
        source: job.source.clone(),
        table: job.table.clone(),
        row_estimate: None,
        sampled: n(0),
        columns,
    })
}

/// A map's row of counts: its proposal published and its first cut opened.
pub(crate) async fn take(
    pool: &PgPool,
    services: &RunReportServices,
    run: &ValuesRun,
    rows: &RunRows,
) -> Result<(), Refusal> {
    if rows.statement != PROFILE_READ {
        return Err(refused(StatusCode::UNPROCESSABLE_ENTITY, "not_the_profile"));
    }
    let job = match job_of(pool, &run.run).await {
        Ok(job) => job,
        Err(FactsError::Refused(why)) => {
            return Err(refused(StatusCode::UNPROCESSABLE_ENTITY, why));
        }
        Err(FactsError::Failed(e)) => return Err(internal(e)),
    };
    let profile = rows
        .rows
        .first()
        .and_then(|row| profile(&job, row))
        .ok_or_else(|| refused(StatusCode::UNPROCESSABLE_ENTITY, "profile_invalid"))?;
    let ports = (
        services.drafts.as_deref(),
        services.proposal_alerts.as_ref(),
    );
    crate::job_map::publish(pool, ports, &job.parameters, &job.dataset, &profile)
        .await
        .map_err(|status| refused(status, "not_published"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn a_row_of_counts_is_the_tables_profile() {
        let job = MapJob {
            dataset: "clients".into(),
            source: "core".into(),
            table: "public.clients".into(),
            schema: "public".into(),
            name: "clients".into(),
            columns: vec![SourceColumn {
                name: "email".into(),
                data_type: "varchar".into(),
                nullable: Some(false),
            }],
            sample: 40,
            patterns: Patterns::default(),
            parameters: json!({}),
        };
        let row: Vec<Value> = [40, 20, 20, 15, 18, 20, 0, 0, 0, 0, 0]
            .into_iter()
            .map(Value::from)
            .collect();
        let p = profile(&job, &row).unwrap();
        assert_eq!(p.sampled, 40);
        let c = &p.columns[0];
        assert_eq!((c.empty, c.distinct, c.looks.email), (20, 20, 20));
        assert_eq!((c.min_length, c.max_length), (Some(15), Some(18)));
        assert!(!c.nullable && !c.primary_key);
        let mut short = row.clone();
        short.pop();
        assert!(profile(&job, &short).is_none());
        let mut empty = row;
        empty[3] = Value::Null;
        assert_eq!(profile(&job, &empty).unwrap().columns[0].min_length, None);
    }
}
