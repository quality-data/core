//! What a job's compile reads from the core's database (decision 0090,
//! amendment A1 point 2): the server gathers it and hands it to the pure
//! compiler, `quality-jobs`. Names, numbers and settings only.

use chrono::NaiveDate;
use quality_rules::check::{compile, Compiled};
use sqlx::PgPool;

use crate::catalog::Catalog;
use crate::mapping::Mapping;

/// The next value of gold's run order: each compile of a gold writer
/// takes one, so a later compile of the same cut wins the flip (point 4).
pub async fn next_run_seq(pool: &PgPool) -> Result<i64, sqlx::Error> {
    sqlx::query_scalar("SELECT nextval('quality.gold_run_seq')")
        .fetch_one(pool)
        .await
}

/// One of a dataset's remediation SLAs as approved now (decision 0059).
#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
pub struct DatasetSla {
    pub dimension: String,
    pub grp: Option<String>,
    pub days: i32,
}

/// A dataset's SLAs, in a fixed order so two compiles write the same SQL.
pub async fn dataset_slas(pool: &PgPool, dataset: &str) -> Result<Vec<DatasetSla>, sqlx::Error> {
    sqlx::query_as(
        "SELECT dimension, grp, days FROM quality.remediation_slas
         WHERE dataset = $1 ORDER BY dimension, grp NULLS FIRST",
    )
    .bind(dataset)
    .fetch_all(pool)
    .await
}

/// What an evaluate's compile reads besides the job (decision 0090,
/// amendment A1 point 3): the dataset's mapping and rules at the catalog's
/// `main`, the cut's accepted load and the previous cut of the index.
#[derive(Debug, Clone)]
pub struct EvaluateFacts {
    /// The catalog's commit the mapping and rules were read at.
    pub commit: String,
    pub mapping: Mapping,
    pub rules: Vec<Compiled>,
    /// The cut's extraction, the load its bronze and silver rows carry.
    pub load: Option<String>,
    /// The dataset's latest cut before this one in the index.
    pub previous: Option<NaiveDate>,
}

/// Why an evaluate's facts could not be gathered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FactsError {
    /// A class the job runner is told: the job cannot be compiled as it is.
    Refused(&'static str),
    /// The database or the catalog failed.
    Failed(String),
}

/// `dataset`'s facts for an evaluate of `cut`. A rules file that does not
/// compile against the mapping, or that references another dataset (its
/// cut is not resolved for Trino yet), refuses the job.
pub async fn evaluate_facts(
    pool: &PgPool,
    catalog: &dyn Catalog,
    dataset: &str,
    cut: NaiveDate,
) -> Result<EvaluateFacts, FactsError> {
    let snapshot = catalog
        .snapshot(dataset)
        .await
        .map_err(FactsError::Failed)?
        .ok_or(FactsError::Refused("no_mapping"))?;
    let mapping =
        Mapping::parse(&snapshot.mapping).map_err(|_| FactsError::Refused("mapping_invalid"))?;
    let schema = mapping.schema();
    let mut rules = Vec::new();
    for source in &snapshot.rules {
        let file =
            quality_rules::parse(&source.yaml).map_err(|_| FactsError::Refused("rules_invalid"))?;
        if !file.references.is_empty() {
            return Err(FactsError::Refused("references_not_compiled"));
        }
        let (compiled, problems) = compile(&file, &schema);
        if !problems.is_empty() {
            return Err(FactsError::Refused("rules_invalid"));
        }
        rules.extend(compiled);
    }
    let failed = |e: sqlx::Error| FactsError::Failed(e.to_string());
    let load: Option<Option<String>> = sqlx::query_scalar(
        "SELECT extract_job::text FROM quality.cuts WHERE dataset = $1 AND cut = $2",
    )
    .bind(dataset)
    .bind(cut)
    .fetch_optional(pool)
    .await
    .map_err(failed)?;
    let previous: Option<NaiveDate> = sqlx::query_scalar(
        "SELECT max(cut) FROM quality.run_summary WHERE dataset = $1 AND cut < $2",
    )
    .bind(dataset)
    .bind(cut)
    .fetch_one(pool)
    .await
    .map_err(failed)?;
    Ok(EvaluateFacts {
        commit: snapshot.commit,
        mapping,
        rules,
        load: load.flatten(),
        previous,
    })
}
