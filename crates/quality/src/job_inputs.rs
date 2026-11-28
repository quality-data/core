//! What a job's compile reads from the core's database (decision 0090,
//! amendment A1 point 2): the server gathers it and hands it to the pure
//! compiler, `quality-jobs`. Names, numbers and settings only.

use sqlx::PgPool;

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
