//! A job's bare counts through gold (decision 0090, ruling 53): a count
//! over silver, bronze or the scratch rows, which the job's own user may
//! not read back (point 7.3), goes into `job_counts` by an
//! `INSERT … SELECT`; the guard that waits on it, or the placeholder it
//! fills, reads it back as the dataset's report user, which reads gold.
//! The table carries `dataset` first, so gold's row filter by dataset
//! holds for it as for every other gold table.

use quality_rules_sql::quote_text;

use crate::gold::GOLD;
use crate::{Error, Statement};

/// The counts table, made at install with gold.
pub const JOB_COUNTS: &str = "\"lake\".\"quality_gold\".\"job_counts\"";

/// The column a count is read back as.
pub const COUNT_COLUMN: &str = "n";

/// The install's statement for the table.
pub(crate) fn create() -> Statement {
    Statement {
        sql: format!(
            "CREATE TABLE IF NOT EXISTS {GOLD}.\"job_counts\" \
             (dataset VARCHAR, run VARCHAR, step VARCHAR, n BIGINT) \
             WITH (partitioning = ARRAY['dataset'])"
        ),
    }
}

fn of_run(dataset: &str, run: &str) -> Result<String, Error> {
    Ok(format!(
        "{JOB_COUNTS} WHERE dataset = {} AND run = {}",
        quote_text(dataset)?,
        quote_text(run)?
    ))
}

/// `count`, a query of one row of one `bigint`, written as the run's
/// count `step`; run as the job's own user.
pub fn count_into(
    dataset: &str,
    run: &str,
    step: &str,
    count: &Statement,
) -> Result<Statement, Error> {
    Ok(Statement {
        sql: format!(
            "INSERT INTO {JOB_COUNTS} (dataset, run, step, n) \
             SELECT {}, {}, {}, CAST(q.c AS BIGINT) FROM ({}) AS q (c)",
            quote_text(dataset)?,
            quote_text(run)?,
            quote_text(step)?,
            count.sql.replace('\n', " ")
        ),
    })
}

/// The run's count `step` read back: one row, one `bigint` named
/// [`COUNT_COLUMN`]; run as the report user.
pub fn count_read(dataset: &str, run: &str, step: &str) -> Result<Statement, Error> {
    Ok(Statement {
        sql: format!(
            "SELECT n FROM {} AND step = {}",
            of_run(dataset, run)?,
            quote_text(step)?
        ),
    })
}

/// The counts an earlier attempt of the run left, read as the report
/// user: a retry clears them first, so each step is written once.
pub fn run_counts(dataset: &str, run: &str) -> Result<Statement, Error> {
    Ok(Statement {
        sql: format!("SELECT count(*) FROM {}", of_run(dataset, run)?),
    })
}

/// An earlier attempt's counts go, only after [`run_counts`] showed some
/// (an empty `DELETE` fails its commit).
pub fn delete_run_counts(dataset: &str, run: &str) -> Result<Statement, Error> {
    Ok(Statement {
        sql: format!("DELETE FROM {}", of_run(dataset, run)?),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_count_goes_in_by_insert_and_comes_back_by_its_step() {
        let count = Statement {
            sql: "SELECT count(*)\nFROM s".into(),
        };
        assert_eq!(
            count_into("loans", "r-1", "scratch", &count).unwrap().sql,
            "INSERT INTO \"lake\".\"quality_gold\".\"job_counts\" (dataset, run, step, n) \
             SELECT 'loans', 'r-1', 'scratch', CAST(q.c AS BIGINT) FROM (SELECT count(*) FROM s) AS q (c)"
        );
        assert_eq!(
            count_read("loans", "r-1", "scratch").unwrap().sql,
            "SELECT n FROM \"lake\".\"quality_gold\".\"job_counts\" \
             WHERE dataset = 'loans' AND run = 'r-1' AND step = 'scratch'"
        );
        assert!(run_counts("loans", "r-1")
            .unwrap()
            .sql
            .starts_with("SELECT count(*) FROM"));
        assert!(delete_run_counts("loans", "r-1")
            .unwrap()
            .sql
            .ends_with("WHERE dataset = 'loans' AND run = 'r-1'"));
        assert!(count_read("lo\u{0}ans", "r", "s").is_err());
        assert!(create()
            .sql
            .ends_with("WITH (partitioning = ARRAY['dataset'])"));
    }
}
