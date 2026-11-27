//! A console statement's bundle (decision 0054; decision 0090, rulings 80
//! and 84): the statement wrapped as one query with a hard limit, run as
//! the run's own console user; a text read whose columns come with its
//! answer, posted on the values route.

use quality::bundle::{Bundle, Expect};
use quality::console::{wrapped, MAX_ROWS};
use quality::console_run::RESULT_READ;
use quality_rules_trino::Statement;

use crate::steps::Steps;
use crate::{CompileError, Job};

/// The statement's bundle; one that is empty, too long or several is
/// refused as at the ask.
pub fn console(job: &Job<'_>, sql: &str) -> Result<Bundle, CompileError> {
    let sql = wrapped(sql).map_err(|_| CompileError::Incomplete("statement"))?;
    let mut steps = Steps::default();
    steps.job_read(
        RESULT_READ.into(),
        Statement { sql },
        Expect::Text {
            max_rows: MAX_ROWS as u64 + 1,
        },
    );
    Ok(job.bundle(steps))
}

#[cfg(test)]
mod tests {
    use super::*;
    use quality::bundle::RunAs;

    #[test]
    fn a_statement_is_one_limited_text_read_as_its_runs_user() {
        let job = Job {
            run: "0b9c6a1e-0000-4000-8000-000000000091",
            kind: "query",
            dataset: None,
            cut: None,
            attempt: 1,
            run_seq: None,
            catalog_commit: None,
        };
        let b = console(&job, "SELECT count(*) FROM lake.loans_bronze.loans;").unwrap();
        assert!(b.well_formed());
        let s = &b.statements[0];
        assert_eq!((s.id.as_str(), s.run_as), (RESULT_READ, RunAs::Job));
        assert_eq!(s.expects, Expect::Text { max_rows: 201 });
        assert!(s.sql.ends_with("\n) AS q LIMIT 201"));
        assert!(console(&job, "SELECT 1; DROP TABLE t").is_err());
    }
}
