//! A `records` job's bundle (decision 0049; decision 0090, ruling 79): one
//! page of a source table's records, read as the source's own user with
//! its secrets as extra credentials, each column as text, a page and one
//! more at most. Its rows go to the core on the values route, which keeps
//! the page for its asker as it keeps a worker's.

use quality::bundle::{Bundle, Expect};
use quality::source_records::PAGE;
use quality_rules_trino::{self as trino, TablePath};

use crate::steps::Steps;
use crate::{CompileError, Job};

/// The id of the statement whose rows are the page.
pub const PAGE_READ: &str = "page";

/// What a page reads: the source's table, its columns in order, and the
/// page asked.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordsInput<'a> {
    pub source: &'a str,
    pub schema: &'a str,
    pub table: &'a str,
    pub columns: &'a [String],
    /// The column to order by, and whether descending.
    pub order: Option<(&'a str, bool)>,
    pub page: u32,
}

/// The page's bundle: `PAGE + 1` records from the page's first, the last
/// one only saying there are more.
pub fn records(job: &Job<'_>, input: &RecordsInput<'_>) -> Result<Bundle, CompileError> {
    if input.columns.is_empty() {
        return Err(CompileError::Incomplete("columns"));
    }
    let table = TablePath {
        catalog: format!("src_{}", input.source),
        namespace: input.schema.to_owned(),
        table: input.table.to_owned(),
    };
    let max_rows = u64::from(PAGE) + 1;
    let offset = u64::from(input.page) * u64::from(PAGE);
    let mut steps = Steps::default();
    steps.job_read(
        PAGE_READ.into(),
        trino::source_page(&table, input.columns, input.order, offset, max_rows)?,
        Expect::Values {
            columns: input
                .columns
                .iter()
                .map(|c| (c.clone(), "varchar".to_owned()))
                .collect(),
            max_rows,
        },
    );
    steps.source_reads_last();
    Ok(job.bundle(steps))
}

#[cfg(test)]
mod tests {
    use super::*;
    use quality::bundle::RunAs;

    #[test]
    fn a_page_is_one_bounded_values_read_of_the_source() {
        let job = Job {
            run: "0b9c6a1e-0000-4000-8000-000000000061",
            kind: "records",
            dataset: None,
            cut: None,
            attempt: 1,
            run_seq: None,
            catalog_commit: None,
        };
        let columns = ["loan_id".to_string(), "amount".to_string()];
        let input = RecordsInput {
            source: "core",
            schema: "public",
            table: "loans",
            columns: &columns,
            order: Some(("loan_id", true)),
            page: 2,
        };
        let b = records(&job, &input).unwrap();
        assert!(b.well_formed());
        let s = &b.statements[0];
        assert_eq!(
            (s.id.as_str(), s.run_as, s.reads_source),
            (PAGE_READ, RunAs::Job, true)
        );
        assert!(s
            .sql
            .ends_with("ORDER BY \"loan_id\" DESC OFFSET 2000 LIMIT 1001"));
        assert_eq!(
            s.expects,
            Expect::Values {
                columns: vec![
                    ("loan_id".into(), "varchar".into()),
                    ("amount".into(), "varchar".into())
                ],
                max_rows: 1001,
            }
        );
        let none = RecordsInput {
            columns: &[],
            ..input
        };
        assert!(records(&job, &none).is_err());
    }
}
