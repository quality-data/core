//! An `extract` job's bundle (decision 0090, point 9, rulings 59, 64, 66,
//! 67 and 68): one load of the mapped source table into the dataset's
//! bronze (made beforehand by a `prepare`), its load id the run's. A retry
//! first removes its own earlier attempt's rows of that load, after a
//! count through `job_counts`; the source is read with its secrets as
//! extra credentials; the last statement reads bronze's columns and types,
//! which the job's report hands back to the core.

use quality::bundle::{Bundle, Expect};
use quality_rules_trino as trino;
use quality_rules_trino::{Cut, TablePath};

use crate::steps::Steps;
use crate::{CompileError, Job};

/// What an extract reads: the source table and bronze, and the mapped
/// columns with the Trino types the source's listing gives them.
#[derive(Debug, Clone)]
pub struct ExtractInput<'a> {
    /// `src_<source>`, the source's schema and table.
    pub source: TablePath,
    pub bronze: TablePath,
    pub columns: &'a [(String, String)],
}

/// The most columns bronze's listing gives back.
const MAX_COLUMNS: u64 = 4096;

/// The extract's bundle: the run's earlier counts and its earlier load
/// removed, the load inserted, then bronze's columns read back.
pub fn extract(job: &Job<'_>, input: &ExtractInput<'_>) -> Result<Bundle, CompileError> {
    let dataset = job.dataset()?;
    let cut =
        Cut::parse(job.cut.ok_or(CompileError::Incomplete("cut"))?).map_err(trino::Error::from)?;
    let mut steps = Steps::default();
    steps.count_then_delete(
        "counts",
        trino::run_counts(dataset, job.run)?,
        trino::delete_run_counts(dataset, job.run)?,
    );
    let earlier = trino::load_rows(&input.bronze, &cut, job.run)?;
    let after = steps.bare_count(dataset, job.run, "earlier_load", &earlier)?;
    steps.delete_after(
        "earlier_load".into(),
        trino::delete_load(&input.bronze, &cut, job.run)?,
        after,
    );
    let load = trino::extract(
        &input.bronze,
        &input.source,
        input.columns,
        (&cut, job.run),
        None,
    )?;
    steps.source_insert("load".into(), load);
    steps.job_read(
        "bronze_columns".into(),
        trino::table_columns(&input.bronze)?,
        Expect::Rows {
            columns: vec![
                ("column_name".into(), "varchar".into()),
                ("data_type".into(), "varchar".into()),
            ],
            max_rows: MAX_COLUMNS,
        },
    );
    Ok(job.bundle(steps))
}

#[cfg(test)]
mod tests {
    use super::*;
    use quality::bundle::RunAs;

    fn path(catalog: &str, ns: &str, table: &str) -> TablePath {
        TablePath {
            catalog: catalog.into(),
            namespace: ns.into(),
            table: table.into(),
        }
    }

    #[test]
    fn an_extract_removes_its_earlier_load_then_loads_and_reports_bronze() {
        let columns = [("loan_id".to_string(), "varchar(20)".to_string())];
        let input = ExtractInput {
            source: path("src_core", "public", "loans"),
            bronze: path("lake", "loans_bronze", "loans"),
            columns: &columns,
        };
        let job = Job {
            run: "0b9c6a1e-0000-4000-8000-000000000005",
            kind: "extract",
            dataset: Some("loans"),
            cut: Some("2026-10-31"),
            attempt: 2,
            run_seq: None,
            catalog_commit: Some("c0ffee"),
        };
        let b = extract(&job, &input).unwrap();
        assert!(b.well_formed());
        let ids: Vec<&str> = b.statements.iter().map(|s| s.id.as_str()).collect();
        assert_eq!(
            ids,
            [
                "counts_rows",
                "counts",
                "earlier_load_count",
                "earlier_load_rows",
                "earlier_load",
                "load",
                "bronze_columns"
            ]
        );
        let load = &b.statements[5];
        assert!(load.reads_source && load.run_as == RunAs::Job);
        assert!(load.sql.contains(
            "DATE '2026-10-31', '0b9c6a1e-0000-4000-8000-000000000005' FROM \"src_core\".\"public\".\"loans\""
        ));
        let earlier = &b.statements[4];
        assert!(earlier
            .sql
            .ends_with("_load = '0b9c6a1e-0000-4000-8000-000000000005'"));
        assert_eq!(
            earlier.only_if_rows.as_ref().unwrap().after,
            "earlier_load_rows"
        );
        let columns = &b.statements[6];
        assert_eq!(columns.run_as, RunAs::Job);
        assert!(!b.statements.iter().any(|s| s.sql.contains("CREATE")));
        assert_eq!(
            extract(&Job { cut: None, ..job }, &input),
            Err(CompileError::Incomplete("cut"))
        );
    }
}
