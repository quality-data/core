//! A `prepare` job's bundle (decision 0090, ruling 66): a dataset's bronze
//! and silver tables created from its mapping if missing, and any mapped
//! column they lack added, as the dataset's maintenance user. The
//! orchestrator runs it before the dataset's first extract and when the
//! mapping's columns change; an extract never creates a table.

use quality::bundle::Bundle;
use quality::mapping::Mapping;
use quality_rules_trino as trino;
use quality_rules_trino::TablePath;

use crate::steps::Steps;
use crate::{CompileError, Job};

/// The catalog a prepare's statements name: the maintenance catalog over
/// the same warehouse (point 6.4), the only one `job_maint_<d>` is granted.
pub const MAINT_CATALOG: &str = "lake_maint";

/// What a prepare reads: the mapping at the catalog's `main`, the
/// dataset's two tables and the source's listed columns.
#[derive(Debug, Clone)]
pub struct PrepareInput<'a> {
    pub mapping: &'a Mapping,
    pub bronze: TablePath,
    pub silver: TablePath,
    /// The mapped table's columns and their Trino types, as the source's
    /// listing gives them (ruling 68).
    pub source_columns: &'a [(String, String)],
}

/// The prepare's bundle: `bronze`, `bronze_<n>`, `silver`, `silver_<n>`,
/// each table named in [`MAINT_CATALOG`] whatever catalog it was given.
pub fn prepare(job: &Job<'_>, input: &PrepareInput<'_>) -> Result<Bundle, CompileError> {
    job.dataset()?;
    let on_maint = |t: &TablePath| TablePath {
        catalog: MAINT_CATALOG.to_owned(),
        ..t.clone()
    };
    let statements = trino::prepare(
        &on_maint(&input.bronze),
        &on_maint(&input.silver),
        input.mapping,
        input.source_columns,
    )?;
    let mut steps = Steps::default();
    let mut layers = ["bronze", "silver"].into_iter();
    let (mut layer, mut n) = ("", 0);
    for s in statements {
        if s.sql.starts_with("CREATE TABLE") {
            layer = layers.next().unwrap_or("table");
            n = 0;
        }
        let id = if n == 0 {
            layer.to_string()
        } else {
            format!("{layer}_{n}")
        };
        n += 1;
        steps.done(id, s);
    }
    Ok(job.bundle(steps))
}

#[cfg(test)]
mod tests {
    use super::*;
    use quality::bundle::{Expect, RunAs};

    #[test]
    fn a_prepare_creates_and_completes_both_tables_as_the_jobs_user() {
        let mapping = Mapping::parse(
            "dataset: loans\nsource: loans\nfields:\n  Code: { column: code, type: text }\n",
        )
        .unwrap();
        let path = |ns: &str| TablePath {
            catalog: "lake".into(),
            namespace: ns.into(),
            table: "loans".into(),
        };
        let job = Job {
            run: "p-1",
            kind: "prepare",
            dataset: Some("loans"),
            cut: None,
            attempt: 1,
            run_seq: None,
            catalog_commit: Some("c0ffee"),
        };
        let input = PrepareInput {
            mapping: &mapping,
            bronze: path("loans_bronze"),
            silver: path("loans_silver"),
            source_columns: &[("code".into(), "varchar".into())],
        };
        let b = prepare(&job, &input).unwrap();
        assert!(b.well_formed());
        let ids: Vec<&str> = b.statements.iter().map(|s| s.id.as_str()).collect();
        assert_eq!(ids[0], "bronze");
        assert!(ids.contains(&"silver") && ids.contains(&"silver_1"));
        for s in &b.statements {
            assert_eq!(s.run_as, RunAs::Job);
            assert_eq!(s.expects, Expect::Done);
            assert!(s.only_if_rows.is_none());
            assert!(s.sql.contains("\"lake_maint\"."), "{}", s.id);
            assert!(!s.sql.contains("\"lake\"."), "{}", s.id);
        }
        let none = Job {
            dataset: None,
            ..job
        };
        assert_eq!(
            prepare(&none, &input),
            Err(CompileError::Incomplete("dataset"))
        );
    }
}
