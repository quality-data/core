//! A `rule_test` job's bundle (decision 0090, ruling 54): rules as typed,
//! run on Trino through the normal job path against a chosen cut, writing
//! to scratch only: each rule's counts go into `job_counts` and come back
//! as the last statement's rows, counts only (point 7.3). Gold's results
//! are never written.

use quality::bundle::{Bundle, Expect};
use quality_rules::check::Compiled;
use quality_rules::sql::Tables;
use quality_rules_trino as trino;
use quality_rules_trino::{Cut, TablePath};

use crate::evaluate::pin;
use crate::steps::Steps;
use crate::{CompileError, Job};

/// What a rule test reads: the rules as typed and the dataset's silver.
#[derive(Debug, Clone)]
pub struct RuleTestInput<'a> {
    pub rules: &'a [Compiled],
    /// The tables the rules' references read, resolved by the core.
    pub tables: &'a Tables,
    pub silver: TablePath,
    /// The chosen cut's accepted load (`None`: rows from before loads).
    pub load: Option<&'a str>,
}

/// The id of the statement whose rows are the test's counts.
pub const COUNTS: &str = "counts_read";

/// The test's bundle: an earlier attempt's counts cleared, silver pinned,
/// the counts written, then read back by step.
pub fn rule_test(job: &Job<'_>, input: &RuleTestInput<'_>) -> Result<Bundle, CompileError> {
    let dataset = job.dataset()?;
    let cut =
        Cut::parse(job.cut.ok_or(CompileError::Incomplete("cut"))?).map_err(trino::Error::from)?;
    let mut steps = Steps::default();
    steps.count_then_delete(
        "counts",
        trino::run_counts(dataset, job.run)?,
        trino::delete_run_counts(dataset, job.run)?,
    );
    let silver = pin(
        &mut steps,
        &input.silver,
        "silver_snapshot",
        &cut,
        input.load,
    )?;
    steps.insert(
        "rule_counts".into(),
        trino::rule_test_counts(dataset, job.run, &silver, input.rules, input.tables, &cut)?,
    );
    let rows = 3 * u64::try_from(input.rules.len().max(1)).unwrap_or(u64::MAX / 3);
    steps.read(
        COUNTS.into(),
        trino::run_count_rows(dataset, job.run)?,
        Expect::Rows {
            columns: vec![
                ("step".into(), "varchar".into()),
                (trino::COUNT_COLUMN.into(), "bigint".into()),
            ],
            max_rows: rows,
        },
    );
    Ok(job.bundle(steps))
}

#[cfg(test)]
mod tests {
    use super::*;
    use quality::bundle::RunAs;
    use quality_rules::check::{compile, FieldType, Schema};

    fn job(cut: Option<&'static str>) -> Job<'static> {
        Job {
            run: "t-1",
            kind: "rule_test",
            dataset: Some("loans"),
            cut,
            attempt: 1,
            run_seq: None,
            catalog_commit: None,
        }
    }

    #[test]
    fn a_test_writes_counts_only_and_reads_them_back() {
        let yaml = "dataset: loans\nrules:\n  \
                    - { id: R1, field: Code, check: required, severity: major, alert: a }\n";
        let mut schema = Schema::default();
        schema.fields.insert("Code".into(), FieldType::Text);
        let (rules, _) = compile(&quality_rules::parse(yaml).unwrap(), &schema);
        let input = RuleTestInput {
            rules: &rules,
            tables: &Tables::new(),
            silver: TablePath {
                catalog: "lake".into(),
                namespace: "loans_silver".into(),
                table: "loans".into(),
            },
            load: None,
        };
        let b = rule_test(&job(Some("2026-10-31")), &input).unwrap();
        assert!(b.well_formed());
        let ids: Vec<&str> = b.statements.iter().map(|s| s.id.as_str()).collect();
        assert_eq!(
            ids,
            [
                "counts_rows",
                "counts",
                "silver_snapshot",
                "rule_counts",
                COUNTS
            ]
        );
        assert_eq!(b.run_seq, None);
        for s in &b.statements {
            assert!(!s.sql.contains("\"rule_results\"") && !s.sql.contains("run_summary"));
            if s.run_as == RunAs::Job {
                assert!(s.sql.contains("\"job_counts\""), "{}", s.id);
            }
        }
        let last = b.statements.last().unwrap();
        assert_eq!(last.run_as, RunAs::Report);
        assert!(matches!(last.expects, Expect::Rows { max_rows: 3, .. }));
        assert!(b.statements[3]
            .sql
            .contains("FOR VERSION AS OF {{silver_snapshot}} WHERE cut = DATE '2026-10-31'"));
        assert_eq!(
            rule_test(&job(None), &input),
            Err(CompileError::Incomplete("cut"))
        );
    }
}
