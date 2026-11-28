//! A `test_rule` job's bundle (decision 0043; decision 0090, ruling 83):
//! the rules a person typed, tested on a cut as `job_sample_<d>`: silver
//! pinned, then one values read per rule with its counts by outcome, a
//! few records of each and its largest groups of failures. Its rows go to
//! the core on the values route; gold is never written.

use quality::bundle::{Bundle, Expect};
use quality::rule_test::EACH;
use quality::test_rule_run::{rule_read, GROUPS};
use quality_rules::check::Compiled;
use quality_rules::sql::Tables;
use quality_rules_trino::{self as trino, Cut, TablePath, SAMPLE_HEAD};

use crate::evaluate::pin;
use crate::steps::Steps;
use crate::{CompileError, Job};

/// What a test reads and shows.
#[derive(Debug, Clone)]
pub struct TestRuleInput<'a> {
    pub rules: &'a [Compiled],
    pub tables: &'a Tables,
    pub silver: TablePath,
    /// The cut's accepted load (`None`: rows from before loads).
    pub load: Option<&'a str>,
    /// The mapping's key fields, which order the records.
    pub key: &'a [String],
    /// Fields shown besides the key and those a rule reads.
    pub columns: &'a [String],
    /// Records per outcome and per group, at most decision 0043's.
    pub each: u64,
}

/// The test's bundle.
pub fn test_rule(job: &Job<'_>, input: &TestRuleInput<'_>) -> Result<Bundle, CompileError> {
    job.dataset()?;
    let cut =
        Cut::parse(job.cut.ok_or(CompileError::Incomplete("cut"))?).map_err(trino::Error::from)?;
    if input.rules.is_empty() {
        return Err(CompileError::Incomplete("rules"));
    }
    let each = input.each.clamp(1, EACH as u64);
    let mut steps = Steps::default();
    let silver = pin(
        &mut steps,
        &input.silver,
        "silver_snapshot",
        &cut,
        input.load,
    )?;
    for (i, rule) in input.rules.iter().enumerate() {
        let fields = trino::shown_fields(rule, input.key, input.columns);
        let mut columns: Vec<(String, String)> = SAMPLE_HEAD
            .iter()
            .map(|(n, t)| ((*n).to_owned(), (*t).to_owned()))
            .collect();
        columns.extend((0..fields.len()).map(|f| (trino::sample_column(f), "varchar".to_owned())));
        let read = trino::rule_samples(
            &silver,
            rule,
            input.tables,
            &cut,
            &fields,
            input.key.len(),
            (each, GROUPS),
        )?;
        steps.job_read(
            rule_read(i),
            read,
            Expect::Values {
                columns,
                max_rows: trino::sample_rows(each, GROUPS),
            },
        );
    }
    Ok(job.bundle(steps))
}

#[cfg(test)]
mod tests {
    use super::*;
    use quality::bundle::RunAs;
    use quality_rules::check::{compile, FieldType, Schema};

    #[test]
    fn a_test_reads_each_rule_as_the_sample_user_bounded() {
        let yaml = "dataset: loans\nrules:\n  \
                    - { id: R1, field: Code, check: required, severity: major, alert: a }\n";
        let mut schema = Schema::default();
        schema.fields.insert("Code".into(), FieldType::Text);
        schema.fields.insert("Id".into(), FieldType::Text);
        let (rules, _) = compile(&quality_rules::parse(yaml).unwrap(), &schema);
        let key = ["Id".to_string()];
        let input = TestRuleInput {
            rules: &rules,
            tables: &Tables::new(),
            silver: TablePath {
                catalog: "lake".into(),
                namespace: "loans_silver".into(),
                table: "loans".into(),
            },
            load: None,
            key: &key,
            columns: &[],
            each: 10,
        };
        let job = Job {
            run: "0b9c6a1e-0000-4000-8000-000000000081",
            kind: "test_rule",
            dataset: Some("loans"),
            cut: Some("2026-10-31"),
            attempt: 1,
            run_seq: None,
            catalog_commit: None,
        };
        let b = test_rule(&job, &input).unwrap();
        assert!(b.well_formed());
        let ids: Vec<&str> = b.statements.iter().map(|s| s.id.as_str()).collect();
        assert_eq!(ids, ["silver_snapshot", "rule_0"]);
        let s = &b.statements[1];
        assert_eq!(s.run_as, RunAs::Job);
        let Expect::Values { columns, max_rows } = &s.expects else {
            panic!("a values read");
        };
        assert_eq!(*max_rows, 1 + 3 + 30 + 200);
        assert_eq!(columns.len(), 5 + 2, "Id, then Code");
        assert!(s.sql.contains("FOR VERSION AS OF {{silver_snapshot}}"));
        assert!(s.sql.contains("WHERE k <= 10"));
        assert!(s.sql.contains("LIMIT 20"));
        assert!(!s.sql.contains("quality_gold"));
        let none = TestRuleInput {
            rules: &[],
            ..input
        };
        assert!(test_rule(&job, &none).is_err());
    }
}
