//! An evaluate's bundle (decision 0090, amendment A1, rulings 39, 40, 52
//! and 53): complete statements, the job's user only writing, the report
//! user only reading gold and the pins.

use std::collections::BTreeMap;

use quality::bundle::{Bundle, Expect, PlaceholderType, RunAs};
use quality::mapping::Mapping;
use quality_jobs::{evaluate, CompileError, EvaluateInput, Job};
use quality_rules::check::{compile, Compiled, FieldType, Schema};
use quality_rules::sql::Tables;
use quality_rules_trino::TablePath;

const MAPPING: &str = "dataset: loans
source: loans
fields:
  Doc: { column: doc, type: text, semantic: document, roles: [entity] }
  Type: { column: kind, type: text, roles: [segment, dimension] }
  Phone: { column: phone, type: text, priority: critical, roles: [sfc] }
  Amount: { column: amount, type: number, priority: high, roles: [drift, outlier_amount] }
  Updated: { column: updated, type: date, roles: [updated] }
";

const RULES: &str = "dataset: loans
rules:
  - { id: P1, field: Phone, check: required, severity: blocking, alert: a }
  - id: X1
    check: expression
    fails: Updated > :cut AND Type = 'PN'
    dimension: consistency
    severity: minor
    alert: a
";

fn path(ns: &str) -> TablePath {
    TablePath {
        catalog: "lake".into(),
        namespace: ns.into(),
        table: "loans".into(),
    }
}

fn rules() -> Vec<Compiled> {
    let mut schema = Schema::default();
    for (name, t) in [
        ("Doc", FieldType::Text),
        ("Type", FieldType::Text),
        ("Phone", FieldType::Text),
        ("Amount", FieldType::Number),
        ("Updated", FieldType::Date),
    ] {
        schema.fields.insert(name.into(), t);
    }
    let (compiled, problems) = compile(&quality_rules::parse(RULES).unwrap(), &schema);
    assert!(problems.is_empty(), "{problems:?}");
    compiled
}

fn job(cut: Option<&'static str>) -> Job<'static> {
    Job {
        run: "r-9",
        kind: "evaluate",
        dataset: Some("loans"),
        cut,
        attempt: 1,
        run_seq: Some(12),
        catalog_commit: Some("c0ffee"),
    }
}

fn bundle(previous: Option<&str>) -> Result<Bundle, CompileError> {
    let mapping = Mapping::parse(MAPPING).unwrap();
    let rules = rules();
    let types = BTreeMap::from([("amount".to_string(), "decimal(9,2)".to_string())]);
    let input = EvaluateInput {
        mapping: &mapping,
        rules: &rules,
        tables: &Tables::new(),
        silver: path("loans_silver"),
        bronze: path("loans_bronze"),
        load: Some("load-3"),
        previous,
        bronze_types: &types,
        rules_commit: "abc123",
        exact_sort_limit: 100_000,
    };
    evaluate(&job(Some("2026-10-31")), &input)
}

fn ids(b: &Bundle) -> Vec<&str> {
    b.statements.iter().map(|s| s.id.as_str()).collect()
}

#[test]
fn the_bundle_undoes_an_attempt_pins_writes_gold_and_flips() {
    let b = bundle(Some("2026-09-30")).unwrap();
    assert!(b.well_formed(), "{:#?}", ids(&b));
    let ids = ids(&b);
    assert_eq!(ids[..2], ["counts_rows", "counts"]);
    let at = |id: &str| ids.iter().position(|i| *i == id).unwrap();
    assert!(at("run_run_summary") < at("scratch_left"));
    assert!(at("scratch_left") < at("silver_snapshot"));
    assert!(at("silver_snapshot") < at("scratch"));
    for t in [
        "rule_results",
        "field_quality",
        "findings",
        "queue_findings",
    ] {
        assert!(at("scratch") < at(t) && at(t) < at("scratch_end"), "{t}");
    }
    assert!(at("finding_groups") < at("queue_findings"));
    assert!(at("scratch_end") < at("flip") && at("flip") < at("older_rule_results"));
    assert_eq!(ids.last(), Some(&"older_run_summary"));
    let names: Vec<(&str, PlaceholderType)> = b
        .placeholders
        .iter()
        .map(|p| (p.name.as_str(), p.ty))
        .collect();
    assert_eq!(
        names,
        [
            ("started_ms", PlaceholderType::Bigint),
            ("silver_snapshot", PlaceholderType::SnapshotId),
            ("bronze_snapshot", PlaceholderType::SnapshotId),
        ]
    );
    let flip = &b.statements[at("flip")];
    assert!(flip.sql.contains("{{started_ms}}"), "{}", flip.sql);
    let scratch = &b.statements[at("scratch")];
    assert!(scratch.sql.contains(
        "FROM \"lake\".\"loans_silver\".\"loans\" FOR VERSION AS OF {{silver_snapshot}} \
         WHERE cut = DATE '2026-10-31' AND _load = 'load-3'"
    ));
    let fq = &b.statements[at("field_quality")];
    assert!(fq.sql.contains("FOR VERSION AS OF {{bronze_snapshot}}"));
    assert!(
        fq.sql
            .contains("SELECT 'Amount', p.e, p.d, p.s, p.n, false"),
        "bronze's decimal has a shape"
    );
    let findings = &b.statements[at("queue_findings")];
    assert!(findings.sql.contains("cut = DATE '2026-09-30'"));
}

#[test]
fn the_job_user_only_writes_and_the_report_user_reads_only_gold_and_pins() {
    let b = bundle(None).unwrap();
    for s in &b.statements {
        assert!(!s.sql.contains('?'), "nothing bound: {}", s.id);
        match s.run_as {
            RunAs::Job => assert!(
                s.sql.starts_with("INSERT INTO") || s.sql.starts_with("DELETE FROM"),
                "{}: {}",
                s.id,
                s.sql
            ),
            RunAs::Report => {
                assert!(s.sql.starts_with("SELECT "), "{}", s.id);
                assert!(
                    s.sql.contains("\"quality_gold\"") || s.sql.contains("$refs\""),
                    "{}: {}",
                    s.id,
                    s.sql
                );
                assert!(matches!(s.expects, Expect::Rows { max_rows: 1, .. }));
            }
        }
        let guarded = s.sql.starts_with("DELETE")
            || (s.id.starts_with("drift_")
                && !s.id.ends_with("_count")
                && !s.id.ends_with("_rows"));
        assert_eq!(s.only_if_rows.is_some(), guarded, "{}", s.id);
    }
}

#[test]
fn each_drift_variable_has_both_methods_behind_their_guards() {
    let b = bundle(None).unwrap();
    let window = b
        .statements
        .iter()
        .find(|s| s.id == "drift_0_window")
        .unwrap();
    assert_eq!(
        window.only_if_rows.as_ref().unwrap().after,
        "drift_0_window_rows"
    );
    assert!(b.statements.iter().any(|s| s.id == "drift_0_bracket"));
    let guard = b
        .statements
        .iter()
        .find(|s| s.id == "drift_0_window_count")
        .unwrap();
    assert!(guard.sql.contains("job_counts"));
    assert!(guard.sql.contains("<= 100000"));
}

#[test]
fn a_missing_or_bad_cut_is_refused() {
    let mapping = Mapping::parse(MAPPING).unwrap();
    let types = BTreeMap::new();
    let input = EvaluateInput {
        mapping: &mapping,
        rules: &[],
        tables: &Tables::new(),
        silver: path("loans_silver"),
        bronze: path("loans_bronze"),
        load: None,
        previous: Some("2026-02-30"),
        bronze_types: &types,
        rules_commit: "abc",
        exact_sort_limit: 10,
    };
    assert_eq!(
        evaluate(&job(None), &input),
        Err(CompileError::Incomplete("cut"))
    );
    assert!(matches!(
        evaluate(&job(Some("2026-10-31")), &input),
        Err(CompileError::Statement(_))
    ));
}
