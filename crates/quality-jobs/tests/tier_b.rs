//! The contract tier B on Trino (decision 0090, step 15): the compiled
//! bundles of a cut's chain run by a stand-in job runner against Trino 484
//! over RustFS's Iceberg warehouse (`tools/verify/trino_tier_b.sh`), on one
//! synthetic dataset: install, prepare, extract, derive, evaluate (twice
//! as a retry, then a newer run), a rule test and a retract. Skipped
//! unless `QD_TIER_B_URL` is set. The cross-engine cases run in CI only
//! while DuckDB exists; here Trino's own results are checked.

mod stand_in;

use std::collections::BTreeMap;

use quality::layers::{SilverDerivation, SourceClass};
use quality::mapping::Mapping;
use quality_jobs::{
    evaluate, extract, prepare, retract, rule_test, EvaluateInput, ExtractInput, Job, PrepareInput,
    RetractInput, RuleTestInput,
};
use quality_rules::check::{compile, Compiled};
use quality_rules::sql::Tables;
use quality_rules_trino::{self as trino, Accepted, Cut, Pinned, TablePath};
use serde_json::{json, Value};
use stand_in::runner::{rows, run_bundle, url};

const ADMIN: &str = "qd-quality";
const CUT: &str = "2026-10-31";
const EXTRACT: &str = "0b9c6a1e-0000-4000-8000-0000000000b1";

const MAPPING: &str = "dataset: loans
source: loans
key: [Id]
fields:
  Id: { column: loan_id, type: text }
  Doc: { column: doc, type: text, semantic: document, roles: [entity] }
  Type: { column: kind, type: text, roles: [segment, dimension] }
  Phone: { column: phone, type: text, priority: critical, roles: [sfc] }
  Amount: { column: amount, type: number, priority: high, roles: [drift, outlier_amount] }
  Updated: { column: updated, type: date, roles: [updated] }
";

const RULES: &str = "dataset: loans
rules:
  - { id: P1, field: Phone, check: required, severity: blocking, alert: a }
  - { id: A1, field: Amount, check: required, severity: major, alert: a }
";

/// Synthetic records: two phones empty, two amounts missing, five
/// entities (a document twice).
const SOURCE: &str = "INSERT INTO memory.ops.loans VALUES \
    (1, '1001', 'PN', '3001234567', 100e0, DATE '2026-10-01'), \
    (2, '1002', 'PN', '', 200e0, DATE '2026-09-01'), \
    (3, '1003', 'PJ', NULL, NULL, DATE '2026-01-01'), \
    (4, '1004', 'PJ', '3009876543', 400e0, DATE '2025-12-01'), \
    (5, '1001', 'PN', '3001112222', 50e0, NULL), \
    (6, '1005', 'PN', '3005556666', NULL, DATE '2026-10-30')";

fn lake(ns: &str) -> TablePath {
    TablePath {
        catalog: "lake".into(),
        namespace: ns.into(),
        table: "loans".into(),
    }
}

fn job<'a>(run: &'a str, kind: &'a str, cut: Option<&'a str>, run_seq: Option<i64>) -> Job<'a> {
    Job {
        run,
        kind,
        dataset: Some("loans"),
        cut,
        attempt: 1,
        run_seq,
        catalog_commit: Some("c0ffee"),
    }
}

async fn setup(base: &str) {
    for s in trino::install() {
        rows(base, ADMIN, &s.sql).await;
    }
    for sql in [
        "INSERT INTO lake.quality_secrets.findings_key VALUES (X'00112233445566778899aabbccddeeff')",
        "CREATE SCHEMA lake.loans_bronze",
        "CREATE SCHEMA lake.loans_silver",
        "CREATE SCHEMA memory.ops",
        "CREATE TABLE memory.ops.loans (loan_id bigint, doc varchar, kind varchar, \
         phone varchar, amount double, updated date)",
        SOURCE,
    ] {
        rows(base, ADMIN, sql).await;
    }
}

/// Prepare, extract and derive the cut; bronze's types as the extract
/// reported them.
async fn load(base: &str, mapping: &Mapping) -> BTreeMap<String, String> {
    let listed: Vec<(String, String)> = [
        ("loan_id", "bigint"),
        ("doc", "varchar"),
        ("kind", "varchar"),
        ("phone", "varchar"),
        ("amount", "double"),
        ("updated", "date"),
    ]
    .iter()
    .map(|(n, t)| ((*n).into(), (*t).into()))
    .collect();
    let p = prepare(
        &job(
            "0b9c6a1e-0000-4000-8000-0000000000a1",
            "prepare",
            None,
            None,
        ),
        &PrepareInput {
            mapping,
            bronze: lake("loans_bronze"),
            silver: lake("loans_silver"),
            source_columns: &listed,
        },
    )
    .unwrap();
    run_bundle(base, &p, "job_maint_loans", "job_report_loans")
        .await
        .unwrap();
    let source = TablePath {
        catalog: "memory".into(),
        namespace: "ops".into(),
        table: "loans".into(),
    };
    let input = ExtractInput {
        source,
        bronze: lake("loans_bronze"),
        columns: &listed,
        split: Some(("loan_id", 2)),
    };
    let x = extract(&job(EXTRACT, "extract", Some(CUT), None), &input).unwrap();
    run_bundle(base, &x, "job_extract_loans", "job_report_loans")
        .await
        .unwrap();
    // A retry of the same extract leaves one copy of its load.
    let a = run_bundle(base, &x, "job_extract_loans", "job_report_loans")
        .await
        .unwrap();
    let n = rows(base, ADMIN, "SELECT count(*) FROM lake.loans_bronze.loans").await;
    assert_eq!(n, [[json!(6)]], "the retry replaced its own load");
    let types = a["bronze_columns"]
        .iter()
        .map(|r| (r[0].as_str().unwrap().into(), r[1].as_str().unwrap().into()))
        .collect();
    let snapshot = rows(
        base,
        ADMIN,
        &trino::current_snapshot(&lake("loans_bronze")).unwrap(),
    )
    .await[0][0]
        .as_i64()
        .unwrap();
    let cut = Cut::parse(CUT).unwrap();
    let digest = SilverDerivation::of(mapping, |_| SourceClass::Other).digest();
    let derive = trino::derive_silver(
        &lake("loans_silver"),
        mapping,
        (
            &Pinned {
                table: lake("loans_bronze"),
                snapshot,
            },
            &Accepted {
                cut: &cut,
                load: Some(EXTRACT),
            },
        ),
        EXTRACT,
        &digest,
        |_| SourceClass::Other,
    )
    .unwrap();
    rows(base, "job_extract_loans", &derive.sql).await;
    types
}

async fn evaluate_run(
    base: &str,
    run: &str,
    seq: i64,
    mapping: &Mapping,
    rules: &[Compiled],
    types: &BTreeMap<String, String>,
) {
    let input = EvaluateInput {
        mapping,
        rules,
        tables: &Tables::new(),
        references: &[],
        silver: lake("loans_silver"),
        bronze: lake("loans_bronze"),
        load: Some(EXTRACT),
        previous: None,
        bronze_types: types,
        rules_commit: "c0ffee",
        exact_sort_limit: 1_000_000,
    };
    let b = evaluate(&job(run, "evaluate", Some(CUT), Some(seq)), &input).unwrap();
    run_bundle(base, &b, "job_rules_loans", "job_report_loans")
        .await
        .unwrap();
}

fn values(rows: Vec<Vec<Value>>) -> Value {
    Value::from(rows.into_iter().map(Value::from).collect::<Vec<_>>())
}

#[tokio::test]
async fn a_cuts_chain_runs_its_bundles_on_trino_and_gold_holds_one_current_run() {
    let Some(base) = url() else { return };
    setup(&base).await;
    let mapping = Mapping::parse(MAPPING).unwrap();
    let (rules, problems) = compile(&quality_rules::parse(RULES).unwrap(), &mapping.schema());
    assert!(problems.is_empty(), "{problems:?}");
    let types = load(&base, &mapping).await;
    assert_eq!(types["loan_id"], "bigint");

    let results = "SELECT rule, applies, failures FROM gold.quality_gold.v_rule_results \
                   WHERE dataset = 'loans' ORDER BY rule";
    let e1 = "0b9c6a1e-0000-4000-8000-0000000000e1";
    evaluate_run(&base, e1, 1, &mapping, &rules, &types).await;
    let want = json!([["A1", 6, 2], ["P1", 6, 2]]);
    assert_eq!(values(rows(&base, ADMIN, results).await), want);
    // The retry of the same run (its pinned bundle) doubles nothing.
    evaluate_run(&base, e1, 1, &mapping, &rules, &types).await;
    assert_eq!(values(rows(&base, ADMIN, results).await), want);
    let summary = "SELECT records, entities FROM gold.quality_gold.v_run_summary \
                   WHERE dataset = 'loans'";
    assert_eq!(values(rows(&base, ADMIN, summary).await), json!([[6, 5]]));
    let fields = rows(
        &base,
        ADMIN,
        "SELECT count(*) FROM gold.quality_gold.v_field_quality",
    )
    .await;
    assert_eq!(fields, [[json!(6)]]);

    // A newer run flips the cut and its older rows go.
    let e2 = "0b9c6a1e-0000-4000-8000-0000000000e2";
    evaluate_run(&base, e2, 2, &mapping, &rules, &types).await;
    assert_eq!(values(rows(&base, ADMIN, results).await), want);
    let older =
        format!("SELECT count(*) FROM lake.quality_gold.rule_results WHERE run_id = '{e1}'");
    assert_eq!(rows(&base, ADMIN, &older).await, [[json!(0)]]);
    let scratch = "SELECT count(*) FROM lake.quality_scratch.record_icd";
    assert_eq!(rows(&base, ADMIN, scratch).await, [[json!(0)]]);
    let bands = rows(
        &base,
        ADMIN,
        "SELECT sum(entities) FROM gold.quality_gold.v_bands_by_segment",
    )
    .await;
    assert_eq!(bands, [[json!(5)]]);

    // A rule test writes counts only and reads them back.
    let t = rule_test(
        &job(
            "0b9c6a1e-0000-4000-8000-0000000000f1",
            "rule_test",
            Some(CUT),
            None,
        ),
        &RuleTestInput {
            rules: &rules,
            tables: &Tables::new(),
            silver: lake("loans_silver"),
            load: Some(EXTRACT),
        },
    )
    .unwrap();
    let a = run_bundle(&base, &t, "job_rules_loans", "job_report_loans")
        .await
        .unwrap();
    assert_eq!(
        values(a[quality_jobs::COUNTS].clone()),
        json!([
            ["0.fails", 2],
            ["0.passes", 4],
            ["0.skips", 0],
            ["1.fails", 2],
            ["1.passes", 4],
            ["1.skips", 0]
        ])
    );

    // A retract of the cut leaves its tombstone and no detail rows.
    let r = retract(
        &job(
            "0b9c6a1e-0000-4000-8000-0000000000c1",
            "retract",
            Some(CUT),
            Some(3),
        ),
        &RetractInput,
    )
    .unwrap();
    run_bundle(&base, &r, "job_rules_loans", "job_report_loans")
        .await
        .unwrap();
    assert!(rows(&base, ADMIN, results).await.is_empty());
}
