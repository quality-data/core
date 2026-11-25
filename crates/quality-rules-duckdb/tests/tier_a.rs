//! The contract tier A on the step 10 statements (decision 0090, cases 4,
//! 5 and 7): Trino's rule counts, field profile and recency over `VALUES`
//! equal DuckDB's over the same synthetic rows. Runs when `QD_TRINO_URL`
//! names a Trino.

mod trino;

use duckdb::Connection;
use quality_rules_duckdb::Runner;
use quality_rules_lint::lint;
use quality_rules_trino::ShapeKind;
use serde_json::Value;
use trino::{bound, rows, url};

const CUT: &str = "2026-10-31";

/// Synthetic rows: codes of two shapes, blanks, a U+00A0, NULLs, amounts
/// on both sides of the rule, dates at every recency band.
const ROWS: &[(&str, &str, &str)] = &[
    ("'L1001'", "500", "DATE '2026-10-30'"),
    ("'L1002'", "2000", "DATE '2026-08-01'"),
    ("'AB-7'", "NULL", "DATE '2026-05-01'"),
    ("'  '", "1500", "DATE '2025-12-01'"),
    ("chr(160)", "10", "DATE '2024-12-01'"),
    ("NULL", "0", "DATE '2027-01-01'"),
    ("'L1003'", "999", "NULL"),
];

const RULES: &str = "dataset: loans
rules:
  - { id: C1, field: Code, check: required, severity: major, alert: a }
  - { id: A1, check: expression, fails: Amount > 1000, dimension: validity, severity: minor, alert: a }
  - { id: D1, check: expression, fails: Opened > :cut, dimension: timeliness, severity: minor, alert: a }
";

fn values() -> String {
    let rows: Vec<String> = ROWS
        .iter()
        .map(|(c, a, d)| format!("(CAST({c} AS VARCHAR), CAST({a} AS DOUBLE), CAST({d} AS DATE))"))
        .collect();
    format!(
        "(VALUES {}) t(\"Code\", \"Amount\", \"Opened\")",
        rows.join(", ")
    )
}

/// Synthetic numbers of each kind: integers, decimals (one without
/// integer digits, T21's `DECIMAL(p, p)`), and a double, NULLs among them.
fn numbers() -> String {
    let rows: Vec<String> = [
        ("7", "1234.5", "0.25", "1.5"),
        ("-42", "0.07", "0.5", "1e300"),
        ("NULL", "99", "NULL", "-0.1"),
        ("100000", "-3.10", "0.0001", "NULL"),
    ]
    .iter()
    .map(|(i, d, p, f)| {
        format!(
            "(CAST({i} AS BIGINT), CAST({d} AS DECIMAL(9, 2)), CAST({p} AS DECIMAL(4, 4)), \
             CAST({f} AS DOUBLE))"
        )
    })
    .collect();
    format!(
        "(VALUES {}) t(\"Count\", \"Price\", \"Share\", \"Amount\")",
        rows.join(", ")
    )
}

fn runner() -> (Runner, std::path::PathBuf) {
    loaded("loans", &values())
}

/// Each call gets its own folder: the tests run at once in one process.
fn loaded(name: &str, relation: &str) -> (Runner, std::path::PathBuf) {
    static NEXT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
    let n = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!("qd-tier-a-{name}-{}-{n}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join(format!("{name}.parquet")).display().to_string();
    Connection::open_in_memory()
        .unwrap()
        .execute_batch(&format!(
            "COPY (SELECT * FROM {relation}) TO '{path}' (FORMAT parquet)"
        ))
        .unwrap();
    let mut r = Runner::new().unwrap();
    r.load(name, &path).unwrap();
    (r, dir)
}

fn int(v: &Value) -> i64 {
    v.as_i64().unwrap()
}

#[tokio::test]
async fn case_4_rule_counts_agree() {
    let Some(base) = url() else { return };
    let (r, dir) = runner();
    let report = lint(RULES, &r.schema("loans", &[]).unwrap());
    let file = quality_rules_model::parse(RULES).unwrap();
    let tables = r.tables(&file);
    let duck = r.test("loans", &report.compiled, &tables, CUT).unwrap();
    let s = quality_rules_trino::rule_counts(&values(), &report.compiled, &tables).unwrap();
    let t = rows(&base, &bound(&s.sql, CUT, s.cut_params)).await;
    for (i, c) in duck.iter().enumerate() {
        let got = (
            int(&t[0][3 * i]),
            int(&t[0][3 * i + 1]),
            int(&t[0][3 * i + 2]),
        );
        assert_eq!(
            got,
            (c.fails as i64, c.passes as i64, c.skipped as i64),
            "{}",
            c.rule
        );
    }
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn case_5_field_profiles_agree() {
    let Some(base) = url() else { return };
    let (r, dir) = runner();
    profiles_agree(&base, &r, "loans", &values(), &["Code", "Opened", "Amount"]).await;
    std::fs::remove_dir_all(dir).unwrap();
    // Numbers by kind (rulings 26 and 27): integers and decimals by their
    // text, a double with no shape.
    let (r, dir) = loaded("numbers", &numbers());
    let all = ["Count", "Price", "Share", "Amount"];
    profiles_agree(&base, &r, "numbers", &numbers(), &all).await;
    let duck = r.field_quality("numbers", &all).unwrap();
    assert_eq!(duck[2].pattern.as_deref(), Some("9.9999"));
    assert_eq!(
        (duck[3].pattern.as_deref(), duck[3].pattern_count),
        (None, 0)
    );
    std::fs::remove_dir_all(dir).unwrap();
}

/// Trino's profile of each of `fields` over `relation`, of the kind its
/// type in the loaded `dataset` says, equals DuckDB's.
async fn profiles_agree(base: &str, r: &Runner, dataset: &str, relation: &str, fields: &[&str]) {
    let types = r.column_types(dataset).unwrap();
    for f in r.field_quality(dataset, fields).unwrap() {
        let kind = ShapeKind::of_type(&types[&f.field]);
        let s = quality_rules_trino::field_profile(relation, &f.field, kind).unwrap();
        let t = rows(base, &s.sql).await;
        let got = (
            int(&t[0][0]),
            int(&t[0][1]),
            t[0][2].as_str().map(str::to_string),
            int(&t[0][3]),
        );
        assert_eq!(
            got,
            (f.empty, f.distinct, f.pattern.clone(), f.pattern_count),
            "{}",
            f.field
        );
    }
}

#[tokio::test]
async fn case_7_recency_bands_agree() {
    let Some(base) = url() else { return };
    let (r, dir) = runner();
    let duck = r.recency("loans", "Opened", CUT).unwrap();
    let s = quality_rules_trino::recency(&values(), "Opened").unwrap();
    let t = rows(&base, &bound(&s.sql, CUT, s.cut_params)).await;
    let got: Vec<i64> = t[0].iter().map(int).collect();
    assert_eq!(got, duck);
    std::fs::remove_dir_all(dir).unwrap();
}
