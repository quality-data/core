//! The contract tier A (decision 0090, step 9): the same SQL on DuckDB
//! 2.0.0 and on Trino 484 gives the same answers. Inputs are `VALUES`
//! only, no lake. It runs when `QD_TRINO_URL` names a Trino (CI starts
//! one with `regex-library=RE2J`); without it, each test passes empty.

mod cases;
mod trino;

use duckdb::Connection;
use quality_rules_sql::distribution::percentiles;
use quality_rules_sql::Engine;
use serde_json::Value;

use trino::{bound, rows, run, url, Answer};

fn boolean(v: &Value) -> Option<bool> {
    v.as_bool()
}

/// The rules' semantic cases (decision 0033) and the SQL safety corpus
/// (the `attacks` case) give the expected results on Trino, as on DuckDB.
#[tokio::test]
async fn every_case_gives_its_results_on_trino() {
    let Some(base) = url() else { return };
    let mut failures = Vec::new();
    for case in cases::cases() {
        let (sql, params) = cases::query(&case, Engine::Trino);
        let got: Vec<Vec<Option<bool>>> = match run(&base, &bound(&sql, cases::CUT, params)).await {
            Answer::Rows(r) => r
                .iter()
                .map(|row| row.iter().map(boolean).collect())
                .collect(),
            Answer::Error(e) => {
                failures.push(format!("{}: {e}\n  {sql}", case.name));
                continue;
            }
        };
        let expected: Vec<Vec<Option<bool>>> = case.expected.iter().map(|r| r.to_vec()).collect();
        if got != expected {
            failures.push(format!(
                "{}\n  expected {expected:?}\n  got      {got:?}\n  {sql}",
                case.name
            ));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// Trino runs RE2J, never Joni: a back-reference is refused (point 8.7).
#[tokio::test]
async fn the_joni_probe_is_refused() {
    let Some(base) = url() else { return };
    match run(&base, "SELECT regexp_like('aa', '(a)\\1')").await {
        Answer::Error(e) => assert_eq!(e, "INVALID_FUNCTION_ARGUMENT"),
        Answer::Rows(r) => panic!("Joni answered {r:?}"),
    }
    let ok = rows(&base, "SELECT regexp_like('a1', '^(?:[a-z]\\d)$')").await;
    assert_eq!(ok, [[Value::Bool(true)]]);
}

/// The forms point 8 fixes answer alike on both engines: `DOUBLE`
/// literals, `trim` with its set, `IF` for booleans, `substr`.
#[tokio::test]
async fn the_portable_forms_answer_alike() {
    let Some(base) = url() else { return };
    let duck = Connection::open_in_memory().unwrap();
    let probes = [
        "CAST(DOUBLE '1' / DOUBLE '3' AS VARCHAR) = CAST(DOUBLE '1' / DOUBLE '3' AS VARCHAR)",
        "DOUBLE '0.4' * DOUBLE '3' + DOUBLE '0.15' = DOUBLE '1.35'",
        "trim(BOTH concat(' ', chr(160)) FROM concat(chr(160), ' a ', chr(160))) = 'a'",
        "trim(BOTH ' ' FROM concat(chr(160), 'a')) = concat(chr(160), 'a')",
        "trim(LEADING '0' FROM '00120') = '120'",
        "IF(CAST(NULL AS BOOLEAN), DOUBLE '1', DOUBLE '0') = DOUBLE '0'",
        "substr('abcdef', 1, 3) = 'abc'",
    ];
    for p in probes {
        let d: bool = duck
            .query_row(&format!("SELECT {p}"), [], |r| r.get(0))
            .unwrap_or_else(|e| panic!("DuckDB: {e}: {p}"));
        let t = rows(&base, &format!("SELECT {p}")).await;
        assert!(d, "DuckDB: {p}");
        assert_eq!(t, [[Value::Bool(true)]], "Trino: {p}");
    }
}

/// A small deterministic generator: synthetic values only.
fn vector(seed: u64, n: usize, kind: u64) -> Vec<f64> {
    let mut x = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    (0..n)
        .map(|i| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            let u = (x >> 11) as f64 / (1u64 << 53) as f64;
            match (kind, i % 7) {
                (1, _) => (u * 4.0).floor(),
                (2, 0) => f64::NAN,
                (3, 0) => f64::INFINITY,
                (3, 1) => f64::NEG_INFINITY,
                _ => (u - 0.5) * 1e6,
            }
        })
        .collect()
}

fn literal(x: f64) -> String {
    let text = if x.is_nan() {
        "NaN".to_string()
    } else if x.is_infinite() {
        if x > 0.0 { "Infinity" } else { "-Infinity" }.to_string()
    } else {
        format!("{x:?}")
    };
    format!("(CAST('{text}' AS DOUBLE))")
}

/// Any NaN is one answer (the engines' NaN sign bits differ, T1).
fn canon(bits: u64) -> u64 {
    if f64::from_bits(bits).is_nan() {
        u64::MAX
    } else {
        bits
    }
}

/// The canonical window percentiles (point 8.3) are bit-equal on both
/// engines over random vectors with ties, NaN and infinities (T1).
#[tokio::test]
async fn the_window_percentiles_are_bit_equal_on_both_engines() {
    let Some(base) = url() else { return };
    let duck = Connection::open_in_memory().unwrap();
    for (seed, (n, kind)) in [1usize, 2, 3, 4, 101, 1000]
        .into_iter()
        .flat_map(|n| (0..4).map(move |k| (n, k)))
        .enumerate()
    {
        let xs = vector(seed as u64 + 7, n, kind);
        let values: Vec<String> = xs.iter().map(|x| literal(*x)).collect();
        let sql = percentiles(&format!(
            "SELECT x FROM (VALUES {}) t(x)",
            values.join(", ")
        ));
        let mut stmt = duck.prepare(&sql).unwrap();
        let d: Vec<u64> = stmt
            .query_map([], |r| r.get::<_, f64>(1))
            .unwrap()
            .map(|q| canon(q.unwrap().to_bits()))
            .collect();
        let t: Vec<u64> = rows(
            &base,
            &format!("SELECT to_hex(to_ieee754_64(q)) FROM ({sql}) ORDER BY k"),
        )
        .await
        .iter()
        .map(|r| canon(u64::from_str_radix(r[0].as_str().unwrap(), 16).unwrap()))
        .collect();
        assert_eq!(d.len(), 101, "n {n}, kind {kind}");
        assert_eq!(d, t, "n {n}, kind {kind}");
    }
}
