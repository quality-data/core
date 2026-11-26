//! The contract tier A for the distribution statements (decision 0090,
//! step 12): on Trino 484, the bracket method gives the window's
//! percentiles bit for bit. Runs when `QD_TRINO_URL` names a Trino.

mod trino;

use quality_rules_trino::{variable_percentiles, Method};
use trino::{rows, url};

/// Synthetic values: a spread with ties, infinities and a NaN.
fn values() -> String {
    let mut v: Vec<String> = (0..3000)
        .map(|i| format!("(CAST({} AS DOUBLE) / DOUBLE '7')", (i * 7919) % 1013 - 500))
        .collect();
    v.extend(
        [
            "(infinity())",
            "(-infinity())",
            "(nan())",
            "(CAST(NULL AS DOUBLE))",
        ]
        .map(String::from),
    );
    format!("(VALUES {}) t(\"Amount\")", v.join(", "))
}

#[tokio::test]
async fn the_bracket_method_gives_the_windows_bits() {
    let Some(base) = url() else { return };
    let from = values();
    let bits = |m| {
        let sql = variable_percentiles(&from, "Amount", m).unwrap().sql;
        format!("SELECT to_hex(to_ieee754_64(q)) FROM ({sql}) ORDER BY k")
    };
    let window = rows(&base, &bits(Method::Window)).await;
    let bracket = rows(&base, &bits(Method::Bracket)).await;
    assert_eq!(window.len(), 101);
    assert_eq!(window, bracket);
}

/// Ruling 40: `rule_results`' rows, as gold's insert takes them, on Trino
/// over `VALUES`: per rule its applying and failing counts and its names.
#[tokio::test]
async fn the_rule_results_rows_give_each_rules_counts() {
    use quality_rules_compile::{compile, FieldType, Schema};
    use quality_rules_sql::Tables;
    let Some(base) = url() else { return };
    let yaml = "dataset: loans\nrules:\n  \
                - { id: R1, field: Code, check: required, severity: major, alert: a }\n";
    let mut schema = Schema::default();
    schema.fields.insert("Code".into(), FieldType::Text);
    let (compiled, problems) = compile(&quality_rules_model::parse(yaml).unwrap(), &schema);
    assert!(problems.is_empty(), "{problems:?}");
    let from = "(VALUES ('A1'), (''), (CAST(NULL AS VARCHAR)), ('B2')) t(\"Code\")";
    let body = quality_rules_trino::rule_results(from, &compiled, &Tables::new()).unwrap();
    let got = rows(
        &base,
        &trino::bound(&body.sql, "2026-10-31", body.cut_params),
    )
    .await;
    assert_eq!(
        got,
        vec![
            serde_json::json!(["R1", 4, 2, "Code", "required", "completeness", "major"])
                .as_array()
                .unwrap()
                .clone()
        ]
    );
}
