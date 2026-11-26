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
    use quality_rules_sql::{Cut, Tables};
    let Some(base) = url() else { return };
    let yaml = "dataset: loans\nrules:\n  \
                - { id: R1, field: Code, check: required, severity: major, alert: a }\n";
    let mut schema = Schema::default();
    schema.fields.insert("Code".into(), FieldType::Text);
    let (compiled, problems) = compile(&quality_rules_model::parse(yaml).unwrap(), &schema);
    assert!(problems.is_empty(), "{problems:?}");
    let from = "(VALUES ('A1'), (''), (CAST(NULL AS VARCHAR)), ('B2')) t(\"Code\")";
    let cut = Cut::parse("2026-10-31").unwrap();
    let body = quality_rules_trino::rule_results(from, &compiled, &Tables::new(), &cut).unwrap();
    let got = rows(&base, &body.sql).await;
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

/// Ruling 40: `field_quality`'s rows on Trino over `VALUES`: the profile
/// from bronze's columns, a double flagged floating with no shape, and the
/// recency on the updated field only, from silver's date.
#[tokio::test]
async fn the_field_quality_rows_profile_bronze_and_date_silver() {
    use quality::mapping::Mapping;
    use quality_rules_trino::{field_quality, Cut, FieldSources, ShapeKind};
    let Some(base) = url() else { return };
    let mapping = Mapping::parse(
        "dataset: loans\nsource: loans\nfields:\n  \
         Code: { column: code, type: text }\n  \
         Amount: { column: amount, type: number }\n  \
         Updated: { column: updated, type: date, roles: [updated] }\n",
    )
    .unwrap();
    let bronze = "(VALUES ('A1', 1.5e0, '2026-10-01'), ('', 2.0e0, '2025-01-01'), \
                  ('B22', CAST(NULL AS DOUBLE), CAST(NULL AS VARCHAR))) t(code, amount, updated)";
    let silver = "(VALUES (DATE '2026-10-01'), (DATE '2025-01-01'), (CAST(NULL AS DATE))) \
                  t(\"Updated\")";
    let from = FieldSources {
        bronze,
        silver,
        job_id: "j1",
    };
    let kind = |c: &str| ShapeKind::of_type(if c == "amount" { "double" } else { "varchar" });
    let cut = Cut::parse("2026-10-31").unwrap();
    let body = field_quality(&mapping, &from, &[], kind, &cut).unwrap();
    let sql = format!("SELECT * FROM ({}) x ORDER BY 1", body.sql);
    let got = rows(&base, &sql).await;
    let want = serde_json::json!([
        ["Amount", 1, 2, null, 0, true, null, null],
        ["Code", 1, 2, "A9", 1, false, null, null],
        [
            "Updated",
            1,
            2,
            "9999-99-99",
            2,
            false,
            null,
            [0, 1, 0, 0, 0, 1, 0]
        ]
    ]);
    assert_eq!(serde_json::Value::from(got), want);
}

/// Ruling 40: `distribution_checks`' rows on Trino over `VALUES`: each
/// method's guard picks one, both give the same 101 percentiles, and the
/// outliers come per dimension group.
#[tokio::test]
async fn the_distribution_rows_take_one_method_and_the_outlier_groups() {
    use quality::mapping::Mapping;
    use quality_rules_trino::{outlier_rows, percentile_guard, percentile_row};
    let Some(base) = url() else { return };
    let from = "(VALUES (10e0, 'A', 'X'), (11e0, 'A', 'X'), (12e0, 'A', 'Y'), (13e0, 'B', 'Y'), \
                (CAST(NULL AS DOUBLE), 'B', 'Y')) t(\"Income\", \"Office\", \"Type\")";
    for (method, want) in [(Method::Window, 1), (Method::Bracket, 0)] {
        let g = percentile_guard(from, "Income", 4, method).unwrap();
        assert_eq!(
            rows(&base, &g.sql).await,
            vec![vec![serde_json::json!(want)]]
        );
    }
    let w = rows(
        &base,
        &percentile_row(from, "Income", Method::Window).unwrap().sql,
    )
    .await;
    let b = rows(
        &base,
        &percentile_row(from, "Income", Method::Bracket).unwrap().sql,
    )
    .await;
    assert_eq!(w, b);
    assert_eq!(w[0][0], serde_json::json!("Income"));
    assert_eq!(w[0][3].as_array().unwrap().len(), 101);
    assert_eq!(w[0][3][0], serde_json::json!(10.0));
    assert_eq!(w[0][3][100], serde_json::json!(13.0));
    let none = "(VALUES (CAST(NULL AS DOUBLE))) t(\"Income\")";
    let empty = percentile_row(none, "Income", Method::Window).unwrap();
    assert!(rows(&base, &empty.sql).await.is_empty());
    let mapping = Mapping::parse(
        "dataset: d\nsource: d\nfields:\n  \
         Income: { column: i, type: number, roles: [outlier_amount] }\n  \
         Office: { column: o, type: text, roles: [dimension] }\n  \
         Type: { column: t, type: text, roles: [dimension] }\n",
    )
    .unwrap();
    let o = outlier_rows(from, &mapping).unwrap();
    let sql = format!("SELECT * FROM ({}) x ORDER BY 2, 3", o.sql);
    assert_eq!(
        serde_json::Value::from(rows(&base, &sql).await),
        serde_json::json!([
            ["Income", "Office", "A", null, 3, 0, 0],
            ["Income", "Office", "B", null, 1, 0, 0],
            ["Income", "Type", "X", null, 2, 0, 0],
            ["Income", "Type", "Y", null, 2, 0, 0]
        ])
    );
}
