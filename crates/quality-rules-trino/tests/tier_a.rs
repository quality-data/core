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
