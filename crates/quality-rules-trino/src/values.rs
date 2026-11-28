//! Values reads (decision 0090, point 7.4 and rulings 79 to 81): the only
//! statements whose rows carry values, each with a hard `LIMIT`. Their rows
//! go to the core on the values route, never into gold or a report.

use quality_rules_sql::quote_text;

use crate::{quoted, Error, Statement, TablePath};

/// A map's aggregates per column: empty, distinct, shortest and longest,
/// then one per pattern (decision 0038).
pub const PROFILE_PER_COLUMN: [&str; 4] = ["empty", "distinct", "min_length", "max_length"];

/// The name of a map profile's aggregate `what` of the `i`-th column.
pub fn profile_column(i: usize, what: &str) -> String {
    format!("c{i}_{what}")
}

/// A page of a source table's records as text, `limit` rows from `offset`,
/// in `order` (descending if asked) when one is given: each column cast to
/// `varchar` under its own name, `NULL` kept.
pub fn source_page(
    table: &TablePath,
    columns: &[String],
    order: Option<(&str, bool)>,
    offset: u64,
    limit: u64,
) -> Result<Statement, Error> {
    let mut select = Vec::with_capacity(columns.len());
    for c in columns {
        let c = quoted(c)?;
        select.push(format!("CAST({c} AS varchar) AS {c}"));
    }
    let order = match order {
        Some((column, descending)) => format!(
            " ORDER BY {}{}",
            quoted(column)?,
            if descending { " DESC" } else { "" }
        ),
        None => String::new(),
    };
    Ok(Statement {
        sql: format!(
            "SELECT {} FROM {}{order} OFFSET {offset} LIMIT {limit}",
            select.join(", "),
            table.sql()?
        ),
    })
}

/// A map's profile of a source table (decision 0038; decision 0090, ruling
/// 79): over its first `sample` rows, one row of counts: `sampled`, then for
/// each column, in order, [`PROFILE_PER_COLUMN`] and how many values fully
/// match each of `patterns` (named `pattern_<i>`). Empty as decision 0033
/// says: `NULL`, or only spaces including U+00A0. Counts only.
pub fn source_profile(
    table: &TablePath,
    columns: &[String],
    sample: u64,
    patterns: &[&str],
) -> Result<Statement, Error> {
    let mut parts = vec!["count(*) AS \"sampled\"".to_owned()];
    for (i, column) in columns.iter().enumerate() {
        let c = quoted(column)?;
        let v = format!("CAST({c} AS varchar)");
        let empty = format!("({v} IS NULL OR regexp_like({v}, '^[\\s\\x{{00A0}}]*$'))");
        let name = |what: &str| quoted(&profile_column(i, what));
        parts.push(format!(
            "count(*) FILTER (WHERE {empty}) AS {}",
            name("empty")?
        ));
        parts.push(format!(
            "count(DISTINCT CASE WHEN NOT {empty} THEN {v} END) AS {}",
            name("distinct")?
        ));
        parts.push(format!(
            "min(length({v})) FILTER (WHERE NOT {empty}) AS {}",
            name("min_length")?
        ));
        parts.push(format!(
            "max(length({v})) FILTER (WHERE NOT {empty}) AS {}",
            name("max_length")?
        ));
        for (p, pattern) in patterns.iter().enumerate() {
            parts.push(format!(
                "count(*) FILTER (WHERE regexp_like({v}, {})) AS {}",
                quote_text(&format!("^(?:{pattern})$"))?,
                name(&format!("pattern_{p}"))?
            ));
        }
    }
    Ok(Statement {
        sql: format!(
            "WITH s AS (SELECT * FROM {} LIMIT {sample}) SELECT {} FROM s",
            table.sql()?,
            parts.join(", ")
        ),
    })
}

/// The lookup of one entity (decision 0090, rulings 81 and 102): the
/// document in `{{document}}` normalized as a document key is (digits
/// only, no leading zeros) and hashed in Trino with the findings key, then
/// its ICD and band in the latest current run of each of `datasets` whose
/// entity is that hash. One row per dataset: no hash, no other entity.
pub fn entity_lookup(datasets: &[String]) -> Result<Statement, Error> {
    let gold = "\"lake\".\"quality_gold\"";
    let names = datasets
        .iter()
        .map(|d| quote_text(d))
        .collect::<Result<Vec<_>, _>>()?;
    if names.is_empty() {
        return Err(Error::UnknownField("datasets".into()));
    }
    Ok(Statement {
        sql: format!(
            "WITH h AS (SELECT \"key\" FROM {key}), \
             d AS (SELECT lower(to_hex(hmac_sha256(to_utf8(\
             NULLIF(trim(LEADING '0' FROM regexp_replace({{{{document}}}}, '[^0-9]', '')), '')), \
             h.\"key\"))) AS k FROM h), \
             l AS (SELECT dataset, max(cut) AS cut FROM {gold}.\"current_run\" \
             WHERE dataset IN ({names}) GROUP BY dataset) \
             SELECT e.dataset, CAST(e.cut AS varchar) AS cut, e.icd, e.band \
             FROM {gold}.\"entity_icd\" e \
             JOIN {gold}.\"current_run\" r \
             ON e.dataset = r.dataset AND e.cut = r.cut AND e.run_id = r.run_id \
             JOIN l ON l.dataset = e.dataset AND l.cut = e.cut \
             JOIN d ON e.entity = d.k",
            key = crate::FINDINGS_KEY,
            names = names.join(", "),
        ),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn table() -> TablePath {
        TablePath {
            catalog: "src_core".into(),
            namespace: "public".into(),
            table: "loans".into(),
        }
    }

    #[test]
    fn a_page_reads_every_column_as_text_with_a_hard_limit() {
        let cols = ["loan_id".to_string(), "Amount".to_string()];
        let s = source_page(&table(), &cols, Some(("loan_id", true)), 2000, 1001).unwrap();
        assert_eq!(
            s.sql,
            "SELECT CAST(\"loan_id\" AS varchar) AS \"loan_id\", CAST(\"Amount\" AS varchar) AS \"Amount\" \
             FROM \"src_core\".\"public\".\"loans\" ORDER BY \"loan_id\" DESC OFFSET 2000 LIMIT 1001"
        );
        let plain = source_page(&table(), &cols[..1], None, 0, 1001).unwrap();
        assert!(plain.sql.ends_with("\"loans\" OFFSET 0 LIMIT 1001"));
        let odd = source_page(&table(), &["a\"b".into()], None, 0, 1).unwrap();
        assert!(odd
            .sql
            .starts_with("SELECT CAST(\"a\"\"b\" AS varchar) AS \"a\"\"b\" FROM"));
        assert!(source_page(&table(), &["a\u{0}".into()], None, 0, 1).is_err());
    }

    #[test]
    fn a_profile_counts_each_column_of_a_sample() {
        let cols = ["email".to_string()];
        let s = source_profile(&table(), &cols, 40, &[r"\d{5,12}"]).unwrap();
        assert!(s.sql.starts_with(
            "WITH s AS (SELECT * FROM \"src_core\".\"public\".\"loans\" LIMIT 40) SELECT count(*) AS \"sampled\", "
        ));
        assert!(s.sql.contains(
            "count(*) FILTER (WHERE (CAST(\"email\" AS varchar) IS NULL OR regexp_like(CAST(\"email\" AS varchar), '^[\\s\\x{00A0}]*$'))) AS \"c0_empty\""
        ));
        assert!(s.sql.contains(
            "count(*) FILTER (WHERE regexp_like(CAST(\"email\" AS varchar), '^(?:\\d{5,12})$')) AS \"c0_pattern_0\""
        ));
        assert!(s.sql.contains("AS \"c0_max_length\", count(*)"));
        assert!(s.sql.ends_with("AS \"c0_pattern_0\" FROM s"));
        assert!(source_profile(&table(), &cols, 1, &["a'b"])
            .unwrap()
            .sql
            .contains("'^(?:a''b)$'"));
    }

    #[test]
    fn a_lookup_hashes_the_document_in_the_engine_and_reads_its_datasets_only() {
        let s = entity_lookup(&["loans".into(), "cards".into()]).unwrap();
        assert!(s.sql.contains("regexp_replace({{document}}, '[^0-9]', '')"));
        assert!(s
            .sql
            .contains("FROM \"lake\".\"quality_secrets\".\"findings_key\""));
        assert!(s.sql.contains("WHERE dataset IN ('loans', 'cards')"));
        assert!(s.sql.ends_with("JOIN d ON e.entity = d.k"));
        assert!(entity_lookup(&[]).is_err());
    }
}
