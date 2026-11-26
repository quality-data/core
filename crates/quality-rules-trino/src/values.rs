//! Values reads (decision 0090, point 7.4 and rulings 79 to 81): the only
//! statements whose rows carry values, each with a hard `LIMIT`. Their rows
//! go to the core on the values route, never into gold or a report.

use crate::{quoted, Error, Statement, TablePath};

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
}
