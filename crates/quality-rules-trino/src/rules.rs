//! Every rule over a cut in one pass: per rule, the records it fails, those
//! it passes and those it skips (`NULL`), from the same tree DuckDB's SQL
//! comes from.

use quality_rules_compile::Compiled;
use quality_rules_sql::{Engine, Tables};

use crate::{Error, Statement};

/// One row: for each rule in order, its failing, passing and skipped
/// counts (`bigint`), over `from` (a pinned relation).
pub fn rule_counts(from: &str, rules: &[Compiled], tables: &Tables) -> Result<Statement, Error> {
    let mut columns = Vec::new();
    let mut counts = Vec::new();
    let mut cut_params = 0;
    for (i, rule) in rules.iter().enumerate() {
        let sql = quality_rules_sql::rule(rule, Engine::Trino, tables)
            .map_err(|e| Error::Rule(rule.id.clone(), e))?;
        cut_params += sql.cut_params;
        columns.push(format!("{} AS \"r{i}\"", sql.text));
        counts.push(format!(
            "count(*) FILTER (WHERE \"r{i}\"), count(*) FILTER (WHERE NOT \"r{i}\"), \
             count(*) FILTER (WHERE \"r{i}\" IS NULL)"
        ));
    }
    if rules.is_empty() {
        columns.push("1 AS \"r\"".into());
        counts.push("count(*)".into());
    }
    Ok(Statement {
        sql: format!(
            "WITH r AS (SELECT {} FROM {from}) SELECT {} FROM r",
            columns.join(", "),
            counts.join(", ")
        ),
        cut_params,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use quality_rules_compile::{compile, FieldType, Schema};

    #[test]
    fn the_counts_are_three_per_rule_with_the_cut_bound() {
        let yaml = "dataset: loans\nrules:\n  \
                    - { id: R1, field: Code, check: required, severity: major, alert: a }\n  \
                    - { id: R2, check: expression, fails: Opened > :cut, dimension: validity, severity: minor, alert: a }\n";
        let mut schema = Schema::default();
        schema.fields.insert("Code".into(), FieldType::Text);
        schema.fields.insert("Opened".into(), FieldType::Date);
        let file = quality_rules_model::parse(yaml).unwrap();
        let (compiled, problems) = compile(&file, &schema);
        assert!(problems.is_empty(), "{problems:?}");
        let s = rule_counts("t", &compiled, &Tables::new()).unwrap();
        assert_eq!(s.cut_params, 2, "the fails and its negation: {}", s.sql);
        assert!(s.sql.starts_with("WITH r AS (SELECT "));
        assert!(s
            .sql
            .contains("AS \"r1\" FROM t) SELECT count(*) FILTER (WHERE \"r0\")"));
        assert!(s.sql.contains("trim(BOTH concat(' ', chr(160)) FROM"));
        assert_eq!(s.sql.matches("count(*) FILTER").count(), 6);
    }
}
