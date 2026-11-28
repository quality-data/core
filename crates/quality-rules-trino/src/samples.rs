//! A rule tested with its records (decision 0043, point 3; decision 0090,
//! ruling 83): one values read per rule over the cut, read as
//! `job_sample_<d>`: a row naming the fields shown, the rule's counts by
//! outcome, a few records of each outcome by the key, and its largest
//! groups of failures by the shape of what it read, each with a few
//! records. Values come out here as text; the core masks and audits them.

use quality_rules_compile::Compiled;
use quality_rules_sql::{quote_text, Cut, Engine, Tables};

use crate::{quoted, shape_of, Error, Statement};

/// The columns before the fields' values: what the row is (`fields`,
/// `counts`, `record`, `group`), the rule, the outcome, the group's shape
/// and a count.
pub const SAMPLE_HEAD: [(&str, &str); 5] = [
    ("part", "varchar"),
    ("rule", "varchar"),
    ("outcome", "varchar"),
    ("shape", "varchar"),
    ("n", "bigint"),
];

/// The name of the `i`-th field's column.
pub fn sample_column(i: usize) -> String {
    format!("c{i}")
}

/// The most rows a rule's read gives: its fields row, three counts, `each`
/// records of each outcome and `top` groups of `each` records.
pub fn sample_rows(each: u64, top: u64) -> u64 {
    1 + 3 + 3 * each + top * each
}

/// The fields a rule's read shows, each once: the key's (which order the
/// records), those the rule reads, then `extra`.
pub fn shown_fields(rule: &Compiled, key: &[String], extra: &[String]) -> Vec<String> {
    let mut fields: Vec<String> = Vec::new();
    for f in key
        .iter()
        .cloned()
        .chain(rule.reads())
        .chain(extra.iter().cloned())
    {
        if !fields.contains(&f) {
            fields.push(f);
        }
    }
    fields
}

/// The read of `rule` over `from` (a pinned relation) at `cut`, showing
/// `fields` (from [`shown_fields`]), `each` records per outcome and the
/// `top` largest groups of failures.
pub fn rule_samples(
    from: &str,
    rule: &Compiled,
    tables: &Tables,
    cut: &Cut,
    fields: &[String],
    key_len: usize,
    (each, top): (u64, u64),
) -> Result<Statement, Error> {
    let sql = quality_rules_sql::rule_at(rule, Engine::Trino, tables, cut)
        .map_err(|e| Error::Rule(rule.id.clone(), e))?;
    let id = quote_text(&rule.id)?;
    let mut cast = Vec::new();
    let mut names = Vec::new();
    let mut nulls = Vec::new();
    let mut labels = Vec::new();
    for (i, f) in fields.iter().enumerate() {
        let c = quoted(&sample_column(i))?;
        cast.push(format!("CAST({} AS varchar) AS {c}", quoted(f)?));
        nulls.push(format!("CAST(NULL AS varchar) AS {c}"));
        labels.push(format!("{} AS {c}", quote_text(f)?));
        names.push(c);
    }
    let read = rule.reads();
    let shape = if read.is_empty() {
        "'∅'".to_owned()
    } else {
        read.iter()
            .map(|f| shape_of(f))
            .collect::<Result<Vec<_>, Error>>()?
            .join(" || ' · ' || ")
    };
    let order = names
        .iter()
        .take(key_len.max(1))
        .map(|c| format!("{c} ASC NULLS LAST"))
        .collect::<Vec<_>>();
    let order = if order.is_empty() {
        "1".to_owned()
    } else {
        order.join(", ")
    };
    let sep = |v: &[String]| {
        if v.is_empty() {
            String::new()
        } else {
            format!(", {}", v.join(", "))
        }
    };
    let (cast, names, nulls, labels) = (sep(&cast), sep(&names), sep(&nulls), sep(&labels));
    let none = "CAST(NULL AS varchar)";
    Ok(Statement {
        sql: format!(
            "WITH o AS (SELECT ({rule}) AS o, {shape} AS shape{cast} FROM {from}), \
             r AS (SELECT CASE WHEN o THEN 'fails' WHEN NOT o THEN 'passes' ELSE 'skips' END AS outcome, \
             shape{names} FROM o), \
             k AS (SELECT *, row_number() OVER (PARTITION BY outcome ORDER BY {order}) AS k FROM r), \
             g AS (SELECT shape, count(*) AS n FROM r WHERE outcome = 'fails' GROUP BY shape \
             ORDER BY n DESC, shape ASC LIMIT {top}), \
             f AS (SELECT r.*, g.n, row_number() OVER (PARTITION BY r.shape ORDER BY {order}) AS k \
             FROM r JOIN g ON r.shape = g.shape WHERE r.outcome = 'fails') \
             SELECT 'fields' AS part, {id} AS rule, {none} AS outcome, {none} AS shape, \
             CAST(NULL AS bigint) AS n{labels} \
             UNION ALL SELECT 'counts', {id}, outcome, {none}, count(*){nulls} FROM r GROUP BY outcome \
             UNION ALL SELECT 'record', {id}, outcome, {none}, CAST(NULL AS bigint){names} FROM k WHERE k <= {each} \
             UNION ALL SELECT 'group', {id}, 'fails', shape, n{names} FROM f WHERE k <= {each}",
            rule = sql.text,
        ),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use quality_rules_compile::{compile, FieldType, Schema};

    #[test]
    fn a_rules_read_names_its_fields_counts_and_bounds_its_records() {
        let yaml = "dataset: loans\nrules:\n  \
                    - { id: R1, field: Code, check: required, severity: major, alert: a }\n";
        let mut schema = Schema::default();
        schema.fields.insert("Code".into(), FieldType::Text);
        let file = quality_rules_model::parse(yaml).unwrap();
        let (rules, _) = compile(&file, &schema);
        let fields = shown_fields(&rules[0], &["Id".into()], &["Code".into(), "Name".into()]);
        assert_eq!(fields, ["Id", "Code", "Name"]);
        let cut = Cut::parse("2026-10-31").unwrap();
        let s = rule_samples("t", &rules[0], &Tables::new(), &cut, &fields, 1, (10, 20)).unwrap();
        assert!(s.sql.contains("SELECT 'fields' AS part, 'R1' AS rule"));
        assert!(s
            .sql
            .contains("'Id' AS \"c0\", 'Code' AS \"c1\", 'Name' AS \"c2\""));
        assert!(s.sql.contains("CAST(\"Code\" AS varchar) AS \"c1\""));
        assert!(s.sql.contains("ORDER BY \"c0\" ASC NULLS LAST"));
        assert!(s.sql.contains("WHERE k <= 10") && s.sql.contains("LIMIT 20"));
        assert_eq!(sample_rows(10, 20), 234);
    }
}
