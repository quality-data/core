//! Gold's rows read from the job's scratch rows (decision 0090, ruling
//! 40): `group_metrics`, `cells` and `entity_icd`, each written by an
//! `INSERT … SELECT` after the scratch insert, never read back. Each gives
//! its table's columns in gold's order.

use quality::mapping::{FieldRole, Mapping};
use quality_rules_sql::{double, quote_text};

use crate::icd::{with_role, Measured};
use crate::scratch::{cells, entities, groups};
use crate::{Error, Statement};

/// No newline in a statement: it may be bound through `EXECUTE IMMEDIATE`.
fn line(sql: &str) -> String {
    sql.replace('\n', " ")
}

fn none(columns: &[&str]) -> Statement {
    let cols: Vec<String> = columns
        .iter()
        .map(|t| format!("CAST(NULL AS {t})"))
        .collect();
    Statement {
        sql: format!("SELECT {} WHERE false", cols.join(", ")),
        cut_params: 0,
    }
}

/// `group_metrics`' rows: per dimension field, in the mapping's order, its
/// largest groups, an empty group as `''`, with their records, those with
/// a serious failure and their mean ICD.
pub fn group_metrics(job_id: &str, mapping: &Mapping) -> Result<Statement, Error> {
    let dimensions = with_role(mapping, FieldRole::Dimension);
    let per = groups(job_id, mapping)?;
    if per.is_empty() {
        return Ok(none(&["VARCHAR", "VARCHAR", "BIGINT", "BIGINT", "DOUBLE"]));
    }
    let rows: Vec<String> = dimensions
        .iter()
        .zip(per)
        .map(|(d, g)| {
            Ok(format!(
                "SELECT {}, COALESCE(g.grp, ''), g.n, g.serious, g.mean \
                 FROM ({}) AS g (grp, n, serious, mean)",
                quote_text(d)?,
                line(&g.sql)
            ))
        })
        .collect::<Result<_, Error>>()?;
    Ok(Statement {
        sql: rows.join(" UNION ALL "),
        cut_params: 0,
    })
}

/// `cells`' rows: per SFC field, in the mapping's order, its right records
/// among the active ones, the active records, and whether it passes with
/// what it misses and its margin, by the threshold `ceil(0.90 n)` in
/// integers (decision 0058); a cell without active records does not pass.
pub fn cell_rows(job_id: &str, mapping: &Mapping) -> Result<Statement, Error> {
    let names = with_role(mapping, FieldRole::Sfc);
    if names.is_empty() {
        return Ok(none(&[
            "VARCHAR", "BIGINT", "BIGINT", "BOOLEAN", "BIGINT", "BIGINT",
        ]));
    }
    let read = cells(job_id, names.len())?;
    let cols: Vec<String> = (1..=names.len()).map(|j| format!("o{j}")).collect();
    let labels: Vec<String> = names
        .iter()
        .map(|n| quote_text(n))
        .collect::<Result<_, _>>()?;
    Ok(Statement {
        sql: format!(
            "SELECT u.cell, u.ok, c.t, c.t > 0 AND u.ok >= c.h, \
             greatest(c.h - u.ok, 0), greatest(u.ok - c.h, 0) \
             FROM (SELECT x.*, (9 * x.t + 9) / 10 AS h FROM ({}) AS x (t, {})) AS c \
             CROSS JOIN UNNEST(ARRAY[{}], ARRAY[{}]) AS u (cell, ok)",
            read.sql,
            cols.join(", "),
            labels.join(", "),
            cols.iter()
                .map(|c| format!("c.{c}"))
                .collect::<Vec<_>>()
                .join(", ")
        ),
        cut_params: 0,
    })
}

/// `entity_icd`'s rows: each entity's keyed hash (the scratch rows never
/// hold the key in clear), its ICD and its band (high from 0.90, medium
/// from 0.75, low below; none without an ICD).
pub fn entity_icd(job_id: &str, fields: &[Measured]) -> Result<Statement, Error> {
    let e = entities(job_id, fields)?;
    Ok(Statement {
        sql: format!(
            "SELECT e.k, e.icd, CASE WHEN e.icd >= {} THEN 'high' \
             WHEN e.icd >= {} THEN 'medium' WHEN e.icd IS NOT NULL THEN 'low' END \
             FROM ({}) AS e (k, icd, s)",
            double(0.90),
            double(0.75),
            line(&e.sql)
        ),
        cut_params: 0,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SCRATCH;

    const MAPPING: &str = "dataset: loans
source: loans
fields:
  Doc: { column: doc, type: text, roles: [entity] }
  Type: { column: kind, type: text, roles: [dimension] }
  Office: { column: office, type: text, roles: [dimension] }
  Phone: { column: phone, type: text, roles: [sfc] }
  Mail: { column: mail, type: text, roles: [sfc] }
";

    #[test]
    fn groups_per_dimension_in_the_mappings_order() {
        let m = Mapping::parse(MAPPING).unwrap();
        let s = group_metrics("j1", &m).unwrap();
        assert_eq!(s.sql.matches("UNION ALL").count(), 1);
        assert!(s
            .sql
            .starts_with("SELECT 'Office', COALESCE(g.grp, ''), g.n"));
        assert!(s.sql.contains("SELECT 'Type', COALESCE(g.grp, '')"));
        assert!(s.sql.contains("GROUP BY element_at(g, 2)"));
        assert!(s
            .sql
            .contains(&format!("FROM {SCRATCH} WHERE job_id = 'j1'")));
        assert!(!s.sql.contains('\n'));
    }

    #[test]
    fn cells_carry_the_threshold_in_integers() {
        let m = Mapping::parse(MAPPING).unwrap();
        let s = cell_rows("j1", &m).unwrap();
        assert!(s.sql.contains("(9 * x.t + 9) / 10 AS h"));
        assert!(s.sql.contains("AS x (t, o1, o2)"));
        assert!(s
            .sql
            .contains("UNNEST(ARRAY['Mail', 'Phone'], ARRAY[c.o1, c.o2]) AS u (cell, ok)"));
        assert!(s.sql.contains("c.t > 0 AND u.ok >= c.h"));
    }

    #[test]
    fn entities_take_their_band_and_no_dimension_gives_no_row() {
        let e = entity_icd("j1", &[]).unwrap();
        assert!(e
            .sql
            .contains("WHEN e.icd >= DOUBLE '0.9' THEN 'high' WHEN e.icd >= DOUBLE '0.75'"));
        assert!(!e.sql.contains('\n'));
        let m = Mapping::parse("dataset: d\nsource: d\nfields:\n  A: { column: a, type: text }\n")
            .unwrap();
        assert!(group_metrics("j1", &m)
            .unwrap()
            .sql
            .ends_with("WHERE false"));
        assert!(cell_rows("j1", &m).unwrap().sql.ends_with("WHERE false"));
    }
}
