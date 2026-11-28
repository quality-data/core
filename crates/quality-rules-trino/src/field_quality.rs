//! `field_quality`'s rows for gold's insert (decision 0090, ruling 40):
//! written by an `INSERT … SELECT`, never read back. One row per mapped
//! field, in the mapping's order: its profile over bronze (ruling 32), its
//! floating flag, its mean ICD from the job's scratch rows, and the update
//! recency on the mapping's updated field only.

use quality::mapping::{FieldRole, Mapping};
use quality_rules_sql::{quote_text, ShapeKind};

use crate::icd::{role, Measured, SCRATCH};
use crate::profile::{field_profile, recency};
use crate::{Error, Statement};

/// The relations a field's row reads, each already pinned by the caller.
#[derive(Debug, Clone, Copy)]
pub struct FieldSources<'a> {
    /// Bronze's accepted load of the cut: the profile reads each field's
    /// source column there.
    pub bronze: &'a str,
    /// Silver's cut: the recency reads the updated field typed as a date.
    pub silver: &'a str,
    /// The job whose scratch rows give the mean ICD; they are written
    /// before this statement runs.
    pub job_id: &'a str,
}

/// `field_quality`'s rows in gold's column order (`field`, `empty`,
/// `distinct_values`, `pattern`, `pattern_count`, `floating`, `mean_icd`,
/// `recency`). `kind` gives the shape kind of a bronze column (from its
/// type in bronze); `measured` is the ICD's fields in their scratch order.
pub fn field_quality(
    mapping: &Mapping,
    from: &FieldSources<'_>,
    measured: &[Measured],
    kind: impl Fn(&str) -> ShapeKind,
) -> Result<Statement, Error> {
    let updated = role(mapping, FieldRole::Updated);
    let means = if measured.is_empty() {
        String::new()
    } else {
        let cols: Vec<String> = (1..=measured.len())
            .map(|n| format!("avg(element_at(f, {n})) AS m{n}"))
            .collect();
        format!(
            "CROSS JOIN (SELECT {} FROM {SCRATCH} WHERE job_id = {}) m ",
            cols.join(", "),
            quote_text(from.job_id)?
        )
    };
    let mut rows = Vec::new();
    let mut cut_params = 0;
    for (name, f) in &mapping.fields {
        let k = kind(&f.value.column);
        let profile = field_profile(from.bronze, &f.value.column, k)?;
        let mean = match measured.iter().position(|m| &m.name == name) {
            Some(i) => format!("m.m{}", i + 1),
            None => "CAST(NULL AS DOUBLE)".into(),
        };
        let (bands, join) = if updated == Some(name) {
            let r = recency(from.silver, name)?;
            cut_params += r.cut_params;
            let names: Vec<String> = (0..r.cut_params).map(|i| format!("r.b{i}")).collect();
            (
                format!("ARRAY[{}]", names.join(", ")),
                format!(
                    "CROSS JOIN ({}) AS r ({}) ",
                    r.sql,
                    (0..r.cut_params)
                        .map(|i| format!("b{i}"))
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
            )
        } else {
            ("CAST(NULL AS ARRAY(BIGINT))".into(), String::new())
        };
        rows.push(format!(
            "SELECT {}, p.e, p.d, p.s, p.n, {}, {mean}, {bands} \
             FROM ({}) AS p (e, d, s, n) {join}{means}",
            quote_text(name)?,
            k.text("v").is_none(),
            profile.sql.replace('\n', " ")
        ));
    }
    if rows.is_empty() {
        return Ok(Statement {
            sql: "SELECT CAST(NULL AS VARCHAR), CAST(NULL AS BIGINT), CAST(NULL AS BIGINT), \
                  CAST(NULL AS VARCHAR), CAST(NULL AS BIGINT), CAST(NULL AS BOOLEAN), \
                  CAST(NULL AS DOUBLE), CAST(NULL AS ARRAY(BIGINT)) WHERE false"
                .into(),
            cut_params: 0,
        });
    }
    Ok(Statement {
        sql: rows.join(" UNION ALL "),
        cut_params,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::icd::{measured, IcdRule};
    use quality_rules_compile::{compile, FieldType, Schema};
    use quality_rules_sql::Tables;

    const MAPPING: &str = "dataset: loans
source: loans
fields:
  Code: { column: code, type: text }
  Amount: { column: amount, type: number }
  Updated: { column: updated, type: date, roles: [updated] }
";

    fn sources() -> FieldSources<'static> {
        FieldSources {
            bronze: "b",
            silver: "s",
            job_id: "j1",
        }
    }

    fn kind(column: &str) -> ShapeKind {
        ShapeKind::of_type(if column == "amount" {
            "double"
        } else {
            "varchar"
        })
    }

    #[test]
    fn one_row_per_field_with_recency_on_the_updated_field_only() {
        let mapping = Mapping::parse(MAPPING).unwrap();
        let s = field_quality(&mapping, &sources(), &[], kind).unwrap();
        assert_eq!(s.sql.matches("UNION ALL").count(), 2);
        assert_eq!(s.cut_params, 7);
        assert_eq!(
            s.sql.matches("CROSS JOIN (SELECT count(*) FILTER").count(),
            1
        );
        assert!(s
            .sql
            .contains("SELECT 'Updated', p.e, p.d, p.s, p.n, false, "));
        assert!(s
            .sql
            .contains("SELECT 'Amount', p.e, p.d, p.s, p.n, true, "));
        assert!(s.sql.contains("CAST(\"amount\" AS VARCHAR) AS v"));
        assert!(s
            .sql
            .contains("ARRAY[r.b0, r.b1, r.b2, r.b3, r.b4, r.b5, r.b6]"));
        assert!(s.sql.contains("\"Updated\" AS DATE), CAST(? AS DATE))"));
        assert!(!s.sql.contains(SCRATCH));
    }

    #[test]
    fn the_mean_icd_comes_from_the_scratch_rows_in_their_order() {
        let mapping = Mapping::parse(MAPPING).unwrap();
        let yaml = "dataset: loans\nrules:\n  \
                    - { id: A1, field: Amount, check: required, severity: major, alert: a }\n";
        let mut schema = Schema::default();
        schema.fields.insert("Amount".into(), FieldType::Number);
        let (compiled, _) = compile(&quality_rules_model::parse(yaml).unwrap(), &schema);
        let rules: Vec<IcdRule> = compiled
            .iter()
            .map(|r| IcdRule::of(r, &Tables::new()).unwrap())
            .collect();
        let fields = measured(&mapping, &rules);
        let s = field_quality(&mapping, &sources(), &fields, kind).unwrap();
        assert!(s.sql.contains(&format!(
            "CROSS JOIN (SELECT avg(element_at(f, 1)) AS m1 FROM {SCRATCH} WHERE job_id = 'j1') m"
        )));
        assert!(s.sql.contains("true, m.m1, CAST(NULL AS ARRAY(BIGINT))"));
        assert!(s
            .sql
            .contains("false, CAST(NULL AS DOUBLE), CAST(NULL AS ARRAY(BIGINT))"));
    }
}
