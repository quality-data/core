//! `distribution_checks`' rows for gold's insert (decision 0090, ruling
//! 40), never read back: a drift variable's percentiles as one row (its
//! dimension and group `''`, as the index stores them), and the outlier
//! amount's groups per dimension. Both methods' percentile rows go in the
//! bundle, each after its guard count (amendment A1: 1 runs it, 0 skips).

use quality::mapping::{FieldRole, Mapping};
use quality_rules_sql::quote_text;

use crate::distribution::{outliers, variable_percentiles, Method};
use crate::icd::{role, with_role};
use crate::{quoted, Error, Statement};

fn line(sql: &str) -> String {
    sql.replace('\n', " ")
}

/// One row: 1 when `method` is the one for `variable`'s distinct values
/// over `from` and `limit` (`Method::for_distinct`), else 0.
pub fn percentile_guard(
    from: &str,
    variable: &str,
    limit: u64,
    method: Method,
) -> Result<Statement, Error> {
    let op = match method {
        Method::Window => "<=",
        Method::Bracket => ">",
    };
    Ok(Statement {
        sql: format!(
            "SELECT CAST(IF(count(DISTINCT CAST({} AS DOUBLE)) {op} {limit}, 1, 0) AS BIGINT) \
             FROM {from}",
            quoted(variable)?
        ),
    })
}

/// `variable`'s row: its 101 percentiles in order; none without a value.
pub fn percentile_row(from: &str, variable: &str, method: Method) -> Result<Statement, Error> {
    let p = variable_percentiles(from, variable, method)?;
    Ok(Statement {
        sql: format!(
            "SELECT {}, '', '', array_agg(p.q ORDER BY p.k), CAST(NULL AS BIGINT), \
             CAST(NULL AS BIGINT), CAST(NULL AS BIGINT) FROM ({}) AS p (k, q) \
             HAVING count(*) > 0",
            quote_text(variable)?,
            line(&p.sql)
        ),
    })
}

/// The outlier amount's groups in each dimension, in the mapping's order:
/// their positive amounts and those below and above the robust z; no row
/// without an amount or a dimension.
pub fn outlier_rows(from: &str, mapping: &Mapping) -> Result<Statement, Error> {
    let dims = with_role(mapping, FieldRole::Dimension);
    let Some(amount) = role(mapping, FieldRole::OutlierAmount).filter(|_| !dims.is_empty()) else {
        return Ok(Statement {
            sql: "SELECT CAST(NULL AS VARCHAR), CAST(NULL AS VARCHAR), CAST(NULL AS VARCHAR), \
                  CAST(NULL AS ARRAY(DOUBLE)), CAST(NULL AS BIGINT), CAST(NULL AS BIGINT), \
                  CAST(NULL AS BIGINT) WHERE false"
                .into(),
        });
    };
    let rows: Vec<String> = dims
        .iter()
        .map(|d| {
            Ok(format!(
                "SELECT {}, {}, o.g, CAST(NULL AS ARRAY(DOUBLE)), o.n, o.low, o.high \
                 FROM ({}) AS o (g, n, low, high)",
                quote_text(amount)?,
                quote_text(d)?,
                line(&outliers(from, amount, d)?.sql)
            ))
        })
        .collect::<Result<_, Error>>()?;
    Ok(Statement {
        sql: rows.join(" UNION ALL "),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn each_method_has_its_guard_and_one_row() {
        let w = percentile_guard("t", "Income", 10, Method::Window).unwrap();
        assert!(w
            .sql
            .contains("count(DISTINCT CAST(\"Income\" AS DOUBLE)) <= 10, 1, 0)"));
        let b = percentile_guard("t", "Income", 10, Method::Bracket).unwrap();
        assert!(b.sql.contains(") > 10, 1, 0)"));
        let r = percentile_row("t", "Income", Method::Bracket).unwrap();
        assert!(r
            .sql
            .starts_with("SELECT 'Income', '', '', array_agg(p.q ORDER BY p.k)"));
        assert!(r.sql.ends_with("AS p (k, q) HAVING count(*) > 0"));
        assert!(!r.sql.contains('\n'));
    }

    #[test]
    fn outliers_per_dimension_or_none() {
        let m = Mapping::parse(
            "dataset: d\nsource: d\nfields:\n  \
             Income: { column: i, type: number, roles: [outlier_amount] }\n  \
             Office: { column: o, type: text, roles: [dimension] }\n  \
             Type: { column: t, type: text, roles: [dimension] }\n",
        )
        .unwrap();
        let s = outlier_rows("t", &m).unwrap();
        assert_eq!(s.sql.matches(" UNION ALL ").count(), 1);
        assert!(s.sql.starts_with("SELECT 'Income', 'Office', o.g"));
        assert!(s.sql.contains("SELECT 'Income', 'Type', o.g"));
        let m = Mapping::parse(
            "dataset: d\nsource: d\nfields:\n  \
             Income: { column: i, type: number, roles: [outlier_amount] }\n",
        )
        .unwrap();
        assert!(outlier_rows("t", &m).unwrap().sql.ends_with("WHERE false"));
    }
}
