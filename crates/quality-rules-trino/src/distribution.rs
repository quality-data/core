//! A cut's distributions on Trino (decision 0090, point 8.3, step 12): a
//! drift variable's percentiles 0 to 100, exact, and the outliers of an
//! amount by robust z within each group of a dimension. The percentiles
//! are the window SQL both engines share
//! (`quality_rules_sql::distribution`); above a distinct-value count the
//! bracket method gives the same values while sorting only the buckets the
//! ranks fall in.

use quality_rules_sql::distribution::{medians, percentiles};
use quality_rules_sql::double;

use crate::{quoted, Error, Statement};

/// Robust z beyond which an amount is an outlier.
const Z: f64 = 3.5;
/// The least spread a group's MAD is taken as, in log10 units.
const MAD_FLOOR: f64 = 0.10;
/// What scales a MAD to a normal's standard deviation.
const MAD_SCALE: f64 = 0.6745;
/// The most groups kept per dimension, those with the most amounts.
const MOST_GROUPS: usize = 1000;
/// The buckets of the bracket method's first pass.
const BUCKETS: u32 = 4096;

/// How a variable's percentiles are computed: exact either way.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Method {
    /// The window over every distinct value.
    Window,
    /// Buckets first, then a sort of the buckets the ranks fall in.
    Bracket,
}

impl Method {
    /// The window up to `limit` distinct values (the configured
    /// `QD_EXACT_SORT_LIMIT`), the bracket method above it.
    pub fn for_distinct(distinct: u64, limit: u64) -> Self {
        if distinct <= limit {
            Self::Window
        } else {
            Self::Bracket
        }
    }
}

/// Rows `(k, q)`, `k` from 0 to 100: `field`'s percentiles over `from`.
pub fn variable_percentiles(from: &str, field: &str, method: Method) -> Result<Statement, Error> {
    let values = format!("SELECT CAST({} AS DOUBLE) AS x FROM {from}", quoted(field)?);
    let sql = match method {
        Method::Window => percentiles(&values),
        Method::Bracket => bracket(&values),
    };
    Ok(Statement { sql })
}

/// The bracket method: every value in one of `BUCKETS` equal-width buckets
/// of the finite range (`-Infinity` before, `+Infinity` and `NaN` after, in
/// SQL's order), the buckets' running counts, and a sort only of the
/// buckets holding a rank a percentile reads. The same positions and the
/// same interpolation as the window, so the same bits.
fn bracket(values: &str) -> String {
    let at: Vec<String> = (0..=100)
        .map(|k| format!("({k}, {} / {})", double(f64::from(k)), double(100.0)))
        .collect();
    let (b, inf, nan) = (BUCKETS, BUCKETS + 1, BUCKETS + 2);
    let running = "sum(c) OVER (ORDER BY k ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)";
    format!(
        "WITH v AS ({values}),
              d AS (SELECT x FROM v WHERE x IS NOT NULL),
              m AS (SELECT count(*) AS n, min(x) FILTER (WHERE is_finite(x)) AS lo,
                           max(x) FILTER (WHERE is_finite(x)) AS hi FROM d),
              b AS (SELECT d.x, CASE WHEN is_nan(d.x) THEN {nan}
                                     WHEN d.x = -infinity() THEN 0
                                     WHEN d.x = infinity() THEN {inf}
                                     WHEN m.hi = m.lo THEN 1
                                     ELSE least(width_bucket(d.x, m.lo, m.hi, {b}), {b}) END AS k
                      FROM d CROSS JOIN m),
              c AS (SELECT k, count(*) AS c FROM b GROUP BY k),
              r AS (SELECT k, {running} - c AS lo, {running} AS hi FROM c),
              p AS (SELECT * FROM (VALUES {at}) s(pk, p)),
              h AS (SELECT pk, (CAST(n AS DOUBLE) - DOUBLE '1') * p AS h FROM p CROSS JOIN m),
              w AS (SELECT pk, h, floor(h) AS i, ceil(h) AS j FROM h),
              need AS (SELECT r.k, r.lo FROM r WHERE EXISTS (SELECT 1 FROM w
                        WHERE (w.i >= r.lo AND w.i < r.hi) OR (w.j >= r.lo AND w.j < r.hi))),
              s AS (SELECT b.x, need.lo + row_number() OVER (PARTITION BY b.k
                                ORDER BY b.x ASC) - 1 AS pos
                      FROM b JOIN need ON need.k = b.k)
         SELECT w.pk AS k, CASE WHEN w.i = w.j THEN a.x
                                ELSE a.x * (DOUBLE '1' - (w.h - w.i)) + z.x * (w.h - w.i) END AS q
           FROM w JOIN s a ON a.pos = w.i JOIN s z ON z.pos = w.j
          ORDER BY w.pk ASC",
        at = at.join(", "),
    )
}

/// Per group of `dimension` over `from`: its positive amounts and those
/// whose robust z (log10, the MAD floored) is below -3.5 or above 3.5.
pub fn outliers(from: &str, amount: &str, dimension: &str) -> Result<Statement, Error> {
    let (a, d) = (quoted(amount)?, quoted(dimension)?);
    Ok(Statement {
        sql: format!(
            "WITH v AS (SELECT COALESCE(NULLIF(trim(BOTH ' ' FROM CAST({d} AS VARCHAR)), ''), '')
                                 AS g, log10(CAST({a} AS DOUBLE)) AS x
                          FROM {from} WHERE CAST({a} AS DOUBLE) > DOUBLE '0'),
                  m AS ({med}),
                  e AS (SELECT v.g, abs(v.x - m.med) AS x FROM v JOIN m ON m.g = v.g),
                  s AS ({mad}),
                  z AS (SELECT v.g, {k} * (v.x - m.med)
                                    / CASE WHEN s.med < {floor} THEN {floor} ELSE s.med END AS z
                          FROM v JOIN m ON m.g = v.g JOIN s ON s.g = v.g)
             SELECT g, count(*) AS n, count(*) FILTER (WHERE z < {low}),
                    count(*) FILTER (WHERE z > {high})
               FROM z GROUP BY g
              ORDER BY n DESC NULLS LAST, g ASC NULLS LAST LIMIT {MOST_GROUPS}",
            med = medians("v"),
            mad = medians("e"),
            k = double(MAD_SCALE),
            floor = double(MAD_FLOOR),
            low = double(-Z),
            high = double(Z),
        ),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_window_is_the_shared_sql_and_the_bracket_its_positions() {
        let w = variable_percentiles("t", "Amount", Method::Window).unwrap();
        assert_eq!(
            w.sql,
            percentiles("SELECT CAST(\"Amount\" AS DOUBLE) AS x FROM t")
        );
        let b = variable_percentiles("t", "Amount", Method::Bracket)
            .unwrap()
            .sql;
        assert!(b.contains("least(width_bucket(d.x, m.lo, m.hi, 4096), 4096)"));
        assert!(b.contains("a.x * (DOUBLE '1' - (w.h - w.i)) + z.x * (w.h - w.i)"));
        for word in ["approx_percentile", "quantile", "median(", "::"] {
            assert!(!b.contains(word), "{word}");
        }
        assert_eq!(Method::for_distinct(10, 10), Method::Window);
        assert_eq!(Method::for_distinct(11, 10), Method::Bracket);
    }

    #[test]
    fn the_outliers_take_the_shared_medians_and_double_constants() {
        let o = outliers("t", "Income", "Office").unwrap().sql;
        assert!(o.contains(&medians("v")));
        assert!(o.contains("CASE WHEN s.med < DOUBLE '0.1' THEN DOUBLE '0.1' ELSE s.med END"));
        assert!(o.contains("z < DOUBLE '-3.5'") && o.contains("z > DOUBLE '3.5'"));
    }
}
