//! The SQL of a cut's distributions, the same text on every engine
//! (decision 0090, point 8.3): exact percentiles by a window over the
//! distinct values, and each group's median as its middle row or two. No
//! engine's own `quantile_cont` or `median`: their arithmetic is not
//! reproducible on another engine.

use crate::double;

/// The percentiles 0 to 100 of `x` over `values`, a query whose rows have
/// a `DOUBLE` column `x` (`NULL`s skipped): rows `(k, q)`, `k` from 0 to
/// 100, none when no value is left. Between two values at a fraction `d`,
/// `lo * (1 - d) + hi * d`; at a value, the value itself.
pub fn percentiles(values: &str) -> String {
    let at: Vec<String> = (0..=100)
        .map(|k| format!("({k}, {} / {})", double(f64::from(k)), double(100.0)))
        .collect();
    let running = "sum(c) OVER (ORDER BY x ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)";
    format!(
        "WITH v AS ({values}),
              d AS (SELECT x, count(*) AS c FROM v WHERE x IS NOT NULL GROUP BY x),
              r AS (SELECT x, {running} - c AS lo, {running} AS hi FROM d),
              n AS (SELECT sum(c) AS n FROM d),
              p AS (SELECT * FROM (VALUES {at}) s(k, p)),
              h AS (SELECT k, (CAST(n AS DOUBLE) - DOUBLE '1') * p AS h FROM p, n),
              b AS (SELECT k, h, floor(h) AS i, ceil(h) AS j FROM h)
         SELECT b.k, CASE WHEN b.i = b.j THEN a.x
                          ELSE a.x * (DOUBLE '1' - (b.h - b.i)) + z.x * (b.h - b.i) END AS q
           FROM b JOIN r a ON b.i >= a.lo AND b.i < a.hi
                  JOIN r z ON b.j >= z.lo AND b.j < z.hi
          ORDER BY b.k ASC",
        at = at.join(", "),
    )
}

/// Each group's median over `relation`, whose rows have a group `g` and a
/// `DOUBLE` `x` (`NULL`s skipped): rows `(g, med)`. The middle row, or the
/// two middle rows as [`percentiles`] gives them at 0.5.
pub fn medians(relation: &str) -> String {
    format!(
        "SELECT g, CASE WHEN count(*) = 1 THEN min(x)
                        ELSE min(x) * DOUBLE '0.5' + max(x) * DOUBLE '0.5' END AS med
           FROM (SELECT g, x, row_number() OVER (PARTITION BY g ORDER BY x ASC NULLS LAST) AS i,
                        count(*) OVER (PARTITION BY g) AS n
                   FROM {relation} WHERE x IS NOT NULL) o
          WHERE 2 * i IN (n, n + 1, n + 2)
          GROUP BY g"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_percentiles_read_their_values_and_name_no_engine_function() {
        let sql = percentiles("SELECT a AS x FROM t");
        assert!(sql.starts_with("WITH v AS (SELECT a AS x FROM t),"));
        assert!(sql.contains("(100, DOUBLE '100' / DOUBLE '100')"));
        for word in ["quantile", "median", "::", "percentile"] {
            assert!(!sql.contains(word), "{word}");
            assert!(!medians("v").contains(word), "{word}");
        }
    }
}
