//! A cut's distributions (decision 0031's `distribution_checks`, the
//! guide's formulas 7 and 8): a drift variable's percentiles, for the PSI
//! of each cut against the one before; and the outliers of an amount by
//! robust z within each group of a dimension. Counts and percentiles,
//! never a record.

use quality::worker_api::OutlierGroup;

use quality_rules_sql::distribution::{medians, percentiles};
use quality_rules_sql::double;

use crate::{quoted, Error, Runner};

/// Robust z beyond which an amount is an outlier.
const Z: f64 = 3.5;
/// The least spread a group's MAD is taken as, in log10 units.
const MAD_FLOOR: f64 = 0.10;
/// What scales a MAD to a normal's standard deviation, as robust z reads it.
const MAD_SCALE: f64 = 0.6745;
/// The most groups kept per dimension, those with the most amounts.
const MOST_GROUPS: usize = 1000;

impl Runner {
    /// `field`'s percentiles 0 to 100 in the loaded `dataset`, over its
    /// non-empty values; none if it has none.
    pub fn percentiles(&self, dataset: &str, field: &str) -> Result<Option<Vec<f64>>, Error> {
        let sql = percentiles(&format!(
            "SELECT CAST({} AS DOUBLE) AS x FROM {}",
            quoted(field)?,
            quoted(dataset)?
        ));
        let mut stmt = self.con.prepare(&sql)?;
        let all: Vec<Option<f64>> = stmt
            .query_map([], |r| r.get(1))?
            .collect::<Result<_, _>>()?;
        Ok(all
            .into_iter()
            .collect::<Option<Vec<f64>>>()
            .filter(|p| p.len() == 101))
    }

    /// The outliers of `amount` (positive, in log10) within each group of
    /// each of `dimensions`: per group its amounts and those whose robust z
    /// is below -3.5 or above 3.5.
    pub fn outliers(
        &self,
        dataset: &str,
        amount: &str,
        dimensions: &[&str],
    ) -> Result<Vec<OutlierGroup>, Error> {
        let mut out = Vec::new();
        for dimension in dimensions {
            let sql = format!(
                "WITH v AS (SELECT COALESCE(NULLIF(trim(BOTH ' ' FROM CAST({d} AS VARCHAR)), ''),
                                            '') AS g,
                                   log10(CAST({a} AS DOUBLE)) AS x
                              FROM {from} WHERE CAST({a} AS DOUBLE) > DOUBLE '0'),
                      m AS ({med}),
                      e AS (SELECT v.g, abs(v.x - m.med) AS x FROM v JOIN m ON m.g = v.g),
                      s AS ({mad}),
                      z AS (SELECT v.g, {k} * (v.x - m.med)
                                        / CASE WHEN s.med < {floor} THEN {floor} ELSE s.med END
                                        AS z
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
                d = quoted(dimension)?,
                a = quoted(amount)?,
                from = quoted(dataset)?,
            );
            let mut stmt = self.con.prepare(&sql)?;
            let rows = stmt.query_map([], |r| {
                let g: String = r.get(0)?;
                Ok(OutlierGroup {
                    variable: amount.to_string(),
                    dimension: dimension.to_string(),
                    group: (!g.is_empty()).then_some(g),
                    records: r.get(1)?,
                    low: r.get(2)?,
                    high: r.get(3)?,
                })
            })?;
            for row in rows {
                out.push(row?);
            }
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_variables_percentiles_span_its_values() {
        let runner = Runner::new().unwrap();
        runner
            .con
            .execute_batch(
                "CREATE VIEW loans AS SELECT CAST(g AS DOUBLE) AS Amount,
                        CAST(NULL AS DOUBLE) AS Empty FROM range(0, 101) t(g);",
            )
            .unwrap();
        let p = runner.percentiles("loans", "Amount").unwrap().unwrap();
        assert_eq!((p.len(), p[0], p[10], p[100]), (101, 0.0, 10.0, 100.0));
        assert_eq!(runner.percentiles("loans", "Empty").unwrap(), None);
    }

    #[test]
    fn an_amount_far_from_its_groups_median_is_an_outlier() {
        let runner = Runner::new().unwrap();
        // Synthetic incomes: office O-1 around 1000, one of 1 000 000 and one
        // of 1; office O-2 alike, none far; a NULL office; a zero skipped.
        runner
            .con
            .execute_batch(
                "CREATE VIEW loans AS
                 SELECT 'O-1' AS Office, 1000.0 + g AS Income FROM range(0, 20) t(g)
                 UNION ALL SELECT 'O-1', 1000000.0 UNION ALL SELECT 'O-1', 1.0
                 UNION ALL SELECT 'O-2', 2000.0 + g FROM range(0, 5) t(g)
                 UNION ALL SELECT NULL, 500.0 UNION ALL SELECT 'O-2', 0.0;",
            )
            .unwrap();
        let groups = runner.outliers("loans", "Income", &["Office"]).unwrap();
        let seen: Vec<(Option<&str>, i64, i64, i64)> = groups
            .iter()
            .map(|g| (g.group.as_deref(), g.records, g.low, g.high))
            .collect();
        assert_eq!(
            seen,
            [
                (Some("O-1"), 22, 1, 1),
                (Some("O-2"), 5, 0, 0),
                (None, 1, 0, 0)
            ]
        );
        assert!(runner.outliers("loans", "Ghost", &["Office"]).is_err());
    }

    /// The canonical percentiles in Rust, unfused: sorted with `NaN`
    /// last, as SQL sorts it.
    fn reference(xs: &[f64]) -> Vec<f64> {
        let mut s: Vec<f64> = xs.iter().copied().filter(|x| !x.is_nan()).collect();
        s.sort_by(f64::total_cmp);
        s.extend(xs.iter().filter(|x| x.is_nan()));
        (0..=100)
            .map(|k| {
                let h = (s.len() as f64 - 1.0) * (f64::from(k) / 100.0);
                let (i, j) = (h.floor(), h.ceil());
                let (lo, hi) = (s[i as usize], s[j as usize]);
                if i == j {
                    lo
                } else {
                    lo * (1.0 - (h - i)) + hi * (h - i)
                }
            })
            .collect()
    }

    /// A small deterministic generator: synthetic values only.
    fn vector(seed: u64, n: usize, kind: u64) -> Vec<f64> {
        let mut x = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
        (0..n)
            .map(|i| {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                let u = (x >> 11) as f64 / (1u64 << 53) as f64;
                match (kind, i % 7) {
                    (1, _) => (u * 4.0).floor(),
                    (2, 0) => f64::NAN,
                    (3, 0) => f64::INFINITY,
                    (3, 1) => f64::NEG_INFINITY,
                    _ => (u - 0.5) * 1e6,
                }
            })
            .collect()
    }

    fn same(a: f64, b: f64) -> bool {
        a.to_bits() == b.to_bits() || (a.is_nan() && b.is_nan())
    }

    #[test]
    fn the_window_percentiles_are_the_canonical_ones_bit_for_bit() {
        let runner = Runner::new().unwrap();
        let sizes = [1, 2, 3, 4, 5, 10, 101, 1000];
        for (seed, (&n, kind)) in sizes
            .iter()
            .flat_map(|n| (0..4).map(move |k| (n, k)))
            .enumerate()
        {
            let xs = vector(seed as u64 + 1, n, kind);
            runner
                .con
                .execute_batch("CREATE OR REPLACE TABLE loans (Amount DOUBLE)")
                .unwrap();
            {
                let mut app = runner.con.appender("loans").unwrap();
                for x in &xs {
                    app.append_row(duckdb::params![*x]).unwrap();
                }
            }
            let seen = runner.percentiles("loans", "Amount").unwrap().unwrap();
            let want = reference(&xs);
            let off: Vec<usize> = (0..=100).filter(|&k| !same(seen[k], want[k])).collect();
            assert!(off.is_empty(), "n {n}, kind {kind}: off at {off:?}");
        }
    }
}
