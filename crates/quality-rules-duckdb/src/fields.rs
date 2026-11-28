//! Each field's profile in an evaluated cut (decision 0031's
//! `field_quality`): empties as decision 0033 says, distinct values, and
//! the shape the values most often take, each digit a `9` and each letter
//! an `A`, cut at forty characters. A shape and counts, never a value.
//! The shape is taken from the text the field's type says, as on Trino
//! (decision 0090): a floating-point number has none.

use quality::worker_api::FieldQuality;
use quality_rules_sql::ShapeKind;

use crate::{quoted, Error, Runner};

/// The longest shape kept.
const SHAPE: usize = 40;

impl Runner {
    /// The profile of each of `fields` in the loaded `dataset`.
    pub fn field_quality(
        &self,
        dataset: &str,
        fields: &[&str],
    ) -> Result<Vec<FieldQuality>, Error> {
        let from = quoted(dataset)?;
        let types = self.column_types(dataset)?;
        fields
            .iter()
            .map(|field| {
                let c = quoted(field)?;
                let kind = types
                    .get(*field)
                    .map_or(ShapeKind::Text, |t| ShapeKind::of_type(t));
                let t = kind
                    .text(&c)
                    .unwrap_or_else(|| "CAST(NULL AS VARCHAR)".into());
                let sql = format!(
                    "WITH e AS (SELECT CAST({c} AS VARCHAR) AS v, {t} AS t FROM {from}),
                     f AS (SELECT v, t FROM e
                            WHERE NOT (v IS NULL OR regexp_full_match(v, '[\\s\\x{{00A0}}]*'))),
                     s AS (SELECT substr(regexp_replace(regexp_replace(t, '[0-9]', '9', 'g'),
                                                        '[A-Za-z]', 'A', 'g'), 1, {SHAPE}) AS p
                             FROM f WHERE t IS NOT NULL),
                     p AS (SELECT p, count(*) AS n FROM s GROUP BY p
                            ORDER BY n DESC NULLS LAST, p ASC NULLS LAST LIMIT 1)
                     SELECT (SELECT count(*) FROM e) - (SELECT count(*) FROM f),
                            (SELECT count(DISTINCT v) FROM f),
                            (SELECT p FROM p),
                            COALESCE((SELECT n FROM p), 0)"
                );
                self.con.query_row(&sql, [], |r| {
                    Ok(FieldQuality {
                        field: field.to_string(),
                        empty: r.get(0)?,
                        distinct: r.get(1)?,
                        pattern: r.get(2)?,
                        pattern_count: r.get(3)?,
                        floating: kind == ShapeKind::Float,
                        icd: None,
                        recency: None,
                    })
                })
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_fields_profile_is_its_counts_and_commonest_shape() {
        let runner = Runner::new().unwrap();
        // Synthetic rows: codes of two shapes, blanks (U+00A0 too) and NULLs.
        runner
            .con
            .execute_batch(
                "CREATE VIEW loans AS SELECT
                   CASE WHEN g % 10 = 0 THEN NULL WHEN g % 10 = 1 THEN chr(160)
                        WHEN g % 10 = 2 THEN 'AB-' || g ELSE 'L' || (1000 + g) END AS Code,
                   CAST(NULL AS VARCHAR) AS Note,
                   CAST(g AS DOUBLE) / 4 AS Amount,
                   CAST(g / 1000 AS DECIMAL(4, 4)) AS Rate
                 FROM range(0, 100) t(g);",
            )
            .unwrap();
        let profile = runner
            .field_quality("loans", &["Code", "Note", "Amount", "Rate"])
            .unwrap();
        assert_eq!(
            profile[0],
            FieldQuality {
                field: "Code".into(),
                empty: 20,
                distinct: 80,
                pattern: Some("A9999".into()),
                pattern_count: 70,
                floating: false,
                icd: None,
                recency: None,
            }
        );
        assert_eq!(
            (
                profile[1].empty,
                profile[1].distinct,
                profile[1].pattern.as_deref(),
                profile[1].pattern_count
            ),
            (100, 0, None, 0)
        );
        // A double has no shape; a decimal without integer digits takes
        // its leading zero, as on Trino.
        assert_eq!(
            (profile[2].distinct, profile[2].pattern.as_deref()),
            (100, None)
        );
        assert_eq!((profile[2].pattern_count, profile[2].floating), (0, true));
        assert_eq!(profile[3].pattern.as_deref(), Some("9.9999"));
        assert!(runner.field_quality("loans", &["Ghost"]).is_err());
    }
}
