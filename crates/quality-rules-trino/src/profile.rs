//! A field's profile in a cut (decision 0031's `field_quality`), its update
//! recency and the shape of its values: counts and shapes, never a value.
//! The same definitions as DuckDB's (decision 0090, step 4), in Trino's
//! spelling: `regexp_replace` replaces every match, `regexp_like` anchors
//! by `^…$`.

use crate::{quoted, Error, Statement};

/// The longest shape kept.
pub const SHAPE_LEN: usize = 40;

/// The bands' upper bounds in days, after the band of dates past the cut;
/// a last band takes the older ones.
pub const RECENCY_DAYS: [i64; 5] = [30, 90, 180, 365, 730];

/// One row over `from`: the field's empties (decision 0033: `NULL`, blank
/// or only spaces), its distinct values, its commonest shape (each digit
/// `9`, each ASCII letter `A`, cut at [`SHAPE_LEN`]) and how many have it.
pub fn field_profile(from: &str, field: &str) -> Result<Statement, Error> {
    let v = format!("CAST({} AS VARCHAR)", quoted(field)?);
    Ok(Statement {
        sql: format!(
            "WITH e AS (SELECT {v} AS v FROM {from}),
                  f AS (SELECT v FROM e
                         WHERE NOT (v IS NULL OR regexp_like(v, '^[\\s\\x{{00A0}}]*$'))),
                  s AS (SELECT substr(regexp_replace(regexp_replace(v, '[0-9]', '9'),
                                                     '[A-Za-z]', 'A'), 1, {SHAPE_LEN}) AS p
                          FROM f),
                  p AS (SELECT p, count(*) AS n FROM s GROUP BY p
                         ORDER BY n DESC NULLS LAST, p ASC NULLS LAST LIMIT 1)
             SELECT (SELECT count(*) FROM e) - (SELECT count(*) FROM f),
                    (SELECT count(DISTINCT v) FROM f),
                    (SELECT p FROM p),
                    COALESCE((SELECT n FROM p), 0)"
        ),
        cut_params: 0,
    })
}

/// One row over `from`: the records by the age of `field` at the cut,
/// dated after it, then up to each of [`RECENCY_DAYS`], then older.
pub fn recency(from: &str, field: &str) -> Result<Statement, Error> {
    let age = format!(
        "date_diff('day', CAST({} AS DATE), CAST(? AS DATE))",
        quoted(field)?
    );
    let mut bands = vec![format!("count(*) FILTER (WHERE {age} < 0)")];
    let mut lo = 0;
    for hi in RECENCY_DAYS {
        bands.push(format!(
            "count(*) FILTER (WHERE {age} BETWEEN {lo} AND {hi})"
        ));
        lo = hi + 1;
    }
    bands.push(format!("count(*) FILTER (WHERE {age} >= {lo})"));
    Ok(Statement {
        cut_params: bands.len(),
        sql: format!("SELECT {} FROM {from}", bands.join(", ")),
    })
}

/// The shape of `field`'s value (decision 0043, point 3): each digit `9`
/// and each letter `A`, the rest kept, cut at [`SHAPE_LEN`]; `∅` when empty.
pub fn shape_of(field: &str) -> Result<String, Error> {
    let v = format!("CAST({} AS VARCHAR)", quoted(field)?);
    Ok(format!(
        "coalesce(substr(regexp_replace(regexp_replace({v}, '\\p{{N}}', '9'), '\\p{{L}}', 'A'), 1, {SHAPE_LEN}), '∅')"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_profile_uses_trinos_spelling_and_portable_forms() {
        let s = field_profile("t", "Code").unwrap();
        assert_eq!(s.cut_params, 0);
        assert!(s.sql.contains("regexp_like(v, '^[\\s\\x{00A0}]*$')"));
        assert!(s.sql.contains("regexp_replace(v, '[0-9]', '9')"));
        for word in ["regexp_full_match", "'g'", "left(", "::", "[[:"] {
            assert!(!s.sql.contains(word), "{word}");
        }
        assert!(shape_of("Code").unwrap().starts_with(
            "coalesce(substr(regexp_replace(regexp_replace(CAST(\"Code\" AS VARCHAR), '\\p{N}', '9')"
        ));
    }

    #[test]
    fn recency_binds_the_cut_once_per_band() {
        let s = recency("t", "Updated").unwrap();
        assert_eq!(s.cut_params, 7);
        assert!(s.sql.contains("BETWEEN 31 AND 90"));
        assert!(s.sql.ends_with(">= 731) FROM t"));
        assert!(field_profile("t", "a\u{0}b").is_err());
    }
}
