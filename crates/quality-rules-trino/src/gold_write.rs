//! Writing gold (decision 0090, point 4): insert the run's rows, flip by
//! one `run_summary` row with its `run_seq`, then delete the older runs'
//! rows, each only after a count shows some (RustFS refuses an empty
//! delete). No `MERGE`, no `UPDATE`: a row filter refuses them. Every retry
//! is a new run: step 1 never runs twice with the same `run_id`.

use quality_rules_sql::quote_text;

use crate::gold::{GoldTable, GOLD, RUN_SUMMARY};
use crate::{Error, Statement};

/// Which run of which cut a job writes: `run_seq` from the core's
/// sequence, taken when the bundle was compiled.
#[derive(Debug, Clone, Copy)]
pub struct Run<'a> {
    pub dataset: &'a str,
    pub cut: &'a str,
    pub run_id: &'a str,
    pub run_seq: i64,
}

fn names(t: &GoldTable) -> String {
    t.columns
        .iter()
        .map(|(n, _)| format!("\"{n}\""))
        .collect::<Vec<_>>()
        .join(", ")
}

fn keys(run: &Run<'_>) -> Result<String, Error> {
    Ok(format!(
        "{}, DATE {}, {}",
        quote_text(run.dataset)?,
        quote_text(run.cut)?,
        quote_text(run.run_id)?
    ))
}

/// Step 1: the run's rows of `table`, `body` a query giving its own
/// columns in order.
pub fn insert(table: &GoldTable, run: &Run<'_>, body: &Statement) -> Result<Statement, Error> {
    Ok(Statement {
        sql: format!(
            "INSERT INTO {GOLD}.\"{t}\" (dataset, cut, run_id, {cols}) SELECT {k}, b.* FROM ({body}) b",
            t = table.name,
            cols = names(table),
            k = keys(run)?,
            body = body.sql,
        ),
        cut_params: body.cut_params,
    })
}

/// Step 2, the flip: the run's `run_summary` row, `body` a query giving
/// its columns after `run_seq` in order. This one commit switches readers.
pub fn flip(run: &Run<'_>, body: &Statement) -> Result<Statement, Error> {
    Ok(Statement {
        sql: format!(
            "INSERT INTO {GOLD}.\"run_summary\" (dataset, cut, run_id, {cols}) \
             SELECT {k}, {seq}, b.* FROM ({body}) b",
            cols = names(&RUN_SUMMARY),
            k = keys(run)?,
            seq = run.run_seq,
            body = body.sql,
        ),
        cut_params: body.cut_params,
    })
}

fn older(table: &str, run: &Run<'_>) -> Result<String, Error> {
    Ok(format!(
        "{GOLD}.\"{table}\" WHERE dataset = {} AND cut = DATE {} AND run_id <> {}",
        quote_text(run.dataset)?,
        quote_text(run.cut)?,
        quote_text(run.run_id)?
    ))
}

/// Step 3's count: the older runs' rows of `table`.
pub fn older_rows(table: &str, run: &Run<'_>) -> Result<Statement, Error> {
    Ok(Statement {
        sql: format!("SELECT count(*) FROM {}", older(table, run)?),
        cut_params: 0,
    })
}

/// Step 3: the older runs' rows go, whole partitions for a detail table.
/// Issued only after [`older_rows`] counted some.
pub fn delete_older(table: &str, run: &Run<'_>) -> Result<Statement, Error> {
    Ok(Statement {
        sql: format!("DELETE FROM {}", older(table, run)?),
        cut_params: 0,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gold::DETAILS;

    const RUN: Run<'static> = Run {
        dataset: "loans",
        cut: "2026-10-31",
        run_id: "r-2",
        run_seq: 7,
    };

    #[test]
    fn a_run_inserts_flips_then_deletes_the_older() {
        let body = Statement {
            sql: "SELECT 1".into(),
            cut_params: 1,
        };
        let i = insert(&DETAILS[0], &RUN, &body).unwrap();
        assert_eq!(
            i.sql,
            "INSERT INTO \"lake\".\"quality_gold\".\"rule_results\" (dataset, cut, run_id, \
             \"rule\", \"applies\", \"failures\", \"field\", \"pattern\", \"dimension\", \"severity\") \
             SELECT 'loans', DATE '2026-10-31', 'r-2', b.* FROM (SELECT 1) b"
        );
        assert_eq!(i.cut_params, 1);
        let f = flip(&RUN, &body).unwrap();
        assert!(f
            .sql
            .contains("SELECT 'loans', DATE '2026-10-31', 'r-2', 7, b.*"));
        let at = "\"lake\".\"quality_gold\".\"findings\" WHERE dataset = 'loans' \
                  AND cut = DATE '2026-10-31' AND run_id <> 'r-2'";
        assert_eq!(
            older_rows("findings", &RUN).unwrap().sql,
            format!("SELECT count(*) FROM {at}")
        );
        assert_eq!(
            delete_older("findings", &RUN).unwrap().sql,
            format!("DELETE FROM {at}")
        );
        for s in [i.sql, f.sql] {
            assert!(!s.contains("MERGE") && !s.contains("UPDATE"));
        }
    }
}
