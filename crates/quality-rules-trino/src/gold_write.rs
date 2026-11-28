//! Writing gold (decision 0090, point 4): insert the run's rows, flip by
//! one `run_summary` row with its `run_seq`, then delete the older runs'
//! rows, each only after a count shows some (RustFS refuses an empty
//! delete). No `MERGE`, no `UPDATE`: a row filter refuses them. A retry
//! keeps the run's pinned bundle and its `run_id` (ruled by the user
//! 2026-11-25), so every write is idempotent by `run_id`: before its
//! inserts the job deletes what an earlier attempt of the run wrote, by the
//! same count-then-delete, and a retry after a committed insert never
//! doubles the run's rows.

use quality_rules_sql::{quote_text, Cut};

use crate::gold::{GoldTable, GOLD, RUN_SUMMARY};
use crate::{Error, Statement};

/// Which run of which cut a job writes: `run_seq` from the core's
/// sequence, taken when the bundle was compiled.
#[derive(Debug, Clone, Copy)]
pub struct Run<'a> {
    pub dataset: &'a str,
    pub cut: &'a Cut,
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
        "{}, {}, {}",
        quote_text(run.dataset)?,
        run.cut.literal(),
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
    })
}

fn rows_of(table: &str, run: &Run<'_>, op: &str) -> Result<String, Error> {
    Ok(format!(
        "{GOLD}.\"{table}\" WHERE dataset = {} AND cut = {} AND run_id {op} {}",
        quote_text(run.dataset)?,
        run.cut.literal(),
        quote_text(run.run_id)?
    ))
}

fn older(table: &str, run: &Run<'_>) -> Result<String, Error> {
    rows_of(table, run, "<>")
}

/// Step 0's count: what an earlier attempt of this run left in `table`
/// (`run_summary` included, a flip that committed).
pub fn run_rows(table: &str, run: &Run<'_>) -> Result<Statement, Error> {
    Ok(Statement {
        sql: format!("SELECT count(*) FROM {}", rows_of(table, run, "=")?),
    })
}

/// Step 0: an earlier attempt's rows of this run go, before step 1
/// inserts them again. Issued only after [`run_rows`] counted some.
pub fn delete_run(table: &str, run: &Run<'_>) -> Result<Statement, Error> {
    Ok(Statement {
        sql: format!("DELETE FROM {}", rows_of(table, run, "=")?),
    })
}

/// Step 3's count: the older runs' rows of `table`.
pub fn older_rows(table: &str, run: &Run<'_>) -> Result<Statement, Error> {
    Ok(Statement {
        sql: format!("SELECT count(*) FROM {}", older(table, run)?),
    })
}

/// Step 3: the older runs' rows go, whole partitions for a detail table.
/// Issued only after [`older_rows`] counted some.
pub fn delete_older(table: &str, run: &Run<'_>) -> Result<Statement, Error> {
    Ok(Statement {
        sql: format!("DELETE FROM {}", older(table, run)?),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gold::DETAILS;

    #[test]
    fn a_run_inserts_flips_then_deletes_the_older() {
        let cut = Cut::parse("2026-10-31").unwrap();
        let run = Run {
            dataset: "loans",
            cut: &cut,
            run_id: "r-2",
            run_seq: 7,
        };
        let body = Statement {
            sql: "SELECT 1".into(),
        };
        let i = insert(&DETAILS[0], &run, &body).unwrap();
        assert_eq!(
            i.sql,
            "INSERT INTO \"lake\".\"quality_gold\".\"rule_results\" (dataset, cut, run_id, \
             \"rule\", \"applies\", \"failures\", \"field\", \"pattern\", \"dimension\", \"severity\") \
             SELECT 'loans', DATE '2026-10-31', 'r-2', b.* FROM (SELECT 1) b"
        );
        let f = flip(&run, &body).unwrap();
        assert!(f
            .sql
            .contains("SELECT 'loans', DATE '2026-10-31', 'r-2', 7, b.*"));
        let at = "\"lake\".\"quality_gold\".\"findings\" WHERE dataset = 'loans' \
                  AND cut = DATE '2026-10-31' AND run_id <> 'r-2'";
        assert_eq!(
            older_rows("findings", &run).unwrap().sql,
            format!("SELECT count(*) FROM {at}")
        );
        assert_eq!(
            delete_older("findings", &run).unwrap().sql,
            format!("DELETE FROM {at}")
        );
        let this = "\"lake\".\"quality_gold\".\"findings\" WHERE dataset = 'loans' \
                    AND cut = DATE '2026-10-31' AND run_id = 'r-2'";
        assert_eq!(
            run_rows("findings", &run).unwrap().sql,
            format!("SELECT count(*) FROM {this}")
        );
        assert_eq!(
            delete_run("findings", &run).unwrap().sql,
            format!("DELETE FROM {this}")
        );
        for s in [i.sql, f.sql] {
            assert!(!s.contains("MERGE") && !s.contains("UPDATE"));
        }
    }
}
