//! Taking results out of gold (decision 0090, point 4) and replacing a
//! dataset's SLAs. A retract of a cut writes a tombstone `run_summary` row
//! (no detail rows), so the views show nothing for the cut, and the older
//! runs' rows then go; a retract of a dataset deletes its rows in every
//! table. Every delete is issued only after its count shows rows.

use quality_rules_sql::quote_text;

use crate::gold::{GOLD, REMEDIATION_SLAS, RUN_SUMMARY};
use crate::gold_write::Run;
use crate::{Error, Statement};

fn plain(sql: String) -> Statement {
    Statement { sql }
}

/// The tombstone: the run's `run_summary` row with no detail rows; every
/// column after `run_seq` is `NULL` but the job and the time.
pub fn tombstone(run: &Run<'_>, job_id: &str) -> Result<Statement, Error> {
    let cols: Vec<String> = RUN_SUMMARY
        .columns
        .iter()
        .map(|(n, _)| format!("\"{n}\""))
        .collect();
    let values: Vec<String> = RUN_SUMMARY
        .columns
        .iter()
        .map(|(n, ty)| match *n {
            "run_seq" => Ok(run.run_seq.to_string()),
            "job_id" => quote_text(job_id),
            "reported_at" => Ok("current_timestamp".to_string()),
            _ => Ok(format!("CAST(NULL AS {ty})")),
        })
        .collect::<Result<_, _>>()?;
    Ok(plain(format!(
        "INSERT INTO {GOLD}.\"run_summary\" (dataset, cut, run_id, {}) VALUES ({}, {}, {}, {})",
        cols.join(", "),
        quote_text(run.dataset)?,
        run.cut.literal(),
        quote_text(run.run_id)?,
        values.join(", ")
    )))
}

fn of_dataset(table: &str, dataset: &str) -> Result<String, Error> {
    Ok(format!(
        "{GOLD}.\"{table}\" WHERE dataset = {}",
        quote_text(dataset)?
    ))
}

/// A dataset's rows of `table`, counted before [`delete_dataset`].
pub fn dataset_rows(table: &str, dataset: &str) -> Result<Statement, Error> {
    Ok(plain(format!(
        "SELECT count(*) FROM {}",
        of_dataset(table, dataset)?
    )))
}

/// A retract of a dataset, per table: every row of it goes.
pub fn delete_dataset(table: &str, dataset: &str) -> Result<Statement, Error> {
    Ok(plain(format!(
        "DELETE FROM {}",
        of_dataset(table, dataset)?
    )))
}

/// One SLA of a queue.
#[derive(Debug, Clone)]
pub struct Sla {
    pub dimension: String,
    pub group: Option<String>,
    pub days: u32,
}

/// A `slas` job's insert of the dataset's SLAs as run `run_id`; the older
/// runs' rows go after it ([`older_slas`], [`delete_older_slas`]).
pub fn insert_slas(dataset: &str, run_id: &str, slas: &[Sla]) -> Result<Option<Statement>, Error> {
    if slas.is_empty() {
        return Ok(None);
    }
    let rows: Vec<String> = slas
        .iter()
        .map(|s| {
            let grp = match &s.group {
                Some(g) => quote_text(g)?,
                None => "CAST(NULL AS VARCHAR)".to_string(),
            };
            Ok(format!(
                "({}, {}, {}, {grp}, {}, current_timestamp)",
                quote_text(dataset)?,
                quote_text(run_id)?,
                quote_text(&s.dimension)?,
                s.days
            ))
        })
        .collect::<Result<_, Error>>()?;
    let cols: Vec<String> = REMEDIATION_SLAS
        .columns
        .iter()
        .map(|(n, _)| format!("\"{n}\""))
        .collect();
    Ok(Some(plain(format!(
        "INSERT INTO {GOLD}.\"remediation_slas\" (dataset, run_id, {}) VALUES {}",
        cols.join(", "),
        rows.join(", ")
    ))))
}

fn slas_of(dataset: &str, run_id: &str, op: &str) -> Result<String, Error> {
    Ok(format!(
        "{} AND run_id {op} {}",
        of_dataset("remediation_slas", dataset)?,
        quote_text(run_id)?
    ))
}

fn older(dataset: &str, run_id: &str) -> Result<String, Error> {
    slas_of(dataset, run_id, "<>")
}

/// What an earlier attempt of run `run_id` wrote, counted before its
/// delete: a retry keeps its `run_id` (ruled by the user 2026-11-25).
pub fn run_slas(dataset: &str, run_id: &str) -> Result<Statement, Error> {
    Ok(plain(format!(
        "SELECT count(*) FROM {}",
        slas_of(dataset, run_id, "=")?
    )))
}

/// An earlier attempt's SLAs of run `run_id` go, before the insert.
pub fn delete_run_slas(dataset: &str, run_id: &str) -> Result<Statement, Error> {
    Ok(plain(format!(
        "DELETE FROM {}",
        slas_of(dataset, run_id, "=")?
    )))
}

/// The older runs' SLAs of the dataset, counted before their delete.
pub fn older_slas(dataset: &str, run_id: &str) -> Result<Statement, Error> {
    Ok(plain(format!(
        "SELECT count(*) FROM {}",
        older(dataset, run_id)?
    )))
}

/// The older runs' SLAs go, whole partitions.
pub fn delete_older_slas(dataset: &str, run_id: &str) -> Result<Statement, Error> {
    Ok(plain(format!("DELETE FROM {}", older(dataset, run_id)?)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_retracted_cut_is_a_tombstone_then_its_older_rows_go() {
        let cut = quality_rules_sql::Cut::parse("2026-10-31").unwrap();
        let run = Run {
            dataset: "loans",
            cut: &cut,
            run_id: "r-3",
            run_seq: 9,
        };
        let t = tombstone(&run, "job-9").unwrap().sql;
        assert!(t.starts_with(
            "INSERT INTO \"lake\".\"quality_gold\".\"run_summary\" (dataset, cut, run_id, \"run_seq\""
        ));
        assert!(t.contains(
            "VALUES ('loans', DATE '2026-10-31', 'r-3', 9, 'job-9', CAST(NULL AS BIGINT)"
        ));
        assert!(t.contains("current_timestamp"));
        assert_eq!(
            delete_dataset("findings", "loans").unwrap().sql,
            "DELETE FROM \"lake\".\"quality_gold\".\"findings\" WHERE dataset = 'loans'"
        );
        assert!(dataset_rows("findings", "loans")
            .unwrap()
            .sql
            .starts_with("SELECT count(*)"));
    }

    #[test]
    fn a_datasets_slas_are_replaced_by_run() {
        assert!(insert_slas("loans", "r-1", &[]).unwrap().is_none());
        let slas = [
            Sla {
                dimension: "Office".into(),
                group: Some("O-1".into()),
                days: 5,
            },
            Sla {
                dimension: "Office".into(),
                group: None,
                days: 10,
            },
        ];
        let s = insert_slas("loans", "r-1", &slas).unwrap().unwrap().sql;
        assert!(s.contains("('loans', 'r-1', 'Office', 'O-1', 5, current_timestamp)"));
        assert!(s.contains("CAST(NULL AS VARCHAR), 10,"));
        assert!(!s.contains("cut"));
        assert!(delete_run_slas("loans", "r-1")
            .unwrap()
            .sql
            .ends_with("AND run_id = 'r-1'"));
        assert!(run_slas("loans", "r-1")
            .unwrap()
            .sql
            .starts_with("SELECT count(*) FROM"));
        assert!(delete_older_slas("loans", "r-1")
            .unwrap()
            .sql
            .ends_with("AND run_id <> 'r-1'"));
        assert!(older_slas("loans", "r-1")
            .unwrap()
            .sql
            .starts_with("SELECT count(*)"));
    }
}
