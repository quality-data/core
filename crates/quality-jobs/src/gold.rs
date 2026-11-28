//! The jobs that write gold without reading the lake (decision 0090,
//! point 4): a retract of a cut or a dataset, and a dataset's SLAs. Every
//! write is idempotent by `run_id` (ruling 25): an earlier attempt's rows
//! go first, and every `DELETE` waits on a count that showed rows.

use quality::bundle::Bundle;
use quality_rules_trino as trino;
use quality_rules_trino::{Cut, Run, Sla, DETAILS, REMEDIATION_SLAS, RUN_SUMMARY};

use crate::steps::Steps;
use crate::{CompileError, Job};

/// A retract: of the job's cut, or of the whole dataset when it has none.
#[derive(Debug, Clone, Copy, Default)]
pub struct RetractInput;

/// A `slas` job: the dataset's SLAs as approved now.
#[derive(Debug, Clone, Default)]
pub struct SlasInput {
    pub slas: Vec<Sla>,
}

/// Gold's tables: `run_summary` first, so an earlier attempt's flip goes
/// before its rows, and last among the older runs' deletes.
fn tables() -> impl Iterator<Item = &'static str> {
    std::iter::once(RUN_SUMMARY.name).chain(DETAILS.iter().map(|t| t.name))
}

/// A retract's bundle. Of a cut: the run's earlier attempt removed, its
/// tombstone (the flip to no rows), then the older runs' rows. Of a
/// dataset: every row of it in every table, its SLAs included.
pub fn retract(job: &Job<'_>, _input: &RetractInput) -> Result<Bundle, CompileError> {
    let dataset = job.dataset()?;
    let mut steps = Steps::default();
    let Some(cut) = job.cut else {
        for t in tables().chain([REMEDIATION_SLAS.name]) {
            steps.count_then_delete(
                &format!("dataset_{t}"),
                trino::dataset_rows(t, dataset)?,
                trino::delete_dataset(t, dataset)?,
            );
        }
        return Ok(job.bundle(steps));
    };
    let cut = Cut::parse(cut).map_err(trino::Error::from)?;
    let run = Run {
        dataset,
        cut: &cut,
        run_id: job.run,
        run_seq: job.run_seq()?,
    };
    for t in tables() {
        steps.count_then_delete(
            &format!("run_{t}"),
            trino::run_rows(t, &run)?,
            trino::delete_run(t, &run)?,
        );
    }
    steps.insert("tombstone".into(), trino::tombstone(&run, job.run)?);
    for t in tables().skip(1).chain([RUN_SUMMARY.name]) {
        steps.count_then_delete(
            &format!("older_{t}"),
            trino::older_rows(t, &run)?,
            trino::delete_older(t, &run)?,
        );
    }
    Ok(job.bundle(steps))
}

/// A `slas` job's bundle: the run's earlier attempt removed, the SLAs
/// inserted (none when the dataset has none), then the older runs'.
pub fn slas(job: &Job<'_>, input: &SlasInput) -> Result<Bundle, CompileError> {
    let dataset = job.dataset()?;
    let mut steps = Steps::default();
    steps.count_then_delete(
        "run_slas",
        trino::run_slas(dataset, job.run)?,
        trino::delete_run_slas(dataset, job.run)?,
    );
    if let Some(insert) = trino::insert_slas(dataset, job.run, &input.slas)? {
        steps.insert("slas".into(), insert);
    }
    steps.count_then_delete(
        "older_slas",
        trino::older_slas(dataset, job.run)?,
        trino::delete_older_slas(dataset, job.run)?,
    );
    Ok(job.bundle(steps))
}

#[cfg(test)]
mod tests {
    use super::*;
    use quality::bundle::{Bundle, Expect, RunAs};

    fn job(cut: Option<&'static str>) -> Job<'static> {
        Job {
            run: "r-7",
            kind: "retract",
            dataset: Some("loans"),
            cut,
            attempt: 2,
            run_seq: Some(11),
            catalog_commit: None,
        }
    }

    fn ids(b: &Bundle) -> Vec<&str> {
        b.statements.iter().map(|s| s.id.as_str()).collect()
    }

    #[test]
    fn a_cut_retract_removes_the_attempt_flips_to_nothing_then_drops_the_older() {
        let b = retract(&job(Some("2026-10-31")), &RetractInput).unwrap();
        assert!(b.well_formed());
        let ids = ids(&b);
        let n = 1 + DETAILS.len();
        assert_eq!(ids[..2], ["run_run_summary_rows", "run_run_summary"]);
        assert_eq!(ids[2 * n], "tombstone");
        assert_eq!(ids[2 * n + 1], "older_rule_results_rows");
        assert_eq!(ids[ids.len() - 1], "older_run_summary");
        assert_eq!(ids.len(), 4 * n + 1);
        assert_eq!(b.run_id.as_deref(), Some("r-7"));
        assert_eq!(b.run_seq, Some(11));
        for s in &b.statements {
            let delete = s.sql.starts_with("DELETE");
            assert_eq!(delete, s.only_if_rows.is_some(), "{}", s.id);
            assert_eq!(delete, s.expects == Expect::delete(), "{}", s.id);
            let count = s.id.ends_with("_rows");
            assert_eq!(count, s.run_as == RunAs::Report, "{}", s.id);
            // Every write holds its gold table's mutex (ruling 92(b)).
            assert_eq!(count, s.lock.is_none(), "{}", s.id);
            if let Some(lock) = &s.lock {
                assert!(lock.starts_with("lake.quality_gold."), "{lock}");
            }
        }
        let older = &b.statements[2 * n + 2];
        assert!(older.sql.contains("run_id <> 'r-7'"), "{}", older.sql);
        assert!(
            older.sql.contains("cut = DATE '2026-10-31'"),
            "{}",
            older.sql
        );
    }

    #[test]
    fn a_cut_that_is_no_day_is_refused() {
        for bad in ["2026-02-30", "2026-10-31' OR '1'='1"] {
            let refused = retract(&job(Some(bad)), &RetractInput);
            assert!(
                matches!(
                    refused,
                    Err(CompileError::Statement(quality_rules_trino::Error::Sql(_)))
                ),
                "{bad}: {refused:?}"
            );
        }
    }

    #[test]
    fn a_dataset_retract_deletes_every_table_and_needs_no_run_seq() {
        let mut j = job(None);
        j.run_seq = None;
        let b = retract(&j, &RetractInput).unwrap();
        assert!(b.well_formed());
        assert_eq!(b.statements.len(), 2 * (2 + DETAILS.len()));
        assert_eq!(ids(&b).last(), Some(&"dataset_remediation_slas"));
        assert_eq!(
            retract(&job(Some("2026-10-31")).clone_without_seq(), &RetractInput),
            Err(CompileError::Incomplete("run_seq"))
        );
    }

    #[test]
    fn slas_replace_the_runs_then_drop_the_older() {
        let mut j = job(None);
        j.kind = "slas";
        let input = SlasInput {
            slas: vec![Sla {
                dimension: "validity".into(),
                group: None,
                days: 30,
            }],
        };
        let b = slas(&j, &input).unwrap();
        assert!(b.well_formed());
        assert_eq!(
            ids(&b),
            [
                "run_slas_rows",
                "run_slas",
                "slas",
                "older_slas_rows",
                "older_slas"
            ]
        );
        let none = slas(&j, &SlasInput::default()).unwrap();
        assert_eq!(none.statements.len(), 4, "no SLA: only the deletes");
        let mut no_dataset = j;
        no_dataset.dataset = None;
        assert_eq!(
            slas(&no_dataset, &input),
            Err(CompileError::Incomplete("dataset"))
        );
    }

    impl Job<'_> {
        fn clone_without_seq(&self) -> Self {
            Self {
                run_seq: None,
                ..self.clone()
            }
        }
    }
}
