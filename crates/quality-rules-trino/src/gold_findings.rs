//! The findings' relations over gold (decision 0090, ruling 40): the
//! run's own `findings` and `finding_groups`, as its earlier inserts wrote
//! them, and the previous cut's at that cut's current run. `findings`,
//! `finding_groups` and `queues` read them, and their rows go into gold by
//! `INSERT … SELECT`, never read back. The job reads its own dataset's
//! rows only (its row filter).

use quality_rules_sql::quote_text;

use crate::findings::Findings;
use crate::gold::GOLD;
use crate::gold_write::Run;
use crate::Error;

/// A cut's findings and their groups in gold, as relations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GoldFindings {
    pub findings: String,
    pub groups: String,
}

impl GoldFindings {
    /// The relations as `findings`, `finding_groups` and `queues` take them.
    pub fn relations(&self) -> Findings<'_> {
        Findings {
            findings: &self.findings,
            groups: &self.groups,
        }
    }
}

fn of(run: &str, dataset: &str, cut: &str) -> Result<GoldFindings, Error> {
    let at = format!(
        "WHERE dataset = {} AND cut = DATE {} AND run_id = {run}",
        quote_text(dataset)?,
        quote_text(cut)?
    );
    Ok(GoldFindings {
        findings: format!("(SELECT record, rule, priority, since FROM {GOLD}.\"findings\" {at})"),
        groups: format!("(SELECT record, dimension, grp FROM {GOLD}.\"finding_groups\" {at})"),
    })
}

/// The run's own findings and groups, written before the statements that
/// read them.
pub fn run_findings(run: &Run<'_>) -> Result<GoldFindings, Error> {
    of(&quote_text(run.run_id)?, run.dataset, run.cut)
}

/// The findings of `dataset`'s `previous` cut (the core's index gives it
/// at compile, decision 0090 A1) at that cut's current run: the one with
/// the highest `run_seq` in `run_summary`.
pub fn previous_findings(dataset: &str, previous: &str) -> Result<GoldFindings, Error> {
    let current = format!(
        "(SELECT max_by(run_id, run_seq) FROM {GOLD}.\"run_summary\" \
         WHERE dataset = {} AND cut = DATE {})",
        quote_text(dataset)?,
        quote_text(previous)?
    );
    of(&current, dataset, previous)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_runs_rows_and_the_previous_cuts_current_run() {
        let run = Run {
            dataset: "loans",
            cut: "2026-10-31",
            run_id: "r2",
            run_seq: 7,
        };
        let now = run_findings(&run).unwrap();
        assert_eq!(
            now.findings,
            format!(
                "(SELECT record, rule, priority, since FROM {GOLD}.\"findings\" \
                 WHERE dataset = 'loans' AND cut = DATE '2026-10-31' AND run_id = 'r2')"
            )
        );
        assert!(now
            .groups
            .contains("\"finding_groups\" WHERE dataset = 'loans'"));
        let before = previous_findings("loans", "2026-09-30").unwrap();
        assert!(before.findings.ends_with(&format!(
            "AND run_id = (SELECT max_by(run_id, run_seq) FROM {GOLD}.\"run_summary\" \
             WHERE dataset = 'loans' AND cut = DATE '2026-09-30'))"
        )));
        assert!(previous_findings("lo'ans", "x\u{0}").is_err());
    }
}
