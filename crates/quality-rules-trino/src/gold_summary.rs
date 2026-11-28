//! The body of the flip (decision 0090, point 4 and ruling 40): the run's
//! `run_summary` row after `run_seq`, computed in the engine from what the
//! run wrote, and inserted by `flip` as the run's last gold write.

use quality_rules_sql::quote_text;

use crate::gold::GOLD;
use crate::gold_write::Run;
use crate::icd::SCRATCH;
use crate::{Error, Statement};

/// What the summary row takes besides the run's own gold rows.
#[derive(Debug, Clone, Copy)]
pub struct Summary<'a> {
    pub job_id: &'a str,
    /// The rules' commit the bundle was compiled from.
    pub rules_commit: &'a str,
    /// The cut's pinned silver: its records.
    pub silver: &'a str,
    /// The run's start in epoch milliseconds, an SQL expression (a filled
    /// `bigint` placeholder of [`started_ms`]'s answer); none leaves the
    /// duration empty.
    pub started_ms: Option<&'a str>,
}

const NOW_MS: &str = "CAST(to_unixtime(current_timestamp) * 1000 AS BIGINT)";

/// One row, one `bigint`: the engine's clock in epoch milliseconds, read
/// first so the summary can give the run's duration.
pub fn started_ms() -> Statement {
    Statement {
        sql: format!("SELECT {NOW_MS} AS started_ms"),
    }
}

/// `run_summary`'s columns after `run_seq`: the job, the cut's records,
/// the duration, the rules' commit, when it was reported, the IGCD (the
/// mean entity ICD, or the mean record ICD without entities, decision
/// 0057), and the entities with their bands.
pub fn summary(run: &Run<'_>, s: &Summary<'_>) -> Result<Statement, Error> {
    let entities = format!(
        "{GOLD}.\"entity_icd\" WHERE dataset = {} AND cut = {} AND run_id = {}",
        quote_text(run.dataset)?,
        run.cut.literal(),
        quote_text(run.run_id)?
    );
    let duration = match s.started_ms {
        Some(start) => format!("{NOW_MS} - ({start})"),
        None => "CAST(NULL AS BIGINT)".into(),
    };
    Ok(Statement {
        sql: format!(
            "SELECT {job}, (SELECT count(*) FROM {silver}), {duration}, {commit}, \
             CAST(current_timestamp AS TIMESTAMP(6) WITH TIME ZONE), \
             CASE WHEN e.n > 0 THEN e.mean \
             ELSE (SELECT avg(icd) FROM {SCRATCH} WHERE job_id = {job}) END, \
             e.n, e.high, e.medium, e.low \
             FROM (SELECT count(*) AS n, avg(icd) AS mean, \
             count(*) FILTER (WHERE band = 'high') AS high, \
             count(*) FILTER (WHERE band = 'medium') AS medium, \
             count(*) FILTER (WHERE band = 'low') AS low FROM {entities}) e",
            job = quote_text(s.job_id)?,
            silver = s.silver,
            commit = quote_text(s.rules_commit)?,
        ),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gold_write::flip;
    use quality_rules_sql::Cut;

    #[test]
    fn the_flip_takes_the_runs_entities_and_its_duration() {
        let cut = Cut::parse("2026-10-31").unwrap();
        let run = Run {
            dataset: "loans",
            cut: &cut,
            run_id: "r1",
            run_seq: 9,
        };
        let s = Summary {
            job_id: "j1",
            rules_commit: "abc",
            silver: "s",
            started_ms: Some("1764000000000"),
        };
        let body = summary(&run, &s).unwrap();
        assert!(body.sql.starts_with(
            "SELECT 'j1', (SELECT count(*) FROM s), \
             CAST(to_unixtime(current_timestamp) * 1000 AS BIGINT) - (1764000000000), 'abc'"
        ));
        assert!(body.sql.contains(&format!(
            "FROM {GOLD}.\"entity_icd\" WHERE dataset = 'loans' AND cut = DATE '2026-10-31' \
             AND run_id = 'r1') e"
        )));
        assert!(body.sql.contains(&format!(
            "ELSE (SELECT avg(icd) FROM {SCRATCH} WHERE job_id = 'j1') END"
        )));
        let f = flip(&run, &body).unwrap();
        assert!(f
            .sql
            .contains("SELECT 'loans', DATE '2026-10-31', 'r1', 9, b.* FROM (SELECT 'j1'"));
        let none = summary(
            &run,
            &Summary {
                started_ms: None,
                ..s
            },
        )
        .unwrap();
        assert!(none
            .sql
            .contains("(SELECT count(*) FROM s), CAST(NULL AS BIGINT), 'abc'"));
        assert_eq!(started_ms().sql, format!("SELECT {NOW_MS} AS started_ms"));
    }
}
