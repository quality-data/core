//! The findings of a cut and its remediation queues (decision 0059) over
//! the job's scratch rows: each record failing a rule, known by its keyed
//! hash (hashed by the scratch insert), with the guide's priority (formula
//! 5) and its group in each dimension; the cut it opened at, carried from
//! the previous cut's findings; and each queue's open and resolved
//! findings. One `CROSS JOIN UNNEST` over the rules' outcomes, no branch
//! per rule. The SFC closeness is computed in the engine from the same
//! rows, so the bundle needs no result of an earlier statement.

use quality::mapping::{FieldRole, Mapping};
use quality_rules_sql::{double, quote_text};

use crate::cells::Cells;
use crate::icd::{measured, with_role, IcdRule, SCRATCH};
use crate::{Error, Statement};

/// The most queues kept per dimension, those with the most open findings.
const MOST_QUEUES: usize = 1000;

/// A cut's findings and their groups, as relations with the columns
/// `(record, rule, since)` and `(record, dimension, grp)`.
#[derive(Debug, Clone, Copy)]
pub struct Findings<'a> {
    pub findings: &'a str,
    pub groups: &'a str,
}

/// Closeness to the SFC threshold of a share (the guide's formula 5).
fn prox(share: &str) -> String {
    format!(
        "CASE WHEN {share} >= DOUBLE '0.92' THEN DOUBLE '0.2' WHEN {share} >= DOUBLE '0.9' \
         THEN DOUBLE '0.5' WHEN {share} >= DOUBLE '0.8' THEN DOUBLE '1' \
         WHEN {share} >= DOUBLE '0.7' THEN DOUBLE '0.6' ELSE DOUBLE '0.3' END"
    )
}

fn job(job_id: &str) -> Result<String, Error> {
    Ok(format!("job_id = {}", quote_text(job_id)?))
}

/// Each failing `(record, rule, priority, since)`: the priority the
/// greatest term of the measured fields the rule reads (0 with none), each
/// term its weight over 4, what its ICD lacks and its cell's closeness
/// (0.2, the lowest, with no cell or no active record).
pub fn findings(
    job_id: &str,
    mapping: &Mapping,
    rules: &[IcdRule],
    previous: Option<Findings<'_>>,
) -> Result<Statement, Error> {
    let fields = measured(mapping, rules);
    let cells = Cells::of(mapping, rules)?;
    let mut counts = vec!["count(*) FILTER (WHERE act) AS total".to_string()];
    let mut close = Vec::new();
    for (n, f) in fields.iter().enumerate() {
        let p = match cells.names.iter().position(|c| *c == f.name) {
            Some(j) => {
                counts.push(format!(
                    "count(*) FILTER (WHERE act AND element_at(c, {})) AS ok{j}",
                    j + 1
                ));
                let share = format!("CAST(ok{j} AS DOUBLE) / CAST(total AS DOUBLE)");
                format!(
                    "CASE WHEN total = 0 THEN DOUBLE '0.2' ELSE {} END",
                    prox(&share)
                )
            }
            None => "DOUBLE '0.2'".to_string(),
        };
        close.push(format!("{p} AS p{n}"));
    }
    close.push("1 AS unused".into());
    let term = |n: usize, weight: u8| {
        format!(
            "COALESCE({} * (DOUBLE '1' - element_at(u.f, {})) * p.p{n}, DOUBLE '0')",
            double(f64::from(weight) / 4.0),
            n + 1
        )
    };
    let mut cases = Vec::new();
    let mut ids = Vec::new();
    for (i, rule) in rules.iter().enumerate() {
        let own: Vec<String> = fields
            .iter()
            .enumerate()
            .filter(|(_, f)| rule.reads.contains(&f.name))
            .map(|(n, f)| term(n, f.weight))
            .collect();
        let priority = match own.len() {
            0 => "DOUBLE '0'".to_string(),
            1 => own[0].clone(),
            _ => format!("greatest({})", own.join(", ")),
        };
        cases.push(format!("WHEN {} THEN {priority}", i + 1));
        ids.push(quote_text(&rule.id)?);
    }
    if rules.is_empty() {
        cases.push("WHEN 0 THEN DOUBLE '0'".into());
        ids.push("''".into());
    }
    let (join, since) = match previous {
        Some(p) => (
            format!(
                "LEFT JOIN {} q ON q.record = f.record AND q.rule = f.rule",
                p.findings
            ),
            "COALESCE(q.since, CAST(? AS DATE))",
        ),
        None => (String::new(), "CAST(? AS DATE)"),
    };
    let rows = job(job_id)?;
    Ok(Statement {
        sql: format!(
            "WITH t AS (SELECT {counts} FROM {SCRATCH} WHERE {rows}),
                  p AS (SELECT {close} FROM t),
                  u AS (SELECT s.r_key AS record, o.v AS failed, o.i AS i, s.f
                          FROM {SCRATCH} s CROSS JOIN UNNEST(s.r) WITH ORDINALITY AS o(v, i)
                         WHERE s.{rows} AND s.r_key IS NOT NULL),
                  f AS (SELECT u.record, element_at(ARRAY[{ids}], u.i) AS rule,
                               max(CASE u.i {cases} END) AS priority
                          FROM u CROSS JOIN p WHERE u.failed GROUP BY u.record, u.i)
             SELECT f.record, f.rule, f.priority, {since} AS since FROM f {join}",
            counts = counts.join(", "),
            close = close.join(", "),
            ids = ids.join(", "),
            cases = cases.join(" "),
        ),
        cut_params: 1,
    })
}

/// Each finding's record's group in each dimension, over `current` (the
/// cut's findings as just written).
pub fn finding_groups(job_id: &str, mapping: &Mapping, current: &str) -> Result<Statement, Error> {
    let mut each = vec![
        "SELECT CAST(NULL AS VARCHAR) AS record, CAST(NULL AS VARCHAR) AS dimension, \
         CAST(NULL AS VARCHAR) AS grp WHERE FALSE"
            .to_string(),
    ];
    for (j, name) in with_role(mapping, FieldRole::Dimension).iter().enumerate() {
        each.push(format!(
            "SELECT r_key, {}, min(element_at(g, {})) FROM {SCRATCH}
              WHERE {} AND r_key IN (SELECT record FROM {current}) GROUP BY r_key",
            quote_text(name)?,
            j + 1,
            job(job_id)?
        ));
    }
    Ok(Statement {
        sql: each.join(" UNION ALL "),
        cut_params: 0,
    })
}

/// Each queue's open findings by the cut they opened at, and those of the
/// previous cut it no longer has, by the group they had; the queues with
/// the most open findings of each dimension.
pub fn queues(
    mapping: &Mapping,
    current: Findings<'_>,
    previous: Option<Findings<'_>>,
) -> Result<Statement, Error> {
    let names: Vec<String> = with_role(mapping, FieldRole::Dimension)
        .iter()
        .map(|d| quote_text(d))
        .collect::<Result<_, _>>()?;
    let names = if names.is_empty() {
        "CAST(NULL AS VARCHAR)".to_string()
    } else {
        names.join(", ")
    };
    let gone = match previous {
        Some(p) => format!(
            "UNION ALL
             SELECT g.dimension, g.grp, p.since, 0, count(*), DOUBLE '0'
               FROM {} p JOIN {} g ON g.record = p.record
              WHERE g.dimension IN ({names}) AND NOT EXISTS
                    (SELECT 1 FROM {} f WHERE f.record = p.record AND f.rule = p.rule)
              GROUP BY g.dimension, g.grp, p.since",
            p.findings, p.groups, current.findings
        ),
        None => String::new(),
    };
    Ok(Statement {
        sql: format!(
            "WITH u (dimension, grp, since, open, resolved, priority) AS (
                 SELECT g.dimension, g.grp, f.since, count(*), 0, sum(f.priority)
                   FROM {cf} f JOIN {cg} g ON g.record = f.record
                  WHERE g.dimension IN ({names})
                  GROUP BY g.dimension, g.grp, f.since
                 {gone}),
                  q AS (SELECT dimension, grp, since, sum(open) AS open,
                               sum(resolved) AS resolved, sum(priority) AS priority
                          FROM u GROUP BY dimension, grp, since),
                  t AS (SELECT *, sum(open) OVER (PARTITION BY dimension, grp) AS total FROM q),
                  r AS (SELECT *, dense_rank() OVER (PARTITION BY dimension
                                   ORDER BY total DESC NULLS LAST, grp ASC NULLS LAST) AS n
                          FROM t)
             SELECT dimension, grp, since, CAST(open AS BIGINT), CAST(resolved AS BIGINT),
                    CAST(priority AS DOUBLE)
               FROM r WHERE n <= {MOST_QUEUES}
              ORDER BY dimension ASC NULLS LAST, n ASC NULLS LAST, since ASC NULLS LAST",
            cf = current.findings,
            cg = current.groups,
        ),
        cut_params: 0,
    })
}
