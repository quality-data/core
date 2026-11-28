//! What the evaluation reads back from its scratch rows (decision 0090,
//! point 8.4), `WHERE job_id = ?` as a literal of the signed bundle: the
//! ICD's means, each entity's ICD with its commonest segment, each
//! dimension group and each SFC cell; and the delete of the job's rows.
//! Arrays are 1-based: `f[n + 1]` is the `n`th measured field.

use quality::mapping::{FieldRole, Mapping};
use quality_rules_sql::quote_text;

use crate::icd::{weighted, with_role, Measured, SCRATCH};
use crate::{Error, Statement};

/// A critical field's weight: its entity takes its worst record.
const CRITICAL: u8 = 4;
/// The most groups kept per dimension, the largest.
const MOST_GROUPS: usize = 1000;

fn job(job_id: &str) -> Result<String, Error> {
    Ok(format!("{SCRATCH} WHERE job_id = {}", quote_text(job_id)?))
}

fn plain(sql: String) -> Statement {
    Statement { sql }
}

/// One row: the mean record ICD, then each measured field's mean.
pub fn means(job_id: &str, fields: &[Measured]) -> Result<Statement, Error> {
    let mut cols = vec!["avg(icd)".to_string()];
    cols.extend((1..=fields.len()).map(|n| format!("avg(element_at(f, {n}))")));
    Ok(plain(format!(
        "SELECT {} FROM {}",
        cols.join(", "),
        job(job_id)?
    )))
}

/// Each entity's hashed key, ICD and segment, by key: per field the mean
/// of its records (the worst for a critical field), weighted by priority;
/// the segment its commonest, the first by name on a tie.
pub fn entities(job_id: &str, fields: &[Measured]) -> Result<Statement, Error> {
    let per_field: Vec<String> = fields
        .iter()
        .enumerate()
        .map(|(i, f)| {
            let agg = if f.weight == CRITICAL { "min" } else { "avg" };
            format!("{agg}(element_at(f, {})) AS \"e{i}\"", i + 1)
        })
        .collect();
    let icd: Vec<(f64, String)> = fields
        .iter()
        .enumerate()
        .map(|(i, f)| (f64::from(f.weight), format!("\"e{i}\"")))
        .collect();
    let rows = job(job_id)?;
    let mut e = vec!["k".to_string()];
    e.extend(per_field);
    Ok(plain(format!(
        "WITH e AS (SELECT {e} FROM {rows} AND k IS NOT NULL GROUP BY k),
              c AS (SELECT k, s, count(*) AS n FROM {rows} AND k IS NOT NULL AND s IS NOT NULL
                     GROUP BY k, s),
              m AS (SELECT k, s, row_number() OVER (PARTITION BY k
                            ORDER BY n DESC NULLS LAST, s ASC NULLS LAST) AS i FROM c)
         SELECT e.k, {icd}, m.s FROM e LEFT JOIN m ON m.k = e.k AND m.i = 1
          ORDER BY e.k ASC NULLS LAST",
        e = e.join(", "),
        icd = weighted(&icd),
    )))
}

/// Per dimension in the mapping's order, its largest groups: the group,
/// its records, those with a serious failure and their mean ICD.
pub fn groups(job_id: &str, mapping: &Mapping) -> Result<Vec<Statement>, Error> {
    let rows = job(job_id)?;
    (1..=with_role(mapping, FieldRole::Dimension).len())
        .map(|j| {
            Ok(plain(format!(
                "SELECT element_at(g, {j}) AS grp, count(*) AS n,
                        count(*) FILTER (WHERE serious), avg(icd)
                   FROM {rows} GROUP BY element_at(g, {j})
                  ORDER BY n DESC NULLS LAST, grp ASC NULLS LAST LIMIT {MOST_GROUPS}"
            )))
        })
        .collect()
}

/// One row: the active records, then each cell's right ones among them.
pub fn cells(job_id: &str, cells: usize) -> Result<Statement, Error> {
    let mut cols = vec!["count(*) FILTER (WHERE act)".to_string()];
    cols.extend((1..=cells).map(|j| format!("count(*) FILTER (WHERE act AND element_at(c, {j}))")));
    Ok(plain(format!(
        "SELECT {} FROM {}",
        cols.join(", "),
        job(job_id)?
    )))
}

/// How many scratch rows the job has: the delete is issued only when some
/// are (RustFS refuses an empty delete).
pub fn scratch_rows(job_id: &str) -> Result<Statement, Error> {
    Ok(plain(format!("SELECT count(*) FROM {}", job(job_id)?)))
}

/// The job's last statement: its scratch partition goes.
pub fn scratch_delete(job_id: &str) -> Result<Statement, Error> {
    Ok(plain(format!("DELETE FROM {}", job(job_id)?)))
}
