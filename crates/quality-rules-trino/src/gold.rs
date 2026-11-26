//! Gold, the system of record of the results (decision 0090, points 3 and
//! 4): its tables in `quality_gold`, made once at the module's install by
//! its own user (jobs never create a table), the views people read, the
//! scratch table and the findings key's table. Every detail table carries
//! `dataset`, `cut` and `run_id` and is partitioned by them; `run_summary`
//! by `dataset`, with `run_seq`.

use crate::Statement;

/// A gold table: its name and its own columns, after `dataset`, `cut` and
/// `run_id`.
#[derive(Debug, Clone, Copy)]
pub struct GoldTable {
    pub name: &'static str,
    pub columns: &'static [(&'static str, &'static str)],
}

const BIGINT: &str = "BIGINT";
const DOUBLE: &str = "DOUBLE";
const TEXT: &str = "VARCHAR";

/// `run_summary`: the flip of point 4 is one row of it.
pub const RUN_SUMMARY: GoldTable = GoldTable {
    name: "run_summary",
    columns: &[
        ("run_seq", BIGINT),
        ("job_id", TEXT),
        ("records", BIGINT),
        ("duration_ms", BIGINT),
        ("rules_commit", TEXT),
        ("reported_at", "TIMESTAMP(6) WITH TIME ZONE"),
        ("igcd", DOUBLE),
        ("entities", BIGINT),
        ("band_high", BIGINT),
        ("band_medium", BIGINT),
        ("band_low", BIGINT),
    ],
};

/// The detail tables, as the PostgreSQL index stores them (decision 0089,
/// point 2.1), plus `findings` and `finding_groups` (decision 0059) and
/// `entity_icd`; not `job_stats`, whose stage timings are metrics (decision
/// 0090, ruling 70).
pub const DETAILS: &[GoldTable] = &[
    GoldTable {
        name: "rule_results",
        columns: &[
            ("rule", TEXT),
            ("applies", BIGINT),
            ("failures", BIGINT),
            ("field", TEXT),
            ("pattern", TEXT),
            ("dimension", TEXT),
            ("severity", TEXT),
        ],
    },
    GoldTable {
        name: "field_quality",
        columns: &[
            ("field", TEXT),
            ("empty", BIGINT),
            ("distinct_values", BIGINT),
            ("pattern", TEXT),
            ("pattern_count", BIGINT),
            ("floating", "BOOLEAN"),
            ("mean_icd", DOUBLE),
            ("recency", "ARRAY(BIGINT)"),
        ],
    },
    GoldTable {
        name: "group_metrics",
        columns: &[
            ("dimension", TEXT),
            ("grp", TEXT),
            ("records", BIGINT),
            ("serious", BIGINT),
            ("mean_icd", DOUBLE),
        ],
    },
    GoldTable {
        name: "distribution_checks",
        columns: &[
            ("variable", TEXT),
            ("dimension", TEXT),
            ("grp", TEXT),
            ("percentiles", "ARRAY(DOUBLE)"),
            ("records", BIGINT),
            ("low", BIGINT),
            ("high", BIGINT),
        ],
    },
    GoldTable {
        name: "cells",
        columns: &[
            ("cell", TEXT),
            ("ok", BIGINT),
            ("total", BIGINT),
            ("passes", "BOOLEAN"),
            ("missing", BIGINT),
            ("margin", BIGINT),
        ],
    },
    GoldTable {
        name: "queue_findings",
        columns: &[
            ("dimension", TEXT),
            ("grp", TEXT),
            ("since", "DATE"),
            ("open", BIGINT),
            ("resolved", BIGINT),
            ("priority", DOUBLE),
        ],
    },
    GoldTable {
        name: "findings",
        columns: &[
            ("record", TEXT),
            ("rule", TEXT),
            ("priority", DOUBLE),
            ("since", "DATE"),
        ],
    },
    GoldTable {
        name: "finding_groups",
        columns: &[("record", TEXT), ("dimension", TEXT), ("grp", TEXT)],
    },
    // One row per entity (ruled by the user 2026-11-25, closing decision
    // 0089's Open 2): its keyed hash, never its key, so the bands of two
    // cuts can be matched (`band_transitions`).
    GoldTable {
        name: "entity_icd",
        columns: &[
            ("entity", TEXT),
            ("icd", DOUBLE),
            ("band", TEXT),
            ("segment", TEXT),
        ],
    },
];

/// `remediation_slas`: a dataset's SLAs by queue, no cut (ruled by the
/// user 2026-11-25, an exception to point 3.2); a `slas` job replaces them.
pub const REMEDIATION_SLAS: GoldTable = GoldTable {
    name: "remediation_slas",
    columns: &[
        ("dimension", TEXT),
        ("grp", TEXT),
        ("days", "INTEGER"),
        ("updated_at", "TIMESTAMP(6) WITH TIME ZONE"),
    ],
};

/// The schema gold's tables live in, in the lake catalog.
pub const GOLD: &str = "\"lake\".\"quality_gold\"";

fn create(t: &GoldTable, partitioning: &str) -> String {
    let mut cols = vec![
        "dataset VARCHAR".to_string(),
        "cut DATE".to_string(),
        "run_id VARCHAR".to_string(),
    ];
    cols.extend(t.columns.iter().map(|(n, ty)| format!("\"{n}\" {ty}")));
    format!(
        "CREATE TABLE IF NOT EXISTS {GOLD}.\"{}\" ({}) WITH (partitioning = ARRAY[{partitioning}])",
        t.name,
        cols.join(", ")
    )
}

fn plain(sql: String) -> Statement {
    Statement { sql }
}

/// The entities only aggregated (decision 0090, ruling 73): people read
/// these, never `entity_icd`'s rows, so the views run as their owner
/// (`SECURITY DEFINER`); people's row filter by dataset applies to them.
fn entity_views() -> Vec<Statement> {
    let e = "\"gold\".\"quality_gold\".\"v_entity_icd\"";
    [
        (
            "v_icd_distribution",
            format!(
                "SELECT dataset, cut, round(least(floor(icd * 20), 19) / 20, 2) AS from_icd, \
                 count(*) AS entities FROM {e} WHERE icd IS NOT NULL GROUP BY 1, 2, 3"
            ),
        ),
        (
            "v_bands_by_segment",
            format!(
                "SELECT dataset, cut, segment, count(*) FILTER (WHERE band = 'high') AS high, \
                 count(*) FILTER (WHERE band = 'medium') AS medium, \
                 count(*) FILTER (WHERE band = 'low') AS low, count(*) AS entities \
                 FROM {e} WHERE band IS NOT NULL GROUP BY 1, 2, 3"
            ),
        ),
        (
            "v_band_transitions",
            format!(
                "WITH c AS (SELECT dataset, cut, lag(cut) OVER (PARTITION BY dataset ORDER BY cut) \
                 AS previous FROM \"gold\".\"quality_gold\".\"current_run\") \
                 SELECT c.dataset, c.cut, p.band AS from_band, n.band AS to_band, \
                 count(*) AS entities FROM c JOIN {e} n ON n.dataset = c.dataset AND n.cut = c.cut \
                 JOIN {e} p ON p.dataset = c.dataset AND p.cut = c.previous AND p.entity = n.entity \
                 WHERE n.band IS NOT NULL AND p.band IS NOT NULL GROUP BY 1, 2, 3, 4"
            ),
        ),
    ]
    .into_iter()
    .map(|(name, body)| {
        plain(format!(
            "CREATE OR REPLACE VIEW {GOLD}.\"{name}\" SECURITY DEFINER AS {body}"
        ))
    })
    .collect()
}

/// The install's statements, in order: the schemas, gold's tables, the
/// views people read, the jobs' counts (ruling 53), the scratch table and
/// the findings key's table.
/// Each can run twice. The views join with `ON`, never `USING` with `t.*`
/// (T2: `t.*` after `USING` drops the join columns, and a row filter on
/// `dataset` then fails).
pub fn install() -> Vec<Statement> {
    let mut out = vec![
        plain(format!("CREATE SCHEMA IF NOT EXISTS {GOLD}")),
        plain("CREATE SCHEMA IF NOT EXISTS \"lake\".\"quality_scratch\"".into()),
        plain("CREATE SCHEMA IF NOT EXISTS \"lake\".\"quality_secrets\"".into()),
        plain(create(&RUN_SUMMARY, "'dataset'")),
    ];
    out.extend(
        DETAILS
            .iter()
            .map(|t| plain(create(t, "'dataset', 'cut', 'run_id'"))),
    );
    let mut slas = vec!["dataset VARCHAR".to_string(), "run_id VARCHAR".to_string()];
    slas.extend(
        REMEDIATION_SLAS
            .columns
            .iter()
            .map(|(n, ty)| format!("\"{n}\" {ty}")),
    );
    out.push(plain(format!(
        "CREATE TABLE IF NOT EXISTS {GOLD}.\"remediation_slas\" ({}) \
         WITH (partitioning = ARRAY['dataset', 'run_id'])",
        slas.join(", ")
    )));
    out.push(plain(format!(
        "CREATE OR REPLACE VIEW {GOLD}.\"current_run\" SECURITY INVOKER AS \
         SELECT dataset, cut, max_by(run_id, run_seq) AS run_id \
         FROM \"gold\".\"quality_gold\".\"run_summary\" GROUP BY dataset, cut"
    )));
    for t in std::iter::once(&RUN_SUMMARY).chain(DETAILS) {
        out.push(plain(format!(
            "CREATE OR REPLACE VIEW {GOLD}.\"v_{n}\" SECURITY INVOKER AS \
             SELECT t.* FROM \"gold\".\"quality_gold\".\"{n}\" t \
             JOIN \"gold\".\"quality_gold\".\"current_run\" r \
             ON t.dataset = r.dataset AND t.cut = r.cut AND t.run_id = r.run_id",
            n = t.name
        )));
    }
    out.extend(entity_views());
    out.push(crate::job_counts::create());
    out.push(plain(format!(
        "CREATE TABLE IF NOT EXISTS {} (job_id VARCHAR, dataset VARCHAR, k VARCHAR, \
         r_key VARCHAR, s VARCHAR, a DOUBLE, serious BOOLEAN, act BOOLEAN, icd DOUBLE, \
         r ARRAY(BOOLEAN), f ARRAY(DOUBLE), g ARRAY(VARCHAR), c ARRAY(BOOLEAN)) \
         WITH (partitioning = ARRAY['job_id'])",
        crate::SCRATCH
    )));
    out.push(plain(format!(
        "CREATE TABLE IF NOT EXISTS {} (\"key\" VARBINARY)",
        crate::FINDINGS_KEY
    )));
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gold_is_partitioned_as_point_3_says_and_read_through_views() {
        let all = install();
        let sql: Vec<&str> = all.iter().map(|s| s.sql.as_str()).collect();
        let summary = sql
            .iter()
            .find(|s| s.contains("\"run_summary\" ("))
            .unwrap();
        assert!(summary.contains("\"run_seq\" BIGINT"));
        assert!(summary.ends_with("WITH (partitioning = ARRAY['dataset'])"));
        let rules = sql
            .iter()
            .find(|s| s.contains("\"rule_results\" ("))
            .unwrap();
        assert!(rules.starts_with(
            "CREATE TABLE IF NOT EXISTS \"lake\".\"quality_gold\".\"rule_results\" \
             (dataset VARCHAR, cut DATE, run_id VARCHAR, \"rule\" VARCHAR"
        ));
        assert!(rules.ends_with("ARRAY['dataset', 'cut', 'run_id'])"));
        let entities = sql.iter().find(|s| s.contains("\"entity_icd\" (")).unwrap();
        assert!(entities.contains("\"entity\" VARCHAR, \"icd\" DOUBLE, \"band\" VARCHAR"));
        assert!(entities.ends_with("ARRAY['dataset', 'cut', 'run_id'])"));
        assert!(sql.iter().any(|s| s.contains("v_entity_icd")));
        assert!(sql
            .iter()
            .any(|s| s.contains("\"job_counts\" (dataset VARCHAR, run VARCHAR")));
        let views: Vec<&&str> = sql.iter().filter(|s| s.contains("VIEW")).collect();
        assert_eq!(views.len(), 2 + DETAILS.len() + 3);
        let (definer, views): (Vec<&&str>, Vec<&&str>) = views
            .into_iter()
            .partition(|v| v.contains("SECURITY DEFINER"));
        assert_eq!(definer.len(), 3, "the entities' aggregates only");
        assert!(definer.iter().all(|v| v.contains("GROUP BY")));
        for v in views {
            assert!(v.contains("SECURITY INVOKER"));
            assert!(!v.contains("USING"), "{v}");
        }
        assert!(sql
            .iter()
            .any(|s| s.contains("WITH (partitioning = ARRAY['job_id'])")));
        assert!(sql
            .iter()
            .all(|s| !s.contains("DROP") && !s.contains("MERGE")));
    }
}
