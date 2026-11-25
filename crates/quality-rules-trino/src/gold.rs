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
/// point 2.1), plus `findings` and `finding_groups` (decision 0059).
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
        name: "job_stats",
        columns: &[("stage", TEXT), ("position", "INTEGER"), ("ms", BIGINT)],
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
];

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
    Statement { sql, cut_params: 0 }
}

/// The install's statements, in order: the schemas, gold's tables, the
/// views people read, the scratch table and the findings key's table.
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
        let views: Vec<&&str> = sql.iter().filter(|s| s.contains("VIEW")).collect();
        assert_eq!(views.len(), 2 + DETAILS.len());
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
