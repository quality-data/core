//! The flip's body on Trino (tier A; `lake` is a memory catalog in CI):
//! the run's `run_summary` row from its `entity_icd` rows and silver.

mod trino;

use quality_rules_trino::{flip, install, started_ms, summary, Run, Summary, GOLD, SCRATCH};
use serde_json::json;
use trino::{rows, url};

#[tokio::test]
async fn the_flip_counts_the_runs_entities_and_bands() {
    let Some(base) = url() else { return };
    rows(&base, &format!("CREATE SCHEMA IF NOT EXISTS {GOLD}")).await;
    rows(&base, "CREATE SCHEMA IF NOT EXISTS lake.quality_scratch").await;
    for t in ["\"run_summary\" (", "\"entity_icd\" (", SCRATCH] {
        let ddl = install()
            .into_iter()
            .find(|s| s.sql.contains(t))
            .unwrap()
            .sql;
        let ddl = ddl.split(" WITH (partitioning").next().unwrap().to_string();
        rows(&base, &ddl).await;
    }
    rows(
        &base,
        &format!(
            "INSERT INTO {GOLD}.\"entity_icd\" (dataset, cut, run_id, entity, icd, band) VALUES \
             ('flip', DATE '2026-10-31', 'r1', 'h1', 0.95e0, 'high'), \
             ('flip', DATE '2026-10-31', 'r1', 'h2', 0.8e0, 'medium'), \
             ('flip', DATE '2026-10-31', 'r0', 'h3', 0.1e0, 'low')"
        ),
    )
    .await;
    let start = rows(&base, &started_ms().sql).await[0][0].to_string();
    let run = Run {
        dataset: "flip",
        cut: "2026-10-31",
        run_id: "r1",
        run_seq: 4,
    };
    let s = Summary {
        job_id: "j-flip",
        rules_commit: "abc",
        silver: "(VALUES 1, 2, 3) t(x)",
        started_ms: Some(&start),
    };
    let body = summary(&run, &s).unwrap();
    rows(&base, &flip(&run, &body).unwrap().sql).await;
    let got = rows(
        &base,
        &format!(
            "SELECT run_seq, job_id, records, duration_ms >= 0, rules_commit, \
             round(igcd, 6), entities, band_high, band_medium, band_low \
             FROM {GOLD}.\"run_summary\" WHERE dataset = 'flip'"
        ),
    )
    .await;
    assert_eq!(
        serde_json::Value::from(got),
        json!([[4, "j-flip", 3, true, "abc", 0.875, 2, 1, 1, 0]])
    );
}
