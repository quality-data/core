//! Ruling 40's findings relations over gold, on Trino (tier A; `lake` is
//! a memory catalog in CI): the queues read the run's own findings and the
//! previous cut's at its current run only.

mod trino;

use quality::mapping::Mapping;
use quality_rules_trino::{install, previous_findings, queues, run_findings, Run, GOLD};
use serde_json::{json, Value};
use trino::{rows, url};

/// Gold's tables these relations read, without the partitioning the
/// memory connector lacks, and synthetic rows of two runs of the
/// previous cut and one of this cut.
async fn gold(base: &str) {
    rows(base, &format!("CREATE SCHEMA IF NOT EXISTS {GOLD}")).await;
    for t in [
        "\"run_summary\" (",
        "\"findings\" (",
        "\"finding_groups\" (",
    ] {
        let ddl = install()
            .into_iter()
            .find(|s| s.sql.contains(t))
            .unwrap()
            .sql;
        let ddl = ddl.split(" WITH (partitioning").next().unwrap().to_string();
        rows(base, &ddl).await;
    }
    for sql in [
        "INSERT INTO {G}.\"run_summary\" (dataset, cut, run_id, run_seq) VALUES \
         ('loans', DATE '2026-09-30', 'r0', 1), ('loans', DATE '2026-09-30', 'r1', 2)",
        "INSERT INTO {G}.\"findings\" (dataset, cut, run_id, record, rule, priority, since) VALUES \
         ('loans', DATE '2026-09-30', 'r0', 'h1', 'R1', 1e0, DATE '2026-08-31'), \
         ('loans', DATE '2026-09-30', 'r1', 'h1', 'R1', 1e0, DATE '2026-08-31'), \
         ('loans', DATE '2026-09-30', 'r1', 'h2', 'R1', 1e0, DATE '2026-09-30'), \
         ('loans', DATE '2026-10-31', 'r2', 'h1', 'R1', 5e-1, DATE '2026-08-31')",
        "INSERT INTO {G}.\"finding_groups\" (dataset, cut, run_id, record, dimension, grp) VALUES \
         ('loans', DATE '2026-09-30', 'r0', 'h1', 'Type', 'PJ'), \
         ('loans', DATE '2026-09-30', 'r1', 'h1', 'Type', 'PN'), \
         ('loans', DATE '2026-09-30', 'r1', 'h2', 'Type', 'PN'), \
         ('loans', DATE '2026-10-31', 'r2', 'h1', 'Type', 'PN')",
    ] {
        rows(base, &sql.replace("{G}", GOLD)).await;
    }
}

#[tokio::test]
async fn the_queues_read_this_run_and_the_previous_cuts_current_run() {
    let Some(base) = url() else { return };
    gold(&base).await;
    let mapping = Mapping::parse(
        "dataset: loans\nsource: loans\nfields:\n  \
         Type: { column: kind, type: text, roles: [dimension] }\n",
    )
    .unwrap();
    let run = Run {
        dataset: "loans",
        cut: "2026-10-31",
        run_id: "r2",
        run_seq: 3,
    };
    let now = run_findings(&run).unwrap();
    let before = previous_findings("loans", "2026-09-30").unwrap();
    let q = queues(&mapping, now.relations(), Some(before.relations())).unwrap();
    let got = Value::from(rows(&base, &q.sql.replace('\n', " ")).await);
    assert_eq!(
        got,
        json!([
            ["Type", "PN", "2026-08-31", 1, 0, 0.5],
            ["Type", "PN", "2026-09-30", 0, 1, 0.0]
        ])
    );
}
