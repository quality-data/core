//! Ruling 40's gold rows read from the scratch rows, on Trino (tier A):
//! CI's Trino has a `memory` catalog named `lake`, so the scratch table
//! exists without a lake; `group_metrics`, `cells` and `entity_icd` read
//! rows written here by hand. A field's
//! groups and cells are in the mapping's (sorted) order: `Office` before
//! `Type`, `Mail` before `Phone`.

mod trino;

use quality::mapping::Mapping;
use quality_rules_compile::{compile, FieldType, Schema};
use quality_rules_sql::{Cut, Tables};
use quality_rules_trino::{cell_rows, entity_icd, group_metrics, measured, IcdRule, SCRATCH};
use serde_json::{json, Value};
use trino::{rows, url};

const MAPPING: &str = "dataset: loans
source: loans
fields:
  Doc: { column: doc, type: text, roles: [entity] }
  Type: { column: kind, type: text, roles: [dimension] }
  Office: { column: office, type: text, roles: [dimension] }
  Phone: { column: phone, type: text, roles: [sfc] }
  Mail: { column: mail, type: text, roles: [sfc] }
";

const JOB: &str = "0b9c6a1e-0000-4000-8000-0000000000a1";

/// The scratch table (gold's DDL without the partitioning, which the
/// memory connector lacks) and three synthetic rows of one job.
async fn scratch(base: &str) {
    rows(base, "CREATE SCHEMA IF NOT EXISTS lake.quality_scratch").await;
    let ddl = quality_rules_trino::install()
        .into_iter()
        .find(|s| s.sql.contains(SCRATCH))
        .unwrap()
        .sql
        .replace(" WITH (partitioning = ARRAY['job_id'])", "");
    rows(base, &ddl).await;
    rows(
        base,
        &format!(
            "INSERT INTO {SCRATCH} (job_id, dataset, k, s, serious, act, icd, f, g, c) VALUES \
             ('{JOB}', 'loans', 'h1', 'PN', true, true, 0.96e0, ARRAY[0.96e0], \
              ARRAY['N', 'PN'], ARRAY[false, true]), \
             ('{JOB}', 'loans', 'h1', 'PN', false, true, 0.86e0, ARRAY[0.86e0], \
              ARRAY[NULL, 'PN'], ARRAY[true, true]), \
             ('{JOB}', 'loans', 'h2', 'PJ', false, false, 0.8e0, ARRAY[0.8e0], \
              ARRAY['N', NULL], ARRAY[true, false])"
        ),
    )
    .await;
}

fn table(v: Vec<Vec<Value>>) -> Value {
    Value::from(v)
}

#[tokio::test]
async fn the_scratch_rows_give_groups_cells_and_entities() {
    let Some(base) = url() else { return };
    scratch(&base).await;
    let mapping = Mapping::parse(MAPPING).unwrap();

    let g = group_metrics(JOB, &mapping).unwrap();
    let sql = format!(
        "SELECT a, b, c, d, round(e, 6) FROM ({}) x (a, b, c, d, e) ORDER BY a, b",
        g.sql
    );
    assert_eq!(
        table(rows(&base, &sql).await),
        json!([
            ["Office", "", 1, 0, 0.86],
            ["Office", "N", 2, 1, 0.88],
            ["Type", "", 1, 0, 0.8],
            ["Type", "PN", 2, 1, 0.91]
        ])
    );

    let c = cell_rows(JOB, &mapping).unwrap();
    let sql = format!("SELECT * FROM ({}) x ORDER BY 1", c.sql);
    assert_eq!(
        table(rows(&base, &sql).await),
        json!([["Mail", 1, 2, false, 1, 0], ["Phone", 2, 2, true, 0, 0]])
    );

    let mut schema = Schema::default();
    schema.fields.insert("Phone".into(), FieldType::Text);
    let yaml = "dataset: loans\nrules:\n  \
                - { id: P1, field: Phone, check: required, severity: major, alert: a }\n";
    let (compiled, problems) = compile(&quality_rules_model::parse(yaml).unwrap(), &schema);
    assert!(problems.is_empty(), "{problems:?}");
    let rules: Vec<IcdRule> = compiled
        .iter()
        .map(|r| IcdRule::of(r, &Tables::new(), &Cut::parse("2026-10-31").unwrap()).unwrap())
        .collect();
    let e = entity_icd(JOB, &measured(&mapping, &rules)).unwrap();
    let sql = format!(
        "SELECT a, round(b, 6), c FROM ({}) x (a, b, c, d) ORDER BY 1",
        e.sql
    );
    assert_eq!(
        table(rows(&base, &sql).await),
        json!([["h1", 0.91, "high"], ["h2", 0.8, "medium"]])
    );
}

/// Ruling 73: the entities' aggregate views over synthetic entity rows of
/// two cuts: the ICD's twentieths, the bands by segment, and the bands'
/// moves from the previous cut by entity.
#[tokio::test]
async fn the_entity_views_give_only_aggregates() {
    let Some(base) = url() else { return };
    let entities = "(SELECT * FROM (VALUES \
        ('loans', DATE '2026-09-30', 'h1', 0.95e0, 'high', 'PN'), \
        ('loans', DATE '2026-09-30', 'h2', 0.70e0, 'low', 'PJ'), \
        ('loans', DATE '2026-10-31', 'h1', 0.80e0, 'medium', 'PN'), \
        ('loans', DATE '2026-10-31', 'h2', 0.72e0, 'low', 'PJ'), \
        ('loans', DATE '2026-10-31', 'h3', 0.91e0, 'high', 'PN')) \
        AS e (dataset, cut, entity, icd, band, segment))";
    let runs = "(SELECT * FROM (VALUES ('loans', DATE '2026-09-30', 'r1'), \
        ('loans', DATE '2026-10-31', 'r2')) AS r (dataset, cut, run_id))";
    let view = |name: &str| {
        let ddl = quality_rules_trino::install()
            .into_iter()
            .find(|s| s.sql.contains(&format!("\"{name}\" SECURITY DEFINER")))
            .unwrap()
            .sql;
        let body = ddl
            .split_once(" SECURITY DEFINER AS ")
            .unwrap()
            .1
            .to_string();
        format!(
            "SELECT * FROM ({}) x WHERE cut = DATE '2026-10-31' ORDER BY 3, 4",
            body.replace("\"gold\".\"quality_gold\".\"v_entity_icd\"", entities)
                .replace("\"gold\".\"quality_gold\".\"current_run\"", runs)
        )
    };
    assert_eq!(
        table(rows(&base, &view("v_bands_by_segment")).await),
        json!([
            ["loans", "2026-10-31", "PJ", 0, 0, 1, 1],
            ["loans", "2026-10-31", "PN", 1, 1, 0, 2]
        ])
    );
    assert_eq!(
        table(rows(&base, &view("v_band_transitions")).await),
        json!([
            ["loans", "2026-10-31", "high", "medium", 1],
            ["loans", "2026-10-31", "low", "low", 1]
        ])
    );
    assert_eq!(
        table(rows(&base, &view("v_icd_distribution")).await),
        json!([
            ["loans", "2026-10-31", 0.7, 1],
            ["loans", "2026-10-31", 0.8, 1],
            ["loans", "2026-10-31", 0.9, 1]
        ])
    );
}
