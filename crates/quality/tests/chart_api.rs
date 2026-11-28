//! A dataset's charts (decision 0031), on an ephemeral PostgreSQL, driven
//! without a socket with the `Caller` server would put in each request:
//! the dashboard lists the charts that need nothing with the evaluated
//! cuts, and each chart's rows come from the cut asked or the latest,
//! grouped by what the worker reported, only the tenant's. Synthetic names
//! only.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::{Extension, Router};
use contracts::{Access, AuditEntry, AuditTrail, Caller, Module, ModuleRegistry};
use infrastructure::module_migrations::PgModuleMigrator;
use quality::module::QualityModule;
use quality::source_store::Sealer;
use serde_json::{json, Value};
use sqlx::postgres::PgPoolOptions;
use tower::ServiceExt;

const MAIN: &str = "00000000-0000-0000-0000-000000000001";
const OTHER: &str = "00000000-0000-0000-0000-0000000000ff";

struct NoSealer;
impl Sealer for NoSealer {
    fn seal(&self, _: &str) -> Result<String, String> {
        Err("unused".into())
    }
    fn open(&self, _: &str) -> Result<String, String> {
        Err("unused".into())
    }
}

/// May see the results of `loans` and `fresh` (never evaluated) only, or
/// nothing.
struct Viewer(bool);
impl Access for Viewer {
    fn can(&self, action: &str, resource: &str) -> bool {
        self.0
            && action == "quality:ViewResults"
            && matches!(resource, "pipeline/loans" | "pipeline/fresh")
    }
    fn can_some(&self, action: &str) -> bool {
        self.can(action, "pipeline/loans")
    }
}

struct NoLog;
#[async_trait::async_trait]
impl AuditTrail for NoLog {
    async fn record(&self, _: &Caller, _: &AuditEntry) -> Result<(), String> {
        Ok(())
    }
}

async fn fixture() -> (testkit::TestDb, Router) {
    let db = testkit::spawn_test_db().await;
    sqlx::query("CREATE ROLE qd_queue LOGIN PASSWORD 'queue'")
        .execute(&db.pool)
        .await
        .unwrap();
    let mut migrator = PgModuleMigrator::load(&db.pool).await.unwrap();
    ModuleRegistry::new()
        .register(QualityModule::default())
        .run_migrations(&mut migrator)
        .unwrap();
    migrator.run(&db.pool).await.unwrap();
    let options = testkit::app_role(&db).await;
    for sql in [
        "GRANT USAGE ON SCHEMA quality TO app",
        "ALTER TABLE quality.run_summary OWNER TO app",
        "ALTER TABLE quality.rule_results OWNER TO app",
        "ALTER TABLE quality.field_quality OWNER TO app",
        "ALTER TABLE quality.entity_icd OWNER TO app",
        "ALTER TABLE quality.group_metrics OWNER TO app",
        "ALTER TABLE quality.distribution_checks OWNER TO app",
        "ALTER TABLE quality.job_stats OWNER TO app",
        "ALTER TABLE quality.cells OWNER TO app",
        "ALTER TABLE quality.queue_findings OWNER TO app",
        "ALTER TABLE quality.remediation_slas OWNER TO app",
        // Fields of the latest cut of `loans`: one all empty.
        "INSERT INTO quality.run_summary (tenant_id, dataset, cut, job_id, records, duration_ms, rules_commit)
         VALUES ('00000000-0000-0000-0000-000000000001', 'loans', '2026-03-31', gen_random_uuid(), 100, 1, 'c1')",
        "INSERT INTO quality.field_quality (tenant_id, dataset, cut, field, empty, distinct_values, pattern, pattern_count)
         VALUES ('00000000-0000-0000-0000-000000000001', 'loans', '2026-03-31', 'Amount', 20, 70, '9999.9', 60),
                ('00000000-0000-0000-0000-000000000001', 'loans', '2026-03-31', 'Note', 100, 0, NULL, 0)",
    ] {
        sqlx::query(sql).execute(&db.pool).await.unwrap();
    }
    results(&db.pool).await;
    icd(&db.pool).await;
    let app = PgPoolOptions::new()
        .max_connections(2)
        .connect_with(options)
        .await
        .unwrap();
    let router = QualityModule::default()
        .with_sources(app, Arc::new(NoSealer), None)
        .user_router();
    (db, router)
}

/// Two cuts of `loans`, the older without what the charts group by (an
/// older worker's), and another tenant's results for the same cut.
async fn results(pool: &sqlx::PgPool) {
    for (tenant, cut, records) in [(MAIN, "2026-02-28", 90), (OTHER, "2026-03-31", 7)] {
        sqlx::query(
            "INSERT INTO quality.run_summary (tenant_id, dataset, cut, job_id, records, duration_ms, rules_commit)
             VALUES ($1::uuid, 'loans', $2::date, gen_random_uuid(), $3, 1, 'c1')",
        )
        .bind(tenant)
        .bind(cut)
        .bind(records)
        .execute(pool)
        .await
        .unwrap();
    }
    for (tenant, cut, rule, applies, failures, field, dimension) in [
        (MAIN, "2026-02-28", "L-01", 90, 9, None, None),
        (
            MAIN,
            "2026-03-31",
            "L-01",
            100,
            10,
            Some("Amount"),
            Some("validity"),
        ),
        (
            MAIN,
            "2026-03-31",
            "L-02",
            100,
            30,
            Some("Amount"),
            Some("completeness"),
        ),
        (
            MAIN,
            "2026-03-31",
            "L-03",
            0,
            0,
            Some("Term"),
            Some("validity"),
        ),
        (
            OTHER,
            "2026-03-31",
            "L-01",
            7,
            7,
            Some("Amount"),
            Some("validity"),
        ),
    ] {
        sqlx::query(
            "INSERT INTO quality.rule_results (tenant_id, dataset, cut, rule, applies, failures, field, dimension, severity)
             VALUES ($1::uuid, 'loans', $2::date, $3, $4, $5, $6, $7, 'major')",
        )
        .bind(tenant)
        .bind(cut)
        .bind(rule)
        .bind(applies as i64)
        .bind(failures as i64)
        .bind(field)
        .bind(dimension)
        .execute(pool)
        .await
        .unwrap();
    }
}

/// The ICD of both cuts of `loans` (decision 0057): four entities by
/// synthetic hashes, and another tenant's for the same cut.
async fn icd(pool: &sqlx::PgPool) {
    for sql in [
        "UPDATE quality.run_summary SET igcd = 0.8 WHERE tenant_id = $1::uuid AND cut = '2026-02-28'",
        "UPDATE quality.run_summary SET igcd = 0.82 WHERE tenant_id = $1::uuid AND cut = '2026-03-31'",
        "UPDATE quality.field_quality SET mean_icd = 0.6 WHERE tenant_id = $1::uuid AND field = 'Amount'",
        "UPDATE quality.field_quality SET mean_icd = 0.3 WHERE tenant_id = $1::uuid AND field = 'Note'",
        "INSERT INTO quality.entity_icd (tenant_id, dataset, cut, entity, icd, band, segment)
         VALUES ($1::uuid, 'loans', '2026-02-28', 'e1', 0.95, 'high', 'PN'),
                ($1::uuid, 'loans', '2026-02-28', 'e2', 0.80, 'medium', 'PN'),
                ($1::uuid, 'loans', '2026-02-28', 'e3', 0.50, 'low', 'PJ'),
                ($1::uuid, 'loans', '2026-03-31', 'e1', 0.95, 'high', 'PN'),
                ($1::uuid, 'loans', '2026-03-31', 'e2', 0.60, 'low', 'PN'),
                ($1::uuid, 'loans', '2026-03-31', 'e3', 0.80, 'medium', 'PJ'),
                ($1::uuid, 'loans', '2026-03-31', 'e4', 1.00, 'high', 'PJ')",
    ] {
        sqlx::query(sql).bind(MAIN).execute(pool).await.unwrap();
    }
    sqlx::query(
        "INSERT INTO quality.entity_icd (tenant_id, dataset, cut, entity, icd, band, segment)
         VALUES ($1::uuid, 'loans', '2026-03-31', 'e1', 0.10, 'low', 'PN')",
    )
    .bind(OTHER)
    .execute(pool)
    .await
    .unwrap();
}

async fn get(router: &Router, may: bool, uri: &str) -> (StatusCode, Value) {
    let caller = Caller::new(
        "ana".into(),
        None,
        "192.0.2.1".into(),
        None,
        Arc::new(Viewer(may)),
        Arc::new(NoLog),
    );
    let req = Request::builder().uri(uri).body(Body::empty()).unwrap();
    let res = router
        .clone()
        .layer(Extension(caller))
        .oneshot(req)
        .await
        .unwrap();
    let status = res.status();
    let bytes = axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

#[tokio::test]
async fn a_datasets_dashboard_lists_its_charts_and_evaluated_cuts() {
    let (_db, router) = fixture().await;
    let (status, out) = get(&router, true, "/quality/datasets/loans/charts").await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert_eq!(out["cuts"], json!(["2026-03-31", "2026-02-28"]));
    let ids: Vec<&str> = out["charts"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c["id"].as_str().unwrap())
        .collect();
    assert_eq!(
        ids,
        [
            "trend_by_cut",
            "failures_by_dimension",
            "failures_by_field",
            "lowest_icd",
            "profiling",
            "conformity_by_rule",
            "time_by_stage"
        ]
    );
    assert_eq!(
        out["charts"][0],
        json!({"id": "trend_by_cut", "kind": "line", "needs": []})
    );

    // Another dataset's results, or none at all.
    for (may, uri) in [
        (true, "/quality/datasets/cards/charts"),
        (false, "/quality/datasets/loans/charts"),
        (false, "/quality/datasets/loans/charts/failures_by_field"),
    ] {
        assert_eq!(
            get(&router, may, uri).await.0,
            StatusCode::FORBIDDEN,
            "{uri}"
        );
    }
}

#[tokio::test]
async fn a_charts_rows_come_from_the_cut_asked_or_the_latest() {
    let (_db, router) = fixture().await;
    let base = "/quality/datasets/loans/charts";
    let (status, out) = get(&router, true, &format!("{base}/failures_by_field")).await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert_eq!(out["cut"], "2026-03-31");
    assert_eq!(
        out["rows"],
        json!([
            {"field": "Amount", "rules": 2, "applies": 200, "failures": 40},
            {"field": "Term", "rules": 1, "applies": 0, "failures": 0},
        ])
    );

    let (_, out) = get(&router, true, &format!("{base}/conformity_by_rule")).await;
    let conformity: Vec<(&str, &Value)> = out["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| (r["rule"].as_str().unwrap(), &r["conformity"]))
        .collect();
    // A rule that applies to nothing has no conformity, last.
    assert_eq!(
        conformity,
        [
            ("L-02", &json!(0.7)),
            ("L-01", &json!(0.9)),
            ("L-03", &Value::Null)
        ]
    );

    // The older cut, by an older worker: its rule under no dimension.
    let (_, out) = get(
        &router,
        true,
        &format!("{base}/failures_by_dimension?cut=2026-02-28"),
    )
    .await;
    assert_eq!(out["cut"], "2026-02-28");
    assert_eq!(
        out["rows"],
        json!([{"dimension": null, "severity": "major", "rules": 1, "applies": 90, "failures": 9}])
    );

    // Each field's profile: the emptiest first, shares of the records and
    // of the filled ones.
    let (_, out) = get(&router, true, &format!("{base}/profiling")).await;
    assert_eq!(
        out["rows"],
        json!([
            {"field": "Note", "records": 100, "empty": 100, "empty_share": 1.0,
             "distinct_values": 0, "pattern": null, "floating": null, "pattern_share": null},
            {"field": "Amount", "records": 100, "empty": 20, "empty_share": 0.2,
             "distinct_values": 70, "pattern": "9999.9", "floating": null,
             "pattern_share": 0.75},
        ])
    );

    // The trend up to the latest cut, oldest first, only this tenant's.
    let (_, out) = get(&router, true, &format!("{base}/trend_by_cut")).await;
    assert_eq!(
        out["rows"],
        json!([
            {"cut": "2026-02-28", "records": 90, "failures": 9, "conformity": 0.9, "igcd": 0.8},
            {"cut": "2026-03-31", "records": 100, "failures": 40, "conformity": 0.8, "igcd": 0.82},
        ])
    );

    // An unknown chart or cut; a cut not evaluated; no cut yet.
    for uri in [
        format!("{base}/ghost"),
        format!("{base}/trend_by_cut?cut=2026-01-31"),
        format!("{base}/trend_by_cut?cut=yesterday"),
    ] {
        assert_eq!(
            get(&router, true, &uri).await.0,
            StatusCode::NOT_FOUND,
            "{uri}"
        );
    }
    let (status, out) = get(&router, true, "/quality/datasets/fresh/charts/trend_by_cut").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        out,
        json!({"chart": "trend_by_cut", "cut": null, "rows": []})
    );
}

#[tokio::test]
async fn the_icds_charts_read_its_entities_by_band_segment_and_cut() {
    let (_db, router) = fixture().await;
    let base = "/quality/datasets/loans/charts";
    let rows = |out: Value| out["rows"].clone();
    let (status, out) = get(&router, true, &format!("{base}/lowest_icd")).await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert_eq!(
        rows(out),
        json!([{"field": "Note", "icd": 0.3}, {"field": "Amount", "icd": 0.6}])
    );
    // In bins of 0.05; 1.0 counts with 0.95. Only this tenant's.
    let (_, out) = get(&router, true, &format!("{base}/icd_distribution")).await;
    assert_eq!(
        rows(out),
        json!([
            {"from_icd": 0.6, "entities": 1},
            {"from_icd": 0.8, "entities": 1},
            {"from_icd": 0.95, "entities": 2},
        ])
    );
    let (_, out) = get(&router, true, &format!("{base}/bands_by_segment")).await;
    assert_eq!(
        rows(out),
        json!([
            {"segment": "PJ", "high": 1, "medium": 1, "low": 0},
            {"segment": "PN", "high": 1, "medium": 0, "low": 1},
        ])
    );
    // From the previous cut, matched by hash: e4 is new, so not counted.
    let (_, out) = get(&router, true, &format!("{base}/band_transitions")).await;
    assert_eq!(
        rows(out),
        json!([
            {"from_band": "high", "to_band": "high", "entities": 1},
            {"from_band": "medium", "to_band": "low", "entities": 1},
            {"from_band": "low", "to_band": "medium", "entities": 1},
        ])
    );
    // The oldest cut has no previous one.
    let (_, out) = get(
        &router,
        true,
        &format!("{base}/band_transitions?cut=2026-02-28"),
    )
    .await;
    assert_eq!(rows(out), json!([]));
}

#[tokio::test]
async fn the_update_recency_reads_its_fields_records_by_age_the_empty_last() {
    let (db, router) = fixture().await;
    sqlx::query(
        "UPDATE quality.field_quality SET recency = '{0,50,10,5,5,5,5}'
          WHERE dataset = 'loans' AND cut = '2026-03-31' AND field = 'Amount'",
    )
    .execute(&db.pool)
    .await
    .unwrap();
    let (status, out) = get(
        &router,
        true,
        "/quality/datasets/loans/charts/update_recency",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{out}");
    let bands: Vec<Value> = [0, 50, 10, 5, 5, 5, 5]
        .iter()
        .enumerate()
        .map(|(i, n)| json!({"band": format!("b{}", i + 1), "records": n}))
        .chain([json!({"band": "empty", "records": 20})])
        .collect();
    assert_eq!(out["rows"], json!(bands));
}

#[tokio::test]
async fn the_groups_charts_read_each_dimension_by_size_limits_and_change() {
    let (db, router) = fixture().await;
    // Synthetic offices: two cuts, the empty group in the latest.
    sqlx::query(
        "INSERT INTO quality.group_metrics
             (tenant_id, dataset, cut, dimension, grp, records, serious, mean_icd)
         VALUES ($1::uuid, 'loans', '2026-02-28', 'Office', 'O-1', 100, 20, 0.8),
                ($1::uuid, 'loans', '2026-02-28', 'Office', 'O-2', 100, 10, 0.9),
                ($1::uuid, 'loans', '2026-03-31', 'Office', 'O-1', 900, 90, 0.85),
                ($1::uuid, 'loans', '2026-03-31', 'Office', 'O-2', 100, 30, NULL),
                ($1::uuid, 'loans', '2026-03-31', 'Office', '', 40, 10, 0.5)",
    )
    .bind(MAIN)
    .execute(&db.pool)
    .await
    .unwrap();
    let base = "/quality/datasets/loans/charts";
    let rows = |out: Value| out["rows"].as_array().unwrap().clone();

    // p̄ = 120 / 1000; z by size, O-2 then O-1; σz their moving range / 1.128.
    let (status, out) = get(&router, true, &format!("{base}/groups_p_chart")).await;
    assert_eq!(status, StatusCode::OK, "{out}");
    let p = rows(out);
    let sigma = |n: f64| (0.12_f64 * 0.88 / n).sqrt();
    let z = |p: f64, n: f64| (p - 0.12) / sigma(n);
    let sigma_z = (z(0.1, 900.0) - z(0.3, 100.0)).abs() / 1.128;
    let ucl = |n: f64| 0.12 + 3.0 * sigma(n) * sigma_z;
    let groups: Vec<&str> = p.iter().map(|r| r["grp"].as_str().unwrap()).collect();
    assert_eq!(groups, ["O-1", "O-2"], "the empty group is left out");
    for (r, n) in p.iter().zip([900.0, 100.0]) {
        assert_eq!(r["p_bar"], json!(0.12));
        assert!((r["ucl"].as_f64().unwrap() - ucl(n)).abs() < 1e-4, "{r}");
        assert_eq!(r["lcl"], json!(0.0));
        assert_eq!(r["signal"], json!("in"));
    }

    let (_, out) = get(&router, true, &format!("{base}/largest_groups")).await;
    let largest: Vec<(String, i64)> = rows(out)
        .iter()
        .map(|r| {
            (
                r["grp"].as_str().unwrap().into(),
                r["records"].as_i64().unwrap(),
            )
        })
        .collect();
    assert_eq!(
        largest,
        [
            ("O-1".into(), 900),
            ("O-2".into(), 100),
            (String::new(), 40)
        ]
    );

    // From the previous cut: O-2 moved most.
    let (_, out) = get(&router, true, &format!("{base}/groups_changed")).await;
    assert_eq!(
        json!(rows(out)),
        json!([
            {"dimension": "Office", "grp": "O-2", "before": 0.1, "now": 0.3, "change": 0.2},
            {"dimension": "Office", "grp": "O-1", "before": 0.2, "now": 0.1, "change": -0.1},
        ])
    );
}

#[tokio::test]
async fn the_distributions_charts_read_the_outliers_by_group_and_the_psi_by_decile() {
    let (db, router) = fixture().await;
    // A synthetic variable: 0 to 100 at the previous cut, 10 to 110 now;
    // and its outliers by office.
    sqlx::query(
        "INSERT INTO quality.distribution_checks
             (tenant_id, dataset, cut, variable, dimension, grp, percentiles, records, low, high)
         VALUES ($1::uuid, 'loans', '2026-02-28', 'Income', '', '',
                 ARRAY(SELECT g::float8 FROM generate_series(0, 100) g), NULL, NULL, NULL),
                ($1::uuid, 'loans', '2026-03-31', 'Income', '', '',
                 ARRAY(SELECT g::float8 + 10 FROM generate_series(0, 100) g), NULL, NULL, NULL),
                ($1::uuid, 'loans', '2026-03-31', 'Income', 'Office', 'O-2', NULL, 100, 0, 0),
                ($1::uuid, 'loans', '2026-03-31', 'Income', 'Office', 'O-1', NULL, 200, 2, 8)",
    )
    .bind(MAIN)
    .execute(&db.pool)
    .await
    .unwrap();
    let base = "/quality/datasets/loans/charts";
    let (status, out) = get(&router, true, &format!("{base}/outliers_by_group")).await;
    assert_eq!(status, StatusCode::OK, "{out}");
    let seen: Vec<(&str, f64)> = out["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| (r["grp"].as_str().unwrap(), r["share"].as_f64().unwrap()))
        .collect();
    assert_eq!(seen, [("O-1", 0.05), ("O-2", 0.0)]);

    // Bins at 10, 20, …: none now in the first (floored at 0.0001), a
    // tenth in the next eight, a fifth in the last.
    let (status, out) = get(&router, true, &format!("{base}/psi_by_decile")).await;
    assert_eq!(status, StatusCode::OK, "{out}");
    let rows = out["rows"].as_array().unwrap();
    let now: Vec<f64> = rows.iter().map(|r| r["now"].as_f64().unwrap()).collect();
    assert_eq!(now, [0.0001, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.2]);
    assert!(rows.iter().all(|r| r["base"] == json!(0.1)));
    let psi = (0.0001_f64 - 0.1) * (0.0001_f64 / 0.1).ln() + 0.1 * 2_f64.ln();
    assert!(
        (rows[0]["psi"].as_f64().unwrap() - psi).abs() < 1e-4,
        "{}",
        rows[0]
    );
}

#[tokio::test]
async fn the_time_per_stage_reads_each_stage_per_million_record_attributes() {
    let (db, router) = fixture().await;
    // The latest cut: 100 records × 2 fields; the older has no fields, so
    // no volume, and no `rules` stage.
    sqlx::query(
        "INSERT INTO quality.job_stats (tenant_id, dataset, cut, stage, position, ms)
         VALUES ($1::uuid, 'loans', '2026-02-28', 'load', 1, 30),
                ($1::uuid, 'loans', '2026-03-31', 'load', 1, 50),
                ($1::uuid, 'loans', '2026-03-31', 'rules', 2, 7)",
    )
    .bind(MAIN)
    .execute(&db.pool)
    .await
    .unwrap();
    let base = "/quality/datasets/loans/charts/time_by_stage";
    let (status, out) = get(&router, true, base).await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert_eq!(
        out["rows"],
        json!([
            {"stage": "load", "ms": 50, "per_million": 250000.0,
             "previous_ms": 30, "previous_per_million": null},
            {"stage": "rules", "ms": 7, "per_million": 35000.0,
             "previous_ms": null, "previous_per_million": null},
        ])
    );
    let (_, out) = get(&router, true, &format!("{base}?cut=2026-02-28")).await;
    assert_eq!(
        out["rows"],
        json!([{"stage": "load", "ms": 30, "per_million": null,
                "previous_ms": null, "previous_per_million": null}])
    );
}

#[tokio::test]
async fn the_sfc_view_reads_each_cell_with_its_closeness_and_the_share_passing() {
    let (db, router) = fixture().await;
    // Synthetic cells of 100 active records: one passes at risk, one passes
    // well, one in the band, one far; and an older cut's.
    sqlx::query(
        "INSERT INTO quality.cells
             (tenant_id, dataset, cut, cell, ok, total, passes, missing, margin)
         VALUES ($1::uuid, 'loans', '2026-03-31', 'Phone', 91, 100, true, 0, 1),
                ($1::uuid, 'loans', '2026-03-31', 'Name', 99, 100, true, 0, 9),
                ($1::uuid, 'loans', '2026-03-31', 'Email', 85, 100, false, 5, 0),
                ($1::uuid, 'loans', '2026-03-31', 'City', 60, 100, false, 30, 0),
                ($1::uuid, 'loans', '2026-02-28', 'Phone', 10, 100, false, 80, 0)",
    )
    .bind(MAIN)
    .execute(&db.pool)
    .await
    .unwrap();
    let (status, out) = get(&router, true, "/quality/datasets/loans/charts/sfc_view").await;
    assert_eq!(status, StatusCode::OK, "{out}");
    let rows = out["rows"].as_array().unwrap();
    let seen: Vec<(&str, f64, f64, i64)> = rows
        .iter()
        .map(|r| {
            (
                r["cell"].as_str().unwrap(),
                r["p"].as_f64().unwrap(),
                r["closeness"].as_f64().unwrap(),
                r["missing"].as_i64().unwrap(),
            )
        })
        .collect();
    assert_eq!(
        seen,
        [
            ("Name", 0.99, 0.2, 0),
            ("Phone", 0.91, 0.5, 0),
            ("Email", 0.85, 1.0, 5),
            ("City", 0.6, 0.3, 30),
        ]
    );
    assert!(rows.iter().all(|r| r["sfc"] == json!(0.5)), "two of four");
}

#[tokio::test]
async fn the_queues_chart_reads_open_overdue_by_each_sla_resolved_and_the_oldest() {
    let (db, router) = fixture().await;
    // Synthetic offices at the latest cut, by the cut their findings opened
    // at; O-2 has an SLA of its own, the office dimension one of 60 days.
    sqlx::query(
        "INSERT INTO quality.queue_findings
             (tenant_id, dataset, cut, dimension, grp, since, open, resolved, priority)
         VALUES ($1::uuid, 'loans', '2026-03-31', 'Office', 'O-1', '2026-01-15', 5, 0, 1.5),
                ($1::uuid, 'loans', '2026-03-31', 'Office', 'O-1', '2026-03-31', 10, 0, 2.0),
                ($1::uuid, 'loans', '2026-03-31', 'Office', 'O-2', '2026-02-28', 3, 2, 0.75),
                ($1::uuid, 'loans', '2026-03-31', 'Office', '', '2026-02-28', 0, 4, 0)",
    )
    .bind(MAIN)
    .execute(&db.pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO quality.remediation_slas (tenant_id, dataset, dimension, grp, days)
         VALUES ($1::uuid, 'loans', 'Office', NULL, 60), ($1::uuid, 'loans', 'Office', 'O-2', 15)",
    )
    .bind(MAIN)
    .execute(&db.pool)
    .await
    .unwrap();
    let (status, out) = get(
        &router,
        true,
        "/quality/datasets/loans/charts/remediation_queues",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{out}");
    // O-1: mid-January plus 60 days is before the cut; O-2: February plus 15.
    assert_eq!(
        out["rows"],
        json!([
            {"dimension": "Office", "grp": "O-1", "open": 15, "overdue": 5, "resolved": 0,
             "oldest_days": 75, "sla_days": 60, "priority": 3.5},
            {"dimension": "Office", "grp": "O-2", "open": 3, "overdue": 3, "resolved": 2,
             "oldest_days": 31, "sla_days": 15, "priority": 0.75},
            {"dimension": "Office", "grp": "", "open": 0, "overdue": 0, "resolved": 4,
             "oldest_days": null, "sla_days": 60, "priority": 0.0},
        ])
    );
}
