//! A run's report (decision 0090, rulings 75 and 76) on an ephemeral
//! PostgreSQL, driven without a socket: a done evaluate's report is kept
//! once and writes the index from its read-back rows; a wrong digest,
//! rows for a statement that reads nothing, rows beyond what the bundle
//! declared, a repeated report and an unknown caller are refused.
//! Synthetic names only.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::Router;
use contracts::{Module, ModuleRegistry};
use infrastructure::module_migrations::PgModuleMigrator;
use quality::bundle::{self, Bundle, BundleSigner, BundleStatement, Expect, RunAs};
use quality::module::QualityModule;
use quality::run_report_api::RunReportServices;
use quality::worker_api::{MachineCaller, MachineTokens};
use serde_json::{json, Value};
use sqlx::postgres::PgPoolOptions;
use tower::ServiceExt;

const RUN: &str = "0b9c6a1e-0000-4000-8000-000000000031";

struct Tokens;
impl MachineTokens for Tokens {
    fn client(&self, token: &str) -> Option<MachineCaller> {
        Some(MachineCaller {
            client: token.strip_prefix("token-")?.to_string(),
            machine: None,
        })
    }
}

struct Plain;
impl BundleSigner for Plain {
    fn sign(&self, typ: &str, claims: &str) -> String {
        format!("{typ}|{claims}")
    }
    fn verify(&self, typ: &str, token: &str) -> Option<String> {
        token.strip_prefix(&format!("{typ}|")).map(str::to_string)
    }
}

fn read(id: &str, columns: &[(&str, &str)], max_rows: u64) -> BundleStatement {
    BundleStatement {
        id: id.into(),
        sql: "SELECT 1".into(),
        expects: Expect::Rows {
            columns: columns
                .iter()
                .map(|(n, t)| ((*n).into(), (*t).into()))
                .collect(),
            max_rows,
        },
        run_as: RunAs::Report,
        reads_source: false,
        only_if_rows: None,
    }
}

fn bundle() -> Bundle {
    Bundle {
        job_id: RUN.into(),
        kind: "evaluate".into(),
        dataset: Some("loans".into()),
        cut: Some("2026-10-31".into()),
        attempt: 1,
        run_id: Some(RUN.into()),
        run_seq: Some(1),
        catalog_commit: Some("c0ffee".into()),
        compiler_version: "test".into(),
        placeholders: vec![],
        statements: vec![
            BundleStatement {
                id: "flip".into(),
                sql: "INSERT INTO t SELECT 1".into(),
                expects: Expect::insert(),
                run_as: RunAs::Job,
                reads_source: false,
                only_if_rows: None,
            },
            read(
                "index_run_summary",
                &[("records", "bigint"), ("duration_ms", "bigint")],
                1,
            ),
            read(
                "index_rule_results",
                &[
                    ("rule", "varchar"),
                    ("applies", "bigint"),
                    ("failures", "bigint"),
                ],
                2,
            ),
        ],
    }
}

async fn post(router: &Router, token: Option<&str>, body: &Value) -> (StatusCode, Value) {
    let mut req = Request::post(format!("/internal/quality/v1/runs/{RUN}/report"))
        .header("content-type", "application/json");
    if let Some(t) = token {
        req = req.header("authorization", format!("Bearer {t}"));
    }
    let res = router
        .clone()
        .oneshot(req.body(Body::from(body.to_string())).unwrap())
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

/// The database, the app's pool and the route, with the run's bundle
/// pinned and its cut evaluating.
async fn fixture() -> (testkit::TestDb, sqlx::PgPool, Router, Bundle) {
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
        "ALTER TABLE quality.job_bundles OWNER TO app",
        "ALTER TABLE quality.job_reports OWNER TO app",
        "ALTER TABLE quality.run_summary OWNER TO app",
        "ALTER TABLE quality.rule_results OWNER TO app",
        "ALTER TABLE quality.cuts OWNER TO app",
        "ALTER TABLE quality.sources OWNER TO app",
        "ALTER TABLE quality.source_tables OWNER TO app",
    ] {
        sqlx::query(sql).execute(&db.pool).await.unwrap();
    }
    let app = PgPoolOptions::new()
        .max_connections(4)
        .connect_with(options)
        .await
        .unwrap();
    let b = bundle();
    sqlx::query(
        "INSERT INTO quality.job_bundles (run, kind, dataset, cut, digest, bundle, asked_by)
         VALUES ($1, 'evaluate', 'loans', DATE '2026-10-31', $2, $3, 'runner')",
    )
    .bind(RUN)
    .bind(b.digest())
    .bind(bundle::sign(&Plain, &b))
    .execute(&app)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO quality.cuts (dataset, cut, state, due_at) VALUES ('loans', DATE '2026-10-31', 'evaluating', now())",
    )
    .execute(&app)
    .await
    .unwrap();
    let router = QualityModule::default()
        .with_run_reports(
            app.clone(),
            RunReportServices {
                tokens: Arc::new(Tokens),
                signer: Arc::new(Plain),
                drafts: None,
                proposal_alerts: None,
            },
        )
        .router();
    (db, app, router, b)
}

#[tokio::test]
async fn a_done_evaluate_writes_the_index_from_its_read_back_counts() {
    let (_db, app, router, b) = fixture().await;
    let report = |rules: Value| {
        json!({
            "digest": b.digest(),
            "state": "done",
            "statements": [
                { "id": "flip", "state": "done" },
                { "id": "index_run_summary", "state": "done", "rows": [[6, 120]] },
                { "id": "index_rule_results", "state": "done", "rows": rules }
            ],
            "snapshots": [{ "placeholder": "silver_snapshot", "value": "77" }]
        })
    };
    let good = report(json!([["A1", 6, 2], ["P1", 6, 2]]));

    let (status, _) = post(&router, None, &good).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let mut wrong = good.clone();
    wrong["digest"] = json!("0000");
    assert_eq!(
        post(&router, Some("token-runner"), &wrong).await.0,
        StatusCode::CONFLICT
    );
    let mut stray = good.clone();
    stray["statements"][0]["rows"] = json!([[1]]);
    assert_eq!(
        post(&router, Some("token-runner"), &stray).await,
        (
            StatusCode::UNPROCESSABLE_ENTITY,
            json!({ "error": "rows_not_allowed" })
        )
    );
    let too_many = report(json!([["A1", 6, 2], ["P1", 6, 2], ["X1", 1, 1]]));
    assert_eq!(
        post(&router, Some("token-runner"), &too_many).await.0,
        StatusCode::UNPROCESSABLE_ENTITY
    );

    assert_eq!(
        post(&router, Some("token-runner"), &good).await.0,
        StatusCode::NO_CONTENT
    );
    let summary: (i64, i64, String) = sqlx::query_as(
        "SELECT records, duration_ms, rules_commit FROM quality.run_summary WHERE dataset = 'loans'",
    )
    .fetch_one(&app)
    .await
    .unwrap();
    assert_eq!(summary, (6, 120, "c0ffee".to_string()));
    let rules: Vec<(String, i64, i64)> = sqlx::query_as(
        "SELECT rule, applies, failures FROM quality.rule_results WHERE dataset = 'loans' ORDER BY rule",
    )
    .fetch_all(&app)
    .await
    .unwrap();
    assert_eq!(rules, [("A1".into(), 6, 2), ("P1".into(), 6, 2)]);
    let state: String =
        sqlx::query_scalar("SELECT state FROM quality.cuts WHERE dataset = 'loans'")
            .fetch_one(&app)
            .await
            .unwrap();
    assert_eq!(state, "done");
    assert_eq!(
        post(&router, Some("token-runner"), &good).await,
        (StatusCode::CONFLICT, json!({ "error": "already_reported" }))
    );
}

/// Ruling 68: a `tables` run's listing rows become its source's listing.
#[tokio::test]
async fn a_tables_runs_listing_is_kept_as_its_sources() {
    let (_db, app, router, _) = fixture().await;
    for sql in
        ["INSERT INTO quality.sources (name, driver, secrets) VALUES ('core', 'postgres', 'x')"]
    {
        sqlx::query(sql).execute(&app).await.unwrap();
    }
    let run = "0b9c6a1e-0000-4000-8000-000000000032";
    let b = Bundle {
        job_id: run.into(),
        kind: "tables".into(),
        dataset: None,
        cut: None,
        run_id: None,
        run_seq: None,
        catalog_commit: None,
        statements: vec![read(
            "listing",
            &[
                ("table_schema", "varchar"),
                ("table_name", "varchar"),
                ("column_name", "varchar"),
                ("data_type", "varchar"),
                ("is_nullable", "varchar"),
            ],
            100,
        )],
        ..bundle()
    };
    sqlx::query(
        "INSERT INTO quality.job_bundles (run, kind, digest, bundle, asked_by, source)
         VALUES ($1, 'tables', $2, $3, 'runner', 'core')",
    )
    .bind(run)
    .bind(b.digest())
    .bind(bundle::sign(&Plain, &b))
    .execute(&app)
    .await
    .unwrap();
    let report = json!({
        "digest": b.digest(),
        "state": "done",
        "statements": [{ "id": "listing", "state": "done", "rows": [
            ["public", "loans", "loan_id", "bigint", "NO"],
            ["public", "loans", "amount", "decimal(12,2)", "YES"],
            ["public", "offices", "code", "varchar", null]
        ]}]
    });
    let req = Request::post(format!("/internal/quality/v1/runs/{run}/report"))
        .header("content-type", "application/json")
        .header("authorization", "Bearer token-runner")
        .body(Body::from(report.to_string()))
        .unwrap();
    let res = router.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::NO_CONTENT);
    let tables: Value =
        sqlx::query_scalar("SELECT tables FROM quality.source_tables WHERE source = 'core'")
            .fetch_one(&app)
            .await
            .unwrap();
    assert_eq!(
        tables,
        json!([
            {"schema": "public", "name": "loans", "columns": [
                {"name": "loan_id", "data_type": "bigint", "nullable": false},
                {"name": "amount", "data_type": "decimal(12,2)", "nullable": true}]},
            {"schema": "public", "name": "offices", "columns": [
                {"name": "code", "data_type": "varchar"}]}
        ])
    );
}

/// Ruling 76: every index table the report reads back is written, each
/// type as the index keeps it.
#[tokio::test]
async fn the_other_index_tables_come_from_their_read_back_rows() {
    use quality::index_rows::INDEX_TABLES;
    let (_db, app, router, mut b) = fixture().await;
    for sql in [
        "ALTER TABLE quality.field_quality OWNER TO app",
        "ALTER TABLE quality.group_metrics OWNER TO app",
        "ALTER TABLE quality.cells OWNER TO app",
        "ALTER TABLE quality.queue_findings OWNER TO app",
    ] {
        sqlx::query(sql).execute(&_db.pool).await.unwrap();
    }
    let run = "0b9c6a1e-0000-4000-8000-000000000033";
    b.job_id = run.into();
    b.statements.truncate(2);
    for t in INDEX_TABLES {
        let cols: Vec<(&str, &str)> = t.columns.iter().map(|(n, ty)| (*n, ty.trino())).collect();
        b.statements.push(read(&t.statement(), &cols, t.max_rows));
    }
    sqlx::query(
        "INSERT INTO quality.job_bundles (run, kind, dataset, cut, digest, bundle, asked_by)
         VALUES ($1, 'evaluate', 'loans', DATE '2026-10-31', $2, $3, 'runner')",
    )
    .bind(run)
    .bind(b.digest())
    .bind(bundle::sign(&Plain, &b))
    .execute(&app)
    .await
    .unwrap();
    let report = json!({
        "digest": b.digest(),
        "state": "done",
        "statements": [
            { "id": "index_run_summary", "state": "done", "rows": [[6, 120]] },
            { "id": "index_rule_results", "state": "done", "rows": [["P1", 6, 2]] },
            { "id": "index_field_quality", "state": "done", "rows": [
                ["Phone", 2, 4, "9999999999", 4, false, 0.75, [1, 2, 0, 0, 0, 0, 1]]] },
            { "id": "index_group_metrics", "state": "done", "rows": [["Type", "PN", 4, 1, 0.8]] },
            { "id": "index_cells", "state": "done", "rows": [["Phone", 3, 4, false, 1, 0]] },
            { "id": "index_queue_findings", "state": "done", "rows": [["Type", "PN", "2026-10-31", 2, 0, 1.5]] }
        ]
    });
    let req = Request::post(format!("/internal/quality/v1/runs/{run}/report"))
        .header("content-type", "application/json")
        .header("authorization", "Bearer token-runner")
        .body(Body::from(report.to_string()))
        .unwrap();
    let res = router.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::NO_CONTENT);
    let fq: (String, Option<Vec<i64>>, Option<bool>) =
        sqlx::query_as("SELECT field, recency, floating FROM quality.field_quality")
            .fetch_one(&app)
            .await
            .unwrap();
    assert_eq!(
        fq,
        ("Phone".into(), Some(vec![1, 2, 0, 0, 0, 0, 1]), Some(false))
    );
    for (table, n) in [("group_metrics", 1), ("cells", 1), ("queue_findings", 1)] {
        let got: i64 = sqlx::query_scalar(&format!("SELECT count(*) FROM quality.{table}"))
            .fetch_one(&app)
            .await
            .unwrap();
        assert_eq!(got, n, "{table}");
    }
}
