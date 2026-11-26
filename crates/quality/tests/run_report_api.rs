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
