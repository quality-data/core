//! A run's values (decision 0090, rulings 79 and 80) on an ephemeral
//! PostgreSQL, driven without a socket: rows are taken only for a values
//! read of the run's pinned bundle, within its columns and rows, before
//! the run reports, and never through the report. Synthetic values only.

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

const RUN: &str = "0b9c6a1e-0000-4000-8000-000000000051";

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

fn statement(id: &str, expects: Expect, run_as: RunAs) -> BundleStatement {
    BundleStatement {
        id: id.into(),
        sql: "SELECT 1".into(),
        expects,
        run_as,
        reads_source: true,
        only_if_rows: None,
    }
}

/// A run of a kind with one values read of two columns and two rows, and
/// one count read.
fn bundle(kind: &str) -> Bundle {
    Bundle {
        job_id: RUN.into(),
        kind: kind.into(),
        dataset: None,
        cut: None,
        attempt: 1,
        run_id: Some(RUN.into()),
        run_seq: None,
        catalog_commit: None,
        compiler_version: "test".into(),
        placeholders: vec![],
        statements: vec![
            statement(
                "page",
                Expect::Values {
                    columns: vec![
                        ("name".into(), "varchar".into()),
                        ("amount".into(), "varchar".into()),
                    ],
                    max_rows: 2,
                },
                RunAs::Job,
            ),
            statement(
                "count",
                Expect::Rows {
                    columns: vec![("n".into(), "bigint".into())],
                    max_rows: 1,
                },
                RunAs::Job,
            ),
        ],
    }
}

async fn post(
    router: &Router,
    path: &str,
    token: Option<&str>,
    body: &Value,
) -> (StatusCode, Value) {
    let mut req = Request::post(format!("/internal/quality/v1/runs/{RUN}/{path}"))
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

/// The database, the app's pool and the routes, with `b` pinned to the run.
async fn fixture(b: &Bundle) -> (testkit::TestDb, sqlx::PgPool, Router) {
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
    ] {
        sqlx::query(sql).execute(&db.pool).await.unwrap();
    }
    let app = PgPoolOptions::new()
        .max_connections(4)
        .connect_with(options)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO quality.job_bundles (run, kind, digest, bundle, asked_by)
         VALUES ($1, $2, $3, $4, 'runner')",
    )
    .bind(RUN)
    .bind(&b.kind)
    .bind(b.digest())
    .bind(bundle::sign(&Plain, b))
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
    (db, app, router)
}

fn refusal(error: &str) -> (StatusCode, Value) {
    (StatusCode::UNPROCESSABLE_ENTITY, json!({ "error": error }))
}

#[tokio::test]
async fn rows_are_taken_only_for_a_declared_values_read_within_its_limit() {
    let b = bundle("evaluate");
    let (_db, _app, router) = fixture(&b).await;
    let runner = Some("token-runner");
    let good = json!({ "statement": "page", "rows": [["Name Example", "10.00"], [null, "2"]] });

    assert_eq!(
        post(&router, "rows", None, &good).await.0,
        StatusCode::UNAUTHORIZED
    );
    let other = json!({ "statement": "count", "rows": [[1]] });
    assert_eq!(
        post(&router, "rows", runner, &other).await,
        refusal("not_a_values_read")
    );
    let unknown = json!({ "statement": "nope", "rows": [] });
    assert_eq!(
        post(&router, "rows", runner, &unknown).await,
        refusal("no_such_statement")
    );
    let three = json!({ "statement": "page", "rows": [["a", "1"], ["b", "2"], ["c", "3"]] });
    assert_eq!(
        post(&router, "rows", runner, &three).await,
        refusal("rows_not_allowed")
    );
    let narrow = json!({ "statement": "page", "rows": [["a"]] });
    assert_eq!(
        post(&router, "rows", runner, &narrow).await,
        refusal("rows_not_allowed")
    );
    let typed = json!({ "statement": "page", "rows": [["a", 1]] });
    assert_eq!(
        post(&router, "rows", runner, &typed).await,
        refusal("rows_not_allowed")
    );
    // An evaluate reads no values: its kind takes none.
    assert_eq!(
        post(&router, "rows", runner, &good).await,
        refusal("kind_takes_no_values")
    );
    let extra = json!({ "statement": "page", "rows": [], "more": true });
    assert_eq!(
        post(&router, "rows", runner, &extra).await.0,
        StatusCode::UNPROCESSABLE_ENTITY
    );
}

#[tokio::test]
async fn values_never_ride_in_the_report_and_stop_once_it_is_in() {
    let b = bundle("evaluate");
    let (_db, app, router) = fixture(&b).await;
    let runner = Some("token-runner");
    let in_report = json!({
        "digest": b.digest(),
        "state": "failed",
        "statements": [{ "id": "page", "state": "done", "rows": [["Name Example", "10.00"]] }]
    });
    assert_eq!(
        post(&router, "report", runner, &in_report).await,
        refusal("rows_not_allowed")
    );
    let report = json!({
        "digest": b.digest(),
        "state": "failed",
        "statements": [{ "id": "page", "state": "failed" }],
        "error_class": "engine"
    });
    assert_eq!(
        post(&router, "report", runner, &report).await.0,
        StatusCode::NO_CONTENT
    );
    let kept: Value = sqlx::query_scalar("SELECT report FROM quality.job_reports WHERE run = $1")
        .bind(RUN)
        .fetch_one(&app)
        .await
        .unwrap();
    assert!(!kept.to_string().contains("Name Example"));
    let good = json!({ "statement": "page", "rows": [["Name Example", "10.00"]] });
    assert_eq!(
        post(&router, "rows", runner, &good).await,
        (StatusCode::CONFLICT, json!({ "error": "already_reported" }))
    );
    let missing = json!({ "statement": "page", "rows": [] });
    let res = router
        .clone()
        .oneshot(
            Request::post("/internal/quality/v1/runs/no-such-run/rows")
                .header("content-type", "application/json")
                .header("authorization", "Bearer token-runner")
                .body(Body::from(missing.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::NOT_FOUND);
}
