//! A run's source secrets (decision 0090, point 7.5) on an ephemeral
//! PostgreSQL, driven without a socket: a pinned extract's job runner gets
//! its source's user and password, current and previous, recorded first;
//! another kind, a run with no bundle, an unknown caller and a failed
//! record get nothing. Synthetic names and secrets only.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::Router;
use contracts::{Module, ModuleRegistry};
use infrastructure::module_migrations::PgModuleMigrator;
use quality::catalog::{Catalog, CatalogSnapshot, DatasetMapping};
use quality::job_sources::IssueLog;
use quality::module::QualityModule;
use quality::run_source_api::RunSourceServices;
use quality::source::Source;
use quality::source_store::{self, Sealer};
use quality::worker_api::{MachineCaller, MachineTokens};
use serde_json::{json, Value};
use sqlx::postgres::PgPoolOptions;
use tower::ServiceExt;
use uuid::Uuid;

const EXTRACT: &str = "0b9c6a1e-0000-4000-8000-000000000011";
const EVALUATE: &str = "0b9c6a1e-0000-4000-8000-000000000012";

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
impl Sealer for Plain {
    fn seal(&self, plain: &str) -> Result<String, String> {
        Ok(format!("sealed:{plain}"))
    }
    fn open(&self, sealed: &str) -> Result<String, String> {
        Ok(sealed.strip_prefix("sealed:").ok_or("not sealed")?.into())
    }
}

struct Files;
#[async_trait]
impl Catalog for Files {
    async fn snapshot(&self, dataset: &str) -> Result<Option<CatalogSnapshot>, String> {
        Ok(Some(CatalogSnapshot {
            commit: "c0ffee".into(),
            mapping: format!(
                "dataset: {dataset}\nsource: loans\nfrom: {{ source: core, table: public.loans }}\n\
                 fields:\n  Id: {{ column: loan_id, type: text }}\n"
            ),
            rules: vec![],
            others: vec![],
        }))
    }
    async fn mappings(&self) -> Result<Vec<DatasetMapping>, String> {
        Ok(vec![])
    }
}

/// What was recorded; refuses `deny`'s.
#[derive(Default)]
struct Trail(Mutex<Vec<String>>);
#[async_trait]
impl IssueLog for Trail {
    async fn issued(
        &self,
        job: Uuid,
        kind: &str,
        machine: &str,
        _ip: &str,
        resource: &str,
    ) -> Result<(), String> {
        if machine == "deny" {
            return Err("audit down".into());
        }
        self.0
            .lock()
            .unwrap()
            .push(format!("{job} {kind} {machine} {resource}"));
        Ok(())
    }
}

fn map(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| ((*k).into(), (*v).into()))
        .collect()
}

async fn ask(router: &Router, run: &str, token: Option<&str>) -> (StatusCode, Value) {
    let mut req = Request::post(format!("/internal/quality/v1/runs/{run}/source-secrets"));
    if let Some(t) = token {
        req = req.header("authorization", format!("Bearer {t}"));
    }
    let res = router
        .clone()
        .oneshot(req.body(Body::empty()).unwrap())
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
async fn an_extracts_runner_gets_its_sources_credentials_recorded_first() {
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
        "ALTER TABLE quality.sources OWNER TO app",
        "ALTER TABLE quality.job_bundles OWNER TO app",
    ] {
        sqlx::query(sql).execute(&db.pool).await.unwrap();
    }
    let app = PgPoolOptions::new()
        .max_connections(4)
        .connect_with(options)
        .await
        .unwrap();
    let source = |password: &str| Source {
        name: "core".into(),
        driver: "postgres".into(),
        fields: map(&[("host", "db.example"), ("database", "core")]),
        secrets: map(&[("user", "reader"), ("password", password)]),
    };
    source_store::put(&app, &Plain, &source("first"))
        .await
        .unwrap();
    source_store::put(&app, &Plain, &source("second"))
        .await
        .unwrap();
    for (run, kind) in [(EXTRACT, "extract"), (EVALUATE, "evaluate")] {
        sqlx::query(
            "INSERT INTO quality.job_bundles (run, kind, dataset, cut, digest, bundle, asked_by)
             VALUES ($1, $2, 'loans', DATE '2026-10-31', 'd', 'b', 'runner')",
        )
        .bind(run)
        .bind(kind)
        .execute(&app)
        .await
        .unwrap();
    }
    let trail = Arc::new(Trail::default());
    let router = QualityModule::default()
        .with_run_sources(
            app,
            RunSourceServices {
                tokens: Arc::new(Tokens),
                sealer: Arc::new(Plain),
                catalog: Arc::new(Files),
                log: trail.clone(),
            },
        )
        .router();

    let (status, body) = ask(&router, EXTRACT, Some("token-runner")).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        body,
        json!({
            "current": { "password": "second", "user": "reader" },
            "previous": { "password": "first", "user": "reader" }
        })
    );
    assert_eq!(
        *trail.0.lock().unwrap(),
        [format!("{EXTRACT} extract runner source/core")]
    );
    let (status, _) = ask(&router, EVALUATE, Some("token-runner")).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    let (status, _) = ask(
        &router,
        "0b9c6a1e-0000-4000-8000-0000000000ff",
        Some("token-runner"),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, _) = ask(&router, EXTRACT, None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let (status, _) = ask(&router, EXTRACT, Some("token-deny")).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(trail.0.lock().unwrap().len(), 1);
}
