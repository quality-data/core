//! A run's Trino token (decision 0090, point 7.1) on an ephemeral
//! PostgreSQL, driven without a socket: the job runner of a pinned run
//! gets a token of one of its kind's users, recorded first; a run with no
//! bundle, a user its kind does not need, an unknown caller and a failed
//! record get none. Synthetic names only.

use std::sync::{Arc, Mutex};

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::Router;
use contracts::{Module, ModuleRegistry};
use infrastructure::module_migrations::PgModuleMigrator;
use quality::job_sources::IssueLog;
use quality::module::QualityModule;
use quality::run_token_api::{RunTokenServices, TrinoToken, TrinoTokens, TrinoUser};
use quality::worker_api::{MachineCaller, MachineTokens};
use serde_json::{json, Value};
use sqlx::postgres::PgPoolOptions;
use tower::ServiceExt;
use uuid::Uuid;

const RUN: &str = "0b9c6a1e-0000-4000-8000-000000000009";

struct Tokens;
impl MachineTokens for Tokens {
    fn client(&self, token: &str) -> Option<MachineCaller> {
        Some(MachineCaller {
            client: token.strip_prefix("token-")?.to_string(),
            machine: Some("m1".into()),
        })
    }
}

/// What was recorded and issued, in order; refuses to record for `deny`.
#[derive(Default)]
struct Trail(Mutex<Vec<String>>);

#[async_trait::async_trait]
impl IssueLog for Trail {
    async fn issued(
        &self,
        job: Uuid,
        kind: &str,
        machine: &str,
        _ip: &str,
        resource: &str,
    ) -> Result<(), String> {
        if machine.starts_with("deny") {
            return Err("audit down".into());
        }
        self.0
            .lock()
            .unwrap()
            .push(format!("recorded {job} {kind} {machine} {resource}"));
        Ok(())
    }
}

#[async_trait::async_trait]
impl TrinoTokens for Trail {
    async fn issue(
        &self,
        job: Uuid,
        user: TrinoUser,
        dataset: &str,
        actor: &str,
    ) -> Result<TrinoToken, String> {
        let sub = format!("job:{job}:{}:{dataset}", user.as_str());
        self.0
            .lock()
            .unwrap()
            .push(format!("issued {sub} act {actor}"));
        Ok(TrinoToken {
            access_token: format!("jwt-{sub}"),
            expires_in: 900,
        })
    }
}

struct Fixture {
    _db: testkit::TestDb,
    router: Router,
    trail: Arc<Trail>,
}

async fn fixture() -> Fixture {
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
    ] {
        sqlx::query(sql).execute(&db.pool).await.unwrap();
    }
    let app = PgPoolOptions::new()
        .max_connections(4)
        .connect_with(options)
        .await
        .unwrap();
    for (run, kind) in [
        (RUN, "evaluate"),
        ("0b9c6a1e-0000-4000-8000-00000000000a", "extract"),
    ] {
        sqlx::query(
            "INSERT INTO quality.job_bundles (run, kind, dataset, cut, digest, bundle, asked_by)
             VALUES ($1, $2, 'loans', DATE '2026-10-31', 'd', 'b', 'runner/m1')",
        )
        .bind(run)
        .bind(kind)
        .execute(&app)
        .await
        .unwrap();
    }
    let trail = Arc::new(Trail::default());
    let services = RunTokenServices {
        tokens: Arc::new(Tokens),
        issuer: trail.clone(),
        log: trail.clone(),
    };
    let router = QualityModule::default()
        .with_run_tokens(app, services)
        .router();
    Fixture {
        _db: db,
        router,
        trail,
    }
}

impl Fixture {
    async fn ask(&self, run: &str, token: Option<&str>, body: Value) -> (StatusCode, Value) {
        let mut req = Request::post(format!("/internal/quality/v1/runs/{run}/trino-token"))
            .header("content-type", "application/json");
        if let Some(t) = token {
            req = req.header("authorization", format!("Bearer {t}"));
        }
        let res = self
            .router
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

    fn trail(&self) -> Vec<String> {
        self.trail.0.lock().unwrap().clone()
    }
}

#[tokio::test]
async fn a_pinned_runs_job_runner_gets_its_users_token_recorded_first() {
    let f = fixture().await;
    let (status, body) = f
        .ask(RUN, Some("token-runner"), json!({ "user": "rules" }))
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        body,
        json!({ "access_token": format!("jwt-job:{RUN}:rules:loans"), "expires_in": 900 })
    );
    assert_eq!(
        f.trail(),
        [
            format!("recorded {RUN} evaluate runner/m1 trino/job_rules_loans"),
            format!("issued job:{RUN}:rules:loans act runner/m1"),
        ]
    );
    let (status, _) = f
        .ask(RUN, Some("token-runner"), json!({ "user": "report" }))
        .await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn no_bundle_another_kinds_user_an_unknown_caller_or_no_record_gets_nothing() {
    let f = fixture().await;
    let rules = json!({ "user": "rules" });
    let other = "0b9c6a1e-0000-4000-8000-0000000000ff";
    let (status, body) = f.ask(other, Some("token-runner"), rules.clone()).await;
    assert_eq!(
        (status, body),
        (StatusCode::NOT_FOUND, json!({ "error": "no_bundle" }))
    );
    let extract = "0b9c6a1e-0000-4000-8000-00000000000a";
    let (status, body) = f.ask(extract, Some("token-runner"), rules.clone()).await;
    assert_eq!(
        (status, body),
        (
            StatusCode::FORBIDDEN,
            json!({ "error": "user_not_allowed" })
        )
    );
    let (status, _) = f.ask(RUN, None, rules.clone()).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let (status, _) = f.ask("run-1", Some("token-runner"), rules.clone()).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let (status, _) = f
        .ask(RUN, Some("token-runner"), json!({ "user": "catalog" }))
        .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    let (status, _) = f.ask(RUN, Some("token-deny"), rules).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert!(f.trail().is_empty(), "nothing issued: {:?}", f.trail());
}
