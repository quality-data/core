//! The bundle route (decision 0090, point 2) on an ephemeral PostgreSQL,
//! driven without a socket: a job runner's first ask compiles, signs and
//! pins the run's bundle; a retry gets the same one without compiling; the
//! same run for another job, an unknown caller, a malformed run and a
//! compile refusal are refused. Synthetic names only.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::Router;
use contracts::{Module, ModuleRegistry};
use infrastructure::module_migrations::PgModuleMigrator;
use quality::bundle::{self, Bundle, BundleSigner, BundleStatement, Expect, RunAs};
use quality::bundle_api::{BundleServices, CompileRefused, JobCompiler, JobRequest, SignedBundle};
use quality::module::QualityModule;
use quality::worker_api::{MachineCaller, MachineTokens};
use serde_json::{json, Value};
use sqlx::postgres::PgPoolOptions;
use tower::ServiceExt;

/// `token-<client>` belongs to `<client>`.
struct Tokens;
impl MachineTokens for Tokens {
    fn client(&self, token: &str) -> Option<MachineCaller> {
        Some(MachineCaller {
            client: token.strip_prefix("token-")?.to_string(),
            machine: None,
        })
    }
}

/// A stand-in key: the claims behind the `typ`, with no signature to check
/// (the provider's ES256 key is tested where the server wires it).
struct Plain;
impl BundleSigner for Plain {
    fn sign(&self, typ: &str, claims: &str) -> String {
        format!("{typ}|{claims}")
    }
    fn verify(&self, typ: &str, token: &str) -> Option<String> {
        token.strip_prefix(&format!("{typ}|")).map(str::to_string)
    }
}

/// Compiles one `INSERT` per job and counts its compiles; refuses the
/// dataset `broken`.
#[derive(Default)]
struct Compiler(AtomicUsize);
#[async_trait::async_trait]
impl JobCompiler for Compiler {
    async fn compile(&self, run: &str, job: &JobRequest) -> Result<Bundle, CompileRefused> {
        if job.dataset.as_deref() == Some("broken") {
            return Err(CompileRefused("rules_not_approved"));
        }
        let n = self.0.fetch_add(1, Ordering::SeqCst) + 1;
        Ok(Bundle {
            job_id: run.into(),
            kind: job.kind.clone(),
            dataset: job.dataset.clone(),
            cut: job.cut.map(|c| c.to_string()),
            attempt: job.attempt,
            run_id: Some(format!("{run}-r")),
            run_seq: Some(n as u64),
            catalog_commit: Some("c0ffee".into()),
            compiler_version: "test".into(),
            placeholders: vec![],
            statements: vec![BundleStatement {
                id: "rules".into(),
                sql: format!("INSERT INTO t SELECT {n}"),
                expects: Expect::insert(),
                run_as: RunAs::Job,
                reads_source: false,
                only_if_rows: None,
                lock: None,
            }],
        })
    }
}

struct Fixture {
    _db: testkit::TestDb,
    router: Router,
    compiler: Arc<Compiler>,
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
    let compiler = Arc::new(Compiler::default());
    let services = BundleServices {
        tokens: Arc::new(Tokens),
        signer: Arc::new(Plain),
        compiler: compiler.clone(),
    };
    let router = QualityModule::default()
        .with_bundles(app, services)
        .router();
    Fixture {
        _db: db,
        router,
        compiler,
    }
}

impl Fixture {
    async fn ask(&self, run: &str, token: Option<&str>, job: Value) -> (StatusCode, Value) {
        let mut req = Request::post(format!("/internal/quality/v1/runs/{run}/bundle"))
            .header("content-type", "application/json");
        if let Some(t) = token {
            req = req.header("authorization", format!("Bearer {t}"));
        }
        let res = self
            .router
            .clone()
            .oneshot(req.body(Body::from(job.to_string())).unwrap())
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
}

fn evaluate(dataset: &str) -> Value {
    json!({ "kind": "evaluate", "dataset": dataset, "cut": "2026-10-31", "attempt": 1 })
}

#[tokio::test]
async fn a_run_is_compiled_once_and_a_retry_gets_the_same_bundle() {
    let f = fixture().await;
    let (status, first) = f
        .ask("run-1", Some("token-runner"), evaluate("loans"))
        .await;
    assert_eq!(status, StatusCode::OK, "{first}");
    let signed: SignedBundle = serde_json::from_value(first.clone()).unwrap();
    let b = bundle::verify(&Plain, &signed.bundle, "run-1", &signed.digest).unwrap();
    assert_eq!(
        (b.kind.as_str(), b.dataset.as_deref(), b.cut.as_deref()),
        ("evaluate", Some("loans"), Some("2026-10-31"))
    );
    let mut retry = evaluate("loans");
    retry["attempt"] = json!(2);
    let (status, again) = f.ask("run-1", Some("token-other"), retry).await;
    assert_eq!((status, again), (StatusCode::OK, first));
    assert_eq!(f.compiler.0.load(Ordering::SeqCst), 1, "compiled once");
    let (status, _) = f
        .ask("run-2", Some("token-runner"), evaluate("loans"))
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(f.compiler.0.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn another_job_on_a_pinned_run_and_bad_asks_are_refused() {
    let f = fixture().await;
    let (status, _) = f
        .ask("run-1", Some("token-runner"), evaluate("loans"))
        .await;
    assert_eq!(status, StatusCode::OK);
    let (status, body) = f
        .ask("run-1", Some("token-runner"), evaluate("deposits"))
        .await;
    assert_eq!(
        (status, body),
        (
            StatusCode::CONFLICT,
            json!({ "error": "run_has_another_job" })
        )
    );
    let (status, _) = f.ask("run-3", None, evaluate("loans")).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let (status, body) = f
        .ask("run.3", Some("token-runner"), evaluate("loans"))
        .await;
    assert_eq!(
        (status, body),
        (StatusCode::BAD_REQUEST, json!({ "error": "invalid_run" }))
    );
    let (status, body) = f
        .ask("run-4", Some("token-runner"), evaluate("broken"))
        .await;
    assert_eq!(
        (status, body),
        (
            StatusCode::UNPROCESSABLE_ENTITY,
            json!({ "error": "rules_not_approved" })
        )
    );
    let mut unknown = evaluate("loans");
    unknown["sql"] = json!("SELECT 1");
    let (status, _) = f.ask("run-5", Some("token-runner"), unknown).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "unknown fields");
}
