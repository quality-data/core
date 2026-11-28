//! The console (decision 0054), on an ephemeral PostgreSQL, driven without
//! a socket with the `Caller` server would put in each request: a
//! statement is queued as a `query` job and audited as run; what is not
//! one statement is refused before; its result is read only by whoever
//! asked, masked, and revealed with a reason behind
//! `quality:ViewPersonalData` on every dataset (allowed on all, denied on
//! none: decision 0064), each reveal audited with its reason and shape
//! (decision 0069); a worker gets the lake for it and its result of rows
//! too many is refused. Synthetic values only.

pub mod worker;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::{Extension, Router};
use contracts::{
    Access, AuditEntry, AuditTrail, Caller, Change, Module, ModuleRegistry, ViewDetail, ViewPurpose,
};
use infrastructure::module_migrations::PgModuleMigrator;
use quality::module::QualityModule;
use quality::source_store::Sealer;
use serde_json::{json, Value};
use sqlx::postgres::PgPoolOptions;
use tower::ServiceExt;
use uuid::Uuid;

const ANA: &str = "00000000-0000-0000-0000-0000000000a1";
const BEN: &str = "00000000-0000-0000-0000-0000000000b2";
const MAIN: &str = "00000000-0000-0000-0000-000000000001";

struct Plain;
impl Sealer for Plain {
    fn seal(&self, plain: &str) -> Result<String, String> {
        Ok(plain.to_string())
    }
    fn open(&self, sealed: &str) -> Result<String, String> {
        Ok(sealed.to_string())
    }
}

/// May query the lake if `query`; sees personal data on `pipeline/*` if
/// `personal`, except on `pipeline/credits` if `credits_denied`.
struct Querier {
    query: bool,
    personal: bool,
    credits_denied: bool,
}
impl Access for Querier {
    fn can(&self, action: &str, resource: &str) -> bool {
        match (action, resource.strip_prefix("pipeline/")) {
            ("quality:QueryData", _) => resource == "*" && self.query,
            ("quality:ViewPersonalData", Some(d)) => {
                self.personal && !(self.credits_denied && d == "credits")
            }
            _ => false,
        }
    }
    fn can_some(&self, action: &str) -> bool {
        self.can(action, "*")
    }
    fn can_every(&self, action: &str, kind: &str) -> bool {
        (action, kind) == ("quality:ViewPersonalData", "pipeline")
            && self.personal
            && !self.credits_denied
    }
}

/// The audit: entries, views with their detail, and whether it refuses.
#[derive(Default)]
struct Log(Mutex<Vec<AuditEntry>>, Mutex<Vec<ViewDetail>>, AtomicBool);
#[async_trait::async_trait]
impl AuditTrail for Log {
    async fn record(&self, _: &Caller, entry: &AuditEntry) -> Result<(), String> {
        self.0.lock().unwrap().push(entry.clone());
        Ok(())
    }
    async fn record_view(
        &self,
        _: &Caller,
        entry: &AuditEntry,
        detail: &ViewDetail,
    ) -> Result<(), String> {
        if self.2.load(Ordering::SeqCst) {
            return Err("the log is down".into());
        }
        self.0.lock().unwrap().push(entry.clone());
        self.1.lock().unwrap().push(detail.clone());
        Ok(())
    }
}

/// May query the lake; sees personal data on every dataset if `personal`.
fn querier(personal: bool) -> Querier {
    Querier {
        query: true,
        personal,
        credits_denied: false,
    }
}

struct Fixture {
    db: testkit::TestDb,
    router: Router,
    log: Arc<Log>,
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
        "ALTER TABLE quality.jobs OWNER TO app",
        "ALTER TABLE quality.console_results OWNER TO app",
        "ALTER TABLE quality.cuts OWNER TO app",
    ] {
        sqlx::query(sql).execute(&db.pool).await.unwrap();
    }
    let app = PgPoolOptions::new()
        .max_connections(4)
        .connect_with(options)
        .await
        .unwrap();
    let router = QualityModule::default()
        .with_sources(app, Arc::new(Plain), None)
        .user_router();
    Fixture {
        db,
        router,
        log: Arc::default(),
    }
}

impl Fixture {
    async fn call(
        &self,
        who: &str,
        may: Querier,
        method: &str,
        uri: &str,
        body: Value,
    ) -> (StatusCode, Value) {
        let caller = Caller::new(
            who.into(),
            None,
            "192.0.2.1".into(),
            None,
            Arc::new(may),
            self.log.clone(),
        );
        let req = Request::builder()
            .method(method)
            .uri(uri)
            .header("content-type", "application/json")
            .body(Body::from(body.to_string()))
            .unwrap();
        let res = self
            .router
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

    async fn ask(&self, sql: &str) -> (StatusCode, Value) {
        let may = Querier {
            query: true,
            personal: false,
            credits_denied: false,
        };
        self.call(ANA, may, "POST", "/quality/console", json!({ "sql": sql }))
            .await
    }

    /// The worker's part, as its report would leave it.
    async fn ran(&self, job: &str, result: Value) {
        let job = Uuid::parse_str(job).unwrap();
        sqlx::query("UPDATE quality.jobs SET state = 'done' WHERE id = $1")
            .bind(job)
            .execute(&self.db.pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO quality.console_results (tenant_id, job, result) VALUES ($1::uuid, $2, $3)")
            .bind(MAIN)
            .bind(job)
            .bind(result)
            .execute(&self.db.pool)
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn a_statement_is_queued_audited_and_what_is_not_one_is_refused() {
    let f = fixture().await;
    let sql = "SELECT count(*) AS n FROM lake.loans_bronze.loans";
    let (status, out) = f.ask(sql).await;
    assert_eq!(status, StatusCode::ACCEPTED, "{out}");
    let job = out["job"].as_str().unwrap().to_string();
    let (kind, parameters): (String, Value) =
        sqlx::query_as("SELECT kind, parameters FROM quality.jobs WHERE id = $1::uuid")
            .bind(&job)
            .fetch_one(&f.db.pool)
            .await
            .unwrap();
    assert_eq!(kind, "query");
    assert_eq!(
        (parameters["sql"].as_str(), parameters["by"].as_str()),
        (Some(sql), Some(ANA))
    );
    {
        let log = f.log.0.lock().unwrap();
        assert_eq!(log.len(), 1);
        assert_eq!(
            (log[0].action, log[0].change),
            ("quality:QueryData", Change::Ran)
        );
        assert_eq!(log[0].resource, format!("lake/console/{job}"));
    }

    for sql in ["  ", "SELECT 1; SELECT 2"] {
        let (status, out) = f.ask(sql).await;
        assert_eq!(
            (status, out["error"].as_str()),
            (StatusCode::UNPROCESSABLE_ENTITY, Some("statement"))
        );
    }
    let may = Querier {
        query: false,
        personal: true,
        credits_denied: false,
    };
    let (status, _) = f
        .call(
            ANA,
            may,
            "POST",
            "/quality/console",
            json!({ "sql": "SELECT 1" }),
        )
        .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn a_result_is_read_masked_by_everyone_and_audits_nothing() {
    let f = fixture().await;
    let (_, out) = f.ask("SELECT id, doc FROM lake.loans_bronze.loans").await;
    let job = out["job"].as_str().unwrap().to_string();
    let uri = format!("/quality/console/{job}");
    let (status, out) = f.call(ANA, querier(true), "GET", &uri, Value::Null).await;
    assert_eq!(
        (status, out["state"].as_str()),
        (StatusCode::OK, Some("queued"))
    );
    assert_eq!(out["revealable"], false, "nothing to show yet");

    f.ran(&job, json!({ "columns": ["id", "doc"], "rows": [["1", "100200300"], ["2", null]], "more": true, "error": null })).await;
    let before = f.log.0.lock().unwrap().len();
    for personal in [false, true, true] {
        let (_, out) = f
            .call(ANA, querier(personal), "GET", &uri, Value::Null)
            .await;
        assert_eq!(
            (&out["masked"], &out["revealable"]),
            (&json!(true), &json!(personal))
        );
        assert_eq!(out["result"]["rows"][0], json!(["••••", "••••0300"]));
        assert!(out["result"]["rows"][1][1].is_null(), "empty stays empty");
        assert_eq!(out["result"]["more"], true);
    }
    assert_eq!(
        f.log.0.lock().unwrap().len(),
        before,
        "polling writes no row"
    );
    let (status, _) = f.call(BEN, querier(true), "GET", &uri, Value::Null).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "only whoever asked");

    // Allowed on `pipeline/*` but denied on one dataset: not every dataset.
    let one_denied = || Querier {
        query: true,
        personal: true,
        credits_denied: true,
    };
    let (_, out) = f.call(ANA, one_denied(), "GET", &uri, Value::Null).await;
    assert_eq!(out["revealable"], false);
    let reveal = format!("{uri}/reveal");
    let reason = json!({ "purpose": "audit" });
    let (status, _) = f.call(ANA, one_denied(), "POST", &reveal, reason).await;
    assert_eq!(status, StatusCode::FORBIDDEN);

    // A statement that failed: of the engine's words, only their kind.
    let (_, out) = f
        .ask("SELECT CAST(doc AS INT) FROM lake.loans_bronze.loans")
        .await;
    let job = out["job"].as_str().unwrap().to_string();
    let message = "Conversion Error: Could not convert string '100200300x' to INT32";
    f.ran(
        &job,
        json!({ "columns": [], "rows": [], "more": false, "error": message }),
    )
    .await;
    let uri = format!("/quality/console/{job}");
    let (_, out) = f.call(ANA, querier(true), "GET", &uri, Value::Null).await;
    assert_eq!(
        (&out["result"]["error"], &out["revealable"]),
        (&json!("conversion"), &json!(true))
    );
    let reason = json!({ "purpose": "quality_review" });
    let (_, out) = f
        .call(ANA, querier(true), "POST", &format!("{uri}/reveal"), reason)
        .await;
    assert_eq!(out["result"]["error"], message, "whole on a reveal");
}

#[tokio::test]
async fn a_reveal_records_a_free_text_alias_by_its_position() {
    let f = fixture().await;
    let (_, out) = f
        .ask("SELECT id, doc AS \"the doc\", id FROM lake.loans_bronze.loans")
        .await;
    let job = out["job"].as_str().unwrap().to_string();
    let reveal = format!("/quality/console/{job}/reveal");
    f.ran(&job, json!({ "columns": ["id", "the doc", "id"], "rows": [["1", "100200300", "1"]], "more": false, "error": null })).await;
    let before = f.log.0.lock().unwrap().len();

    let reason = json!({ "purpose": "other", "reference": "T-48213" });
    let (status, out) = f
        .call(ANA, querier(true), "POST", &reveal, reason.clone())
        .await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert_eq!(
        (&out["masked"], &out["result"]["rows"][0][1]),
        (&json!(false), &json!("100200300"))
    );
    {
        let log = f.log.0.lock().unwrap();
        assert_eq!(log.len(), before + 1);
        let last = log.last().unwrap();
        assert_eq!(
            (last.action, last.change),
            ("quality:ViewPersonalData", Change::Viewed)
        );
        assert_eq!(last.resource, format!("lake/console/{job}"));
        let view = &f.log.1.lock().unwrap()[0];
        assert_eq!(view.reason.purpose(), ViewPurpose::Other);
        assert_eq!((view.job.as_str(), view.rows_shown), (job.as_str(), 1));
        assert_eq!(
            view.fields_shown,
            ["id", "#2"],
            "an alias by position, a repeat once"
        );
    }
    let (status, _) = f.call(ANA, querier(true), "POST", &reveal, reason).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(f.log.1.lock().unwrap().len(), 2, "a second showing");
}

#[tokio::test]
async fn a_refused_reveal_writes_no_row_and_shows_nothing() {
    let f = fixture().await;
    let (_, out) = f.ask("SELECT id, doc FROM lake.loans_bronze.loans").await;
    let job = out["job"].as_str().unwrap().to_string();
    let reveal = format!("/quality/console/{job}/reveal");
    let fine = json!({ "purpose": "remediation" });
    let before = f.log.0.lock().unwrap().len();
    let refused = |(status, out): (StatusCode, Value)| (status, out["error"].clone());

    let not_done = f
        .call(ANA, querier(true), "POST", &reveal, fine.clone())
        .await;
    assert_eq!(refused(not_done), (StatusCode::CONFLICT, json!("not_done")));
    f.ran(
        &job,
        json!({ "columns": ["id"], "rows": [["1"]], "more": false, "error": null }),
    )
    .await;
    for (reason, code) in [
        (json!({ "purpose": "curiosity" }), "purpose"),
        (
            json!({ "purpose": "audit", "reference": "12345678" }),
            "reference",
        ),
        (json!({ "purpose": "other" }), "reference"),
    ] {
        let out = refused(f.call(ANA, querier(true), "POST", &reveal, reason).await);
        assert_eq!(out, (StatusCode::UNPROCESSABLE_ENTITY, json!(code)));
    }
    let out = refused(
        f.call(ANA, querier(false), "POST", &reveal, fine.clone())
            .await,
    );
    assert_eq!(out, (StatusCode::FORBIDDEN, json!("forbidden")));
    let theirs = f
        .call(BEN, querier(true), "POST", &reveal, fine.clone())
        .await;
    assert_eq!(theirs.0, StatusCode::NOT_FOUND, "another person's");
    f.log.2.store(true, Ordering::SeqCst);
    let (status, out) = f.call(ANA, querier(true), "POST", &reveal, fine).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(out, json!({ "error": "audit_unavailable" }), "no values");
    assert_eq!(
        f.log.0.lock().unwrap().len(),
        before,
        "no row for a refusal"
    );
}

#[tokio::test]
async fn a_worker_gets_the_lake_and_a_result_too_long_is_refused() {
    let f = worker::fixture().await;
    let job: Uuid = sqlx::query_scalar(
        "INSERT INTO quality.jobs (tenant_id, kind, parameters, state, worker, lease_until)
         VALUES ($1::uuid, 'query', '{\"sql\": \"SELECT 1\"}', 'running', 'w1/0',
                 now() + interval '1 minute') RETURNING id",
    )
    .bind(worker::B)
    .fetch_one(&f.db.pool)
    .await
    .unwrap();
    let uri = format!("/internal/jobs/{}/{job}", worker::B);
    let (status, body) = f.call("GET", &uri, "token-w1", None).await;
    assert_eq!(status, StatusCode::OK);
    let details: quality::worker_api::JobDetails = serde_json::from_slice(&body).unwrap();
    assert!(
        details.lake.is_some(),
        "the lake, read as an evaluation reads it"
    );

    let report = format!("{uri}/report");
    let rows = |n: usize| json!({ "columns": ["n"], "rows": vec![vec!["1"]; n], "more": false, "error": null });
    assert_eq!(
        f.call("POST", &report, "token-w1", Some(rows(201))).await.0,
        StatusCode::UNPROCESSABLE_ENTITY
    );
    assert_eq!(
        f.call("POST", &report, "token-w1", Some(rows(2))).await.0,
        StatusCode::NO_CONTENT
    );
}
