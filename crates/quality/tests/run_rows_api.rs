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
use quality::job_map::Drafts;
use quality::module::QualityModule;
use quality::proposal::Alerts;
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
        lock: None,
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
    fixture_with(b, None, None).await
}

/// As [`fixture`], a map's proposal published to `drafts`.
async fn fixture_with(
    b: &Bundle,
    drafts: Option<Arc<dyn Drafts>>,
    catalog: Option<Arc<dyn quality::catalog::Catalog>>,
) -> (testkit::TestDb, sqlx::PgPool, Router) {
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
                proposal_alerts: drafts.as_ref().map(|_| alerts()),
                drafts,
                catalog,
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

/// Ruling 79: a `records` run's page, posted as values, is kept as its
/// job's page, masked on a read as a worker's is.
#[tokio::test]
async fn a_records_runs_page_is_kept_for_its_job() {
    let mut b = bundle("records");
    b.statements.truncate(1);
    let (db, app, router) = fixture(&b).await;
    for sql in [
        "ALTER TABLE quality.jobs OWNER TO app",
        "ALTER TABLE quality.sources OWNER TO app",
        "ALTER TABLE quality.source_tables OWNER TO app",
        "ALTER TABLE quality.source_records OWNER TO app",
    ] {
        sqlx::query(sql).execute(&db.pool).await.unwrap();
    }
    sqlx::query(
        "INSERT INTO quality.sources (name, driver, secrets) VALUES ('core', 'postgres', 'x')",
    )
    .execute(&app)
    .await
    .unwrap();
    let tables = json!([{ "schema": "public", "name": "loans", "columns": [
        { "name": "name", "data_type": "varchar" },
        { "name": "amount", "data_type": "decimal(12,2)" }]}]);
    sqlx::query(
        "INSERT INTO quality.source_tables (source, job, asked_at, tables)
         VALUES ('core', gen_random_uuid(), now(), $1)",
    )
    .bind(&tables)
    .execute(&app)
    .await
    .unwrap();
    let parameters = json!({ "table": "public.loans", "order": "amount", "descending": true,
        "page": 0, "source": "core", "by": "someone" });
    sqlx::query("INSERT INTO quality.jobs (id, kind, parameters) VALUES ($1::uuid, 'records', $2)")
        .bind(RUN)
        .bind(&parameters)
        .execute(&app)
        .await
        .unwrap();

    let job = quality::records_run::job_of(&app, RUN).await.unwrap();
    assert_eq!(job.source, "core");
    assert_eq!(job.columns, ["name", "amount"]);
    assert_eq!(job.ask.order.as_deref(), Some("amount"));

    let rows = json!({ "statement": "page", "rows": [["Name Example", "10.00"], [null, "2.50"]] });
    assert_eq!(
        post(&router, "rows", Some("token-runner"), &rows).await.0,
        StatusCode::NO_CONTENT
    );
    let page: Value =
        sqlx::query_scalar("SELECT page FROM quality.source_records WHERE job = $1::uuid")
            .bind(RUN)
            .fetch_one(&app)
            .await
            .unwrap();
    assert_eq!(
        page,
        json!({ "source": "core", "table": "public.loans", "columns": ["name", "amount"],
            "rows": [["Name Example", "10.00"], [null, "2.50"]], "more": false })
    );

    let gone = json!({ "table": "public.gone", "source": "core" });
    sqlx::query("UPDATE quality.jobs SET parameters = $1 WHERE id = $2::uuid")
        .bind(&gone)
        .bind(RUN)
        .execute(&app)
        .await
        .unwrap();
    assert!(matches!(
        quality::records_run::job_of(&app, RUN).await,
        Err(quality::job_inputs::FactsError::Refused("table_not_listed"))
    ));
    assert!(matches!(
        quality::records_run::job_of(&app, "0b9c6a1e-0000-4000-8000-000000000099").await,
        Err(quality::job_inputs::FactsError::Refused("no_job"))
    ));
}

fn alerts() -> Alerts {
    Alerts {
        required: "required".into(),
        unique: "unique".into(),
        email: "email".into(),
        mobile: "mobile".into(),
        document: "document".into(),
    }
}

/// What a map run published.
#[derive(Default)]
struct Published(std::sync::Mutex<Vec<(String, String)>>);

#[async_trait::async_trait]
impl Drafts for Published {
    async fn publish(
        &self,
        dataset: &str,
        mapping: &str,
        _rules: &[(String, String)],
    ) -> Result<Option<String>, String> {
        let mut kept = self.0.lock().unwrap();
        kept.push((dataset.to_owned(), mapping.to_owned()));
        Ok(Some("c1".into()))
    }
}

/// Ruling 79: a `map` run's row of counts becomes the table's profile, its
/// dataset's proposal is published and the dataset's first cut opened.
#[tokio::test]
async fn a_map_runs_profile_becomes_its_datasets_proposal() {
    let mut b = bundle("map");
    let counts = |n: usize| -> Vec<(String, String)> {
        (0..n)
            .map(|i| (format!("x{i}"), "bigint".to_owned()))
            .collect()
    };
    b.statements = vec![statement(
        "profile",
        Expect::Values {
            columns: counts(21),
            max_rows: 1,
        },
        RunAs::Job,
    )];
    let published = Arc::new(Published::default());
    let (db, app, router) = fixture_with(&b, Some(published.clone()), None).await;
    for sql in [
        "ALTER TABLE quality.jobs OWNER TO app",
        "ALTER TABLE quality.sources OWNER TO app",
        "ALTER TABLE quality.source_tables OWNER TO app",
        "ALTER TABLE quality.cuts OWNER TO app",
    ] {
        sqlx::query(sql).execute(&db.pool).await.unwrap();
    }
    sqlx::query(
        "INSERT INTO quality.sources (name, driver, secrets) VALUES ('core', 'postgres', 'x')",
    )
    .execute(&app)
    .await
    .unwrap();
    let tables = json!([{ "schema": "public", "name": "clients", "columns": [
        { "name": "client_id", "data_type": "integer" },
        { "name": "email", "data_type": "varchar" }]}]);
    sqlx::query(
        "INSERT INTO quality.source_tables (source, job, asked_at, tables)
         VALUES ('core', gen_random_uuid(), now(), $1)",
    )
    .bind(&tables)
    .execute(&app)
    .await
    .unwrap();
    let parameters = json!({ "source": "core", "table": "clients", "sample": 40 });
    sqlx::query(
        "INSERT INTO quality.jobs (id, kind, dataset, parameters) VALUES ($1::uuid, 'map', 'clients', $2)",
    )
    .bind(RUN)
    .bind(&parameters)
    .execute(&app)
    .await
    .unwrap();

    let job = quality::map_run::job_of(&app, RUN).await.unwrap();
    assert_eq!(
        (job.schema.as_str(), job.name.as_str(), job.sample),
        ("public", "clients", 40)
    );
    assert_eq!(job.columns.len(), 2);

    // sampled, then per column: empty, distinct, lengths, six patterns.
    let row = json!([[40, 0, 40, 1, 2, 0, 0, 40, 0, 40, 0, 0, 40, 17, 22, 40, 0, 0, 0, 0, 0]]);
    let body = json!({ "statement": "profile", "rows": row });
    assert_eq!(
        post(&router, "rows", Some("token-runner"), &body).await.0,
        StatusCode::NO_CONTENT
    );
    let kept = published.0.lock().unwrap().clone();
    assert_eq!(kept.len(), 1);
    assert_eq!(kept[0].0, "clients");
    assert!(kept[0].1.contains("email"));
    let cuts: i64 =
        sqlx::query_scalar("SELECT count(*) FROM quality.cuts WHERE dataset = 'clients'")
            .fetch_one(&app)
            .await
            .unwrap();
    assert_eq!(cuts, 1);
}

/// Ruling 83: each rule's read of a `test_rule` run is merged into the
/// test's report, kept for its asker.
#[tokio::test]
async fn a_test_rule_runs_reads_merge_into_its_report() {
    let mut b = bundle("test_rule");
    b.cut = Some("2026-10-31".into());
    let columns: Vec<(String, String)> = [
        ("part", "varchar"),
        ("rule", "varchar"),
        ("outcome", "varchar"),
        ("shape", "varchar"),
        ("n", "bigint"),
        ("c0", "varchar"),
    ]
    .iter()
    .map(|(n, t)| ((*n).to_owned(), (*t).to_owned()))
    .collect();
    let read = |id: &str, columns: &Vec<(String, String)>| {
        statement(
            id,
            Expect::Values {
                columns: columns.clone(),
                max_rows: 234,
            },
            RunAs::Job,
        )
    };
    b.statements = vec![read("rule_0", &columns), read("rule_1", &columns)];
    let (db, app, router) = fixture(&b).await;
    for sql in [
        "ALTER TABLE quality.rule_tests OWNER TO app",
        "ALTER TABLE quality.jobs OWNER TO app",
    ] {
        sqlx::query(sql).execute(&db.pool).await.unwrap();
    }
    sqlx::query(
        "INSERT INTO quality.jobs (id, kind, dataset, parameters) VALUES ($1::uuid, 'test_rule', 'loans', '{}')",
    )
    .bind(RUN)
    .execute(&app)
    .await
    .unwrap();
    let runner = Some("token-runner");
    let first = json!({ "statement": "rule_0", "rows": [
        ["fields", "R1", null, null, null, "Id"],
        ["counts", "R1", "fails", null, 1, null],
        ["counts", "R1", "passes", null, 3, null],
        ["record", "R1", "fails", null, null, "Synthetic 1"],
        ["group", "R1", "fails", "Aaaaa 9", 1, "Synthetic 1"]
    ]});
    let second = json!({ "statement": "rule_1", "rows": [
        ["fields", "R2", null, null, null, "Id"],
        ["counts", "R2", "skips", null, 4, null]
    ]});
    for body in [&first, &second, &first] {
        assert_eq!(
            post(&router, "rows", runner, body).await.0,
            StatusCode::NO_CONTENT
        );
    }
    let report: Value =
        sqlx::query_scalar("SELECT report FROM quality.rule_tests WHERE job = $1::uuid")
            .bind(RUN)
            .fetch_one(&app)
            .await
            .unwrap();
    assert_eq!(report["cut"], "2026-10-31");
    assert_eq!(report["records"], 4);
    let rules = report["rules"].as_array().unwrap();
    assert_eq!(rules.len(), 2, "a repeated read replaces its rule");
    assert_eq!(rules[0]["rule"], "R1");
    assert_eq!(rules[0]["applies"], 4);
    assert_eq!(rules[0]["groups"][0]["count"], 1);
    assert_eq!(rules[1]["skips"], 4);
    let bad = json!({ "statement": "rule_0", "rows": [["counts", "R1", "fails", null, 1, null]] });
    assert_eq!(
        post(&router, "rows", runner, &bad).await,
        refusal("rows_invalid")
    );
}

/// Ruling 84: a console run's text read comes with its columns; its result
/// is kept as today's, and a failed run keeps only its error's class.
#[tokio::test]
async fn a_console_runs_result_comes_with_its_columns() {
    let mut b = bundle("query");
    b.statements = vec![statement(
        "result",
        Expect::Text { max_rows: 201 },
        RunAs::Job,
    )];
    let (db, app, router) = fixture(&b).await;
    for sql in [
        "ALTER TABLE quality.jobs OWNER TO app",
        "ALTER TABLE quality.console_results OWNER TO app",
    ] {
        sqlx::query(sql).execute(&db.pool).await.unwrap();
    }
    sqlx::query("INSERT INTO quality.jobs (id, kind, parameters) VALUES ($1::uuid, 'query', $2)")
        .bind(RUN)
        .bind(json!({ "sql": "SELECT 1", "by": "someone", "datasets": ["loans"] }))
        .execute(&app)
        .await
        .unwrap();
    let job = quality::console_run::job_of(&app, RUN).await.unwrap();
    assert_eq!(job.datasets, ["loans"]);
    assert!(!format!("{job:?}").contains("SELECT"));
    let runner = Some("token-runner");
    let no_columns = json!({ "statement": "result", "rows": [["1"]] });
    assert_eq!(
        post(&router, "rows", runner, &no_columns).await,
        refusal("rows_not_allowed")
    );
    let wide = json!({ "statement": "result", "columns": ["n"], "rows": [["1", "2"]] });
    assert_eq!(
        post(&router, "rows", runner, &wide).await,
        refusal("rows_not_allowed")
    );
    let good = json!({ "statement": "result", "columns": ["name", "n"],
        "rows": [["Name Example", "1"], [null, "2"]] });
    assert_eq!(
        post(&router, "rows", runner, &good).await.0,
        StatusCode::NO_CONTENT
    );
    let kept: Value =
        sqlx::query_scalar("SELECT result FROM quality.console_results WHERE job = $1::uuid")
            .bind(RUN)
            .fetch_one(&app)
            .await
            .unwrap();
    assert_eq!(
        kept,
        json!({ "columns": ["name", "n"], "rows": [["Name Example", "1"], [null, "2"]],
            "more": false, "error": null })
    );
    let report = json!({ "digest": b.digest(), "state": "failed",
        "statements": [{ "id": "result", "state": "failed" }], "error_class": "syntax" });
    assert_eq!(
        post(&router, "report", runner, &report).await.0,
        StatusCode::NO_CONTENT
    );
    let kept: Value = sqlx::query_scalar(
        "SELECT result->'error' FROM quality.console_results WHERE job = $1::uuid",
    )
    .bind(RUN)
    .fetch_one(&app)
    .await
    .unwrap();
    assert_eq!(kept, json!("syntax"));
}

struct Main;

#[async_trait::async_trait]
impl quality::catalog::Catalog for Main {
    async fn snapshot(
        &self,
        dataset: &str,
    ) -> Result<Option<quality::catalog::CatalogSnapshot>, String> {
        Ok(
            (dataset == "loans").then(|| quality::catalog::CatalogSnapshot {
                commit: "c1".into(),
                mapping:
                    "dataset: loans\nsource: loans\nfields:\n  Code: { column: code, type: text }\n"
                        .into(),
                rules: vec![],
                others: vec![],
            }),
        )
    }
    async fn mappings(&self) -> Result<Vec<quality::catalog::DatasetMapping>, String> {
        Ok(vec![])
    }
}

/// Ruling 92(a): the orchestrator lists the schedules and asks a fire's
/// steps; the same fire gets the same answer, a blocked cut none.
#[tokio::test]
async fn a_schedules_fire_answers_its_chain_once() {
    let b = bundle("evaluate");
    let (db, app, router) = fixture_with(&b, None, Some(Arc::new(Main))).await;
    for sql in [
        "ALTER TABLE quality.schedules OWNER TO app",
        "ALTER TABLE quality.schedule_fires OWNER TO app",
        "ALTER TABLE quality.cuts OWNER TO app",
        "ALTER TABLE quality.jobs OWNER TO app",
    ] {
        sqlx::query(sql).execute(&db.pool).await.unwrap();
    }
    let id: uuid::Uuid = sqlx::query_scalar(
        "INSERT INTO quality.schedules (cron, timezone, fires, dataset, rerun)
         VALUES ('0 2 * * *', 'UTC', 'cut', 'loans', 'block') RETURNING id",
    )
    .fetch_one(&app)
    .await
    .unwrap();
    assert_listed(&router, id).await;
    let fire = |at: &str| {
        let body = json!({ "fire_at": at }).to_string();
        Request::post(format!("/internal/quality/v1/schedules/{id}/fires"))
            .header("content-type", "application/json")
            .header("authorization", "Bearer token-orchestrator")
            .body(Body::from(body))
            .unwrap()
    };
    let answer = |res: axum::response::Response| async move {
        let status = res.status();
        let bytes = axum::body::to_bytes(res.into_body(), usize::MAX)
            .await
            .unwrap();
        (
            status,
            serde_json::from_slice::<Value>(&bytes).unwrap_or(Value::Null),
        )
    };
    let (status, first) = answer(
        router
            .clone()
            .oneshot(fire("2026-10-31T02:00:00Z"))
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let steps = first["steps"].as_array().unwrap();
    let kinds: Vec<&str> = steps.iter().map(|s| s["kind"].as_str().unwrap()).collect();
    assert_eq!(kinds, ["prepare", "extract", "evaluate"]);
    assert_eq!(steps[0]["setup"]["key"], "dataset:loans");
    assert_eq!(
        steps[2]["parameters"],
        json!({ "dataset": "loans", "cut": "2026-10-31" })
    );
    let (_, again) = answer(
        router
            .clone()
            .oneshot(fire("2026-10-31T02:00:00Z"))
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(again, first, "the same fire, the same answer");
    // Another fire of the same day: the cut is there, the schedule blocks.
    let (_, blocked) = answer(
        router
            .clone()
            .oneshot(fire("2026-10-31T03:00:00Z"))
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(blocked, json!({ "steps": [] }));
    let gone = Request::post(format!(
        "/internal/quality/v1/schedules/{}/fires",
        uuid::Uuid::nil()
    ))
    .header("content-type", "application/json")
    .header("authorization", "Bearer token-orchestrator")
    .body(Body::from(
        json!({ "fire_at": "2026-10-31T02:00:00Z" }).to_string(),
    ))
    .unwrap();
    assert_eq!(
        router.clone().oneshot(gone).await.unwrap().status(),
        StatusCode::NOT_FOUND
    );
}

/// The schedules as the orchestrator lists them: `id` alone.
async fn assert_listed(router: &Router, id: uuid::Uuid) {
    let get = |token: &'static str| {
        Request::get("/internal/quality/v1/schedules")
            .header("authorization", format!("Bearer {token}"))
            .body(Body::empty())
            .unwrap()
    };
    let res = router
        .clone()
        .oneshot(get("token-orchestrator"))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap();
    let listed: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(
        listed,
        json!({ "schedules": [{ "id": id, "cron": "0 2 * * *", "timezone": "UTC", "enabled": true }] })
    );
}
