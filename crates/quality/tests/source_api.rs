//! The sources' routes for people on an ephemeral PostgreSQL, driven without
//! a socket, with the `Caller` server would put in each request (decision
//! 0036): what a caller may do is its own, every change is audited (secrets
//! as changed), no secret comes back, and a source a mapping reads is not
//! deleted. Synthetic values only.

use std::sync::{Arc, Mutex};

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::{Extension, Router};
use contracts::{Access, AuditEntry, AuditTrail, Caller, Change, Module, ModuleRegistry};
use infrastructure::module_migrations::PgModuleMigrator;
use quality::catalog::{Catalog, CatalogSnapshot, DatasetMapping};
use quality::module::QualityModule;
use quality::source_store::Sealer;
use serde_json::{json, Value};
use sqlx::postgres::PgPoolOptions;
use tower::ServiceExt;
use uuid::Uuid;

/// Reverses the text behind a prefix.
struct Mirror;
impl Sealer for Mirror {
    fn seal(&self, plain: &str) -> Result<String, String> {
        Ok(format!(
            "sealed:{}",
            plain.chars().rev().collect::<String>()
        ))
    }
    fn open(&self, sealed: &str) -> Result<String, String> {
        let body = sealed.strip_prefix("sealed:").ok_or("not sealed")?;
        Ok(body.chars().rev().collect())
    }
}

/// May view every source, manage only `core` and map into `loans` and
/// `held`; or nothing at all.
struct Steward(bool);
impl Access for Steward {
    fn can(&self, action: &str, resource: &str) -> bool {
        self.0
            && (action == "quality:ViewSources"
                || (action == "quality:ManageSources" && resource == "source/core")
                || (action == "quality:ManageDatasets"
                    && (resource == "dataset/loans" || resource == "dataset/held")))
    }
    fn can_some(&self, action: &str) -> bool {
        self.can(action, "source/core")
    }
}

#[derive(Default)]
struct Log(Mutex<Vec<AuditEntry>>);
#[async_trait::async_trait]
impl AuditTrail for Log {
    async fn record(&self, _: &Caller, entry: &AuditEntry) -> Result<(), String> {
        self.0.lock().unwrap().push(entry.clone());
        Ok(())
    }
}

/// The catalog's `main`: these mappings, for the main tenant only.
struct Mappings(Vec<DatasetMapping>);
#[async_trait::async_trait]
impl Catalog for Mappings {
    async fn snapshot(&self, _: Uuid, dataset: &str) -> Result<Option<CatalogSnapshot>, String> {
        Ok(self
            .0
            .iter()
            .find(|m| m.dataset == dataset)
            .map(|m| CatalogSnapshot {
                commit: "main".into(),
                mapping: m.yaml.clone(),
                rules: Vec::new(),
                others: Vec::new(),
            }))
    }
    async fn mappings(&self, tenant: Uuid) -> Result<Vec<DatasetMapping>, String> {
        assert_eq!(tenant.to_string(), tenancy::MAIN_ID, "the request's tenant");
        Ok(self.0.clone())
    }
}

/// A mapping of `dataset`, reading `from` if given.
fn mapping(dataset: &str, from: Option<&str>) -> DatasetMapping {
    let from = from.map_or(String::new(), |s| {
        format!("from: {{ source: {s}, table: dbo.{dataset} }}\n")
    });
    let yaml = format!(
        "dataset: {dataset}\nsource: {dataset}\n{from}key: [id]\nfields:\n  id: {{ column: ID, type: text }}\n"
    );
    DatasetMapping {
        dataset: dataset.to_string(),
        yaml,
    }
}

struct Fixture {
    _db: testkit::TestDb,
    router: Router,
    log: Arc<Log>,
}

async fn fixture(catalog: Option<Arc<dyn Catalog>>) -> Fixture {
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
        "ALTER TABLE quality.source_tables OWNER TO app",
        "ALTER TABLE quality.jobs OWNER TO app",
        "ALTER TABLE quality.cuts OWNER TO app",
        "GRANT SELECT ON quality.run_summary TO app",
    ] {
        sqlx::query(sql).execute(&db.pool).await.unwrap();
    }
    let app = infrastructure::db::with_tenancy(PgPoolOptions::new(), tenancy::Mode::Single)
        .max_connections(4)
        .connect_with(options)
        .await
        .unwrap();
    let router = QualityModule::default()
        .with_sources(app, Arc::new(Mirror), catalog)
        .user_router();
    Fixture {
        _db: db,
        router,
        log: Arc::new(Log::default()),
    }
}

impl Fixture {
    async fn call(
        &self,
        may: bool,
        method: &str,
        uri: &str,
        body: Option<Value>,
    ) -> (StatusCode, Value) {
        let caller = Caller::new(
            "00000000-0000-0000-0000-0000000000a1".into(),
            None,
            "192.0.2.1".into(),
            None,
            Arc::new(Steward(may)),
            self.log.clone(),
        );
        let mut req = Request::builder().method(method).uri(uri);
        if body.is_some() {
            req = req.header("content-type", "application/json");
        }
        let body = body.map_or_else(Body::empty, |b| Body::from(b.to_string()));
        let res = self
            .router
            .clone()
            .layer(Extension(caller))
            .oneshot(req.body(body).unwrap())
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

    fn changes(&self) -> Vec<(String, Change)> {
        let log = self.log.0.lock().unwrap();
        log.iter().map(|e| (e.resource.clone(), e.change)).collect()
    }
}

fn core(password: Option<&str>) -> Value {
    let mut secrets = json!({ "user": "reader" });
    if let Some(p) = password {
        secrets["password"] = json!(p);
    }
    json!({
        "driver": "postgres",
        "fields": { "host": "db.example", "database": "core" },
        "secrets": secrets,
    })
}

#[tokio::test]
async fn a_steward_creates_and_edits_a_source_audited_and_never_sees_its_secrets() {
    let f = fixture(None).await;
    let uri = "/quality/sources/core";
    assert_eq!(
        f.call(true, "PUT", uri, Some(core(Some("first-synthetic"))))
            .await
            .0,
        StatusCode::CREATED
    );
    // The same fields, the secrets left out: kept, recorded as changed.
    let mut edit = core(None);
    edit["secrets"] = json!({});
    edit["fields"]["port"] = json!("6432");
    assert_eq!(
        f.call(true, "PUT", uri, Some(edit)).await.0,
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        f.call(true, "PUT", uri, Some(core(Some("second-synthetic"))))
            .await
            .0,
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        f.changes(),
        [
            ("source/core".to_string(), Change::Created),
            ("source/core".to_string(), Change::Changed),
            ("source/core".to_string(), Change::SecretsChanged),
        ]
    );

    let (status, listed) = f.call(true, "GET", "/quality/sources", None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(listed[0]["name"], "core");
    // Fields are replaced whole: the last edit sent no port.
    assert_eq!(
        listed[0]["fields"],
        json!({ "host": "db.example", "database": "core" })
    );
    assert_eq!(listed[0]["secrets"], json!(["password", "user"]));
    assert_eq!(listed[0]["previous"], true);
    assert!(
        !listed.to_string().contains("synthetic"),
        "no secret comes back"
    );

    let (status, drivers) = f.call(true, "GET", "/quality/drivers", None).await;
    assert_eq!(status, StatusCode::OK);
    assert!(drivers
        .as_array()
        .unwrap()
        .iter()
        .any(|d| d["id"] == "sqlserver" && d["engine"] == "trino"));
    let field = |driver: &str, key: &str| {
        drivers
            .as_array()
            .unwrap()
            .iter()
            .find(|d| d["id"] == driver)
            .unwrap()["fields"]
            .as_array()
            .unwrap()
            .iter()
            .find(|f| f["key"] == key)
            .cloned()
    };
    assert_eq!(
        field("sqlserver", "trust_server_certificate"),
        Some(json!({ "key": "trust_server_certificate", "kind": "flag", "required": false }))
    );
    assert_eq!(
        field("oracle", "owner"),
        Some(json!({ "key": "owner", "kind": "text", "required": false }))
    );
}

#[tokio::test]
async fn what_a_caller_may_not_do_or_a_source_that_does_not_hold_is_refused_unaudited() {
    let f = fixture(None).await;
    // Not allowed: managing another source, or anything without access.
    assert_eq!(
        f.call(true, "PUT", "/quality/sources/other", Some(core(Some("x"))))
            .await
            .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        f.call(false, "GET", "/quality/sources", None).await.0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        f.call(false, "GET", "/quality/drivers", None).await.0,
        StatusCode::FORBIDDEN
    );
    // Not holding together: every reason, by code and field.
    let (status, body) = f
        .call(true, "PUT", "/quality/sources/core", Some(core(None)))
        .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(
        body["errors"],
        json!([{ "code": "missing", "field": "password" }])
    );
    assert!(f.changes().is_empty(), "nothing done, nothing audited");
}

#[tokio::test]
async fn saving_a_source_lists_its_tables_and_a_steward_may_list_them_again() {
    let f = fixture(None).await;
    let tables = "/quality/sources/core/tables";
    assert_eq!(
        f.call(true, "GET", tables, None).await.0,
        StatusCode::NOT_FOUND
    );
    f.call(
        true,
        "PUT",
        "/quality/sources/core",
        Some(core(Some("synthetic"))),
    )
    .await;
    let (status, listing) = f.call(true, "GET", tables, None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        listing,
        json!({
            "state": "listing", "listed_at": null, "tables": [], "truncated": false,
            "attempt": null, "max_attempts": null, "reason": null,
        })
    );
    let (status, queued) = f.call(true, "POST", tables, None).await;
    assert_eq!(status, StatusCode::ACCEPTED);
    let jobs: Vec<(Uuid, Option<String>)> = sqlx::query_as(
        "SELECT id, dataset FROM quality.jobs
          WHERE kind = 'tables' AND parameters = '{\"source\": \"core\"}' ORDER BY created_at",
    )
    .fetch_all(&f._db.pool)
    .await
    .unwrap();
    assert_eq!(jobs.len(), 2, "one on saving, one asked again");
    assert_eq!(queued["job"], json!(jobs[1].0));
    assert!(jobs.iter().all(|(_, dataset)| dataset.is_none()));
    assert_eq!(
        f.changes().last(),
        Some(&("source/core".to_string(), Change::Ran))
    );

    // Without access, neither seen nor asked.
    assert_eq!(
        f.call(false, "GET", tables, None).await.0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        f.call(false, "POST", tables, None).await.0,
        StatusCode::FORBIDDEN
    );
}

#[test]
fn the_module_describes_its_routes_for_the_openapi_document() {
    let doc = QualityModule::default().openapi();
    let paths: Vec<_> = doc.paths.paths.keys().cloned().collect();
    assert_eq!(
        paths,
        [
            "/quality/console",
            "/quality/console/{job}",
            "/quality/console/{job}/reveal",
            "/quality/datasets",
            "/quality/datasets/{dataset}",
            "/quality/datasets/{dataset}/charts",
            "/quality/datasets/{dataset}/charts/{chart}",
            "/quality/datasets/{dataset}/cuts",
            "/quality/datasets/{dataset}/drafts",
            "/quality/datasets/{dataset}/fields",
            "/quality/datasets/{dataset}/files/{file}",
            "/quality/datasets/{dataset}/history",
            "/quality/datasets/{dataset}/queues/slas",
            "/quality/datasets/{dataset}/rules",
            "/quality/datasets/{dataset}/split",
            "/quality/drafts",
            "/quality/drafts/{dataset}/{id}",
            "/quality/drafts/{dataset}/{id}/approve",
            "/quality/drafts/{dataset}/{id}/changes",
            "/quality/drafts/{dataset}/{id}/files/{file}",
            "/quality/drafts/{dataset}/{id}/history",
            "/quality/drivers",
            "/quality/lake/namespaces/{namespace}",
            "/quality/lake/tables",
            "/quality/lake/tables/{namespace}/{table}",
            "/quality/maps/{job}",
            "/quality/rules/check",
            "/quality/rules/schema",
            "/quality/rules/test",
            "/quality/rules/tests/{job}",
            "/quality/rules/tests/{job}/reveal",
            "/quality/rules/tidy",
            "/quality/schedules",
            "/quality/schedules/{id}",
            "/quality/sources",
            "/quality/sources/{name}",
            "/quality/sources/{name}/maps",
            "/quality/sources/{name}/records",
            "/quality/sources/{name}/records/{job}",
            "/quality/sources/{name}/records/{job}/reveal",
            "/quality/sources/{name}/tables"
        ]
    );
    let schemas = doc.components.expect("its schemas").schemas;
    for schema in [
        "DriverOut",
        "SourceView",
        "SourceBody",
        "Refusals",
        "InUse",
        "RunOut",
        "ListingView",
    ] {
        assert!(schemas.contains_key(schema), "missing {schema}");
    }
}

fn catalog(mappings: Vec<DatasetMapping>) -> Option<Arc<dyn Catalog>> {
    Some(Arc::new(Mappings(mappings)))
}

#[tokio::test]
async fn a_steward_deletes_a_source_no_mapping_reads_audited() {
    let read = vec![
        mapping("advisors", Some("ledger")),
        mapping("offices", None),
    ];
    let f = fixture(catalog(read)).await;
    let uri = "/quality/sources/core";
    let created = f
        .call(true, "PUT", uri, Some(core(Some("synthetic"))))
        .await;
    assert_eq!(created.0, StatusCode::CREATED);

    assert_eq!(
        f.call(true, "DELETE", uri, None).await.0,
        StatusCode::NO_CONTENT
    );
    let (_, listed) = f.call(true, "GET", "/quality/sources", None).await;
    assert_eq!(listed, json!([]));
    // Once gone, it is absent; absent is not audited.
    assert_eq!(
        f.call(true, "DELETE", uri, None).await.0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        f.changes(),
        [
            ("source/core".to_string(), Change::Created),
            ("source/core".to_string(), Change::Deleted),
        ]
    );
}

#[tokio::test]
async fn a_source_is_not_deleted_if_forbidden_read_or_the_catalog_cannot_say() {
    let f = fixture(catalog(vec![mapping("loans", Some("core"))])).await;
    let uri = "/quality/sources/core";
    let created = f
        .call(true, "PUT", uri, Some(core(Some("synthetic"))))
        .await;
    assert_eq!(created.0, StatusCode::CREATED);

    // Not the caller's to manage.
    assert_eq!(
        f.call(false, "DELETE", uri, None).await.0,
        StatusCode::FORBIDDEN
    );
    let other = "/quality/sources/other";
    assert_eq!(
        f.call(true, "DELETE", other, None).await.0,
        StatusCode::FORBIDDEN
    );
    // A dataset's mapping reads it: named, nothing deleted.
    let (status, body) = f.call(true, "DELETE", uri, None).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body, json!({ "datasets": ["loans"] }));
    let (_, listed) = f.call(true, "GET", "/quality/sources", None).await;
    assert_eq!(listed[0]["name"], "core");
    assert!(
        !body.to_string().contains("synthetic"),
        "no secret comes back"
    );
    assert_eq!(f.changes(), [("source/core".to_string(), Change::Created)]);
}

#[tokio::test]
async fn without_a_catalog_no_source_is_deleted() {
    let f = fixture(None).await;
    let uri = "/quality/sources/core";
    let created = f
        .call(true, "PUT", uri, Some(core(Some("synthetic"))))
        .await;
    assert_eq!(created.0, StatusCode::CREATED);
    let unavailable = f.call(true, "DELETE", uri, None).await.0;
    assert_eq!(unavailable, StatusCode::SERVICE_UNAVAILABLE);
    let (_, listed) = f.call(true, "GET", "/quality/sources", None).await;
    assert_eq!(listed[0]["name"], "core");
    assert_eq!(f.changes(), [("source/core".to_string(), Change::Created)]);
}

#[tokio::test]
async fn a_steward_asks_for_a_map_of_a_sources_table_audited() {
    let f = fixture(catalog(vec![mapping("held", None)])).await;
    assert_eq!(
        f.call(
            true,
            "PUT",
            "/quality/sources/core",
            Some(core(Some("synthetic")))
        )
        .await
        .0,
        StatusCode::CREATED
    );
    let maps = "/quality/sources/core/maps";
    let (status, out) = f
        .call(
            true,
            "POST",
            maps,
            Some(json!({ "table": "public.loans", "dataset": "loans" })),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);
    let job = out["job"].as_str().unwrap();
    assert_eq!(out["dataset"], "loans");
    let (kind, dataset, parameters): (String, String, Value) =
        sqlx::query_as("SELECT kind, dataset, parameters FROM quality.jobs WHERE id = $1::uuid")
            .bind(job)
            .fetch_one(&f._db.pool)
            .await
            .unwrap();
    assert_eq!((kind.as_str(), dataset.as_str()), ("map", "loans"));
    assert_eq!(
        (
            &parameters["source"],
            &parameters["table"],
            &parameters["sample"]
        ),
        (&json!("core"), &json!("public.loans"), &json!(10000))
    );
    // The installation's patterns, which its profile and its proposal use.
    assert_eq!(parameters["patterns"]["mobile"], r"3\d{9}");
    assert!(
        parameters["requested_by"].is_string(),
        "who asked: its cut's"
    );
    let last = f.log.0.lock().unwrap().last().cloned().unwrap();
    assert_eq!(
        (last.action, last.resource.as_str(), last.change),
        ("quality:ManageDatasets", "dataset/loans", Change::Ran)
    );

    for (uri, body, expected) in [
        (
            maps,
            json!({ "table": "public.cards", "dataset": "cards" }),
            StatusCode::FORBIDDEN,
        ),
        (
            "/quality/sources/ghost/maps",
            json!({ "table": "t", "dataset": "loans" }),
            StatusCode::NOT_FOUND,
        ),
        (
            maps,
            json!({ "table": "a.b.c.d", "dataset": "loans" }),
            StatusCode::UNPROCESSABLE_ENTITY,
        ),
        (
            maps,
            json!({ "table": "t", "dataset": "loans", "sample": 0 }),
            StatusCode::UNPROCESSABLE_ENTITY,
        ),
        // Never over a dataset the catalog has.
        (
            maps,
            json!({ "table": "t", "dataset": "held" }),
            StatusCode::CONFLICT,
        ),
    ] {
        assert_eq!(
            f.call(true, "POST", uri, Some(body)).await.0,
            expected,
            "{uri}"
        );
    }
}

#[tokio::test]
async fn a_map_says_how_far_it_went_then_its_first_cut() {
    let f = fixture(catalog(Vec::new())).await;
    f.call(
        true,
        "PUT",
        "/quality/sources/core",
        Some(core(Some("synthetic"))),
    )
    .await;
    let (_, out) = f
        .call(
            true,
            "POST",
            "/quality/sources/core/maps",
            Some(json!({ "table": "public.loans", "dataset": "loans" })),
        )
        .await;
    let uri = format!("/quality/maps/{}", out["job"].as_str().unwrap());
    let (status, progress) = f.call(true, "GET", &uri, None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        (&progress["dataset"], &progress["state"], &progress["cut"]),
        (&json!("loans"), &json!("queued"), &Value::Null)
    );

    // Mapped: the dataset's cut that its report opened.
    sqlx::query("UPDATE quality.jobs SET state = 'done' WHERE kind = 'map'")
        .execute(&f._db.pool)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO quality.cuts (tenant_id, dataset, cut, due_at)
         VALUES ($1::uuid, 'loans', DATE '2026-11-04', now())",
    )
    .bind(tenancy::MAIN_ID)
    .execute(&f._db.pool)
    .await
    .unwrap();
    let (status, progress) = f.call(true, "GET", &uri, None).await;
    assert_eq!(status, StatusCode::OK, "{progress}");
    assert_eq!(progress["state"], "done");
    assert_eq!(
        (&progress["cut"]["cut"], &progress["cut"]["state"]),
        (&json!("2026-11-04"), &json!("extracting"))
    );

    let ghost = format!("/quality/maps/{}", Uuid::new_v4());
    assert_eq!(
        f.call(true, "GET", &ghost, None).await.0,
        StatusCode::NOT_FOUND
    );
}
