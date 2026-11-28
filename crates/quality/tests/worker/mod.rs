//! What the workers' routes' tests share (decisions 0018, 0035 to 0038):
//! an ephemeral PostgreSQL connected as the application's role, whose row
//! security leaves it the main tenant's rows, the module's worker routes over it, and fakes for
//! the services they use. Synthetic values only.

use std::sync::{Arc, Mutex};

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::Router;
use contracts::{Module, ModuleRegistry};
use infrastructure::module_migrations::PgModuleMigrator;
use quality::catalog::{Catalog, CatalogSnapshot, DatasetMapping, RulesSource};
use quality::firing::{open_cut, CutRequest};
use quality::job_map::Drafts;
use quality::job_sources::IssueLog;
use quality::lake::{LakeAccess, LakeIssuer, TableCatalog};
use quality::manifest::ManifestStore;
use quality::module::QualityModule;
use quality::proposal::Alerts;
use quality::schedule::Rerun;
use quality::source_store::Sealer;
use quality::worker_api::{
    Distribution, EntityIcdRow, FieldQuality, GroupMetrics, MachineCaller, MachineTokens,
    OutlierGroup, QueueFindings, Report, RuleCounts, SfcCell, StageTime, WorkerServices,
};
use serde_json::json;
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use tower::ServiceExt;
use uuid::Uuid;

/// The installation's tenant, the main one (decision 0076): the one the
/// application's role sees. `B` is the same, for the tests written with
/// two tenants.
pub const A: &str = "00000000-0000-0000-0000-000000000001";
pub const B: &str = A;

/// `token-<client>` belongs to `<client>`; `token-<client>@<machine>` to a
/// role's client, for that machine (a token with a `machine` claim).
#[derive(Debug)]
pub struct Tokens;
impl MachineTokens for Tokens {
    fn client(&self, token: &str) -> Option<MachineCaller> {
        let caller = token.strip_prefix("token-")?;
        let (client, machine) = match caller.split_once('@') {
            Some((client, machine)) => (client, Some(machine.to_string())),
            None => (caller, None),
        };
        Some(MachineCaller {
            client: client.to_string(),
            machine,
        })
    }
}

/// Echoes what it was asked for, as keys: the grant, as JSON, the secret.
#[derive(Debug)]
pub struct Issuer;
#[async_trait::async_trait]
impl LakeIssuer for Issuer {
    async fn issue(
        &self,
        session: &str,
        grant: &quality::lake::LakeGrant,
    ) -> Result<LakeAccess, String> {
        Ok(LakeAccess {
            endpoint: "https://lake.test".into(),
            warehouse: grant.warehouse.clone(),
            access_key: session.into(),
            secret_key: serde_json::to_string(grant).unwrap(),
            session_token: "token".into(),
            expires_at: "2026-10-30T12:15:00Z".into(),
        })
    }
}

/// Has `clientes` only, for every tenant.
#[derive(Debug)]
pub struct OneDataset;
#[async_trait::async_trait]
impl Catalog for OneDataset {
    async fn snapshot(&self, dataset: &str) -> Result<Option<CatalogSnapshot>, String> {
        Ok((dataset == "clientes").then(|| CatalogSnapshot {
            commit: "commit-of-main".into(),
            mapping: MAPPING.into(),
            others: Vec::new(),
            rules: vec![RulesSource {
                file: "a.yaml".into(),
                yaml: "dataset: clientes".into(),
            }],
        }))
    }

    async fn mappings(&self) -> Result<Vec<DatasetMapping>, String> {
        Ok(Vec::new())
    }
}

/// `clientes`, read from the source `core`.
pub const MAPPING: &str = "dataset: clientes
source: clientes
from: { source: core, table: public.clients }
fields:
  Documento: { column: documento, type: text }
";

/// Reverses the text behind a prefix.
#[derive(Debug)]
pub struct Mirror;
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

/// Keeps each credential given (job, machine, resource); refuses all when
/// `down`.
#[derive(Debug, Default)]
pub struct Issued {
    pub given: Mutex<Vec<(Uuid, String, String)>>,
    pub down: std::sync::atomic::AtomicBool,
}
#[async_trait::async_trait]
impl IssueLog for Issued {
    async fn issued(
        &self,
        job: Uuid,
        _: &str,
        machine: &str,
        _: &str,
        resource: &str,
    ) -> Result<(), String> {
        if self.down.load(std::sync::atomic::Ordering::SeqCst) {
            return Err("the audit log is down".into());
        }
        self.given
            .lock()
            .unwrap()
            .push((job, machine.into(), resource.into()));
        Ok(())
    }
}

/// Keeps each dataset published, by name, with its mapping; a dataset only once.
#[derive(Debug, Default)]
pub struct Drafted(pub Mutex<Vec<(String, String)>>);
#[async_trait::async_trait]
impl Drafts for Drafted {
    async fn publish(
        &self,
        dataset: &str,
        mapping: &str,
        _: &[(String, String)],
    ) -> Result<Option<String>, String> {
        let mut published = self.0.lock().unwrap();
        if published.iter().any(|(d, _)| d == dataset) {
            return Ok(None);
        }
        published.push((dataset.into(), mapping.into()));
        Ok(Some("main-commit".into()))
    }
}

/// The bronze tables there are, by `namespace.table`, with their prefix;
/// and the namespaces made.
#[derive(Debug, Default)]
pub struct Tables(pub Mutex<Vec<(String, String)>>, pub Mutex<Vec<String>>);
#[async_trait::async_trait]
impl TableCatalog for Tables {
    async fn ensure_namespace(&self, _: &str, namespace: &str) -> Result<(), String> {
        self.1.lock().unwrap().push(namespace.into());
        Ok(())
    }
    async fn tables(&self, _: &str, namespace: &str) -> Result<Vec<String>, String> {
        let tables = self.0.lock().unwrap();
        Ok(tables
            .iter()
            .filter_map(|(n, _)| n.strip_prefix(&format!("{namespace}.")).map(str::to_string))
            .collect())
    }

    async fn table_prefix(
        &self,
        _: &str,
        namespace: &str,
        table: &str,
    ) -> Result<Option<String>, String> {
        let name = format!("{namespace}.{table}");
        let tables = self.0.lock().unwrap();
        Ok(tables
            .iter()
            .find(|(n, _)| *n == name)
            .map(|(_, p)| p.clone()))
    }
    async fn drop_table(&self, _: &str, namespace: &str, table: &str) -> Result<bool, String> {
        let mut tables = self.0.lock().unwrap();
        let before = tables.len();
        tables.retain(|(t, _)| t != &format!("{namespace}.{table}"));
        Ok(tables.len() < before)
    }
}

/// Keeps what it is given, once per key, as a bucket with `If-None-Match`.
#[derive(Debug, Default)]
pub struct Bucket(pub Mutex<Vec<(String, String, Vec<u8>)>>);
#[async_trait::async_trait]
impl ManifestStore for Bucket {
    async fn put_once(&self, warehouse: &str, key: &str, body: Vec<u8>) -> Result<bool, String> {
        let mut objects = self.0.lock().unwrap();
        if objects.iter().any(|(w, k, _)| w == warehouse && k == key) {
            return Ok(false);
        }
        objects.push((warehouse.into(), key.into(), body));
        Ok(true)
    }
}

#[derive(Debug)]
pub struct Fixture {
    pub db: testkit::TestDb,
    pub app: PgPool,
    pub router: Router,
    pub bucket: Arc<Bucket>,
    pub issued: Arc<Issued>,
    pub tables: Arc<Tables>,
    pub drafted: Arc<Drafted>,
}

pub async fn fixture() -> Fixture {
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
        "ALTER TABLE quality.run_summary OWNER TO app",
        "ALTER TABLE quality.rule_results OWNER TO app",
        "ALTER TABLE quality.field_quality OWNER TO app",
        "ALTER TABLE quality.schedules OWNER TO app",
        "ALTER TABLE quality.cuts OWNER TO app",
        "ALTER TABLE quality.sources OWNER TO app",
        "ALTER TABLE quality.source_tables OWNER TO app",
        "ALTER TABLE quality.lake_tables OWNER TO app",
        "ALTER TABLE quality.source_records OWNER TO app",
        "ALTER TABLE quality.console_results OWNER TO app",
        "ALTER TABLE quality.entity_keys OWNER TO app",
        "ALTER TABLE quality.entity_icd OWNER TO app",
        "ALTER TABLE quality.group_metrics OWNER TO app",
        "ALTER TABLE quality.distribution_checks OWNER TO app",
        "ALTER TABLE quality.job_stats OWNER TO app",
        "ALTER TABLE quality.cells OWNER TO app",
        "ALTER TABLE quality.queue_findings OWNER TO app",
        "ALTER TABLE quality.trino_holds OWNER TO app",
    ] {
        sqlx::query(sql).execute(&db.pool).await.unwrap();
    }
    let app = PgPoolOptions::new()
        .max_connections(4)
        .connect_with(options)
        .await
        .unwrap();
    let bucket = Arc::new(Bucket::default());
    let issued = Arc::new(Issued::default());
    let tables = Arc::new(Tables::default());
    let drafted = Arc::new(Drafted::default());
    let services = WorkerServices {
        tokens: Arc::new(Tokens),
        lake: Some(Arc::new(Issuer)),
        catalog: Some(Arc::new(OneDataset)),
        manifests: Some(bucket.clone()),
        sources: Some(Arc::new(Mirror)),
        issues: Some(issued.clone()),
        tables: Some(tables.clone()),
        drafts: Some(drafted.clone()),
        proposal_alerts: Some(Alerts {
            required: "Empty".into(),
            unique: "Repeated".into(),
            email: "Not an email".into(),
            mobile: "Not a mobile".into(),
            document: "Not a document".into(),
        }),
    };
    let router = QualityModule::serving(app.clone(), services).router();
    Fixture {
        db,
        app,
        router,
        bucket,
        issued,
        tables,
        drafted,
    }
}

impl Fixture {
    /// A running job of `tenant`, held by `worker` for `lease` seconds.
    pub async fn job(&self, tenant: &str, worker: &str, lease: i32) -> Uuid {
        sqlx::query_scalar(
            "INSERT INTO quality.jobs (tenant_id, kind, dataset, state, worker, lease_until)
             VALUES ($1::uuid, 'evaluate', 'clientes', 'running', $2,
                     now() + make_interval(secs => $3)) RETURNING id",
        )
        .bind(tenant)
        .bind(worker)
        .bind(f64::from(lease))
        .fetch_one(&self.db.pool)
        .await
        .unwrap()
    }

    pub async fn call(
        &self,
        method: &str,
        uri: &str,
        token: &str,
        body: Option<serde_json::Value>,
    ) -> (StatusCode, Vec<u8>) {
        let mut req = Request::builder()
            .method(method)
            .uri(uri)
            .header("authorization", format!("Bearer {token}"));
        if body.is_some() {
            req = req.header("content-type", "application/json");
        }
        let body = body.map_or_else(Body::empty, |b| Body::from(b.to_string()));
        let res = self
            .router
            .clone()
            .oneshot(req.body(body).unwrap())
            .await
            .unwrap();
        let status = res.status();
        let bytes = axum::body::to_bytes(res.into_body(), usize::MAX)
            .await
            .unwrap();
        (status, bytes.to_vec())
    }

    /// The failures recorded for the cut, as the application sees it.
    pub async fn failures(&self) -> Vec<(String, i64)> {
        async {
            sqlx::query_as("SELECT rule, failures FROM quality.rule_results ORDER BY rule")
                .fetch_all(&self.app)
                .await
                .unwrap()
        }
        .await
    }

    /// What each rule's counts are grouped by, as the application sees it.
    /// Each field's profile in the tenant's cuts: field, empty, distinct
    /// values, shape and its count.
    pub async fn field_quality(&self) -> Vec<Profile> {
        async {
            sqlx::query_as(
                "SELECT field, empty, distinct_values, pattern, pattern_count, recency
                   FROM quality.field_quality ORDER BY field",
            )
            .fetch_all(&self.app)
            .await
            .unwrap()
        }
        .await
    }

    pub async fn grouping(&self) -> Vec<Grouping> {
        async {
            sqlx::query_as(
                "SELECT rule, field, pattern, dimension, severity
                   FROM quality.rule_results ORDER BY rule",
            )
            .fetch_all(&self.app)
            .await
            .unwrap()
        }
        .await
    }
}

/// A rule, its field, pattern, dimension and severity.
/// A field's profile as stored: counts, shape, and its records by age.
pub type Profile = (String, i64, i64, Option<String>, i64, Option<Vec<i64>>);

pub type Grouping = (
    String,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
);

/// An office's queues: findings open since two cuts, one resolved, and
/// the empty office's.
fn queues() -> Vec<QueueFindings> {
    let queue = |group: Option<&str>, since: &str, (open, resolved), priority| QueueFindings {
        dimension: "Office".into(),
        group: group.map(Into::into),
        since: since.into(),
        open,
        resolved,
        priority,
    };
    vec![
        queue(Some("O-1"), "2021-02-10", (4, 1), 1.2),
        queue(None, "2021-03-10", (2, 0), 0.4),
    ]
}

pub fn report(failures: i64) -> serde_json::Value {
    serde_json::to_value(Report {
        cut: "2021-03-10".into(),
        records: 100,
        duration_ms: 1200,
        rules_commit: "abc123".into(),
        snapshots: Vec::new(),
        files: Vec::new(),
        compiled: Vec::new(),
        compiler: String::new(),
        fields: vec![
            FieldQuality {
                field: "Nit".into(),
                empty: 3,
                distinct: 97,
                pattern: Some("999999999".into()),
                pattern_count: 90,
                floating: false,
                icd: Some(0.8),
                recency: None,
            },
            FieldQuality {
                field: "Updated".into(),
                empty: 1,
                distinct: 40,
                pattern: Some("9999-99-99".into()),
                pattern_count: 99,
                floating: false,
                icd: None,
                recency: Some(vec![0, 50, 20, 10, 10, 5, 4]),
            },
        ],
        // Two entities by their hashes, synthetic: one high, one low.
        igcd: Some(0.825),
        entities: vec![
            EntityIcdRow("aa".repeat(32), Some(0.95), Some("PN".into())),
            EntityIcdRow("bb".repeat(32), Some(0.7), None),
        ],
        // Two synthetic offices, one of them empty.
        groups: vec![
            GroupMetrics {
                dimension: "Office".into(),
                group: Some("O-1".into()),
                records: 60,
                serious: 6,
                icd: Some(0.9),
            },
            GroupMetrics {
                dimension: "Office".into(),
                group: None,
                records: 40,
                serious: 10,
                icd: None,
            },
        ],
        // A synthetic drift variable, and its outliers by office.
        distributions: vec![Distribution {
            variable: "Income".into(),
            percentiles: (0..=100).map(f64::from).collect(),
        }],
        outliers: vec![OutlierGroup {
            variable: "Income".into(),
            dimension: "Office".into(),
            group: Some("O-1".into()),
            records: 60,
            low: 1,
            high: 2,
        }],
        stages: stages(),
        cells: cells(),
        queues: queues(),
        rules: vec![
            RuleCounts {
                rule: "V-01".into(),
                applies: 100,
                failures,
                field: Some("Nit".into()),
                pattern: Some("required".into()),
                dimension: Some("completeness".into()),
                severity: Some("blocking".into()),
            },
            // As an older worker reports it: counts only.
            RuleCounts {
                rule: "V-02".into(),
                applies: 40,
                failures: 0,
                field: None,
                pattern: None,
                dimension: None,
                severity: None,
            },
        ],
    })
    .unwrap()
}

/// The stages a worker timed, in order.
fn stages() -> Vec<StageTime> {
    vec![
        StageTime {
            stage: "load".into(),
            ms: 700,
        },
        StageTime {
            stage: "rules".into(),
            ms: 500,
        },
    ]
}

/// Two synthetic SFC cells: one passes by 2, one misses 5.
fn cells() -> Vec<SfcCell> {
    vec![
        SfcCell {
            field: "Phone".into(),
            ok: 92,
            total: 100,
        },
        SfcCell {
            field: "City".into(),
            ok: 85,
            total: 100,
        },
    ]
}

impl Fixture {
    /// `clientes`' cut of 2021-03-10, opened; its extraction
    /// held by `w1/0`.
    pub async fn cut(&self) -> Uuid {
        let job = async {
            let mut tx = self.app.begin().await.unwrap();
            let request = CutRequest {
                dataset: "clientes",
                date: chrono::NaiveDate::from_ymd_opt(2021, 3, 10).unwrap(),
                rerun: Rerun::Block,
                schedule: None,
                requested_by: None,
                due_at: chrono::Utc::now(),
            };
            let (_, _, job) = open_cut(&mut tx, &request).await.unwrap();
            tx.commit().await.unwrap();
            job.unwrap()
        }
        .await;
        self.hold(job).await;
        job
    }

    pub async fn hold(&self, job: Uuid) {
        sqlx::query(
            "UPDATE quality.jobs SET state = 'running', worker = 'w1/0',
                    lease_until = now() + interval '1 minute' WHERE id = $1",
        )
        .bind(job)
        .execute(&self.db.pool)
        .await
        .unwrap();
    }

    /// The cut's state, snapshot and evaluation job.
    pub async fn cut_state(&self) -> (String, Option<String>, Option<Uuid>) {
        sqlx::query_as("SELECT state, bronze_snapshot, evaluate_job FROM quality.cuts")
            .fetch_one(&self.db.pool)
            .await
            .unwrap()
    }
}

pub fn extracted(rows: i64) -> serde_json::Value {
    json!({
        "cut": "2021-03-10",
        "duration_ms": 900,
        "source": "clientes",
        "schema": [{ "name": "documento", "type": "varchar" }],
        "ranges": [{ "range": "all", "rows": rows }],
        "snapshot": "7135000000000000001",
    })
}
