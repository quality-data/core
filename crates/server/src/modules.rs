//! The feature modules this server mounts, and their migrations on
//! PostgreSQL. `bootstrap-admin` applies them after `infrastructure`'s (it
//! creates their bookkeeping table); `serve` applies them again, which does
//! nothing when they are in place, and refuses to start if one already
//! applied has changed.

use std::sync::Arc;
use std::time::Duration;

use application::lake::{Lake, LakeGrant, WriteOnce};
use auth_gateway::AuthorizationServer;
use contracts::{MigrationReport, ModuleRegistry};
use domain::{AuditEventType, AuditRepository, NewActionAudit};
use infrastructure::lake_admins::AdapterLakeAdmins;
use infrastructure::lake_policies::SessionPolicies;
use infrastructure::module_migrations::PgModuleMigrator;
use infrastructure::standard_lake::StandardLake;
use infrastructure::{PgAuditRepository, PgLakeAdminKeys, PgPool, SecretBox};
use quality::bundle_api::{self, BundleServices};
use quality::catalog::Catalog;
use quality::draft_api::DraftCatalog;
use quality::job_map::Drafts;
use quality::job_sources::IssueLog;
use quality::lake::{LakeAccess, LakeIssuer, TableCatalog};
use quality::manifest::ManifestStore;
use quality::module::QualityModule;
use quality::proposal::Patterns;
use quality::run_token_api::RunTokenServices;
use quality::source_store::Sealer;
use quality::worker_api::WorkerServices;
use texts::Language;

use crate::catalog_wiring::{proposal_alerts, DoltSnapshots};
use uuid::Uuid;

/// The modules as their schema: what `migrate` and `init` need. Adding a
/// feature is ONE line here and one in [`serving`]; its crate carries the
/// rest.
pub(crate) fn registry() -> ModuleRegistry {
    ModuleRegistry::new().register(QualityModule::default())
}

/// The modules with the services their routes use. The workers' routes need
/// the OIDC provider (on-premise) to know the machine behind a token;
/// without one they are not served. With the lake (`LAKE_ENDPOINT`, the
/// issuer's keys and the main lake's adapter, which writes each grant's
/// session policy: decision 0078), jobs come with credentials for their
/// warehouse;
/// with the catalog (`DOLT_URL`), with their dataset's mapping and rules;
/// and evaluations leave their manifests in the warehouse through the
/// same issuer. The sources' routes for people seal their secrets with the
/// installation's key, as the settings do, and ask the catalog which
/// mappings read a source before deleting it.
pub(crate) fn serving(
    pool: &PgPool,
    provider: Option<AuthorizationServer>,
    secret_box: SecretBox,
    dolt: Option<Arc<DoltSnapshots>>,
) -> Result<ModuleRegistry, Box<dyn std::error::Error>> {
    let lake = standard_lake(pool, &secret_box)?;
    let manifests = lake
        .clone()
        .map(|l| Arc::new(LakeManifests(l)) as Arc<dyn ManifestStore>);
    let tables = lake
        .clone()
        .map(|l| Arc::new(LakeTables(l)) as Arc<dyn TableCatalog>);
    let lake = lake.map(|l| Arc::new(LakeSts(l)) as Arc<dyn LakeIssuer>);
    let catalog = dolt.clone().map(|d| d as Arc<dyn Catalog>);
    let drafts = dolt.clone().map(|d| d as Arc<dyn Drafts>);
    let review = dolt.map(|d| d as Arc<dyn DraftCatalog>);
    let bundles = provider
        .clone()
        .and_then(|p| bundle_services(p, pool.clone(), catalog.clone(), manifests.clone()));
    let run_tokens = provider
        .clone()
        .and_then(|p| run_token_services(p, pool.clone()));
    let run_reports = provider.clone().and_then(|p| {
        p.sign_es256(quality::bundle::BUNDLE_TYP, "{}")?;
        Some(quality::run_report_api::RunReportServices {
            tokens: Arc::new(BundleTokens(p.clone())),
            signer: Arc::new(ProviderBundles(p)),
            drafts: drafts.clone(),
            proposal_alerts: Some(proposal_alerts(Language::Es)),
            catalog: catalog.clone(),
        })
    });
    let run_sources = provider.clone().zip(catalog.clone()).map(|(p, catalog)| {
        quality::run_source_api::RunSourceServices {
            tokens: Arc::new(BundleTokens(p)),
            sealer: Arc::new(BoxSealer(secret_box.clone())),
            catalog,
            log: Arc::new(Issues(Arc::new(PgAuditRepository::new(pool.clone())))),
        }
    });
    let quality = match provider {
        Some(p) => QualityModule::serving(
            pool.clone(),
            WorkerServices {
                tokens: Arc::new(ProviderTokens(p)),
                lake,
                catalog: catalog.clone(),
                manifests,
                tables,
                drafts,
                proposal_alerts: Some(proposal_alerts(Language::Es)),
                sources: Some(Arc::new(BoxSealer(secret_box.clone()))),
                issues: Some(Arc::new(Issues(Arc::new(PgAuditRepository::new(
                    pool.clone(),
                ))))),
            },
        ),
        None => QualityModule::default(),
    }
    .with_sources(pool.clone(), Arc::new(BoxSealer(secret_box)), catalog)
    .with_patterns(semantic_patterns()?);
    let quality = match review {
        Some(review) => quality.with_drafts(review),
        None => quality,
    };
    let quality = match bundles {
        Some(services) => quality.with_bundles(pool.clone(), services),
        None => quality,
    };
    let quality = match run_tokens {
        Some(services) => quality.with_run_tokens(pool.clone(), services),
        None => quality,
    };
    let quality = match run_reports {
        Some(services) => quality.with_run_reports(pool.clone(), services),
        None => quality,
    };
    let quality = match run_sources {
        Some(services) => quality.with_run_sources(pool.clone(), services),
        None => quality,
    };
    Ok(ModuleRegistry::new().register(quality))
}

/// The lake over its standard protocols (decision 0078, step 8), each
/// grant's session policy asked of the main lake's adapter and cached;
/// `None` without the lake's issuer or a way to its adapter: then no job
/// gets lake credentials, rather than a policy the core would write.
fn standard_lake(
    pool: &PgPool,
    sealer: &SecretBox,
) -> Result<Option<Arc<dyn Lake>>, Box<dyn std::error::Error>> {
    let Some(issuer) = infrastructure::rustfs_sts::issuer_from_env()? else {
        return Ok(None);
    };
    let keys = Arc::new(PgLakeAdminKeys::new(pool.clone(), sealer.clone()));
    let admins = match AdapterLakeAdmins::from_env(keys) {
        Ok(admins) => Arc::new(admins),
        Err(e) => {
            tracing::warn!(error = %e, "no lake admin adapter: jobs get no lake credentials");
            return Ok(None);
        }
    };
    let policies = Arc::new(SessionPolicies::new(admins.clone(), admins));
    Ok(Some(Arc::new(StandardLake::new(issuer, policies))))
}

/// The installation's semantic patterns (decision 0038), from
/// `QUALITY_SEMANTIC_PATTERNS` (or its `_FILE`): a JSON object with the ones
/// it changes (`email`, `mobile`, `document`, `date`, `number`, `flag`); the
/// rest are Colombia's. Each must be one DuckDB takes, or the server does
/// not start.
fn semantic_patterns() -> Result<Patterns, Box<dyn std::error::Error>> {
    patterns_from(infrastructure::oidc_es256::key_from_env(
        "QUALITY_SEMANTIC_PATTERNS",
    )?)
}

fn patterns_from(json: Option<String>) -> Result<Patterns, Box<dyn std::error::Error>> {
    let Some(json) = json else {
        return Ok(Patterns::default());
    };
    let patterns: Patterns =
        serde_json::from_str(&json).map_err(|e| format!("QUALITY_SEMANTIC_PATTERNS: {e}"))?;
    if let Some(kind) = patterns.refused() {
        return Err(format!("QUALITY_SEMANTIC_PATTERNS: {kind} is not a valid RE2 pattern").into());
    }
    Ok(patterns)
}

/// Credentials given to jobs, in the audit log: the machine as the service
/// account, the job, and the job's kind as its action (decision 0030).
struct Issues(Arc<dyn AuditRepository>);

#[async_trait::async_trait]
impl IssueLog for Issues {
    async fn issued(
        &self,
        job: Uuid,
        kind: &str,
        machine: &str,
        ip: &str,
        resource: &str,
    ) -> Result<(), String> {
        self.0
            .record_action(&NewActionAudit {
                event_type: AuditEventType::CredentialsIssued,
                admin_id: None,
                service_account: Some(machine.to_string()),
                job_id: Some(job.to_string()),
                action: format!("job:{kind}"),
                resource: resource.to_string(),
                ip: ip.to_string(),
                user_agent: None,
                occurred_at: std::time::SystemTime::now(),
            })
            .await
            .map(|_| ())
            .map_err(|e| e.to_string())
    }
}

/// The sources' secrets, sealed with the installation's key.
struct BoxSealer(SecretBox);

impl Sealer for BoxSealer {
    fn seal(&self, plain: &str) -> Result<String, String> {
        self.0.encrypt(plain)
    }

    fn open(&self, sealed: &str) -> Result<String, String> {
        self.0.decrypt(sealed)
    }
}

/// Where a bronze table's files are, as the lake's catalog says (decision
/// 0037), each call within a grant of its own.
struct LakeTables(Arc<dyn Lake>);

#[async_trait::async_trait]
impl TableCatalog for LakeTables {
    async fn ensure_namespace(&self, warehouse: &str, namespace: &str) -> Result<(), String> {
        let made = self.0.ensure_namespace(warehouse, namespace).await;
        made.map_err(|e| e.to_string())
    }

    async fn tables(&self, warehouse: &str, namespace: &str) -> Result<Vec<String>, String> {
        let tables = self.0.tables(warehouse, namespace).await;
        tables.map_err(|e| e.to_string())
    }

    async fn table_prefix(
        &self,
        warehouse: &str,
        namespace: &str,
        table: &str,
    ) -> Result<Option<String>, String> {
        let at = self.0.table_location(warehouse, namespace, table).await;
        at.map_err(|e| e.to_string())
    }

    async fn drop_table(
        &self,
        warehouse: &str,
        namespace: &str,
        table: &str,
    ) -> Result<bool, String> {
        let dropped = self.0.drop_table(warehouse, namespace, table).await;
        dropped.map_err(|e| e.to_string())
    }
}

/// A cut's manifests, each written once with credentials for that write.
struct LakeManifests(Arc<dyn Lake>);

#[async_trait::async_trait]
impl ManifestStore for LakeManifests {
    async fn put_once(&self, warehouse: &str, key: &str, body: Vec<u8>) -> Result<bool, String> {
        let written = self.0.write_once(warehouse, key, &body).await;
        written
            .map(|w| w == WriteOnce::Written)
            .map_err(|e| e.to_string())
    }

    async fn get(&self, warehouse: &str, key: &str) -> Result<Option<Vec<u8>>, String> {
        self.0
            .read_object(warehouse, key)
            .await
            .map_err(|e| e.to_string())
    }
}

/// A job's lake credentials, within its grant, from the lake's STS.
struct LakeSts(Arc<dyn Lake>);

#[async_trait::async_trait]
impl LakeIssuer for LakeSts {
    async fn issue(&self, session: &str, grant: &LakeGrant) -> Result<LakeAccess, String> {
        let c = self
            .0
            .credentials(grant, session, Duration::from_secs(15 * 60))
            .await
            .map_err(|e| e.to_string())?;
        Ok(LakeAccess {
            endpoint: self.0.properties().s3_endpoint.clone(),
            warehouse: grant.warehouse.clone(),
            access_key: c.access_key,
            secret_key: c.secret_key,
            session_token: c.session_token,
            expires_at: chrono::DateTime::<chrono::Utc>::from(c.expires_at).to_rfc3339(),
        })
    }
}

/// The workers' tokens, as the provider issued them.
struct ProviderTokens(AuthorizationServer);

impl quality::worker_api::MachineTokens for ProviderTokens {
    fn client(&self, token: &str) -> Option<quality::worker_api::MachineCaller> {
        let (client, machine) = self
            .0
            .machine_caller(token, quality::worker_api::AUDIENCE)?;
        Some(quality::worker_api::MachineCaller { client, machine })
    }
}

/// The bundle route's services (decision 0090, point 2): bundles signed
/// with the provider's ES256 key, asked with machine tokens for the job
/// runners' audience; `None` without that key.
fn bundle_services(
    p: AuthorizationServer,
    pool: PgPool,
    catalog: Option<Arc<dyn Catalog>>,
    manifests: Option<Arc<dyn ManifestStore>>,
) -> Option<BundleServices> {
    p.sign_es256(quality::bundle::BUNDLE_TYP, "{}")?;
    Some(BundleServices {
        tokens: Arc::new(BundleTokens(p.clone())),
        signer: Arc::new(ProviderBundles(p)),
        compiler: Arc::new(crate::job_compiler::TrinoJobs {
            pool,
            catalog,
            manifests,
        }),
    })
}

/// The run token route's services (decision 0090, point 7.1): asked with
/// the job runners' machine tokens, recorded in the audit log, issued with
/// the provider's ES256 key; `None` without that key.
fn run_token_services(p: AuthorizationServer, pool: PgPool) -> Option<RunTokenServices> {
    p.sign_es256("at+jwt", "{}")?;
    Some(RunTokenServices {
        tokens: Arc::new(BundleTokens(p.clone())),
        issuer: Arc::new(crate::trino_tokens::ProviderTrinoTokens::new(
            p,
            pool.clone(),
        )),
        log: Arc::new(Issues(Arc::new(PgAuditRepository::new(pool)))),
    })
}

/// The job runners' tokens for the bundle route, as the provider issued
/// them.
struct BundleTokens(AuthorizationServer);

impl quality::worker_api::MachineTokens for BundleTokens {
    fn client(&self, token: &str) -> Option<quality::worker_api::MachineCaller> {
        let (client, machine) = self.0.machine_caller(token, bundle_api::AUDIENCE)?;
        Some(quality::worker_api::MachineCaller { client, machine })
    }
}

/// The provider's ES256 key, which signs job bundles.
struct ProviderBundles(AuthorizationServer);

impl quality::bundle::BundleSigner for ProviderBundles {
    fn sign(&self, typ: &str, claims: &str) -> String {
        // `bundle_services` wires it only once the key has signed.
        self.0
            .sign_es256(typ, claims)
            .expect("the provider's ES256 key")
    }

    fn verify(&self, typ: &str, token: &str) -> Option<String> {
        self.0.verify_es256(typ, token)
    }
}

pub(crate) async fn migrate(
    modules: &ModuleRegistry,
    pool: &PgPool,
) -> Result<MigrationReport, Box<dyn std::error::Error>> {
    let mut migrator = PgModuleMigrator::load(pool).await?;
    migrator.verify(modules.modules().iter().flat_map(|m| {
        m.migrations()
            .into_iter()
            .map(move |mig| (format!("{}::{}", m.name(), mig.name), mig.sql))
    }))?;
    let report = modules.run_migrations(&mut migrator)?;
    migrator.run(pool).await?;
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_installations_patterns_must_be_valid_re2() {
        assert_eq!(patterns_from(None).unwrap(), Patterns::default());
        let mobile = patterns_from(Some(r#"{ "mobile": "9\\d{8}" }"#.into())).unwrap();
        assert_eq!(
            (mobile.mobile.as_str(), mobile.email),
            (r"9\d{8}", Patterns::default().email)
        );
        for bad in [
            r#"{ "mobile": "(unclosed" }"#,
            r#"{ "mobile": " " }"#,
            r#"{ "phone": "x" }"#,
            "not json",
        ] {
            assert!(patterns_from(Some(bad.into())).is_err(), "{bad}");
        }
    }
}
