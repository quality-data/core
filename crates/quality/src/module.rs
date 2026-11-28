//! The quality core as a feature module (`contracts::Module`): its schema
//! and its routes. `server` registers it.

use std::sync::Arc;

use axum::Router;
use contracts::{Drain, Migration, Module};
use sqlx::PgPool;

use crate::bundle_api::{self, BundleServices};
use crate::catalog::Catalog;
use crate::chart_api;
use crate::console_api;
use crate::cut_api;
use crate::dataset_api;
use crate::delete_api;
use crate::deploy_api;
use crate::draft_api::{self, DraftCatalog};
use crate::draft_history;
use crate::drain::QualityDrain;
use crate::edit_api;
use crate::field_api;
use crate::lake_tables;
use crate::map_api;
use crate::proposal::Patterns;
use crate::records_api;
use crate::rule_test;
use crate::rules_api;
use crate::run_report_api::{self, RunReportServices};
use crate::run_source_api::{self, RunSourceServices};
use crate::run_token_api::{self, RunTokenServices};
use crate::schedule_api;
use crate::sla_api;
use crate::source_api;
use crate::source_store::Sealer;
use crate::split_api;
use crate::tables_api;
use crate::unconfigured::Unconfigured;
use crate::worker_api::{self, WorkerServices};

/// The quality module. [`Default`] carries only its schema (migrations);
/// [`QualityModule::serving`] its routes too.
#[derive(Default)]
pub struct QualityModule {
    workers: Option<(PgPool, WorkerServices)>,
    sources: Option<SourceServices>,
    patterns: Patterns,
    drafts: Option<Arc<dyn DraftCatalog>>,
    bundles: Option<(PgPool, BundleServices)>,
    run_tokens: Option<(PgPool, RunTokenServices)>,
    run_sources: Option<(PgPool, RunSourceServices)>,
    run_reports: Option<(PgPool, RunReportServices)>,
}

/// What the sources' routes use: the store, its sealer, and the catalog.
struct SourceServices {
    pool: PgPool,
    sealer: Arc<dyn Sealer>,
    catalog: Option<Arc<dyn Catalog>>,
}

impl std::fmt::Debug for QualityModule {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QualityModule")
            .field("worker_routes", &self.workers.is_some())
            .field("source_routes", &self.sources.is_some())
            .finish()
    }
}

impl QualityModule {
    /// With the workers' routes, which read the jobs through `pool`.
    pub fn serving(pool: PgPool, services: WorkerServices) -> Self {
        Self {
            workers: Some((pool, services)),
            sources: None,
            patterns: Patterns::default(),
            drafts: None,
            bundles: None,
            run_tokens: None,
            run_sources: None,
            run_reports: None,
        }
    }

    /// With the routes for people: the sources', their secrets sealed by
    /// `sealer`, _Run now_ and the schedules; without the `catalog`, no
    /// source can be deleted and no dataset run or scheduled.
    #[must_use]
    pub fn with_sources(
        mut self,
        pool: PgPool,
        sealer: Arc<dyn Sealer>,
        catalog: Option<Arc<dyn Catalog>>,
    ) -> Self {
        self.sources = Some(SourceServices {
            pool,
            sealer,
            catalog,
        });
        self
    }

    /// With the installation's semantic patterns, for the maps it queues
    /// (decision 0038); Colombia's by default.
    #[must_use]
    pub fn with_patterns(mut self, patterns: Patterns) -> Self {
        self.patterns = patterns;
        self
    }

    /// With the bundle route for job runners (decision 0090, point 2).
    #[must_use]
    pub fn with_bundles(mut self, pool: PgPool, services: BundleServices) -> Self {
        self.bundles = Some((pool, services));
        self
    }

    /// With the route that gives a run's job runner its Trino tokens
    /// (decision 0090, point 7.1).
    #[must_use]
    pub fn with_run_tokens(mut self, pool: PgPool, services: RunTokenServices) -> Self {
        self.run_tokens = Some((pool, services));
        self
    }

    /// With the route that gives a run's job runner its source's secrets
    /// (decision 0090, point 7.5).
    #[must_use]
    pub fn with_run_sources(mut self, pool: PgPool, services: RunSourceServices) -> Self {
        self.run_sources = Some((pool, services));
        self
    }

    /// With the route that takes a run's report from its job runner
    /// (decision 0090, rulings 75 and 76).
    #[must_use]
    pub fn with_run_reports(mut self, pool: PgPool, services: RunReportServices) -> Self {
        self.run_reports = Some((pool, services));
        self
    }

    /// With the drafts' routes for people, over the catalog.
    #[must_use]
    pub fn with_drafts(mut self, drafts: Arc<dyn DraftCatalog>) -> Self {
        self.drafts = Some(drafts);
        self
    }
}

/// A migration of `migrations/<name>.sql`, by its name.
macro_rules! migration {
    ($name:literal) => {
        Migration::new(
            $name,
            include_str!(concat!("../migrations/", $name, ".sql")),
        )
    };
}

impl Module for QualityModule {
    fn name(&self) -> &'static str {
        "quality"
    }

    fn migrations(&self) -> Vec<Migration> {
        vec![
            migration!("0001_jobs"),
            migration!("0002_queue"),
            migration!("0003_results"),
            migration!("0004_schedules"),
            migration!("0005_manifests"),
            migration!("0006_sources"),
            migration!("0007_source_tables"),
            migration!("0008_rule_tests"),
            migration!("0009_lake_tables"),
            migration!("0010_source_records"),
            migration!("0011_schedule_triggers"),
            migration!("0012_console"),
            migration!("0013_rule_results_grouping"),
            migration!("0014_field_quality"),
            migration!("0015_entity_keys"),
            migration!("0016_icd"),
            migration!("0017_recency"),
            migration!("0018_group_metrics"),
            migration!("0019_distribution_checks"),
            migration!("0020_job_stats"),
            migration!("0021_cells"),
            migration!("0022_queue_findings"),
            migration!("0023_remediation_slas"),
            migration!("0024_queue_machines"),
            migration!("0025_trino_hold"),
            migration!("0026_jobs_uses_trino"),
            migration!("0027_claim_in_maintenance"),
            migration!("0028_claim_without_trino_hold"),
            migration!("0029_gold_run_seq"),
            migration!("0030_job_bundles"),
            migration!("0031_field_quality_floating"),
            migration!("0032_job_reports"),
            migration!("0033_job_bundles_source"),
            migration!("0034_schedule_fires"),
            migration!("0035_chain_starts"),
        ]
    }

    /// Its jobs and schedules, counted on the pool its routes use.
    fn drain(&self) -> Option<Arc<dyn Drain>> {
        let pool = match (&self.sources, &self.workers) {
            (Some(s), _) => s.pool.clone(),
            (None, Some((pool, _))) => pool.clone(),
            (None, None) => return None,
        };
        Some(Arc::new(QualityDrain::new(pool)))
    }

    fn router(&self) -> Router {
        let workers = match &self.workers {
            Some((pool, services)) => worker_api::routes(pool.clone(), services.clone()),
            None => Router::new(),
        };
        let workers = match &self.bundles {
            Some((pool, services)) => {
                workers.merge(bundle_api::routes(pool.clone(), services.clone()))
            }
            None => workers,
        };
        let workers = match &self.run_tokens {
            Some((pool, services)) => {
                workers.merge(run_token_api::routes(pool.clone(), services.clone()))
            }
            None => workers,
        };
        let workers = match &self.run_sources {
            Some((pool, services)) => {
                workers.merge(run_source_api::routes(pool.clone(), services.clone()))
            }
            None => workers,
        };
        match &self.run_reports {
            Some((pool, services)) => {
                workers.merge(run_report_api::routes(pool.clone(), services.clone()))
            }
            None => workers,
        }
    }

    fn openapi(&self) -> utoipa::openapi::OpenApi {
        let mut doc = <source_api::SourceDoc as utoipa::OpenApi>::openapi();
        doc.merge(<cut_api::CutDoc as utoipa::OpenApi>::openapi());
        doc.merge(<dataset_api::DatasetDoc as utoipa::OpenApi>::openapi());
        doc.merge(<chart_api::ChartDoc as utoipa::OpenApi>::openapi());
        doc.merge(<schedule_api::ScheduleDoc as utoipa::OpenApi>::openapi());
        doc.merge(<map_api::MapDoc as utoipa::OpenApi>::openapi());
        doc.merge(<tables_api::TablesDoc as utoipa::OpenApi>::openapi());
        doc.merge(<lake_tables::LakeTablesDoc as utoipa::OpenApi>::openapi());
        doc.merge(<draft_api::DraftDoc as utoipa::OpenApi>::openapi());
        doc.merge(<rules_api::RulesDoc as utoipa::OpenApi>::openapi());
        doc.merge(<edit_api::EditDoc as utoipa::OpenApi>::openapi());
        doc.merge(<deploy_api::DeployDoc as utoipa::OpenApi>::openapi());
        doc.merge(<split_api::SplitDoc as utoipa::OpenApi>::openapi());
        doc.merge(<field_api::FieldDoc as utoipa::OpenApi>::openapi());
        doc.merge(<draft_history::HistoryDoc as utoipa::OpenApi>::openapi());
        doc.merge(<rule_test::TestDoc as utoipa::OpenApi>::openapi());
        doc.merge(<records_api::RecordsDoc as utoipa::OpenApi>::openapi());
        doc.merge(<console_api::ConsoleDoc as utoipa::OpenApi>::openapi());
        doc.merge(<delete_api::DeleteDoc as utoipa::OpenApi>::openapi());
        doc.merge(<sla_api::SlaDoc as utoipa::OpenApi>::openapi());
        doc
    }

    fn user_router(&self) -> Router {
        // Without a catalog the drafts' routes are still served, and say
        // so: `503 catalog_not_configured` (decision 0026).
        let d: Arc<dyn DraftCatalog> = match &self.drafts {
            Some(d) => d.clone(),
            None => Arc::new(Unconfigured),
        };
        let drafts = draft_api::routes(d.clone(), self.sources.as_ref().map(|s| s.pool.clone()))
            .merge(rules_api::routes(d.clone()))
            .merge(edit_api::routes(d.clone()))
            .merge(deploy_api::routes(d.clone()))
            .merge(split_api::routes(d.clone()))
            .merge(field_api::routes(d.clone()))
            .merge(draft_history::routes(d.clone()));
        // A rule's test reads the catalog (to lint) and queues a job;
        // deleting reaches the catalog, PostgreSQL and the lake (0055).
        let tests = match &self.sources {
            Some(s) => rule_test::routes(s.pool.clone(), d.clone()).merge(delete_api::routes(
                s.pool.clone(),
                d,
                s.catalog.clone(),
                self.workers.as_ref().and_then(|(_, w)| w.tables.clone()),
            )),
            None => Router::new(),
        };
        drafts.merge(tests).merge(match &self.sources {
            Some(s) => source_api::routes(s.pool.clone(), s.sealer.clone(), s.catalog.clone())
                .merge(cut_api::routes(s.pool.clone(), s.catalog.clone()))
                .merge(dataset_api::routes(s.pool.clone(), s.catalog.clone()))
                .merge(chart_api::routes(s.pool.clone(), s.catalog.clone()))
                .merge(sla_api::routes(s.pool.clone(), s.catalog.clone()))
                .merge(schedule_api::routes(s.pool.clone(), s.catalog.clone()))
                .merge(map_api::routes(
                    s.pool.clone(),
                    self.patterns.clone(),
                    s.catalog.clone(),
                ))
                .merge(tables_api::routes(s.pool.clone()))
                .merge(records_api::routes(s.pool.clone(), s.catalog.clone()))
                .merge(console_api::routes(s.pool.clone()))
                .merge(lake_tables::routes(s.pool.clone())),
            None => Router::new(),
        })
    }
}
