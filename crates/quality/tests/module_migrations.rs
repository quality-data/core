//! The quality module's migrations on PostgreSQL, through the module seam:
//! applied once after infrastructure's, idempotent, refused if changed.

use contracts::{Migration, Module, ModuleRegistry};
use infrastructure::module_migrations::{ModuleMigrationError, PgModuleMigrator};
use quality::module::QualityModule;

async fn migrate(registry: &ModuleRegistry, pool: &infrastructure::PgPool) -> Vec<String> {
    let mut migrator = PgModuleMigrator::load(pool).await.unwrap();
    registry.run_migrations(&mut migrator).unwrap();
    migrator.run(pool).await.unwrap()
}

#[tokio::test]
async fn the_queue_table_is_created_once_with_forced_row_security() {
    let db = testkit::spawn_test_db().await;
    // The installer creates the queue's role before the migrations grant it.
    sqlx::query("CREATE ROLE qd_queue LOGIN")
        .execute(&db.pool)
        .await
        .unwrap();
    let registry = ModuleRegistry::new().register(QualityModule::default());
    assert_eq!(
        migrate(&registry, &db.pool).await,
        [
            "quality::0001_jobs",
            "quality::0002_queue",
            "quality::0003_results",
            "quality::0004_schedules",
            "quality::0005_manifests",
            "quality::0006_sources",
            "quality::0007_source_tables",
            "quality::0008_rule_tests",
            "quality::0009_lake_tables",
            "quality::0010_source_records",
            "quality::0011_schedule_triggers",
            "quality::0012_console",
            "quality::0013_rule_results_grouping",
            "quality::0014_field_quality",
            "quality::0015_entity_keys",
            "quality::0016_icd",
            "quality::0017_recency",
            "quality::0018_group_metrics",
            "quality::0019_distribution_checks",
            "quality::0020_job_stats",
            "quality::0021_cells",
            "quality::0022_queue_findings",
            "quality::0023_remediation_slas",
            "quality::0024_queue_machines",
            "quality::0025_trino_hold",
            "quality::0026_jobs_uses_trino",
            "quality::0027_claim_in_maintenance",
            "quality::0028_claim_without_trino_hold",
            "quality::0029_gold_run_seq",
            "quality::0030_job_bundles",
            "quality::0031_field_quality_floating",
            "quality::0032_job_reports"
        ]
    );
    assert_eq!(migrate(&registry, &db.pool).await, Vec::<String>::new());

    for table in [
        "quality.jobs",
        "quality.run_summary",
        "quality.rule_results",
        "quality.schedules",
        "quality.cuts",
        "quality.sources",
        "quality.source_tables",
        "quality.rule_tests",
        "quality.lake_tables",
        "quality.field_quality",
        "quality.entity_keys",
        "quality.entity_icd",
        "quality.group_metrics",
        "quality.distribution_checks",
        "quality.job_stats",
        "quality.cells",
        "quality.queue_findings",
        "quality.remediation_slas",
        "quality.trino_holds",
    ] {
        let (enabled, forced): (bool, bool) = sqlx::query_as(
            "SELECT relrowsecurity, relforcerowsecurity FROM pg_class WHERE oid = $1::regclass",
        )
        .bind(table)
        .fetch_one(&db.pool)
        .await
        .unwrap();
        assert!(enabled && forced, "{table}");
    }
    let (tenant,): (String,) = sqlx::query_as(
        "INSERT INTO quality.jobs (kind, dataset) VALUES ('evaluate', 'clientes') RETURNING tenant_id::text",
    )
    .fetch_one(&db.pool)
    .await
    .unwrap();
    assert_eq!(tenant, "00000000-0000-0000-0000-000000000001");
}

/// A module whose first migration changed after it was applied.
struct Edited(&'static str);

impl Module for Edited {
    fn name(&self) -> &'static str {
        "edited"
    }
    fn migrations(&self) -> Vec<Migration> {
        vec![Migration::new("0001_a", self.0)]
    }
}

#[tokio::test]
async fn a_migration_changed_after_it_was_applied_is_refused() {
    let db = testkit::spawn_test_db().await;
    migrate(
        &ModuleRegistry::new().register(Edited("CREATE TABLE a (b int);")),
        &db.pool,
    )
    .await;
    let migrator = PgModuleMigrator::load(&db.pool).await.unwrap();
    let changed = migrator.verify([("edited::0001_a".to_string(), "CREATE TABLE a (b bigint);")]);
    assert!(matches!(changed, Err(ModuleMigrationError::Changed { id }) if id == "edited::0001_a"));
    assert!(migrator
        .verify([("edited::0001_a".to_string(), "CREATE TABLE a (b int);")])
        .is_ok());
}

/// Two processes starting together (bootstrap-admin and serve, or two
/// replicas) apply each migration once.
#[tokio::test]
async fn two_processes_migrating_together_apply_each_migration_once() {
    let db = testkit::spawn_test_db().await;
    sqlx::query("CREATE ROLE qd_queue LOGIN")
        .execute(&db.pool)
        .await
        .unwrap();
    let run = || async {
        let mut migrator = PgModuleMigrator::load(&db.pool).await.unwrap();
        ModuleRegistry::new()
            .register(QualityModule::default())
            .run_migrations(&mut migrator)
            .unwrap();
        migrator.run(&db.pool).await.unwrap()
    };
    let (a, b) = tokio::join!(run(), run());
    let mut applied: Vec<String> = a.into_iter().chain(b).collect();
    applied.sort();
    assert_eq!(
        applied,
        [
            "quality::0001_jobs",
            "quality::0002_queue",
            "quality::0003_results",
            "quality::0004_schedules",
            "quality::0005_manifests",
            "quality::0006_sources",
            "quality::0007_source_tables",
            "quality::0008_rule_tests",
            "quality::0009_lake_tables",
            "quality::0010_source_records",
            "quality::0011_schedule_triggers",
            "quality::0012_console",
            "quality::0013_rule_results_grouping",
            "quality::0014_field_quality",
            "quality::0015_entity_keys",
            "quality::0016_icd",
            "quality::0017_recency",
            "quality::0018_group_metrics",
            "quality::0019_distribution_checks",
            "quality::0020_job_stats",
            "quality::0021_cells",
            "quality::0022_queue_findings",
            "quality::0023_remediation_slas",
            "quality::0024_queue_machines",
            "quality::0025_trino_hold",
            "quality::0026_jobs_uses_trino",
            "quality::0027_claim_in_maintenance",
            "quality::0028_claim_without_trino_hold",
            "quality::0029_gold_run_seq",
            "quality::0030_job_bundles",
            "quality::0031_field_quality_floating",
            "quality::0032_job_reports"
        ]
    );
}
