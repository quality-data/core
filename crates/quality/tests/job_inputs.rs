//! What a job's compile reads (decision 0090, amendment A1) on an
//! ephemeral PostgreSQL, as the app's role: gold's run order and a
//! dataset's SLAs, the installation's only. Synthetic names only.

use contracts::ModuleRegistry;
use infrastructure::module_migrations::PgModuleMigrator;
use quality::job_inputs::{dataset_slas, next_run_seq, DatasetSla};
use quality::module::QualityModule;
use sqlx::postgres::PgPoolOptions;

const OTHER: &str = "00000000-0000-0000-0000-0000000000ff";

#[tokio::test]
async fn a_compile_reads_the_run_order_and_the_datasets_slas() {
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
        "ALTER TABLE quality.remediation_slas OWNER TO app",
        "ALTER SEQUENCE quality.gold_run_seq OWNER TO app",
    ] {
        sqlx::query(sql).execute(&db.pool).await.unwrap();
    }
    sqlx::query(
        "INSERT INTO quality.remediation_slas (tenant_id, dataset, dimension, grp, days)
         VALUES ($1::uuid, 'loans', 'Office', NULL, 3)",
    )
    .bind(OTHER)
    .execute(&db.pool)
    .await
    .unwrap();
    let app = PgPoolOptions::new()
        .max_connections(2)
        .connect_with(options)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO quality.remediation_slas (dataset, dimension, grp, days) VALUES
         ('loans', 'Office', 'North', 10), ('loans', 'Office', NULL, 20),
         ('loans', 'Advisor', NULL, 5), ('cards', 'Office', NULL, 7)",
    )
    .execute(&app)
    .await
    .unwrap();

    let first = next_run_seq(&app).await.unwrap();
    assert_eq!(next_run_seq(&app).await.unwrap(), first + 1);
    let sla = |dimension: &str, grp: Option<&str>, days| DatasetSla {
        dimension: dimension.into(),
        grp: grp.map(Into::into),
        days,
    };
    assert_eq!(
        dataset_slas(&app, "loans").await.unwrap(),
        vec![
            sla("Advisor", None, 5),
            sla("Office", None, 20),
            sla("Office", Some("North"), 10),
        ],
        "this installation's, in a fixed order"
    );
    assert!(dataset_slas(&app, "ghost").await.unwrap().is_empty());
}
