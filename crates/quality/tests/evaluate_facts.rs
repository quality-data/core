//! What an evaluate's compile gathers (decision 0090, amendment A1 point
//! 3) on an ephemeral PostgreSQL, as the app's role, with a stand-in
//! catalog: the mapping and rules at its commit, the cut's load and the
//! previous cut; a dataset with no mapping, rules that do not compile and
//! rules that reference another dataset refuse the job. Synthetic names.

use async_trait::async_trait;
use chrono::NaiveDate;
use contracts::ModuleRegistry;
use infrastructure::module_migrations::PgModuleMigrator;
use quality::catalog::{Catalog, CatalogSnapshot, DatasetMapping, RulesSource};
use quality::job_inputs::{evaluate_facts, FactsError, ReferenceFacts};
use quality::module::QualityModule;
use sqlx::postgres::PgPoolOptions;

const MAPPING: &str = "dataset: loans\nsource: loans\nfields:\n  \
                       Code: { column: code, type: text }\n";
const RULES: &str = "dataset: loans\nrules:\n  \
                     - { id: R1, field: Code, check: required, severity: major, alert: a }\n";
const MISSING: &str = "dataset: loans\nreferences:\n  \
                       o: { dataset: nowhere, key: Code }\nrules: []\n";
const REFERENCES: &str = "dataset: loans\nreferences:\n  \
                          o: { dataset: offices, key: Code }\nrules: []\n";

/// `loans` with `rules`; `broken` with rules on a field it lacks.
struct Files(&'static str);

#[async_trait]
impl Catalog for Files {
    async fn snapshot(&self, dataset: &str) -> Result<Option<CatalogSnapshot>, String> {
        let rules = match dataset {
            "loans" => self.0,
            "broken" => {
                "dataset: broken\nrules:\n  \
                         - { id: R1, field: Nope, check: required, severity: major, alert: a }\n"
            }
            _ => return Ok(None),
        };
        Ok(Some(CatalogSnapshot {
            commit: "c0ffee".into(),
            mapping: MAPPING.replace("loans", dataset),
            rules: vec![RulesSource {
                file: "r.yaml".into(),
                yaml: rules.into(),
            }],
            others: if self.0 == REFERENCES {
                vec![DatasetMapping {
                    dataset: "offices".into(),
                    yaml: MAPPING.replace("loans", "offices"),
                }]
            } else {
                Vec::new()
            },
        }))
    }

    async fn mappings(&self) -> Result<Vec<DatasetMapping>, String> {
        Ok(Vec::new())
    }
}

fn day(d: &str) -> NaiveDate {
    d.parse().unwrap()
}

#[tokio::test]
async fn an_evaluate_reads_the_catalog_the_cuts_load_and_the_previous_cut() {
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
        "ALTER TABLE quality.cuts OWNER TO app",
        "ALTER TABLE quality.run_summary OWNER TO app",
    ] {
        sqlx::query(sql).execute(&db.pool).await.unwrap();
    }
    let app = PgPoolOptions::new()
        .max_connections(2)
        .connect_with(options)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO quality.cuts (dataset, cut, extract_job, due_at)
         VALUES ('loans', DATE '2026-10-31', '0b9c6a1e-0000-4000-8000-000000000003', now())",
    )
    .execute(&app)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO quality.run_summary (dataset, cut, job_id, records, duration_ms, rules_commit)
         VALUES ('loans', DATE '2026-08-31', gen_random_uuid(), 1, 1, 'a'),
                ('loans', DATE '2026-09-30', gen_random_uuid(), 1, 1, 'a'),
                ('loans', DATE '2026-10-31', gen_random_uuid(), 1, 1, 'a'),
                ('cards', DATE '2026-10-15', gen_random_uuid(), 1, 1, 'a')",
    )
    .execute(&app)
    .await
    .unwrap();

    let f = evaluate_facts(&app, &Files(RULES), "loans", day("2026-10-31"))
        .await
        .unwrap();
    assert_eq!(f.commit, "c0ffee");
    assert_eq!(f.mapping.dataset.value, "loans");
    assert_eq!(f.rules.len(), 1);
    assert_eq!(
        f.load.as_deref(),
        Some("0b9c6a1e-0000-4000-8000-000000000003")
    );
    assert_eq!(f.previous, Some(day("2026-09-30")));
    let first = evaluate_facts(&app, &Files(RULES), "loans", day("2026-08-31"))
        .await
        .unwrap();
    assert_eq!((first.load, first.previous), (None, None));

    let refs = evaluate_facts(&app, &Files(REFERENCES), "loans", day("2026-10-31")).await;
    assert_eq!(
        refs.unwrap_err(),
        FactsError::Refused("reference_cut_missing"),
        "offices has no such cut yet"
    );
    sqlx::query(
        "INSERT INTO quality.cuts (dataset, cut, extract_job, due_at)
         VALUES ('offices', DATE '2026-10-31', '0b9c6a1e-0000-4000-8000-000000000004', now())",
    )
    .execute(&app)
    .await
    .unwrap();
    let f = evaluate_facts(&app, &Files(REFERENCES), "loans", day("2026-10-31"))
        .await
        .unwrap();
    assert_eq!(f.tables["o"].path, ["lake", "offices_silver", "offices"]);
    assert_eq!(
        f.references,
        [ReferenceFacts {
            name: "o".into(),
            namespace: "offices_silver".into(),
            table: "offices".into(),
            load: Some("0b9c6a1e-0000-4000-8000-000000000004".into()),
        }]
    );
    for (catalog, dataset, class) in [
        (Files(RULES), "deposits", "no_mapping"),
        (Files(RULES), "broken", "rules_invalid"),
        (Files(MISSING), "loans", "reference_has_no_mapping"),
    ] {
        let refused = evaluate_facts(&app, &catalog, dataset, day("2026-10-31")).await;
        assert_eq!(
            refused.unwrap_err(),
            FactsError::Refused(class),
            "{dataset}"
        );
    }
}
