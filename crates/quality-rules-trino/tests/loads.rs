//! Bronze and silver loads on Trino (decision 0090, point 5, step 13).

use quality::layers::SourceClass;
use quality::mapping::Mapping;
use quality_rules_trino::{
    accepted, create_silver, delete_load, derive_silver, loads_of_cut, migrate_to_loads, prepare,
    Accepted, Cut, Pinned, TablePath,
};

fn cut() -> Cut {
    Cut::parse("2026-10-31").unwrap()
}

fn path(ns: &str) -> TablePath {
    TablePath {
        catalog: "lake".into(),
        namespace: ns.into(),
        table: "clients_2026".into(),
    }
}

const MAPPING: &str = "dataset: clients
source: clients_2026
fields:
  Documento: { column: doc, type: text, semantic: document, roles: [entity] }
  Saldo: { column: balance, type: number }
";

#[test]
fn a_reader_takes_the_accepted_load_pinned() {
    let p = Pinned {
        table: path("clients_bronze"),
        snapshot: 9,
    };
    let at = "\"lake\".\"clients_bronze\".\"clients_2026\" FOR VERSION AS OF 9";
    let load = Accepted {
        cut: &cut(),
        load: Some("job-1"),
    };
    assert_eq!(
        accepted(&p, &load).unwrap(),
        format!("(SELECT * FROM {at} WHERE cut = DATE '2026-10-31' AND _load = 'job-1')")
    );
    let legacy = Accepted {
        cut: &cut(),
        load: None,
    };
    assert!(accepted(&p, &legacy)
        .unwrap()
        .ends_with("AND _load IS NULL)"));
}

#[test]
fn silver_is_derived_by_one_insert_with_its_digest_and_load() {
    let m = Mapping::parse(MAPPING).unwrap();
    let create = create_silver(&path("clients_silver"), &m).unwrap().sql;
    assert!(create.ends_with(
        "\"_mapping\" VARCHAR, cut DATE, _load VARCHAR) WITH (partitioning = ARRAY['cut', '_load'])"
    ));
    let p = Pinned {
        table: path("clients_bronze"),
        snapshot: 9,
    };
    let a = Accepted {
        cut: &cut(),
        load: Some("job-1"),
    };
    let s = derive_silver(
        &path("clients_silver"),
        &m,
        (&p, &a),
        "job-2",
        "d1g3st",
        |_| SourceClass::Other,
    )
    .unwrap()
    .sql;
    assert!(s.starts_with(
        "INSERT INTO \"lake\".\"clients_silver\".\"clients_2026\" (\"Documento\", \"Saldo\", \
         \"_entity_key\", \"_entity_key_normalized\", \"_mapping\", cut, _load) SELECT "
    ));
    assert!(s.contains("trim(LEADING '0' FROM regexp_replace("));
    assert!(s.contains("'d1g3st', DATE '2026-10-31', 'job-2' FROM (SELECT "));
    assert!(!s.contains("DELETE") && !s.contains("MERGE"));
}

#[test]
fn an_old_table_moves_to_loads_and_a_superseded_load_goes_by_partition() {
    let steps: Vec<String> = migrate_to_loads(&path("clients_bronze"))
        .unwrap()
        .into_iter()
        .map(|s| s.sql)
        .collect();
    assert!(steps[0].ends_with("ADD COLUMN IF NOT EXISTS _load VARCHAR"));
    assert!(steps[1].ends_with("SET PROPERTIES partitioning = ARRAY['cut', '_load']"));
    assert!(steps[2].ends_with("EXECUTE optimize"));
    assert_eq!(
        loads_of_cut(&path("clients_bronze"), &cut()).unwrap().sql,
        "SELECT partition._load FROM \"lake\".\"clients_bronze\".\"clients_2026$partitions\" \
         WHERE partition.cut = DATE '2026-10-31'"
    );
    assert!(delete_load(&path("clients_bronze"), &cut(), "job-1")
        .unwrap()
        .sql
        .ends_with("WHERE cut = DATE '2026-10-31' AND _load = 'job-1'"));
}

/// Rulings 66 and 68: a `prepare` job creates bronze (typed as Trino lists
/// the source) and silver if missing, then adds any mapped column they
/// lack, so a changed mapping prepares again.
#[test]
fn prepare_creates_both_tables_then_adds_missing_columns() {
    let m = Mapping::parse(MAPPING).unwrap();
    let listed = [
        ("doc".to_string(), "varchar(20)".to_string()),
        ("balance".to_string(), "decimal(12,2)".to_string()),
        ("other".to_string(), "tinyint".to_string()),
    ];
    assert!(
        prepare(&path("b"), &path("s"), &m, &listed[..1]).is_err(),
        "balance not listed"
    );
    let steps: Vec<String> = prepare(
        &path("clients_bronze"),
        &path("clients_silver"),
        &m,
        &listed,
    )
    .unwrap()
    .into_iter()
    .map(|s| s.sql)
    .collect();
    assert_eq!(
        steps[0],
        "CREATE TABLE IF NOT EXISTS \"lake\".\"clients_bronze\".\"clients_2026\" \
         (\"doc\" varchar(20), \"balance\" decimal(12,2), cut DATE, _load VARCHAR) \
         WITH (partitioning = ARRAY['cut', '_load'])"
    );
    assert_eq!(
        steps[1],
        "ALTER TABLE \"lake\".\"clients_bronze\".\"clients_2026\" ADD COLUMN IF NOT EXISTS \"doc\" varchar(20)"
    );
    assert!(steps[3].starts_with("CREATE TABLE IF NOT EXISTS \"lake\".\"clients_silver\""));
    assert!(steps[3].contains("\"Documento\" VARCHAR, \"Saldo\" DOUBLE"));
    assert!(steps
        .iter()
        .skip(4)
        .all(|s| s.contains("ADD COLUMN IF NOT EXISTS")));
    assert!(steps.iter().all(|s| !s.contains("DROP")));
}
