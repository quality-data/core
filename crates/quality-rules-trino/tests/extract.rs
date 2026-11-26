//! Extraction through Trino (decision 0090, point 9, step 13).

use quality_rules_trino::{
    bronze_type, create_bronze, create_catalog, extract, split, Connector, Conversion, TablePath,
};

fn t(catalog: &str, ns: &str, table: &str) -> TablePath {
    TablePath {
        catalog: catalog.into(),
        namespace: ns.into(),
        table: table.into(),
    }
}

#[test]
fn the_type_table_says_what_bronze_keeps() {
    use Conversion::*;
    let cases = [
        ("timestamp(7)", "timestamp(6)", TruncatedPrecision),
        ("timestamp(3)", "timestamp(6)", Exact),
        (
            "timestamp(7) with time zone",
            "timestamp(6) with time zone",
            OffsetDropped,
        ),
        ("time(7)", "time(6)", TruncatedPrecision),
        ("tinyint", "smallint", Exact),
        ("number", "varchar", Text),
        ("char(5)", "char(5)", Exact),
        ("decimal(38,10)", "decimal(38,10)", Exact),
    ];
    for (from, to, c) in cases {
        assert_eq!(bronze_type(from), (to.to_string(), c), "{from}");
    }
}

#[test]
fn a_source_catalog_holds_no_secret() {
    let s = create_catalog("core1", Connector::Oracle, "jdbc:oracle:thin:@db:1521/ORCL")
        .unwrap()
        .sql;
    assert!(s.starts_with("CREATE CATALOG \"src_core1\" USING oracle WITH ("));
    assert!(s.contains("\"password-credential-name\" = 'password'"));
    assert!(s.contains("\"unsupported-type-handling\" = 'CONVERT_TO_VARCHAR'"));
    assert!(s.contains("\"oracle.connection-pool.enabled\" = 'false'"));
    assert!(!s.contains("\"connection-password\""));
    let q = create_catalog("x", Connector::SqlServer, "jdbc:sqlserver://h;encrypt=true")
        .unwrap()
        .sql;
    assert!(!q.contains("connection-pool"));
}

#[test]
fn a_load_is_one_insert_of_union_all_ranges() {
    let cols = vec![
        ("id".to_string(), "bigint".to_string()),
        ("at".to_string(), "timestamp(7)".to_string()),
    ];
    let bronze = t("lake", "loans_bronze", "loans");
    let create = create_bronze(&bronze, &cols).unwrap().sql;
    assert!(create.contains("\"at\" timestamp(6), cut DATE, _load VARCHAR"));
    assert!(create.ends_with("WITH (partitioning = ARRAY['cut', '_load'])"));
    let ranges = split(1, 100, 4);
    assert_eq!(
        ranges,
        [(1, Some(26)), (26, Some(51)), (51, Some(76)), (76, None)]
    );
    let s = extract(
        &bronze,
        &t("src_core1", "ops", "loans"),
        &cols,
        ("2026-10-31", "job-1"),
        Some(("id", &ranges)),
    )
    .unwrap()
    .sql;
    assert!(s.starts_with(
        "INSERT INTO \"lake\".\"loans_bronze\".\"loans\" (\"id\", \"at\", cut, _load) SELECT \
         CAST(\"id\" AS bigint) AS \"id\", CAST(\"at\" AS timestamp(6)) AS \"at\", \
         DATE '2026-10-31', 'job-1' FROM \"src_core1\".\"ops\".\"loans\" WHERE \"id\" >= 1"
    ));
    assert_eq!(s.matches(" UNION ALL ").count(), 3);
    assert!(s.ends_with("WHERE \"id\" >= 76"));
    let one = extract(
        &bronze,
        &t("src_core1", "ops", "loans"),
        &cols,
        ("2026-10-31", "job-1"),
        None,
    )
    .unwrap()
    .sql;
    assert!(!one.contains("UNION ALL") && !one.contains("WHERE"));
    assert_eq!(split(5, 5, 4), [(5, None)]);
}
