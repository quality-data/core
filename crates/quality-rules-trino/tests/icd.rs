//! The ICD's statements on Trino (decision 0090, step 11): one scratch
//! insert with the keys hashed in the engine, and the reads back.

use quality::mapping::Mapping;
use quality_rules_compile::{compile, FieldType, Schema};
use quality_rules_sql::Tables;
use quality_rules_trino::{
    cells, entities, groups, means, measured, scratch_delete, scratch_insert, scratch_rows,
    IcdRule, FINDINGS_KEY, SCRATCH,
};

const MAPPING: &str = "dataset: loans
source: loans
fields:
  Doc: { column: doc, type: text, semantic: document, roles: [entity] }
  Type: { column: kind, type: text, roles: [segment, dimension] }
  Phone: { column: phone, type: text, priority: critical, expires: true, roles: [sfc] }
  Amount: { column: amount, type: number, priority: high }
  Updated: { column: updated, type: date, roles: [updated] }
";

const RULES: &str = "dataset: loans
rules:
  - { id: P1, field: Phone, check: required, severity: blocking, alert: a }
  - { id: A1, field: Amount, check: required, severity: major, alert: a }
  - id: X1
    check: expression
    fails: Amount > 1000 AND Type = 'PN'
    dimension: consistency
    severity: minor
    alert: a
";

const JOB: &str = "0b9c6a1e-0000-4000-8000-000000000001";

fn setup() -> (Mapping, Vec<IcdRule>) {
    let mapping = Mapping::parse(MAPPING).unwrap();
    let mut schema = Schema::default();
    for (name, t) in [
        ("Doc", FieldType::Text),
        ("Type", FieldType::Text),
        ("Phone", FieldType::Text),
        ("Amount", FieldType::Number),
        ("Updated", FieldType::Date),
    ] {
        schema.fields.insert(name.into(), t);
    }
    let file = quality_rules_model::parse(RULES).unwrap();
    let (compiled, problems) = compile(&file, &schema);
    assert!(problems.is_empty(), "{problems:?}");
    let rules = compiled
        .iter()
        .map(|c| IcdRule::of(c, &Tables::new()).unwrap())
        .collect();
    (mapping, rules)
}

#[test]
fn the_scratch_insert_evaluates_once_and_hashes_in_the_engine() {
    let (mapping, rules) = setup();
    let s = scratch_insert(JOB, "loans", "silver", &mapping, &rules).unwrap();
    assert!(s.sql.starts_with(&format!(
        "INSERT INTO {SCRATCH} (job_id, dataset, k, r_key, s, a, serious, act, icd, r, f, g, c)"
    )));
    assert!(s.sql.contains(&format!(
        "CROSS JOIN (SELECT \"key\" FROM {FINDINGS_KEY}) h"
    )));
    assert_eq!(
        s.sql.matches("hmac_sha256(to_utf8(").count(),
        2,
        "both keys"
    );
    assert!(s
        .sql
        .contains("CAST(ARRAY[\"r0\", \"r1\", \"r2\"] AS ARRAY(BOOLEAN))"));
    assert!(s.sql.contains("trim(LEADING '0' FROM regexp_replace("));
    assert!(s
        .sql
        .contains("CASE WHEN \"Updated\" IS NULL THEN NULL ELSE power(DOUBLE '0.5'"));
    for word in ["::", "GROUP BY ALL", "'g'", "mode(", "greatest(NULL"] {
        assert!(!s.sql.contains(word), "{word}");
    }
    let rule_cuts: usize = rules.iter().map(|r| r.sql.cut_params).sum();
    assert_eq!(
        s.cut_params,
        rule_cuts + 1,
        "the rules' and the A component's"
    );
    assert_eq!(s.sql.matches('?').count(), s.cut_params);
}

#[test]
fn the_reads_back_name_the_job_and_the_arrays_by_position() {
    let (mapping, rules) = setup();
    let fields = measured(&mapping, &rules);
    let names: Vec<&str> = fields.iter().map(|f| f.name.as_str()).collect();
    assert_eq!(names, ["Amount", "Phone", "Type"]);
    let at = format!("{SCRATCH} WHERE job_id = '{JOB}'");
    let m = means(JOB, &fields).unwrap();
    assert_eq!(
        m.sql,
        format!(
            "SELECT avg(icd), avg(element_at(f, 1)), avg(element_at(f, 2)), \
             avg(element_at(f, 3)) FROM {at}"
        )
    );
    let e = entities(JOB, &fields).unwrap().sql;
    assert!(
        e.contains("min(element_at(f, 2)) AS \"e1\""),
        "Phone is critical"
    );
    assert!(e.contains("ORDER BY n DESC NULLS LAST, s ASC NULLS LAST"));
    let g = groups(JOB, &mapping).unwrap();
    assert_eq!(g.len(), 1);
    assert!(g[0].sql.contains("GROUP BY element_at(g, 1)"));
    assert!(cells(JOB, 1).unwrap().sql.contains("element_at(c, 1)"));
    assert_eq!(
        scratch_rows(JOB).unwrap().sql,
        format!("SELECT count(*) FROM {at}")
    );
    assert_eq!(
        scratch_delete(JOB).unwrap().sql,
        format!("DELETE FROM {at}")
    );
    assert!(scratch_delete("a'; DROP")
        .unwrap()
        .sql
        .ends_with("'a''; DROP'"));
}
