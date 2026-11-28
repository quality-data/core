//! The findings' statements on Trino (decision 0090, step 11): by one
//! `UNNEST` over the scratch rows, with the previous cut's join.

use quality::mapping::Mapping;
use quality_rules_compile::{compile, FieldType, Schema};
use quality_rules_sql::{Cut, Tables};
use quality_rules_trino::{finding_groups, findings, queues, Findings, IcdRule, SCRATCH};

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

fn cut() -> Cut {
    Cut::parse("2026-10-31").unwrap()
}

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
        .map(|c| IcdRule::of(c, &Tables::new(), &cut()).unwrap())
        .collect();
    (mapping, rules)
}

#[test]
fn the_findings_unnest_the_outcomes_and_carry_the_previous_since() {
    let (mapping, rules) = setup();
    let first = findings(JOB, &mapping, &rules, None, &cut()).unwrap();
    assert!(!first.sql.contains('?'));
    assert!(first
        .sql
        .contains("CROSS JOIN UNNEST(s.r) WITH ORDINALITY AS o(v, i)"));
    assert!(first
        .sql
        .contains("element_at(ARRAY['P1', 'A1', 'X1'], u.i)"));
    assert!(first.sql.ends_with("DATE '2026-10-31' AS since FROM f "));
    // Phone is the SFC cell: its closeness comes from the active rows.
    assert!(first
        .sql
        .contains("count(*) FILTER (WHERE act AND element_at(c, 1)) AS ok0"));
    assert!(
        first.sql.contains("WHEN 3 THEN greatest("),
        "X1 reads two fields"
    );
    for word in ["::", "GROUP BY ALL", "USING ("] {
        assert!(!first.sql.contains(word), "{word}");
    }
    let before = Findings {
        findings: "prev_f",
        groups: "prev_g",
    };
    let next = findings(JOB, &mapping, &rules, Some(before), &cut()).unwrap();
    assert!(next
        .sql
        .contains("LEFT JOIN prev_f q ON q.record = f.record AND q.rule = f.rule"));
    assert!(next.sql.contains("COALESCE(q.since, DATE '2026-10-31')"));
}

#[test]
fn the_groups_and_queues_read_the_cuts_findings() {
    let (mapping, _) = setup();
    let g = finding_groups(JOB, &mapping, "cur_f").unwrap();
    assert!(g.sql.contains(&format!(
        "SELECT r_key, 'Type', min(element_at(g, 1)) FROM {SCRATCH}"
    )));
    assert!(g.sql.contains("r_key IN (SELECT record FROM cur_f)"));
    let current = Findings {
        findings: "cur_f",
        groups: "cur_g",
    };
    let q = queues(&mapping, current, None).unwrap();
    assert!(q
        .sql
        .contains("FROM cur_f f JOIN cur_g g ON g.record = f.record"));
    assert!(!q.sql.contains("UNION ALL"));
    let before = Findings {
        findings: "prev_f",
        groups: "prev_g",
    };
    let q = queues(&mapping, current, Some(before)).unwrap();
    assert!(q
        .sql
        .contains("FROM prev_f p JOIN prev_g g ON g.record = p.record"));
    assert!(q.sql.contains("NOT EXISTS\n"), "{}", q.sql);
}
