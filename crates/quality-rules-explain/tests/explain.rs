//! Rules explained in Spanish and English, from synthetic rules.

use quality_rules_compile::{compile, Compiled, FieldType, Schema};
use quality_rules_explain::explain;
use quality_rules_model::{parse, NamedFormat};
use quality_rules_sql::Tables;
use quality_rules_tree::{parse_expr, Expr};
use texts::Language;

fn schema() -> Schema {
    let mut s = Schema::default();
    for (name, t) in [
        ("client_type", FieldType::Text),
        ("ActividadLaboral", FieldType::Text),
        ("NombreEmpresa", FieldType::Text),
        ("CodigoCiiu", FieldType::Text),
        ("Celular", FieldType::Text),
        ("Documento", FieldType::Text),
        ("Nacimiento", FieldType::Date),
        ("Expedicion", FieldType::Date),
    ] {
        s.fields.insert(name.into(), t);
    }
    s
}

fn rules(yaml: &str) -> Vec<Compiled> {
    let file = parse(&format!(
        "dataset: c\npopulations:\n  individuals: client_type = 'PN'\nplaceholders:\n  text: [ACTUALIZAR]\ntables:\n  t:\n    when: ActividadLaboral\n    then: CodigoCiiu\n    rows:\n      - {{ when: JUBILADO, allowed: [\"8430\"] }}\nrules:\n{yaml}"
    ))
    .unwrap();
    let (compiled, problems) = compile(&file, &schema());
    assert!(problems.is_empty(), "{problems:?}");
    compiled
}

fn es(yaml: &str) -> quality_rules_explain::Explanation {
    explain(&rules(yaml)[0], Language::Es, &Tables::new())
}

#[test]
fn explains_a_required_rule_in_spanish_and_english() {
    let rule = &rules("  - { id: R, field: NombreEmpresa, check: required, population: individuals, when: \"ActividadLaboral = 'EMPLEADO'\" }\n")[0];
    let e = explain(rule, Language::Es, &Tables::new());
    assert_eq!(
        e.applies,
        "client_type es «PN» y ActividadLaboral es «EMPLEADO»"
    );
    assert_eq!(
        e.fails,
        "NombreEmpresa está vacío o NombreEmpresa es uno de: «ACTUALIZAR»"
    );
    assert_eq!(e.skips, None);
    assert_eq!(
        (e.kind.as_str(), e.dimension.as_str(), e.severity.as_str()),
        ("de cruce", "completitud", "bloqueante")
    );
    let en = explain(rule, Language::En, &Tables::new());
    assert_eq!(
        en.applies,
        "client_type is «PN» and ActividadLaboral is «EMPLEADO»"
    );
    assert_eq!(
        en.fails,
        "NombreEmpresa is empty or NombreEmpresa is one of: «ACTUALIZAR»"
    );
    assert!(en
        .text(Language::En)
        .starts_with("Applies to: client_type is «PN»"));
    assert!(e.duckdb.unwrap().starts_with("CASE WHEN (NOT coalesce("));
    assert!(e.trino.is_some());
}

#[test]
fn explains_each_kind_of_pattern() {
    let d = es("  - { id: D, field: Celular, check: format, named: mobile_co }\n");
    assert_eq!(d.applies, "todos los registros");
    assert_eq!(
        d.fails,
        "Celular no es un celular colombiano (3 y nueve dígitos)"
    );
    assert_eq!(d.skips.as_deref(), Some("Celular está vacío"));
    assert_eq!(d.kind, "de campo");
    let r = es("  - { id: X, field: Expedicion, check: range, min: Nacimiento + INTERVAL 18 YEAR, max: ':cut' }\n");
    assert_eq!(
        r.fails,
        "Expedicion es menor que Nacimiento + 18 años o Expedicion es mayor que el corte"
    );
    let u = es("  - { id: U, field: Documento, check: unique, normalize: true }\n");
    assert_eq!(
        u.fails,
        "otro registro tiene el mismo Documento (solo letras y dígitos)"
    );
    let c = es("  - { id: C, check: combination, table: t }\n");
    assert_eq!(
        c.fails,
        "ActividadLaboral es uno de: «JUBILADO» y CodigoCiiu no es ninguno de: «8430»"
    );
    let i = es("  - { id: I, check: implies, if: \"ActividadLaboral = 'EMPLEADO' OR ActividadLaboral = 'INDEPENDIENTE'\", then: not is_empty(NombreEmpresa) }\n");
    assert_eq!(i.fails, "(ActividadLaboral es «EMPLEADO» o ActividadLaboral es «INDEPENDIENTE») y NombreEmpresa está vacío");
}

#[test]
fn lists_name_their_first_values() {
    let d = es("  - { id: D, field: CodigoCiiu, check: domain, in: [\"1\", \"2\", \"3\", \"4\", \"5\", \"6\", \"7\"] }\n");
    assert_eq!(
        d.fails,
        "CodigoCiiu no es ninguno de: «1», «2», «3», «4», «5» y 2 más"
    );
}

/// Every phrase has its text: a missing key would come back as the key.
#[test]
fn no_phrase_is_missing_in_either_language() {
    let exprs = [
        "a = 1 AND b <> 'x' OR c < 2 AND d <= 3 OR e > 4 AND f >= 5",
        "NOT (a IS NULL) AND b IS NOT NULL",
        "a NOT IN ('x') AND b BETWEEN 1 AND 2 AND c NOT BETWEEN 1 AND 2",
        "a LIKE 'x%' AND b NOT LIKE 'y'",
        "is_empty(a) AND NOT is_empty(b) AND regexp_like(c, 'x') AND NOT regexp_like(d, 'y')",
        "in_reference(a, 'r.c') AND NOT in_reference(b, 'r.c') AND in_list(c, 'l')",
        "days_since(a) > days_between(a, b) + years_between(a, :cut) + length(c) + abs(d) + round(e)",
        "coalesce(a, b) = TRUE OR c = FALSE OR d = NULL OR e = DATE '2024-01-31'",
        "a > :cut - INTERVAL 1 YEAR - INTERVAL 2 MONTH - INTERVAL 1 DAY",
    ];
    let mut trees: Vec<Expr> = exprs.iter().map(|e| parse_expr(e).unwrap()).collect();
    for format in [
        NamedFormat::Email,
        NamedFormat::MobileCo,
        NamedFormat::Divipola,
        NamedFormat::AddressCo,
        NamedFormat::PersonName,
        NamedFormat::NotOnlySymbols,
        NamedFormat::Capitalized,
        NamedFormat::NormalizedSpaces,
    ] {
        let m = Expr::Matches {
            expr: Box::new(Expr::Field("x".into())),
            format,
        };
        trees.push(Expr::Not(Box::new(m.clone())));
        trees.push(m);
    }
    trees.push(Expr::Duplicated(vec![Expr::Normalized(Box::new(
        Expr::Field("x".into()),
    ))]));
    let all_patterns = rules(
        "  - { id: A, field: Celular, check: required }\n  - { id: B, field: Documento, check: unique }\n  - { id: C, field: Expedicion, check: fresh, max_age: 2 years }\n  - { id: D, field: Celular, check: differs, from: Documento, severity: minor }\n  - { id: E, check: expression, fails: Celular = 'x', dimension: validity }\n  - { id: F, field: Celular, check: length, max: 3 }\n",
    );
    for language in [Language::Es, Language::En] {
        for tree in &trees {
            let rule = Compiled {
                fails: tree.clone(),
                ..all_patterns[0].clone()
            };
            let text = explain(&rule, language, &Tables::new()).text(language);
            assert!(!text.contains("quality.explain."), "{language:?}: {text}");
        }
        for rule in &all_patterns {
            let text = explain(rule, language, &Tables::new()).text(language);
            assert!(!text.contains("quality.explain."), "{language:?}: {text}");
        }
    }
}
