//! The semantics of decision 0033 as cases: synthetic rows, rules in YAML,
//! and the result each rule must give on each row (`None` does not apply).
//! The DuckDB test runs them, and the contract tier A runs them on Trino
//! (decision 0090). Shared by both.

#![allow(dead_code)]

use quality_rules_compile::{compile, FieldType, Schema};
use quality_rules_sql::{quote_ident, quote_text, Engine, Tables};

pub(crate) const CUT: &str = "2026-10-31";

pub(crate) struct Case {
    pub(crate) name: &'static str,
    pub(crate) fields: &'static [(&'static str, FieldType)],
    pub(crate) rows: &'static [&'static [Option<&'static str>]],
    pub(crate) rules: &'static str,
    /// Per row, one result per compiled rule.
    pub(crate) expected: &'static [&'static [Option<bool>]],
}

const HEAD: &str = "dataset: clients
populations:
  individuals: client_type = 'PN'
placeholders:
  text: [ACTUALIZAR]
  date: [\"1900-01-01\"]
  email: [\"notiene@*\"]
tables:
  activity_ciiu:
    when: Actividad
    then: Ciiu
    rows:
      - { when: JUBILADO/PENSIONADO, allowed: [\"8430\"] }
      - { when: \"*\", forbidden: [\"8430\"] }
rules:
";

const NBSP: &str = "\u{a0}";

/// The query that gives each row's results, in row order, and how many
/// times it binds the cut.
pub(crate) fn query(case: &Case, engine: Engine) -> (String, usize) {
    let mut schema = Schema::default();
    for (name, t) in case.fields {
        schema.fields.insert((*name).to_string(), *t);
    }
    // The age is derived only where the case has a birth date.
    let derived = if case.fields.iter().any(|(n, _)| *n == "Nacimiento") {
        "derived:\n  age: years_between(Nacimiento, :cut)\n"
    } else {
        ""
    };
    let yaml = format!("{derived}{HEAD}{}", case.rules);
    let file = quality_rules_model::parse(&yaml).expect(case.name);
    let (compiled, problems) = compile(&file, &schema);
    assert!(problems.is_empty(), "{}: {problems:?}", case.name);
    let mut cut_params = 0;
    let columns: Vec<String> = compiled
        .iter()
        .map(|c| {
            let sql = quality_rules_sql::rule(c, engine, &Tables::new()).expect(case.name);
            cut_params += sql.cut_params;
            sql.text
        })
        .collect();
    let names: Vec<String> = case
        .fields
        .iter()
        .map(|(n, _)| quote_ident(n).unwrap())
        .collect();
    let rows: Vec<String> = case
        .rows
        .iter()
        .enumerate()
        .map(|(i, row)| {
            let cells: Vec<String> = case
                .fields
                .iter()
                .zip(row.iter())
                .map(|((_, t), v)| cell(*t, *v))
                .collect();
            format!("({i}, {})", cells.join(", "))
        })
        .collect();
    let sql = format!(
        "SELECT {} FROM (VALUES {}) AS t(\"__row\", {}) ORDER BY \"__row\"",
        columns.join(", "),
        rows.join(", "),
        names.join(", ")
    );
    (sql, cut_params)
}

fn cell(t: FieldType, v: Option<&str>) -> String {
    let (literal, ty) = match t {
        FieldType::Text | FieldType::Email => (v.map(|s| quote_text(s).unwrap()), "VARCHAR"),
        FieldType::Number => (v.map(str::to_string), "DOUBLE"),
        FieldType::Date => (v.map(|d| format!("DATE '{d}'")), "DATE"),
        FieldType::Boolean => (v.map(str::to_string), "BOOLEAN"),
    };
    format!("CAST({} AS {ty})", literal.unwrap_or_else(|| "NULL".into()))
}

const T: Option<bool> = Some(true);
const F: Option<bool> = Some(false);
const N: Option<bool> = None;

pub(crate) fn cases() -> Vec<Case> {
    [one_field(), across_fields(), attacks()]
        .into_iter()
        .flatten()
        .collect()
}

/// The patterns of one value.
fn one_field() -> Vec<Case> {
    use FieldType::*;
    vec![
        Case {
            name: "domain skips an empty value, compares trimmed and case-insensitive",
            fields: &[("Actividad", Text)],
            rows: &[&[Some("EMPLEADO")], &[None], &[Some("")], &[Some("DESEMPLEADO")], &[Some(" empleado ")], &[Some("pensionado\u{a0}")]],
            rules: "  - { id: D, field: Actividad, check: domain, in: [EMPLEADO, PENSIONADO] }\n",
            expected: &[&[F], &[N], &[N], &[T], &[F], &[F]],
        },
        Case {
            name: "required: NULL, blanks, the no-break space and fillers are empty",
            fields: &[("Empresa", Text)],
            rows: &[&[Some("ACME")], &[None], &[Some("")], &[Some("   ")], &[Some(NBSP)], &[Some("actualizar ")]],
            rules: "  - { id: R, field: Empresa, check: required }\n",
            expected: &[&[F], &[T], &[T], &[T], &[T], &[T]],
        },
        Case {
            name: "format: anchored to the whole trimmed value",
            fields: &[("Celular", Text)],
            rows: &[&[Some("3001234567")], &[Some("x3001234567")], &[Some("30012345678")], &[Some(" 3001234567 ")], &[None]],
            rules: "  - { id: F, field: Celular, check: format, pattern: '3\\d{9}' }\n  - { id: M, field: Celular, check: format, named: mobile_co }\n",
            expected: &[&[F, F], &[T, T], &[T, T], &[F, F], &[N, N]],
        },
        Case {
            name: "placeholder: e-mail fillers with `*`",
            fields: &[("Correo", Email)],
            rows: &[&[Some("notiene@x.com")], &[Some("a@x.co")], &[Some("NOTIENE@Y")], &[Some("Actualizar")], &[None]],
            rules: "  - { id: P, field: Correo, check: placeholder }\n  - { id: E, field: Correo, check: format, named: email }\n",
            expected: &[&[T, F], &[F, F], &[T, T], &[T, T], &[N, N]],
        },
        Case {
            name: "unique on a normalized key, in the same pass",
            fields: &[("Documento", Text)],
            rows: &[&[Some("1.234")], &[Some("1234")], &[Some("999")], &[None]],
            rules: "  - { id: U, field: Documento, check: unique, normalize: true }\n",
            expected: &[&[T], &[T], &[F], &[N]],
        },
    ]
}

/// Populations, dates, tables, sums and implications.
fn across_fields() -> Vec<Case> {
    use FieldType::*;
    vec![
        Case {
            name: "population and condition: does not apply outside them",
            fields: &[("client_type", Text), ("Actividad", Text), ("Empresa", Text)],
            rows: &[
                &[Some("PN"), Some("EMPLEADO"), None],
                &[Some("PN"), Some("EMPLEADO"), Some("ACME")],
                &[Some("PJ"), Some("EMPLEADO"), None],
                &[Some("PN"), Some("PENSIONADO"), None],
                &[Some("PN"), None, None],
            ],
            rules: "  - { id: R, field: Empresa, check: required, population: individuals, when: \"Actividad = 'EMPLEADO'\" }\n",
            expected: &[&[T], &[F], &[N], &[N], &[N]],
        },
        Case {
            name: "range against another field and the cut; fresh; the age",
            fields: &[("Nacimiento", Date), ("Expedicion", Date), ("Actualizacion", Date)],
            rows: &[
                &[Some("2000-02-29"), Some("2018-02-28"), Some("2026-01-31")],
                &[Some("2000-02-29"), Some("2018-02-27"), Some("2025-09-30")],
                &[Some("2000-01-01"), Some("2026-11-01"), Some("1900-01-01")],
                &[Some("2010-11-01"), None, None],
            ],
            rules: "  - { id: X, field: Expedicion, check: range, min: Nacimiento + INTERVAL 18 YEAR, max: ':cut' }\n  - { id: A, field: Actualizacion, check: fresh, max_age: 13 months }\n  - { id: Y, check: expression, fails: age < 18, dimension: accuracy }\n",
            expected: &[&[F, F, F], &[T, F, F], &[T, T, F], &[N, N, T]],
        },
        Case {
            name: "combination: one failure whichever row",
            fields: &[("Actividad", Text), ("Ciiu", Text)],
            rows: &[
                &[Some("JUBILADO/PENSIONADO"), Some("8430")],
                &[Some("JUBILADO/PENSIONADO"), Some("4711")],
                &[Some("EMPLEADO"), Some("8430")],
                &[Some("EMPLEADO"), Some("4711")],
                &[None, Some("8430")],
            ],
            rules: "  - { id: C, check: combination, table: activity_ciiu }\n",
            expected: &[&[F], &[T], &[T], &[F], &[N]],
        },
        Case {
            name: "sum with tolerance; implies both ways",
            fields: &[("Activo", Number), ("Pasivo", Number), ("Patrimonio", Number), ("EsPep", Text), ("Cargo", Text)],
            rows: &[
                &[Some("100"), Some("40"), Some("60"), Some("SI"), Some("Alcalde")],
                &[Some("100"), Some("40"), Some("60.5"), Some("NO"), Some("Alcalde")],
                &[Some("100"), Some("40"), Some("75"), Some("SI"), None],
                &[Some("100"), None, Some("60"), Some("NO"), None],
            ],
            rules: "  - { id: S, field: Patrimonio, check: sum, parts: [Activo, '- Pasivo'], tolerance: 1 }\n  - { id: I, check: implies, if: \"EsPep = 'SI'\", then: not is_empty(Cargo), both_ways: true }\n",
            expected: &[&[F, F], &[F, T], &[T, T], &[N, F]],
        },
        Case {
            name: "length and named formats",
            fields: &[("Nombre", Text)],
            rows: &[&[Some("José Pérez")], &[Some("José  Pérez")], &[Some("Ñandú")], &[Some("ana")], &[Some("--")]],
            rules: "  - { id: P, field: Nombre, check: format, named: person_name }\n  - { id: C, field: Nombre, check: format, named: capitalized }\n  - { id: S, field: Nombre, check: format, named: not_only_symbols }\n  - { id: L, field: Nombre, check: length, min: 3 }\n",
            expected: &[&[F, F, F, F], &[T, F, F, F], &[F, F, F, F], &[F, T, F, F], &[T, T, T, T]],
        },
    ]
}

/// Quotes, comments and backslashes in names and texts.
fn attacks() -> Vec<Case> {
    use FieldType::*;
    vec![
        Case {
            name: "attacks stay values: quotes, comments, backslashes in names and texts",
            fields: &[("Tipo \"raro\"", Text), ("Nota", Text)],
            rows: &[
                &[Some("O'Brien"), Some("'); DROP TABLE t; --")],
                &[Some("x"), Some("a\\b")],
                &[Some("\u{2019}) OR 1=1 --"), Some("/* y */")],
            ],
            rules: "  - { id: Q, field: 'Tipo \"raro\"', check: domain, in: [\"O'Brien\", \"\u{2019}) OR 1=1 --\"] }\n  - { id: B, field: Nota, check: domain, in: [\"'); DROP TABLE t; --\", 'a\\b'] }\n",
            expected: &[&[F, F], &[T, F], &[F, T]],
        },
    ]
}
