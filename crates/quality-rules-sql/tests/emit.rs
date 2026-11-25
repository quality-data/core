//! What the writer emits: names quoted, values escaped and checked, the cut
//! bound, and the engines' differences (decision 0033, point 6).

use quality_rules_sql::{expr, quote_ident, quote_text, Engine, Error, Table, Tables};
use quality_rules_tree::{parse_expr, Expr, Function, Literal};

fn sql(text: &str, engine: Engine) -> String {
    expr(&parse_expr(text).unwrap(), engine, &Tables::new())
        .unwrap()
        .text
}

fn both(text: &str) -> String {
    let d = sql(text, Engine::DuckDb);
    assert_eq!(d, sql(text, Engine::Trino), "{text}");
    d
}

#[test]
fn quotes_names_and_texts() {
    assert_eq!(
        quote_ident("Tipo \"raro\"").unwrap(),
        "\"Tipo \"\"raro\"\"\""
    );
    assert_eq!(quote_text("O'Brien").unwrap(), "'O''Brien'");
    assert_eq!(quote_text("a\\b").unwrap(), "'a\\b'");
    assert_eq!(quote_text("a\u{0}b"), Err(Error::ControlCharacter));
    assert_eq!(quote_ident("a\nb"), Err(Error::ControlCharacter));
    assert_eq!(
        both("\"Campo con espacio\" = 'it''s'"),
        "(\"Campo con espacio\" = 'it''s')"
    );
}

#[test]
fn checks_numbers_and_dates_it_did_not_parse() {
    let number = |n: &str| {
        expr(
            &Expr::Literal(Literal::Number(n.into())),
            Engine::DuckDb,
            &Tables::new(),
        )
    };
    assert_eq!(number("12.5e-3").unwrap().text, "12.5e-3");
    assert_eq!(
        number("1; DROP TABLE t"),
        Err(Error::BadNumber("1; DROP TABLE t".into()))
    );
    let date = expr(
        &Expr::Literal(Literal::Date("2024-01-31' OR '1".into())),
        Engine::Trino,
        &Tables::new(),
    );
    assert!(matches!(date, Err(Error::BadDate(_))));
}

#[test]
fn binds_the_cut() {
    let s = expr(
        &parse_expr("a < :cut AND b > :cut - INTERVAL 13 MONTH").unwrap(),
        Engine::Trino,
        &Tables::new(),
    )
    .unwrap();
    assert_eq!(s.cut_params, 2);
    assert_eq!(
        s.text,
        "((\"a\" < CAST(? AS DATE)) AND (\"b\" > (CAST(? AS DATE) - INTERVAL '13' MONTH)))"
    );
}

#[test]
fn writes_what_the_engines_spell_differently() {
    assert_eq!(
        sql("regexp_like(x, '3\\d{9}')", Engine::DuckDb),
        "regexp_matches(CAST(\"x\" AS VARCHAR), '^(?:3\\d{9})$')"
    );
    assert_eq!(
        sql("regexp_like(x, '3\\d{9}')", Engine::Trino),
        "regexp_like(CAST(\"x\" AS VARCHAR), '^(?:3\\d{9})$')"
    );
    let n = |engine| {
        expr(
            &Expr::Normalized(Box::new(Expr::Field("d".into()))),
            engine,
            &Tables::new(),
        )
        .unwrap()
        .text
    };
    assert!(n(Engine::DuckDb).ends_with("'', 'g'))"));
    assert!(n(Engine::Trino).ends_with("''))"));
}

#[test]
fn writes_the_platforms_functions() {
    assert_eq!(
        both("is_empty(x)"),
        "(\"x\" IS NULL OR trim(BOTH concat(' ', chr(160)) FROM CAST(\"x\" AS VARCHAR)) = '')"
    );
    assert_eq!(both("x / 2"), "(CAST(\"x\" AS DOUBLE) / 2)");
    assert_eq!(both("CAST(x AS INTEGER)"), "TRY_CAST(\"x\" AS BIGINT)");
    assert_eq!(both("x LIKE 'a%'"), "(\"x\" LIKE 'a%' ESCAPE '\\')");
    assert!(both("years_between(a, :cut)")
        .starts_with("(year(CAST(? AS DATE)) - year(\"a\") - CASE WHEN"));
    let dup = Expr::Duplicated(vec![Expr::Field("k".into())]);
    assert_eq!(
        expr(&dup, Engine::Trino, &Tables::new()).unwrap().text,
        "(count(*) OVER (PARTITION BY \"k\") > 1)"
    );
}

#[test]
fn a_reference_reads_the_table_the_platform_resolved() {
    let e = Expr::Call {
        function: Function::InReference,
        args: vec![
            Expr::Field("CodigoAsesor".into()),
            Expr::Literal(Literal::Text("advisors.Codigo".into())),
        ],
    };
    let mut tables = Tables::new();
    assert_eq!(
        expr(&e, Engine::Trino, &tables),
        Err(Error::UnknownReference("advisors.Codigo".into()))
    );
    tables.insert(
        "advisors".into(),
        Table {
            path: vec!["lake".into(), "advisors_silver".into(), "advisors".into()],
            filter: None,
        },
    );
    assert_eq!(
        expr(&e, Engine::Trino, &tables).unwrap().text,
        "(\"CodigoAsesor\" IN (SELECT \"Codigo\" FROM \"lake\".\"advisors_silver\".\"advisors\" WHERE \"Codigo\" IS NOT NULL))"
    );
}

#[test]
fn an_unexpanded_list_is_refused() {
    let e = Expr::Call {
        function: Function::InList,
        args: vec![
            Expr::Field("x".into()),
            Expr::Literal(Literal::Text("l".into())),
        ],
    };
    assert_eq!(
        expr(&e, Engine::DuckDb, &Tables::new()),
        Err(Error::NotExpanded("in_list"))
    );
    let empty = Expr::InList {
        expr: Box::new(Expr::Field("x".into())),
        list: vec![],
        negated: true,
    };
    assert_eq!(
        expr(&empty, Engine::DuckDb, &Tables::new()).unwrap().text,
        "TRUE"
    );
}

#[test]
fn a_reference_may_keep_only_some_records() {
    let e = parse_expr("in_reference(Codigo, 'activos.Codigo')").unwrap();
    let mut tables = Tables::new();
    let filter = Some(parse_expr("Estado = 'ACTIVO' AND Alta < :cut").unwrap());
    tables.insert(
        "activos".into(),
        Table {
            path: vec!["asesores".into()],
            filter,
        },
    );
    let s = expr(&e, Engine::DuckDb, &tables).unwrap();
    assert_eq!(
        s.text,
        "(\"Codigo\" IN (SELECT \"Codigo\" FROM \"asesores\" WHERE \"Codigo\" IS NOT NULL AND ((\"Estado\" = 'ACTIVO') AND (\"Alta\" < CAST(? AS DATE)))))"
    );
    assert_eq!(s.cut_params, 1);
}
