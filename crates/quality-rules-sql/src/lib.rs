//! The neutral tree written as each engine's SQL (decision 0019, point 3).
//! Only the tree reaches SQL, and only through here (decision 0033, point
//! 6): fields are quoted, values are escaped literals checked for their
//! type, `:cut` is a bound parameter or a checked `DATE` literal
//! ([`Cut`], decision 0090, ruling 52), and every function is fixed SQL per
//! engine. The same tree gives DuckDB and Trino; tests run both.

mod cut;
pub mod distribution;
mod formats;
mod shape;
mod write;

use std::collections::BTreeMap;

use quality_rules_compile::Compiled;
use quality_rules_tree::Expr;

pub use cut::Cut;
pub use formats::regex as format_regex;
pub use shape::ShapeKind;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Engine {
    DuckDb,
    Trino,
}

/// A statement's SQL and how many times the cut's date must be bound to
/// it, in order (`?` placeholders).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Sql {
    pub text: String,
    pub cut_params: usize,
}

/// The tables behind the dataset's references, by reference name, as the
/// platform resolves them: never from a rule.
pub type Tables = BTreeMap<String, Table>;

/// One reference's table: its path (catalog, schema, table) and the
/// condition its records must meet, if the reference declares one; on
/// Trino, `pinned` is the relation read instead of the path: the
/// referenced silver at its snapshot and cut, as the core wrote it
/// (decision 0090, ruling 61).
#[derive(Debug, Clone, PartialEq)]
pub struct Table {
    pub path: Vec<String>,
    pub filter: Option<Expr>,
    pub pinned: Option<String>,
}

/// What a tree may hold that cannot be written safely.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    /// NUL or another control character in a name or a text.
    ControlCharacter,
    BadNumber(String),
    BadDate(String),
    /// A reference the platform did not resolve to a table.
    UnknownReference(String),
    /// A node `compile` expands before SQL (`in_list`).
    NotExpanded(&'static str),
}

/// One expression as `engine`'s SQL, the cut a `?` parameter.
pub fn expr(e: &Expr, engine: Engine, tables: &Tables) -> Result<Sql, Error> {
    let mut w = write::Writer::new(engine, tables, None);
    w.expr(e)?;
    Ok(w.finish())
}

/// One expression as `engine`'s SQL with `cut` written in as a literal:
/// complete SQL, nothing to bind (`cut_params` is 0).
pub fn expr_at(e: &Expr, engine: Engine, tables: &Tables, cut: &Cut) -> Result<Sql, Error> {
    let mut w = write::Writer::new(engine, tables, Some(cut));
    w.expr(e)?;
    Ok(w.finish())
}

/// A compiled rule's three-valued result as `engine`'s SQL.
pub fn rule(rule: &Compiled, engine: Engine, tables: &Tables) -> Result<Sql, Error> {
    expr(&rule.result(), engine, tables)
}

/// A compiled rule's three-valued result as `engine`'s SQL at `cut`.
pub fn rule_at(rule: &Compiled, engine: Engine, tables: &Tables, cut: &Cut) -> Result<Sql, Error> {
    expr_at(&rule.result(), engine, tables, cut)
}

/// A name between double quotes, inner quotes doubled.
pub fn quote_ident(name: &str) -> Result<String, Error> {
    if name.chars().any(char::is_control) {
        return Err(Error::ControlCharacter);
    }
    Ok(format!("\"{}\"", name.replace('"', "\"\"")))
}

/// A text between single quotes, inner quotes doubled. Neither engine reads
/// `\` as an escape in a literal (verified, decision 0033); control
/// characters but the tab are refused (DuckDB would keep a NUL).
pub fn quote_text(text: &str) -> Result<String, Error> {
    if text.chars().any(|c| c.is_control() && c != '\t') {
        return Err(Error::ControlCharacter);
    }
    Ok(format!("'{}'", text.replace('\'', "''")))
}

/// A numeric constant of the engine's arithmetic as a `DOUBLE` literal
/// (decision 0090, point 8.1), the same text on every engine, so no
/// decimal arithmetic enters what it computes. `x` is finite.
pub fn double(x: f64) -> String {
    debug_assert!(x.is_finite(), "a constant is finite");
    format!("DOUBLE '{x}'")
}

#[cfg(test)]
mod double_tests {
    use super::double;

    #[test]
    fn a_constant_is_a_double_literal_of_its_shortest_text() {
        assert_eq!(double(0.4), "DOUBLE '0.4'");
        assert_eq!(double(365.0), "DOUBLE '365'");
        assert_eq!(double(-3.5), "DOUBLE '-3.5'");
        assert_eq!(double(0.6745), "DOUBLE '0.6745'");
    }
}
