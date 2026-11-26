//! A compiled rule explained (decision 0019, point 9): what it applies to,
//! when it fails, when it skips, whether it reads other fields, its
//! dimension and severity, in Spanish or English from `texts`, and its SQL
//! for each engine, all from the same tree. Pure: no I/O.

mod words;

use std::collections::BTreeSet;

use quality_rules_compile::Compiled;
use quality_rules_sql::{Engine, Tables};
use quality_rules_tree::{Expr, Literal};
use texts::{text_in, Language};

use words::Words;

/// A rule as read: each part a phrase in the language asked for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Explanation {
    pub id: String,
    pub applies: String,
    pub fails: String,
    pub skips: Option<String>,
    /// Of one field, or across fields (it reads others).
    pub kind: String,
    pub dimension: String,
    pub severity: String,
    /// `None` when the platform did not resolve a reference it reads.
    pub duckdb: Option<String>,
    pub trino: Option<String>,
}

impl Explanation {
    /// The parts with their labels, one per line, for a panel or a terminal.
    pub fn text(&self, language: Language) -> String {
        let label = |k: &str| text_in(language, &format!("quality.explain.label.{k}"), &[]);
        let mut lines = vec![
            format!("{}: {}", label("applies"), self.applies),
            format!("{}: {}", label("fails"), self.fails),
        ];
        if let Some(s) = &self.skips {
            lines.push(format!("{}: {s}", label("skips")));
        }
        lines.push(format!(
            "{}: {} · {} · {}",
            label("kind"),
            self.kind,
            self.dimension,
            self.severity
        ));
        for (engine, sql) in [("DuckDB", &self.duckdb), ("Trino", &self.trino)] {
            if let Some(sql) = sql {
                lines.push(format!("{engine}: {sql}"));
            }
        }
        lines.join("\n")
    }
}

/// Explains a compiled rule in `language`; `tables` resolves the
/// references its SQL reads, as for running it.
pub fn explain(rule: &Compiled, language: Language, tables: &Tables) -> Explanation {
    let w = Words { language };
    let t = |k: String| text_in(language, &format!("quality.explain.{k}"), &[]);
    let applies = match &rule.applies {
        Expr::Literal(Literal::Boolean(true)) => t("everyone".into()),
        other => w.expr(other),
    };
    let sql = |engine| {
        quality_rules_sql::rule(rule, engine, tables)
            .ok()
            .map(|s| s.text)
    };
    Explanation {
        id: rule.id.clone(),
        applies,
        fails: w.expr(&rule.fails),
        skips: rule.skips.as_ref().map(|s| w.expr(s)),
        kind: t(format!(
            "kind.{}",
            if across(rule) { "across" } else { "field" }
        )),
        dimension: t(format!("dimension.{}", rule.dimension.name())),
        severity: t(format!("severity.{}", rule.severity.name())),
        duckdb: sql(Engine::DuckDb),
        trino: sql(Engine::Trino),
    }
}

/// A rule reads other fields than its own (decision 0019, point 6).
fn across(rule: &Compiled) -> bool {
    let fields: BTreeSet<&str> = [&rule.applies, &rule.fails]
        .into_iter()
        .flat_map(Expr::walk)
        .filter_map(|e| match e {
            Expr::Field(name) => Some(name.as_str()),
            _ => None,
        })
        .collect();
    match &rule.field {
        Some(own) => fields.iter().any(|f| f != own),
        None => fields.len() > 1,
    }
}
