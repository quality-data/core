//! The SFC cells (decision 0058) as the scratch insert reads and keeps
//! them: per field holding the `sfc` role, whether it is filled and failing
//! no rule that reads it; and whether the record is active. Emptiness and
//! the active test come from the rule language's tree, written for Trino.

use quality::mapping::{FieldRole, Mapping};
use quality_rules_sql::{Engine, Tables};
use quality_rules_tree::{Expr, Function, Literal};

use crate::icd::IcdRule;
use crate::Error;

#[derive(Debug, Default)]
pub(crate) struct Cells {
    pub(crate) names: Vec<String>,
    filled: Vec<String>,
    rules: Vec<Vec<usize>>,
    active: String,
}

fn sql(e: &Expr) -> Result<String, Error> {
    Ok(quality_rules_sql::expr(e, Engine::Trino, &Tables::new())?.text)
}

fn call(function: Function, x: Expr) -> Expr {
    Expr::Call {
        function,
        args: vec![x],
    }
}

impl Cells {
    pub(crate) fn of(mapping: &Mapping, rules: &[IcdRule]) -> Result<Self, Error> {
        let mut cells = Self {
            active: "TRUE".to_string(),
            ..Self::default()
        };
        for (name, f) in &mapping.fields {
            let field = || Expr::Field(name.clone());
            if f.value.roles.contains(&FieldRole::Active) {
                cells.active = sql(&Expr::InList {
                    expr: Box::new(call(Function::Upper, call(Function::Trim, field()))),
                    list: f
                        .value
                        .active_values
                        .iter()
                        .map(|v| {
                            let v = v.trim_matches([' ', '\u{a0}']).to_uppercase();
                            Expr::Literal(Literal::Text(v))
                        })
                        .collect(),
                    negated: false,
                })?;
            }
            if f.value.roles.contains(&FieldRole::Sfc) {
                cells.names.push(name.clone());
                cells
                    .filled
                    .push(sql(&Expr::Not(Box::new(call(Function::IsEmpty, field()))))?);
                cells.rules.push(
                    (0..rules.len())
                        .filter(|&i| rules[i].reads.iter().any(|r| r == name))
                        .collect(),
                );
            }
        }
        Ok(cells)
    }

    /// The columns read over the cut: each cell filled, the record active.
    pub(crate) fn read(&self) -> Vec<String> {
        let mut out: Vec<String> = self
            .filled
            .iter()
            .enumerate()
            .map(|(j, f)| format!("{f} AS \"__n{j}\""))
            .collect();
        out.push(format!("COALESCE({}, FALSE) AS \"__act\"", self.active));
        out
    }

    /// Each cell right: filled and failing no rule that reads it.
    pub(crate) fn kept(&self) -> Vec<String> {
        self.rules
            .iter()
            .enumerate()
            .map(|(j, rules)| {
                let fails: Vec<String> = rules
                    .iter()
                    .map(|i| format!("COALESCE(\"r{i}\", FALSE)"))
                    .collect();
                if fails.is_empty() {
                    format!("\"__n{j}\"")
                } else {
                    format!("(\"__n{j}\" AND NOT ({}))", fails.join(" OR "))
                }
            })
            .collect()
    }
}
