//! The checks on the whole file: what compiles but is suspect (decision
//! 0019, point 8; 0033), and the sizes of decision 0034.

use std::collections::{BTreeMap, BTreeSet};

use quality_rules_compile::{Compiled, FieldType, Schema};
use quality_rules_model::{Pattern, RulesFile, ValueSet};
use quality_rules_tree::{BinaryOp, Expr, Function, Literal};

use super::{rule_of, Code, Finding};

pub(crate) fn file(
    file: &RulesFile,
    yaml: &str,
    schema: &Schema,
    compiled: &[Compiled],
) -> Vec<Finding> {
    let mut out = Vec::new();
    ids_and_alerts(file, &mut out);
    required_and_placeholder(file, &mut out);
    duplicates(file, compiled, &mut out);
    for c in compiled {
        never_holds(file, c, &mut out);
        regexes(file, c, &mut out);
        text_on_number(file, schema, c, &mut out);
    }
    sizes(file, yaml, &mut out);
    out
}

fn ids_and_alerts(file: &RulesFile, out: &mut Vec<Finding>) {
    let mut seen = BTreeSet::new();
    for rule in &file.rules {
        let id = &rule.id.value;
        if !seen.insert(id.as_str()) {
            out.push(
                Finding::new(Code::DuplicateId, Some(id), Some(rule.id.at)).with("id", id.clone()),
            );
        }
        if rule
            .alert
            .as_ref()
            .is_none_or(|a| a.value.trim().is_empty())
        {
            out.push(Finding::new(Code::NoAlert, Some(id), Some(rule.id.at)));
        }
    }
}

/// `required` already counts fillers as empty (decision 0033, point 4).
fn required_and_placeholder(file: &RulesFile, out: &mut Vec<Finding>) {
    let required: BTreeSet<&str> = file
        .rules
        .iter()
        .filter(|r| {
            r.check
                .as_ref()
                .is_some_and(|c| c.value == Pattern::Required)
        })
        .flat_map(|r| {
            r.field
                .as_ref()
                .map(|f| f.value.names())
                .unwrap_or_default()
        })
        .collect();
    for rule in file.rules.iter().filter(|r| {
        r.check
            .as_ref()
            .is_some_and(|c| c.value == Pattern::Placeholder)
    }) {
        for name in rule
            .field
            .as_ref()
            .map(|f| f.value.names())
            .unwrap_or_default()
        {
            if required.contains(name) {
                let f = Finding::new(
                    Code::RequiredAndPlaceholder,
                    Some(&rule.id.value),
                    Some(rule.id.at),
                );
                out.push(f.with("field", name));
            }
        }
    }
}

/// Two rules that check the same thing count each failure twice.
fn duplicates(file: &RulesFile, compiled: &[Compiled], out: &mut Vec<Finding>) {
    let mut seen: BTreeMap<String, &str> = BTreeMap::new();
    for c in compiled {
        let signature = format!(
            "{:?}|{:?}|{}|{}|{:?}",
            c.field, c.pattern, c.applies, c.fails, c.skips
        );
        match seen.get(&signature) {
            Some(first) => {
                let at = rule_of(file, &c.id).map(|r| r.id.at);
                out.push(Finding::new(Code::DuplicateRule, Some(&c.id), at).with("other", *first));
            }
            None => {
                seen.insert(signature, c.id.as_str());
            }
        }
    }
}

/// The parts of a conjunction.
fn conjuncts(e: &Expr) -> Vec<&Expr> {
    match e {
        Expr::Binary {
            op: BinaryOp::And,
            left,
            right,
        } => {
            let mut v = conjuncts(left);
            v.extend(conjuncts(right));
            v
        }
        other => vec![other],
    }
}

/// A condition that no record can meet: `FALSE`, one field equal to two
/// different values, or a field both empty and not.
fn never_holds(file: &RulesFile, c: &Compiled, out: &mut Vec<Finding>) {
    let parts = conjuncts(&c.applies);
    let mut equals: BTreeMap<String, String> = BTreeMap::new();
    let mut nulls: BTreeMap<String, bool> = BTreeMap::new();
    let mut never = false;
    for p in parts {
        match p {
            Expr::Literal(Literal::Boolean(false)) => never = true,
            Expr::Binary {
                op: BinaryOp::Eq,
                left,
                right,
            } if is_constant(right) => {
                let previous = equals.insert(left.to_string(), right.to_string());
                never |= previous.is_some_and(|v| v != right.to_string());
            }
            Expr::IsNull { expr, negated } => {
                let previous = nulls.insert(expr.to_string(), *negated);
                never |= previous.is_some_and(|v| v != *negated);
            }
            _ => {}
        }
    }
    if never {
        let at = rule_of(file, &c.id).and_then(|r| r.when.as_ref().map(|w| w.at).or(Some(r.id.at)));
        out.push(Finding::new(Code::NeverHolds, Some(&c.id), at));
    }
}

fn is_constant(e: &Expr) -> bool {
    e.walk().iter().all(|n| {
        matches!(
            n,
            Expr::Literal(_)
                | Expr::Call {
                    function: Function::Upper | Function::Trim,
                    ..
                }
        )
    })
}

/// What RE2 (DuckDB) and RE2J (Trino) lack: a pattern that uses it fails on
/// one engine or means something else (decision 0033, point 6).
fn regexes(file: &RulesFile, c: &Compiled, out: &mut Vec<Finding>) {
    let tree = [&c.applies, &c.fails];
    for node in tree.iter().flat_map(|e| e.walk()) {
        if let Expr::Call {
            function: Function::RegexpLike,
            args,
        } = node
        {
            if let Some(Expr::Literal(Literal::Text(pattern))) = args.get(1) {
                if let Some(construct) = unsupported_regex(pattern) {
                    let at = rule_of(file, &c.id)
                        .and_then(|r| r.pattern.as_ref().map(|p| p.at).or(Some(r.id.at)));
                    out.push(
                        Finding::new(Code::Regex, Some(&c.id), at).with("construct", construct),
                    );
                }
            }
        }
    }
}

fn unsupported_regex(pattern: &str) -> Option<&'static str> {
    let outside = outside_classes(pattern);
    let backreference = outside
        .as_bytes()
        .windows(2)
        .any(|w| w[0] == b'\\' && (b'1'..=b'9').contains(&w[1]));
    if backreference {
        return Some("\\1");
    }
    ["(?=", "(?!", "(?<=", "(?<!", "(?>", "++", "*+", "?+"]
        .into_iter()
        .find(|c| outside.contains(c))
}

/// The pattern without its character classes (`[…]`), where `*+` or `(?=`
/// are plain characters; escapes are kept as written.
fn outside_classes(pattern: &str) -> String {
    let mut out = String::new();
    let mut chars = pattern.chars().peekable();
    let mut in_class = false;
    while let Some(c) = chars.next() {
        match c {
            '\\' => {
                let next = chars.next();
                if !in_class {
                    out.push(c);
                    out.extend(next);
                }
            }
            '[' if !in_class => {
                in_class = true;
                // A `]` right after `[` or `[^` is a member, not the end.
                if chars.peek() == Some(&'^') {
                    chars.next();
                }
                if chars.peek() == Some(&']') {
                    chars.next();
                }
            }
            ']' if in_class => in_class = false,
            _ if in_class => {}
            _ => out.push(c),
        }
    }
    out
}

/// A text pattern on a field mapped as a number reads `1234.0`.
fn text_on_number(file: &RulesFile, schema: &Schema, c: &Compiled, out: &mut Vec<Finding>) {
    let numeric = c.field.as_ref().and_then(|f| schema.fields.get(f)) == Some(&FieldType::Number);
    if numeric && matches!(c.pattern, Pattern::Length | Pattern::Format) {
        let at = rule_of(file, &c.id).and_then(|r| r.check.as_ref().map(|c| c.at));
        out.push(
            Finding::new(Code::TextOnNumber, Some(&c.id), at)
                .with("field", c.field.clone().unwrap_or_default()),
        );
    }
}

/// Decision 0034: a file of 30 rules or 300 lines, lists of 50 values.
fn sizes(file: &RulesFile, yaml: &str, out: &mut Vec<Finding>) {
    let lines = yaml.lines().count();
    if file.rules.len() > 30 || lines > 300 {
        let f = Finding::new(Code::LargeFile, None, None);
        out.push(
            f.with("rules", file.rules.len().to_string())
                .with("lines", lines.to_string()),
        );
    }
    let long = |n: usize| n > 50;
    for (name, list) in &file.lists {
        if long(list.value.len()) {
            out.push(
                Finding::new(Code::LongList, None, Some(list.at))
                    .with("name", name.clone())
                    .with("count", list.value.len().to_string()),
            );
        }
    }
    for rule in &file.rules {
        for set in [&rule.in_, &rule.not_in].into_iter().flatten() {
            if let ValueSet::Values(values) = &set.value {
                if long(values.len()) {
                    let f = Finding::new(Code::LongList, Some(&rule.id.value), Some(set.at));
                    out.push(
                        f.with("name", rule.id.value.clone())
                            .with("count", values.len().to_string()),
                    );
                }
            }
        }
    }
}
