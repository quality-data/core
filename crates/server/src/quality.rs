//! `server quality rules lint | explain | tidy` (decision 0019): the rule
//! language on local files, for people and for CI. Each rules file is
//! checked against the mapping of the dataset it names, given with
//! `--mapping` (decision 0090, ruling 63): its fields and types are the
//! schema, and its references are the other mappings given. No data is
//! read here: a rule's examples, and a rule test, run on Trino as a
//! `rule_test` job against a chosen cut (rulings 54 and 63).
//!
//! ```text
//! server quality rules lint    FILE... --mapping FILE... [--type FIELD=email]... [--lang es|en]
//! server quality rules explain FILE... --mapping FILE... [--lang es|en]
//! server quality rules tidy    FILE... [--check] [--lang es|en]
//! ```
//!
//! `tidy` (decision 0041) needs no mapping: it rewrites each file tidied,
//! or with `--check` names those that would change and fails.

use std::collections::BTreeMap;

use quality::mapping::{Mapping, MappingError};
use quality_rules::check::{FieldType, Schema};
use quality_rules::lint::{lint, tidy, Finding, Severity};
use quality_rules::sql::{Table, Tables};
use quality_rules::tree::parse_expr;
use quality_rules::{explain, model::RulesFile};
use texts::{text_in, Language};

type Failure = Box<dyn std::error::Error>;

struct Options {
    command: String,
    files: Vec<String>,
    mappings: Vec<String>,
    emails: Vec<String>,
    check: bool,
    language: Language,
}

pub(crate) fn run() -> Result<(), Failure> {
    let o = options(std::env::args().skip(2).collect())?;
    if o.command == "tidy" {
        return tidy_files(&o);
    }
    let mappings = load(&o)?;
    let mut blocking = false;
    let mut examples = false;
    for path in &o.files {
        let yaml = std::fs::read_to_string(path)?;
        let (b, e) = one_file(&o, &mappings, path, &yaml)?;
        blocking |= b;
        examples |= e;
    }
    if examples {
        eprintln!(
            "{}",
            text_in(o.language, "quality.cli.examplesOnTrino", &[])
        );
    }
    if blocking {
        return Err(text_in(o.language, "quality.cli.blocked", &[]).into());
    }
    Ok(())
}

/// Each file tidied in place; with `--check`, only named if it would change.
fn tidy_files(o: &Options) -> Result<(), Failure> {
    let mut untidy = false;
    for path in &o.files {
        let yaml = std::fs::read_to_string(path)?;
        let tidied = tidy(&yaml);
        if tidied == yaml {
            continue;
        }
        let file = [("file", path.clone())];
        if o.check {
            untidy = true;
            eprintln!("{}", text_in(o.language, "quality.cli.untidy", &file));
        } else {
            std::fs::write(path, &tidied)?;
            println!("{}", text_in(o.language, "quality.cli.tidied", &file));
        }
    }
    if untidy {
        return Err(text_in(o.language, "quality.cli.untidyFiles", &[]).into());
    }
    Ok(())
}

/// The mappings given, by dataset.
fn load(o: &Options) -> Result<BTreeMap<String, Mapping>, Failure> {
    let mut out = BTreeMap::new();
    for path in &o.mappings {
        let mapping = Mapping::parse(&std::fs::read_to_string(path)?)
            .map_err(|e| mapping_error(o.language, path, &e))?;
        out.insert(mapping.dataset.value.clone(), mapping);
    }
    Ok(out)
}

pub(crate) fn mapping_error(language: Language, path: &str, e: &MappingError) -> String {
    let t = |key: &str, values: &[(&str, String)]| {
        text_in(language, &format!("quality.cli.mapping.{key}"), values)
    };
    match e {
        MappingError::Parse(p) => {
            let place =
                p.at.map_or(String::new(), |a| format!(":{}:{}", a.line, a.column));
            format!(
                "{path}{place}: {}",
                t("parse", &[("message", p.message.clone())])
            )
        }
        MappingError::UnknownKey { field } => {
            format!("{path}: {}", t("unknownKey", &[("field", field.clone())]))
        }
        MappingError::SameColumn { column, at } => {
            format!(
                "{path}:{}:{}: {}",
                at.line,
                at.column,
                t("sameColumn", &[("column", column.clone())])
            )
        }
        MappingError::ReadFrom { at } => {
            format!("{path}:{}:{}: {}", at.line, at.column, t("readFrom", &[]))
        }
        MappingError::OneField { role, field, at } | MappingError::RoleType { role, field, at } => {
            let key = match e {
                MappingError::OneField { .. } => "oneField",
                _ => "roleType",
            };
            let values = [("role", role.name().to_string()), ("field", field.clone())];
            format!("{path}:{}:{}: {}", at.line, at.column, t(key, &values))
        }
        MappingError::ActiveValues { field, at } => {
            let values = [("field", field.clone())];
            format!(
                "{path}:{}:{}: {}",
                at.line,
                at.column,
                t("activeValues", &values)
            )
        }
    }
}

/// The mapping's schema, with the fields `--type` names as emails.
fn schema(mapping: &Mapping, emails: &[String]) -> Schema {
    let mut s = mapping.schema();
    for (name, t) in &mut s.fields {
        if emails.contains(name) && *t == FieldType::Text {
            *t = FieldType::Email;
        }
    }
    s
}

/// The tables the file's references read: those of a dataset whose mapping
/// was given, by its name.
fn tables(file: &RulesFile, mappings: &BTreeMap<String, Mapping>) -> Tables {
    file.references
        .iter()
        .filter(|(_, r)| mappings.contains_key(&r.dataset.value))
        .filter_map(|(name, r)| {
            let filter = match &r.filter {
                Some(w) => Some(parse_expr(&w.value).ok()?),
                None => None,
            };
            let path = vec![r.dataset.value.clone()];
            Some((
                name.clone(),
                Table {
                    path,
                    filter,
                    pinned: None,
                },
            ))
        })
        .collect()
}

/// One rules file: lint always, then explain. Whether it has errors, and
/// whether it has examples (run on Trino, not here).
fn one_file(
    o: &Options,
    mappings: &BTreeMap<String, Mapping>,
    path: &str,
    yaml: &str,
) -> Result<(bool, bool), Failure> {
    let file: Option<RulesFile> = quality_rules::parse(yaml).ok();
    let schema = match &file {
        Some(f) => {
            let dataset = f.dataset.value.clone();
            let mapping = mappings.get(&dataset).ok_or_else(|| {
                text_in(o.language, "quality.cli.noMapping", &[("dataset", dataset)])
            })?;
            schema(mapping, &o.emails)
        }
        None => Schema::default(),
    };
    let report = lint(yaml, &schema);
    for f in &report.findings {
        println!("{}", finding(o.language, path, f));
    }
    let blocking = report
        .findings
        .iter()
        .any(|f| f.severity() == Severity::Error);
    let examples = !report.examples.is_empty();
    let Some(file) = file.filter(|_| !blocking) else {
        return Ok((blocking, examples));
    };
    if o.command == "explain" {
        let tables = tables(&file, mappings);
        for rule in &report.compiled {
            println!(
                "{}\n{}\n",
                rule.id,
                explain::explain(rule, o.language, &tables).text(o.language)
            );
        }
    }
    Ok((false, examples))
}

/// `file:line:column: QR003 error: <message>`, the message in `language`.
fn finding(language: Language, path: &str, f: &Finding) -> String {
    let message = quality::rules_text::message(language, f);
    let place =
        f.at.map_or(String::new(), |p| format!(":{}:{}", p.line, p.column));
    let severity = match f.severity() {
        Severity::Error => "error",
        Severity::Warning => "warning",
    };
    let severity = text_in(language, &format!("quality.cli.{severity}"), &[]);
    format!("{path}{place}: {} {severity}: {message}", f.code.id())
}

fn options(args: Vec<String>) -> Result<Options, Failure> {
    let usage = || -> Failure { text_in(Language::Es, "quality.cli.usage", &[]).into() };
    let mut it = args.into_iter();
    if it.next().as_deref() != Some("rules") {
        return Err(usage());
    }
    let command = it
        .next()
        .filter(|c| ["lint", "explain", "tidy"].contains(&c.as_str()))
        .ok_or_else(usage)?;
    let mut o = Options {
        command,
        files: Vec::new(),
        mappings: Vec::new(),
        emails: Vec::new(),
        check: false,
        language: Language::Es,
    };
    while let Some(arg) = it.next() {
        let mut value = || it.next().ok_or_else(usage);
        match arg.as_str() {
            "--type" => {
                let v = value()?;
                match v.split_once('=') {
                    Some((field, "email")) => o.emails.push(field.to_string()),
                    _ => return Err(usage()),
                }
            }
            "--mapping" => o.mappings.push(value()?),
            "--check" if o.command == "tidy" => o.check = true,
            "--lang" => o.language = Language::parse(&value()?).ok_or_else(usage)?,
            flag if flag.starts_with("--") => return Err(usage()),
            _ => o.files.push(arg),
        }
    }
    if o.files.is_empty() {
        return Err(usage());
    }
    Ok(o)
}
