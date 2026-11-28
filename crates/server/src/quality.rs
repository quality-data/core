//! `server quality rules lint | explain | tidy` (decision 0019): the rule
//! language on local files, for people and for CI. Any dataset: each rules
//! file runs against the dataset it names, loaded from a Parquet file with
//! `--data <dataset>=<file>`, its schema read from that file; its
//! references read the other datasets loaded. With `--mapping` a dataset
//! is read through its mapping (decision 0020): `--data` then names the
//! mapping's source, and the rules name the mapping's fields. A rule test
//! runs on Trino as a `rule_test` job against a chosen cut (decision 0090,
//! ruling 54), not here.
//!
//! ```text
//! server quality rules lint    FILE... --data DATASET=PARQUET... [--mapping FILE]... [--cut DATE] [--type FIELD=email]... [--lang es|en]
//! server quality rules explain FILE... --data DATASET=PARQUET... [--lang es|en]
//! server quality rules tidy    FILE... [--check] [--lang es|en]
//! ```
//!
//! `tidy` (decision 0041) needs no data: it rewrites each file tidied, or
//! with `--check` names those that would change and fails.

use quality::mapping::{Mapping, MappingError};
use quality_rules::lint::{lint, tidy, Finding, Report, Severity};
use quality_rules::{explain, model::RulesFile};
use quality_rules_duckdb::Runner;
use texts::{text_in, Language};

type Failure = Box<dyn std::error::Error>;

struct Options {
    command: String,
    files: Vec<String>,
    data: Vec<(String, String)>,
    mappings: Vec<String>,
    emails: Vec<String>,
    cut: Option<String>,
    check: bool,
    language: Language,
}

pub(crate) fn run() -> Result<(), Failure> {
    let o = options(std::env::args().skip(2).collect())?;
    if o.command == "tidy" {
        return tidy_files(&o);
    }
    let runner = load(&o)?;
    let mut blocking = false;
    for path in &o.files {
        let yaml = std::fs::read_to_string(path)?;
        blocking |= one_file(&o, &runner, path, &yaml)?;
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

/// The datasets: through their mapping when they have one (`--data` names
/// the mapping's source), as their Parquet file has them otherwise.
fn load(o: &Options) -> Result<Runner, Failure> {
    let mut runner = Runner::new()?;
    let mut mapped_sources = Vec::new();
    for path in &o.mappings {
        let mapping = Mapping::parse(&std::fs::read_to_string(path)?)
            .map_err(|e| mapping_error(o.language, path, &e))?;
        let source = &mapping.source.value;
        let (_, parquet) = o
            .data
            .iter()
            .find(|(name, _)| name == source)
            .ok_or_else(|| {
                text_in(
                    o.language,
                    "quality.cli.noData",
                    &[("dataset", source.clone())],
                )
            })?;
        runner.load_mapped(&mapping, parquet).map_err(|e| match e {
            quality_rules_duckdb::Error::InvalidColumnName(column) => text_in(
                o.language,
                "quality.cli.mapping.noColumn",
                &[("column", column), ("source", source.clone())],
            ),
            other => other.to_string(),
        })?;
        mapped_sources.push(source.clone());
    }
    for (dataset, path) in o
        .data
        .iter()
        .filter(|(name, _)| !mapped_sources.contains(name))
    {
        runner.load(dataset, path)?;
    }
    Ok(runner)
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

/// One rules file: lint always, then explain. True if it has errors.
fn one_file(o: &Options, runner: &Runner, path: &str, yaml: &str) -> Result<bool, Failure> {
    let file: Option<RulesFile> = quality_rules::parse(yaml).ok();
    let dataset = file
        .as_ref()
        .map(|f| f.dataset.value.clone())
        .unwrap_or_default();
    if file.is_some() && !runner.has(&dataset) {
        let message = text_in(o.language, "quality.cli.noData", &[("dataset", dataset)]);
        return Err(message.into());
    }
    let schema = if file.is_some() {
        runner.schema(&dataset, &o.emails)?
    } else {
        Default::default()
    };
    let report = lint(yaml, &schema);
    let mut findings = report.findings.clone();
    findings.extend(examples(o, runner, &report)?);
    for f in &findings {
        println!("{}", finding(o.language, path, f));
    }
    let blocking = findings.iter().any(|f| f.severity() == Severity::Error);
    let Some(file) = file.filter(|_| !blocking) else {
        return Ok(blocking);
    };
    if o.command == "explain" {
        let tables = runner.tables(&file);
        for rule in &report.compiled {
            println!(
                "{}\n{}\n",
                rule.id,
                explain::explain(rule, o.language, &tables).text(o.language)
            );
        }
    }
    Ok(false)
}

/// The rules' examples, run when there is a cut to run them against.
fn examples(o: &Options, runner: &Runner, report: &Report) -> Result<Vec<Finding>, Failure> {
    let needs_cut = report.examples.iter().any(|q| q.cut_params > 0);
    match (&o.cut, needs_cut) {
        (Some(cut), _) => Ok(runner.examples(&report.examples, cut)?),
        (None, false) => Ok(runner.examples(&report.examples, "1970-01-01")?),
        (None, true) => {
            eprintln!(
                "{}",
                text_in(o.language, "quality.cli.examplesNeedCut", &[])
            );
            Ok(Vec::new())
        }
    }
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
        data: Vec::new(),
        mappings: Vec::new(),
        emails: Vec::new(),
        cut: None,
        check: false,
        language: Language::Es,
    };
    while let Some(arg) = it.next() {
        let mut value = || it.next().ok_or_else(usage);
        match arg.as_str() {
            "--data" => {
                let v = value()?;
                let (dataset, path) = v.split_once('=').ok_or_else(usage)?;
                o.data.push((dataset.to_string(), path.to_string()));
            }
            "--type" => {
                let v = value()?;
                match v.split_once('=') {
                    Some((field, "email")) => o.emails.push(field.to_string()),
                    _ => return Err(usage()),
                }
            }
            "--cut" => o.cut = Some(value()?),
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
