//! The records' ICD into the scratch table (decision 0090, point 8.4): one
//! `INSERT … SELECT` evaluates every rule once on the pinned cut and keeps,
//! per record, its rules' outcomes, its fields' ICD (decision 0057: C, S and
//! A, weighted and renormalized over those that apply), its dimension
//! groups and its SFC cells, with its record and entity keys hashed in the
//! engine by the findings key read from a table, never from the SQL text.

use quality::mapping::{FieldRole, Mapping, Priority, Semantic};
use quality_rules_compile::Compiled;
use quality_rules_model::Severity;
use quality_rules_sql::{double, quote_text, Engine, Sql, Tables};

use crate::{quoted, Error, Statement};

/// The scratch table, made once at install by the module's own user.
pub const SCRATCH: &str = "\"lake\".\"quality_scratch\".\"record_icd\"";
/// The one-row table of the findings key (`key varbinary`), readable by
/// rule jobs only (point 8.6).
pub const FINDINGS_KEY: &str = "\"lake\".\"quality_secrets\".\"findings_key\"";

/// The components' weights: C, S and A.
const W_C: f64 = 0.40;
const W_S: f64 = 0.15;
const W_A: f64 = 0.15;

/// One rule as the ICD reads it: its Trino SQL, its severity and fields.
#[derive(Debug, Clone)]
pub struct IcdRule {
    pub id: String,
    pub sql: Sql,
    pub severity: Severity,
    /// One field: it counts in C; more, in S of each.
    pub reads: Vec<String>,
}

impl IcdRule {
    pub fn of(rule: &Compiled, tables: &Tables) -> Result<Self, Error> {
        Ok(Self {
            id: rule.id.clone(),
            sql: quality_rules_sql::rule(rule, Engine::Trino, tables)
                .map_err(|e| Error::Rule(rule.id.clone(), e))?,
            severity: rule.severity,
            reads: rule.reads(),
        })
    }
}

fn severity(s: Severity) -> f64 {
    match s {
        Severity::Blocking => 1.0,
        Severity::Major => 0.6,
        Severity::Minor => 0.3,
    }
}

fn one(condition: &str) -> String {
    format!("IF({condition}, DOUBLE '1', DOUBLE '0')")
}

/// `1 - failed weight / applying weight` over `rules` (index, severity).
fn conformity(rules: &[(usize, f64)]) -> Option<String> {
    if rules.is_empty() {
        return None;
    }
    let sum = |term: &dyn Fn(usize) -> String| {
        rules
            .iter()
            .map(|(i, s)| format!("{} * {}", double(*s), term(*i)))
            .collect::<Vec<_>>()
            .join(" + ")
    };
    Some(format!(
        "DOUBLE '1' - ({}) / NULLIF({}, DOUBLE '0')",
        sum(&|i| one(&format!("\"r{i}\""))),
        sum(&|i| one(&format!("\"r{i}\" IS NOT NULL")))
    ))
}

/// `Σ w·x / Σ w·[x applies]`: `NULL` when none applies.
pub(crate) fn weighted(parts: &[(f64, String)]) -> String {
    if parts.is_empty() {
        return "CAST(NULL AS DOUBLE)".to_string();
    }
    let (top, bottom): (Vec<_>, Vec<_>) = parts
        .iter()
        .map(|(w, x)| {
            let w = double(*w);
            (
                format!("{w} * COALESCE({x}, DOUBLE '0')"),
                format!("{w} * {}", one(&format!("{x} IS NOT NULL"))),
            )
        })
        .unzip();
    format!(
        "({}) / NULLIF({}, DOUBLE '0')",
        top.join(" + "),
        bottom.join(" + ")
    )
}

/// A field with a component: its name, priority weight and parts; the
/// `n`th is `f[n + 1]` of the scratch row.
#[derive(Debug, Clone)]
pub struct Measured {
    pub name: String,
    pub weight: u8,
    parts: Vec<(f64, String)>,
}

/// The fields with a component, in the mapping's order; A only for fields
/// that expire when the dataset has an update date.
pub fn measured(mapping: &Mapping, rules: &[IcdRule]) -> Vec<Measured> {
    let timely = role(mapping, FieldRole::Updated).is_some();
    let mut out = Vec::new();
    for (name, f) in &mapping.fields {
        let (mut c, mut s) = (Vec::new(), Vec::new());
        for (i, r) in rules.iter().enumerate() {
            if r.reads.iter().any(|x| x == name) {
                let to = if r.reads.len() == 1 { &mut c } else { &mut s };
                to.push((i, severity(r.severity)));
            }
        }
        let mut parts = Vec::new();
        parts.extend(conformity(&c).map(|x| (W_C, format!("({x})"))));
        parts.extend(conformity(&s).map(|x| (W_S, format!("({x})"))));
        if timely && f.value.expires {
            parts.push((W_A, "\"__a\"".to_string()));
        }
        if !parts.is_empty() {
            out.push(Measured {
                name: name.clone(),
                weight: f.value.priority.weight(),
                parts,
            });
        }
    }
    out
}

pub(crate) fn role(mapping: &Mapping, role: FieldRole) -> Option<&String> {
    mapping
        .fields
        .iter()
        .find(|(_, f)| f.value.roles.contains(&role))
        .map(|(name, _)| name)
}

pub(crate) fn with_role(mapping: &Mapping, role: FieldRole) -> Vec<&String> {
    mapping
        .fields
        .iter()
        .filter(|(_, f)| f.value.roles.contains(&role))
        .map(|(name, _)| name)
        .collect()
}

/// `names` normalized as one key (a document's digits without leading
/// zeros, any other trimmed of spaces and upper case), joined by `|`.
pub(crate) fn key(mapping: &Mapping, names: &[&String]) -> Result<String, Error> {
    let mut parts = Vec::new();
    for name in names {
        let document = mapping.fields[*name].value.semantic == Some(Semantic::Document);
        let v = format!("CAST({} AS VARCHAR)", quoted(name)?);
        parts.push(if document {
            format!("NULLIF(trim(LEADING '0' FROM regexp_replace({v}, '[^0-9]', '')), '')")
        } else {
            format!("NULLIF(upper(trim(BOTH ' ' FROM {v})), '')")
        });
    }
    Ok(if parts.is_empty() {
        "CAST(NULL AS VARCHAR)".into()
    } else {
        parts.join(" || '|' || ")
    })
}

/// A key's keyed hash, lower-case hex, by the key read in the engine.
fn hashed(k: &str) -> String {
    format!("lower(to_hex(hmac_sha256(to_utf8({k}), h.\"key\")))")
}

/// Whether a record has a serious failure: a blocking or major rule that
/// reads a critical or high field fails.
fn serious(mapping: &Mapping, rules: &[IcdRule]) -> String {
    let weighty = |n: &String| {
        mapping
            .fields
            .get(n)
            .is_some_and(|f| matches!(f.value.priority, Priority::Critical | Priority::High))
    };
    let failing: Vec<String> = rules
        .iter()
        .enumerate()
        .filter(|(_, r)| matches!(r.severity, Severity::Blocking | Severity::Major))
        .filter(|(_, r)| r.reads.iter().any(weighty))
        .map(|(i, _)| format!("COALESCE(\"r{i}\", FALSE)"))
        .collect();
    if failing.is_empty() {
        "FALSE".into()
    } else {
        failing.join(" OR ")
    }
}

fn array(items: Vec<String>, ty: &str) -> String {
    format!("CAST(ARRAY[{}] AS ARRAY({ty}))", items.join(", "))
}

/// The scratch `INSERT` of the job's records over `from` (the pinned cut).
pub fn scratch_insert(
    job_id: &str,
    dataset: &str,
    from: &str,
    mapping: &Mapping,
    rules: &[IcdRule],
) -> Result<Statement, Error> {
    let fields = measured(mapping, rules);
    let cut = "CAST(? AS DATE)";
    let mut read: Vec<String> = rules
        .iter()
        .enumerate()
        .map(|(i, r)| format!("{} AS \"r{i}\"", r.sql.text))
        .collect();
    let entity = with_role(mapping, FieldRole::Entity);
    let own: Vec<&String> = mapping.key.iter().collect();
    read.push(format!("{} AS \"__k\"", key(mapping, &entity)?));
    let record = if own.is_empty() { &entity } else { &own };
    read.push(format!("{} AS \"__r\"", key(mapping, record)?));
    let segment = role(mapping, FieldRole::Segment)
        .map(|s| quoted(s))
        .transpose()?;
    read.push(format!(
        "CAST({} AS VARCHAR) AS \"__s\"",
        segment.as_deref().unwrap_or("NULL")
    ));
    let a = match role(mapping, FieldRole::Updated) {
        Some(u) => {
            let u = quoted(u)?;
            format!(
                "CASE WHEN {u} IS NULL THEN NULL ELSE power(DOUBLE '0.5', \
                 CAST(greatest(date_diff('day', {u}, {cut}), 0) AS DOUBLE) / DOUBLE '365') END"
            )
        }
        None => "CAST(NULL AS DOUBLE)".into(),
    };
    let cut_params = rules.iter().map(|r| r.sql.cut_params).sum::<usize>()
        + usize::from(role(mapping, FieldRole::Updated).is_some());
    read.push(format!("{a} AS \"__a\""));
    let dimensions = with_role(mapping, FieldRole::Dimension);
    for (j, d) in dimensions.iter().enumerate() {
        read.push(format!(
            "NULLIF(trim(BOTH ' ' FROM CAST({} AS VARCHAR)), '') AS \"__g{j}\"",
            quoted(d)?
        ));
    }
    let cells = crate::cells::Cells::of(mapping, rules)?;
    read.extend(cells.read());
    let f: Vec<String> = fields.iter().map(|m| weighted(&m.parts)).collect();
    let record_icd: Vec<(f64, String)> = fields
        .iter()
        .enumerate()
        .map(|(n, m)| {
            (
                f64::from(m.weight),
                format!("element_at(x.\"__f\", {})", n + 1),
            )
        })
        .collect();
    let sql = format!(
        "INSERT INTO {SCRATCH} (job_id, dataset, k, r_key, s, a, serious, act, icd, r, f, g, c)
         SELECT {job}, {ds}, {k}, {rk}, x.\"__s\", x.\"__a\", x.\"__serious\", x.\"__act\", {icd},
                x.\"__rs\", x.\"__f\", x.\"__g\", x.\"__c\"
           FROM (SELECT d.\"__k\", d.\"__r\", d.\"__s\", d.\"__a\", d.\"__act\",
                        {serious} AS \"__serious\", {rs} AS \"__rs\", {fs} AS \"__f\",
                        {gs} AS \"__g\", {cs} AS \"__c\"
                   FROM (SELECT {read} FROM {from}) d) x
          CROSS JOIN (SELECT \"key\" FROM {FINDINGS_KEY}) h",
        job = quote_text(job_id)?,
        ds = quote_text(dataset)?,
        k = hashed("x.\"__k\""),
        rk = hashed("x.\"__r\""),
        icd = weighted(&record_icd),
        serious = serious(mapping, rules),
        rs = array(
            (0..rules.len()).map(|i| format!("\"r{i}\"")).collect(),
            "BOOLEAN"
        ),
        fs = array(f, "DOUBLE"),
        gs = array(
            (0..dimensions.len())
                .map(|j| format!("\"__g{j}\""))
                .collect(),
            "VARCHAR"
        ),
        cs = array(cells.kept(), "BOOLEAN"),
        read = read.join(", "),
    );
    Ok(Statement { sql, cut_params })
}
