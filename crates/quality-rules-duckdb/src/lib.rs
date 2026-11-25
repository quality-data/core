//! The rule language run on an embedded DuckDB, for any dataset: each
//! dataset is a view over its Parquet file, its schema is read from it, and
//! a rules file runs against the dataset it names, its references against
//! the others loaded. Only counts come out, never values: the data may be
//! a cooperative's (AGENTS.md); but for a test's read of records
//! ([`sample`], decision 0043), whose caller masks and audits them.

use std::collections::BTreeMap;

use duckdb::types::Value;
use duckdb::Connection;
use quality::mapping::{Kind, Mapping};
use quality_rules_compile::{Compiled, FieldType, Schema};
use quality_rules_lint::{check_examples, ExampleQuery, Finding};
use quality_rules_model::RulesFile;
use quality_rules_sql::{quote_ident, quote_text, Engine, Table, Tables};
use quality_rules_tree::parse_expr;

pub use console::Rows;
pub use duckdb::Error;
pub use extract::{Column, SourceConnection};
pub use failures::FailGroup;
pub use findings::Previous;
pub use icd::{EntityIcd, IcdReport, IcdRule};
pub use lake::{LakeConnection, LakeRead, SilverAt};
pub use maintain::Snapshot;
pub use quality::worker_api::band;
pub use recency::RECENCY_DAYS;
pub use sample::{fields_read, Outcome, Sampled, Shown};

mod console;
mod distribution;
mod extract;
mod failures;
mod fields;
mod findings;
mod groups;
mod icd;
mod keys;
mod lake;
mod maintain;
mod profile;
mod recency;
mod records;
mod sample;
mod sfc;
mod silver;
mod silver_legacy;
mod source;
mod tables;

/// One rule's counts over a dataset's cut.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Counts {
    pub rule: String,
    /// The records in the rule's population where its condition holds and
    /// its field is not skipped.
    pub applies: u64,
    pub fails: u64,
    pub passes: u64,
    /// Every other record (`NULL`).
    pub skipped: u64,
}

impl Counts {
    /// Passing records among those it applies to, as a percentage.
    pub fn conformity(&self) -> Option<f64> {
        (self.applies > 0).then(|| 100.0 * self.passes as f64 / self.applies as f64)
    }
}

/// An in-memory DuckDB with the datasets loaded.
pub struct Runner {
    con: Connection,
    datasets: BTreeMap<String, String>,
    /// The datasets loaded through a mapping, with the schema it gives.
    mapped: BTreeMap<String, Schema>,
    /// Whether extensions load only from an extension directory
    /// ([`Runner::set_extension_directory`]).
    offline: bool,
}

impl std::fmt::Debug for Runner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Runner")
            .field("datasets", &self.datasets.keys())
            .finish()
    }
}

impl Runner {
    pub fn new() -> Result<Self, Error> {
        Ok(Self {
            con: Connection::open_in_memory()?,
            datasets: BTreeMap::new(),
            mapped: BTreeMap::new(),
            offline: false,
        })
    }

    /// Loads a dataset as a view over its Parquet file.
    pub fn load(&mut self, dataset: &str, parquet: &str) -> Result<(), Error> {
        let (name, path) = (quoted(dataset)?, quote_text(parquet).map_err(invalid)?);
        self.con.execute_batch(&format!(
            "CREATE OR REPLACE VIEW {name} AS SELECT * FROM read_parquet({path})"
        ))?;
        self.datasets
            .insert(dataset.to_string(), parquet.to_string());
        Ok(())
    }

    /// Loads a dataset through its mapping: the source's Parquet file as
    /// a view of its own, and the dataset as a view that reads each field
    /// from its column, typed as mapped (decision 0020). A number mapped as
    /// text keeps no `.0`, so codes read as written.
    pub fn load_mapped(&mut self, mapping: &Mapping, parquet: &str) -> Result<(), Error> {
        let source = format!("{}__source", mapping.dataset.value);
        self.load(&source, parquet)?;
        self.map(mapping, &source, parquet)
    }

    /// The dataset as a view over its `source` view, each field read from
    /// its column, typed as mapped; `origin` says where the source came from.
    fn map(&mut self, mapping: &Mapping, source: &str, origin: &str) -> Result<(), Error> {
        let select = self.typed_select(mapping, source)?;
        self.view(mapping, &select, origin)
    }

    /// The dataset as a view of `select`, loaded through its mapping.
    fn view(&mut self, mapping: &Mapping, select: &str, origin: &str) -> Result<(), Error> {
        let name = quoted(&mapping.dataset.value)?;
        self.con
            .execute_batch(&format!("CREATE OR REPLACE VIEW {name} AS {select}"))?;
        self.datasets
            .insert(mapping.dataset.value.clone(), origin.to_string());
        self.mapped
            .insert(mapping.dataset.value.clone(), mapping.schema());
        Ok(())
    }

    /// Each field read from its column of the `source` view, typed as
    /// mapped: what silver holds (decision 0061).
    fn typed_select(&self, mapping: &Mapping, source: &str) -> Result<String, Error> {
        let types = self.column_types(source)?;
        let mut select = Vec::new();
        for (field, f) in &mapping.fields {
            let column = &f.value.column;
            let source_type = types
                .get(column)
                .ok_or_else(|| Error::InvalidColumnName(column.clone()))?;
            let c = quoted(column)?;
            let value = match f.value.kind {
                Kind::Text if field_type(source_type) == FieldType::Number => {
                    format!("regexp_replace(CAST({c} AS VARCHAR), '\\.0$', '')")
                }
                Kind::Text => format!("CAST({c} AS VARCHAR)"),
                Kind::Number => format!("TRY_CAST({c} AS DOUBLE)"),
                Kind::Date => format!("TRY_CAST({c} AS DATE)"),
                Kind::Boolean => format!("TRY_CAST({c} AS BOOLEAN)"),
            };
            select.push(format!("{value} AS {}", quoted(field)?));
        }
        Ok(format!(
            "SELECT {} FROM {}",
            select.join(", "),
            quoted(source)?
        ))
    }

    fn column_types(&self, view: &str) -> Result<BTreeMap<String, String>, Error> {
        let mut stmt = self.con.prepare(&format!(
            "SELECT column_name, column_type FROM (DESCRIBE {})",
            quoted(view)?
        ))?;
        let rows = stmt.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))?;
        rows.collect()
    }

    pub fn has(&self, dataset: &str) -> bool {
        self.datasets.contains_key(dataset)
    }

    /// A dataset's fields and types, as its Parquet file has them; `email`
    /// says which texts hold addresses (the mapping's semantic type).
    pub fn schema(&self, dataset: &str, email: &[String]) -> Result<Schema, Error> {
        if let Some(schema) = self.mapped.get(dataset) {
            return Ok(schema.clone());
        }
        let mut schema = Schema::default();
        for (name, ty) in self.column_types(dataset)? {
            let t = if email.contains(&name) {
                FieldType::Email
            } else {
                field_type(&ty)
            };
            schema.fields.insert(name, t);
        }
        Ok(schema)
    }

    /// The tables behind a rules file's references: the loaded datasets
    /// they name, with their `where` (lint reports a `where` that does
    /// not parse; such a reference is left out here).
    pub fn tables(&self, file: &RulesFile) -> Tables {
        file.references
            .iter()
            .filter(|(_, r)| self.has(&r.dataset.value))
            .filter_map(|(name, r)| {
                let filter = match &r.filter {
                    Some(w) => Some(parse_expr(&w.value).ok()?),
                    None => None,
                };
                let path = vec![r.dataset.value.clone()];
                Some((name.clone(), Table { path, filter }))
            })
            .collect()
    }

    /// Runs the lint's example queries and compares what they give.
    pub fn examples(&self, queries: &[ExampleQuery], cut: &str) -> Result<Vec<Finding>, Error> {
        let mut findings = Vec::new();
        for q in queries {
            let mut stmt = self.con.prepare(&q.sql)?;
            let cuts = vec![cut; q.cut_params];
            let results = stmt
                .query_map(duckdb::params_from_iter(cuts.iter()), |r| {
                    r.get::<_, Value>(0)
                })?
                .map(|v| v.map(as_bool))
                .collect::<Result<Vec<_>, _>>()?;
            findings.extend(check_examples(q, &results));
        }
        Ok(findings)
    }

    /// Every rule over the dataset's cut, in one pass: counts per rule.
    pub fn test(
        &self,
        dataset: &str,
        rules: &[Compiled],
        tables: &Tables,
        cut: &str,
    ) -> Result<Vec<Counts>, Error> {
        if rules.is_empty() {
            return Ok(Vec::new());
        }
        let mut columns = Vec::new();
        let mut cut_params = 0;
        for (i, rule) in rules.iter().enumerate() {
            let sql = quality_rules_sql::rule(rule, Engine::DuckDb, tables)
                .map_err(|e| invalid(format!("{}: {e:?}", rule.id)))?;
            cut_params += sql.cut_params;
            columns.push(format!("{} AS \"r{i}\"", sql.text));
        }
        let counts: Vec<String> = (0..rules.len())
            .map(|i| format!("count(*) FILTER (WHERE \"r{i}\"), count(*) FILTER (WHERE NOT \"r{i}\"), count(*) FILTER (WHERE \"r{i}\" IS NULL)"))
            .collect();
        let sql = format!(
            "WITH r AS (SELECT {} FROM {}) SELECT {} FROM r",
            columns.join(", "),
            quoted(dataset)?,
            counts.join(", ")
        );
        let mut stmt = self.con.prepare(&sql)?;
        let cuts = vec![cut; cut_params];
        let row: Vec<u64> = stmt.query_row(duckdb::params_from_iter(cuts.iter()), |r| {
            (0..rules.len() * 3).map(|i| r.get::<_, u64>(i)).collect()
        })?;
        Ok(rules
            .iter()
            .zip(row.chunks(3))
            .map(|(rule, c)| Counts {
                rule: rule.id.clone(),
                applies: c[0] + c[1],
                fails: c[0],
                passes: c[1],
                skipped: c[2],
            })
            .collect())
    }
}

fn quoted(name: &str) -> Result<String, Error> {
    quote_ident(name).map_err(invalid)
}

fn invalid(e: impl std::fmt::Debug) -> Error {
    Error::InvalidParameterName(format!("{e:?}"))
}

fn as_bool(v: Value) -> Option<bool> {
    match v {
        Value::Boolean(b) => Some(b),
        _ => None,
    }
}

/// The rule language's type for a DuckDB column type.
fn field_type(ty: &str) -> FieldType {
    let t = ty.to_ascii_uppercase();
    let numeric = [
        "TINYINT",
        "SMALLINT",
        "INTEGER",
        "BIGINT",
        "HUGEINT",
        "UTINYINT",
        "USMALLINT",
        "UINTEGER",
        "UBIGINT",
        "FLOAT",
        "DOUBLE",
        "DECIMAL",
    ];
    if numeric.iter().any(|n| t.starts_with(n)) {
        FieldType::Number
    } else if t == "DATE" || t.starts_with("TIMESTAMP") {
        FieldType::Date
    } else if t == "BOOLEAN" {
        FieldType::Boolean
    } else {
        FieldType::Text
    }
}
