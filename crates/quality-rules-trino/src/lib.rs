//! The statements of a quality job on Trino 484 (decision 0090, step 10):
//! the core writes every statement a job runner submits, from the same
//! tree DuckDB's SQL comes from (`quality-rules-sql`, `Engine::Trino`).
//! Pure: it takes a mapping, compiled rules and table paths and returns
//! SQL text, never a connection. Every read is pinned to a snapshot
//! (`FOR VERSION AS OF`); the cut is a bound parameter (`?`), in order.

mod cells;
mod distribution;
mod extract;
mod findings;
mod gold;
mod gold_write;
mod icd;
mod loads;
mod profile;
mod read;
mod rules;
mod scratch;

pub use distribution::{outliers, variable_percentiles, Method};
pub use extract::{
    bronze_type, create_bronze, create_catalog, extract, split, Connector, Conversion, Range,
};
pub use findings::{finding_groups, findings, queues, Findings};
pub use gold::{install, GoldTable, DETAILS, GOLD, RUN_SUMMARY};
pub use gold_write::{delete_older, flip, insert, older_rows, Run};
pub use icd::{measured, scratch_insert, IcdRule, Measured, FINDINGS_KEY, SCRATCH};
pub use loads::{
    accepted, create_silver, delete_load, derive_silver, loads_of_cut, migrate_to_loads, Accepted,
    LOAD,
};
pub use profile::{field_profile, recency, shape_of, RECENCY_DAYS, SHAPE_LEN};
pub use read::{current_snapshot, pinned, typed_select, Pinned, TablePath};
pub use rules::rule_counts;
pub use scratch::{cells, entities, groups, means, scratch_delete, scratch_rows};

use quality_rules_sql::quote_ident;

/// One statement and how many times the cut is bound to it, in order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Statement {
    pub sql: String,
    pub cut_params: usize,
}

/// What cannot be written safely, or a rule that does not compile to SQL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Sql(quality_rules_sql::Error),
    /// A rule's SQL could not be written: its id and why.
    Rule(String, quality_rules_sql::Error),
    /// A field the mapping does not have.
    UnknownField(String),
}

impl From<quality_rules_sql::Error> for Error {
    fn from(e: quality_rules_sql::Error) -> Self {
        Self::Sql(e)
    }
}

fn quoted(name: &str) -> Result<String, Error> {
    Ok(quote_ident(name)?)
}
