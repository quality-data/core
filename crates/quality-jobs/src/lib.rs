//! A job's bundle compiled for Trino (decision 0090, amendment A1): from
//! the job, its run and what the core has approved, the ordered statements
//! a job runner submits. Pure: the server gathers the input (PostgreSQL,
//! the catalog, the sequence) and signs what this returns.

mod console;
mod evaluate;
mod extract;
mod gold;
mod map;
mod prepare;
mod records;
mod rule_test;
mod steps;
mod tables;
mod test_rule;

pub use console::console;
pub use evaluate::{
    evaluate, AtSnapshots, EvaluateInput, ReferencePin, INDEX_RULES, INDEX_SUMMARY,
};
pub use extract::{extract, ExtractInput};
pub use gold::{retract, slas, RetractInput, SlasInput};
pub use map::{map, MapInput, PROFILE};
pub use prepare::{prepare, PrepareInput};
pub use records::{records, RecordsInput, PAGE_READ};
pub use rule_test::{rule_test, RuleTestInput, COUNTS};
pub use tables::{tables, LISTING, MAX_LISTING_ROWS};
pub use test_rule::{test_rule, TestRuleInput};

use quality::bundle::Bundle;

/// The version written in every bundle, so a job runner's report names the
/// compiler that wrote its statements.
pub const COMPILER_VERSION: &str = concat!("quality-jobs ", env!("CARGO_PKG_VERSION"));

/// The job as the operator handed it, with the run's ids.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Job<'a> {
    /// The run, as the job runner names it.
    pub run: &'a str,
    pub kind: &'a str,
    pub dataset: Option<&'a str>,
    /// `YYYY-MM-DD`.
    pub cut: Option<&'a str>,
    pub attempt: u32,
    /// For gold's writers: the core's sequence, taken at this compile.
    pub run_seq: Option<i64>,
    pub catalog_commit: Option<&'a str>,
}

/// Why a job could not be compiled: a class, never the rules' content.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompileError {
    /// The job lacks what its kind needs (a dataset, a cut, a `run_seq`).
    Incomplete(&'static str),
    /// A statement could not be written safely.
    Statement(quality_rules_trino::Error),
}

impl From<quality_rules_trino::Error> for CompileError {
    fn from(e: quality_rules_trino::Error) -> Self {
        Self::Statement(e)
    }
}

impl Job<'_> {
    fn dataset(&self) -> Result<&str, CompileError> {
        self.dataset.ok_or(CompileError::Incomplete("dataset"))
    }

    fn run_seq(&self) -> Result<i64, CompileError> {
        self.run_seq.ok_or(CompileError::Incomplete("run_seq"))
    }

    fn bundle(&self, steps: steps::Steps) -> Bundle {
        Bundle {
            job_id: self.run.to_owned(),
            kind: self.kind.to_owned(),
            dataset: self.dataset.map(str::to_owned),
            cut: self.cut.map(str::to_owned),
            attempt: self.attempt,
            run_id: Some(self.run.to_owned()),
            run_seq: self.run_seq.and_then(|s| u64::try_from(s).ok()),
            catalog_commit: self.catalog_commit.map(str::to_owned),
            compiler_version: COMPILER_VERSION.to_owned(),
            placeholders: steps.placeholders,
            statements: steps.statements,
        }
    }
}
