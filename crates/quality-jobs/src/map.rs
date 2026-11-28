//! A `map` job's bundle (decision 0038; decision 0090, ruling 79): a
//! source table's profile over a sample, read as the source's own user
//! with its secrets as extra credentials: one row of counts, never a
//! value, posted on the values route; the core turns it into the
//! dataset's proposal as it does a worker's.

use quality::bundle::{Bundle, Expect};
use quality_rules_trino::{self as trino, TablePath, PROFILE_PER_COLUMN};

use crate::steps::Steps;
use crate::{CompileError, Job};

/// The id of the statement whose row is the profile.
pub const PROFILE: &str = "profile";

/// What a map profiles: the source's table, its columns in order, the
/// sample's size and the patterns each value is matched against.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MapInput<'a> {
    pub source: &'a str,
    pub schema: &'a str,
    pub table: &'a str,
    pub columns: &'a [String],
    pub sample: u64,
    pub patterns: &'a [&'a str],
}

/// The profile's bundle: one row of `bigint`s named as
/// [`trino::source_profile`] names them.
pub fn map(job: &Job<'_>, input: &MapInput<'_>) -> Result<Bundle, CompileError> {
    if input.columns.is_empty() {
        return Err(CompileError::Incomplete("columns"));
    }
    let table = TablePath {
        catalog: format!("src_{}", input.source),
        namespace: input.schema.to_owned(),
        table: input.table.to_owned(),
    };
    let count = |name: String| (name, "bigint".to_owned());
    let mut columns = vec![count("sampled".into())];
    for i in 0..input.columns.len() {
        for what in PROFILE_PER_COLUMN {
            columns.push(count(trino::profile_column(i, what)));
        }
        for p in 0..input.patterns.len() {
            columns.push(count(trino::profile_column(i, &format!("pattern_{p}"))));
        }
    }
    let mut steps = Steps::default();
    steps.job_read(
        PROFILE.into(),
        trino::source_profile(&table, input.columns, input.sample, input.patterns)?,
        Expect::Values {
            columns,
            max_rows: 1,
        },
    );
    steps.source_reads_last();
    Ok(job.bundle(steps))
}

#[cfg(test)]
mod tests {
    use super::*;
    use quality::bundle::RunAs;

    #[test]
    fn a_profile_is_one_row_of_counts_read_as_the_source() {
        let job = Job {
            run: "0b9c6a1e-0000-4000-8000-000000000071",
            kind: "map",
            dataset: Some("clients"),
            cut: None,
            attempt: 1,
            run_seq: None,
            catalog_commit: None,
        };
        let columns = ["id".to_string(), "email".to_string()];
        let input = MapInput {
            source: "core",
            schema: "public",
            table: "clients",
            columns: &columns,
            sample: 10_000,
            patterns: &["a", "b"],
        };
        let b = map(&job, &input).unwrap();
        assert!(b.well_formed());
        let s = &b.statements[0];
        assert_eq!(
            (s.id.as_str(), s.run_as, s.reads_source),
            (PROFILE, RunAs::Job, true)
        );
        let Expect::Values { columns, max_rows } = &s.expects else {
            panic!("a values read");
        };
        assert_eq!(*max_rows, 1);
        assert_eq!(columns.len(), 1 + 2 * 6);
        assert_eq!(columns[1].0, "c0_empty");
        assert_eq!(columns[12].0, "c1_pattern_1");
        assert!(columns.iter().all(|(_, t)| t == "bigint"));
        assert!(s.sql.contains("LIMIT 10000"));
    }
}
