//! A `tables` job's bundle (decision 0090, ruling 68): a source's listing
//! read from its Trino catalog's `information_schema` as the source's own
//! user, with its secrets as extra credentials; the rows (schema, table,
//! column, Trino type: names only) go back in the report, and the core
//! keeps them in `quality.source_tables`.

use quality::bundle::{Bundle, Expect};
use quality_rules_trino as trino;

use crate::steps::Steps;
use crate::{CompileError, Job};

/// The most rows a listing gives back: as many tables as the core keeps,
/// with up to 100 columns each on average.
pub const MAX_LISTING_ROWS: u64 = 1_000_000;

/// The id of the statement whose rows are the listing.
pub const LISTING: &str = "listing";

/// The listing's bundle for `source`, its system schemas left out.
pub fn tables(job: &Job<'_>, source: &str, system: &[&str]) -> Result<Bundle, CompileError> {
    let mut steps = Steps::default();
    let text = |n: &str| (n.to_string(), "varchar".to_string());
    steps.job_read(
        LISTING.into(),
        trino::source_listing(source, system)?,
        Expect::Rows {
            columns: vec![
                text("table_schema"),
                text("table_name"),
                text("column_name"),
                text("data_type"),
            ],
            max_rows: MAX_LISTING_ROWS,
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
    fn a_listing_reads_the_sources_catalog_as_its_user() {
        let job = Job {
            run: "0b9c6a1e-0000-4000-8000-000000000041",
            kind: "tables",
            dataset: None,
            cut: None,
            attempt: 1,
            run_seq: None,
            catalog_commit: None,
        };
        let b = tables(&job, "core", &["pg_catalog"]).unwrap();
        assert!(b.well_formed());
        let s = &b.statements[0];
        assert_eq!(
            (s.id.as_str(), s.run_as, s.reads_source),
            (LISTING, RunAs::Job, true)
        );
        assert!(s.sql.starts_with(
            "SELECT table_schema, table_name, column_name, data_type FROM \"src_core\".\"information_schema\".\"columns\""
        ));
        assert!(s
            .sql
            .contains("NOT IN ('information_schema', 'pg_catalog')"));
        assert!(tables(&job, "x\"y", &[]).is_err() || tables(&job, "x\u{0}", &[]).is_err());
    }
}
