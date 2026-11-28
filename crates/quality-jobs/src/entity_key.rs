//! An `entity_key` job's bundle (decision 0090, rulings 81, 85 and 102):
//! one values read, as the run's lookup user `job_lookup_r<run>`, whose
//! `{{document}}` text placeholder the job runner fills from the run's
//! document route; Trino hashes it with the findings key and answers the
//! entity's ICD and band per dataset. The document is never in the bundle.

use quality::bundle::{Bundle, Expect, PlaceholderType};
use quality_rules_trino as trino;

use crate::steps::Steps;
use crate::{CompileError, Job};

/// The id of the statement whose rows are the entity's results.
pub const ENTITY_READ: &str = "entity";
/// The placeholder the typed document fills.
pub const DOCUMENT: &str = "document";

/// The lookup's bundle over `datasets`, those its asker may view.
pub fn entity_key(job: &Job<'_>, datasets: &[String]) -> Result<Bundle, CompileError> {
    if datasets.is_empty() {
        return Err(CompileError::Incomplete("datasets"));
    }
    let text = |n: &str| (n.to_owned(), "varchar".to_owned());
    let mut steps = Steps::default();
    steps.text_placeholder(DOCUMENT, PlaceholderType::Text);
    steps.job_read(
        ENTITY_READ.into(),
        trino::entity_lookup(datasets)?,
        Expect::Values {
            columns: vec![
                text("dataset"),
                text("cut"),
                ("icd".into(), "double".into()),
                text("band"),
            ],
            max_rows: u64::try_from(datasets.len())
                .unwrap_or(u64::MAX)
                .min(quality::bundle::MAX_VALUES_ROWS),
        },
    );
    Ok(job.bundle(steps))
}

#[cfg(test)]
mod tests {
    use super::*;
    use quality::bundle::RunAs;

    #[test]
    fn a_lookup_carries_a_text_token_and_never_the_document() {
        let job = Job {
            run: "0b9c6a1e-0000-4000-8000-000000000101",
            kind: "entity_key",
            dataset: None,
            cut: None,
            attempt: 1,
            run_seq: None,
            catalog_commit: None,
        };
        let b = entity_key(&job, &["loans".into()]).unwrap();
        assert!(b.well_formed());
        assert_eq!(b.placeholders[0].ty, PlaceholderType::Text);
        assert!(b.placeholders[0].from.is_none());
        let s = &b.statements[0];
        assert_eq!((s.id.as_str(), s.run_as), (ENTITY_READ, RunAs::Job));
        assert!(s.sql.contains("{{document}}"));
        let other = Job {
            kind: "records",
            ..job.clone()
        };
        assert!(!entity_key(&other, &["loans".into()]).unwrap().well_formed());
        assert!(entity_key(&job, &[]).is_err());
    }
}
