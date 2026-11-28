//! A job's Trino subject (decision 0090, point 7.1):
//! `job:<job uuid>:<kind>:<dataset or source>`, which Trino's user mapping
//! turns into one user per kind and dataset (`job_rules_<d>`, …,
//! `job_source_<source>`, `job_catalog`). No tenant segment. Only the kinds
//! below and names of `[a-z][a-z0-9_]{0,62}` are taken, so no subject can
//! pass for another kind's user or for a look-alike.

use std::fmt;

/// The kinds of a job's Trino user.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobUserKind {
    /// Rule evaluation's writes into gold.
    Rules,
    /// Counts read back from gold.
    Report,
    /// Bronze and silver loads.
    Extract,
    /// Samples (point 7.4).
    Sample,
    /// Maintenance (`lake_maint`).
    Maint,
    /// A source's listing, profile and page; the target is the source.
    Source,
    /// Creating and dropping a source's catalog; the target is the source.
    Catalog,
    /// One console statement; the target names its run (decision 0090,
    /// ruling 84).
    Console,
}

impl JobUserKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Rules => "rules",
            Self::Report => "report",
            Self::Extract => "extract",
            Self::Sample => "sample",
            Self::Maint => "maint",
            Self::Source => "source",
            Self::Catalog => "catalog",
            Self::Console => "console",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        [
            Self::Rules,
            Self::Report,
            Self::Extract,
            Self::Sample,
            Self::Maint,
            Self::Source,
            Self::Catalog,
            Self::Console,
        ]
        .into_iter()
        .find(|k| k.as_str() == s)
    }
}

/// A job's subject in its Trino token.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobSubject {
    job: String,
    kind: JobUserKind,
    target: String,
}

/// Why a subject was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidJobSubject;

/// Whether `s` is a job id as Trino's mapping takes it: a UUID, lower
/// case, hyphenated (`[0-9a-f-]{36}`, in its 8-4-4-4-12 groups).
fn is_job(s: &str) -> bool {
    s.len() == 36
        && s.bytes().enumerate().all(|(i, b)| match i {
            8 | 13 | 18 | 23 => b == b'-',
            _ => b.is_ascii_digit() || (b'a'..=b'f').contains(&b),
        })
}

/// Whether `s` is a dataset's or a source's name as Trino's mapping takes
/// it: `[a-z][a-z0-9_]{0,62}`.
fn is_name(s: &str) -> bool {
    let mut bytes = s.bytes();
    matches!(bytes.next(), Some(b'a'..=b'z'))
        && s.len() <= 63
        && bytes.all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
}

impl JobSubject {
    /// The subject of `job`'s `kind` user for `target` (a dataset, or a
    /// source for `source` and `catalog`).
    pub fn new(job: &str, kind: JobUserKind, target: &str) -> Result<Self, InvalidJobSubject> {
        if !is_job(job) || !is_name(target) {
            return Err(InvalidJobSubject);
        }
        Ok(Self {
            job: job.to_string(),
            kind,
            target: target.to_string(),
        })
    }

    /// A subject as written in a token, only in its exact form.
    pub fn parse(s: &str) -> Result<Self, InvalidJobSubject> {
        let mut parts = s.split(':');
        let (Some("job"), Some(job), Some(kind), Some(target), None) = (
            parts.next(),
            parts.next(),
            parts.next(),
            parts.next(),
            parts.next(),
        ) else {
            return Err(InvalidJobSubject);
        };
        let kind = JobUserKind::parse(kind).ok_or(InvalidJobSubject)?;
        Self::new(job, kind, target)
    }

    pub fn job(&self) -> &str {
        &self.job
    }

    pub fn kind(&self) -> JobUserKind {
        self.kind
    }

    pub fn target(&self) -> &str {
        &self.target
    }

    /// The Trino user its token maps to.
    pub fn trino_user(&self) -> String {
        match self.kind {
            JobUserKind::Catalog => "job_catalog".into(),
            kind => format!("job_{}_{}", kind.as_str(), self.target),
        }
    }
}

impl fmt::Display for JobSubject {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "job:{}:{}:{}", self.job, self.kind.as_str(), self.target)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const JOB: &str = "7f0c2a52-1c1e-4c43-9a55-0d2f1e0b6a10";

    #[test]
    fn each_kind_names_its_trino_user() {
        let job = JOB;
        for (kind, target, user) in [
            (JobUserKind::Rules, "loans", "job_rules_loans"),
            (JobUserKind::Report, "loans", "job_report_loans"),
            (JobUserKind::Extract, "loans", "job_extract_loans"),
            (JobUserKind::Sample, "loans", "job_sample_loans"),
            (JobUserKind::Maint, "gold", "job_maint_gold"),
            (JobUserKind::Source, "core_db", "job_source_core_db"),
            (JobUserKind::Catalog, "core_db", "job_catalog"),
            (
                JobUserKind::Console,
                "r0b9c6a1e000040008000000000000091",
                "job_console_r0b9c6a1e000040008000000000000091",
            ),
        ] {
            let s = JobSubject::new(job, kind, target).unwrap();
            assert_eq!(s.trino_user(), user);
            let text = s.to_string();
            assert_eq!(text, format!("job:{JOB}:{}:{target}", kind.as_str()));
            assert_eq!(JobSubject::parse(&text), Ok(s));
        }
    }

    #[test]
    fn look_alikes_are_refused() {
        let job = JOB;
        for target in [
            "",
            "Loans",
            "1loans",
            "loans-x",
            "loans:x",
            "loans.x",
            &"a".repeat(64),
        ] {
            assert!(
                JobSubject::new(job, JobUserKind::Rules, target).is_err(),
                "{target}"
            );
        }
        for s in [
            format!("job:{JOB}:admin:loans"),
            format!("job:{JOB}:rules:loans:x"),
            format!("job:{JOB}:rules"),
            format!("job:{}:rules:loans", JOB.to_uppercase()),
            format!("job:{}:rules:loans", JOB.replace('-', "")),
            format!("job:00000000-0000-0000-0000-00000000000a:{JOB}:rules:loans"),
            format!("jobs:{JOB}:rules:loans"),
            "job:x:rules:loans".to_string(),
        ] {
            assert_eq!(JobSubject::parse(&s), Err(InvalidJobSubject), "{s}");
        }
    }
}
