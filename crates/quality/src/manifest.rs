//! A cut's files (decision 0035, point 5): what an extraction and an
//! evaluation read, with what, and what came out, written once next to the
//! lake's tables so an audit needs no database. Names, hashes, snapshot ids, compiled SQL and
//! counts: never a value from a source.

use async_trait::async_trait;
use axum::http::StatusCode;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::cuts::{ExtractReport, RangeRead, SourceColumn};
use crate::worker_api::{Report, RuleCounts, RulesFileRun, SnapshotRead};

/// The manifest's format, for whoever reads it years later.
pub const FORMAT: u32 = 1;

/// `manifests/<dataset>/<cut>/evaluate/<job>.json`: one per evaluation, so a
/// replaced cut keeps both runs.
pub fn evaluate_key(dataset: &str, cut: &str, job: Uuid) -> String {
    format!("manifests/{dataset}/{cut}/evaluate/{job}.json")
}

/// `manifests/<dataset>/<cut>/extract/<job>.json`: one per extraction.
pub fn extract_key(dataset: &str, cut: &str, job: Uuid) -> String {
    format!("manifests/{dataset}/{cut}/extract/{job}.json")
}

/// An extraction's manifest: what it read from the source and the bronze
/// snapshot it committed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExtractManifest {
    pub format: u32,
    pub job: Uuid,
    pub dataset: String,
    pub cut: String,
    /// When the report arrived (RFC 3339, UTC).
    pub reported_at: String,
    pub duration_ms: i64,
    /// The source's table, as the mapping names it.
    pub source: String,
    /// The source's columns as the extraction saw them.
    pub schema: Vec<SourceColumn>,
    /// Each range read and its rows.
    pub ranges: Vec<RangeRead>,
    /// The bronze snapshot committed (decision 0017).
    pub snapshot: String,
    /// The silver snapshot committed, if silver was written (decision 0061).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub silver_snapshot: Option<String>,
}

impl ExtractManifest {
    /// From the job's report.
    pub fn new(job: Uuid, dataset: &str, report: &ExtractReport, reported_at: String) -> Self {
        Self {
            format: FORMAT,
            job,
            dataset: dataset.to_string(),
            cut: report.cut.clone(),
            reported_at,
            duration_ms: report.duration_ms,
            source: report.source.clone(),
            schema: report.schema.clone(),
            ranges: report.ranges.clone(),
            snapshot: report.snapshot.clone(),
            silver_snapshot: report.silver_snapshot.clone(),
        }
    }
}

/// An evaluation's manifest.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EvaluateManifest {
    pub format: u32,
    pub job: Uuid,
    pub dataset: String,
    pub cut: String,
    /// When the report arrived (RFC 3339, UTC).
    pub reported_at: String,
    pub duration_ms: i64,
    pub records: i64,
    /// The catalog's commit the rules came from (decision 0021).
    pub catalog_commit: String,
    /// The rule compiler's version.
    pub compiler: String,
    /// Each rules file that ran, with its SHA-256.
    pub files: Vec<RulesFileRun>,
    /// The bronze snapshots read (decision 0017).
    pub snapshots: Vec<SnapshotRead>,
    /// Per rule: its SQL as the compiler gave it, and its counts.
    pub rules: Vec<RuleRun>,
}

/// One rule as it ran.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuleRun {
    pub rule: String,
    pub sql: Option<String>,
    pub applies: i64,
    pub failures: i64,
}

impl EvaluateManifest {
    /// From the job's report.
    pub fn new(job: Uuid, dataset: &str, report: &Report, reported_at: String) -> Self {
        let sql = |rule: &str| {
            report
                .compiled
                .iter()
                .find(|c| c.rule == rule)
                .map(|c| c.sql.clone())
        };
        Self {
            format: FORMAT,
            job,
            dataset: dataset.to_string(),
            cut: report.cut.clone(),
            reported_at,
            duration_ms: report.duration_ms,
            records: report.records,
            catalog_commit: report.rules_commit.clone(),
            compiler: report.compiler.clone(),
            files: report.files.clone(),
            snapshots: report.snapshots.clone(),
            rules: report
                .rules
                .iter()
                .map(
                    |RuleCounts {
                         rule,
                         applies,
                         failures,
                         ..
                     }| RuleRun {
                        rule: rule.clone(),
                        sql: sql(rule),
                        applies: *applies,
                        failures: *failures,
                    },
                )
                .collect(),
        }
    }
}

/// Where manifests are written: the warehouse bucket.
#[async_trait]
pub trait ManifestStore: Send + Sync {
    /// Writes `body` at `key` unless something is there already; `false`
    /// if it was (a repeated report).
    async fn put_once(&self, warehouse: &str, key: &str, body: Vec<u8>) -> Result<bool, String>;

    /// The manifest at `key`, or `None` if there is none: a backfill reads
    /// a retained cut's back (decision 0090, step 22). A store that cannot
    /// read refuses.
    async fn get(&self, _warehouse: &str, _key: &str) -> Result<Option<Vec<u8>>, String> {
        Err("this manifest store does not read".into())
    }
}

/// A job's manifest, written once before what it reports is stored: the
/// database never points to a manifest that is not there. A repeated
/// report finds it written; its key either way.
pub(crate) async fn write(
    store: &dyn ManifestStore,
    key: String,
    manifest: &impl Serialize,
) -> Result<String, StatusCode> {
    let body =
        serde_json::to_vec_pretty(manifest).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    store
        .put_once(crate::lake::WAREHOUSE, &key, body)
        .await
        .map_err(|e| {
            tracing::error!("manifest: {e}");
            StatusCode::SERVICE_UNAVAILABLE
        })?;
    Ok(key)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::worker_api::CompiledRule;

    #[test]
    fn a_manifest_carries_each_rules_sql_and_counts_under_its_jobs_key() {
        let job = Uuid::nil();
        assert_eq!(
            evaluate_key("loans", "2021-03-10", job),
            "manifests/loans/2021-03-10/evaluate/00000000-0000-0000-0000-000000000000.json"
        );
        let report = Report {
            cut: "2021-03-10".into(),
            records: 10,
            duration_ms: 5,
            rules_commit: "abc".into(),
            rules: vec![RuleCounts {
                rule: "V-01".into(),
                applies: 10,
                failures: 2,
                field: None,
                pattern: None,
                dimension: None,
                severity: None,
            }],
            snapshots: vec![SnapshotRead {
                dataset: "loans".into(),
                snapshot: "42".into(),
                layer: Some("silver".into()),
            }],
            files: vec![RulesFileRun {
                file: "a.yaml".into(),
                sha256: "00".into(),
            }],
            compiled: vec![CompiledRule {
                rule: "V-01".into(),
                sql: "x IS NULL".into(),
            }],
            compiler: "0.1.0".into(),
            fields: Vec::new(),
            igcd: None,
            entities: Vec::new(),
            groups: Vec::new(),
            distributions: Vec::new(),
            outliers: Vec::new(),
            stages: Vec::new(),
            cells: Vec::new(),
            queues: Vec::new(),
        };
        let m = EvaluateManifest::new(job, "loans", &report, "2026-10-31T00:00:00Z".into());
        assert_eq!(m.rules[0].sql.as_deref(), Some("x IS NULL"));
        assert_eq!((m.rules[0].applies, m.rules[0].failures), (10, 2));
        assert_eq!(m.snapshots[0].snapshot, "42");
    }
}
