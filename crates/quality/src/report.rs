//! What an `evaluate` job reports (decisions 0018, 0031 and 0035): counts
//! per rule, and what it read and ran, for its manifest. Re-exported by
//! [`crate::worker_api`].

use serde::{Deserialize, Serialize};

/// An `evaluate` job's results for a dataset's cut: counts only.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Report {
    /// `YYYY-MM-DD`.
    pub cut: String,
    pub records: i64,
    pub duration_ms: i64,
    /// The rules' version the job evaluated.
    pub rules_commit: String,
    pub rules: Vec<RuleCounts>,
    /// The bronze snapshots it read: its dataset's and each reference's.
    #[serde(default)]
    pub snapshots: Vec<SnapshotRead>,
    /// Each rules file that ran, with its SHA-256.
    #[serde(default)]
    pub files: Vec<RulesFileRun>,
    /// Each rule's SQL as the compiler gave it to the engine.
    #[serde(default)]
    pub compiled: Vec<CompiledRule>,
    /// The rule compiler's version.
    #[serde(default)]
    pub compiler: String,
    /// Each field's profile in the cut (decision 0031's `field_quality`);
    /// none from an older worker.
    #[serde(default)]
    pub fields: Vec<FieldQuality>,
    /// The cut's IGCD (decision 0057): the mean over its entities, or over
    /// its records without an entity; none without rules or from an older
    /// worker.
    #[serde(default)]
    pub igcd: Option<f64>,
    /// Each entity's ICD in the cut, by the keyed hash of its key.
    #[serde(default)]
    pub entities: Vec<EntityIcdRow>,
    /// Each group of each dimension (decision 0031's `group_metrics`).
    #[serde(default)]
    pub groups: Vec<GroupMetrics>,
    /// Each drift variable's percentiles (decision 0031's
    /// `distribution_checks`).
    #[serde(default)]
    pub distributions: Vec<Distribution>,
    /// The outlier amount's outliers within each group of each dimension.
    #[serde(default)]
    pub outliers: Vec<OutlierGroup>,
    /// How long each stage took, in the order they ran (decision 0031's time
    /// per stage); none from an older worker.
    #[serde(default)]
    pub stages: Vec<StageTime>,
    /// Each SFC cell's counts among the active records (decision 0058).
    #[serde(default)]
    pub cells: Vec<SfcCell>,
    /// Each remediation queue's findings by the cut they opened at
    /// (decision 0059); the findings themselves go to the lake.
    #[serde(default)]
    pub queues: Vec<QueueFindings>,
}

impl Report {
    /// Every entity by a hash (64 hex characters): never a key a worker
    /// sent by mistake.
    pub fn entities_hashed(&self) -> bool {
        self.entities
            .iter()
            .all(|e| e.0.len() == 64 && e.0.bytes().all(|b| b.is_ascii_hexdigit()))
    }
}

/// An entity's ICD (decision 0057), compact as there is one per client:
/// `[hash, icd, segment]`, the hash hex HMAC-SHA-256 of the normalized key
/// with the entity key, never the key.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EntityIcdRow(pub String, pub Option<f64>, pub Option<String>);

/// A group of a dimension in a cut: a value of a field holding the
/// `dimension` role (an office, an advisor), none when empty.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GroupMetrics {
    pub dimension: String,
    pub group: Option<String>,
    pub records: i64,
    /// Records with a blocking or major failure in a critical or high
    /// field: serious, until rules carry their cause.
    pub serious: i64,
    pub icd: Option<f64>,
}

/// A remediation queue's findings in a cut (decision 0059), by the cut
/// they opened at (`since`): those still open, those resolved at this cut,
/// and the open ones' summed priority. A queue is a group of a dimension,
/// none when empty.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QueueFindings {
    pub dimension: String,
    pub group: Option<String>,
    /// `YYYY-MM-DD`.
    pub since: String,
    pub open: i64,
    pub resolved: i64,
    pub priority: f64,
}

/// A drift variable's percentiles 0 to 100 in a cut: the PSI of a cut
/// against the one before reads them.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Distribution {
    pub variable: String,
    pub percentiles: Vec<f64>,
}

/// The outlier amount in a group of a dimension: its positive amounts, and
/// those whose robust z is below -3.5 (`low`) or above 3.5 (`high`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutlierGroup {
    pub variable: String,
    pub dimension: String,
    pub group: Option<String>,
    pub records: i64,
    pub low: i64,
    pub high: i64,
}

/// A stage of an evaluation (`load`, `rules`, `profile`, `icd`, `recency`,
/// `distributions`) and its milliseconds.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StageTime {
    pub stage: String,
    pub ms: i64,
}

/// High from 0.90, medium from 0.75, low below (the guide's formulas).
pub fn band(icd: f64) -> &'static str {
    if icd >= 0.90 {
        "high"
    } else if icd >= 0.75 {
        "medium"
    } else {
        "low"
    }
}

/// An SFC cell in a cut (decision 0058): a field holding the `sfc` role,
/// the active records (`total`, the guide's n) and those where it is right
/// (`ok`): filled, and no rule that reads it fails.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SfcCell {
    pub field: String,
    pub ok: i64,
    pub total: i64,
}

/// Whether a cell passes and how far it is from changing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CellState {
    pub passes: bool,
    /// Right records it lacks to pass.
    pub missing: i64,
    /// Right records it can lose and still pass.
    pub margin: i64,
}

/// The guide's §4 in integers, so 89.99 % never rounds to a pass: the
/// threshold `ceil(0.90 n)`; a cell without active records does not pass
/// and misses nothing.
pub fn cell(ok: i64, total: i64) -> CellState {
    let threshold = (9 * total.max(0) + 9) / 10;
    CellState {
        passes: total > 0 && ok >= threshold,
        missing: (threshold - ok).max(0),
        margin: (ok - threshold).max(0),
    }
}

/// A field's profile in a cut: counts, and the shape its values most often
/// take (each digit a `9`, each letter an `A`), never a value.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FieldQuality {
    pub field: String,
    /// Records where it is empty (`NULL` or blank, decision 0033).
    pub empty: i64,
    /// Distinct values among the rest.
    pub distinct: i64,
    /// The commonest shape, cut at forty characters; none if all empty.
    pub pattern: Option<String>,
    /// The records with that shape.
    pub pattern_count: i64,
    /// A floating-point number, which has no shape (decision 0090).
    #[serde(default)]
    pub floating: bool,
    /// Its mean ICD over the records where it has one (decision 0057).
    #[serde(default)]
    pub icd: Option<f64>,
    /// The `updated` field's records by age at the cut (decision 0031): after
    /// the cut, up to each of 30, 90, 180, 365 and 730 days, then older.
    #[serde(default)]
    pub recency: Option<Vec<i64>>,
}

/// A rules file as a job ran it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RulesFileRun {
    pub file: String,
    pub sha256: String,
}

/// A rule's SQL as the engine ran it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompiledRule {
    pub rule: String,
    pub sql: String,
}

/// One dataset's table as a job read it, by Iceberg snapshot id (text: a
/// 64-bit id does not survive a JavaScript number).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotRead {
    pub dataset: String,
    pub snapshot: String,
    /// `silver` when the cut was read from silver (decision 0061); absent
    /// when from bronze, through the mapping.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub layer: Option<String>,
}

/// One rule's counts: the records it applies to and those that fail it,
/// with what the charts group it by (decision 0031): its field, pattern,
/// dimension and severity as compiled. A report from an older worker has
/// none of them.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RuleCounts {
    pub rule: String,
    pub applies: i64,
    pub failures: i64,
    #[serde(default)]
    pub field: Option<String>,
    #[serde(default)]
    pub pattern: Option<String>,
    #[serde(default)]
    pub dimension: Option<String>,
    #[serde(default)]
    pub severity: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn state(ok: i64, total: i64) -> (bool, i64, i64) {
        let s = cell(ok, total);
        (s.passes, s.missing, s.margin)
    }

    #[test]
    fn a_cell_passes_from_ninety_percent_with_its_missing_and_margin() {
        // ceil(0.90 · 100) = 90; ceil(0.90 · 7) = 7; ceil(0.90 · 11) = 10.
        assert_eq!(state(92, 100), (true, 0, 2));
        assert_eq!(state(90, 100), (true, 0, 0));
        assert_eq!(state(85, 100), (false, 5, 0));
        assert_eq!(state(6, 7), (false, 1, 0), "85.7 %");
        assert_eq!(state(10, 11), (true, 0, 0), "90.9 %");
        assert_eq!(
            state(8999, 10000),
            (false, 1, 0),
            "89.99 % does not round up"
        );
        assert_eq!(state(0, 0), (false, 0, 0), "no active records");
    }
}
