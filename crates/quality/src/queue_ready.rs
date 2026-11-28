//! Whether the queue keeps up (decision 0071, point 8): the jobs ready to
//! be taken and how long the oldest has waited, by kind. Counts and seconds
//! only: no dataset or job leaves here. [`QueueGauges`]
//! holds them for the API's metrics file, every kind of [`KINDS`] and no
//! other.

use std::collections::HashMap;
use std::sync::atomic::AtomicU64;

use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::registry::Registry;
use sqlx::PgPool;

/// Every kind of job `quality.jobs` allows (its `jobs_kind_check`): the
/// fixed list a metric's `kind` label takes its value from.
pub const KINDS: [&str; 10] = [
    "map",
    "extract",
    "evaluate",
    "test_rule",
    "maintain",
    "tables",
    "lake_tables",
    "records",
    "query",
    "entity_key",
];

/// The ready jobs of one kind.
#[derive(Debug, Clone, PartialEq, sqlx::FromRow)]
pub struct Ready {
    pub kind: String,
    pub jobs: i64,
    /// Since the oldest became ready (`not_before`), in seconds.
    pub oldest_seconds: f64,
}

/// The kinds with a job ready now, queued and past its backoff.
pub async fn ready(pool: &PgPool) -> Result<Vec<Ready>, sqlx::Error> {
    sqlx::query_as(
        "SELECT kind, count(*) AS jobs, \
                extract(epoch FROM now() - min(not_before))::float8 AS oldest_seconds \
           FROM quality.jobs \
          WHERE state = 'queued' AND not_before <= now() \
          GROUP BY kind",
    )
    .fetch_all(pool)
    .await
}

type ByKind = Family<[(&'static str, &'static str); 1], Gauge>;
type SecondsByKind = Family<[(&'static str, &'static str); 1], Gauge<f64, AtomicU64>>;

/// `qd_queue_ready_jobs` and `qd_queue_oldest_ready_seconds`, by kind.
#[derive(Debug, Clone, Default)]
pub struct QueueGauges {
    ready: ByKind,
    oldest: SecondsByKind,
}

impl QueueGauges {
    /// The two gauges, registered in the API's `registry`.
    pub fn register(registry: &mut Registry) -> Self {
        let gauges = Self::default();
        registry.register(
            "qd_queue_ready_jobs",
            "Jobs ready to be taken now, by kind.",
            gauges.ready.clone(),
        );
        registry.register(
            "qd_queue_oldest_ready_seconds",
            "How long the oldest ready job of each kind has waited, in seconds.",
            gauges.oldest.clone(),
        );
        gauges
    }

    /// The rows of every tenant, summed by kind: each kind of [`KINDS`] is
    /// set, 0 when none is ready; a kind outside it never becomes a label.
    pub fn set(&self, rows: &[Ready]) {
        let mut by_kind: HashMap<&str, (i64, f64)> = HashMap::new();
        for row in rows {
            let entry = by_kind.entry(row.kind.as_str()).or_default();
            entry.0 += row.jobs;
            entry.1 = entry.1.max(row.oldest_seconds);
        }
        for kind in KINDS {
            let (jobs, oldest) = by_kind.get(kind).copied().unwrap_or_default();
            self.ready.get_or_create(&[("kind", kind)]).set(jobs);
            self.oldest.get_or_create(&[("kind", kind)]).set(oldest);
        }
    }
}

#[cfg(test)]
mod tests {
    use prometheus_client::registry::Registry;

    use super::{QueueGauges, Ready, KINDS};

    fn ready(kind: &str, jobs: i64, oldest_seconds: f64) -> Ready {
        Ready {
            kind: kind.into(),
            jobs,
            oldest_seconds,
        }
    }

    #[test]
    fn the_queue_gauges_take_only_the_fixed_kinds() {
        let mut registry = Registry::default();
        let gauges = QueueGauges::register(&mut registry);
        // Two tenants' rows summed; a kind outside the list is dropped.
        gauges.set(&[
            ready("evaluate", 2, 30.0),
            ready("evaluate", 1, 90.0),
            ready("not-a-kind", 5, 1.0),
        ]);
        let mut text = String::new();
        prometheus_client::encoding::text::encode(&mut text, &registry).unwrap();
        let samples = logging::metrics::samples(&text).unwrap();
        assert_eq!(samples.len(), 2 * KINDS.len());
        for s in &samples {
            assert_eq!(s.labels.len(), 1, "{s:?}");
            assert!(KINDS.contains(&s.label("kind").unwrap()), "{s:?}");
        }
        let value = |name: &str, kind: &str| {
            samples
                .iter()
                .find(|s| s.name == name && s.label("kind") == Some(kind))
                .unwrap()
                .value
        };
        assert!((value("qd_queue_ready_jobs", "evaluate") - 3.0).abs() < f64::EPSILON);
        assert!((value("qd_queue_oldest_ready_seconds", "evaluate") - 90.0).abs() < f64::EPSILON);
        assert!(value("qd_queue_ready_jobs", "map").abs() < f64::EPSILON);
    }

    /// The kinds `quality.jobs` allows, from the last migration that says them.
    fn kinds_allowed() -> Vec<String> {
        let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/migrations");
        let mut files: Vec<_> = std::fs::read_dir(dir)
            .unwrap()
            .map(|f| f.unwrap().path())
            .collect();
        files.sort();
        let sql = files
            .iter()
            .rev()
            .map(|f| std::fs::read_to_string(f).unwrap())
            .find(|sql| sql.contains("ADD CONSTRAINT jobs_kind_check"))
            .unwrap();
        let check = sql.split("jobs_kind_check").last().unwrap();
        let list = &check[check.find("IN (").unwrap() + 4..];
        list[..list.find(')').unwrap()]
            .split(',')
            .map(|k| k.trim().trim_matches('\'').to_string())
            .collect()
    }

    #[test]
    fn the_kinds_are_the_ones_the_queue_allows() {
        let mut kinds: Vec<_> = KINDS.iter().map(ToString::to_string).collect();
        let mut allowed = kinds_allowed();
        kinds.sort();
        allowed.sort();
        assert_eq!(kinds, allowed);
    }
}
