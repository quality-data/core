//! The jobs a person asks for, handed to the orchestrator (decision 0090,
//! rulings 92(a), 100 and 103): each `records`, `map`, `tables`,
//! `test_rule` or console (`query`) job queued within its hour starts one
//! chain of one step, through the [`Chains`] port (the server calls the
//! orchestrator's `POST …/internal/chains` with the core's machine
//! certificate and its own machine token). A job is handed over once: the
//! chain the orchestrator answered is kept; a failed call is tried again
//! on the next round.

use async_trait::async_trait;
use serde_json::{json, Value};
use sqlx::PgPool;
use uuid::Uuid;

use crate::schedules_api::Step;

/// The kinds a person asks for that run on Trino as a chain of their own.
pub const ON_DEMAND: &[&str] = &[
    "records",
    "map",
    "tables",
    "test_rule",
    "query",
    "entity_key",
];

/// The most jobs handed over in one round.
const ROUND: i64 = 50;

/// The port to the orchestrator.
#[async_trait]
pub trait Chains: Send + Sync {
    /// Starts `job`'s chain of `steps`; the orchestrator's chain id.
    async fn start(&self, job: Uuid, steps: &[Step]) -> Result<String, String>;
}

/// A job's one step: its kind, and what its bundle is asked with (its
/// dataset, cut and source, as the job names them).
pub fn step_of(kind: &str, dataset: Option<&str>, parameters: &Value) -> Step {
    let mut p = json!({});
    if let Some(d) = dataset {
        p["dataset"] = json!(d);
    }
    for key in ["cut", "source"] {
        if let Some(v) = parameters.get(key).and_then(Value::as_str) {
            p[key] = json!(v);
        }
    }
    Step {
        kind: kind.to_owned(),
        parameters: p,
        setup: None,
    }
}

/// Hands every on-demand job not handed over yet to the orchestrator; how
/// many it started.
pub async fn hand_over(pool: &PgPool, chains: &dyn Chains) -> Result<u32, sqlx::Error> {
    let kinds: Vec<String> = ON_DEMAND.iter().map(|k| (*k).to_owned()).collect();
    let jobs: Vec<(Uuid, String, Option<String>, Value)> = sqlx::query_as(
        "SELECT j.id, j.kind, j.dataset, j.parameters FROM quality.jobs j
          WHERE j.kind = ANY($1) AND j.state = 'queued'
            AND j.created_at > now() - interval '1 hour'
            AND NOT EXISTS (SELECT 1 FROM quality.chain_starts c WHERE c.job = j.id)
          ORDER BY j.created_at LIMIT $2",
    )
    .bind(&kinds)
    .bind(ROUND)
    .fetch_all(pool)
    .await?;
    let mut started = 0;
    for (id, kind, dataset, parameters) in jobs {
        let steps = [step_of(&kind, dataset.as_deref(), &parameters)];
        match chains.start(id, &steps).await {
            Ok(chain) => {
                sqlx::query(
                    "INSERT INTO quality.chain_starts (job, chain_id) VALUES ($1, $2)
                     ON CONFLICT DO NOTHING",
                )
                .bind(id)
                .bind(&chain)
                .execute(pool)
                .await?;
                tracing::info!(job = %id, kind, chain, "chain started");
                started += 1;
            }
            Err(e) => tracing::warn!(job = %id, kind, "starting its chain: {e}"),
        }
    }
    Ok(started)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_step_carries_what_the_bundle_is_asked_with_only() {
        let s = step_of(
            "records",
            None,
            &json!({ "source": "core", "table": "public.loans", "by": "someone" }),
        );
        assert_eq!(s.parameters, json!({ "source": "core" }));
        let t = step_of(
            "test_rule",
            Some("loans"),
            &json!({ "cut": "2026-10-31", "yaml": "rules" }),
        );
        assert_eq!(
            t.parameters,
            json!({ "dataset": "loans", "cut": "2026-10-31" })
        );
    }
}
