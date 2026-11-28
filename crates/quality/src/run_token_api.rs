//! A run's Trino token (decision 0090, point 7.1): the job runner that
//! holds a run's pinned bundle asks the core, with its machine token, for
//! a token of one of the run's Trino users (`job_<user>_<dataset>`). The
//! core gives only the users the run's kind needs, records the token in
//! the audit log first (decision 0030, point 3) and never keeps or prints
//! it. A run with no pinned bundle gets none.

use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use axum::extract::{Path, State};
use axum::http::{header::AUTHORIZATION, HeaderMap, StatusCode};
use axum::routing::post;
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sqlx::PgPool;
use uuid::Uuid;

use crate::job_sources::{self, IssueLog};
use crate::worker_api::MachineTokens;

/// A job's Trino user kind, as Trino's user mapping names it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TrinoUser {
    /// Reads its dataset's layers and writes gold and the scratch rows.
    Rules,
    /// Reads gold and its tables' `$refs`.
    Report,
    /// Prepares and maintains its dataset's bronze and silver.
    Maint,
    /// Reads its sources and loads its dataset's bronze.
    Extract,
    /// Reads one source's catalog: its listing (a `tables` job) or a page
    /// of a table's records (a `records` job).
    Source,
}

impl TrinoUser {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Rules => "rules",
            Self::Report => "report",
            Self::Maint => "maint",
            Self::Extract => "extract",
            Self::Source => "source",
        }
    }
}

/// The Trino users a run of `kind` needs: those of the kinds compiled for
/// Trino, each over its dataset.
fn users_of(kind: &str) -> &'static [TrinoUser] {
    match kind {
        "evaluate" | "backfill" | "rule_test" | "retract" | "slas" => {
            &[TrinoUser::Rules, TrinoUser::Report]
        }
        "prepare" => &[TrinoUser::Maint],
        "extract" => &[TrinoUser::Extract, TrinoUser::Report],
        "tables" | "records" => &[TrinoUser::Source],
        _ => &[],
    }
}

/// A token as given: kept in the job runner's memory only.
#[derive(Clone, PartialEq, Eq, Serialize)]
pub struct TrinoToken {
    pub access_token: String,
    pub expires_in: u64,
}

impl fmt::Debug for TrinoToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TrinoToken")
            .field("expires_in", &self.expires_in)
            .finish_non_exhaustive()
    }
}

/// The port to the provider: a token whose subject is
/// `job:<job>:<user>:<dataset>`, `act` the job runner's machine.
#[async_trait]
pub trait TrinoTokens: Send + Sync {
    async fn issue(
        &self,
        job: Uuid,
        user: TrinoUser,
        dataset: &str,
        actor: &str,
    ) -> Result<TrinoToken, String>;
}

/// What the route uses besides the database.
#[derive(Clone)]
pub struct RunTokenServices {
    /// Machine tokens for `quality-jobs`, as the bundle route takes them.
    pub tokens: Arc<dyn MachineTokens>,
    pub issuer: Arc<dyn TrinoTokens>,
    pub log: Arc<dyn IssueLog>,
}

impl fmt::Debug for RunTokenServices {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RunTokenServices").finish_non_exhaustive()
    }
}

/// Which user the job runner asks a token for.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TokenAsk {
    pub user: TrinoUser,
}

#[derive(Clone)]
struct RunTokenApi {
    pool: PgPool,
    services: RunTokenServices,
}

/// `POST /internal/quality/v1/runs/{run}/trino-token`.
pub fn routes(pool: PgPool, services: RunTokenServices) -> Router {
    Router::new()
        .route(
            "/internal/quality/v1/runs/{run}/trino-token",
            post(token_of),
        )
        .with_state(RunTokenApi { pool, services })
}

type Refusal = (StatusCode, Json<Value>);

fn refused(status: StatusCode, error: &str) -> Refusal {
    (status, Json(json!({ "error": error })))
}

async fn token_of(
    State(api): State<RunTokenApi>,
    Path(run): Path<String>,
    headers: HeaderMap,
    Json(ask): Json<TokenAsk>,
) -> Result<Json<TrinoToken>, Refusal> {
    let caller = headers
        .get(AUTHORIZATION)
        .and_then(|h| h.to_str().ok())
        .and_then(|h| h.strip_prefix("Bearer "))
        .and_then(|t| api.services.tokens.client(t))
        .ok_or_else(|| refused(StatusCode::UNAUTHORIZED, "unauthorized"))?;
    let job = Uuid::parse_str(&run).map_err(|_| refused(StatusCode::BAD_REQUEST, "invalid_run"))?;
    let pinned: Option<(String, Option<String>, Option<String>)> =
        sqlx::query_as("SELECT kind, dataset, source FROM quality.job_bundles WHERE run = $1")
            .bind(&run)
            .fetch_optional(&api.pool)
            .await
            .map_err(|e| {
                tracing::error!("reading the run's bundle: {e}");
                refused(StatusCode::INTERNAL_SERVER_ERROR, "internal")
            })?;
    let (kind, dataset, source) =
        pinned.ok_or_else(|| refused(StatusCode::NOT_FOUND, "no_bundle"))?;
    // A source's user is over its source; every other over the dataset.
    let target = if ask.user == TrinoUser::Source {
        source
    } else {
        dataset
    };
    let dataset = target
        .filter(|_| users_of(&kind).contains(&ask.user))
        .ok_or_else(|| refused(StatusCode::FORBIDDEN, "user_not_allowed"))?;
    let actor = caller.worker_prefix();
    let resource = format!("trino/job_{}_{dataset}", ask.user.as_str());
    job_sources::record(
        Some(api.services.log.as_ref()),
        job,
        &kind,
        &actor,
        &headers,
        &resource,
    )
    .await
    .map_err(|status| refused(status, "not_recorded"))?;
    let token = api
        .services
        .issuer
        .issue(job, ask.user, &dataset, &actor)
        .await
        .map_err(|e| {
            tracing::error!(run, "issuing a Trino token: {e}");
            refused(StatusCode::SERVICE_UNAVAILABLE, "not_issued")
        })?;
    tracing::info!(run, kind, user = ask.user.as_str(), "trino token given");
    Ok(Json(token))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_the_kinds_compiled_for_trino_get_users_and_a_token_never_prints() {
        assert_eq!(users_of("evaluate"), [TrinoUser::Rules, TrinoUser::Report]);
        assert!(users_of("derive").is_empty());
        assert_eq!(users_of("extract"), [TrinoUser::Extract, TrinoUser::Report]);
        assert_eq!(users_of("prepare"), [TrinoUser::Maint]);
        let t = TrinoToken {
            access_token: "secret-token".into(),
            expires_in: 900,
        };
        assert!(!format!("{t:?}").contains("secret"));
        let ask: TokenAsk = serde_json::from_str(r#"{"user":"report"}"#).unwrap();
        assert_eq!(ask.user, TrinoUser::Report);
        assert!(serde_json::from_str::<TokenAsk>(r#"{"user":"job_rules_x"}"#).is_err());
    }
}
