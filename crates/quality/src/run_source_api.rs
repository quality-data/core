//! A run's source secrets (decision 0090, point 7.5): the job runner of a
//! pinned extract asks, with its machine token, for its source's user and
//! password, current and previous, which it sends as Trino extra
//! credentials with each statement that reads the source (retrying with
//! the previous on a login error). Recorded in the audit log first
//! (decision 0030, points 2 and 3); never kept, logged or printed.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::{header::AUTHORIZATION, HeaderMap, StatusCode};
use axum::routing::post;
use axum::{Json, Router};
use serde::Serialize;
use serde_json::{json, Value};
use sqlx::PgPool;
use uuid::Uuid;

use crate::catalog::Catalog;
use crate::job_sources::{self, IssueLog};
use crate::mapping::Mapping;
use crate::source_store::{self, Sealer, StoreError};
use crate::worker_api::MachineTokens;

/// The extra credentials' names, as the source catalogs declare them.
const NAMES: [&str; 2] = ["user", "password"];

/// The kinds whose bundles read a source.
fn reads_source(kind: &str) -> bool {
    matches!(kind, "extract" | "tables" | "records" | "map")
}

/// A source's extra credentials: kept in the job runner's memory only.
#[derive(Clone, PartialEq, Eq, Serialize)]
pub struct SourceSecrets {
    pub current: BTreeMap<String, String>,
    /// Tried only when the current ones are refused.
    pub previous: Option<BTreeMap<String, String>>,
}

impl fmt::Debug for SourceSecrets {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SourceSecrets").finish_non_exhaustive()
    }
}

/// What the route uses besides the database.
#[derive(Clone)]
pub struct RunSourceServices {
    pub tokens: Arc<dyn MachineTokens>,
    pub sealer: Arc<dyn Sealer>,
    pub catalog: Arc<dyn Catalog>,
    pub log: Arc<dyn IssueLog>,
}

impl fmt::Debug for RunSourceServices {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RunSourceServices").finish_non_exhaustive()
    }
}

#[derive(Clone)]
struct Api {
    pool: PgPool,
    services: RunSourceServices,
}

/// `POST /internal/quality/v1/runs/{run}/source-secrets`.
pub fn routes(pool: PgPool, services: RunSourceServices) -> Router {
    Router::new()
        .route(
            "/internal/quality/v1/runs/{run}/source-secrets",
            post(secrets_of),
        )
        .with_state(Api { pool, services })
}

type Refusal = (StatusCode, Json<Value>);

fn refused(status: StatusCode, error: &str) -> Refusal {
    (status, Json(json!({ "error": error })))
}

fn only_credentials(all: &BTreeMap<String, String>) -> BTreeMap<String, String> {
    all.iter()
        .filter(|(k, _)| NAMES.contains(&k.as_str()))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}

async fn secrets_of(
    State(api): State<Api>,
    Path(run): Path<String>,
    headers: HeaderMap,
) -> Result<Json<SourceSecrets>, Refusal> {
    let caller = headers
        .get(AUTHORIZATION)
        .and_then(|h| h.to_str().ok())
        .and_then(|h| h.strip_prefix("Bearer "))
        .and_then(|t| api.services.tokens.client(t))
        .ok_or_else(|| refused(StatusCode::UNAUTHORIZED, "unauthorized"))?;
    let job = Uuid::parse_str(&run).map_err(|_| refused(StatusCode::BAD_REQUEST, "invalid_run"))?;
    let internal = |e: String| {
        tracing::error!(run, "source secrets: {e}");
        refused(StatusCode::INTERNAL_SERVER_ERROR, "internal")
    };
    let pinned: Option<(String, Option<String>, Option<String>)> =
        sqlx::query_as("SELECT kind, dataset, source FROM quality.job_bundles WHERE run = $1")
            .bind(&run)
            .fetch_optional(&api.pool)
            .await
            .map_err(|e| internal(e.to_string()))?;
    let (kind, dataset, named) =
        pinned.ok_or_else(|| refused(StatusCode::NOT_FOUND, "no_bundle"))?;
    if !reads_source(&kind) {
        return Err(refused(StatusCode::FORBIDDEN, "reads_no_source"));
    }
    // A source's job names its source; a dataset's reads its mapping's.
    let source = match (named, dataset) {
        (Some(source), _) => source,
        (None, Some(dataset)) => {
            let snapshot = api
                .services
                .catalog
                .snapshot(&dataset)
                .await
                .map_err(internal)?
                .ok_or_else(|| refused(StatusCode::CONFLICT, "no_mapping"))?;
            Mapping::parse(&snapshot.mapping)
                .ok()
                .and_then(|m| m.from)
                .map(|f| f.value.source)
                .ok_or_else(|| refused(StatusCode::CONFLICT, "no_source"))?
        }
        (None, None) => return Err(refused(StatusCode::CONFLICT, "no_source")),
    };
    let credentials = source_store::credentials(&api.pool, api.services.sealer.as_ref(), &source)
        .await
        .map_err(|e| match e {
            StoreError::NotFound => refused(StatusCode::CONFLICT, "no_source"),
            other => internal(format!("{other:?}")),
        })?;
    let actor = caller.worker_prefix();
    job_sources::record(
        Some(api.services.log.as_ref()),
        job,
        &kind,
        &actor,
        &headers,
        &format!("source/{source}"),
    )
    .await
    .map_err(|status| refused(status, "not_recorded"))?;
    tracing::info!(run, kind, source, "source secrets given");
    Ok(Json(SourceSecrets {
        current: only_credentials(&credentials.source.secrets),
        previous: credentials.previous.as_ref().map(only_credentials),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_a_sources_user_and_password_go_and_they_never_print() {
        let all = BTreeMap::from([
            ("user".to_string(), "reader".to_string()),
            ("password".to_string(), "synthetic".to_string()),
            ("wallet".to_string(), "other".to_string()),
        ]);
        let s = SourceSecrets {
            current: only_credentials(&all),
            previous: None,
        };
        assert_eq!(s.current.len(), 2);
        assert!(!format!("{s:?}").contains("synthetic"));
        assert!(reads_source("extract") && reads_source("tables") && reads_source("records"));
        assert!(reads_source("map"));
        assert!(!reads_source("evaluate"));
    }
}
