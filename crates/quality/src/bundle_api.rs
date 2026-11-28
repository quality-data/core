//! The bundle route (decision 0090, point 2; ruled by the user
//! 2026-11-25): when `engine-trino`'s job runner claims a run from the
//! operator, it asks the core for the run's bundle with its machine token.
//! The core compiles the job from what is approved at that moment, signs it
//! with the provider's ES256 key and pins it to the run: every later ask
//! for the run, a retry included, gets the same signed bundle.

use std::sync::Arc;

use axum::{
    extract::{Path, State},
    http::{header::AUTHORIZATION, HeaderMap, StatusCode},
    routing::post,
    Json, Router,
};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;

use crate::bundle::{self, Bundle, BundleSigner};
use crate::worker_api::MachineTokens;

/// The audience a job runner asks its machine token for: only the clients
/// registered with it (the module that runs the job runners) get one.
pub const AUDIENCE: &str = "quality-jobs";

/// What the job runner asks for: the run's job as the operator handed it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JobRequest {
    pub kind: String,
    #[serde(default)]
    pub dataset: Option<String>,
    #[serde(default)]
    pub cut: Option<chrono::NaiveDate>,
    pub attempt: u32,
}

/// Why a job cannot be compiled: a class the job runner reports, never the
/// rules' content.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompileRefused(pub &'static str);

/// The port that compiles a run's job from what is approved now: the
/// statements of its kind, its catalog commit and, for gold's writers, a
/// new `run_seq`.
#[async_trait::async_trait]
pub trait JobCompiler: Send + Sync {
    async fn compile(&self, run: &str, job: &JobRequest) -> Result<Bundle, CompileRefused>;
}

/// What the bundle route uses besides the database.
#[derive(Clone)]
pub struct BundleServices {
    /// Machine tokens for [`AUDIENCE`].
    pub tokens: Arc<dyn MachineTokens>,
    pub signer: Arc<dyn BundleSigner>,
    pub compiler: Arc<dyn JobCompiler>,
}

impl std::fmt::Debug for BundleServices {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BundleServices").finish_non_exhaustive()
    }
}

/// The signed bundle as the job runner takes it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignedBundle {
    /// The run, as the bundle names it.
    pub job_id: String,
    /// The lower-case hex SHA-256 of the bundle's JSON.
    pub digest: String,
    /// The compact JWS (ES256, `typ` [`bundle::BUNDLE_TYP`]); its payload
    /// is the bundle's JSON.
    pub bundle: String,
}

#[derive(Clone)]
struct BundleApi {
    pool: PgPool,
    services: BundleServices,
}

/// `POST /internal/quality/v1/runs/{run}/bundle`.
pub fn routes(pool: PgPool, services: BundleServices) -> Router {
    Router::new()
        .route("/internal/quality/v1/runs/{run}/bundle", post(bundle_of))
        .with_state(BundleApi { pool, services })
}

#[derive(sqlx::FromRow)]
struct Pinned {
    kind: String,
    dataset: Option<String>,
    cut: Option<chrono::NaiveDate>,
    digest: String,
    bundle: String,
}

/// An operator's run id: a UUID or a short name of `[A-Za-z0-9_-]`.
fn is_run(run: &str) -> bool {
    !run.is_empty()
        && run.len() <= 64
        && run
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}

async fn bundle_of(
    State(api): State<BundleApi>,
    Path(run): Path<String>,
    headers: HeaderMap,
    Json(job): Json<JobRequest>,
) -> Result<Json<SignedBundle>, (StatusCode, Json<serde_json::Value>)> {
    let caller = headers
        .get(AUTHORIZATION)
        .and_then(|h| h.to_str().ok())
        .and_then(|h| h.strip_prefix("Bearer "))
        .and_then(|t| api.services.tokens.client(t))
        .ok_or_else(|| refused(StatusCode::UNAUTHORIZED, "unauthorized"))?;
    if !is_run(&run) {
        return Err(refused(StatusCode::BAD_REQUEST, "invalid_run"));
    }
    if let Some(pinned) = api.pinned(&run).await? {
        return pinned_for(&run, &job, pinned);
    }
    let compiled = api
        .services
        .compiler
        .compile(&run, &job)
        .await
        .map_err(|CompileRefused(class)| refused(StatusCode::UNPROCESSABLE_ENTITY, class))?;
    if compiled.job_id != run || compiled.kind != job.kind || !compiled.well_formed() {
        tracing::error!(
            run,
            kind = job.kind,
            "the compiler gave another job's bundle"
        );
        return Err(refused(StatusCode::INTERNAL_SERVER_ERROR, "compile_failed"));
    }
    let digest = compiled.digest();
    let token = bundle::sign(api.services.signer.as_ref(), &compiled);
    sqlx::query(
        "INSERT INTO quality.job_bundles (run, kind, dataset, cut, digest, bundle, asked_by)
         VALUES ($1, $2, $3, $4, $5, $6, $7) ON CONFLICT DO NOTHING",
    )
    .bind(&run)
    .bind(&job.kind)
    .bind(&job.dataset)
    .bind(job.cut)
    .bind(&digest)
    .bind(&token)
    .bind(caller.worker_prefix())
    .execute(&api.pool)
    .await
    .map_err(internal)?;
    // Two job runners may compile at once: the first row wins for both.
    let pinned = api
        .pinned(&run)
        .await?
        .ok_or_else(|| refused(StatusCode::INTERNAL_SERVER_ERROR, "internal"))?;
    tracing::info!(
        run,
        kind = job.kind,
        digest = pinned.digest,
        "bundle pinned"
    );
    pinned_for(&run, &job, pinned)
}

impl BundleApi {
    async fn pinned(
        &self,
        run: &str,
    ) -> Result<Option<Pinned>, (StatusCode, Json<serde_json::Value>)> {
        sqlx::query_as(
            "SELECT kind, dataset, cut, digest, bundle FROM quality.job_bundles WHERE run = $1",
        )
        .bind(run)
        .fetch_optional(&self.pool)
        .await
        .map_err(internal)
    }
}

/// The pinned bundle, if it was compiled for this same job.
fn pinned_for(
    run: &str,
    job: &JobRequest,
    pinned: Pinned,
) -> Result<Json<SignedBundle>, (StatusCode, Json<serde_json::Value>)> {
    if (pinned.kind.as_str(), &pinned.dataset, pinned.cut)
        != (job.kind.as_str(), &job.dataset, job.cut)
    {
        return Err(refused(StatusCode::CONFLICT, "run_has_another_job"));
    }
    Ok(Json(SignedBundle {
        job_id: run.to_string(),
        digest: pinned.digest,
        bundle: pinned.bundle,
    }))
}

fn refused(status: StatusCode, class: &str) -> (StatusCode, Json<serde_json::Value>) {
    (status, Json(serde_json::json!({ "error": class })))
}

fn internal(e: sqlx::Error) -> (StatusCode, Json<serde_json::Value>) {
    tracing::error!("job bundles: {e}");
    refused(StatusCode::INTERNAL_SERVER_ERROR, "internal")
}

/// A compiler of no kind yet: every job is refused as `kind_not_compiled`
/// until its kind's compile lands (decision 0090, step 18).
#[derive(Debug, Default)]
pub struct NoKindsYet;

#[async_trait::async_trait]
impl JobCompiler for NoKindsYet {
    async fn compile(&self, _: &str, _: &JobRequest) -> Result<Bundle, CompileRefused> {
        Err(CompileRefused("kind_not_compiled"))
    }
}
