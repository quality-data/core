//! The core's calls to the orchestrator (decision 0090, rulings 92(a), 97,
//! 100 and 103), under `ORCHESTRATOR_INTERNAL_URL`
//! (`https://<master>/modules/orchestrator/internal`): at start, the core's
//! own flow (`POST …/core-flow {engine: trino, line: -, timeout_secs}`,
//! idempotent, tried until made); then every two seconds the jobs a person
//! asked for and not handed over yet start their chains with
//! `POST …/chains {job_id, steps}`, presenting the core's
//! machine certificate (`LAKE_CLIENT_CERT_FILE`, with `LAKE_CA_FILE`) and
//! the core's own machine token (client `qd-core`, audience
//! `ORCHESTRATOR_AUDIENCE`, `orchestrator` by default, signed as a
//! module's machine token). Not started without `ORCHESTRATOR_INTERNAL_URL`
//! or the OIDC provider.

use std::time::Duration;

use auth_gateway::AuthorizationServer;
use infrastructure::lake_http::LakeClient;
use infrastructure::PgPool;
use quality::chain_starts::{hand_over, Chains};
use quality::schedules_api::Step;
use uuid::Uuid;

/// Each run of the core's flow times out after this, unless
/// `ORCHESTRATOR_CORE_FLOW_TIMEOUT` (seconds) says otherwise.
const FLOW_TIMEOUT_SECS: u64 = 3600;

struct OrchestratorChains {
    http: LakeClient,
    /// `…/modules/orchestrator/internal`, no trailing `/`.
    base: String,
    flow_timeout: u64,
    audience: String,
    provider: AuthorizationServer,
    pool: PgPool,
    registered: tokio::sync::OnceCell<()>,
}

impl OrchestratorChains {
    /// The core's own token (ruling 103): its client `qd-core` registered
    /// the first time, allowed the orchestrator's audience.
    async fn token(&self) -> Result<String, String> {
        self.registered
            .get_or_try_init(|| async {
                infrastructure::oidc::ensure_own_client(
                    &self.pool,
                    auth_gateway::CORE_CLIENT,
                    "Quality Data core",
                    &self.audience,
                )
                .await
                .map(|_| ())
            })
            .await?;
        self.provider
            .own_machine_token(auth_gateway::CORE_CLIENT, &self.audience)
            .await
            .map(|t| t.access_token)
            .map_err(|e| format!("the core's token: {e:?}"))
    }
}

#[async_trait::async_trait]
impl Chains for OrchestratorChains {
    async fn start(&self, job: Uuid, steps: &[Step]) -> Result<String, String> {
        let token = self.token().await?;
        let res = self
            .http
            .get()
            .post(format!("{}/chains", self.base))
            .bearer_auth(token)
            .json(&serde_json::json!({ "job_id": job, "steps": steps }))
            .send()
            .await
            .map_err(|e| format!("the orchestrator: {}", e.without_url()))?;
        let status = res.status();
        if !status.is_success() {
            return Err(format!("the orchestrator answered {status}"));
        }
        let body: serde_json::Value = res.json().await.map_err(|e| e.without_url().to_string())?;
        body.get("chain_id")
            .and_then(|c| {
                c.as_str()
                    .map(str::to_owned)
                    .or_else(|| Some(c.to_string()))
            })
            .filter(|c| !c.is_empty() && c != "null")
            .ok_or_else(|| "the orchestrator answered no chain_id".to_string())
    }
}

impl OrchestratorChains {
    /// Makes the core's flow, or finds it made; `false` to try again.
    async fn core_flow(&self) -> bool {
        let token = match self.token().await {
            Ok(t) => t,
            Err(e) => {
                tracing::warn!("the core's flow: {e}");
                return false;
            }
        };
        let sent = self
            .http
            .get()
            .post(format!("{}/core-flow", self.base))
            .bearer_auth(token)
            .json(&serde_json::json!({
                "engine": "trino",
                "line": "-",
                "timeout_secs": self.flow_timeout,
            }))
            .send()
            .await;
        match sent.map(|r| r.status()) {
            Ok(s) if s.is_success() => {
                tracing::info!(
                    status = s.as_u16(),
                    "the core's flow is in the orchestrator"
                );
                true
            }
            Ok(s) if s.as_u16() == 409 => {
                tracing::error!("the core's flow: the orchestrator has another flow core/chains");
                true
            }
            Ok(s) => {
                tracing::warn!(status = s.as_u16(), "the core's flow: not made yet");
                false
            }
            Err(e) => {
                tracing::warn!("the core's flow: {}", e.without_url());
                false
            }
        }
    }
}

/// Starts the hand-over loop, when configured.
pub(crate) fn spawn(pool: &PgPool, provider: Option<AuthorizationServer>) -> Result<(), String> {
    let var = |name: &str| std::env::var(name).ok().filter(|v| !v.trim().is_empty());
    let (Some(base), Some(provider)) = (var("ORCHESTRATOR_INTERNAL_URL"), provider) else {
        return Ok(());
    };
    let chains = OrchestratorChains {
        http: LakeClient::from_env(Duration::from_secs(10))?,
        base: base.trim_end_matches('/').to_owned(),
        flow_timeout: var("ORCHESTRATOR_CORE_FLOW_TIMEOUT")
            .and_then(|v| v.parse().ok())
            .filter(|t| (1..=86_400).contains(t))
            .unwrap_or(FLOW_TIMEOUT_SECS),
        audience: var("ORCHESTRATOR_AUDIENCE").unwrap_or_else(|| "orchestrator".into()),
        provider,
        pool: pool.clone(),
        registered: tokio::sync::OnceCell::new(),
    };
    let pool = pool.clone();
    let Ok(rt) = tokio::runtime::Handle::try_current() else {
        return Ok(());
    };
    rt.spawn(async move {
        while !chains.core_flow().await {
            tokio::time::sleep(Duration::from_secs(30)).await;
        }
        loop {
            if let Err(e) = hand_over(&pool, &chains).await {
                tracing::warn!("handing jobs to the orchestrator: {e}");
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    });
    Ok(())
}
