//! Jobs' Trino tokens (decision 0090, point 7.1, ruling 60): the route of
//! `quality::run_token_api` asks the provider for a token whose subject is
//! the job's Trino user, signed ES256 as the installation's own machine
//! client [`OWN_CLIENT`], which the API registers the first time it needs
//! it (audience `trino`, no usable secret: the API signs its tokens
//! itself).

use auth_gateway::{AuthorizationServer, JobSubject, JobUserKind};
use infrastructure::PgPool;
use quality::run_token_api::{TrinoToken, TrinoTokens, TrinoUser};
use tokio::sync::OnceCell;
use uuid::Uuid;

/// The own machine client that signs jobs' Trino tokens.
pub(crate) const OWN_CLIENT: &str = "qd-api-jobs";

pub(crate) struct ProviderTrinoTokens {
    pub(crate) provider: AuthorizationServer,
    pub(crate) pool: PgPool,
    pub(crate) registered: OnceCell<()>,
}

impl ProviderTrinoTokens {
    pub(crate) fn new(provider: AuthorizationServer, pool: PgPool) -> Self {
        Self {
            provider,
            pool,
            registered: OnceCell::new(),
        }
    }
}

#[async_trait::async_trait]
impl TrinoTokens for ProviderTrinoTokens {
    async fn issue(
        &self,
        job: Uuid,
        user: TrinoUser,
        dataset: &str,
        actor: &str,
    ) -> Result<TrinoToken, String> {
        self.registered
            .get_or_try_init(|| async {
                let new = infrastructure::oidc::ensure_own_client(
                    &self.pool,
                    OWN_CLIENT,
                    "Quality Data jobs on Trino",
                    "trino",
                )
                .await?;
                if new {
                    tracing::info!(client = OWN_CLIENT, "registered the jobs' Trino client");
                }
                Ok::<(), String>(())
            })
            .await?;
        let kind = match user {
            TrinoUser::Rules => JobUserKind::Rules,
            TrinoUser::Report => JobUserKind::Report,
            TrinoUser::Maint => JobUserKind::Maint,
            TrinoUser::Extract => JobUserKind::Extract,
            TrinoUser::Sample => JobUserKind::Sample,
            TrinoUser::Source => JobUserKind::Source,
            TrinoUser::Console => JobUserKind::Console,
            TrinoUser::Lookup => JobUserKind::Lookup,
        };
        let subject = JobSubject::new(&job.to_string(), kind, dataset)
            .map_err(|_| "not a job's Trino user".to_string())?;
        let token = self
            .provider
            .trino_job_token(OWN_CLIENT, &subject, actor)
            .await
            .map_err(|e| format!("{e:?}"))?;
        Ok(TrinoToken {
            access_token: token.access_token,
            expires_in: token.expires_in,
        })
    }
}
