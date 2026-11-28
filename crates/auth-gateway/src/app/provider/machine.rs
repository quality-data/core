//! The `client_credentials` grant (RFC 6749 §4.4): a machine (a worker, the
//! API) gets a short access token for one audience, with no person,
//! no id_token and no refresh token (decision 0030: dynamic credentials).
//! A client with a secret gets a day (0046), a module's 15 minutes
//! (quality-sdk decision 0009); a machine role's
//! client, authenticated by one machine's certificate, 15 minutes that name
//! the machine (0066, point 6).

use std::time::Duration;

use serde_json::{json, Map, Value};

use super::{
    epoch, AuthorizationServer, TokenError, TokenRequest, TokenResponse, MACHINE_CERT_TOKEN_TTL,
};
use crate::domain::{ClientAuth, ClientId, JobSubject, Machine, OidcClient};

/// How long a job's token lasts (decision 0062): as a job's lake
/// credentials (0030), whatever a client's lifetime is.
/// The audience of jobs' Trino tokens (decision 0090, point 7.1).
pub const TRINO_AUDIENCE: &str = "trino";

const JOB_TOKEN_TTL: Duration = Duration::from_secs(15 * 60);

/// The core's own internal client, whose token goes with its calls to
/// modules (decision 0090, ruling 103).
pub const CORE_CLIENT: &str = "qd-core";

impl AuthorizationServer {
    pub(super) async fn client_credentials(
        &self,
        req: &TokenRequest,
    ) -> Result<TokenResponse, TokenError> {
        let client = self.requested_client(req).await?;
        let machine = match client.auth {
            ClientAuth::Certificate { role } => Some(self.certified_machine(role, req).await?),
            _ => {
                self.check_secret(&client, req)?;
                None
            }
        };
        if !client.machine() {
            return Err(TokenError::UnauthorizedClient);
        }
        let audience = requested_audience(req, &client)?;
        self.machine_token(&client, &audience, machine.as_ref())
            .await
    }

    /// A token of this installation's own machine client `client_id` for
    /// `audience`, signed here with no secret: the API is the issuer, so
    /// it does not ask itself over HTTP for its own token. The client must be registered for `client_credentials` and
    /// that audience, so deleting or changing it stops this too.
    pub async fn own_machine_token(
        &self,
        client_id: &str,
        audience: &str,
    ) -> Result<TokenResponse, TokenError> {
        let client = self.own_client(client_id, audience).await?;
        self.machine_token(&client, audience, None).await
    }

    /// A token for one job (decision 0062), signed with the key of the
    /// installation's own machine client `client_id`, which must be allowed
    /// `audience` as for [`Self::own_machine_token`]: `subject` names the
    /// job, `actor` the machine it is given to (RFC 8693 `act`). It lasts
    /// 15 minutes and carries no policy.
    pub async fn job_token(
        &self,
        client_id: &str,
        audience: &str,
        subject: &str,
        actor: &str,
    ) -> Result<TokenResponse, TokenError> {
        let client = self.own_client(client_id, audience).await?;
        let signer = self.signers.for_client(&client)?;
        let now = self.clock.now();
        let claims = json!({
            "iss": self.issuer,
            "sub": subject,
            "aud": audience,
            "act": { "sub": actor },
            "iat": epoch(now),
            "exp": epoch(now + JOB_TOKEN_TTL),
            "jti": self.code_tokens.generate(),
        });
        Ok(TokenResponse {
            access_token: signer.sign("at+jwt", &claims.to_string()),
            id_token: None,
            expires_in: JOB_TOKEN_TTL.as_secs(),
            scope: String::new(),
        })
    }

    /// A job's Trino token (decision 0090, point 7.1): `sub` its
    /// [`JobSubject`], one Trino user per kind and dataset, `aud` `trino`,
    /// `act` the job runner's machine client, 15 minutes, always ES256.
    /// `client_id` is the installation's own machine client allowed
    /// `trino`. The caller records it before giving it (decision 0030,
    /// point 3); `None` without an ES256 key.
    pub async fn trino_job_token(
        &self,
        client_id: &str,
        subject: &JobSubject,
        actor: &str,
    ) -> Result<TokenResponse, TokenError> {
        self.own_client(client_id, TRINO_AUDIENCE).await?;
        let now = self.clock.now();
        let claims = json!({
            "iss": self.issuer,
            "sub": subject.to_string(),
            "aud": TRINO_AUDIENCE,
            "act": { "sub": actor },
            "iat": epoch(now),
            "exp": epoch(now + JOB_TOKEN_TTL),
            "jti": self.code_tokens.generate(),
        });
        let access_token = self
            .sign_es256("at+jwt", &claims.to_string())
            .ok_or_else(|| TokenError::Internal("no ES256 key".into()))?;
        Ok(TokenResponse {
            access_token,
            id_token: None,
            expires_in: JOB_TOKEN_TTL.as_secs(),
            scope: String::new(),
        })
    }

    /// The installation's own machine client `client_id`, if it is one and
    /// is allowed `audience`.
    async fn own_client(&self, client_id: &str, audience: &str) -> Result<OidcClient, TokenError> {
        let id = ClientId::parse(client_id).map_err(|_| TokenError::InvalidClient)?;
        let client = self
            .clients
            .find(&id)
            .await
            .map_err(|e| TokenError::Internal(e.to_string()))?
            .ok_or(TokenError::InvalidClient)?;
        if !client.machine() {
            return Err(TokenError::UnauthorizedClient);
        }
        if !client.settings.audiences.iter().any(|a| a == audience) {
            return Err(TokenError::InvalidTarget);
        }
        Ok(client)
    }

    /// The client's machine token for `audience`, already allowed; with the
    /// `machine` whose certificate asked for it, named in the token.
    async fn machine_token(
        &self,
        client: &OidcClient,
        audience: &str,
        machine: Option<&Machine>,
    ) -> Result<TokenResponse, TokenError> {
        let signer = self.signers.for_client(client)?;
        let subject = client
            .settings
            .service_account
            .clone()
            .unwrap_or_else(|| client.id.to_string());
        let policies = self.machine_policies(client).await;

        let ttl = match client.auth {
            ClientAuth::Certificate { .. } => MACHINE_CERT_TOKEN_TTL,
            _ => client.token_ttl(),
        };

        let now = self.clock.now();
        let mut claims = Map::new();
        claims.insert("iss".into(), json!(self.issuer));
        claims.insert("sub".into(), json!(subject));
        claims.insert("aud".into(), json!(audience));
        claims.insert("client_id".into(), json!(client.id.as_str()));
        if let Some(machine) = machine {
            claims.insert("machine".into(), json!(machine.name));
        }
        claims.insert("iat".into(), json!(epoch(now)));
        claims.insert("exp".into(), json!(epoch(now + ttl)));
        claims.insert("jti".into(), json!(self.code_tokens.generate()));
        claims.insert("policy".into(), json!(policies));
        Ok(TokenResponse {
            access_token: signer.sign("at+jwt", &Value::Object(claims).to_string()),
            id_token: None,
            expires_in: ttl.as_secs(),
            scope: String::new(),
        })
    }

    /// The client a live machine token of ours was issued to, if it was
    /// issued for `audience`: how a service of this installation (the API's
    /// worker routes) knows which machine calls it. A person's token has no
    /// `client_id` claim and is refused.
    pub fn machine_client(&self, access_token: &str, audience: &str) -> Option<String> {
        self.machine_caller(access_token, audience)
            .map(|(client, _)| client)
    }

    /// As [`Self::machine_client`], with the machine the token names (its
    /// `machine` claim) when a role's client got it with a machine's
    /// certificate (decision 0066, point 6); `None` for a client with a
    /// secret.
    pub fn machine_caller(
        &self,
        access_token: &str,
        audience: &str,
    ) -> Option<(String, Option<String>)> {
        let claims: Value =
            serde_json::from_str(&self.signers.verify("at+jwt", access_token)?).ok()?;
        let live = claims.get("iss")?.as_str()? == self.issuer
            && claims.get("exp")?.as_u64()? > epoch(self.clock.now())
            && claims.get("aud")?.as_str()? == audience;
        let client = live.then(|| claims.get("client_id")?.as_str().map(str::to_string))??;
        let machine = match claims.get("machine") {
            None => None,
            Some(m) => Some(m.as_str()?.to_string()),
        };
        Some((client, machine))
    }

    /// The policies of the service account the client acts as; none without
    /// one, or if they cannot be read (nothing is granted then).
    async fn machine_policies(&self, client: &OidcClient) -> Vec<String> {
        let (Some(source), Some(account)) = (&self.policies, &client.settings.service_account)
        else {
            return Vec::new();
        };
        source
            .service_account_policies(account)
            .await
            .unwrap_or_else(|e| {
                tracing::warn!("policy claim of {}: {e}", client.id);
                Vec::new()
            })
    }
}

/// The audience of a machine token: the one asked for (`resource` or
/// `audience`, the same if both come), which must be one of the client's, or
/// else its first.
pub(super) fn requested_audience(
    req: &TokenRequest,
    client: &OidcClient,
) -> Result<String, TokenError> {
    let resource = req.resource.as_deref().filter(|r| !r.is_empty());
    let audience = req.audience.as_deref().filter(|a| !a.is_empty());
    let asked = match (resource, audience) {
        (Some(r), Some(a)) if r != a => return Err(TokenError::InvalidRequest),
        (r, a) => r.or(a),
    };
    let allowed = &client.settings.audiences;
    match asked {
        Some(a) if allowed.iter().any(|x| x == a) => Ok(a.to_string()),
        Some(_) => Err(TokenError::InvalidTarget),
        None => allowed.first().cloned().ok_or(TokenError::InvalidTarget),
    }
}
