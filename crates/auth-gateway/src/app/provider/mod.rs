//! The gateway as an OAuth2 / OIDC provider: the authorization-code flow with
//! PKCE (RFC 6749, RFC 7636) for the applications registered in
//! [`OidcClientRegistry`], signing an id_token (OIDC Core) and a JWT access
//! token (RFC 9068) with the [`TokenSigner`] of each application's algorithm;
//! and the `client_credentials` grant (RFC 6749 §4.4) for machines.
//!
//! The person signs in with whatever the gateway already offers (password,
//! directory, OAuth); `/authorize` only needs their session. Deliberately
//! narrow: `response_type=code` only, PKCE `S256` required unless a
//! confidential client is flagged otherwise, exact redirect URIs, and no
//! refresh tokens yet — an application asks again (`prompt=none` comes back
//! without a screen while the session lives).

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde_json::{json, Map, Value};

use crate::app::session::AuthenticatedSession;
use crate::domain::{
    AccountPolicies, ActionHolder, AdminAccount, AdminRepository, AuthorizationCode,
    AuthorizationCodeStore, ClientId, Clock, MachineCertificate, MachineRegistry,
    OAuthSecretGenerator, OidcClient, OidcClientRegistry, ResetTokenGenerator, SigningAlg,
    TokenSigner,
};

mod client_auth;
mod code;
mod machine;
mod module_client_requests;
mod module_clients;
mod signers;
mod userinfo;

pub use module_clients::{
    ClientChange, MachineClient, ModuleClients, ModuleClientsError, ModuleClientsRequest,
    PeopleClient, RoleClaimRequest, PREVIOUS_SECRET_TTL,
};
use signers::Signers;

/// How long a code waits to be redeemed: the application does it right away.
pub const CODE_TTL: Duration = Duration::from_secs(60);

/// How long the tokens last: a day (0046). There is no revocation list, so a
/// token works until it expires.
pub const TOKEN_TTL: Duration = Duration::from_secs(24 * 3600);

/// How long the tokens of a module's client last (quality-sdk decision
/// 0009, as 0089): 15 minutes, so a client disabled at the module's removal
/// is cut off everywhere within them, Trino included.
pub const MODULE_TOKEN_TTL: Duration = Duration::from_secs(15 * 60);

impl OidcClient {
    /// How long a token of this client lasts, but a machine certificate's.
    pub(crate) fn token_ttl(&self) -> Duration {
        if self.settings.module.is_some() {
            MODULE_TOKEN_TTL
        } else {
            TOKEN_TTL
        }
    }
}

/// How long a token asked with a machine's certificate lasts (0066, point
/// 6; 0030): 15 minutes, whatever [`TOKEN_TTL`] is. The machine asks again
/// with its certificate, so a retired one is out within them.
pub const MACHINE_CERT_TOKEN_TTL: Duration = Duration::from_secs(15 * 60);

/// The scopes the gateway knows; others are ignored, as OIDC allows.
pub const SCOPES_SUPPORTED: &[&str] = &["openid", "email", "profile"];

/// The query of `GET /authorize`, as it came.
#[derive(Debug, Clone, Default)]
pub struct AuthorizeRequest {
    pub response_type: Option<String>,
    pub client_id: Option<String>,
    pub redirect_uri: Option<String>,
    pub scope: Option<String>,
    pub state: Option<String>,
    pub code_challenge: Option<String>,
    pub code_challenge_method: Option<String>,
    pub nonce: Option<String>,
    pub prompt: Option<String>,
}

/// What `/authorize` does with the browser.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthorizeOutcome {
    /// Back to the application, with a code or an OAuth error.
    Redirect(String),
    /// No session: sign in, then repeat the same request.
    SignIn,
    /// The client or its redirect URI is unknown, so there is nowhere safe to
    /// send an error: show it instead (RFC 6749 §4.1.2.1).
    Invalid(&'static str),
}

/// The form of `POST /token`. The client's credentials come from the form or
/// from HTTP Basic; the HTTP layer merges them here.
#[derive(Debug, Clone, Default)]
pub struct TokenRequest {
    pub grant_type: Option<String>,
    pub code: Option<String>,
    pub redirect_uri: Option<String>,
    pub client_id: Option<String>,
    pub client_secret: Option<String>,
    pub code_verifier: Option<String>,
    /// `client_credentials`: the audience asked for, as RFC 8707's `resource`
    /// or the common `audience`.
    pub resource: Option<String>,
    pub audience: Option<String>,
    /// The certificate Traefik's internal entry point verified, when the
    /// deployment trusts its header: how a machine role's client
    /// authenticates (RFC 8705 `tls_client_auth`). Ignored by other clients.
    pub client_certificate: Option<MachineCertificate>,
}

/// A successful `/token`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TokenResponse {
    pub access_token: String,
    /// Only for a person (`authorization_code`), not for a machine.
    pub id_token: Option<String>,
    pub expires_in: u64,
    /// The scopes granted; empty for a machine.
    pub scope: String,
}

/// Why `/token` refused, as the OAuth error codes (RFC 6749 §5.2, RFC 8707).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TokenError {
    InvalidRequest,
    InvalidClient,
    InvalidGrant,
    UnauthorizedClient,
    UnsupportedGrantType,
    InvalidTarget,
    Internal(String),
}

impl TokenError {
    /// The `error` field of the response.
    pub fn code(&self) -> &'static str {
        match self {
            Self::InvalidRequest => "invalid_request",
            Self::InvalidClient => "invalid_client",
            Self::InvalidGrant => "invalid_grant",
            Self::UnauthorizedClient => "unauthorized_client",
            Self::UnsupportedGrantType => "unsupported_grant_type",
            Self::InvalidTarget => "invalid_target",
            Self::Internal(_) => "server_error",
        }
    }
}

/// The OAuth2 / OIDC provider use cases.
#[derive(Clone)]
pub struct AuthorizationServer {
    clients: Arc<dyn OidcClientRegistry>,
    codes: Arc<dyn AuthorizationCodeStore>,
    /// Generates the codes and hashes them (and the client secrets).
    code_tokens: Arc<dyn ResetTokenGenerator>,
    /// The `S256` transform, to check the PKCE verifier.
    secrets: Arc<dyn OAuthSecretGenerator>,
    signers: Signers,
    admins: Arc<dyn AdminRepository>,
    clock: Arc<dyn Clock>,
    issuer: String,
    /// The names of each account's IAM policies, for the `policy` claim;
    /// without it, the claim is not sent.
    policies: Option<Arc<dyn AccountPolicies>>,
    /// The installation's machines, whose certificates authenticate the
    /// machine roles' clients; without them, none of those does.
    machines: Option<Arc<dyn MachineRegistry>>,
    /// Which actions a person holds, for a module client's role claim;
    /// without it, the claim is sent empty.
    actions: Option<Arc<dyn ActionHolder>>,
}

impl std::fmt::Debug for AuthorizationServer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuthorizationServer")
            .field("issuer", &self.issuer)
            .finish_non_exhaustive()
    }
}

impl AuthorizationServer {
    /// `signer` is the default key (`EdDSA`); others are added with
    /// [`Self::with_signer`].
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        clients: Arc<dyn OidcClientRegistry>,
        codes: Arc<dyn AuthorizationCodeStore>,
        code_tokens: Arc<dyn ResetTokenGenerator>,
        secrets: Arc<dyn OAuthSecretGenerator>,
        signer: Arc<dyn TokenSigner>,
        admins: Arc<dyn AdminRepository>,
        clock: Arc<dyn Clock>,
        issuer: impl Into<String>,
    ) -> Self {
        Self {
            clients,
            codes,
            code_tokens,
            secrets,
            signers: Signers::new(signer),
            admins,
            clock,
            issuer: issuer.into().trim_end_matches('/').to_string(),
            policies: None,
            machines: None,
            actions: None,
        }
    }

    /// Compute the role claims of module clients (quality-sdk decision
    /// 0009) from the actions each person holds.
    pub fn with_actions(mut self, actions: Arc<dyn ActionHolder>) -> Self {
        self.actions = Some(actions);
        self
    }

    /// Authenticate the machine roles' clients with the certificates of
    /// these machines (0066, point 6).
    pub fn with_machines(mut self, machines: Arc<dyn MachineRegistry>) -> Self {
        self.machines = Some(machines);
        self
    }

    /// Send each account's IAM policy names in the `policy` claim.
    pub fn with_policies(mut self, policies: Arc<dyn AccountPolicies>) -> Self {
        self.policies = Some(policies);
        self
    }

    /// Also sign with this key, for the applications set to its algorithm
    /// (`ES256`); it is published in the JWK Set too.
    pub fn with_signer(mut self, signer: Arc<dyn TokenSigner>) -> Self {
        self.signers.add(signer);
        self
    }

    /// The `iss` of every token: the URL the discovery document lives under.
    pub fn issuer(&self) -> &str {
        &self.issuer
    }

    /// The OIDC discovery document (`/.well-known/openid-configuration`).
    pub fn discovery(&self) -> Value {
        let base = &self.issuer;
        json!({
            "issuer": base,
            "authorization_endpoint": format!("{base}/oauth/authorize"),
            "token_endpoint": format!("{base}/oauth/token"),
            "userinfo_endpoint": format!("{base}/oauth/userinfo"),
            "jwks_uri": format!("{base}/oauth/jwks"),
            "response_types_supported": ["code"],
            "grant_types_supported": ["authorization_code", "client_credentials"],
            "subject_types_supported": ["public"],
            "id_token_signing_alg_values_supported": self.signers.algorithms(),
            "scopes_supported": SCOPES_SUPPORTED,
            "token_endpoint_auth_methods_supported":
                ["client_secret_basic", "client_secret_post", "none", "tls_client_auth"],
            "code_challenge_methods_supported": ["S256"],
            "claims_supported":
                ["sub", "iss", "aud", "exp", "iat", "auth_time", "nonce", "email", "name", "role", "policy"],
            "authorization_response_iss_parameter_supported": true,
        })
    }

    /// `claims` signed with the ES256 key, for what the core signs that is
    /// no application's token (decision 0090: a job's statement bundle,
    /// which job runners verify against the JWK Set); `None` without that
    /// key.
    pub fn sign_es256(&self, typ: &str, claims: &str) -> Option<String> {
        Some(self.signers.get(SigningAlg::ES256)?.sign(typ, claims))
    }

    /// The claims of what [`Self::sign_es256`] signed with `typ`.
    pub fn verify_es256(&self, typ: &str, token: &str) -> Option<String> {
        self.signers.get(SigningAlg::ES256)?.verify(typ, token)
    }

    /// The public keys (`/jwks`), of every algorithm.
    pub fn jwks(&self) -> String {
        self.signers.jwks()
    }

    /// `GET /authorize`: check the request and, with a session, issue a code.
    pub async fn authorize(
        &self,
        req: &AuthorizeRequest,
        session: Option<&AuthenticatedSession>,
    ) -> AuthorizeOutcome {
        // Until the client and its redirect URI check out, an error cannot go
        // back to the application: a forged request would make the gateway an
        // open redirector.
        let client = match self.authorize_client(req).await {
            Ok(client) => client,
            Err(outcome) => return outcome,
        };
        let Some(redirect_uri) = req
            .redirect_uri
            .as_deref()
            .filter(|u| client.allows_redirect(u))
        else {
            return AuthorizeOutcome::Invalid("redirect_uri is not registered for this client");
        };
        let error = |code: &str| {
            AuthorizeOutcome::Redirect(self.error_redirect(
                redirect_uri,
                code,
                req.state.as_deref(),
            ))
        };

        if req.response_type.as_deref() != Some("code") {
            return error("unsupported_response_type");
        }
        let scopes = granted_scopes(req.scope.as_deref());
        if !scopes.iter().any(|s| s == "openid") {
            return error("invalid_scope");
        }
        // A challenge that comes is checked even when the client may skip it.
        let challenge = match req.code_challenge.as_deref().filter(|c| !c.is_empty()) {
            Some(c) if req.code_challenge_method.as_deref() == Some("S256") => Some(c.to_string()),
            None if client.pkce_optional() => None,
            _ => return error("invalid_request"),
        };
        let Some(session) = session else {
            return if req.prompt.as_deref() == Some("none") {
                error("login_required")
            } else {
                AuthorizeOutcome::SignIn
            };
        };
        if !self.session_meets_policy(&client, session).await {
            return error("access_denied");
        }

        let code = self.code_tokens.generate();
        let record = AuthorizationCode {
            client_id: client.id.clone(),
            admin_id: session.admin_id.clone(),
            redirect_uri: redirect_uri.to_string(),
            code_challenge: challenge,
            nonce: req.nonce.clone().filter(|n| !n.is_empty()),
            scopes,
            auth_time: session.signed_in_at,
            expires_at: self.clock.now() + CODE_TTL,
        };
        if let Err(e) = self
            .codes
            .insert(&self.code_tokens.hash(&code), &record)
            .await
        {
            tracing::error!("oidc: storing the authorization code failed: {e}");
            return error("server_error");
        }
        let mut params = vec![("code", code.as_str())];
        if let Some(state) = req.state.as_deref() {
            params.push(("state", state));
        }
        params.push(("iss", &self.issuer));
        AuthorizeOutcome::Redirect(with_query(redirect_uri, &params))
    }

    /// The client of an `/authorize` request, if it is registered and its
    /// tokens can be signed.
    async fn authorize_client(
        &self,
        req: &AuthorizeRequest,
    ) -> Result<OidcClient, AuthorizeOutcome> {
        let Some(client_id) = req
            .client_id
            .as_deref()
            .and_then(|c| ClientId::parse(c).ok())
        else {
            return Err(AuthorizeOutcome::Invalid("unknown client"));
        };
        let client = match self.clients.find(&client_id).await {
            Ok(Some(c)) if !c.disabled() => c,
            Ok(_) => return Err(AuthorizeOutcome::Invalid("unknown client")),
            Err(e) => {
                tracing::error!("oidc: client lookup failed: {e}");
                return Err(AuthorizeOutcome::Invalid("temporarily unavailable"));
            }
        };
        if let Err(TokenError::Internal(why)) = self.signers.for_client(&client) {
            tracing::error!("oidc: {why}");
            return Err(AuthorizeOutcome::Invalid(
                "this application's signing key is not configured",
            ));
        }
        Ok(client)
    }

    /// Whether the person of `session` has the policy the client requires.
    async fn session_meets_policy(
        &self,
        client: &OidcClient,
        session: &AuthenticatedSession,
    ) -> bool {
        if client.settings.required_policy.is_none() {
            return true;
        }
        match self.admins.find_by_id(&session.admin_id).await {
            Ok(Some(admin)) => {
                let names = self.policy_names(&admin).await;
                meets_policy(client, names.as_deref())
            }
            Ok(None) => false,
            Err(e) => {
                tracing::error!("oidc: account lookup failed: {e}");
                false
            }
        }
    }

    /// `POST /token`: redeem a code for an id_token and an access token, or
    /// give a machine its access token.
    pub async fn token(&self, req: &TokenRequest) -> Result<TokenResponse, TokenError> {
        match req.grant_type.as_deref() {
            Some("authorization_code") => self.redeem_code(req).await,
            Some("client_credentials") => self.client_credentials(req).await,
            _ => Err(TokenError::UnsupportedGrantType),
        }
    }

    fn error_redirect(&self, redirect_uri: &str, code: &str, state: Option<&str>) -> String {
        let mut params = vec![("error", code)];
        if let Some(state) = state {
            params.push(("state", state));
        }
        params.push(("iss", &self.issuer));
        with_query(redirect_uri, &params)
    }
}

/// Whether an account with the policy names `names` (`None`: unknown) may
/// get tokens for `client`. Without a source of names, a required policy is
/// never met.
fn meets_policy(client: &OidcClient, names: Option<&[String]>) -> bool {
    match &client.settings.required_policy {
        None => true,
        Some(required) => names.is_some_and(|n| n.iter().any(|p| p == required)),
    }
}

/// The known scopes of a space-separated `scope`, in their canonical order.
fn granted_scopes(scope: Option<&str>) -> Vec<String> {
    let asked: Vec<&str> = scope.unwrap_or_default().split_whitespace().collect();
    SCOPES_SUPPORTED
        .iter()
        .filter(|s| asked.contains(s))
        .map(|s| s.to_string())
        .collect()
}

/// The claims about the person: `email` and `name` as the scopes allow, and
/// always the role.
fn identity_claims(claims: &mut Map<String, Value>, admin: &AdminAccount, scopes: &[String]) {
    if scopes.iter().any(|s| s == "email") {
        claims.insert("email".into(), json!(admin.email.as_str()));
        // Accounts are created from a verified address (invitation, provider
        // or directory), so the gateway vouches for it.
        claims.insert("email_verified".into(), json!(true));
    }
    if scopes.iter().any(|s| s == "profile") {
        if let Some(name) = &admin.display_name {
            claims.insert("name".into(), json!(name));
        }
    }
    claims.insert("role".into(), json!(admin.role.as_str()));
}

fn epoch(t: SystemTime) -> u64 {
    t.duration_since(UNIX_EPOCH).unwrap_or_default().as_secs()
}

/// `uri` with `params` added to its query, percent-encoded.
fn with_query(uri: &str, params: &[(&str, &str)]) -> String {
    let mut out = uri.to_string();
    let mut sep = if uri.contains('?') { '&' } else { '?' };
    for (k, v) in params {
        out.push(sep);
        out.push_str(k);
        out.push('=');
        out.push_str(&percent_encode(v));
        sep = '&';
    }
    out
}

fn percent_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~') {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

#[cfg(test)]
mod tests;
