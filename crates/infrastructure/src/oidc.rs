//! Adapters for the gateway as an OIDC provider: [`Ed25519Signer`] signs the
//! tokens (JWS, `alg: EdDSA`, RFC 8037) and publishes its key as a JWK Set;
//! [`PgOidcClients`] and [`PgAuthorizationCodes`] keep the registered
//! applications and the outstanding codes (`oidc_clients`,
//! `oidc_authorization_codes`; the first in `oidc_clients.rs`). The ES256 key
//! is in [`crate::oidc_es256`]; the `oidc-client` commands in `oidc_cli`.

use std::sync::Arc;

use async_trait::async_trait;
use auth_gateway::{
    AdminRepository, AuthorizationCode, AuthorizationCodeStore, AuthorizationServer, ClientId,
    TokenSigner,
};
use base64::engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD};
use base64::Engine;
use domain::{AdminId, RepositoryError};
use ring::rand::SystemRandom;
use ring::signature::{Ed25519KeyPair, KeyPair, UnparsedPublicKey, ED25519};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Row};

use crate::admin_repo::{from_epoch, to_epoch};
pub use crate::oidc_cli::{ensure_own_client, register_client, update_client};
pub use crate::oidc_clients::PgOidcClients;
use crate::oidc_es256::{es256_signer_from_env, key_from_env, split_keys};
use crate::{OAuthSecrets, ResetTokenAdapter, SystemClock};

/// Signs with an Ed25519 key (PKCS#8, base64: `OIDC_SIGNING_KEY`, from
/// `server oidc-keygen`). The `kid` is derived from the public key, so a new
/// key is told apart without configuring one.
///
/// To rotate it, the old one goes to `OIDC_SIGNING_KEY_PREVIOUS`: tokens are
/// signed with the new one, and the old one still verifies and stays in the
/// JWK Set until the tokens it signed expire.
pub struct Ed25519Signer {
    pair: Ed25519KeyPair,
    kid: String,
    /// Previous keys: (public key, kid), for verifying and the JWK Set only.
    previous: Vec<(Vec<u8>, String)>,
}

impl std::fmt::Debug for Ed25519Signer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Ed25519Signer")
            .field("kid", &self.kid)
            .finish_non_exhaustive()
    }
}

impl Ed25519Signer {
    pub fn from_pkcs8_base64(private_key: &str) -> Result<Self, String> {
        let pkcs8 = STANDARD
            .decode(private_key.trim().as_bytes())
            .map_err(|_| "OIDC_SIGNING_KEY: not base64".to_string())?;
        let pair = Ed25519KeyPair::from_pkcs8_maybe_unchecked(&pkcs8)
            .map_err(|_| "OIDC_SIGNING_KEY: not an Ed25519 PKCS#8 key".to_string())?;
        let kid = kid_of(pair.public_key().as_ref());
        Ok(Self {
            pair,
            kid,
            previous: Vec::new(),
        })
    }

    /// Also verify (and publish) these previous keys, PKCS#8 in base64.
    pub fn with_previous<'a>(
        mut self,
        keys: impl IntoIterator<Item = &'a str>,
    ) -> Result<Self, String> {
        for key in keys {
            let old = Self::from_pkcs8_base64(key)
                .map_err(|e| e.replace("OIDC_SIGNING_KEY", "OIDC_SIGNING_KEY_PREVIOUS"))?;
            if old.kid != self.kid && !self.previous.iter().any(|(_, k)| *k == old.kid) {
                self.previous
                    .push((old.pair.public_key().as_ref().to_vec(), old.kid));
            }
        }
        Ok(self)
    }

    /// The public keys it verifies with, the current one first.
    fn public_keys(&self) -> impl Iterator<Item = (&[u8], &str)> {
        std::iter::once((self.pair.public_key().as_ref(), self.kid.as_str())).chain(
            self.previous
                .iter()
                .map(|(key, kid)| (key.as_slice(), kid.as_str())),
        )
    }

    /// A new private key, PKCS#8 in base64.
    pub fn generate() -> Result<String, String> {
        let pkcs8 = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new())
            .map_err(|_| "could not generate the key".to_string())?;
        Ok(STANDARD.encode(pkcs8.as_ref()))
    }

    fn header(&self, typ: &str) -> Value {
        json!({ "alg": "EdDSA", "typ": typ, "kid": self.kid })
    }
}

impl TokenSigner for Ed25519Signer {
    fn sign(&self, typ: &str, claims: &str) -> String {
        let signing_input = format!(
            "{}.{}",
            URL_SAFE_NO_PAD.encode(self.header(typ).to_string()),
            URL_SAFE_NO_PAD.encode(claims)
        );
        let signature = self.pair.sign(signing_input.as_bytes());
        format!(
            "{signing_input}.{}",
            URL_SAFE_NO_PAD.encode(signature.as_ref())
        )
    }

    fn verify(&self, typ: &str, token: &str) -> Option<String> {
        let mut parts = token.split('.');
        let (header, claims, signature) = (parts.next()?, parts.next()?, parts.next()?);
        if parts.next().is_some() {
            return None;
        }
        // The header is checked as values, not bytes: what matters is that it
        // names this key, this algorithm and the expected type.
        let decoded: Value = serde_json::from_slice(&URL_SAFE_NO_PAD.decode(header).ok()?).ok()?;
        if decoded.get("alg")? != "EdDSA" || decoded.get("typ")? != typ {
            return None;
        }
        let kid = decoded.get("kid")?.as_str()?;
        let (key, _) = self.public_keys().find(|(_, k)| *k == kid)?;
        let signature = URL_SAFE_NO_PAD.decode(signature).ok()?;
        UnparsedPublicKey::new(&ED25519, key)
            .verify(format!("{header}.{claims}").as_bytes(), &signature)
            .ok()?;
        String::from_utf8(URL_SAFE_NO_PAD.decode(claims).ok()?).ok()
    }

    fn algorithm(&self) -> &'static str {
        "EdDSA"
    }

    fn jwks(&self) -> String {
        let keys: Vec<Value> = self
            .public_keys()
            .map(|(key, kid)| {
                json!({
                    "kty": "OKP",
                    "crv": "Ed25519",
                    "x": URL_SAFE_NO_PAD.encode(key),
                    "kid": kid,
                    "alg": "EdDSA",
                    "use": "sig",
                })
            })
            .collect();
        json!({ "keys": keys }).to_string()
    }
}

pub(crate) fn kid_of(public_key: &[u8]) -> String {
    URL_SAFE_NO_PAD.encode(&Sha256::digest(public_key)[..12])
}

/// The previous signing keys (`OIDC_SIGNING_KEY_PREVIOUS`), separated by
/// commas or whitespace.
pub fn previous_keys_from_env() -> Vec<String> {
    split_keys(&std::env::var("OIDC_SIGNING_KEY_PREVIOUS").unwrap_or_default())
}

fn backend(err: sqlx::Error) -> RepositoryError {
    RepositoryError::Backend(err.to_string())
}

/// The outstanding authorization codes, in `oidc_authorization_codes`.
#[derive(Debug, Clone)]
pub struct PgAuthorizationCodes {
    pool: PgPool,
}

impl PgAuthorizationCodes {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl AuthorizationCodeStore for PgAuthorizationCodes {
    async fn insert(
        &self,
        code_hash: &str,
        code: &AuthorizationCode,
    ) -> Result<(), RepositoryError> {
        // The codes nobody redeemed go as new ones come: there are few, and
        // this needs no background job.
        sqlx::query("DELETE FROM oidc_authorization_codes WHERE expires_at < now()")
            .execute(&self.pool)
            .await
            .map_err(backend)?;
        sqlx::query(
            "INSERT INTO oidc_authorization_codes \
             (code_hash, client_id, admin_id, redirect_uri, code_challenge, nonce, scopes, \
              auth_time, expires_at) \
             VALUES ($1, $2, $3::uuid, $4, $5, $6, $7, to_timestamp($8), to_timestamp($9))",
        )
        .bind(code_hash)
        .bind(code.client_id.as_str())
        .bind(code.admin_id.as_str())
        .bind(&code.redirect_uri)
        // The column predates optional PKCE: an empty challenge is none.
        .bind(code.code_challenge.as_deref().unwrap_or(""))
        .bind(&code.nonce)
        .bind(&code.scopes)
        .bind(to_epoch(code.auth_time))
        .bind(to_epoch(code.expires_at))
        .execute(&self.pool)
        .await
        .map_err(backend)?;
        Ok(())
    }

    async fn take(&self, code_hash: &str) -> Result<Option<AuthorizationCode>, RepositoryError> {
        // DELETE ... RETURNING: a second redemption finds no row.
        let row = sqlx::query(
            "DELETE FROM oidc_authorization_codes WHERE code_hash = $1 \
             RETURNING client_id, admin_id::text AS admin_id, redirect_uri, code_challenge, \
             nonce, scopes, EXTRACT(EPOCH FROM auth_time)::bigint AS auth_time, \
             EXTRACT(EPOCH FROM expires_at)::bigint AS expires_at",
        )
        .bind(code_hash)
        .fetch_optional(&self.pool)
        .await
        .map_err(backend)?;
        let Some(row) = row else {
            return Ok(None);
        };
        let client: String = row.try_get("client_id").map_err(backend)?;
        let admin: String = row.try_get("admin_id").map_err(backend)?;
        Ok(Some(AuthorizationCode {
            client_id: ClientId::parse(&client)
                .map_err(|e| RepositoryError::Backend(format!("stored client {client:?}: {e}")))?,
            admin_id: AdminId::new(admin),
            redirect_uri: row.try_get("redirect_uri").map_err(backend)?,
            code_challenge: Some(
                row.try_get::<String, _>("code_challenge")
                    .map_err(backend)?,
            )
            .filter(|c| !c.is_empty()),
            nonce: row.try_get("nonce").map_err(backend)?,
            scopes: row.try_get("scopes").map_err(backend)?,
            auth_time: from_epoch(row.try_get("auth_time").map_err(backend)?),
            expires_at: from_epoch(row.try_get("expires_at").map_err(backend)?),
        }))
    }
}

/// The provider over Postgres, if `OIDC_SIGNING_KEY` (or its `_FILE`) is set
/// (else `None`): its issuer is `OIDC_ISSUER` or, without it, `default_issuer`.
/// With `OIDC_SIGNING_KEY_ES256` it also signs with ES256, for the
/// applications set to it. The machines' certificates authenticate the
/// machine roles' clients (decision 0066).
pub fn provider_from_env(
    pool: &PgPool,
    admins: Arc<dyn AdminRepository>,
    default_issuer: &str,
) -> Result<Option<AuthorizationServer>, String> {
    let Some(key) = key_from_env("OIDC_SIGNING_KEY")? else {
        return Ok(None);
    };
    let issuer = std::env::var("OIDC_ISSUER")
        .ok()
        .filter(|i| !i.trim().is_empty())
        .unwrap_or_else(|| default_issuer.to_string());
    let mut server = AuthorizationServer::new(
        Arc::new(PgOidcClients::new(pool.clone())),
        Arc::new(PgAuthorizationCodes::new(pool.clone())),
        Arc::new(ResetTokenAdapter),
        Arc::new(OAuthSecrets),
        Arc::new(
            Ed25519Signer::from_pkcs8_base64(&key)?
                .with_previous(previous_keys_from_env().iter().map(String::as_str))?,
        ),
        admins,
        Arc::new(SystemClock),
        issuer,
    )
    .with_policies(Arc::new(PgAccountPolicies::new(pool.clone())))
    .with_machines(Arc::new(crate::PgMachines::new(pool.clone())));
    if let Some(es256) = es256_signer_from_env()? {
        server = server.with_signer(Arc::new(es256));
    }
    Ok(Some(server))
}

/// The names of an account's IAM policies (ADR 0016), for the `policy`
/// claim: its own and its groups'; `admin`, `quality-admin` too.
#[derive(Debug, Clone)]
pub struct PgAccountPolicies {
    pool: PgPool,
}

impl PgAccountPolicies {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait::async_trait]
impl auth_gateway::AccountPolicies for PgAccountPolicies {
    async fn policies(
        &self,
        admin: &auth_gateway::AdminAccount,
    ) -> Result<Vec<String>, RepositoryError> {
        let mut names: Vec<String> = sqlx::query_scalar(
            "SELECT policy FROM iam_attachments
              WHERE principal_kind = 'user' AND principal_id = $1::uuid
             UNION
             SELECT a.policy FROM iam_attachments a
               JOIN iam_group_members m
                 ON a.principal_kind = 'group' AND a.principal_id = m.group_id
              WHERE m.admin_id = $1::uuid
             ORDER BY 1",
        )
        .bind(admin.id.as_str())
        .fetch_all(&self.pool)
        .await
        .map_err(backend)?;
        if admin.role == domain::Role::admin() && !names.iter().any(|n| n == "quality-admin") {
            names.insert(0, "quality-admin".to_string());
        }
        Ok(names)
    }

    async fn service_account_policies(
        &self,
        service_account: &str,
    ) -> Result<Vec<String>, RepositoryError> {
        // An expired service account grants nothing.
        sqlx::query_scalar(
            "SELECT a.policy FROM iam_attachments a
               JOIN service_accounts s
                 ON a.principal_kind = 'service_account' AND a.principal_id = s.id
              WHERE s.id = $1::uuid AND (s.expires_at IS NULL OR s.expires_at > now())
             ORDER BY 1",
        )
        .bind(service_account)
        .fetch_all(&self.pool)
        .await
        .map_err(backend)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn signer() -> Ed25519Signer {
        Ed25519Signer::from_pkcs8_base64(&Ed25519Signer::generate().unwrap()).unwrap()
    }

    #[test]
    fn signs_and_verifies_its_own_tokens() {
        let s = signer();
        let token = s.sign("at+jwt", r#"{"sub":"ana"}"#);
        assert_eq!(token.split('.').count(), 3);
        assert_eq!(
            s.verify("at+jwt", &token).as_deref(),
            Some(r#"{"sub":"ana"}"#)
        );
        // Another type, another key or a touched payload do not verify.
        assert!(s.verify("JWT", &token).is_none());
        assert!(signer().verify("at+jwt", &token).is_none());
        let mut parts: Vec<&str> = token.split('.').collect();
        let forged = URL_SAFE_NO_PAD.encode(r#"{"sub":"eva"}"#);
        parts[1] = &forged;
        assert!(s.verify("at+jwt", &parts.join(".")).is_none());
    }

    #[test]
    fn a_rotated_key_still_verifies_what_the_previous_one_signed() {
        let old_key = Ed25519Signer::generate().unwrap();
        let old = Ed25519Signer::from_pkcs8_base64(&old_key).unwrap();
        let signed_before = old.sign("JWT", r#"{"a":1}"#);
        let new = Ed25519Signer::from_pkcs8_base64(&Ed25519Signer::generate().unwrap())
            .unwrap()
            .with_previous([old_key.as_str()])
            .unwrap();
        // What came before still works; what is new is signed with the new key.
        assert_eq!(
            new.verify("JWT", &signed_before).as_deref(),
            Some(r#"{"a":1}"#)
        );
        let signed_after = new.sign("JWT", "{}");
        assert!(old.verify("JWT", &signed_after).is_none());
        assert_eq!(new.verify("JWT", &signed_after).as_deref(), Some("{}"));
        // The JWK Set publishes both, the current one first.
        let jwks: Value = serde_json::from_str(&new.jwks()).unwrap();
        assert_eq!(jwks["keys"].as_array().unwrap().len(), 2);
        assert_eq!(jwks["keys"][0]["kid"], new.kid.as_str());
        // Without the previous key, what was signed with it no longer works.
        let alone = Ed25519Signer::from_pkcs8_base64(&Ed25519Signer::generate().unwrap()).unwrap();
        assert!(alone.verify("JWT", &signed_before).is_none());
    }

    #[test]
    fn the_jwks_carries_the_public_key_under_the_tokens_kid() {
        let s = signer();
        let jwks: Value = serde_json::from_str(&s.jwks()).unwrap();
        let key = &jwks["keys"][0];
        assert_eq!(key["kty"], "OKP");
        assert_eq!(key["crv"], "Ed25519");
        let header: Value = serde_json::from_slice(
            &URL_SAFE_NO_PAD
                .decode(s.sign("JWT", "{}").split('.').next().unwrap())
                .unwrap(),
        )
        .unwrap();
        assert_eq!(header["kid"], key["kid"]);
        assert_eq!(header["alg"], "EdDSA");
        // The JWK's `x` verifies the signature by itself, as a relying party
        // would.
        let x = URL_SAFE_NO_PAD.decode(key["x"].as_str().unwrap()).unwrap();
        let token = s.sign("JWT", r#"{"a":1}"#);
        let (input, sig) = token.rsplit_once('.').unwrap();
        UnparsedPublicKey::new(&ED25519, x)
            .verify(input.as_bytes(), &URL_SAFE_NO_PAD.decode(sig).unwrap())
            .unwrap();
    }
}
