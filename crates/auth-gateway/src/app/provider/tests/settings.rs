//! The per-application settings: the signing algorithm, PKCE made optional
//! for a confidential client, a required policy, and machine tokens.

use super::*;
use crate::domain::SigningAlg;

const SECRET: &str = "s3cret";

fn confidential() -> ClientAuth {
    ClientAuth::Secret {
        hash: format!("h:{SECRET}"),
    }
}

/// Signs as `alg:typ|claims`: a second key, told apart from [`PlainSigner`].
struct TaggedSigner(&'static str);
impl TokenSigner for TaggedSigner {
    fn sign(&self, typ: &str, claims: &str) -> String {
        format!("{}:{typ}|{claims}", self.0)
    }
    fn verify(&self, typ: &str, token: &str) -> Option<String> {
        token
            .strip_prefix(&format!("{}:{typ}|", self.0))
            .map(str::to_string)
    }
    fn algorithm(&self) -> &'static str {
        self.0
    }
    fn jwks(&self) -> String {
        format!(r#"{{"keys":[{{"kid":"{}"}}]}}"#, self.0)
    }
}

/// Policy names that can change between `/authorize` and `/token`; the
/// service accounts have `worker`.
struct Switch(Mutex<Vec<String>>);

#[async_trait::async_trait]
impl crate::domain::AccountPolicies for Switch {
    async fn policies(&self, _admin: &AdminAccount) -> Result<Vec<String>, RepositoryError> {
        Ok(self.0.lock().unwrap().clone())
    }
    async fn service_account_policies(&self, id: &str) -> Result<Vec<String>, RepositoryError> {
        Ok(vec![format!("worker-of-{id}")])
    }
}

fn redirected_error(outcome: &AuthorizeOutcome) -> Option<String> {
    let AuthorizeOutcome::Redirect(to) = outcome else {
        return None;
    };
    let query = to.strip_prefix(&format!("{REDIRECT}?"))?;
    query
        .split('&')
        .find_map(|p| p.strip_prefix("error="))
        .map(str::to_string)
}

fn without_pkce() -> AuthorizeRequest {
    AuthorizeRequest {
        code_challenge: None,
        code_challenge_method: None,
        ..request()
    }
}

fn with_secret(code: &str) -> TokenRequest {
    TokenRequest {
        client_secret: Some(SECRET.into()),
        code_verifier: None,
        ..token_request(code)
    }
}

#[tokio::test]
async fn a_flagged_confidential_client_may_skip_pkce_and_keeps_the_nonce() {
    let settings = ClientSettings {
        pkce_required: false,
        ..ClientSettings::default()
    };
    let f = fixture_with(confidential(), settings);
    let outcome = f.server.authorize(&without_pkce(), Some(&f.session)).await;
    assert_eq!(redirected_error(&outcome), None, "{outcome:?}");
    let tokens = f.server.token(&with_secret("code-1")).await.unwrap();
    assert_eq!(claims(tokens.id_token.as_deref().unwrap())["nonce"], "n-1");

    // Without its secret, the code is no good.
    f.server.authorize(&without_pkce(), Some(&f.session)).await;
    let no_secret = TokenRequest {
        client_secret: None,
        ..with_secret("code-3")
    };
    assert_eq!(
        f.server.token(&no_secret).await,
        Err(TokenError::InvalidClient)
    );
    // A verifier for a code issued without a challenge is a downgrade.
    f.server.authorize(&without_pkce(), Some(&f.session)).await;
    let downgrade = TokenRequest {
        code_verifier: Some(VERIFIER.into()),
        ..with_secret("code-4")
    };
    assert_eq!(
        f.server.token(&downgrade).await,
        Err(TokenError::InvalidGrant)
    );
    // A challenge that comes is still checked.
    f.server.authorize(&request(), Some(&f.session)).await;
    let wrong = TokenRequest {
        code_verifier: Some("x".repeat(43)),
        ..with_secret("code-5")
    };
    assert_eq!(f.server.token(&wrong).await, Err(TokenError::InvalidGrant));
}

#[tokio::test]
async fn public_and_unflagged_clients_still_require_pkce() {
    let off = ClientSettings {
        pkce_required: false,
        ..ClientSettings::default()
    };
    for f in [
        fixture_with(ClientAuth::Public, off),
        fixture(confidential()),
    ] {
        let outcome = f.server.authorize(&without_pkce(), Some(&f.session)).await;
        assert_eq!(
            redirected_error(&outcome).as_deref(),
            Some("invalid_request")
        );
    }
    // And `/token` asks for the verifier before spending anything.
    let f = fixture(confidential());
    f.server.authorize(&request(), Some(&f.session)).await;
    assert_eq!(
        f.server.token(&with_secret("code-1")).await,
        Err(TokenError::InvalidRequest)
    );
}

#[tokio::test]
async fn a_required_policy_denies_the_others_with_access_denied() {
    let settings = ClientSettings {
        required_policy: Some("quality-admin".into()),
        ..ClientSettings::default()
    };
    // Without a source of policies, it is never met.
    let f = fixture_with(ClientAuth::Public, settings.clone());
    let outcome = f.server.authorize(&request(), Some(&f.session)).await;
    assert_eq!(redirected_error(&outcome).as_deref(), Some("access_denied"));
    assert!(matches!(&outcome, AuthorizeOutcome::Redirect(to) if to.contains("state=st%201")));

    let mut f = fixture_with(ClientAuth::Public, settings);
    let policies = Arc::new(Switch(Mutex::new(vec!["auditor".into()])));
    f.server = f.server.with_policies(policies.clone());
    let outcome = f.server.authorize(&request(), Some(&f.session)).await;
    assert_eq!(redirected_error(&outcome).as_deref(), Some("access_denied"));

    *policies.0.lock().unwrap() = vec!["quality-admin".into()];
    let outcome = f.server.authorize(&request(), Some(&f.session)).await;
    assert_eq!(redirected_error(&outcome), None);
    // Taken away before the code is redeemed: no tokens.
    *policies.0.lock().unwrap() = vec!["auditor".into()];
    assert_eq!(
        f.server.token(&token_request("code-1")).await,
        Err(TokenError::InvalidGrant)
    );
}

#[tokio::test]
async fn each_client_is_signed_with_its_algorithm_if_the_key_is_there() {
    let es256 = ClientSettings {
        token_alg: SigningAlg::ES256,
        ..ClientSettings::default()
    };
    // Without the key: said on the page, not a broken token.
    let f = fixture_with(ClientAuth::Public, es256.clone());
    assert!(matches!(
        f.server.authorize(&request(), Some(&f.session)).await,
        AuthorizeOutcome::Invalid(_)
    ));

    let mut f = fixture_with(ClientAuth::Public, es256);
    f.server = f.server.with_signer(Arc::new(TaggedSigner("ES256")));
    f.server.authorize(&request(), Some(&f.session)).await;
    let tokens = f.server.token(&token_request("code-1")).await.unwrap();
    assert!(tokens.access_token.starts_with("ES256:at+jwt|"));
    assert!(tokens.id_token.unwrap().starts_with("ES256:JWT|"));
    // Any key verifies; the discovery and the JWK Set list both.
    assert_eq!(
        f.server.userinfo(&tokens.access_token).await.unwrap()["sub"],
        "ana"
    );
    let d = f.server.discovery();
    assert_eq!(
        d["id_token_signing_alg_values_supported"],
        json!(["EdDSA", "ES256"])
    );
    assert_eq!(
        d["grant_types_supported"],
        json!(["authorization_code", "client_credentials"])
    );
    let jwks: Value = serde_json::from_str(&f.server.jwks()).unwrap();
    assert_eq!(jwks["keys"], json!([{ "kid": "ES256" }]));
}

fn machine_settings(service_account: Option<&str>) -> ClientSettings {
    ClientSettings {
        client_credentials: true,
        audiences: vec!["sql-query".into(), "lake".into()],
        service_account: service_account.map(str::to_string),
        ..ClientSettings::default()
    }
}

fn machine_request() -> TokenRequest {
    TokenRequest {
        grant_type: Some("client_credentials".into()),
        client_id: Some("gestion".into()),
        client_secret: Some(SECRET.into()),
        ..TokenRequest::default()
    }
}

#[tokio::test]
async fn a_machine_gets_a_token_for_one_of_its_audiences() {
    let mut f = fixture_with(confidential(), machine_settings(None));
    f.server = f
        .server
        .with_policies(Arc::new(Switch(Mutex::new(Vec::new()))));
    let tokens = f.server.token(&machine_request()).await.unwrap();
    assert!(tokens.id_token.is_none());
    assert!(tokens.scope.is_empty());
    assert_eq!(tokens.expires_in, 24 * 3600);
    let at = claims(&tokens.access_token);
    assert_eq!(at["aud"], "sql-query");
    assert_eq!(at["sub"], "gestion");
    assert_eq!(at["client_id"], "gestion");
    assert_eq!(at["iss"], ISSUER);
    assert_eq!(at["exp"], 1_000_000 + 24 * 3600);
    assert!(at["jti"].is_string());
    assert_eq!(at["policy"], json!([]));

    // Another of its audiences, as `resource` or `audience`.
    for req in [
        TokenRequest {
            resource: Some("lake".into()),
            ..machine_request()
        },
        TokenRequest {
            audience: Some("lake".into()),
            ..machine_request()
        },
    ] {
        let tokens = f.server.token(&req).await.unwrap();
        assert_eq!(claims(&tokens.access_token)["aud"], "lake");
    }

    // Acting as a service account: its id and its policies.
    let mut f = fixture_with(confidential(), machine_settings(Some("sa-1")));
    f.server = f
        .server
        .with_policies(Arc::new(Switch(Mutex::new(Vec::new()))));
    let at = claims(
        &f.server
            .token(&machine_request())
            .await
            .unwrap()
            .access_token,
    );
    assert_eq!(at["sub"], "sa-1");
    assert_eq!(at["policy"], json!(["worker-of-sa-1"]));
}

#[tokio::test]
async fn a_service_reads_which_machine_a_token_belongs_to() {
    let f = fixture_with(confidential(), machine_settings(None));
    let token = f
        .server
        .token(&machine_request())
        .await
        .unwrap()
        .access_token;
    assert_eq!(
        f.server.machine_client(&token, "sql-query").as_deref(),
        Some("gestion")
    );
    // A client with a secret names no machine.
    assert_eq!(
        f.server.machine_caller(&token, "sql-query"),
        Some(("gestion".into(), None))
    );
    // For another audience, tampered, or a person's: none.
    assert_eq!(f.server.machine_client(&token, "lake"), None);
    assert_eq!(
        f.server.machine_client(&format!("{token}x"), "sql-query"),
        None
    );
    let f = fixture(ClientAuth::Public);
    f.server.authorize(&request(), Some(&f.session)).await;
    let person = f.server.token(&token_request("code-1")).await.unwrap();
    assert_eq!(
        f.server.machine_client(&person.access_token, "sql-query"),
        None
    );
}

#[tokio::test]
async fn a_machine_token_needs_the_secret_an_allowed_audience_and_the_flag() {
    let f = fixture_with(confidential(), machine_settings(None));
    let wrong_secret = TokenRequest {
        client_secret: Some("otro".into()),
        ..machine_request()
    };
    assert_eq!(
        f.server.token(&wrong_secret).await,
        Err(TokenError::InvalidClient)
    );
    let other_audience = TokenRequest {
        resource: Some("billing".into()),
        ..machine_request()
    };
    assert_eq!(
        f.server.token(&other_audience).await,
        Err(TokenError::InvalidTarget)
    );
    let two_audiences = TokenRequest {
        resource: Some("sql-query".into()),
        audience: Some("lake".into()),
        ..machine_request()
    };
    assert_eq!(
        f.server.token(&two_audiences).await,
        Err(TokenError::InvalidRequest)
    );

    // Not flagged, or public (even if flagged): not for this grant.
    let f = fixture(confidential());
    assert_eq!(
        f.server.token(&machine_request()).await,
        Err(TokenError::UnauthorizedClient)
    );
    let f = fixture_with(ClientAuth::Public, machine_settings(None));
    let public = TokenRequest {
        client_secret: None,
        ..machine_request()
    };
    assert_eq!(
        f.server.token(&public).await,
        Err(TokenError::UnauthorizedClient)
    );
}

#[test]
fn settings_that_need_a_secret_are_refused_for_a_public_client() {
    let no_pkce = ClientSettings {
        pkce_required: false,
        ..ClientSettings::default()
    };
    assert!(no_pkce.check(&ClientAuth::Public).is_err());
    assert!(no_pkce.check(&confidential()).is_ok());
    assert!(machine_settings(None).check(&ClientAuth::Public).is_err());
    assert!(machine_settings(None).check(&confidential()).is_ok());
    let no_audience = ClientSettings {
        audiences: Vec::new(),
        ..machine_settings(None)
    };
    assert!(no_audience.check(&confidential()).is_err());
}

#[tokio::test]
async fn the_es256_key_signs_what_is_no_applications_token() {
    let f = fixture_with(ClientAuth::Public, ClientSettings::default());
    assert_eq!(
        f.server.sign_es256("qd-bundle+jwt", "{}"),
        None,
        "no ES256 key"
    );
    let server = f.server.with_signer(Arc::new(TaggedSigner("ES256")));
    let token = server
        .sign_es256("qd-bundle+jwt", "{\"job_id\":\"j\"}")
        .unwrap();
    assert_eq!(
        server.verify_es256("qd-bundle+jwt", &token).as_deref(),
        Some("{\"job_id\":\"j\"}")
    );
    assert_eq!(server.verify_es256("JWT", &token), None, "another type");
}
