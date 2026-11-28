//! A job's token for a capability's audience (decision 0062): signed as the installation's
//! own machine client, naming the job and the machine it goes to, for 15
//! minutes whatever the provider's other tokens last.

use super::settings::TaggedSigner;
use super::*;
use crate::domain::{JobSubject, JobUserKind};

const SUBJECT: &str = "job:00000000-0000-0000-0000-00000000000a:\
                       7f0c2a52-1c1e-4c43-9a55-0d2f1e0b6a10:read:src_core";

fn machine() -> ClientSettings {
    ClientSettings {
        client_credentials: true,
        audiences: vec!["sql-query".into()],
        ..ClientSettings::default()
    }
}

fn secret() -> ClientAuth {
    ClientAuth::Secret { hash: "h:s".into() }
}

#[tokio::test]
async fn a_jobs_token_names_the_job_and_its_machine_for_fifteen_minutes() {
    let f = fixture_with(secret(), machine());
    let token = f
        .server
        .job_token("gestion", "sql-query", SUBJECT, "qd-worker-1")
        .await
        .unwrap();
    assert_eq!(token.expires_in, 15 * 60);
    assert!(token.id_token.is_none());
    assert!(token.access_token.starts_with("at+jwt|"));
    let c = claims(&token.access_token);
    assert_eq!(c["iss"], ISSUER);
    assert_eq!(c["aud"], "sql-query");
    assert_eq!(c["sub"], SUBJECT);
    assert_eq!(c["act"], serde_json::json!({ "sub": "qd-worker-1" }));
    assert_eq!(c["iat"], 1_000_000);
    assert_eq!(c["exp"], 1_000_000 + 15 * 60);
    assert!(c["jti"].is_string());
    // Not a machine's token: no client, no policy.
    assert!(c.get("client_id").is_none() && c.get("policy").is_none());
}

#[tokio::test]
async fn a_jobs_token_needs_the_installations_own_client_allowed_the_audience() {
    let f = fixture_with(secret(), machine());
    let other = f.server.job_token("gestion", "lake", SUBJECT, "w").await;
    assert_eq!(other.unwrap_err().code(), "invalid_target");
    let unknown = f.server.job_token("nadie", "sql-query", SUBJECT, "w").await;
    assert_eq!(unknown.unwrap_err().code(), "invalid_client");
    let person = fixture(secret());
    let refused = person
        .server
        .job_token("gestion", "sql-query", SUBJECT, "w")
        .await;
    assert_eq!(refused.unwrap_err().code(), "unauthorized_client");
}

#[tokio::test]
async fn a_jobs_token_is_no_machine_token_for_the_api() {
    let both = ClientSettings {
        audiences: vec!["sql-query".into(), "quality-api".into()],
        ..machine()
    };
    let f = fixture_with(secret(), both);
    let token = f
        .server
        .job_token("gestion", "quality-api", SUBJECT, "w")
        .await
        .unwrap();
    assert_eq!(
        f.server.machine_client(&token.access_token, "quality-api"),
        None
    );
}

fn trino_machine() -> ClientSettings {
    ClientSettings {
        client_credentials: true,
        audiences: vec!["trino".into()],
        ..ClientSettings::default()
    }
}

const JOB: &str = "7f0c2a52-1c1e-4c43-9a55-0d2f1e0b6a10";

#[tokio::test]
async fn a_jobs_trino_token_names_its_kind_and_dataset_signed_es256() {
    let f = fixture_with(secret(), trino_machine());
    let subject = JobSubject::new(JOB, JobUserKind::Rules, "loans").unwrap();
    let no_key = f
        .server
        .trino_job_token("gestion", &subject, "runner")
        .await;
    assert_eq!(no_key.unwrap_err().code(), "server_error", "no ES256 key");
    let server = f.server.with_signer(Arc::new(TaggedSigner("ES256")));
    let token = server
        .trino_job_token("gestion", &subject, "engine-trino")
        .await
        .unwrap();
    assert_eq!(token.expires_in, 15 * 60);
    let claims = server.verify_es256("at+jwt", &token.access_token).unwrap();
    let c: serde_json::Value = serde_json::from_str(&claims).unwrap();
    assert_eq!(c["sub"], format!("job:{JOB}:rules:loans"));
    assert_eq!(c["aud"], "trino");
    assert_eq!(c["act"], serde_json::json!({ "sub": "engine-trino" }));
    assert_eq!(c["exp"], 1_000_000 + 15 * 60);
}

#[tokio::test]
async fn a_jobs_trino_token_needs_a_client_allowed_trino() {
    let f = fixture_with(secret(), machine());
    let server = f.server.with_signer(Arc::new(TaggedSigner("ES256")));
    let subject = JobSubject::new(JOB, JobUserKind::Report, "loans").unwrap();
    let refused = server.trino_job_token("gestion", &subject, "w").await;
    assert_eq!(refused.unwrap_err().code(), "invalid_target");
    let unknown = server.trino_job_token("nadie", &subject, "w").await;
    assert_eq!(unknown.unwrap_err().code(), "invalid_client");
}

#[tokio::test]
async fn the_cores_own_token_is_a_machine_token_of_its_own_client() {
    let orchestrator = ClientSettings {
        client_credentials: true,
        audiences: vec!["orchestrator".into()],
        ..ClientSettings::default()
    };
    let f = fixture_with(secret(), orchestrator);
    let token = f
        .server
        .own_machine_token("gestion", "orchestrator")
        .await
        .unwrap();
    let (client, machine) = f
        .server
        .machine_caller(&token.access_token, "orchestrator")
        .unwrap();
    assert_eq!((client.as_str(), machine), ("gestion", None));
    let refused = f.server.own_machine_token("gestion", "trino").await;
    assert_eq!(refused.unwrap_err().code(), "invalid_target");
}
