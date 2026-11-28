//! The OIDC provider's applications on Postgres (`oidc_clients`), connected
//! as the application's role: each way of authenticating comes back as it
//! was stored, and the table refuses what the domain refuses: machine tokens
//! with neither a secret nor a machine role, a machine role with a secret,
//! redirect URIs or no `client_credentials` (decision 0066, point 6). Needs
//! Docker.

use auth_gateway::{
    ClientAuth, ClientId, ClientSettings, MachineRole, OidcClient, OidcClientRegistry,
};
use infrastructure::oidc::PgOidcClients;
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;

async fn app_pool(db: &testkit::TestDb) -> PgPool {
    PgPoolOptions::new()
        .max_connections(2)
        .connect_with(testkit::app_role(db).await)
        .await
        .unwrap()
}

fn machine_settings() -> ClientSettings {
    ClientSettings {
        client_credentials: true,
        audiences: vec!["quality-worker".into()],
        ..ClientSettings::default()
    }
}

fn client(
    id: &str,
    auth: ClientAuth,
    redirect_uris: &[&str],
    settings: ClientSettings,
) -> OidcClient {
    OidcClient {
        id: ClientId::parse(id).unwrap(),
        name: id.into(),
        redirect_uris: redirect_uris.iter().map(|u| u.to_string()).collect(),
        auth,
        settings,
    }
}

#[tokio::test]
async fn oidc_clients_round_trip_each_way_of_authenticating() {
    let db = testkit::spawn_test_db().await;
    let clients = PgOidcClients::new(app_pool(&db).await);
    let worker = ClientAuth::Certificate {
        role: MachineRole::Worker,
    };
    let cases = [
        client(
            "web",
            ClientAuth::Public,
            &["https://a/cb"],
            ClientSettings::default(),
        ),
        client(
            "reports-ui",
            ClientAuth::Secret { hash: "h".into() },
            &["https://t/cb"],
            ClientSettings::default(),
        ),
        client("qd-worker", worker.clone(), &[], machine_settings()),
        client(
            "qd-coordinator",
            ClientAuth::Certificate {
                role: MachineRole::TrinoCoordinator,
            },
            &[],
            machine_settings(),
        ),
    ];
    for c in &cases {
        assert!(clients.insert(c).await.unwrap(), "{}", c.id);
        assert_eq!(clients.find(&c.id).await.unwrap().as_ref(), Some(c));
    }
    // An installation's own upsert does not write a role's client.
    assert!(clients.upsert(&cases[2]).await.is_err());
    assert_eq!(
        clients.find(&cases[2].id).await.unwrap().unwrap().auth,
        worker
    );
}

#[tokio::test]
async fn oidc_clients_refuse_what_the_domain_refuses() {
    let db = testkit::spawn_test_db().await;
    let pool = app_pool(&db).await;
    let clients = PgOidcClients::new(pool.clone());
    let worker = || ClientAuth::Certificate {
        role: MachineRole::Worker,
    };
    let refused = [
        // Machine tokens with neither a secret nor a role.
        client("a", ClientAuth::Public, &[], machine_settings()),
        // A role with redirect URIs, or without client_credentials.
        client("b", worker(), &["https://b/cb"], machine_settings()),
        client("c", worker(), &[], ClientSettings::default()),
    ];
    for c in &refused {
        assert!(clients.insert(c).await.is_err(), "{}", c.id);
    }
    // A role with a secret, and a role that is not one.
    for (id, secret, role) in [("d", Some("h"), "worker"), ("e", None, "admin")] {
        let inserted = sqlx::query(
            "INSERT INTO oidc_clients (client_id, name, redirect_uris, secret_hash, \
             grant_client_credentials, audiences, machine_role) \
             VALUES ($1, $1, '{}', $2, true, '{w}', $3)",
        )
        .bind(id)
        .bind(secret)
        .bind(role)
        .execute(&pool)
        .await;
        assert!(inserted.is_err(), "{id}");
    }
    let none: i64 = sqlx::query_scalar("SELECT count(*) FROM oidc_clients")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(none, 0);
}

fn args(s: &str) -> Vec<String> {
    s.split_whitespace().map(str::to_string).collect()
}

#[tokio::test]
async fn oidc_clients_the_commands_register_a_machine_role_without_a_secret() {
    use infrastructure::oidc::{register_client, update_client};
    let db = testkit::spawn_test_db().await;
    let pool = app_pool(&db).await;
    let clients = PgOidcClients::new(pool.clone());
    let worker = "qd-worker Workers --machine-role worker --client-credentials --audience qa";
    let lines = register_client(&pool, &args(worker)).await.unwrap();
    assert!(
        lines.iter().all(|l| !l.contains("client_secret")),
        "{lines:?}"
    );
    assert!(lines.iter().any(|l| l.starts_with("machine_role: worker")));
    let id = ClientId::parse("qd-worker").unwrap();
    let stored = clients.find(&id).await.unwrap().unwrap();
    let role = MachineRole::Worker;
    assert_eq!(stored.auth, ClientAuth::Certificate { role });
    for bad in [
        "a A --machine-role worker --client-credentials --audience qa --public",
        "b B https://b/cb --machine-role worker --client-credentials --audience qa",
        "c C --machine-role worker",
    ] {
        assert!(register_client(&pool, &args(bad)).await.is_err(), "{bad}");
    }

    // A secret client becomes one: its secret goes.
    let api = "qd-api Api --client-credentials --audience qa";
    register_client(&pool, &args(api)).await.unwrap();
    let to_role = "qd-api --machine-role trino-coordinator";
    update_client(&pool, &args(to_role)).await.unwrap();
    let id = ClientId::parse("qd-api").unwrap();
    let stored = clients.find(&id).await.unwrap().unwrap();
    let role = MachineRole::TrinoCoordinator;
    assert_eq!(stored.auth, ClientAuth::Certificate { role });
    let with_uri = "qd-api --redirect-uri https://x/cb";
    assert!(update_client(&pool, &args(with_uri)).await.is_err());
}

/// Ruling 60 of decision 0090: the API's own client for jobs' Trino tokens
/// is registered once, as a machine client for `trino` signing ES256, and
/// a second start leaves it (and any change a person made) as it is.
#[tokio::test]
async fn the_own_client_for_trino_is_registered_once() {
    let db = testkit::spawn_test_db().await;
    let pool = app_pool(&db).await;
    let ensure = || infrastructure::oidc::ensure_own_client(&pool, "qd-api-jobs", "Jobs", "trino");
    assert!(ensure().await.unwrap());
    assert!(!ensure().await.unwrap());
    let found = PgOidcClients::new(pool.clone())
        .find(&ClientId::parse("qd-api-jobs").unwrap())
        .await
        .unwrap()
        .unwrap();
    assert!(found.machine());
    assert_eq!(found.settings.audiences, ["trino"]);
    assert_eq!(found.settings.token_alg, auth_gateway::SigningAlg::ES256);
    assert!(matches!(found.auth, ClientAuth::Secret { .. }));
    assert!(found.redirect_uris.is_empty());
}
