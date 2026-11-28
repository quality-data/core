//! The `oidc-client` and `oidc-client-update` commands: register an
//! application with the OIDC provider, or change one, with its settings
//! (signing algorithm, PKCE, required policy, machine tokens).

use auth_gateway::{
    ClientAuth, ClientId, ClientSettings, MachineRole, OidcClient, OidcClientRegistry,
    ResetTokenGenerator, SigningAlg,
};
use sqlx::PgPool;

use crate::oidc::PgOidcClients;
use crate::oidc_clients::auth_columns;
use crate::ResetTokenAdapter;

const REGISTER_USAGE: &str = "usage: oidc-client ID NAME REDIRECT_URI... [--public] \
     [--home URL] [--detail TEXT] [--icon URL] [--alg EdDSA|ES256] [--no-pkce] \
     [--require-policy NAME] [--client-credentials --audience A [--audience B]... \
     [--service-account ID] [--machine-role ROLE]]";

const UPDATE_USAGE: &str = "usage: oidc-client-update ID [--name NAME] \
     [--redirect-uri URI]... [--home URL | --no-home] [--detail TEXT] [--icon URL] \
     [--alg EdDSA|ES256] [--pkce | --no-pkce] [--require-policy NAME | --no-require-policy] \
     [--client-credentials | --no-client-credentials] [--audience A]... \
     [--service-account ID | --no-service-account] [--machine-role ROLE]";

mod flags;

use flags::{check_redirect_uris, Flags};

/// The service account, if set, must exist.
async fn check_service_account(pool: &PgPool, settings: &ClientSettings) -> Result<(), String> {
    let Some(id) = &settings.service_account else {
        return Ok(());
    };
    let found: Option<i32> =
        sqlx::query_scalar("SELECT 1 FROM service_accounts WHERE id = $1::uuid")
            .bind(id)
            .fetch_optional(pool)
            .await
            .map_err(|e| e.to_string())?;
    found
        .map(|_| ())
        .ok_or_else(|| format!("there is no service account {id}"))
}

/// Register an application from the arguments of `oidc-client` (see
/// [`REGISTER_USAGE`]). Without `--public` it gets a secret, in the lines
/// returned this one time; only its hash is kept. A machine client
/// (`--client-credentials`) needs no redirect URI.
pub async fn register_client(pool: &PgPool, args: &[String]) -> Result<Vec<String>, String> {
    let flags = Flags::parse(args, false)?;
    let [id, name, redirect_uris @ ..] = flags.positional.as_slice() else {
        return Err(REGISTER_USAGE.into());
    };
    if redirect_uris.is_empty() && flags.client_credentials != Some(true) {
        return Err(REGISTER_USAGE.into());
    }
    check_redirect_uris(redirect_uris)?;
    let id = ClientId::parse(id).map_err(|e| e.to_string())?;
    let (auth, secret) = new_auth(&flags, redirect_uris)?;
    let mut settings = ClientSettings::default();
    flags.apply(&mut settings)?;
    settings.check(&auth)?;
    check_service_account(pool, &settings).await?;
    let client = OidcClient {
        id: id.clone(),
        name: name.to_string(),
        redirect_uris: redirect_uris.to_vec(),
        auth,
        settings,
    };
    if !PgOidcClients::new(pool.clone())
        .insert(&client)
        .await
        .map_err(|e| e.to_string())?
    {
        return Err(format!("there is already an application {id}"));
    }
    set_listing(pool, &id, &flags).await?;
    let mut lines = vec![format!("client_id: {id}")];
    lines.push(match (secret, &client.auth) {
        (Some(secret), _) => {
            format!("client_secret (save it now, it is not shown again): {secret}")
        }
        (None, ClientAuth::Certificate { role }) => machine_role_line(*role),
        (None, _) => "public: no secret, PKCE only".to_string(),
    });
    lines.extend(describe(&client.settings));
    Ok(lines)
}

/// The installation's own machine client `id` for `audience`, registered
/// if it is not (decision 0090, ruling 60): the API signs its tokens itself
/// (`own_client`), so its secret is made and dropped here, never shown.
/// An existing client is left as it is, so a person may change or delete
/// it. True if it was registered now.
pub async fn ensure_own_client(
    pool: &PgPool,
    id: &str,
    name: &str,
    audience: &str,
) -> Result<bool, String> {
    let id = ClientId::parse(id).map_err(|e| e.to_string())?;
    let clients = PgOidcClients::new(pool.clone());
    if clients
        .find(&id)
        .await
        .map_err(|e| e.to_string())?
        .is_some()
    {
        return Ok(false);
    }
    let hash = ResetTokenAdapter.hash(&ResetTokenAdapter.generate());
    let auth = ClientAuth::Secret { hash };
    let settings = ClientSettings {
        token_alg: SigningAlg::ES256,
        client_credentials: true,
        audiences: vec![audience.to_owned()],
        ..ClientSettings::default()
    };
    settings.check(&auth)?;
    let client = OidcClient {
        id,
        name: name.to_owned(),
        redirect_uris: Vec::new(),
        auth,
        settings,
    };
    // Two APIs starting at once: the second insert finds the first's row.
    clients.insert(&client).await.map_err(|e| e.to_string())
}

/// How a new client authenticates, and its secret if it gets one: by the
/// certificate of a machine of `--machine-role` (no secret, no redirect
/// URI, never public), public, or with a new secret.
fn new_auth(
    flags: &Flags,
    redirect_uris: &[String],
) -> Result<(ClientAuth, Option<String>), String> {
    if let Some(role) = flags.machine_role {
        if flags.public || !redirect_uris.is_empty() {
            return Err("--machine-role: no --public and no redirect URI; \
                        its machines' certificates authenticate it"
                .into());
        }
        return Ok((ClientAuth::Certificate { role }, None));
    }
    if flags.public {
        return Ok((ClientAuth::Public, None));
    }
    let secret = ResetTokenAdapter.generate();
    let hash = ResetTokenAdapter.hash(&secret);
    Ok((ClientAuth::Secret { hash }, Some(secret)))
}

fn machine_role_line(role: MachineRole) -> String {
    format!(
        "machine_role: {} — no secret: the certificate of an approved or draining \
         machine of that role (tls_client_auth, on the internal entry point); \
         its tokens last 15 minutes",
        role.as_str()
    )
}

/// Change an existing application from the arguments of
/// `oidc-client-update` (see [`UPDATE_USAGE`]): only what is given changes.
/// Its secret, or being public, stays, unless `--machine-role` makes it a
/// machine role's client, whose secret goes.
pub async fn update_client(pool: &PgPool, args: &[String]) -> Result<Vec<String>, String> {
    let flags = Flags::parse(args, true)?;
    let [id] = flags.positional.as_slice() else {
        return Err(UPDATE_USAGE.into());
    };
    let id = ClientId::parse(id).map_err(|e| e.to_string())?;
    let mut client = PgOidcClients::new(pool.clone())
        .find(&id)
        .await
        .map_err(|e| e.to_string())?
        .ok_or_else(|| format!("there is no application {id}"))?;
    if let Some(name) = &flags.name {
        client.name.clone_from(name);
    }
    if !flags.redirect_uris.is_empty() {
        check_redirect_uris(&flags.redirect_uris)?;
        client.redirect_uris.clone_from(&flags.redirect_uris);
    }
    if let Some(role) = flags.machine_role {
        client.auth = ClientAuth::Certificate { role };
    }
    if client.machine_role().is_some() && !client.redirect_uris.is_empty() {
        return Err("a machine role's client has no redirect URI".into());
    }
    flags.apply(&mut client.settings)?;
    client.settings.check(&client.auth)?;
    check_service_account(pool, &client.settings).await?;
    let s = &client.settings;
    let (secret_hash, machine_role) = auth_columns(&client.auth);
    sqlx::query(
        "UPDATE oidc_clients SET name = $2, redirect_uris = $3, token_alg = $4, \
         pkce_required = $5, required_policy = $6, grant_client_credentials = $7, \
         audiences = $8, service_account_id = $9::uuid, secret_hash = $10, \
         machine_role = $11 WHERE client_id = $1",
    )
    .bind(id.as_str())
    .bind(&client.name)
    .bind(&client.redirect_uris)
    .bind(s.token_alg.as_str())
    .bind(s.pkce_required)
    .bind(&s.required_policy)
    .bind(s.client_credentials)
    .bind(&s.audiences)
    .bind(&s.service_account)
    .bind(secret_hash)
    .bind(machine_role)
    .execute(pool)
    .await
    .map_err(|e| e.to_string())?;
    set_listing(pool, &id, &flags).await?;
    let mut lines = vec![format!("updated: {id}")];
    if let Some(role) = client.machine_role() {
        lines.push(machine_role_line(role));
    }
    lines.extend(describe(&client.settings));
    Ok(lines)
}

/// `--home`, `--detail` and `--icon`: where it starts, what it is for and its
/// icon, to list it in the avatar's «Aplicaciones». Only those given change
/// (`--no-home` takes it off the list).
async fn set_listing(pool: &PgPool, id: &ClientId, flags: &Flags) -> Result<(), String> {
    let changes = [
        ("home_url", flags.home.clone()),
        ("detail", flags.detail.clone().map(Some)),
        ("icon_url", flags.icon.clone().map(Some)),
    ];
    for (column, value) in changes {
        let Some(value) = value else { continue };
        // `column` is one of the three names above, never input.
        sqlx::query(&format!(
            "UPDATE oidc_clients SET {column} = $2 WHERE client_id = $1"
        ))
        .bind(id.as_str())
        .bind(value)
        .execute(pool)
        .await
        .map_err(|e| e.to_string())?;
    }
    Ok(())
}

/// The settings, one line each, for the operator.
fn describe(s: &ClientSettings) -> Vec<String> {
    let mut lines = vec![
        format!("token_alg: {}", s.token_alg.as_str()),
        format!(
            "pkce: {}",
            if s.pkce_required {
                "required"
            } else {
                "optional (code bound to the secret)"
            }
        ),
        format!(
            "required_policy: {}",
            s.required_policy.as_deref().unwrap_or("-")
        ),
    ];
    if s.client_credentials {
        lines.push(format!(
            "client_credentials: audiences {} (default {}), subject {}",
            s.audiences.join(", "),
            s.audiences.first().map_or("-", String::as_str),
            s.service_account
                .as_deref()
                .map_or("the client id".to_string(), |a| format!(
                    "service account {a}"
                ))
        ));
    }
    if s.token_alg == SigningAlg::ES256 {
        lines.push("note: the server needs OIDC_SIGNING_KEY_ES256 to sign its tokens".into());
    }
    lines
}
