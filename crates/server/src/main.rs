//! Composition root — the only place that knows every layer.
//!
//! The binary's commands, dispatched from the first argument:
//!
//!   * `serve` (the default when no argument is given) — boot the HTTP server
//!     (see [`serve`]).
//!   * `bootstrap-admin` — create the first administrator from environment
//!     secrets, so no password is ever committed to the repository.
//!   * `recover-admin` — after restoring a backup on a new server.
//!   * `admin-access` — the administrator's link to choose a password, from
//!     the server (see [`admin_access`], decision 0086).
//!   * `audit verify` — every tenant's audit chain recomputed; exits 1 on a
//!     break (see [`audit`], decision 0070).
//!   * `ca-init` — make the installation's CA, once (decision 0066).
//!   * `app-machine` — the application VM's machine and its first
//!     certificate, signed on the spot (decision 0066, point 16).
//!   * `updater-certificate` — the update agent's machine and its
//!     certificate for the internal entry point (decision 0003).
//!   * `oidc-keygen`, `oidc-client`, `oidc-client-update` — the gateway as an
//!     OIDC provider: its keys and the applications that use it.
//!   * `timezone [set NAME]` — the platform's time zone (decision 0079).
//!   * `openapi` — print the OpenAPI spec.
//!   * `quality rules lint | explain | tidy` — the rule language on local
//!     files, against their mappings (see [`quality`]).
//!   * `quality catalog import` — a dataset's mapping and rules into the
//!     tenant's catalog (see [`quality_catalog`]).

mod admin_access;
mod app_machine;
mod audit;
mod background;
mod callers;
mod catalog_wiring;
mod commands;
mod config;
mod database;
mod drain_wiring;
mod job_compiler;
mod machine_gauge;
mod machines_wiring;
mod metrics_wiring;
mod modules;
mod quality;
mod quality_catalog;
mod replication_wiring;
mod serve;
mod serve_services;
mod settings_wiring;
mod startup;
mod timezone;
mod trino_tokens;
mod updater_certificate;
mod updates_wiring;

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        // Surface the error's Display — a clear, operator-facing message such as
        // "DATABASE_URL is required and must be non-empty" — rather than the
        // Debug form the `Termination` default would print. A missing or
        // malformed secret must fail fast with a message an operator can act on,
        // and exit non-zero so an orchestrator sees the failure.
        eprintln!("server: {error}");
        std::process::exit(1);
    }
}

/// Dispatch the CLI command. Any `Err` is rendered by [`main`] as a clear,
/// operator-facing message before exiting non-zero.
async fn run() -> Result<(), Box<dyn std::error::Error>> {
    match std::env::args().nth(1).as_deref() {
        Some("serve") | None => serve::serve().await,
        Some("bootstrap-admin") => commands::bootstrap_admin().await,
        Some("recover-admin") => commands::recover_admin().await,
        Some("admin-access") => admin_access::run().await,
        Some("audit") => audit::run().await,
        Some("ca-init") => commands::ca_init(),
        Some("app-machine") => app_machine::run().await,
        Some("updater-certificate") => updater_certificate::run().await,
        Some("oidc-keygen") => commands::oidc_keygen(),
        Some("oidc-client") => commands::oidc_client().await,
        Some("oidc-client-update") => commands::oidc_client_update().await,
        Some("timezone") => timezone::run().await,
        // Print the OpenAPI spec to stdout and exit. Needs no database; the
        // monorepo's `gen:api` script pipes this into the TypeScript client
        // generator.
        Some("quality") if std::env::args().nth(2).as_deref() == Some("catalog") => {
            quality_catalog::run().await
        }
        Some("quality") => quality::run(),
        Some("openapi") => {
            // The API's document, with every module's routes for people.
            let doc = modules::registry().openapi(api::ApiDoc::document());
            println!("{}", doc.to_pretty_json()?);
            Ok(())
        }
        Some(other) => {
            eprintln!(
                "server: unknown command {other:?}\n\n\
                 Commands:\n  \
                 serve            Run the HTTP server (default)\n  \
                 bootstrap-admin  Create the first admin from \
                 ADMIN_BOOTSTRAP_EMAIL / ADMIN_BOOTSTRAP_PASSWORD\n  \
                 recover-admin    Recover the admin after restoring a backup\n  \
                 admin-access     The administrator's link to choose a password: \
                 --email ADDRESS [--force]\n  \
                 audit verify     Verify the audit chain: [--tenant ID] [--lang es|en] \
                 (exits 1 on a break)\n  \
                 ca-init          Make the installation's CA: --name INSTALLATION \
                 --cert PATH --key PATH (prints its SHA-256)\n  \
                 app-machine      The application VM's machine and its first \
                 certificate: --name NAME --dir PATH (the node agent's)\n  \
                 updater-certificate  The update agent's certificate: --name NAME \
                 --dir PATH (/etc/quality-data/internal)\n  \
                 oidc-keygen      Print a new OIDC_SIGNING_KEY \
                 (--es256: OIDC_SIGNING_KEY_ES256)\n  \
                 oidc-client      Register an application: ID NAME REDIRECT_URI... \
                 [--public] [--alg ES256] [--no-pkce] [--require-policy NAME] \
                 [--client-credentials --audience A [--service-account ID] \
                 [--machine-role ROLE: no secret, its machines' certificates]]\n  \
                 oidc-client-update  Change an application: ID [the same options]\n  \
                 timezone         Print the platform's time zone; set NAME: set it \
                 (an IANA name)\n  \
                 openapi          Print the OpenAPI spec (for client generation)\n  \
                 quality rules lint|explain FILE... --mapping FILE... [--type FIELD=email] \
                 [--lang es|en]\n  \
                 quality catalog import --mapping FILE RULES... [--tenant ID] [--author \"Name <email>\"]"
            );
            Err("unknown command".into())
        }
    }
}

/// Install the logs (`logging`, decision 0026): JSON or `LOG_FORMAT=pretty`,
/// filtered by `RUST_LOG` (default `info`). Called once at the start of a
/// long-running command; the `openapi` subcommand deliberately skips it so
/// its stdout carries only the spec.
fn init_tracing() {
    logging::init("server");
}
