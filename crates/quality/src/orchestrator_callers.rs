//! Who may call `/internal/quality/v1/*` (decision 0090, rulings 104 and
//! 109): besides a live machine token of the core's audience, the calling
//! machine client must be the machine client of the module that provides
//! `orchestration` (`<module>-machine`), or the machine client of a module
//! that requires it (ruling 47's gate: only such a module's client is
//! granted the `quality-jobs` audience). Both are read by whoever keeps
//! [`Provider`] up to date; until one is known, no caller passes.

use std::sync::{Arc, RwLock};

use crate::worker_api::{MachineCaller, MachineTokens};

/// The capability whose provider's machine client may call.
pub const ORCHESTRATION: &str = "orchestration";

/// The job runners' audience, which ruling 47 grants only to the machine
/// client of a module that requires [`ORCHESTRATION`].
pub const JOB_RUNNERS_AUDIENCE: &str = "quality-jobs";

/// Whether a module's registered client is the machine client of a module
/// that requires [`ORCHESTRATION`] (ruling 109, by ruling 47's gate): it has
/// machine tokens and the [`JOB_RUNNERS_AUDIENCE`].
pub fn is_requirers_client(client_credentials: bool, audiences: &[String]) -> bool {
    client_credentials && audiences.iter().any(|a| a == JOB_RUNNERS_AUDIENCE)
}

/// The machine clients that may call now: the [`ORCHESTRATION`]
/// provider's and those of the modules that require it.
#[derive(Debug, Clone, Default)]
pub struct Provider(Arc<RwLock<Allowed>>);

#[derive(Debug, Default)]
struct Allowed {
    provider: Option<String>,
    requirers: Vec<String>,
}

impl Provider {
    /// Takes the installed modules as the core keeps them (a list of
    /// `{name, provides, …}`): the first that provides orchestration.
    pub fn update(&self, modules: &serde_json::Value) {
        let found = provider_of(modules);
        if let Ok(mut kept) = self.0.write() {
            kept.provider = found;
        }
    }

    /// Takes the machine clients of the installed modules that require
    /// orchestration (ruling 109), replacing those taken before.
    pub fn update_requirers(&self, clients: Vec<String>) {
        if let Ok(mut kept) = self.0.write() {
            kept.requirers = clients;
        }
    }

    fn allows(&self, client: &str) -> bool {
        let Ok(kept) = self.0.read() else {
            return false;
        };
        kept.provider
            .as_ref()
            .is_some_and(|m| client == format!("{m}-machine"))
            || kept.requirers.iter().any(|c| c == client)
    }
}

/// The module of `modules` that provides [`ORCHESTRATION`].
pub fn provider_of(modules: &serde_json::Value) -> Option<String> {
    modules.as_array()?.iter().find_map(|m| {
        let provides = m.get("provides")?.as_array()?;
        provides
            .iter()
            .any(|p| p.as_str() == Some(ORCHESTRATION))
            .then(|| m.get("name")?.as_str().map(str::to_owned))?
    })
}

/// Machine tokens that pass only for the orchestration provider's client
/// and the clients of the modules that require orchestration.
pub struct OrchestratorOnly {
    pub tokens: Arc<dyn MachineTokens>,
    pub provider: Provider,
}

impl std::fmt::Debug for OrchestratorOnly {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OrchestratorOnly")
            .field("provider", &self.provider)
            .finish_non_exhaustive()
    }
}

impl MachineTokens for OrchestratorOnly {
    fn client(&self, token: &str) -> Option<MachineCaller> {
        let caller = self.tokens.client(token)?;
        if self.provider.allows(&caller.client) {
            Some(caller)
        } else {
            tracing::warn!(
                client = caller.client,
                "an internal quality route refused a client of no module that provides or requires orchestration"
            );
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    struct Any;
    impl MachineTokens for Any {
        fn client(&self, token: &str) -> Option<MachineCaller> {
            Some(MachineCaller {
                client: token.to_owned(),
                machine: None,
            })
        }
    }

    #[test]
    fn only_the_orchestration_providers_machine_client_passes() {
        let provider = Provider::default();
        let only = OrchestratorOnly {
            tokens: Arc::new(Any),
            provider: provider.clone(),
        };
        assert!(only.client("orchestrator-machine").is_none(), "none known");
        provider.update(&json!([
            { "name": "lake-rustfs", "provides": ["lake"] },
            { "name": "orchestrator", "provides": ["orchestration"] }
        ]));
        assert!(only.client("orchestrator-machine").is_some());
        assert!(only.client("lake-rustfs-machine").is_none());
        assert!(only.client("orchestrator").is_none(), "its people client");
        provider.update(&json!([]));
        assert!(only.client("orchestrator-machine").is_none(), "removed");
    }

    #[test]
    fn a_module_that_requires_orchestration_passes_with_its_machine_client() {
        let provider = Provider::default();
        let only = OrchestratorOnly {
            tokens: Arc::new(Any),
            provider: provider.clone(),
        };
        provider.update_requirers(vec!["engine-trino-machine".into()]);
        assert!(
            only.client("engine-trino-machine").is_some(),
            "no provider needed"
        );
        provider.update(&json!([
            { "name": "orchestrator", "provides": ["orchestration"] }
        ]));
        assert!(only.client("engine-trino-machine").is_some());
        assert!(only.client("orchestrator-machine").is_some());
        assert!(only.client("lake-rustfs-machine").is_none());
        provider.update_requirers(Vec::new());
        assert!(only.client("engine-trino-machine").is_none(), "removed");
        assert!(only.client("orchestrator-machine").is_some());
    }

    #[test]
    fn a_requirers_client_is_a_machine_client_with_the_job_runners_audience() {
        let jobs = vec!["core".to_owned(), JOB_RUNNERS_AUDIENCE.to_owned()];
        assert!(is_requirers_client(true, &jobs));
        assert!(!is_requirers_client(false, &jobs), "no machine tokens");
        assert!(!is_requirers_client(true, &["core".to_owned()]));
    }
}
