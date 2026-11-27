//! Who may call `/internal/quality/v1/*` (decision 0090, ruling 104):
//! besides a live machine token of the core's audience, the calling
//! machine client must be the machine client of the module that provides
//! `orchestration` (`<module>-machine`). The provider is read from the
//! installed modules by whoever keeps [`Provider`] up to date; until it is
//! known, no caller passes.

use std::sync::{Arc, RwLock};

use crate::worker_api::{MachineCaller, MachineTokens};

/// The capability whose provider's machine client may call.
pub const ORCHESTRATION: &str = "orchestration";

/// The module that provides [`ORCHESTRATION`] now, if any.
#[derive(Debug, Clone, Default)]
pub struct Provider(Arc<RwLock<Option<String>>>);

impl Provider {
    /// Takes the installed modules as the core keeps them (a list of
    /// `{name, provides, …}`): the first that provides orchestration.
    pub fn update(&self, modules: &serde_json::Value) {
        let found = provider_of(modules);
        if let Ok(mut kept) = self.0.write() {
            *kept = found;
        }
    }

    fn client(&self) -> Option<String> {
        self.0.read().ok()?.as_ref().map(|m| format!("{m}-machine"))
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

/// Machine tokens that pass only for the orchestration provider's client.
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
        let allowed = self.provider.client()?;
        if caller.client == allowed {
            Some(caller)
        } else {
            tracing::warn!(
                client = caller.client,
                "an internal quality route refused a client that is not the orchestrator's"
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
}
