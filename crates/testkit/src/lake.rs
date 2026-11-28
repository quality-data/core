//! A lake in memory, for tests (decision 0078, step 2): both ports,
//! `application::lake::{Lake, LakeAdmin}`, over maps, with no network. It
//! keeps what each operation would leave in a lake, so a test can check
//! that running an operation twice changes nothing.

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::time::{Duration, SystemTime};

use application::lake::{
    CatalogPrefix, Lake, LakeAdmin, LakeCredentialKind, LakeCredentials, LakeError, LakeGrant,
    LakeHealth, LakeKeys, LakeProperties, ManagedPolicy, ReplicaTarget, ReplicationState,
    ReplicationSync, RetentionRules, SessionPolicy, UrlStyle, WarehouseMade, WriteOnce,
};
use async_trait::async_trait;

/// What the fake lake holds.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct LakeState {
    pub warehouses: BTreeMap<String, RetentionRules>,
    /// `(warehouse, namespace)` to its tables, by name, and their prefix.
    pub tables: BTreeMap<(String, String), BTreeMap<String, String>>,
    pub objects: BTreeMap<(String, String), Vec<u8>>,
    pub replicas: BTreeMap<String, ReplicaTarget>,
    pub managed: Vec<ManagedPolicy>,
    pub keys: BTreeMap<String, String>,
    /// How many credentials and keys were made, for the next ones' names.
    pub issued: u32,
}

/// The lake in memory. Its properties are RustFS's as verified.
#[derive(Debug)]
pub struct InMemoryLake {
    properties: LakeProperties,
    pub state: Mutex<LakeState>,
}

impl Default for InMemoryLake {
    fn default() -> Self {
        Self {
            properties: LakeProperties {
                catalog_uri: "https://lake.test/iceberg".into(),
                catalog_prefix: CatalogPrefix::FromConfig,
                s3_endpoint: "https://lake.test".into(),
                region: "us-east-1".into(),
                url_style: UrlStyle::Path,
                catalog_sigv4_service: "s3".into(),
                sts_role: None,
                sts_min_duration: Duration::from_secs(900),
                vends_credentials: false,
                drop_purges_files: false,
            },
            state: Mutex::default(),
        }
    }
}

impl InMemoryLake {
    /// A copy of what it holds now.
    pub fn snapshot(&self) -> LakeState {
        self.state.lock().unwrap().clone()
    }

    fn known(&self, warehouse: &str) -> Result<std::sync::MutexGuard<'_, LakeState>, LakeError> {
        let state = self.state.lock().unwrap();
        if state.warehouses.contains_key(warehouse) {
            Ok(state)
        } else {
            Err(LakeError::Unknown(format!("warehouse {warehouse}")))
        }
    }
}

fn checked(grant: &LakeGrant) -> Result<(), LakeError> {
    grant
        .check()
        .map_err(|e| LakeError::Invalid(format!("grant on {}: {e}", grant.warehouse)))
}

#[async_trait]
impl Lake for InMemoryLake {
    fn properties(&self) -> &LakeProperties {
        &self.properties
    }

    async fn credentials(
        &self,
        grant: &LakeGrant,
        session: &str,
        duration: Duration,
    ) -> Result<LakeCredentials, LakeError> {
        checked(grant)?;
        let mut state = self.known(&grant.warehouse)?;
        state.issued += 1;
        let duration = self.properties.credentials_duration(duration);
        Ok(LakeCredentials {
            access_key: format!("job-{session}-{}", state.issued),
            secret_key: format!("secret-{}", state.issued),
            session_token: format!("token-{}", state.issued),
            expires_at: SystemTime::now() + duration,
        })
    }

    async fn ensure_namespace(&self, warehouse: &str, namespace: &str) -> Result<(), LakeError> {
        let mut state = self.known(warehouse)?;
        let key = (warehouse.to_string(), namespace.to_string());
        state.tables.entry(key).or_default();
        Ok(())
    }

    async fn tables(&self, warehouse: &str, namespace: &str) -> Result<Vec<String>, LakeError> {
        let state = self.known(warehouse)?;
        let key = (warehouse.to_string(), namespace.to_string());
        Ok(state
            .tables
            .get(&key)
            .map(|t| t.keys().cloned().collect())
            .unwrap_or_default())
    }

    async fn table_location(
        &self,
        warehouse: &str,
        namespace: &str,
        table: &str,
    ) -> Result<Option<String>, LakeError> {
        let state = self.known(warehouse)?;
        let key = (warehouse.to_string(), namespace.to_string());
        Ok(state.tables.get(&key).and_then(|t| t.get(table)).cloned())
    }

    async fn drop_table(
        &self,
        warehouse: &str,
        namespace: &str,
        table: &str,
    ) -> Result<bool, LakeError> {
        let mut state = self.known(warehouse)?;
        let key = (warehouse.to_string(), namespace.to_string());
        let Some(location) = state.tables.get_mut(&key).and_then(|t| t.remove(table)) else {
            return Ok(false);
        };
        let prefix = format!("{location}/");
        state
            .objects
            .retain(|(w, k), _| w != warehouse || !k.starts_with(&prefix));
        if state.tables.get(&key).is_some_and(BTreeMap::is_empty) {
            state.tables.remove(&key);
        }
        Ok(true)
    }

    async fn write_once(
        &self,
        warehouse: &str,
        key: &str,
        body: &[u8],
    ) -> Result<WriteOnce, LakeError> {
        let mut state = self.known(warehouse)?;
        let at = (warehouse.to_string(), key.to_string());
        if state.objects.contains_key(&at) {
            return Ok(WriteOnce::AlreadyThere);
        }
        state.objects.insert(at, body.to_vec());
        Ok(WriteOnce::Written)
    }

    async fn read_object(&self, warehouse: &str, key: &str) -> Result<Option<Vec<u8>>, LakeError> {
        let state = self.known(warehouse)?;
        Ok(state
            .objects
            .get(&(warehouse.to_string(), key.to_string()))
            .cloned())
    }
}

#[async_trait]
impl LakeAdmin for InMemoryLake {
    async fn ensure_warehouse(
        &self,
        name: &str,
        rules: &RetentionRules,
    ) -> Result<WarehouseMade, LakeError> {
        let mut state = self.state.lock().unwrap();
        let was = state.warehouses.insert(name.to_string(), rules.clone());
        Ok(match was {
            None => WarehouseMade::Made,
            Some(_) => WarehouseMade::Present,
        })
    }

    async fn replicate(
        &self,
        name: &str,
        replica: &ReplicaTarget,
    ) -> Result<ReplicationState, LakeError> {
        let mut state = self.known(name)?;
        state.replicas.insert(name.to_string(), replica.clone());
        Ok(ReplicationState::Configured)
    }

    /// Its state; nothing waits or fails in memory.
    async fn replication(&self, name: &str) -> Result<ReplicationSync, LakeError> {
        let state = self.known(name)?;
        let state = if state.replicas.contains_key(name) {
            ReplicationState::Configured
        } else {
            ReplicationState::NotConfigured
        };
        Ok(ReplicationSync {
            state,
            pending: Some(0),
            failed: Some(0),
        })
    }

    async fn set_managed_policies(&self, policies: &[ManagedPolicy]) -> Result<(), LakeError> {
        self.state.lock().unwrap().managed = policies.to_vec();
        Ok(())
    }

    async fn session_policy(&self, grant: &LakeGrant) -> Result<SessionPolicy, LakeError> {
        checked(grant)?;
        Ok(SessionPolicy(format!("{grant:?}")))
    }

    async fn ensure_credentials(
        &self,
        kind: LakeCredentialKind,
        held: Option<&LakeKeys>,
    ) -> Result<LakeKeys, LakeError> {
        let mut state = self.state.lock().unwrap();
        if kind.keys_are_given() {
            let keys = held.ok_or_else(|| {
                LakeError::Invalid(format!("{kind:?}: its keys are given, none came"))
            })?;
            state
                .keys
                .insert(keys.access_key.clone(), keys.secret_key.clone());
            return Ok(keys.clone());
        }
        if let Some(keys) = held {
            if state.keys.get(&keys.access_key) == Some(&keys.secret_key) {
                return Ok(keys.clone());
            }
        }
        state.issued += 1;
        let keys = LakeKeys {
            access_key: format!("{kind:?}-{}", state.issued).to_lowercase(),
            secret_key: format!("secret-{}", state.issued),
        };
        state
            .keys
            .insert(keys.access_key.clone(), keys.secret_key.clone());
        Ok(keys)
    }

    async fn health(&self) -> Result<LakeHealth, LakeError> {
        Ok(LakeHealth::Answered {
            drives_online: 4,
            drives_offline: 0,
        })
    }
}

impl InMemoryLake {
    /// A table made as the catalog would, at `tables/<n>`: for tests that
    /// need one before the code under test reads it.
    pub fn add_table(&self, warehouse: &str, namespace: &str, table: &str) -> String {
        let mut state = self.state.lock().unwrap();
        state.issued += 1;
        let location = format!("tables/{:08}", state.issued);
        let key = (warehouse.to_string(), namespace.to_string());
        state
            .tables
            .entry(key)
            .or_default()
            .insert(table.to_string(), location.clone());
        location
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const WH: &str = "qd-tenant-00000000000000000000000000000001";

    async fn with_warehouse() -> InMemoryLake {
        let lake = InMemoryLake::default();
        lake.ensure_warehouse(WH, &RetentionRules::default())
            .await
            .unwrap();
        lake
    }

    #[tokio::test]
    async fn provisioning_twice_changes_nothing() {
        let lake = with_warehouse().await;
        let replica = ReplicaTarget {
            endpoint: "https://replica.test".into(),
            source_pool: false,
        };
        lake.replicate(WH, &replica).await.unwrap();
        let once = lake.snapshot();
        lake.ensure_warehouse(WH, &RetentionRules::default())
            .await
            .unwrap();
        lake.replicate(WH, &replica).await.unwrap();
        assert_eq!(lake.snapshot(), once);
        assert_eq!(
            lake.ensure_warehouse(WH, &RetentionRules::default()).await,
            Ok(WarehouseMade::Present)
        );
        assert_eq!(
            lake.replication(WH).await.unwrap().state,
            ReplicationState::Configured
        );
    }

    #[tokio::test]
    async fn a_namespace_twice_and_an_object_written_once() {
        let lake = with_warehouse().await;
        lake.ensure_namespace(WH, "clients_bronze").await.unwrap();
        lake.ensure_namespace(WH, "clients_bronze").await.unwrap();
        assert!(lake.tables(WH, "clients_bronze").await.unwrap().is_empty());
        let key = "manifests/2026-11-23.json";
        assert_eq!(
            lake.write_once(WH, key, b"one").await.unwrap(),
            WriteOnce::Written
        );
        assert_eq!(
            lake.write_once(WH, key, b"two").await.unwrap(),
            WriteOnce::AlreadyThere
        );
        let kept = lake.snapshot().objects[&(WH.to_string(), key.to_string())].clone();
        assert_eq!(kept, b"one");
    }

    #[tokio::test]
    async fn a_drop_takes_the_files_and_the_empty_namespace() {
        let lake = with_warehouse().await;
        let location = lake.add_table(WH, "clients_bronze", "clients");
        let file = format!("{location}/data/part-0.parquet");
        lake.write_once(WH, &file, b"rows").await.unwrap();
        assert_eq!(
            lake.table_location(WH, "clients_bronze", "clients")
                .await
                .unwrap(),
            Some(location)
        );
        assert!(lake
            .drop_table(WH, "clients_bronze", "clients")
            .await
            .unwrap());
        assert!(!lake
            .drop_table(WH, "clients_bronze", "clients")
            .await
            .unwrap());
        let state = lake.snapshot();
        assert!(state.objects.is_empty() && state.tables.is_empty());
    }

    #[tokio::test]
    async fn credentials_need_a_grant_that_checks_and_a_warehouse() {
        let lake = with_warehouse().await;
        let minute = Duration::from_secs(60);
        let issued = lake
            .credentials(&LakeGrant::read_only(WH), "job-1", minute)
            .await
            .unwrap();
        assert!(issued.expires_at >= SystemTime::now() + Duration::from_secs(890));
        let bad = lake
            .credentials(&LakeGrant::read_only("Bad*"), "j", minute)
            .await;
        assert!(matches!(bad, Err(LakeError::Invalid(_))));
        let other = "qd-tenant-other";
        let unknown = lake
            .credentials(&LakeGrant::read_only(other), "j", minute)
            .await;
        assert!(matches!(unknown, Err(LakeError::Unknown(_))));
    }

    #[tokio::test]
    async fn the_lakes_own_keys_are_kept_while_the_lake_takes_them() {
        let lake = InMemoryLake::default();
        let admin = LakeCredentialKind::Admin;
        let made = lake.ensure_credentials(admin, None).await.unwrap();
        assert_eq!(
            lake.ensure_credentials(admin, Some(&made)).await.unwrap(),
            made
        );
        let stale = LakeKeys {
            access_key: made.access_key.clone(),
            secret_key: "not-it".into(),
        };
        assert_ne!(
            lake.ensure_credentials(admin, Some(&stale)).await.unwrap(),
            made
        );
    }
}
