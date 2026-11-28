//! `application::lake::Lake` over the standard clients (decision 0078,
//! step 8): STS for a job's credentials, the Iceberg REST catalog for
//! namespaces and tables, S3 for objects written once and a table's files.
//! What each call may do is a grant; its session policy is the main lake's
//! adapter's ([`SessionPolicies`]). Names no vendor.

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use application::lake::{
    Lake, LakeCredentials, LakeError, LakeGrant, LakeProperties, LakeTable, WriteOnce,
};
use async_trait::async_trait;

use crate::lake_catalog::TableLookup;
use crate::lake_objects::ManifestWriter;
use crate::lake_policies::SessionPolicies;
use crate::lake_sts::StsIssuer;

/// The lake, over its standard protocols.
#[derive(Clone)]
pub struct StandardLake {
    issuer: StsIssuer,
    catalog: TableLookup,
    objects: ManifestWriter,
    policies: Arc<SessionPolicies>,
}

impl std::fmt::Debug for StandardLake {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StandardLake")
            .field("endpoint", &self.issuer.endpoint())
            .finish_non_exhaustive()
    }
}

impl StandardLake {
    pub fn new(issuer: StsIssuer, policies: Arc<SessionPolicies>) -> Self {
        Self {
            catalog: TableLookup::new(issuer.clone()),
            objects: ManifestWriter::new(issuer.clone()),
            issuer,
            policies,
        }
    }

    async fn policy(&self, grant: &LakeGrant) -> Result<String, LakeError> {
        Ok(self.policies.policy(grant).await?.0)
    }
}

/// A standard client's error, as the port says it: the lake's.
fn lake(e: String) -> LakeError {
    LakeError::Lake(e)
}

/// The prefix a key is written under: its first segment (`manifests/`).
fn under(key: &str) -> Option<String> {
    let (first, rest) = key.split_once('/')?;
    (!first.is_empty() && !rest.is_empty()).then(|| format!("{first}/"))
}

#[async_trait]
impl Lake for StandardLake {
    fn properties(&self) -> &LakeProperties {
        self.issuer.properties()
    }

    async fn credentials(
        &self,
        grant: &LakeGrant,
        session: &str,
        duration: Duration,
    ) -> Result<LakeCredentials, LakeError> {
        let policy = self.policy(grant).await?;
        let c = self
            .issuer
            .assume_role(session, &policy, duration)
            .await
            .map_err(|e| lake(e.to_string()))?;
        Ok(LakeCredentials {
            access_key: c.access_key,
            secret_key: c.secret_key,
            session_token: c.session_token,
            expires_at: SystemTime::from(c.expires_at),
        })
    }

    async fn ensure_namespace(&self, warehouse: &str, namespace: &str) -> Result<(), LakeError> {
        let grant = LakeGrant {
            make_namespaces: true,
            ..LakeGrant::on(warehouse)
        };
        let policy = self.policy(&grant).await?;
        self.catalog
            .ensure_namespace(warehouse, namespace, &policy)
            .await
            .map_err(lake)
    }

    async fn tables(&self, warehouse: &str, namespace: &str) -> Result<Vec<String>, LakeError> {
        let policy = self.policy(&LakeGrant::read_only(warehouse)).await?;
        self.catalog
            .tables(warehouse, namespace, &policy)
            .await
            .map_err(lake)
    }

    async fn table_location(
        &self,
        warehouse: &str,
        namespace: &str,
        table: &str,
    ) -> Result<Option<String>, LakeError> {
        let policy = self.policy(&LakeGrant::read_only(warehouse)).await?;
        self.catalog
            .prefix(warehouse, namespace, table, &policy)
            .await
            .map_err(lake)
    }

    async fn drop_table(
        &self,
        warehouse: &str,
        namespace: &str,
        table: &str,
    ) -> Result<bool, LakeError> {
        let Some(location) = self.table_location(warehouse, namespace, table).await? else {
            return Ok(false);
        };
        let grant = LakeGrant {
            drop_tables: vec![LakeTable {
                namespace: namespace.into(),
                name: table.into(),
                location: location.clone(),
            }],
            ..LakeGrant::on(warehouse)
        };
        let policy = self.policy(&grant).await?;
        self.catalog
            .drop(warehouse, (namespace, table), &location, &policy)
            .await
            .map(|()| true)
            .map_err(lake)
    }

    async fn write_once(
        &self,
        warehouse: &str,
        key: &str,
        body: &[u8],
    ) -> Result<WriteOnce, LakeError> {
        let prefix = under(key)
            .ok_or_else(|| LakeError::Invalid(format!("write {key}: not under a prefix")))?;
        let grant = LakeGrant {
            write_objects_under: Some(prefix),
            ..LakeGrant::on(warehouse)
        };
        let policy = self.policy(&grant).await?;
        let written = self
            .objects
            .put_once(warehouse, key, body.to_vec(), &policy)
            .await
            .map_err(lake)?;
        Ok(if written {
            WriteOnce::Written
        } else {
            WriteOnce::AlreadyThere
        })
    }

    async fn read_object(&self, warehouse: &str, key: &str) -> Result<Option<Vec<u8>>, LakeError> {
        let policy = self.policy(&LakeGrant::read_only(warehouse)).await?;
        self.objects
            .get(warehouse, key, &policy)
            .await
            .map_err(lake)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_object_is_written_under_its_first_segment() {
        assert_eq!(
            under("manifests/2026-11-24.json").as_deref(),
            Some("manifests/")
        );
        assert_eq!(under("top.json"), None);
        assert_eq!(under("/x"), None);
        assert_eq!(under("manifests/"), None);
    }
}
