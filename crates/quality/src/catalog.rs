//! The versioned catalog as the quality core sees it (decision 0021): a
//! dataset's mapping and rules at a commit of `main`, which is what runs
//! read. Only the application talks to the catalog; a worker gets this
//! snapshot with its job and records the commit it evaluated.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

/// The catalog's database: one installation is one organisation (decision
/// 0076, point 5), so it is a fixed name, the one the main tenant's had.
pub const DATABASE: &str = "tenant_00000000000000000000000000000001";

/// What a catalog answers when the installation has none (no `DOLT_URL`):
/// the routes say `catalog_not_configured`, not `catalog_unavailable`.
pub const NOT_CONFIGURED: &str = "no catalog is configured";

/// A dataset's mapping and rules files at one commit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogSnapshot {
    pub commit: String,
    /// The mapping's YAML ([`crate::mapping::Mapping`]).
    pub mapping: String,
    pub rules: Vec<RulesSource>,
    /// The other datasets' mappings, what the rules' references read.
    pub others: Vec<DatasetMapping>,
}

/// A dataset's mapping, as YAML.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatasetMapping {
    pub dataset: String,
    pub yaml: String,
}

/// One rules file: its name in the catalog and its YAML.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RulesSource {
    pub file: String,
    pub yaml: String,
}

/// Reads the catalog's `main`.
#[async_trait]
pub trait Catalog: Send + Sync {
    /// The dataset's snapshot now; `None` if the catalog has no mapping for
    /// it.
    async fn snapshot(&self, dataset: &str) -> Result<Option<CatalogSnapshot>, String>;

    /// Every dataset's mapping now, by dataset; none if there is no catalog
    /// yet.
    async fn mappings(&self) -> Result<Vec<DatasetMapping>, String>;

    /// The dataset's snapshot at `commit` (a run's manifest's, decision
    /// 0090 step 22's backfill); `None` if it had no mapping then. A
    /// catalog that keeps no history cannot answer.
    async fn snapshot_at(
        &self,
        _dataset: &str,
        _commit: &str,
    ) -> Result<Option<CatalogSnapshot>, String> {
        Err("this catalog keeps no history".into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_catalog_keeps_the_main_tenants_database() {
        assert_eq!(DATABASE, "tenant_00000000000000000000000000000001");
    }
}
