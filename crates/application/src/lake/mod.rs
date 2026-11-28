//! The lake as the capability `lake` (decision 0078): two ports that name
//! no vendor. [`Lake`] is the data, spoken over the standard protocols (S3,
//! Iceberg REST, STS); [`LakeAdmin`] is the provisioning, whose one
//! implementation in the core is the client of the lake's adapter
//! (`lake-admin/1`). What a job may do is a [`LakeGrant`], never a policy.
//! [`LakeAdmins`] gives each lake's adapter (point 9: one per lake).

mod properties;
mod wire;

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
pub use contracts::{LakeGrant, LakeRead, LakeTable};

pub use properties::{CatalogPrefix, LakeProperties, UrlStyle};

/// Why a lake did not do what it was asked, as `lake-admin/1`'s error
/// codes say it. The message names the operation and the warehouse,
/// never a key or a policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LakeError {
    /// The request itself (a grant that does not check, a bad name).
    Invalid(String),
    /// The caller's certificate was refused.
    Refused(String),
    /// No such warehouse or pool.
    Unknown(String),
    /// The lake holds a state the request cannot reach.
    Conflict(String),
    /// The lake did not answer as expected; worth trying again.
    Lake(String),
    /// Not reachable now; worth trying again.
    Unavailable(String),
}

impl LakeError {
    /// Whether trying again later may succeed.
    pub fn retryable(&self) -> bool {
        matches!(self, Self::Lake(_) | Self::Unavailable(_))
    }
}

impl std::fmt::Display for LakeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (code, message) = match self {
            Self::Invalid(m) => ("invalid", m),
            Self::Refused(m) => ("refused", m),
            Self::Unknown(m) => ("unknown", m),
            Self::Conflict(m) => ("conflict", m),
            Self::Lake(m) => ("lake", m),
            Self::Unavailable(m) => ("unavailable", m),
        };
        write!(f, "{code}: {message}")
    }
}

impl std::error::Error for LakeError {}

/// A job's temporary credentials, issued by STS within a grant's session
/// policy. The secret and the token never show in `Debug`.
#[derive(Clone, PartialEq, Eq)]
pub struct LakeCredentials {
    pub access_key: String,
    pub secret_key: String,
    pub session_token: String,
    pub expires_at: SystemTime,
}

impl std::fmt::Debug for LakeCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LakeCredentials")
            .field("access_key", &self.access_key)
            .field("expires_at", &self.expires_at)
            .finish_non_exhaustive()
    }
}

/// Whether an object written once was written now.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteOnce {
    Written,
    /// It was there already (`If-None-Match: *` refused): left as it was.
    AlreadyThere,
}

/// Port: the lake's data, over its standard protocols.
#[async_trait]
pub trait Lake: Send + Sync {
    /// What the lake declares it supports.
    fn properties(&self) -> &LakeProperties;
    /// A job's credentials within `grant`, for at least `duration`;
    /// `session` names them in the lake's logs.
    async fn credentials(
        &self,
        grant: &LakeGrant,
        session: &str,
        duration: Duration,
    ) -> Result<LakeCredentials, LakeError>;
    /// Makes `namespace` unless it is there.
    async fn ensure_namespace(&self, warehouse: &str, namespace: &str) -> Result<(), LakeError>;
    /// The tables of `namespace`, by name; none if it is not there.
    async fn tables(&self, warehouse: &str, namespace: &str) -> Result<Vec<String>, LakeError>;
    /// The table's prefix in the warehouse, as the catalog says; `None` if
    /// there is no such table.
    async fn table_location(
        &self,
        warehouse: &str,
        namespace: &str,
        table: &str,
    ) -> Result<Option<String>, LakeError>;
    /// Drops the table with its files, and its namespace if left empty;
    /// `false` if there was no such table.
    async fn drop_table(
        &self,
        warehouse: &str,
        namespace: &str,
        table: &str,
    ) -> Result<bool, LakeError>;
    /// Writes `body` at `key` unless an object is there.
    async fn write_once(
        &self,
        warehouse: &str,
        key: &str,
        body: &[u8],
    ) -> Result<WriteOnce, LakeError>;
    /// The object at `key`, or `None` if there is none (a run's manifest,
    /// read back for a backfill).
    async fn read_object(&self, warehouse: &str, key: &str) -> Result<Option<Vec<u8>>, LakeError>;
}

/// How long a warehouse keeps what is written (0028's retention).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RetentionRules {
    /// Every write kept as a version.
    pub versioning: bool,
    /// Days a version cannot be deleted or shortened (object lock, in
    /// compliance mode); `None` for no lock.
    pub locked_days: Option<u32>,
    /// Days after which a noncurrent version expires; `None` never.
    pub noncurrent_expire_days: Option<u32>,
}

/// What making a warehouse came to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WarehouseMade {
    /// It was not there and was made.
    Made,
    /// It was there; nothing changed.
    Present,
    /// A step was refused or not reached: which, and what happened, in a
    /// few words, without a key.
    Failed { step: String, detail: String },
}

/// Where a warehouse is replicated: the replica lake, as the lake reaches
/// it, and whether the source lake is a pool of nodes (the lake works out
/// from it how its replication reaches the replica).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaTarget {
    pub endpoint: String,
    pub source_pool: bool,
}

/// A warehouse's replication, as the lake reports it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplicationState {
    NotConfigured,
    Configured,
    /// Configured in part: the step it stopped at, in a few words.
    Failed {
        step: String,
        detail: String,
    },
}

/// A warehouse's replication as it stands, with what waits to be copied now
/// and what failed to be copied in the last hour, when the lake says them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicationSync {
    pub state: ReplicationState,
    pub pending: Option<u64>,
    pub failed: Option<u64>,
}

/// What a managed access policy (0028) allows, on every warehouse.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ManagedAccess {
    /// List the warehouses and their objects, and read under a prefix.
    ReadUnder(String),
    /// Read every object.
    Read,
    /// Read and write every object.
    ReadWrite,
    /// Everything, the lake's administration included.
    Administer,
}

/// A managed access policy, by the name a person or role signs in with.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManagedPolicy {
    pub name: String,
    pub access: ManagedAccess,
}

/// A grant written in the lake's own policy language. Its content never
/// shows in `Debug` or in a log.
#[derive(Clone, PartialEq, Eq)]
pub struct SessionPolicy(pub String);

impl std::fmt::Debug for SessionPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SessionPolicy({} bytes)", self.0.len())
    }
}

/// The lake's own credentials (the user's ruling of 2026-11-23 on 0078's
/// Open 4); a job's are [`Lake::credentials`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LakeCredentialKind {
    Root,
    Admin,
    StsIssuer,
    /// A query engine's user (Trino's coordinator), limited to the
    /// installation's warehouses (decision 0091, ruling 50).
    QueryEngine,
}

impl LakeCredentialKind {
    /// Whether the keys are made outside the lake (read-only files the
    /// agent generated: the STS issuer's, a query engine's) and the lake
    /// takes them as they are: `held` is required and never replaced
    /// (decision 0091, ruling 46).
    pub fn keys_are_given(self) -> bool {
        matches!(self, Self::StsIssuer | Self::QueryEngine)
    }
}

/// A pair of the lake's own keys, kept sealed by the core (point 9). The
/// secret never shows in `Debug`.
#[derive(Clone, PartialEq, Eq)]
pub struct LakeKeys {
    pub access_key: String,
    pub secret_key: String,
}

impl std::fmt::Debug for LakeKeys {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("LakeKeys(…)")
    }
}

/// The lake's health, as far as it says it (0078's Open 7), asked with
/// the admin keys the core keeps for it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LakeHealth {
    /// No admin keys are kept for the lake: nothing was asked.
    NoKeys,
    /// The keys were taken; its drives, online and not.
    Answered {
        drives_online: u32,
        drives_offline: u32,
    },
    /// The lake refused the keys: its own error code.
    KeysRefused { code: String },
}

/// What Settings → Lake shows of a lake: its health, or that it could not
/// be reached, in a few words without a key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LakeProbe {
    NoKeys,
    Answered {
        drives_online: u32,
        drives_offline: u32,
    },
    KeysRefused {
        code: String,
    },
    Unreachable {
        detail: String,
    },
}

impl From<Result<LakeHealth, LakeError>> for LakeProbe {
    fn from(health: Result<LakeHealth, LakeError>) -> Self {
        match health {
            Ok(LakeHealth::NoKeys) => Self::NoKeys,
            Ok(LakeHealth::Answered {
                drives_online,
                drives_offline,
            }) => Self::Answered {
                drives_online,
                drives_offline,
            },
            Ok(LakeHealth::KeysRefused { code }) => Self::KeysRefused { code },
            Err(e) => Self::Unreachable {
                detail: e.to_string(),
            },
        }
    }
}

/// Port: the lake's provisioning. Every operation can run twice: run again
/// with the same input, it changes nothing.
#[async_trait]
pub trait LakeAdmin: Send + Sync {
    /// Makes the warehouse unless it is there, with `rules`.
    async fn ensure_warehouse(
        &self,
        name: &str,
        rules: &RetentionRules,
    ) -> Result<WarehouseMade, LakeError>;
    /// Replicates the warehouse into `replica`, made there unless it is.
    async fn replicate(
        &self,
        name: &str,
        replica: &ReplicaTarget,
    ) -> Result<ReplicationState, LakeError>;
    /// The warehouse's replication as it stands, and how far behind it is.
    async fn replication(&self, name: &str) -> Result<ReplicationSync, LakeError>;
    /// Sets the lake's managed policies to exactly `policies`.
    async fn set_managed_policies(&self, policies: &[ManagedPolicy]) -> Result<(), LakeError>;
    /// `grant` in the lake's policy language; changes nothing.
    async fn session_policy(&self, grant: &LakeGrant) -> Result<SessionPolicy, LakeError>;
    /// The lake's own keys of `kind`: `held` (the ones the core keeps) if
    /// the lake still takes them, else new ones made, for the core to seal.
    /// For a kind whose keys are given ([`LakeCredentialKind::keys_are_given`])
    /// the lake makes or updates its user with `held` and answers them;
    /// without `held` it is [`LakeError::Invalid`], and it never answers
    /// new ones (decision 0091, ruling 46).
    async fn ensure_credentials(
        &self,
        kind: LakeCredentialKind,
        held: Option<&LakeKeys>,
    ) -> Result<LakeKeys, LakeError>;
    /// The lake's health now.
    async fn health(&self) -> Result<LakeHealth, LakeError>;
}

/// Port: each lake's provisioning, by the lake and where the core finds
/// it: the main lake's address, or the replica's machine by its name.
pub trait LakeAdmins: Send + Sync {
    fn for_lake(&self, lake: &auth_gateway::Lake, node: &str) -> Arc<dyn LakeAdmin>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_secret_shows_in_debug() {
        let credentials = LakeCredentials {
            access_key: "AK".into(),
            secret_key: "the-secret".into(),
            session_token: "the-token".into(),
            expires_at: SystemTime::UNIX_EPOCH,
        };
        let keys = LakeKeys {
            access_key: "AK".into(),
            secret_key: "the-secret".into(),
        };
        let policy = SessionPolicy("{\"Statement\":[]}".into());
        let shown = format!("{credentials:?} {keys:?} {policy:?}");
        for secret in ["the-secret", "the-token", "Statement"] {
            assert!(!shown.contains(secret), "{shown}");
        }
        assert!(shown.contains("AK"));
    }

    #[test]
    fn a_lake_not_reached_is_shown_unreachable() {
        let refused = LakeHealth::KeysRefused { code: "c".into() };
        assert_eq!(
            LakeProbe::from(Ok(refused)),
            LakeProbe::KeysRefused { code: "c".into() }
        );
        let down = LakeProbe::from(Err(LakeError::Unavailable(
            "GET health: no connection".into(),
        )));
        let detail = "unavailable: GET health: no connection".to_string();
        assert_eq!(down, LakeProbe::Unreachable { detail });
    }

    #[test]
    fn only_the_lakes_own_trouble_is_worth_trying_again() {
        assert!(LakeError::Lake("x".into()).retryable());
        assert!(LakeError::Unavailable("x".into()).retryable());
        for e in [
            LakeError::Invalid("x".into()),
            LakeError::Refused("x".into()),
            LakeError::Unknown("x".into()),
            LakeError::Conflict("x".into()),
        ] {
            assert!(!e.retryable(), "{e}");
        }
        assert_eq!(
            LakeError::Unknown("warehouse w".into()).to_string(),
            "unknown: warehouse w"
        );
    }
}
