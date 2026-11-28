//! The authentication gateway: signing people in — with a password, the
//! company directory or an external OAuth/OIDC provider — and keeping their
//! sessions. Embedded in each installation's server today; the same crate is
//! meant to run on its own as the cloud's identity provider.
//!
//! Layered like the rest of the backend: [`domain`] holds the model and the
//! ports, which `infrastructure` implements.

pub mod app;
pub mod domain;
pub mod http;

pub use self::app::{
    AccessError, AdminAccess, AuditService, AuthenticatedSession, BeginOutcome, BootstrapError,
    BootstrapOutcome, BootstrapService, CompleteOutcome, CreatedUser, DirectoryLogin,
    DirectorySlot, IssuedSession, LoginError, LoginRequest, LoginService, MailLoader, MailNotice,
    MailSlot, MailTone, Mailer, NewUser, OAuthAuthenticatedAdmin, OAuthLoginService, OutgoingMail,
    PasswordResetService, ResetError, SessionError, SessionService, UserChanges, UsersError,
    UsersService, ACTIVATION_FORM_PATH, ACTIVATION_TTL, ASSIGNABLE_ROLES, DIRECTORY_USER_ROLE,
    RESET_FORM_PATH,
};
pub use self::app::{
    AuthorizationServer, AuthorizeOutcome, AuthorizeRequest, ClientChange, ModuleClients,
    ModuleClientsError, ModuleClientsRequest, TokenError, TokenRequest, TokenResponse, CORE_CLIENT,
};
pub use self::domain::{
    is_join_token, CertificateSigner, CertificateTerm, JobSubject, JobUserKind, JoinTokenMint,
    Machine, MachineChange, MachineId, MachineRegistry, MachineRole, MachineState, NewJoinToken,
    RedeemRefusal, SignedCertificate, SigningRefusal, JOIN_TOKEN_LEN, JOIN_TOKEN_PREFIX,
};
pub use self::domain::{
    AccountPolicies, ActionHolder, AuthorizationCode, AuthorizationCodeStore, ClientAuth, ClientId,
    ClientIdError, ClientSettings, ListedApp, ModuleClient, OidcClient, OidcClientRegistry,
    PreviousSecret, RoleClaim, RoleValue, SigningAlg, TokenSigner,
};
pub use self::domain::{
    AccountStatus, AccountSummary, AdminAccount, AdminRepository, AuthorizeParams, CsrfToken,
    DirectoryAccounts, DirectoryAuthenticator, DirectoryError, DirectoryUser, ExchangeRequest,
    IpLockoutStore, LinkedIdentity, LockoutPolicy, LockoutState, NewAdmin, OAuthError,
    OAuthIdentity, OAuthIdentityRepository, OAuthProvider, OAuthSecretGenerator, PasswordHash,
    PasswordHashError, PasswordHasher, PasswordPolicy, PasswordPolicyError, PasswordRequirement,
    PasswordResetStore, PendingAuthStore, PendingAuthorization, PkcePair, ProfilePicture,
    ProviderId, ResetTokenGenerator, ServicePrincipal, ServiceTokens, Session, SessionPolicy,
    SessionRepository, SessionToken, SessionTokenGenerator,
};
pub use self::domain::{
    AlertChange, AlertKey, AlertSeverity, FiringAlert, MachineAlertStore, ALERTING_HEARTBEAT,
};
pub use self::domain::{
    CertificateReader, MachineCertificate, MachineSessions, MACHINE_ORGANIZATION,
};
pub use self::domain::{
    InstallFile, InstallFiles, JoinGrant, JoinTokenCheck, Lake, LakeRootKeyStore, LakeRootKeys,
    MAIN_POOL,
};
pub use self::domain::{LakeAdminKeyStore, LakeAdminKeys, LakeAdminKeysKept};
pub use self::domain::{
    LakePool, LakePoolStore, PoolLayout, PoolRefusal, MAX_POOL_DRIVES, MAX_POOL_NODES,
    MIN_POOL_DRIVES, MIN_POOL_NODES, REPLICATION_ALIAS, RUSTFS_PEER_PORT,
};
pub use self::domain::{
    Line, MachineCharts, MachineSeries, SeriesRange, SeriesUnavailable, SeriesWindow,
};
pub use self::domain::{
    ReplicationOutcome, ReplicationStateStore, ReplicationStep, WarehouseState, Warehouses,
    REPLICATION_PROXY_PORT, REPLICA_RETENTION_DAYS, SOURCE_NONCURRENT_DAYS,
};
