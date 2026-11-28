//! The identity use cases: signing in (password, directory, OAuth),
//! sessions, accounts, password resets
//! and the audit trail they write. Wires the [`crate::domain`] ports together.

pub mod account_mail;
pub mod audit;
pub mod auth;
pub mod machines;
pub mod mail;
pub mod mail_notice;
pub mod oauth;
pub mod password_reset;
pub mod provider;
pub mod session;
pub mod users;

pub use audit::AuditService;
pub use auth::{
    BootstrapError, BootstrapOutcome, BootstrapService, DirectoryLogin, DirectorySlot, LoginError,
    LoginRequest, LoginService, DIRECTORY_USER_ROLE,
};
pub use mail::{MailLoader, MailSlot, Mailer, OutgoingMail};
pub use mail_notice::{MailNotice, MailTone};
pub use oauth::{BeginOutcome, CompleteOutcome, OAuthAuthenticatedAdmin, OAuthLoginService};
pub use password_reset::{
    AccessError, AdminAccess, PasswordResetService, ResetError, ACTIVATION_FORM_PATH,
    ACTIVATION_TTL, RESET_FORM_PATH,
};
pub use provider::{
    AuthorizationServer, AuthorizeOutcome, AuthorizeRequest, ClientChange, ModuleClients,
    ModuleClientsError, ModuleClientsRequest, TokenError, TokenRequest, TokenResponse, CORE_CLIENT,
    TRINO_AUDIENCE,
};
pub use session::{AuthenticatedSession, IssuedSession, SessionError, SessionService};
pub use users::{CreatedUser, NewUser, UserChanges, UsersError, UsersService, ASSIGNABLE_ROLES};
