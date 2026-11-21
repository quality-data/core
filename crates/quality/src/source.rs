//! Sources (decision 0036): a source is a connection of a driver, the same
//! shape for every engine. The drivers are a catalog: each says its fields,
//! which of them are secret, and the engine that reads it. Pure: the store
//! seals the secrets, the worker connects.

use std::collections::BTreeMap;

/// The engine that reads a driver's sources.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Engine {
    /// DuckDB with the extension it loads.
    DuckDb { extension: &'static str },
    /// Trino with the connector of the source's catalog (decision 0029).
    Trino { connector: &'static str },
}

/// What a field holds, and so how it is kept and shown.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FieldKind {
    Text,
    Port,
    /// `true` or `false`; left out, `false`.
    Flag,
    /// Kept only sealed, in two versions; never sent back to a browser.
    Secret,
}

/// One field of a driver's form.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Field {
    pub key: &'static str,
    pub kind: FieldKind,
    pub required: bool,
}

/// A driver of the catalog.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Driver {
    pub id: &'static str,
    pub engine: Engine,
    pub default_port: Option<u16>,
    pub fields: &'static [Field],
}

const fn field(key: &'static str, kind: FieldKind, required: bool) -> Field {
    Field {
        key,
        kind,
        required,
    }
}

const HOST: Field = field("host", FieldKind::Text, true);
const PORT: Field = field("port", FieldKind::Port, false);
const DATABASE: Field = field("database", FieldKind::Text, true);
const USER: Field = field("user", FieldKind::Secret, true);
const PASSWORD: Field = field("password", FieldKind::Secret, true);
const OPTIONS: Field = field("options", FieldKind::Text, false);

/// Server, port, database, user, password and the driver's options.
const SERVER: &[Field] = &[HOST, PORT, DATABASE, USER, PASSWORD, OPTIONS];

/// SQL Server's: [`SERVER`] and whether its certificate is accepted
/// without being verified (off unless set: a self-signed server).
const SQLSERVER: &[Field] = &[
    HOST,
    PORT,
    DATABASE,
    USER,
    PASSWORD,
    OPTIONS,
    field("trust_server_certificate", FieldKind::Flag, false),
];

/// The drivers there are. A new engine is a new entry and its adapter.
pub const DRIVERS: &[Driver] = &[
    Driver {
        id: "postgres",
        engine: Engine::DuckDb {
            extension: "postgres",
        },
        default_port: Some(5432),
        fields: SERVER,
    },
    Driver {
        id: "mysql",
        engine: Engine::DuckDb { extension: "mysql" },
        default_port: Some(3306),
        fields: SERVER,
    },
    Driver {
        id: "sqlserver",
        engine: Engine::Trino {
            connector: "sqlserver",
        },
        default_port: Some(1433),
        fields: SQLSERVER,
    },
    Driver {
        id: "oracle",
        engine: Engine::Trino {
            connector: "oracle",
        },
        default_port: Some(1521),
        fields: &[
            HOST,
            PORT,
            field("service", FieldKind::Text, true),
            USER,
            PASSWORD,
            OPTIONS,
            // The Oracle schema whose tables it reads; left out, the
            // connected user (decision 0077 point 8).
            field("owner", FieldKind::Text, false),
        ],
    },
    Driver {
        id: "openedge",
        engine: Engine::Trino {
            connector: "openedge",
        },
        default_port: None,
        fields: SERVER,
    },
];

/// The driver `id`, if there is one.
pub fn driver(id: &str) -> Option<&'static Driver> {
    DRIVERS.iter().find(|d| d.id == id)
}

/// A source as an administrator writes it: the fields that are not secret
/// in `fields`, the secret ones in `secrets`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Source {
    pub name: String,
    pub driver: String,
    pub fields: BTreeMap<String, String>,
    pub secrets: BTreeMap<String, String>,
}

/// Why a source is refused; the field it is about, by key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SourceError {
    /// Not 1–63 of `a-z`, `0-9`, `_`, starting with a letter.
    Name,
    UnknownDriver,
    UnknownField(String),
    /// A secret field written in the clear, or a clear one as a secret.
    WrongPlace(String),
    Missing(String),
    Port(String),
    /// Empty, longer than [`MAX_VALUE`], or with control characters.
    Value(String),
}

/// The longest value a field may have.
pub const MAX_VALUE: usize = 1_000;

/// A source's name: what a mapping and a Trino catalog call it.
pub fn valid_name(name: &str) -> bool {
    let b = name.as_bytes();
    (1..=63).contains(&b.len())
        && b[0].is_ascii_lowercase()
        && b.iter()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || *c == b'_')
}

impl Source {
    /// Its driver, if the source holds together; every reason otherwise.
    pub fn validate(&self) -> Result<&'static Driver, Vec<SourceError>> {
        let mut errors = Vec::new();
        if !valid_name(&self.name) {
            errors.push(SourceError::Name);
        }
        let Some(driver) = driver(&self.driver) else {
            errors.push(SourceError::UnknownDriver);
            return Err(errors);
        };
        for (place, values) in [(false, &self.fields), (true, &self.secrets)] {
            for (key, value) in values {
                let Some(f) = driver.fields.iter().find(|f| f.key == key) else {
                    errors.push(SourceError::UnknownField(key.clone()));
                    continue;
                };
                if (f.kind == FieldKind::Secret) != place {
                    errors.push(SourceError::WrongPlace(key.clone()));
                } else if value.trim().is_empty()
                    || value.chars().count() > MAX_VALUE
                    || value.chars().any(char::is_control)
                {
                    errors.push(SourceError::Value(key.clone()));
                } else if f.kind == FieldKind::Port && !value.parse::<u16>().is_ok_and(|p| p > 0) {
                    errors.push(SourceError::Port(key.clone()));
                } else if f.kind == FieldKind::Flag && !matches!(value.as_str(), "true" | "false") {
                    errors.push(SourceError::Value(key.clone()));
                }
            }
        }
        for f in driver.fields.iter().filter(|f| f.required) {
            let values = if f.kind == FieldKind::Secret {
                &self.secrets
            } else {
                &self.fields
            };
            if !values.contains_key(f.key) {
                errors.push(SourceError::Missing(f.key.to_string()));
            }
        }
        if errors.is_empty() {
            Ok(driver)
        } else {
            Err(errors)
        }
    }

    /// Whether the flag `key` is set; left out, it is not.
    pub fn flag(&self, key: &str) -> bool {
        flag(&self.fields, key)
    }

    /// The port it connects to: its own, or the driver's.
    pub fn port(&self) -> Option<u16> {
        self.fields
            .get("port")
            .and_then(|p| p.parse().ok())
            .or_else(|| driver(&self.driver).and_then(|d| d.default_port))
    }
}

/// Whether the flag `key` is set in a source's `fields`; left out, it is
/// not.
pub fn flag(fields: &BTreeMap<String, String>, key: &str) -> bool {
    fields.get(key).is_some_and(|v| v == "true")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn map(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    fn postgres() -> Source {
        Source {
            name: "core".into(),
            driver: "postgres".into(),
            fields: map(&[("host", "db.example"), ("database", "core")]),
            secrets: map(&[("user", "reader"), ("password", "synthetic")]),
        }
    }

    #[test]
    fn every_driver_says_its_engine_and_keeps_its_credentials_secret() {
        for d in DRIVERS {
            for key in ["user", "password"] {
                let f = d.fields.iter().find(|f| f.key == key).unwrap();
                assert_eq!(f.kind, FieldKind::Secret, "{} {key}", d.id);
            }
        }
        assert_eq!(
            driver("postgres").unwrap().engine,
            Engine::DuckDb {
                extension: "postgres"
            }
        );
        assert!(matches!(
            driver("sqlserver").unwrap().engine,
            Engine::Trino { .. }
        ));
        assert!(driver("excel").is_none());
    }

    #[test]
    fn a_source_holds_together_with_its_drivers_fields() {
        let s = postgres();
        assert_eq!(s.validate().unwrap().id, "postgres");
        assert_eq!(s.port(), Some(5432));
        let mut own = s.clone();
        own.fields.insert("port".into(), "6432".into());
        assert_eq!(own.port(), Some(6432));
        // Oracle names a service, not a database.
        let mut oracle = s.clone();
        oracle.driver = "oracle".into();
        assert_eq!(
            oracle.validate().unwrap_err(),
            [
                SourceError::UnknownField("database".into()),
                SourceError::Missing("service".into())
            ]
        );
    }

    #[test]
    fn an_oracle_owner_and_a_trusted_certificate_are_optional() {
        let mut oracle = postgres();
        oracle.driver = "oracle".into();
        oracle.fields = map(&[("host", "db.example"), ("service", "FREEPDB1")]);
        assert_eq!(oracle.validate().unwrap().id, "oracle");
        oracle.fields.insert("owner".into(), "QS_APP".into());
        assert_eq!(oracle.validate().unwrap().id, "oracle");
        let mut sql = postgres();
        sql.driver = "sqlserver".into();
        assert!(!sql.flag("trust_server_certificate"), "off unless set");
        for (value, valid) in [("true", true), ("false", true), ("yes", false)] {
            sql.fields
                .insert("trust_server_certificate".into(), value.into());
            assert_eq!(sql.validate().is_ok(), valid, "{value}");
        }
        sql.fields
            .insert("trust_server_certificate".into(), "true".into());
        assert!(sql.flag("trust_server_certificate"));
        // Each driver's own: Postgres has neither.
        let mut pg = postgres();
        pg.fields.insert("owner".into(), "HR".into());
        pg.fields
            .insert("trust_server_certificate".into(), "true".into());
        assert_eq!(
            pg.validate().unwrap_err(),
            [
                SourceError::UnknownField("owner".into()),
                SourceError::UnknownField("trust_server_certificate".into())
            ]
        );
    }

    #[test]
    fn a_source_that_does_not_hold_together_says_every_reason() {
        let mut s = postgres();
        s.name = "Core-1".into();
        s.fields.insert("password".into(), "in the clear".into());
        s.fields.insert("port".into(), "70000".into());
        s.fields.insert("options".into(), "a\nb".into());
        s.secrets.remove("user");
        assert_eq!(
            s.validate().unwrap_err(),
            [
                SourceError::Name,
                SourceError::Value("options".into()),
                SourceError::WrongPlace("password".into()),
                SourceError::Port("port".into()),
                SourceError::Missing("user".into()),
            ]
        );
        s.driver = "excel".into();
        assert_eq!(
            s.validate().unwrap_err(),
            [SourceError::Name, SourceError::UnknownDriver]
        );
    }

    #[test]
    fn a_name_is_what_a_mapping_and_a_trino_catalog_can_call_it() {
        assert!(valid_name("core_2"));
        for bad in ["", "2core", "Core", "core-2", &"a".repeat(64)] {
            assert!(!valid_name(bad), "{bad}");
        }
    }
}
