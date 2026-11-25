//! A dataset's mapping (decision 0020): its fields, each with the source
//! column it reads, its type in silver and its semantic type. The rules
//! name the fields; the mapping connects them to whatever the source calls
//! them, so the same rules serve another source with another mapping.
//! Its fields can take roles, which switch charts on (decision 0031).

use std::collections::{BTreeMap, BTreeSet};

use quality_rules::check::{FieldType, Schema};
use quality_rules::{At, ParseError, Position};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Mapping {
    /// The dataset, as its rules name it.
    pub dataset: At<String>,
    /// What the dataset holds, for whoever reads its rules.
    pub description: Option<String>,
    /// The source's extract the dataset reads (its bronze table).
    pub source: At<String>,
    /// Where an extraction reads it: a source by name and its table
    /// (decision 0036).
    pub from: Option<At<ReadFrom>>,
    /// The fields that identify a record.
    #[serde(default)]
    pub key: Vec<String>,
    /// Field → where it comes from and what it is.
    pub fields: BTreeMap<String, At<FieldMapping>>,
}

/// A source's table, as an extraction reads it.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ReadFrom {
    /// The source's name (Settings → Sources).
    pub source: String,
    /// `table`, `schema.table` or `database.schema.table`, as the source
    /// names it.
    pub table: String,
}

#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct FieldMapping {
    /// The source's column.
    pub column: String,
    /// What the field holds: the dataset's documentation, field by field.
    pub description: Option<String>,
    #[serde(rename = "type")]
    pub kind: Kind,
    pub semantic: Option<Semantic>,
    /// What the field is for in the charts (decision 0031): proposed by the
    /// map job, confirmed by a steward with the mapping.
    #[serde(default)]
    pub roles: Vec<FieldRole>,
    /// How much the field weighs in a record's ICD (decision 0057).
    #[serde(default)]
    pub priority: Priority,
    /// Its value loses validity over time (contact, income): timeliness
    /// counts in its ICD (decision 0057).
    #[serde(default)]
    pub expires: bool,
    /// On the field holding the `active` role, the values that make a
    /// record active, compared trimmed and in upper case (decision 0058).
    #[serde(default)]
    pub active_values: Vec<String>,
}

/// A field's priority: its weight in a record's ICD (decision 0057).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Priority {
    Critical,
    High,
    /// Until a steward sets one: every field weighs the same.
    #[default]
    Medium,
    Low,
}

impl Priority {
    /// Its weight: critical 4, high 3, medium 2, low 1.
    pub fn weight(self) -> u8 {
        match self {
            Self::Critical => 4,
            Self::High => 3,
            Self::Medium => 2,
            Self::Low => 1,
        }
    }
}

/// A field's role, which switches charts on (decision 0031).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum FieldRole {
    /// Part of the key of the entity the dataset describes (decision 0022).
    Entity,
    /// A grouping dimension: office, advisor, product, channel.
    Dimension,
    /// The segment: client type, loan type. One field.
    Segment,
    /// The date of the last update. One field, a date.
    Updated,
    /// The amount outliers are sought in. One field, a number.
    OutlierAmount,
    /// A variable whose drift between cuts is measured; a number.
    Drift,
    /// Takes part in the SFC indicator: a cell (decision 0058).
    Sfc,
    /// Says whether a record is active, by its `active_values`: the SFC
    /// indicator's population. One field, text.
    Active,
}

impl FieldRole {
    /// Its name in a mapping and in a chart's needs.
    pub fn name(self) -> &'static str {
        match self {
            Self::Entity => "entity",
            Self::Dimension => "dimension",
            Self::Segment => "segment",
            Self::Updated => "updated",
            Self::OutlierAmount => "outlier_amount",
            Self::Drift => "drift",
            Self::Sfc => "sfc",
            Self::Active => "active",
        }
    }

    /// Held by one field at most.
    fn single(self) -> bool {
        matches!(
            self,
            Self::Segment | Self::Updated | Self::OutlierAmount | Self::Active
        )
    }

    /// The type its field must have, if one.
    fn kind(self) -> Option<Kind> {
        match self {
            Self::Updated => Some(Kind::Date),
            Self::OutlierAmount | Self::Drift => Some(Kind::Number),
            Self::Active => Some(Kind::Text),
            _ => None,
        }
    }
}

/// The field's type in silver: what its values are compared as.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Kind {
    /// Codes are text, so their leading zeros survive (decision 0033).
    Text,
    Number,
    Date,
    Boolean,
}

/// What the values mean, inferred by the map job and confirmed by people.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Semantic {
    Document,
    Email,
    Mobile,
    Amount,
    Code,
    FreeText,
    Flag,
}

/// Something in a mapping that does not hold together.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MappingError {
    Parse(ParseError),
    /// A key field that is not among the fields.
    UnknownKey {
        field: String,
    },
    /// Two fields read the same column.
    SameColumn {
        column: String,
        at: Position,
    },
    /// `from` names no valid source or table.
    ReadFrom {
        at: Position,
    },
    /// A role one field at most may hold, held by a second.
    OneField {
        role: FieldRole,
        field: String,
        at: Position,
    },
    /// A role on a field of a type it cannot have.
    RoleType {
        role: FieldRole,
        field: String,
        at: Position,
    },
    /// The `active` field without `active_values`, or `active_values` on
    /// another field.
    ActiveValues {
        field: String,
        at: Position,
    },
}

impl Mapping {
    pub fn parse(yaml: &str) -> Result<Self, MappingError> {
        let mapping: Self = serde_saphyr::from_str(yaml).map_err(|e| {
            MappingError::Parse(ParseError {
                at: e.location().map(|l| Position {
                    line: l.line(),
                    column: l.column(),
                }),
                message: e.to_string().lines().next().unwrap_or_default().to_string(),
            })
        })?;
        mapping.check()?;
        Ok(mapping)
    }

    fn check(&self) -> Result<(), MappingError> {
        if let Some(field) = self.key.iter().find(|k| !self.fields.contains_key(*k)) {
            return Err(MappingError::UnknownKey {
                field: field.clone(),
            });
        }
        if let Some(from) = self.from.as_ref().filter(|f| !f.value.is_valid()) {
            return Err(MappingError::ReadFrom { at: from.at });
        }
        let mut seen = BTreeMap::new();
        for f in self.fields.values() {
            if seen.insert(f.value.column.as_str(), ()).is_some() {
                return Err(MappingError::SameColumn {
                    column: f.value.column.clone(),
                    at: f.at,
                });
            }
        }
        self.check_roles()
    }

    fn check_roles(&self) -> Result<(), MappingError> {
        let mut held = BTreeSet::new();
        for (name, f) in &self.fields {
            for &role in &f.value.roles {
                let error = |e: fn(FieldRole, String, Position) -> MappingError| {
                    Err(e(role, name.clone(), f.at))
                };
                if role.kind().is_some_and(|k| k != f.value.kind) {
                    return error(|role, field, at| MappingError::RoleType { role, field, at });
                }
                if role.single() && !held.insert(role) {
                    return error(|role, field, at| MappingError::OneField { role, field, at });
                }
            }
            let values = &f.value.active_values;
            let wrong = if f.value.roles.contains(&FieldRole::Active) {
                values.is_empty() || values.iter().any(|v| v.trim().is_empty())
            } else {
                !values.is_empty()
            };
            if wrong {
                return Err(MappingError::ActiveValues {
                    field: name.clone(),
                    at: f.at,
                });
            }
        }
        Ok(())
    }

    /// The roles its fields hold.
    pub fn roles(&self) -> BTreeSet<FieldRole> {
        self.fields
            .values()
            .flat_map(|f| f.value.roles.iter().copied())
            .collect()
    }

    /// The JSON Schema of a mapping file, for editors.
    pub fn json_schema() -> schemars::Schema {
        let mut schema = schemars::schema_for!(Mapping);
        schema.insert("title".into(), "Quality Data dataset mapping".into());
        schema
    }

    /// The fields and types the rules compile against.
    pub fn schema(&self) -> Schema {
        let fields = self
            .fields
            .iter()
            .map(|(name, f)| {
                let t = match (f.value.kind, f.value.semantic) {
                    (Kind::Text, Some(Semantic::Email)) => FieldType::Email,
                    (Kind::Text, _) => FieldType::Text,
                    (Kind::Number, _) => FieldType::Number,
                    (Kind::Date, _) => FieldType::Date,
                    (Kind::Boolean, _) => FieldType::Boolean,
                };
                (name.clone(), t)
            })
            .collect();
        Schema { fields }
    }
}

impl ReadFrom {
    /// The table's schema (all before its last `.`, if any) and its name.
    pub fn schema_and_table(&self) -> (Option<&str>, &str) {
        match self.table.rsplit_once('.') {
            Some((schema, table)) => (Some(schema), table),
            None => (None, &self.table),
        }
    }

    /// A source's name, and one to three parts of a table's name.
    pub fn is_valid(&self) -> bool {
        let parts: Vec<&str> = self.table.split('.').collect();
        crate::source::valid_name(&self.source)
            && parts.len() <= 3
            && parts.iter().all(|p| {
                !p.is_empty()
                    && p.len() <= 128
                    && p.bytes()
                        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'$' | b'#'))
            })
    }
}
