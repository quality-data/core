//! A dataset's tables in the lake (decisions 0017, 0037 and 0061): its
//! bronze table, as extracted, and its silver table, typed and normalized
//! from it; each in a namespace of the dataset's, named after the source's
//! extract the mapping reads.

use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::mapping::{FieldRole, Kind, Mapping, Semantic};

/// Silver's column with the entity's key normalized.
pub const ENTITY_KEY: &str = "_entity_key";
/// Silver's column saying whether the entity's key normalized.
pub const KEY_NORMALIZED: &str = "_entity_key_normalized";
/// Silver's column with the digest of what derived the row.
pub const DERIVED_BY: &str = "_mapping";

/// The derivation's version: raised whenever an engine's silver SQL for
/// the same derivation changes (each engine's snapshot test says so).
pub const DERIVATION_VERSION: u32 = 1;

/// How the entity's key is normalized, as every engine writes it.
const KEY_RULE: &str = "document: digits only, leading zeros trimmed; \
                        other: spaces trimmed, upper case; empty is NULL; joined by |";

/// What a field's source column is, as far as silver's typing tells them
/// apart: a number read as text drops its `.0`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceClass {
    Number,
    Other,
}

/// One field of a derivation, in the mapping's order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DerivedField {
    pub name: String,
    pub column: String,
    pub kind: Kind,
    pub source: SourceClass,
    pub document: bool,
}

/// What derives a dataset's silver from its bronze (decision 0090, point
/// 8.5), apart from any engine's SQL: silver's `_mapping` is its digest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SilverDerivation {
    pub version: u32,
    pub fields: Vec<DerivedField>,
    /// The fields holding the `entity` role, in the mapping's order.
    pub entity: Vec<String>,
    pub key: &'static str,
}

impl SilverDerivation {
    /// The mapping's derivation, `source` telling each column's class.
    pub fn of(mapping: &Mapping, source: impl Fn(&str) -> SourceClass) -> Self {
        let fields = mapping
            .fields
            .iter()
            .map(|(name, f)| DerivedField {
                name: name.clone(),
                column: f.value.column.clone(),
                kind: f.value.kind,
                source: source(&f.value.column),
                document: f.value.semantic == Some(Semantic::Document),
            })
            .collect();
        let entity = mapping
            .fields
            .iter()
            .filter(|(_, f)| f.value.roles.contains(&FieldRole::Entity))
            .map(|(name, _)| name.clone())
            .collect();
        Self {
            version: DERIVATION_VERSION,
            fields,
            entity,
            key: KEY_RULE,
        }
    }

    /// The lower-case hex SHA-256 of its canonical JSON: the fields in
    /// the order declared here, no spaces.
    pub fn digest(&self) -> String {
        let json = serde_json::to_string(self).expect("a derivation serializes");
        format!("{:x}", Sha256::digest(json.as_bytes()))
    }
}

/// The bronze table an extraction writes: `<dataset>_bronze.<source>`.
pub fn bronze(mapping: &Mapping) -> (String, String) {
    (
        format!("{}_bronze", mapping.dataset.value),
        mapping.source.value.clone(),
    )
}

/// The silver table derived from it, `<dataset>_silver.<source>`; none
/// when a field's name would take one of silver's own columns (`cut`, or
/// any starting with `_`), so the dataset is read from bronze (decision
/// 0061, point 2).
pub fn silver(mapping: &Mapping) -> Option<(String, String)> {
    let clash = mapping
        .fields
        .keys()
        .any(|f| f == "cut" || f.starts_with('_'));
    (!clash).then(|| {
        (
            format!("{}_silver", mapping.dataset.value),
            mapping.source.value.clone(),
        )
    })
}

/// A dataset's namespaces: bronze's and silver's.
pub fn namespaces(dataset: &str) -> [String; 2] {
    [format!("{dataset}_bronze"), format!("{dataset}_silver")]
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mapping(field: &str) -> Mapping {
        Mapping::parse(&format!(
            "dataset: loans\nsource: loans_2026\nfields:\n  {field}: {{ column: c1, type: text }}\n"
        ))
        .unwrap()
    }

    #[test]
    fn a_derivations_digest_is_its_canonical_jsons() {
        let m = Mapping::parse(
            "dataset: loans\nsource: loans_2026\nfields:\n  \
             Doc: { column: c1, type: text, semantic: document, roles: [entity] }\n  \
             Amount: { column: c2, type: number }\n",
        )
        .unwrap();
        let numeric = |c: &str| {
            if c == "c1" {
                SourceClass::Number
            } else {
                SourceClass::Other
            }
        };
        let d = SilverDerivation::of(&m, numeric);
        let json = serde_json::to_string(&d).unwrap();
        assert!(json.starts_with(
            "{\"version\":1,\"fields\":[{\"name\":\"Amount\",\"column\":\"c2\",\
             \"kind\":\"number\",\"source\":\"other\",\"document\":false},"
        ));
        assert!(json.contains("\"entity\":[\"Doc\"]"));
        let digest = d.digest();
        assert_eq!(digest.len(), 64);
        assert!(digest.chars().all(|c| matches!(c, '0'..='9' | 'a'..='f')));
        assert_eq!(digest, format!("{:x}", Sha256::digest(json.as_bytes())));
        let other = SilverDerivation::of(&m, |_| SourceClass::Other);
        assert_ne!(other.digest(), digest, "the source's class counts");
    }

    #[test]
    fn a_dataset_has_silver_unless_a_field_takes_its_columns() {
        let m = mapping("Documento");
        assert_eq!(bronze(&m), ("loans_bronze".into(), "loans_2026".into()));
        assert_eq!(
            silver(&m),
            Some(("loans_silver".into(), "loans_2026".into()))
        );
        assert_eq!(silver(&mapping("cut")), None);
        assert_eq!(silver(&mapping("_mapping")), None);
        assert_eq!(namespaces("loans"), ["loans_bronze", "loans_silver"]);
    }
}
