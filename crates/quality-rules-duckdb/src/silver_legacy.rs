//! The digest silver's rows carried before decision 0090 step 7: the
//! SHA-256 of DuckDB's own derivation text, as the string builder of that
//! time wrote it. Frozen: it never changes with the derivation's SQL, and
//! it goes once no kept row carries it.

use quality::layers::{SilverDerivation, SourceClass, ENTITY_KEY, KEY_NORMALIZED};
use quality::mapping::Kind;
use sha2::{Digest, Sha256};

use crate::{quoted, Error};

/// The derivation's text as it was written over the `source` view.
fn legacy_select(d: &SilverDerivation, source: &str) -> Result<String, Error> {
    let mut select = Vec::new();
    for f in &d.fields {
        let c = quoted(&f.column)?;
        let value = match f.kind {
            Kind::Text if f.source == SourceClass::Number => {
                format!("regexp_replace(CAST({c} AS VARCHAR), '\\.0$', '')")
            }
            Kind::Text => format!("CAST({c} AS VARCHAR)"),
            Kind::Number => format!("TRY_CAST({c} AS DOUBLE)"),
            Kind::Date => format!("TRY_CAST({c} AS DATE)"),
            Kind::Boolean => format!("TRY_CAST({c} AS BOOLEAN)"),
        };
        select.push(format!("{value} AS {}", quoted(&f.name)?));
    }
    let typed = format!("SELECT {} FROM {}", select.join(", "), quoted(source)?);
    if d.entity.is_empty() {
        return Ok(format!("SELECT * FROM ({typed})"));
    }
    let (mut parts, mut present) = (Vec::new(), Vec::new());
    for name in &d.entity {
        let document = d.fields.iter().any(|f| &f.name == name && f.document);
        let v = format!("CAST({} AS VARCHAR)", quoted(name)?);
        parts.push(if document {
            format!("NULLIF(ltrim(regexp_replace({v}, '[^0-9]', '', 'g'), '0'), '')")
        } else {
            format!("NULLIF(upper(trim({v})), '')")
        });
        present.push(format!("NULLIF(trim({v}), '') IS NOT NULL"));
    }
    let key = parts.join(" || '|' || ");
    Ok(format!(
        "SELECT *, {key} AS {}, CASE WHEN ({key}) IS NOT NULL THEN TRUE WHEN {} THEN FALSE END AS {} \
         FROM ({typed})",
        quoted(ENTITY_KEY)?,
        present.join(" OR "),
        quoted(KEY_NORMALIZED)?,
    ))
}

/// What `_mapping` held for this derivation over `source`
/// before the digest moved to the derivation itself.
pub(crate) fn legacy_digest(d: &SilverDerivation, source: &str) -> Result<String, Error> {
    let select = legacy_select(d, source)?;
    Ok(format!("{:x}", Sha256::digest(select.as_bytes())))
}
