//! Bronze and silver loaded append-only (decision 0090, point 5): every
//! extraction is a new `_load` (its job id) in the `(cut, _load)`
//! partition, never a delete and insert; readers take the cut's accepted
//! load, pinned to its snapshot. Silver keeps the bronze load it was
//! derived from and the derivation's digest (`_mapping`, point 8.5).
//! Existing tables evolve to `(cut, _load)` and are optimized once, so a
//! superseded load's delete drops whole files (T3).

use quality::layers::{SourceClass, DERIVED_BY, ENTITY_KEY, KEY_NORMALIZED};
use quality::mapping::{FieldRole, Kind, Mapping};
use quality_rules_sql::{quote_text, Cut};

use crate::extract::bronze_type;
use crate::icd::{key, with_role};
use crate::read::{pinned, typed_select, Pinned, TablePath};
use crate::{quoted, Error, Statement};

/// The load column every bronze and silver row carries.
pub const LOAD: &str = "_load";

/// The cut's accepted load: its id, or `None` for rows written before
/// loads existed (`_load` `NULL`, read as the cut's load until replaced).
#[derive(Debug, Clone, Copy)]
pub struct Accepted<'a> {
    pub cut: &'a Cut,
    pub load: Option<&'a str>,
}

fn plain(sql: String) -> Statement {
    Statement { sql }
}

fn table(t: &TablePath) -> Result<String, Error> {
    Ok(format!(
        "{}.{}.{}",
        quoted(&t.catalog)?,
        quoted(&t.namespace)?,
        quoted(&t.table)?
    ))
}

/// The accepted load of a cut at its recorded snapshot, as a relation.
pub fn accepted(p: &Pinned, a: &Accepted<'_>) -> Result<String, Error> {
    accepted_in(&pinned(p)?, a)
}

/// The accepted load of a cut in `from`, a pinned table (`pinned_by`).
pub fn accepted_in(from: &str, a: &Accepted<'_>) -> Result<String, Error> {
    let load = match a.load {
        Some(l) => format!("{LOAD} = {}", quote_text(l)?),
        None => format!("{LOAD} IS NULL"),
    };
    Ok(format!(
        "(SELECT * FROM {from} WHERE cut = {} AND {load})",
        a.cut.literal()
    ))
}

/// A field's silver type.
fn silver_type(kind: Kind) -> &'static str {
    match kind {
        Kind::Text => "VARCHAR",
        Kind::Number => "DOUBLE",
        Kind::Date => "DATE",
        Kind::Boolean => "BOOLEAN",
    }
}

/// Silver's columns but `cut` and `_load`, in the derivation's order.
fn silver_columns(mapping: &Mapping) -> Vec<(String, &'static str)> {
    let mut cols: Vec<(String, &'static str)> = mapping
        .fields
        .iter()
        .map(|(n, f)| (n.clone(), silver_type(f.value.kind)))
        .collect();
    if !with_role(mapping, FieldRole::Entity).is_empty() {
        cols.push((ENTITY_KEY.into(), "VARCHAR"));
        cols.push((KEY_NORMALIZED.into(), "BOOLEAN"));
    }
    cols.push((DERIVED_BY.into(), "VARCHAR"));
    cols
}

/// A new silver table, partitioned by `(cut, _load)` from the start.
pub fn create_silver(t: &TablePath, mapping: &Mapping) -> Result<Statement, Error> {
    let mut defs = Vec::new();
    for (n, ty) in silver_columns(mapping) {
        defs.push(format!("{} {ty}", quoted(&n)?));
    }
    defs.push(format!("cut DATE, {LOAD} VARCHAR"));
    Ok(plain(format!(
        "CREATE TABLE IF NOT EXISTS {} ({}) WITH (partitioning = ARRAY['cut', '{LOAD}'])",
        table(t)?,
        defs.join(", ")
    )))
}

/// A `prepare` job's statements (decision 0090, rulings 66 and 68): the
/// dataset's bronze table (each mapped column, typed by `bronze_type` of
/// the type Trino lists it with in `source_columns`) and its silver table,
/// each created if missing, then every column added if missing, so a
/// mapping whose columns changed is prepared again. Each can run twice.
pub fn prepare(
    bronze: &TablePath,
    silver: &TablePath,
    mapping: &Mapping,
    source_columns: &[(String, String)],
) -> Result<Vec<Statement>, Error> {
    let mut bronze_cols: Vec<(String, String)> = Vec::new();
    for f in mapping.fields.values() {
        let c = &f.value.column;
        if bronze_cols.iter().any(|(n, _)| n == c) {
            continue;
        }
        let (_, ty) = source_columns
            .iter()
            .find(|(n, _)| n == c)
            .ok_or_else(|| Error::UnknownField(c.clone()))?;
        bronze_cols.push((c.clone(), bronze_type(ty).0));
    }
    let silver_cols: Vec<(String, String)> = silver_columns(mapping)
        .into_iter()
        .map(|(n, t)| (n, t.to_string()))
        .collect();
    let mut out = Vec::new();
    for (t, cols) in [(bronze, bronze_cols), (silver, silver_cols)] {
        let mut defs = Vec::new();
        for (n, ty) in &cols {
            defs.push(format!("{} {ty}", quoted(n)?));
        }
        defs.push(format!("cut DATE, {LOAD} VARCHAR"));
        let at = table(t)?;
        out.push(plain(format!(
            "CREATE TABLE IF NOT EXISTS {at} ({}) WITH (partitioning = ARRAY['cut', '{LOAD}'])",
            defs.join(", ")
        )));
        for (n, ty) in &cols {
            out.push(plain(format!(
                "ALTER TABLE {at} ADD COLUMN IF NOT EXISTS {} {ty}",
                quoted(n)?
            )));
        }
    }
    Ok(out)
}

/// The silver derivation of one bronze load: every field typed as mapped,
/// the entity's key normalized and whether it did, the derivation's
/// `digest`, the cut and the bronze load it came from. One `INSERT`.
pub fn derive_silver(
    silver: &TablePath,
    mapping: &Mapping,
    bronze: (&Pinned, &Accepted<'_>),
    load: &str,
    digest: &str,
    source: impl Fn(&str) -> SourceClass,
) -> Result<Statement, Error> {
    let from = accepted(bronze.0, bronze.1)?;
    let typed = typed_select(mapping, &format!("{from} b"), source)?;
    let entity = with_role(mapping, FieldRole::Entity);
    let mut extra = Vec::new();
    if !entity.is_empty() {
        let k = key(mapping, &entity)?;
        let present: Vec<String> = entity
            .iter()
            .map(|f| {
                Ok(format!(
                    "NULLIF(trim(BOTH concat(' ', chr(160)) FROM CAST({} AS VARCHAR)), '') IS NOT NULL",
                    quoted(f)?
                ))
            })
            .collect::<Result<_, Error>>()?;
        extra.push(k.clone());
        extra.push(format!(
            "CASE WHEN ({k}) IS NOT NULL THEN TRUE WHEN {} THEN FALSE END",
            present.join(" OR ")
        ));
    }
    extra.push(quote_text(digest)?);
    extra.push(bronze.1.cut.literal());
    extra.push(quote_text(load)?);
    let mut names = Vec::new();
    for (n, _) in silver_columns(mapping) {
        names.push(quoted(&n)?);
    }
    names.push(format!("cut, {LOAD}"));
    let fields: Vec<String> = mapping
        .fields
        .keys()
        .map(|f| quoted(f))
        .collect::<Result<_, _>>()?;
    Ok(plain(format!(
        "INSERT INTO {} ({}) SELECT {}, {} FROM ({typed}) s",
        table(silver)?,
        names.join(", "),
        fields.join(", "),
        extra.join(", ")
    )))
}

/// An existing bronze or silver table moved to loads, in order: the
/// column, the partitioning, and one `optimize` that rewrites the old
/// spec's files (T3: until then a load's delete writes delete files).
/// Each can run twice.
pub fn migrate_to_loads(t: &TablePath) -> Result<Vec<Statement>, Error> {
    let t = table(t)?;
    Ok(vec![
        plain(format!(
            "ALTER TABLE {t} ADD COLUMN IF NOT EXISTS {LOAD} VARCHAR"
        )),
        plain(format!(
            "ALTER TABLE {t} SET PROPERTIES partitioning = ARRAY['cut', '{LOAD}']"
        )),
        plain(format!("ALTER TABLE {t} EXECUTE optimize")),
    ])
}

/// The loads a cut has, from `"t$partitions"`: a superseded one is deleted
/// only when listed here (RustFS refuses an empty delete).
pub fn loads_of_cut(t: &TablePath, cut: &Cut) -> Result<Statement, Error> {
    let parts = TablePath {
        table: format!("{}$partitions", t.table),
        ..t.clone()
    };
    Ok(plain(format!(
        "SELECT partition.{LOAD} FROM {} WHERE partition.cut = {}",
        table(&parts)?,
        cut.literal()
    )))
}

/// A load's rows of a cut, counted: an extract's retry deletes its own
/// earlier attempt's rows only after this showed some (decision 0090,
/// ruling 67).
pub fn load_rows(t: &TablePath, cut: &Cut, load: &str) -> Result<Statement, Error> {
    Ok(plain(format!(
        "SELECT count(*) FROM {} WHERE cut = {} AND {LOAD} = {}",
        table(t)?,
        cut.literal(),
        quote_text(load)?
    )))
}

/// A table's columns and their types as Trino declares them, in order:
/// what an extract reports of bronze (ruling 64).
pub fn table_columns(t: &TablePath) -> Result<Statement, Error> {
    Ok(plain(format!(
        "SELECT column_name, data_type FROM {}.\"information_schema\".\"columns\" \
         WHERE table_schema = {} AND table_name = {} ORDER BY ordinal_position",
        quoted(&t.catalog)?,
        quote_text(&t.namespace)?,
        quote_text(&t.table)?
    )))
}

/// A superseded load's rows, by partition only: whole files go.
pub fn delete_load(t: &TablePath, cut: &Cut, load: &str) -> Result<Statement, Error> {
    Ok(plain(format!(
        "DELETE FROM {} WHERE cut = {} AND {LOAD} = {}",
        table(t)?,
        cut.literal(),
        quote_text(load)?
    )))
}
