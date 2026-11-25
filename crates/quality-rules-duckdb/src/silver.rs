//! Silver, written (decision 0061): a dataset's cut typed and normalized
//! from its bronze cut, by the very statement a read of bronze through the
//! mapping runs ([`Runner::map`]'s fields, `keys.rs`'s entity key), so the
//! two cannot differ. Each row carries the digest of that statement
//! (`_mapping`): a read takes silver only when the digest is the one its
//! own mapping gives. Only counts and snapshot ids come out.

use std::collections::BTreeMap;

use quality::layers::{
    self, SilverDerivation, SourceClass, DERIVED_BY, ENTITY_KEY, KEY_NORMALIZED,
};
use quality::mapping::{FieldRole, Kind, Mapping};
use quality_rules_compile::FieldType;
use quality_rules_sql::quote_text;

use crate::keys::entity_key;
use crate::lake::SilverAt;
use crate::silver_legacy::legacy_digest;
use crate::{field_type, invalid, quoted, Error, Runner};

/// A field's silver type, as DuckDB names it.
fn duck_type(kind: Kind) -> &'static str {
    match kind {
        Kind::Text => "VARCHAR",
        Kind::Number => "DOUBLE",
        Kind::Date => "DATE",
        Kind::Boolean => "BOOLEAN",
    }
}

fn entity_fields(mapping: &Mapping) -> Vec<&String> {
    mapping
        .fields
        .iter()
        .filter(|(_, f)| f.value.roles.contains(&FieldRole::Entity))
        .map(|(name, _)| name)
        .collect()
}

/// Silver's columns but `cut`, in the order the derivation gives them,
/// with their types: the fields, the entity's key if there is one, and
/// the digest.
fn columns(mapping: &Mapping) -> Vec<(String, &'static str)> {
    let mut columns: Vec<_> = mapping
        .fields
        .iter()
        .map(|(name, f)| (name.clone(), duck_type(f.value.kind)))
        .collect();
    if !entity_fields(mapping).is_empty() {
        columns.push((ENTITY_KEY.into(), "VARCHAR"));
        columns.push((KEY_NORMALIZED.into(), "BOOLEAN"));
    }
    columns.push((DERIVED_BY.into(), "VARCHAR"));
    columns
}

fn lake_table((namespace, table): (&str, &str)) -> Result<String, Error> {
    Ok(format!("lake.{}.{}", quoted(namespace)?, quoted(table)?))
}

/// How a silver table stands against a mapping.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Fit {
    /// Every column is there with its type, or these are missing.
    Missing(Vec<(String, &'static str)>),
    /// A column has another type: the table cannot take the mapping.
    Conflict,
}

impl Runner {
    /// The rows of the dataset's silver, from its `source` view: each field
    /// typed as mapped and, with an entity, its key normalized and whether
    /// it did (`NULL` when its fields are empty).
    pub(crate) fn silver_select(&self, mapping: &Mapping, source: &str) -> Result<String, Error> {
        let typed = self.typed_select(mapping, source)?;
        let Some(key) = entity_key(mapping)? else {
            return Ok(format!("SELECT * FROM ({typed})"));
        };
        let mut present = Vec::new();
        for field in entity_fields(mapping) {
            present.push(format!(
                "NULLIF(trim(CAST({} AS VARCHAR)), '') IS NOT NULL",
                quoted(field)?
            ));
        }
        Ok(format!(
            "SELECT *, {key} AS {}, CASE WHEN ({key}) IS NOT NULL THEN TRUE WHEN {} THEN FALSE END AS {} \
             FROM ({typed})",
            quoted(ENTITY_KEY)?,
            present.join(" OR "),
            quoted(KEY_NORMALIZED)?,
        ))
    }

    /// The mapping's derivation over its `source` view, whose digest is
    /// what silver's `_mapping` holds (decision 0090, point 8.5).
    pub(crate) fn derivation(
        &self,
        mapping: &Mapping,
        source: &str,
    ) -> Result<SilverDerivation, Error> {
        let types = self.column_types(source)?;
        Ok(SilverDerivation::of(mapping, |column| {
            match types.get(column).map(|t| field_type(t.as_str())) {
                Some(FieldType::Number) => SourceClass::Number,
                _ => SourceClass::Other,
            }
        }))
    }

    /// Creates the silver table with the mapping's columns and `cut`,
    /// partitioned by `cut` (decision 0061, point 2).
    pub fn create_silver(&self, silver: (&str, &str), mapping: &Mapping) -> Result<(), Error> {
        let mut defs = Vec::new();
        for (name, ty) in columns(mapping) {
            defs.push(format!("{} {ty}", quoted(&name)?));
        }
        defs.push("cut DATE".into());
        self.con.execute_batch(&format!(
            "CREATE TABLE {} ({}) PARTITIONED BY (cut);",
            lake_table(silver)?,
            defs.join(", ")
        ))
    }

    /// How `table` (`catalog.namespace.table`'s parts) stands against the
    /// mapping. Its columns come from `DESCRIBE`, which reads the table
    /// itself: `duckdb_columns()` lists none for an attached Iceberg
    /// catalog's tables, so every column looked missing (found end to end
    /// on 2026-11-10).
    pub(crate) fn fit(
        &self,
        (database, namespace, table): (&str, &str, &str),
        mapping: &Mapping,
    ) -> Result<Fit, Error> {
        let mut stmt = self.con.prepare(&format!(
            "SELECT column_name, column_type FROM (DESCRIBE {}.{}.{})",
            quoted(database)?,
            quoted(namespace)?,
            quoted(table)?
        ))?;
        let there: BTreeMap<String, String> = stmt
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?
            .collect::<Result<_, _>>()?;
        let mut missing = Vec::new();
        for (name, ty) in columns(mapping) {
            match there.get(&name) {
                None => missing.push((name, ty)),
                Some(t) if !t.eq_ignore_ascii_case(ty) => return Ok(Fit::Conflict),
                Some(_) => {}
            }
        }
        Ok(Fit::Missing(missing))
    }

    /// Replaces the cut in silver with bronze's at `bronze_snapshot`, in one
    /// transaction, adding the columns a new field needs first: the rows
    /// written and silver's snapshot after the commit; `None`, writing
    /// nothing, when a field's type changed (decision 0061, point 5).
    pub fn replace_silver(
        &self,
        mapping: &Mapping,
        silver: (&str, &str),
        cut: &str,
        bronze_snapshot: &str,
    ) -> Result<Option<(i64, String)>, Error> {
        let snapshot = bronze_snapshot.parse::<i64>().map_err(invalid)?;
        let source = self.bronze_cut(mapping, cut, Some(snapshot))?.0;
        let target = lake_table(silver)?;
        let Fit::Missing(missing) = self.fit(("lake", silver.0, silver.1), mapping)? else {
            return Ok(None);
        };
        for (name, ty) in missing {
            self.con.execute_batch(&format!(
                "ALTER TABLE {target} ADD COLUMN {} {ty};",
                quoted(&name)?
            ))?;
        }
        let select = self.silver_select(mapping, &source)?;
        let digest = quote_text(&self.derivation(mapping, &source)?.digest()).map_err(invalid)?;
        let mut names = Vec::new();
        for (name, _) in columns(mapping) {
            names.push(quoted(&name)?);
        }
        let cut = quote_text(cut).map_err(invalid)?;
        self.con.execute_batch(&format!(
            "BEGIN;\n\
             DELETE FROM {target} WHERE cut = CAST({cut} AS DATE);\n\
             INSERT INTO {target} ({}, cut) SELECT *, {digest}, CAST({cut} AS DATE) FROM ({select});\n\
             COMMIT;",
            names.join(", ")
        ))?;
        let rows: i64 = self.con.query_row(
            &format!("SELECT count(*) FROM {target} WHERE cut = CAST({cut} AS DATE)"),
            [],
            |r| r.get(0),
        )?;
        Ok(Some((rows, self.latest_snapshot(&target)?.to_string())))
    }

    /// The mapping's fields of the cut in silver, at `at`, if silver holds
    /// the cut under this mapping: it has rows there, all derived by the
    /// statement the mapping gives over bronze's `source` view (decision
    /// 0061, point 4). The select, the table and the snapshot; `None`
    /// otherwise, and the cut is read from bronze.
    pub(crate) fn silver_cut(
        &self,
        mapping: &Mapping,
        cut: &str,
        source: &str,
        at: SilverAt,
    ) -> Result<Option<(String, String, i64)>, Error> {
        let Some((namespace, table)) = layers::silver(mapping) else {
            return Ok(None);
        };
        if !self.table_exists(&namespace, &table)? {
            return Ok(None);
        }
        let table = lake_table((&namespace, &table))?;
        let snapshot = match at {
            SilverAt::Pinned(id) => id,
            // A table created and never written has no snapshot.
            SilverAt::Current => match self.latest_snapshot(&table) {
                Ok(id) => id,
                Err(Error::QueryReturnedNoRows) => return Ok(None),
                Err(e) => return Err(e),
            },
        };
        let derivation = self.derivation(mapping, source)?;
        let digests = [derivation.digest(), legacy_digest(&derivation, source)?];
        let from = format!(
            "{table} AT (VERSION => {snapshot}) WHERE cut = CAST({} AS DATE)",
            quote_text(cut).map_err(invalid)?
        );
        if !self.holds(&from, &digests)? {
            return Ok(None);
        }
        let mut fields = Vec::new();
        for field in mapping.fields.keys() {
            fields.push(quoted(field)?);
        }
        let select = format!("SELECT {} FROM {from}", fields.join(", "));
        Ok(Some((select, table, snapshot)))
    }

    /// Whether the rows `from` gives are some, every one derived by one of
    /// `digests`: the derivation's, or the frozen legacy one.
    fn holds(&self, from: &str, digests: &[String]) -> Result<bool, Error> {
        let (rows, other): (i64, i64) = self.con.query_row(
            &format!(
                "SELECT count(*), count(*) FILTER (WHERE {} IS NULL OR {} NOT IN ({})) FROM {from}",
                quoted(DERIVED_BY)?,
                quoted(DERIVED_BY)?,
                vec!["?"; digests.len()].join(", ")
            ),
            duckdb::params_from_iter(digests.iter()),
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        Ok(rows > 0 && other == 0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MAPPING: &str = "dataset: clients
source: clients_2026
fields:
  Documento: { column: doc, type: text, semantic: document, roles: [entity] }
  Saldo: { column: balance, type: number }
  Apertura: { column: opened, type: date }
";

    /// A runner with a synthetic `clients__source` view of five rows.
    fn runner() -> Runner {
        let runner = Runner::new().unwrap();
        runner
            .con
            .execute_batch(
                "CREATE VIEW clients__source AS SELECT * FROM (VALUES
                   (' 00123 ', '10.5', '2024-01-31'),
                   ('C.C. 456', 'x', 'n/a'),
                   ('N/A', '3', '2024-02-29'),
                   ('  ', '1', NULL),
                   (NULL, NULL, NULL)
                 ) AS t(doc, balance, opened);",
            )
            .unwrap();
        runner
    }

    #[test]
    fn silver_types_the_fields_and_says_which_keys_normalized() {
        let runner = runner();
        let mapping = Mapping::parse(MAPPING).unwrap();
        let select = runner.silver_select(&mapping, "clients__source").unwrap();
        let mut stmt = runner
            .con
            .prepare(&format!(
                "SELECT Saldo, CAST(Apertura AS VARCHAR), _entity_key, _entity_key_normalized \
                 FROM ({select}) ORDER BY Saldo NULLS LAST, _entity_key"
            ))
            .unwrap();
        type Row = (Option<f64>, Option<String>, Option<String>, Option<bool>);
        let rows: Vec<Row> = stmt
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        assert_eq!(
            rows,
            [
                (Some(1.0), None, None, None),
                (Some(3.0), Some("2024-02-29".into()), None, Some(false)),
                (
                    Some(10.5),
                    Some("2024-01-31".into()),
                    Some("123".into()),
                    Some(true)
                ),
                (None, None, Some("456".into()), Some(true)),
                (None, None, None, None),
            ]
        );
    }

    /// The derivation's SQL at `DERIVATION_VERSION` 1. A change of the SQL
    /// for the same derivation raises the version, and this text with it.
    const SILVER_V1: &str = "SELECT *, NULLIF(trim(LEADING '0' FROM regexp_replace(CAST(\"Documento\" AS VARCHAR), '[^0-9]', '', 'g')), '') AS \"_entity_key\", \
        CASE WHEN (NULLIF(trim(LEADING '0' FROM regexp_replace(CAST(\"Documento\" AS VARCHAR), '[^0-9]', '', 'g')), '')) IS NOT NULL THEN TRUE \
        WHEN NULLIF(trim(CAST(\"Documento\" AS VARCHAR)), '') IS NOT NULL THEN FALSE END AS \"_entity_key_normalized\" \
        FROM (SELECT TRY_CAST(\"opened\" AS DATE) AS \"Apertura\", CAST(\"doc\" AS VARCHAR) AS \"Documento\", \
        TRY_CAST(\"balance\" AS DOUBLE) AS \"Saldo\" FROM \"clients__source\")";

    /// What the string builder before decision 0090 step 7 wrote and
    /// hashed into `_mapping`, for the same mapping and source.
    const LEGACY: &str = "SELECT *, NULLIF(ltrim(regexp_replace(CAST(\"Documento\" AS VARCHAR), '[^0-9]', '', 'g'), '0'), '') AS \"_entity_key\", \
        CASE WHEN (NULLIF(ltrim(regexp_replace(CAST(\"Documento\" AS VARCHAR), '[^0-9]', '', 'g'), '0'), '')) IS NOT NULL THEN TRUE \
        WHEN NULLIF(trim(CAST(\"Documento\" AS VARCHAR)), '') IS NOT NULL THEN FALSE END AS \"_entity_key_normalized\" \
        FROM (SELECT TRY_CAST(\"opened\" AS DATE) AS \"Apertura\", CAST(\"doc\" AS VARCHAR) AS \"Documento\", \
        TRY_CAST(\"balance\" AS DOUBLE) AS \"Saldo\" FROM \"clients__source\")";

    #[test]
    fn the_silver_sql_changes_only_with_the_derivations_version() {
        let runner = runner();
        let mapping = Mapping::parse(MAPPING).unwrap();
        let select = runner.silver_select(&mapping, "clients__source").unwrap();
        assert_eq!(layers::DERIVATION_VERSION, 1);
        assert_eq!(select, SILVER_V1, "raise DERIVATION_VERSION with the SQL");
    }

    #[test]
    fn the_digest_follows_the_derivation() {
        let runner = runner();
        let mapping = Mapping::parse(MAPPING).unwrap();
        let d = runner.derivation(&mapping, "clients__source").unwrap();
        let digest = d.digest();
        assert_eq!(digest.len(), 64);
        assert_eq!(
            runner
                .derivation(&mapping, "clients__source")
                .unwrap()
                .digest(),
            digest
        );
        let changed = Mapping::parse(&MAPPING.replace("type: date", "type: text")).unwrap();
        let other = runner.derivation(&changed, "clients__source").unwrap();
        assert_ne!(other.digest(), digest);
    }

    #[test]
    fn the_legacy_digest_is_what_duckdb_hashed_before() {
        let runner = runner();
        let mapping = Mapping::parse(MAPPING).unwrap();
        let d = runner.derivation(&mapping, "clients__source").unwrap();
        let before: String = runner
            .con
            .query_row("SELECT sha256(?)", [LEGACY], |r| r.get(0))
            .unwrap();
        assert_eq!(legacy_digest(&d, "clients__source").unwrap(), before);
        assert_ne!(d.digest(), before);
    }

    #[test]
    fn a_table_lacks_a_new_fields_column_or_cannot_take_a_changed_type() {
        let runner = runner();
        runner
            .con
            .execute_batch(
                "CREATE SCHEMA s; CREATE TABLE s.clients (Documento VARCHAR, Saldo DOUBLE, \
                 _entity_key VARCHAR, _entity_key_normalized BOOLEAN, _mapping VARCHAR, cut DATE);",
            )
            .unwrap();
        let at = ("memory", "s", "clients");
        let mapping = Mapping::parse(MAPPING).unwrap();
        assert_eq!(
            runner.fit(at, &mapping).unwrap(),
            Fit::Missing(vec![("Apertura".into(), "DATE")])
        );
        let changed = Mapping::parse(&MAPPING.replace("type: number", "type: text")).unwrap();
        assert_eq!(runner.fit(at, &changed).unwrap(), Fit::Conflict);
    }

    #[test]
    fn a_cut_is_read_from_silver_only_with_rows_all_of_its_derivation() {
        let runner = runner();
        runner
            .con
            .execute_batch(
                "CREATE TABLE written (_mapping VARCHAR, cut DATE);
                 INSERT INTO written VALUES ('d1', DATE '2026-03-31'), ('d1', DATE '2026-03-31'),
                   ('d1', DATE '2026-04-30'), ('d2', DATE '2026-04-30'), (NULL, DATE '2026-05-31');",
            )
            .unwrap();
        let at = |cut: &str| format!("written WHERE cut = DATE '{cut}'");
        let d = |names: &[&str]| names.iter().map(|n| n.to_string()).collect::<Vec<_>>();
        assert!(runner.holds(&at("2026-03-31"), &d(&["d1"])).unwrap());
        assert!(
            runner.holds(&at("2026-03-31"), &d(&["d2", "d1"])).unwrap(),
            "the legacy digest"
        );
        assert!(
            !runner.holds(&at("2026-03-31"), &d(&["d2"])).unwrap(),
            "another mapping"
        );
        assert!(
            !runner.holds(&at("2026-04-30"), &d(&["d1"])).unwrap(),
            "two mappings"
        );
        assert!(
            !runner.holds(&at("2026-05-31"), &d(&["d1", "d2"])).unwrap(),
            "no digest"
        );
        assert!(
            !runner.holds(&at("2026-06-30"), &d(&["d1"])).unwrap(),
            "no rows"
        );
    }
}
