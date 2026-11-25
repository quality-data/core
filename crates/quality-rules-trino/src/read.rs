//! Reading a lake table: pinned to the snapshot the job recorded, and each
//! mapped field typed as silver holds it.

use quality::layers::SourceClass;
use quality::mapping::{Kind, Mapping};

use crate::{quoted, Error};

/// A table of the lake catalog: catalog, namespace and table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TablePath {
    pub catalog: String,
    pub namespace: String,
    pub table: String,
}

impl TablePath {
    fn sql(&self) -> Result<String, Error> {
        Ok(format!(
            "{}.{}.{}",
            quoted(&self.catalog)?,
            quoted(&self.namespace)?,
            quoted(&self.table)?
        ))
    }
}

/// A table read at one snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Pinned {
    pub table: TablePath,
    pub snapshot: i64,
}

/// The table as a relation at its snapshot (point 8.8).
pub fn pinned(p: &Pinned) -> Result<String, Error> {
    Ok(format!(
        "{} FOR VERSION AS OF {}",
        p.table.sql()?,
        p.snapshot
    ))
}

/// The table's current snapshot, from its `$refs` (the `main` branch), to
/// pin every later read of the job to it.
pub fn current_snapshot(t: &TablePath) -> Result<String, Error> {
    let refs = TablePath {
        table: format!("{}$refs", t.table),
        ..t.clone()
    };
    Ok(format!(
        "SELECT snapshot_id FROM {} WHERE name = 'main'",
        refs.sql()?
    ))
}

/// Each field of `mapping` read from its column of `from` (a relation),
/// typed as mapped; `source` tells each column's class. A number read as
/// text drops a trailing `.0`, as on DuckDB.
pub fn typed_select(
    mapping: &Mapping,
    from: &str,
    source: impl Fn(&str) -> SourceClass,
) -> Result<String, Error> {
    let mut select = Vec::new();
    for (field, f) in &mapping.fields {
        let column = &f.value.column;
        let c = quoted(column)?;
        let value = match f.value.kind {
            Kind::Text if source(column) == SourceClass::Number => {
                format!("regexp_replace(CAST({c} AS VARCHAR), '\\.0$', '')")
            }
            Kind::Text => format!("CAST({c} AS VARCHAR)"),
            Kind::Number => format!("TRY_CAST({c} AS DOUBLE)"),
            Kind::Date => format!("TRY_CAST({c} AS DATE)"),
            Kind::Boolean => format!("TRY_CAST({c} AS BOOLEAN)"),
        };
        select.push(format!("{value} AS {}", quoted(field)?));
    }
    Ok(format!("SELECT {} FROM {from}", select.join(", ")))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn path() -> TablePath {
        TablePath {
            catalog: "lake".into(),
            namespace: "loans_silver".into(),
            table: "loans".into(),
        }
    }

    #[test]
    fn a_read_is_pinned_to_its_snapshot() {
        let p = Pinned {
            table: path(),
            snapshot: 42,
        };
        assert_eq!(
            pinned(&p).unwrap(),
            "\"lake\".\"loans_silver\".\"loans\" FOR VERSION AS OF 42"
        );
        assert_eq!(
            current_snapshot(&path()).unwrap(),
            "SELECT snapshot_id FROM \"lake\".\"loans_silver\".\"loans$refs\" WHERE name = 'main'"
        );
    }

    #[test]
    fn each_field_is_typed_as_mapped() {
        let m = Mapping::parse(
            "dataset: loans\nsource: loans\nfields:\n  \
             Code: { column: code, type: text }\n  \
             Amount: { column: amt, type: number }\n  \
             Opened: { column: opened, type: date }\n",
        )
        .unwrap();
        let sql = typed_select(&m, "b", |c| {
            if c == "code" {
                SourceClass::Number
            } else {
                SourceClass::Other
            }
        })
        .unwrap();
        assert_eq!(
            sql,
            "SELECT TRY_CAST(\"amt\" AS DOUBLE) AS \"Amount\", \
             regexp_replace(CAST(\"code\" AS VARCHAR), '\\.0$', '') AS \"Code\", \
             TRY_CAST(\"opened\" AS DATE) AS \"Opened\" FROM b"
        );
    }
}
