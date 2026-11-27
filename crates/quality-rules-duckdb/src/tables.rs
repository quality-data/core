//! A listing job's reading of a source (decision 0042): its tables as
//! DuckDB sees the attached source, by schema and name, with their columns
//! (name and declared type, in order), the system's schemas left out. The
//! catalog only, never a row; views come as tables
//! (`tools/verify/source_tables.py`). The lake is read the same way
//! (decision 0043): its namespaces as schemas; an Iceberg table's columns
//! show only once it is described (`tools/verify/lake_listing.py`).

use std::collections::BTreeMap;

use quality::source_tables::{SourceColumn, SourceTable};
use quality_rules_sql::quote_text;

use crate::{invalid, quoted, Error, Runner};

impl Runner {
    /// The attached source's tables outside `system`, at most `max`, and
    /// whether there were more.
    pub fn list_tables(
        &self,
        system: &[&str],
        max: usize,
    ) -> Result<(Vec<SourceTable>, bool), Error> {
        let left_out = system
            .iter()
            .map(|s| quote_text(s).map_err(invalid))
            .collect::<Result<Vec<_>, _>>()?;
        let outside = if left_out.is_empty() {
            String::new()
        } else {
            format!("AND schema_name NOT IN ({})", left_out.join(", "))
        };
        let sql = format!(
            "SELECT schema_name, table_name FROM duckdb_tables() \
             WHERE database_name = 'src' {outside} \
             ORDER BY schema_name, table_name LIMIT {}",
            max + 1
        );
        let mut stmt = self.con.prepare(&sql)?;
        let mut tables: Vec<SourceTable> = stmt
            .query_map([], |r| {
                Ok(SourceTable {
                    schema: r.get(0)?,
                    name: r.get(1)?,
                    columns: Vec::new(),
                })
            })?
            .collect::<Result<_, _>>()?;
        let truncated = tables.len() > max;
        tables.truncate(max);
        let at: BTreeMap<(String, String), usize> = tables
            .iter()
            .enumerate()
            .map(|(i, t)| ((t.schema.clone(), t.name.clone()), i))
            .collect();
        let sql = format!(
            "SELECT schema_name, table_name, column_name, data_type FROM duckdb_columns() \
             WHERE database_name = 'src' {outside} \
             ORDER BY schema_name, table_name, column_index"
        );
        let mut stmt = self.con.prepare(&sql)?;
        let mut rows = stmt.query([])?;
        while let Some(r) = rows.next()? {
            if let Some(&i) = at.get(&(r.get(0)?, r.get(1)?)) {
                tables[i].columns.push(SourceColumn {
                    name: r.get(2)?,
                    data_type: r.get(3)?,
                    nullable: None,
                });
            }
        }
        Ok((tables, truncated))
    }

    /// The attached lake's tables, at most `max`, and whether there were
    /// more: its namespaces as schemas, each table described for its
    /// columns.
    pub fn list_lake(&self, max: usize) -> Result<(Vec<SourceTable>, bool), Error> {
        let sql = format!(
            "SELECT schema_name, table_name FROM duckdb_tables() \
             WHERE database_name = 'lake' ORDER BY schema_name, table_name LIMIT {}",
            max + 1
        );
        let mut stmt = self.con.prepare(&sql)?;
        let mut tables: Vec<SourceTable> = stmt
            .query_map([], |r| {
                Ok(SourceTable {
                    schema: r.get(0)?,
                    name: r.get(1)?,
                    columns: Vec::new(),
                })
            })?
            .collect::<Result<_, _>>()?;
        let truncated = tables.len() > max;
        tables.truncate(max);
        for t in &mut tables {
            let sql = format!("DESCRIBE lake.{}.{}", quoted(&t.schema)?, quoted(&t.name)?);
            let mut stmt = self.con.prepare(&sql)?;
            t.columns = stmt
                .query_map([], |r| {
                    Ok(SourceColumn {
                        name: r.get(0)?,
                        data_type: r.get(1)?,
                        nullable: None,
                    })
                })?
                .collect::<Result<_, _>>()?;
        }
        Ok((tables, truncated))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_listing_leaves_the_system_out_and_cuts_at_its_most() {
        let runner = Runner::new().unwrap();
        runner
            .con
            .execute_batch(
                "ATTACH ':memory:' AS src;
                 CREATE SCHEMA src.loans;
                 CREATE SCHEMA src.pg_catalog_copy;
                 CREATE TABLE src.main.clients (id INTEGER, email VARCHAR, opened DATE);
                 CREATE TABLE src.loans.credits (id INTEGER, amount DECIMAL(14,2));
                 CREATE TABLE src.pg_catalog_copy.pg_class (oid INTEGER);",
            )
            .unwrap();
        let (tables, truncated) = runner.list_tables(&["pg_catalog_copy"], 10).unwrap();
        let named: Vec<_> = tables
            .iter()
            .map(|t| {
                let columns: Vec<_> = t
                    .columns
                    .iter()
                    .map(|c| format!("{} {}", c.name, c.data_type))
                    .collect();
                (t.schema.as_str(), t.name.as_str(), columns.join(", "))
            })
            .collect();
        assert_eq!(
            named,
            [
                (
                    "loans",
                    "credits",
                    "id INTEGER, amount DECIMAL(14,2)".into()
                ),
                (
                    "main",
                    "clients",
                    "id INTEGER, email VARCHAR, opened DATE".into()
                )
            ]
        );
        assert!(!truncated);
        let (tables, truncated) = runner.list_tables(&[], 2).unwrap();
        assert_eq!(tables.len(), 2);
        assert!(truncated);
    }

    #[test]
    fn the_lake_is_listed_by_namespace_each_table_with_its_columns() {
        let runner = Runner::new().unwrap();
        runner
            .con
            .execute_batch(
                "ATTACH ':memory:' AS lake;
                 CREATE SCHEMA lake.loans_bronze;
                 CREATE TABLE lake.loans_bronze.loans (\"Id\" VARCHAR, amount DOUBLE, cut DATE);
                 CREATE TABLE lake.loans_bronze.\"odd name\" (x INTEGER);",
            )
            .unwrap();
        let (tables, truncated) = runner.list_lake(10).unwrap();
        let named: Vec<_> = tables
            .iter()
            .map(|t| {
                let columns: Vec<_> = t.columns.iter().map(|c| c.name.as_str()).collect();
                (t.schema.as_str(), t.name.as_str(), columns.join(", "))
            })
            .collect();
        assert_eq!(
            named,
            [
                ("loans_bronze", "loans", "Id, amount, cut".into()),
                ("loans_bronze", "odd name", "x".into())
            ]
        );
        assert!(!truncated);
        assert!(runner.list_lake(1).unwrap().1, "cut at its most");
    }
}
