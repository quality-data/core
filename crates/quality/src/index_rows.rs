//! Which of gold's tables the core's PostgreSQL index keeps, and how
//! (decision 0090, ruling 76): an evaluate reads each one's rows of its
//! own run back as the report user (statement `index_<table>`), and the
//! report's rows replace the cut's in the index table of the same name
//! and columns. One definition for the compiler and the report's writer.

use serde_json::Value;
use sqlx::{Postgres, Transaction};

/// A column's Trino type, as the read declares it and the writer binds it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnType {
    Varchar,
    Bigint,
    Double,
    Boolean,
    Date,
    BigintArray,
}

impl ColumnType {
    /// Trino's name, as a bundle's `Rows` read declares it.
    pub fn trino(self) -> &'static str {
        match self {
            Self::Varchar => "varchar",
            Self::Bigint => "bigint",
            Self::Double => "double",
            Self::Boolean => "boolean",
            Self::Date => "date",
            Self::BigintArray => "array(bigint)",
        }
    }

    /// Whether `v` is a value of this type as Trino answers it (or null).
    pub fn takes(self, v: &Value) -> bool {
        v.is_null()
            || match self {
                Self::Varchar | Self::Date => v.is_string(),
                Self::Bigint => v.is_i64(),
                Self::Double => v.is_number(),
                Self::Boolean => v.is_boolean(),
                Self::BigintArray => v
                    .as_array()
                    .is_some_and(|a| a.iter().all(|x| x.is_i64() || x.is_null())),
            }
    }
}

use ColumnType::{Bigint, BigintArray, Boolean, Date, Double, Varchar};

/// One index table: its name in gold and in the index, its columns after
/// `dataset` and `cut`, and the most rows a run gives it.
#[derive(Debug, Clone, Copy)]
pub struct IndexTable {
    pub name: &'static str,
    pub columns: &'static [(&'static str, ColumnType)],
    pub max_rows: u64,
}

impl IndexTable {
    /// The statement that reads it back.
    pub fn statement(&self) -> String {
        format!("index_{}", self.name)
    }
}

/// The index's detail tables, written from a done evaluate's report.
pub const INDEX_TABLES: &[IndexTable] = &[
    IndexTable {
        name: "rule_results",
        columns: &[("rule", Varchar), ("applies", Bigint), ("failures", Bigint)],
        max_rows: 10_000,
    },
    IndexTable {
        name: "field_quality",
        columns: &[
            ("field", Varchar),
            ("empty", Bigint),
            ("distinct_values", Bigint),
            ("pattern", Varchar),
            ("pattern_count", Bigint),
            ("floating", Boolean),
            ("mean_icd", Double),
            ("recency", BigintArray),
        ],
        max_rows: 10_000,
    },
    IndexTable {
        name: "group_metrics",
        columns: &[
            ("dimension", Varchar),
            ("grp", Varchar),
            ("records", Bigint),
            ("serious", Bigint),
            ("mean_icd", Double),
        ],
        max_rows: 100_000,
    },
    IndexTable {
        name: "cells",
        columns: &[
            ("cell", Varchar),
            ("ok", Bigint),
            ("total", Bigint),
            ("passes", Boolean),
            ("missing", Bigint),
            ("margin", Bigint),
        ],
        max_rows: 10_000,
    },
    IndexTable {
        name: "queue_findings",
        columns: &[
            ("dimension", Varchar),
            ("grp", Varchar),
            ("since", Date),
            ("open", Bigint),
            ("resolved", Bigint),
            ("priority", Double),
        ],
        max_rows: 100_000,
    },
];

/// Writes `rows` (each as `table`'s columns, checked by the caller) as the
/// cut's rows of `table`, after the run's summary was replaced.
pub async fn write(
    tx: &mut Transaction<'_, Postgres>,
    table: &IndexTable,
    dataset: &str,
    cut: chrono::NaiveDate,
    rows: &[Vec<Value>],
) -> Result<(), sqlx::Error> {
    let names: Vec<&str> = table.columns.iter().map(|(n, _)| *n).collect();
    let marks: Vec<String> = (3..3 + names.len()).map(|i| format!("${i}")).collect();
    let sql = format!(
        "INSERT INTO quality.{} (dataset, cut, {}) VALUES ($1, $2, {})",
        table.name,
        names.join(", "),
        marks.join(", ")
    );
    for row in rows {
        let mut q = sqlx::query(&sql).bind(dataset).bind(cut);
        for ((_, ty), v) in table.columns.iter().zip(row) {
            q = match ty {
                Varchar => q.bind(v.as_str().map(str::to_owned)),
                Bigint => q.bind(v.as_i64()),
                Double => q.bind(v.as_f64()),
                Boolean => q.bind(v.as_bool()),
                Date => q.bind(v.as_str().and_then(|d| d.parse::<chrono::NaiveDate>().ok())),
                BigintArray => q.bind(v.as_array().map(|a| {
                    a.iter()
                        .map(|x| x.as_i64().unwrap_or(0))
                        .collect::<Vec<i64>>()
                })),
            };
        }
        q.execute(&mut **tx).await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn each_type_takes_what_trino_answers_for_it() {
        assert!(BigintArray.takes(&json!([1, 2, null])));
        assert!(!Bigint.takes(&json!("1")));
        assert!(Double.takes(&json!(0.5)) && Double.takes(&json!(1)));
        assert!(Date.takes(&json!("2026-10-31")) && Varchar.takes(&Value::Null));
        assert_eq!(INDEX_TABLES[0].statement(), "index_rule_results");
    }
}
