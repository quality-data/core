//! Extraction through Trino's connectors (decision 0090, point 9): a
//! catalog per source with no password (extra credentials, point 7.5),
//! every column cast explicitly by a type table into what Iceberg keeps,
//! and one `INSERT` per load whose branches are `UNION ALL`ed key-range
//! scans (T20: the branches read at once, one snapshot). Text keys are
//! never split.

use quality_rules_sql::{quote_text, Cut};

use crate::loads::LOAD;
use crate::read::TablePath;
use crate::{quoted, Error, Statement};

/// A source's connector.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Connector {
    SqlServer,
    Oracle,
    Postgresql,
    Mysql,
}

impl Connector {
    fn name(self) -> &'static str {
        match self {
            Self::SqlServer => "sqlserver",
            Self::Oracle => "oracle",
            Self::Postgresql => "postgresql",
            Self::Mysql => "mysql",
        }
    }
}

/// How a column's value reaches bronze (point 9.6).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Conversion {
    Exact,
    /// Kept as lossless text (silver casts it per the mapping).
    Text,
    /// Sub-microsecond digits dropped.
    TruncatedPrecision,
    /// Kept as the instant, in UTC.
    OffsetDropped,
}

/// The bronze type of a column Trino reads as `ty`, and what that costs.
pub fn bronze_type(ty: &str) -> (String, Conversion) {
    let t = ty.trim().to_ascii_lowercase();
    let precision =
        |prefix: &str| -> Option<u32> { t.strip_prefix(prefix)?.split(')').next()?.parse().ok() };
    if let Some(p) = precision("timestamp(") {
        if t.ends_with("with time zone") {
            return (
                "timestamp(6) with time zone".into(),
                Conversion::OffsetDropped,
            );
        }
        let c = if p > 6 {
            Conversion::TruncatedPrecision
        } else {
            Conversion::Exact
        };
        return ("timestamp(6)".into(), c);
    }
    if let Some(p) = precision("time(") {
        let c = if p > 6 {
            Conversion::TruncatedPrecision
        } else {
            Conversion::Exact
        };
        return ("time(6)".into(), c);
    }
    match t.as_str() {
        "tinyint" => ("smallint".into(), Conversion::Exact),
        // Oracle's unconstrained NUMBER: Iceberg refuses it (point 9.5).
        "number" => ("varchar".into(), Conversion::Text),
        _ if t.starts_with("char(") => (t, Conversion::Exact),
        _ => (t, Conversion::Exact),
    }
}

fn plain(sql: String) -> Statement {
    Statement { sql }
}

/// A source's listing (decision 0090, rulings 68 and point 9): every
/// column of every table its catalog `src_<source>` shows, with the type
/// name Trino gives it, the system schemas left out (`exclude`, lower
/// case), in order. Names and types only, never a value.
pub fn source_listing(source: &str, exclude: &[&str]) -> Result<Statement, Error> {
    let mut system: Vec<String> = vec!["'information_schema'".into()];
    for s in exclude {
        system.push(quote_text(&s.to_ascii_lowercase())?);
    }
    Ok(plain(format!(
        "SELECT table_schema, table_name, column_name, data_type, is_nullable FROM {}.\"information_schema\".\"columns\" \
         WHERE lower(table_schema) NOT IN ({}) \
         ORDER BY table_schema, table_name, ordinal_position",
        quoted(&format!("src_{source}"))?,
        system.join(", ")
    )))
}

/// The source's catalog `src_<source>`: where it is, and no secret. The
/// user and password come with each statement as extra credentials.
pub fn create_catalog(
    source: &str,
    connector: Connector,
    connection_url: &str,
) -> Result<Statement, Error> {
    let mut props = vec![
        format!("\"connection-url\" = {}", quote_text(connection_url)?),
        "\"user-credential-name\" = 'user'".to_string(),
        "\"password-credential-name\" = 'password'".to_string(),
        "\"unsupported-type-handling\" = 'CONVERT_TO_VARCHAR'".to_string(),
    ];
    if connector == Connector::Oracle {
        // Its pooled connections ignore extra credentials.
        props.push("\"oracle.connection-pool.enabled\" = 'false'".into());
    }
    Ok(plain(format!(
        "CREATE CATALOG {} USING {} WITH ({})",
        quoted(&format!("src_{source}"))?,
        connector.name(),
        props.join(", ")
    )))
}

/// A new bronze table for `columns` (name, Trino type), partitioned by
/// `(cut, _load)` from the start.
pub fn create_bronze(t: &TablePath, columns: &[(String, String)]) -> Result<Statement, Error> {
    let mut defs = Vec::new();
    for (n, ty) in columns {
        defs.push(format!("{} {}", quoted(n)?, bronze_type(ty).0));
    }
    defs.push(format!("cut DATE, {LOAD} VARCHAR"));
    Ok(plain(format!(
        "CREATE TABLE IF NOT EXISTS {}.{}.{} ({}) WITH (partitioning = ARRAY['cut', '{LOAD}'])",
        quoted(&t.catalog)?,
        quoted(&t.namespace)?,
        quoted(&t.table)?,
        defs.join(", ")
    )))
}

/// A key range: from, included, to the next range's start, or open.
pub type Range = (i64, Option<i64>);

/// `n` contiguous ranges over a numeric key from `lo` to `hi`, both
/// included: the first `n - 1` half-open, the last closed.
pub fn split(lo: i64, hi: i64, n: u32) -> Vec<Range> {
    let n = i64::from(n.max(1));
    let span = (hi - lo).max(0) / n + 1;
    let mut out = Vec::new();
    let mut at = lo;
    for i in 0..n {
        if i == n - 1 || at + span > hi {
            out.push((at, None));
            break;
        }
        out.push((at, Some(at + span)));
        at += span;
    }
    out
}

/// One load of a source table into bronze: every column cast by the type
/// table; with `ranges` on a numeric key, one branch per range, `UNION
/// ALL`ed in the same `INSERT` (one commit).
pub fn extract(
    bronze: &TablePath,
    source: &TablePath,
    columns: &[(String, String)],
    at: (&Cut, &str),
    ranges: Option<(&str, &[Range])>,
) -> Result<Statement, Error> {
    let bounds: Option<(&str, Vec<Bound>)> = ranges.map(|(key, rs)| {
        let b = rs
            .iter()
            .map(|(lo, hi)| (lo.to_string(), hi.map(|h| h.to_string())))
            .collect();
        (key, b)
    });
    bounded(
        bronze,
        source,
        columns,
        at,
        bounds.as_ref().map(|(k, b)| (*k, b.as_slice())),
    )
}

/// A range's bounds as SQL: from, included, to the next's start, or open.
type Bound = (String, Option<String>);

/// One load split into `n` contiguous ranges over a numeric `key` whose
/// smallest and largest values the job runner writes in place of the
/// bigint tokens `{{lo}}` and `{{hi}}` (read by [`key_range`] in the same
/// job, amendment A1), so the core needs no value at compile: range `i`
/// starts at `lo + i * ((hi - lo) / n + 1)`, as [`split`] cuts them.
pub fn extract_split(
    bronze: &TablePath,
    source: &TablePath,
    columns: &[(String, String)],
    at: (&Cut, &str),
    key: &str,
    (lo, hi): (&str, &str),
    n: u32,
) -> Result<Statement, Error> {
    for t in [lo, hi] {
        if !quality::bundle::is_statement_id(t) {
            return Err(Error::BadPlaceholder(t.to_owned()));
        }
    }
    let (lo, hi) = (format!("{{{{{lo}}}}}"), format!("{{{{{hi}}}}}"));
    let n = n.max(1);
    let start = |i: u32| format!("{lo} + {i} * (({hi} - {lo}) / {n} + 1)");
    let bounds: Vec<Bound> = (0..n)
        .map(|i| (start(i), (i + 1 < n).then(|| start(i + 1))))
        .collect();
    bounded(bronze, source, columns, at, Some((key, &bounds)))
}

/// One row, two bigints `lo` and `hi`: a numeric key's smallest and
/// largest values in the source table, 0 when it has none.
pub fn key_range(source: &TablePath, key: &str) -> Result<Statement, Error> {
    let k = quoted(key)?;
    Ok(plain(format!(
        "SELECT CAST(COALESCE(min({k}), 0) AS BIGINT) AS lo, \
         CAST(COALESCE(max({k}), 0) AS BIGINT) AS hi FROM {}.{}.{}",
        quoted(&source.catalog)?,
        quoted(&source.namespace)?,
        quoted(&source.table)?
    )))
}

fn bounded(
    bronze: &TablePath,
    source: &TablePath,
    columns: &[(String, String)],
    (cut, load): (&Cut, &str),
    ranges: Option<(&str, &[Bound])>,
) -> Result<Statement, Error> {
    let mut names = Vec::new();
    let mut casts = Vec::new();
    for (n, ty) in columns {
        let q = quoted(n)?;
        casts.push(format!("CAST({q} AS {}) AS {q}", bronze_type(ty).0));
        names.push(q);
    }
    let from = format!(
        "{}.{}.{}",
        quoted(&source.catalog)?,
        quoted(&source.namespace)?,
        quoted(&source.table)?
    );
    let head = format!(
        "SELECT {}, {}, {} FROM {from}",
        casts.join(", "),
        cut.literal(),
        quote_text(load)?
    );
    let body = match ranges {
        Some((key, rs)) if !rs.is_empty() => {
            let k = quoted(key)?;
            rs.iter()
                .map(|(lo, hi)| match hi {
                    Some(hi) => format!("{head} WHERE {k} >= {lo} AND {k} < {hi}"),
                    None => format!("{head} WHERE {k} >= {lo}"),
                })
                .collect::<Vec<_>>()
                .join(" UNION ALL ")
        }
        _ => head,
    };
    Ok(plain(format!(
        "INSERT INTO {}.{}.{} ({}, cut, {LOAD}) {body}",
        quoted(&bronze.catalog)?,
        quoted(&bronze.namespace)?,
        quoted(&bronze.table)?,
        names.join(", ")
    )))
}
