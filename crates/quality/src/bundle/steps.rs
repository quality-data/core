//! A bundle's two step forms (decision 0090, amendment A1): a statement
//! that runs only when an earlier count showed rows, and typed `{{name}}`
//! tokens the job runner fills from earlier one-row answers. Only decimal
//! integers are ever written into SQL.

use serde::{Deserialize, Serialize};

use super::{is_statement_id, Expect};

/// The count a statement waits on: `after` names an earlier statement
/// whose answer is one row of one `bigint`; when it is 0 the statement is
/// skipped and reported `skipped`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OnlyIfRows {
    pub after: String,
}

/// The types a placeholder may carry: decimal integers only, never text.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlaceholderType {
    /// An Iceberg snapshot id: `[0-9]{1,19}`.
    SnapshotId,
    /// A `bigint`: `-?[0-9]{1,19}`.
    Bigint,
}

/// Where a placeholder's value comes from: a column of the one-row answer
/// of an earlier statement.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlaceholderFrom {
    pub statement: String,
    pub column: String,
}

/// A `{{name}}` token the job runner fills (amendment A1): the pinned
/// snapshot read at run time (point 8.8) and carried to later reads.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Placeholder {
    pub name: String,
    #[serde(rename = "type")]
    pub ty: PlaceholderType,
    pub from: PlaceholderFrom,
}

impl PlaceholderType {
    /// Whether the job runner may write `value` in place of the token.
    pub fn takes(self, value: &str) -> bool {
        let digits = match self {
            Self::SnapshotId => value,
            Self::Bigint => value.strip_prefix('-').unwrap_or(value),
        };
        (1..=19).contains(&digits.len())
            && digits.bytes().all(|b| b.is_ascii_digit())
            && value.parse::<i64>().is_ok()
    }
}

/// The `{{name}}` tokens of `sql`, in order; `None` when a `{{` is not a
/// well formed token, so no stray braces reach the runner.
pub fn tokens(sql: &str) -> Option<Vec<&str>> {
    let mut out = Vec::new();
    let mut rest = sql;
    loop {
        let Some(at) = rest.find("{{") else {
            return (!rest.contains("}}")).then_some(out);
        };
        let after = &rest[at + 2..];
        let end = after.find("}}")?;
        let name = &after[..end];
        if rest[..at].contains("}}") || !is_statement_id(name) {
            return None;
        }
        out.push(name);
        rest = &after[end + 2..];
    }
}

/// `sql` with each token written as its value, as the job runner fills it;
/// `None` for a token without a value the type takes.
pub fn fill(sql: &str, values: &[(&Placeholder, &str)]) -> Option<String> {
    let mut out = sql.to_owned();
    for name in tokens(sql)? {
        let (p, v) = values.iter().find(|(p, _)| p.name == name)?;
        if !p.ty.takes(v) {
            return None;
        }
        out = out.replacen(&format!("{{{{{name}}}}}"), v, 1);
    }
    Some(out)
}

/// A read of one row whose `column` is a `bigint`.
pub(super) fn one_row_column(e: &Expect, column: &str) -> bool {
    matches!(e, Expect::Rows { columns, max_rows: 1 }
        if columns.iter().any(|(n, t)| n == column && t == "bigint"))
}

/// A read of one row of one `bigint`: a count.
pub(super) fn is_count_read(e: &Expect) -> bool {
    matches!(e, Expect::Rows { columns, max_rows: 1 }
        if columns.len() == 1 && columns[0].1 == "bigint")
}

#[cfg(test)]
mod tests {
    use super::super::{Bundle, BundleStatement, RunAs};
    use super::*;

    fn read(id: &str, column: &str) -> BundleStatement {
        BundleStatement {
            id: id.into(),
            sql: format!("SELECT 1 AS {column}"),
            expects: Expect::Rows {
                columns: vec![(column.into(), "bigint".into())],
                max_rows: 1,
            },
            run_as: RunAs::Job,
            reads_source: false,
            only_if_rows: None,
        }
    }

    fn delete(id: &str, sql: &str, after: Option<&str>) -> BundleStatement {
        BundleStatement {
            id: id.into(),
            sql: sql.into(),
            expects: Expect::delete(),
            run_as: RunAs::Job,
            reads_source: false,
            only_if_rows: after.map(|a| OnlyIfRows { after: a.into() }),
        }
    }

    fn snapshot() -> Placeholder {
        Placeholder {
            name: "silver_snapshot".into(),
            ty: PlaceholderType::SnapshotId,
            from: PlaceholderFrom {
                statement: "pin".into(),
                column: "snapshot".into(),
            },
        }
    }

    fn bundle(placeholders: Vec<Placeholder>, statements: Vec<BundleStatement>) -> Bundle {
        Bundle {
            job_id: "j".into(),
            kind: "evaluate".into(),
            dataset: Some("loans".into()),
            cut: None,
            attempt: 1,
            run_id: None,
            run_seq: None,
            catalog_commit: None,
            compiler_version: "t".into(),
            placeholders,
            statements,
        }
    }

    const PINNED: &str = "DELETE FROM t FOR VERSION AS OF {{silver_snapshot}} WHERE x = 1";

    #[test]
    fn a_guarded_delete_and_a_filled_token_are_well_formed() {
        let b = bundle(
            vec![snapshot()],
            vec![
                read("pin", "snapshot"),
                read("count", "n"),
                delete("drop", PINNED, Some("count")),
            ],
        );
        assert!(b.well_formed());
        let json = b.canonical();
        assert!(json.contains(
            "\"placeholders\":[{\"name\":\"silver_snapshot\",\"type\":\"snapshot_id\",\
             \"from\":{\"statement\":\"pin\",\"column\":\"snapshot\"}}],\"statements\":"
        ));
        assert!(json.contains("\"only_if_rows\":{\"after\":\"count\"}"));
        let plain = bundle(vec![], vec![read("pin", "snapshot")]);
        assert!(!plain.canonical().contains("placeholders"));
        assert!(!plain.canonical().contains("only_if_rows"));
    }

    #[test]
    fn a_later_non_count_or_unknown_guard_is_refused() {
        let later = bundle(
            vec![],
            vec![
                delete("drop", "DELETE FROM t", Some("count")),
                read("count", "n"),
            ],
        );
        assert!(!later.well_formed(), "a guard after the statement");
        let not_count = bundle(
            vec![],
            vec![
                delete("first", "DELETE FROM t", None),
                delete("drop", "DELETE FROM t", Some("first")),
            ],
        );
        assert!(!not_count.well_formed(), "a guard that is no count");
        let unknown = bundle(vec![], vec![delete("drop", "DELETE FROM t", Some("x"))]);
        assert!(!unknown.well_formed());
    }

    #[test]
    fn undeclared_late_or_text_tokens_are_refused() {
        let undeclared = bundle(vec![], vec![delete("drop", PINNED, None)]);
        assert!(!undeclared.well_formed());
        let late = bundle(
            vec![snapshot()],
            vec![delete("drop", PINNED, None), read("pin", "snapshot")],
        );
        assert!(!late.well_formed(), "filled from a later answer");
        let mut wrong_column = snapshot();
        wrong_column.from.column = "other".into();
        let b = bundle(
            vec![wrong_column],
            vec![read("pin", "snapshot"), delete("drop", PINNED, None)],
        );
        assert!(!b.well_formed(), "a column the answer has not");
        let twice = bundle(vec![snapshot(), snapshot()], vec![read("pin", "snapshot")]);
        assert!(!twice.well_formed());
        for sql in ["{{ x }}", "{{X}}", "a }} b", "{{x", "{{x}} }}"] {
            assert_eq!(tokens(sql), None, "{sql}");
        }
        assert_eq!(tokens("a {{x}} b {{y_1}}"), Some(vec!["x", "y_1"]));
    }

    #[test]
    fn only_decimal_integers_fill_a_token() {
        let p = snapshot();
        assert_eq!(
            fill(PINNED, &[(&p, "8273645512309")]).as_deref(),
            Some("DELETE FROM t FOR VERSION AS OF 8273645512309 WHERE x = 1")
        );
        for v in ["", "-1", "1 OR 1=1", "12345678901234567890", "1e5", "0x1"] {
            assert_eq!(fill(PINNED, &[(&p, v)]), None, "{v}");
        }
        assert!(PlaceholderType::Bigint.takes("-9223372036854775808"));
        assert!(!PlaceholderType::Bigint.takes("9223372036854775808"));
        assert!(!PlaceholderType::Bigint.takes("-"));
        assert_eq!(fill(PINNED, &[]), None, "a token without its value");
    }
}
