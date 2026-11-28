//! Building a bundle's statements in order: reads, counts, writes and the
//! deletes guarded by a count (`only_if_rows`, amendment A1).

use quality::bundle::{BundleStatement, Expect, OnlyIfRows, Placeholder, RunAs};
use quality_rules_trino::Statement;

/// The column Trino names an unaliased `count(*)`.
const COUNT: &str = "_col0";

#[derive(Debug, Default)]
pub(crate) struct Steps {
    pub(crate) placeholders: Vec<Placeholder>,
    pub(crate) statements: Vec<BundleStatement>,
}

impl Steps {
    fn push(&mut self, id: String, sql: String, expects: Expect, after: Option<String>) {
        // A count reads gold, which only the report user may (point 7.3:
        // a bare `SELECT` by a rule user is refused).
        let run_as = match expects {
            Expect::Rows { .. } => RunAs::Report,
            _ => RunAs::Job,
        };
        self.statements.push(BundleStatement {
            id,
            sql,
            expects,
            run_as,
            reads_source: false,
            only_if_rows: after.map(|after| OnlyIfRows { after }),
        });
    }

    /// An `INSERT`.
    pub(crate) fn insert(&mut self, id: String, s: Statement) {
        self.push(id, s.sql, Expect::insert(), None);
    }

    /// A count, then the `DELETE` it guards: `<id>_rows`, then `<id>`.
    pub(crate) fn count_then_delete(&mut self, id: &str, count: Statement, delete: Statement) {
        let rows = format!("{id}_rows");
        let expects = Expect::Rows {
            columns: vec![(COUNT.into(), "bigint".into())],
            max_rows: 1,
        };
        self.push(rows.clone(), count.sql, expects, None);
        self.push(id.into(), delete.sql, Expect::delete(), Some(rows));
    }
}
