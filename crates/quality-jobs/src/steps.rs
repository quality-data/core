//! Building a bundle's statements in order: reads, counts, writes and the
//! deletes guarded by a count (`only_if_rows`, amendment A1).

use quality::bundle::{
    BundleStatement, Expect, OnlyIfRows, Placeholder, PlaceholderFrom, PlaceholderType, RunAs,
};
use quality_rules_trino::{self as trino, Statement, COUNT_COLUMN};

use crate::CompileError;

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

    /// An `INSERT` that runs only when the count read `after` showed rows.
    pub(crate) fn insert_after(&mut self, id: String, s: Statement, after: String) {
        self.push(id, s.sql, Expect::insert(), Some(after));
    }

    /// A `DELETE` that runs only when the count read `after` showed rows.
    pub(crate) fn delete_after(&mut self, id: String, s: Statement, after: String) {
        self.push(id, s.sql, Expect::delete(), Some(after));
    }

    /// A read of one row of one `bigint` named `column`, as the report user.
    pub(crate) fn read_one(&mut self, id: String, s: Statement, column: &str) {
        let expects = Expect::Rows {
            columns: vec![(column.into(), "bigint".into())],
            max_rows: 1,
        };
        self.push(id, s.sql, expects, None);
    }

    /// `{{name}}`, filled from `column` of the one-row answer of `statement`.
    pub(crate) fn placeholder(
        &mut self,
        name: &str,
        ty: PlaceholderType,
        statement: &str,
        column: &str,
    ) {
        self.placeholders.push(Placeholder {
            name: name.into(),
            ty,
            from: PlaceholderFrom {
                statement: statement.into(),
                column: column.into(),
            },
        });
    }

    /// A count the job's own user may not read back (ruling 53): written
    /// into gold's `job_counts` as `<id>_count`, read back as the report
    /// user as `<id>_rows`, whose id it returns.
    pub(crate) fn bare_count(
        &mut self,
        dataset: &str,
        run: &str,
        id: &str,
        count: &Statement,
    ) -> Result<String, CompileError> {
        let read = format!("{id}_rows");
        self.insert(
            format!("{id}_count"),
            trino::count_into(dataset, run, id, count)?,
        );
        self.read_one(
            read.clone(),
            trino::count_read(dataset, run, id)?,
            COUNT_COLUMN,
        );
        Ok(read)
    }
}
