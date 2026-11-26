//! Building a bundle's statements in order: reads, counts, writes and the
//! deletes guarded by a count (`only_if_rows`, amendment A1).

use quality::bundle::{BundleStatement, Expect, OnlyIfRows, Placeholder, RunAs};
use quality_rules_trino::Statement;

use crate::CompileError;

/// The column Trino names an unaliased `count(*)`.
const COUNT: &str = "_col0";

#[derive(Debug, Default)]
pub(crate) struct Steps {
    pub(crate) placeholders: Vec<Placeholder>,
    pub(crate) statements: Vec<BundleStatement>,
}

/// A statement's SQL with nothing bound: the statements here take no cut.
fn unbound(s: Statement) -> Result<String, CompileError> {
    if s.cut_params == 0 {
        Ok(s.sql)
    } else {
        Err(CompileError::Incomplete("cut_params"))
    }
}

impl Steps {
    fn push(&mut self, id: String, sql: String, expects: Expect, after: Option<String>) {
        self.statements.push(BundleStatement {
            id,
            sql,
            expects,
            run_as: RunAs::Job,
            reads_source: false,
            only_if_rows: after.map(|after| OnlyIfRows { after }),
        });
    }

    /// An `INSERT`.
    pub(crate) fn insert(&mut self, id: String, s: Statement) -> Result<(), CompileError> {
        self.push(id, unbound(s)?, Expect::insert(), None);
        Ok(())
    }

    /// A count, then the `DELETE` it guards: `<id>_rows`, then `<id>`.
    pub(crate) fn count_then_delete(
        &mut self,
        id: &str,
        count: Statement,
        delete: Statement,
    ) -> Result<(), CompileError> {
        let rows = format!("{id}_rows");
        let expects = Expect::Rows {
            columns: vec![(COUNT.into(), "bigint".into())],
            max_rows: 1,
        };
        self.push(rows.clone(), unbound(count)?, expects, None);
        self.push(id.into(), unbound(delete)?, Expect::delete(), Some(rows));
        Ok(())
    }
}
