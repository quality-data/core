//! A job's bare counts through gold (decision 0090, ruling 53): a count
//! over silver, bronze or the scratch rows, which the job's own user may
//! not read back (point 7.3), goes into `job_counts` by an
//! `INSERT … SELECT`; the guard that waits on it, or the placeholder it
//! fills, reads it back as the dataset's report user, which reads gold.
//! The table carries `dataset` first, so gold's row filter by dataset
//! holds for it as for every other gold table.

use quality_rules_compile::Compiled;
use quality_rules_sql::{quote_text, Cut, Tables};

use crate::gold::GOLD;
use crate::rules::rule_counts;
use crate::{Error, Statement};

/// The counts table, made at install with gold.
pub const JOB_COUNTS: &str = "\"lake\".\"quality_gold\".\"job_counts\"";

/// The column a count is read back as.
pub const COUNT_COLUMN: &str = "n";

/// The install's statement for the table.
pub(crate) fn create() -> Statement {
    Statement {
        sql: format!(
            "CREATE TABLE IF NOT EXISTS {GOLD}.\"job_counts\" \
             (dataset VARCHAR, run VARCHAR, step VARCHAR, n BIGINT) \
             WITH (partitioning = ARRAY['dataset'])"
        ),
    }
}

fn of_run(dataset: &str, run: &str) -> Result<String, Error> {
    Ok(format!(
        "{JOB_COUNTS} WHERE dataset = {} AND run = {}",
        quote_text(dataset)?,
        quote_text(run)?
    ))
}

/// `count`, a query of one row of one `bigint`, written as the run's
/// count `step`; run as the job's own user.
pub fn count_into(
    dataset: &str,
    run: &str,
    step: &str,
    count: &Statement,
) -> Result<Statement, Error> {
    Ok(Statement {
        sql: format!(
            "INSERT INTO {JOB_COUNTS} (dataset, run, step, n) \
             SELECT {}, {}, {}, CAST(q.c AS BIGINT) FROM ({}) AS q (c)",
            quote_text(dataset)?,
            quote_text(run)?,
            quote_text(step)?,
            count.sql.replace('\n', " ")
        ),
    })
}

/// The run's count `step` read back: one row, one `bigint` named
/// [`COUNT_COLUMN`]; run as the report user.
pub fn count_read(dataset: &str, run: &str, step: &str) -> Result<Statement, Error> {
    Ok(Statement {
        sql: format!(
            "SELECT n FROM {} AND step = {}",
            of_run(dataset, run)?,
            quote_text(step)?
        ),
    })
}

/// The counts an earlier attempt of the run left, read as the report
/// user: a retry clears them first, so each step is written once.
pub fn run_counts(dataset: &str, run: &str) -> Result<Statement, Error> {
    Ok(Statement {
        sql: format!("SELECT count(*) FROM {}", of_run(dataset, run)?),
    })
}

/// An earlier attempt's counts go, only after [`run_counts`] showed some
/// (an empty `DELETE` fails its commit).
pub fn delete_run_counts(dataset: &str, run: &str) -> Result<Statement, Error> {
    Ok(Statement {
        sql: format!("DELETE FROM {}", of_run(dataset, run)?),
    })
}

/// A rule test's counts (ruling 54): per rule of `rules`, in order, its
/// failing, passing and skipped records over `from` at `cut`, written as
/// the steps `<i>.fails`, `<i>.passes` and `<i>.skips`, `<i>` its index.
pub fn rule_test_counts(
    dataset: &str,
    run: &str,
    from: &str,
    rules: &[Compiled],
    tables: &Tables,
    cut: &Cut,
) -> Result<Statement, Error> {
    let counts = rule_counts(from, rules, tables, cut)?;
    let mut names = Vec::new();
    let mut rows = Vec::new();
    for i in 0..rules.len() {
        names.push(format!("f{i}, p{i}, s{i}"));
        for (c, what) in [("f", "fails"), ("p", "passes"), ("s", "skips")] {
            rows.push(format!("ROW('{i}.{what}', {c}{i})"));
        }
    }
    if rules.is_empty() {
        names.push("c".into());
        rows.push("ROW('records', c)".into());
    }
    Ok(Statement {
        sql: format!(
            "INSERT INTO {JOB_COUNTS} (dataset, run, step, n) \
             SELECT {}, {}, u.step, u.n FROM ({}) AS c ({}) \
             CROSS JOIN UNNEST(ARRAY[{}]) AS u (step, n)",
            quote_text(dataset)?,
            quote_text(run)?,
            counts.sql,
            names.join(", "),
            rows.join(", ")
        ),
    })
}

/// Every count of the run, by step: what a rule test hands back (counts
/// only, point 7.3); run as the report user.
pub fn run_count_rows(dataset: &str, run: &str) -> Result<Statement, Error> {
    Ok(Statement {
        sql: format!(
            "SELECT step, n FROM {} ORDER BY step ASC NULLS LAST",
            of_run(dataset, run)?
        ),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_count_goes_in_by_insert_and_comes_back_by_its_step() {
        let count = Statement {
            sql: "SELECT count(*)\nFROM s".into(),
        };
        assert_eq!(
            count_into("loans", "r-1", "scratch", &count).unwrap().sql,
            "INSERT INTO \"lake\".\"quality_gold\".\"job_counts\" (dataset, run, step, n) \
             SELECT 'loans', 'r-1', 'scratch', CAST(q.c AS BIGINT) FROM (SELECT count(*) FROM s) AS q (c)"
        );
        assert_eq!(
            count_read("loans", "r-1", "scratch").unwrap().sql,
            "SELECT n FROM \"lake\".\"quality_gold\".\"job_counts\" \
             WHERE dataset = 'loans' AND run = 'r-1' AND step = 'scratch'"
        );
        assert!(run_counts("loans", "r-1")
            .unwrap()
            .sql
            .starts_with("SELECT count(*) FROM"));
        assert!(delete_run_counts("loans", "r-1")
            .unwrap()
            .sql
            .ends_with("WHERE dataset = 'loans' AND run = 'r-1'"));
        assert!(count_read("lo\u{0}ans", "r", "s").is_err());
        assert!(create()
            .sql
            .ends_with("WITH (partitioning = ARRAY['dataset'])"));
    }

    #[test]
    fn a_rule_test_writes_three_counts_per_rule_and_reads_them_back() {
        use quality_rules_compile::{compile, FieldType, Schema};
        let yaml = "dataset: loans\nrules:\n  \
                    - { id: R1, field: Code, check: required, severity: major, alert: a }\n";
        let mut schema = Schema::default();
        schema.fields.insert("Code".into(), FieldType::Text);
        let (compiled, _) = compile(&quality_rules_model::parse(yaml).unwrap(), &schema);
        let cut = Cut::parse("2026-10-31").unwrap();
        let s = rule_test_counts("loans", "r-1", "t", &compiled, &Tables::new(), &cut)
            .unwrap()
            .sql;
        assert!(s.starts_with(
            "INSERT INTO \"lake\".\"quality_gold\".\"job_counts\" (dataset, run, step, n) \
             SELECT 'loans', 'r-1', u.step, u.n FROM (WITH r AS"
        ));
        assert!(s.ends_with(
            "AS c (f0, p0, s0) CROSS JOIN UNNEST(ARRAY[ROW('0.fails', f0), \
             ROW('0.passes', p0), ROW('0.skips', s0)]) AS u (step, n)"
        ));
        let none = rule_test_counts("loans", "r-1", "t", &[], &Tables::new(), &cut).unwrap();
        assert!(none
            .sql
            .contains("AS c (c) CROSS JOIN UNNEST(ARRAY[ROW('records', c)])"));
        assert!(run_count_rows("loans", "r-1")
            .unwrap()
            .sql
            .ends_with("AND run = 'r-1' ORDER BY step ASC NULLS LAST"));
    }
}
