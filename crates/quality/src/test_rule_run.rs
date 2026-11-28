//! A `test_rule` job run on Trino (decision 0043; decision 0090, ruling
//! 83): the rules file a person typed, compiled against the dataset's
//! mapping on the catalog's `main`, tested on the asked cut as one values
//! read per rule by `job_sample_<d>`; each read's rows, posted on the
//! values route, become that rule's counts, records and failure groups in
//! today's test report, kept for its asker and masked on every read.

use axum::http::StatusCode;
use chrono::NaiveDate;
use serde_json::Value;
use sqlx::PgPool;
use uuid::Uuid;

use crate::catalog::Catalog;
use crate::job_inputs::{cut_load, references, FactsError, ReferenceFacts};
use crate::mapping::Mapping;
use crate::rule_test::{FailGroup, FieldValue, RuleTested, TestReport, TestedRecord, EACH};
use crate::run_rows_api::{internal, refused, Refusal, RunRows, ValuesRun};
use quality_rules::check::{compile, Compiled};
use quality_rules::sql::Tables;

/// The job's kind.
pub const KIND: &str = "test_rule";
/// The failure groups a test reads, largest first (decision 0043).
pub const GROUPS: u64 = 20;

/// The id of the `i`-th rule's read.
pub fn rule_read(i: usize) -> String {
    format!("rule_{i}")
}

/// A queued test as the compile reads it.
#[derive(Debug, Clone)]
pub struct TestRuleFacts {
    pub dataset: String,
    pub cut: NaiveDate,
    pub commit: String,
    pub mapping: Mapping,
    /// The rules asked: one (and its parts), or every rule of the file.
    pub rules: Vec<Compiled>,
    pub tables: Tables,
    /// Each referenced dataset's silver and the load of the same cut,
    /// read pinned as an evaluate reads them (ruling 105).
    pub references: Vec<ReferenceFacts>,
    /// The cut's extraction, the load its silver rows carry.
    pub load: Option<String>,
    /// Fields shown besides the key and those a rule reads.
    pub columns: Vec<String>,
    pub each: u64,
}

fn failed(e: impl std::fmt::Display) -> FactsError {
    FactsError::Failed(e.to_string())
}

/// The `test_rule` job `run`, its text compiled against the dataset's
/// mapping on `main`; a text that no longer compiles refuses the job.
pub async fn facts_of(
    pool: &PgPool,
    catalog: &dyn Catalog,
    run: &str,
) -> Result<TestRuleFacts, FactsError> {
    let id = Uuid::parse_str(run).map_err(|_| FactsError::Refused("invalid_run"))?;
    let found: Option<(Option<String>, Value)> =
        sqlx::query_as("SELECT dataset, parameters FROM quality.jobs WHERE id = $1 AND kind = $2")
            .bind(id)
            .bind(KIND)
            .fetch_optional(pool)
            .await
            .map_err(failed)?;
    let (dataset, p) = found.ok_or(FactsError::Refused("no_job"))?;
    let dataset = dataset.ok_or(FactsError::Refused("job_incomplete"))?;
    let text = |k: &str| p.get(k).and_then(Value::as_str);
    let cut = text("cut")
        .and_then(|c| NaiveDate::parse_from_str(c, "%Y-%m-%d").ok())
        .ok_or(FactsError::Refused("job_incomplete"))?;
    let yaml = text("yaml").ok_or(FactsError::Refused("job_incomplete"))?;
    let snapshot = catalog
        .snapshot(&dataset)
        .await
        .map_err(FactsError::Failed)?
        .ok_or(FactsError::Refused("no_mapping"))?;
    let mapping =
        Mapping::parse(&snapshot.mapping).map_err(|_| FactsError::Refused("mapping_invalid"))?;
    let file = quality_rules::parse(yaml).map_err(|_| FactsError::Refused("rules_invalid"))?;
    let mut tables = Tables::new();
    let mut referenced = std::collections::BTreeMap::new();
    references(&file, &snapshot.others, &mut tables, &mut referenced)?;
    let (compiled, problems) = compile(&file, &mapping.schema());
    if !problems.is_empty() {
        return Err(FactsError::Refused("rules_invalid"));
    }
    let only = text("rule");
    let rules = compiled
        .into_iter()
        .filter(|r| only.is_none_or(|id| r.id == id || r.id.starts_with(&format!("{id}."))))
        .collect();
    let load = cut_load(pool, &dataset, cut).await?;
    let mut pins = Vec::new();
    for (name, (namespace, table, other)) in referenced {
        let load = cut_load(pool, &other, cut)
            .await?
            .ok_or(FactsError::Refused("reference_cut_missing"))?;
        pins.push(ReferenceFacts {
            name,
            namespace,
            table,
            load,
        });
    }
    let columns = p
        .get("columns")
        .and_then(|c| serde_json::from_value(c.clone()).ok())
        .unwrap_or_default();
    Ok(TestRuleFacts {
        dataset,
        cut,
        commit: snapshot.commit,
        mapping,
        rules,
        tables,
        references: pins,
        load: load.flatten(),
        columns,
        each: p.get("each").and_then(Value::as_u64).unwrap_or(EACH as u64),
    })
}

/// One rule's tested result from its read's rows: the fields row names
/// the values that follow.
fn rule_of(rows: &[Vec<Value>]) -> Option<RuleTested> {
    let text = |v: &Value| v.as_str().map(str::to_owned);
    let head = rows
        .iter()
        .find(|r| r.first().and_then(Value::as_str) == Some("fields"))?;
    let rule = text(head.get(1)?)?;
    let fields: Vec<String> = head[5..].iter().map(text).collect::<Option<_>>()?;
    let values = |r: &[Value]| -> Vec<FieldValue> {
        fields
            .iter()
            .zip(&r[5..])
            .map(|(f, v)| FieldValue {
                field: f.clone(),
                value: text(v),
            })
            .collect()
    };
    let mut t = RuleTested {
        rule,
        applies: 0,
        passes: 0,
        fails: 0,
        skips: 0,
        records: Vec::new(),
        groups: Vec::new(),
    };
    for r in rows {
        let outcome = r.get(2).and_then(text).unwrap_or_default();
        let n = r.get(4).and_then(Value::as_u64).unwrap_or(0);
        match r.first().and_then(Value::as_str) {
            Some("counts") => match outcome.as_str() {
                "fails" => t.fails = n,
                "passes" => t.passes = n,
                "skips" => t.skips = n,
                _ => return None,
            },
            Some("record") => t.records.push(TestedRecord {
                outcome,
                values: values(r),
            }),
            Some("group") => {
                let shape = r.get(3).and_then(text)?;
                let record = TestedRecord {
                    outcome: "fails".into(),
                    values: values(r),
                };
                match t.groups.iter_mut().find(|g| g.shape == shape) {
                    Some(g) => g.records.push(record),
                    None => t.groups.push(FailGroup {
                        shape,
                        count: n,
                        records: vec![record],
                    }),
                }
            }
            Some("fields") => {}
            _ => return None,
        }
    }
    t.applies = t.fails + t.passes;
    t.groups
        .sort_by(|a, b| b.count.cmp(&a.count).then(a.shape.cmp(&b.shape)));
    Some(t)
}

/// A rule's rows merged into the test's report: its rule replaced or
/// added, the cut's records from its counts.
pub(crate) async fn take(pool: &PgPool, run: &ValuesRun, rows: &RunRows) -> Result<(), Refusal> {
    let known = run.bundle.statements.iter().any(|s| s.id == rows.statement);
    if !rows.statement.starts_with("rule_") || !known {
        return Err(refused(StatusCode::UNPROCESSABLE_ENTITY, "not_a_rule_read"));
    }
    let tested = rule_of(&rows.rows)
        .ok_or_else(|| refused(StatusCode::UNPROCESSABLE_ENTITY, "rows_invalid"))?;
    let id =
        Uuid::parse_str(&run.run).map_err(|_| refused(StatusCode::BAD_REQUEST, "invalid_run"))?;
    let cut = run.bundle.cut.clone().unwrap_or_default();
    let mut tx = pool.begin().await.map_err(internal)?;
    let empty = TestReport {
        cut: cut.clone(),
        records: 0,
        rules: Vec::new(),
    };
    sqlx::query("DELETE FROM quality.rule_tests WHERE created_at < now() - interval '1 day'")
        .execute(&mut *tx)
        .await
        .map_err(internal)?;
    sqlx::query(
        "INSERT INTO quality.rule_tests (job, report) VALUES ($1, $2) ON CONFLICT DO NOTHING",
    )
    .bind(id)
    .bind(serde_json::to_value(&empty).map_err(internal)?)
    .execute(&mut *tx)
    .await
    .map_err(internal)?;
    let kept: Value =
        sqlx::query_scalar("SELECT report FROM quality.rule_tests WHERE job = $1 FOR UPDATE")
            .bind(id)
            .fetch_one(&mut *tx)
            .await
            .map_err(internal)?;
    let mut report: TestReport = serde_json::from_value(kept).unwrap_or(empty);
    report.records = tested.applies + tested.skips;
    match report.rules.iter_mut().find(|r| r.rule == tested.rule) {
        Some(r) => *r = tested,
        None => report.rules.push(tested),
    }
    sqlx::query("UPDATE quality.rule_tests SET report = $2 WHERE job = $1")
        .bind(id)
        .bind(serde_json::to_value(&report).map_err(internal)?)
        .execute(&mut *tx)
        .await
        .map_err(internal)?;
    tx.commit().await.map_err(internal)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn rows(v: Value) -> Vec<Vec<Value>> {
        serde_json::from_value(v).unwrap()
    }

    #[test]
    fn a_rules_rows_become_its_tested_result_in_any_order() {
        let got = rule_of(&rows(json!([
            ["group", "R1", "fails", "AAA", 2, "2", null],
            ["counts", "R1", "fails", null, 2, null, null],
            ["fields", "R1", null, null, null, "Id", "Code"],
            ["record", "R1", "passes", null, null, "1", "ABC"],
            ["counts", "R1", "passes", null, 3, null, null],
            ["group", "R1", "fails", "AAA", 2, "3", null],
            ["group", "R1", "fails", "99", 5, "4", "12"]
        ])))
        .unwrap();
        assert_eq!(got.rule, "R1");
        assert_eq!(
            (got.applies, got.fails, got.passes, got.skips),
            (5, 2, 3, 0)
        );
        assert_eq!(got.records.len(), 1);
        assert_eq!(got.records[0].values[1].field, "Code");
        assert_eq!(got.groups.len(), 2);
        assert_eq!(
            (got.groups[0].shape.as_str(), got.groups[0].count),
            ("99", 5)
        );
        assert_eq!(got.groups[1].records.len(), 2);
        assert!(rule_of(&rows(json!([["counts", "R1", "fails", null, 1]]))).is_none());
        assert!(rule_of(&rows(json!([
            ["fields", "R1", null, null, null],
            ["counts", "R1", "odd", null, 1]
        ])))
        .is_none());
    }
}
