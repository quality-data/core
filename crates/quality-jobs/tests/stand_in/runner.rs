//! The contract tier B's stand-in job runner (decision 0090, step 15,
//! ruled 2026-11-25): it runs a bundle's statements in order as the job's
//! or the report user, fills each `{{token}}` from the earlier one-row
//! answer it names, and skips a statement whose `only_if_rows` count was
//! 0, as `engine-trino`'s runner does (amendment A1). Tests only.

#![allow(dead_code)]

use std::collections::BTreeMap;

use quality::bundle::{fill, Bundle, RunAs};
use serde_json::Value;

/// Trino's address from `QD_TIER_B_URL`; `None` skips the tier
/// (`tools/verify/trino_tier_b.sh` sets it to Trino 484 over RustFS).
pub(crate) fn url() -> Option<String> {
    std::env::var("QD_TIER_B_URL")
        .ok()
        .filter(|u| !u.is_empty())
}

/// Runs `sql` as `user`: its rows, or Trino's error name and message.
pub(crate) async fn run(base: &str, user: &str, sql: &str) -> Result<Vec<Vec<Value>>, String> {
    let http = reqwest::Client::new();
    let get = |r: reqwest::RequestBuilder| async move {
        r.header("X-Trino-User", user)
            .send()
            .await
            .map_err(|e| e.to_string())?
            .json::<Value>()
            .await
            .map_err(|e| e.to_string())
    };
    let mut page = get(http
        .post(format!("{base}/v1/statement"))
        .body(sql.to_string()))
    .await?;
    let mut rows = Vec::new();
    loop {
        if let Some(e) = page.get("error") {
            return Err(format!("{}: {}", e["errorName"], e["message"]));
        }
        if let Some(data) = page.get("data").and_then(Value::as_array) {
            rows.extend(
                data.iter()
                    .map(|r| r.as_array().cloned().unwrap_or_default()),
            );
        }
        let Some(next) = page.get("nextUri").and_then(Value::as_str) else {
            return Ok(rows);
        };
        page = get(http.get(next)).await?;
    }
}

/// The rows of `sql` as `user`, failing the test on an error.
pub(crate) async fn rows(base: &str, user: &str, sql: &str) -> Vec<Vec<Value>> {
    run(base, user, sql)
        .await
        .unwrap_or_else(|e| panic!("{e}: {sql}"))
}

/// What a bundle's statements answered, by id; a skipped one is absent.
pub(crate) type Answers = BTreeMap<String, Vec<Vec<Value>>>;

/// Runs `b` as the runner would, the job's statements as `job_user` and
/// the report's as `report_user`.
pub(crate) async fn run_bundle(
    base: &str,
    b: &Bundle,
    job_user: &str,
    report_user: &str,
) -> Result<Answers, String> {
    assert!(b.well_formed(), "a bundle the runner takes");
    let mut answers = Answers::new();
    for s in &b.statements {
        if let Some(g) = &s.only_if_rows {
            let n = answers
                .get(&g.after)
                .and_then(|r| r.first())
                .and_then(|r| r.first())
                .and_then(Value::as_i64)
                .ok_or_else(|| format!("{}: no count from {}", s.id, g.after))?;
            if n == 0 {
                continue;
            }
        }
        let mut values: Vec<(&quality::bundle::Placeholder, String)> = Vec::new();
        for p in &b.placeholders {
            // A typed document is no answer's; tier B runs no lookup.
            let Some(from) = &p.from else {
                continue;
            };
            let Some(rows) = answers.get(&from.statement) else {
                continue;
            };
            let at = column(b, &from.statement, &from.column);
            if let Some(v) = rows.first().and_then(|r| r.get(at)) {
                values.push((p, v.to_string().trim_matches('"').to_string()));
            }
        }
        let filled: Vec<(&quality::bundle::Placeholder, &str)> =
            values.iter().map(|(p, v)| (*p, v.as_str())).collect();
        let sql = fill(&s.sql, &filled).ok_or_else(|| format!("{}: a token unfilled", s.id))?;
        let user = match s.run_as {
            RunAs::Job => job_user,
            RunAs::Report => report_user,
        };
        let got = run(base, user, &sql)
            .await
            .map_err(|e| format!("{}: {e}", s.id))?;
        answers.insert(s.id.clone(), got);
    }
    Ok(answers)
}

/// The position of `column` in the answer of `statement`.
fn column(b: &Bundle, statement: &str, column: &str) -> usize {
    let s = b.statements.iter().find(|s| s.id == statement).unwrap();
    match &s.expects {
        quality::bundle::Expect::Rows { columns, .. } => {
            columns.iter().position(|(n, _)| n == column).unwrap()
        }
        _ => 0,
    }
}
