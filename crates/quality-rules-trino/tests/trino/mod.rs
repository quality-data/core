//! A test's own client of Trino's HTTP protocol, for the contract tier A
//! (decision 0090, step 9): it posts one statement, follows `nextUri` and
//! gives the rows, or Trino's error name. The core has no Trino client;
//! this one lives in tests only.

#![allow(dead_code)]

use serde_json::Value;

/// Trino's address from `QD_TRINO_URL`; `None` skips the tier (CI sets it
/// to a Trino 484 container with `regex-library=RE2J`).
pub(crate) fn url() -> Option<String> {
    std::env::var("QD_TRINO_URL").ok().filter(|u| !u.is_empty())
}

/// What a statement gave: its rows, or the error Trino named.
#[derive(Debug)]
pub(crate) enum Answer {
    Rows(Vec<Vec<Value>>),
    Error(String),
}

/// Runs `sql` as the user `qd-tier-a`.
pub(crate) async fn run(base: &str, sql: &str) -> Answer {
    let http = reqwest::Client::new();
    let mut page: Value = http
        .post(format!("{base}/v1/statement"))
        .header("X-Trino-User", "qd-tier-a")
        .body(sql.to_string())
        .send()
        .await
        .expect("Trino answers")
        .json()
        .await
        .expect("a JSON page");
    let mut rows = Vec::new();
    loop {
        if let Some(error) = page.get("error") {
            let name = error["errorName"].as_str().unwrap_or("UNKNOWN");
            return Answer::Error(name.to_string());
        }
        if let Some(data) = page.get("data").and_then(Value::as_array) {
            rows.extend(
                data.iter()
                    .map(|r| r.as_array().cloned().unwrap_or_default()),
            );
        }
        let Some(next) = page.get("nextUri").and_then(Value::as_str) else {
            return Answer::Rows(rows);
        };
        page = http
            .get(next)
            .header("X-Trino-User", "qd-tier-a")
            .send()
            .await
            .expect("Trino answers")
            .json()
            .await
            .expect("a JSON page");
    }
}

/// The rows of `sql`, failing the test on an error.
pub(crate) async fn rows(base: &str, sql: &str) -> Vec<Vec<Value>> {
    match run(base, sql).await {
        Answer::Rows(rows) => rows,
        Answer::Error(e) => panic!("{e}: {sql}"),
    }
}
