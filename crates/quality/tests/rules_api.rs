//! A dataset's rules for people, driven without a socket over a catalog in
//! memory, with the `Caller` server would put in each request (decision
//! 0019): `main`'s rules files read with each rule in words and their
//! findings, and a rules file checked as typed against the dataset's
//! mapping; only for whoever may view the dataset's rules. Synthetic names
//! only.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::{Extension, Router};
use contracts::{Access, AuditEntry, AuditTrail, Caller, Module};
use quality::draft_api::{Approval, DraftCatalog, DraftFiles, DraftInfo};
use quality::module::QualityModule;
use serde_json::{json, Value};
use tower::ServiceExt;

const MAPPING: &str =
    "dataset: loans\nsource: loans\nkey: [Id]\nfields:\n  Id: { column: id, type: text }\n";
const GOOD: &str = "dataset: loans\nrules:\n  - id: L-01\n    field: Id\n    check: required\n    severity: major\n    cause: capture\n    alert: Empty\n";
const BAD: &str = "dataset: loans\nrules:\n  - id: L-01\n    field: Nope\n    check: required\n    severity: major\n    cause: capture\n    alert: Empty\n";

const GLOBALS: &str = "dataset: loans\nnamed:\n  filled:\n    check: required\n    severity: major\n    cause: capture\n    alert: Empty\n";
const EXTENDS: &str = "dataset: loans\nrules:\n  - id: L-01\n    field: Id\n    extends: filled\n";

/// `main` has `loans` with one rules file, and `people` with the table's
/// global rules and a file that extends them; nothing else.
struct Main;

#[async_trait::async_trait]
impl DraftCatalog for Main {
    async fn list(&self, _: &str) -> Result<Vec<DraftInfo>, String> {
        Ok(Vec::new())
    }
    async fn read(&self, branch: &str, dataset: &str) -> Result<Option<DraftFiles>, String> {
        // Rules are read from main, and a draft of `people` for its global rules.
        if branch == "proposal/people/d-1" {
            return Ok(Some(DraftFiles {
                commit: "c2".into(),
                mapping: MAPPING.into(),
                rules: vec![(
                    "reglas_globales.yaml".into(),
                    GLOBALS.replace("filled", "fresh"),
                )],
            }));
        }
        assert_eq!(branch, "main", "rules are read from main");
        let rules = match dataset {
            "loans" => vec![("Id.yaml".into(), GOOD.into())],
            // With the table's global rules (decision 0045).
            "people" => vec![
                ("reglas_globales.yaml".into(), GLOBALS.into()),
                ("Id.yaml".into(), EXTENDS.into()),
            ],
            _ => return Ok(None),
        };
        Ok(Some(DraftFiles {
            commit: "c1".into(),
            mapping: MAPPING.into(),
            rules,
        }))
    }
    async fn approve(&self, _: &str, _: &str, _: &str) -> Result<Approval, String> {
        unreachable!("nothing here writes")
    }
    async fn discard(&self, _: &str) -> Result<bool, String> {
        unreachable!("nothing here writes")
    }
    async fn save_file(
        &self,
        _: &quality::draft_api::FileEdit<'_>,
        _: bool,
    ) -> Result<quality::draft_api::FileSaved, String> {
        unreachable!("no edits here")
    }
}

/// May view the rules of `loans` and `gone` (not on `main`), not `cards`'.
struct Analyst;
impl Access for Analyst {
    fn can(&self, action: &str, resource: &str) -> bool {
        action == "quality:ViewRules"
            && matches!(
                resource,
                "catalog/loans" | "catalog/people" | "catalog/gone"
            )
    }
    fn can_some(&self, action: &str) -> bool {
        action == "quality:ViewRules"
    }
}

struct NoLog;
#[async_trait::async_trait]
impl AuditTrail for NoLog {
    async fn record(&self, _: &Caller, _: &AuditEntry) -> Result<(), String> {
        unreachable!("reading is not audited")
    }
}

fn router() -> Router {
    QualityModule::default()
        .with_drafts(Arc::new(Main))
        .user_router()
}

async fn call(method: &str, uri: &str, body: Option<Value>) -> (StatusCode, Value) {
    let caller = Caller::new(
        "00000000-0000-0000-0000-0000000000a1".into(),
        None,
        "192.0.2.1".into(),
        None,
        Arc::new(Analyst),
        Arc::new(NoLog),
    );
    let req = Request::builder()
        .method(method)
        .uri(uri)
        .header("content-type", "application/json")
        .body(body.map_or_else(Body::empty, |b| Body::from(b.to_string())))
        .unwrap();
    let res = router()
        .layer(Extension(caller))
        .oneshot(req)
        .await
        .unwrap();
    let status = res.status();
    let bytes = axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

#[tokio::test]
async fn mains_rules_are_read_each_in_words() {
    let (status, out) = call("GET", "/quality/datasets/loans/rules", None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(out["commit"], "c1");
    assert_eq!(out["mapping_reads"], true);
    let file = &out["files"][0];
    assert_eq!(file["file"], "Id.yaml");
    assert_eq!(file["findings"], json!([]), "in its field's own file");
    let rule = &file["rules"][0];
    assert_eq!(rule["id"], "L-01");
    assert_eq!(rule["field"], "Id");
    assert_eq!(rule["reads"], json!(["Id"]), "the fields it reads");
    assert_eq!(rule["pattern"], "required");
    for part in [
        "kind",
        "dimension",
        "severity",
        "applies",
        "fails",
        "duckdb",
        "trino",
    ] {
        assert!(
            rule[part].as_str().is_some_and(|s| !s.is_empty()),
            "{part}: {rule}"
        );
    }
}

#[tokio::test]
async fn only_whoever_may_view_a_datasets_rules_reads_them() {
    let (status, _) = call("GET", "/quality/datasets/cards/rules", None).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    let (status, _) = call("GET", "/quality/datasets/gone/rules", None).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, _) = call("GET", "/quality/datasets/a%20b/rules", None).await;
    assert_eq!(status, StatusCode::FORBIDDEN, "not a dataset's name");
}

#[tokio::test]
async fn a_rules_file_is_checked_as_typed() {
    let check = |yaml: &str, dataset: &str| {
        let body = json!({ "dataset": dataset, "yaml": yaml });
        async move { call("POST", "/quality/rules/check", Some(body)).await }
    };
    let (status, out) = check(GOOD, "loans").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(out["findings"], json!([]));
    assert_eq!(out["rules"][0]["id"], "L-01");
    assert_eq!(out["fields"], json!(["Id"]), "for completion");

    let (status, out) = check(BAD, "loans").await;
    assert_eq!(status, StatusCode::OK);
    let finding = &out["findings"][0];
    assert!(
        finding["code"].as_str().unwrap().starts_with("QR"),
        "{finding}"
    );
    assert_eq!(finding["severity"], "error");
    assert_eq!(finding["rule"], "L-01");
    assert!(finding["line"].as_u64().is_some(), "{finding}");
    assert!(!finding["message"].as_str().unwrap().is_empty());

    let (status, out) = check("rules: [", "loans").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(out["findings"][0]["severity"], "error", "a syntax error");

    assert_eq!(check(GOOD, "cards").await.0, StatusCode::FORBIDDEN);
    assert_eq!(check(GOOD, "gone").await.0, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn a_file_named_says_which_rules_are_not_its_fields() {
    let check = |file: &str| {
        let body = json!({ "dataset": "loans", "yaml": GOOD, "file": file });
        async move { call("POST", "/quality/rules/check", Some(body)).await }
    };
    let (status, out) = check("Id.yaml").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(out["findings"], json!([]), "L-01 is Id's: in its own file");
    let (_, out) = check("amounts.yaml").await;
    let finding = &out["findings"][0];
    assert_eq!(
        (finding["code"].as_str(), finding["severity"].as_str()),
        (Some("QR210"), Some("warning"))
    );
    assert!(
        finding["message"].as_str().unwrap().contains("Id.yaml"),
        "{finding}"
    );
}

#[tokio::test]
async fn a_rule_that_extends_a_global_one_reads_as_what_it_does() {
    let (status, out) = call("GET", "/quality/datasets/people/rules", None).await;
    assert_eq!(status, StatusCode::OK);
    let files = out["files"].as_array().unwrap();
    let file = files.iter().find(|f| f["file"] == "Id.yaml").unwrap();
    assert_eq!(file["findings"], json!([]), "{file}");
    let rule = &file["rules"][0];
    assert_eq!(rule["pattern"], "required", "the named rule's");
    assert_eq!(rule["extends"], json!({ "named": "filled", "line": 5 }));
    assert!(rule["fails"].as_str().is_some_and(|s| !s.is_empty()));

    // Checked as typed, with main's global rules: an unknown one is an error.
    let body = json!({ "dataset": "people", "yaml": EXTENDS.replace("filled", "nowhere"), "file": "Id.yaml" });
    let (_, out) = call("POST", "/quality/rules/check", Some(body)).await;
    assert_eq!(out["findings"][0]["code"], "QR012", "{out}");

    // On a draft, with the draft's global rules: a named rule only it has.
    let fresh = EXTENDS.replace("filled", "fresh");
    let on = |draft: Option<&str>| json!({ "dataset": "people", "yaml": fresh, "file": "Id.yaml", "draft": draft });
    let (_, out) = call("POST", "/quality/rules/check", Some(on(Some("d-1")))).await;
    assert_eq!(out["findings"], json!([]), "{out}");
    let (_, out) = call("POST", "/quality/rules/check", Some(on(None))).await;
    assert_eq!(
        out["findings"][0]["code"], "QR012",
        "main has no such named rule"
    );
}

#[tokio::test]
async fn the_yaml_schema_is_served_for_completion() {
    let (status, out) = call("GET", "/quality/rules/schema", None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        out,
        serde_json::to_value(quality_rules::json_schema()).unwrap()
    );
    assert!(out["$defs"]["Pattern"]["enum"].is_array());
}

#[tokio::test]
async fn a_rules_file_is_tidied() {
    let untidy = GOOD.replace("check: required", "check: 'required'  ");
    let (status, out) = call(
        "POST",
        "/quality/rules/tidy",
        Some(json!({ "yaml": untidy })),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(out["yaml"], GOOD);
}
