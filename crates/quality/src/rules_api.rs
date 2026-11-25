//! A dataset's rules for people (decision 0019, its screens _Rules of a
//! dataset_ and _Rule_), in the module's `user_router`: the rules files
//! `main` has, each rule as read and each file's lint findings; and a rules
//! file checked as it is typed, against the fields of the dataset's mapping
//! on `main`. Both need `quality:ViewRules` on `catalog/<dataset>`; nothing
//! here writes. The texts come in the request's language. The JSON Schema
//! of the YAML is served too, for the editor's completion, and a rules
//! file is tidied (decision 0041).

use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Extension, Json, Router};
use contracts::Caller;
use quality_rules::check::Schema;
use quality_rules::explain::explain;
use quality_rules::lint::{lint_globals, lint_with, placement, Report, Severity};
use quality_rules::model::GLOBAL_FILE;
use quality_rules::sql::Tables;
use serde::{Deserialize, Serialize};
use utoipa::{OpenApi, ToSchema};

use crate::draft_api::{DraftCatalog, VIEW};
use crate::mapping::Mapping;
use crate::refusal::refuse;
use crate::rules_text;
use crate::source::valid_name;

pub(crate) const MAIN: &str = "main";

/// `GET /quality/datasets/{dataset}/rules`, `POST /quality/rules/check`,
/// `GET /quality/rules/schema` and `POST /quality/rules/tidy`.
pub fn routes(catalog: Arc<dyn DraftCatalog>) -> Router {
    Router::new()
        .route("/quality/datasets/{dataset}/rules", get(read))
        .route("/quality/rules/check", post(check))
        .route("/quality/rules/schema", get(schema))
        .route("/quality/rules/tidy", post(tidy))
        .with_state(catalog)
}

/// The routes' OpenAPI paths and schemas.
#[derive(Debug, OpenApi)]
#[openapi(
    paths(read, check, schema, tidy),
    components(schemas(
        RulesOut,
        RulesFileView,
        RuleView,
        ExtendsOut,
        FindingOut,
        CheckBody,
        CheckOut,
        TidyBody
    ))
)]
pub struct RulesDoc;

/// A dataset's rules on `main`.
#[derive(Debug, Serialize, ToSchema)]
pub struct RulesOut {
    /// The `main` commit they were read at.
    pub commit: String,
    /// Whether the dataset's mapping reads; if not, no rule is read.
    pub mapping_reads: bool,
    pub files: Vec<RulesFileView>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct RulesFileView {
    pub file: String,
    pub yaml: String,
    pub rules: Vec<RuleView>,
    pub findings: Vec<FindingOut>,
}

/// A rule as read (decision 0019, point 9).
#[derive(Debug, Serialize, ToSchema)]
pub struct RuleView {
    pub id: String,
    pub field: Option<String>,
    /// The dataset's fields it reads, each once; a derived field's are
    /// those it is computed from (decision 0043, point 2).
    pub reads: Vec<String>,
    /// Its pattern as the YAML writes it (`required`, `format`, …).
    pub pattern: String,
    /// Of one field, or across fields; in words.
    pub kind: String,
    pub dimension: String,
    pub severity: String,
    pub applies: String,
    pub fails: String,
    pub skips: Option<String>,
    /// Its SQL on DuckDB, unless it reads a reference not resolved here.
    pub duckdb: Option<String>,
    pub trino: Option<String>,
    /// The named rule it extends and the line that says so: this rule,
    /// as read, is what it does (decision 0045).
    pub extends: Option<ExtendsOut>,
}

/// A rule's `extends`: the named rule, and the line where it is cited.
#[derive(Debug, Serialize, ToSchema)]
pub struct ExtendsOut {
    pub named: String,
    pub line: u64,
}

/// A lint finding: its `QR` code, how serious, where and what it says.
#[derive(Debug, Serialize, ToSchema)]
pub struct FindingOut {
    pub code: String,
    /// `error` blocks saving; `warning` does not.
    pub severity: String,
    pub rule: Option<String>,
    pub line: Option<u64>,
    pub column: Option<u64>,
    pub message: String,
}

/// A rules file to check, of `dataset`.
#[derive(Debug, Deserialize, ToSchema)]
pub struct CheckBody {
    pub dataset: String,
    pub yaml: String,
    /// The file's name, to say which rules are not in their field's file
    /// (decision 0044).
    pub file: Option<String>,
    /// The draft it is typed on: checked with that draft's global rules
    /// (decision 0045); `main`'s otherwise.
    pub draft: Option<String>,
}

/// What a rules file says, as typed.
#[derive(Debug, Serialize, ToSchema)]
pub struct CheckOut {
    pub rules: Vec<RuleView>,
    pub findings: Vec<FindingOut>,
    /// The fields of the dataset's mapping, for completion (only when
    /// checked through `POST /quality/rules/check`).
    pub fields: Vec<String>,
}

/// The table's global rules among a dataset's files, if it has them (0045).
pub(crate) fn global_rules(files: &[(String, String)]) -> Option<&str> {
    files
        .iter()
        .find(|(file, _)| file == GLOBAL_FILE)
        .map(|(_, yaml)| yaml.as_str())
}

/// The global rules of a draft's branch, when it is one and has them;
/// otherwise `main`'s, from `files` (decision 0045).
pub(crate) async fn draft_globals(
    catalog: &dyn DraftCatalog,
    dataset: &str,
    branch: Option<&str>,
    files: &[(String, String)],
) -> Option<String> {
    if let Some(branch) = branch {
        if let Ok(Some(draft)) = catalog.read(branch, dataset).await {
            return global_rules(&draft.rules).map(str::to_string);
        }
    }
    global_rules(files).map(str::to_string)
}

/// A rules file linted: the global rules themselves as such; any other
/// with them, and with its name the rules not in their field's file (0044).
pub(crate) fn linted(
    yaml: &str,
    schema: &Schema,
    file: Option<&str>,
    globals: Option<&str>,
) -> Report {
    if file == Some(GLOBAL_FILE) {
        return lint_globals(yaml, schema);
    }
    let mut report = lint_with(yaml, schema, globals);
    if let Some(file) = file {
        let placed = placement(file, yaml, &report);
        report.findings.extend(placed);
    }
    report
}

/// A rules file linted against `schema`, with the table's global rules,
/// and each compiled rule explained.
pub(crate) fn view(
    yaml: &str,
    schema: &Schema,
    file: Option<&str>,
    globals: Option<&str>,
) -> CheckOut {
    let language = texts::current();
    let report = linted(yaml, schema, file, globals);
    let extends = |id: &str| {
        report
            .extends
            .iter()
            .find(|e| id == e.rule || id.starts_with(&format!("{}.", e.rule)))
            .map(|e| ExtendsOut {
                named: e.named.clone(),
                line: e.at.line,
            })
    };
    let tables = Tables::new();
    let rules = report
        .compiled
        .iter()
        .map(|rule| {
            let e = explain(rule, language, &tables);
            RuleView {
                id: e.id,
                field: rule.field.clone(),
                reads: rule.reads(),
                pattern: rule.pattern.name().to_string(),
                kind: e.kind,
                dimension: e.dimension,
                severity: e.severity,
                applies: e.applies,
                fails: e.fails,
                skips: e.skips,
                duckdb: e.duckdb,
                trino: e.trino,
                extends: extends(&rule.id),
            }
        })
        .collect();
    let findings = report
        .findings
        .iter()
        .map(|f| FindingOut {
            code: f.code.id().to_string(),
            severity: match f.severity() {
                Severity::Error => "error",
                Severity::Warning => "warning",
            }
            .to_string(),
            rule: f.rule.clone(),
            line: f.at.map(|p| p.line),
            column: f.at.map(|p| p.column),
            message: rules_text::message(language, f),
        })
        .collect();
    CheckOut {
        rules,
        findings,
        fields: Vec::new(),
    }
}

/// The dataset's mapping and rules files on `main`, if it has them.
pub(crate) async fn on_main(
    catalog: &dyn DraftCatalog,
    dataset: &str,
) -> Result<Option<crate::draft_api::DraftFiles>, (StatusCode, &'static str)> {
    catalog.read(MAIN, dataset).await.map_err(|e| {
        tracing::error!("rules: catalog: {e}");
        let code = crate::refusal::catalog_code(&e);
        (StatusCode::SERVICE_UNAVAILABLE, code)
    })
}

/// A dataset's rules files on `main`, each rule as read and its findings.
#[utoipa::path(get, path = "/quality/datasets/{dataset}/rules", operation_id = "quality_read_rules",
    tag = "rules", params(("dataset" = String, Path)),
    responses((status = 200, body = RulesOut), (status = 401), (status = 403), (status = 404),
        (status = 503, description = "No catalog to read them")))]
async fn read(
    State(catalog): State<Arc<dyn DraftCatalog>>,
    Extension(caller): Extension<Caller>,
    Path(dataset): Path<String>,
) -> Response {
    if !valid_name(&dataset) || !caller.can(VIEW, &format!("catalog/{dataset}")) {
        return refuse(StatusCode::FORBIDDEN, "forbidden");
    }
    let files = match on_main(catalog.as_ref(), &dataset).await {
        Ok(Some(files)) => files,
        Ok(None) => return refuse(StatusCode::NOT_FOUND, "not_found"),
        Err((code, error)) => return refuse(code, error),
    };
    let schema = Mapping::parse(&files.mapping).ok().map(|m| m.schema());
    let globals = global_rules(&files.rules).map(str::to_string);
    let views = files
        .rules
        .into_iter()
        .map(|(file, yaml)| {
            let out = schema.as_ref().map_or(
                CheckOut {
                    rules: Vec::new(),
                    findings: Vec::new(),
                    fields: Vec::new(),
                },
                |s| view(&yaml, s, Some(&file), globals.as_deref()),
            );
            RulesFileView {
                file,
                yaml,
                rules: out.rules,
                findings: out.findings,
            }
        })
        .collect();
    Json(RulesOut {
        commit: files.commit,
        mapping_reads: schema.is_some(),
        files: views,
    })
    .into_response()
}

/// A rules file checked as typed: lint and each rule as read.
#[utoipa::path(post, path = "/quality/rules/check", operation_id = "quality_check_rules",
    tag = "rules", request_body = CheckBody,
    responses((status = 200, body = CheckOut), (status = 401), (status = 403),
        (status = 404, description = "The dataset is not on `main`"),
        (status = 422, description = "Its mapping on `main` does not read (`mapping`)"),
        (status = 503, description = "No catalog to read it")))]
async fn check(
    State(catalog): State<Arc<dyn DraftCatalog>>,
    Extension(caller): Extension<Caller>,
    Json(body): Json<CheckBody>,
) -> Response {
    if !valid_name(&body.dataset) || !caller.can(VIEW, &format!("catalog/{}", body.dataset)) {
        return refuse(StatusCode::FORBIDDEN, "forbidden");
    }
    let files = match on_main(catalog.as_ref(), &body.dataset).await {
        Ok(Some(files)) => files,
        Ok(None) => return refuse(StatusCode::NOT_FOUND, "not_found"),
        Err((code, error)) => return refuse(code, error),
    };
    // Checked with its version's global rules (decision 0045).
    let branch = body
        .draft
        .as_deref()
        .and_then(|id| crate::draft_api::branch(&body.dataset, id).ok());
    let globals = draft_globals(
        catalog.as_ref(),
        &body.dataset,
        branch.as_deref(),
        &files.rules,
    )
    .await;
    match Mapping::parse(&files.mapping) {
        Ok(m) => {
            let mut out = view(
                &body.yaml,
                &m.schema(),
                body.file.as_deref(),
                globals.as_deref(),
            );
            out.fields = m.fields.keys().cloned().collect();
            Json(out).into_response()
        }
        Err(_) => refuse(StatusCode::UNPROCESSABLE_ENTITY, "mapping"),
    }
}

/// The JSON Schema of a rules file (`rules.schema.json`), for completion.
#[utoipa::path(get, path = "/quality/rules/schema", operation_id = "quality_rules_schema",
    tag = "rules",
    responses((status = 200, body = Object, description = "JSON Schema, draft 2020-12"),
        (status = 401), (status = 403)))]
async fn schema(Extension(caller): Extension<Caller>) -> Response {
    if !caller.can_some(VIEW) {
        return refuse(StatusCode::FORBIDDEN, "forbidden");
    }
    Json(quality_rules::json_schema()).into_response()
}

/// A rules file, to tidy or tidied.
#[derive(Debug, Deserialize, Serialize, ToSchema)]
pub struct TidyBody {
    pub yaml: String,
}

/// A rules file tidied (decision 0041): needless quotes and spaces gone,
/// rules one blank line apart, long values folded; it reads the same.
#[utoipa::path(post, path = "/quality/rules/tidy", operation_id = "quality_tidy_rules",
    tag = "rules", request_body = TidyBody,
    responses((status = 200, body = TidyBody), (status = 401), (status = 403)))]
async fn tidy(Extension(caller): Extension<Caller>, Json(body): Json<TidyBody>) -> Response {
    if !caller.can_some(VIEW) {
        return refuse(StatusCode::FORBIDDEN, "forbidden");
    }
    let yaml = quality_rules::lint::tidy(&body.yaml);
    Json(TidyBody { yaml }).into_response()
}
