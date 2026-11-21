//! Sources for people (decision 0036), in the module's `user_router`: the
//! driver catalog for the form, a tenant's sources without their secrets,
//! and creating or editing one. What a caller may do is asked of its
//! [`Caller`] (`quality:ViewSources`, `quality:ManageSources` on
//! `source/<name>`), and every change is audited through it, secrets as
//! changed. A request's body may carry secrets: it is never logged. A
//! source a mapping on the catalog's `main` reads is not deleted. A source
//! on Trino keeps its catalog there (decision 0048, [`source_catalog`]).

use std::collections::BTreeMap;
use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, put};
use axum::{Extension, Json, Router};
use contracts::{AuditEntry, Caller, Change};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sqlx::PgPool;
use utoipa::{OpenApi, ToSchema};
use uuid::Uuid;

use crate::catalog::{Catalog, DatasetMapping};
use crate::mapping::Mapping;
use crate::source::{Driver, Engine, FieldKind, Source, SourceError, DRIVERS};
use crate::source_catalog::{self, Trino};
use crate::source_store::{self, Put, Sealer, SourceView, StoreError};
use crate::tables_api;

pub const VIEW: &str = "quality:ViewSources";
pub const MANAGE: &str = "quality:ManageSources";

#[derive(Clone)]
struct SourceApi {
    pool: PgPool,
    sealer: Arc<dyn Sealer>,
    catalog: Option<Arc<dyn Catalog>>,
    trino: Option<Arc<dyn Trino>>,
}

/// `GET /quality/drivers`, `GET /quality/sources`,
/// `PUT` and `DELETE /quality/sources/{name}`.
pub fn routes(
    pool: PgPool,
    sealer: Arc<dyn Sealer>,
    catalog: Option<Arc<dyn Catalog>>,
    trino: Option<Arc<dyn Trino>>,
) -> Router {
    Router::new()
        .route("/quality/drivers", get(drivers))
        .route("/quality/sources", get(list))
        .route(
            "/quality/sources/{name}",
            put(put_source).delete(delete_source),
        )
        .with_state(SourceApi {
            pool,
            sealer,
            catalog,
            trino,
        })
}

/// The routes' OpenAPI paths and schemas, for the installation's document.
#[derive(Debug, OpenApi)]
#[openapi(
    paths(drivers, list, put_source, delete_source),
    components(schemas(DriverOut, EngineOut, DriverFieldOut, FieldKindOut, SourceView,
        SourceBody, Refusal, Refusals, InUse)),
    tags((name = "sources", description = "A tenant's sources and the driver catalog")),
)]
pub struct SourceDoc;

fn resource(name: &str) -> String {
    format!("source/{name}")
}

fn forbidden() -> Response {
    (StatusCode::FORBIDDEN, Json(json!({ "error": "forbidden" }))).into_response()
}

fn internal(what: &str) -> Response {
    tracing::error!("sources: {what}");
    StatusCode::INTERNAL_SERVER_ERROR.into_response()
}

/// Trino did not take the source's catalog statement.
fn trino_failed(name: &str, why: &str) -> Response {
    tracing::error!(source = name, "sources: trino catalog: {why}");
    (StatusCode::BAD_GATEWAY, Json(json!({ "error": "trino" }))).into_response()
}

/// The source's catalog made again as it is saved, if it has one and there
/// is a Trino.
async fn sync_catalog(api: &SourceApi, name: &str, driver: &str) -> Result<(), Box<Response>> {
    let Some(trino) = api.trino.as_deref() else {
        return Ok(());
    };
    if !source_catalog::has_catalog(driver) {
        return Ok(());
    }
    let saved = source_store::credentials(&api.pool, api.sealer.as_ref(), name)
        .await
        .map_err(|e| Box::new(internal(&format!("{e:?}"))))?;
    source_catalog::replace(trino, &saved.source)
        .await
        .map_err(|e| Box::new(trino_failed(name, &e)))
}

/// A driver of the catalog, for its form.
#[derive(Debug, Serialize, ToSchema)]
pub struct DriverOut {
    pub id: &'static str,
    pub engine: EngineOut,
    pub default_port: Option<u16>,
    pub fields: Vec<DriverFieldOut>,
}

/// The engine that reads a driver's sources.
#[derive(Debug, Serialize, ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum EngineOut {
    Duckdb,
    Trino,
}

/// One field of a driver's form; a `secret` one is never sent back.
#[derive(Debug, Serialize, ToSchema)]
pub struct DriverFieldOut {
    pub key: &'static str,
    pub kind: FieldKindOut,
    pub required: bool,
}

#[derive(Debug, Serialize, ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum FieldKindOut {
    Text,
    Port,
    Flag,
    Secret,
}

fn driver_out(d: &Driver) -> DriverOut {
    let engine = match d.engine {
        Engine::DuckDb { .. } => EngineOut::Duckdb,
        Engine::Trino { .. } => EngineOut::Trino,
    };
    let fields = d
        .fields
        .iter()
        .map(|f| DriverFieldOut {
            key: f.key,
            kind: match f.kind {
                FieldKind::Text => FieldKindOut::Text,
                FieldKind::Port => FieldKindOut::Port,
                FieldKind::Flag => FieldKindOut::Flag,
                FieldKind::Secret => FieldKindOut::Secret,
            },
            required: f.required,
        })
        .collect();
    DriverOut {
        id: d.id,
        engine,
        default_port: d.default_port,
        fields,
    }
}

/// The catalog, for the form of each driver.
#[utoipa::path(
    get,
    path = "/quality/drivers",
    operation_id = "quality_drivers",
    responses(
        (status = 200, description = "The driver catalog", body = Vec<DriverOut>),
        (status = 401, description = "No valid session"),
        (status = 403, description = "May neither view nor manage any source"),
    ),
    tag = "sources",
)]
async fn drivers(Extension(caller): Extension<Caller>) -> Response {
    if !caller.can_some(VIEW) && !caller.can_some(MANAGE) {
        return forbidden();
    }
    let drivers: Vec<_> = DRIVERS.iter().map(driver_out).collect();
    Json(drivers).into_response()
}

/// The sources the caller may see, without their secrets.
#[utoipa::path(
    get,
    path = "/quality/sources",
    operation_id = "quality_list_sources",
    responses(
        (status = 200, description = "The sources the caller may view, secrets by name only",
            body = Vec<SourceView>),
        (status = 401, description = "No valid session"),
        (status = 403, description = "May view no source"),
    ),
    tag = "sources",
)]
async fn list(State(api): State<SourceApi>, Extension(caller): Extension<Caller>) -> Response {
    if !caller.can_some(VIEW) {
        return forbidden();
    }
    match source_store::list(&api.pool, api.sealer.as_ref()).await {
        Ok(sources) => {
            let seen: Vec<SourceView> = sources
                .into_iter()
                .filter(|s| caller.can(VIEW, &resource(&s.name)))
                .collect();
            Json(seen).into_response()
        }
        Err(e) => internal(&format!("{e:?}")),
    }
}

/// A source as the form sends it: secrets left out keep their value.
#[derive(Debug, Deserialize, ToSchema)]
pub struct SourceBody {
    pub driver: String,
    #[serde(default)]
    pub fields: BTreeMap<String, String>,
    #[serde(default)]
    pub secrets: BTreeMap<String, String>,
}

/// One reason a source is refused: a code the UI's texts translate, and the
/// field it is about.
#[derive(Debug, Serialize, ToSchema)]
pub struct Refusal {
    /// `name`, `unknown_driver`, `unknown_field`, `wrong_place`, `missing`,
    /// `port` or `value`.
    pub code: &'static str,
    pub field: Option<String>,
}

/// Every reason a source is refused.
#[derive(Debug, Serialize, ToSchema)]
pub struct Refusals {
    pub errors: Vec<Refusal>,
}

fn refusal(e: SourceError) -> Refusal {
    let (code, field) = match e {
        SourceError::Name => ("name", None),
        SourceError::UnknownDriver => ("unknown_driver", None),
        SourceError::UnknownField(f) => ("unknown_field", Some(f)),
        SourceError::WrongPlace(f) => ("wrong_place", Some(f)),
        SourceError::Missing(f) => ("missing", Some(f)),
        SourceError::Port(f) => ("port", Some(f)),
        SourceError::Value(f) => ("value", Some(f)),
    };
    Refusal { code, field }
}

/// Creates or edits the source `name`, and audits what it did.
#[utoipa::path(
    put,
    path = "/quality/sources/{name}",
    operation_id = "quality_put_source",
    params(("name" = String, Path, description = "The source's name")),
    request_body = SourceBody,
    responses(
        (status = 201, description = "Created; its tables are listed"),
        (status = 204, description = "Edited: fields replaced whole, secrets left out kept; its tables listed again"),
        (status = 401, description = "No valid session"),
        (status = 403, description = "May not manage this source"),
        (status = 422, description = "The source does not hold together: every reason",
            body = Refusals),
        (status = 502, description = "Saved and audited, but Trino did not take its catalog: \
            saving it again retries"),
    ),
    tag = "sources",
)]
async fn put_source(
    State(api): State<SourceApi>,
    Extension(caller): Extension<Caller>,
    Path(name): Path<String>,
    Json(body): Json<SourceBody>,
) -> Response {
    let resource = resource(&name);
    if !caller.can(MANAGE, &resource) {
        return forbidden();
    }
    let source = Source {
        name: name.clone(),
        driver: body.driver.clone(),
        fields: body.fields,
        secrets: body.secrets,
    };
    if let Some(field) = source_catalog::refused_field(&source) {
        let errors = vec![Refusal {
            code: "value",
            field: Some(field),
        }];
        return (StatusCode::UNPROCESSABLE_ENTITY, Json(Refusals { errors })).into_response();
    }
    let change = match source_store::put(&api.pool, api.sealer.as_ref(), &source).await {
        Ok(Put::Created) => Change::Created,
        Ok(Put::Changed) => Change::Changed,
        Ok(Put::SecretsChanged) => Change::SecretsChanged,
        Err(StoreError::Invalid(errors)) => {
            let errors = errors.into_iter().map(refusal).collect();
            return (StatusCode::UNPROCESSABLE_ENTITY, Json(Refusals { errors })).into_response();
        }
        Err(StoreError::NotFound) => return StatusCode::NOT_FOUND.into_response(),
        Err(StoreError::Seal) => return internal("sealing"),
        Err(StoreError::Db(e)) => return internal(&e.to_string()),
    };
    // Whoever saved it sees its tables a moment later (decision 0042).
    tables_api::list_after_save(&api.pool, &name, &body.driver).await;
    let entry = AuditEntry {
        action: MANAGE,
        resource,
        change,
    };
    if let Err(e) = caller.audit(entry).await {
        return internal(&format!("audit: {e}"));
    }
    match sync_catalog(&api, &name, &body.driver).await {
        Ok(()) if change == Change::Created => StatusCode::CREATED.into_response(),
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(refused) => *refused,
    }
}

/// The datasets whose mapping on the catalog's `main` reads the source.
#[derive(Debug, Serialize, ToSchema)]
pub struct InUse {
    pub datasets: Vec<String>,
}

/// The datasets whose mapping reads `source` (`from.source`). A mapping
/// that does not parse cannot say: an error, so nothing is deleted blindly.
fn reading(mappings: &[DatasetMapping], source: &str) -> Result<Vec<String>, String> {
    let mut datasets = Vec::new();
    for m in mappings {
        let parsed = Mapping::parse(&m.yaml).map_err(|_| format!("mapping {}", m.dataset))?;
        if parsed.from.is_some_and(|f| f.value.source == source) {
            datasets.push(m.dataset.clone());
        }
    }
    Ok(datasets)
}

fn unavailable() -> Response {
    let body = json!({ "error": "catalog_unavailable" });
    (StatusCode::SERVICE_UNAVAILABLE, Json(body)).into_response()
}

/// Deletes the source `name`, unless a dataset's mapping reads it, and
/// audits it.
#[utoipa::path(
    delete,
    path = "/quality/sources/{name}",
    operation_id = "quality_delete_source",
    params(("name" = String, Path, description = "The source's name")),
    responses(
        (status = 204, description = "Deleted, both versions of its secrets with it"),
        (status = 401, description = "No valid session"),
        (status = 403, description = "May not manage this source"),
        (status = 404, description = "No source by that name"),
        (status = 409, description = "Datasets' mappings on the catalog's main read it",
            body = InUse),
        (status = 502, description = "Trino did not drop its catalog: nothing deleted"),
        (status = 503, description = "No catalog to ask which mappings read it"),
    ),
    tag = "sources",
)]
async fn delete_source(
    State(api): State<SourceApi>,
    Extension(caller): Extension<Caller>,
    Path(name): Path<String>,
) -> Response {
    let resource = resource(&name);
    if !caller.can(MANAGE, &resource) {
        return forbidden();
    }
    // Held until it is deleted: no draft that reads it is approved meanwhile.
    let held = match source_store::hold(&api.pool, &name, true).await {
        Ok(Some(held)) => held,
        Ok(None) => return StatusCode::NOT_FOUND.into_response(),
        Err(e) => return internal(&e.to_string()),
    };
    let Some(catalog) = &api.catalog else {
        return unavailable();
    };
    let Ok(tenant) = Uuid::parse_str(&tenancy::key()) else {
        return internal("the request's tenant");
    };
    let mappings = match catalog.mappings(tenant).await {
        Ok(m) => m,
        Err(e) => {
            tracing::error!("sources: catalog: {e}");
            return unavailable();
        }
    };
    match reading(&mappings, &name) {
        Ok(datasets) if datasets.is_empty() => {}
        Ok(datasets) => return (StatusCode::CONFLICT, Json(InUse { datasets })).into_response(),
        Err(e) => return internal(&e),
    }
    // Its catalog first: no catalog with its credentials outlives it.
    if let Some(trino) = api.trino.as_deref() {
        match source_store::driver_of(&api.pool, &name).await {
            Ok(Some(driver)) if source_catalog::has_catalog(&driver) => {
                if let Err(e) = source_catalog::drop(trino, &name).await {
                    return trino_failed(&name, &e);
                }
            }
            Ok(_) => {}
            Err(e) => return internal(&e.to_string()),
        }
    }
    match source_store::delete_held(held, &name).await {
        Ok(true) => {}
        Ok(false) => return StatusCode::NOT_FOUND.into_response(),
        Err(e) => return internal(&e.to_string()),
    }
    let entry = AuditEntry {
        action: MANAGE,
        resource,
        change: Change::Deleted,
    };
    match caller.audit(entry).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => internal(&format!("audit: {e}")),
    }
}
