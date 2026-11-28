//! The server's `JobCompiler` (decision 0090, amendment A1 point 2): it
//! gathers a job's input from PostgreSQL and compiles it with the pure
//! `quality-jobs`. Kinds not compiled yet are refused as
//! `kind_not_compiled`.

use std::collections::BTreeMap;
use std::sync::Arc;

use infrastructure::PgPool;
use quality::bundle::Bundle;
use quality::bundle_api::{CompileRefused, JobCompiler, JobRequest};
use quality::catalog::Catalog;
use quality::job_inputs::{dataset_slas, evaluate_facts, next_run_seq, source_columns, FactsError};
use quality::mapping::Mapping;
use quality_jobs::{
    CompileError, EvaluateInput, ExtractInput, Job, PrepareInput, ReferencePin, RetractInput,
    SlasInput,
};
use quality_rules_trino::{Sla, TablePath};

/// The window method's most distinct values when `QD_EXACT_SORT_LIMIT` is
/// not set (decision 0090, point 8.3: to be measured per tier).
const EXACT_SORT_LIMIT: u64 = 1_000_000;

pub(crate) struct TrinoJobs {
    pub(crate) pool: PgPool,
    /// The catalog's `main`, which an evaluate's mapping and rules come from.
    pub(crate) catalog: Option<Arc<dyn Catalog>>,
}

fn exact_sort_limit() -> u64 {
    std::env::var("QD_EXACT_SORT_LIMIT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(EXACT_SORT_LIMIT)
}

fn lake(namespace: String, table: String) -> TablePath {
    TablePath {
        catalog: "lake".into(),
        namespace,
        table,
    }
}

fn database(e: impl std::fmt::Display) -> CompileRefused {
    tracing::error!("job compile: {e}");
    CompileRefused("internal")
}

/// How many key ranges an extract reads at once (`QD_EXTRACT_RANGES`,
/// point 9): 1, one scan, unless set; T20 saw no speed-up on one node.
fn extract_ranges() -> u32 {
    std::env::var("QD_EXTRACT_RANGES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1)
}

/// The record key's first column, if the source lists it as an integer
/// (text keys are never split, point 9).
fn split_key<'a>(mapping: &Mapping, columns: &'a [(String, String)]) -> Option<&'a str> {
    let field = mapping.key.first()?;
    let column = &mapping.fields.get(field)?.value.column;
    let (name, ty) = columns.iter().find(|(n, _)| n == column)?;
    let ty = ty.to_ascii_lowercase();
    let integer = ["tinyint", "smallint", "integer", "bigint"].contains(&ty.as_str())
        || (ty.starts_with("decimal(") && ty.ends_with(",0)"));
    integer.then_some(name.as_str())
}

fn facts(e: FactsError) -> CompileRefused {
    match e {
        FactsError::Refused(class) => CompileRefused(class),
        FactsError::Failed(e) => database(e),
    }
}

fn refused(e: CompileError) -> CompileRefused {
    match e {
        CompileError::Incomplete(what) => {
            tracing::warn!(what, "job compile: the job is incomplete");
            CompileRefused("job_incomplete")
        }
        CompileError::Statement(e) => {
            tracing::error!("job compile: {e:?}");
            CompileRefused("compile_failed")
        }
    }
}

#[async_trait::async_trait]
impl JobCompiler for TrinoJobs {
    async fn compile(&self, run: &str, job: &JobRequest) -> Result<Bundle, CompileRefused> {
        let cut = job.cut.map(|c| c.format("%Y-%m-%d").to_string());
        let mut j = Job {
            run,
            kind: &job.kind,
            dataset: job.dataset.as_deref(),
            cut: cut.as_deref(),
            attempt: job.attempt,
            run_seq: None,
            catalog_commit: None,
        };
        match job.kind.as_str() {
            "retract" => {
                if j.cut.is_some() {
                    j.run_seq = Some(next_run_seq(&self.pool).await.map_err(database)?);
                }
                quality_jobs::retract(&j, &RetractInput).map_err(refused)
            }
            "slas" => {
                let dataset = j.dataset.ok_or(CompileRefused("job_incomplete"))?;
                let slas = dataset_slas(&self.pool, dataset)
                    .await
                    .map_err(database)?
                    .into_iter()
                    .filter_map(|s| {
                        Some(Sla {
                            dimension: s.dimension,
                            group: s.grp,
                            days: u32::try_from(s.days).ok()?,
                        })
                    })
                    .collect();
                quality_jobs::slas(&j, &SlasInput { slas }).map_err(refused)
            }
            "evaluate" => self.evaluate(j, job).await,
            "prepare" => self.prepare(j).await,
            "extract" => self.extract(j).await,
            _ => Err(CompileRefused("kind_not_compiled")),
        }
    }
}

impl TrinoJobs {
    /// The dataset's mapping at the catalog's `main` now, and its commit.
    async fn mapping(&self, dataset: &str) -> Result<(Mapping, String), CompileRefused> {
        let catalog = self
            .catalog
            .as_deref()
            .ok_or(CompileRefused("catalog_not_configured"))?;
        let snapshot = catalog
            .snapshot(dataset)
            .await
            .map_err(database)?
            .ok_or(CompileRefused("no_mapping"))?;
        let mapping =
            Mapping::parse(&snapshot.mapping).map_err(|_| CompileRefused("mapping_invalid"))?;
        Ok((mapping, snapshot.commit))
    }

    /// An extract of the mapped source table into bronze (ruling 67).
    async fn extract(&self, j: Job<'_>) -> Result<Bundle, CompileRefused> {
        let dataset = j.dataset.ok_or(CompileRefused("job_incomplete"))?;
        let (mapping, commit) = self.mapping(dataset).await?;
        let listed = source_columns(&self.pool, &mapping).await.map_err(facts)?;
        let mapped: Vec<&str> = mapping
            .fields
            .values()
            .map(|f| f.value.column.as_str())
            .collect();
        let columns: Vec<(String, String)> = listed
            .into_iter()
            .filter(|(n, _)| mapped.contains(&n.as_str()))
            .collect();
        let from = mapping
            .from
            .as_ref()
            .ok_or(CompileRefused("mapping_has_no_source"))?;
        let (schema, table) = from.value.schema_and_table();
        let schema = schema.ok_or(CompileRefused("source_table_has_no_schema"))?;
        let source = TablePath {
            catalog: format!("src_{}", from.value.source),
            namespace: schema.to_owned(),
            table: table.to_owned(),
        };
        let (bronze_ns, bronze_table) = quality::layers::bronze(&mapping);
        let j = Job {
            catalog_commit: Some(&commit),
            ..j
        };
        let split = split_key(&mapping, &columns).map(|k| (k, extract_ranges()));
        let input = ExtractInput {
            source,
            bronze: lake(bronze_ns, bronze_table),
            columns: &columns,
            split,
        };
        quality_jobs::extract(&j, &input).map_err(refused)
    }

    /// A prepare from the dataset's mapping at the catalog's `main` now.
    async fn prepare(&self, j: Job<'_>) -> Result<Bundle, CompileRefused> {
        let dataset = j.dataset.ok_or(CompileRefused("job_incomplete"))?;
        let catalog = self
            .catalog
            .as_deref()
            .ok_or(CompileRefused("catalog_not_configured"))?;
        let snapshot = catalog
            .snapshot(dataset)
            .await
            .map_err(database)?
            .ok_or(CompileRefused("no_mapping"))?;
        let mapping = quality::mapping::Mapping::parse(&snapshot.mapping)
            .map_err(|_| CompileRefused("mapping_invalid"))?;
        let (bronze_ns, bronze_table) = quality::layers::bronze(&mapping);
        let (silver_ns, silver_table) =
            quality::layers::silver(&mapping).ok_or(CompileRefused("no_silver"))?;
        let columns = source_columns(&self.pool, &mapping)
            .await
            .map_err(|e| match e {
                FactsError::Refused(class) => CompileRefused(class),
                FactsError::Failed(e) => database(e),
            })?;
        let j = Job {
            catalog_commit: Some(&snapshot.commit),
            ..j
        };
        let input = PrepareInput {
            mapping: &mapping,
            bronze: lake(bronze_ns, bronze_table),
            silver: lake(silver_ns, silver_table),
            source_columns: &columns,
        };
        quality_jobs::prepare(&j, &input).map_err(refused)
    }

    /// An evaluate from the catalog's `main` now, with a new `run_seq`.
    async fn evaluate(&self, j: Job<'_>, job: &JobRequest) -> Result<Bundle, CompileRefused> {
        let (Some(dataset), Some(cut)) = (j.dataset, job.cut) else {
            return Err(CompileRefused("job_incomplete"));
        };
        let catalog = self
            .catalog
            .as_deref()
            .ok_or(CompileRefused("catalog_not_configured"))?;
        let facts = evaluate_facts(&self.pool, catalog, dataset, cut)
            .await
            .map_err(|e| match e {
                FactsError::Refused(class) => CompileRefused(class),
                FactsError::Failed(e) => database(e),
            })?;
        let (bronze_ns, bronze_table) = quality::layers::bronze(&facts.mapping);
        let (silver_ns, silver_table) =
            quality::layers::silver(&facts.mapping).ok_or(CompileRefused("no_silver"))?;
        let run_seq = next_run_seq(&self.pool).await.map_err(database)?;
        let j = Job {
            run_seq: Some(run_seq),
            catalog_commit: Some(&facts.commit),
            ..j
        };
        let previous = facts.previous.map(|d| d.format("%Y-%m-%d").to_string());
        let references: Vec<ReferencePin<'_>> = facts
            .references
            .iter()
            .map(|r| ReferencePin {
                name: &r.name,
                silver: lake(r.namespace.clone(), r.table.clone()),
                load: r.load.as_deref(),
            })
            .collect();
        let input = EvaluateInput {
            mapping: &facts.mapping,
            rules: &facts.rules,
            tables: &facts.tables,
            references: &references,
            silver: lake(silver_ns, silver_table),
            bronze: lake(bronze_ns, bronze_table),
            load: facts.load.as_deref(),
            previous: previous.as_deref(),
            bronze_types: &BTreeMap::new(),
            rules_commit: &facts.commit,
            exact_sort_limit: exact_sort_limit(),
        };
        quality_jobs::evaluate(&j, &input).map_err(refused)
    }
}
