//! The server's `JobCompiler` (decision 0090, amendment A1 point 2): it
//! gathers a job's input from PostgreSQL and compiles it with the pure
//! `quality-jobs`. Kinds not compiled yet are refused as
//! `kind_not_compiled`.

use infrastructure::PgPool;
use quality::bundle::Bundle;
use quality::bundle_api::{CompileRefused, JobCompiler, JobRequest};
use quality::job_inputs::{dataset_slas, next_run_seq};
use quality_jobs::{CompileError, Job, RetractInput, SlasInput};
use quality_rules_trino::Sla;

pub(crate) struct TrinoJobs {
    pub(crate) pool: PgPool,
}

fn database(e: impl std::fmt::Display) -> CompileRefused {
    tracing::error!("job compile: {e}");
    CompileRefused("internal")
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
            _ => Err(CompileRefused("kind_not_compiled")),
        }
    }
}
