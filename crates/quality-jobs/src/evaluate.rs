//! An `evaluate` job's bundle (decision 0090, amendment A1): the rules on
//! a cut's pinned silver, every result written into gold by
//! `INSERT … SELECT` (ruling 40), then the flip and the older runs'
//! delete (point 4). An earlier attempt of the run is undone first
//! (ruling 25); counts the job's user may not read back go through
//! `job_counts` (ruling 53); the cut is a literal (ruling 52).

use std::collections::BTreeMap;

use quality::bundle::{Bundle, PlaceholderType};
use quality::mapping::{FieldRole, Kind, Mapping};
use quality_rules::check::Compiled;
use quality_rules::sql::Tables;
use quality_rules_trino as trino;
use quality_rules_trino::{
    Accepted, Cut, FieldSources, IcdRule, Method, Pinned, Run, ShapeKind, Summary, TablePath,
    DETAILS, RUN_SUMMARY,
};

use crate::steps::Steps;
use crate::{CompileError, Job};

/// What the core gathers for an evaluate: the dataset's mapping and
/// approved rules, its tables, the cut's accepted load and the previous
/// cut from the index.
#[derive(Debug, Clone)]
pub struct EvaluateInput<'a> {
    pub mapping: &'a Mapping,
    pub rules: &'a [Compiled],
    /// The tables the rules' references read, resolved by the core.
    pub tables: &'a Tables,
    /// Each reference's silver, pinned in the job at this cut (ruling 61).
    pub references: &'a [ReferencePin<'a>],
    pub silver: TablePath,
    pub bronze: TablePath,
    /// The cut's accepted load (`None`: rows from before loads).
    pub load: Option<&'a str>,
    /// The dataset's latest cut before this one in the index, if any.
    pub previous: Option<&'a str>,
    /// Bronze's column types, as Trino names them, for the profile's shape;
    /// a column not in it takes its mapping's kind.
    pub bronze_types: &'a BTreeMap<String, String>,
    pub rules_commit: &'a str,
    /// The window method's most distinct values (`QD_EXACT_SORT_LIMIT`).
    pub exact_sort_limit: u64,
    /// For a backfill (step 22): the silver and bronze snapshots a cut's
    /// manifest recorded, read as they were instead of their `$refs` now.
    pub at: AtSnapshots,
}

/// Snapshots a backfill's evaluation is pinned to (decision 0090, point
/// 11.3): each one given is read `FOR VERSION AS OF` it; one not given is
/// read from its table's `$refs` in the job, as a new evaluation does.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AtSnapshots {
    pub silver: Option<i64>,
    pub bronze: Option<i64>,
}

/// A referenced dataset's silver, read at the same cut (`as_of`'s only
/// choice today) and pinned by its own placeholder.
#[derive(Debug, Clone)]
pub struct ReferencePin<'a> {
    /// The reference's name in the rules (a key of `tables`).
    pub name: &'a str,
    pub silver: TablePath,
    /// The referenced cut's accepted load.
    pub load: Option<&'a str>,
}

const SILVER_PIN: &str = "silver_snapshot";
const BRONZE_PIN: &str = "bronze_snapshot";
const SNAPSHOT_COLUMN: &str = "snapshot_id";
const STARTED: &str = "started_ms";

fn gold_tables() -> impl Iterator<Item = &'static str> {
    std::iter::once(RUN_SUMMARY.name).chain(DETAILS.iter().map(|t| t.name))
}

fn detail(name: &str) -> &'static trino::GoldTable {
    DETAILS
        .iter()
        .find(|t| t.name == name)
        .expect("a gold table")
}

/// The evaluate's bundle, in order: an earlier attempt undone, the start
/// time and both pins read, the scratch rows, gold's rows, the scratch
/// rows dropped, the flip, then the older runs' rows.
pub fn evaluate(job: &Job<'_>, input: &EvaluateInput<'_>) -> Result<Bundle, CompileError> {
    let dataset = job.dataset()?;
    let cut =
        Cut::parse(job.cut.ok_or(CompileError::Incomplete("cut"))?).map_err(trino::Error::from)?;
    let previous = input
        .previous
        .map(Cut::parse)
        .transpose()
        .map_err(trino::Error::from)?;
    let run = Run {
        dataset,
        cut: &cut,
        run_id: job.run,
        run_seq: job.run_seq()?,
    };
    let mut steps = Steps::default();
    undo_attempt(&mut steps, &run)?;
    let started = steps.bare_count(dataset, job.run, "started", &trino::started_ms())?;
    steps.placeholder(
        STARTED,
        PlaceholderType::Bigint,
        &started,
        trino::COUNT_COLUMN,
    );
    let (silver, bronze, tables) = pins(&mut steps, input, &cut)?;
    let tables = &tables;
    let icd: Vec<IcdRule> = input
        .rules
        .iter()
        .map(|r| IcdRule::of(r, tables, &cut))
        .collect::<Result<_, _>>()?;
    let measured = trino::measured(input.mapping, &icd);
    steps.insert(
        "scratch".into(),
        trino::scratch_insert(job.run, dataset, &silver, input.mapping, &icd, &cut)?,
    );
    let rows = |t: &str, body: trino::Statement| trino::insert(detail(t), &run, &body);
    steps.insert(
        "rule_results".into(),
        rows(
            "rule_results",
            trino::rule_results(&silver, input.rules, tables, &cut)?,
        )?,
    );
    let sources = FieldSources {
        bronze: &bronze,
        silver: &silver,
        job_id: job.run,
    };
    let kind = |column: &str| ShapeKind::of_type(bronze_type(input, column));
    let fq = trino::field_quality(input.mapping, &sources, &measured, kind, &cut)?;
    steps.insert("field_quality".into(), rows("field_quality", fq)?);
    let gm = trino::group_metrics(job.run, input.mapping)?;
    steps.insert("group_metrics".into(), rows("group_metrics", gm)?);
    let cells = trino::cell_rows(job.run, input.mapping)?;
    steps.insert("cells".into(), rows("cells", cells)?);
    let entities = trino::entity_icd(job.run, &measured)?;
    steps.insert("entity_icd".into(), rows("entity_icd", entities)?);
    findings(&mut steps, &run, input, &icd, previous.as_ref())?;
    distribution(&mut steps, &run, &silver, input)?;
    let end = steps.bare_count(
        dataset,
        job.run,
        "scratch_end",
        &trino::scratch_rows(job.run)?,
    )?;
    steps.delete_after("scratch_end".into(), trino::scratch_delete(job.run)?, end);
    let started_ms = format!("{{{{{STARTED}}}}}");
    let summary = Summary {
        job_id: job.run,
        rules_commit: input.rules_commit,
        silver: &silver,
        started_ms: Some(&started_ms),
    };
    steps.insert(
        "flip".into(),
        trino::flip(&run, &trino::summary(&run, &summary)?)?,
    );
    for t in gold_tables().skip(1).chain([RUN_SUMMARY.name]) {
        steps.count_then_delete(
            &format!("older_{t}"),
            trino::older_rows(t, &run)?,
            trino::delete_older(t, &run)?,
        );
    }
    Ok(job.bundle(steps))
}

/// `column`'s type in bronze; without it, the mapping's kind as silver
/// types it: a number as a double (no shape), anything else as text.
fn bronze_type<'a>(input: &'a EvaluateInput<'_>, column: &str) -> &'a str {
    if let Some(t) = input.bronze_types.get(column) {
        return t;
    }
    let number = input
        .mapping
        .fields
        .values()
        .any(|f| f.value.column == column && f.value.kind == Kind::Number);
    if number {
        "double"
    } else {
        "varchar"
    }
}

/// The run's pins: its silver and bronze, then each reference's silver,
/// with the tables the rules read them through.
fn pins(
    steps: &mut Steps,
    input: &EvaluateInput<'_>,
    cut: &Cut,
) -> Result<(String, String, Tables), CompileError> {
    let at =
        |table: &TablePath, snapshot: Option<i64>, name: &str, steps: &mut Steps| match snapshot {
            Some(snapshot) => {
                let p = Pinned {
                    table: table.clone(),
                    snapshot,
                };
                Ok(trino::accepted(
                    &p,
                    &Accepted {
                        cut,
                        load: input.load,
                    },
                )?)
            }
            None => pin(steps, table, name, cut, input.load),
        };
    let silver = at(&input.silver, input.at.silver, SILVER_PIN, steps)?;
    let bronze = at(&input.bronze, input.at.bronze, BRONZE_PIN, steps)?;
    let mut tables = input.tables.clone();
    for (i, r) in input.references.iter().enumerate() {
        let read = pin(steps, &r.silver, &format!("ref_{i}_snapshot"), cut, r.load)?;
        let table = tables
            .get_mut(r.name)
            .ok_or(CompileError::Incomplete("reference"))?;
        table.pinned = Some(read);
    }
    Ok((silver, bronze, tables))
}

/// What an earlier attempt of the run left: its counts, its gold rows
/// (its flip first) and its scratch rows.
fn undo_attempt(steps: &mut Steps, run: &Run<'_>) -> Result<(), CompileError> {
    steps.count_then_delete(
        "counts",
        trino::run_counts(run.dataset, run.run_id)?,
        trino::delete_run_counts(run.dataset, run.run_id)?,
    );
    for t in gold_tables() {
        steps.count_then_delete(
            &format!("run_{t}"),
            trino::run_rows(t, run)?,
            trino::delete_run(t, run)?,
        );
    }
    let left = trino::scratch_rows(run.run_id)?;
    let after = steps.bare_count(run.dataset, run.run_id, "scratch_left", &left)?;
    steps.delete_after(
        "scratch_left".into(),
        trino::scratch_delete(run.run_id)?,
        after,
    );
    Ok(())
}

/// `table`'s snapshot read as the report user (its `$refs`, ruling 39)
/// into the placeholder `name`; the cut's accepted load at it.
pub(crate) fn pin(
    steps: &mut Steps,
    table: &TablePath,
    name: &str,
    cut: &Cut,
    load: Option<&str>,
) -> Result<String, CompileError> {
    let read = trino::Statement {
        sql: trino::current_snapshot(table)?,
    };
    steps.read_one(name.into(), read, SNAPSHOT_COLUMN);
    steps.placeholder(name, PlaceholderType::SnapshotId, name, SNAPSHOT_COLUMN);
    let at = trino::pinned_by(table, name)?;
    Ok(trino::accepted_in(&at, &Accepted { cut, load })?)
}

/// The run's findings over the scratch rows, their groups, then the
/// queues against the previous cut's current run.
fn findings(
    steps: &mut Steps,
    run: &Run<'_>,
    input: &EvaluateInput<'_>,
    icd: &[IcdRule],
    previous: Option<&Cut>,
) -> Result<(), CompileError> {
    let before = previous
        .map(|p| trino::previous_findings(run.dataset, p))
        .transpose()?;
    let rows = |t: &str, body: trino::Statement| trino::insert(detail(t), run, &body);
    let f = trino::findings(
        run.run_id,
        input.mapping,
        icd,
        before.as_ref().map(trino::GoldFindings::relations),
        run.cut,
    )?;
    steps.insert("findings".into(), rows("findings", f)?);
    let now = trino::run_findings(run)?;
    let g = trino::finding_groups(run.run_id, input.mapping, &now.findings)?;
    steps.insert("finding_groups".into(), rows("finding_groups", g)?);
    let q = trino::queues(
        input.mapping,
        now.relations(),
        before.as_ref().map(trino::GoldFindings::relations),
    )?;
    steps.insert("queue_findings".into(), rows("queue_findings", q)?);
    Ok(())
}

/// Each drift variable's percentiles by the one method its distinct count
/// picks (both in the bundle, each after its guard), then the outliers.
fn distribution(
    steps: &mut Steps,
    run: &Run<'_>,
    silver: &str,
    input: &EvaluateInput<'_>,
) -> Result<(), CompileError> {
    let table = detail("distribution_checks");
    let drift = input
        .mapping
        .fields
        .iter()
        .filter(|(_, f)| f.value.roles.contains(&FieldRole::Drift));
    for (i, (name, _)) in drift.enumerate() {
        for (m, method) in [("window", Method::Window), ("bracket", Method::Bracket)] {
            let id = format!("drift_{i}_{m}");
            let guard = trino::percentile_guard(silver, name, input.exact_sort_limit, method)?;
            let after = steps.bare_count(run.dataset, run.run_id, &id, &guard)?;
            let row = trino::percentile_row(silver, name, method)?;
            steps.insert_after(id, trino::insert(table, run, &row)?, after);
        }
    }
    let outliers = trino::outlier_rows(silver, input.mapping)?;
    steps.insert("outliers".into(), trino::insert(table, run, &outliers)?);
    Ok(())
}
