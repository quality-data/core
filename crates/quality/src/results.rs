//! An evaluation's results in PostgreSQL (decisions 0018 and 0031): the
//! cut's `run_summary`, `rule_results` and `field_quality`, replaced whole
//! by its report; each rule's counts with what the charts group them by;
//! and its ICD (decision 0057): the IGCD and the bands in the summary, each
//! field's mean, and `entity_icd`; the `updated` field's records by age;
//! each group of each dimension (`group_metrics`); and the drift variables'
//! percentiles and the outliers by group (`distribution_checks`); how long
//! each stage of the evaluation took (`job_stats`); each SFC cell with
//! whether it passes, its missing and margin (`cells`, decision 0058); and
//! each remediation queue's findings by the cut they opened at (decision
//! 0059, `queue_findings`).

use sqlx::PgPool;
use uuid::Uuid;

use crate::cuts;
use crate::worker_api::{band, cell, Report, RuleCounts};

/// Replaces the cut's summary and its rules' counts, and marks its cut
/// done, in one transaction.
pub(crate) async fn store(
    pool: &PgPool,
    job: Uuid,
    dataset: &str,
    report: &Report,
    manifest: Option<&str>,
) -> Result<(), sqlx::Error> {
    let mut tx = pool.begin().await?;
    sqlx::query("DELETE FROM quality.run_summary WHERE dataset = $1 AND cut = $2::date")
        .bind(dataset)
        .bind(&report.cut)
        .execute(&mut *tx)
        .await?;
    sqlx::query(
        "INSERT INTO quality.run_summary
             (dataset, cut, job_id, records, duration_ms, rules_commit, manifest, igcd)
         VALUES ($1, $2::date, $3, $4, $5, $6, $7, $8)",
    )
    .bind(dataset)
    .bind(&report.cut)
    .bind(job)
    .bind(report.records)
    .bind(report.duration_ms)
    .bind(&report.rules_commit)
    .bind(manifest)
    .bind(report.igcd)
    .execute(&mut *tx)
    .await?;
    let rules = &report.rules;
    let column = |f: fn(&RuleCounts) -> Option<&str>| rules.iter().map(f).collect::<Vec<_>>();
    sqlx::query(
        "INSERT INTO quality.rule_results
             (dataset, cut, rule, applies, failures, field, pattern, dimension, severity)
         SELECT $1, $2::date, r.*
           FROM unnest($3::text[], $4::bigint[], $5::bigint[],
                       $6::text[], $7::text[], $8::text[], $9::text[])
             AS r (rule, applies, failures, field, pattern, dimension, severity)",
    )
    .bind(dataset)
    .bind(&report.cut)
    .bind(rules.iter().map(|r| r.rule.as_str()).collect::<Vec<_>>())
    .bind(rules.iter().map(|r| r.applies).collect::<Vec<_>>())
    .bind(rules.iter().map(|r| r.failures).collect::<Vec<_>>())
    .bind(column(|r| r.field.as_deref()))
    .bind(column(|r| r.pattern.as_deref()))
    .bind(column(|r| r.dimension.as_deref()))
    .bind(column(|r| r.severity.as_deref()))
    .execute(&mut *tx)
    .await?;
    let fields = &report.fields;
    sqlx::query(
        "INSERT INTO quality.field_quality
             (dataset, cut, field, empty, distinct_values, pattern, pattern_count,
              mean_icd, floating)
         SELECT $1, $2::date, f.*
           FROM unnest($3::text[], $4::bigint[], $5::bigint[], $6::text[], $7::bigint[],
                       $8::float8[], $9::boolean[])
             AS f (field, empty, distinct_values, pattern, pattern_count, mean_icd,
                   floating)",
    )
    .bind(dataset)
    .bind(&report.cut)
    .bind(fields.iter().map(|f| f.field.as_str()).collect::<Vec<_>>())
    .bind(fields.iter().map(|f| f.empty).collect::<Vec<_>>())
    .bind(fields.iter().map(|f| f.distinct).collect::<Vec<_>>())
    .bind(
        fields
            .iter()
            .map(|f| f.pattern.as_deref())
            .collect::<Vec<_>>(),
    )
    .bind(fields.iter().map(|f| f.pattern_count).collect::<Vec<_>>())
    .bind(fields.iter().map(|f| f.icd).collect::<Vec<_>>())
    .bind(fields.iter().map(|f| f.floating).collect::<Vec<_>>())
    .execute(&mut *tx)
    .await?;
    recency(&mut tx, dataset, report).await?;
    groups(&mut tx, dataset, report).await?;
    cells(&mut tx, dataset, report).await?;
    distributions(&mut tx, dataset, report).await?;
    entities(&mut tx, dataset, report).await?;
    stages(&mut tx, dataset, report).await?;
    queues(&mut tx, dataset, report).await?;
    cuts::finish_evaluation(&mut tx, job).await?;
    tx.commit().await
}

/// The `updated` field's records by age, on its row.
async fn recency(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    dataset: &str,
    report: &Report,
) -> Result<(), sqlx::Error> {
    for f in report.fields.iter().filter(|f| f.recency.is_some()) {
        sqlx::query(
            "UPDATE quality.field_quality SET recency = $4
              WHERE dataset = $1 AND cut = $2::date AND field = $3",
        )
        .bind(dataset)
        .bind(&report.cut)
        .bind(&f.field)
        .bind(&f.recency)
        .execute(&mut **tx)
        .await?;
    }
    Ok(())
}

/// Each group of each dimension, an empty group as ''.
async fn groups(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    dataset: &str,
    report: &Report,
) -> Result<(), sqlx::Error> {
    let rows = &report.groups;
    sqlx::query(
        "INSERT INTO quality.group_metrics
             (dataset, cut, dimension, grp, records, serious, mean_icd)
         SELECT $1, $2::date, g.*
           FROM unnest($3::text[], $4::text[], $5::bigint[], $6::bigint[], $7::float8[])
             AS g (dimension, grp, records, serious, mean_icd)",
    )
    .bind(dataset)
    .bind(&report.cut)
    .bind(
        rows.iter()
            .map(|g| g.dimension.as_str())
            .collect::<Vec<_>>(),
    )
    .bind(
        rows.iter()
            .map(|g| g.group.as_deref().unwrap_or(""))
            .collect::<Vec<_>>(),
    )
    .bind(rows.iter().map(|g| g.records).collect::<Vec<_>>())
    .bind(rows.iter().map(|g| g.serious).collect::<Vec<_>>())
    .bind(rows.iter().map(|g| g.icd).collect::<Vec<_>>())
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// Each SFC cell, with what the guide derives from its counts.
async fn cells(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    dataset: &str,
    report: &Report,
) -> Result<(), sqlx::Error> {
    let rows = &report.cells;
    let states: Vec<_> = rows.iter().map(|c| cell(c.ok, c.total)).collect();
    sqlx::query(
        "INSERT INTO quality.cells
             (dataset, cut, cell, ok, total, passes, missing, margin)
         SELECT $1, $2::date, c.*
           FROM unnest($3::text[], $4::bigint[], $5::bigint[], $6::bool[], $7::bigint[],
                       $8::bigint[])
             AS c (cell, ok, total, passes, missing, margin)",
    )
    .bind(dataset)
    .bind(&report.cut)
    .bind(rows.iter().map(|c| c.field.as_str()).collect::<Vec<_>>())
    .bind(rows.iter().map(|c| c.ok).collect::<Vec<_>>())
    .bind(rows.iter().map(|c| c.total).collect::<Vec<_>>())
    .bind(states.iter().map(|s| s.passes).collect::<Vec<_>>())
    .bind(states.iter().map(|s| s.missing).collect::<Vec<_>>())
    .bind(states.iter().map(|s| s.margin).collect::<Vec<_>>())
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// Each drift variable's percentiles, then each outlier group.
async fn distributions(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    dataset: &str,
    report: &Report,
) -> Result<(), sqlx::Error> {
    for d in &report.distributions {
        sqlx::query(
            "INSERT INTO quality.distribution_checks
                 (dataset, cut, variable, dimension, grp, percentiles)
             VALUES ($1, $2::date, $3, '', '', $4)",
        )
        .bind(dataset)
        .bind(&report.cut)
        .bind(&d.variable)
        .bind(&d.percentiles)
        .execute(&mut **tx)
        .await?;
    }
    let rows = &report.outliers;
    sqlx::query(
        "INSERT INTO quality.distribution_checks
             (dataset, cut, variable, dimension, grp, records, low, high)
         SELECT $1, $2::date, o.*
           FROM unnest($3::text[], $4::text[], $5::text[], $6::bigint[], $7::bigint[],
                       $8::bigint[])
             AS o (variable, dimension, grp, records, low, high)",
    )
    .bind(dataset)
    .bind(&report.cut)
    .bind(rows.iter().map(|o| o.variable.as_str()).collect::<Vec<_>>())
    .bind(
        rows.iter()
            .map(|o| o.dimension.as_str())
            .collect::<Vec<_>>(),
    )
    .bind(
        rows.iter()
            .map(|o| o.group.as_deref().unwrap_or(""))
            .collect::<Vec<_>>(),
    )
    .bind(rows.iter().map(|o| o.records).collect::<Vec<_>>())
    .bind(rows.iter().map(|o| o.low).collect::<Vec<_>>())
    .bind(rows.iter().map(|o| o.high).collect::<Vec<_>>())
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// Each queue's findings by since, an empty group as ''.
async fn queues(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    dataset: &str,
    report: &Report,
) -> Result<(), sqlx::Error> {
    let rows = &report.queues;
    sqlx::query(
        "INSERT INTO quality.queue_findings
             (dataset, cut, dimension, grp, since, open, resolved, priority)
         SELECT $1, $2::date, q.dimension, q.grp, q.since::date, q.open, q.resolved,
                q.priority
           FROM unnest($3::text[], $4::text[], $5::text[], $6::bigint[], $7::bigint[],
                       $8::float8[])
             AS q (dimension, grp, since, open, resolved, priority)",
    )
    .bind(dataset)
    .bind(&report.cut)
    .bind(
        rows.iter()
            .map(|q| q.dimension.as_str())
            .collect::<Vec<_>>(),
    )
    .bind(
        rows.iter()
            .map(|q| q.group.as_deref().unwrap_or(""))
            .collect::<Vec<_>>(),
    )
    .bind(rows.iter().map(|q| q.since.as_str()).collect::<Vec<_>>())
    .bind(rows.iter().map(|q| q.open).collect::<Vec<_>>())
    .bind(rows.iter().map(|q| q.resolved).collect::<Vec<_>>())
    .bind(rows.iter().map(|q| q.priority).collect::<Vec<_>>())
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// Each entity's ICD and band, then the summary's count of entities and of
/// each band; none without entities.
async fn entities(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    dataset: &str,
    report: &Report,
) -> Result<(), sqlx::Error> {
    let rows = &report.entities;
    if rows.is_empty() {
        return Ok(());
    }
    sqlx::query(
        "INSERT INTO quality.entity_icd (dataset, cut, entity, icd, band, segment)
         SELECT $1, $2::date, e.*
           FROM unnest($3::text[], $4::float8[], $5::text[], $6::text[])
             AS e (entity, icd, band, segment)",
    )
    .bind(dataset)
    .bind(&report.cut)
    .bind(rows.iter().map(|e| e.0.as_str()).collect::<Vec<_>>())
    .bind(rows.iter().map(|e| e.1).collect::<Vec<_>>())
    .bind(rows.iter().map(|e| e.1.map(band)).collect::<Vec<_>>())
    .bind(rows.iter().map(|e| e.2.as_deref()).collect::<Vec<_>>())
    .execute(&mut **tx)
    .await?;
    sqlx::query(
        "UPDATE quality.run_summary s
            SET entities = e.n, band_high = e.high, band_medium = e.medium, band_low = e.low
           FROM (SELECT count(*) AS n,
                        count(*) FILTER (WHERE band = 'high') AS high,
                        count(*) FILTER (WHERE band = 'medium') AS medium,
                        count(*) FILTER (WHERE band = 'low') AS low
                   FROM quality.entity_icd
                  WHERE dataset = $1 AND cut = $2::date) e
          WHERE s.dataset = $1 AND s.cut = $2::date",
    )
    .bind(dataset)
    .bind(&report.cut)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// Each stage's milliseconds, in the order it ran.
async fn stages(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    dataset: &str,
    report: &Report,
) -> Result<(), sqlx::Error> {
    let rows = &report.stages;
    sqlx::query(
        "INSERT INTO quality.job_stats (dataset, cut, stage, position, ms)
         SELECT $1, $2::date, s.stage, s.position::integer, s.ms
           FROM unnest($3::text[], $4::bigint[]) WITH ORDINALITY AS s (stage, ms, position)",
    )
    .bind(dataset)
    .bind(&report.cut)
    .bind(rows.iter().map(|s| s.stage.as_str()).collect::<Vec<_>>())
    .bind(rows.iter().map(|s| s.ms).collect::<Vec<_>>())
    .execute(&mut **tx)
    .await?;
    Ok(())
}
