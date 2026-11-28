/**
 * A dataset's charts as figures (decision 0031): each chart of the catalog,
 * by id, from the rows the API gives for a cut, drawn with the guide's
 * primitives. Pure, for the dashboard to put on the page.
 */

import { bars, figure, lines, num, pct, type Figure, type Lang } from "@knime-orchestador/charts";
import { bandsBySegment, distribution, lowestIcd, transitions } from "./dashboardIcd";
import { outliers, psi } from "./dashboardDistribution";
import { groupsChanged, largestGroups, pChart, SPLIT, splitBy } from "./dashboardGroups";
import { queues } from "./dashboardQueues";
import { recency } from "./dashboardRecency";
import { timeByStage } from "./dashboardStages";
import { sfcView } from "./dashboardSfc";

/** One row of a chart, as its query names its columns. */
export type Row = Record<string, unknown>;

/** The texts a figure needs, by key under `quality:charts.`; a key it does
 * not have gives its last part (a dimension or severity it does not name). */
export type Texts = (key: string) => string;

/** The most bars a chart draws; its table has every row. */
export const MOST_BARS = 15;

const n = (v: unknown) => (typeof v === "number" ? v : Number(v ?? 0));
const s = (v: unknown) => (v === null || v === undefined || v === "" ? "—" : String(v));
const named = (tr: Texts, kind: string, v: unknown) =>
  v === null || v === undefined || v === "" ? "—" : tr(`${kind}Name.${String(v)}`);
const share = (lang: Lang, v: unknown) =>
  v === null || v === undefined ? "—" : pct(lang, n(v), 1);

/** Conformity by cut, and the IGCD (decision 0057) once a cut has one. */
function trend(lang: Lang, rows: Row[], tr: Texts): Figure {
  const value = (v: unknown) => (v === null || v === undefined ? null : n(v));
  const series = [
    {
      label: tr("conformity"),
      color: "var(--viz-s1)",
      values: rows.map((r) => value(r.conformity)),
    },
  ];
  const igcd = rows.map((r) => value(r.igcd));
  if (igcd.some((v) => v !== null)) {
    series.push({ label: tr("igcd"), color: "var(--viz-s2)", values: igcd });
  }
  const known = series.flatMap((x) => x.values).filter((v): v is number => v !== null);
  const low = Math.min(...known, 1);
  return {
    title: tr("trend_by_cut"),
    live: true,
    plot: {
      svg: lines({
        xs: rows.map((r) => s(r.cut)),
        series,
        yMin: Math.max(0, Math.floor(low * 10) / 10 - 0.1),
        yMax: 1,
        yFmt: (v) => pct(lang, v),
      }),
      legend: series.map((x) => ({ label: x.label, color: x.color, kind: "line" as const })),
      table: {
        head: [tr("cut"), tr("records"), tr("failures"), tr("conformity"), tr("igcd")],
        rows: rows.map((r) => [
          s(r.cut),
          num(lang, n(r.records)),
          num(lang, n(r.failures)),
          share(lang, r.conformity),
          r.igcd === null || r.igcd === undefined ? "—" : num(lang, n(r.igcd), 3),
        ]),
      },
    },
  };
}

/** Bars of failures by a label, the most first; every row in the table. */
function failures(
  lang: Lang,
  rows: Row[],
  title: string,
  label: (r: Row) => string,
  head: string[],
  cells: (r: Row) => string[],
): Figure {
  const drawn = rows.slice(0, MOST_BARS);
  const max = Math.max(1, ...drawn.map((r) => n(r.failures)));
  return {
    title,
    live: true,
    plot: {
      svg: bars({
        rows: drawn.map((r) => ({ label: label(r), value: n(r.failures), muted: !n(r.failures) })),
        max,
        fmt: (v) => num(lang, v),
      }),
      table: { head, rows: rows.map(cells) },
    },
  };
}

function worstRules(lang: Lang, rows: Row[], tr: Texts): Figure {
  const applied = rows.filter((r) => r.conformity !== null && r.conformity !== undefined);
  return {
    title: tr("conformity_by_rule"),
    live: true,
    plot: {
      svg: bars({
        rows: applied
          .slice(0, MOST_BARS)
          .map((r) => ({ label: `${s(r.rule)} · ${s(r.field)}`, value: n(r.conformity) })),
        max: 1,
        fmt: (v) => pct(lang, v, 1),
      }),
      table: {
        head: [
          tr("rule"),
          tr("field"),
          tr("dimension"),
          tr("severity"),
          tr("applies"),
          tr("failures"),
          tr("conformity"),
        ],
        rows: rows.map((r) => [
          s(r.rule),
          s(r.field),
          named(tr, "dimension", r.dimension),
          named(tr, "severity", r.severity),
          num(lang, n(r.applies)),
          num(lang, n(r.failures)),
          share(lang, r.conformity),
        ]),
      },
    },
  };
}

function profiling(lang: Lang, rows: Row[], tr: Texts): Figure {
  return {
    title: tr("profiling"),
    live: true,
    plot: {
      svg: bars({
        rows: rows
          .slice(0, MOST_BARS)
          .map((r) => ({ label: s(r.field), value: n(r.empty_share), muted: !n(r.empty_share) })),
        max: 1,
        fmt: (v) => pct(lang, v, 1),
      }),
      table: {
        head: [
          tr("field"),
          tr("empty"),
          tr("emptyShare"),
          tr("distinct"),
          tr("pattern"),
          tr("patternShare"),
        ],
        rows: rows.map((r) => [
          s(r.field),
          num(lang, n(r.empty)),
          share(lang, r.empty_share),
          num(lang, n(r.distinct_values)),
          r.floating === true ? tr("floating") : s(r.pattern),
          share(lang, r.pattern_share),
        ]),
      },
    },
  };
}

/** The chart's figure; none for a chart this page does not know. */
export function chartFigure(id: string, rows: Row[], lang: Lang, tr: Texts): Figure | undefined {
  if (!rows.length) return undefined;
  const counts = (r: Row) => [
    num(lang, n(r.rules)),
    num(lang, n(r.applies)),
    num(lang, n(r.failures)),
  ];
  switch (id) {
    case "trend_by_cut":
      return trend(lang, rows, tr);
    case "failures_by_dimension":
      return failures(
        lang,
        rows,
        tr(id),
        (r) => `${named(tr, "dimension", r.dimension)} · ${named(tr, "severity", r.severity)}`,
        [tr("dimension"), tr("severity"), tr("rules"), tr("applies"), tr("failures")],
        (r) => [
          named(tr, "dimension", r.dimension),
          named(tr, "severity", r.severity),
          ...counts(r),
        ],
      );
    case "failures_by_field":
      return failures(
        lang,
        rows,
        tr(id),
        (r) => s(r.field),
        [tr("field"), tr("rules"), tr("applies"), tr("failures")],
        (r) => [s(r.field), ...counts(r)],
      );
    case "lowest_icd":
      return lowestIcd(lang, rows, tr, MOST_BARS);
    case "icd_distribution":
      return distribution(lang, rows, tr);
    case "bands_by_segment":
      return bandsBySegment(lang, rows, tr, MOST_BARS);
    case "band_transitions":
      return transitions(lang, rows, tr);
    case "sfc_view":
      return sfcView(lang, rows, tr);
    case "groups_p_chart":
      return pChart(lang, rows, tr);
    case "largest_groups":
      return largestGroups(lang, rows, tr);
    case "groups_changed":
      return groupsChanged(lang, rows, tr);
    case "outliers_by_group":
      return outliers(lang, rows, tr);
    case "psi_by_decile":
      return psi(lang, rows, tr);
    case "remediation_queues":
      return queues(lang, rows, tr);
    case "update_recency":
      return recency(lang, rows, tr);
    case "profiling":
      return profiling(lang, rows, tr);
    case "conformity_by_rule":
      return worstRules(lang, rows, tr);
    case "time_by_stage":
      return timeByStage(lang, rows, tr);
    default:
      return undefined;
  }
}

/** The chart drawn as HTML, every value escaped by the primitives; a
 * chart by group once per dimension (or variable), named after it. */
export function chartHtml(id: string, rows: Row[], lang: Lang, tr: Texts): string {
  const draw = (rs: Row[], title?: string) => {
    const f = chartFigure(id, rs, lang, tr);
    return f ? figure(lang, title ? { ...f, title: `${f.title} · ${title}` } : f) : "";
  };
  const key = SPLIT[id];
  if (!key) return draw(rows);
  return splitBy(rows, key)
    .map(([d, rs]) => draw(rs, d))
    .join("");
}
