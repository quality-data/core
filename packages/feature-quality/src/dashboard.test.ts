import { describe, expect, it } from "vitest";
import { chartFigure, chartHtml, MOST_BARS, type Row } from "./dashboard";

// Keys as texts, so the test reads what each place asks for.
const tr = (key: string) => `[${key}]`;

describe("a dataset's charts", () => {
  it("draws the trend as one line with a row per cut in its table", () => {
    const rows: Row[] = [
      { cut: "2026-02-28", records: 90, failures: 9, conformity: 0.9 },
      { cut: "2026-03-31", records: 100, failures: 40, conformity: 0.8 },
    ];
    const f = chartFigure("trend_by_cut", rows, "en", tr)!;
    expect(f.title).toBe("[trend_by_cut]");
    expect(f.live).toBe(true);
    expect(f.plot.svg).toContain("viz-line");
    expect(f.plot.table.rows).toEqual([
      ["2026-02-28", "90", "9", "90.0%", "—"],
      ["2026-03-31", "100", "40", "80.0%", "—"],
    ]);
  });

  it("names dimensions and severities, and a rule with no conformity has none", () => {
    const f = chartFigure(
      "failures_by_dimension",
      [{ dimension: "validity", severity: "major", rules: 2, applies: 200, failures: 40 }],
      "es",
      tr,
    )!;
    expect(f.plot.table.rows[0]).toEqual([
      "[dimensionName.validity]",
      "[severityName.major]",
      "2",
      "200",
      "40",
    ]);
    const rules = chartFigure(
      "conformity_by_rule",
      [
        {
          rule: "L-03",
          field: null,
          dimension: null,
          severity: "minor",
          applies: 0,
          failures: 0,
          conformity: null,
        },
      ],
      "en",
      tr,
    )!;
    expect(rules.plot.table.rows[0]).toEqual([
      "L-03",
      "—",
      "—",
      "[severityName.minor]",
      "0",
      "0",
      "—",
    ]);
  });

  it("draws the most failing bars only, every row in the table", () => {
    const rows: Row[] = Array.from({ length: MOST_BARS + 5 }, (_, i) => ({
      field: `Field${i}`,
      rules: 1,
      applies: 10,
      failures: 30 - i,
    }));
    const f = chartFigure("failures_by_field", rows, "en", tr)!;
    expect(f.plot.table.rows).toHaveLength(MOST_BARS + 5);
    expect(f.plot.svg.match(/class="viz-mark"/g)).toHaveLength(MOST_BARS);
  });

  it("escapes what it draws, and draws nothing without rows or for a chart it does not know", () => {
    const html = chartHtml(
      "failures_by_field",
      [{ field: "<b>", rules: 1, applies: 1, failures: 1 }],
      "en",
      tr,
    );
    expect(html).toContain("&lt;b&gt;");
    expect(html).not.toContain("<b>");
    expect(html).not.toContain("viz-demo");
    expect(chartHtml("trend_by_cut", [], "en", tr)).toBe("");
    expect(chartFigure("ghost", [{ a: 1 }], "en", tr)).toBeUndefined();
  });
});

describe("a dataset's profile", () => {
  it("profiles fields by their empty share, with their commonest shape", () => {
    const f = chartFigure(
      "profiling",
      [
        {
          field: "Note",
          records: 100,
          empty: 100,
          empty_share: 1,
          distinct_values: 0,
          pattern: null,
          pattern_share: null,
        },
        {
          field: "Amount",
          records: 100,
          empty: 20,
          empty_share: 0.2,
          distinct_values: 70,
          pattern: "9999.9",
          pattern_share: 0.75,
        },
        {
          field: "Rate",
          records: 100,
          empty: 0,
          empty_share: 0,
          distinct_values: 90,
          pattern: null,
          floating: true,
          pattern_share: 0,
        },
      ],
      "en",
      tr,
    )!;
    expect(f.plot.table.rows).toEqual([
      ["Note", "100", "100.0%", "0", "—", "—"],
      ["Amount", "20", "20.0%", "70", "9999.9", "75.0%"],
      ["Rate", "0", "0.0%", "90", "[floating]", "0.0%"],
    ]);
  });
});
