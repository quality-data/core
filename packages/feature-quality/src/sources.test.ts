import { describe, expect, it } from "vitest";
import { QualityError, type Driver } from "./api";
import {
  deleteRefusal,
  filterTables,
  missing,
  refusalText,
  sourceBody,
  tableRef,
  validName,
} from "./sources";

const postgres: Driver = {
  id: "postgres",
  engine: "duckdb",
  default_port: 5432,
  fields: [
    { key: "host", kind: "text", required: true },
    { key: "port", kind: "port", required: false },
    { key: "database", kind: "text", required: true },
    { key: "user", kind: "secret", required: true },
    { key: "password", kind: "secret", required: true },
  ],
};

describe("sourceBody", () => {
  it("splits the form by the driver, leaving empty secrets out to keep them", () => {
    const body = sourceBody(postgres, {
      host: " db.example ",
      database: "core",
      user: "reader",
      password: "",
      port: "",
    });
    expect(body).toEqual({
      driver: "postgres",
      fields: { host: "db.example", database: "core" },
      secrets: { user: "reader" },
    });
  });
});

describe("an optional flag", () => {
  const sqlserver: Driver = {
    ...postgres,
    id: "sqlserver",
    fields: [
      ...postgres.fields,
      { key: "trust_server_certificate", kind: "flag", required: false },
    ],
  };
  it("is sent only when set, so off is left out", () => {
    const base = { host: "db", database: "core", user: "reader", password: "synthetic" };
    expect(sourceBody(sqlserver, base).fields).toEqual({ host: "db", database: "core" });
    expect(sourceBody(sqlserver, { ...base, trust_server_certificate: "true" }).fields).toEqual({
      host: "db",
      database: "core",
      trust_server_certificate: "true",
    });
    expect(missing(sqlserver, base, [])).toEqual([]);
  });
});

describe("missing", () => {
  it("names the required fields still empty, but not a secret already set", () => {
    expect(missing(postgres, { host: "db" }, ["user", "password"])).toEqual(["database"]);
    expect(missing(postgres, { host: "db", database: "core" }, [])).toEqual(["user", "password"]);
  });
});

describe("names and refusals", () => {
  it("takes what a mapping may call a source", () => {
    expect(validName("core_2")).toBe(true);
    expect(validName("Core")).toBe(false);
    expect(validName("2core")).toBe(false);
  });

  it("says each refusal in the person's language, with its field", () => {
    expect(refusalText({ code: "missing", field: "password" })).toContain("Contraseña");
    expect(refusalText({ code: "something_new" })).not.toBe("");
  });

  it("says which datasets keep a source from being deleted", () => {
    const inUse = new QualityError(409, { datasets: ["loans", "clients"] });
    expect(deleteRefusal(inUse)).toContain("loans, clients");
    expect(deleteRefusal(new QualityError(502, null))).toBe(deleteRefusal(new Error("x")));
  });
});

describe("filterTables", () => {
  const tables = [
    { schema: "public", name: "clients", columns: [] },
    { schema: "loans", name: "credits", columns: [] },
    { schema: "loans", name: "client_guarantees", columns: [] },
  ];

  it("keeps every table for an empty search", () => {
    expect(filterTables(tables, "  ")).toEqual(tables);
  });

  it("matches every word against schema.name, any case", () => {
    expect(filterTables(tables, "CLIENT").map(tableRef)).toEqual([
      "public.clients",
      "loans.client_guarantees",
    ]);
    expect(filterTables(tables, "loans client").map(tableRef)).toEqual(["loans.client_guarantees"]);
    expect(filterTables(tables, "nothing")).toEqual([]);
  });
});
