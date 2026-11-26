"""superset-init's stand-in for decision 0089's verification (superset_*.sh):
migrate and initialise the metadata database, the module's roles, the
dashboard bundle (a small definition with fixed UUIDs, standing in for step
9's generator), its import with overwrite, its normalised export, the
embedded record with a fixed UUID, and what the scripts compare. Synthetic
names only; prints names, UUIDs, ids and counts, never a secret.

    python superset_verify_init.py migrate | bundle DIR ENDPOINT | import DIR
        | archive DIR ZIP | export DIR | embed DASH EMBED ORIGIN | state | edit-chart UUID TITLE
        | copy-chart UUID TITLE | grant-guest
"""

import io
import json
import os
import subprocess
import sys
import zipfile

import yaml

DB_UUID = "8f6a3c1e-0089-4b00-9000-0000000000db"
DS_UUID = "8f6a3c1e-0089-4c00-9000-0000000000d5"
CHARTS = {  # uuid -> (title, viz, params)
    "8f6a3c1e-0089-4c10-9000-0000000000c1": ("Records by cut", "table",
                                              {"query_mode": "raw", "all_columns": ["dataset", "cut", "records"],
                                               "order_by_cols": [], "row_limit": 1000}),
    "8f6a3c1e-0089-4c10-9000-0000000000c2": ("Conformity by cut", "table",
                                              {"query_mode": "raw", "all_columns": ["dataset", "cut", "conformity"],
                                               "order_by_cols": [], "row_limit": 1000}),
}
DASH_UUID = "8f6a3c1e-0089-4d00-9000-00000000da5b"
TIMESTAMP = "2026-01-01T00:00:00+00:00"
ROLES = {"QD Viewer": "Gamma", "QD Author": "Alpha", "QD Guest": "Gamma"}


def dump(path, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        yaml.safe_dump(data, f, sort_keys=True, allow_unicode=True)


def bundle(out, endpoint):
    """The source files, as the module's repository would keep them."""
    dump(f"{out}/metadata.yaml", {"version": "1.0.0", "type": "assets", "timestamp": TIMESTAMP})
    dump(f"{out}/databases/Quality_gold.yaml", {
        "database_name": "Quality gold", "sqlalchemy_uri": f"trino://bi-superset@{endpoint}/gold",
        "cache_timeout": None, "expose_in_sqllab": True, "allow_run_async": False, "allow_ctas": False,
        "allow_cvas": False, "allow_dml": False, "allow_csv_upload": False, "impersonate_user": False,
        "extra": {"metadata_params": {}, "engine_params": {}, "metadata_cache_timeout": {},
                  "schemas_allowed_for_csv_upload": []},
        "configuration_method": "sqlalchemy_form", "uuid": DB_UUID, "version": "1.0.0"})
    col = lambda n, t: {"column_name": n, "verbose_name": None, "is_dttm": t == "DATE", "is_active": True,  # noqa: E731
                        "type": t, "advanced_data_type": None, "groupby": True, "filterable": True,
                        "expression": None, "description": None, "python_date_format": None,
                        "datetime_format": None, "extra": None}
    dump(f"{out}/datasets/Quality_gold/chart_trend_by_cut.yaml", {
        "table_name": "chart_trend_by_cut", "main_dttm_col": "cut", "currency_code_column": None,
        "description": None, "default_endpoint": None, "offset": 0, "cache_timeout": None, "catalog": "gold",
        "schema": "quality_gold", "sql": None, "params": None, "template_params": None,
        "filter_select_enabled": True, "fetch_values_predicate": None, "extra": None, "normalize_columns": False,
        "always_filter_main_dttm": False, "folders": None, "uuid": DS_UUID,
        "metrics": [{"metric_name": "count", "verbose_name": None, "metric_type": None, "expression": "COUNT(*)",
                     "description": None, "d3format": None, "currency": None, "extra": None,
                     "warning_text": None}],
        "columns": [col("conformity", "DOUBLE"), col("cut", "DATE"), col("dataset", "VARCHAR"),
                    col("records", "BIGINT"), col("run_id", "VARCHAR")],
        "version": "1.0.0", "database_uuid": DB_UUID})
    position = {"DASHBOARD_VERSION_KEY": "v2", "ROOT_ID": {"children": ["GRID_ID"], "id": "ROOT_ID", "type": "ROOT"},
                "GRID_ID": {"children": ["ROW-1"], "id": "GRID_ID", "parents": ["ROOT_ID"], "type": "GRID"},
                "HEADER_ID": {"id": "HEADER_ID", "meta": {"text": "Quality indicators"}, "type": "HEADER"},
                "ROW-1": {"children": [], "id": "ROW-1", "meta": {"background": "BACKGROUND_TRANSPARENT"},
                          "parents": ["ROOT_ID", "GRID_ID"], "type": "ROW"}}
    for i, (uuid, (title, viz, params)) in enumerate(sorted(CHARTS.items())):
        cid = f"CHART-{i + 1}"
        position["ROW-1"]["children"].append(cid)
        position[cid] = {"children": [], "id": cid, "meta": {"chartId": i + 1, "height": 50, "sliceName": title, "uuid": uuid,
                                                             "width": 6}, "parents": ["ROOT_ID", "GRID_ID", "ROW-1"],
                         "type": "CHART"}
        dump(f"{out}/charts/{title.replace(' ', '_')}.yaml", {
            "slice_name": title, "description": None, "certified_by": None, "certification_details": None,
            "viz_type": viz, "params": dict(params, viz_type=viz, annotation_layers=[]),
            # A guest may only ask for what the stored chart asks (Superset's
            # query_context_modified): without a query context, nothing.
            "query_context": json.dumps({"queries": [{"columns": params["all_columns"], "orderby": [],
                                                      "row_limit": params["row_limit"]}],
                                         "result_format": "json", "result_type": "full"}, sort_keys=True), "cache_timeout": None,
            "uuid": uuid, "version": "1.0.0", "dataset_uuid": DS_UUID})
    dump(f"{out}/dashboards/Quality_indicators.yaml", {
        "dashboard_title": "Quality indicators", "description": None, "css": None, "slug": "qd-quality-en",
        "certified_by": None, "certification_details": None, "published": True, "uuid": DASH_UUID,
        "position": position, "metadata": {"native_filter_configuration": [], "color_scheme": ""},
        "theme_uuid": None, "version": "1.0.0"})


def contents(src, root=""):
    """A bundle directory or zip as {path: text}; the importer takes the paths
    without the archive's root folder, as Superset's API strips it."""
    if src.endswith(".zip"):
        z = zipfile.ZipFile(src)
        return {n.split("/", 1)[1]: z.read(n).decode() for n in z.namelist() if not n.endswith("/")}
    out = {}
    for base, _, files in os.walk(src):
        for f in files:
            p = os.path.join(base, f)
            out[root + os.path.relpath(p, src)] = open(p).read()
    return out


def app_context():
    from superset.app import create_app

    return create_app().app_context()


INIT_USER = "bi-superset-init"


def as_admin():
    """Imports and exports run as superset-init's own user: an Admin that owns
    the shipped objects, inactive, with no password anyone knows."""
    from flask import g
    from superset import security_manager as sm

    g.user = sm.find_user(username=INIT_USER)


def archive(src, out):
    """The deterministic archive: sorted names, a fixed date and mode, one root."""
    with zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED) as z:
        files = contents(src, "bundle/")
        for rel in sorted(files):
            info = zipfile.ZipInfo(rel, date_time=(2026, 1, 1, 0, 0, 0))
            info.external_attr = 0o644 << 16
            z.writestr(info, files[rel])


def export_name(name):
    """Superset names a file <title>_<id>.yaml; the source keeps <title>.yaml."""
    import re

    return re.sub(r"_[0-9]+\.yaml$", ".yaml", name)


def normalise(name, text):
    data = yaml.safe_load(text)
    if name.endswith("metadata.yaml"):
        data["timestamp"] = TIMESTAMP
    if isinstance(data, dict) and isinstance(data.get("position"), dict):
        # The importer needs a chartId; the source numbers the charts by UUID.
        charts = sorted((v for v in data["position"].values() if isinstance(v, dict) and v.get("type") == "CHART"),
                        key=lambda v: v["meta"]["uuid"])
        for i, v in enumerate(charts):
            v["meta"]["chartId"] = i + 1
    if isinstance(data, dict) and isinstance(data.get("columns"), list):
        data["columns"].sort(key=lambda c: c["column_name"])
        data["metrics"].sort(key=lambda m: m["metric_name"])
    if isinstance(data, dict) and data.get("query_context"):
        # The import writes the installation's dataset id into it.
        qc = json.loads(data["query_context"])
        qc.pop("datasource", None)
        data["query_context"] = json.dumps(qc, sort_keys=True)
    for chart_params in [data.get("params")] if isinstance(data, dict) else []:
        if isinstance(chart_params, dict):
            chart_params.pop("datasource", None)
    return yaml.safe_dump(data, sort_keys=True, allow_unicode=True)


def main(cmd, *args):
    if cmd == "bundle":
        return bundle(*args)
    if cmd == "archive":
        return archive(*args)
    if cmd == "migrate":
        subprocess.run(["superset", "db", "upgrade"], check=True, capture_output=True)
        subprocess.run(["superset", "init"], check=True, capture_output=True)
    with app_context():
        from superset import db, security_manager as sm
        from superset.connectors.sqla.models import SqlaTable
        from superset.models.core import Database
        from superset.models.dashboard import Dashboard
        from superset.models.embedded_dashboard import EmbeddedDashboard
        from superset.models.slice import Slice

        if cmd == "migrate":
            for role, base in ROLES.items():
                sm.copy_role(base, role, merge=False)
            # The guest role holds no datasource permission: a guest reaches a
            # dataset only through a chart of the dashboard its token names.
            for role in ("QD Viewer", "QD Author"):
                r = sm.find_role(role)
                pvms = [sm.find_permission_view_menu("all_datasource_access", "all_datasource_access")]
                if role == "QD Author":  # Alpha plus Superset's own sql_lab role
                    pvms += list(sm.find_role("sql_lab").permissions)
                for pvm in pvms:
                    if pvm and pvm not in r.permissions:
                        r.permissions.append(pvm)
            if not sm.find_user(username=INIT_USER):
                u = sm.add_user(INIT_USER, "superset-init", "bi-superset", "bi-superset-init@invalid.example",
                                [sm.find_role("Admin")], password=os.urandom(24).hex())
                u.active = False
            db.session.commit()
            print("roles", *sorted(r.name for r in sm.get_all_roles()))
        elif cmd == "import":
            # The assets importer overwrites everything, matched by UUID; the
            # dashboard importer would keep an existing database, dataset and chart.
            from superset.commands.importers.v1.assets import ImportAssetsCommand

            as_admin()
            ImportAssetsCommand(contents(args[0])).run()
            print("imported")
        elif cmd == "export":
            from superset.commands.export.assets import ExportAssetsCommand

            as_admin()
            buf = io.BytesIO()
            with zipfile.ZipFile(buf, "w") as z:
                for name, content in ExportAssetsCommand().run():
                    rel = export_name(name.split("/", 1)[1] if name.startswith("dashboard_export") else name)
                    z.writestr(rel, normalise(rel, content()))
            out = args[0]
            for n in zipfile.ZipFile(buf).namelist():
                p = os.path.join(out, n)
                os.makedirs(os.path.dirname(p), exist_ok=True)
                open(p, "w").write(zipfile.ZipFile(buf).read(n).decode())
            print("exported", len(zipfile.ZipFile(buf).namelist()))
        elif cmd == "embed":
            dash = db.session.query(Dashboard).filter_by(uuid=args[0]).one()
            emb = db.session.query(EmbeddedDashboard).filter_by(dashboard_id=dash.id).one_or_none()
            if emb is None:
                emb = EmbeddedDashboard(uuid=args[1], dashboard_id=dash.id)
                db.session.add(emb)
            emb.allow_domain_list = args[2]
            db.session.commit()
            print("embedded", emb.uuid)
        elif cmd == "state":
            rows = lambda m: sorted([str(o.uuid), o.id] for o in db.session.query(m).all())  # noqa: E731
            print(json.dumps({"databases": rows(Database), "datasets": rows(SqlaTable), "charts": rows(Slice),
                              "dashboards": rows(Dashboard),
                              "embedded": sorted([str(e.uuid), e.dashboard_id]
                                                 for e in db.session.query(EmbeddedDashboard).all()),
                              "chart_titles": sorted(s.slice_name for s in db.session.query(Slice).all()),
                              "uri": [d.sqlalchemy_uri for d in db.session.query(Database).all()]}))
        elif cmd == "noperson":  # a connection with no person: the module's client token
            from sqlalchemy import inspect, text

            d = db.session.query(Database).filter_by(uuid=DB_UUID).one()
            with d.get_sqla_engine(catalog="gold", schema="quality_gold") as engine:
                tables = sorted(inspect(engine).get_table_names("quality_gold"))
                with engine.connect() as c:
                    n = c.execute(text("SELECT count(*) FROM quality_gold.chart_trend_by_cut")).scalar()
            print(json.dumps({"tables": tables, "rows": n}))
        elif cmd == "edit-chart":
            s = db.session.query(Slice).filter_by(uuid=args[0]).one()
            s.slice_name = args[1]
            db.session.commit()
        elif cmd == "copy-chart":
            s = db.session.query(Slice).filter_by(uuid=args[0]).one()
            db.session.add(Slice(slice_name=args[1], viz_type=s.viz_type, params=s.params,
                                 datasource_id=s.datasource_id, datasource_type=s.datasource_type))
            db.session.commit()


if __name__ == "__main__":
    main(*sys.argv[1:])
