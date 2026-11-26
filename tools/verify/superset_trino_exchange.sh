#!/bin/bash
# Verification V2 of decision 0089: Superset's queries reach Trino as the
# person, with query-trino's per-job token shape. Trino 484 with JWT
# authentication against the stand-in's key, behind the internal entry
# point (mTLS, the installation's CA), a user mapping for person-*, file
# rules from a URL (refresh 10 s) with a row filter on `dataset`, an Iceberg
# catalog over RustFS 1.0.5 (`gold`, read-only) with a quality_gold table of
# two datasets. Superset 6.1.0 with the module's connection mutator, which
# exchanges the person's token at the stand-in (RFC 8693). Checks: the
# mutator gets the effective user for a chart query, an explore query and
# SQL Lab; Trino's query log shows person-<id>; ana (loans) and bruno
# (savings) get only their rows for the same chart; the per-person cache
# (and, with it off, whether bruno gets ana's rows); a rule withdrawn; a
# token past exp, for a new statement and for a running one; a connection
# with no person; whether a guest session's request carries the guest token
# where the mutator reads it. Only names, counts and verdicts are printed.
#
# Containers, volumes and the network qd-verify-tx-…, removed on exit. Run
# alone, from the repository root:
#
#   bash tools/verify/superset_trino_exchange.sh
set -u
cd "$(dirname "$0")/../.."
P=qd-verify-tx
. tools/verify/superset_lib.sh
superset_wait_quiet
superset_image || exit 1
superset_lake || exit 1
superset_files
rules
superset_edge_up || exit 1
superset_trino || exit 1
superset_gold
superset_stores_up || exit 1
chmod 777 "$D/bundle"
EMBEDDED=1 superset_up || exit 1
sinit bundle /bundle trino.internal:8443 && sinit import /bundle >/dev/null && sinit embed "$DASH_UUID" "$EMBED_UUID" "https://$APP" >/dev/null
STATE=$(sinit state 2>/dev/null | tail -1)
DBID=$(jq -r '.databases[0][1]' <<<"$STATE")
DSID=$(jq -r '.datasets[0][1]' <<<"$STATE")
C1=$(jq -r '.charts[0][1]' <<<"$STATE")
DASHID=$(jq -r '.dashboards[0][1]' <<<"$STATE")
expect "the bundle imported: one database, one dataset, two charts, one dashboard" "1 1 2 1" \
  "$(jq -r '[.databases, .datasets, .charts, .dashboards | length] | join(" ")' <<<"$STATE")"
expect "Trino's version" 484 "$(tj "$(mint qd-quality 60)" "SELECT node_version FROM system.runtime.nodes")"

# chart PERSON REFERER-PATH [CURL OPTION...]: chart 1's data as the page
# asks it; prints the datasets of its rows and whether it came from the cache.
chart() {
  local who=$1 ref=$2 body
  shift 2
  body=$(jq -n --argjson ds "$DSID" --argjson s "$C1" --argjson dash "$DASHID" --argjson f "${FORCE:-false}" \
    '{datasource: {id: $ds, type: "table"}, force: $f, form_data: {slice_id: $s, dashboardId: $dash, viz_type: "table"},
      queries: [{columns: ["dataset", "cut", "records"], row_limit: 100, orderby: []}],
      result_format: "json", result_type: "full"}')
  bi "$who" /api/v1/chart/data -X POST -H 'Content-Type: application/json' -H "Referer: https://$APP/bi$ref" \
    -d "$body" "$@" | jq -r 'if .result then (.result[0] | ([.data[].dataset] | unique | join(",")) + " cached=" +
      (.is_cached // false | tostring)) else "error: " + ((.message // .errors[0].message // "?") | tostring | .[0:120]) end'
}
sqllab() {
  local body
  body=$(jq -n --argjson db "$DBID" '{database_id: $db, catalog: "gold", schema: "quality_gold", runAsync: false,
    sql: "SELECT dataset, count(*) AS n FROM quality_gold.chart_trend_by_cut GROUP BY dataset ORDER BY dataset",
    client_id: "v2sql", tab: "v2"}')
  bi "$1" /api/v1/sqllab/execute/ -X POST -H 'Content-Type: application/json' -H "X-CSRFToken: $(csrf "$1")" \
    -H "Referer: https://$APP/bi/sqllab/" -d "$body" |
    jq -r 'if (.data | length) > 0 then ([.data[].dataset] | join(",")) else "error: " + (tostring | .[0:300]) end'
}
mlog() { tail -n "${2:-1}" "$D/log/mutator.jsonl" | jq -r "$1"; }
id_of() { jq -r --arg p "$1" '.[$p].id' "$D/c/people.json"; }
flush() { docker exec "$P-valkey" sh -c 'valkey-cli -a "$(cat /c/valkey_password)" --no-auth-warning flushall' >/dev/null; }

echo "== The module's own client: lists gold's tables, reads no row"
expect "a connection with no person (superset-init's): its tables and rows" \
  '{"tables":["chart_trend_by_cut"],"rows":0}' "$(sinit noperson 2>/dev/null | tail -1 | jq -c .)"
expect "  the mutator saw no person and used the client token" "none" "$(mlog .kind)"
expect "  Trino ran it as bi-superset" "bi-superset" "$(tj "$(mint qd-quality 60)" \
  "SELECT DISTINCT \"user\" FROM system.runtime.queries WHERE query LIKE 'SELECT count(*) FROM quality_gold%'")"

echo "== Chart, explore and SQL Lab, each as the person"
for p in ana bruno carla; do signin "$p" >/dev/null; done
expect "ana, the dashboard's chart: loans only" "loans cached=false" "$(chart ana /superset/dashboard/qd-quality-en/)"
expect "  the mutator: ana's account id, a person's subject, from the dashboard" \
  "$(id_of ana) person dashboard" "$(mlog '[.username, .kind, (.source | ascii_downcase | sub("querysource."; ""))] | join(" ")')"
expect "bruno, the same chart: savings only" "savings cached=false" "$(chart bruno /superset/dashboard/qd-quality-en/)"
expect "ana, the chart in explore (forced, no cache): loans only" "loans cached=false" \
  "$(FORCE=true chart ana "/explore/?slice_id=$C1")"
expect "  the mutator: from explore" "$(id_of ana) chart" \
  "$(mlog '[.username, (.source | ascii_downcase | sub("querysource."; ""))] | join(" ")')"
expect "carla (author, both datasets) in SQL Lab" "loans,savings" "$(sqllab carla)"
expect "  the mutator: from SQL Lab, the token found by username (SQL Lab runs in a request of its own)" \
  "$(id_of carla) person-by-username sql_lab" \
  "$(mlog '[.username, .kind, (.source | ascii_downcase | sub("querysource."; ""))] | join(" ")')"
expect "ana (viewer) has no SQL Lab" "error" "$(sqllab ana | cut -d: -f1)"
expect "Trino's query log: the users of the chart table's queries" \
  "$(printf '%s\n' bi-superset "person-$(id_of ana)" "person-$(id_of bruno)" "person-$(id_of carla)" | sort | tr '\n' ' ')" \
  "$(tj "$(mint qd-quality 60)" "SELECT DISTINCT \"user\" FROM system.runtime.queries
     WHERE query LIKE '%chart_trend_by_cut%' AND \"user\" <> 'qd-quality'" | sort | tr '\n' ' ')"
expect "every exchange: the machine client, a person's subject, exp never past the subject's" \
  "bi-superset-machine person" "$(applog 'e.get("subject_kind")' | jq -r '[.client, .subject_kind] | join(" ")' | sort -u | tr '\n' ' ' | sed 's/ $//')"
note "the act claim {sub: bi-superset} is in each Trino token; Trino's query log shows the user only (system.runtime.queries has no claim)"
expect "exchanged tokens reused from the process's memory, not asked again" yes \
  "$(grep -c '"token": "reused"' "$D/log/mutator.jsonl" | awk '{print ($1 > 0) ? "yes" : "no"}')"
expect "Valkey holds no Trino token (no JWT in any key's value)" 0 \
  "$(docker exec "$P-valkey" sh -c 'a="-a $(cat /c/valkey_password) --no-auth-warning"; for n in 0 1 2 3; do
     valkey-cli $a -n $n --scan | while read -r k; do valkey-cli $a -n $n --raw dump "$k"; done; done' | grep -c 'eyJ')"

echo "== The cache is per person (CACHE_QUERY_BY_USER on)"
flush
expect "ana's chart, first" "loans cached=false" "$(chart ana /superset/dashboard/qd-quality-en/)"
expect "ana's chart, again: from the cache" "loans cached=true" "$(chart ana /superset/dashboard/qd-quality-en/)"
expect "bruno's same chart: his own rows, not ana's cache" "savings cached=false" "$(chart bruno /superset/dashboard/qd-quality-en/)"

echo "== A rule withdrawn (refresh 10 s)"
jq '.ana.datasets = []' "$D/c/people.json" >"$D/p.json" && cat "$D/p.json" >"$D/c/people.json"
rules
sleep 13
R=$(chart ana /superset/dashboard/qd-quality-en/)
note "ana's chart 13 s after the withdrawal, not forced (within the cache's 5 minutes): $R"
expect "  the cached rows are still served (the cache outlives a withdrawn permission)" "loans cached=true" "$R"
expect "  forced (no cache): refused by Trino" "Access Denied" \
  "$(FORCE=true chart ana /superset/dashboard/qd-quality-en/ | grep -o 'Access Denied' || echo rows)"
jq '.ana.datasets = ["loans"]' "$D/c/people.json" >"$D/p.json" && cat "$D/p.json" >"$D/c/people.json"
rules

echo "== A Trino token past exp"
T=$(mint "person-$(id_of ana)" 6)
sleep 8
expect "a new statement with an expired token" "ERROR: HTTP 401" "$(tj "$T" 'SELECT 1')"
T=$(mint "person-$(id_of ana)" 10)
R=$(docker run --rm --network "$P" -v "$D/c:/c:ro" -e T="$T" --entrypoint sh "$CURL" -c '
  c="--cacert /c/ca.crt --cert /c/bi-superset.crt --key /c/bi-superset.key -s"
  r=$(curl $c -H "Authorization: Bearer $T" --data-binary "SELECT x FROM UNNEST(sequence(1, 300000)) t(x)" https://trino.internal:8443/v1/statement)
  n=$(printf "%s" "$r" | sed -n "s/.*\"nextUri\":\"\([^\"]*\)\".*/\1/p")
  sleep 12
  curl $c -o /dev/null -w "%{http_code}" -H "Authorization: Bearer $T" "$n"')
note "the next page of a running statement, 12 s after its token (10 s) expired: HTTP $R"
expect "  a running statement's next page with the expired token is served" 200 "$R"

echo "== A guest session (embedded): the guest token where the mutator reads it"
G=$(web nobody "https://$APP/api/test/guest?person=ana&dataset=loans" | jq -r .token)
flush
expect "a guest chart query: loans only" "loans cached=false" \
  "$(chart guest /superset/dashboard/qd-quality-en/ -H "X-GuestToken: $G")"
expect "  the mutator: the guest token read from the request header, exchanged" "person-$(id_of ana) guest" \
  "$(mlog '[.username, .kind] | join(" ")')"
expect "  the provider took it as a guest subject" guest "$(applog 'e.get("subject_kind")' | tail -1 | jq -r .subject_kind)"

echo "== With CACHE_QUERY_BY_USER off"
docker rm -f "$P-superset" >/dev/null
flush
CACHE_BY_USER=0 EMBEDDED=1 superset_up || exit 1
for p in ana bruno; do signin "$p" >/dev/null; done
expect "ana's chart" "loans cached=false" "$(chart ana /superset/dashboard/qd-quality-en/)"
R=$(chart bruno /superset/dashboard/qd-quality-en/)
note "bruno's same chart with the per-person cache off: $R"
expect "  without the per-person cache bruno gets ana's cached rows" "loans cached=true" "$R"

echo
echo "passed $PASSED, failed $FAILED"
[ "$FAILED" = 0 ] && echo "ALL OK" || exit 1
