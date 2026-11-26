#!/bin/bash
# Verification V3 of decision 0089: the embedded SDK works with a guest
# token the core issues. Superset 6.1.0 under /bi behind Traefik with the
# application's frame headers, EMBEDDED_SUPERSET on, GUEST_TOKEN_JWT_ALGO
# ES256 with the public key only; the stand-in issues the guest token as
# the core's route would (ES256, aud bi-superset, user person-<id>, the
# embedded dashboard, rls_rules dataset = '<dataset>') after its own check;
# a page of the application's origin with @superset-ui/embedded-sdk 0.4.0
# (the npm archive, checked by its SHA-512), driven by this machine's
# headless Chromium (grafana_browser.py, as 0073's V1). Trino 484 and RustFS
# as V2. Checks: a good token draws the dashboard; another key, another
# aud, an expired one, another dashboard are refused; the RLS clause is in
# every chart query (Trino's log); a changed filter or dataset cannot show
# another dataset; the SDK renews through fetchGuestToken before exp and
# the page keeps working past it (guest tokens of 30 s here; the record
# sets 300 s: the behaviour, not the length, is shown); Superset's own
# guest-token endpoint is unused; the first load's time and requests, and
# the second's; Superset down gives the page a state it can read (and
# whether the SDK's promise says so).
#
# Containers, volumes and the network qd-verify-v3-…, removed on exit, the
# browser stopped by its PID. Run alone, from the repository root:
#
#   bash tools/verify/superset_embedded_guest.sh
set -u
cd "$(dirname "$0")/../.."
P=qd-verify-v3
SDK_VERSION=0.4.0
SDK_SHA512=k/PnzvxO0xfeaAO6DRWsC7nG9oH3ooyBQNTRi/EXrK4DIH4ufx6Wy3AD3qDkCjGRw3KMdavCdwAiu2u4TVQ7oA==
SDK_TGZ=${SDK_TGZ:-$HOME/.cache/qd-verify/embedded-sdk-$SDK_VERSION.tgz}
. tools/verify/superset_lib.sh
superset_wait_quiet
superset_image || exit 1
if [ ! -f "$SDK_TGZ" ]; then
  mkdir -p "$(dirname "$SDK_TGZ")"
  curl -sSfL -m 120 -o "$SDK_TGZ.part" \
    "https://registry.npmjs.org/@superset-ui/embedded-sdk/-/embedded-sdk-$SDK_VERSION.tgz" && mv "$SDK_TGZ.part" "$SDK_TGZ"
fi
expect "the SDK $SDK_VERSION archive's SHA-512" "$SDK_SHA512" "$(openssl dgst -sha512 -binary "$SDK_TGZ" | base64 -w0)"
superset_lake || exit 1
superset_files
tar xzf "$SDK_TGZ" -O package/bundle/index.js >"$D/c/sdk.js"
rules
chmod 777 "$D/bundle"
superset_edge_up || exit 1
superset_trino || exit 1
superset_gold
superset_stores_up || exit 1
EMBEDDED=1 superset_up || exit 1
sinit bundle /bundle/src trino.internal:8443 && sinit import /bundle/src >/dev/null &&
  sinit embed "$DASH_UUID" "$EMBED_UUID" "https://$APP" >/dev/null
STATE=$(sinit state 2>/dev/null | tail -1)
DSID=$(jq -r '.datasets[0][1]' <<<"$STATE")
C1=$(jq -r '.charts[0][1]' <<<"$STATE")
DASHID=$(jq -r '.dashboards[0][1]' <<<"$STATE")

# gchart TOKEN [FILTER-VALUE]: chart 1's data with a guest token, optionally
# asking for one dataset: the datasets of its rows, or the error.
gchart() {
  local body
  body=$(jq -n --argjson ds "$DSID" --argjson s "$C1" --argjson dash "$DASHID" --arg v "${2:-}" \
    '{datasource: {id: $ds, type: "table"}, force: true, form_data: {slice_id: $s, dashboardId: $dash, viz_type: "table"},
      queries: [{columns: ["dataset", "cut", "records"], row_limit: 100, orderby: [],
                 filters: (if $v == "" then [] else [{col: "dataset", op: "==", val: $v}] end)}],
      result_format: "json", result_type: "full"}')
  web guest "https://$APP/bi/api/v1/chart/data" -X POST -H 'Content-Type: application/json' -H "X-GuestToken: $1" \
    -H "Referer: https://$APP/bi/embedded/$EMBED_UUID" -d "$body" -w '\n%{http_code}' | python3 -I -c 'import json, sys
body, code = sys.stdin.read().rsplit("\n", 1)
try: r = json.loads(body)
except Exception: r = {}
rows = r.get("result", [{}])[0].get("data") if "result" in r else None
print(code, ",".join(sorted({x["dataset"] for x in rows})) if rows is not None else (r.get("msg") or r.get("message") or "")[:60])'
}
guest() { web nobody "https://$APP/api/test/guest?person=${1:-ana}&dataset=${2:-loans}&${3:-}" | jq -r '.token // .error'; }
spki=$(openssl x509 -in "$D/c/app.crt" -pubkey -noout | openssl pkey -pubin -outform der | openssl dgst -sha256 -binary | base64)
browse() { python3 -I tools/verify/grafana_browser.py "$APP" "$TRAEFIK_IP" "$spki" "${PROFILE:-$D/chrome-$RANDOM}" "$1"; }
get() { jq -r --arg k "$1" '.[$k] | if type == "object" or type == "array" then tojson else tostring end' "$D/out.json"; }

echo "== Guest tokens: the good one and the refused ones"
expect "a token signed by the core's key: loans rows" "200 loans" "$(gchart "$(guest)")"
expect "signed by another key" 401 "$(gchart "$(guest ana loans key=other)" | cut -d' ' -f1)"
expect "for another audience" 401 "$(gchart "$(guest ana loans aud=elsewhere)" | cut -d' ' -f1)"
expect "expired" 401 "$(gchart "$(guest ana loans ttl=-30)" | cut -d' ' -f1)"
expect "naming another dashboard" 403 "$(gchart "$(guest ana loans dashboard=8f6a3c1e-0089-4d00-9000-0000000000ff)" | cut -d' ' -f1)"
expect "the core refuses a guest token for a dataset ana may not view" forbidden "$(guest ana savings)"
expect "a filter asking for savings with ana's loans token: no row" "200 " "$(gchart "$(guest)" savings)"

echo "== The page: the SDK, fetchGuestToken, the frame (guest tokens of 30 s)"
ROWS="f.querySelectorAll('table tbody tr').length"
READY="f && f.body && $ROWS >= 4"
PAGE="https://$APP/page?person=ana&dataset=loans&ttl=30"
cat >"$D/s1.json" <<JSON
[{"mark":"first"},{"go":"$PAGE"},{"wait":"$READY","timeout":120,"as":"t"},{"net":"first"},
 {"eval":"f.location.pathname","as":"where"},{"eval":"window.embedded === true","as":"embedded"},
 {"eval":"f.body.innerText.includes('savings')","as":"savings"},{"eval":"$ROWS","as":"rows"},{"eval":"window.fetches","as":"fetches0"},
 {"sleep":70},{"eval":"window.fetches","as":"fetches"},{"eval":"window.failures","as":"failures"},
 {"mark":"second"},{"go":"$PAGE"},{"wait":"$READY","timeout":120,"as":"t2"},{"net":"second"},{"cookies":"cookies"}]
JSON
PROFILE=$D/chrome-ana browse "$D/s1.json" >"$D/out.json"
expect "the frame shows the dashboard under /bi/embedded" "/bi/embedded/$EMBED_UUID" "$(get where)"
expect "the SDK's embedDashboard resolved" true "$(get embedded)"
expect "the two charts' rows in the frame (two cuts each)" 4 "$(get rows)"
expect "no savings row in the frame" false "$(get savings)"
note "first load: $(get t) s to the rows, requests $(get first); a second load in the same browser: $(get t2) s, requests $(get second)"
F=$(get fetches)
expect "fetchGuestToken was called again before exp, more than once in 70 s" yes \
  "$([ "$F" -ge 3 ] 2>/dev/null && echo yes || echo "no ($F)")"
expect "no renewal failed" 0 "$(get failures)"
note "cookies in the page's browser: $(get cookies)"

echo "== The page keeps working past the first tokens' exp"
cat >"$D/s2.json" <<JSON
[{"go":"$PAGE"},{"wait":"$READY","timeout":120,"as":"t"},{"sleep":70},
 {"eval":"(window.dash.getDashboardPermalink('qd').then(u => window.link = 'ok', e => window.link = 'error: ' + e), 1)","as":"z"},
 {"sleep":3},{"eval":"String(window.link)","as":"link"},{"eval":"window.fetches","as":"fetches"},
 {"eval":"$ROWS","as":"still"}]
JSON
browse "$D/s2.json" >"$D/out.json"
expect "after 70 s (two tokens' lives) the frame still shows the rows" 4 "$(get still)"
expect "and an API call of the frame (the SDK's permalink) works with the renewed token" ok "$(get link)"
note "tokens fetched by the page in that time: $(get fetches)"

echo "== Trino's log: the RLS clause in every guest chart query"
Q=$(tj "$(mint qd-quality 60)" "SELECT count(*), count_if(query LIKE '%dataset = ''loans''%') FROM system.runtime.queries
  WHERE \"user\" = 'person-$ANA' AND query LIKE '%chart_trend_by_cut%'")
expect "ana's chart queries, and those carrying dataset = 'loans'" yes \
  "$(awk -F'\t' '{ print ($1 > 0 && $1 == $2) ? "yes" : "no (" $0 ")" }' <<<"$Q")"
note "ana's chart queries in Trino: $(cut -f1 <<<"$Q")"

echo "== Superset's own guest-token endpoint is unused"
expect "requests to /bi/api/v1/security/guest_token" 0 "$(grep -c 'security/guest_token' "$D/log/access.json")"

echo "== Superset down"
docker stop "$P-superset" >/dev/null
cat >"$D/s3.json" <<JSON
[{"go":"$PAGE"},{"sleep":15},{"eval":"window.embedded === true","as":"embedded"},
 {"eval":"String(window.embedError || '')","as":"error"},
 {"eval":"(fetch('/bi/health').then(r => window.health = r.status), 1)","as":"x"},{"sleep":2},
 {"eval":"window.health","as":"health"},{"eval":"f ? f.body.innerText.slice(0, 60) : 'unreadable'","as":"frame"}]
JSON
browse "$D/s3.json" >"$D/out.json"
expect "after 15 s the SDK has resolved anyway (the frame loaded Traefik's error page)" true "$(get embedded)"
expect "the page reads /bi/health" 502 "$(get health)"
note "the frame's text: $(get frame | tr '\n' ' '); the SDK's error: $(get error)"

echo
echo "passed $PASSED, failed $FAILED"
[ "$FAILED" = 0 ] && echo "ALL OK" || exit 1
