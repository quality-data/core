# shellcheck shell=bash
# Shared by decision 0089's verification scripts (superset_*.sh). On top of
# trino_engine_lib.sh (the throwaway network $P, RustFS 1.0.5 and the
# counters): apache/superset:6.1.0 extended as the module would (the Trino
# driver, Authlib and the PostgreSQL driver pinned), its metadata database
# (PostgreSQL 16) and Valkey, the stand-in provider and page
# (superset_standins.py), Traefik v3.6 serving qd.verify.example (Superset
# under /bi with the application's frame headers, the provider under /api)
# and the internal entry point trino.internal:8443 (mTLS, the installation's
# CA) in front of Trino 484 with JWT authentication. No port is published;
# throwaway keys and secrets in "$D", never printed; synthetic people and
# rows. Sourced after P is set; the trap removes every "$P-…" container,
# volume and the network, also on failure.
: "${P:?set P to qd-verify-<name> before sourcing}"
export LC_ALL=C
. tools/verify/trino_engine_lib.sh
ROOT=$PWD
APP=qd.verify.example
SUPERSET_BASE=apache/superset:6.1.0@sha256:16b50bbef6648912a79e3293d418fefa743ce555d36c5af6b85d859119ed7f88
# The extended image's tag names what it holds; built once, kept as a cache.
SUPERSET_IMAGE=qd-verify-superset:6.1.0-trino0.340.0-authlib1.9.4-pg2.9.9
VALKEY_IMAGE=valkey/valkey:9.0.8-alpine
POSTGRES_IMAGE=postgres:16-alpine
TRAEFIK_IMAGE=traefik:v3.6
CURL=curlimages/curl:8.11.1
EMBED_UUID=8f6a3c1e-0089-4e00-9000-00000000e0b1
DASH_UUID=8f6a3c1e-0089-4d00-9000-00000000da5b
PASSED=0
ok() { printf '[OK  ] %s\n' "$*"; PASSED=$((PASSED + 1)); }
ko() { printf '[FAIL] %s\n' "$*"; FAILED=$((FAILED + 1)); }
note() { printf '[NOTE] %s\n' "$*"; }

# Waits for a quiet machine (no compiler, no other qd-verify- container,
# load under 4), then watches it: load above 8 ends the script.
superset_wait_quiet() {
  local why
  while :; do
    why=
    pgrep -x rustc >/dev/null && why="rustc running"
    pgrep -x cc1plus >/dev/null && why="cc1plus running"
    [ -z "$why" ] && [ "$(docker ps --format '{{.Names}}' | grep '^qd-verify-' | grep -cv "^$P-")" -gt 0 ] &&
      why="other qd-verify- containers"
    [ -z "$why" ] && awk -v l="$(cut -d' ' -f1 /proc/loadavg)" 'BEGIN { exit !(l >= 4) }' && why="load"
    [ -z "$why" ] && break
    echo "[wait] $why"
    sleep 60
  done
  (while sleep 10; do
    awk -v l="$(cut -d' ' -f1 /proc/loadavg)" 'BEGIN { exit !(l > 8) }' || continue
    echo "[STOP] load above 8: removing $P- containers"
    docker ps -aq --filter "name=^$P-" | xargs -r docker rm -f >/dev/null 2>&1
    kill -TERM $$
    exit
  done) &
  WATCHDOG=$!
  trap 'kill $WATCHDOG 2>/dev/null; for f in "$D"/chrome-*.pid; do [ -f "$f" ] && kill "$(cat "$f")" 2>/dev/null; done; te_cleanup' EXIT
}

superset_image() {
  docker image inspect "$SUPERSET_IMAGE" >/dev/null 2>&1 && return 0
  mkdir -p "$D/build"
  cat >"$D/build/Dockerfile" <<DOCKER
FROM $SUPERSET_BASE
USER root
RUN uv pip install --python /app/.venv/bin/python --no-cache \
    trino==0.340.0 authlib==1.9.4 psycopg2-binary==2.9.9
USER superset
DOCKER
  docker build -q -t "$SUPERSET_IMAGE" "$D/build" >/dev/null
}

# --- Keys, certificates, people -------------------------------------------------
sl_ca() {
  openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes -days 1 -subj "/O=qd/CN=qd verify CA" \
    -addext "basicConstraints=critical,CA:TRUE" -keyout "$D/c/ca.key" -out "$D/c/ca.crt" 2>/dev/null
}
sl_cert() { # NAME DNS [MORE-DNS]
  openssl req -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes -subj "/O=qd/CN=$2" \
    -keyout "$D/c/$1.k" -out "$D/c/$1.csr" 2>/dev/null
  printf 'basicConstraints=CA:FALSE\nextendedKeyUsage=serverAuth,clientAuth\nsubjectAltName=DNS:%s%s\n' "$2" "${3:+,DNS:$3}" >"$D/c/ext"
  openssl x509 -req -in "$D/c/$1.csr" -CA "$D/c/ca.crt" -CAkey "$D/c/ca.key" -CAcreateserial -extfile "$D/c/ext" \
    -days 1 -out "$D/c/$1.crt" 2>/dev/null
  openssl pkey -in "$D/c/$1.k" -out "$D/c/$1.key"
  rm -f "$D/c/$1.k" "$D/c/$1.csr" "$D/c/ext"
}
sl_eckey() { openssl ecparam -name prime256v1 -genkey -noout 2>/dev/null | openssl pkcs8 -topk8 -nocrypt -out "$D/c/$1"; }
# Account ids are invented; ana views loans, bruno savings, carla authors,
# root administers, dario has no BI role.
ANA=0b1e3f7a-0089-4a00-8000-0000000000a1
BRUNO=0b1e3f7a-0089-4a00-8000-0000000000b2
superset_files() {
  mkdir -p "$D/c" "$D/log" "$D/jar" "$D/dyn" "$D/bundle"
  sl_ca
  sl_cert app "$APP"
  sl_cert trino-internal trino.internal trino-b.internal
  sl_cert bi-superset bi-superset
  sl_eckey provider.key
  sl_eckey guest.key
  sl_eckey other.key
  openssl pkey -in "$D/c/guest.key" -pubout -out "$D/c/guest.pub" 2>/dev/null
  for s in superset_secret db_password valkey_password client_secret machine_secret; do
    openssl rand -hex 24 >"$D/c/$s"
  done
  cat >"$D/c/people.json" <<JSON
{"ana":   {"id": "$ANA", "bi_roles": ["viewer"], "datasets": ["loans"], "active": true},
 "bruno": {"id": "$BRUNO", "bi_roles": ["viewer"], "datasets": ["savings"], "active": true},
 "carla": {"id": "0b1e3f7a-0089-4a00-8000-0000000000c3", "bi_roles": ["author"], "datasets": ["loans", "savings"], "active": true},
 "root":  {"id": "0b1e3f7a-0089-4a00-8000-0000000000d4", "bi_roles": ["admin"], "datasets": ["loans", "savings"], "active": true},
 "dario": {"id": "0b1e3f7a-0089-4a00-8000-0000000000e5", "bi_roles": [], "datasets": [], "active": true}}
JSON
  printf '{"catalogs": [{"allow": "none"}]}\n' >"$D/c/rules.json"
  : >"$D/c/sdk.js"
  cp tools/verify/superset_standins.py "$D/c/standins.py"
  cp tools/verify/superset_verify_config.py "$D/c/superset_config.py"
  cp tools/verify/superset_verify_init.py "$D/c/superset_init.py"
  cp infra/traefik/dynamic/middlewares.yml "$D/dyn/middlewares.yml"
  cat >"$D/dyn/routes.yml" <<YML
tls:
  certificates:
    - { certFile: /c/app.crt, keyFile: /c/app.key }
    - { certFile: /c/trino-internal.crt, keyFile: /c/trino-internal.key }
  options:
    internal-mtls: { clientAuth: { caFiles: [/c/ca.crt], clientAuthType: RequireAndVerifyClientCert }, sniStrict: true }
http:
  routers:
    bi:    { rule: "Host(\`$APP\`) && PathPrefix(\`/bi\`)", entryPoints: [websecure], tls: {}, priority: 30, service: superset, middlewares: [app-frame-headers] }
    api:   { rule: "Host(\`$APP\`) && PathPrefix(\`/api\`)", entryPoints: [websecure], tls: {}, priority: 20, service: app, middlewares: [api-chain] }
    web:   { rule: "Host(\`$APP\`)", entryPoints: [websecure], tls: {}, priority: 1, service: app, middlewares: [web-chain] }
    trino: { rule: "Host(\`trino.internal\`) || Host(\`trino-b.internal\`)", entryPoints: [internal], tls: { options: internal-mtls }, service: trino }
  services:
    superset: { loadBalancer: { servers: [{ url: "http://superset:8088" }] } }
    app:      { loadBalancer: { servers: [{ url: "http://app:8080" }] } }
    trino:    { loadBalancer: { servers: [{ url: "http://trino:8080" }] } }
YML
  cat >"$D/c/traefik.yml" <<'YML'
global: { checkNewVersion: false, sendAnonymousUsage: false }
entryPoints: { websecure: { address: ":443" }, internal: { address: ":8443" } }
providers: { file: { directory: /dyn } }
log: { level: ERROR }
accessLog: { filePath: /log/access.json, format: json, fields: { headers: { defaultMode: drop } } }
YML
  chmod 755 "$D"
  chmod -R a+rX "$D/c" "$D/dyn" "$D/bundle"
  chmod 777 "$D/log" "$D/jar"
}

# --- The stand-in provider and page, Traefik ---------------------------------
superset_edge_up() {
  docker run -d --name "$P-app" --network "$P" --network-alias app -v "$D/c:/c:ro" -v "$D/log:/log" \
    -e KEY_FILE=/c/provider.key -e GUEST_KEY_FILE=/c/guest.key -e OTHER_KEY_FILE=/c/other.key \
    -e PEOPLE_FILE=/c/people.json -e LOG_FILE=/log/app.jsonl -e CLIENT_SECRET_FILE=/c/client_secret \
    -e MACHINE_SECRET_FILE=/c/machine_secret -e REDIRECT_URI="https://$APP/bi/oauth-authorized/quality-data" \
    -e RULES_FILE=/c/rules.json -e SDK_FILE=/c/sdk.js -e GUEST_AUD=bi-superset -e EMBED_UUID="$EMBED_UUID" \
    -e TOKEN_TTL="${TOKEN_TTL:-86400}" -e TRINO_TTL="${TRINO_TTL:-900}" -e GUEST_TTL="${GUEST_TTL:-300}" \
    --entrypoint /app/.venv/bin/python "$SUPERSET_IMAGE" -I /c/standins.py serve >/dev/null || return 1
  docker run -d --name "$P-traefik" --network "$P" --network-alias "$APP" --network-alias trino.internal \
    --network-alias trino-b.internal \
    -v "$D/c:/c:ro" -v "$D/dyn:/dyn:ro" -v "$D/log:/log" "$TRAEFIK_IMAGE" --configFile=/c/traefik.yml >/dev/null
  TRAEFIK_IP=$(docker inspect -f "{{(index .NetworkSettings.Networks \"$P\").IPAddress}}" "$P-traefik")
}
# mint SUB TTL [AUD]: a Trino JWT from the stand-in's key.
mint() { docker exec -e KEY_FILE=/c/provider.key "$P-app" /app/.venv/bin/python -I /c/standins.py mint "$@"; }
# applog FILTER: the stand-in's log entries matching a Python expression on e.
applog() {
  python3 -I -c 'import json,sys
for l in open(sys.argv[1]):
    e = json.loads(l)
    if eval(sys.argv[2], {"e": e}): print(l, end="")' "$D/log/${2:-app}.jsonl" "$1" 2>/dev/null
}

# --- Trino 484 behind the internal entry point --------------------------------
# JWT from the stand-in's key (aud trino), the user mapping for person-*,
# bi-superset and qd-quality, rules from the stand-in's URL every 10 s.
superset_trino() {
  openssl pkey -in "$D/c/provider.key" -pubout -out "$D/c/provider.pub" 2>/dev/null
  chmod 644 "$D/c/provider.pub"
  [ -f "$D/root.env" ] && te_lake_catalog gold "iceberg.security=read_only" && te_lake_catalog lake
  cat >"$D/config.properties" <<PROPS
coordinator=true
node-scheduler.include-coordinator=true
http-server.http.port=8080
discovery.uri=http://localhost:8080
query.max-memory=1GB
query.max-memory-per-node=1GB
http-server.process-forwarded=true
http-server.authentication.type=jwt
http-server.authentication.allow-insecure-over-http=false
http-server.authentication.jwt.key-file=/etc/trino/jwt.pem
http-server.authentication.jwt.required-issuer=https://$APP/api/oauth
http-server.authentication.jwt.required-audience=trino
http-server.authentication.jwt.user-mapping.pattern=^(person-[0-9a-f-]{36}|bi-superset|qd-quality)\$
internal-communication.shared-secret=verify-only
PROPS
  printf 'access-control.name=file\nsecurity.config-file=http://app:8080/api/rules.json\nsecurity.refresh-period=%s\n' \
    "${RULES_REFRESH:-10s}" >"$D/access-control.properties"
  printf -- '-server\n-Xmx2G\n-XX:+UseG1GC\n-XX:+ExitOnOutOfMemoryError\n-Djdk.attach.allowAttachSelf=true\n' >"$D/jvm.config"
  [ -f "$D/admin.env" ] || : >"$D/admin.env"
  chmod 755 "$D/catalog"
  chmod 644 "$D/config.properties" "$D/jvm.config" "$D/access-control.properties"
  find "$D/catalog" -type f -exec chmod 644 {} +
  docker run -d --name "$P-trino" --network "$P" --network-alias trino --memory 3g --env-file "$D/admin.env" \
    -v "$D/config.properties:/etc/trino/config.properties:ro" -v "$D/jvm.config:/etc/trino/jvm.config:ro" \
    -v "$D/access-control.properties:/etc/trino/access-control.properties:ro" \
    -v "$D/c/provider.pub:/etc/trino/jwt.pem:ro" -v "$D/catalog:/etc/trino/catalog:ro" "$TRINO_IMAGE" >/dev/null || return 1
  for _ in $(seq 1 90); do
    docker logs "$P-trino" 2>&1 | grep -q "SERVER STARTED" && return 0
    docker ps -q --filter "name=^$P-trino$" | grep -q . || break
    sleep 2
  done
  echo "Trino did not start:"; docker logs "$P-trino" 2>&1 | tail -20
  return 1
}
# tj TOKEN SQL: a statement through the internal entry point with the
# module's client certificate, every page followed: TSV rows, or "ERROR: …".
tj() {
  docker run --rm --network "$P" -v "$D/c:/c:ro" -e T="$1" -e Q="$2" --entrypoint sh "$CURL" -c '
    c="--cacert /c/ca.crt --cert /c/bi-superset.crt --key /c/bi-superset.key -s"
    r=$(curl $c -H "Authorization: Bearer $T" --data-binary "$Q" https://trino.internal:8443/v1/statement -w "\n%{http_code}")
    code=${r##*
}
    [ "$code" = 200 ] || { echo "{\"error\":{\"message\":\"HTTP $code\"}}"; exit 0; }
    r=${r%
*}
    while :; do
      printf "%s\n" "$r"
      n=$(printf "%s" "$r" | sed -n "s/.*\"nextUri\":\"\([^\"]*\)\".*/\1/p")
      [ -n "$n" ] || break
      r=$(curl $c -H "Authorization: Bearer $T" "$n")
    done' | jq -rR '(fromjson? // {"error": {"message": .}}) |
      (.data // [] | .[] | map(tostring) | join("\t")), (.error.message // empty | "ERROR: " + .)'
}
# The gold the dashboard reads: one chart table and a view, two datasets.
superset_gold() {
  local q
  q=$(mint qd-quality 600)
  tj "$q" "CREATE SCHEMA lake.quality_gold" >/dev/null
  tj "$q" "CREATE TABLE lake.quality_gold.chart_trend_by_cut (dataset varchar, cut date, run_id varchar,
    records bigint, conformity double) WITH (partitioning = ARRAY['dataset'])" >/dev/null
  tj "$q" "INSERT INTO lake.quality_gold.chart_trend_by_cut VALUES
    ('loans', DATE '2026-09-30', 'r1', 1000, 0.91), ('loans', DATE '2026-10-31', 'r1', 1010, 0.93),
    ('savings', DATE '2026-09-30', 'r1', 500, 0.81), ('savings', DATE '2026-10-31', 'r1', 520, 0.85)"
}
# rules: the merged rules query-trino would serve from the core's neutral
# grants (people.json: each active person's datasets): the module's own user
# lists gold's tables and reads no row; each person reads the rows of the
# datasets they may view.
rules() {
  jq '
    def person: select(.value.active and (.value.datasets | length) > 0) | {user: ("person-" + .value.id),
      catalog: "gold", schema: "quality_gold", privileges: ["SELECT"],
      filter: ("dataset IN (" + (.value.datasets | map("'\''" + . + "'\''") | join(", ")) + ")")};
    {catalogs: [{user: "qd-quality", allow: "all"}, {user: "person-.*|bi-superset", catalog: "gold", allow: "read-only"},
                {user: "qd-quality", catalog: "system", allow: "all"}, {allow: "none"}],
     schemas: [{user: "qd-quality", owner: true}, {user: "person-.*|bi-superset", catalog: "gold", schema: "quality_gold", owner: false}],
     tables: [{user: "qd-quality", privileges: ["SELECT", "INSERT", "DELETE", "UPDATE", "OWNERSHIP"]},
              (to_entries[] | person),
              {user: "bi-superset", catalog: "gold", schema: "quality_gold", privileges: ["SELECT"], filter: "false"}],
     queries: [{user: "qd-quality", allow: ["execute", "view", "kill"]}, {allow: ["execute"]}]}' "$D/c/people.json" >"$D/c/rules.json.new"
  chmod 644 "$D/c/rules.json.new"
  mv "$D/c/rules.json.new" "$D/c/rules.json"
}

# --- Superset, its database and Valkey ------------------------------------------
superset_stores_up() {
  docker run -d --name "$P-db" --network "$P" --network-alias db -v "$D/c:/c:ro" -e POSTGRES_USER=superset \
    -e POSTGRES_DB=superset -e POSTGRES_PASSWORD_FILE=/c/db_password "$POSTGRES_IMAGE" >/dev/null || return 1
  docker run -d --name "$P-valkey" --network "$P" --network-alias valkey -v "$D/c:/c:ro" --entrypoint sh \
    "$VALKEY_IMAGE" -c 'exec valkey-server --requirepass "$(cat /c/valkey_password)" --save "" --appendonly no' \
    >/dev/null || return 1
  for _ in $(seq 30); do docker exec "$P-db" pg_isready -q -U superset 2>/dev/null && return 0; sleep 1; done
  return 1
}
# superset_run [DOCKER OPTION...] -- COMMAND...: a Superset container's
# environment, as the module's compose gives it.
superset_env() {
  printf '%s\n' -v "$D/c:/c:ro" -v "$D/log:/log" -v "$D/bundle:/bundle" -e PYTHONPATH=/c \
    -e SUPERSET_CONFIG_PATH=/c/superset_config.py -e SUPERSET_APP_ROOT="${APP_ROOT:-/bi}" -e APP_HOST="$APP" \
    -e SUPERSET_SECRET_KEY_FILE=/c/superset_secret -e DB_PASSWORD_FILE=/c/db_password \
    -e VALKEY_PASSWORD_FILE=/c/valkey_password -e OIDC_CLIENT_SECRET_FILE=/c/client_secret \
    -e MACHINE_SECRET_FILE=/c/machine_secret -e REQUESTS_CA_BUNDLE=/c/ca.crt \
    -e CLIENT_CERT=/c/bi-superset.crt -e CLIENT_KEY=/c/bi-superset.key -e GUEST_PUBLIC_KEY_FILE=/c/guest.pub \
    -e GUEST_AUD=bi-superset -e EMBEDDED="${EMBEDDED:-0}" -e CACHE_BY_USER="${CACHE_BY_USER:-1}" \
    -e SESSION_SERVER_SIDE="${SESSION_SERVER_SIDE:-0}" \
    -e SESSION_LIFETIME="${SESSION_LIFETIME:-86000}" -e SESSION_GUARD="${SESSION_GUARD:-1}" \
    -e MUTATOR_LOG=/log/mutator.jsonl
}
# sinit ARGS...: superset_verify_init.py (superset-init's stand-in) in a one-shot container.
sinit() {
  local env
  mapfile -t env < <(superset_env)
  docker run --rm --network "$P" "${env[@]}" --entrypoint /app/.venv/bin/python "$SUPERSET_IMAGE" \
    /c/superset_init.py "$@"
}
superset_up() {
  local env
  mapfile -t env < <(superset_env)
  sinit migrate >"$D/log/init.out" 2>&1 || { tail -20 "$D/log/init.out"; return 1; }
  docker run -d --name "$P-superset" --network "$P" --network-alias superset "${env[@]}" \
    --entrypoint /app/.venv/bin/gunicorn "$SUPERSET_IMAGE" -w 1 --threads 8 -k gthread -b 0.0.0.0:8088 \
    --timeout 120 --limit-request-line 0 --limit-request-field_size 0 "superset.app:create_app()" >/dev/null || return 1
  for _ in $(seq 90); do
    [ "$(docker exec "$P-superset" curl -s -o /dev/null -w '%{http_code}' "http://localhost:8088${APP_ROOT:-/bi}/health")" = 200 ] &&
      return 0
    docker ps -q --filter "name=^$P-superset$" | grep -q . || break
    sleep 2
  done
  echo "Superset did not start:"; docker logs "$P-superset" 2>&1 | tail -20
  return 1
}

# --- A person's browser, as curl ------------------------------------------------
# web PERSON URL [CURL OPTION...]: through Traefik with the person's jar.
web() {
  local who=$1 url=$2
  shift 2
  docker run --rm --network "$P" -v "$D/c:/c:ro" -v "$D/jar:/jar" "$CURL" -s -m 60 --cacert /c/ca.crt \
    -b "/jar/$who" -c "/jar/$who" "$@" "$url"
}
# signin PERSON: signed in to Quality Data, then Superset's OIDC sign-in
# followed to its end; prints the HTTP code and the last URL.
signin() {
  web "$1" "https://$APP/api/test/signin?person=$1" -o /dev/null
  web "$1" "https://$APP/bi/login/quality-data" -L -o /dev/null -w '%{http_code} %{url_effective}'
}
# bi PERSON PATH [CURL OPTION...]: Superset's API with the person's session.
bi() {
  local who=$1 path=$2
  shift 2
  web "$who" "https://$APP/bi$path" "$@"
}
# csrf PERSON: a CSRF token for the person's session.
csrf() { bi "$1" /api/v1/security/csrf_token/ | jq -r .result; }
# superset_lake: te_rustfs, again after a pause when RustFS's admin API is
# not ready yet (it can answer 503 just after /health does).
superset_lake() {
  local i
  for i in 1 2 3; do
    te_rustfs && return 0
    docker rm -f -v "$P-lake" >/dev/null 2>&1
    sleep 10
  done
  return 1
}
