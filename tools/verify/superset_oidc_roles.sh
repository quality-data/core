#!/bin/bash
# Verification V1 of decision 0089: Superset 6.1.0 accepts the core's OIDC
# roles. Superset with AUTH_OAUTH against the stand-in provider (ES256, the
# claim bi_roles), AUTH_ROLES_MAPPING viewer/author/admin to QD Viewer, QD
# Author and Admin, AUTH_ROLES_SYNC_AT_LOGIN, under /bi behind Traefik.
# Checks: each person gets exactly their role; a person with no bi_roles is
# refused and no user row is made; a role withdrawn is removed at the next
# sign-in; the username stored is the account id; Superset's session and
# the provider's access token's exp (60 s here); where Flask-AppBuilder
# keeps the access token (the session cookie, or Valkey with server-side
# sessions). Only names, roles, statuses and verdicts are printed.
#
# Containers, volumes and the network qd-verify-sso-…, removed on exit. Run
# alone, from the repository root:
#
#   bash tools/verify/superset_oidc_roles.sh
set -u
cd "$(dirname "$0")/../.."
P=qd-verify-sso
. tools/verify/superset_lib.sh
superset_wait_quiet
superset_image || exit 1
superset_files
TOKEN_TTL=60
superset_edge_up || exit 1
superset_stores_up || exit 1
superset_up || exit 1
expect "Superset's version" 6.1.0 "$(docker exec "$P-superset" /app/.venv/bin/python -c \
  'from importlib.metadata import version; print(version("apache-superset"))' 2>/dev/null)"
note "roles made by superset-init: $(grep '^roles' "$D/log/init.out" | cut -d' ' -f2- | tr '\n' ' ')"

me() { bi "$1" /api/v1/me/ | jq -r '.result.username // "none"'; }
roles() { bi "$1" /api/v1/me/roles/ | jq -r '[.result.roles // {} | keys[]] | join(",")'; }
users() { docker exec "$P-db" psql -U superset -tAc "SELECT count(*) FROM ab_user WHERE username = '$1'"; }
id_of() { jq -r --arg p "$1" '.[$p].id' "$D/c/people.json"; }

echo "== Each person signs in with their role"
for who in ana:QD\ Viewer carla:QD\ Author root:Admin; do
  p=${who%%:*}
  R=$(signin "$p")
  expect "$p: sign-in ends on Superset's welcome page" "200 https://$APP/bi/superset/welcome/" "$R"
  expect "$p: exactly the role" "${who#*:}" "$(roles "$p")"
  expect "$p: the username stored is the account id" "$(id_of "$p")" "$(me "$p")"
done
expect "the authorization request carried PKCE (S256)" true \
  "$(applog 'e.get("grant") == "authorization_code"' | tail -1 | jq -r .pkce)"
expect "the redirect URI the provider saw is under /bi" "https://$APP/bi/oauth-authorized/quality-data" \
  "$(applog '"redirect_uri" in e' | tail -1 | jq -r .redirect_uri)"

echo "== A person with no bi_roles"
R=$(signin dario)
case "$R" in *"/bi/login"*) ok "dario is sent back to the sign-in page ($R)" ;; *) ko "dario: $R" ;; esac
expect "dario has no Superset session" none "$(me dario)"
expect "no user row is made for dario" 0 "$(users "$(id_of dario)")"

echo "== A role withdrawn is removed at the next sign-in"
jq '.carla.bi_roles = ["viewer"]' "$D/c/people.json" >"$D/p.json" && cat "$D/p.json" >"$D/c/people.json"
expect "carla, before signing in again, keeps her role" "QD Author" "$(roles carla)"
rm -f "$D/jar/carla"
signin carla >/dev/null
expect "carla, signed in again: the new role only" "QD Viewer" "$(roles carla)"
jq '.carla.bi_roles = []' "$D/c/people.json" >"$D/p.json" && cat "$D/p.json" >"$D/c/people.json"
rm -f "$D/jar/carla"
R=$(signin carla)
case "$R" in *"/bi/login"*) ok "carla with no role left is refused ($R)" ;; *) ko "carla with no role: $R" ;; esac
expect "  and her user row stays (Superset keeps the row; sign-in is refused)" 1 "$(users "$(id_of carla)")"

echo "== Where the access token is kept (default: Flask's signed session cookie)"
# cookie_has_token PERSON: whether the session cookie's payload, decoded
# without the key, holds an ES256 JWT (header eyJ...).
cookie_has_token() {
  local v
  v=$(awk '$6 == "session" { print $7 }' "$D/jar/$1")
  python3 -I -c 'import base64, sys, zlib
v = sys.argv[1]
z = v.startswith(".")
p = v.lstrip(".").split(".")[0]
raw = base64.urlsafe_b64decode(p + "=" * (-len(p) % 4))
raw = zlib.decompress(raw) if z else raw
print("token in the cookie" if b"eyJ" in raw else ("session id only" if len(raw) < 60 else "no token"))' "$v" 2>/dev/null || echo "no session cookie"
}
expect "ana's session cookie, decoded without the key" "token in the cookie" "$(cookie_has_token ana)"

echo "== The session against the access token's exp (60 s), with the module's guard"
sleep 65
expect "ana's session after the token expired" none "$(me ana)"
rm -f "$D/jar/ana"
signin ana >/dev/null
expect "ana signs in again with a fresh token" "$ANA" "$(me ana)"

echo "== Without the guard (Flask's session alone), with server-side sessions in Valkey"
docker rm -f "$P-superset" >/dev/null
SESSION_GUARD=0 SESSION_SERVER_SIDE=1 SESSION_LIFETIME=86000 superset_up || exit 1
rm -f "$D/jar/bruno"
signin bruno >/dev/null
expect "bruno signed in" "$BRUNO" "$(me bruno)"
expect "bruno's session cookie with server-side sessions" "session id only" "$(cookie_has_token bruno)"
expect "Valkey holds the session (db 3 has keys)" yes \
  "$(docker exec "$P-valkey" sh -c 'valkey-cli -a "$(cat /c/valkey_password)" --no-auth-warning -n 3 dbsize' | awk '{print ($1 > 0) ? "yes" : "no"}')"
sleep 65
R=$(me bruno)
note "bruno's session 65 s after his token expired, without the guard: $([ "$R" = none ] && echo ended || echo "still open")"
expect "without the guard Superset's session outlives the access token" "$BRUNO" "$R"

echo
echo "passed $PASSED, failed $FAILED"
[ "$FAILED" = 0 ] && echo "ALL OK" || exit 1
