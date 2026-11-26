#!/bin/bash
# Verification V6 of decision 0089: Superset 6.1.0 served under /bi on the
# application's host (SUPERSET_APP_ROOT=/bi), behind Traefik v3.6 with the
# application's frame headers (infra/traefik/dynamic/middlewares.yml,
# app-frame-headers on /bi). Checks: the health check through Traefik; the
# pages' links and static files under /bi; Superset's redirects stay under
# /bi; the OIDC redirect URI under /bi and the sign-in ending there; the
# cookies' path and attributes; the API under /bi; the embedded route under
# /bi with the frame headers. Only paths, statuses, header names and values
# that hold no secret are printed. Trino 484 (no lake) only because
# importing the bundle's database connects to it.
#
# Containers, volumes and the network qd-verify-v6-…, removed on exit. Run
# alone, from the repository root:
#
#   bash tools/verify/superset_subpath.sh
set -u
cd "$(dirname "$0")/../.."
P=qd-verify-v6
. tools/verify/superset_lib.sh
superset_wait_quiet
superset_image || exit 1
superset_files
rules
chmod 777 "$D/bundle"
superset_edge_up || exit 1
superset_trino || exit 1
superset_stores_up || exit 1
EMBEDDED=1 superset_up || exit 1
sinit bundle /bundle/src trino.internal:8443 && sinit import /bundle/src >/dev/null &&
  sinit embed "$DASH_UUID" "$EMBED_UUID" "https://$APP" >/dev/null
# hdr PERSON PATH NAME: one response header of a GET through Traefik.
hdr() { web "$1" "https://$APP$2" -o /dev/null -D - | tr -d '\r' | sed -n "s/^$3: //Ip" | head -1; }
code() { web "$1" "https://$APP$2" -o /dev/null -w '%{http_code}'; }

echo "== Health and redirects"
expect "/bi/health through Traefik" "200 OK" "$(web nobody "https://$APP/bi/health" -w ' %{http_code}' | awk '{print $2, $1}')"
expect "/bi/ sends a person with no session to the sign-in, under /bi" "https://$APP/bi/login/" \
  "$(web nobody "https://$APP/bi/" -L -o /dev/null -w '%{url_effective}' | sed 's|?.*||')"
expect "/health without /bi is not Superset's (the application's)" 404 "$(code nobody /health)"

echo "== The sign-in page's links and static files"
web nobody "https://$APP/bi/login/" >"$D/login.html"
OUT=$(grep -o '\(src\|href\)="/[^"]*"' "$D/login.html" | sed 's/^[a-z]*="//; s/"$//' | grep -v '^/bi/' | sort -u | head -5)
expect "every absolute link of the sign-in page is under /bi" "" "$OUT"
ASSET=$(grep -o 'src="/bi/static/[^"]*\.js"' "$D/login.html" | head -1 | sed 's/^src="//; s/"$//')
expect "a script of the page ($ASSET)" 200 "$(code nobody "$ASSET")"
expect "the application's root is untouched (the stand-in's page)" 404 "$(code nobody /static/assets/x.js)"

echo "== The OIDC sign-in under /bi"
LOC=$(hdr nobody /bi/login/quality-data location)
expect "the authorize request's redirect_uri" "https://$APP/bi/oauth-authorized/quality-data" \
  "$(python3 -I -c 'import sys, urllib.parse as u; print(u.parse_qs(u.urlsplit(sys.argv[1]).query)["redirect_uri"][0])' "$LOC")"
web ana "https://$APP/api/test/signin?person=ana" -o /dev/null
R=$(web ana "https://$APP/bi/login/quality-data" -L -o /dev/null -D "/jar/ana.headers" -w '%{http_code} %{url_effective}')
expect "the sign-in ends under /bi" "200 https://$APP/bi/superset/welcome/" "$R"
expect "Superset's session cookie: its attributes, sorted" "HttpOnly Path=/bi SameSite=Lax Secure" \
  "$(tr -d '\r' <"$D/jar/ana.headers" | sed -n 's/^set-cookie: //Ip' | grep '^session=' | tail -1 |
     tr ';' '\n' | sed 's/^ *//' | grep -v '^session=\|^Expires=' | sort | tr '\n' ' ' | sed 's/ $//')"
expect "the API under /bi with the session" "$ANA" "$(bi ana /api/v1/me/ | jq -r .result.username)"

echo "== The embedded route under /bi, with the frame headers"
expect "/bi/embedded/<uuid>, framed by the application's page (its Referer)" 200 \
  "$(web nobody "https://$APP/bi/embedded/$EMBED_UUID" -H "Referer: https://$APP/page" -o /dev/null -w '%{http_code}')"
expect "  framed by another origin" 403 \
  "$(web nobody "https://$APP/bi/embedded/$EMBED_UUID" -H "Referer: https://elsewhere.example/" -o /dev/null -w '%{http_code}')"
expect "  X-Frame-Options (the application's)" SAMEORIGIN "$(hdr nobody "/bi/embedded/$EMBED_UUID" x-frame-options)"
note "  Content-Security-Policy: $(hdr nobody "/bi/embedded/$EMBED_UUID" content-security-policy | cut -c1-300)"
expect "  frame-ancestors 'self' in the policy" yes \
  "$(hdr nobody "/bi/embedded/$EMBED_UUID" content-security-policy | grep -q "frame-ancestors 'self'" && echo yes || echo no)"
expect "a dashboard's page under /bi (signed in)" 200 "$(code ana "/bi/superset/dashboard/qd-quality-en/")"

echo
echo "passed $PASSED, failed $FAILED"
[ "$FAILED" = 0 ] && echo "ALL OK" || exit 1
