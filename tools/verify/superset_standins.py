"""Stand-ins for decision 0089's verification (superset_*.sh): Quality Data's
OIDC provider (authorization code, token exchange for Trino, the module's
client token), the core's guest-token route, Trino's rules served from a URL
and the application's page with the embedded SDK. It runs on the extended
Superset image's Python (PyJWT and cryptography). Tokens are ES256, signed
with throwaway keys; nothing here prints or logs a token, only what was
issued to whom.

    python -I superset_standins.py serve
    python -I superset_standins.py mint SUB TTL [AUD]     # a Trino JWT on stdout

Environment: KEY_FILE (the provider's EC key), GUEST_KEY_FILE (the core's
guest-token key), OTHER_KEY_FILE (a key nobody trusts), PEOPLE_FILE (JSON:
person -> id, bi_roles, datasets, active), LOG_FILE, ISSUER, TOKEN_TTL,
TRINO_TTL, GUEST_TTL, CLIENT_SECRET_FILE, MACHINE_SECRET_FILE,
REDIRECT_URI, RULES_FILE, SDK_FILE, GUEST_AUD, EMBED_UUID.
"""

import base64
import hashlib
import hmac
import json
import os
import secrets
import sys
import threading
import time
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import jwt

E = os.environ.get
ISSUER = E("ISSUER", "https://qd.verify.example/api/oauth")
LOCK = threading.Lock()
CODES = {}
EXCHANGE = "urn:ietf:params:oauth:grant-type:token-exchange"


def read(name):
    return open(E(name)).read().strip()


def key(name="KEY_FILE"):
    return open(E(name), "rb").read()


def public_jwk():
    from cryptography.hazmat.primitives.serialization import load_pem_private_key

    nums = load_pem_private_key(key(), None).public_key().public_numbers()
    b = lambda n: base64.urlsafe_b64encode(n.to_bytes(32, "big")).rstrip(b"=").decode()  # noqa: E731
    return {"kty": "EC", "crv": "P-256", "kid": "verify", "use": "sig", "alg": "ES256", "x": b(nums.x), "y": b(nums.y)}


def sign(claims, typ="at+jwt", which="KEY_FILE"):
    return jwt.encode(claims, key(which), algorithm="ES256", headers={"kid": "verify", "typ": typ})


def people():
    return json.load(open(E("PEOPLE_FILE")))


def by_id(account):
    return next((n for n, p in people().items() if p["id"] == account), None)


def log(entry):
    with LOCK, open(E("LOG_FILE"), "a") as f:
        f.write(json.dumps(entry) + "\n")


def trino_token(sub, ttl, act=None, cap=None, aud="trino"):
    now = int(time.time())
    exp = now + ttl if cap is None else min(now + ttl, cap)
    claims = {"iss": ISSUER, "aud": aud, "sub": sub, "iat": now, "exp": exp, "jti": secrets.token_hex(8)}
    if act:
        claims["act"] = {"sub": act}
    return sign(claims), exp


def subject(token):
    """Who a subject token is: (person, kind, exp) or (None, reason, None)."""
    try:
        head = jwt.get_unverified_header(token)
        if head.get("typ") == "at+jwt":
            c = jwt.decode(token, public_pem("KEY_FILE"), algorithms=["ES256"],
                           audience="bi-superset", issuer=ISSUER)
            return by_id(c["sub"]), "person", c["exp"]
        c = jwt.decode(token, public_pem("GUEST_KEY_FILE"), algorithms=["ES256"], audience=E("GUEST_AUD"))
        if c.get("type") != "guest":
            return None, "not-guest", None
        return by_id(c["user"]["username"].removeprefix("person-")), "guest", c["exp"]
    except jwt.ExpiredSignatureError:
        return None, "expired", None
    except Exception as e:  # noqa: BLE001
        return None, "bad:" + type(e).__name__, None


def public_pem(name):
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.serialization import load_pem_private_key

    return load_pem_private_key(key(name), None).public_key().public_bytes(
        serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo)


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *args):
        pass

    def send(self, status, body=b"", ctype="application/json", headers=()):
        if isinstance(body, (dict, list)):
            body = json.dumps(body).encode()
        elif isinstance(body, str):
            body = body.encode()
        self.send_response(status)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        for k, v in headers:
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(body)
        return status

    def form(self):
        n = int(self.headers.get("Content-Length") or 0)
        return dict(urllib.parse.parse_qsl(self.rfile.read(n).decode())) if n else {}

    def cookie(self, name):
        for part in (self.headers.get("Cookie") or "").split(";"):
            k, _, v = part.strip().partition("=")
            if k == name:
                return v
        return None

    def client(self, f):
        """The authenticated client of a token request, or None."""
        basic = self.headers.get("Authorization", "")
        if basic.startswith("Basic "):
            cid, _, sec = base64.b64decode(basic[6:]).decode().partition(":")
            cid, sec = urllib.parse.unquote(cid), urllib.parse.unquote(sec)
        else:
            cid, sec = f.get("client_id", ""), f.get("client_secret", "")
        want = {"bi-superset": "CLIENT_SECRET_FILE", "bi-superset-machine": "MACHINE_SECRET_FILE"}.get(cid)
        return cid if want and hmac.compare_digest(sec, read(want)) else None

    def do_GET(self):
        self.route("GET")

    def do_POST(self):
        self.route("POST")

    def route(self, method):
        url = urllib.parse.urlsplit(self.path)
        q = dict(urllib.parse.parse_qsl(url.query))
        entry = {"t": round(time.time(), 3), "method": method, "path": url.path}
        try:
            entry["status"] = self.app(method, url.path.removeprefix("/api"), q, entry)
        finally:
            log(entry)

    def app(self, method, path, q, entry):
        if path == "/oauth/.well-known/openid-configuration":
            return self.send(200, {"issuer": ISSUER, "authorization_endpoint": ISSUER + "/authorize",
                                   "token_endpoint": ISSUER + "/token", "userinfo_endpoint": ISSUER + "/userinfo",
                                   "jwks_uri": ISSUER + "/jwks", "id_token_signing_alg_values_supported": ["ES256"],
                                   "code_challenge_methods_supported": ["S256"]})
        if path == "/oauth/jwks":
            return self.send(200, {"keys": [public_jwk()]})
        if path == "/test/signin":
            c = f"qd_session={q['person']}; Path=/; Secure; HttpOnly; SameSite=Strict; Max-Age=86400"
            return self.send(302, headers=[("Set-Cookie", c), ("Location", q.get("back", "/"))])
        if path == "/oauth/authorize":
            return self.authorize(q, entry)
        if path == "/oauth/token" and method == "POST":
            return self.token(self.form(), entry)
        if path == "/oauth/userinfo":
            try:
                c = jwt.decode((self.headers.get("Authorization") or "")[7:], public_pem("KEY_FILE"),
                               algorithms=["ES256"], audience="bi-superset", issuer=ISSUER)
            except Exception:  # noqa: BLE001
                return self.send(401, {"error": "invalid_token"})
            return self.send(200, self.identity(by_id(c["sub"])))
        if path == "/test/guest":  # stands for POST /quality/datasets/{dataset}/dashboard-token
            return self.guest(q, entry)
        if path == "/rules.json":  # stands for query-trino's merged rules
            return self.send(200, open(E("RULES_FILE")).read())
        if path == "/sdk.js":
            return self.send(200, open(E("SDK_FILE"), "rb").read(), "text/javascript")
        if path == "/page":
            return self.page(q)
        if path == "/page.js":
            return self.page_js()
        return self.send(404, {"error": "not found"})

    def identity(self, person):
        p = people()[person]
        claims = {"sub": p["id"], "email": f"{person}@verify.example", "name": person.title(),
                  "preferred_username": p["id"]}
        if p["bi_roles"]:
            claims["bi_roles"] = p["bi_roles"]
        return claims

    def authorize(self, q, entry):
        if q.get("client_id") != "bi-superset" or q.get("redirect_uri") != E("REDIRECT_URI"):
            entry["refused"] = "client or redirect_uri"
            return self.send(400, "invalid request", "text/plain")
        who = self.cookie("qd_session")
        if not who:
            return self.send(302, headers=[("Location", "/signin")])
        code = secrets.token_urlsafe(24)
        with LOCK:
            CODES[code] = (who, q.get("nonce"), q.get("code_challenge"), time.time())
        entry["redirect_uri"] = q["redirect_uri"]
        loc = q["redirect_uri"] + "?" + urllib.parse.urlencode({"code": code, "state": q.get("state", "")})
        return self.send(302, headers=[("Location", loc)])

    def token(self, f, entry):
        cid = self.client(f)
        entry["client"], entry["grant"] = cid, f.get("grant_type")
        if not cid:
            return self.send(401, {"error": "invalid_client"})
        if f.get("grant_type") == "authorization_code" and cid == "bi-superset":
            with LOCK:
                rec = CODES.pop(f.get("code", ""), None)
            if not rec or time.time() - rec[3] > 60:
                return self.send(400, {"error": "invalid_grant"})
            if rec[2] and base64.urlsafe_b64encode(hashlib.sha256(f.get("code_verifier", "").encode()).digest()
                                                   ).rstrip(b"=").decode() != rec[2]:
                return self.send(400, {"error": "invalid_grant"})
            person, nonce = rec[0], rec[1]
            now, ttl = int(time.time()), int(E("TOKEN_TTL", "86400"))
            ident = self.identity(person)
            base = {"iss": ISSUER, "aud": "bi-superset", "iat": now, "exp": now + ttl, **ident}
            idt = dict(base, **({"nonce": nonce} if nonce else {}))
            at = dict(base, client_id="bi-superset", jti=secrets.token_hex(8), scope="openid email profile")
            entry["issued_to"], entry["ttl"], entry["pkce"] = person, ttl, bool(rec[2])
            return self.send(200, {"access_token": sign(at), "id_token": sign(idt, "JWT"), "token_type": "Bearer",
                                   "expires_in": ttl, "scope": "openid email profile"})
        if f.get("grant_type") == "client_credentials" and cid == "bi-superset-machine":
            tok, exp = trino_token("bi-superset", int(E("TRINO_TTL", "900")))
            entry["issued_to"] = "bi-superset"
            return self.send(200, {"access_token": tok, "token_type": "Bearer", "expires_in": exp - int(time.time())})
        if f.get("grant_type") == EXCHANGE and cid == "bi-superset-machine":
            if f.get("audience") != "trino":
                return self.send(400, {"error": "invalid_target"})
            person, kind, exp = subject(f.get("subject_token", ""))
            entry["subject_kind"] = kind
            if not person or not people()[person].get("active", True):
                entry["refused"] = kind if not person else "inactive"
                return self.send(400, {"error": "invalid_grant"})
            tok, texp = trino_token("person-" + people()[person]["id"], int(E("TRINO_TTL", "900")),
                                    act="bi-superset", cap=exp)
            entry["issued_to"], entry["exp_capped"] = person, texp == exp
            return self.send(200, {"access_token": tok, "issued_token_type": "urn:ietf:params:oauth:token-type:jwt",
                                   "token_type": "Bearer", "expires_in": texp - int(time.time())})
        return self.send(400, {"error": "unsupported_grant_type"})

    def guest(self, q, entry):
        person, dataset = q.get("person", ""), q.get("dataset", "")
        p = people().get(person)
        entry["person"], entry["dataset"] = person, dataset
        if not p or dataset not in p["datasets"]:
            entry["refused"] = True
            return self.send(403, {"error": "forbidden"})
        now = int(time.time())
        claims = {"type": "guest", "aud": q.get("aud", E("GUEST_AUD")),
                  "user": {"username": "person-" + p["id"]},
                  "resources": [{"type": "dashboard", "id": q.get("dashboard", E("EMBED_UUID"))}],
                  "rls_rules": [{"clause": f"dataset = '{dataset}'"}],
                  "iat": now, "exp": now + int(q.get("ttl", E("GUEST_TTL", "300"))), "jti": secrets.token_hex(8)}
        entry["exp"] = claims["exp"]
        which = "OTHER_KEY_FILE" if q.get("key") == "other" else "GUEST_KEY_FILE"
        return self.send(200, {"token": sign(claims, "JWT", which)})

    def page(self, q):
        """The application's page: no inline script or style (its CSP is the
        application's); page.js embeds the dashboard, its token from the core's route."""
        html = ('<!doctype html><title>Quality Data</title><h1>Dashboard</h1><div id=mount></div>'
                '<script src="/api/sdk.js"></script><script src="/api/page.js"></script>')
        return self.send(200, html, "text/html")

    def page_js(self):
        js = """
window.fetches = 0; window.failures = 0;
const q = new URLSearchParams(location.search);
async function fetchGuestToken() {
  window.fetches++;
  const r = await fetch('/api/test/guest?person=' + q.get('person') + '&dataset=' + q.get('dataset') +
    '&ttl=' + (q.get('ttl') || ''));
  if (!r.ok) { window.failures++; throw new Error('refused'); }
  return (await r.json()).token;
}
const mount = document.getElementById('mount');
new MutationObserver(() => { const f = mount.querySelector('iframe');
  if (f) { f.style.width = '1280px'; f.style.height = '900px'; f.style.border = '0'; } })
  .observe(mount, {childList: true});
supersetEmbeddedSdk.embedDashboard({id: '%s', supersetDomain: location.origin + '/bi', mountPoint: mount,
  fetchGuestToken, dashboardUiConfig: {hideTitle: true, filters: {expanded: true}}})
  .then((d) => { window.embedded = true; window.dash = d; }, (e) => { window.embedError = String(e); });
""" % E("EMBED_UUID")
        return self.send(200, js, "text/javascript")

if __name__ == "__main__":
    if sys.argv[1] == "mint":
        print(trino_token(sys.argv[2], int(sys.argv[3]), aud=sys.argv[4] if len(sys.argv) > 4 else "trino")[0])
    else:
        ThreadingHTTPServer(("0.0.0.0", 8080), Handler).serve_forever()
