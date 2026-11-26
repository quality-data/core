"""superset_config.py for decision 0089's verification (superset_*.sh), as
the module bi-superset would ship it: sign-in with Quality Data's OIDC
provider (the stand-in), roles from the claim bi_roles, the connection
mutator that exchanges the person's token for a Trino token, caches per
person in Valkey, the guest token checked with the core's public key only,
served under SUPERSET_APP_ROOT. Every value comes from the environment or a
*_FILE; nothing here logs a token. The mutator writes one JSON line per
connection (who, what kind of subject, where from) to MUTATOR_LOG.
"""

import hashlib
import json
import os
import threading
import time

import requests
from flask import g, has_request_context, request, session
from flask_appbuilder.security.manager import AUTH_OAUTH
from superset.security import SupersetSecurityManager

E = os.environ.get


def secret(name):
    return open(E(name + "_FILE")).read().strip()


APP = E("APP_HOST", "qd.verify.example")
ISSUER = f"https://{APP}/api/oauth"
SECRET_KEY = secret("SUPERSET_SECRET_KEY")
SQLALCHEMY_DATABASE_URI = f"postgresql+psycopg2://superset:{secret('DB_PASSWORD')}@db:5432/superset"
ENABLE_PROXY_FIX = True
PROXY_FIX_CONFIG = {"x_for": 1, "x_proto": 1, "x_host": 1, "x_port": 1, "x_prefix": 0}
SESSION_COOKIE_SECURE = True
SESSION_COOKIE_SAMESITE = "Lax"
PERMANENT_SESSION_LIFETIME = int(E("SESSION_LIFETIME", "86000"))
TALISMAN_ENABLED = E("TALISMAN", "1") == "1"
WTF_CSRF_ENABLED = True

# --- Sign-in: the core's provider, the only one, roles from bi_roles -----------
AUTH_TYPE = AUTH_OAUTH
AUTH_USER_REGISTRATION = True
AUTH_USER_REGISTRATION_ROLE = None
AUTH_ROLES_SYNC_AT_LOGIN = True
AUTH_ROLES_MAPPING = {"viewer": ["QD Viewer"], "author": ["QD Author"], "admin": ["Admin"]}
OAUTH_PROVIDERS = [{
    "name": "quality-data", "icon": "fa-key", "token_key": "access_token",
    "remote_app": {
        "client_id": "bi-superset", "client_secret": secret("OIDC_CLIENT_SECRET"),
        "server_metadata_url": ISSUER + "/.well-known/openid-configuration",
        "client_kwargs": {"scope": "openid email profile", "code_challenge_method": "S256"},
    },
}]
AUTH_ROLES_KEY = "bi_roles"
_LOCK = threading.Lock()


class QdSecurityManager(SupersetSecurityManager):
    def oauth_user_info(self, provider, response=None):
        claims = (response or {}).get("userinfo") or {}
        roles = claims.get(AUTH_ROLES_KEY) or []
        # No claim, no entry: FAB would register a user with no role.
        if not roles:
            return {"username": None}
        return {"username": claims["sub"], "email": claims.get("email", ""),
                "first_name": claims.get("name", ""), "last_name": "", "role_keys": roles}


CUSTOM_SECURITY_MANAGER = QdSecurityManager


def _session_token():
    pair = session.get("oauth")
    return pair[0] if pair else None


_BY_USER = {}  # account id -> the person's latest access token: this process's memory only


def _expired_session_guard():
    """Ends Superset's session when the provider's access token has expired;
    otherwise remembers the token by username, for SQL Lab (it runs in a test
    request context of its own, with no session)."""
    if not has_request_context():
        return None
    tok = _session_token()
    if tok is None:
        return None
    from flask_login import current_user

    if getattr(current_user, "is_authenticated", False) and not getattr(current_user, "is_guest_user", False):
        with _LOCK:
            _BY_USER[current_user.username] = tok
    if E("SESSION_GUARD", "1") != "1":
        return None
    try:
        import jwt

        exp = jwt.decode(tok, options={"verify_signature": False})["exp"]
    except Exception:  # noqa: BLE001
        return None
    if exp <= time.time():
        from flask_login import logout_user

        logout_user()
        session.clear()
    return None


def FLASK_APP_MUTATOR(app):  # noqa: N802
    app.before_request(_expired_session_guard)


# --- Caches: Valkey, per person, 5 minutes; no async queries, no Celery --------
_VALKEY = f"redis://:{secret('VALKEY_PASSWORD')}@valkey:6379"
CACHE_CONFIG = {"CACHE_TYPE": "RedisCache", "CACHE_DEFAULT_TIMEOUT": 300, "CACHE_KEY_PREFIX": "superset_",
                "CACHE_REDIS_URL": _VALKEY + "/0"}
DATA_CACHE_CONFIG = {"CACHE_TYPE": "RedisCache", "CACHE_DEFAULT_TIMEOUT": 300, "CACHE_KEY_PREFIX": "data_",
                     "CACHE_REDIS_URL": _VALKEY + "/1"}
FILTER_STATE_CACHE_CONFIG = dict(CACHE_CONFIG, CACHE_KEY_PREFIX="filter_")
EXPLORE_FORM_DATA_CACHE_CONFIG = dict(CACHE_CONFIG, CACHE_KEY_PREFIX="explore_")
RATELIMIT_STORAGE_URI = _VALKEY + "/2"
if E("SESSION_SERVER_SIDE") == "1":
    from redis import Redis

    SESSION_SERVER_SIDE = True
    SESSION_TYPE = "redis"
    SESSION_REDIS = Redis.from_url(_VALKEY + "/3")

FEATURE_FLAGS = {
    "EMBEDDED_SUPERSET": E("EMBEDDED", "0") == "1",
    "CACHE_QUERY_BY_USER": E("CACHE_BY_USER", "1") == "1",
    "GLOBAL_ASYNC_QUERIES": False,
    "ALERT_REPORTS": False,
    "ENABLE_TEMPLATE_PROCESSING": False,
}

# --- Embedded: the core signs guest tokens (ES256), Superset holds the public key
if E("GUEST_PUBLIC_KEY_FILE"):
    GUEST_TOKEN_JWT_ALGO = "ES256"
    GUEST_TOKEN_JWT_SECRET = open(E("GUEST_PUBLIC_KEY_FILE")).read()
    GUEST_TOKEN_JWT_AUDIENCE = E("GUEST_AUD", "bi-superset")
    GUEST_ROLE_NAME = "QD Guest"

# --- The connection mutator: the person's own Trino token ----------------------
_TRINO = {}  # sha256(subject token) -> (trino token, exp): this process's memory only
_TOKEN_URL = ISSUER + "/token"


def _machine():
    return ("bi-superset-machine", secret("MACHINE_SECRET"))


def _issue(data):
    r = requests.post(_TOKEN_URL, data=data, auth=_machine(), timeout=10)
    r.raise_for_status()
    body = r.json()
    return body["access_token"], time.time() + body["expires_in"]


def _trino_token(subject):
    key = hashlib.sha256((subject or "client").encode()).hexdigest()
    with _LOCK:
        hit = _TRINO.get(key)
    if hit and hit[1] - 30 > time.time():
        return hit[0], "reused"
    if subject:
        tok = _issue({"grant_type": "urn:ietf:params:oauth:grant-type:token-exchange", "audience": "trino",
                      "subject_token": subject,
                      "subject_token_type": "urn:ietf:params:oauth:token-type:access_token"})
    else:
        tok = _issue({"grant_type": "client_credentials", "audience": "trino"})
    with _LOCK:
        _TRINO[key] = tok
    return tok[0], "issued"


def _subject():
    if not has_request_context():
        return None, "none"
    user = getattr(g, "user", None)
    if getattr(user, "is_guest_user", False):
        raw = request.headers.get("X-GuestToken") or request.form.get("guest_token")
        return raw, "guest" if raw else "guest-without-token"
    tok = _session_token()
    if tok:
        return tok, "person"
    with _LOCK:
        tok = _BY_USER.get(getattr(user, "username", None))
    return (tok, "person-by-username") if tok else (None, "none")


def DB_CONNECTION_MUTATOR(uri, params, username, security_manager, source):  # noqa: N802
    from trino.auth import JWTAuthentication

    subject, kind = _subject()
    entry = {"t": round(time.time(), 3), "username": username, "source": str(source), "kind": kind}
    try:
        token, how = _trino_token(subject)
        entry["token"] = how
    except Exception as e:  # noqa: BLE001
        entry["error"] = type(e).__name__
        _log(entry)
        raise
    _log(entry)
    import jwt

    trino_user = jwt.decode(token, options={"verify_signature": False})["sub"]
    http = requests.Session()
    http.verify = E("REQUESTS_CA_BUNDLE")
    http.cert = (E("CLIENT_CERT"), E("CLIENT_KEY"))
    args = params.setdefault("connect_args", {})
    args.update({"http_scheme": "https", "auth": JWTAuthentication(token), "http_session": http})
    return uri.set(username=trino_user, password=None), params


def _log(entry):
    with _LOCK, open(E("MUTATOR_LOG", "/log/mutator.jsonl"), "a") as f:
        f.write(json.dumps(entry) + "\n")
