"""Fake IronFlock platform services for end-to-end SDK tests.

Joins the router's service listener and serves what the real platform
serves on the harness's data realms, following the real services' contracts
(the models live next to this file, each pinned to a backend version):

  realm-2-26-dev   app 26's data backend: fleetdb-service v1.4.0
                   (fake_fleetdb.py) for the template OWN_TEMPLATE below, and
                   fleetfiles-service v0.2.0 (fake_fleetfiles.py) with an
                   S3-compatible store for the presigned URLs
  realm-2-77-dev   the provider app 77's data backend (fleetdb, PROVIDER_TEMPLATE)
  ironflock.auth   ironflock-auth v0.4.0 (fake_auth.py): the router's
                   `identity` authorizer callout (auth.authorize) and its
                   dynamic WAMP-CRA authenticator for app containers
                   (auth.userapp.authenticate), over the platform facts of
                   fake_auth.PlatformDB

It records every payload an SDK sends it (before validating it), so tests can
compare what different SDKs put on the wire. router/start.sh starts
it next to the router; README.md lists what it does not model.

Test hooks (procedures on realm-2-26-dev; nothing here records them):
  test.recorded()                 -> [{"kind", "uri", "args", "kwargs"}]
                                     kind: call, publish, provider-call, http
  test.clear_recorded()           -> clears the recordings only
  test.reset()                    -> recordings, stored rows (the fixture
                                     rows come back), files, file settings,
                                     chunk budgets, write refusals, grants;
                                     not the credential epochs
  test.provider.publish(t, row, bulk=False)
                                  -> publishes row (a list when bulk) as is on
                                     transformed[.bulk].<t> of the provider realm
  test.fleetdb.insert(t, rows, provider=False) -> count
                                     stores rows through fleetdb's write path
                                     (tsp required) without republishing them
  test.fleetdb.chunk_bytes(t, n, provider=False)
                                  -> the chunk budget of t's reads in bytes
                                     (None: the default, 8 MiB; the table
                                     `chunked` has 512): with receive_progress
                                     a larger result comes back chunked
  test.fleetdb.refuse_writes(reason=None, provider=False)
                                  -> "STORAGE_FULL": appends fail with
                                     sys.dataservice.error.storage_full, any
                                     other reason with ...storage_overusage;
                                     None: accept writes again
  test.grants(data_access)        -> app 26's data-access grant (default
                                     ["weather"]; ["weather", "*"] makes
                                     sys.appaccess.list answer)
  test.files.configure(presign_available=None, presign_base_url=None,
                       inline_max_bytes=None) -> the settings in effect
  test.auth.rotate(app=26, stage="DEV", grace_expired=False)
                                  -> {"authid", "secret", "epoch"}: the next
                                     credential epoch; the previous one stays
                                     valid for 15 minutes unless grace_expired
                                     (the router caches an admission for its
                                     auth cache TTL, 10 s here)
  test.auth.reset()               -> every credential epoch back to 1
  test.authz_log(clear=False)     -> the identity services' verdicts

Environment (defaults = the harness router/start.sh runs):

  FAKE_PLATFORM_ROUTER         router URL for the data-realm sessions
                               (default ws://127.0.0.1:18084/ws-svc)
  FAKE_PLATFORM_S3_PORT        fake object store port (default 18091)
  FAKE_PLATFORM_AUTHID/SECRET  backend credential (fake-backend/fake-backend-secret)
  FAKE_PLATFORM_OWN_REALM      default realm-2-26-dev
  FAKE_PLATFORM_PROVIDER_REALM default realm-2-77-dev
  FAKE_PLATFORM_AUTH_REALM     realm of the ironflock-auth session (default
                               ironflock.auth; empty: no auth session)
  FAKE_PLATFORM_AUTH_ROUTER    URL for that session (default FAKE_PLATFORM_ROUTER)
  FAKE_PLATFORM_AUTH_AUTHID/SECRET  its credential (svc_auth/svc-auth-secret)
  FAKE_PLATFORM_USERAPP_AUTH   1 (default): it also registers
                               auth.userapp.authenticate (start.sh
                               AUTH_MODE=dynamic); 0: it does not
                               (AUTH_MODE=static, static router users)
"""

import asyncio
import os
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

# `python -I` keeps the script's directory off sys.path; the models live here.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import fake_auth  # noqa: E402
import fake_fleetdb  # noqa: E402
import fake_fleetfiles  # noqa: E402

try:
    from autobahn.asyncio.component import Component, run
    from autobahn.wamp.exception import ApplicationError
    from autobahn.wamp.types import PublishOptions, RegisterOptions, SubscribeOptions
except ImportError:  # test_fake_platform.py imports this module for its harness data only
    Component = run = ApplicationError = PublishOptions = RegisterOptions = SubscribeOptions = None

URL = os.environ.get("FAKE_PLATFORM_ROUTER", "ws://127.0.0.1:18084/ws-svc")
S3_PORT = int(os.environ.get("FAKE_PLATFORM_S3_PORT", "18091"))
AUTHID = os.environ.get("FAKE_PLATFORM_AUTHID", "fake-backend")
SECRET = os.environ.get("FAKE_PLATFORM_SECRET", "fake-backend-secret")
SWARM, APP, PROVIDER = fake_auth.SWARM, fake_auth.OWN_APP, fake_auth.PROVIDER_APP
OWN_REALM = os.environ.get("FAKE_PLATFORM_OWN_REALM", "realm-%d-%d-dev" % (SWARM, APP))
PROVIDER_REALM = os.environ.get("FAKE_PLATFORM_PROVIDER_REALM", "realm-%d-%d-dev" % (SWARM, PROVIDER))
AUTH_REALM = os.environ.get("FAKE_PLATFORM_AUTH_REALM", "ironflock.auth")
AUTH_URL = os.environ.get("FAKE_PLATFORM_AUTH_ROUTER", URL)
AUTH_AUTHID = os.environ.get("FAKE_PLATFORM_AUTH_AUTHID", "svc_auth")
AUTH_SECRET = os.environ.get("FAKE_PLATFORM_AUTH_SECRET", "svc-auth-secret")
USERAPP_AUTH = os.environ.get("FAKE_PLATFORM_USERAPP_AUTH", "1") == "1"


def col(cid, data_type, **extra):
    return {"id": cid, "name": cid, "description": "", "dataType": data_type, **extra}


# The data templates of the two databackends. Every table has the mandatory
# tsp column. x-chunk-bytes / x-rows are harness-only (see fake_fleetdb.TableDef).
OWN_TEMPLATE = {
    "tables": [
        {"tablename": "sensordata", "description": "Conformance scenario readings",
         "columns": [col("tsp", "timestamp"), col("temperature", "numeric"), col("n", "bigint"),
                     col("source", "string")]},
        {"tablename": "e2e_table", "columns": [col("tsp", "timestamp"), col("temperature", "numeric"),
                                                col("source", "string")]},
        {"tablename": "restart_table", "columns": [col("tsp", "timestamp"), col("source", "string")]},
        {"tablename": "credentials", "maintainLatestFlagFor": ["machine"],
         "columns": [col("tsp", "timestamp"), col("machine", "string"), col("api_key", "string", secret=True)]},
        {"tablename": "chunked", "x-chunk-bytes": 512,
         "columns": [col("tsp", "timestamp"), col("seq", "bigint"), col("payload", "string")]},
    ],
    "transforms": [
        {"tablename": "sensordata_hourly", "columns": [col("tsp", "timestamp"), col("avg_temperature", "numeric")],
         "x-rows": [{"tsp": 1767225600000, "avg_temperature": 21.5}, {"tsp": 1767229200000, "avg_temperature": 22.0},
                    {"tsp": 1767232800000, "avg_temperature": 22.5}]},
    ],
}
OWN_FIXTURES = {"credentials": [{"tsp": "2026-01-01T00:00:00Z", "machine": "m1", "api_key": "right"}]}

PROVIDER_TEMPLATE = {
    "tables": [
        {"tablename": "readings", "description": "raw readings",
         "columns": [col("tsp", "timestamp"), col("temp", "numeric"), col("api_key", "string", secret=True)]},
        {"tablename": "private_stats", "private": True, "columns": [col("tsp", "timestamp"), col("value", "numeric")]},
    ],
    "transforms": [
        {"tablename": "hourly", "columns": [col("tsp", "timestamp"), col("temp_avg", "numeric")],
         "x-rows": [{"tsp": 1767225600000, "temp_avg": 19.75}]},
    ],
}
PROVIDER_FIXTURES = {"readings": [{"tsp": "2026-01-01T00:00:00Z", "temp": 19.5, "api_key": "k-1"},
                                  {"tsp": "2026-01-01T01:00:00Z", "temp": 20.0, "api_key": "k-2"}]}

FILE_NAMESPACES = [{"name": "default"},
                   {"name": "frames", "description": "Raw frames", "contentTypes": ["image/jpeg"],
                    "maxObjectBytes": 1048576}]
SAD_KEY = 3317
PUBLIC_BASE_URL = "https://files.example.test"

recorded = []
authz_log = []  # identity-service verdicts; kept apart so test.recorded stays SDK-only
sessions = {"own": None, "provider": None}
loop_holder = {"loop": None}


def record(kind, uri, args, kwargs):
    recorded.append({"kind": kind, "uri": uri, "args": list(args), "kwargs": dict(kwargs)})


def publisher(name):
    """A model's publish callback: publishes on the session of that realm
    (dropped while it is not joined, as a router outage drops events)."""
    def publish(topic, args, exclude_authrole=None):
        def send():
            session = sessions[name]
            if session is None:
                return
            options = PublishOptions(exclude_me=False, exclude_authrole=exclude_authrole) \
                if exclude_authrole else PublishOptions(exclude_me=False)
            session.publish(topic, *args, options=options)
        loop = loop_holder["loop"]
        if loop is not None and threading.current_thread() is not threading.main_thread():
            loop.call_soon_threadsafe(send)
        else:
            send()
    return publish


platform = fake_auth.PlatformDB()
# Fixture rows and test.fleetdb.insert rows are written as the harness's
# device would publish them (DEVICE_KEY 42, the session authid = its serial).
DEVICE_WRITER = ({"DEVICE_KEY": str(fake_auth.DEVICE_KEY)}, {"publisher_authid": fake_auth.SERIAL})
own_backend = fake_fleetdb.Backend(SWARM, APP, "DEV", OWN_TEMPLATE, platform, publisher("own"),
                                   fixtures=OWN_FIXTURES, writer=DEVICE_WRITER)
provider_backend = fake_fleetdb.Backend(SWARM, PROVIDER, "DEV", PROVIDER_TEMPLATE, platform, publisher("provider"),
                                        fixtures=PROVIDER_FIXTURES, writer=DEVICE_WRITER)
platform.add_databackend(SWARM, APP, "DEV", own_backend.catalog)
platform.add_databackend(SWARM, PROVIDER, "DEV", provider_backend.catalog)
files = fake_fleetfiles.FileService(SWARM, SAD_KEY, FILE_NAMESPACES, PUBLIC_BASE_URL,
                                    lambda: "http://127.0.0.1:%d" % S3_PORT, publisher("own"))


# ----------------------------------------------------------------- S3 server

class S3(BaseHTTPRequestHandler):
    """The object store presigned URLs point at (fake_fleetfiles.FileService)."""

    def log_message(self, *a):
        pass

    def _reply(self, status, headers, body, head=False):
        self.send_response(status)
        for k, v in headers.items():
            self.send_header(k, v)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if not head:
            self.wfile.write(body)

    def do_PUT(self):
        path, _, query = self.path.partition("?")
        n = int(self.headers.get("Content-Length", "-1"))
        if self.headers.get("Transfer-Encoding", "").lower() == "chunked" or n < 0:
            recorded.append({"kind": "http", "uri": "PUT " + path, "args": [None],
                             "kwargs": {"content_type": self.headers.get("Content-Type")}})
            self._reply(*fake_fleetfiles._s3_error(411, "MissingContentLength", "You must provide the Content-Length HTTP header."))
            return
        body = self.rfile.read(n)
        recorded.append({"kind": "http", "uri": "PUT " + path, "args": [len(body)],
                         "kwargs": {"content_type": self.headers.get("Content-Type")}})
        self._reply(*files.s3_put(path, query, body, self.headers.get("Content-Type")))

    def do_GET(self, head=False):
        path, _, query = self.path.partition("?")
        recorded.append({"kind": "http", "uri": ("HEAD " if head else "GET ") + path, "args": [],
                         "kwargs": {"range": self.headers.get("Range")}})
        status, headers, body = files.s3_get(path, query, self.headers.get("Range"))
        self._reply(status, headers, body, head=head)

    def do_HEAD(self):
        self.do_GET(head=True)


def start_s3():
    srv = ThreadingHTTPServer(("127.0.0.1", S3_PORT), S3)
    threading.Thread(target=srv.serve_forever, daemon=True).start()


# ------------------------------------------------------------ WAMP bindings

def call_details(details):
    d = {}
    if details is not None:
        for key in ("caller_authid", "caller_authrole"):
            if getattr(details, key, None) is not None:
                d[key] = getattr(details, key)
        if getattr(details, "progress", None) is not None:
            d["progress"] = lambda index, rows: details.progress(index, rows)
    return d


def wamp_errors(fn):
    try:
        return fn()
    except fake_fleetdb.WampError as e:
        raise ApplicationError(e.error, *e.wamp_args, **e.wamp_kwargs)


async def bind_backend(session, backend, kind):
    for uri, _ in backend.procedures():
        def make(u):
            def handler(*args, details=None, **kwargs):
                record(kind, u, args, kwargs)
                return wamp_errors(lambda: backend.call(u, list(args), kwargs, call_details(details)))
            return handler
        await session.register(make(uri), uri, options=RegisterOptions(details_arg="details"))

    for topic, _, _ in backend.write_topics():
        def on_event(*args, details=None, **kwargs):
            topic_ = details.topic
            record("publish", topic_, args, kwargs)
            d = {}
            if details.publisher_authid is not None:
                d["publisher_authid"] = details.publisher_authid
            if details.publisher_authrole is not None:
                d["publisher_authrole"] = details.publisher_authrole
            backend.event(topic_, list(args), kwargs, d)
        await session.subscribe(on_event, topic, options=SubscribeOptions(details_arg="details"))


def transport(url):
    return [{"type": "websocket", "url": url, "serializers": ["msgpack"], "max_retries": -1, "max_retry_delay": 2}]


def own_component():
    comp = Component(transports=transport(URL), realm=OWN_REALM,
                     authentication={"wampcra": {"authid": AUTHID, "secret": SECRET}})

    @comp.on_join
    async def joined(session, details):
        await bind_backend(session, own_backend, "call")

        for uri in fake_fleetfiles.URIS:
            def make(u):
                def handler(*args, details=None, **kwargs):
                    record("call", u, args, kwargs)
                    return files.call(u, list(args), kwargs, call_details(details))
                return handler
            await session.register(make(uri), uri, options=RegisterOptions(details_arg="details"))

        def reset():
            recorded.clear()
            own_backend.reset()
            provider_backend.reset()
            files.reset()
            platform.reset()
            return True

        def clear_recorded():
            recorded.clear()
            return True

        def provider_publish(table, row, bulk=False):
            s = sessions["provider"]
            if s is None:
                raise ApplicationError("test.error.no_provider", "provider session not joined")
            s.publish(("transformed.bulk." if bulk else "transformed.") + table, row)
            return True

        def backend_of(provider):
            return provider_backend if provider else own_backend

        def fleetdb_insert(table, rows, provider=False):
            return wamp_errors(lambda: backend_of(provider).insert(table, rows))

        def fleetdb_chunk_bytes(table, n, provider=False):
            wamp_errors(lambda: backend_of(provider).set_chunk_bytes(table, n))
            return True

        def fleetdb_refuse_writes(reason=None, provider=False):
            backend_of(provider).refuse_writes(reason)
            return True

        def set_grants(data_access):
            platform.set_grants(data_access)
            return True

        def files_configure(presign_available=None, presign_base_url=None, inline_max_bytes=None):
            return files.configure(presign_available, presign_base_url, inline_max_bytes)

        def auth_rotate(app=APP, stage="DEV", grace_expired=False):
            return platform.rotate(app, stage, grace_expired)

        def auth_reset():
            platform.reset_credentials()
            return True

        def get_authz_log(clear=False):
            out = list(authz_log)
            if clear:
                authz_log.clear()
            return out

        hooks = {
            "test.recorded": lambda: recorded,
            "test.clear_recorded": clear_recorded,
            "test.reset": reset,
            "test.provider.publish": provider_publish,
            "test.fleetdb.insert": fleetdb_insert,
            "test.fleetdb.chunk_bytes": fleetdb_chunk_bytes,
            "test.fleetdb.refuse_writes": fleetdb_refuse_writes,
            "test.grants": set_grants,
            "test.files.configure": files_configure,
            "test.auth.rotate": auth_rotate,
            "test.auth.reset": auth_reset,
            "test.authz_log": get_authz_log,
        }
        for uri, fn in hooks.items():
            await session.register(fn, uri)
        sessions["own"] = session
        print("fake platform: own realm ready", flush=True)

    @comp.on_leave
    async def left(session, details):
        sessions["own"] = None

    return comp


def provider_component():
    comp = Component(transports=transport(URL), realm=PROVIDER_REALM,
                     authentication={"wampcra": {"authid": AUTHID, "secret": SECRET}})

    @comp.on_join
    async def joined(session, details):
        await bind_backend(session, provider_backend, "provider-call")
        sessions["provider"] = session
        print("fake platform: provider realm ready", flush=True)

    @comp.on_leave
    async def left(session, details):
        sessions["provider"] = None

    return comp


def authenticate_userapp(realm, authid, details=None):
    """auth.userapp.authenticate: args [realm, authid, details]."""
    result, verdict = fake_auth.userapp_authenticate(platform, realm, authid)
    authz_log.append({"kind": "authenticate", "realm": realm, "authid": authid, "role": result.get("role"),
                      "session_authid": result.get("authid"), "reason": verdict.get("reason"),
                      "cred": verdict.get("cred")})
    return result


def authorize(session, uri, action, options=None):
    """auth.authorize: args [session, uri, action, options]."""
    verdict, why = fake_auth.authorize(platform, session or {}, uri, action, options, static_mode=not USERAPP_AUTH)
    authz_log.append({"kind": "authorize", "realm": (session or {}).get("realm"), "authid": (session or {}).get("authid"),
                      "authrole": (session or {}).get("authrole"), "action": action, "uri": uri,
                      "match": (options or {}).get("match"), "allow": verdict["allow"],
                      "cache_prefix": verdict.get("cache_prefix"), "why": why})
    return verdict


def auth_component():
    comp = Component(transports=transport(AUTH_URL), realm=AUTH_REALM,
                     authentication={"wampcra": {"authid": AUTH_AUTHID, "secret": AUTH_SECRET}})

    @comp.on_join
    async def joined(session, details):
        await session.register(authorize, "auth.authorize")
        if USERAPP_AUTH:
            await session.register(authenticate_userapp, "auth.userapp.authenticate")
        print("fake platform: auth realm ready (userapp authenticator: %s)" % USERAPP_AUTH, flush=True)

    return comp


if __name__ == "__main__":
    start_s3()
    components = [own_component(), provider_component()]
    if AUTH_REALM:
        components.append(auth_component())
    # autobahn 25.12.2's run() calls asyncio.get_event_loop(), which raises on
    # Python 3.14 when no loop is set (3.12 and 3.13 warn): give it one.
    loop_holder["loop"] = asyncio.new_event_loop()
    asyncio.set_event_loop(loop_holder["loop"])
    run(components, log_level="warn")
