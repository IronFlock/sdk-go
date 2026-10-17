# Integration and conformance tests

End-to-end tests of the Go SDK against the [IronFlock router](https://github.com/IronFlock/router), the
IronFlock platform's WAMP router, and a fake IronFlock platform, plus a cross-SDK conformance check that the Go,
Python and JavaScript SDKs put the same messages on the wire. They are behind the `integration` build tag and skip
unless their environment is configured.

The router runs from a binary or from its container image. Its client roles (`app`, `app_reader`) are copied verbatim
from the router's reference config, and app sessions authenticate the way production does: dynamic WAMP-CRA with a
role chosen per realm. The fake platform follows the contracts of the current backends, each pinned in its model:
fleetdb-service v1.4.0, fleetfiles-service v0.2.0, ironflock-auth v0.4.0 with REaccounting's
`device.f_authenticate_userapp`, RESWARM's cross-app discovery (reswarm-backend v1.4.1), and the device agent's
per-app credential (v1.2.1). autobahn-python is test tooling only: the fake platform and the Python SDK's reference
run use it.

## Pieces

| Path | Purpose |
| --- | --- |
| `fake_platform.py` | Joins the router's service listener as the platform's services: app 26's data backend on `realm-2-26-dev` (fleetdb and fleetfiles, with an S3-compatible store for presigned URLs), a provider app's backend on `realm-2-77-dev`, and ironflock-auth on `ironflock.auth` (the `identity` authorizer callout `auth.authorize` and the dynamic WAMP-CRA authenticator `auth.userapp.authenticate`). Holds the harness data (templates, fixture rows, file namespaces) and records every payload an SDK sends it. Test hooks below |
| `fake_fleetdb.py`, `fake_fleetfiles.py`, `fake_auth.py` | The models behind it: ports of the real services' request checks, refusals and answers (standard library only; each names its pinned source) |
| `test_fake_platform.py`, `contracts/` | Offline self-test: the models against the verdicts the real services gave for the same inputs (contract tables, see [contracts/README.md](contracts/README.md)), the credential fixture, and the backend behaviors the tests rely on |
| `router/start.sh`, `stop.sh`, `restart.sh` | Start the router (binary or image) and the fake platform; stop both; restart only the router (an outage of a few seconds, for `TestRouterRestartRecovery`). `lib.sh` holds their shared code, `test_scripts.sh` checks their path handling |
| `router/config.template.yaml`, `gen_config.py` | The router config. `gen_config.py` copies the production `app`, `app_reader` and `svc_auth` roles, and the fleetdb and fleetfiles rules the fake platform's role combines, byte-for-byte from the router's reference config |
| `router/perapp_env/` | The per-app credential (`APP_AUTH_ID.txt`, `APP_AUTH_SECRET.txt`) of app 26 on the harness's device, in the format and with the derivation the device agent uses, written as it writes them to `/data/env` (no trailing newline). The Go suites and package `wamp`'s `TestRealRouter` connect with it |
| `py_scenario.py`, `js_scenario.mjs` | The conformance scenario with the Python and JavaScript SDKs: writes, per step, what the platform recorded, what the SDK returned and its error class |
| `scenario_test.go`, `scenario_divergences.json` | The same scenario with the Go SDK; compares the recorded wire payloads and error classes with a reference run, except for the steps the divergence list names |
| `e2e_test.go`, `py_peer.py` | Live tests: device functions called across SDKs, table subscriptions through the platform republish (bulk rows unrolled), cross-app realtime and guards, per-app credential rotation, router restart recovery, keepalive detection of a silently dead link |

## Quick start

You need Go, Python 3.11 or newer with the `ironflock` package (the reference SDK; it brings autobahn for the fake
platform: ironflock 1.9.1 pins autobahn 25.12.2, which needs 3.11), and either the router image and Docker or a router
binary. All commands run from the repository root.

```shell
python3.11 -m venv ~/.venvs/ironflock && ~/.venvs/ironflock/bin/pip install ironflock==1.9.1   # or any python3 >= 3.11
export PYTHON=~/.venvs/ironflock/bin/python

# Router image (pulling it needs read access to record-1283's Artifact Registry and
# `gcloud auth configure-docker europe-docker.pkg.dev`):
ROUTER_IMAGE=europe-docker.pkg.dev/record-1283/eu.gcr.io/router:v0.8.7 just test-integration

# Or a router binary built from a router checkout, and the reference config from its source tree:
just router-build /path/to/router
ROUTER_BIN=integration/router/.bin/router \
ROUTER_REF_CONFIG=/path/to/router/examples/config.yaml \
  just test-integration
```

`just test-integration` runs `just test-harness` (the offline checks: `test_fake_platform.py` and `test_scripts.sh`),
starts the harness, runs the Python reference, package `wamp`'s real-router tests and the integration suite against
it, and stops the harness again.

**Building the router.** `just router-build SRC [OUT] [REF]` builds `REF` (default `HEAD`) of the router
checkout `SRC` into `OUT` (default `integration/router/.bin/router`, ignored by git). It builds
a `git archive` of the ref in a temporary directory against the nexus fork version this SDK pins, so the checkout is
left untouched (`go mod tidy` needs the module proxy unless the modules are cached). Router refs from v0.8.2 on
require `github.com/ironflock/nexus/v3` directly; older refs still replace upstream nexus with a local path, and the
recipe points that replace at the last fork commit with the upstream module path. Binary mode on macOS needs such a
build; the image's binary is Linux-only. To build the image yourself, run
`docker buildx build -t router:dev --load .` in a router checkout.

## Step by step

Useful when you keep the harness running while you work; `PYTHON` is exported as above. Paths that a test or script
uses from another directory are absolute (`$PWD/...`); `IRONFLOCK_TEST_RESTART_ROUTER` is relative to `integration/`,
where `go test` runs the test, so it needs no quoting whatever the checkout's path.

```shell
# 1. Router and fake platform. Image instead of binary: ROUTER_IMAGE=... in place of
#    the two ROUTER_* lines.
ROUTER_BIN=/path/to/router \
ROUTER_REF_CONFIG=/path/to/router/examples/config.yaml \
  integration/router/start.sh

# 2. Python reference run, with the device identity the fake platform admits and the
#    per-app credential.
DEVICE_SERIAL_NUMBER=06a0bf96-a539-4d6a-8471-ac7adc67616e SWARM_KEY=2 APP_KEY=26 ENV=DEV \
DEVICE_KEY=42 APP_NAME=interop DEVICE_NAME=interop-dev \
IRONFLOCK_ENV_DIR=$PWD/integration/router/perapp_env \
IRONFLOCK_TEST_PLATFORM_URL=ws://localhost:18082/ws-ua-usr \
  "$PYTHON" -I integration/py_scenario.py /tmp/py_reference.json

# 3. The Go suites: package wamp's real-router tests, then the integration suite,
#    compared with that reference.
IRONFLOCK_TEST_ROUTER_URL=ws://localhost:18082/ws-ua-usr \
  go test -race -count=1 -run TestRealRouter -v ./wamp/
IRONFLOCK_TEST_PLATFORM_URL=ws://localhost:18082/ws-ua-usr \
IRONFLOCK_TEST_ENV_DIR=$PWD/integration/router/perapp_env \
IRONFLOCK_TEST_REFERENCE=/tmp/py_reference.json \
IRONFLOCK_TEST_PYTHON="$PYTHON" \
IRONFLOCK_TEST_RESTART_ROUTER=router/restart.sh \
  go test -tags integration -race -count=1 -v ./integration/...

# 4. Stop the router and the fake platform.
integration/router/stop.sh
```

The reference run must use the per-app credential, as the Go tests do. Without it the Python SDK falls back to the
legacy device credential (serial number as authid and secret), which the platform admits only on the realm of an app
installed on the device. The cross-app steps then fail, and the wire comparison with the Go run fails with them.

## The harness

`start.sh` generates the router config from the template and the reference config, starts the router, waits until
its `/healthz` answers, starts the fake platform and waits until its three sessions (own realm, provider realm, auth
realm) have joined. If anything fails it prints the logs and stops what it started. It refuses to start when the
instance in its state directory is still running, when a port is taken, or when `PYTHON` is older than 3.11.

| Port (variable) | Default | What |
| --- | --- | --- |
| `ROUTER_PUBLIC_PORT` | 18082 | `/ws-ua-usr`, the SDKs' listener |
| `ROUTER_OPS_PORT` | 18083 | `/healthz`, `/readyz`, `/stats` |
| `ROUTER_INTERNAL_PORT` | 18084 | `/ws-svc`, the fake platform's listener |
| `FAKE_PLATFORM_S3_PORT` | 18091 | the fake platform's object store (presigned URLs point here) |

`AUTH_MODE=dynamic` (the default) is production's auth shape: the router asks `auth.userapp.authenticate`, which the
fake platform answers like ironflock-auth (below). `AUTH_MODE=static` uses static users with one role on every realm,
so cross-app access cannot work there, and their sessions keep the authid they presented.
`IDENTITY_AUTHORIZER_MODE=shadow` makes the `identity` authorizer callout advisory for debugging: the router logs what
it would deny (`authorizer shadow: would deny`), and the static role rules alone decide.

**Binary mode** (`ROUTER_BIN`, plus `ROUTER_REF_CONFIG`) runs the router as a background process; its output goes to
`router.log` in the state directory. **Image mode** (`ROUTER_IMAGE`) needs Docker. It pulls the image if it is not
present and, unless `ROUTER_REF_CONFIG` is set, copies the reference config out of it
(`/etc/router/config.yaml`, the router's `examples/config.yaml`). The container, named
`ironflock-go-router-<ROUTER_PUBLIC_PORT>`, mounts the generated config read-only and publishes its ports on
127.0.0.1 only; use `ws://127.0.0.1:...` with a client that does not fall back from `::1` to 127.0.0.1 when it
resolves `localhost`. Read its log with `docker logs` while it runs; `stop.sh` saves it to `router.log` before it
removes the container.

The state directory (`STATE_DIR`, default `integration/router/.run`, ignored by git) holds the generated
`config.yaml`, the logs, the PIDs or the container ID, and `harness.env`, the settings `start.sh` used. `stop.sh` and
`restart.sh` act on the instance recorded there and need no other variables; give them the same `STATE_DIR` when it
is not the default, also to the test that runs `IRONFLOCK_TEST_RESTART_ROUTER` (export it). `stop.sh` signals a
recorded PID only while its command line still matches what `start.sh` launched, and keeps the logs.

`restart.sh` stops only the router, keeps it down for `RESTART_DOWNTIME` seconds and starts it again with the same
config. It returns once the router is healthy and the fake platform, which keeps running, has rejoined all its
realms.

A second instance next to a running one needs other ports and its own state directory:

```shell
export STATE_DIR=/tmp/ironflock-harness-2
ROUTER_PUBLIC_PORT=19082 ROUTER_OPS_PORT=19083 ROUTER_INTERNAL_PORT=19084 FAKE_PLATFORM_S3_PORT=19091 \
ROUTER_IMAGE=europe-docker.pkg.dev/record-1283/eu.gcr.io/router:v0.8.7 \
  integration/router/start.sh
# ... tests with IRONFLOCK_TEST_PLATFORM_URL=ws://localhost:19082/ws-ua-usr and
#     IRONFLOCK_TEST_RESTART_ROUTER=router/restart.sh (STATE_DIR is exported)
integration/router/stop.sh
```

`gen_config.py --check` proves that the copied roles parse to exactly the reference roles. `start.sh` runs it when
`$PYTHON` has PyYAML (`pip install pyyaml`), as CI does.

### Identity

The harness's platform has one device: key 42 in swarm 2, serial `06a0bf96-a539-4d6a-8471-ac7adc67616e`, running app
26 (`interop`) in DEV. App 77 (`weather`) has a DEV data backend in the same swarm, and app 26 holds the data-access
grant `["weather"]` (`fake_auth.PlatformDB`).

- **The per-app credential** in `perapp_env/` is what the device agent writes for app 26: authid
  `app-26-dev-e1@<serial>`, secret `base64(HMAC-SHA256(key, "ironflock/app-cred/v1|<serial>|26|DEV|1"))`, where the
  key is the UTF-8 bytes of the device's stored `app_cred_key` string (`fake_auth.APP_CRED_KEY` for the harness's
  device).
- **`auth.userapp.authenticate`** follows ironflock-auth v0.4.0's `Userapp.Authenticate` over REaccounting's
  `device.f_authenticate_userapp` (legacy mode `own_realm_only`): the per-app credential gets `app` on its app's realm
  and `app_reader` on a granted provider's realm, for the current credential epoch or the previous one within 15
  minutes of a rotation; the legacy credential (serial, serial) gets `app` on the realm of an app installed on the
  device in that realm's stage, nothing else. Every session's authid becomes the device serial, so a callee sees
  `caller_authid` = serial and fleetdb stamps rows with it, whichever credential was used.
- **`auth.authorize`** follows ironflock-auth v0.4.0's `Policy.Decide`: on a data realm, REGISTER of a digit-led name
  only for the session's own device with the realm's app and stage (stage in any case; prefix and wildcard patterns
  when they fix those segments); CALL and PUBLISH of any digit-led name within the realm's swarm. SUBSCRIBE to such
  names never reaches the callout: the `app` role refuses it statically. (The router's docs/AUTHORIZATION.md also
  binds the publisher of a function URI to its device; ironflock-auth does not, and the harness follows
  ironflock-auth.)

The router caches an authentication for 10 s and an authorization verdict for 30 s (its `auth.cache_ttl_sec` and the
authorizer's `cache_ttl_sec` in `config.template.yaml`).

### The fake platform

Per declared table, the fake fleetdb registers exactly what fleetdb v1.4.0 registers (`append.2.26.<t>`,
`appendBulk.2.26.<t>`, `history.transformed.<t>`, `history.transformed.series.<t>`, `secret.reveal.<t>` and
`secret.verify.<t>` for tables with a secret column, `history.transformed.<transform>`, the same four for the system
table `error-logs`, `sys.appaccess.resolve` and `sys.appaccess.list`) and subscribes `2.26.<t>` and `bulk.2.26.<t>`, so
an undeclared table answers `wamp.error.no_such_procedure` and a publish to it reaches nobody, as in production. App
26's tables (`OWN_TEMPLATE` in `fake_platform.py`, every one with the mandatory `tsp` column): `sensordata`,
`e2e_table`, `restart_table`, `credentials` (secret column `api_key`, entity key `machine`, one fixture row), `chunked`
(a 512-byte chunk budget) and the transform `sensordata_hourly`; app 77's: `readings` (secret `api_key`, fixture
rows), `private_stats` (private) and the transform `hourly`.

What it checks and answers like the real services:

- **Requests**: fleetdb's typia checks of every history, series, secret and resolve payload (refusal:
  `wamp.error.runtime_error` with the TypeGuardError `{method, path, expected, value}` as its argument), including the
  series shape `{metrics: [{ref, method}], limit, timeRange, bucketMs?, groupBy?, filterAnd?}`; the typed refusals
  (`sys.dataservice.error.invalid_time_range`, `invalid_metric`, `invalid_group_by`, `invalid_filter`,
  `secret_column`, `not_a_secret_column`, `invalid_limit`, `rate_limited`, `result_too_large`, `series_too_many_groups`,
  `entity_key_conflict`, `secret_sentinel_unresolvable`, `secret_ciphertext_rejected`, `storage_full`,
  `storage_overusage`, `wamp.error.not_authorized` for private tables and non-app secret callers).
- **Writes**: the template's column paths (`args[0].<column>` by default, so a kwargs-only publish carries no row),
  `device_key` from `DEVICE_KEY`, `authid` from the publisher, a mandatory `tsp` (an append without one fails with
  `wamp.error.runtime_error` and the argument `{}`, a publish without one is dropped), bulk writes all-or-nothing,
  entity-key carry-over; the republish on `transformed.<t>` / `transformed.bulk.<t>` typed per column (tsp as epoch
  ms), with `device_key` and `authid`, secrets masked as `__secret__`, private tables excluded from `app_reader`.
- **Reads**: the newest `limit` rows after `offset`, returned oldest first; `tsp` as epoch ms; `SELECT *` with
  `device_key` and `authid`, or `columns` (plus `tsp`, `device_key`, `authid`) and `columnPaths`; `timeRange` as
  [start, end) truncated to whole seconds (series bounds are exact); the `latest` marker (latest row per entity key,
  or the single latest row); filters with SQL's three-valued logic, groups, `IN` / `NOT IN` with the empty set;
  filters on unknown columns or with values the column type cannot take are dropped. Series: buckets aligned to the
  range start, `limit` as the bucket budget (`bucketMs` widened to fit it), columns `tsp`, the `groupBy` columns and
  `"<METHOD>:<ref>"`, empty buckets absent. Secret reveal returns the plaintext rows newest first (it bypasses the
  reversal). A transform read applies only `limit` (at most 3000), `offset` and `filterAnd`.
- **Large results**: with `receive_progress`, a result over the table's chunk budget comes back as progressive
  results `[chunkIndex, rows]` (rows newest first) and a final `{chunked: true, chunkCount, totalRows, order: "asc"}`
  (concatenate the chunks and reverse them for the single-shot answer); without it, `result_too_large`. The budget is
  fleetdb's 8 MiB, 512 bytes for the table `chunked`, or what `test.fleetdb.chunk_bytes` sets.
- **Files** (fleetfiles v0.2.0): the `{success, payload}` / `{success: false, code, reason}` envelope; an unknown
  namespace is `NO_SUCH_NAMESPACE` before anything else; every key and non-empty list prefix goes through
  `naming.NormalizeKey` (a malformed one, such as a prefix ending in `/`, is `INTERNAL`); `files.read.list` lists
  folder-style unless the call sends a string `delimiter` (`""` lists recursively), objects and common prefixes share
  the page budget, `limit` absent or <= 0 means 200 and is clamped to 1000; ranged `files.read.get` (`offset`,
  `length` clamped to the inline cap, `INVALID_RANGE`); a plain get over the inline cap is `TOO_LARGE`; presigned URLs
  (TTL clamped to 60..3600 s, 300 s for `app_reader`, `NOT_SUPPORTED` when presigning is off) that the fake S3 store
  checks for their signature and expiry; `files.events` after put, delete and copy.

Test hooks (procedures on `realm-2-26-dev`; the recordings never include them):

| Procedure | What it does |
| --- | --- |
| `test.recorded()`, `test.clear_recorded()` | The recorded payloads (`{kind, uri, args, kwargs}`, kind `call`, `publish`, `provider-call` or `http`); clear them |
| `test.reset()` | Clears the recordings, stored rows (fixture rows come back), files, file settings, chunk budgets, write refusals and grants; not the credential epochs |
| `test.provider.publish(t, row, bulk=False)` | Publishes `row` (a list when `bulk`) as is on `transformed[.bulk].<t>` of the provider realm |
| `test.fleetdb.insert(t, rows, provider=False)` | Stores rows through fleetdb's write path (tsp required) as device 42 wrote them, without a republish; returns the count |
| `test.fleetdb.chunk_bytes(t, n, provider=False)` | Sets table `t`'s chunk budget (`None`: the default) |
| `test.fleetdb.refuse_writes(reason=None, provider=False)` | `"STORAGE_FULL"`: appends fail with `sys.dataservice.error.storage_full`; any other reason: `...storage_overusage`; `None`: accept writes |
| `test.grants(data_access)` | App 26's data-access grant (default `["weather"]`; `["weather", "*"]` makes `sys.appaccess.list` answer) |
| `test.files.configure(presign_available=None, presign_base_url=None, inline_max_bytes=None)` | Presigning off (the catalog says so, the URL verbs answer `NOT_SUPPORTED`), minted URLs pointing elsewhere (e.g. a closed port), another inline cap; returns the settings |
| `test.auth.rotate(app=26, stage="DEV", grace_expired=False)` | Moves the credential to the next epoch (returns `{authid, secret, epoch}`); the previous one stays valid for 15 minutes unless `grace_expired` |
| `test.auth.reset()` | Every credential epoch back to 1 |
| `test.authz_log(clear=False)` | The identity services' verdicts, with reasons |

Local deviations from production: static realm/role bindings instead of fleetdb's runtime binding; one test service
role standing in for fleetdb and fleetfiles; one session per realm serving both; rows in memory, so a value Postgres
would refuse fails with `wamp.error.runtime_error` and a short pg-like error object; timestamps parse as ISO 8601
only; `LIKE` on numeric, timestamp and json values matches nothing; no continuous aggregates, cron transforms,
alarms, subscriber gate or realtime size guard; no files on the provider realm; the catalog reports
`inline_max_bytes` 1024 instead of 6291456, so 5000-byte objects take the presigned path; the object store has no
versions or multipart uploads; the identity callout answers only data-realm names (the device-agent, appliance and
account families have no sessions here).

### Contract tables

`test_fake_platform.py` (standard library only; `just test-harness`, CI's harness job) replays the inputs of
`contracts/*.json` through the models and compares their answers with the real services' verdicts: fleetdb's typia
validators compiled from its source, fleetfiles' `naming.NormalizeKey`, ironflock-auth's `Policy.Decide`, and
REaccounting's `f_authenticate_userapp` run in Postgres. When a backend changes, `contracts/regen.sh` regenerates the
tables from the new sources and the self-test shows where the fake no longer matches
([contracts/README.md](contracts/README.md)).

## The conformance scenario

`py_scenario.py`, `js_scenario.mjs` and `scenario_test.go` run the same steps with the same arguments. Every table
row carries a `tsp`; `append_without_tsp` pins fleetdb's refusal of a row without one. Each step records the wire
payloads, the result and the error class: `<Type>: <code>` for the SDKs' typed errors (`CrossAppAccessError`,
`FileStoreError`), `WampError: <uri>` for a WAMP error from the router or a backend, otherwise the type name and a
colon (Python, JavaScript) or `Error: <message>` (Go).

`TestCrossSDKScenario` compares the Go run's recorded payloads and error classes with the reference
(`IRONFLOCK_TEST_REFERENCE`, a `py_scenario.py` output), step by step, except for the steps
`scenario_divergences.json` lists: a JSON array of `{"step": ..., "reason": ...}` naming a step whose Go run
deliberately differs from the reference SDK's, with the reason. Such a step still runs and must be in the reference;
it is compared with the payloads `scenario_test.go` documents for it (`documentedSteps`) instead, and the reason is
logged. Today the list is empty: ironflock-py 1.9.1 records what the Go run records on every step.

JavaScript (ironflock-js 1.9.1) agrees with Python on every step. To run it, build ironflock-js and run
`IRONFLOCK_JS_SDK=/path/to/ironflock-js/dist/index.mjs node integration/js_scenario.mjs /tmp/js_out.json` with the
environment of step 2. (From a container, point `IRONFLOCK_TEST_PLATFORM_URL` at the host and forward
`127.0.0.1:<FAKE_PLATFORM_S3_PORT>` to it as well: presigned URLs name 127.0.0.1.)

## Environment

### Harness scripts

| Variable | Default | Meaning |
| --- | --- | --- |
| `ROUTER_BIN` | `router` from `PATH` | Router binary (binary mode) |
| `ROUTER_IMAGE` | | Router image (image mode, needs Docker); set this or `ROUTER_BIN` |
| `ROUTER_REF_CONFIG` | copied out of the image | The router's reference config, `examples/config.yaml`; required in binary mode |
| `PYTHON` | `python3` | Python 3.11 or newer with autobahn (`pip install ironflock`); runs the fake platform and `gen_config.py` |
| `AUTH_MODE` | `dynamic` | `dynamic` (production) or `static` |
| `IDENTITY_AUTHORIZER_MODE` | `enforce` | `enforce` or `shadow` |
| `ROUTER_PUBLIC_PORT`, `ROUTER_OPS_PORT`, `ROUTER_INTERNAL_PORT`, `FAKE_PLATFORM_S3_PORT` | 18082, 18083, 18084, 18091 | Ports, see above |
| `IRONFLOCK_LOG_LEVEL` | `info` | The router's log level (`debug`, `info`, `warn`, `error`) |
| `STATE_DIR` | `integration/router/.run` | Generated config, logs, PIDs or container ID, settings (all three scripts) |
| `RESTART_DOWNTIME` | 3 | Seconds `restart.sh` keeps the router down |

### Go tests

| Variable | Meaning |
| --- | --- |
| `IRONFLOCK_TEST_PLATFORM_URL` | Router URL, e.g. `ws://localhost:18082/ws-ua-usr`; all tests in `integration/` skip without it |
| `IRONFLOCK_TEST_ROUTER_URL` | Router URL for package `wamp`'s real-router tests (`TestRealRouter`, no build tag); they skip without it |
| `IRONFLOCK_TEST_REFERENCE` | A reference SDK run (`py_scenario.py` output) to compare wire payloads against |
| `IRONFLOCK_TEST_OUT` | Where to write the Go run of the scenario |
| `IRONFLOCK_TEST_PYTHON` | A Python with the `ironflock` package, for the cross-SDK live tests |
| `IRONFLOCK_TEST_ENV_DIR` | Credential files (`APP_AUTH_ID.txt`, `APP_AUTH_SECRET.txt`) copied into each test's `/data/env` |
| `IRONFLOCK_TEST_RESTART_ROUTER` | Shell command that restarts the router, run with `bash -c` from `integration/` (`TestRouterRestartRecovery`), e.g. `router/restart.sh` |

### Scenario drivers

`py_scenario.py`, `js_scenario.mjs`, and `py_peer.py` (which the Go tests start):

| Variable | Meaning |
| --- | --- |
| `IRONFLOCK_TEST_PLATFORM_URL` | Router URL (default `ws://localhost:18082/ws-ua-usr`) |
| `IRONFLOCK_ENV_DIR` | The SDK's `/data/env` stand-in; point it at the per-app credential |
| `DEVICE_SERIAL_NUMBER`, `SWARM_KEY`, `APP_KEY`, `ENV`, `DEVICE_KEY`, `APP_NAME`, `DEVICE_NAME` | The device identity, as in step 2 |
| `IRONFLOCK_JS_SDK` | `js_scenario.mjs` only: the SDK module to import (default: the `ironflock` package) |

### Fake platform

`start.sh` sets the first five; the others are for running `fake_platform.py` by hand.

| Variable | Default | Meaning |
| --- | --- | --- |
| `FAKE_PLATFORM_ROUTER` | `ws://127.0.0.1:18084/ws-svc` | Router URL of the data-realm sessions |
| `FAKE_PLATFORM_AUTH_ROUTER` | `FAKE_PLATFORM_ROUTER` | Router URL of the ironflock-auth session |
| `FAKE_PLATFORM_AUTH_REALM` | `ironflock.auth` | Realm of the ironflock-auth session; empty: none |
| `FAKE_PLATFORM_USERAPP_AUTH` | `1` | `1`: it also registers `auth.userapp.authenticate` (`AUTH_MODE=dynamic`) |
| `FAKE_PLATFORM_S3_PORT` | 18091 | Port of the fake object store |
| `FAKE_PLATFORM_AUTHID`, `FAKE_PLATFORM_SECRET` | `fake-backend`, `fake-backend-secret` | The backend sessions' credential |
| `FAKE_PLATFORM_AUTH_AUTHID`, `FAKE_PLATFORM_AUTH_SECRET` | `svc_auth`, `svc-auth-secret` | The ironflock-auth session's credential |
| `FAKE_PLATFORM_OWN_REALM`, `FAKE_PLATFORM_PROVIDER_REALM` | `realm-2-26-dev`, `realm-2-77-dev` | The app's and the provider app's data realms |

## CI

`.github/workflows/ci.yml` has three jobs:

- **test**: `go vet`, unit tests with `-race`, cross-builds for linux/arm, linux/arm64, linux/386, windows/amd64 and
  darwin/arm64, `go mod tidy -diff`, and golangci-lint (the same checks as `just check`).
- **harness**: on Python 3.11 and 3.14, without a router: `test_fake_platform.py`, `test_scripts.sh`, shellcheck of
  the harness scripts, and the fake platform starting up with `ironflock==1.9.1`.
- **integration**: starts the harness in image mode, runs the Python reference (`ironflock==1.9.1`), package `wamp`'s
  real-router tests and the integration suite, including `TestRouterRestartRecovery` with `restart.sh`, then stops the
  harness and prints its logs on failure. The image comes from Artifact Registry through workload identity federation,
  so the job runs only once both repository variables below are set, and only for pushes and for pull requests from
  this repository: GitHub gives pull requests from forks no OIDC token. The image is the `ROUTER_IMAGE` repository
  variable, default `europe-docker.pkg.dev/record-1283/eu.gcr.io/router:v0.8.7`.

| Repository variable | Meaning |
| --- | --- |
| `ROUTER_PULL_WIF_PROVIDER` | Full resource name of the workload identity provider the job authenticates with (`projects/<number>/locations/global/workloadIdentityPools/<pool>/providers/<provider>`) |
| `ROUTER_PULL_SERVICE_ACCOUNT` | Email of the read-only service account the job impersonates to pull the router image |
| `ROUTER_IMAGE` | Optional: the router image to test against |

The variables have names of their own on purpose: the organization defines `GCP_WORKLOAD_IDENTITY_PROVIDER` and
`GCP_SERVICE_ACCOUNT` for another release pipeline (service account `reagent-release`), whose provider's attribute
condition refuses this repository. This repository must not inherit them.

### CI setup

One-time setup by a project owner of `record-1283`, with `gcloud` and `gh`: a dedicated service account that may only
read the `eu.gcr.io` repository (location `europe`), and a provider in the existing workload identity pool
`github-pool` (project number 615643915869) that admits only this repository's tokens.

```shell
PROJECT_ID=record-1283
PROJECT_NUMBER=615643915869          # gcloud projects describe record-1283 --format='value(projectNumber)'
POOL=github-pool                     # the existing pool for GitHub Actions
REPO=IronFlock/sdk-go
REPO_ID=$(gh api "repos/$REPO" --jq .id)   # numeric and never reused, unlike the name
PROVIDER=ironflock-go
SA_NAME=ironflock-go-router-pull
SA="$SA_NAME@$PROJECT_ID.iam.gserviceaccount.com"

# 1. A provider for this repository in the existing pool; its attribute condition admits
#    only this repository (the pool's other providers keep their own conditions).
gcloud iam workload-identity-pools providers create-oidc "$PROVIDER" \
  --project="$PROJECT_ID" --location=global --workload-identity-pool="$POOL" \
  --display-name="$REPO" \
  --issuer-uri="https://token.actions.githubusercontent.com" \
  --attribute-mapping="google.subject=assertion.sub,attribute.repository=assertion.repository,attribute.repository_id=assertion.repository_id" \
  --attribute-condition="assertion.repository == '$REPO' && assertion.repository_id == '$REPO_ID'"

# 2. The service account, with read access to the eu.gcr.io repository and nothing else.
gcloud iam service-accounts create "$SA_NAME" \
  --project="$PROJECT_ID" --display-name="ironflock-go CI: pulls router (read-only)"
gcloud artifacts repositories add-iam-policy-binding eu.gcr.io \
  --project="$PROJECT_ID" --location=europe \
  --member="serviceAccount:$SA" --role=roles/artifactregistry.reader

# 3. Workflows of this repository may impersonate it.
gcloud iam service-accounts add-iam-policy-binding "$SA" \
  --project="$PROJECT_ID" --role=roles/iam.workloadIdentityUser \
  --member="principalSet://iam.googleapis.com/projects/$PROJECT_NUMBER/locations/global/workloadIdentityPools/$POOL/attribute.repository/$REPO"

# 4. The repository variables (not secrets: neither value is sensitive).
gh variable set ROUTER_PULL_WIF_PROVIDER --repo "$REPO" \
  --body "projects/$PROJECT_NUMBER/locations/global/workloadIdentityPools/$POOL/providers/$PROVIDER"
gh variable set ROUTER_PULL_SERVICE_ACCOUNT --repo "$REPO" --body "$SA"
# Optional: another router image (default ...router:v0.8.7).
gh variable set ROUTER_IMAGE --repo "$REPO" \
  --body "europe-docker.pkg.dev/record-1283/eu.gcr.io/router:v0.8.7"
```

Instead of step 1, the existing provider of the pool can admit this repository too, by extending its attribute
condition (read it first, keep it, add this repository) and then naming that provider in `ROUTER_PULL_WIF_PROVIDER`.
Impersonation stays per service account, so this does not give the other pipeline's workflows this account, nor this
repository theirs:

```shell
EXISTING=<the existing provider>
OLD=$(gcloud iam workload-identity-pools providers describe "$EXISTING" \
  --project="$PROJECT_ID" --location=global --workload-identity-pool="$POOL" --format='value(attributeCondition)')
gcloud iam workload-identity-pools providers update-oidc "$EXISTING" \
  --project="$PROJECT_ID" --location=global --workload-identity-pool="$POOL" \
  --attribute-condition="($OLD) || (assertion.repository == '$REPO' && assertion.repository_id == '$REPO_ID')"
# The binding of step 3 matches on attribute.repository: the provider's attribute
# mapping must map it (attribute.repository=assertion.repository).
```

The pool's project already has the APIs workload identity federation needs (`iam`, `iamcredentials`, `sts`); a new
project would need `gcloud services enable iam.googleapis.com cloudresourcemanager.googleapis.com
iamcredentials.googleapis.com sts.googleapis.com`. New providers and IAM bindings can take up to five minutes to take
effect. To check the setup: `gcloud artifacts repositories get-iam-policy eu.gcr.io --location=europe
--project=record-1283` lists the reader binding, `gcloud iam service-accounts get-iam-policy "$SA"` the impersonation
binding, and the next push or pull request runs the integration job.
