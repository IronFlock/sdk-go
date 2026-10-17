#!/usr/bin/env bash
# Starts ironflock-router with the production `app` / `app_reader` roles, plus
# the fake platform, for the integration tests (see ../README.md).
#
# The router comes from a binary or from an image:
#
#   ROUTER_BIN=/path/to/router \
#   ROUTER_REF_CONFIG=/path/to/router/examples/config.yaml ./start.sh
#
#   ROUTER_IMAGE=europe-docker.pkg.dev/record-1283/eu.gcr.io/router:v0.8.7 ./start.sh
#
# ROUTER_REF_CONFIG is the router's reference config, the source of the
# production roles. With ROUTER_IMAGE it is optional: by default it is copied
# out of the image.
#
# PYTHON                    Python 3.11+ with autobahn, e.g. after `pip install
#                           ironflock` (default python3)
# AUTH_MODE                 dynamic (default): production's dynamic WAMP-CRA;
#                           the fake platform answers auth.userapp.authenticate
#                           as ironflock-auth does (the per-app credential gets
#                           `app` on realm-2-26-dev, `app_reader` on
#                           realm-2-77-dev, and the session authid becomes the
#                           device serial). static: static users, one role on
#                           every realm.
# IDENTITY_AUTHORIZER_MODE  enforce (default), or shadow: the identity callout
#                           only logs its verdicts
# ROUTER_PUBLIC_PORT        18082, /ws-ua-usr (the SDKs)
# ROUTER_OPS_PORT           18083, /healthz /readyz /stats
# ROUTER_INTERNAL_PORT      18084, /ws-svc (the fake platform)
# FAKE_PLATFORM_S3_PORT     18091, the fake platform's object store
# IRONFLOCK_LOG_LEVEL       the router's log level (default info)
# STATE_DIR                 generated config, logs, PIDs or container ID
#                           (default .run next to this script)
set -euo pipefail
# shellcheck source=lib.sh
. "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

# ------------------------------------------------------------------ settings

if [ -n "${ROUTER_IMAGE:-}" ] && [ -n "${ROUTER_BIN:-}" ]; then
  die "set ROUTER_BIN or ROUTER_IMAGE, not both"
fi
if [ -n "${ROUTER_IMAGE:-}" ]; then
  ROUTER_SOURCE=image
  ROUTER_BIN=
  command -v docker > /dev/null || die "ROUTER_IMAGE needs docker"
  docker info > /dev/null 2>&1 || die "cannot reach the docker daemon"
else
  ROUTER_SOURCE=binary
  ROUTER_IMAGE=
  bin="$(absolute "${ROUTER_BIN:-router}" 2>/dev/null || true)"
  if [ -z "$bin" ] || [ ! -x "$bin" ]; then
    die "no router: set ROUTER_BIN to an ironflock-router binary or ROUTER_IMAGE to its image"
  fi
  ROUTER_BIN="$bin"
  [ -n "${ROUTER_REF_CONFIG:-}" ] || die "set ROUTER_REF_CONFIG to the router's examples/config.yaml"
fi
if [ -n "${ROUTER_REF_CONFIG:-}" ]; then
  [ -f "$ROUTER_REF_CONFIG" ] || die "ROUTER_REF_CONFIG: no such file: $ROUTER_REF_CONFIG"
  ROUTER_REF_CONFIG="$(absolute_file "$ROUTER_REF_CONFIG")" || die "cannot resolve ROUTER_REF_CONFIG: $ROUTER_REF_CONFIG"
fi

case "${AUTH_MODE:=dynamic}" in
  dynamic) USERAPP_PROFILE=userapp_cra USERAPP_AUTH=1 ;;
  static) USERAPP_PROFILE=userapp_static USERAPP_AUTH=0 ;;
  *) die "AUTH_MODE must be dynamic or static" ;;
esac
case "${IDENTITY_AUTHORIZER_MODE:=enforce}" in
  enforce | shadow) ;;
  *) die "IDENTITY_AUTHORIZER_MODE must be enforce or shadow" ;;
esac
IRONFLOCK_LOG_LEVEL="${IRONFLOCK_LOG_LEVEL:-}"

: "${ROUTER_PUBLIC_PORT:=18082}" "${ROUTER_OPS_PORT:=18083}" "${ROUTER_INTERNAL_PORT:=18084}"
: "${FAKE_PLATFORM_S3_PORT:=18091}"
ports() { printf '%s\n' "$ROUTER_PUBLIC_PORT" "$ROUTER_OPS_PORT" "$ROUTER_INTERNAL_PORT" "$FAKE_PLATFORM_S3_PORT"; }
for port in $(ports); do
  case "$port" in '' | *[!0-9]*) die "not a port number: $port" ;; esac
done
if [ -n "$(ports | sort | uniq -d)" ]; then
  die "the four ports must differ: $(ports | paste -sd ' ' -)"
fi
CONTAINER_NAME=
if [ "$ROUTER_SOURCE" = image ]; then CONTAINER_NAME="ironflock-go-router-$ROUTER_PUBLIC_PORT"; fi

PY="$(absolute "${PYTHON:-python3}" 2>/dev/null || true)"
[ -n "$PY" ] || die "python not found: set PYTHON"
if ! "$PY" -I -c 'import sys; sys.exit(sys.version_info < (3, 11))' 2>/dev/null; then
  die "$PY is older than Python 3.11, which autobahn (and ironflock-py) need: set PYTHON"
fi
if ! "$PY" -I -c 'import autobahn.asyncio, msgpack' 2>/dev/null; then
  die "$PY cannot import autobahn with msgpack support: pip install ironflock (or set PYTHON)"
fi

resolve_state_dir create || die "cannot create the state directory $STATE_DIR"

# ------------------------------------------------- refuse to start twice

if (load_settings; router_running || platform_running); then
  die "already running (state in $STATE_DIR); run stop.sh first"
fi
for port in $(ports); do
  if port_open "$port"; then
    die "port $port is in use (ROUTER_PUBLIC_PORT, ROUTER_OPS_PORT, ROUTER_INTERNAL_PORT, FAKE_PLATFORM_S3_PORT pick others)"
  fi
done

# Leftovers of an earlier run in this state directory (nothing runs there).
if [ -f "$STATE_DIR/router.cid" ]; then
  docker rm --force "$(cat "$STATE_DIR/router.cid")" > /dev/null 2>&1 || true
fi
rm -f "$STATE_DIR/router.cid" "$STATE_DIR/router.pid" "$STATE_DIR/fake_platform.pid" "$STATE_DIR/harness.env"
: > "$STATE_DIR/router.log"
: > "$STATE_DIR/fake_platform.log"

# ------------------------------------------------------------ router config

if [ "$ROUTER_SOURCE" = image ]; then
  if ! docker image inspect "$ROUTER_IMAGE" > /dev/null 2>&1; then
    docker pull "$ROUTER_IMAGE" || die "cannot pull $ROUTER_IMAGE (logged in to its registry?)"
  fi
  if [ -z "${ROUTER_REF_CONFIG:-}" ]; then
    ROUTER_REF_CONFIG="$STATE_DIR/reference-config.yaml"
    cid="$(docker create "$ROUTER_IMAGE")" || die "cannot create a container of $ROUTER_IMAGE"
    copied=0
    if docker cp "$cid:$IMAGE_REF_CONFIG" "$ROUTER_REF_CONFIG" > /dev/null; then copied=1; fi
    docker rm "$cid" > /dev/null || true
    [ "$copied" = 1 ] || die "cannot copy $IMAGE_REF_CONFIG out of $ROUTER_IMAGE"
  fi
fi

# gen_config.py copies the production role blocks out of the reference config;
# --check (needs PyYAML) proves the copies parse to the reference roles.
check=
if "$PY" -I -c 'import yaml' 2>/dev/null; then
  check=--check
else
  note "PyYAML is not installed for $PY: skipping the role check (gen_config.py --check)"
fi
if ! ROUTER_REF_CONFIG="$ROUTER_REF_CONFIG" STATE_DIR="$STATE_DIR" "$PY" -I "$HERE/gen_config.py" ${check:+"$check"}; then
  die "gen_config.py failed"
fi
# The image runs as an unprivileged user that must read the mounted config.
chmod 644 "$STATE_DIR/config.yaml"

save_settings

# ------------------------------------------------------------------ launch

# On failure: show the logs, then stop whatever was started.
on_failure() {
  note "router log (tail):"
  router_log_tail 30 >&2 || true
  note "fake platform log (tail):"
  tail -n 30 "$STATE_DIR/fake_platform.log" >&2 2>/dev/null || true
  "$HERE/stop.sh" >&2 || true
}
trap on_failure EXIT

if [ "$ROUTER_SOURCE" = image ]; then
  router_create || die "cannot create the router container"
fi
router_launch || die "cannot start the router"
wait_router_healthy 30 || die "the router did not come up"

(
  cd "$STATE_DIR"
  FAKE_PLATFORM_ROUTER="ws://127.0.0.1:$ROUTER_INTERNAL_PORT/ws-svc" \
  FAKE_PLATFORM_AUTH_ROUTER="ws://127.0.0.1:$ROUTER_INTERNAL_PORT/ws-svc" \
  FAKE_PLATFORM_AUTH_REALM=ironflock.auth \
  FAKE_PLATFORM_USERAPP_AUTH="$USERAPP_AUTH" \
  FAKE_PLATFORM_S3_PORT="$FAKE_PLATFORM_S3_PORT" \
    nohup "$PY" -I "$INTEGRATION_DIR/fake_platform.py" >> "$STATE_DIR/fake_platform.log" 2>&1 < /dev/null &
  echo $! > "$STATE_DIR/fake_platform.pid"
)
# Ready once the own, provider and auth realm sessions have joined (the
# router's /readyz answers before auth.authorize is registered).
wait_platform_ready "$PLATFORM_SESSIONS" 30 || die "the fake platform did not come up"

trap - EXIT

if [ "$ROUTER_SOURCE" = image ]; then
  router="container $(container_id | cut -c1-12) of $ROUTER_IMAGE"
else
  router="pid $(cat "$STATE_DIR/router.pid"), $ROUTER_BIN"
fi
echo "router         ws://localhost:$ROUTER_PUBLIC_PORT/ws-ua-usr ($router)"
echo "               AUTH_MODE=$AUTH_MODE, identity authorizer $IDENTITY_AUTHORIZER_MODE, ops :$ROUTER_OPS_PORT"
echo "fake platform  pid $(cat "$STATE_DIR/fake_platform.pid"), services ws://127.0.0.1:$ROUTER_INTERNAL_PORT/ws-svc, S3 :$FAKE_PLATFORM_S3_PORT"
echo "state          $STATE_DIR"
