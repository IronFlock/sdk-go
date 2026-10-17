# shellcheck shell=bash
# Shared by start.sh, stop.sh and restart.sh (sourced, not run).
#
# Portable to macOS (bash 3.2, BSD userland) and Linux: no /proc, no `timeout`,
# no GNU-only flags. Process identity is checked with `ps -p PID -o command=`.

# Constants and settings are used by the scripts that source this file.
# shellcheck disable=SC2034

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
INTEGRATION_DIR="$(cd "$HERE/.." && pwd -P)"

# The reference config inside the ironflock-router image (the router's
# examples/config.yaml, copied there by its Dockerfile), and where the
# generated config is mounted in the container.
IMAGE_REF_CONFIG=/etc/router/config.yaml
CONTAINER_CONFIG=/etc/router/harness.yaml

# The fake platform's sessions: own data realm, provider realm, auth realm.
PLATFORM_SESSIONS=3

# Generated config, logs, PIDs or the container ID, and harness.env: the
# settings start.sh used, so that stop.sh and restart.sh act on that instance
# whatever their own environment says.
STATE_DIR="${STATE_DIR:-$HERE/.run}"

die() { echo "${0##*/}: $*" >&2; exit 1; }
note() { echo "${0##*/}: $*" >&2; }

# resolve_state_dir create|existing: make STATE_DIR absolute, creating it
# first with "create"; fails if it does not exist.
resolve_state_dir() {
  if [ "$1" = create ]; then mkdir -p "$STATE_DIR" || return 1; fi
  [ -d "$STATE_DIR" ] || return 1
  STATE_DIR="$(cd "$STATE_DIR" && pwd -P)"
}

# absolute COMMAND: a command's path made absolute against the current
# directory; a bare command name is looked up in PATH.
absolute() {
  case "$1" in
    /*) printf '%s\n' "$1" ;;
    */*) printf '%s/%s\n' "$(cd "$(dirname "$1")" && pwd -P)" "$(basename "$1")" ;;
    *) command -v "$1" ;;
  esac
}

# absolute_file PATH: a file path made absolute against the current directory,
# whatever its shape (never a PATH lookup); fails if its directory does not
# exist.
absolute_file() {
  local dir
  case "$1" in
    /*) printf '%s\n' "$1" ;;
    *)
      dir="$(cd "$(dirname "$1")" && pwd -P)" || return 1
      printf '%s/%s\n' "$dir" "$(basename "$1")"
      ;;
  esac
}

# The settings recorded in harness.env.
SETTINGS="ROUTER_SOURCE ROUTER_BIN ROUTER_IMAGE CONTAINER_NAME ROUTER_PUBLIC_PORT ROUTER_OPS_PORT
ROUTER_INTERNAL_PORT FAKE_PLATFORM_S3_PORT AUTH_MODE USERAPP_PROFILE USERAPP_AUTH IDENTITY_AUTHORIZER_MODE
IRONFLOCK_LOG_LEVEL PY"

save_settings() {
  local v
  for v in $SETTINGS; do printf '%s=%q\n' "$v" "${!v-}"; done > "$STATE_DIR/harness.env.tmp"
  mv "$STATE_DIR/harness.env.tmp" "$STATE_DIR/harness.env"
}

# load_settings: the recorded settings of the instance in STATE_DIR. Without
# harness.env (nothing started there, or a state directory of an older version
# of these scripts) it infers the router source from the state files and fails.
load_settings() {
  if [ -f "$STATE_DIR/harness.env" ]; then
    # shellcheck source=/dev/null
    . "$STATE_DIR/harness.env"
    return 0
  fi
  if [ -f "$STATE_DIR/router.cid" ]; then ROUTER_SOURCE=image; else ROUTER_SOURCE=binary; fi
  return 1
}

# pid_running FILE PATTERN: the PID recorded in FILE is alive and its command
# line contains PATTERN, so a recycled PID is never mistaken for ours.
pid_running() {
  local pid cmd
  [ -f "$1" ] || return 1
  pid="$(cat "$1")"
  case "$pid" in '' | *[!0-9]*) return 1 ;; esac
  cmd="$(ps -p "$pid" -o command= 2>/dev/null)" || return 1
  case "$cmd" in *"$2"*) return 0 ;; esac
  return 1
}

# stop_pid FILE PATTERN: SIGTERM, then SIGKILL after 10 s; fails if the
# process survives both.
stop_pid() {
  local pid
  pid_running "$1" "$2" || return 0
  pid="$(cat "$1")"
  kill -TERM "$pid" 2>/dev/null || true
  for _ in $(seq 1 100); do pid_running "$1" "$2" || return 0; sleep 0.1; done
  kill -KILL "$pid" 2>/dev/null || true
  for _ in $(seq 1 20); do pid_running "$1" "$2" || return 0; sleep 0.1; done
  note "pid $pid ($2) did not exit"
  return 1
}

# container_id: the recorded container ID; fails if there is none.
container_id() {
  local id
  id="$(cat "$STATE_DIR/router.cid" 2>/dev/null)" && [ -n "$id" ] && echo "$id"
}

router_running() {
  case "$ROUTER_SOURCE" in
    image)
      local cid
      cid="$(container_id)" || return 1
      [ "$(docker inspect -f '{{.State.Running}}' "$cid" 2>/dev/null)" = true ]
      ;;
    *) pid_running "$STATE_DIR/router.pid" "$STATE_DIR/config.yaml" ;;
  esac
}

platform_running() { pid_running "$STATE_DIR/fake_platform.pid" fake_platform.py; }

# port_open PORT: something accepts TCP connections on 127.0.0.1:PORT.
port_open() { (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null; }

# router_create (image mode): create the router container and record its ID.
# Its ports are published on 127.0.0.1 only, under the same numbers inside and
# outside; the generated config is mounted read-only.
router_create() {
  docker create \
    --name "$CONTAINER_NAME" \
    --label "ironflock-go.harness.state-dir=$STATE_DIR" \
    --publish "127.0.0.1:$ROUTER_PUBLIC_PORT:$ROUTER_PUBLIC_PORT" \
    --publish "127.0.0.1:$ROUTER_OPS_PORT:$ROUTER_OPS_PORT" \
    --publish "127.0.0.1:$ROUTER_INTERNAL_PORT:$ROUTER_INTERNAL_PORT" \
    --mount "type=bind,source=$STATE_DIR/config.yaml,target=$CONTAINER_CONFIG,readonly" \
    --env "USERAPP_PROFILE=$USERAPP_PROFILE" \
    --env "IDENTITY_AUTHORIZER_MODE=$IDENTITY_AUTHORIZER_MODE" \
    --env "ROUTER_PUBLIC_PORT=$ROUTER_PUBLIC_PORT" \
    --env "ROUTER_OPS_PORT=$ROUTER_OPS_PORT" \
    --env "ROUTER_INTERNAL_PORT=$ROUTER_INTERNAL_PORT" \
    ${IRONFLOCK_LOG_LEVEL:+--env "IRONFLOCK_LOG_LEVEL=$IRONFLOCK_LOG_LEVEL"} \
    "$ROUTER_IMAGE" -c "$CONTAINER_CONFIG" > "$STATE_DIR/router.cid"
}

# router_launch: start the router process, or the container router_create
# made.
router_launch() {
  case "$ROUTER_SOURCE" in
    image) docker start "$(container_id)" > /dev/null ;;
    *)
      # The router interpolates ${VAR:-default} in its config from its
      # environment; the template takes ports, profile and authorizer mode
      # from these variables.
      env USERAPP_PROFILE="$USERAPP_PROFILE" \
        IDENTITY_AUTHORIZER_MODE="$IDENTITY_AUTHORIZER_MODE" \
        ROUTER_PUBLIC_PORT="$ROUTER_PUBLIC_PORT" \
        ROUTER_OPS_PORT="$ROUTER_OPS_PORT" \
        ROUTER_INTERNAL_PORT="$ROUTER_INTERNAL_PORT" \
        ${IRONFLOCK_LOG_LEVEL:+IRONFLOCK_LOG_LEVEL="$IRONFLOCK_LOG_LEVEL"} \
        nohup "$ROUTER_BIN" -c "$STATE_DIR/config.yaml" >> "$STATE_DIR/router.log" 2>&1 < /dev/null &
      echo $! > "$STATE_DIR/router.pid"
      ;;
  esac
}

# router_stop: stop the router process, or stop (not remove) the container.
router_stop() {
  case "$ROUTER_SOURCE" in
    image)
      local cid
      cid="$(container_id)" || return 0
      docker stop -t 10 "$cid" > /dev/null
      ;;
    *) stop_pid "$STATE_DIR/router.pid" "$STATE_DIR/config.yaml" ;;
  esac
}

# router_log_tail N: the router's last N log lines.
router_log_tail() {
  case "$ROUTER_SOURCE" in
    image) docker logs --tail "$1" "$(container_id)" 2>&1 ;;
    *) tail -n "$1" "$STATE_DIR/router.log" 2>/dev/null ;;
  esac
}

# wait_router_healthy SECONDS: until /healthz on the ops port answers 200 (only
# once the router routes calls); fails early if the router exits.
wait_router_healthy() {
  local deadline=$((SECONDS + $1))
  while :; do
    curl -fsS -o /dev/null --max-time 2 "http://127.0.0.1:$ROUTER_OPS_PORT/healthz" 2>/dev/null && return 0
    router_running || { note "the router exited"; return 1; }
    [ "$SECONDS" -lt "$deadline" ] || { note "the router is not healthy after $1 s"; return 1; }
    sleep 0.2
  done
}

# wait_port_closed PORT SECONDS
wait_port_closed() {
  local deadline=$((SECONDS + $2))
  while port_open "$1"; do
    [ "$SECONDS" -lt "$deadline" ] || return 1
    sleep 0.1
  done
}

# platform_ready_count: how many joins the fake platform has logged (each of
# its sessions logs "realm ready" on every join, rejoins included).
platform_ready_count() {
  local n
  n="$(grep -c 'realm ready' "$STATE_DIR/fake_platform.log" 2>/dev/null || true)"
  echo "${n:-0}"
}

# wait_platform_ready COUNT SECONDS: until the fake platform has logged COUNT
# joins; fails early if it exits.
wait_platform_ready() {
  local deadline=$((SECONDS + $2))
  while [ "$(platform_ready_count)" -lt "$1" ]; do
    platform_running || { note "the fake platform exited"; return 1; }
    [ "$SECONDS" -lt "$deadline" ] || { note "the fake platform has not joined all its realms after $2 s"; return 1; }
    sleep 0.2
  done
}
