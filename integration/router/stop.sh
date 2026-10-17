#!/usr/bin/env bash
# Stops what start.sh started: the fake platform, then the router process or
# container. A recorded PID is signalled only while its command line still
# matches what start.sh launched. In image mode the container's log is saved
# to router.log before the container is removed. Logs and the generated config
# stay in STATE_DIR (default .run next to this script).
#
# Exits non-zero only if something it found running could not be stopped.
set -uo pipefail
# shellcheck source=lib.sh
. "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

resolve_state_dir existing || exit 0 # nothing was ever started there
load_settings || true
rc=0

# stop_recorded NAME PIDFILE PATTERN
stop_recorded() {
  local pid
  if pid_running "$2" "$3"; then
    pid="$(cat "$2")"
    stop_pid "$2" "$3" || return 1
    echo "stopped the $1 (pid $pid)"
  fi
  rm -f "$2"
}

stop_recorded "fake platform" "$STATE_DIR/fake_platform.pid" fake_platform.py || rc=1

case "$ROUTER_SOURCE" in
  image)
    if cid="$(container_id)"; then
      if ! docker info > /dev/null 2>&1; then
        note "cannot reach the docker daemon: the router container $cid is left alone"
        rc=1
      elif docker inspect "$cid" > /dev/null 2>&1; then
        docker stop -t 10 "$cid" > /dev/null
        docker logs "$cid" > "$STATE_DIR/router.log" 2>&1
        if docker rm "$cid" > /dev/null; then
          echo "removed the router container ${cid:0:12} (its log is in $STATE_DIR/router.log)"
          rm -f "$STATE_DIR/router.cid"
        else
          rc=1
        fi
      else
        rm -f "$STATE_DIR/router.cid" # already gone
      fi
    fi
    ;;
  *) stop_recorded router "$STATE_DIR/router.pid" "$STATE_DIR/config.yaml" || rc=1 ;;
esac

# Keep the settings while anything is left, so that stop.sh can be retried.
if [ "$rc" = 0 ]; then rm -f "$STATE_DIR/harness.env"; fi
exit "$rc"
