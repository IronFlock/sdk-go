#!/usr/bin/env bash
# Restarts only the router of a running harness: an outage that the fake
# platform (which keeps running) and the SDK under test have to ride out. For
# TestRouterRestartRecovery:
#
#   IRONFLOCK_TEST_RESTART_ROUTER=router/restart.sh
#
# (relative: go test runs the test in integration/, so the command needs no
# quoting whatever the checkout path; give it STATE_DIR when that is not the
# default, e.g. by exporting it).
#
# Stops the router, keeps it down for RESTART_DOWNTIME seconds (default 3),
# starts it again with the same config, and returns once it is healthy and the
# fake platform has rejoined all its realms. It acts on the instance recorded
# in STATE_DIR (default .run next to this script) and needs no other
# environment.
set -euo pipefail
# shellcheck source=lib.sh
. "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

DOWNTIME="${RESTART_DOWNTIME:-3}"
case "$DOWNTIME" in '' | *[!0-9.]*) die "RESTART_DOWNTIME must be a number of seconds" ;; esac

if ! resolve_state_dir existing || ! load_settings; then
  die "no harness in $STATE_DIR (run start.sh first)"
fi
router_running || die "the router is not running"
platform_running || die "the fake platform is not running"

joins="$(platform_ready_count)"
router_stop || die "cannot stop the router"
wait_port_closed "$ROUTER_PUBLIC_PORT" 15 || die "port $ROUTER_PUBLIC_PORT is still open after stopping the router"
echo "router stopped; starting it again in $DOWNTIME s"
sleep "$DOWNTIME"

router_launch || die "cannot start the router"
wait_router_healthy 30 || die "the router did not come back"
wait_platform_ready $((joins + PLATFORM_SESSIONS)) 60 || die "the fake platform did not rejoin"
echo "router restarted; the fake platform has rejoined"
