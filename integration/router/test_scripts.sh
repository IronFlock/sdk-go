#!/usr/bin/env bash
# Checks the harness scripts' path handling without a router or Docker (CI
# runs it in the harness job):
#
#   integration/router/test_scripts.sh [HARNESS_DIR]
#
# HARNESS_DIR defaults to this script's directory (another copy of start.sh
# and lib.sh can be checked by naming it).
set -euo pipefail

HARNESS="$(cd "${1:-$(dirname "${BASH_SOURCE[0]}")}" && pwd -P)"
TMP="$(mktemp -d "${TMPDIR:-/tmp}/ironflock-go-scripts.XXXXXX")"
trap 'rm -rf -- "$TMP"' EXIT
TMP="$(cd "$TMP" && pwd -P)"
TRUE_BIN="$(command -v true)"
case "$TRUE_BIN" in /*) ;; *) TRUE_BIN=/usr/bin/true ;; esac
fails=0

fail() { echo "FAIL: $*" >&2; fails=$((fails + 1)); }

# start.sh must get past its ROUTER_REF_CONFIG handling for every shape of a
# relative file path and stop at the next check (AUTH_MODE=bogus), creating
# nothing; a missing file must be named.
mkdir -p "$TMP/work/sub"
for f in ref.yaml sub/ref.yaml env; do echo "roles: []" > "$TMP/work/$f"; done
for name in ref.yaml ./ref.yaml sub/ref.yaml env "$TMP/work/ref.yaml"; do
  out="$(cd "$TMP/work" && env -u ROUTER_IMAGE ROUTER_BIN="$TRUE_BIN" ROUTER_REF_CONFIG="$name" AUTH_MODE=bogus \
    STATE_DIR="$TMP/state" bash "$HARNESS/start.sh" 2>&1)" && rc=0 || rc=$?
  case "$out" in
    *"AUTH_MODE must be dynamic or static"*) ;;
    *) fail "ROUTER_REF_CONFIG=$name: start.sh stopped before AUTH_MODE (rc=$rc): ${out:-<no output>}" ;;
  esac
  [ ! -e "$TMP/state" ] || fail "ROUTER_REF_CONFIG=$name: start.sh created the state directory"
done
out="$(cd "$TMP/work" && env -u ROUTER_IMAGE ROUTER_BIN="$TRUE_BIN" ROUTER_REF_CONFIG=missing.yaml AUTH_MODE=bogus \
  STATE_DIR="$TMP/state" bash "$HARNESS/start.sh" 2>&1)" || true
case "$out" in
  *"no such file: missing.yaml"*) ;;
  *) fail "a missing ROUTER_REF_CONFIG is not named: ${out:-<no output>}" ;;
esac

# lib.sh: absolute_file never looks a file name up in PATH; absolute still does
# for commands.
got="$(cd "$TMP/work" && bash -c '. "$1/lib.sh" && absolute_file env && absolute_file sub/ref.yaml' _ "$HARNESS" 2>&1)" || true
want="$(printf '%s\n%s' "$TMP/work/env" "$TMP/work/sub/ref.yaml")"
[ "$got" = "$want" ] || fail "absolute_file: got '$got', want '$want'"
got="$(cd "$TMP/work" && bash -c '. "$1/lib.sh" && absolute bash' _ "$HARNESS" 2>&1)" || true
[ "$got" = "$(command -v bash)" ] || fail "absolute bash: got '$got', want the PATH lookup '$(command -v bash)'"

if [ "$fails" -gt 0 ]; then
  echo "test_scripts.sh: $fails failure(s)" >&2
  exit 1
fi
echo "test_scripts.sh: ok"
