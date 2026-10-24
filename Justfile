# Development tasks for ironflock-go.

# Platforms the SDK must build for (IronFlock devices are often 32-bit ARM).
targets := "linux/arm linux/arm64 linux/386 windows/amd64 darwin/arm64"
harness := "integration/router"
# Release contract (see REDeployments/docs/RELEASE.md): the module tag
# v<version> is the release; VERSION is the module's version in lockstep with
# ironflock-py and ironflock-js.
version := `tr -d '[:space:]' < VERSION`
module := "github.com/ironflock/sdk-go"
# Pinned linters and scanners, the same versions as the IronFlock CI harness.
golangci_lint_version := "v2.15.0"
govulncheck_version := "v1.9.0"
cyclonedx_gomod_version := "v1.12.0"

# Unit tests as CI runs them: race detector, shuffled order, no cache.
test:
	go test -race -shuffle=on -count=1 ./...

# Lint as CI runs it (non-mutating): gofmt, go.mod tidiness, vet, build,
# golangci-lint (.golangci.yml, gosec included).
lint:
	#!/usr/bin/env bash
	set -euo pipefail
	echo "==> gofmt"
	unformatted=$(gofmt -l $(git ls-files '*.go'))
	if [ -n "$unformatted" ]; then echo "unformatted (run: gofmt -w):"; echo "$unformatted"; exit 1; fi
	echo "==> go mod tidy (diff)"
	go mod tidy -diff
	echo "==> go vet"
	go vet -tags integration ./...
	echo "==> go build"
	go build ./...
	echo "==> golangci-lint"
	go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@{{golangci_lint_version}} run ./...
	echo "==> lint OK"

# Lint, then cross-build for the device platforms (CI: the gate's build jobs)
check: lint
	for target in {{targets}}; do GOOS=${target%/*} GOARCH=${target#*/} go build ./... || exit 1; done

# govulncheck as SARIF for CI (security.yml `sarif-recipe`). Duplicate
# result.stacks are removed (Code Scanning rejects them).
sarif:
	#!/usr/bin/env bash
	set -euo pipefail
	mkdir -p build/sarif
	go run golang.org/x/vuln/cmd/govulncheck@{{govulncheck_version}} -format sarif ./... > build/sarif/govulncheck.sarif
	jq '.runs |= map(.results |= map(if .stacks then .stacks |= unique else . end))' build/sarif/govulncheck.sarif > build/sarif/govulncheck.sarif.tmp
	mv build/sarif/govulncheck.sarif.tmp build/sarif/govulncheck.sarif
	echo "SARIF written to build/sarif/"

# CycloneDX SBOM of the Go module graph (security.yml `sbom-recipe`).
sbom:
	#!/usr/bin/env bash
	set -euo pipefail
	mkdir -p build/sbom
	go run github.com/CycloneDX/cyclonedx-gomod/cmd/cyclonedx-gomod@{{cyclonedx_gomod_version}} \
		mod -licenses -json -output build/sbom/sdk-go.cdx.json .
	echo "SBOM written to build/sbom/"

# Unit test coverage report
coverage:
	go test -count=1 -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out | tail -1

# Start the router harness (ROUTER_BIN and ROUTER_REF_CONFIG, or ROUTER_IMAGE; see integration/README.md)
router-up:
	{{harness}}/start.sh

# Build a router binary from a checkout (default ref HEAD), against the nexus fork commit this SDK pins
router-build src out="integration/router/.bin/router" ref="HEAD":
	#!/usr/bin/env bash
	set -euo pipefail
	# Build a copy of the ref against the nexus fork commit this SDK pins,
	# leaving the checkout untouched.
	nexus="$(go list -m -f '{{{{.Path}}@{{{{.Version}}' github.com/ironflock/nexus/v3)"
	out="$(mkdir -p "$(dirname "{{out}}")" && cd "$(dirname "{{out}}")" && pwd -P)/$(basename "{{out}}")"
	tmp="$(mktemp -d "${TMPDIR:-/tmp}/router-build.XXXXXX")"
	trap 'rm -rf -- "$tmp"' EXIT
	git -C "{{src}}" archive "{{ref}}" | tar -x -C "$tmp"
	cd "$tmp"
	if grep -q 'github.com/ironflock/nexus/v3' go.mod; then
		go get "$nexus"
	else
		# Ref predates the fork's module rename: it imports the upstream path and
		# replaces it with a local checkout. Point that replace at the last fork
		# commit that still declares the upstream path.
		nexus="github.com/ironflock/nexus/v3@v3.0.0-20261001140357-5a989b085bbb"
		go mod edit -replace="github.com/gammazero/nexus/v3=$nexus"
	fi
	go mod tidy
	# cmd/router since the router repo rename (2026-10-17), cmd/ironflock-router before.
	cmd=./cmd/router; [ -d cmd/router ] || cmd=./cmd/ironflock-router
	go build -o "$out" "$cmd"
	echo "built $out ($(git -C "{{src}}" describe --tags --always "{{ref}}"), nexus $nexus)"

# Offline checks of the integration harness: the fake platform against the real services' contract tables, and the scripts
test-harness:
	"${PYTHON:-python3}" -I integration/test_fake_platform.py
	integration/router/test_scripts.sh

# Stop the router harness
router-down:
	{{harness}}/stop.sh

# Integration tests: start the harness (ROUTER_BIN or ROUTER_IMAGE), run the Python reference and the Go suites, stop it
test-integration: test-harness
	#!/usr/bin/env bash
	set -euo pipefail
	harness="$PWD/{{harness}}"
	# Absolute, so that restart.sh (run from integration/ by the test, as
	# router/restart.sh) finds the instance.
	mkdir -p "${STATE_DIR:=$harness/.run}"
	STATE_DIR="$(cd "$STATE_DIR" && pwd -P)"
	export STATE_DIR
	py="${PYTHON:-python3}"
	case "$py" in
		/*) ;;
		*/*) py="$PWD/$py" ;;
		*) py="$(command -v "$py")" || { echo "${PYTHON:-python3} not found: set PYTHON" >&2; exit 1; } ;;
	esac
	url="ws://localhost:${ROUTER_PUBLIC_PORT:-18082}/ws-ua-usr"
	tmp="$(mktemp -d "${TMPDIR:-/tmp}/ironflock-go-integration.XXXXXX")"
	trap 'rm -rf -- "$tmp"' EXIT
	"$harness/start.sh"
	trap '"$harness/stop.sh"; rm -rf -- "$tmp"' EXIT
	# The Python SDK's run of the conformance scenario is the wire reference.
	cp -R "$harness/perapp_env" "$tmp/env"
	env DEVICE_SERIAL_NUMBER=06a0bf96-a539-4d6a-8471-ac7adc67616e SWARM_KEY=2 APP_KEY=26 ENV=DEV \
		DEVICE_KEY=42 APP_NAME=interop DEVICE_NAME=interop-dev \
		IRONFLOCK_ENV_DIR="$tmp/env" IRONFLOCK_TEST_PLATFORM_URL="$url" \
		"$py" -I integration/py_scenario.py "$tmp/py_reference.json"
	IRONFLOCK_TEST_ROUTER_URL="$url" go test -race -count=1 -run TestRealRouter -v ./wamp/
	IRONFLOCK_TEST_PLATFORM_URL="$url" \
	IRONFLOCK_TEST_ENV_DIR="$harness/perapp_env" \
	IRONFLOCK_TEST_REFERENCE="$tmp/py_reference.json" \
	IRONFLOCK_TEST_PYTHON="$py" \
	IRONFLOCK_TEST_RESTART_ROUTER=router/restart.sh \
		go test -tags integration -race -count=1 -v ./integration/...

# ------------------------------------------------------------------ release --
#   just release-patch = _require-clean -> bump-patch -> _release-pr -> _publish
#
# _release-pr opens a pull request with the version bump, GitHub merges it once
# ci-ok and security / report are green, and the merged commit is tagged
# v<version>: that tag is the Go module release. _publish creates the GitHub
# release and checks that the Go module proxy serves the version. Every step
# runs as its own `just` process, so each reads the bumped VERSION.

# Releases come off a clean tree: the release commit carries the version bump
# and nothing else.
_require-clean:
	#!/usr/bin/env bash
	set -euo pipefail
	dirty=$(git status --porcelain 2>/dev/null || true)
	if [ -n "$dirty" ]; then
		echo "release: uncommitted changes here — commit them first." >&2
		printf '%s\n' "$dirty" >&2
		exit 1
	fi

bump-patch:
	#!/usr/bin/env bash
	set -euo pipefail
	IFS=. read -r major minor patch <<< "{{version}}"
	printf '%s.%s.%s\n' "$major" "$minor" "$((patch + 1))" > VERSION
	echo "version is now $(cat VERSION)"

bump-minor:
	#!/usr/bin/env bash
	set -euo pipefail
	IFS=. read -r major minor patch <<< "{{version}}"
	printf '%s.%s.%s\n' "$major" "$((minor + 1))" 0 > VERSION
	echo "version is now $(cat VERSION)"

bump-major:
	#!/usr/bin/env bash
	set -euo pipefail
	echo "a major version needs the module path /v2 (go.mod and every import): not a bump" >&2
	exit 1

# Version bump through a pull request (main is protected): branch, commit only
# VERSION, open the PR, merge it automatically once the required checks pass,
# then tag the merged commit on main and push the tag.
_release-pr:
	#!/usr/bin/env bash
	set -euo pipefail
	tag="v{{version}}"
	branch="release/${tag}"
	title="release sdk-go ${tag}"
	git fetch --quiet origin main
	if [ "$(git rev-parse --abbrev-ref HEAD)" != main ] || [ "$(git rev-parse HEAD)" != "$(git rev-parse origin/main)" ]; then
		echo "release: run from an up-to-date main (git switch main && git pull --ff-only)" >&2
		git checkout -- VERSION
		exit 1
	fi
	git switch -c "$branch"
	git add -- VERSION
	git commit -m "$title"
	git push -u origin "$branch"
	gh pr create --base main --head "$branch" --title "$title" \
		--body "Version bump only, opened by \`just release-${RELEASE_KIND:-patch}\`. Merged automatically once the required checks pass; the tag on the merged commit is the module release."
	gh pr merge "$branch" --squash --auto --delete-branch
	echo "==> waiting for the required checks and the merge of ${branch}"
	sha=""
	for _ in $(seq 1 120); do
		read -r state sha <<<"$(gh pr view "$branch" --json state,mergeCommit --jq '"\(.state) \(.mergeCommit.oid // "")"')"
		case "$state" in
			MERGED) break ;;
			CLOSED) echo "release PR ${branch} was closed without merging" >&2; exit 1 ;;
		esac
		sleep 20
	done
	[ "${state:-}" = MERGED ] && [ -n "$sha" ] || { echo "release PR ${branch} not merged after 40 min: gh pr checks ${branch}" >&2; exit 1; }
	git switch main
	git pull --ff-only
	git branch -D "$branch" 2>/dev/null || true
	git tag -a "$tag" -m "$title" "$sha"
	git push origin "$tag"
	echo "==> tagged ${tag} on ${sha}"

# GitHub release for the tag, then the module proxy must serve the version.
_publish:
	#!/usr/bin/env bash
	set -euo pipefail
	tag="v$(tr -d '[:space:]' < VERSION)"
	gh release view "$tag" >/dev/null 2>&1 || gh release create "$tag" --verify-tag --title "$tag" --generate-notes
	for _ in $(seq 1 20); do
		got=$(GOPROXY=https://proxy.golang.org GOFLAGS=-mod=mod go list -m -f '{{{{.Version}}' "{{module}}@${tag}" 2>/dev/null || true)
		[ "$got" = "$tag" ] && { echo "==> {{module}}@${tag} on proxy.golang.org"; exit 0; }
		sleep 15
	done
	echo "proxy.golang.org does not serve {{module}}@${tag} yet" >&2
	exit 1

release-patch:
	just _require-clean
	just bump-patch
	just _release-pr
	just _publish

release-minor:
	just _require-clean
	just bump-minor
	RELEASE_KIND=minor just _release-pr
	just _publish
