//go:build integration

// Package integration holds end-to-end tests against the router and a
// fake IronFlock platform (see README.md in this directory). They are
// excluded from normal builds; run them with
//
//	IRONFLOCK_TEST_PLATFORM_URL=ws://localhost:18082/ws-ua-usr go test -tags integration ./integration/...
package integration

import (
	"os"
	"path/filepath"
	"testing"
)

// Device identity the fake platform admits: the device (key 42) of app 26
// in swarm 2, whose legacy credential is (serial, serial) (see
// fake_platform.py).
const (
	testSerial   = "06a0bf96-a539-4d6a-8471-ac7adc67616e"
	testSwarmKey = "2"
	testAppKey   = "26"
	testDeviceID = "42"
)

// platformURL returns the router URL of the fake platform, skipping the test
// when it is not configured.
func platformURL(t *testing.T) string {
	t.Helper()
	url := os.Getenv("IRONFLOCK_TEST_PLATFORM_URL")
	if url == "" {
		t.Skip("IRONFLOCK_TEST_PLATFORM_URL not set; see integration/README.md")
	}
	return url
}

// deviceEnv sets the environment the device agent would inject.
func deviceEnv(t *testing.T) {
	t.Helper()
	t.Setenv("DEVICE_SERIAL_NUMBER", testSerial)
	t.Setenv("SWARM_KEY", testSwarmKey)
	t.Setenv("APP_KEY", testAppKey)
	t.Setenv("ENV", "DEV")
	t.Setenv("DEVICE_KEY", testDeviceID)
	t.Setenv("APP_NAME", "interop")
	t.Setenv("DEVICE_NAME", "interop-dev")
	t.Setenv("DEVICE_ENDPOINT_URL", "")
	t.Setenv("RESWARM_URL", "")
	t.Setenv("APP_AUTH_ID", "")
	t.Setenv("APP_AUTH_SECRET", "")
	t.Setenv("IRONFLOCK_ENV_DIR", envDir(t))
}

// envDir returns a fresh /data/env stand-in. With IRONFLOCK_TEST_ENV_DIR set
// (e.g. a per-app credential: APP_AUTH_ID.txt, APP_AUTH_SECRET.txt) its files
// are copied in, so a test may rewrite them without touching the original.
func envDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	src := os.Getenv("IRONFLOCK_TEST_ENV_DIR")
	if src == "" {
		return dir
	}
	entries, err := os.ReadDir(src)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		data, err := os.ReadFile(filepath.Join(src, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, e.Name()), data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}
