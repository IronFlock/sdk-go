package wamp

import (
	"bytes"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/ironflock/sdk-go/internal/env"
)

// The device agent writes /data/env readable by root only. An app container
// running as another user cannot read the per-app credential there: it gets
// the start-time environment, and the process says so once (finding 28 of
// the round-3 review).
func TestAppCredentialsWarnsOnceWhenTheMirrorIsUnreadable(t *testing.T) {
	if runtime.GOOS == "windows" || os.Geteuid() == 0 {
		t.Skip("file permissions do not apply to this process")
	}
	dir := t.TempDir()
	for name, v := range map[string]string{"APP_AUTH_ID": "app-26-dev-e2@sn", "APP_AUTH_SECRET": "rotated"} {
		if err := os.WriteFile(filepath.Join(dir, name+".txt"), []byte(v), 0); err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv("IRONFLOCK_ENV_DIR", dir)
	t.Setenv("APP_AUTH_ID", "app-26-dev-e1@sn")
	t.Setenv("APP_AUTH_SECRET", "start-time")

	env.ResetWarning()
	t.Cleanup(env.ResetWarning)
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })

	for range 3 { // every connection attempt resolves the credential again
		if id, secret := AppCredentials("sn"); id != "app-26-dev-e1@sn" || secret != "start-time" {
			t.Fatalf("AppCredentials = %q, %q; want the start-time environment", id, secret)
		}
	}
	out := buf.String()
	if n := strings.Count(out, "level=WARN"); n != 1 {
		t.Fatalf("%d warnings, want 1:\n%s", n, out)
	}
	if !strings.Contains(out, "APP_AUTH_ID.txt: permission denied") && !strings.Contains(out, "APP_AUTH_SECRET.txt: permission denied") {
		t.Errorf("the warning does not name the file: %s", out)
	}
}
