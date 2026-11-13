package env

import (
	"bytes"
	"errors"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// mirror points IRONFLOCK_ENV_DIR at a fresh directory holding files (name
// to content, without .txt), and returns it.
func mirror(t *testing.T, files map[string]string) string {
	t.Helper()
	dir := t.TempDir()
	for name, content := range files {
		if err := os.WriteFile(filepath.Join(dir, name+".txt"), []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv("IRONFLOCK_ENV_DIR", dir)
	return dir
}

// skipUnlessPermissionsApply skips the test where file permissions do not
// keep a process out: as root, and on Windows.
func skipUnlessPermissionsApply(t *testing.T) {
	t.Helper()
	if runtime.GOOS == "windows" || os.Geteuid() == 0 {
		t.Skip("file permissions do not apply to this process")
	}
}

func TestLookup(t *testing.T) {
	t.Setenv("MY_PORT", "30012")
	mirror(t, map[string]string{"MY_PORT": " 30099\n"})
	if v, err := Lookup("MY_PORT"); v != "30099" || err != nil {
		t.Errorf("the file must win: %q, %v", v, err)
	}
	mirror(t, map[string]string{"MY_PORT": " \n"})
	if v, err := Lookup("MY_PORT"); v != "30012" || err != nil {
		t.Errorf("an empty file counts as absent: %q, %v", v, err)
	}
	mirror(t, nil)
	if v, err := Lookup("MY_PORT"); v != "30012" || err != nil {
		t.Errorf("no file: %q, %v", v, err)
	}
	t.Setenv("IRONFLOCK_ENV_DIR", filepath.Join(t.TempDir(), "missing"))
	if v, err := Lookup("MY_PORT"); v != "30012" || err != nil {
		t.Errorf("no mirror: %q, %v", v, err)
	}
	notDir := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(notDir, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("IRONFLOCK_ENV_DIR", notDir)
	if v, err := Lookup("MY_PORT"); v != "30012" || err != nil {
		t.Errorf("a mirror path that is a file: %q, %v", v, err)
	}
	if v := ReadInjected("MY_PORT"); v != "30012" {
		t.Errorf("ReadInjected = %q", v)
	}
}

// A mirror the app cannot read — older agents write it readable by root only —
// is reported, and the start-time environment is used.
func TestLookupReportsAnUnreadableMirror(t *testing.T) {
	skipUnlessPermissionsApply(t)
	t.Setenv("APP_AUTH_SECRET", "start-time-secret")

	dir := mirror(t, map[string]string{"APP_AUTH_SECRET": "rotated-secret"})
	if err := os.Chmod(filepath.Join(dir, "APP_AUTH_SECRET.txt"), 0); err != nil {
		t.Fatal(err)
	}
	v, err := Lookup("APP_AUTH_SECRET")
	if v != "start-time-secret" || !errors.Is(err, fs.ErrPermission) {
		t.Errorf("unreadable file: %q, %v", v, err)
	}
	if v := ReadInjected("APP_AUTH_SECRET"); v != "start-time-secret" {
		t.Errorf("ReadInjected = %q", v)
	}

	dir = mirror(t, map[string]string{"APP_AUTH_SECRET": "rotated-secret"})
	if err := os.Chmod(dir, 0); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })
	v, err = Lookup("APP_AUTH_SECRET")
	if v != "start-time-secret" || !errors.Is(err, fs.ErrPermission) {
		t.Errorf("unreadable directory: %q, %v", v, err)
	}
	if _, err := Lookup("NOT_MIRRORED"); !errors.Is(err, fs.ErrPermission) {
		t.Errorf("a name the directory hides is reported too: %v", err)
	}
}

func TestWarnUnreadableLogsOncePerProcess(t *testing.T) {
	ResetWarning()
	t.Cleanup(ResetWarning)
	t.Setenv("IRONFLOCK_ENV_DIR", "/data/env")
	var buf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&buf, nil))

	WarnUnreadable(log, nil)
	if buf.Len() != 0 {
		t.Fatalf("a nil error was logged: %s", buf.String())
	}
	cause := &fs.PathError{Op: "open", Path: "/data/env/APP_AUTH_SECRET.txt", Err: fs.ErrPermission}
	WarnUnreadable(log, cause)
	WarnUnreadable(log, cause)
	WarnUnreadable(slog.New(slog.NewTextHandler(&buf, nil)), cause)
	out := buf.String()
	if n := strings.Count(out, "level=WARN"); n != 1 {
		t.Fatalf("%d warnings, want 1:\n%s", n, out)
	}
	for _, part := range []string{"/data/env/APP_AUTH_SECRET.txt: permission denied",
		"using the environment the container started with", "run the app's container as root"} {
		if !strings.Contains(out, part) {
			t.Errorf("warning %q lacks %q", out, part)
		}
	}
}
