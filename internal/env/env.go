// Package env reads the values the IronFlock device agent injects into an app
// container.
package env

import (
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
)

// DefaultDir is where the device agent mirrors injected values, one file per
// name. Overridable with the IRONFLOCK_ENV_DIR environment variable.
const DefaultDir = "/data/env"

// markerName names a file the device agent writes into its mirror when it
// creates the app's container, and never removes: a readable one tells the
// agent's mirror from a directory of the app's own (or none).
const markerName = "DEVICE_SERIAL_NUMBER"

// Dir returns the directory holding the agent-maintained value files.
func Dir() string {
	if dir, ok := os.LookupEnv("IRONFLOCK_ENV_DIR"); ok {
		return dir
	}
	return DefaultDir
}

// ReadInjected returns a platform-injected value by name: the live env FILE
// first, the environment variable second.
//
// The device agent mirrors injected values to /data/env/{name}.txt (a bind
// mount) — the only channel that reaches a RUNNING container, so a rotated
// credential or a tunnel port allocated after start is picked up without a
// restart. Environment variables are the start-time snapshot and serve as the
// fallback. An empty (or whitespace-only) file counts as absent, and so does
// a file that cannot be read (see Lookup).
func ReadInjected(name string) string {
	value, _ := Lookup(name)
	return value
}

// Lookup is ReadInjected that also reports a file that exists but cannot be
// read: err is then the read error (permission denied, typically), and value
// the environment variable. A missing file is not an error.
//
// Older agents write /data/env readable by root only (a 0700 directory of
// 0600 files), so an app container there that runs as another user cannot
// read it and sees only the values it started with.
func Lookup(name string) (value string, err error) {
	value, ok, err := readFile(name)
	if ok && value != "" {
		return value, nil
	}
	return os.Getenv(name), err
}

// LookupLive returns a value the device agent removes from its mirror when
// it no longer applies, such as an instance's cloud tunnel port
// (<name>_CLOUD). A file for name is the value, also when it is empty.
// Without one, the agent's mirror decides: while it is present — its
// DEVICE_SERIAL_NUMBER.txt is readable — there is no value (the agent
// removed it), and the environment variable is not consulted, as it still
// holds the value the container started with. Without the mirror (outside
// the device agent, or an app's own /data/env) the value is the environment
// variable. err reports a file that exists but cannot be read; value is then
// the environment variable.
func LookupLive(name string) (value string, err error) {
	value, ok, err := readFile(name)
	switch {
	case ok:
		return value, nil // the agent's word, also when it is empty
	case err != nil:
		return os.Getenv(name), err
	}
	_, mirrored, err := readFile(markerName)
	if mirrored {
		return "", nil // the agent removed it
	}
	return os.Getenv(name), err
}

// readFile reads the mirror file of name, trimmed. ok is false when there is
// no such file, or when it cannot be read (err).
func readFile(name string) (value string, ok bool, err error) {
	data, err := os.ReadFile(filepath.Join(Dir(), name+".txt"))
	switch {
	case err == nil:
		return strings.TrimSpace(string(data)), true, nil
	case absent(err):
		return "", false, nil
	}
	return "", false, err
}

// absent reports whether err, the error of reading a mirror file, means
// that the file is not there: it does not exist, or the mirror directory is
// not a directory.
func absent(err error) bool {
	return errors.Is(err, fs.ErrNotExist) || errors.Is(err, syscall.ENOTDIR)
}

// warned is set once WarnUnreadable has logged.
var warned atomic.Bool

// WarnUnreadable logs err, a failure to read the device agent's mirror (as
// Lookup and LookupLive report it), as a warning to log (slog.Default() when
// nil) — once per process: the mirror's permissions do not change while the
// app runs, and its values are read again and again. A nil err logs
// nothing.
func WarnUnreadable(log *slog.Logger, err error) {
	if err == nil || !warned.CompareAndSwap(false, true) {
		return
	}
	if log == nil {
		log = slog.Default()
	}
	dir := Dir()
	log.Warn(fmt.Sprintf("Cannot read the values the IronFlock device agent keeps up to date in %s (%v); "+
		"using the environment the container started with instead. Values the agent changes while the app "+
		"runs (a rotated app credential, an instance's cloud tunnel ports) will not reach this app. Older agents "+
		"make %s readable by root only: update the device agent, or run the app's container as root.",
		dir, err, dir))
}

// ResetWarning makes the next WarnUnreadable log again. For tests.
func ResetWarning() { warned.Store(false) }
