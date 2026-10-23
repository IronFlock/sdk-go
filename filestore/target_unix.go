//go:build unix

package filestore

import (
	"os"
	"path/filepath"
	"syscall"
)

// prepareTarget creates the parent directories of path and returns path as
// an absolute name whose directory part holds no symbolic link, "." or
// "..", reached as open(2) reaches path: a relative path from the working
// directory as the system knows it (not $PWD, which a shell sets to the
// path it entered the directory through), each ".." after the links before
// it. A path whose last element is "", "." or ".." names a directory
// (EISDIR); an empty path names nothing (ENOENT).
func prepareTarget(path string) (string, error) {
	if path == "" {
		return "", &os.PathError{Op: "open", Path: path, Err: syscall.ENOENT}
	}
	i := len(path)
	for i > 0 && !os.IsPathSeparator(path[i-1]) {
		i--
	}
	dir, name := path[:i], path[i:]
	if name == "" || name == "." || name == ".." {
		return "", &os.PathError{Op: "open", Path: path, Err: syscall.EISDIR}
	}
	if dir == "" {
		dir = "."
	} else if err := os.MkdirAll(dir, 0o777); err != nil { //nolint:gosec // G301: the caller's download directory, created like mkdir -p (umask applies)
		// MkdirAll takes dir as given, without cleaning it: the system
		// applies each ".." after the links before it.
		return "", err
	}
	dir, err := filepath.EvalSymlinks(dir)
	if err != nil {
		return "", err
	}
	if !filepath.IsAbs(dir) {
		// What is left of a relative directory is "." or leading ".."s, to
		// be taken from the physical working directory, which holds no
		// links: joining is exact.
		wd, err := syscall.Getwd()
		if err != nil {
			return "", err
		}
		dir = filepath.Join(wd, dir)
	}
	return filepath.Join(dir, name), nil
}
