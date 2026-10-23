package filestore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"unicode/utf8"
)

// Direct (presigned) transfers: the bytes go straight to the object store
// over HTTP(S); the router carries only the small call that mints the URL.

// errorBodyChars is how much of an object store error response is read
// (in characters) to classify it and to quote it in CodeInternal reasons.
const errorBodyChars = 400

// drainLimit bounds how much of an unread response body is discarded so the
// connection can be reused; larger remainders close the connection instead.
const drainLimit = 64 << 10

// defaultHTTPClient returns the client used when New is given none: a
// private copy of http.DefaultTransport — proxies from the environment
// (HTTP_PROXY, HTTPS_PROXY, NO_PROXY), system TLS roots with verification —
// and no overall timeout, since a multi-gigabyte transfer may legitimately
// take hours. Transfers are bounded by the caller's context instead.
func defaultHTTPClient() *http.Client {
	var tr *http.Transport
	if dt, ok := http.DefaultTransport.(*http.Transport); ok {
		tr = dt.Clone()
	} else {
		tr = &http.Transport{ForceAttemptHTTP2: true}
	}
	tr.Proxy = http.ProxyFromEnvironment
	return &http.Client{Transport: tr}
}

// upload PUTs size bytes to a presigned target with exactly the minted
// headers and an explicit Content-Length (object stores reject chunked
// presigned uploads). The minted method is ignored, as in the Python and
// JavaScript SDKs: a presigned upload is always a PUT. Any 2xx status is
// success.
//
// open returns the body positioned at its start. When rewindable is true it
// may be called again, which lets the transport retry on a stale pooled
// connection and follow a 307/308 redirect; otherwise it is called once.
// Only the first size bytes of the body are sent, and a body that ends
// early fails the upload.
func (s *FileStore) upload(ctx context.Context, t *UploadTarget, size int64, open func() io.Reader, rewindable bool) error {
	if t.URL == "" {
		return newError(CodeInternal, "the service returned no upload URL")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, t.URL, nil)
	if err != nil {
		// Not quoted: a presigned URL is a bearer credential.
		return wrapError(CodeInternal, "the service returned an invalid upload URL", stripURL(err))
	}
	state := &uploadState{}
	newBody := func() io.ReadCloser {
		if size == 0 {
			return http.NoBody
		}
		return io.NopCloser(&sizedReader{r: open(), remain: size, size: size, state: state})
	}
	req.Body = newBody()
	req.ContentLength = size
	if rewindable {
		req.GetBody = func() (io.ReadCloser, error) { return newBody(), nil }
	}
	for name, value := range t.Headers {
		if strings.EqualFold(name, "Host") {
			req.Host = value
			continue
		}
		req.Header.Set(name, value)
	}

	resp, err := s.http.Do(req)
	// The transport may hold on to a request body after Do returns; the
	// source belongs to the caller again once this returns.
	state.done.Store(true)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		if berr := state.err.get(); berr != nil {
			return fmt.Errorf("filestore: read upload body: %w", berr)
		}
		return unreachable(err)
	}
	defer drainClose(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return httpError(resp)
	}
	return nil
}

// download GETs a presigned URL and returns the response body, which the
// caller must close. Failures are classified by httpError; read errors on
// the returned body are reported by readError.
func (s *FileStore) download(ctx context.Context, rawURL string) (io.ReadCloser, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, wrapError(CodeInternal, "the service returned an invalid download URL", stripURL(err))
	}
	// The stored bytes, verbatim: left to itself the transport would ask for
	// gzip and transparently decompress an object stored with
	// Content-Encoding: gzip.
	req.Header.Set("Accept-Encoding", "identity")
	resp, err := s.http.Do(req)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		return nil, unreachable(err)
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		defer drainClose(resp.Body)
		return nil, httpError(resp)
	}
	return resp.Body, nil
}

// readError reports a failure while reading a download body: the context's
// error when it ended the transfer, otherwise the read error, wrapped.
func readError(ctx context.Context, err error) error {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	return fmt.Errorf("filestore: download from the object store interrupted: %w", err)
}

// copyDownload streams a download body to w, telling read failures (see
// readError) from write failures.
func copyDownload(ctx context.Context, w io.Writer, body io.Reader) error {
	tr := &trackingReader{r: body}
	if _, err := io.Copy(w, tr); err != nil {
		if tr.err != nil {
			return readError(ctx, tr.err)
		}
		return fmt.Errorf("filestore: write object: %w", err)
	}
	return nil
}

// unreachable reports that the object store could not be reached at all
// (DNS, connection refused, TLS, proxy). The URL is stripped from the
// message and the cause: a presigned URL is a bearer credential.
func unreachable(err error) *Error {
	err = stripURL(err)
	return wrapError(CodePresignUnreachable,
		fmt.Sprintf("could not reach the object store directly (%v); a proxy may allow only the router", err), err)
}

// stripURL removes the *url.Error layer, which quotes the request URL.
func stripURL(err error) error {
	var ue *url.Error
	if errors.As(err, &ue) && ue.Err != nil {
		return ue.Err
	}
	return err
}

// httpError classifies an object store error response, in this order:
//
//   - a body mentioning RequestTimeTooSkewed (any status): CodeClockSkew —
//     SigV4 rejects requests more than 15 minutes out of step, which on a
//     device without NTP would otherwise read as a credential problem;
//   - 401 or 403: CodeNotAuthorized;
//   - 404: CodeNoSuchObject;
//   - anything else: CodeInternal, quoting the first 400 characters of the
//     body.
func httpError(resp *http.Response) *Error {
	// 4 bytes per character is enough for 400 characters of any UTF-8 text.
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4*errorBodyChars))
	body := truncateChars(strings.ToValidUTF8(string(raw), "�"), errorBodyChars)
	switch {
	case strings.Contains(body, "RequestTimeTooSkewed"):
		return newError(CodeClockSkew,
			"the device clock is too far out of step for the object store to accept the request; check NTP")
	case resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden:
		return newError(CodeNotAuthorized, fmt.Sprintf("the object store rejected the signed URL (HTTP %d)", resp.StatusCode))
	case resp.StatusCode == http.StatusNotFound:
		return newError(CodeNoSuchObject, "the object store has no such object")
	}
	return newError(CodeInternal, fmt.Sprintf("object store returned HTTP %d: %s", resp.StatusCode, body))
}

// truncateChars returns the first n characters (runes) of s.
func truncateChars(s string, n int) string {
	if utf8.RuneCountInString(s) <= n {
		return s
	}
	i := 0
	for range n {
		_, size := utf8.DecodeRuneInString(s[i:])
		i += size
	}
	return s[:i]
}

// drainClose discards a bounded remainder of body, so the connection can be
// reused, and closes it.
func drainClose(body io.ReadCloser) {
	_, _ = io.Copy(io.Discard, io.LimitReader(body, drainLimit))
	_ = body.Close()
}

// closeQuietly closes something that was only read from: a response body, a
// file opened for reading. Nothing written can be lost, so the error of
// Close carries no information.
func closeQuietly(c io.Closer) { _ = c.Close() }

// firstError records the first error reported to it. It is safe for
// concurrent use: the transport may still read a request body after Do
// returns.
type firstError struct {
	mu  sync.Mutex
	err error
}

func (f *firstError) set(err error) {
	f.mu.Lock()
	if f.err == nil {
		f.err = err
	}
	f.mu.Unlock()
}

func (f *firstError) get() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.err
}

// errUploadFinished is returned to a transport that reads a request body
// after the upload has returned.
var errUploadFinished = errors.New("filestore: upload already finished")

// uploadState is shared by every body reader of one upload.
type uploadState struct {
	// done is set once the upload has returned: the source is not read
	// after that (a read already in progress may still complete).
	done atomic.Bool
	// err is the first error reading the source.
	err firstError
}

// sizedReader yields exactly size bytes of r: it stops after size bytes and
// fails with io.ErrUnexpectedEOF when r ends early. Read errors are recorded
// in the upload state, so the upload can report them rather than a network
// failure.
type sizedReader struct {
	r      io.Reader
	remain int64
	size   int64
	state  *uploadState
}

func (b *sizedReader) Read(p []byte) (int, error) {
	if b.state.done.Load() {
		return 0, errUploadFinished
	}
	if b.remain <= 0 {
		return 0, io.EOF
	}
	if int64(len(p)) > b.remain {
		p = p[:b.remain]
	}
	n, err := b.r.Read(p)
	b.remain -= int64(n)
	switch {
	case err == io.EOF && b.remain > 0:
		err = fmt.Errorf("body ended after %d of %d bytes: %w", b.size-b.remain, b.size, io.ErrUnexpectedEOF)
		b.state.err.set(err)
	case err == io.EOF:
		// Exactly size bytes: a clean end.
	case err != nil:
		b.state.err.set(err)
	}
	return n, err
}

// trackingReader records the error its reader returned, telling read
// failures from write failures in io.Copy.
type trackingReader struct {
	r   io.Reader
	err error
}

func (t *trackingReader) Read(p []byte) (int, error) {
	n, err := t.r.Read(p)
	if err != nil && err != io.EOF {
		t.err = err
	}
	return n, err
}

// readExactly reads exactly size bytes from r; a reader that ends early
// fails with io.ErrUnexpectedEOF.
func readExactly(r io.Reader, size int64) ([]byte, error) {
	const preallocMax = 64 << 20
	var (
		data []byte
		n    int64
		err  error
	)
	if size <= preallocMax {
		data = make([]byte, size)
		var m int
		m, err = io.ReadFull(r, data)
		n = int64(m)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			err = nil // reported as a short read below
		}
	} else {
		// No cap known on the inline path: grow with the data rather than
		// trusting size for one huge allocation.
		var buf bytes.Buffer
		n, err = buf.ReadFrom(io.LimitReader(r, size))
		data = buf.Bytes()
	}
	if err != nil {
		return nil, fmt.Errorf("filestore: read object body: %w", err)
	}
	if n < size {
		return nil, fmt.Errorf("filestore: read object body: ended after %d of %d bytes: %w", n, size, io.ErrUnexpectedEOF)
	}
	return data, nil
}

// renameFile is os.Rename; tests replace it to simulate a file that cannot
// be replaced.
var renameFile = os.Rename

// openStaging is os.OpenFile, as createTemp opens a staging file; tests
// replace it to observe the file.
var openStaging = os.OpenFile

// maxLinks bounds the chain of symbolic links resolveLink follows, as
// filepath.EvalSymlinks bounds it.
const maxLinks = 255

// stagingBaseMax is how many bytes of the target's name a staging file's
// name keeps. The staging name adds up to 19 bytes, and file systems cap a
// name at 255 bytes, so it fits wherever the target's own name does.
const stagingBaseMax = 200

// writeFileAtomic writes the file at path through fill, as
// FileStore.GetToFile describes: the parent directories of path are
// created, and path is reached as open(2) reaches it (see prepareTarget),
// following symbolic links. A regular file, or one that does not exist yet,
// is staged and renamed into place by replaceFile, so a failing fill leaves
// it as it was. Anything else (a FIFO, a device) cannot be replaced by a
// rename and is written in place.
func writeFileAtomic(path string, fill func(w io.Writer) error) error {
	abs, err := prepareTarget(path)
	if err != nil {
		return err
	}
	fi, err := os.Stat(abs) // follows the links, as open(2) does
	switch {
	case err == nil && !fi.Mode().IsRegular():
		return writeInPlace(abs, fill)
	case errors.Is(err, os.ErrNotExist):
		fi = nil // to be created: abs is missing, or a link that dangles
	case err != nil:
		return err
	}
	dst, err := resolveLink(abs)
	if err != nil {
		return err
	}
	return replaceFile(dst, fi, fill)
}

// resolveLink returns the file that the chain of symbolic links starting at
// path ends at, or path itself when it is not a link. The file need not
// exist: the last link may dangle.
func resolveLink(path string) (string, error) {
	p := path
	for range maxLinks {
		fi, err := os.Lstat(p)
		switch {
		case errors.Is(err, os.ErrNotExist):
			return p, nil // to be created
		case err != nil:
			return "", err
		case fi.Mode()&os.ModeSymlink == 0:
			return p, nil
		}
		target, err := os.Readlink(p)
		if err != nil {
			return "", err
		}
		if p, err = linkDestination(p, target); err != nil {
			return "", err
		}
	}
	return "", &os.PathError{Op: "open", Path: path, Err: syscall.ELOOP}
}

// linkDestination returns the file that the symbolic link at p, whose
// content is target, points to. Its directory part is resolved the way the
// kernel resolves it: a relative target from the directory holding the
// link, and each ".." after the links before it.
func linkDestination(p, target string) (string, error) {
	switch {
	case filepath.IsAbs(target):
	case target != "" && os.IsPathSeparator(target[0]):
		// Rooted on p's volume (Windows: \dir\file).
		target = filepath.VolumeName(p) + target
	default:
		// Not filepath.Join: its lexical Clean would apply a ".." in target
		// before the links leading up to it are resolved.
		target = filepath.Dir(p) + string(filepath.Separator) + target
	}
	i := len(target)
	for i > 0 && !os.IsPathSeparator(target[i-1]) {
		i--
	}
	dir, name := target[:i], target[i:]
	if name == "" || name == "." || name == ".." {
		return "", &os.PathError{Op: "open", Path: p, Err: syscall.EISDIR}
	}
	dir, err := filepath.EvalSymlinks(dir)
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, name), nil
}

// replaceFile writes dst through fill to a new staging file in dst's
// directory, which is synced and renamed over dst only once fill has
// succeeded. fi describes the existing dst (nil when there is none): the
// staging file is created with dst's owner bits only, gets dst's owner and
// group as far as the process may (keepOwner), and only then dst's
// permission bits (which also restores any the umask took). So it is never
// more permissive than dst, and no group but dst's can open it while the
// download is written into it. A new file gets 0666 minus the umask.
//
// Where dst is a mount point (a single file bind-mounted into a container),
// rename fails with EBUSY: the complete staging file is then copied into dst
// instead.
func replaceFile(dst string, fi os.FileInfo, fill func(w io.Writer) error) (err error) {
	perm, create := os.FileMode(0o666), os.FileMode(0o666)
	if fi != nil {
		// Until keepOwner has run the file has the process's group, or the
		// directory's: dst's group bits must not apply to it yet.
		perm = fi.Mode().Perm()
		create = perm & 0o700
	}
	tmp, err := createTemp(filepath.Dir(dst), filepath.Base(dst), create)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tmp.Close()
			_ = os.Remove(tmp.Name())
		}
	}()
	if fi != nil {
		keepOwner(tmp, fi)
		if err := tmp.Chmod(perm); err != nil {
			return err
		}
	}
	if err := fill(tmp); err != nil {
		return err
	}
	if err := tmp.Sync(); err != nil {
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := renameFile(tmp.Name(), dst); !errors.Is(err, syscall.EBUSY) {
		return err
	}
	if err := copyFile(dst, tmp.Name()); err != nil {
		return err
	}
	// dst is complete: a staging file that cannot be removed is no reason to
	// report the download as failed.
	_ = os.Remove(tmp.Name())
	return nil
}

// copyFile overwrites the existing file dst, in place, with the content of
// the file src.
func copyFile(dst, src string) error {
	in, err := os.Open(src) //nolint:gosec // G304: the caller's own file
	if err != nil {
		return err
	}
	defer closeQuietly(in)
	out, err := os.OpenFile(dst, os.O_WRONLY|os.O_TRUNC, 0) //nolint:gosec // G304: the caller's own file
	if err != nil {
		return err
	}
	_, err = io.Copy(out, in)
	if err == nil {
		err = out.Sync()
	}
	if cerr := out.Close(); err == nil {
		err = cerr
	}
	return err
}

// writeInPlace writes the file at path through fill directly, as Python's
// open(path, "wb") does, following symbolic links: for targets a rename
// cannot replace, such as a FIFO or a device.
func writeInPlace(path string, fill func(w io.Writer) error) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o666) //nolint:gosec // G304: the caller's own file
	if err != nil {
		return err
	}
	err = fill(f)
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	return err
}

// createTemp creates a new, hidden staging file in dir for the target name
// base: "." + base + "." + random + ".tmp", keeping at most stagingBaseMax
// bytes of base. Unlike os.CreateTemp it creates the file with mode perm
// (before the umask), because the file becomes the final download; the
// descriptor it returns is writable whatever perm is.
func createTemp(dir, base string, perm os.FileMode) (*os.File, error) {
	prefix := "." + truncateBytes(base, stagingBaseMax) + "."
	for range 100 {
		name := filepath.Join(dir, prefix+strconv.FormatUint(rand.Uint64(), 36)+".tmp") //nolint:gosec // G404: a unique staging name, not a secret
		f, err := openStaging(name, os.O_RDWR|os.O_CREATE|os.O_EXCL, perm)
		if errors.Is(err, os.ErrExist) {
			continue
		}
		return f, err
	}
	return nil, fmt.Errorf("filestore: could not create a temporary file in %s", dir)
}

// truncateBytes returns s cut to at most n bytes, at a character boundary
// when s is valid UTF-8.
func truncateBytes(s string, n int) string {
	if len(s) <= n {
		return s
	}
	// s[n] starts a character, or continues one that began at most
	// utf8.UTFMax-1 bytes earlier.
	i, low := n, max(n-(utf8.UTFMax-1), 0)
	for i > low && !utf8.RuneStart(s[i]) {
		i--
	}
	return s[:i]
}
