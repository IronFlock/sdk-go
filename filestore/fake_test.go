package filestore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/ironflock/sdk-go/wamp"
)

// --- fake WAMP caller --------------------------------------------------------

// replyFunc computes a scripted reply from the call's positional args.
type replyFunc func(args []any) (any, error)

// fakeCall is one recorded call.
type fakeCall struct {
	URI    string
	Args   []any
	Kwargs map[string]any
	Opts   *wamp.CallOptions
	Retry  time.Duration
}

// fakeCaller stands in for *wamp.Connection: it records every call and
// answers with a scripted reply per URI. A reply is
//   - an error: returned as the call error
//   - a replyFunc: called with the args
//   - a *wamp.Result: returned as is
//   - anything else: returned as the single positional result
type fakeCaller struct {
	mu      sync.Mutex
	replies map[string]any
	calls   []fakeCall
}

func newFakeCaller(replies map[string]any) *fakeCaller {
	return &fakeCaller{replies: maps.Clone(replies)}
}

func (f *fakeCaller) Call(ctx context.Context, uri string, args []any, kwargs map[string]any, opts *wamp.CallOptions, retry time.Duration) (*wamp.Result, error) {
	f.mu.Lock()
	f.calls = append(f.calls, fakeCall{URI: uri, Args: args, Kwargs: kwargs, Opts: opts, Retry: retry})
	reply, ok := f.replies[uri]
	f.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("fake: no reply scripted for %s", uri)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if fn, ok := reply.(func([]any) (any, error)); ok {
		reply = replyFunc(fn)
	}
	if fn, ok := reply.(replyFunc); ok {
		var err error
		if reply, err = fn(args); err != nil {
			return nil, err
		}
	}
	switch r := reply.(type) {
	case error:
		return nil, r
	case *wamp.Result:
		return r, nil
	}
	return &wamp.Result{Args: []any{reply}}, nil
}

// set replaces the reply for uri.
func (f *fakeCaller) set(uri string, reply any) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.replies[uri] = reply
}

// count returns how many times uri was called.
func (f *fakeCaller) count(uri string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, c := range f.calls {
		if c.URI == uri {
			n++
		}
	}
	return n
}

// callsTo returns the calls to uri, in order.
func (f *fakeCaller) callsTo(uri string) []fakeCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []fakeCall
	for _, c := range f.calls {
		if c.URI == uri {
			out = append(out, c)
		}
	}
	return out
}

// uris returns the URIs called, in order.
func (f *fakeCaller) uris() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, len(f.calls))
	for i, c := range f.calls {
		out[i] = c.URI
	}
	return out
}

// allCalls returns a copy of every recorded call.
func (f *fakeCaller) allCalls() []fakeCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]fakeCall(nil), f.calls...)
}

// --- payloads ----------------------------------------------------------------

// ok wraps payload in a success envelope.
func ok(payload map[string]any) map[string]any {
	return map[string]any{"success": true, "payload": payload}
}

// fail is a failure envelope.
func fail(code, reason string) map[string]any {
	return map[string]any{"success": false, "code": code, "reason": reason}
}

// catalogPayload is the test suites' CATALOG_PAYLOAD, with overrides
// applied. It returns a fresh map on every call.
func catalogPayload(overrides map[string]any) map[string]any {
	p := map[string]any{
		"namespaces": []any{
			map[string]any{
				"name":             "frames",
				"description":      "Camera frames",
				"private":          false,
				"content_types":    []any{"image/jpeg"},
				"max_object_bytes": int64(1048576),
				"quota_bytes":      int64(0),
			},
		},
		"inline_max_bytes": int64(6291456),
		"list_max_limit":   int64(1000),
		"quota_bytes":      int64(10737418240),
		"sad_key":          int64(3317),
		"public_base_url":  "https://files.ironflock.com",
	}
	maps.Copy(p, overrides)
	return p
}

// directCatalog is a catalog with a small inline cap and the direct path
// available.
func directCatalog(inlineMax int64) map[string]any {
	return ok(catalogPayload(map[string]any{
		"inline_max_bytes":  inlineMax,
		"presign_available": true,
		"presign_max_bytes": int64(5 << 30),
	}))
}

// newStore returns a FileStore on a fake caller scripted with replies on
// top of the default catalog.
func newStore(t *testing.T, replies map[string]any) (*FileStore, *fakeCaller) {
	t.Helper()
	all := map[string]any{URINamespaces: ok(catalogPayload(nil))}
	maps.Copy(all, replies)
	fc := newFakeCaller(all)
	fs := New(fc, nil)
	// What IronFlock.Stop does for the store Files returns.
	t.Cleanup(fs.CloseIdleConnections)
	return fs, fc
}

// fileError asserts err is an *Error and returns it.
func fileError(t *testing.T, err error) *Error {
	t.Helper()
	var fe *Error
	if !errors.As(err, &fe) {
		t.Fatalf("error %v (%T) is not a *filestore.Error", err, err)
	}
	return fe
}

// wantError asserts err is an *Error with code and, when reason is not "",
// that reason.
func wantError(t *testing.T, err error, code, reason string) {
	t.Helper()
	fe := fileError(t, err)
	if fe.Code != code {
		t.Fatalf("code = %q, want %q (reason %q)", fe.Code, code, fe.Reason)
	}
	if reason != "" && fe.Reason != reason {
		t.Fatalf("reason = %q, want %q", fe.Reason, reason)
	}
}

// eventually polls cond until it holds or 5 s have passed, and reports
// whether it held.
func eventually(cond func() bool) bool {
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(5 * time.Millisecond)
	}
	return true
}

// --- fake object store -------------------------------------------------------

// httpRequest is one request the fake object store received.
type httpRequest struct {
	Method           string
	Path             string
	RawQuery         string
	Header           http.Header
	Host             string
	ContentLength    int64
	TransferEncoding []string
	Body             []byte
}

// objectStore is an httptest.Server standing in for presigned S3 URLs. It
// records every request (with its full body) and answers with respond,
// which defaults to an empty 200.
type objectStore struct {
	srv     *httptest.Server
	mu      sync.Mutex
	reqs    []httpRequest
	respond func(w http.ResponseWriter, r *http.Request)
}

func newObjectStore(t *testing.T, respond func(w http.ResponseWriter, r *http.Request)) *objectStore {
	t.Helper()
	s := &objectStore{respond: respond}
	s.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		s.mu.Lock()
		s.reqs = append(s.reqs, httpRequest{
			Method:           r.Method,
			Path:             r.URL.Path,
			RawQuery:         r.URL.RawQuery,
			Header:           r.Header.Clone(),
			Host:             r.Host,
			ContentLength:    r.ContentLength,
			TransferEncoding: r.TransferEncoding,
			Body:             body,
		})
		s.mu.Unlock()
		if s.respond != nil {
			s.respond(w, r)
		}
	}))
	t.Cleanup(s.srv.Close)
	return s
}

// url returns the server URL for path, with a fake signature.
func (s *objectStore) url(path string) string {
	return s.srv.URL + path + "?X-Amz-Signature=secret-signature"
}

func (s *objectStore) requests() []httpRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]httpRequest(nil), s.reqs...)
}

// respondWith returns a responder writing status and body.
func respondWith(status int, body string) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(status)
		_, _ = io.WriteString(w, body)
	}
}

// unreachableURL returns a URL nothing listens on.
func unreachableURL(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	return "http://" + addr + "/put?X-Amz-Signature=secret-signature"
}

// dirEntries returns the names in dir.
func dirEntries(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	names := make([]string, len(entries))
	for i, e := range entries {
		names[i] = e.Name()
	}
	return names
}
