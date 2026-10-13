package filestore

import (
	"context"
	"encoding/base64"
	"errors"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ironflock/sdk-go/wamp"
)

// The URL comes back with the write, so an app can stamp it into a table row
// in the same call.
func TestPutReturnsAStableURL(t *testing.T) {
	fs, _ := newStore(t, map[string]any{URIPut: ok(map[string]any{
		"namespace": "frames", "key": "part-1.jpg", "size": int64(3), "etag": "e1", "content_type": "image/jpeg",
		"last_modified": "2026-01-01T00:00:00Z", "checksum_sha256": "abc=", "url": "https://ignored.example",
	})})
	info, err := fs.Put(context.Background(), "part-1.jpg", []byte("hey"), Namespace("frames"), ContentType("image/jpeg"))
	if err != nil {
		t.Fatal(err)
	}
	want := &ObjectInfo{
		Namespace: "frames", Key: "part-1.jpg", Size: 3, ETag: "e1", ContentType: "image/jpeg",
		LastModified: "2026-01-01T00:00:00Z", ChecksumSHA256: "abc=",
		URL: "https://files.ironflock.com/f/3317/frames/part-1.jpg",
	}
	if !reflect.DeepEqual(info, want) {
		t.Errorf("info = %+v\nwant   %+v", info, want)
	}
}

func TestObjectInfoURL(t *testing.T) {
	ctx := context.Background()
	t.Run("missing namespace uses the default", func(t *testing.T) {
		fs, _ := newStore(t, map[string]any{URIStat: ok(map[string]any{"key": "a b.jpg"})})
		info, err := fs.Stat(ctx, "a b.jpg", Namespace("frames"))
		if err != nil {
			t.Fatal(err)
		}
		if info.URL != "https://files.ironflock.com/f/3317/default/a%20b.jpg" {
			t.Errorf("URL = %q", info.URL)
		}
	})
	t.Run("no key, no URL and no catalog", func(t *testing.T) {
		fs, fc := newStore(t, map[string]any{URIStat: ok(map[string]any{"namespace": "frames", "size": int64(1)})})
		info, err := fs.Stat(ctx, "a")
		if err != nil {
			t.Fatal(err)
		}
		if info.URL != "" || fc.count(URINamespaces) != 0 {
			t.Errorf("URL = %q, catalog fetches = %d", info.URL, fc.count(URINamespaces))
		}
	})
	t.Run("no HTTP edge", func(t *testing.T) {
		fs, _ := newStore(t, map[string]any{
			URINamespaces: ok(map[string]any{"namespaces": []any{}, "sad_key": int64(1), "public_base_url": ""}),
			URIStat:       ok(map[string]any{"namespace": "frames", "key": "a"}),
		})
		info, err := fs.Stat(ctx, "a")
		if err != nil {
			t.Fatal(err)
		}
		if info.URL != "" {
			t.Errorf("URL = %q, want none", info.URL)
		}
	})
	t.Run("catalog failure fails the call", func(t *testing.T) {
		down := errors.New("down")
		fs, _ := newStore(t, map[string]any{URINamespaces: down, URIStat: ok(map[string]any{"namespace": "frames", "key": "a"})})
		if _, err := fs.Stat(ctx, "a"); !errors.Is(err, down) {
			t.Errorf("err = %v, want %v", err, down)
		}
	})
}

func TestURL(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name string
		key  string
		opts []Option
		want string
	}{
		{"escapes but keeps folder separators", "a/b c.jpg", []Option{Namespace("frames")},
			"https://files.ironflock.com/f/3317/frames/a/b%20c.jpg"},
		{"default namespace", "a.jpg", nil, "https://files.ironflock.com/f/3317/default/a.jpg"},
		// The file service takes "" for the default namespace in every call;
		// the edge has no empty namespace segment.
		{"explicit empty namespace is the default", "a.jpg", []Option{Namespace("")},
			"https://files.ironflock.com/f/3317/default/a.jpg"},
		{"explicit empty namespace, folder key", "frames/cam1.jpg", []Option{Namespace("")},
			"https://files.ironflock.com/f/3317/default/frames/cam1.jpg"},
		{"unreserved kept", "AZaz09-._~/x", nil, "https://files.ironflock.com/f/3317/default/AZaz09-._~/x"},
		{"reserved and sub-delims escaped", "a+b&c=d?e#f%g!*'()$,;:@", nil,
			"https://files.ironflock.com/f/3317/default/a%2Bb%26c%3Dd%3Fe%23f%25g%21%2A%27%28%29%24%2C%3B%3A%40"},
		{"utf-8", "ä/€.txt", nil, "https://files.ironflock.com/f/3317/default/%C3%A4/%E2%82%AC.txt"},
		{"namespace not escaped", "k", []Option{Namespace("my ns")}, "https://files.ironflock.com/f/3317/my ns/k"},
		{"version", "a.jpg", []Option{Version("e1")}, "https://files.ironflock.com/f/3317/default/a.jpg?v=e1"},
		{"version escaped like encodeURIComponent", "a.jpg", []Option{Version(`"e 1/2"&!*'()`)},
			"https://files.ironflock.com/f/3317/default/a.jpg?v=%22e%201%2F2%22%26!*'()"},
	}
	fs, fc := newStore(t, nil)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := fs.URL(ctx, tc.key, tc.opts...)
			if err != nil {
				t.Fatal(err)
			}
			if got != tc.want {
				t.Errorf("URL = %q\nwant  %q", got, tc.want)
			}
		})
	}
	if n := fc.count(URINamespaces); n != 1 {
		t.Errorf("catalog fetched %d times, want 1 (URL is offline after that)", n)
	}
}

// A namespace given as "" is the default namespace everywhere: the file
// service maps it so for every call (fleetfiles handlers.go wrap), so the
// URL an ObjectInfo carries and the one URL composes for the same option
// must agree — a "//" segment would make the edge answer 404, or address
// the wrong object for a folder-style key ("/f/3317//frames/cam1.jpg" is
// cleaned to namespace "frames", key "cam1.jpg").
func TestEmptyNamespaceURLsAgree(t *testing.T) {
	ctx := context.Background()
	fs, fc := newStore(t, map[string]any{URIStat: replyFunc(func(args []any) (any, error) {
		a := args[0].(map[string]any)
		ns := a["namespace"].(string)
		if ns == "" {
			ns = "default"
		}
		return ok(map[string]any{"namespace": ns, "key": a["key"], "size": int64(1)}), nil
	})})
	for _, key := range []string{"top.txt", "frames/cam1.jpg"} {
		info, err := fs.Stat(ctx, key, Namespace(""))
		if err != nil {
			t.Fatal(err)
		}
		u, err := fs.URL(ctx, key, Namespace(""))
		if err != nil {
			t.Fatal(err)
		}
		if u != info.URL || !strings.Contains(u, "/f/3317/default/") {
			t.Errorf("%s: URL = %q, Stat's URL = %q; want both in the default namespace", key, u, info.URL)
		}
	}
	// The call itself still sends the namespace as given.
	if got := listArg(t, fc.callsTo(URIStat)[0], "namespace"); got != "" {
		t.Errorf("stat sent namespace %q, want \"\"", got)
	}
}

func TestURLBases(t *testing.T) {
	ctx := context.Background()
	t.Run("trailing slashes trimmed", func(t *testing.T) {
		fs, _ := newStore(t, map[string]any{URINamespaces: ok(catalogPayload(map[string]any{
			"public_base_url": "https://files.ironflock.com///", "cloud_base_url": "https://i5-files.app.ironflock.com/",
		}))})
		u, _ := fs.URL(ctx, "a.jpg", Namespace("frames"))
		c, _ := fs.CloudURL(ctx, "a.jpg", Namespace("frames"))
		if u != "https://files.ironflock.com/f/3317/frames/a.jpg" || c != "https://i5-files.app.ironflock.com/f/3317/frames/a.jpg" {
			t.Errorf("URL = %q, CloudURL = %q", u, c)
		}
	})
	t.Run("no HTTP edge", func(t *testing.T) {
		fs, _ := newStore(t, map[string]any{URINamespaces: ok(map[string]any{"namespaces": []any{}, "sad_key": int64(1), "public_base_url": ""})})
		if u, err := fs.URL(ctx, "a.jpg"); err != nil || u != "" {
			t.Errorf("URL = %q, %v; want empty", u, err)
		}
	})
	t.Run("cloud URL composes on the tunnel base", func(t *testing.T) {
		fs, _ := newStore(t, map[string]any{URINamespaces: ok(catalogPayload(map[string]any{
			"cloud_base_url": "https://i5-files.app.ironflock.com",
		}))})
		c, _ := fs.CloudURL(ctx, "a/b c.jpg", Namespace("frames"), Version("e 1"))
		if c != "https://i5-files.app.ironflock.com/f/3317/frames/a/b%20c.jpg?v=e%201" {
			t.Errorf("CloudURL = %q", c)
		}
		if c, _ := fs.CloudURL(ctx, "a.jpg", Namespace("")); c != "https://i5-files.app.ironflock.com/f/3317/default/a.jpg" {
			t.Errorf("CloudURL with the explicit empty namespace = %q", c)
		}
		if u, _ := fs.URL(ctx, "a.jpg", Namespace("frames")); u != "https://files.ironflock.com/f/3317/frames/a.jpg" {
			t.Errorf("the public base changed: URL = %q", u)
		}
	})
	t.Run("no tunnel base", func(t *testing.T) {
		fs, _ := newStore(t, nil)
		if c, err := fs.CloudURL(ctx, "a.jpg", Namespace("frames")); err != nil || c != "" {
			t.Errorf("CloudURL = %q, %v; want empty", c, err)
		}
	})
	t.Run("catalog failure", func(t *testing.T) {
		fs, _ := newStore(t, map[string]any{URINamespaces: fail(CodeNotAuthorized, "no")})
		if _, err := fs.URL(ctx, "a"); err == nil {
			t.Error("URL succeeded without a catalog")
		}
		if _, err := fs.CloudURL(ctx, "a"); err == nil {
			t.Error("CloudURL succeeded without a catalog")
		}
	})
}

func TestList(t *testing.T) {
	ctx := context.Background()
	fs, _ := newStore(t, map[string]any{URIList: ok(map[string]any{
		"objects": []any{
			map[string]any{"namespace": "frames", "key": "a/1.jpg", "size": int64(3), "etag": "e"},
			"junk",
			map[string]any{"key": "a/2 x.jpg"},
		},
		"prefixes":     []any{"a/sub/"},
		"is_truncated": true,
		"cursor":       "c1",
	})})
	res, err := fs.List(ctx, Namespace("frames"), Prefix("a/"))
	if err != nil {
		t.Fatal(err)
	}
	want := &ListResult{
		Objects: []ObjectInfo{
			{Namespace: "frames", Key: "a/1.jpg", Size: 3, ETag: "e", URL: "https://files.ironflock.com/f/3317/frames/a/1.jpg"},
			{Key: "a/2 x.jpg", URL: "https://files.ironflock.com/f/3317/default/a/2%20x.jpg"},
		},
		Prefixes:    []string{"a/sub/"},
		IsTruncated: true,
		Cursor:      "c1",
	}
	if !reflect.DeepEqual(res, want) {
		t.Errorf("list = %+v\nwant   %+v", res, want)
	}

	empty, _ := newStore(t, map[string]any{URIList: ok(map[string]any{})})
	res, err = empty.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if res.Objects == nil || res.Prefixes == nil || len(res.Objects) != 0 || res.IsTruncated || res.Cursor != "" {
		t.Errorf("empty list = %#v", res)
	}
}

// listPages scripts files.read.list to answer with pages in turn (repeating
// the last one).
func listPages(pages ...map[string]any) replyFunc {
	var i atomic.Int32
	return func([]any) (any, error) {
		n := int(i.Add(1)) - 1
		return pages[min(n, len(pages)-1)], nil
	}
}

func page(truncated bool, cursor string, keys ...string) map[string]any {
	objects := make([]any, len(keys))
	for i, k := range keys {
		objects[i] = map[string]any{"namespace": "frames", "key": k}
	}
	return ok(map[string]any{"objects": objects, "is_truncated": truncated, "cursor": cursor})
}

func collectKeys(t *testing.T, fs *FileStore, ctx context.Context, opts ...Option) ([]string, error) {
	t.Helper()
	var keys []string
	for obj, err := range fs.Iter(ctx, opts...) {
		if err != nil {
			if obj != nil {
				t.Error("an error was yielded with an object")
			}
			return keys, err
		}
		keys = append(keys, obj.Key)
	}
	return keys, nil
}

func listArg(t *testing.T, c fakeCall, field string) any {
	t.Helper()
	return c.Args[0].(map[string]any)[field]
}

// Guarding on the cursor as well as the truncation flag: a server reporting
// truncation without one would otherwise loop forever on page one.
func TestIterStopsWhenTheCursorRunsOut(t *testing.T) {
	ctx := context.Background()
	fs, fc := newStore(t, map[string]any{URIList: listPages(
		page(true, "c1", "a"),
		page(true, "", "b"),
		page(true, "c3", "never"),
	)})
	keys, err := collectKeys(t, fs, ctx, Namespace("frames"), Prefix("p"), Limit(50), Cursor("ignored"))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(keys, []string{"a", "b"}) {
		t.Errorf("keys = %v, want [a b]", keys)
	}
	calls := fc.callsTo(URIList)
	if len(calls) != 2 {
		t.Fatalf("list called %d times, want 2", len(calls))
	}
	for i, wantCursor := range []string{"", "c1"} {
		want := map[string]any{"namespace": "frames", "prefix": "p", "limit": int64(50), "cursor": wantCursor, "delimiter": ""}
		if !reflect.DeepEqual(calls[i].Args, []any{want}) {
			t.Errorf("page %d args = %v, want [%v]", i, calls[i].Args, want)
		}
	}
}

func TestIter(t *testing.T) {
	ctx := context.Background()
	t.Run("not truncated stops despite a cursor", func(t *testing.T) {
		fs, fc := newStore(t, map[string]any{URIList: listPages(page(false, "c1", "a", "b"))})
		keys, err := collectKeys(t, fs, ctx)
		if err != nil || !reflect.DeepEqual(keys, []string{"a", "b"}) || fc.count(URIList) != 1 {
			t.Errorf("keys = %v, err = %v, pages = %d", keys, err, fc.count(URIList))
		}
		if got := listArg(t, fc.callsTo(URIList)[0], "limit"); got != int64(DefaultListLimit) {
			t.Errorf("limit = %v, want the default", got)
		}
	})
	t.Run("an error ends the iteration", func(t *testing.T) {
		boom := fail(CodeNoSuchNamespace, "gone")
		var n atomic.Int32
		fs, fc := newStore(t, map[string]any{URIList: replyFunc(func([]any) (any, error) {
			if n.Add(1) == 1 {
				return page(true, "c1", "a"), nil
			}
			return boom, nil
		})})
		keys, err := collectKeys(t, fs, ctx)
		wantError(t, err, CodeNoSuchNamespace, "gone")
		if !reflect.DeepEqual(keys, []string{"a"}) || fc.count(URIList) != 2 {
			t.Errorf("keys = %v, pages = %d", keys, fc.count(URIList))
		}
	})
	t.Run("break stops paging", func(t *testing.T) {
		fs, fc := newStore(t, map[string]any{URIList: listPages(page(true, "c1", "a", "b"), page(false, "", "c"))})
		for obj, err := range fs.Iter(ctx) {
			if err != nil {
				t.Fatal(err)
			}
			if obj.Key == "a" {
				break
			}
		}
		if n := fc.count(URIList); n != 1 {
			t.Errorf("list called %d times after break, want 1", n)
		}
	})
	t.Run("cancelled context", func(t *testing.T) {
		fs, fc := newStore(t, map[string]any{URIList: listPages(page(true, "c1", "a"), page(false, "", "b"))})
		cctx, cancel := context.WithCancel(ctx)
		defer cancel()
		var keys []string
		var gotErr error
		for obj, err := range fs.Iter(cctx) {
			if err != nil {
				gotErr = err
				break
			}
			keys = append(keys, obj.Key)
			cancel()
		}
		if !errors.Is(gotErr, context.Canceled) || !reflect.DeepEqual(keys, []string{"a"}) || fc.count(URIList) != 1 {
			t.Errorf("keys = %v, err = %v, pages = %d", keys, gotErr, fc.count(URIList))
		}
	})
	t.Run("objects carry URLs", func(t *testing.T) {
		fs, _ := newStore(t, map[string]any{URIList: listPages(page(false, "", "a b"))})
		for obj, err := range fs.Iter(ctx) {
			if err != nil {
				t.Fatal(err)
			}
			if obj.URL != "https://files.ironflock.com/f/3317/frames/a%20b" {
				t.Errorf("URL = %q", obj.URL)
			}
		}
	})
}

// A missing object is false; anything else must be returned. Swallowing
// NOT_AUTHORIZED would report "not there" to an app not allowed to look.
func TestExists(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name    string
		reply   any
		want    bool
		wantErr bool
	}{
		{"present", ok(map[string]any{"namespace": "frames", "key": "a"}), true, false},
		{"missing", fail(CodeNoSuchObject, "gone"), false, false},
		{"not authorized", fail(CodeNotAuthorized, "no"), false, true},
		{"no such namespace", fail(CodeNoSuchNamespace, "no"), false, true},
		{"no file service", &wamp.Error{URI: wamp.URINoSuchProcedure}, false, true},
		{"not connected", errors.New("not connected"), false, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fs, _ := newStore(t, map[string]any{URIStat: tc.reply})
			got, err := fs.Exists(ctx, "a", Namespace("frames"))
			if got != tc.want || (err != nil) != tc.wantErr {
				t.Errorf("Exists = %v, %v; want %v, error %v", got, err, tc.want, tc.wantErr)
			}
		})
	}
}

func TestGetInline(t *testing.T) {
	ctx := context.Background()
	get := func(fields map[string]any) map[string]any {
		p := map[string]any{"namespace": "frames", "key": "a.jpg", "size": int64(3)}
		for k, v := range fields {
			p[k] = v
		}
		return ok(p)
	}
	hey := base64.StdEncoding.EncodeToString([]byte("hey"))
	cases := []struct {
		name       string
		reply      any
		want       []byte
		wantCode   string
		wantReason string
	}{
		{"base64", get(map[string]any{"data": hey, "encoding": "base64"}), []byte("hey"), "", ""},
		{"no encoding", get(map[string]any{"data": hey}), []byte("hey"), "", ""},
		{"empty encoding", get(map[string]any{"data": hey, "encoding": ""}), []byte("hey"), "", ""},
		{"unpadded", get(map[string]any{"data": "aGk"}), []byte("hi"), "", ""},
		{"empty", get(map[string]any{"data": "", "encoding": "base64"}), []byte{}, "", ""},
		{"no data", get(nil), []byte{}, "", ""},
		{"unsupported encoding", get(map[string]any{"data": hey, "encoding": "gzip"}), nil,
			CodeInternal, "unsupported transfer encoding 'gzip'"},
		{"invalid base64", get(map[string]any{"data": "!!not base64!!", "encoding": "base64"}), nil,
			CodeInternal, "object payload was not valid base64"},
		{"missing object", fail(CodeNoSuchObject, "gone"), nil, CodeNoSuchObject, "gone"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fs, fc := newStore(t, map[string]any{URIGet: tc.reply})
			data, err := fs.Get(ctx, "a.jpg", Namespace("frames"))
			if tc.wantCode != "" {
				wantError(t, err, tc.wantCode, tc.wantReason)
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(data, tc.want) {
				t.Errorf("data = %q, want %q", data, tc.want)
			}
			if fc.count(URIReadURL) != 0 {
				t.Error("an inline get minted a URL")
			}
		})
	}
}

// The service has no move verb: the copy must land before the source is
// removed, so a failure never destroys the only copy.
func TestMoveIsCopyThenDelete(t *testing.T) {
	ctx := context.Background()
	record := func() replyFunc {
		return func(args []any) (any, error) {
			a := args[0].(map[string]any)
			key := a["key"]
			if to, ok := a["to"]; ok {
				key = to
			}
			return ok(map[string]any{"namespace": "archive", "key": key, "size": int64(1)}), nil
		}
	}
	fs, fc := newStore(t, map[string]any{URICopy: record(), URIDelete: record()})
	info, err := fs.Move(ctx, "old.jpg", "new.jpg", Namespace("frames"), ToNamespace("archive"))
	if err != nil {
		t.Fatal(err)
	}
	var order []string
	for _, u := range fc.uris() {
		if u != URINamespaces {
			order = append(order, u)
		}
	}
	if !reflect.DeepEqual(order, []string{URICopy, URIDelete}) {
		t.Fatalf("calls = %v, want copy then delete", order)
	}
	if want := []any{map[string]any{"namespace": "frames", "key": "old.jpg", "to": "new.jpg", "to_namespace": "archive"}}; !reflect.DeepEqual(fc.callsTo(URICopy)[0].Args, want) {
		t.Errorf("copy args = %v", fc.callsTo(URICopy)[0].Args)
	}
	if want := []any{map[string]any{"namespace": "frames", "key": "old.jpg"}}; !reflect.DeepEqual(fc.callsTo(URIDelete)[0].Args, want) {
		t.Errorf("delete args = %v, want the source", fc.callsTo(URIDelete)[0].Args)
	}
	if info.Key != "new.jpg" || info.Namespace != "archive" || info.URL != "https://files.ironflock.com/f/3317/archive/new.jpg" {
		t.Errorf("info = %+v, want the copy", info)
	}
}

func TestMoveIsNotAtomic(t *testing.T) {
	ctx := context.Background()
	t.Run("failed copy deletes nothing", func(t *testing.T) {
		fs, fc := newStore(t, map[string]any{URICopy: fail(CodeQuotaExceeded, "full"), URIDelete: ok(nil)})
		_, err := fs.Move(ctx, "a", "b")
		wantError(t, err, CodeQuotaExceeded, "full")
		if fc.count(URIDelete) != 0 {
			t.Error("the source was deleted although the copy failed")
		}
	})
	t.Run("failed delete is returned", func(t *testing.T) {
		fs, fc := newStore(t, map[string]any{
			URICopy:   ok(map[string]any{"namespace": "default", "key": "b"}),
			URIDelete: fail(CodeNotAuthorized, "no delete"),
		})
		info, err := fs.Move(ctx, "a", "b")
		wantError(t, err, CodeNotAuthorized, "no delete")
		if info != nil || fc.count(URICopy) != 1 {
			t.Errorf("info = %v, copies = %d", info, fc.count(URICopy))
		}
	})
}

func TestShareURL(t *testing.T) {
	ctx := context.Background()
	fs, _ := newStore(t, map[string]any{URIReadURL: ok(map[string]any{"url": "https://s3.example/get?sig=1", "method": "GET"})})
	u, err := fs.ShareURL(ctx, "a b.txt", TTL(2*time.Minute))
	if err != nil || u != "https://s3.example/get?sig=1" {
		t.Errorf("ShareURL = %q, %v", u, err)
	}
	for _, reply := range []map[string]any{{"url": ""}, {}, {"url": nil}} {
		fs, _ := newStore(t, map[string]any{URIReadURL: ok(reply)})
		_, err := fs.ShareURL(ctx, "a")
		wantError(t, err, CodeInternal, "the service returned no download URL")
	}
}

// UploadURL returns the JavaScript SDK's shape: Method defaults to PUT and
// Headers to an empty map.
func TestUploadURL(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name    string
		payload map[string]any
		want    UploadTarget
	}{
		{"full", map[string]any{"url": "https://s3.example/put", "method": "PUT",
			"headers": map[string]any{"Content-Type": "application/octet-stream"}, "expires_in": int64(60)},
			UploadTarget{URL: "https://s3.example/put", Method: "PUT",
				Headers: map[string]string{"Content-Type": "application/octet-stream"}, ExpiresIn: 60}},
		{"defaults", map[string]any{"url": "https://s3.example/put"},
			UploadTarget{URL: "https://s3.example/put", Method: "PUT", Headers: map[string]string{}}},
		{"lenient", map[string]any{"url": "u", "method": "POST", "headers": map[string]any{"X-N": int64(1)}, "expires_in": "30"},
			UploadTarget{URL: "u", Method: "POST", Headers: map[string]string{"X-N": "1"}, ExpiresIn: 30}},
		{"no url is not checked", map[string]any{},
			UploadTarget{Method: "PUT", Headers: map[string]string{}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fs, _ := newStore(t, map[string]any{URIWriteURL: ok(tc.payload)})
			got, err := fs.UploadURL(ctx, "u.bin")
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(*got, tc.want) {
				t.Errorf("target = %+v, want %+v", *got, tc.want)
			}
		})
	}
}

func TestDelete(t *testing.T) {
	fs, _ := newStore(t, map[string]any{URIDelete: fail(CodeNoSuchObject, "gone")})
	err := fs.Delete(context.Background(), "a")
	wantError(t, err, CodeNoSuchObject, "gone")
	if !strings.Contains(err.Error(), "gone") {
		t.Errorf("Error() = %q", err.Error())
	}
}

// A large body well inside the inline cap travels as one base64 string.
func TestPutLargeInlineBody(t *testing.T) {
	big := []byte(strings.Repeat("A", 300*1024))
	fs, fc := newStore(t, map[string]any{URIPut: ok(map[string]any{"namespace": "frames", "key": "big.bin", "size": int64(len(big))})})
	if _, err := fs.Put(context.Background(), "big.bin", big, Namespace("frames")); err != nil {
		t.Fatal(err)
	}
	sent, isString := fc.callsTo(URIPut)[0].Args[0].(map[string]any)["data"].(string)
	if !isString {
		t.Fatal("data is not a string")
	}
	decoded, err := base64.StdEncoding.DecodeString(sent)
	if err != nil || len(decoded) != len(big) {
		t.Errorf("data decodes to %d bytes (%v), want %d", len(decoded), err, len(big))
	}
}
