package ironflock

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ironflock/nexus/v3/client"
	"github.com/ironflock/nexus/v3/router"
	"github.com/ironflock/nexus/v3/router/auth"
	nxwamp "github.com/ironflock/nexus/v3/wamp"

	"github.com/ironflock/sdk-go/wamp"
)

// Tests against a real wamp.Connection and an in-process nexus router.

const (
	routerRealm  = "realm-1-2-dev" // SWARM_KEY 1, APP_KEY 2, DEV
	routerAuthID = "app-auth-id"
	routerSecret = "app-secret"
)

// craKeys is the WAMP-CRA key store of the test router: one credential.
type craKeys struct{}

func (craKeys) AuthKey(authid, _ string) ([]byte, error) {
	if authid != routerAuthID {
		return nil, errors.New("no such user")
	}
	return []byte(routerSecret), nil
}
func (craKeys) PasswordInfo(string) (string, int, int) { return "", 0, 0 }
func (craKeys) AuthRole(string) (string, error)        { return "app", nil }
func (craKeys) Provider() string                       { return "static" }

// testRouter is an in-process nexus router behind a WebSocket server.
type testRouter struct {
	r        router.Router
	srv      *httptest.Server
	url      string
	handlers sync.WaitGroup
}

// newTestRouter starts a router, with routerRealm when withRealm is set.
// It is closed when the test ends, after the connections the test made
// (cleanups registered later run first).
func newTestRouter(t *testing.T, withRealm bool) *testRouter {
	t.Helper()
	cfg := &router.Config{}
	if withRealm {
		cfg.RealmConfigs = []*router.RealmConfig{routerRealmConfig()}
	}
	r, err := router.NewRouter(cfg, log.New(io.Discard, "", 0))
	if err != nil {
		t.Fatalf("router: %v", err)
	}
	tr := &testRouter{r: r}
	wss := router.NewWebsocketServer(r)
	tr.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		tr.handlers.Add(1)
		defer tr.handlers.Done()
		wss.ServeHTTP(w, req)
	}))
	tr.url = "ws" + strings.TrimPrefix(tr.srv.URL, "http") + "/ws-ua-usr"
	t.Cleanup(func() {
		tr.srv.Close()
		tr.handlers.Wait()
		tr.r.Close()
	})
	return tr
}

func routerRealmConfig() *router.RealmConfig {
	return &router.RealmConfig{
		URI:            routerRealm,
		Authenticators: []auth.Authenticator{auth.NewCRAuthenticator(craKeys{}, time.Second)},
	}
}

func (tr *testRouter) addRealm(t *testing.T) {
	t.Helper()
	if err := tr.r.AddRealm(routerRealmConfig()); err != nil {
		t.Fatalf("AddRealm: %v", err)
	}
}

// local returns a trusted in-process client on routerRealm.
func (tr *testRouter) local(t *testing.T) *client.Client {
	t.Helper()
	cli, err := client.ConnectLocal(tr.r, client.Config{Realm: routerRealm, Logger: log.New(io.Discard, "", 0)})
	if err != nil {
		t.Fatalf("local client: %v", err)
	}
	t.Cleanup(func() { _ = cli.Close() })
	return cli
}

// realFlock returns an IronFlock on routerRealm of tr with a real
// wamp.Connection, not started. It is stopped when the test ends.
func realFlock(t *testing.T, tr *testRouter, opts ...Option) *IronFlock {
	t.Helper()
	setIdentityEnv(t)
	logger, _ := newTestLogger()
	f, err := New(append([]Option{
		WithURL(tr.url), WithCredentials(routerAuthID, routerSecret),
		WithSwarmKey(1), WithAppKey(2), WithEnv("DEV"), WithLogger(logger),
	}, opts...)...)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := f.Stop(ctx); err != nil {
			t.Errorf("Stop: %v", err)
		}
	})
	return f
}

// startedRealFlock is realFlock, started.
func startedRealFlock(t *testing.T, tr *testRouter, opts ...Option) *IronFlock {
	t.Helper()
	f := realFlock(t, tr, opts...)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := f.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	return f
}

// publish publishes from a local client, acknowledged.
func publish(t *testing.T, cli *client.Client, topic string, args ...any) {
	t.Helper()
	if err := cli.Publish(topic, nxwamp.Dict{nxwamp.OptAcknowledge: true}, args, nil); err != nil {
		t.Errorf("publish %s: %v", topic, err)
	}
}

func timeout(t *testing.T, d time.Duration) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	t.Cleanup(cancel)
	return ctx
}

// A Start that fails — here because the app's realm does not exist yet —
// can be retried on the same instance, with a real connection: once the
// realm is there, the next Start joins it.
func TestStartRetryWithARealConnection(t *testing.T) {
	tr := newTestRouter(t, false)
	f := realFlock(t, tr)
	if err := f.Start(timeout(t, 300*time.Millisecond)); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Start without the realm = %v, want a deadline error", err)
	}
	if f.IsConnected() {
		t.Fatal("connected without the realm")
	}

	tr.addRealm(t)
	if err := f.Start(timeout(t, 5*time.Second)); err != nil {
		t.Fatalf("Start once the realm exists: %v", err)
	}
	if !f.IsConnected() {
		t.Fatal("not connected after the second Start")
	}
	rows := make(chan any, 1)
	if err := tr.local(t).Subscribe("1.2.sensordata", func(ev *nxwamp.Event) { rows <- ev.Arguments[0] }, nil); err != nil {
		t.Fatalf("local subscribe: %v", err)
	}
	if err := f.PublishToTable(timeout(t, 5*time.Second), "sensordata", Row{"temp": 21.5}); err != nil {
		t.Fatalf("PublishToTable: %v", err)
	}
	if row := recv(t, rows, "the row"); fmt.Sprint(row) != "map[temp:21.5]" {
		t.Errorf("row %v", row)
	}
}

// An operation issued before Start waits for it and then goes through, with
// a real connection.
func TestOperationBeforeStartWithARealConnection(t *testing.T) {
	tr := newTestRouter(t, true)
	f := realFlock(t, tr)
	rows := make(chan any, 1)
	if err := tr.local(t).Subscribe("1.2.sensordata", func(ev *nxwamp.Event) { rows <- ev.Arguments[0] }, nil); err != nil {
		t.Fatalf("local subscribe: %v", err)
	}
	published := make(chan error, 1)
	go func() { published <- f.PublishToTable(timeout(t, 5*time.Second), "sensordata", Row{"temp": 7}) }()
	eventually(t, "PublishToTable to wait for Start", func() bool { return f.startWaiters.Load() == 1 })
	if err := f.Start(timeout(t, 5*time.Second)); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := recv(t, published, "PublishToTable"); err != nil {
		t.Fatalf("PublishToTable before Start: %v", err)
	}
	if row := recv(t, rows, "the row"); fmt.Sprint(row) != "map[temp:7]" {
		t.Errorf("row %v", row)
	}
}

// concurrencyProbe is a table event handler that records how many of its
// calls overlap. It also writes a plain map, which the race detector reports
// should two calls run at once.
type concurrencyProbe struct {
	active, maxActive, calls atomic.Int32
	latest                   map[any]any
}

func (p *concurrencyProbe) handle(ev *Event) {
	n := p.active.Add(1)
	defer p.active.Add(-1)
	for m := p.maxActive.Load(); n > m && !p.maxActive.CompareAndSwap(m, n); m = p.maxActive.Load() {
	}
	row := ev.Row()
	p.latest[row["device"]] = row
	p.calls.Add(1)
}

// SubscribeToTable calls its handler one event at a time across the row and
// the bulk feed, as the Python and JavaScript SDKs do.
func TestSubscribeToTableDeliversOneEventAtATime(t *testing.T) {
	tr := newTestRouter(t, true)
	f := startedRealFlock(t, tr)
	probe := &concurrencyProbe{latest: map[any]any{}}
	ts, err := f.SubscribeToTable(timeout(t, 5*time.Second), "sensordata", probe.handle)
	if err != nil {
		t.Fatal(err)
	}

	const n, batch = 150, 3
	rowsPub, bulkPub := tr.local(t), tr.local(t)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := range n {
			publish(t, rowsPub, "transformed.sensordata", map[string]any{"device": int64(i % 5), "n": int64(i)})
		}
	}()
	go func() {
		defer wg.Done()
		for i := range n {
			rows := make([]any, batch)
			for j := range rows {
				rows[j] = map[string]any{"device": int64((i + j) % 5), "n": int64(i)}
			}
			publish(t, bulkPub, "transformed.bulk.sensordata", rows)
		}
	}()
	wg.Wait()
	deadline := time.Now().Add(10 * time.Second)
	for probe.calls.Load() < n*(1+batch) && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if got := probe.calls.Load(); got != n*(1+batch) {
		t.Fatalf("%d events delivered, want %d", got, n*(1+batch))
	}
	if m := probe.maxActive.Load(); m != 1 {
		t.Errorf("the handler ran %d times at once", m)
	}
	if err := ts.Unsubscribe(timeout(t, 5*time.Second)); err != nil {
		t.Fatal(err)
	}
}

// No handler call starts once TableSubscription.Unsubscribe has returned:
// not for events still queued for either feed, nor for the remaining rows of
// a bulk event being delivered.
func TestSubscribeToTableDeliversNothingAfterUnsubscribe(t *testing.T) {
	t.Run("queued events", func(t *testing.T) {
		tr := newTestRouter(t, true)
		f := startedRealFlock(t, tr)
		ctx := timeout(t, 10*time.Second)
		// The table subscription shares a DeliveryGroup with a probe
		// subscription: when the probe's event arrives, every event queued
		// before it has had its turn.
		group := wamp.NewDeliveryGroup()
		entered, release := make(chan struct{}), make(chan struct{})
		var unsubscribed atomic.Bool
		var calls, late atomic.Int32
		ts, err := f.SubscribeToTable(ctx, "sensordata", func(*Event) {
			if unsubscribed.Load() {
				late.Add(1)
			}
			if calls.Add(1) == 1 {
				close(entered)
				<-release
			}
		}, SubscribeOptions{Group: group})
		if err != nil {
			t.Fatal(err)
		}
		marker, drained := make(chan struct{}, 1), make(chan struct{}, 1)
		if _, err := f.Subscribe(ctx, "test.marker", func(*Event) { marker <- struct{}{} }); err != nil {
			t.Fatal(err)
		}
		if _, err := f.Subscribe(ctx, "test.drained", func(*Event) { drained <- struct{}{} }, SubscribeOptions{Group: group}); err != nil {
			t.Fatal(err)
		}

		pub := tr.local(t)
		publish(t, pub, "transformed.sensordata", map[string]any{"n": int64(0)})
		recv(t, entered, "the first event")
		for i := range 20 {
			publish(t, pub, "transformed.sensordata", map[string]any{"n": int64(i)})
			publish(t, pub, "transformed.bulk.sensordata", []any{map[string]any{"n": int64(i)}, map[string]any{"n": int64(i)}})
		}
		// The marker's subscription has its own queue: once its handler
		// runs, the events published before it are queued in the group.
		publish(t, pub, "test.marker", nil)
		recv(t, marker, "the marker")

		if err := ts.Unsubscribe(ctx); err != nil {
			t.Fatal(err)
		}
		unsubscribed.Store(true)
		close(release)
		publish(t, pub, "test.drained", nil)
		recv(t, drained, "the group to deliver its queue")
		if n := late.Load(); n != 0 {
			t.Errorf("%d handler calls after Unsubscribe returned", n)
		}
		if n := calls.Load(); n != 1 {
			t.Errorf("%d handler calls, want only the one under way", n)
		}
	})

	t.Run("rows of a bulk event", func(t *testing.T) {
		tr := newTestRouter(t, true)
		f := startedRealFlock(t, tr)
		ctx := timeout(t, 10*time.Second)
		group := wamp.NewDeliveryGroup() // see "queued events"
		var ts atomic.Pointer[TableSubscription]
		var calls atomic.Int32
		unsubscribed := make(chan error, 1)
		sub, err := f.SubscribeToTable(ctx, "sensordata", func(*Event) {
			if calls.Add(1) == 1 {
				unsubscribed <- ts.Load().Unsubscribe(ctx) // from the handler, in the middle of the batch
			}
		}, SubscribeOptions{Group: group})
		if err != nil {
			t.Fatal(err)
		}
		ts.Store(sub)
		drained := make(chan struct{}, 1)
		if _, err := f.Subscribe(ctx, "test.drained", func(*Event) { drained <- struct{}{} }, SubscribeOptions{Group: group}); err != nil {
			t.Fatal(err)
		}
		pub := tr.local(t)
		publish(t, pub, "transformed.bulk.sensordata", []any{
			map[string]any{"n": int64(1)}, map[string]any{"n": int64(2)}, map[string]any{"n": int64(3)},
		})
		if err := recv(t, unsubscribed, "Unsubscribe"); err != nil {
			t.Fatal(err)
		}
		// Queued behind the rest of the batch.
		publish(t, pub, "test.drained", nil)
		recv(t, drained, "the drain event")
		if n := calls.Load(); n != 1 {
			t.Errorf("%d handler calls, want 1: rows after Unsubscribe were delivered", n)
		}
	})
}

// fakeConn follows wamp.Connection's lifecycle: the same script has the same
// outcomes on both, so tests with the fake cannot pass where the real
// connection would fail.
func TestFakeConnFollowsWampConnection(t *testing.T) {
	tr := newTestRouter(t, true)
	logger, _ := newTestLogger()
	cfg := wamp.Config{
		SwarmKey: 1, AppKey: 2, Stage: wamp.StageDevelopment, URL: tr.url,
		SerialNumber: "test-serial", AuthID: routerAuthID, AuthSecret: routerSecret, Logger: logger,
	}
	for name, newConn := range map[string]func() wampConn{
		"fake":            func() wampConn { return &fakeConn{} },
		"wamp.Connection": func() wampConn { return wamp.NewConnection() },
	} {
		t.Run(name, func(t *testing.T) {
			ctx := timeout(t, 5*time.Second)
			expect := func(what string, err error, ok bool) {
				t.Helper()
				if !ok {
					t.Errorf("%s: %v", what, err)
				}
			}
			publish := func(c wampConn) error {
				return c.Publish(ctx, "com.example.contract", nil, nil, &wamp.PublishOptions{Acknowledge: true}, 0)
			}

			// Before Configure.
			c := newConn()
			err := c.Start(ctx)
			expect("Start before Configure", err, errors.Is(err, wamp.ErrNotConfigured))
			err = c.WaitSession(ctx, time.Millisecond)
			expect("WaitSession before Configure", err, errors.Is(err, wamp.ErrNotConfigured))
			err = c.Stop(ctx)
			expect("Stop before Configure", err, err == nil)
			err = publish(c)
			expect("Publish after Stop before Configure", err, errors.Is(err, wamp.ErrNotConfigured))

			// A failed Start, then a successful one.
			c = newConn()
			err = c.Configure(cfg)
			expect("Configure", err, err == nil && c.URL() == tr.url)
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			err = c.Start(cancelled)
			expect("Start with a done context", err, errors.Is(err, context.Canceled) && !errors.Is(err, wamp.ErrStopped))
			expect("IsOpen after a failed Start", nil, !c.IsOpen())
			err = c.WaitSession(ctx, 10*time.Millisecond)
			expect("WaitSession after a failed Start", err, errors.Is(err, wamp.ErrNotConnected))
			err = c.Configure(cfg)
			expect("Configure after a Start", err, err != nil && !errors.Is(err, wamp.ErrStopped))
			err = c.Start(ctx)
			expect("Start after a failed Start", err, err == nil)
			expect("IsOpen", nil, c.IsOpen())
			err = c.Start(ctx)
			expect("Start while started", err, err != nil && !errors.Is(err, wamp.ErrStopped))
			err = publish(c)
			expect("Publish", err, err == nil)

			// Stop is final.
			err = c.Stop(ctx)
			expect("Stop", err, err == nil)
			expect("IsOpen after Stop", nil, !c.IsOpen())
			err = c.Start(ctx)
			expect("Start after Stop", err, errors.Is(err, wamp.ErrStopped))
			err = c.WaitSession(ctx, time.Millisecond)
			expect("WaitSession after Stop", err, errors.Is(err, wamp.ErrStopped))
			err = publish(c)
			expect("Publish after Stop", err, errors.Is(err, wamp.ErrStopped))
			_, err = c.Call(ctx, "com.example.contract", nil, nil, nil, time.Second)
			expect("Call after Stop", err, errors.Is(err, wamp.ErrStopped))
			err = c.Configure(cfg)
			expect("Configure after Stop", err, errors.Is(err, wamp.ErrStopped))
			err = c.Stop(ctx)
			expect("a second Stop", err, err == nil)
		})
	}
}

// A device function's result reaches the caller as every other payload of
// the package is sent (see normalize), through a real router: a map with
// integer keys, which the router cannot decode as the WAMP client's codec
// encodes it, would otherwise never be answered.
func TestDeviceFunctionResultsAreNormalizedOnTheWire(t *testing.T) {
	tr := newTestRouter(t, true)
	callee := startedRealFlock(t, tr)
	caller := startedRealFlock(t, tr)
	when := time.Date(2026, 10, 8, 14, 0, 0, 123456789, time.FixedZone("CEST", 2*3600))
	results := map[string]func() (any, error){
		"intkey":  func() (any, error) { return map[int]string{1: "a"}, nil },
		"time":    func() (any, error) { return Result{Kwargs: map[string]any{"at": when}}, nil },
		"utf8":    func() (any, error) { return "sensor\xff\xfeline", nil },
		"complex": func() (any, error) { return complex(1, 2), nil },
		"error":   func() (any, error) { return nil, errors.New("bad frame \xff") },
		"panic":   func() (any, error) { panic("bad frame \xff") },
	}
	for name, fn := range results {
		if _, err := callee.RegisterDeviceFunction(timeout(t, 5*time.Second), "ret_"+name,
			func(context.Context, *Invocation) (any, error) { return fn() }); err != nil {
			t.Fatal(err)
		}
	}
	call := func(name string) (*Result, error) {
		return caller.CallDeviceFunction(timeout(t, 3*time.Second), 12, "ret_"+name)
	}

	res, err := call("intkey")
	if err != nil || !reflect.DeepEqual(res.Value(), map[string]any{"1": "a"}) {
		t.Errorf("intkey: %v, %v", res, err)
	}
	res, err = call("time")
	if err != nil || res.Kwargs["at"] != "2026-10-08T12:00:00.123456789Z" {
		t.Errorf("time: %#v, %v", res, err)
	}
	res, err = call("utf8")
	if err != nil || res.Value() != "sensor��line" {
		t.Errorf("utf8: %#v, %v", res, err)
	}
	_, err = call("complex")
	var werr *WampError
	if !errors.As(err, &werr) || werr.URI != wamp.URIRuntimeError ||
		!strings.Contains(fmt.Sprint(werr.Args...), "cannot be sent: unsupported type complex128") {
		t.Errorf("complex: %v, want a prompt runtime_error", err)
	}
	_, err = call("error")
	if !errors.As(err, &werr) || werr.URI != wamp.URIRuntimeError || !reflect.DeepEqual(werr.Args, []any{"bad frame �"}) {
		t.Errorf("error: %#v", err)
	}
	_, err = call("panic")
	if !errors.As(err, &werr) || werr.URI != wamp.URIRuntimeError || len(werr.Args) != 1 ||
		!strings.HasSuffix(fmt.Sprint(werr.Args[0]), ".ret_panic' panicked: bad frame �") {
		t.Errorf("panic: %v", err)
	}
}

// registerStreamingRead registers a nexus callee for procedure that answers
// as fleetdb answers a large read: rows (newest first) in progressive results
// [chunkIndex, rows] of per rows each, and the summary. A caller that does
// not ask for progressive results is refused with result_too_large. After
// the first chunk the callee waits for hold, unless it is nil.
func registerStreamingRead(t *testing.T, cli *client.Client, procedure string, rows []any, per int, hold <-chan struct{}) {
	t.Helper()
	err := cli.Register(procedure, func(ctx context.Context, inv *nxwamp.Invocation) client.InvokeResult {
		if inv.Details[nxwamp.OptReceiveProgress] != true {
			return client.InvokeResult{Err: URIResultTooLarge, Args: nxwamp.List{"caller cannot receive progressive results"}}
		}
		chunks := 0
		for i := 0; i < len(rows); i += per {
			if err := cli.SendProgress(ctx, nxwamp.List{chunks, rows[i:min(i+per, len(rows))]}, nil); err != nil {
				return client.InvokeResult{Err: "test.error.progress", Args: nxwamp.List{err.Error()}}
			}
			chunks++
			if hold != nil {
				select {
				case <-hold:
				case <-ctx.Done():
					return client.InvokeResult{Err: nxwamp.ErrCanceled}
				}
			}
		}
		return client.InvokeResult{Args: nxwamp.List{map[string]any{
			"chunked": true, "chunkCount": chunks, "totalRows": len(rows), "order": "asc"}}}
	}, nil)
	if err != nil {
		t.Fatalf("Register %s: %v", procedure, err)
	}
}

// A large read through a real connection and router: the data backend's
// progressive results reach the read through the connection's OnProgress,
// and GetHistory returns the rows of a single read, in ascending order. A
// read whose context ends while it streams fails with the context's error,
// and the connection serves the next read.
func TestGetHistoryReassemblesAStreamedResult(t *testing.T) {
	tr := newTestRouter(t, true)
	f := startedRealFlock(t, tr)
	callee := tr.local(t)
	const n = 250
	rows := make([]any, n)
	for i := range rows {
		rows[i] = map[string]any{"tsp": int64(n - i), "payload": strings.Repeat("x", 100)}
	}
	registerStreamingRead(t, callee, "history.transformed.big", rows, 40, nil)
	hold := make(chan struct{})
	defer close(hold)
	registerStreamingRead(t, callee, "history.transformed.held", rows, 40, hold)

	got, err := f.GetHistory(timeout(t, 10*time.Second), "big", &TableQueryParams{Limit: n})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != n {
		t.Fatalf("%d rows, want %d", len(got), n)
	}
	for i, r := range got {
		if r["tsp"] != int64(i+1) {
			t.Fatalf("row %d has tsp %v: not in ascending order", i, r["tsp"])
		}
	}

	// Without progressive results the read is refused, as fleetdb refuses it.
	_, err = f.Connection().Call(timeout(t, 5*time.Second), "history.transformed.big", []any{map[string]any{"limit": n}}, nil, nil, 0)
	if WampURI(err) != URIResultTooLarge {
		t.Errorf("a call without OnProgress: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	if _, err := f.GetHistory(ctx, "held", &TableQueryParams{Limit: n}); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("a read whose context ends while it streams: %v", err)
	}
	if got, err := f.GetHistory(timeout(t, 10*time.Second), "big", &TableQueryParams{Limit: n}); err != nil || len(got) != n {
		t.Errorf("the next read: %d rows, %v", len(got), err)
	}
}
