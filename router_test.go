package ironflock

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
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
	ln       *connListener
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
	tr.srv = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		tr.handlers.Add(1)
		defer tr.handlers.Done()
		wss.ServeHTTP(w, req)
	}))
	tr.ln = &connListener{Listener: tr.srv.Listener}
	tr.srv.Listener = tr.ln
	tr.srv.Start()
	tr.url = "ws" + strings.TrimPrefix(tr.srv.URL, "http") + "/ws-ua-usr"
	t.Cleanup(func() {
		tr.srv.Close()
		tr.ln.drop(false)
		tr.handlers.Wait()
		tr.r.Close()
	})
	return tr
}

// connListener remembers the connections it accepts, so that a test can drop
// them — httptest does not close hijacked (WebSocket) connections — and
// refuses new ones while it is paused.
type connListener struct {
	net.Listener
	mu     sync.Mutex
	conns  []net.Conn
	paused bool
}

func (l *connListener) Accept() (net.Conn, error) {
	for {
		conn, err := l.Listener.Accept()
		if err != nil {
			return nil, err
		}
		l.mu.Lock()
		paused := l.paused
		if !paused {
			l.conns = append(l.conns, conn)
		}
		l.mu.Unlock()
		if !paused {
			return conn, nil
		}
		_ = conn.Close()
	}
}

// drop closes every connection accepted so far, and with pause refuses new
// ones until resume.
func (l *connListener) drop(pause bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.paused = pause
	for _, c := range l.conns {
		_ = c.Close()
	}
	l.conns = nil
}

func (l *connListener) resume() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.paused = false
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

// serveAppends registers procedure on a local client as the data backend
// serves an append procedure: each row it is called with goes to the
// returned channel, and the call is answered {"success": true}.
func serveAppends(t *testing.T, tr *testRouter, procedure string) <-chan any {
	t.Helper()
	rows := make(chan any, 8)
	if err := tr.local(t).Register(procedure, func(_ context.Context, inv *nxwamp.Invocation) client.InvokeResult {
		rows <- inv.Arguments[0]
		return client.InvokeResult{Args: nxwamp.List{map[string]any{"success": true}}}
	}, nil); err != nil {
		t.Fatalf("local register %s: %v", procedure, err)
	}
	return rows
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
	rows := serveAppends(t, tr, "append.1.2.sensordata")
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
	rows := serveAppends(t, tr, "append.1.2.sensordata")
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

// --- subscription handles and the reconnect window, through a real connection

// lookup asks the router for the ID of the subscription of topic (meta
// procedure wamp.subscription.lookup) or of the registration of procedure
// (wamp.registration.lookup); 0 means there is none.
func (tr *testRouter) lookup(t *testing.T, meta nxwamp.URI, uri string) nxwamp.ID {
	t.Helper()
	res, err := tr.local(t).Call(timeout(t, 5*time.Second), string(meta), nil, nxwamp.List{uri}, nil, nil)
	if err != nil {
		t.Fatalf("%s(%s): %v", meta, uri, err)
	}
	if len(res.Arguments) == 0 {
		return 0
	}
	id, _ := nxwamp.AsID(res.Arguments[0])
	return id
}

// envFreeRealFlock is realFlock without the identity environment: every
// setting is an option, so that a parallel test can use it (t.Setenv rules
// out t.Parallel). It is not started, and it is stopped when the test ends.
func envFreeRealFlock(t *testing.T, tr *testRouter, opts ...Option) *IronFlock {
	t.Helper()
	logger, _ := newTestLogger()
	f, err := New(append([]Option{
		WithSerialNumber("test-serial-123"), WithDeviceKey("12"), WithDeviceName("test-device"),
		WithAppName("TestApp"), WithURL(tr.url), WithCredentials(routerAuthID, routerSecret),
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

// A TableSubscription is active while both of its feeds are live at the
// router: not while the connection is down, again once the reconnect has
// restored them, not without its bulk feed, and not after Unsubscribe.
func TestTableSubscriptionIsActiveWhileBothFeedsAreLive(t *testing.T) {
	tr := newTestRouter(t, true)
	ft := newFakeTable(t, tr, "t")
	f := startedRealFlock(t, tr)
	ctx := timeout(t, 20*time.Second)
	ts, err := f.SubscribeToTable(ctx, "t", func(*Event) {})
	if err != nil {
		t.Fatal(err)
	}
	if ts.Table() != "t" {
		t.Errorf("Table() = %q", ts.Table())
	}
	if !ts.Active() || !ts.Rows.Active() || !ts.Bulk.Active() {
		t.Fatalf("not active once subscribed: rows %v, bulk %v", ts.Rows.Active(), ts.Bulk.Active())
	}
	eventually(t, "the baseline read", func() bool { return ft.store.returned.Load() == 1 })

	tr.ln.drop(true)
	eventuallyWithin(t, 5*time.Second, "the connection to drop", func() bool { return !f.IsConnected() })
	if ts.Active() {
		t.Error("active while the connection is down")
	}
	tr.ln.resume()
	eventuallyWithin(t, 10*time.Second, "both feeds to be restored", ts.Active)

	// Both feeds or none: without the bulk feed the handle is not active.
	if err := ts.Bulk.Unsubscribe(ctx); err != nil {
		t.Fatal(err)
	}
	if ts.Active() || !ts.Rows.Active() {
		t.Errorf("Active() = %v with the row feed (active %v) only", ts.Active(), ts.Rows.Active())
	}
	if err := f.Unsubscribe(ctx, ts); err != nil {
		t.Fatal(err)
	}
	if ts.Active() || ts.Rows.Active() {
		t.Error("active after Unsubscribe")
	}
}

// While the connection is down, Unsubscribe and Unregister do not wait for
// it: they remove the handle locally and return at once. The handles removed
// meanwhile are not restored after the reconnect, while the other handles of
// the same topic and table are, and keep their events.
func TestUnsubscribeAndUnregisterDoNotWaitForTheConnection(t *testing.T) {
	tr := newTestRouter(t, true)
	ft := newFakeTable(t, tr, "t")
	solo := newFakeTable(t, tr, "solo")
	f := startedRealFlock(t, tr)
	ctx := timeout(t, 30*time.Second)

	// One delivery group for every handler: once the barrier's event has been
	// handled, so has every event published before it to a handler that is
	// subscribed (rows held for a catch-up aside).
	opts := SubscribeOptions{Group: wamp.NewDeliveryGroup()}
	gone, kept := &received{}, &received{}
	goneRows, keptRows := &received{}, &received{}
	sub1, err := f.Subscribe(ctx, "com.example.topic", gone.handle, opts)
	if err != nil {
		t.Fatal(err)
	}
	sub2, err := f.Subscribe(ctx, "com.example.topic", kept.handle, opts)
	if err != nil {
		t.Fatal(err)
	}
	ts1, err := f.SubscribeToTable(ctx, "t", goneRows.handle, opts)
	if err != nil {
		t.Fatal(err)
	}
	ts2, err := f.SubscribeToTable(ctx, "t", keptRows.handle, opts)
	if err != nil {
		t.Fatal(err)
	}
	soloSub, err := f.SubscribeToTable(ctx, "solo", goneRows.handle, opts)
	if err != nil {
		t.Fatal(err)
	}
	reg, err := f.RegisterDeviceFunction(ctx, "toggle", func(context.Context, *Invocation) (any, error) { return "on", nil })
	if err != nil {
		t.Fatal(err)
	}
	barrier := make(chan struct{}, 4)
	if _, err := f.Subscribe(ctx, "test.barrier", func(*Event) { barrier <- struct{}{} }, opts); err != nil {
		t.Fatal(err)
	}
	eventually(t, "the baseline reads", func() bool { return ft.store.returned.Load() == 2 && solo.store.returned.Load() == 1 })

	tr.ln.drop(true)
	eventuallyWithin(t, 5*time.Second, "the connection to drop", func() bool { return !f.IsConnected() })
	for _, removal := range []struct {
		name   string
		remove func(context.Context) error
	}{
		{"IronFlock.Unsubscribe(*Subscription)", func(ctx context.Context) error { return f.Unsubscribe(ctx, sub1) }},
		{"IronFlock.Unsubscribe(*TableSubscription)", func(ctx context.Context) error { return f.Unsubscribe(ctx, ts1) }},
		{"TableSubscription.Unsubscribe", soloSub.Unsubscribe},
		{"IronFlock.Unregister", func(ctx context.Context) error { return f.Unregister(ctx, reg) }},
	} {
		began := time.Now()
		if err := removal.remove(timeout(t, 3*time.Second)); err != nil {
			t.Errorf("%s while disconnected: %v", removal.name, err)
		}
		if took := time.Since(began); took > 500*time.Millisecond {
			t.Errorf("%s took %v while disconnected: it waited for the connection", removal.name, took)
		}
	}
	if f.IsConnected() {
		t.Fatal("reconnected while removing: the removals were not made while disconnected")
	}

	tr.ln.resume()
	eventuallyWithin(t, 10*time.Second, "the kept handles to be restored", func() bool { return sub2.Active() && ts2.Active() })
	for name, active := range map[string]bool{
		"Subscription": sub1.Active(), "TableSubscription": ts1.Active() || ts1.Rows.Active() || ts1.Bulk.Active(),
		"TableSubscription of a table of its own": soloSub.Active(), "Registration": reg.Active(),
	} {
		if active {
			t.Errorf("the %s removed while disconnected was restored after the reconnect", name)
		}
	}
	// The kept table subscription catches up first; then nothing it could
	// read again is stored.
	eventuallyWithin(t, 5*time.Second, "the catch-up read", func() bool { return ft.store.returned.Load() == 3 })

	pub := tr.local(t)
	publish(t, pub, "com.example.topic", "after the reconnect")
	ft.put(trow(100))
	publish(t, pub, "transformed.bulk.t", []any{map[string]any(trow(101))})
	solo.put(trow(5))
	publish(t, pub, "test.barrier")
	recv(t, barrier, "the barrier")
	eventually(t, "the rows of the kept table subscription", func() bool { return len(keptRows.events()) == 2 })
	wantTsps(t, keptRows, 100, 101)
	if n := len(kept.events()); n != 1 {
		t.Errorf("the kept Subscription got %d events, want 1", n)
	}
	if n, m := len(gone.events()), len(goneRows.events()); n+m != 0 {
		t.Errorf("handlers removed while disconnected got %d events and %d rows", n, m)
	}
	if n := len(ft.reads()); n != 3 {
		t.Errorf("%d reads of table t, want the two baselines and the kept handle's catch-up", n)
	}
	for _, topic := range []string{"transformed.solo", "transformed.bulk.solo"} {
		if id := tr.lookup(t, nxwamp.MetaProcSubLookup, topic); id != 0 {
			t.Errorf("%s is subscribed again after the reconnect", topic)
		}
	}
	if id := tr.lookup(t, nxwamp.MetaProcRegLookup, "1.12.2.DEV.toggle"); id != 0 {
		t.Error("the device function is registered again after the reconnect")
	}
	if _, err := pub.Call(ctx, "1.12.2.DEV.toggle", nil, nil, nil, nil); err == nil {
		t.Error("the unregistered device function answered")
	}

	// A second removal is a no-op, as is a removal after Stop.
	if err := errors.Join(f.Unsubscribe(ctx, sub1), f.Unsubscribe(ctx, ts1), soloSub.Unsubscribe(ctx), f.Unregister(ctx, reg)); err != nil {
		t.Errorf("a second removal: %v", err)
	}
	if err := f.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	began := time.Now()
	if err := errors.Join(f.Unsubscribe(ctx, sub2), f.Unsubscribe(ctx, ts2)); err != nil || time.Since(began) > 500*time.Millisecond {
		t.Errorf("removals after Stop: %v after %v", err, time.Since(began))
	}
}

// Two handles on one topic are independent while connected too: removing a
// table subscription leaves another of the same table, and a plain
// subscription of one of its feeds, with their events. The router keeps a
// feed's subscription until its last handle is gone.
func TestHandlesOnOneTopicAreIndependent(t *testing.T) {
	tr := newTestRouter(t, true)
	ft := newFakeTable(t, tr, "t")
	f := startedRealFlock(t, tr)
	ctx := timeout(t, 10*time.Second)
	opts := SubscribeOptions{Group: wamp.NewDeliveryGroup()} // see TestUnsubscribeAndUnregisterDoNotWaitForTheConnection
	first, second, raw := &received{}, &received{}, &received{}
	ts1, err := f.SubscribeToTable(ctx, "t", first.handle, opts)
	if err != nil {
		t.Fatal(err)
	}
	ts2, err := f.SubscribeToTable(ctx, "t", second.handle, opts)
	if err != nil {
		t.Fatal(err)
	}
	bulk, err := f.Subscribe(ctx, "transformed.bulk.t", raw.handle, opts)
	if err != nil {
		t.Fatal(err)
	}
	barrier := make(chan struct{}, 4)
	if _, err := f.Subscribe(ctx, "test.barrier", func(*Event) { barrier <- struct{}{} }, opts); err != nil {
		t.Fatal(err)
	}
	eventually(t, "the baseline reads", func() bool { return ft.store.returned.Load() == 2 })

	if err := f.Unsubscribe(ctx, ts1); err != nil {
		t.Fatal(err)
	}
	if ts1.Active() || !ts2.Active() || !bulk.Active() {
		t.Fatalf("active: removed %v, kept %v, plain %v", ts1.Active(), ts2.Active(), bulk.Active())
	}
	pub := tr.local(t)
	ft.put(trow(1))
	publish(t, pub, "transformed.bulk.t", []any{map[string]any(trow(2))})
	publish(t, pub, "test.barrier")
	recv(t, barrier, "the barrier")
	wantTsps(t, second, 1, 2)
	if n, m := len(first.events()), len(raw.events()); n != 0 || m != 1 {
		t.Errorf("the removed handle got %d events, the plain subscription %d (want 0 and 1)", n, m)
	}
	for _, topic := range []string{"transformed.t", "transformed.bulk.t"} {
		if tr.lookup(t, nxwamp.MetaProcSubLookup, topic) == 0 {
			t.Errorf("%s was unsubscribed at the router while another handle has it", topic)
		}
	}

	if err := f.Unsubscribe(ctx, ts2); err != nil {
		t.Fatal(err)
	}
	if tr.lookup(t, nxwamp.MetaProcSubLookup, "transformed.t") != 0 {
		t.Error("transformed.t is still subscribed at the router after its last handle")
	}
	if tr.lookup(t, nxwamp.MetaProcSubLookup, "transformed.bulk.t") == 0 {
		t.Error("transformed.bulk.t was unsubscribed at the router while the plain subscription has it")
	}
	if err := f.Unsubscribe(ctx, bulk); err != nil {
		t.Fatal(err)
	}
	if tr.lookup(t, nxwamp.MetaProcSubLookup, "transformed.bulk.t") != 0 {
		t.Error("transformed.bulk.t is still subscribed at the router after its last handle")
	}
}

// opsOn returns the operations of gatedOps that keep accepts, on f.
func opsOn(f *IronFlock, keep func(gatedOp) bool) []gatedOp {
	var ops []gatedOp
	for _, op := range gatedOps() {
		if keep(op) {
			do := op.do
			op.do = func(*testFlock) error { return do(&testFlock{IronFlock: f}) }
			ops = append(ops, op)
		}
	}
	return ops
}

// Without a reconnect window the operations that use it — the table
// operations, Publish, Subscribe, SubscribeToTable, RegisterDeviceFunction —
// behave exactly like the others: they wait the default 10s for a
// connection — not less, as the Python and JavaScript SDKs used to — and
// then fail with an error that names the wait.
func TestWithoutAReconnectWindowTableOperationsWaitTheDefaultTenSeconds(t *testing.T) {
	if testing.Short() {
		t.Skip("waits the default 10s for a connection")
	}
	t.Parallel() // the wait overlaps the other parallel tests'
	tr := newTestRouter(t, true)
	f := envFreeRealFlock(t, tr, WithReconnectWindow(0))
	if err := f.Start(timeout(t, 5*time.Second)); err != nil {
		t.Fatal(err)
	}
	tr.ln.drop(true)
	eventuallyWithin(t, 5*time.Second, "the connection to drop", func() bool { return !f.IsConnected() })

	type outcome struct {
		name string
		err  error
		took time.Duration
	}
	ops := opsOn(f, gatedOp.windowed)
	results := make(chan outcome, len(ops))
	for _, op := range ops {
		go func() {
			began := time.Now()
			err := op.do(nil)
			results <- outcome{op.name, err, time.Since(began)}
		}()
	}
	const wait = wamp.DefaultSessionWaitTimeout
	if wait != 10*time.Second {
		t.Fatalf("DefaultSessionWaitTimeout = %v, want 10s", wait)
	}
	for range ops {
		o := recvWithin(t, results, 2*wait, "an operation")
		if !errors.Is(o.err, wamp.ErrNotConnected) || !strings.Contains(fmt.Sprint(o.err), "no session after 10s") {
			t.Errorf("%s = %v, want the not-connected error after 10s", o.name, o.err)
		}
		if o.took < wait || o.took > wait+3*time.Second {
			t.Errorf("%s failed after %v, want after the default %v", o.name, o.took, wait)
		}
	}
}

// Without a reconnect window a table call the data backend does not serve
// fails at once, not retried; with one it is retried until the data backend
// serves it.
func TestWithoutAReconnectWindowTableCallsAreNotRetried(t *testing.T) {
	tr := newTestRouter(t, true)
	f := startedRealFlock(t, tr, WithReconnectWindow(0))
	for _, op := range opsOn(f, func(op gatedOp) bool { return op.kind == tableOp }) {
		began := time.Now()
		err := op.do(nil)
		took := time.Since(began)
		if err != nil && WampURI(err) != wamp.URINoSuchProcedure {
			t.Errorf("%s = %v", op.name, err)
		}
		if took >= wamp.RetryFirstDelay {
			t.Errorf("%s took %v without a reconnect window: it was retried", op.name, took)
		}
	}

	g := startedRealFlock(t, tr, WithReconnectWindow(5*time.Second))
	const late = 300 * time.Millisecond
	callee := tr.local(t)
	registered := make(chan error, 1)
	go func() {
		time.Sleep(late)
		registered <- callee.Register("append.1.2.late", func(context.Context, *nxwamp.Invocation) client.InvokeResult {
			return client.InvokeResult{Args: nxwamp.List{map[string]any{"success": true}}}
		}, nil)
	}()
	began := time.Now()
	_, err := g.AppendToTable(timeout(t, 10*time.Second), "late", Row{"tsp": int64(1)})
	took := time.Since(began)
	if rerr := recv(t, registered, "the late registration"); rerr != nil {
		t.Fatal(rerr)
	}
	if err != nil {
		t.Fatalf("an append the data backend serves late, within the window: %v", err)
	}
	if took < late {
		t.Errorf("served after %v, before the procedure was registered", took)
	}
}

// Stop ends every wait at once, with an error wrapping wamp.ErrStopped: of an
// operation waiting for the connection — a table operation, Publish,
// Subscribe or RegisterDeviceFunction within its window, any other within
// its 10s — and of a table call between its retries.
func TestStopEndsEveryWait(t *testing.T) {
	trUp, trDown := newTestRouter(t, true), newTestRouter(t, true)
	up := startedRealFlock(t, trUp, WithReconnectWindow(time.Hour))
	down := startedRealFlock(t, trDown, WithReconnectWindow(time.Hour))
	trDown.ln.drop(true)
	eventuallyWithin(t, 5*time.Second, "the connection to drop", func() bool { return !down.IsConnected() })

	type outcome struct {
		name string
		err  error
	}
	results := make(chan outcome, 64)
	n := 0
	run := func(name string, op func() error) {
		n++
		go func() { results <- outcome{name, op()} }()
	}
	run("AppendToTable between its retries", func() error {
		_, err := up.AppendToTable(bg, "unserved", Row{"tsp": int64(1)})
		return err
	})
	for _, op := range gatedOps() {
		run(op.name+" waiting for the connection", func() error { return op.do(&testFlock{IronFlock: down}) })
	}
	// The append retries after 0.5s, then 1s, then 2s: by now it is in its
	// third backoff, and the others wait for the connection.
	time.Sleep(1600 * time.Millisecond)
	if len(results) != 0 {
		o := <-results
		t.Fatalf("%s ended before Stop: %v", o.name, o.err)
	}

	began := time.Now()
	stops := make(chan error, 2)
	for _, f := range []*IronFlock{up, down} {
		go func() { stops <- f.Stop(bg) }()
	}
	for range n {
		o := recv(t, results, "an operation to end")
		if !errors.Is(o.err, wamp.ErrStopped) {
			t.Errorf("%s = %v, want an error wrapping ErrStopped", o.name, o.err)
		}
	}
	if took := time.Since(began); took > time.Second {
		t.Errorf("the waits ended %v after Stop", took)
	}
	for range 2 {
		if err := recv(t, stops, "Stop"); err != nil {
			t.Errorf("Stop: %v", err)
		}
	}
}

// Stop ends the waits for the connection at once, also while a consumed
// app's connection is still closing: it stops the connection alongside the
// consumed apps, not after them.
func TestStopEndsTheWaitsWhileAConsumedAppCloses(t *testing.T) {
	tr := newTestRouter(t, true)
	setIdentityEnv(t)
	closing, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	factory := &fakeFactory{setup: func(c *fakeConn) {
		c.stopFn = func(ctx context.Context) error { // a goodbye the provider's router is slow to answer
			once.Do(func() { close(closing) })
			select {
			case <-release:
			case <-ctx.Done():
			}
			return nil
		}
	}}
	defer close(release)
	logger, _ := newTestLogger()
	f, err := newWithConn(wamp.NewConnection(), factory.newConn, WithURL(tr.url),
		WithCredentials(routerAuthID, routerSecret), WithSwarmKey(1), WithAppKey(2), WithEnv("DEV"),
		WithLogger(logger), WithReconnectWindow(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { // after a failure; Stop is idempotent
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = f.Stop(ctx)
	})
	if err := tr.local(t).Register(uriAppAccessResolve, func(context.Context, *nxwamp.Invocation) client.InvokeResult {
		return client.InvokeResult{Args: nxwamp.List{resolveResult()}}
	}, nil); err != nil {
		t.Fatal(err)
	}
	if err := f.Start(timeout(t, 5*time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := f.ConnectToApp(timeout(t, 5*time.Second), "weatherstation"); err != nil {
		t.Fatal(err)
	}
	tr.ln.drop(true)
	eventuallyWithin(t, 5*time.Second, "the connection to drop", func() bool { return !f.IsConnected() })
	waiting := make(chan error, 1)
	go func() { waiting <- f.PublishToTable(bg, "sensordata", Row{"tsp": int64(1)}) }()
	time.Sleep(50 * time.Millisecond)

	stopped := make(chan error, 1)
	go func() { stopped <- f.Stop(bg) }()
	recv(t, closing, "the consumed app to begin closing")
	select {
	case err := <-waiting:
		if !errors.Is(err, wamp.ErrStopped) {
			t.Errorf("PublishToTable = %v, want an error wrapping ErrStopped", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the wait for the connection outlasts Stop while a consumed app closes")
	}
	select {
	case <-stopped:
		t.Fatal("Stop returned before the consumed app had closed")
	default:
	}
	release <- struct{}{} // the goodbye is answered
	if err := recv(t, stopped, "Stop"); err != nil {
		t.Error(err)
	}
}

// backendComingBack registers procedure on a local client as a data backend
// that is coming back after a restart serves it: the first call is refused as
// the router refuses a call of a procedure not registered yet, the second as
// a nexus callee refuses a call that overtook its registration, and the
// third succeeds. It returns the number of calls served.
func backendComingBack(t *testing.T, tr *testRouter, procedure string) *atomic.Int32 {
	t.Helper()
	var calls atomic.Int32
	if err := tr.local(t).Register(procedure, func(context.Context, *nxwamp.Invocation) client.InvokeResult {
		switch calls.Add(1) {
		case 1:
			return client.InvokeResult{Err: nxwamp.ErrNoSuchProcedure}
		case 2:
			return client.InvokeResult{Err: nxwamp.ErrInvalidArgument, Args: nxwamp.List{"client has no handler for registration 4711"}}
		}
		return client.InvokeResult{Args: nxwamp.List{map[string]any{"success": true}}}
	}, nil); err != nil {
		t.Fatalf("local register %s: %v", procedure, err)
	}
	return &calls
}

// A table publish is an append: a row written while the data backend
// registers its procedures again is retried, not lost — and nothing is
// published to the table topics.
func TestATablePublishRidesOutABackendRestart(t *testing.T) {
	tr := newTestRouter(t, true)
	f := startedRealFlock(t, tr)
	published := make(chan string, 8)
	watcher := tr.local(t)
	for _, topic := range []string{"1.2.sensordata", "bulk.1.2.sensordata", "1.2.error-logs", "test.barrier"} {
		if err := watcher.Subscribe(topic, func(*nxwamp.Event) { published <- topic }, nil); err != nil {
			t.Fatalf("local subscribe %s: %v", topic, err)
		}
	}
	ops := []struct {
		name, procedure string
		do              func() error
	}{
		{"PublishToTable", "append.1.2.sensordata", func() error {
			return f.PublishToTable(timeout(t, 10*time.Second), "sensordata", Row{"tsp": int64(1)})
		}},
		{"PublishRowsToTable", "appendBulk.1.2.sensordata", func() error {
			return f.PublishRowsToTable(timeout(t, 10*time.Second), "sensordata", []Row{{"tsp": int64(1)}})
		}},
		{"ReportError", "append.1.2.error-logs", func() error {
			_, err := f.ReportError(timeout(t, 10*time.Second), "boom")
			return err
		}},
	}
	t.Run("ops", func(t *testing.T) {
		for _, op := range ops {
			calls := backendComingBack(t, tr, op.procedure)
			t.Run(op.name, func(t *testing.T) {
				t.Parallel() // each waits out two retry delays
				if err := op.do(); err != nil {
					t.Fatalf("%s across a data backend restart: %v", op.name, err)
				}
				if n := calls.Load(); n != 3 {
					t.Errorf("%s called %s %d times, want 3", op.name, op.procedure, n)
				}
			})
		}
	})
	// The router delivers one publisher's events in order: a publication to a
	// table topic would arrive before the barrier.
	if err := f.Publish(timeout(t, 5*time.Second), "test.barrier"); err != nil {
		t.Fatal(err)
	}
	if topic := recv(t, published, "the barrier"); topic != "test.barrier" {
		t.Errorf("published to %s", topic)
	}
}

// A refused table publish fails with an error that names the publish and,
// for a refusal without a reason, points at the missing tsp. It is not
// retried: the call reached the data backend.
func TestARefusedTablePublishFails(t *testing.T) {
	tr := newTestRouter(t, true)
	f := startedRealFlock(t, tr)
	var calls atomic.Int32
	if err := tr.local(t).Register("append.1.2.sensordata", func(context.Context, *nxwamp.Invocation) client.InvokeResult {
		calls.Add(1)
		return client.InvokeResult{Err: nxwamp.URI(wamp.URIRuntimeError), Args: nxwamp.List{map[string]any{}}}
	}, nil); err != nil {
		t.Fatal(err)
	}
	err := f.PublishToTable(timeout(t, 5*time.Second), "sensordata", Row{"temperature": 21.5})
	if err == nil || !strings.HasPrefix(err.Error(), "Publish to table 'sensordata' failed") || !strings.Contains(err.Error(), "missing tsp") {
		t.Errorf("a refused PublishToTable = %v, want the publish named and the tsp hint", err)
	}
	if WampURI(err) != wamp.URIRuntimeError {
		t.Errorf("WampURI = %q", WampURI(err))
	}
	if n := calls.Load(); n != 1 {
		t.Errorf("called %d times, want 1", n)
	}
}
