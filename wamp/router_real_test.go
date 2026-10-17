package wamp

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ironflock/nexus/v3/client"
	nxwamp "github.com/ironflock/nexus/v3/wamp"
)

// Tests against a real router: the IronFlock router with the fake platform, as
// integration/router/start.sh runs them. They run only when
// IRONFLOCK_TEST_ROUTER_URL is set, e.g. to ws://localhost:18082/ws-ua-usr.
//
// The connection joins realm-2-26-dev with the harness's per-app credential
// (realRouterCredential), which the router admits with the production `app`
// role. That role registers only device-function URIs
// <swarm>.<device>.<app>.<STAGE>.<name>, and the router's identity check
// reserves them to the device they name, so the tests register and call
// functions of device 42 (realRouterFunction), whose credential it is, and
// publish and subscribe names of their own.
const realRouterRealm = "realm-2-26-dev"

// realRouterCredential returns the harness's per-app credential of device
// 42 for app 26 (DEV), as the device agent writes it into /data/env: the
// files in integration/router/perapp_env, read as the SDK reads
// /data/env.
func realRouterCredential(t *testing.T) (authID, secret string) {
	t.Helper()
	read := func(name string) string {
		data, err := os.ReadFile(filepath.Join("..", "integration", "router", "perapp_env", name+".txt"))
		if err != nil {
			t.Fatalf("the harness's per-app credential: %v", err)
		}
		return strings.TrimSpace(string(data))
	}
	return read("APP_AUTH_ID"), read("APP_AUTH_SECRET")
}

// realRouterFunction returns the URI of the device function name of the
// harness's device 42.
func realRouterFunction(name string) string { return "2.42.26.DEV." + name }

func realRouterConn(t *testing.T, url string, mutate func(*Config)) (*Connection, *logBuffer) {
	t.Helper()
	logs := &logBuffer{}
	c := NewConnection()
	c.t.initialRetryDelay = 50 * time.Millisecond
	c.t.retryFirstDelay = 50 * time.Millisecond
	authID, secret := realRouterCredential(t)
	cfg := Config{
		SwarmKey:     2,
		AppKey:       26,
		Stage:        StageDevelopment,
		URL:          url,
		SerialNumber: "router-real-test",
		AuthID:       authID,
		AuthSecret:   secret,
		Logger:       slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
	}
	if mutate != nil {
		mutate(&cfg)
	}
	if err := c.Configure(cfg); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	t.Cleanup(func() {
		if err := c.Stop(ctxTimeout(t, 10*time.Second)); err != nil {
			t.Errorf("Stop: %v", err)
		}
		if t.Failed() {
			t.Logf("connection log:\n%s", logs)
		}
	})
	return c, logs
}

func TestRealRouter(t *testing.T) {
	routerURL := os.Getenv("IRONFLOCK_TEST_ROUTER_URL")
	if routerURL == "" {
		t.Skip("IRONFLOCK_TEST_ROUTER_URL not set")
	}
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())

	// The connection under test reaches the router through a proxy that can
	// drop its TCP connection: a transport loss, which the reconnect subtest
	// needs (the app role may not kill sessions).
	proxy := newDropProxy(t, routerURL)
	connects := make(chan struct{}, 8)
	c, logs := realRouterConn(t, proxy.url, func(cfg *Config) {
		cfg.OnConnect = func() { connects <- struct{}{} }
	})
	if err := c.Start(ctxTimeout(t, 15*time.Second)); err != nil {
		t.Fatalf("Start: %v\n%s", err, logs)
	}
	recv(t, connects, "OnConnect")
	if c.Realm() != realRouterRealm || !c.IsOpen() {
		t.Fatalf("realm %q open %v", c.Realm(), c.IsOpen())
	}
	ctx := ctxTimeout(t, 60*time.Second)

	topic := "com.example.gosdk.topic." + suffix
	handler, events := eventCollector(16)
	sub, err := c.Subscribe(ctx, topic, handler, nil)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	publish := func(t *testing.T, seq int) {
		t.Helper()
		f := false
		err := c.Publish(ctx, topic, []any{seq, "two", 3.5}, map[string]any{"foo": "bar", "nested": map[string]any{"n": 1}},
			&PublishOptions{Acknowledge: true, ExcludeMe: &f}, 0)
		if err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}

	t.Run("publish subscribe round trip", func(t *testing.T) {
		publish(t, 1)
		ev := recv(t, events, "own event")
		if ev.Topic != topic || !reflect.DeepEqual(ev.Args, []any{int64(1), "two", 3.5}) {
			t.Fatalf("event = %+v", ev)
		}
		if !reflect.DeepEqual(ev.Kwargs, map[string]any{"foo": "bar", "nested": map[string]any{"n": int64(1)}}) {
			t.Fatalf("kwargs = %#v", ev.Kwargs)
		}
	})

	proc := realRouterFunction("gosdk_add_" + suffix)
	reg, err := c.Register(ctx, proc, func(_ context.Context, inv *Invocation) (any, error) {
		a, _ := inv.Args[0].(int64)
		b, _ := inv.Args[1].(int64)
		if b < 0 {
			return nil, &Error{URI: "app.error.negative", Args: []any{"b must be positive"}, Kwargs: map[string]any{"b": b}}
		}
		return Result{Args: []any{a + b}, Kwargs: map[string]any{"caller_known": inv.Details["caller"] != nil}}, nil
	}, nil)
	if err != nil {
		t.Fatalf("Register: %v", err)
	}

	t.Run("register call round trip", func(t *testing.T) {
		res, err := c.Call(ctx, proc, []any{20, 22}, nil, nil, 0)
		if err != nil {
			t.Fatalf("Call: %v", err)
		}
		if res.Value() != int64(42) {
			t.Fatalf("result = %#v", res.Args)
		}
		_, err = c.Call(ctx, proc, []any{1, -1}, nil, nil, 0)
		var werr *Error
		if !errors.As(err, &werr) || werr.URI != "app.error.negative" ||
			!reflect.DeepEqual(werr.Args, []any{"b must be positive"}) ||
			!reflect.DeepEqual(werr.Kwargs, map[string]any{"b": int64(-1)}) {
			t.Fatalf("callee error = %#v", err)
		}
	})

	t.Run("identity check refuses another device's function", func(t *testing.T) {
		_, err := c.Register(ctx, "2.43.26.DEV.gosdk_other_"+suffix, echoHandler(""), nil)
		var werr *Error
		if !errors.As(err, &werr) || werr.URI != URINotAuthorized {
			t.Fatalf("Register of device 43's function = %v, want %s", err, URINotAuthorized)
		}
	})

	t.Run("force_reregister takes over", func(t *testing.T) {
		other, _ := realRouterConn(t, routerURL, nil)
		if err := other.Start(ctxTimeout(t, 15*time.Second)); err != nil {
			t.Fatal(err)
		}
		_, err := other.Register(ctx, proc, func(context.Context, *Invocation) (any, error) { return "taken over", nil }, nil)
		if err != nil {
			t.Fatalf("force_reregister by a second session: %v", err)
		}
		eventually(t, 5*time.Second, "the evicted registration to turn inactive", func() bool { return !reg.Active() })
		res, err := c.Call(ctx, proc, []any{1, 1}, nil, nil, 0)
		if err != nil || res.Value() != "taken over" {
			t.Fatalf("Call after takeover = %v, %v", res, err)
		}
		if err := other.Stop(ctxTimeout(t, 10*time.Second)); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("reconnect restores", func(t *testing.T) {
		proxy.dropAll()
		recv(t, connects, "OnConnect after the transport was dropped")
		if !sub.Active() || !reg.Active() {
			t.Fatalf("not restored: sub=%v reg=%v", sub.Active(), reg.Active())
		}
		// The registration was taken over and released by the subtest
		// above; the restore registered it again.
		res, err := c.Call(ctx, proc, []any{2, 3}, nil, nil, 2*time.Second)
		if err != nil || res.Value() != int64(5) {
			t.Fatalf("Call after reconnect = %v, %v", res, err)
		}
		publish(t, 2)
		if ev := recv(t, events, "event after reconnect"); ev.Args[0] != int64(2) {
			t.Fatalf("event = %+v", ev)
		}
	})

	t.Run("unsubscribe", func(t *testing.T) {
		if err := sub.Unsubscribe(ctx); err != nil {
			t.Fatalf("Unsubscribe: %v", err)
		}
		if sub.Active() {
			t.Fatal("still active")
		}
		publish(t, 3)
		noRecv(t, events, "event after unsubscribe")
	})

	t.Run("unregister", func(t *testing.T) {
		if err := reg.Unregister(ctx); err != nil {
			t.Fatalf("Unregister: %v", err)
		}
		_, err := c.Call(ctx, proc, []any{1, 2}, nil, nil, 0)
		var werr *Error
		if !errors.As(err, &werr) || werr.URI != URINoSuchProcedure {
			t.Fatalf("Call after Unregister = %v", err)
		}
	})

	t.Run("retry window reaches a late registration", func(t *testing.T) {
		late := realRouterFunction("gosdk_late_" + suffix)
		other, _ := realRouterConn(t, routerURL, nil)
		if err := other.Start(ctxTimeout(t, 15*time.Second)); err != nil {
			t.Fatal(err)
		}
		const retried = "Procedure not registered yet; retrying"
		before := logs.count(retried)
		type outcome struct {
			res *Result
			err error
		}
		called := make(chan outcome, 1)
		go func() {
			res, err := c.Call(ctx, late, nil, nil, nil, 10*time.Second)
			called <- outcome{res, err}
		}()
		eventually(t, 5*time.Second, "a no_such_procedure retry", func() bool { return logs.count(retried) > before })
		if _, err := other.Register(ctx, late, func(context.Context, *Invocation) (any, error) { return "late", nil }, nil); err != nil {
			t.Fatalf("Register: %v", err)
		}
		if got := recv(t, called, "the retried call"); got.err != nil || got.res.Value() != "late" {
			t.Fatalf("Call = %v, %v", got.res, got.err)
		}
	})

	// Callers on another connection that poll a device function while it is
	// being registered are served as soon as it is, never refused because
	// their INVOCATION came right behind REGISTERED.
	t.Run("register race with polling callers", func(t *testing.T) {
		callers, _ := realRouterConn(t, routerURL, nil)
		if err := callers.Start(ctxTimeout(t, 15*time.Second)); err != nil {
			t.Fatal(err)
		}
		var mu sync.Mutex
		outcomes := map[string]int{}
		var firstRefusal error
		const rounds, pollers = 50, 4
		for i := range rounds {
			proc := realRouterFunction(fmt.Sprintf("gosdk_race%d_%s", i, suffix))
			var wg sync.WaitGroup
			for range pollers {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for ctx.Err() == nil {
						_, err := callers.Call(ctx, proc, nil, nil, nil, 0)
						key := "ok"
						var werr *Error
						if errors.As(err, &werr) {
							key = werr.URI
						} else if err != nil {
							key = err.Error()
						}
						mu.Lock()
						outcomes[key]++
						if key == "wamp.error.invalid_argument" && firstRefusal == nil {
							firstRefusal = err
						}
						mu.Unlock()
						if key != URINoSuchProcedure {
							return
						}
					}
				}()
			}
			reg, err := c.Register(ctx, proc, func(context.Context, *Invocation) (any, error) { return "ok", nil }, nil)
			if err != nil {
				t.Fatalf("round %d: Register: %v", i, err)
			}
			wg.Wait()
			if err := reg.Unregister(ctx); err != nil {
				t.Fatalf("round %d: Unregister: %v", i, err)
			}
		}
		mu.Lock()
		defer mu.Unlock()
		t.Logf("outcomes: %v", outcomes)
		if n := outcomes["wamp.error.invalid_argument"]; n > 0 {
			t.Fatalf("%d calls refused right after REGISTERED; first: %v", n, firstRefusal)
		}
		if outcomes["ok"] != rounds*pollers {
			t.Fatalf("served %d calls, want %d", outcomes["ok"], rounds*pollers)
		}
	})

	// The router passes a callee's progressive results on to a caller that
	// asks for them with OnProgress, as fleetdb streams a large read.
	t.Run("progressive results", func(t *testing.T) {
		callee, _ := realRouterConn(t, routerURL, nil)
		if err := callee.Start(ctxTimeout(t, 15*time.Second)); err != nil {
			t.Fatal(err)
		}
		cli := callee.currentSession().cli // a nexus callee that can send progressive results
		streamed := realRouterFunction("gosdk_progress_" + suffix)
		if err := cli.Register(streamed, func(ctx context.Context, _ *nxwamp.Invocation) client.InvokeResult {
			for i := range 3 {
				if err := cli.SendProgress(ctx, nxwamp.List{i}, nil); err != nil {
					return client.InvokeResult{Err: "test.error.progress", Args: nxwamp.List{err.Error()}}
				}
			}
			return client.InvokeResult{Args: nxwamp.List{"done"}}
		}, nxwamp.Dict{}); err != nil {
			t.Fatalf("Register: %v", err)
		}
		var got []any
		res, err := c.Call(ctx, streamed, nil, nil, &CallOptions{OnProgress: func(r *Result) { got = append(got, r.Value()) }}, 0)
		if err != nil || res.Value() != "done" {
			t.Fatalf("Call = %v, %v", res, err)
		}
		if want := []any{int64(0), int64(1), int64(2)}; !reflect.DeepEqual(got, want) {
			t.Fatalf("progressive results %v, want %v", got, want)
		}
	})

	t.Run("wrong secret is fatal with FailOnAuthError", func(t *testing.T) {
		var failures atomic.Int32
		bad, _ := realRouterConn(t, routerURL, func(cfg *Config) {
			cfg.AuthSecret = "wrong-secret"
			cfg.FailOnAuthError = true
			cfg.OnAuthFailure = func(string) { failures.Add(1) }
		})
		err := bad.Start(ctxTimeout(t, 15*time.Second))
		var authErr *AuthError
		if !errors.As(err, &authErr) || !IsFatalAuthReason(authErr.Reason) {
			t.Fatalf("Start = %v, want *AuthError", err)
		}
		eventually(t, 5*time.Second, "OnAuthFailure", func() bool { return failures.Load() == 1 })
		t.Logf("the router refused the credential with %s", authErr.Reason)
	})

	t.Run("missing realm is not fatal", func(t *testing.T) {
		var mu sync.Mutex
		var reasons []string
		missing, _ := realRouterConn(t, routerURL, func(cfg *Config) {
			cfg.SwarmKey, cfg.AppKey = 999999, 999999
			cfg.FailOnAuthError = true
			cfg.FirstConnectTimeout = 1500 * time.Millisecond
		})
		missing.t.onRetry = func(reason string, _ time.Duration) {
			mu.Lock()
			reasons = append(reasons, reason)
			mu.Unlock()
		}
		err := missing.Start(context.Background())
		if !errors.Is(err, ErrNotConnected) || !strings.Contains(err.Error(), "within 1.5s") {
			t.Fatalf("Start = %v, want the first-connect timeout", err)
		}
		mu.Lock()
		if len(reasons) == 0 || reasons[0] != URINoSuchRealm {
			t.Fatalf("refusal reasons = %v, want %s", reasons, URINoSuchRealm)
		}
		mu.Unlock()
		// Not fatal: the connection can be started again (and waits again).
		if err := missing.WaitSession(context.Background(), time.Millisecond); !errors.Is(err, ErrNotConnected) {
			t.Fatalf("WaitSession after the failed Start = %v, want ErrNotConnected", err)
		}
		err = missing.Start(context.Background())
		if !errors.Is(err, ErrNotConnected) || errors.Is(err, ErrStopped) {
			t.Fatalf("second Start = %v, want the first-connect timeout again", err)
		}
	})
}

// dropProxy forwards TCP connections to a router and drops all of them on
// request: a transport loss the router did not cause. In stall mode it
// accepts new connections but never answers them, like a load balancer
// whose upstream is gone. Frozen, it stops moving bytes on the connections
// it forwards, like a dead link that does not reset them.
type dropProxy struct {
	ln     net.Listener
	target string
	url    string // the router URL with the proxy as its host

	stall   atomic.Bool   // accept new connections, read their request, never answer
	stalled chan struct{} // receives a value for each request a stalled connection sent
	frozen  atomic.Bool   // stop reading the forwarded connections

	mu     sync.Mutex
	conns  []net.Conn
	closed bool
	wg     sync.WaitGroup
}

func newDropProxy(t *testing.T, routerURL string) *dropProxy {
	t.Helper()
	u, err := url.Parse(routerURL)
	if err != nil || u.Host == "" {
		t.Fatalf("router URL %q: %v", routerURL, err)
	}
	if u.Port() == "" {
		t.Fatalf("router URL %q: the proxy needs an explicit port", routerURL)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &dropProxy{ln: ln, target: u.Host, stalled: make(chan struct{}, 16)}
	u.Host = ln.Addr().String()
	p.url = u.String()
	p.wg.Add(1)
	go p.serve()
	t.Cleanup(p.close)
	return p
}

func (p *dropProxy) serve() {
	defer p.wg.Done()
	for {
		down, err := p.ln.Accept()
		if err != nil {
			return
		}
		if p.stall.Load() {
			if !p.track(1, down) {
				return
			}
			go p.hold(down)
			continue
		}
		up, err := net.DialTimeout("tcp", p.target, 5*time.Second)
		if err != nil {
			_ = down.Close()
			continue
		}
		if !p.track(2, down, up) {
			return
		}
		go p.pipe(up, down)
		go p.pipe(down, up)
	}
}

// track registers conns and their n goroutines, or closes conns if the
// proxy is closed.
func (p *dropProxy) track(n int, conns ...net.Conn) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		for _, c := range conns {
			_ = c.Close()
		}
		return false
	}
	p.conns = append(p.conns, conns...)
	p.wg.Add(n)
	return true
}

// hold reads the HTTP request of a stalled connection, reports it, and then
// only drains the connection until it is closed.
func (p *dropProxy) hold(conn net.Conn) {
	defer p.wg.Done()
	br := bufio.NewReader(conn)
	if _, err := http.ReadRequest(br); err == nil {
		select {
		case p.stalled <- struct{}{}:
		default:
		}
	}
	_, _ = io.Copy(io.Discard, br)
}

func (p *dropProxy) pipe(dst, src net.Conn) {
	defer p.wg.Done()
	defer func() {
		_ = dst.Close()
		_ = src.Close()
	}()
	buf := make([]byte, 32<<10)
	for {
		for p.frozen.Load() {
			if p.isClosed() {
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
		n, err := src.Read(buf)
		if n > 0 {
			if _, werr := dst.Write(buf[:n]); werr != nil {
				return
			}
		}
		if err != nil {
			return
		}
	}
}

func (p *dropProxy) isClosed() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.closed
}

// dropAll closes every proxied connection, both sides.
func (p *dropProxy) dropAll() {
	p.mu.Lock()
	conns := p.conns
	p.conns = nil
	p.mu.Unlock()
	for _, c := range conns {
		_ = c.Close()
	}
}

func (p *dropProxy) close() {
	p.mu.Lock()
	p.closed = true
	p.mu.Unlock()
	_ = p.ln.Close()
	p.dropAll()
	p.wg.Wait()
}
