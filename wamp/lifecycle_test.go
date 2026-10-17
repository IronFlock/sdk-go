package wamp

import (
	"context"
	"errors"
	"math/rand/v2"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	nxwamp "github.com/ironflock/nexus/v3/wamp"
)

func TestJoinWampCRAOverMsgpack(t *testing.T) {
	tr := newTestRouter(t, true)
	c, _ := startTestConn(t, tr)

	if !c.IsOpen() {
		t.Fatal("IsOpen = false after Start")
	}
	if got := tr.auth.attempts(); !slices.Equal(got, []string{testAuthID}) {
		t.Fatalf("HELLO authids = %v, want [%s]", got, testAuthID)
	}
	if got := tr.protocols(); len(got) != 1 || got[0] != "wamp.2.msgpack" {
		t.Fatalf("offered WebSocket subprotocols = %q, want only wamp.2.msgpack", got)
	}
	if c.Realm() != testRealm || c.SerialNumber() != testSerial || c.URL() != tr.url {
		t.Fatalf("accessors: %q %q %q", c.Realm(), c.SerialNumber(), c.URL())
	}
	if err := c.WaitSession(context.Background(), 0); err != nil {
		t.Fatalf("WaitSession: %v", err)
	}
}

func TestPrimaryConnectionRetriesWrongSecret(t *testing.T) {
	tr := newTestRouter(t, true)
	c, _ := newTestConn(t, tr, func(cfg *Config, _ *Connection) { cfg.AuthSecret = "wrong" })

	err := c.Start(ctxTimeout(t, 300*time.Millisecond))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Start = %v, want deadline exceeded", err)
	}
	if n := len(tr.auth.attempts()); n < 2 {
		t.Fatalf("join attempts = %d, want retries", n)
	}
	// A failed Start stops the supervisor, not the connection: it is
	// configured and waits to be started again.
	if err := c.WaitSession(context.Background(), time.Millisecond); !errors.Is(err, ErrNotConnected) {
		t.Fatalf("WaitSession after failed Start = %v, want ErrNotConnected", err)
	}
	// Once the router accepts the secret, a new Start joins.
	tr.keys.set(testAuthID, "wrong")
	if err := c.Start(ctxTimeout(t, 5*time.Second)); err != nil {
		t.Fatalf("Start after a failed Start: %v", err)
	}
	if !c.IsOpen() {
		t.Fatal("not open after the second Start")
	}
}

// A non-fatal failed Start leaves the connection configured but not
// started: no further attempts are made, operations wait for a session, and
// Start may be called again — whatever ended the first one.
func TestStartAfterFailedStart(t *testing.T) {
	tests := []struct {
		name    string
		opt     connOption
		ctx     func(t *testing.T) context.Context
		wantErr func(error) bool
	}{
		{
			name:    "ctx deadline while the realm is missing",
			ctx:     func(t *testing.T) context.Context { return ctxTimeout(t, 200*time.Millisecond) },
			wantErr: func(err error) bool { return errors.Is(err, context.DeadlineExceeded) },
		},
		{
			name: "ctx cancelled before Start",
			ctx: func(t *testing.T) context.Context {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return ctx
			},
			wantErr: func(err error) bool { return errors.Is(err, context.Canceled) },
		},
		{
			name: "first-connect timeout with FailOnAuthError",
			opt: func(cfg *Config, _ *Connection) {
				cfg.FailOnAuthError = true
				cfg.FirstConnectTimeout = 300 * time.Millisecond
			},
			ctx:     func(*testing.T) context.Context { return context.Background() },
			wantErr: func(err error) bool { return errors.Is(err, ErrNotConnected) && !errors.Is(err, ErrStopped) },
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tr := newTestRouter(t, false)
			var dials atomic.Int32
			opts := []connOption{countDials(&dials)}
			if tt.opt != nil {
				opts = append(opts, tt.opt)
			}
			c, logs := newTestConn(t, tr, opts...)

			err := c.Start(tt.ctx(t))
			if !tt.wantErr(err) {
				t.Fatalf("first Start = %v", err)
			}
			if !closed(c.done) {
				t.Fatal("supervisor still running after Start returned")
			}
			if stillDialing(&dials) {
				t.Fatal("still connecting after a failed Start")
			}
			if c.IsOpen() {
				t.Fatal("open after a failed Start")
			}
			if err := c.WaitSession(context.Background(), time.Millisecond); !errors.Is(err, ErrNotConnected) {
				t.Fatalf("WaitSession after a failed Start = %v, want ErrNotConnected", err)
			}

			tr.addRealm()
			if err := c.Start(ctxTimeout(t, 5*time.Second)); err != nil {
				t.Fatalf("second Start: %v\n%s", err, logs)
			}
			if !c.IsOpen() {
				t.Fatal("not open after the second Start")
			}
			err = c.Publish(ctxTimeout(t, 5*time.Second), "after.restart", nil, nil, &PublishOptions{Acknowledge: true}, 0)
			if err != nil {
				t.Fatalf("Publish after the second Start: %v", err)
			}
		})
	}
}

// countDials counts the connection's TCP dials. Dials run on its
// supervisor, so once the supervisor has exited the count is final; the
// router's side would also count a request of an attempt that was abandoned
// as Start returned, which reaches the server later (finding 37).
func countDials(dials *atomic.Int32) connOption {
	return func(_ *Config, c *Connection) { c.t.onDial = func() { dials.Add(1) } }
}

// stillDialing reports whether the connection whose dials are counted dials
// again within several retry periods of fastTunables.
func stillDialing(dials *atomic.Int32) bool {
	n := dials.Load()
	time.Sleep(60 * time.Millisecond)
	return dials.Load() != n
}

// The check of TestStartAfterFailedStart sees a connection that keeps
// retrying.
func TestStillDialingSeesRetries(t *testing.T) {
	tr := newTestRouter(t, false)
	var dials atomic.Int32
	c, _ := newTestConn(t, tr, countDials(&dials))
	started := make(chan error, 1)
	go func() { started <- c.Start(context.Background()) }()
	eventually(t, 5*time.Second, "a dial", func() bool { return dials.Load() > 0 })
	if !stillDialing(&dials) {
		t.Fatal("no dial seen from a connection that keeps retrying")
	}
	if err := c.Stop(ctxTimeout(t, 5*time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := recv(t, started, "Start"); !errors.Is(err, ErrStopped) {
		t.Fatalf("Start = %v, want ErrStopped", err)
	}
}

// An operation waiting for a session outlives a failed Start: it is served
// by the session of the next Start within its window.
func TestOperationWaitsAcrossFailedStart(t *testing.T) {
	tr := newTestRouter(t, false)
	c, _ := newTestConn(t, tr) // the default 10s session wait
	handler, events := eventCollector(4)
	subscribed := make(chan error, 1)
	go func() {
		_, err := c.Subscribe(context.Background(), "across.restart", handler, nil)
		subscribed <- err
	}()

	if err := c.Start(ctxTimeout(t, 200*time.Millisecond)); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("first Start = %v, want deadline exceeded", err)
	}
	noRecv(t, subscribed, "Subscribe outcome before the next Start")

	tr.addRealm()
	if err := c.Start(ctxTimeout(t, 5*time.Second)); err != nil {
		t.Fatalf("second Start: %v", err)
	}
	if err := recv(t, subscribed, "Subscribe"); err != nil {
		t.Fatalf("Subscribe across the failed Start: %v", err)
	}
	localPublish(t, tr.local(t), "across.restart", nxwamp.List{"hello"}, nil)
	if ev := recv(t, events, "event"); ev.Args[0] != "hello" {
		t.Fatalf("event = %+v", ev)
	}
}

// Stop is final even when it races a failing Start: whichever finishes
// first, the connection ends up stopped and Start returns ErrStopped.
func TestStopRacingFailedStartIsFinal(t *testing.T) {
	tr := newTestRouter(t, false)
	for i := range 20 {
		c, _ := newTestConn(t, tr)
		ctx, cancel := context.WithCancel(context.Background())
		started := make(chan error, 1)
		before := len(tr.protocols())
		go func() { started <- c.Start(ctx) }()
		if i%4 != 0 { // otherwise possibly before the first attempt
			eventually(t, 5*time.Second, "an attempt", func() bool { return len(tr.protocols()) > before })
		}
		stopped := make(chan error, 1)
		if i%2 == 0 {
			go cancel()
			go func() { stopped <- c.Stop(ctxTimeout(t, 5*time.Second)) }()
		} else {
			go func() { stopped <- c.Stop(ctxTimeout(t, 5*time.Second)) }()
			go cancel()
		}
		if err := recv(t, stopped, "Stop"); err != nil {
			t.Fatalf("round %d: Stop: %v", i, err)
		}
		if err := recv(t, started, "Start"); err == nil {
			t.Fatalf("round %d: Start succeeded without a realm", i)
		}
		if err := c.Start(ctxTimeout(t, 5*time.Second)); !errors.Is(err, ErrStopped) {
			t.Fatalf("round %d: Start after Stop = %v, want ErrStopped", i, err)
		}
		if err := c.WaitSession(context.Background(), time.Millisecond); !errors.Is(err, ErrStopped) {
			t.Fatalf("round %d: WaitSession after Stop = %v, want ErrStopped", i, err)
		}
	}
}

// waitParkedOnMutex waits until a goroutine running fn is parked on a
// sync.Mutex, and reports an error to t if none is within 5s. It may run on
// any goroutine.
func waitParkedOnMutex(t *testing.T, fn string) {
	buf := make([]byte, 1<<20)
	deadline := time.Now().Add(5 * time.Second)
	for {
		n := runtime.Stack(buf, true)
		if n == len(buf) {
			buf = make([]byte, 2*len(buf))
			continue
		}
		for _, g := range strings.Split(string(buf[:n]), "\n\n") {
			header, _, _ := strings.Cut(g, "\n")
			if strings.Contains(header, "[sync.Mutex.Lock") && strings.Contains(g, fn) {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Errorf("no goroutine in %s parked on a mutex", fn)
			return
		}
		time.Sleep(time.Millisecond)
	}
}

// When Start's wait ends — its ctx, or FirstConnectTimeout — just as the
// first join completes, Start reports the session it announced, instead of
// failing and closing it without OnDisconnect (finding 36). The test makes
// the wait end while the supervisor publishes the session, and lets the
// supervisor go on only once Start has decided to give up.
func TestStartSucceedsWhenTheSessionComesUpAsTheWaitEnds(t *testing.T) {
	tests := []struct {
		name string
		opt  func(t *testing.T, cancel context.CancelFunc) connOption
	}{
		{"ctx", func(t *testing.T, cancel context.CancelFunc) connOption {
			return func(_ *Config, c *Connection) {
				c.t.beforePublish = func() {
					cancel()
					waitParkedOnMutex(t, "(*Connection).abortStart")
				}
			}
		}},
		{"first-connect timeout", func(t *testing.T, _ context.CancelFunc) connOption {
			return func(cfg *Config, c *Connection) {
				cfg.FailOnAuthError = true
				cfg.FirstConnectTimeout = 100 * time.Millisecond
				c.t.beforePublish = func() { waitParkedOnMutex(t, "(*Connection).abortStart") }
			}
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tr := newTestRouter(t, true)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			connects := make(chan struct{}, 4)
			disconnects := make(chan string, 4)
			var hooked sync.Once
			c, logs := newTestConn(t, tr, func(cfg *Config, c *Connection) {
				cfg.OnConnect = func() { connects <- struct{}{} }
				cfg.OnDisconnect = func(reason string) { disconnects <- reason }
				tt.opt(t, cancel)(cfg, c)
				hook := c.t.beforePublish
				c.t.beforePublish = func() { hooked.Do(hook) }
			})
			if err := c.Start(ctx); err != nil {
				t.Fatalf("Start = %v, want nil: the session came up\n%s", err, logs)
			}
			recv(t, connects, "OnConnect")
			if !c.IsOpen() {
				t.Fatal("not open after a successful Start")
			}
			if err := c.Publish(ctxTimeout(t, 5*time.Second), "after.start", nil, nil, &PublishOptions{Acknowledge: true}, 0); err != nil {
				t.Fatalf("Publish on the session: %v", err)
			}
			noRecv(t, disconnects, "OnDisconnect")
			noRecv(t, connects, "a second OnConnect")
		})
	}
}

// Starts with very short deadlines, most of which end around the join: each
// session announced is a successful Start's, so one successful Start means
// one OnConnect, and no OnDisconnect.
func TestStartStormAnnouncesOnlyTheSuccessfulStart(t *testing.T) {
	tr := newTestRouter(t, true)
	for round := range 40 {
		var connects, disconnects atomic.Int32
		c, _ := newTestConn(t, tr, func(cfg *Config, _ *Connection) {
			cfg.OnConnect = func() { connects.Add(1) }
			cfg.OnDisconnect = func(string) { disconnects.Add(1) }
		})
		for attempt := 0; ; attempt++ {
			if attempt == 1000 {
				t.Fatalf("round %d: no Start succeeded", round)
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(rand.IntN(5))*time.Millisecond)
			err := c.Start(ctx)
			cancel()
			if err == nil {
				break
			}
		}
		if err := c.Stop(ctxTimeout(t, 5*time.Second)); err != nil {
			t.Fatal(err)
		}
		// The supervisors have exited, so every callback is queued: run one
		// behind them.
		barrier := make(chan struct{})
		c.runCallback("barrier", func() { close(barrier) })
		recv(t, barrier, "the callbacks")
		if n, m := connects.Load(), disconnects.Load(); n != 1 || m != 0 {
			t.Fatalf("round %d: OnConnect called %d times and OnDisconnect %d times for one successful Start", round, n, m)
		}
	}
}

func TestFailOnAuthErrorStopsAtWrongSecret(t *testing.T) {
	tr := newTestRouter(t, true)
	var calls atomic.Int32
	reasons := make(chan string, 4)
	c, logs := newTestConn(t, tr, func(cfg *Config, _ *Connection) {
		cfg.AuthSecret = "wrong"
		cfg.FailOnAuthError = true
		cfg.OnAuthFailure = func(reason string) {
			calls.Add(1)
			reasons <- reason
		}
	})

	err := c.Start(ctxTimeout(t, 5*time.Second))
	var authErr *AuthError
	if !errors.As(err, &authErr) {
		t.Fatalf("Start = %T %v, want *AuthError", err, err)
	}
	if authErr.Realm != testRealm || authErr.Reason != URIAuthenticationFailed {
		t.Fatalf("AuthError = %+v", authErr)
	}
	if got := recv(t, reasons, "OnAuthFailure"); got != URIAuthenticationFailed {
		t.Fatalf("OnAuthFailure(%q)", got)
	}
	time.Sleep(100 * time.Millisecond) // several retry periods
	if n := len(tr.auth.attempts()); n != 1 {
		t.Fatalf("join attempts = %d, want exactly 1\n%s", n, logs)
	}
	if n := calls.Load(); n != 1 {
		t.Fatalf("OnAuthFailure called %d times", n)
	}
	err = c.WaitSession(context.Background(), time.Millisecond)
	if !errors.Is(err, ErrStopped) || !errors.As(err, &authErr) {
		t.Fatalf("WaitSession = %v, want ErrStopped wrapping *AuthError", err)
	}
	if _, err := c.Call(context.Background(), "x.y", nil, nil, nil, 0); !errors.Is(err, ErrStopped) {
		t.Fatalf("Call after fatal auth = %v", err)
	}
	// A fatal denial is final: Start cannot be retried.
	err = c.Start(ctxTimeout(t, 5*time.Second))
	if !errors.Is(err, ErrStopped) || !errors.As(err, &authErr) {
		t.Fatalf("Start after fatal auth = %v, want ErrStopped wrapping *AuthError", err)
	}
	if n := len(tr.auth.attempts()); n != 1 {
		t.Fatalf("join attempts after the retried Start = %d, want 1", n)
	}
}

// reasonAccessRevoked is the close reason of a session whose cross-app grant
// the platform revoked; the platform then refuses the session's re-HELLO
// with wamp.error.authentication_failed.
const reasonAccessRevoked = "ironflock.close.access_revoked"

// denialGrace shortens the time a refusal must persist to be final.
func denialGrace(grace time.Duration) connOption {
	return func(_ *Config, c *Connection) { c.t.authDenialGrace = grace }
}

// Once a FailOnAuthError connection has been established, a refusal is not
// final at once: the router refuses with authentication_failed also
// while it cannot verify the credential (an authenticator or database
// outage). A refusal that passes is ridden out like any reconnect (finding
// 20).
func TestTransientDenialAfterJoinIsRiddenOut(t *testing.T) {
	tr := newTestRouter(t, true)
	connects := make(chan struct{}, 4)
	disconnects := make(chan string, 4)
	failures := make(chan string, 4)
	c, logs := startTestConn(t, tr, denialGrace(time.Minute), func(cfg *Config, _ *Connection) {
		cfg.FailOnAuthError = true
		cfg.OnConnect = func() { connects <- struct{}{} }
		cfg.OnDisconnect = func(reason string) { disconnects <- reason }
		cfg.OnAuthFailure = func(reason string) { failures <- reason }
	})
	recv(t, connects, "OnConnect")
	handler, events := eventCollector(4)
	if _, err := c.Subscribe(ctxTimeout(t, 5*time.Second), "after.outage", handler, nil); err != nil {
		t.Fatal(err)
	}

	// The connection drops while the platform cannot verify the credential.
	tr.keys.set(testAuthID, "unverifiable")
	joins := len(tr.auth.attempts())
	tr.ln.dropAll()
	recv(t, disconnects, "OnDisconnect")
	eventually(t, 5*time.Second, "refused re-HELLOs", func() bool { return len(tr.auth.attempts()) >= joins+3 })
	tr.keys.set(testAuthID, testSecret)

	recv(t, connects, "OnConnect once the platform verifies again")
	noRecv(t, failures, "OnAuthFailure for a refusal that passed")
	if !c.IsOpen() {
		t.Fatal("not open")
	}
	localPublish(t, tr.local(t), "after.outage", nxwamp.List{"back"}, nil)
	recv(t, events, "an event on the restored subscription")
	if !strings.Contains(logs.String(), "final only once it persists") {
		t.Fatalf("the refusal grace not logged:\n%s", logs)
	}
}

// The platform's revocation: the session is closed with
// ironflock.close.access_revoked, and every re-HELLO is refused. That
// persists: once refused for the grace — at least 3 attempts — the
// connection stops for good, and OnAuthFailure is called once.
func TestPersistentDenialAfterJoinIsFinal(t *testing.T) {
	tr := newTestRouter(t, true)
	const grace = 300 * time.Millisecond
	connects := make(chan struct{}, 4)
	disconnects := make(chan string, 4)
	var failures atomic.Int32
	failed := make(chan string, 4)
	c, logs := startTestConn(t, tr, denialGrace(grace), func(cfg *Config, _ *Connection) {
		cfg.FailOnAuthError = true
		cfg.OnConnect = func() { connects <- struct{}{} }
		cfg.OnDisconnect = func(reason string) { disconnects <- reason }
		cfg.OnAuthFailure = func(reason string) {
			failures.Add(1)
			failed <- reason
		}
	})
	recv(t, connects, "OnConnect")
	joins := len(tr.auth.attempts())

	tr.keys.set(testAuthID, "revoked")
	revoked := time.Now()
	if n := tr.r.KillSessionsByAuthrole(testRealm, "device", reasonAccessRevoked, "access revoked", 0); n != 1 {
		t.Fatalf("killed %d sessions", n)
	}
	if got := recv(t, disconnects, "OnDisconnect"); got != reasonAccessRevoked {
		t.Fatalf("OnDisconnect(%q), want %q", got, reasonAccessRevoked)
	}
	if got := recv(t, failed, "OnAuthFailure"); got != URIAuthenticationFailed {
		t.Fatalf("OnAuthFailure(%q), want %q", got, URIAuthenticationFailed)
	}
	if el := time.Since(revoked); el < grace {
		t.Fatalf("final after %v, before the refusals lasted %v\n%s", el, grace, logs)
	}
	refused := len(tr.auth.attempts()) - joins
	if refused < 3 {
		t.Fatalf("final after %d refused attempts, want at least 3", refused)
	}

	err := c.WaitSession(context.Background(), 50*time.Millisecond)
	var authErr *AuthError
	if !errors.Is(err, ErrStopped) || !errors.As(err, &authErr) || authErr.Reason != URIAuthenticationFailed {
		t.Fatalf("WaitSession = %v, want ErrStopped wrapping the *AuthError", err)
	}
	if _, err := c.Call(context.Background(), "x.y", nil, nil, nil, 0); !errors.Is(err, ErrStopped) {
		t.Fatalf("Call = %v, want ErrStopped", err)
	}
	time.Sleep(60 * time.Millisecond) // several retry periods
	if n := len(tr.auth.attempts()) - joins; n != refused {
		t.Fatalf("%d attempts after the denial was final", n-refused)
	}
	if n := failures.Load(); n != 1 {
		t.Fatalf("OnAuthFailure called %d times", n)
	}
	noRecv(t, connects, "a reconnect")
}

// A session the router ends with an auth reason is not final by itself
// either: the next join decides, and here it succeeds.
func TestAuthCloseAfterJoinReconnects(t *testing.T) {
	tr := newTestRouter(t, true)
	connects := make(chan struct{}, 4)
	disconnects := make(chan string, 4)
	failures := make(chan string, 4)
	c, _ := startTestConn(t, tr, func(cfg *Config, _ *Connection) {
		cfg.FailOnAuthError = true
		cfg.OnConnect = func() { connects <- struct{}{} }
		cfg.OnDisconnect = func(reason string) { disconnects <- reason }
		cfg.OnAuthFailure = func(reason string) { failures <- reason }
	})
	recv(t, connects, "OnConnect")
	if n := tr.r.KillSessionsByAuthrole(testRealm, "device", URINotAuthorized, "denied", 0); n != 1 {
		t.Fatalf("killed %d sessions", n)
	}
	if got := recv(t, disconnects, "OnDisconnect"); got != URINotAuthorized {
		t.Fatalf("OnDisconnect(%q), want %q", got, URINotAuthorized)
	}
	recv(t, connects, "OnConnect after the reconnect")
	noRecv(t, failures, "OnAuthFailure")
	if !c.IsOpen() {
		t.Fatal("not open")
	}
}

func TestNonFatalSessionCloseReconnectsWithFailOnAuthError(t *testing.T) {
	tr := newTestRouter(t, true)
	connects := make(chan struct{}, 4)
	c, _ := startTestConn(t, tr, func(cfg *Config, _ *Connection) {
		cfg.FailOnAuthError = true
		cfg.OnConnect = func() { connects <- struct{}{} }
	})
	recv(t, connects, "first OnConnect")
	tr.ln.dropAll()
	recv(t, connects, "OnConnect after transport loss")
	if !c.IsOpen() {
		t.Fatal("not reconnected")
	}
}

func TestCredentialsResolvedPerAttempt(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("IRONFLOCK_ENV_DIR", dir)
	t.Setenv("APP_AUTH_ID", "")
	t.Setenv("APP_AUTH_SECRET", "")
	write := func(id, secret string) {
		t.Helper()
		for name, v := range map[string]string{"APP_AUTH_ID": id, "APP_AUTH_SECRET": secret} {
			if err := os.WriteFile(filepath.Join(dir, name+".txt"), []byte(v+"\n"), 0o600); err != nil {
				t.Fatal(err)
			}
		}
	}
	write("stale-id", "stale-secret")

	tr := newTestRouter(t, true)
	tr.keys.set("rotated-id", "rotated-secret")
	c, _ := newTestConn(t, tr, func(cfg *Config, _ *Connection) { cfg.AuthID, cfg.AuthSecret = "", "" })

	started := make(chan error, 1)
	go func() { started <- c.Start(context.Background()) }()
	eventually(t, 5*time.Second, "a refused attempt", func() bool { return len(tr.auth.attempts()) >= 2 })
	write("rotated-id", "rotated-secret")
	if err := recv(t, started, "Start"); err != nil {
		t.Fatalf("Start: %v", err)
	}
	got := tr.auth.attempts()
	if got[0] != "stale-id" || got[len(got)-1] != "rotated-id" {
		t.Fatalf("authids = %v, want stale-id first and rotated-id last", got)
	}
}

func TestLegacySerialCredentialFallback(t *testing.T) {
	t.Setenv("IRONFLOCK_ENV_DIR", t.TempDir())
	t.Setenv("APP_AUTH_ID", "")
	t.Setenv("APP_AUTH_SECRET", "")
	tr := newTestRouter(t, true)
	tr.keys.set(testSerial, testSerial)
	startTestConn(t, tr, func(cfg *Config, _ *Connection) { cfg.AuthID, cfg.AuthSecret = "", "" })
	if got := tr.auth.attempts(); got[len(got)-1] != testSerial {
		t.Fatalf("authids = %v, want the serial number", got)
	}
}

func TestStartWaitsForMissingRealm(t *testing.T) {
	tr := newTestRouter(t, false)
	var refusals atomic.Int32
	c, _ := newTestConn(t, tr, func(_ *Config, c *Connection) {
		c.t.onRetry = func(reason string, _ time.Duration) {
			if reason == URINoSuchRealm {
				refusals.Add(1)
			}
		}
	})
	started := make(chan error, 1)
	go func() { started <- c.Start(context.Background()) }()

	eventually(t, 5*time.Second, "no_such_realm refusals", func() bool { return refusals.Load() >= 3 })
	select {
	case err := <-started:
		t.Fatalf("Start returned while the realm is missing: %v", err)
	default:
	}
	tr.addRealm()
	if err := recv(t, started, "Start"); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if !c.IsOpen() {
		t.Fatal("not open")
	}
}

func TestNoSuchRealmStreakWidensAndResets(t *testing.T) {
	tr := newTestRouter(t, false)
	type retry struct {
		reason string
		delay  time.Duration
	}
	var mu sync.Mutex
	var retries []retry
	snapshot := func() []retry {
		mu.Lock()
		defer mu.Unlock()
		return slices.Clone(retries)
	}
	c, logs := newTestConn(t, tr, func(_ *Config, c *Connection) {
		c.t.baseMaxRetryDelay = 10 * time.Millisecond
		c.t.noSuchRealmBackoffAfter = 60 * time.Millisecond
		c.t.noSuchRealmMaxRetryDelay = 40 * time.Millisecond
		c.t.onRetry = func(reason string, d time.Duration) {
			mu.Lock()
			retries = append(retries, retry{reason, d})
			mu.Unlock()
		}
	})
	started := make(chan error, 1)
	go func() { started <- c.Start(context.Background()) }()

	eventually(t, 5*time.Second, "the widened cap", func() bool {
		rs := snapshot()
		return len(rs) > 0 && rs[len(rs)-1].delay == 40*time.Millisecond
	})
	if n := logs.count("still does not exist after 0.06s; slowing reconnect attempts to at most one per 0.04s"); n != 1 {
		t.Fatalf("slow-down logged %d times\n%s", n, logs)
	}
	for _, r := range snapshot() {
		if r.reason != URINoSuchRealm {
			t.Fatalf("unexpected retry reason %q", r.reason)
		}
	}

	// The realm appears: the join resets the streak ...
	tr.addRealm()
	if err := recv(t, started, "Start"); err != nil {
		t.Fatalf("Start: %v", err)
	}
	mu.Lock()
	retries = nil
	mu.Unlock()

	// ... so when it disappears again the fast cap applies at first.
	tr.removeRealm()
	eventually(t, 5*time.Second, "retries after the realm vanished", func() bool { return len(snapshot()) >= 3 })
	rs := snapshot()
	if rs[0].delay != c.t.initialRetryDelay {
		t.Fatalf("first delay after a join = %v, want %v", rs[0].delay, c.t.initialRetryDelay)
	}
	for _, r := range rs[:3] {
		if r.delay > 10*time.Millisecond {
			t.Fatalf("cap not reset after a join: %+v", rs)
		}
	}
}

func TestFirstConnectTimeoutWithFailOnAuthError(t *testing.T) {
	tr := newTestRouter(t, false)
	var dials atomic.Int32
	c, _ := newTestConn(t, tr, countDials(&dials), func(cfg *Config, _ *Connection) {
		cfg.FailOnAuthError = true
		cfg.FirstConnectTimeout = 200 * time.Millisecond
	})
	start := time.Now()
	err := c.Start(context.Background())
	if err == nil || err.Error() != "failed to connect to realm realm-1-2-dev within 0.2s" {
		t.Fatalf("Start = %v", err)
	}
	if !errors.Is(err, ErrNotConnected) {
		t.Fatalf("Start error does not wrap ErrNotConnected: %v", err)
	}
	if time.Since(start) > 2*time.Second {
		t.Fatalf("Start took %v", time.Since(start))
	}
	// Torn down: no further attempts (the router refuses them for the
	// missing realm before authentication, so only dials show them), but not
	// stopped for good.
	if stillDialing(&dials) {
		t.Fatal("still connecting after a failed Start")
	}
	if err := c.WaitSession(context.Background(), time.Millisecond); !errors.Is(err, ErrNotConnected) {
		t.Fatalf("WaitSession = %v, want ErrNotConnected", err)
	}
}

func TestStopUnblocksStart(t *testing.T) {
	tr := newTestRouter(t, false)
	c, _ := newTestConn(t, tr)
	started := make(chan error, 1)
	go func() { started <- c.Start(context.Background()) }()
	eventually(t, 5*time.Second, "an attempt", func() bool { return tr.protocols() != nil })
	if err := c.Stop(ctxTimeout(t, 5*time.Second)); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if err := recv(t, started, "Start"); !errors.Is(err, ErrStopped) {
		t.Fatalf("Start = %v, want ErrStopped", err)
	}
}

func TestLifecycleErrors(t *testing.T) {
	ctx := context.Background()
	t.Run("not configured", func(t *testing.T) {
		c := NewConnection()
		checks := map[string]error{
			"Start":       c.Start(ctx),
			"WaitSession": c.WaitSession(ctx, time.Millisecond),
			"Publish":     c.Publish(ctx, "a.b", nil, nil, nil, 0),
		}
		_, checks["Call"] = c.Call(ctx, "a.b", nil, nil, nil, 0)
		_, checks["Subscribe"] = c.Subscribe(ctx, "a.b", func(*Event) {}, nil)
		_, checks["Register"] = c.Register(ctx, "a.b", func(context.Context, *Invocation) (any, error) { return nil, nil }, nil)
		for op, err := range checks {
			if !errors.Is(err, ErrNotConfigured) {
				t.Errorf("%s = %v, want ErrNotConfigured", op, err)
			}
		}
		if c.IsOpen() {
			t.Error("IsOpen before Configure")
		}
		if err := c.Stop(ctx); err != nil {
			t.Errorf("Stop before Configure = %v", err)
		}
	})

	t.Run("stopped", func(t *testing.T) {
		tr := newTestRouter(t, true)
		c, _ := startTestConn(t, tr)
		sub, err := c.Subscribe(ctx, "a.b", func(*Event) {}, nil)
		if err != nil {
			t.Fatal(err)
		}
		for range 2 { // idempotent
			if err := c.Stop(ctxTimeout(t, 5*time.Second)); err != nil {
				t.Fatalf("Stop: %v", err)
			}
		}
		if c.IsOpen() || sub.Active() {
			t.Fatal("open after Stop")
		}
		checks := map[string]error{
			"Start":       c.Start(ctx),
			"WaitSession": c.WaitSession(ctx, time.Millisecond),
			"Publish":     c.Publish(ctx, "a.b", nil, nil, nil, 0),
			"Configure":   c.Configure(Config{SerialNumber: "x", URL: tr.url}),
		}
		_, checks["Call"] = c.Call(ctx, "a.b", nil, nil, nil, 0)
		_, checks["Subscribe"] = c.Subscribe(ctx, "a.b", func(*Event) {}, nil)
		for op, err := range checks {
			if !errors.Is(err, ErrStopped) {
				t.Errorf("%s = %v, want ErrStopped", op, err)
			}
		}
		if err := sub.Unsubscribe(ctx); err != nil {
			t.Errorf("Unsubscribe after Stop = %v", err)
		}
	})

	t.Run("start twice and configure after start", func(t *testing.T) {
		tr := newTestRouter(t, true)
		c, _ := startTestConn(t, tr)
		if err := c.Start(ctx); err == nil || !strings.Contains(err.Error(), "already started") {
			t.Fatalf("second Start = %v", err)
		}
		if err := c.Configure(Config{SerialNumber: "x", URL: tr.url}); err == nil {
			t.Fatal("Configure after Start accepted")
		}
	})

	t.Run("wait session timeout text", func(t *testing.T) {
		tr := newTestRouter(t, true)
		c, _ := newTestConn(t, tr) // configured, never started
		start := time.Now()
		err := c.WaitSession(ctx, 50*time.Millisecond)
		want := "Not connected to the IronFlock router (realm 'realm-1-2-dev', no session after 0.05s). " +
			"Ensure Start() was called and the connection is established."
		if err == nil || err.Error() != want || !errors.Is(err, ErrNotConnected) {
			t.Fatalf("WaitSession = %v", err)
		}
		if el := time.Since(start); el < 50*time.Millisecond {
			t.Fatalf("returned after %v", el)
		}
		if err := c.WaitSession(ctxTimeout(t, 20*time.Millisecond), time.Minute); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("WaitSession with expiring ctx = %v", err)
		}
	})
}

func TestConfigureResolution(t *testing.T) {
	t.Setenv("DEVICE_ENDPOINT_URL", "")
	t.Setenv("RESWARM_URL", "")
	t.Setenv("DEVICE_SERIAL_NUMBER", "")

	c := NewConnection()
	if err := c.Configure(Config{SwarmKey: 7, AppKey: 9}); err == nil ||
		!strings.Contains(err.Error(), "DEVICE_SERIAL_NUMBER") {
		t.Fatalf("Configure without serial = %v", err)
	}

	t.Setenv("DEVICE_SERIAL_NUMBER", "env-serial")
	c = NewConnection()
	if err := c.Configure(Config{SwarmKey: 7, AppKey: 9, Stage: "prod"}); err != nil {
		t.Fatal(err)
	}
	if c.Realm() != "realm-7-9-prod" || c.SerialNumber() != "env-serial" || c.URL() != StudioWSURI {
		t.Fatalf("resolved %q %q %q", c.Realm(), c.SerialNumber(), c.URL())
	}

	c = NewConnection()
	if err := c.Configure(Config{Realm: "custom", SerialNumber: "s", URL: "ftp://x"}); err == nil {
		t.Fatal("invalid URL accepted")
	}

	t.Setenv("RESWARM_URL", "https://unknown.example")
	c = NewConnection()
	if err := c.Configure(Config{SerialNumber: "s"}); err == nil || !strings.Contains(err.Error(), "unknown.example") {
		t.Fatalf("unknown RESWARM_URL = %v", err)
	}
	t.Setenv("RESWARM_URL", "")
	t.Setenv("DEVICE_ENDPOINT_URL", "http://10.0.0.5:8080/some/path")
	c = NewConnection()
	if err := c.Configure(Config{SerialNumber: "s"}); err != nil {
		t.Fatal(err)
	}
	if c.URL() != "http://10.0.0.5:8080/ws-ua-usr" || c.dialURL != "ws://10.0.0.5:8080/ws-ua-usr" {
		t.Fatalf("URL %q dial %q", c.URL(), c.dialURL)
	}
	if c.Realm() != "realm-0-0-dev" {
		t.Fatalf("default stage realm = %q", c.Realm())
	}
}

func TestCallbacksOnConnectAndDisconnect(t *testing.T) {
	tr := newTestRouter(t, true)
	connects := make(chan struct{}, 8)
	disconnects := make(chan string, 8)
	c, _ := startTestConn(t, tr, func(cfg *Config, _ *Connection) {
		cfg.OnConnect = func() { connects <- struct{}{} }
		cfg.OnDisconnect = func(reason string) { disconnects <- reason }
	})
	recv(t, connects, "OnConnect")

	tr.ln.dropAll()
	if got := recv(t, disconnects, "OnDisconnect"); got != reasonTransportLost {
		t.Fatalf("OnDisconnect(%q), want %q", got, reasonTransportLost)
	}
	recv(t, connects, "OnConnect after reconnect")

	tr.removeRealm()
	if got := recv(t, disconnects, "OnDisconnect"); got != string(nxwamp.CloseSystemShutdown) {
		t.Fatalf("OnDisconnect(%q), want system_shutdown", got)
	}
	eventually(t, 2*time.Second, "IsOpen false", func() bool { return !c.IsOpen() })
	tr.addRealm()
	recv(t, connects, "OnConnect after the realm came back")

	// Stop is not a lost session.
	if err := c.Stop(ctxTimeout(t, 5*time.Second)); err != nil {
		t.Fatal(err)
	}
	noRecv(t, disconnects, "OnDisconnect after Stop")
}

func TestPanickingCallbackIsRecovered(t *testing.T) {
	tr := newTestRouter(t, true)
	connects := make(chan struct{}, 4)
	var n atomic.Int32
	c, logs := startTestConn(t, tr, func(cfg *Config, _ *Connection) {
		cfg.OnConnect = func() {
			connects <- struct{}{}
			if n.Add(1) == 1 {
				panic("callback boom")
			}
		}
	})
	recv(t, connects, "OnConnect")
	tr.ln.dropAll()
	recv(t, connects, "OnConnect after reconnect")
	eventually(t, 2*time.Second, "panic log", func() bool { return strings.Contains(logs.String(), "callback boom") })
	if !c.IsOpen() {
		t.Fatal("connection not open")
	}
}

func TestStopSendsGoodbye(t *testing.T) {
	tr := newTestRouter(t, true)
	c, _ := startTestConn(t, tr)
	obs := tr.local(t)
	left := make(chan struct{}, 1)
	if err := obs.Subscribe(string(nxwamp.MetaEventSessionOnLeave), func(*nxwamp.Event) { left <- struct{}{} }, nil); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	if err := c.Stop(ctxTimeout(t, 5*time.Second)); err != nil {
		t.Fatal(err)
	}
	recv(t, left, "session leave")
	if el := time.Since(start); el > time.Second {
		t.Fatalf("Stop took %v", el)
	}
}

// Stop does not wait for the network once the GOODBYE cannot be sent: not
// past its own deadline, and not past the response timeout. The WebSocket
// peer's Close waits for its writer, and a write stuck on a full socket
// buffer (a dead link that did not reset the connection) gives up only at
// its 60s deadline.
func TestStopDoesNotWaitForStuckWriter(t *testing.T) {
	tests := []struct {
		name    string
		ctx     func(t *testing.T) context.Context
		wantErr error
		within  time.Duration
	}{
		{"past its deadline", func(t *testing.T) context.Context { return ctxTimeout(t, 200*time.Millisecond) },
			context.DeadlineExceeded, 2 * time.Second},
		{"without a deadline", func(*testing.T) context.Context { return context.Background() },
			nil, 4 * time.Second}, // the GOODBYE gives up after the 2s response timeout
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tr := newTestRouter(t, true)
			proxy := newDropProxy(t, tr.url)
			c, _ := startTestConn(t, tr, func(cfg *Config, _ *Connection) { cfg.URL = proxy.url })
			t.Cleanup(func() { // before the connection's cleanup Stop
				proxy.frozen.Store(false)
				proxy.dropAll()
			})
			jamWriter(t, c, proxy)

			stopped := make(chan error, 1)
			begin := time.Now()
			go func() { stopped <- c.Stop(tt.ctx(t)) }()
			select {
			case err := <-stopped:
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("Stop = %v, want %v", err, tt.wantErr)
				}
				if el := time.Since(begin); el > tt.within {
					t.Fatalf("Stop took %v", el)
				}
			case <-time.After(tt.within + 3*time.Second):
				t.Fatal("Stop waits for the stuck writer")
			}
			eventually(t, 5*time.Second, "the supervisor to exit", func() bool { return closed(c.done) })
		})
	}
}

// jamWriter freezes the link through proxy and publishes until c's
// WebSocket writer is stuck on a full socket buffer.
func jamWriter(t *testing.T, c *Connection, proxy *dropProxy) {
	t.Helper()
	proxy.frozen.Store(true)
	payload := make([]byte, 1<<20)
	published := make(chan struct{}, 1)
	go func() {
		for c.Publish(context.Background(), "stuck.topic", []any{payload}, nil, nil, 0) == nil {
			select {
			case published <- struct{}{}:
			default:
			}
		}
	}()
	// The socket buffers are full once no publish has completed for a while.
	deadline := time.Now().Add(20 * time.Second)
	for {
		select {
		case <-published:
			if time.Now().After(deadline) {
				t.Fatal("the WebSocket writer never got stuck")
			}
		case <-time.After(300 * time.Millisecond):
			return
		}
	}
}

// Stop racing a connection that has just dropped must not wait for the
// GOODBYE to be handed to the dead writer.
func TestStopRacingDroppedConnection(t *testing.T) {
	tr := newTestRouter(t, true)
	for i := range 10 {
		c, _ := startTestConn(t, tr)
		tr.ln.dropAll()
		if i%2 == 1 {
			time.Sleep(time.Duration(i) * 100 * time.Microsecond)
		}
		start := time.Now()
		if err := c.Stop(ctxTimeout(t, 5*time.Second)); err != nil {
			t.Fatal(err)
		}
		if el := time.Since(start); el > c.t.responseTimeout/2 {
			t.Fatalf("round %d: Stop took %v", i, el)
		}
	}
}
