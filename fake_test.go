package ironflock

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"reflect"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ironflock/sdk-go/internal/wampx"
	"github.com/ironflock/sdk-go/wamp"
)

// fakeCall is one recorded Call.
type fakeCall struct {
	Procedure string
	Args      []any
	Kwargs    map[string]any
	Opts      *wamp.CallOptions
	Window    time.Duration
}

// fakePublish is one recorded Publish.
type fakePublish struct {
	Topic  string
	Args   []any
	Kwargs map[string]any
	Opts   *wamp.PublishOptions
	Window time.Duration
}

// fakeSub is one recorded SubscribeWithin.
type fakeSub struct {
	Topic   string
	Handler wamp.EventHandler
	Opts    *wamp.SubscribeOptions
	Window  time.Duration
	Sub     *wamp.Subscription
	Removed bool
}

// fakeReg is one recorded RegisterWithin.
type fakeReg struct {
	Procedure string
	Handler   wamp.InvocationHandler
	Opts      *wamp.RegisterOptions
	Window    time.Duration
	Reg       *wamp.Registration
	Removed   bool
}

// fakeConn is a recording wampConn that follows wamp.Connection's lifecycle
// and errors:
//
//   - Configure fails after the first Start and after Stop.
//   - Start fails with wamp.ErrNotConfigured before Configure, with
//     wamp.ErrStopped after Stop, with an error while a Start is running or
//     has succeeded, and with ctx's error when ctx is done already.
//     Otherwise it runs startFn and joins when that returns nil. A failed
//     Start may be called again, unless Stop ran meanwhile or the failure is
//     a fatal *wamp.AuthError of a FailOnAuthError connection: then the
//     connection is stopped for good.
//   - Stop is final: IsOpen turns false at once, and stopFn may then block
//     like the router's GOODBYE.
//   - Operations fail with wamp.ErrNotConfigured before Configure and with
//     wamp.ErrStopped after Stop; otherwise they wait for the join within
//     their window (the default session wait when 0: sessionWait, or
//     wamp.DefaultSessionWaitTimeout) and fail with an error wrapping
//     wamp.ErrNotConnected when it passes.
//
// Operations are recorded with their arguments once they have a session;
// the hooks script their results and errors (nil hooks succeed). It is safe
// for concurrent use. Hooks are read under the lock and run outside it.
type fakeConn struct {
	mu sync.Mutex

	cfg        *wamp.Config
	configures int
	starts     int
	stops      int
	waits      []time.Duration // the timeouts of WaitSession calls
	everStarts bool            // a Start got past its checks
	starting   bool            // a Start is running or has succeeded
	joined     bool            // the last Start succeeded
	stopped    bool
	fatal      *wamp.AuthError
	up         chan struct{} // closed once joined
	down       chan struct{} // closed by Stop

	// sessionWait stands in for wamp.Config.SessionWaitTimeout: the wait of
	// an operation without a window (0: wamp.DefaultSessionWaitTimeout).
	sessionWait time.Duration

	calls     []fakeCall
	publishes []fakePublish
	subs      []*fakeSub
	regs      []*fakeReg

	callFn         func(c fakeCall) (*wamp.Result, error)
	publishFn      func(p fakePublish) error
	subscribeFn    func(topic string) error
	registerFn     func(procedure string) error
	unsubscribeErr error
	configureErr   error
	startFn        func(ctx context.Context, cfg wamp.Config) error
	stopFn         func(ctx context.Context) error // runs once IsOpen is false
	stopErr        error

	// joins counts the joins as wamp.Connection does: Start counts the
	// first, rejoin the next ones (see joinsOf).
	joins wampx.Joins
}

var _ wampConn = (*fakeConn)(nil)

// joinCounter returns the fake's join counter (see joinsOf).
func (c *fakeConn) joinCounter() *wampx.Joins { return &c.joins }

// rejoin simulates a reconnect as wamp.Connection makes it: a new session
// joins — counted before its restore — and once the restore is done the join
// hooks run. It returns the join's number.
func (c *fakeConn) rejoin() int64 {
	join := c.joins.Begin()
	c.joins.Restored(join, nil)
	return join
}

// drop ends the session of a joined fake: IsOpen turns false, and operations
// wait for the next session — the gap of a reconnect — until reconnect.
func (c *fakeConn) drop() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.chans()
	c.joined = false
	c.up = make(chan struct{})
}

// reconnect ends the gap drop began: a new session joins, counted as rejoin
// counts it, and the operations waiting for it go on. It returns the join's
// number.
func (c *fakeConn) reconnect() int64 {
	c.mu.Lock()
	join := c.joins.Begin()
	c.joined = true
	close(c.up)
	c.mu.Unlock()
	c.joins.Restored(join, nil)
	return join
}

// errFakeAlreadyStarted stands in for wamp.Connection's (unexported) error
// for a second Start.
var errFakeAlreadyStarted = errors.New("wamp: connection already started")

// chans returns the join and stop channels, creating them on first use.
// c.mu must be held.
func (c *fakeConn) chans() (up, down chan struct{}) {
	if c.up == nil {
		c.up, c.down = make(chan struct{}), make(chan struct{})
	}
	return c.up, c.down
}

// stoppedErrLocked is wamp.Connection's error after Stop. c.mu must be held.
func (c *fakeConn) stoppedErrLocked() error {
	if c.fatal != nil {
		return fmt.Errorf("%w: %w", wamp.ErrStopped, c.fatal)
	}
	return wamp.ErrStopped
}

func (c *fakeConn) Configure(cfg wamp.Config) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.configures++
	switch {
	case c.stopped:
		return c.stoppedErrLocked()
	case c.everStarts:
		return errors.New("wamp: Configure called after Start")
	case c.configureErr != nil:
		return c.configureErr
	}
	if cfg.Realm == "" {
		cfg.Realm = wamp.RealmName(cfg.SwarmKey, cfg.AppKey, cfg.Stage)
	}
	c.cfg = &cfg
	return nil
}

func (c *fakeConn) Start(ctx context.Context) error {
	c.mu.Lock()
	c.starts++
	switch {
	case c.cfg == nil:
		c.mu.Unlock()
		return wamp.ErrNotConfigured
	case c.stopped:
		err := c.stoppedErrLocked()
		c.mu.Unlock()
		return err
	case c.starting:
		c.mu.Unlock()
		return errFakeAlreadyStarted
	}
	c.everStarts, c.starting = true, true
	fn, cfg := c.startFn, *c.cfg
	c.mu.Unlock()

	err := ctx.Err() // no join can win against a context that is done already
	if err != nil {
		err = fmt.Errorf("wamp: no session on realm %s: %w", cfg.Realm, err)
	} else if fn != nil {
		err = fn(ctx, cfg)
	}
	c.mu.Lock()
	if err == nil && c.stopped {
		err = c.stoppedErrLocked() // Stop ended the Start first
	}
	if err != nil {
		var aerr *wamp.AuthError
		if cfg.FailOnAuthError && errors.As(err, &aerr) && !c.stopped {
			_, down := c.chans()
			c.stopped, c.fatal = true, aerr
			close(down)
		}
		if !c.stopped {
			c.starting = false // may be started again
		}
		c.mu.Unlock()
		return err
	}
	join := c.joins.Begin() // before the session is up, as wamp.Connection counts it
	up, _ := c.chans()
	c.joined = true
	close(up)
	c.mu.Unlock()
	c.joins.Restored(join, nil)
	return nil
}

func (c *fakeConn) Stop(ctx context.Context) error {
	c.mu.Lock()
	c.stops++
	if !c.stopped {
		_, down := c.chans()
		c.stopped = true
		close(down)
	}
	fn, err := c.stopFn, c.stopErr
	c.mu.Unlock()
	if fn != nil {
		return fn(ctx)
	}
	return err
}

func (c *fakeConn) IsOpen() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.joined && !c.stopped
}

func (c *fakeConn) URL() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.cfg == nil {
		return ""
	}
	return c.cfg.URL
}

// WaitSession waits for the join as wamp.Connection.WaitSession does.
func (c *fakeConn) WaitSession(ctx context.Context, timeout time.Duration) error {
	c.mu.Lock()
	c.waits = append(c.waits, timeout)
	c.mu.Unlock()
	return c.session(ctx, timeout)
}

// session waits for the join, at most window (0: the default session wait).
func (c *fakeConn) session(ctx context.Context, window time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if window <= 0 {
		c.mu.Lock()
		window = cmp.Or(c.sessionWait, wamp.DefaultSessionWaitTimeout)
		c.mu.Unlock()
	}
	var timer *time.Timer
	for {
		c.mu.Lock()
		switch {
		case c.cfg == nil:
			c.mu.Unlock()
			return wamp.ErrNotConfigured
		case c.stopped:
			err := c.stoppedErrLocked()
			c.mu.Unlock()
			return err
		case c.joined:
			c.mu.Unlock()
			return nil
		}
		up, down := c.chans()
		c.mu.Unlock()
		if timer == nil {
			timer = time.NewTimer(window)
			defer timer.Stop()
		}
		select {
		case <-up:
		case <-down:
		case <-timer.C:
			return fmt.Errorf("fake: no session after %v: %w", window, wamp.ErrNotConnected)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (c *fakeConn) Call(ctx context.Context, procedure string, args []any, kwargs map[string]any, opts *wamp.CallOptions, retryWindow time.Duration) (*wamp.Result, error) {
	if err := c.session(ctx, retryWindow); err != nil {
		return nil, err
	}
	call := fakeCall{Procedure: procedure, Args: args, Kwargs: kwargs, Opts: opts, Window: retryWindow}
	c.mu.Lock()
	c.calls = append(c.calls, call)
	fn := c.callFn
	c.mu.Unlock()
	if fn != nil {
		return fn(call)
	}
	return &wamp.Result{}, nil
}

func (c *fakeConn) Publish(ctx context.Context, topic string, args []any, kwargs map[string]any, opts *wamp.PublishOptions, waitWindow time.Duration) error {
	if err := c.session(ctx, waitWindow); err != nil {
		return err
	}
	pub := fakePublish{Topic: topic, Args: args, Kwargs: kwargs, Opts: opts, Window: waitWindow}
	c.mu.Lock()
	c.publishes = append(c.publishes, pub)
	fn := c.publishFn
	c.mu.Unlock()
	if fn != nil {
		return fn(pub)
	}
	return nil
}

func (c *fakeConn) SubscribeWithin(ctx context.Context, topic string, handler wamp.EventHandler, opts *wamp.SubscribeOptions, waitWindow time.Duration) (*wamp.Subscription, error) {
	if err := c.session(ctx, waitWindow); err != nil {
		return nil, err
	}
	c.mu.Lock()
	fn := c.subscribeFn
	c.mu.Unlock()
	if fn != nil {
		if err := fn(topic); err != nil {
			return nil, err
		}
	}
	s := &fakeSub{Topic: topic, Handler: handler, Opts: opts, Window: waitWindow, Sub: new(wamp.Subscription)}
	c.mu.Lock()
	c.subs = append(c.subs, s)
	c.mu.Unlock()
	return s.Sub, nil
}

func (c *fakeConn) Unsubscribe(ctx context.Context, sub *wamp.Subscription) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, s := range c.subs {
		if s.Sub == sub {
			s.Removed = true
		}
	}
	return c.unsubscribeErr
}

func (c *fakeConn) RegisterWithin(ctx context.Context, procedure string, handler wamp.InvocationHandler, opts *wamp.RegisterOptions, waitWindow time.Duration) (*wamp.Registration, error) {
	if err := c.session(ctx, waitWindow); err != nil {
		return nil, err
	}
	c.mu.Lock()
	fn := c.registerFn
	c.mu.Unlock()
	if fn != nil {
		if err := fn(procedure); err != nil {
			return nil, err
		}
	}
	r := &fakeReg{Procedure: procedure, Handler: handler, Opts: opts, Window: waitWindow, Reg: new(wamp.Registration)}
	c.mu.Lock()
	c.regs = append(c.regs, r)
	c.mu.Unlock()
	return r.Reg, nil
}

func (c *fakeConn) Unregister(ctx context.Context, reg *wamp.Registration) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, r := range c.regs {
		if r.Reg == reg {
			r.Removed = true
		}
	}
	return nil
}

// config returns the Configure argument (zero when not configured).
func (c *fakeConn) config() wamp.Config {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.cfg == nil {
		return wamp.Config{}
	}
	return *c.cfg
}

func (c *fakeConn) allCalls() []fakeCall {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.calls)
}

func (c *fakeConn) callsTo(procedure string) []fakeCall {
	var out []fakeCall
	for _, call := range c.allCalls() {
		if call.Procedure == procedure {
			out = append(out, call)
		}
	}
	return out
}

func (c *fakeConn) lastCall(t *testing.T) fakeCall {
	t.Helper()
	calls := c.allCalls()
	if len(calls) == 0 {
		t.Fatal("no call was made")
	}
	return calls[len(calls)-1]
}

func (c *fakeConn) allPublishes() []fakePublish {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.publishes)
}

func (c *fakeConn) lastPublish(t *testing.T) fakePublish {
	t.Helper()
	pubs := c.allPublishes()
	if len(pubs) == 0 {
		t.Fatal("nothing was published")
	}
	return pubs[len(pubs)-1]
}

func (c *fakeConn) allSubs() []fakeSub {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]fakeSub, len(c.subs))
	for i, s := range c.subs {
		out[i] = *s
	}
	return out
}

func (c *fakeConn) allRegs() []fakeReg {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]fakeReg, len(c.regs))
	for i, r := range c.regs {
		out[i] = *r
	}
	return out
}

func (c *fakeConn) stopCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.stops
}

func (c *fakeConn) configureCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.configures
}

func (c *fakeConn) startCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.starts
}

// fire delivers ev to every live subscription of topic, synchronously.
func (c *fakeConn) fire(topic string, ev *wamp.Event) {
	c.mu.Lock()
	var handlers []wamp.EventHandler
	for _, s := range c.subs {
		if s.Topic == topic && !s.Removed {
			handlers = append(handlers, s.Handler)
		}
	}
	c.mu.Unlock()
	for _, h := range handlers {
		h(ev)
	}
}

// fakeFactory creates the fake connections of consumed apps.
type fakeFactory struct {
	mu    sync.Mutex
	conns []*fakeConn
	// setup scripts every new connection; set it before the connections are
	// created.
	setup func(c *fakeConn)
}

func (ff *fakeFactory) newConn() wampConn {
	c := &fakeConn{}
	ff.mu.Lock()
	setup := ff.setup
	ff.mu.Unlock()
	if setup != nil {
		setup(c)
	}
	ff.mu.Lock()
	ff.conns = append(ff.conns, c)
	ff.mu.Unlock()
	return c
}

func (ff *fakeFactory) all() []*fakeConn {
	ff.mu.Lock()
	defer ff.mu.Unlock()
	return slices.Clone(ff.conns)
}

func (ff *fakeFactory) setSetup(setup func(c *fakeConn)) {
	ff.mu.Lock()
	defer ff.mu.Unlock()
	ff.setup = setup
}

// connFor returns the consumed connection configured for provider app key.
func (ff *fakeFactory) connFor(t *testing.T, appKey int) *fakeConn {
	t.Helper()
	for _, c := range ff.all() {
		if c.config().AppKey == appKey {
			return c
		}
	}
	t.Fatalf("no consumed connection for provider app key %d", appKey)
	return nil
}

// logBuffer collects log output; safe for concurrent writers.
type logBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *logBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *logBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func newTestLogger() (*slog.Logger, *logBuffer) {
	b := &logBuffer{}
	return slog.New(slog.NewTextHandler(b, &slog.HandlerOptions{Level: slog.LevelDebug})), b
}

// platformEnv lists every environment variable the root package reads.
var platformEnv = []string{
	"DEVICE_SERIAL_NUMBER", "DEVICE_NAME", "DEVICE_KEY", "APP_NAME", "SWARM_KEY", "APP_KEY", "ENV",
	"RESWARM_URL", "DEVICE_ENDPOINT_URL", "APP_AUTH_ID", "APP_AUTH_SECRET", "IRONFLOCK_ENV_DIR",
	"INSTANCE_KEY", "TUNNEL_DOMAIN", "CLOUD_TUNNEL_DOMAIN",
}

// unsetEnv unsets names for the duration of the test.
func unsetEnv(t *testing.T, names ...string) {
	t.Helper()
	for _, name := range names {
		t.Setenv(name, "") // restores the original value after the test
		if err := os.Unsetenv(name); err != nil {
			t.Fatal(err)
		}
	}
}

// setIdentityEnv sets the identity the device agent injects, with every
// other platform variable unset and the env-file directory an empty temp
// directory.
func setIdentityEnv(t *testing.T) {
	t.Helper()
	unsetEnv(t, platformEnv...)
	for k, v := range map[string]string{
		"DEVICE_SERIAL_NUMBER": "test-serial-123",
		"DEVICE_NAME":          "test-device",
		"DEVICE_KEY":           "12",
		"APP_NAME":             "TestApp",
		"SWARM_KEY":            "10",
		"APP_KEY":              "20",
		"ENV":                  "DEV",
	} {
		t.Setenv(k, v)
	}
	t.Setenv("IRONFLOCK_ENV_DIR", t.TempDir())
}

// testFlock is an IronFlock wired to fakes.
type testFlock struct {
	*IronFlock
	own     *fakeConn
	factory *fakeFactory
	logs    *logBuffer
}

// newTestFlock builds an IronFlock from the current environment and opts,
// with a fake own connection and fake consumed-app connections. It is not
// started, and it is stopped when the test ends.
func newTestFlock(t *testing.T, opts ...Option) *testFlock {
	t.Helper()
	log, logs := newTestLogger()
	own := &fakeConn{}
	factory := &fakeFactory{}
	f, err := newWithConn(own, factory.newConn, append([]Option{WithLogger(log)}, opts...)...)
	if err != nil {
		t.Fatalf("newWithConn: %v", err)
	}
	f.cleanupTimeout = time.Second
	f.runStopTimeout = time.Second
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = f.Stop(ctx)
	})
	return &testFlock{IronFlock: f, own: own, factory: factory, logs: logs}
}

// unstartedFlock is newTestFlock with the standard identity environment.
func unstartedFlock(t *testing.T, opts ...Option) *testFlock {
	t.Helper()
	setIdentityEnv(t)
	return newTestFlock(t, opts...)
}

// flock is unstartedFlock, started.
func flock(t *testing.T, opts ...Option) *testFlock {
	t.Helper()
	f := unstartedFlock(t, opts...)
	f.start(t)
	return f
}

// start starts tf, failing the test if Start fails.
func (tf *testFlock) start(t *testing.T) {
	t.Helper()
	if err := tf.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
}

// resultOf returns a call result whose single positional value is v.
func resultOf(v any) *wamp.Result { return &wamp.Result{Args: []any{v}} }

// wampErr returns a router refusal.
func wampErr(uri string, args ...any) *wamp.Error {
	return &wamp.Error{URI: uri, Args: args}
}

// checkGoroutines fails the test when, at its end, more goroutines run than
// at its start (after a grace period for goroutines that are winding down).
// Register it first, so its cleanup runs after every other cleanup.
func checkGoroutines(t *testing.T) {
	t.Helper()
	// os/signal starts its delivery goroutine on the first Notify and keeps
	// it for the life of the process; start it before taking the baseline.
	signalLoopOnce.Do(func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt)
		signal.Stop(c)
	})
	before := runtime.NumGoroutine()
	t.Cleanup(func() {
		deadline := time.Now().Add(2 * time.Second)
		for {
			n := runtime.NumGoroutine()
			if n <= before {
				return
			}
			if time.Now().After(deadline) {
				buf := make([]byte, 1<<16)
				buf = buf[:runtime.Stack(buf, true)]
				t.Errorf("goroutine leak: %d goroutines at the end, %d at the start\n%s", n, before, buf)
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	})
}

var signalLoopOnce sync.Once

// recv waits up to 5 s for a value from ch.
func recv[T any](t *testing.T, ch <-chan T, what string) T {
	t.Helper()
	return recvWithin(t, ch, 5*time.Second, what)
}

// recvWithin waits up to timeout for a value from ch.
func recvWithin[T any](t *testing.T, ch <-chan T, timeout time.Duration, what string) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(timeout):
		t.Fatalf("timed out waiting for %s", what)
	}
	var zero T
	return zero
}

// eventually polls cond until it holds or a second passes.
func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	eventuallyWithin(t, time.Second, what, cond)
}

// eventuallyWithin polls cond until it holds or timeout passes: for what
// takes a reconnect, which the default backoff makes a second or two.
func eventuallyWithin(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

// mustContain fails unless s contains every part.
func mustContain(t *testing.T, s string, parts ...string) {
	t.Helper()
	for _, p := range parts {
		if !strings.Contains(s, p) {
			t.Errorf("%q does not contain %q", s, p)
		}
	}
}

// readCall returns call without its options, failing the test unless they
// ask for progressive results and hold nothing else: the options of a read
// the data backend may answer in chunks.
func readCall(t *testing.T, call fakeCall) fakeCall {
	t.Helper()
	if call.Opts == nil || call.Opts.OnProgress == nil {
		t.Errorf("%s does not ask for progressive results (options %#v)", call.Procedure, call.Opts)
	} else {
		rest := *call.Opts
		rest.OnProgress = nil
		if !reflect.DeepEqual(rest, wamp.CallOptions{}) {
			t.Errorf("%s: options %#v besides OnProgress", call.Procedure, rest)
		}
	}
	call.Opts = nil
	return call
}

// answerProgressively makes c answer every call as fleetdb answers a large
// read: a progressive result with each of progress as its Args, passed to the
// call's OnProgress in order before the call returns (as wamp.Connection
// does), then final. A call that does not ask for progressive results is
// refused with result_too_large.
func answerProgressively(c *fakeConn, progress [][]any, final any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.callFn = func(call fakeCall) (*wamp.Result, error) {
		if call.Opts == nil || call.Opts.OnProgress == nil {
			return nil, wampErr(URIResultTooLarge, "This query returned 9 MB (30 rows).",
				map[string]any{"detail": "caller cannot receive progressive results"})
		}
		for _, args := range progress {
			call.Opts.OnProgress(&wamp.Result{Args: args, Details: map[string]any{"progress": true}})
		}
		return resultOf(final), nil
	}
}
