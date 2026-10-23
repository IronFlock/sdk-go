// Package wamp is the WAMP connection underneath the IronFlock SDK.
//
// A Connection joins one realm of the IronFlock router with WAMP-CRA
// authentication over a msgpack WebSocket, keeps the session alive across
// router restarts and network failures, and restores every subscription and
// registration after each reconnect. Most apps use it only through the
// ironflock package; it is exported for advanced use.
package wamp

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ironflock/nexus/v3/client"
	nxwamp "github.com/ironflock/nexus/v3/wamp"
	"github.com/ironflock/sdk-go/internal/wampx"
)

// Reconnect and wait tunables. They mirror the Python and JavaScript SDKs.
const (
	// DefaultSessionWaitTimeout is how long an operation waits for a session
	// when the caller gives no window of its own.
	DefaultSessionWaitTimeout = 10 * time.Second
	// DefaultFirstConnectTimeout bounds Start for connections with
	// FailOnAuthError set (consumed-app connections). The primary connection
	// waits for its realm without a bound.
	DefaultFirstConnectTimeout = 50 * time.Second

	// InitialRetryDelay is the delay before the first reconnect attempt.
	InitialRetryDelay = 1 * time.Second
	// RetryDelayGrowth multiplies the delay after every failed attempt.
	RetryDelayGrowth = 1.5
	// RetryDelayJitter is the relative random jitter applied to each delay.
	RetryDelayJitter = 0.1
	// BaseMaxRetryDelay caps the reconnect delay while the router is
	// reachable and retrying is expected to succeed soon (router restart,
	// concurrent-install race).
	BaseMaxRetryDelay = 2 * time.Second
	// NoSuchRealmBackoffAfter is how long a realm may be missing before
	// reconnects slow down: past it the realm is most likely never going to
	// appear (the app was deleted, or has no data backend for this stage).
	NoSuchRealmBackoffAfter = 60 * time.Second
	// NoSuchRealmMaxRetryDelay is the reconnect ceiling once
	// NoSuchRealmBackoffAfter has passed.
	NoSuchRealmMaxRetryDelay = 120 * time.Second

	// RetryFirstDelay and RetryMaxDelay bound the backoff between retries of
	// a call whose procedure is not registered yet (see Connection.Call).
	RetryFirstDelay = 500 * time.Millisecond
	RetryMaxDelay   = 5 * time.Second

	// DefaultKeepAlive is the WebSocket ping interval. A connection whose
	// pong is missing for two intervals is dropped and reconnected, so a
	// silently half-open socket (NAT/idle timeout, router restart behind a
	// load balancer) is noticed within ~40s even when the app only
	// subscribes.
	DefaultKeepAlive = 20 * time.Second
)

// Errors returned by Connection.
var (
	// ErrNotConfigured: an operation or Start was attempted before Configure.
	ErrNotConfigured = errors.New("wamp: connection is not configured — call Configure() and Start() before performing WAMP operations")
	// ErrNotConnected: no session became available within the wait window.
	ErrNotConnected = errors.New("wamp: not connected to the IronFlock router")
	// ErrStopped: the connection was stopped for good — by Stop, or by a
	// fatal auth denial (FailOnAuthError).
	ErrStopped = errors.New("wamp: connection stopped")
)

// AuthError reports that the router refused this connection's credentials or
// role (a reason IsFatalAuthReason accepts) for good. It is fatal only for
// connections with FailOnAuthError set (see there for when a refusal counts
// as for good); the primary connection keeps retrying.
type AuthError struct {
	Realm  string
	Reason string
}

func (e *AuthError) Error() string {
	return fmt.Sprintf("authentication denied for realm %s: %s", e.Realm, e.Reason)
}

// Config configures a Connection.
type Config struct {
	// SwarmKey, AppKey and Stage name the realm:
	// realm-<SwarmKey>-<AppKey>-<dev|prod>.
	SwarmKey int
	AppKey   int
	Stage    Stage
	// Realm overrides the realm name derived from SwarmKey/AppKey/Stage.
	Realm string

	// URL is the router WebSocket URL. Empty resolves it with
	// WebSocketURI("") (DEVICE_ENDPOINT_URL, RESWARM_URL, cloud default).
	URL string

	// SerialNumber is the device identity; empty reads DEVICE_SERIAL_NUMBER.
	SerialNumber string
	// AuthID and AuthSecret are the WAMP-CRA credential. When both are empty
	// the credential is resolved with AppCredentials(SerialNumber) on EVERY
	// connection attempt, so a credential the agent rotates while the app
	// runs is picked up on the next reconnect.
	AuthID     string
	AuthSecret string

	// FailOnAuthError treats an authentication/authorization denial (a reason
	// IsFatalAuthReason accepts: a refused join, or a session the router
	// closes with such a reason) as fatal: reconnecting stops, OnAuthFailure
	// is called, and operations fail with ErrStopped wrapping an *AuthError.
	// Before the connection has been established a denial is fatal at once,
	// and Start returns the *AuthError. Once it has been established, a
	// denial is fatal only when it persists — 3 refusals or more since the
	// last join, the first one at least 60s ago — since the router
	// refuses the same way while it cannot verify access (see
	// IsFatalAuthReason); until then the connection reconnects as after any
	// other failure. FailOnAuthError also bounds Start by
	// FirstConnectTimeout. Used for consumed-app connections; the primary
	// connection retries through every failure.
	FailOnAuthError bool
	// OnAuthFailure is called (once, on its own goroutine) with the reason
	// of the last refusal when FailOnAuthError stops the connection.
	OnAuthFailure func(reason string)

	// OnConnect is called (on its own goroutine) after every successful join,
	// once subscriptions and registrations are restored.
	OnConnect func()
	// OnDisconnect is called (on its own goroutine) when an established
	// session is lost, with the close reason if one is known. Every
	// OnConnect is followed by one OnDisconnect, unless Stop ends the
	// session.
	OnDisconnect func(reason string)

	// SessionWaitTimeout overrides DefaultSessionWaitTimeout.
	SessionWaitTimeout time.Duration
	// FirstConnectTimeout overrides DefaultFirstConnectTimeout.
	FirstConnectTimeout time.Duration
	// KeepAlive overrides DefaultKeepAlive; a negative value disables
	// WebSocket keepalive pings.
	KeepAlive time.Duration
	// TLSConfig is used for wss:// URLs; nil uses Go's defaults.
	TLSConfig *tls.Config
	// Logger receives connection lifecycle logs; nil uses slog.Default().
	Logger *slog.Logger
}

// Connection is a self-healing WAMP session on one realm.
//
// Lifecycle: Configure, then Start (blocks until the first join; a failed
// Start may be retried), then any number of concurrent operations, then Stop,
// which is final. A Connection is safe for concurrent use by multiple
// goroutines.
//
// Reconnects: one supervisor goroutine owns the connect/retry loop. Every
// involuntary close — a refused join (wamp.error.no_such_realm while the data
// backend provisions the realm), a router restart, a dropped or silently dead
// socket (keepalive), even a clean GOODBYE — schedules the next attempt with
// exponential backoff (InitialRetryDelay, growth RetryDelayGrowth, jitter
// RetryDelayJitter, capped at BaseMaxRetryDelay). After NoSuchRealmBackoffAfter
// of continuous no_such_realm refusals the cap widens to
// NoSuchRealmMaxRetryDelay until the realm appears or any other close reason
// is seen. After each join every tracked subscription and registration is
// restored. One that fails to restore stays tracked and is retried while the
// session lasts — after 1s, doubling up to 30s, with ±15% jitter: the
// router accepted it before, so its refusal is a passing one, such as
// the router's identity check failing closed while its authorizer is
// unavailable — and again after every reconnect.
//
// Contexts: every operation waits for a session within its context (and its
// wait window). Subscribe, Register, Unsubscribe, UnsubscribeTopic and
// Unregister also wait within their context for subscription changes in
// progress: another such operation's router round trip, or the restore after
// a reconnect. When the context ends first, a Subscribe or Register returns
// its error having sent nothing; an Unsubscribe, UnsubscribeTopic or
// Unregister returns it too, but has taken effect locally already and tells
// the router in the background (see Unsubscribe). A Call returns its
// context's error as soon as the context ends, and the router is told to
// cancel the call in the background (see Call). The router round trip of
// Publish, Subscribe, Register, Unsubscribe and Unregister is not
// interruptible once sent; it is bounded by the router response timeout
// (DefaultSessionWaitTimeout).
type Connection struct {
	cfg     Config
	log     *slog.Logger
	t       tunables
	dialURL string

	// mu guards the lifecycle state below. Lock order: state before mu.
	mu         sync.Mutex
	configured bool
	started    bool            // a Start is running or has succeeded
	stopped    bool            // for good: by Stop or a fatal auth denial
	runCtx     context.Context // cancelled when stopped
	cancelRun  context.CancelFunc
	// endSupervisor cancels the context of the last Start's supervisor, a
	// child of runCtx: a failed Start ends the supervisor without stopping
	// the connection.
	endSupervisor context.CancelFunc
	done          chan struct{} // closed when the last Start's supervisor exits; nil before Start
	sess          *session      // current, restored session; nil while down
	established   bool          // a session has been published once
	upCh          chan struct{} // closed while sess is set, replaced when it goes
	peer          *observedPeer // WebSocket of the current session or join
	fatal         *AuthError

	stopFlag atomic.Bool // mirrors stopped for hot paths

	// callbacks runs OnConnect, OnDisconnect and OnAuthFailure in order.
	callbacks serialExecutor

	// state serializes changes of the tracked subscriptions and
	// registrations — including their WAMP round trips — with the restore
	// after each join. groups and regs change only while it is held; groups
	// is copy-on-write, so that UnsubscribeTopic can find a topic's handlers
	// without waiting for it.
	state  stateLock
	groups atomic.Pointer[[]*subGroup]
	regs   []*Registration

	// joins counts the sessions joined, and calls the ironflock package's
	// hooks once a join has restored the subscriptions and registrations:
	// table subscriptions catch up on the rows stored while the connection
	// was down (see runSession).
	joins wampx.Joins
}

// stateLock is a mutex whose waiters give up when their context ends: a
// one-slot semaphore. The zero value is unlocked.
type stateLock struct {
	once sync.Once
	sem  chan struct{}
}

// lock acquires l, or returns ctx's error — at once if ctx has ended
// already, even if l is free. If ctx ends just as l comes free, lock may
// acquire l all the same: a caller that must not go on once ctx has ended
// checks it again.
func (l *stateLock) lock(ctx context.Context) error {
	l.once.Do(func() { l.sem = make(chan struct{}, 1) })
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case l.sem <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (l *stateLock) unlock() { <-l.sem }

// NewConnection returns an unconfigured Connection.
func NewConnection() *Connection {
	return &Connection{t: defaultTunables()}
}

// The ironflock package reaches a Connection's join counter and a
// DeliveryGroup's queue through package wampx, which is internal.
func init() {
	wampx.Bind(wampx.Binding{
		Joins: func(conn any) *wampx.Joins {
			if c, ok := conn.(*Connection); ok && c != nil {
				return &c.joins
			}
			return nil
		},
		RunInGroup: func(group any, fn func()) bool {
			g, ok := group.(*DeliveryGroup)
			return ok && g != nil && g.queue.submit(fn)
		},
	})
}

// Configure sets the connection parameters. It resolves the realm, the
// serial number and the router URL, and fails when they cannot be resolved.
// It must be called before Start and must not be called after, even after a
// failed Start.
func (c *Connection) Configure(cfg Config) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stopped {
		return c.stoppedErrLocked()
	}
	if c.started || c.done != nil {
		return errors.New("wamp: Configure called after Start")
	}
	if c.t.now == nil {
		c.t = defaultTunables()
	}

	if cfg.Stage == "" {
		cfg.Stage = StageDevelopment
	} else {
		cfg.Stage = StageFromEnv(string(cfg.Stage))
	}
	if cfg.Realm == "" {
		cfg.Realm = RealmName(cfg.SwarmKey, cfg.AppKey, cfg.Stage)
	}
	serial, err := SerialNumber(cfg.SerialNumber)
	if err != nil {
		return err
	}
	cfg.SerialNumber = serial
	if cfg.URL == "" {
		if cfg.URL, err = WebSocketURI(""); err != nil {
			return err
		}
	}
	dialURL, err := toDialURL(cfg.URL)
	if err != nil {
		return err
	}
	if cfg.SessionWaitTimeout <= 0 {
		cfg.SessionWaitTimeout = DefaultSessionWaitTimeout
	}
	if cfg.FirstConnectTimeout <= 0 {
		cfg.FirstConnectTimeout = DefaultFirstConnectTimeout
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}

	c.cfg = cfg
	c.dialURL = dialURL
	c.log = logger.With("realm", cfg.Realm)
	if (cfg.AuthID == "") != (cfg.AuthSecret == "") {
		c.log.Warn("Only one of AuthID and AuthSecret is set; using the injected app credential instead")
	}
	if c.runCtx == nil {
		c.runCtx, c.cancelRun = context.WithCancel(context.Background())
		c.upCh = make(chan struct{})
	}
	c.configured = true
	return nil
}

// Start starts the supervisor and blocks until the first successful join.
//
// The primary connection waits until ctx is done: a realm that does not exist
// yet (the concurrent-install race) is retried until it appears. With
// FailOnAuthError the wait is additionally bounded by FirstConnectTimeout,
// and a fatal auth denial returns an *AuthError immediately.
//
// When the wait ends — ctx, or FirstConnectTimeout — just as the first join
// completes, Start returns nil: a session announced through OnConnect is
// never torn down by a failing Start.
//
// When Start fails, its supervisor has exited by the time it returns, so no
// further attempt is made; an attempt still in its WebSocket handshake is
// abandoned at once. Unless the connection was stopped for good — by Stop, or
// by a fatal auth denial — it is then configured but not started again, and
// Start may be called again: tracked subscriptions and registrations are
// kept for the next join, and operations waiting for a session keep waiting
// within their own windows. Start on a started connection is an error; on a
// stopped one it returns ErrStopped.
func (c *Connection) Start(ctx context.Context) error {
	c.mu.Lock()
	switch {
	case !c.configured:
		c.mu.Unlock()
		return ErrNotConfigured
	case c.stopped:
		err := c.stoppedErrLocked()
		c.mu.Unlock()
		return err
	case c.started:
		c.mu.Unlock()
		return errAlreadyStarted
	}
	c.started = true
	sctx, endSupervisor := context.WithCancel(c.runCtx)
	c.endSupervisor = endSupervisor
	done := make(chan struct{})
	c.done = done
	up, stopped := c.upCh, c.runCtx.Done()
	c.mu.Unlock()

	c.log.Info("Starting connection to IronFlock app realm", "url", c.cfg.URL)
	go c.supervise(sctx, done)

	var timeout <-chan time.Time
	if c.cfg.FailOnAuthError {
		timer := time.NewTimer(c.cfg.FirstConnectTimeout)
		defer timer.Stop()
		timeout = timer.C
	}
	var err error
	select {
	case <-up:
		return nil
	case <-stopped: // Stop or a fatal auth denial
	case <-ctx.Done():
		err = fmt.Errorf("wamp: no session on realm %s: %w", c.cfg.Realm, ctx.Err())
	case <-timeout:
		err = &connectTimeoutError{realm: c.cfg.Realm, timeout: c.cfg.FirstConnectTimeout}
	}
	if !c.abortStart(done) {
		return nil // the session came up just as the wait ended
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	switch {
	case c.fatal != nil:
		return c.fatal // the cause, whichever way the wait ended
	case err == nil:
		return c.stoppedErrLocked()
	}
	return err
}

// abortStart ends the supervisor of a failed Start and waits until it has
// exited (done), closing the connection forcibly once startTeardown has
// passed. Unless the connection was stopped for good meanwhile, it is then
// configured but not started again: on its way out the supervisor withdrew
// its session and peer, and left an open upCh behind. The tracked
// subscriptions and registrations are kept.
//
// The supervisor must be gone before Start may run again: it reads and
// writes the connection's session state until it exits.
//
// It aborts nothing, and reports false, when the supervisor has published
// its session already — the wait ended just as the join completed, and
// OnConnect is on its way — and the connection is not stopped: the Start has
// succeeded after all. The check and the end of the supervisor are one
// critical section, as is establish's check of the supervisor's context and
// its publishing of the session, so exactly one of them wins. When Start
// begins c.sess is nil (a failed Start's supervisor withdraws its session
// before it exits), so a session found here is this Start's.
func (c *Connection) abortStart(done <-chan struct{}) (aborted bool) {
	c.mu.Lock()
	if c.sess != nil && !c.stopped {
		c.mu.Unlock()
		return false
	}
	c.endSupervisor()
	c.mu.Unlock()
	timer := time.NewTimer(c.t.startTeardown)
	select {
	case <-done:
		timer.Stop()
	case <-timer.C:
		c.forceClose()
		<-done
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.stopped {
		c.started = false
	}
	return true
}

// stopInternal marks the connection stopped and cancels the supervisor. It
// returns the supervisor's done channel (nil if it never started).
func (c *Connection) stopInternal() <-chan struct{} {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.stopped {
		c.stopped = true
		c.stopFlag.Store(true)
		if c.cancelRun != nil {
			c.cancelRun()
		}
	}
	return c.done
}

// Stop ends the session (WAMP GOODBYE) and the supervisor, and waits for
// them within ctx. It is idempotent and final: a stopped connection cannot
// be started again, and its operations fail with ErrStopped. A Start still
// running returns ErrStopped. An attempt still connecting is abandoned at
// once, even in the middle of its WebSocket handshake.
//
// When ctx ends first the WebSocket is closed without waiting for the
// router's GOODBYE and ctx.Err() is returned; the supervisor exits shortly
// after. Event and procedure handlers still running are not waited for, but
// no queued event is delivered after Stop.
func (c *Connection) Stop(ctx context.Context) error {
	done := c.stopInternal()
	if done == nil {
		return nil
	}
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		c.forceClose()
		return ctx.Err()
	}
}

// IsOpen reports whether a session is currently established.
func (c *Connection) IsOpen() bool {
	s := c.currentSession()
	return s != nil && s.alive()
}

// currentSession returns the current, restored session, or nil.
func (c *Connection) currentSession() *session {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.sess
}

// Realm returns the configured realm name.
func (c *Connection) Realm() string { return c.cfg.Realm }

// URL returns the resolved router URL.
func (c *Connection) URL() string { return c.cfg.URL }

// SerialNumber returns the device serial number.
func (c *Connection) SerialNumber() string { return c.cfg.SerialNumber }

// WaitSession blocks until a session is established, at most timeout (0
// uses SessionWaitTimeout) or until ctx is done.
//
// Errors: ErrNotConfigured before Configure; ErrStopped after Stop or a fatal
// auth denial (wrapped with the *AuthError); otherwise, on timeout, an error
// wrapping ErrNotConnected whose message reads "Not connected to the
// IronFlock router (realm '<realm>', no session after <n>s). Ensure Start()
// was called and the connection is established."; ctx.Err() when ctx ends
// first.
func (c *Connection) WaitSession(ctx context.Context, timeout time.Duration) error {
	if timeout <= 0 {
		timeout = c.defaultWait()
	}
	_, err := c.waitSession(ctx, time.Now().Add(timeout), timeout)
	return err
}

func (c *Connection) defaultWait() time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.cfg.SessionWaitTimeout > 0 {
		return c.cfg.SessionWaitTimeout
	}
	return DefaultSessionWaitTimeout
}

// waitSession returns the current session, waiting for one until deadline.
// reported is the wait named in the timeout error.
func (c *Connection) waitSession(ctx context.Context, deadline time.Time, reported time.Duration) (*session, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var timer *time.Timer
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()
	for {
		c.mu.Lock()
		if !c.configured {
			c.mu.Unlock()
			return nil, ErrNotConfigured
		}
		if c.stopped {
			err := c.stoppedErrLocked()
			c.mu.Unlock()
			return nil, err
		}
		s, wake, stop, realm := c.sess, c.upCh, c.runCtx.Done(), c.cfg.Realm
		c.mu.Unlock()
		if s != nil {
			if s.alive() {
				return s, nil
			}
			// Gone, but the supervisor has not noticed yet.
			wake = s.down
		}
		if timer == nil {
			wait := time.Until(deadline)
			if wait <= 0 {
				return nil, &notConnectedError{realm: realm, wait: reported}
			}
			timer = time.NewTimer(wait)
		}
		select {
		case <-wake:
		case <-stop:
		case <-timer.C:
			return nil, &notConnectedError{realm: realm, wait: reported}
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// sleep waits d, returning early with ctx's error or ErrStopped.
func (c *Connection) sleep(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-c.runCtx.Done():
		return c.stoppedErr()
	}
}

// Call calls a remote procedure.
//
// retryWindow rides out a platform restart for procedures the platform
// serves on the app's realm: Call waits up to that long for a session, and
// retries while the procedure is not served yet — the router answers
// wamp.error.no_such_procedure (after a router or data-backend restart the
// backend re-registers its procedures a few seconds after the app's session
// is back), or the callee's WAMP client refuses the call with
// wamp.error.invalid_argument "client has no handler for registration …"
// because the call overtook its registration (nexus clients install the
// handler only after REGISTERED), or its callee is unregistering it (see
// Unregister) — sleeping RetryFirstDelay, doubling up to RetryMaxDelay, as
// long as the next attempt still starts within the window.
// In all these cases the call never reached a handler, so a retry cannot run
// it twice (IsNotServedYet tells these refusals). Every other error,
// including a connection lost mid-call, is returned as it comes. retryWindow
// 0 waits the default session timeout and does not retry.
//
// When ctx ends, Call returns ctx.Err() at once — also when the result is
// arriving just then — and the router is told to cancel the call, which
// interrupts the callee's handler. That exchange finishes in the background,
// within the router response timeout (DefaultSessionWaitTimeout); it may
// outlive Call, and Stop. A call whose session ends before it is answered
// fails with an error wrapping ErrNotConnected (ErrStopped after Stop): it
// may have run.
//
// opts.OnProgress receives the call's progressive results (see
// CallOptions.OnProgress); without it, a progressive result from the callee
// fails the call, and receive_progress in opts.Extra is refused before the
// call is sent.
//
// Errors from the router or callee are returned as *Error.
func (c *Connection) Call(ctx context.Context, procedure string, args []any, kwargs map[string]any, opts *CallOptions, retryWindow time.Duration) (*Result, error) {
	if err := validateURI("procedure", procedure); err != nil {
		return nil, err
	}
	if opts != nil && opts.OnProgress == nil {
		// nexus would answer the call with the first progressive result,
		// and the next ones would race the end of the call's wait, which
		// can wedge its receive loop (see Connection.awaitEnd).
		if asked, _ := opts.Extra[nxwamp.OptReceiveProgress].(bool); asked {
			return nil, fmt.Errorf("wamp: call of procedure '%s': receive_progress in CallOptions.Extra "+
				"needs CallOptions.OnProgress", procedure)
		}
	}
	var deadline time.Time
	if retryWindow > 0 {
		deadline = time.Now().Add(retryWindow)
	}
	delay := c.t.retryFirstDelay
	for {
		var s *session
		var err error
		if retryWindow > 0 {
			s, err = c.waitSession(ctx, deadline, max(time.Until(deadline), 0).Round(time.Millisecond))
		} else {
			wait := c.defaultWait()
			s, err = c.waitSession(ctx, time.Now().Add(wait), wait)
		}
		if err != nil {
			return nil, err
		}
		// A fresh queue per attempt: a retried attempt was refused before
		// any handler ran, so it sent no progressive results.
		var progress *progressQueue
		var progcb client.ProgressHandler
		if opts != nil && opts.OnProgress != nil {
			progress = newProgressQueue(opts.OnProgress, c.log, procedure)
			progcb = progress.add
		}
		res, err := s.call(ctx, procedure, callOptions(opts), wireArgs(args, kwargs), nxwamp.Dict(kwargs), progcb)
		if progress != nil {
			// nexus has handed over every progressive result that came
			// before the final one: pass them all on. A failed call drops
			// what is left; nexus may still be finishing it in the
			// background, and its late results are dropped too.
			progress.close(err != nil)
		}
		if err == nil {
			if progress == nil && isProgressive(res) {
				return nil, fmt.Errorf("wamp: call of procedure '%s': the callee sent a progressive result, "+
					"which needs CallOptions.OnProgress", procedure)
			}
			return newResult(res), nil
		}
		err = c.callError(procedure, err)
		why := notServedYet(err)
		if retryWindow <= 0 || why == "" || time.Now().Add(delay).After(deadline) {
			return nil, err
		}
		c.log.Debug(why+"; retrying", "procedure", procedure, "retry_in", delay)
		if err := c.sleep(ctx, delay); err != nil {
			return nil, err
		}
		delay = min(delay*2, c.t.retryMaxDelay)
	}
}

// noHandlerYet starts the text of the wamp.error.invalid_argument a nexus
// client answers an INVOCATION with when it has no handler for its
// registration (yet).
const noHandlerYet = "client has no handler for registration"

// IsNotServedYet reports whether err is the refusal of a call that reached
// no handler because its procedure is not served yet: the router's
// wamp.error.no_such_procedure (no callee has registered it), or the
// callee's wamp.error.invalid_argument whose first argument starts with
// "client has no handler for registration" — a nexus client gives that
// answer to a call that overtook its registration (it installs the handler
// only after REGISTERED), or that arrives while it unregisters the
// procedure. Such a call never ran, so calling again cannot run it twice.
// These are the refusals Call's retry window retries.
func IsNotServedYet(err error) bool { return notServedYet(err) != "" }

// notServedYet tells whether err is the refusal of a call that reached no
// handler because its procedure is not served yet (see IsNotServedYet). It
// returns why, for the log, or "" for any other error.
func notServedYet(err error) string {
	var werr *Error
	if !errors.As(err, &werr) || werr == nil {
		return ""
	}
	switch werr.URI {
	case URINoSuchProcedure:
		return "Procedure not registered yet"
	case string(nxwamp.ErrInvalidArgument):
		if len(werr.Args) > 0 {
			if text, ok := werr.Args[0].(string); ok && strings.HasPrefix(text, noHandlerYet) {
				return "Procedure still being registered by its callee"
			}
		}
	}
	return ""
}

// Publish publishes an event, waiting up to waitWindow (0: the default
// session timeout) for a session. With opts.Acknowledge it waits for the
// router to confirm, and a refusal is returned as *Error.
func (c *Connection) Publish(ctx context.Context, topic string, args []any, kwargs map[string]any, opts *PublishOptions, waitWindow time.Duration) error {
	if err := validateURI("topic", topic); err != nil {
		return err
	}
	if waitWindow <= 0 {
		waitWindow = c.defaultWait()
	}
	s, err := c.waitSession(ctx, time.Now().Add(waitWindow), waitWindow)
	if err != nil {
		return err
	}
	err = s.cli.Publish(topic, publishOptions(opts), wireArgs(args, kwargs), nxwamp.Dict(kwargs))
	return c.requestError("publish to topic '"+topic+"'", publishPrefix, err)
}

// Subscribe subscribes handler to topic and tracks the subscription so it is
// restored after every reconnect. Several handlers may subscribe to the same
// topic (and match policy): they share one WAMP subscription and each
// receives every event. A refusal is returned as *Error.
//
// The WAMP subscription takes the options of the first subscription of its
// topic. Subscribing a topic that is already subscribed with a different
// match policy is an error: nexus can hold only one subscription per topic
// string. opts.Group applies to this handler, not to the WAMP subscription.
//
// Subscribe waits up to the default session timeout for a session, also for
// the next one when the session is lost while the subscription is made;
// SubscribeWithin takes the wait. When ctx ends while Subscribe waits for a
// session or for subscription changes in progress, it returns ctx.Err() and
// has sent nothing.
func (c *Connection) Subscribe(ctx context.Context, topic string, handler EventHandler, opts *SubscribeOptions) (*Subscription, error) {
	return c.SubscribeWithin(ctx, topic, handler, opts, 0)
}

// SubscribeWithin is Subscribe waiting up to waitWindow (0: the default
// session timeout) for a session, as Publish does. It is not retried: a
// refusal is returned as it comes.
func (c *Connection) SubscribeWithin(ctx context.Context, topic string, handler EventHandler, opts *SubscribeOptions, waitWindow time.Duration) (*Subscription, error) {
	if err := validateURI("topic", topic); err != nil {
		return nil, err
	}
	if handler == nil {
		return nil, errors.New("wamp: subscribe: nil handler")
	}
	var extra map[string]any
	var group *DeliveryGroup
	if opts != nil {
		extra, group = opts.Extra, opts.Group
	}
	match, err := matchPolicy(optMatch(opts), extra)
	if err != nil {
		return nil, err
	}
	options := subscribeOptions(opts, match)

	wait := waitWindow
	if wait <= 0 {
		wait = c.defaultWait()
	}
	deadline := time.Now().Add(wait)
	for {
		s, err := c.waitSession(ctx, deadline, wait)
		if err != nil {
			return nil, err
		}
		sub, retry, err := c.subscribeOn(ctx, s, newSubscription(c, topic, match, handler, group), options)
		if !retry {
			return sub, err
		}
	}
}

func optMatch(opts *SubscribeOptions) string {
	if opts == nil {
		return ""
	}
	return opts.Match
}

// subscribeOn subscribes sub on s, in a new group made with options unless
// its topic has one already. retry reports that s was lost before the
// subscription could be made, so the caller should try the next session.
func (c *Connection) subscribeOn(ctx context.Context, s *session, sub *Subscription, options nxwamp.Dict) (_ *Subscription, retry bool, err error) {
	if err := c.state.lock(ctx); err != nil {
		return nil, false, err
	}
	defer c.state.unlock()
	if err := ctx.Err(); err != nil { // ctx ended as the lock came free
		return nil, false, err
	}
	if c.currentSession() != s {
		return nil, true, nil
	}
	g := c.findGroup(sub.topic)
	if g != nil && g.match != sub.match {
		return nil, false, fmt.Errorf("wamp: topic '%s' is already subscribed with match policy %q; "+
			"one connection can subscribe a topic with only one match policy", sub.topic, matchName(g.match))
	}
	tracked := g != nil
	if !tracked {
		g = &subGroup{topic: sub.topic, match: sub.match, options: options}
	}
	// The handle joins the group before the SUBSCRIBE: the router may send
	// the first events right behind SUBSCRIBED.
	sub.group = g
	g.add(sub)
	if tracked && g.sess.Load() == s {
		return sub, false, nil // the WAMP subscription exists
	}
	if err := s.subscribe(sub.topic, g.onEvent, cloneDict(g.options)); err != nil {
		g.remove(sub)
		if !s.alive() && !c.stopFlag.Load() {
			return nil, true, nil
		}
		return nil, false, c.requestError("subscribe to topic '"+sub.topic+"'", subscribePrefix(sub.topic), err)
	}
	if hook := c.t.afterSubscribe; hook != nil {
		hook()
	}
	g.sess.Store(s)
	if !tracked {
		c.setGroups(append(slices.Clone(c.groupList()), g))
	}
	return sub, false, nil
}

func matchName(m string) string {
	if m == "" {
		return "exact"
	}
	return m
}

// groupList returns the tracked groups. The slice must not be modified.
func (c *Connection) groupList() []*subGroup {
	if gs := c.groups.Load(); gs != nil {
		return *gs
	}
	return nil
}

// setGroups replaces the tracked groups; c.state must be held.
func (c *Connection) setGroups(gs []*subGroup) { c.groups.Store(&gs) }

func (c *Connection) findGroup(topic string) *subGroup {
	for _, g := range c.groupList() {
		if g.topic == topic {
			return g
		}
	}
	return nil
}

// Unsubscribe removes a subscription. It takes effect at once: the handler
// is called for no further event — events still queued for it are dropped;
// a call already under way is not waited for — and the subscription is never
// restored again. Once no handler of its topic remains, the WAMP
// subscription is ended at the router, after any subscription change in
// progress; when ctx ends first, Unsubscribe returns ctx.Err() and the
// router is told in the background. Other handlers of the topic keep the
// WAMP subscription and their events. Unsubscribing an already-removed
// subscription is a no-op, and so is a nil one.
//
// Unsubscribe does not wait for a session: while the connection is down it
// removes the subscription locally and returns — the router dropped the
// session's subscriptions with the session, and the subscription is not
// restored after the reconnect. While connected it returns once the router
// has answered; a refusal is returned, and the local removal stands.
func (c *Connection) Unsubscribe(ctx context.Context, sub *Subscription) error {
	if sub == nil || sub.conn != c || sub.group == nil || !sub.detach() {
		return nil
	}
	return c.untrack(ctx, "unsubscribe from topic '"+sub.topic+"'", func() error {
		return c.untrackSubscriptions(sub.group, sub)
	})
}

// UnsubscribeTopic removes every current subscription of topic, the way
// Unsubscribe removes one.
func (c *Connection) UnsubscribeTopic(ctx context.Context, topic string) error {
	g := c.findGroup(topic)
	if g == nil {
		return nil
	}
	var subs []*Subscription
	for _, sub := range g.list() {
		if sub.detach() {
			subs = append(subs, sub)
		}
	}
	if len(subs) == 0 {
		return nil
	}
	return c.untrack(ctx, "unsubscribe from topic '"+topic+"'", func() error {
		return c.untrackSubscriptions(g, subs...)
	})
}

// untrack runs finish, which completes the removal of a subscription or
// registration that has been detached already, holding c.state. When ctx
// ends before c.state is free, finish runs in the background, and untrack
// returns ctx.Err(); what, the operation, names a failure in the log then.
func (c *Connection) untrack(ctx context.Context, what string, finish func() error) error {
	if err := c.state.lock(ctx); err != nil {
		go func() { //nolint:gosec // G118: finishes after the caller's context ended, by design (see above)
			_ = c.state.lock(context.Background()) // never fails
			defer c.state.unlock()
			if err := finish(); err != nil {
				c.log.Warn("Failed to "+what+" after the caller's context ended", "error", err)
			}
		}()
		return err
	}
	defer c.state.unlock()
	return finish()
}

// untrackSubscriptions removes the detached subs from g, and g itself — with
// its WAMP subscription — once no handler is left in it. c.state must be
// held.
func (c *Connection) untrackSubscriptions(g *subGroup, subs ...*Subscription) error {
	for _, sub := range subs {
		g.remove(sub)
	}
	if len(g.list()) > 0 || !slices.Contains(c.groupList(), g) {
		return nil
	}
	c.setGroups(slices.DeleteFunc(slices.Clone(c.groupList()), func(x *subGroup) bool { return x == g }))
	return c.dropSubscription(g)
}

// dropSubscription ends the WAMP subscription of an untracked group, if it
// lives on the current session. c.state must be held.
func (c *Connection) dropSubscription(g *subGroup) error {
	s := g.sess.Swap(nil)
	if s == nil || s != c.currentSession() {
		return nil // died with its session: nothing to undo
	}
	err := c.requestError("unsubscribe from topic '"+g.topic+"'", unsubscribePrefix(g.topic), s.cli.Unsubscribe(g.topic))
	if isGone(err, "wamp.error.no_such_subscription") {
		c.log.Debug("Ignoring stale subscription handle", "topic", g.topic, "error", err)
		return nil
	}
	return err
}

// Register registers handler as procedure and tracks the registration so it
// is restored after every reconnect. ForceReregister defaults to true. A
// refusal is returned as *Error.
//
// A procedure can be registered once per connection: registering it again
// before Unregister fails with wamp.error.procedure_already_exists.
//
// The handler's results and *Error payloads are sent as given, encoded by
// the WAMP client's msgpack codec — not converted the way the ironflock
// package converts its payloads (IronFlock.RegisterDeviceFunction does that
// for its handlers). Return JSON-like values: nil, booleans, numbers,
// strings, []byte, []any and map[string]any. A value the codec cannot encode,
// or that the router cannot decode — a map with non-string keys, for one —
// never reaches the caller, who gets no answer; a string that is not valid
// UTF-8 makes an autobahn-python caller (the Python SDK) drop its session.
//
// Register waits up to the default session timeout for a session, also for
// the next one when the session is lost while the registration is made;
// RegisterWithin takes the wait. When ctx ends while Register waits for a
// session or for subscription changes in progress, it returns ctx.Err() and
// has sent nothing.
func (c *Connection) Register(ctx context.Context, procedure string, handler InvocationHandler, opts *RegisterOptions) (*Registration, error) {
	return c.RegisterWithin(ctx, procedure, handler, opts, 0)
}

// RegisterWithin is Register waiting up to waitWindow (0: the default
// session timeout) for a session, as Publish does. It is not retried: a
// refusal is returned as it comes.
func (c *Connection) RegisterWithin(ctx context.Context, procedure string, handler InvocationHandler, opts *RegisterOptions, waitWindow time.Duration) (*Registration, error) {
	if err := validateURI("procedure", procedure); err != nil {
		return nil, err
	}
	if handler == nil {
		return nil, errors.New("wamp: register: nil handler")
	}
	var match string
	var extra map[string]any
	if opts != nil {
		match, extra = opts.Match, opts.Extra
	}
	policy, err := matchPolicy(match, extra)
	if err != nil {
		return nil, err
	}
	reg := &Registration{conn: c, procedure: procedure, handler: handler, options: registerOptions(opts, policy)}

	wait := waitWindow
	if wait <= 0 {
		wait = c.defaultWait()
	}
	deadline := time.Now().Add(wait)
	for {
		s, err := c.waitSession(ctx, deadline, wait)
		if err != nil {
			return nil, err
		}
		retry, err := c.registerOn(ctx, s, reg)
		if !retry {
			if err != nil {
				return nil, err
			}
			return reg, nil
		}
	}
}

func (c *Connection) registerOn(ctx context.Context, s *session, reg *Registration) (retry bool, err error) {
	if err := c.state.lock(ctx); err != nil {
		return false, err
	}
	defer c.state.unlock()
	if err := ctx.Err(); err != nil { // ctx ended as the lock came free
		return false, err
	}
	if c.currentSession() != s {
		return true, nil
	}
	if i := slices.IndexFunc(c.regs, func(r *Registration) bool { return r.procedure == reg.procedure }); i >= 0 {
		prev := c.regs[i]
		if !prev.removed.Load() {
			return false, &Error{URI: URIProcedureAlreadyExists, Args: []any{
				fmt.Sprintf("procedure '%s' is already registered on this connection", reg.procedure)}}
		}
		// Unregistered, but its removal is still waiting for c.state:
		// finish it here, so that the procedure is registered afresh.
		if err := c.untrackRegistration(prev); err != nil {
			c.log.Warn("Failed to unregister the previous registration; registering the procedure anew",
				"procedure", reg.procedure, "error", err)
		}
	}
	if err := s.register(reg.procedure, reg.invokeOn(s), cloneDict(reg.options)); err != nil {
		if !s.alive() && !c.stopFlag.Load() {
			return true, nil
		}
		return false, c.requestError("register procedure '"+reg.procedure+"'", registerPrefix(reg.procedure), err)
	}
	reg.sess.Store(s)
	c.regs = append(c.regs, reg)
	return false, nil
}

// Unregister removes a registration, the way Unsubscribe removes a
// subscription: at once, the handler is called no more, and the registration
// is never restored again. Until the router has dropped it — Unregister
// returns once it has — the router may still route calls here, and they are
// refused without reaching the handler: with wamp.error.no_such_procedure
// while the removal waits for a subscription change in progress, and, for
// about one round trip once the UNREGISTER is sent, with
// wamp.error.invalid_argument "client has no handler for registration N"
// (the WAMP client drops the handler before it sends UNREGISTER). Call with
// a retry window retries both (see IsNotServedYet); the Python and
// JavaScript SDKs retry only no_such_procedure. The router is told after any
// subscription change in progress; when ctx ends first, Unregister returns
// ctx.Err() and the router is told in the background. A refusal is returned,
// and the local removal stands. Unregistering an already-removed
// registration is a no-op, and so is a nil one.
//
// Unregister does not wait for a session: while the connection is down it
// removes the registration locally and returns — the router dropped the
// session's registrations with the session, and the registration is not
// restored after the reconnect.
func (c *Connection) Unregister(ctx context.Context, reg *Registration) error {
	if reg == nil || reg.conn != c || !reg.removed.CompareAndSwap(false, true) {
		return nil
	}
	return c.untrack(ctx, "unregister procedure '"+reg.procedure+"'", func() error {
		return c.untrackRegistration(reg)
	})
}

// untrackRegistration removes reg, detached already, from the tracked
// registrations, and ends its WAMP registration if it lives on the current
// session. c.state must be held.
func (c *Connection) untrackRegistration(reg *Registration) error {
	if !slices.Contains(c.regs, reg) {
		return nil
	}
	c.regs = slices.DeleteFunc(c.regs, func(r *Registration) bool { return r == reg })
	s := reg.sess.Swap(nil)
	if s == nil || s != c.currentSession() {
		return nil // died with its session: nothing to undo
	}
	err := c.requestError("unregister procedure '"+reg.procedure+"'", unregisterPrefix(reg.procedure), s.cli.Unregister(reg.procedure))
	if isGone(err, "wamp.error.no_such_registration") {
		// Evicted by another session's force_reregister, or lost with the
		// session: either way it is no longer registered.
		c.log.Debug("Ignoring stale registration handle", "procedure", reg.procedure, "error", err)
		return nil
	}
	return err
}

// isGone reports whether an UNSUBSCRIBE or UNREGISTER failed only because
// there is nothing left to undo: the session is gone, nexus no longer knows
// the handle, or the router answers goneURI.
func isGone(err error, goneURI string) bool {
	if err == nil {
		return false
	}
	var werr *Error
	if errors.As(err, &werr) {
		return werr.URI == goneURI
	}
	return errors.Is(err, ErrNotConnected) || errors.Is(err, ErrStopped) ||
		errors.Is(err, client.ErrNotSubscribed) || errors.Is(err, client.ErrNotRegistered)
}

// Subscription is a tracked subscription. The handle stays valid across
// reconnects.
type Subscription struct {
	conn    *Connection
	topic   string
	match   string
	handler EventHandler

	group   *subGroup       // set before the handle is returned
	own     serialExecutor  // delivers the events, unless a DeliveryGroup does
	queue   *serialExecutor // &own, or the DeliveryGroup's
	removed atomic.Bool
}

func newSubscription(c *Connection, topic, match string, handler EventHandler, group *DeliveryGroup) *Subscription {
	sub := &Subscription{conn: c, topic: topic, match: match, handler: handler}
	sub.queue = &sub.own
	if group != nil {
		sub.queue = &group.queue
	}
	return sub
}

// detach removes the subscription locally: its handler is called for no
// further event. It reports false if the subscription was removed already.
func (s *Subscription) detach() bool {
	if !s.removed.CompareAndSwap(false, true) {
		return false
	}
	// Drop the events still queued. In a DeliveryGroup they are skipped
	// when their turn comes (see Connection.runEventHandler).
	if s.queue == &s.own {
		s.own.close()
	}
	return true
}

// Topic returns the subscribed topic (or pattern).
func (s *Subscription) Topic() string { return s.topic }

// Match returns the match policy ("" for exact).
func (s *Subscription) Match() string { return s.match }

// Active reports whether the subscription is currently live at the router —
// false while the connection is down, after a failed restore until a retry
// succeeds, and after Unsubscribe.
func (s *Subscription) Active() bool {
	if s == nil || s.removed.Load() || s.group == nil {
		return false
	}
	live := s.group.sess.Load()
	return live != nil && live == s.conn.currentSession() && live.alive()
}

// Unsubscribe is shorthand for Connection.Unsubscribe.
func (s *Subscription) Unsubscribe(ctx context.Context) error {
	if s == nil {
		return nil
	}
	return s.conn.Unsubscribe(ctx, s)
}

// Registration is a tracked registration. The handle stays valid across
// reconnects.
type Registration struct {
	conn      *Connection
	procedure string
	handler   InvocationHandler

	options nxwamp.Dict
	sess    atomic.Pointer[session] // session holding the WAMP registration
	removed atomic.Bool
}

// Procedure returns the registered procedure URI.
func (r *Registration) Procedure() string { return r.procedure }

// Active reports whether the registration is currently live at the router —
// false while the connection is down, after a failed restore until a retry
// succeeds, once another session has taken the procedure over (until the
// next reconnect registers it again), and after Unregister.
func (r *Registration) Active() bool {
	if r == nil || r.removed.Load() {
		return false
	}
	live := r.sess.Load()
	if live == nil || live != r.conn.currentSession() || !live.alive() {
		return false
	}
	id, ok := live.cli.RegistrationID(r.procedure)
	return ok && !live.peer.isEvicted(id)
}

// Unregister is shorthand for Connection.Unregister.
func (r *Registration) Unregister(ctx context.Context) error {
	if r == nil {
		return nil
	}
	return r.conn.Unregister(ctx, r)
}
