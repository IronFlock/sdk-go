// Package ironflock is the Go SDK of the IronFlock IoT DevOps platform.
//
// An app running on an IronFlock device uses it to publish data into its
// fleet's tables, read table history, subscribe to realtime rows, call and
// register device functions, read other apps' shared data, and store files.
// When the app runs on a device registered in IronFlock, the SDK reads its
// identity and credentials from the environment the device agent injects
// and joins the app's private data realm of the fleet automatically.
//
//	ifl, err := ironflock.New()
//	if err != nil { log.Fatal(err) }
//	err = ifl.Run(context.Background(), func(ctx context.Context) error {
//		for {
//			row := ironflock.Row{"tsp": time.Now(), "temperature": 22.5}
//			if err := ifl.PublishToTable(ctx, "sensordata", row); err != nil {
//				log.Print(err)
//			}
//			select {
//			case <-ctx.Done():
//				return nil
//			case <-time.After(3 * time.Second):
//			}
//		}
//	})
//
// Every table has a mandatory tsp column, the row's timestamp: a row the app
// writes carries it (a time.Time is sent as RFC 3339 in UTC), unless the
// table's data template reads tsp from another part of the message.
//
// The connection reconnects on its own and restores every subscription and
// registered device function after a reconnect. Table operations ride out a
// platform restart for up to the reconnect window (60s by default) before
// they fail.
package ironflock

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/ironflock/sdk-go/filestore"
	"github.com/ironflock/sdk-go/internal/env"
	"github.com/ironflock/sdk-go/wamp"
)

// Aliases of the connection-level types, so most apps need only this package.
type (
	Event             = wamp.Event
	EventHandler      = wamp.EventHandler
	Invocation        = wamp.Invocation
	InvocationHandler = wamp.InvocationHandler
	Result            = wamp.Result
	WampError         = wamp.Error
	SubscribeOptions  = wamp.SubscribeOptions
	RegisterOptions   = wamp.RegisterOptions
	CallOptions       = wamp.CallOptions
	Subscription      = wamp.Subscription
	Registration      = wamp.Registration
	Stage             = wamp.Stage
	FileStore         = filestore.FileStore
	FileStoreError    = filestore.Error
)

// Stages.
const (
	StageDevelopment = wamp.StageDevelopment
	StageProduction  = wamp.StageProduction
)

// ErrorLogsTable is the per-data-backend error table ReportError writes to.
// It behaves like any other table (GetHistory, SubscribeToTable, boards).
const ErrorLogsTable = "error-logs"

// DefaultReconnectWindow is how long a table operation rides out a platform
// restart by default (see WithReconnectWindow).
const DefaultReconnectWindow = 60 * time.Second

// Shutdown budgets.
const (
	// defaultRunStopTimeout bounds the Stop that Run performs on shutdown.
	defaultRunStopTimeout = 10 * time.Second
	// defaultCleanupTimeout bounds tearing down what a failed or abandoned
	// operation left behind (a consumed-app connection whose open failed, a
	// half-made table subscription).
	defaultCleanupTimeout = 5 * time.Second
)

// Option configures New.
type Option func(*config)

type config struct {
	serialNumber    string
	deviceName      *string
	deviceKey       *string
	appName         *string
	swarmKey        *int
	appKey          *int
	env             *string
	reswarmURL      string
	url             string
	authID          string
	authSecret      string
	reconnectWindow time.Duration
	logger          *slog.Logger
}

// WithSerialNumber sets the device serial number (default: the
// DEVICE_SERIAL_NUMBER environment variable), which publications and table
// writes carry as DEVICE_SERIAL_NUMBER in their metadata. It does not change
// the identity the connection authenticates with while the device agent
// injects a per-app credential (APP_AUTH_ID/APP_AUTH_SECRET): the platform
// identifies the device from that credential. The serial is the credential
// only in the legacy (serial, serial) fallback, without an injected per-app
// credential. To present another credential, use WithCredentials.
func WithSerialNumber(serial string) Option { return func(c *config) { c.serialNumber = serial } }

// WithDeviceName overrides the DEVICE_NAME environment variable.
func WithDeviceName(name string) Option { return func(c *config) { c.deviceName = &name } }

// WithDeviceKey overrides the DEVICE_KEY environment variable.
func WithDeviceKey(key string) Option { return func(c *config) { c.deviceKey = &key } }

// WithAppName overrides the APP_NAME environment variable.
func WithAppName(name string) Option { return func(c *config) { c.appName = &name } }

// WithSwarmKey overrides the SWARM_KEY environment variable.
func WithSwarmKey(key int) Option { return func(c *config) { c.swarmKey = &key } }

// WithAppKey overrides the APP_KEY environment variable.
func WithAppKey(key int) Option { return func(c *config) { c.appKey = &key } }

// WithEnv overrides the ENV environment variable ("PROD" in any case selects
// the production realm; anything else the development realm).
func WithEnv(env string) Option { return func(c *config) { c.env = &env } }

// WithReswarmURL sets the studio URL used to look up the router (default:
// the RESWARM_URL environment variable). DEVICE_ENDPOINT_URL and WithURL
// take precedence.
func WithReswarmURL(u string) Option { return func(c *config) { c.reswarmURL = u } }

// WithURL sets the router WebSocket URL directly, e.g.
// "wss://cbw.ironflock.com/ws-ua-usr".
func WithURL(u string) Option { return func(c *config) { c.url = u } }

// WithCredentials sets the WAMP-CRA credential explicitly instead of the
// per-app credential the device agent injects (APP_AUTH_ID/APP_AUTH_SECRET)
// or the legacy (serial, serial) fallback.
func WithCredentials(authID, secret string) Option {
	return func(c *config) { c.authID, c.authSecret = authID, secret }
}

// WithReconnectWindow sets how long a table operation (AppendToTable,
// AppendRowsToTable, PublishToTable, PublishRowsToTable, ReportError,
// GetHistory, GetSeriesHistory, RevealSecrets, VerifySecret, and a consumed
// app's history reads) rides out a platform restart before it fails: it
// waits that long for the connection to come back and for the platform to
// serve the app's tables again. A table operation issued before Start waits
// for Start within the same window. Default 60s; 0 turns it off (the
// default 10s connection wait, no retries). Other operations, file calls
// included, wait 10s for the connection and are not retried.
func WithReconnectWindow(d time.Duration) Option {
	return func(c *config) { c.reconnectWindow = d }
}

// WithLogger sets the logger (default slog.Default()).
func WithLogger(l *slog.Logger) Option { return func(c *config) { c.logger = l } }

// wampConn is the connection surface the SDK uses; *wamp.Connection
// implements it. Tests substitute a fake.
type wampConn interface {
	Configure(cfg wamp.Config) error
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
	IsOpen() bool
	URL() string
	Call(ctx context.Context, procedure string, args []any, kwargs map[string]any, opts *wamp.CallOptions, retryWindow time.Duration) (*wamp.Result, error)
	Publish(ctx context.Context, topic string, args []any, kwargs map[string]any, opts *wamp.PublishOptions, waitWindow time.Duration) error
	Subscribe(ctx context.Context, topic string, handler wamp.EventHandler, opts *wamp.SubscribeOptions) (*wamp.Subscription, error)
	Unsubscribe(ctx context.Context, sub *wamp.Subscription) error
	Register(ctx context.Context, procedure string, handler wamp.InvocationHandler, opts *wamp.RegisterOptions) (*wamp.Registration, error)
	Unregister(ctx context.Context, reg *wamp.Registration) error
	WaitSession(ctx context.Context, timeout time.Duration) error
}

// IronFlock is a connection to the IronFlock platform for one app on one
// device. It is safe for concurrent use.
//
// Lifecycle: New, then Start (or Run, which starts, runs the app's main
// function and stops), then Stop, which is final.
//
// Operations may be issued before Start, for example by goroutines started
// before Run: they wait for Start to configure the connection, and then for
// the connection, within the time they wait for a connection anyway — the
// reconnect window for table operations (see WithReconnectWindow), 10s for
// the others, file calls included. When that time passes first they fail
// with an error wrapping wamp.ErrNotConnected, and Stop makes them fail with
// one wrapping wamp.ErrStopped.
type IronFlock struct {
	log *slog.Logger

	serialNumber string
	deviceName   string
	deviceKey    string
	appName      string
	swarmKey     int
	appKey       int
	stage        Stage
	reswarmURL   string
	url          string
	authID       string
	authSecret   string

	reconnectWindow time.Duration

	conn    wampConn
	newConn func() wampConn

	mu         sync.Mutex
	configured bool // by Start, once (see ready)
	started    bool // a Start is running or has succeeded
	stopped    bool
	files      *filestore.FileStore
	consumed   map[string]*consumedEntry
	// stopDone is closed when the first Stop has finished; nil before it.
	stopDone chan struct{}

	// ready is closed once Start has configured conn. Operations issued
	// before wait for it (see awaitStart).
	ready chan struct{}
	// startWaiters counts the operations waiting for ready.
	startWaiters atomic.Int32

	// lifetime is cancelled when Stop begins. It ends the waits for Start
	// and the context Run passes to main, and consumed-app opens run on it:
	// detached from the callers that share an open (one caller giving up
	// must not fail it for the others), but aborted by Stop.
	lifetime    context.Context
	endLifetime context.CancelFunc

	runStopTimeout time.Duration
	cleanupTimeout time.Duration
	// sessionWait is how long an operation without a window of its own
	// waits for Start: the connection's default session wait.
	sessionWait time.Duration
	// beforeOpenPublished and afterOpenPublished, set by tests, are called by
	// a successful consumed-app open that found the instance not stopped,
	// right before and right after it publishes its outcome, with f.mu held
	// (see runOpen).
	beforeOpenPublished func()
	afterOpenPublished  func()
}

// New creates an IronFlock instance from the environment the device agent
// injects (DEVICE_SERIAL_NUMBER, DEVICE_KEY, DEVICE_NAME, APP_NAME,
// SWARM_KEY, APP_KEY, ENV, DEVICE_ENDPOINT_URL / RESWARM_URL,
// APP_AUTH_ID / APP_AUTH_SECRET) and opts. It fails only when no serial
// number is available; other missing variables are logged as a warning and
// fail the operations that need them.
func New(opts ...Option) (*IronFlock, error) {
	return newWithConn(wamp.NewConnection(), func() wampConn { return wamp.NewConnection() }, opts...)
}

// newWithConn is New with the own connection and the factory of consumed-app
// connections injected.
func newWithConn(conn wampConn, newConn func() wampConn, opts ...Option) (*IronFlock, error) {
	c := config{reconnectWindow: DefaultReconnectWindow}
	for _, opt := range opts {
		if opt != nil {
			opt(&c)
		}
	}
	log := c.logger
	if log == nil {
		log = slog.Default()
	}

	serial, err := wamp.SerialNumber(c.serialNumber)
	if err != nil {
		return nil, missingConfigf("%v", err)
	}
	if (c.authID == "") != (c.authSecret == "") {
		return nil, invalidf("WithCredentials needs both an auth id and a secret")
	}

	f := &IronFlock{
		log:             log,
		serialNumber:    serial,
		deviceName:      stringSetting(c.deviceName, "DEVICE_NAME"),
		deviceKey:       stringSetting(c.deviceKey, "DEVICE_KEY"),
		appName:         stringSetting(c.appName, "APP_NAME"),
		swarmKey:        keySetting(log, c.swarmKey, "SWARM_KEY"),
		appKey:          keySetting(log, c.appKey, "APP_KEY"),
		stage:           wamp.StageFromEnv(stringSetting(c.env, "ENV")),
		reswarmURL:      c.reswarmURL,
		url:             c.url,
		authID:          c.authID,
		authSecret:      c.authSecret,
		reconnectWindow: max(c.reconnectWindow, 0),
		conn:            conn,
		newConn:         newConn,
		consumed:        make(map[string]*consumedEntry),
		ready:           make(chan struct{}),
		runStopTimeout:  defaultRunStopTimeout,
		cleanupTimeout:  defaultCleanupTimeout,
		sessionWait:     wamp.DefaultSessionWaitTimeout,
	}
	f.lifetime, f.endLifetime = context.WithCancel(context.Background())

	var missing []string
	if f.deviceKey == "" {
		missing = append(missing, "DEVICE_KEY")
	}
	if f.appName == "" {
		missing = append(missing, "APP_NAME")
	}
	if f.swarmKey <= 0 {
		missing = append(missing, "SWARM_KEY")
	}
	if f.appKey <= 0 {
		missing = append(missing, "APP_KEY")
	}
	if len(missing) > 0 {
		log.Warn("Warning: The following environment variables must be present: " + strings.Join(missing, ", "))
	}
	return f, nil
}

// stringSetting returns the option value when set, else the environment
// variable name.
func stringSetting(opt *string, name string) string {
	if opt != nil {
		return *opt
	}
	return os.Getenv(name)
}

// keySetting returns the option value when set, else the environment variable
// name parsed as an integer. Unset means 0; a value that is not an integer is
// logged and also reads as 0 (unset).
func keySetting(log *slog.Logger, opt *int, name string) int {
	if opt != nil {
		return *opt
	}
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return 0
	}
	v, err := strconv.Atoi(raw)
	if err != nil {
		log.Warn(fmt.Sprintf("%s=%q is not an integer; treating it as unset", name, raw))
		return 0
	}
	return v
}

// Connection returns the underlying connection, for advanced use. Its
// operations do not wait for Start as the IronFlock's do: until Start has
// configured the connection they fail with wamp.ErrNotConfigured.
func (f *IronFlock) Connection() *wamp.Connection {
	c, _ := f.conn.(*wamp.Connection)
	return c
}

// IsConnected reports whether the connection to the platform is established.
func (f *IronFlock) IsConnected() bool { return f.conn.IsOpen() }

// Stage returns the stage of the realm the app joins: StageDevelopment
// ("DEV") or StageProduction ("PROD"). The cross-app API names stages in
// lower case ("dev", "prod"; see Stage.Lower).
func (f *IronFlock) Stage() Stage { return f.stage }

// SerialNumber returns the device serial number.
func (f *IronFlock) SerialNumber() string { return f.serialNumber }

// DeviceKey returns the device key ("" when unknown).
func (f *IronFlock) DeviceKey() string { return f.deviceKey }

// DeviceName returns the device name ("" when unknown).
func (f *IronFlock) DeviceName() string { return f.deviceName }

// AppName returns the app name ("" when unknown).
func (f *IronFlock) AppName() string { return f.appName }

// SwarmKey returns the swarm key (0 when unknown).
func (f *IronFlock) SwarmKey() int { return f.swarmKey }

// AppKey returns the app key (0 when unknown).
func (f *IronFlock) AppKey() int { return f.appKey }

// Files returns the app's managed object storage. It does no I/O, so it may
// be called before Start; a file call issued before Start waits for it like
// any other operation (see IronFlock).
//
// A file call waits up to 10s for the connection and is not retried: unlike
// table operations, file calls do not use the reconnect window (see
// WithReconnectWindow), so during a platform restart a call can fail with
// filestore.CodeNotAvailable until the file service has registered again —
// as it also does whenever it binds the data backend anew. Such a call ran
// nowhere, so the app may repeat it. Stop closes the idle HTTP connections
// of the store's direct transfers.
func (f *IronFlock) Files() *filestore.FileStore {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.files == nil {
		f.files = filestore.New(startGatedCaller{f}, nil)
	}
	return f.files
}

// startGatedCaller is the filestore.Caller of Files: the connection, with
// calls issued before Start waiting for it (see awaitStart).
type startGatedCaller struct{ f *IronFlock }

func (c startGatedCaller) Call(ctx context.Context, procedure string, args []any, kwargs map[string]any, opts *wamp.CallOptions, retryWindow time.Duration) (*wamp.Result, error) {
	return c.f.call(ctx, procedure, args, kwargs, opts, retryWindow)
}

// awaitStart holds an operation issued before Start until Start has
// configured the connection. window is the time the operation waits for a
// session (0: sessionWait), and the wait for Start counts against it: when
// the operation had to wait (waited), rest is what is left of window. It
// fails with ctx's error when ctx ends, with wamp.ErrStopped when Stop is
// called, and with an error wrapping wamp.ErrNotConnected when window passes
// first.
func (f *IronFlock) awaitStart(ctx context.Context, window time.Duration) (rest time.Duration, waited bool, err error) {
	select {
	case <-f.ready:
		return window, false, nil
	default:
	}
	if f.lifetime.Err() != nil {
		return 0, false, wamp.ErrStopped
	}
	if err := ctx.Err(); err != nil {
		return 0, false, err
	}
	if window <= 0 {
		window = f.sessionWait
	}
	deadline := time.Now().Add(window)
	timer := time.NewTimer(window)
	defer timer.Stop()
	f.startWaiters.Add(1)
	defer f.startWaiters.Add(-1)
	select {
	case <-f.ready:
		// At least a moment: a window of 0 would mean the default again.
		return max(time.Until(deadline), time.Nanosecond), true, nil
	case <-f.lifetime.Done():
		return 0, true, wamp.ErrStopped
	case <-timer.C:
		return 0, true, fmt.Errorf("ironflock: the connection was not started within %v: %w", window, wamp.ErrNotConnected)
	case <-ctx.Done():
		if f.lifetime.Err() != nil {
			return 0, true, wamp.ErrStopped // ctx may be lifetime itself
		}
		return 0, true, ctx.Err()
	}
}

// gate holds an operation issued before Start (see awaitStart) and returns
// the window to pass on to the connection: window, or what is left of it
// after the wait for Start. An operation without a window (0) waits for the
// session, within what is left of the default session wait, here instead.
func (f *IronFlock) gate(ctx context.Context, window time.Duration) (time.Duration, error) {
	rest, waited, err := f.awaitStart(ctx, window)
	if err != nil || !waited {
		return window, err
	}
	if window > 0 {
		return rest, nil
	}
	return 0, f.conn.WaitSession(ctx, rest)
}

// call is f.conn.Call behind the wait for Start.
func (f *IronFlock) call(ctx context.Context, procedure string, args []any, kwargs map[string]any, opts *CallOptions, retryWindow time.Duration) (*Result, error) {
	window, err := f.gate(ctx, retryWindow)
	if err != nil {
		return nil, err
	}
	return f.conn.Call(ctx, procedure, args, kwargs, opts, window)
}

// Start configures and opens the connection, and blocks until the app's
// realm is joined or ctx is done. A realm that does not exist yet (the
// data backend is still being provisioned) is waited for.
//
// Start fails at once, with an error wrapping ErrMissingConfig, when
// SWARM_KEY or APP_KEY is unknown: the realm could never be joined. When it
// fails otherwise — ctx ends before the join, or the router URL cannot be
// resolved — no further connection attempt is made, and Start may be called
// again; operations waiting for the connection keep waiting within their
// windows.
//
// Start on an IronFlock that is started already, or whose Start is still
// running, fails with ErrAlreadyStarted; after Stop it fails with an error
// wrapping wamp.ErrStopped.
func (f *IronFlock) Start(ctx context.Context) error {
	// The realm realm-<SWARM_KEY>-<APP_KEY>-<stage> can never exist without
	// both keys, so waiting for it would only hang.
	if err := f.requireKeys(); err != nil {
		return err
	}

	f.mu.Lock()
	switch {
	case f.stopped:
		f.mu.Unlock()
		return fmt.Errorf("ironflock: Start after Stop: %w", wamp.ErrStopped)
	case f.started:
		f.mu.Unlock()
		return ErrAlreadyStarted
	}
	if !f.configured {
		// The connection is configured once, by the first Start that gets
		// this far: a wamp.Connection keeps its configuration across a
		// failed Start.
		url, err := f.routerURL()
		if err == nil {
			err = f.conn.Configure(wamp.Config{
				SwarmKey:     f.swarmKey,
				AppKey:       f.appKey,
				Stage:        f.stage,
				URL:          url,
				SerialNumber: f.serialNumber,
				AuthID:       f.authID,
				AuthSecret:   f.authSecret,
				Logger:       f.log,
			})
		}
		if err != nil {
			f.mu.Unlock()
			return err
		}
		f.configured = true
		close(f.ready) // operations waiting for Start go on to the connection
	}
	f.started = true
	f.mu.Unlock()

	if f.authID == "" {
		// The connection reads the per-app credential from the device
		// agent's mirror on every attempt (wamp.AppCredentials): say on the
		// app's logger, once, when the mirror cannot be read.
		_, idErr := env.Lookup("APP_AUTH_ID")
		_, secretErr := env.Lookup("APP_AUTH_SECRET")
		env.WarnUnreadable(f.log, cmp.Or(idErr, secretErr))
	}
	if err := f.conn.Start(ctx); err != nil {
		// The connection makes no further attempt and may be started again.
		f.mu.Lock()
		f.started = false
		f.mu.Unlock()
		return err
	}
	return nil
}

// routerURL resolves the router URL: WithURL, else DEVICE_ENDPOINT_URL, else
// the studio URL (WithReswarmURL / RESWARM_URL), else the public cloud.
func (f *IronFlock) routerURL() (string, error) {
	if f.url != "" {
		return f.url, nil
	}
	return wamp.WebSocketURI(f.reswarmURL)
}

// runContextKey marks the context Run passes to main: the context of f's main
// holds a value under runContextKey{f}. A key per instance keeps the mark of
// every enclosing Run visible when Runs nest (another IronFlock's Run inside
// main).
type runContextKey struct{ f *IronFlock }

// Stop closes every consumed-app connection and the connection itself, and
// waits for that within ctx. It is idempotent: a later Stop waits for the
// first one within its own ctx. Stop is final.
//
// Stop first ends what waits on the instance: consumed-app connections still
// opening are aborted (their ConnectToApp fails with an error wrapping
// wamp.ErrStopped), operations waiting for Start fail with wamp.ErrStopped,
// and the context Run passes to main is cancelled, so Run returns once main
// does. Stop does not wait for main, which may call Stop itself — even with
// that context, or one derived from it (in another IronFlock's Run, say):
// Stop does not let it cut the shutdown short, but stops within 10s, like the
// Stop Run performs after main. Neither the deadline that context inherits
// from Run's nor one main sets on it bounds the shutdown further. Afterwards
// operations fail with an error wrapping wamp.ErrStopped.
//
// Stop also closes the idle HTTP connections of the store Files returns (see
// filestore.FileStore.CloseIdleConnections): a direct transfer still in
// progress is not interrupted, and its connection returns to the pool when
// it ends.
func (f *IronFlock) Stop(ctx context.Context) error {
	if ctx.Value(runContextKey{f}) != nil {
		// main's context, which this Stop cancels.
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(context.WithoutCancel(ctx), f.runStopTimeout)
		defer cancel()
	}

	f.mu.Lock()
	if f.stopDone != nil {
		// Stopped or stopping: wait for the first Stop to finish.
		done := f.stopDone
		f.mu.Unlock()
		select {
		case <-done:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	done := make(chan struct{})
	f.stopDone = done
	f.stopped = true
	entries := make([]*consumedEntry, 0, len(f.consumed))
	for _, e := range f.consumed {
		entries = append(entries, e)
	}
	f.consumed = make(map[string]*consumedEntry)
	f.mu.Unlock()
	defer close(done)

	// Abort consumed-app opens still in flight (one that completes anyway
	// closes its own connection: see runOpen), wake the operations waiting
	// for Start and end Run's main context.
	f.endLifetime()
	f.closeConsumed(ctx, entries)
	err := f.conn.Stop(ctx)
	f.mu.Lock()
	files := f.files
	f.mu.Unlock()
	if files != nil {
		files.CloseIdleConnections()
	}
	return err
}

// closeConsumed closes the consumed apps of entries concurrently, waiting for
// opens still in flight within ctx. Failures are logged.
func (f *IronFlock) closeConsumed(ctx context.Context, entries []*consumedEntry) {
	var wg sync.WaitGroup
	for _, e := range entries {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// An attempt that had not finished when Stop took its snapshot
			// closes its own connection (runOpen decides under f.mu, under
			// which the snapshot was taken); a finished one is always closed
			// here, even when ctx has ended already.
			select {
			case <-e.done:
			default:
				select {
				case <-e.done:
				case <-ctx.Done():
					return
				}
			}
			if e.app == nil {
				return // the open failed: nothing to close
			}
			if err := e.app.close(ctx); err != nil {
				f.log.Warn(fmt.Sprintf("Failed to close consumed app '%s': %v", e.app.App, err))
			}
		}()
	}
	wg.Wait()
}

// Run starts the connection, runs main, and stops the connection (within
// 10s) when main returns. The context Run passes to main is cancelled when
// ctx is done, when Stop is called (from main, a handler or another
// goroutine) and when the process receives SIGINT or SIGTERM; main is
// expected to return then. With a nil main Run waits for one of these.
//
// Run returns main's error; when its Start fails, it stops the IronFlock and
// returns the Start error. Once shutdown has begun, a second SIGINT or
// SIGTERM is no longer handled: it terminates the process, even while main
// is still winding down.
//
// Run on an IronFlock that is started already, or whose Start is still
// running (another Run, say), fails with ErrAlreadyStarted and leaves it
// running.
func (f *IronFlock) Run(ctx context.Context, main func(ctx context.Context) error) error {
	runCtx, cancel := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer cancel()
	// Stop ends Run too, as the Python SDK's stop() cancels the main task.
	unhookStop := context.AfterFunc(f.lifetime, cancel)
	defer unhookStop()
	// Restore the default signal behavior as soon as shutdown begins — a
	// signal, ctx done, Stop, or main returning — so that another signal
	// terminates the process while main winds down or Run stops.
	unhookSignals := context.AfterFunc(runCtx, cancel)
	defer unhookSignals()
	runCtx = context.WithValue(runCtx, runContextKey{f}, true)

	err := f.Start(runCtx)
	if errors.Is(err, ErrAlreadyStarted) {
		return err // started by someone else: not Run's to stop
	}
	if err == nil {
		if main != nil {
			err = main(runCtx)
		} else {
			<-runCtx.Done()
		}
	}

	cancel() // main may have returned on its own
	stopCtx, stopCancel := context.WithTimeout(context.WithoutCancel(ctx), f.runStopTimeout)
	defer stopCancel()
	if serr := f.Stop(stopCtx); serr != nil {
		f.log.Warn(fmt.Sprintf("Stopping the IronFlock connection failed: %v", serr))
	}
	return err
}

// requireKeys fails unless SWARM_KEY and APP_KEY are known.
func (f *IronFlock) requireKeys() error {
	if f.swarmKey <= 0 {
		return missingConfigf("SWARM_KEY not set in environment variables!")
	}
	if f.appKey <= 0 {
		return missingConfigf("APP_KEY not set in environment variables!")
	}
	return nil
}

// withDeviceMetadata returns kwargs with the device metadata added under the
// caller's keys (caller keys win). Unknown values are sent as nil.
func (f *IronFlock) withDeviceMetadata(kwargs map[string]any) map[string]any {
	out := make(map[string]any, len(kwargs)+3)
	out["DEVICE_SERIAL_NUMBER"] = f.serialNumber
	out["DEVICE_KEY"] = nilIfEmpty(f.deviceKey)
	out["DEVICE_NAME"] = nilIfEmpty(f.deviceName)
	for k, v := range kwargs {
		out[k] = v
	}
	return out
}

func nilIfEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}

// cleanupContext returns a context for tearing down what an operation left
// behind: it keeps ctx's values but not its cancellation (ctx may be the
// reason for the teardown), bounded by the cleanup budget.
func (f *IronFlock) cleanupContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), f.cleanupTimeout)
}
