package ironflock

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ironflock/sdk-go/wamp"
)

// resolveResult is what sys.appaccess.resolve answers for "weatherstation".
func resolveResult() map[string]any {
	return map[string]any{
		"app":              "weatherstation",
		"provider_app_key": int64(77),
		"stages": map[string]any{
			"dev": map[string]any{
				"tables": []any{map[string]any{
					"tablename":   "readings",
					"description": "raw readings",
					// A provider marks a column secret: true (and only when true).
					"columns": []any{
						map[string]any{"id": "temperature", "name": "Temperature", "dataType": "number"},
						map[string]any{"id": "api_token", "name": "API token", "dataType": "string", "secret": true},
						map[string]any{"id": "pin", "dataType": "string", "secret": true},
					},
				}},
				"transforms": []any{map[string]any{"tablename": "hourly_agg", "columns": []any{}}},
			},
			"prod": map[string]any{
				"tables":     []any{map[string]any{"tablename": "readings", "columns": []any{}}},
				"transforms": []any{},
			},
		},
	}
}

// listResult is what sys.appaccess.list answers: weatherstation (dev and
// prod), vibration (dev only) and prodonly (prod only).
func listResult() []any {
	return []any{
		map[string]any{
			"app": "weatherstation", "provider_app_key": int64(77),
			"stages": map[string]any{
				"dev":  map[string]any{"tables": []any{map[string]any{"tablename": "readings"}}, "transforms": []any{}},
				"prod": map[string]any{"tables": []any{map[string]any{"tablename": "readings"}}, "transforms": []any{}},
			},
		},
		map[string]any{
			"app": "vibration", "provider_app_key": int64(88),
			"stages": map[string]any{
				"dev": map[string]any{"tables": []any{map[string]any{"tablename": "accel"}}, "transforms": []any{}},
			},
		},
		map[string]any{
			"app": "prodonly", "provider_app_key": int64(99),
			"stages": map[string]any{
				"prod": map[string]any{"tables": []any{map[string]any{"tablename": "x"}}, "transforms": []any{}},
			},
		},
	}
}

// consumer is a started IronFlock whose own realm answers resolve and list.
type consumer struct {
	*testFlock
	mu          sync.Mutex
	resolve     any     // answer of sys.appaccess.resolve
	resolveErrs []error // consumed one per resolve call
	list        any
	listErr     error
}

func newConsumer(t *testing.T, opts ...Option) *consumer {
	t.Helper()
	setIdentityEnv(t)
	return consumerOf(t, newTestFlock(t, opts...))
}

// consumerOf wires tf's own realm to answer resolve and list, and starts it.
func consumerOf(t *testing.T, tf *testFlock) *consumer {
	t.Helper()
	c := &consumer{testFlock: tf, resolve: resolveResult(), list: listResult()}
	c.own.callFn = func(call fakeCall) (*wamp.Result, error) {
		c.mu.Lock()
		defer c.mu.Unlock()
		switch call.Procedure {
		case uriAppAccessResolve:
			if len(c.resolveErrs) > 0 {
				err := c.resolveErrs[0]
				c.resolveErrs = c.resolveErrs[1:]
				return nil, err
			}
			return resultOf(c.resolve), nil
		case uriAppAccessList:
			if c.listErr != nil {
				return nil, c.listErr
			}
			return resultOf(c.list), nil
		}
		return resultOf("call-result"), nil
	}
	if err := c.Start(bg); err != nil {
		t.Fatal(err)
	}
	return c
}

func (c *consumer) setResolve(v any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.resolve = v
}

func (c *consumer) failResolve(errs ...error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.resolveErrs = append(c.resolveErrs, errs...)
}

func (c *consumer) cached() map[string]*consumedEntry {
	c.IronFlock.mu.Lock()
	defer c.IronFlock.mu.Unlock()
	out := make(map[string]*consumedEntry, len(c.consumed))
	for k, v := range c.consumed {
		out[k] = v
	}
	return out
}

// denyAuth makes consumed connections for provider app key appKey (0: all)
// fail to start with a fatal auth denial, reporting it the way
// wamp.Connection does.
func denyAuth(appKey int, reason string) func(c *fakeConn) {
	return func(c *fakeConn) {
		c.startFn = func(ctx context.Context, cfg wamp.Config) error {
			if appKey != 0 && cfg.AppKey != appKey {
				return nil
			}
			cfg.OnAuthFailure(reason)
			return &wamp.AuthError{Realm: wamp.RealmName(cfg.SwarmKey, cfg.AppKey, cfg.Stage), Reason: reason}
		}
	}
}

// assertCrossAppError fails the test unless err is a *CrossAppAccessError
// with code.
func assertCrossAppError(t *testing.T, err error, code string) {
	t.Helper()
	_ = asCrossAppError(t, err, code)
}

// asCrossAppError returns err as a *CrossAppAccessError, failing the test
// unless it is one with code.
func asCrossAppError(t *testing.T, err error, code string) *CrossAppAccessError {
	t.Helper()
	var cerr *CrossAppAccessError
	if !errors.As(err, &cerr) {
		t.Fatalf("err = %T %v, want *CrossAppAccessError", err, err)
	}
	if cerr.Code != code {
		t.Fatalf("code %s (%v), want %s", cerr.Code, err, code)
	}
	return cerr
}

func TestConnectToAppResolvesAndOpensASecondConnection(t *testing.T) {
	checkGoroutines(t)
	c := newConsumer(t)
	app, err := c.ConnectToApp(bg, "weatherstation")
	if err != nil {
		t.Fatal(err)
	}

	resolve := c.own.callsTo(uriAppAccessResolve)
	wantResolve := []fakeCall{{Procedure: uriAppAccessResolve, Args: []any{map[string]any{"app": "weatherstation"}}}}
	if !reflect.DeepEqual(resolve, wantResolve) {
		t.Errorf("resolve calls %#v", resolve)
	}

	conns := c.factory.all()
	if len(conns) != 1 {
		t.Fatalf("%d consumed connections", len(conns))
	}
	cfg := conns[0].config()
	if cfg.SwarmKey != 10 || cfg.AppKey != 77 || cfg.Stage != StageDevelopment {
		t.Errorf("realm %d %d %s", cfg.SwarmKey, cfg.AppKey, cfg.Stage)
	}
	if wamp.RealmName(cfg.SwarmKey, cfg.AppKey, cfg.Stage) != "realm-10-77-dev" {
		t.Error("wrong realm")
	}
	if cfg.URL != wamp.StudioWSURI || cfg.SerialNumber != "test-serial-123" {
		t.Errorf("URL %q serial %q", cfg.URL, cfg.SerialNumber)
	}
	if !cfg.FailOnAuthError || cfg.OnAuthFailure == nil {
		t.Error("a consumed connection must fail on auth denials")
	}
	if cfg.AuthID != "" || cfg.AuthSecret != "" {
		t.Error("without WithCredentials the credential is resolved per connection attempt")
	}

	if app.App != "weatherstation" || app.Stage != "dev" || !app.IsConnected() {
		t.Errorf("app %q %q connected %v", app.App, app.Stage, app.IsConnected())
	}
	wantTables := []TableInfo{{
		Tablename:   "readings",
		Description: "raw readings",
		Columns: []map[string]any{
			{"id": "temperature", "name": "Temperature", "dataType": "number"},
			{"id": "api_token", "name": "API token", "dataType": "string", "secret": true},
			{"id": "pin", "dataType": "string", "secret": true},
		},
	}}
	if !reflect.DeepEqual(app.Tables, wantTables) {
		t.Errorf("tables %#v", app.Tables)
	}
	if !reflect.DeepEqual(app.Transforms, []TableInfo{{Tablename: "hourly_agg", Columns: []map[string]any{}}}) {
		t.Errorf("transforms %#v", app.Transforms)
	}
	if app.Connection() != nil {
		t.Error("Connection() of a fake must be nil")
	}
}

func TestConnectToAppStage(t *testing.T) {
	c := newConsumer(t)
	for _, stage := range []string{"prod", "PROD", " Prod "} {
		app, err := c.ConnectToApp(bg, "weatherstation", ConnectToAppOptions{Stage: stage})
		if err != nil {
			t.Fatal(err)
		}
		if app.Stage != "prod" || len(app.Transforms) != 0 {
			t.Errorf("stage %q: app %q transforms %#v", stage, app.Stage, app.Transforms)
		}
	}
	if conns := c.factory.all(); len(conns) != 1 || conns[0].config().Stage != StageProduction {
		t.Errorf("one prod connection expected, got %d", len(conns))
	}

	_, err := c.ConnectToApp(bg, "weatherstation", ConnectToAppOptions{Stage: "staging"})
	if !errors.Is(err, ErrInvalidArgument) || !strings.HasSuffix(err.Error(), "stage must be 'dev' or 'prod'") {
		t.Errorf("invalid stage: %v", err)
	}
	_, err = c.ConnectToApp(bg, "")
	if !errors.Is(err, ErrInvalidArgument) || !strings.HasSuffix(err.Error(), "appName must not be empty!") {
		t.Errorf("empty name: %v", err)
	}
}

func TestConnectToAppDefaultsToTheOwnStage(t *testing.T) {
	setIdentityEnv(t)
	t.Setenv("ENV", "PROD")
	c := consumerOf(t, newTestFlock(t))
	app, err := c.ConnectToApp(bg, "weatherstation")
	if err != nil {
		t.Fatal(err)
	}
	if app.Stage != "prod" || c.factory.all()[0].config().Stage != StageProduction {
		t.Errorf("stage %q", app.Stage)
	}

	c = newConsumer(t, WithEnv("prod"))
	if app, err := c.ConnectToApp(bg, "weatherstation"); err != nil || app.Stage != "prod" {
		t.Errorf("WithEnv: %v %v", app, err)
	}
}

func TestConsumedConnectionURLAndCredentials(t *testing.T) {
	// The URL of the own connection (here from RESWARM_URL), and the
	// explicit credential.
	setIdentityEnv(t)
	t.Setenv("RESWARM_URL", "http://localhost:8086")
	c := consumerOf(t, newTestFlock(t, WithCredentials("app-id", "app-secret")))
	if _, err := c.ConnectToApp(bg, "weatherstation"); err != nil {
		t.Fatal(err)
	}
	cfg := c.factory.all()[0].config()
	if cfg.URL != wamp.LocalhostWSURI {
		t.Errorf("URL %q", cfg.URL)
	}
	if cfg.AuthID != "app-id" || cfg.AuthSecret != "app-secret" {
		t.Errorf("credential %q %q", cfg.AuthID, cfg.AuthSecret)
	}

	c2 := &consumer{testFlock: flock(t, WithURL("ws://router/ws"))}
	c2.own.callFn = func(fakeCall) (*wamp.Result, error) { return resultOf(resolveResult()), nil }
	if _, err := c2.ConnectToApp(bg, "weatherstation"); err != nil {
		t.Fatal(err)
	}
	if got := c2.factory.all()[0].config().URL; got != "ws://router/ws" {
		t.Errorf("WithURL: %q", got)
	}
}

func TestConnectToAppMapsResolveRefusals(t *testing.T) {
	cases := map[string]string{
		"sys.appaccess.error.no_grant":               CodeNoGrant,
		"sys.appaccess.error.provider_not_installed": CodeProviderNotInstalled,
		"sys.appaccess.error.unknown_app":            CodeUnknownApp,
		"wamp.error.not_authorized":                  CodeNotAuthorized,
		"wamp.error.authorization_failed":            CodeNotAuthorized,
		"wamp.error.authentication_failed":           CodeNotAuthorized,
	}
	for uri, code := range cases {
		c := newConsumer(t)
		c.failResolve(wampErr(uri, "denied"))
		_, err := c.ConnectToApp(bg, "weatherstation")
		cerr := asCrossAppError(t, err, code)
		if cerr.Message != uri+`: "denied"` {
			t.Errorf("%s: message %q", uri, cerr.Message)
		}
		if WampURI(err) != uri {
			t.Errorf("%s: the WAMP error must stay reachable", uri)
		}
		if len(c.factory.all()) != 0 {
			t.Errorf("%s: a connection was opened", uri)
		}
	}
}

func TestConnectToAppKeepsUnmappedRefusalsReadable(t *testing.T) {
	c := newConsumer(t)
	c.failResolve(wampErr("wamp.error.runtime_error", "boom"))
	_, err := c.ConnectToApp(bg, "weatherstation")
	var werr *WampError
	if !errors.As(err, &werr) || werr.URI != "wamp.error.runtime_error" {
		t.Fatalf("err = %v", err)
	}
	var cerr *CrossAppAccessError
	if errors.As(err, &cerr) {
		t.Error("an unmapped refusal is not a CrossAppAccessError")
	}
	mustContain(t, err.Error(), "sys.appaccess.resolve", "wamp.error.runtime_error", "boom")
}

func TestConnectToAppWithoutABackendForTheStage(t *testing.T) {
	stageless := resolveResult()
	stageless["stages"] = map[string]any{"prod": stageless["stages"].(map[string]any)["prod"]}
	emptyCatalog := resolveResult()
	emptyCatalog["stages"].(map[string]any)["dev"] = map[string]any{}
	noKey := resolveResult()
	delete(noKey, "provider_app_key")

	cases := []struct {
		name    string
		resolve any
		message string
	}{
		// Without an answer the message names the app as requested...
		{"no answer", nil, "App 'WeatherStation' has no dev data backend in this project"},
		{"empty answer", map[string]any{}, "App 'WeatherStation' has no dev data backend in this project"},
		{"not an object", "nope", "App 'WeatherStation' has no dev data backend in this project"},
		// ...with one, as the platform names it.
		{"stage absent", stageless, "App 'weatherstation' has no dev data backend in this project"},
		{"empty catalog", emptyCatalog, "App 'weatherstation' has no dev data backend in this project"},
		{"no provider key", noKey, "(the platform sent no provider_app_key)"},
	}
	for _, tc := range cases {
		c := newConsumer(t)
		c.setResolve(tc.resolve)
		_, err := c.ConnectToApp(bg, "WeatherStation")
		cerr := asCrossAppError(t, err, CodeProviderNotInstalled)
		if !strings.Contains(cerr.Message, tc.message) {
			t.Errorf("%s: message %q", tc.name, cerr.Message)
		}
		if len(c.factory.all()) != 0 {
			t.Errorf("%s: a connection was opened", tc.name)
		}
		if len(c.cached()) != 0 {
			t.Errorf("%s: the failed attempt stayed cached", tc.name)
		}
	}
}

func TestConnectToAppCachesPerAppAndStage(t *testing.T) {
	c := newConsumer(t)
	first, err := c.ConnectToApp(bg, "weatherstation")
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"weatherstation", "WeatherStation"} {
		again, err := c.ConnectToApp(bg, name)
		if err != nil || again != first {
			t.Errorf("%q: %v (same handle: %v)", name, err, again == first)
		}
	}
	if n := len(c.own.callsTo(uriAppAccessResolve)); n != 1 {
		t.Errorf("%d resolve calls", n)
	}
	prod, err := c.ConnectToApp(bg, "weatherstation", ConnectToAppOptions{Stage: "prod"})
	if err != nil || prod == first {
		t.Errorf("prod: %v (distinct: %v)", err, prod != first)
	}
	if n := len(c.factory.all()); n != 2 {
		t.Errorf("%d connections", n)
	}
}

func TestConnectToAppSharesConcurrentAttempts(t *testing.T) {
	checkGoroutines(t)
	c := newConsumer(t)
	release := make(chan struct{})
	c.factory.setSetup(func(fc *fakeConn) {
		fc.startFn = func(ctx context.Context, _ wamp.Config) error {
			<-release
			return nil
		}
	})

	// A caller that gives up does not fail the attempt for the others.
	impatient, cancel := context.WithCancel(bg)
	impatientErr := make(chan error, 1)
	go func() {
		_, err := c.ConnectToApp(impatient, "weatherstation")
		impatientErr <- err
	}()
	eventually(t, "the consumed connection", func() bool { return len(c.factory.all()) == 1 })
	cancel()
	if err := <-impatientErr; !errors.Is(err, context.Canceled) {
		t.Errorf("impatient caller: %v", err)
	}

	var wg sync.WaitGroup
	apps := make([]*ConsumedApp, 5)
	for i := range apps {
		wg.Add(1)
		go func() {
			defer wg.Done()
			app, err := c.ConnectToApp(bg, "weatherstation")
			if err != nil {
				t.Error(err)
			}
			apps[i] = app
		}()
	}
	time.Sleep(10 * time.Millisecond) // let the callers queue up on the attempt
	close(release)
	wg.Wait()
	for i, app := range apps {
		if app == nil || app != apps[0] {
			t.Errorf("caller %d got a different handle", i)
		}
	}
	if n := len(c.own.callsTo(uriAppAccessResolve)); n != 1 {
		t.Errorf("%d resolve calls", n)
	}
	if n := len(c.factory.all()); n != 1 {
		t.Errorf("%d connections", n)
	}
}

func TestFailedAttemptsAreEvictedBeforeWaitersWake(t *testing.T) {
	c := newConsumer(t)
	c.failResolve(wampErr("sys.appaccess.error.no_grant"))
	_, err := c.ConnectToApp(bg, "weatherstation")
	assertCrossAppError(t, err, CodeNoGrant)
	if len(c.cached()) != 0 {
		t.Fatal("the failed attempt is still cached when its caller returns")
	}
	app, err := c.ConnectToApp(bg, "weatherstation")
	if err != nil || app.App != "weatherstation" {
		t.Fatalf("retry: %v", err)
	}
}

func TestConnectionErrorsTearDownTheConsumedConnection(t *testing.T) {
	c := newConsumer(t)
	boom := errors.New("connection refused")
	c.factory.setSetup(func(fc *fakeConn) {
		fc.startFn = func(context.Context, wamp.Config) error { return boom }
	})
	_, err := c.ConnectToApp(bg, "weatherstation")
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v", err)
	}
	mustContain(t, err.Error(), "Connection to app 'weatherstation' (dev) failed: connection refused")
	if conn := c.factory.all()[0]; conn.stopCount() != 1 {
		t.Error("a connection whose open failed must be stopped")
	}
	if len(c.cached()) != 0 {
		t.Error("the failed attempt stayed cached")
	}
}

func TestCloseEvictsSoTheNextCallReconnects(t *testing.T) {
	c := newConsumer(t)
	first, err := c.ConnectToApp(bg, "weatherstation")
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Close(bg); err != nil {
		t.Fatal(err)
	}
	if first.IsConnected() || c.factory.all()[0].stopCount() != 1 {
		t.Error("Close must stop the connection")
	}
	if err := first.Close(bg); err != nil {
		t.Errorf("second Close: %v", err)
	}
	second, err := c.ConnectToApp(bg, "weatherstation")
	if err != nil || second == first {
		t.Fatalf("reconnect: %v (new handle: %v)", err, second != first)
	}
	if n := len(c.factory.all()); n != 2 {
		t.Errorf("%d connections", n)
	}

	// Closing the old handle again must not evict the new one.
	_ = first.Close(bg)
	if again, _ := c.ConnectToApp(bg, "weatherstation"); again != second {
		t.Error("a stale Close evicted a newer handle")
	}

	boom := errors.New("stop failed")
	c.factory.all()[1].mu.Lock()
	c.factory.all()[1].stopErr = boom
	c.factory.all()[1].mu.Unlock()
	if err := second.Close(bg); !errors.Is(err, boom) {
		t.Errorf("Close error %v", err)
	}
}

func TestStopClosesEveryConsumedAppAndTheOwnConnection(t *testing.T) {
	checkGoroutines(t)
	c := newConsumer(t)
	if _, err := c.ConnectToApp(bg, "weatherstation"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.ConnectToApp(bg, "weatherstation", ConnectToAppOptions{Stage: "prod"}); err != nil {
		t.Fatal(err)
	}
	c.factory.all()[1].mu.Lock()
	c.factory.all()[1].stopErr = errors.New("router gone")
	c.factory.all()[1].mu.Unlock()

	if err := c.Stop(bg); err != nil {
		t.Fatal(err)
	}
	for i, conn := range c.factory.all() {
		if conn.stopCount() != 1 {
			t.Errorf("consumed connection %d stopped %d times", i, conn.stopCount())
		}
	}
	if c.own.stopCount() != 1 {
		t.Error("own connection not stopped")
	}
	if len(c.cached()) != 0 {
		t.Error("cache not cleared")
	}
	mustContain(t, c.logs.String(), "Failed to close consumed app 'weatherstation'", "router gone")
}

// An attempt Stop aborts fails with an error wrapping wamp.ErrStopped, not
// with the context.Canceled of the cancellation behind it: the caller's
// context was never cancelled.
func TestStopAbortsAttemptsInFlight(t *testing.T) {
	checkGoroutines(t)
	for name, abort := range map[string]func(c *consumer){
		"while joining": func(c *consumer) {
			c.factory.setSetup(func(fc *fakeConn) {
				fc.startFn = func(ctx context.Context, cfg wamp.Config) error {
					<-ctx.Done() // a realm that never appears
					return fmt.Errorf("wamp: no session on realm %s: %w", cfg.Realm, ctx.Err())
				}
			})
		},
		"while resolving": func(c *consumer) {
			c.own.mu.Lock()
			answer := c.own.callFn
			c.own.callFn = func(call fakeCall) (*wamp.Result, error) {
				if call.Procedure == uriAppAccessResolve {
					<-c.lifetime.Done() // the call's context, cancelled by Stop
					return nil, context.Canceled
				}
				return answer(call)
			}
			c.own.mu.Unlock()
		},
	} {
		t.Run(name, func(t *testing.T) {
			c := newConsumer(t)
			abort(c)
			errc := make(chan error, 1)
			go func() {
				_, err := c.ConnectToApp(bg, "weatherstation")
				errc <- err
			}()
			eventually(t, "the attempt", func() bool {
				return len(c.factory.all()) == 1 || len(c.own.callsTo(uriAppAccessResolve)) == 1
			})
			if err := c.Stop(bg); err != nil {
				t.Fatal(err)
			}
			err := recv(t, errc, "ConnectToApp")
			if !errors.Is(err, wamp.ErrStopped) || errors.Is(err, context.Canceled) {
				t.Fatalf("err = %v, want ErrStopped and not context.Canceled", err)
			}
			mustContain(t, err.Error(), "connection to app 'weatherstation' (dev) aborted by Stop")
			for _, conn := range c.factory.all() {
				if conn.stopCount() == 0 {
					t.Error("the aborted connection was not stopped")
				}
			}
			if len(c.cached()) != 0 {
				t.Error("the aborted attempt is cached")
			}
		})
	}
}

func TestStopAbortsConnectToAllApps(t *testing.T) {
	checkGoroutines(t)
	c := newConsumer(t)
	c.factory.setSetup(func(fc *fakeConn) {
		fc.startFn = func(ctx context.Context, cfg wamp.Config) error {
			<-ctx.Done()
			return fmt.Errorf("wamp: no session on realm %s: %w", cfg.Realm, ctx.Err())
		}
	})
	errc := make(chan error, 1)
	go func() {
		_, err := c.ConnectToAllApps(bg, ConnectToAllAppsOptions{StopOnError: true})
		errc <- err
	}()
	eventually(t, "the attempts", func() bool { return len(c.factory.all()) == 2 })
	if err := c.Stop(bg); err != nil {
		t.Fatal(err)
	}
	if err := recv(t, errc, "ConnectToAllApps"); !errors.Is(err, wamp.ErrStopped) || errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want ErrStopped and not context.Canceled", err)
	}
}

// A denial that races Stop is still reported as the denial.
func TestDenialRacingStopKeepsItsCode(t *testing.T) {
	checkGoroutines(t)
	c := newConsumer(t)
	c.factory.setSetup(func(fc *fakeConn) {
		fc.startFn = func(ctx context.Context, cfg wamp.Config) error {
			<-ctx.Done() // Stop has begun
			cfg.OnAuthFailure("wamp.error.not_authorized")
			return &wamp.AuthError{Realm: cfg.Realm, Reason: "wamp.error.not_authorized"}
		}
	})
	errc := make(chan error, 1)
	go func() {
		_, err := c.ConnectToApp(bg, "weatherstation")
		errc <- err
	}()
	eventually(t, "the consumed connection", func() bool { return len(c.factory.all()) == 1 })
	if err := c.Stop(bg); err != nil {
		t.Fatal(err)
	}
	cerr := asCrossAppError(t, recv(t, errc, "ConnectToApp"), CodeNotAuthorized)
	if errors.Is(cerr, wamp.ErrStopped) {
		t.Errorf("the denial was rewritten: %v", cerr)
	}
}

// Stop with an expired context still closes a consumed connection whose
// open completes at the same time. A completing open decides under the lock
// Stop takes its snapshot of the cache under: either it finds Stop begun and
// closes the connection itself, or it publishes its handle before the
// snapshot, which Stop then closes even with an expired context. The seam
// lets a Stop arrive between the open's check and its publication: an open
// that checked, released the lock and then published would hand out a
// connection that nobody closes.
func TestStopWithAnExpiredContextClosesAnOpenPublishingMeanwhile(t *testing.T) {
	checkGoroutines(t)
	c := newConsumer(t)
	expired, cancel := context.WithCancel(bg)
	cancel()
	stopped := make(chan struct{})
	var once sync.Once
	c.beforeOpenPublished = func() {
		once.Do(func() {
			if c.IronFlock.mu.TryLock() {
				c.IronFlock.mu.Unlock()
				t.Error("the open publishes its outcome without the lock Stop takes its snapshot under")
			}
			// Stop must not be able to finish before the publication. It
			// cannot call Stop itself: the open holds the lock Stop needs.
			go func() {
				_ = c.Stop(expired)
				close(stopped)
			}()
			select {
			case <-stopped:
			case <-time.After(50 * time.Millisecond):
			}
		})
	}
	app, err := c.ConnectToApp(bg, "weatherstation")
	if app == nil && !errors.Is(err, wamp.ErrStopped) {
		t.Fatalf("ConnectToApp = %v, %v", app, err)
	}
	recv(t, stopped, "Stop")
	conn := c.factory.all()[0]
	if conn.stopCount() != 1 || conn.IsOpen() {
		t.Errorf("the consumed connection outlived Stop: stopped %d times, open %v", conn.stopCount(), conn.IsOpen())
	}
	_ = c.Stop(bg)
	if conn.stopCount() != 1 {
		t.Errorf("stopped %d times", conn.stopCount())
	}
}

// A successful open holds the lock Stop takes its snapshot of the cache under
// until its outcome is published: released any earlier, a Stop in between
// would miss the connection, which then outlives it (the race the test above
// sets up). Nothing else contends for the lock here, so a release before the
// publication is caught every time.
func TestOpenPublishesItsOutcomeUnderTheLock(t *testing.T) {
	checkGoroutines(t)
	c := newConsumer(t)
	var checked atomic.Bool
	c.afterOpenPublished = func() {
		checked.Store(true)
		if c.IronFlock.mu.TryLock() {
			c.IronFlock.mu.Unlock()
			t.Error("the open released the lock before it published its outcome")
		}
	}
	if _, err := c.ConnectToApp(bg, "weatherstation"); err != nil {
		t.Fatal(err)
	}
	if !checked.Load() {
		t.Fatal("the open did not reach afterOpenPublished")
	}
}

// Close drops the handle from the cache before it stops the connection: a
// ConnectToApp that comes in while the connection is still saying goodbye
// opens a fresh one instead of getting the handle being closed.
func TestCloseEvictsBeforeStopping(t *testing.T) {
	checkGoroutines(t)
	c := newConsumer(t)
	first, err := c.ConnectToApp(bg, "weatherstation")
	if err != nil {
		t.Fatal(err)
	}
	conn := c.factory.all()[0]
	entered, goodbye := make(chan struct{}), make(chan struct{})
	release := sync.OnceFunc(func() { close(goodbye) })
	t.Cleanup(release) // before the Stop of the cleanup, should the test fail
	enter := sync.OnceFunc(func() { close(entered) })
	conn.mu.Lock()
	conn.stopFn = func(context.Context) error {
		enter()
		<-goodbye // the GOODBYE round trip
		return nil
	}
	conn.mu.Unlock()
	closed := make(chan error, 1)
	go func() { closed <- first.Close(bg) }()
	recv(t, entered, "Close to stop the connection")

	second, err := c.ConnectToApp(bg, "weatherstation")
	if err != nil {
		t.Fatal(err)
	}
	if second == first || !second.IsConnected() || len(c.factory.all()) != 2 {
		t.Fatalf("ConnectToApp during Close: same handle %v, connected %v, %d connections",
			second == first, second.IsConnected(), len(c.factory.all()))
	}
	release()
	if err := recv(t, closed, "Close"); err != nil {
		t.Fatal(err)
	}
	if again, err := c.ConnectToApp(bg, "weatherstation"); err != nil || again != second {
		t.Errorf("the new handle was not kept: %v", err)
	}
}

func TestAnAttemptCompletingAfterStopIsClosed(t *testing.T) {
	checkGoroutines(t)
	c := newConsumer(t)
	release := make(chan struct{})
	c.factory.setSetup(func(fc *fakeConn) {
		fc.startFn = func(context.Context, wamp.Config) error {
			<-release // joins regardless of cancellation
			return nil
		}
	})
	errc := make(chan error, 1)
	go func() {
		_, err := c.ConnectToApp(bg, "weatherstation")
		errc <- err
	}()
	eventually(t, "the consumed connection", func() bool { return len(c.factory.all()) == 1 })

	ctx, cancel := context.WithTimeout(bg, 20*time.Millisecond)
	defer cancel()
	_ = c.Stop(ctx) // gives up waiting for the attempt
	close(release)

	if err := <-errc; !errors.Is(err, wamp.ErrStopped) {
		t.Errorf("err = %v, want ErrStopped", err)
	}
	if c.factory.all()[0].stopCount() != 1 {
		t.Error("the connection opened after Stop was not closed")
	}
}

func TestFatalAuthDenialWhileOpening(t *testing.T) {
	checkGoroutines(t)
	for name, setup := range map[string]func(c *fakeConn){
		"callback and AuthError": denyAuth(0, "wamp.error.not_authorized"),
		"AuthError only": func(fc *fakeConn) {
			fc.startFn = func(_ context.Context, cfg wamp.Config) error {
				return &wamp.AuthError{Realm: "realm-10-77-dev", Reason: "wamp.error.authorization_failed"}
			}
		},
		"callback and another error": func(fc *fakeConn) {
			fc.startFn = func(_ context.Context, cfg wamp.Config) error {
				cfg.OnAuthFailure("wamp.error.not_authorized")
				return wamp.ErrStopped
			}
		},
	} {
		t.Run(name, func(t *testing.T) {
			c := newConsumer(t)
			injectPerAppCredential(t)
			c.factory.setSetup(setup)
			var onError atomic.Int32
			_, err := c.ConnectToApp(bg, "weatherstation", ConnectToAppOptions{
				OnError: func(*CrossAppAccessError) { onError.Add(1) },
			})
			cerr := asCrossAppError(t, err, CodeNotAuthorized)
			if !strings.HasPrefix(cerr.Message, "Access to app 'weatherstation' (dev) denied: wamp.error.") ||
				!strings.HasSuffix(cerr.Message, ". The grant may have been revoked.") {
				t.Errorf("message %q", cerr.Message)
			}
			if onError.Load() != 0 {
				t.Error("OnError must not be called while ConnectToApp is pending")
			}
			if len(c.cached()) != 0 {
				t.Error("the denied attempt stayed cached")
			}
			if c.factory.all()[0].stopCount() != 1 {
				t.Error("the denied connection was not stopped")
			}
		})
	}
}

func TestGrantRevokedAfterConnecting(t *testing.T) {
	checkGoroutines(t)
	c := newConsumer(t)
	injectPerAppCredential(t)
	errs := make(chan *CrossAppAccessError, 2)
	first, err := c.ConnectToApp(bg, "weatherstation", ConnectToAppOptions{
		OnError: func(err *CrossAppAccessError) { errs <- err },
	})
	if err != nil {
		t.Fatal(err)
	}
	// A later ConnectToApp with its own OnError reuses the handle; its
	// callback is not registered.
	if _, err := c.ConnectToApp(bg, "weatherstation", ConnectToAppOptions{
		OnError: func(*CrossAppAccessError) { t.Error("OnError of a cache hit was called") },
	}); err != nil {
		t.Fatal(err)
	}

	conn := c.factory.all()[0]
	go conn.config().OnAuthFailure("wamp.error.not_authorized") // as wamp.Connection does, on its own goroutine

	select {
	case got := <-errs:
		if got.Code != CodeNotAuthorized || !strings.Contains(got.Message, "revoked") {
			t.Errorf("OnError got %v", got)
		}
		var aerr *wamp.AuthError
		if !errors.As(got, &aerr) || aerr.Reason != "wamp.error.not_authorized" || aerr.Realm != "realm-10-77-dev" {
			t.Errorf("cause %#v", got.Err)
		}
	case <-time.After(time.Second):
		t.Fatal("OnError was not called")
	}
	eventually(t, "the revoked connection to be stopped", func() bool { return conn.stopCount() == 1 })
	if len(c.cached()) != 0 {
		t.Error("the revoked handle stayed cached")
	}
	reconnected, err := c.ConnectToApp(bg, "weatherstation")
	if err != nil || reconnected == first {
		t.Errorf("reconnect: %v (new handle: %v)", err, reconnected != first)
	}
	select {
	case got := <-errs:
		t.Errorf("OnError called again: %v", got)
	default:
	}
}

// injectPerAppCredential writes the per-app credential the device agent
// injects into the test's /data/env (see setIdentityEnv).
func injectPerAppCredential(t *testing.T) {
	t.Helper()
	writeEnvFiles(t, map[string]string{"APP_AUTH_ID": "app-20-dev-e1@test-serial-123", "APP_AUTH_SECRET": "per-app-secret"})
}

// Cross-app access needs the per-app credential the device agent injects.
// Without one (an older agent, say) the app presents its legacy device
// credential, which the platform refuses on another app's realm: the denial
// says so instead of suggesting a revoked grant, which exists.
func TestDenialWithTheLegacyCredentialNamesThePerAppCredential(t *testing.T) {
	for _, tc := range []struct {
		name   string
		opts   []Option
		files  bool
		legacy bool
	}{
		{"legacy credential", nil, false, true},
		{"injected per-app credential", nil, true, false},
		{"explicit credential", []Option{WithCredentials("app-20-dev-e1@test-serial-123", "secret")}, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			setIdentityEnv(t)
			if tc.files {
				injectPerAppCredential(t)
			}
			c := consumerOf(t, newTestFlock(t, tc.opts...))
			c.factory.setSetup(denyAuth(0, wamp.URIAuthenticationFailed))
			_, err := c.ConnectToApp(bg, "weatherstation")
			cerr := asCrossAppError(t, err, CodeNotAuthorized)
			const prefix = "Access to app 'weatherstation' (dev) denied: wamp.error.authentication_failed. "
			if !strings.HasPrefix(cerr.Message, prefix) {
				t.Fatalf("message %q", cerr.Message)
			}
			why := strings.TrimPrefix(cerr.Message, prefix)
			if tc.legacy {
				for _, part := range []string{"per-app credential the device agent injects", "APP_AUTH_ID", "legacy"} {
					if !strings.Contains(why, part) {
						t.Errorf("%q does not mention %q", why, part)
					}
				}
				if strings.Contains(why, "revoked") {
					t.Errorf("%q blames the grant", why)
				}
			} else if why != "The grant may have been revoked." {
				t.Errorf("message %q", cerr.Message)
			}
		})
	}
}

func TestOnErrorPanicsAreRecovered(t *testing.T) {
	c := newConsumer(t)
	if _, err := c.ConnectToApp(bg, "weatherstation", ConnectToAppOptions{
		OnError: func(*CrossAppAccessError) { panic("callback bug") },
	}); err != nil {
		t.Fatal(err)
	}
	c.factory.all()[0].config().OnAuthFailure("wamp.error.not_authorized")
	mustContain(t, c.logs.String(), "ConnectToApp OnError callback panicked: callback bug")
}

func TestConsumedAppGetHistory(t *testing.T) {
	c := newConsumer(t, WithReconnectWindow(5*time.Second))
	app, err := c.ConnectToApp(bg, "weatherstation")
	if err != nil {
		t.Fatal(err)
	}
	conn := c.factory.all()[0]
	answer(conn, []any{map[string]any{"temperature": 21.5}})

	rows, err := app.GetHistory(bg, "readings", &TableQueryParams{Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(rows, []Row{{"temperature": 21.5}}) {
		t.Errorf("rows %#v", rows)
	}
	want := fakeCall{
		Procedure: "history.transformed.readings",
		Args:      []any{map[string]any{"limit": int64(100), "offset": int64(0)}},
		Window:    5 * time.Second, // the consumer's reconnect window
	}
	if got := readCall(t, conn.lastCall(t)); !reflect.DeepEqual(got, want) {
		t.Errorf("call %#v\nwant %#v", got, want)
	}

	if _, err := app.GetHistory(bg, "hourly_agg", nil); err != nil {
		t.Fatal(err)
	}
	got := conn.lastCall(t)
	if got.Procedure != "history.transformed.hourly_agg" ||
		!reflect.DeepEqual(got.Args, []any{map[string]any{"limit": int64(10), "offset": int64(0)}}) {
		t.Errorf("transform call %#v", got)
	}

	if _, err := app.GetHistory(bg, "readings", &TableQueryParams{
		Limit: 100, FilterAnd: []Filter{Latest(), Where("device_key", "=", 42)}, Columns: []string{"temperature"},
	}); err != nil {
		t.Fatal(err)
	}
	wantPayload := map[string]any{
		"limit": int64(100), "offset": int64(0), "columns": []any{"temperature"},
		"filterAnd": []any{
			map[string]any{"latest": true},
			map[string]any{"column": "device_key", "operator": "=", "value": int64(42)},
		},
	}
	if got := conn.lastCall(t).Args[0]; !reflect.DeepEqual(got, wantPayload) {
		t.Errorf("payload %#v", got)
	}

	calls := len(conn.allCalls())
	_, err = app.GetHistory(bg, "readings", &TableQueryParams{Limit: 0})
	if !errors.Is(err, ErrInvalidArgument) || !strings.Contains(err.Error(), "Invalid query parameters") {
		t.Errorf("limit 0: %v", err)
	}
	_, err = app.GetHistory(bg, "", nil)
	if !errors.Is(err, ErrInvalidArgument) || !strings.HasSuffix(err.Error(), "Tablename must not be empty!") {
		t.Errorf("empty table: %v", err)
	}
	if len(conn.allCalls()) != calls {
		t.Error("an invalid query reached the provider")
	}
}

func TestConsumedAppCatalogGuard(t *testing.T) {
	c := newConsumer(t)
	app, err := c.ConnectToApp(bg, "weatherstation")
	if err != nil {
		t.Fatal(err)
	}
	_, err = app.GetHistory(bg, "secret", nil)
	cerr := asCrossAppError(t, err, CodePrivateTable)
	if want := "'secret' is not shared by app 'weatherstation' (dev). Available tables/transforms: readings, hourly_agg"; cerr.Message != want {
		t.Errorf("message %q\nwant %q", cerr.Message, want)
	}
	_, err = app.SubscribeToTable(bg, "secret", func(*Event) {})
	assertCrossAppError(t, err, CodePrivateTable)
	_, err = app.GetSeriesHistory(bg, "secret", SeriesQueryParams{Metrics: []SeriesMetric{{"temp", MethodAvg}}, Limit: 10,
		TimeRange: &TimeRange{Start: int64(0)}})
	cerr = asCrossAppError(t, err, CodePrivateTable)
	if want := "'secret' is not a shared table of app 'weatherstation' (dev). Available tables: readings"; cerr.Message != want {
		t.Errorf("series message %q", cerr.Message)
	}
	// Matching is exact.
	_, err = app.GetHistory(bg, "Readings", nil)
	assertCrossAppError(t, err, CodePrivateTable)

	conn := c.factory.all()[0]
	if len(conn.allCalls())+len(conn.allSubs()) != 0 {
		t.Error("a private table reached the provider")
	}

	empty := &ConsumedApp{App: "a", Stage: "dev", conn: conn}
	_, err = empty.GetHistory(bg, "x", nil)
	cerr = asCrossAppError(t, err, CodePrivateTable)
	if !strings.HasSuffix(cerr.Message, "Available tables/transforms: none") {
		t.Errorf("message %q", cerr.Message)
	}
}

func TestConsumedAppSecretColumnGuard(t *testing.T) {
	c := newConsumer(t)
	app, err := c.ConnectToApp(bg, "weatherstation")
	if err != nil {
		t.Fatal(err)
	}
	conn := c.factory.all()[0]

	filters := map[string][]Filter{
		"predicate":    {Where("api_token", "=", "hunter2")},
		"in a group":   {Or(Where("temperature", ">", 20), And(Where("api_token", "=", "x")))},
		"IS NULL":      {IsNotNull("api_token")},
		"after others": {Latest(), Where("temperature", ">", 1), Where("api_token", "=", "x")},
	}
	for name, fs := range filters {
		_, err := app.GetHistory(bg, "readings", &TableQueryParams{Limit: 10, FilterAnd: fs})
		cerr := asCrossAppError(t, err, CodeSecretColumn)
		want := "Cannot filter on secret column(s) api_token of 'readings' (app 'weatherstation'): the stored values are " +
			"encrypted with a random IV, so no comparison can ever match. Filter by the columns that identify the row instead."
		if cerr.Message != want {
			t.Errorf("%s: message %q", name, cerr.Message)
		}
	}
	_, err = app.GetHistory(bg, "readings", &TableQueryParams{Limit: 10, FilterAnd: []Filter{
		Where("pin", "=", "1"), Where("api_token", "=", "x"), Where("pin", "=", "2"),
	}})
	mustContain(t, asCrossAppError(t, err, CodeSecretColumn).Message, "secret column(s) api_token, pin of")

	_, err = app.GetHistory(bg, "readings", &TableQueryParams{Limit: 10, Columns: []string{"temperature", "pin", "api_token", "pin"}})
	cerr := asCrossAppError(t, err, CodeSecretColumn)
	want := "Cannot read secret column(s) api_token, pin of 'readings' (app 'weatherstation'): secret values are readable " +
		"only by the owning app's own containers, so a cross-app read returns the '__secret__' placeholder. Drop them from 'columns'."
	if cerr.Message != want {
		t.Errorf("message %q\nwant %q", cerr.Message, want)
	}
	if len(conn.allCalls()) != 0 {
		t.Fatal("a secret-column query reached the provider")
	}

	// The rest of the table stays readable.
	if _, err := app.GetHistory(bg, "readings", &TableQueryParams{
		Limit: 10, Columns: []string{"temperature"}, FilterAnd: []Filter{Where("temperature", ">", 20)},
	}); err != nil {
		t.Fatal(err)
	}
	if got := conn.lastCall(t).Args[0].(map[string]any)["columns"]; !reflect.DeepEqual(got, []any{"temperature"}) {
		t.Errorf("columns %#v", got)
	}
	// So do tables without secret columns.
	if _, err := app.GetHistory(bg, "hourly_agg", &TableQueryParams{Limit: 1, Columns: []string{"api_token"}}); err != nil {
		t.Errorf("transform without secrets: %v", err)
	}
}

func TestConsumedAppGetSeriesHistory(t *testing.T) {
	c := newConsumer(t)
	app, err := c.ConnectToApp(bg, "weatherstation")
	if err != nil {
		t.Fatal(err)
	}
	conn := c.factory.all()[0]
	q := SeriesQueryParams{
		Metrics: []SeriesMetric{{"temperature", MethodAvg}}, Limit: 100,
		TimeRange: &TimeRange{Start: "2026-01-01T00:00:00.000Z"},
		FilterAnd: []Filter{Or(Where("temperature", ">", 30), IsNull("temperature"))},
	}
	if _, err := app.GetSeriesHistory(bg, "readings", q); err != nil {
		t.Fatal(err)
	}
	want := fakeCall{
		Procedure: "history.transformed.series.readings",
		Args: []any{map[string]any{
			"metrics": []any{map[string]any{"ref": "temperature", "method": "AVG"}}, "limit": int64(100),
			"timeRange": []any{"2026-01-01T00:00:00.000Z", nil},
			"filterAnd": []any{map[string]any{"combinator": "OR", "filters": []any{
				map[string]any{"column": "temperature", "operator": ">", "value": int64(30)},
				map[string]any{"column": "temperature", "operator": "IS NULL"},
			}}},
		}},
		Window: DefaultReconnectWindow,
	}
	if got := readCall(t, conn.lastCall(t)); !reflect.DeepEqual(got, want) {
		t.Errorf("call %#v\nwant %#v", got, want)
	}

	_, err = app.GetSeriesHistory(bg, "hourly_agg", q)
	cerr := asCrossAppError(t, err, CodePrivateTable)
	if want := "'hourly_agg' is a transform of app 'weatherstation' (dev); series history is available for tables only — use GetHistory instead."; cerr.Message != want {
		t.Errorf("message %q", cerr.Message)
	}
	bad := q
	bad.Metrics = []SeriesMetric{{"temperature", "MEAN"}}
	if _, err := app.GetSeriesHistory(bg, "readings", bad); !errors.Is(err, ErrInvalidArgument) ||
		!strings.Contains(err.Error(), "Invalid series query parameters") {
		t.Errorf("invalid method: %v", err)
	}
	bad = q
	bad.FilterAnd = []Filter{Latest()}
	if _, err := app.GetSeriesHistory(bg, "readings", bad); !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("latest marker: %v", err)
	}
	if _, err := app.GetSeriesHistory(bg, "", q); !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("empty table: %v", err)
	}
	if n := len(conn.allCalls()); n != 1 {
		t.Errorf("%d calls reached the provider", n)
	}
}

func TestConsumedAppMapsProviderRefusals(t *testing.T) {
	c := newConsumer(t)
	app, err := c.ConnectToApp(bg, "weatherstation")
	if err != nil {
		t.Fatal(err)
	}
	conn := c.factory.all()[0]
	series := SeriesQueryParams{Metrics: []SeriesMetric{{"temp", MethodAvg}}, Limit: 10, TimeRange: &TimeRange{Start: int64(0)}}

	fail(conn, wampErr("wamp.error.not_authorized"))
	_, err = app.GetHistory(bg, "readings", nil)
	assertCrossAppError(t, err, CodeNotAuthorized)
	_, err = app.GetSeriesHistory(bg, "readings", series)
	assertCrossAppError(t, err, CodeNotAuthorized)

	fail(conn, wampErr("wamp.error.runtime_error", "boom"))
	_, err = app.GetHistory(bg, "readings", nil)
	if WampURI(err) != "wamp.error.runtime_error" {
		t.Fatalf("err = %v", err)
	}
	var cerr *CrossAppAccessError
	if errors.As(err, &cerr) {
		t.Error("an unmapped refusal is not a CrossAppAccessError")
	}
	if want := `getHistory('readings') on app 'weatherstation' (dev) failed with WAMP error 'wamp.error.runtime_error' — ["boom"]`; err.Error() != want {
		t.Errorf("message %q\nwant %q", err, want)
	}
	// A refusal on a read does not evict the handle.
	if again, _ := c.ConnectToApp(bg, "weatherstation"); again != app {
		t.Error("a failed read evicted the handle")
	}

	answer(conn, "not rows")
	_, err = app.GetHistory(bg, "readings", nil)
	if !errors.Is(err, errUnexpectedResult) {
		t.Errorf("bad result: %v", err)
	}
}

func TestConsumedAppSubscribeToTable(t *testing.T) {
	c := newConsumer(t)
	app, err := c.ConnectToApp(bg, "weatherstation")
	if err != nil {
		t.Fatal(err)
	}
	conn := c.factory.all()[0]
	var rows []any
	opts := SubscribeOptions{Extra: map[string]any{"custom_option": true}}
	ts, err := app.SubscribeToTable(bg, "readings", func(ev *Event) { rows = append(rows, ev.Row()["temp"]) }, opts)
	if err != nil {
		t.Fatal(err)
	}
	subs := conn.allSubs()
	if len(subs) != 2 || subs[0].Topic != "transformed.readings" || subs[1].Topic != "transformed.bulk.readings" ||
		!reflect.DeepEqual(subs[1].Opts.Extra, opts.Extra) {
		t.Fatalf("subscriptions %#v", subs)
	}
	if subs[0].Opts.Group == nil || subs[0].Opts.Group != subs[1].Opts.Group {
		t.Error("the feeds do not deliver through one DeliveryGroup")
	}
	if len(c.own.allSubs()) != 0 {
		t.Error("subscribed on the own realm")
	}
	conn.fire("transformed.bulk.readings", &Event{Args: []any{[]any{
		map[string]any{"temp": int64(1)}, map[string]any{"temp": int64(2)},
	}}})
	if !reflect.DeepEqual(rows, []any{int64(1), int64(2)}) {
		t.Errorf("rows %#v", rows)
	}
	if err := ts.Unsubscribe(bg); err != nil {
		t.Fatal(err)
	}
}

func TestListConsumableApps(t *testing.T) {
	c := newConsumer(t)
	infos, err := c.ListConsumableApps(bg)
	if err != nil {
		t.Fatal(err)
	}
	want := []fakeCall{{Procedure: uriAppAccessList, Args: []any{}}}
	if got := c.own.callsTo(uriAppAccessList); !reflect.DeepEqual(got, want) {
		t.Errorf("calls %#v", got)
	}
	if len(infos) != 3 || infos[0].App != "weatherstation" || infos[0].ProviderAppKey != 77 ||
		infos[1].App != "vibration" || infos[2].App != "prodonly" {
		t.Fatalf("infos %#v", infos)
	}
	if infos[1].Catalog("prod") != nil || infos[1].Catalog("dev") == nil || infos[1].Catalog("dev").Tables[0].Tablename != "accel" {
		t.Errorf("vibration catalog %#v", infos[1].Stages)
	}
	if infos[0].Catalog("staging") != nil {
		t.Error("unknown stage must have no catalog")
	}
	if len(c.factory.all()) != 0 {
		t.Error("listing opened connections")
	}
}

func TestListConsumableAppsResultShapes(t *testing.T) {
	c := newConsumer(t)
	c.mu.Lock()
	c.list = nil
	c.mu.Unlock()
	infos, err := c.ListConsumableApps(bg)
	if err != nil || infos == nil || len(infos) != 0 {
		t.Errorf("no answer: %#v, %v", infos, err)
	}

	c.mu.Lock()
	c.list = []any{
		"garbage",
		map[string]any{"app": "x", "provider_app_key": "not a number"},
		map[string]any{"app": "empty", "provider_app_key": int64(5), "stages": map[string]any{"dev": map[string]any{}}},
	}
	c.mu.Unlock()
	infos, err = c.ListConsumableApps(bg)
	if err != nil {
		t.Fatal(err)
	}
	if len(infos) != 1 || infos[0].App != "empty" || infos[0].Catalog("dev") != nil {
		t.Errorf("infos %#v (an empty catalog counts as no backend)", infos)
	}
	mustContain(t, c.logs.String(), "Skipping entry 0", "Skipping entry 1")

	c.mu.Lock()
	c.list = map[string]any{"apps": 1}
	c.mu.Unlock()
	if _, err := c.ListConsumableApps(bg); !errors.Is(err, errUnexpectedResult) {
		t.Errorf("not a list: %v", err)
	}
}

func TestListConsumableAppsErrors(t *testing.T) {
	c := newConsumer(t)
	c.mu.Lock()
	c.listErr = wampErr("sys.appaccess.error.no_grant", "denied")
	c.mu.Unlock()
	_, err := c.ListConsumableApps(bg)
	assertCrossAppError(t, err, CodeNoGrant)

	c.mu.Lock()
	c.listErr = wampErr("wamp.error.runtime_error", "boom")
	c.mu.Unlock()
	_, err = c.ListConsumableApps(bg)
	if WampURI(err) != "wamp.error.runtime_error" {
		t.Errorf("err = %v", err)
	}
	mustContain(t, err.Error(), "sys.appaccess.list", "wamp.error.runtime_error")
}

func appNames(apps []*ConsumedApp) []string {
	names := make([]string, len(apps))
	for i, a := range apps {
		names[i] = a.App
	}
	return names
}

func TestConnectToAllAppsOpensEveryProviderOfTheStage(t *testing.T) {
	checkGoroutines(t)
	c := newConsumer(t)
	apps, err := c.ConnectToAllApps(bg)
	if err != nil {
		t.Fatal(err)
	}
	if got := appNames(apps); !reflect.DeepEqual(got, []string{"weatherstation", "vibration"}) {
		t.Errorf("apps %v", got)
	}
	if n := len(c.factory.all()); n != 2 {
		t.Errorf("%d connections", n)
	}
	c.factory.connFor(t, 77)
	c.factory.connFor(t, 88)
	if len(c.own.callsTo(uriAppAccessResolve)) != 0 {
		t.Error("ConnectToAllApps must open from the list, without resolving")
	}

	// ConnectToApp returns the warmed handle.
	warmed, err := c.ConnectToApp(bg, "weatherstation")
	if err != nil || warmed != apps[0] {
		t.Errorf("warmed handle: %v (same: %v)", err, warmed == apps[0])
	}
	if len(c.own.callsTo(uriAppAccessResolve)) != 0 || len(c.factory.all()) != 2 {
		t.Error("a cached handle was opened again")
	}

	// A second ConnectToAllApps reuses every handle.
	again, err := c.ConnectToAllApps(bg)
	if err != nil || !reflect.DeepEqual(again, apps) {
		t.Errorf("second call: %v", err)
	}

	if err := c.Stop(bg); err != nil {
		t.Fatal(err)
	}
	for _, conn := range c.factory.all() {
		if conn.stopCount() != 1 {
			t.Error("Stop did not close a connection opened by ConnectToAllApps")
		}
	}
}

func TestConnectToAllAppsStage(t *testing.T) {
	c := newConsumer(t)
	apps, err := c.ConnectToAllApps(bg, ConnectToAllAppsOptions{Stage: "prod"})
	if err != nil {
		t.Fatal(err)
	}
	if got := appNames(apps); !reflect.DeepEqual(got, []string{"weatherstation", "prodonly"}) {
		t.Errorf("apps %v", got)
	}
	for _, a := range apps {
		if a.Stage != "prod" {
			t.Errorf("%s: stage %q", a.App, a.Stage)
		}
	}
	if _, err := c.ConnectToAllApps(bg, ConnectToAllAppsOptions{Stage: "staging"}); !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("invalid stage: %v", err)
	}

	setIdentityEnv(t)
	t.Setenv("ENV", "PROD")
	c = consumerOf(t, newTestFlock(t))
	apps, err = c.ConnectToAllApps(bg)
	if err != nil {
		t.Fatal(err)
	}
	if got := appNames(apps); !reflect.DeepEqual(got, []string{"weatherstation", "prodonly"}) {
		t.Errorf("own stage prod: %v", got)
	}
}

func TestConnectToAllAppsReusesHandlesOfConnectToApp(t *testing.T) {
	c := newConsumer(t)
	first, err := c.ConnectToApp(bg, "WeatherStation")
	if err != nil {
		t.Fatal(err)
	}
	apps, err := c.ConnectToAllApps(bg)
	if err != nil {
		t.Fatal(err)
	}
	if apps[0] != first || len(c.factory.all()) != 2 {
		t.Errorf("the handle of ConnectToApp was not reused (%d connections)", len(c.factory.all()))
	}
}

func TestConnectToAllAppsReportsFailedProviders(t *testing.T) {
	c := newConsumer(t)
	c.factory.setSetup(denyAuth(88, "wamp.error.not_authorized"))
	var mu sync.Mutex
	var reported []error
	apps, err := c.ConnectToAllApps(bg, ConnectToAllAppsOptions{OnError: func(err error) {
		mu.Lock()
		defer mu.Unlock()
		reported = append(reported, err)
	}})
	if err != nil {
		t.Fatal(err)
	}
	if got := appNames(apps); !reflect.DeepEqual(got, []string{"weatherstation"}) {
		t.Errorf("apps %v", got)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(reported) != 1 {
		t.Fatalf("reported %v", reported)
	}
	assertCrossAppError(t, reported[0], CodeNotAuthorized)
}

func TestConnectToAllAppsStopOnError(t *testing.T) {
	c := newConsumer(t)
	// Both fail; vibration (second in the list) fails first, but the first
	// failure in list order is returned once every open has settled.
	release := make(chan struct{})
	c.factory.setSetup(func(fc *fakeConn) {
		fc.startFn = func(_ context.Context, cfg wamp.Config) error {
			if cfg.AppKey == 77 {
				<-release
				return errors.New("weatherstation failed")
			}
			defer close(release)
			return errors.New("vibration failed")
		}
	})
	called := false
	_, err := c.ConnectToAllApps(bg, ConnectToAllAppsOptions{StopOnError: true, OnError: func(error) { called = true }})
	if err == nil || !strings.Contains(err.Error(), "weatherstation failed") {
		t.Errorf("err = %v, want the first failure in list order", err)
	}
	if called {
		t.Error("OnError must not be called with StopOnError")
	}

	// Providers that opened stay cached.
	c2 := newConsumer(t)
	c2.factory.setSetup(denyAuth(88, "wamp.error.not_authorized"))
	_, err = c2.ConnectToAllApps(bg, ConnectToAllAppsOptions{StopOnError: true})
	assertCrossAppError(t, err, CodeNotAuthorized)
	if _, err := c2.ConnectToApp(bg, "weatherstation"); err != nil || len(c2.factory.all()) != 2 {
		t.Errorf("the opened provider was not kept: %v", err)
	}
}

func TestConnectToAllAppsOnErrorPanicsAreRecovered(t *testing.T) {
	c := newConsumer(t)
	c.factory.setSetup(denyAuth(0, "wamp.error.not_authorized"))
	calls := 0
	apps, err := c.ConnectToAllApps(bg, ConnectToAllAppsOptions{OnError: func(error) {
		calls++
		panic("callback bug")
	}})
	if err != nil || len(apps) != 0 {
		t.Fatalf("%v %v", apps, err)
	}
	if calls != 2 {
		t.Errorf("OnError called %d times, want 2 (once per failed provider)", calls)
	}
	mustContain(t, c.logs.String(), "ConnectToAllApps OnError callback panicked: callback bug")
}

func TestConnectToAllAppsReportsLaterRevocations(t *testing.T) {
	c := newConsumer(t)
	reported := make(chan error, 1)
	if _, err := c.ConnectToAllApps(bg, ConnectToAllAppsOptions{OnError: func(err error) { reported <- err }}); err != nil {
		t.Fatal(err)
	}
	c.factory.connFor(t, 88).config().OnAuthFailure("wamp.error.not_authorized")
	select {
	case err := <-reported:
		assertCrossAppError(t, err, CodeNotAuthorized)
		mustContain(t, err.Error(), "Access to app 'vibration' (dev) denied")
	case <-time.After(time.Second):
		t.Fatal("revocation not reported")
	}
}

func TestConnectToAllAppsPropagatesListErrors(t *testing.T) {
	c := newConsumer(t)
	c.mu.Lock()
	c.listErr = wampErr("sys.appaccess.error.no_grant")
	c.mu.Unlock()
	_, err := c.ConnectToAllApps(bg)
	assertCrossAppError(t, err, CodeNoGrant)
	if len(c.factory.all()) != 0 {
		t.Error("connections were opened")
	}
}

func TestConnectToAllAppsHonoursTheCallersContext(t *testing.T) {
	checkGoroutines(t)
	c := newConsumer(t)
	release := make(chan struct{})
	c.factory.setSetup(func(fc *fakeConn) {
		fc.startFn = func(context.Context, wamp.Config) error {
			<-release
			return nil
		}
	})
	ctx, cancel := context.WithTimeout(bg, 20*time.Millisecond)
	defer cancel()
	if _, err := c.ConnectToAllApps(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("err = %v", err)
	}
	close(release)
	// The opens completed in the background and are cached.
	apps, err := c.ConnectToAllApps(bg)
	if err != nil || len(apps) != 2 || len(c.factory.all()) != 2 {
		t.Errorf("%v %v (%d connections)", appNames(apps), err, len(c.factory.all()))
	}
}

func TestStopWithAnExpiredContextStillClosesOpenedApps(t *testing.T) {
	c := newConsumer(t)
	if _, err := c.ConnectToApp(bg, "weatherstation"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(bg)
	cancel()
	_ = c.Stop(ctx)
	if n := c.factory.all()[0].stopCount(); n != 1 {
		t.Errorf("consumed connection stopped %d times", n)
	}
}

func TestStopLogsTheCloseFailureOnce(t *testing.T) {
	c := newConsumer(t)
	if _, err := c.ConnectToApp(bg, "weatherstation"); err != nil {
		t.Fatal(err)
	}
	conn := c.factory.all()[0]
	conn.mu.Lock()
	conn.stopErr = errors.New("router gone")
	conn.mu.Unlock()
	_ = c.Stop(bg)
	logs := c.logs.String()
	mustContain(t, logs, "Failed to close consumed app 'weatherstation': router gone")
	if strings.Contains(logs, "Close of the connection") {
		t.Errorf("the close failure is wrapped twice: %s", logs)
	}
}

func TestAFailedConfigureStillStopsTheConnection(t *testing.T) {
	c := newConsumer(t)
	c.factory.setSetup(func(fc *fakeConn) { fc.configureErr = errors.New("bad config") })
	_, err := c.ConnectToApp(bg, "weatherstation")
	if err == nil || !strings.Contains(err.Error(), "bad config") {
		t.Fatalf("err = %v", err)
	}
	if c.factory.all()[0].stopCount() != 1 {
		t.Error("the connection was not stopped")
	}
}

func TestConnectToAppWithADoneContextStartsNothing(t *testing.T) {
	c := newConsumer(t)
	ctx, cancel := context.WithCancel(bg)
	cancel()
	if _, err := c.ConnectToApp(ctx, "weatherstation"); !errors.Is(err, context.Canceled) {
		t.Errorf("err = %v", err)
	}
	if len(c.own.callsTo(uriAppAccessResolve)) != 0 || len(c.cached()) != 0 {
		t.Error("an attempt was started")
	}
}
