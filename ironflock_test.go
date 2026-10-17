package ironflock

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ironflock/sdk-go/internal/env"
	"github.com/ironflock/sdk-go/wamp"
)

func TestNewReadsTheInjectedEnvironment(t *testing.T) {
	f := unstartedFlock(t)
	if got := f.SerialNumber(); got != "test-serial-123" {
		t.Errorf("SerialNumber = %q", got)
	}
	if got := f.DeviceName(); got != "test-device" {
		t.Errorf("DeviceName = %q", got)
	}
	if got := f.DeviceKey(); got != "12" {
		t.Errorf("DeviceKey = %q", got)
	}
	if got := f.AppName(); got != "TestApp" {
		t.Errorf("AppName = %q", got)
	}
	if f.SwarmKey() != 10 || f.AppKey() != 20 {
		t.Errorf("keys = %d, %d", f.SwarmKey(), f.AppKey())
	}
	if f.Stage() != StageDevelopment {
		t.Errorf("Stage = %q", f.Stage())
	}
	if f.reconnectWindow != DefaultReconnectWindow {
		t.Errorf("reconnect window = %v", f.reconnectWindow)
	}
	if strings.Contains(f.logs.String(), "must be present") {
		t.Errorf("unexpected missing-variable warning: %s", f.logs)
	}
	if f.IsConnected() {
		t.Error("IsConnected before Start")
	}
	if f.Connection() != nil {
		t.Error("Connection() of a fake must be nil")
	}
}

func TestNewOptionsOverrideTheEnvironment(t *testing.T) {
	f := flock(t,
		WithSerialNumber("other-serial"), WithDeviceName("n"), WithDeviceKey("99"), WithAppName("a"),
		WithSwarmKey(1), WithAppKey(2), WithEnv("prod"), WithReconnectWindow(5*time.Second))
	if f.SerialNumber() != "other-serial" || f.DeviceName() != "n" || f.DeviceKey() != "99" || f.AppName() != "a" {
		t.Errorf("identity = %q %q %q %q", f.SerialNumber(), f.DeviceName(), f.DeviceKey(), f.AppName())
	}
	if f.SwarmKey() != 1 || f.AppKey() != 2 || f.Stage() != StageProduction || f.reconnectWindow != 5*time.Second {
		t.Errorf("keys %d %d stage %q window %v", f.SwarmKey(), f.AppKey(), f.Stage(), f.reconnectWindow)
	}
}

func TestNewWithoutSerialNumberFails(t *testing.T) {
	setIdentityEnv(t)
	unsetEnv(t, "DEVICE_SERIAL_NUMBER")
	_, err := newWithConn(&fakeConn{}, nil)
	if !errors.Is(err, ErrMissingConfig) {
		t.Fatalf("err = %v, want ErrMissingConfig", err)
	}
	mustContain(t, err.Error(), "DEVICE_SERIAL_NUMBER")

	// An explicit serial number is enough.
	if _, err := newWithConn(&fakeConn{}, nil, WithSerialNumber("custom-serial")); err != nil {
		t.Fatal(err)
	}
}

func TestNewWarnsAboutMissingVariables(t *testing.T) {
	setIdentityEnv(t)
	unsetEnv(t, "DEVICE_KEY", "APP_NAME")
	f := newTestFlock(t)
	mustContain(t, f.logs.String(), "Warning: The following environment variables must be present: DEVICE_KEY, APP_NAME")

	setIdentityEnv(t)
	unsetEnv(t, "DEVICE_KEY", "APP_NAME", "SWARM_KEY", "APP_KEY")
	f = newTestFlock(t)
	mustContain(t, f.logs.String(),
		"Warning: The following environment variables must be present: DEVICE_KEY, APP_NAME, SWARM_KEY, APP_KEY")
}

func TestNewParsesKeys(t *testing.T) {
	cases := []struct {
		raw     string
		want    int
		warning bool
	}{
		{"7", 7, false},
		{" 7 ", 7, false},
		{"", 0, false},
		{"abc", 0, true},
		{"7.5", 0, true},
	}
	for _, tc := range cases {
		setIdentityEnv(t)
		t.Setenv("SWARM_KEY", tc.raw)
		f := newTestFlock(t)
		if f.SwarmKey() != tc.want {
			t.Errorf("SWARM_KEY=%q: got %d, want %d", tc.raw, f.SwarmKey(), tc.want)
		}
		if got := strings.Contains(f.logs.String(), "is not an integer"); got != tc.warning {
			t.Errorf("SWARM_KEY=%q: parse warning %v, want %v: %s", tc.raw, got, tc.warning, f.logs)
		}
	}
}

func TestStageFromEnv(t *testing.T) {
	cases := map[string]Stage{
		"DEV": StageDevelopment, "PROD": StageProduction, "": StageDevelopment, "dev": StageDevelopment,
		"prod": StageProduction, "Prod": StageProduction, "production": StageDevelopment, "staging": StageDevelopment,
	}
	for env, want := range cases {
		setIdentityEnv(t)
		t.Setenv("ENV", env)
		if got := newTestFlock(t).Stage(); got != want {
			t.Errorf("ENV=%q: stage %q, want %q", env, got, want)
		}
	}
	setIdentityEnv(t)
	unsetEnv(t, "ENV")
	if got := newTestFlock(t).Stage(); got != StageDevelopment {
		t.Errorf("no ENV: stage %q", got)
	}
}

func TestNewCredentialsAreBothOrNeither(t *testing.T) {
	setIdentityEnv(t)
	for _, opt := range []Option{WithCredentials("id", ""), WithCredentials("", "secret")} {
		if _, err := newWithConn(&fakeConn{}, nil, opt); !errors.Is(err, ErrInvalidArgument) {
			t.Errorf("err = %v, want ErrInvalidArgument", err)
		}
	}
}

func TestNewReconnectWindow(t *testing.T) {
	if f := flock(t); f.reconnectWindow != DefaultReconnectWindow || DefaultReconnectWindow != 60*time.Second {
		t.Errorf("default window %v (DefaultReconnectWindow %v), want 60s", f.reconnectWindow, DefaultReconnectWindow)
	}
	for _, d := range []time.Duration{0, time.Nanosecond, 90 * time.Second} {
		if f := flock(t, WithReconnectWindow(d)); f.reconnectWindow != d {
			t.Errorf("WithReconnectWindow(%v) = %v", d, f.reconnectWindow)
		}
	}
	// A negative window is refused, not taken for 0; the last option counts.
	for _, opts := range [][]Option{
		{WithReconnectWindow(-time.Second)},
		{WithReconnectWindow(-time.Nanosecond)},
		{WithReconnectWindow(time.Minute), WithReconnectWindow(-time.Minute)},
	} {
		for name, create := range map[string]func() (*IronFlock, error){
			"newWithConn": func() (*IronFlock, error) { return newWithConn(&fakeConn{}, nil, opts...) },
			"New":         func() (*IronFlock, error) { return New(opts...) },
		} {
			f, err := create()
			if f != nil || !errors.Is(err, ErrInvalidArgument) {
				t.Errorf("%s with a negative window = %v, %v, want an error wrapping ErrInvalidArgument", name, f, err)
				continue
			}
			mustContain(t, err.Error(), "WithReconnectWindow must not be negative")
		}
	}
	if _, err := newWithConn(&fakeConn{}, nil, WithReconnectWindow(-time.Minute), WithReconnectWindow(0)); err != nil {
		t.Errorf("a negative window overridden by 0: %v", err)
	}
}

func TestStartConfiguresTheConnection(t *testing.T) {
	f := unstartedFlock(t)
	if err := f.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	cfg := f.own.config()
	if cfg.SwarmKey != 10 || cfg.AppKey != 20 || cfg.Stage != StageDevelopment {
		t.Errorf("realm config = %d %d %q", cfg.SwarmKey, cfg.AppKey, cfg.Stage)
	}
	if cfg.URL != wamp.StudioWSURI {
		t.Errorf("URL = %q", cfg.URL)
	}
	if cfg.SerialNumber != "test-serial-123" || cfg.AuthID != "" || cfg.AuthSecret != "" {
		t.Errorf("identity = %q %q %q", cfg.SerialNumber, cfg.AuthID, cfg.AuthSecret)
	}
	if cfg.FailOnAuthError {
		t.Error("the primary connection must not fail on auth errors")
	}
	if cfg.Logger == nil {
		t.Error("logger not passed on")
	}
	if !f.IsConnected() {
		t.Error("IsConnected after Start")
	}
	if err := f.Start(context.Background()); !errors.Is(err, ErrAlreadyStarted) {
		t.Errorf("a second Start = %v, want ErrAlreadyStarted", err)
	}
	if f.own.startCount() != 1 || f.own.stopCount() != 0 {
		t.Errorf("started %d, stopped %d times", f.own.startCount(), f.own.stopCount())
	}
}

func TestStartURLPrecedence(t *testing.T) {
	cases := []struct {
		name string
		env  map[string]string
		opts []Option
		want string
	}{
		{"default", nil, nil, wamp.StudioWSURI},
		{"RESWARM_URL", map[string]string{"RESWARM_URL": "http://localhost:8086"}, nil, wamp.LocalhostWSURI},
		{"WithReswarmURL", nil, []Option{WithReswarmURL("https://studio.ironflock.dev")}, wamp.StudioDevWSURI},
		{"DEVICE_ENDPOINT_URL", map[string]string{"DEVICE_ENDPOINT_URL": "ws://10.0.0.1:18080/x", "RESWARM_URL": "http://localhost:8086"}, nil, "ws://10.0.0.1:18080/ws-ua-usr"},
		{"WithURL", map[string]string{"DEVICE_ENDPOINT_URL": "ws://10.0.0.1:18080/x"}, []Option{WithURL("ws://explicit/ws")}, "ws://explicit/ws"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			setIdentityEnv(t)
			for k, v := range tc.env {
				t.Setenv(k, v)
			}
			f := newTestFlock(t, tc.opts...)
			if err := f.Start(context.Background()); err != nil {
				t.Fatal(err)
			}
			if got := f.own.config().URL; got != tc.want {
				t.Errorf("URL = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestStartWithUnknownStudioURLFails(t *testing.T) {
	setIdentityEnv(t)
	t.Setenv("RESWARM_URL", "https://appliance.example")
	f := newTestFlock(t)
	if err := f.Start(context.Background()); err == nil {
		t.Fatal("want an error for an unknown RESWARM_URL")
	}
	if f.own.startCount() != 0 {
		t.Error("the connection must not be started")
	}
}

func TestStartPassesExplicitCredentials(t *testing.T) {
	f := unstartedFlock(t, WithCredentials("app-id", "app-secret"))
	if err := f.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if cfg := f.own.config(); cfg.AuthID != "app-id" || cfg.AuthSecret != "app-secret" {
		t.Errorf("credential = %q %q", cfg.AuthID, cfg.AuthSecret)
	}
}

func TestStartWithoutKeysFailsFast(t *testing.T) {
	setIdentityEnv(t)
	unsetEnv(t, "APP_KEY")
	f := newTestFlock(t)
	err := f.Start(context.Background())
	if !errors.Is(err, ErrMissingConfig) {
		t.Fatalf("err = %v, want ErrMissingConfig", err)
	}
	mustContain(t, err.Error(), "APP_KEY not set in environment variables!")
	if f.own.configureCount() != 0 || f.own.startCount() != 0 {
		t.Error("the connection must not be touched")
	}
}

// A failed Start leaves the instance able to start again (a wamp.Connection
// is configured once and restartable after a failed Start; the fake follows
// it), whatever ended it.
func TestStartCanBeRetriedAfterAFailure(t *testing.T) {
	f := unstartedFlock(t)
	failures := 1
	f.own.startFn = func(ctx context.Context, cfg wamp.Config) error {
		if failures > 0 {
			failures--
			<-ctx.Done() // the realm is not there yet
			return fmt.Errorf("wamp: no session on realm %s: %w", cfg.Realm, ctx.Err())
		}
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := f.Start(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("first Start: %v", err)
	}
	if f.IsConnected() {
		t.Fatal("connected after a failed Start")
	}
	if err := f.Start(context.Background()); err != nil {
		t.Fatalf("second Start: %v", err)
	}
	if !f.IsConnected() {
		t.Error("not connected after the second Start")
	}
	if f.own.configureCount() != 1 || f.own.startCount() != 2 || f.own.stopCount() != 0 {
		t.Errorf("configured %d, started %d, stopped %d times",
			f.own.configureCount(), f.own.startCount(), f.own.stopCount())
	}
	if err := f.PublishToTable(bg, "sensordata", Row{"temp": 1}); err != nil {
		t.Errorf("PublishToTable after the second Start: %v", err)
	}
}

// Start while another Start is still running is refused with
// ErrAlreadyStarted, and the running Start is not disturbed.
func TestStartWhileStarting(t *testing.T) {
	checkGoroutines(t)
	f := unstartedFlock(t)
	entered, release := make(chan struct{}), make(chan struct{})
	f.own.startFn = func(context.Context, wamp.Config) error {
		close(entered)
		<-release
		return nil
	}
	first := make(chan error, 1)
	go func() { first <- f.Start(context.Background()) }()
	recv(t, entered, "the first Start")
	if err := f.Start(context.Background()); !errors.Is(err, ErrAlreadyStarted) {
		t.Errorf("Start while starting = %v, want ErrAlreadyStarted", err)
	}
	close(release)
	if err := recv(t, first, "the first Start"); err != nil {
		t.Fatalf("first Start: %v", err)
	}
	if f.own.startCount() != 1 || !f.IsConnected() {
		t.Errorf("started %d times, connected %v", f.own.startCount(), f.IsConnected())
	}
}

func TestStopIsIdempotent(t *testing.T) {
	f := flock(t)
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := f.Stop(context.Background()); err != nil {
				t.Errorf("Stop: %v", err)
			}
		}()
	}
	wg.Wait()
	if err := f.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if n := f.own.stopCount(); n != 1 {
		t.Errorf("connection stopped %d times", n)
	}
	if err := f.Start(context.Background()); !errors.Is(err, wamp.ErrStopped) {
		t.Errorf("Start after Stop: %v", err)
	}
	if _, err := f.ConnectToApp(context.Background(), "weatherstation"); !errors.Is(err, wamp.ErrStopped) {
		t.Errorf("ConnectToApp after Stop: %v", err)
	}
	if err := f.PublishToTable(bg, "sensordata", Row{"temp": 1}); !errors.Is(err, wamp.ErrStopped) {
		t.Errorf("PublishToTable after Stop: %v", err)
	}
}

func TestStopReturnsTheConnectionError(t *testing.T) {
	f := flock(t)
	boom := errors.New("boom")
	f.own.stopErr = boom
	if err := f.Stop(context.Background()); !errors.Is(err, boom) {
		t.Errorf("Stop: %v", err)
	}
}

func TestRunRunsMainAndStops(t *testing.T) {
	checkGoroutines(t)
	f := unstartedFlock(t)
	sentinel := errors.New("main failed")
	err := f.Run(context.Background(), func(ctx context.Context) error {
		if !f.IsConnected() {
			t.Error("main runs after Start")
		}
		for range 3 {
			if err := f.Publish(ctx, "test.publish.com", 1, "two", 3, Kwargs{"foo": "bar"}); err != nil {
				return err
			}
		}
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("Run = %v, want main's error", err)
	}
	if n := len(f.own.allPublishes()); n != 3 {
		t.Errorf("%d publications", n)
	}
	if f.own.startCount() != 1 || f.own.stopCount() != 1 {
		t.Errorf("started %d, stopped %d", f.own.startCount(), f.own.stopCount())
	}
}

func TestRunCancelsMainOnContextDone(t *testing.T) {
	checkGoroutines(t)
	f := unstartedFlock(t)
	ctx, cancel := context.WithCancel(context.Background())
	mainCtxDone := make(chan struct{})
	err := f.Run(ctx, func(ctx context.Context) error {
		cancel()
		<-ctx.Done()
		close(mainCtxDone)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-mainCtxDone:
	default:
		t.Error("main's context was not cancelled")
	}
	if f.own.stopCount() != 1 {
		t.Error("Run did not stop the connection")
	}
}

func TestRunWithoutMainRunsUntilContextDone(t *testing.T) {
	checkGoroutines(t)
	f := unstartedFlock(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := f.Run(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if f.own.startCount() != 1 || f.own.stopCount() != 1 {
		t.Errorf("started %d, stopped %d", f.own.startCount(), f.own.stopCount())
	}
}

func TestRunReturnsTheStartError(t *testing.T) {
	checkGoroutines(t)
	f := unstartedFlock(t)
	boom := errors.New("cannot connect")
	f.own.startFn = func(context.Context, wamp.Config) error { return boom }
	called := false
	err := f.Run(context.Background(), func(context.Context) error { called = true; return nil })
	if !errors.Is(err, boom) {
		t.Fatalf("Run = %v", err)
	}
	if called {
		t.Error("main must not run when Start fails")
	}
	if f.own.stopCount() != 1 {
		t.Error("Run did not stop the connection after the failed Start")
	}
}

func TestRunStopsWithAFreshContext(t *testing.T) {
	f := unstartedFlock(t)
	ctx, cancel := context.WithCancel(context.Background())
	err := f.Run(ctx, func(context.Context) error {
		cancel() // the caller's context is already done when Run stops
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if f.own.stopCount() != 1 {
		t.Errorf("stopped %d times", f.own.stopCount())
	}
}

// Stop ends Run, wherever it is called from: main's context is cancelled
// and Run returns, as the Python SDK's stop() cancels the main task.
func TestRunEndsWhenStoppedElsewhere(t *testing.T) {
	checkGoroutines(t)
	f := unstartedFlock(t)
	inMain := make(chan struct{})
	ran := make(chan error, 1)
	go func() {
		ran <- f.Run(context.Background(), func(ctx context.Context) error {
			close(inMain)
			<-ctx.Done()
			return ctx.Err()
		})
	}()
	recv(t, inMain, "main")
	if err := f.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if err := recv(t, ran, "Run to return after Stop"); !errors.Is(err, context.Canceled) {
		t.Errorf("Run = %v, want main's error", err)
	}
	if f.own.stopCount() != 1 {
		t.Errorf("stopped %d times", f.own.stopCount())
	}
}

func TestRunWithoutMainEndsOnStop(t *testing.T) {
	checkGoroutines(t)
	f := unstartedFlock(t)
	ran := make(chan error, 1)
	go func() { ran <- f.Run(context.Background(), nil) }()
	eventually(t, "Run to connect", f.IsConnected)
	if err := f.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if err := recv(t, ran, "Run to return after Stop"); err != nil {
		t.Errorf("Run = %v", err)
	}
}

// main may stop the instance itself, with its own context, which Stop
// cancels: Stop still completes, and Run returns once main does (the Go
// counterpart of the Python SDK's test_simple_publish_integration).
func TestRunMainCallsStop(t *testing.T) {
	checkGoroutines(t)
	f := unstartedFlock(t)
	var stopErr, ctxErr error
	release := make(chan struct{})
	f.own.stopFn = func(ctx context.Context) error {
		// Like the router's GOODBYE round trip: Stop waits for it within
		// its context.
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	err := f.Run(context.Background(), func(ctx context.Context) error {
		if err := f.Publish(ctx, "test.publish.com", "before stop"); err != nil {
			return err
		}
		go func() {
			// Release the GOODBYE only once main's context is cancelled:
			// the Stop below must outlive that.
			<-ctx.Done()
			close(release)
		}()
		stopErr = f.Stop(ctx)
		<-ctx.Done()
		ctxErr = ctx.Err()
		return nil
	})
	if err != nil {
		t.Fatalf("Run = %v", err)
	}
	if stopErr != nil {
		t.Errorf("Stop from main = %v", stopErr)
	}
	if !errors.Is(ctxErr, context.Canceled) {
		t.Errorf("main's context: %v, want cancelled by Stop", ctxErr)
	}
	if f.own.stopCount() != 1 {
		t.Errorf("stopped %d times", f.own.stopCount())
	}
}

// Run on an instance that is started already (or whose Start is running)
// fails with ErrAlreadyStarted and leaves it running: Run does not own it.
func TestRunOnAStartedInstanceLeavesItRunning(t *testing.T) {
	checkGoroutines(t)
	f := flock(t)
	called := false
	err := f.Run(context.Background(), func(context.Context) error { called = true; return nil })
	if !errors.Is(err, ErrAlreadyStarted) {
		t.Fatalf("Run = %v, want ErrAlreadyStarted", err)
	}
	if called {
		t.Error("main ran")
	}
	if f.own.stopCount() != 0 || !f.IsConnected() {
		t.Errorf("stopped %d times, connected %v", f.own.stopCount(), f.IsConnected())
	}
}

func TestSecondRunDoesNotStopTheFirst(t *testing.T) {
	checkGoroutines(t)
	f := unstartedFlock(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	inMain := make(chan struct{})
	first := make(chan error, 1)
	go func() {
		first <- f.Run(ctx, func(ctx context.Context) error {
			close(inMain)
			<-ctx.Done()
			return nil
		})
	}()
	recv(t, inMain, "the first Run's main")
	if err := f.Run(context.Background(), func(context.Context) error { return nil }); !errors.Is(err, ErrAlreadyStarted) {
		t.Errorf("second Run = %v, want ErrAlreadyStarted", err)
	}
	if f.own.stopCount() != 0 || !f.IsConnected() {
		t.Errorf("the second Run stopped the first one's connection (stops %d)", f.own.stopCount())
	}
	cancel()
	if err := recv(t, first, "the first Run"); err != nil {
		t.Errorf("first Run = %v", err)
	}
	if f.own.stopCount() != 1 {
		t.Errorf("stopped %d times", f.own.stopCount())
	}
}

func TestRunAfterStop(t *testing.T) {
	checkGoroutines(t)
	f := unstartedFlock(t)
	if err := f.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	err := f.Run(context.Background(), func(context.Context) error {
		t.Error("main ran after Stop")
		return nil
	})
	if !errors.Is(err, wamp.ErrStopped) {
		t.Errorf("Run after Stop = %v, want ErrStopped", err)
	}
}

func TestFilesIsBuiltOnceAndLazily(t *testing.T) {
	f := unstartedFlock(t)
	if f.files != nil {
		t.Fatal("Files built before first use")
	}
	first := f.Files()
	var wg sync.WaitGroup
	got := make([]*FileStore, 8)
	for i := range got {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got[i] = f.Files()
		}()
	}
	wg.Wait()
	for i, fs := range got {
		if fs != first {
			t.Errorf("Files() call %d returned a different store", i)
		}
	}
}

func TestNewUsesAWampConnection(t *testing.T) {
	setIdentityEnv(t)
	log, _ := newTestLogger()
	f, err := New(WithLogger(log))
	if err != nil {
		t.Fatal(err)
	}
	if f.Connection() == nil {
		t.Error("Connection() must expose the *wamp.Connection")
	}
	if c, ok := f.newConn().(*wamp.Connection); !ok || c == nil || c == f.Connection() {
		t.Error("consumed apps must get connections of their own")
	}

	unsetEnv(t, "DEVICE_SERIAL_NUMBER")
	if _, err := New(); !errors.Is(err, ErrMissingConfig) {
		t.Errorf("New without a serial number: %v", err)
	}
}

// gatedOp is an operation run on an IronFlock that has not been started.
type gatedOp struct {
	name string
	kind opKind
	do   func(f *testFlock) error
}

// opKind tells how long an operation waits for a connection, and whether it
// is retried.
type opKind int

const (
	// plainOp waits the default session wait, and is not retried.
	plainOp opKind = iota
	// windowOp waits within the reconnect window, and is not retried:
	// Publish, Subscribe, SubscribeToTable, RegisterDeviceFunction.
	windowOp
	// tableOp waits within the reconnect window, and retries a call the
	// data backend does not serve yet.
	tableOp
)

// windowed reports whether op waits within the reconnect window.
func (op gatedOp) windowed() bool { return op.kind != plainOp }

func gatedOps() []gatedOp {
	noop := func(context.Context, *Invocation) (any, error) { return nil, nil }
	q := &TableQueryParams{Limit: 1}
	return []gatedOp{
		{"PublishToTable", tableOp, func(f *testFlock) error { return f.PublishToTable(bg, "sensordata", Row{"temp": 1}) }},
		{"PublishRowsToTable", tableOp, func(f *testFlock) error {
			return f.PublishRowsToTable(bg, "sensordata", []Row{{"temp": 1}})
		}},
		{"AppendToTable", tableOp, func(f *testFlock) error {
			_, err := f.AppendToTable(bg, "sensordata", Row{"temp": 1})
			return err
		}},
		{"AppendRowsToTable", tableOp, func(f *testFlock) error {
			_, err := f.AppendRowsToTable(bg, "sensordata", []Row{{"temp": 1}})
			return err
		}},
		{"ReportError", tableOp, func(f *testFlock) error {
			_, err := f.ReportError(bg, "boom")
			return err
		}},
		{"ReportError with Append", tableOp, func(f *testFlock) error {
			_, err := f.ReportError(bg, "boom", ReportErrorOptions{Append: true})
			return err
		}},
		{"GetHistory", tableOp, func(f *testFlock) error {
			_, err := f.GetHistory(bg, "sensordata", q)
			return err
		}},
		{"GetSeriesHistory", tableOp, func(f *testFlock) error {
			_, err := f.GetSeriesHistory(bg, "sensordata", SeriesQueryParams{
				Metrics: []SeriesMetric{{"temp", MethodAvg}}, Limit: 1, TimeRange: &TimeRange{Start: int64(0)}})
			return err
		}},
		{"RevealSecrets", tableOp, func(f *testFlock) error {
			_, err := f.RevealSecrets(bg, "sensordata", q)
			return err
		}},
		{"VerifySecret", tableOp, func(f *testFlock) error {
			_, err := f.VerifySecret(bg, "sensordata", "pin", "1234", q)
			return err
		}},
		{"Publish", windowOp, func(f *testFlock) error { return f.Publish(bg, "com.example.topic", 1) }},
		{"Call", plainOp, func(f *testFlock) error {
			_, err := f.Call(bg, "com.example.procedure")
			return err
		}},
		{"CallDeviceFunction", plainOp, func(f *testFlock) error {
			_, err := f.CallDeviceFunction(bg, 13, "toggle")
			return err
		}},
		{"SetDeviceLocation", plainOp, func(f *testFlock) error {
			_, err := f.SetDeviceLocation(bg, 8.4, 49.0)
			return err
		}},
		{"Subscribe", windowOp, func(f *testFlock) error {
			_, err := f.Subscribe(bg, "com.example.topic", func(*Event) {})
			return err
		}},
		{"SubscribeToTable", windowOp, func(f *testFlock) error {
			_, err := f.SubscribeToTable(bg, "sensordata", func(*Event) {})
			return err
		}},
		{"RegisterDeviceFunction", windowOp, func(f *testFlock) error {
			_, err := f.RegisterDeviceFunction(bg, "toggle", noop)
			return err
		}},
		{"ListConsumableApps", plainOp, func(f *testFlock) error {
			_, err := f.ListConsumableApps(bg)
			return err
		}},
		{"ConnectToApp", plainOp, func(f *testFlock) error {
			_, err := f.ConnectToApp(bg, "weatherstation")
			return err
		}},
		{"Files().Catalog", plainOp, func(f *testFlock) error {
			_, err := f.Files().Catalog(bg)
			return err
		}},
	}
}

// answerServices makes the own fake connection answer the platform services
// the gated operations call.
func answerServices(c *fakeConn) {
	c.callFn = func(call fakeCall) (*wamp.Result, error) {
		switch call.Procedure {
		case uriAppAccessResolve:
			return resultOf(resolveResult()), nil
		case uriAppAccessList:
			return resultOf(listResult()), nil
		case "files.read.namespaces":
			return resultOf(map[string]any{"success": true, "payload": map[string]any{
				"namespaces": []any{map[string]any{"name": "default"}}}}), nil
		}
		return &wamp.Result{}, nil
	}
}

// traffic counts the operations that reached c.
func traffic(c *fakeConn) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.calls) + len(c.publishes) + len(c.subs) + len(c.regs)
}

// An operation issued before Start waits for Start instead of failing, and
// goes through once Start has configured the connection.
func TestOperationsBeforeStartWaitForIt(t *testing.T) {
	checkGoroutines(t)
	for _, op := range gatedOps() {
		t.Run(op.name, func(t *testing.T) {
			f := unstartedFlock(t)
			answerServices(f.own)
			done := make(chan error, 1)
			go func() { done <- op.do(f) }()
			eventually(t, "the operation to wait for Start", func() bool { return f.startWaiters.Load() == 1 })
			if n := traffic(f.own); n != 0 {
				t.Fatalf("%d operations reached the connection before Start", n)
			}
			f.start(t)
			if err := recv(t, done, op.name); err != nil {
				t.Fatalf("%s: %v", op.name, err)
			}
			if traffic(f.own) == 0 {
				t.Fatal("nothing reached the connection")
			}
			if n := f.startWaiters.Load(); n != 0 {
				t.Errorf("%d operations still waiting", n)
			}
		})
	}
}

// The wait for Start counts against the operation's window: what is left of
// it is the connection's wait for a session (and a table call's retry
// window).
func TestOperationsBeforeStartKeepTheirWindow(t *testing.T) {
	const window = time.Hour
	f := unstartedFlock(t, WithReconnectWindow(window))
	answerServices(f.own)
	ops := []func() error{
		func() error { return f.PublishToTable(bg, "sensordata", Row{"temp": 1}) },
		func() error {
			_, err := f.AppendToTable(bg, "sensordata", Row{"temp": 1})
			return err
		},
		func() error { return f.Publish(bg, "com.example.topic", 1) },
		func() error {
			_, err := f.Subscribe(bg, "com.example.topic", func(*Event) {})
			return err
		},
		func() error {
			_, err := f.RegisterDeviceFunction(bg, "toggle", func(context.Context, *Invocation) (any, error) { return nil, nil })
			return err
		},
		func() error {
			_, err := f.Call(bg, "com.example.procedure")
			return err
		},
	}
	errs := make(chan error, len(ops))
	for _, op := range ops {
		go func() { errs <- op() }()
	}
	eventually(t, "the operations to wait for Start", func() bool { return f.startWaiters.Load() == int32(len(ops)) })
	f.start(t)
	for range ops {
		if err := recv(t, errs, "an operation"); err != nil {
			t.Fatal(err)
		}
	}
	within := func(d, of time.Duration) bool { return d > of-5*time.Second && d <= of }
	if w := f.own.lastPublish(t).Window; !within(w, window) {
		t.Errorf("Publish waits %v for a session, want what is left of %v", w, window)
	}
	if subs := f.own.allSubs(); len(subs) != 1 || !within(subs[0].Window, window) {
		t.Errorf("subscriptions %#v, want one waiting what is left of %v", subs, window)
	}
	if regs := f.own.allRegs(); len(regs) != 1 || !within(regs[0].Window, window) {
		t.Errorf("registrations %#v, want one waiting what is left of %v", regs, window)
	}
	if n := len(f.own.callsTo("append.10.20.sensordata")); n != 2 {
		t.Errorf("%d appends, want 2 (PublishToTable appends)", n)
	}
	for _, c := range f.own.allCalls() {
		switch c.Procedure {
		case "append.10.20.sensordata":
			if !within(c.Window, window) {
				t.Errorf("a table write retries for %v, want what is left of %v", c.Window, window)
			}
		case "com.example.procedure":
			if c.Window != 0 {
				t.Errorf("Call has a retry window: %v", c.Window)
			}
		}
	}
	// A plain call waits for the session separately, and is not retried.
	f.own.mu.Lock()
	waits := slices.Clone(f.own.waits)
	f.own.mu.Unlock()
	if len(waits) != 1 || !within(waits[0], wamp.DefaultSessionWaitTimeout) {
		t.Errorf("session waits %v, want one of what is left of %v", waits, wamp.DefaultSessionWaitTimeout)
	}
}

// When Start does not come within the window, the operation fails as one
// that found no session does: with an error wrapping wamp.ErrNotConnected.
func TestOperationsBeforeStartTimeOut(t *testing.T) {
	checkGoroutines(t)
	const window, sessionWait = 80 * time.Millisecond, 40 * time.Millisecond
	for _, op := range gatedOps() {
		t.Run(op.name, func(t *testing.T) {
			f := unstartedFlock(t, WithReconnectWindow(window))
			f.sessionWait = sessionWait
			want := sessionWait
			if op.windowed() {
				want = window
			}
			began := time.Now()
			err := op.do(f)
			took := time.Since(began)
			if !errors.Is(err, wamp.ErrNotConnected) || errors.Is(err, wamp.ErrNotConfigured) {
				t.Fatalf("%s = %v, want an error wrapping ErrNotConnected", op.name, err)
			}
			// The wait is named in seconds, as the connection names its own.
			mustContain(t, err.Error(), "the connection was not started within "+inSeconds(want)+":")
			if took < want {
				t.Errorf("%s failed after %v, before its window of %v", op.name, took, want)
			}
		})
	}
	// Without a reconnect window, the operations that use it wait the
	// default, as the others do.
	for _, op := range gatedOps() {
		if !op.windowed() {
			continue
		}
		f := unstartedFlock(t, WithReconnectWindow(0))
		f.sessionWait = sessionWait
		began := time.Now()
		err := op.do(f)
		if !errors.Is(err, wamp.ErrNotConnected) || !strings.Contains(err.Error(), "within "+inSeconds(sessionWait)+":") {
			t.Errorf("%s without a reconnect window = %v", op.name, err)
		}
		if took := time.Since(began); took < sessionWait {
			t.Errorf("%s without a reconnect window failed after %v, before the default wait %v", op.name, took, sessionWait)
		}
	}
	// The default reconnect window, a minute, is named in seconds too.
	if got, want := seconds(DefaultReconnectWindow), "60s"; got != want {
		t.Errorf("seconds(%v) = %q, want %q", DefaultReconnectWindow, got, want)
	}
}

// inSeconds writes d as the not-connected errors name a wait: in seconds.
func inSeconds(d time.Duration) string { return fmt.Sprintf("%gs", d.Seconds()) }

// Stop wakes the operations waiting for Start: they fail with an error
// wrapping wamp.ErrStopped, as do operations issued after it.
func TestStopWakesOperationsWaitingForStart(t *testing.T) {
	checkGoroutines(t)
	for _, op := range gatedOps() {
		t.Run(op.name, func(t *testing.T) {
			f := unstartedFlock(t)
			done := make(chan error, 1)
			go func() { done <- op.do(f) }()
			eventually(t, "the operation to wait for Start", func() bool { return f.startWaiters.Load() == 1 })
			if err := f.Stop(bg); err != nil {
				t.Fatal(err)
			}
			if err := recv(t, done, op.name); !errors.Is(err, wamp.ErrStopped) {
				t.Errorf("%s woken by Stop = %v, want ErrStopped", op.name, err)
			}
			if err := op.do(f); !errors.Is(err, wamp.ErrStopped) {
				t.Errorf("%s after Stop = %v, want ErrStopped", op.name, err)
			}
			if n := traffic(f.own); n != 0 {
				t.Errorf("%d operations reached the connection", n)
			}
		})
	}
}

// An operation waiting for Start gives up when its context ends.
func TestOperationsWaitingForStartHonourTheirContext(t *testing.T) {
	checkGoroutines(t)
	f := unstartedFlock(t)
	ctx, cancel := context.WithCancel(bg)
	done := make(chan error, 1)
	go func() { done <- f.PublishToTable(ctx, "sensordata", Row{"temp": 1}) }()
	eventually(t, "the operation to wait for Start", func() bool { return f.startWaiters.Load() == 1 })
	cancel()
	if err := recv(t, done, "PublishToTable"); !errors.Is(err, context.Canceled) {
		t.Errorf("PublishToTable = %v, want context.Canceled", err)
	}
	if n := f.startWaiters.Load(); n != 0 {
		t.Errorf("%d operations still waiting", n)
	}
}

// Start configures the connection once, even when it fails: operations
// issued after a failed Start wait for the connection, within their window,
// and are served by the next Start.
func TestOperationsAfterAFailedStartWaitForTheNextOne(t *testing.T) {
	checkGoroutines(t)
	f := unstartedFlock(t)
	fail := true
	f.own.startFn = func(ctx context.Context, cfg wamp.Config) error {
		if fail {
			return fmt.Errorf("wamp: no session on realm %s: %w", cfg.Realm, context.DeadlineExceeded)
		}
		return nil
	}
	if err := f.Start(bg); err == nil {
		t.Fatal("the first Start succeeded")
	}
	fail = false
	done := make(chan error, 1)
	go func() { done <- f.PublishToTable(bg, "sensordata", Row{"temp": 1}) }()
	eventually(t, "the publication to wait for a session", func() bool {
		f.own.mu.Lock()
		defer f.own.mu.Unlock()
		return f.own.up != nil // the fake's session wait has begun
	})
	f.start(t)
	if err := recv(t, done, "PublishToTable"); err != nil {
		t.Fatal(err)
	}
	if n := f.startWaiters.Load(); n != 0 {
		t.Errorf("%d operations waited for Start", n)
	}
}

// persistConnGoroutines counts the goroutines of pooled HTTP connections.
func persistConnGoroutines() int {
	buf := make([]byte, 1<<20)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			buf = buf[:n]
			break
		}
		buf = make([]byte, 2*len(buf))
	}
	return strings.Count(string(buf), "net/http.(*persistConn)")
}

// Stop closes the keep-alive connections the store Files returns keeps idle
// after a direct transfer, and with them their goroutines, while the object
// store keeps them open.
func TestStopClosesTheFileStoresIdleConnections(t *testing.T) {
	objects := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(make([]byte, 100))
	}))
	defer objects.Close()
	f := flock(t)
	f.own.callFn = func(c fakeCall) (*wamp.Result, error) {
		switch c.Procedure {
		case "files.read.namespaces":
			return resultOf(map[string]any{"success": true, "payload": map[string]any{
				"namespaces":        []any{map[string]any{"name": "default"}},
				"inline_max_bytes":  int64(10),
				"presign_available": true,
				"presign_max_bytes": int64(5 << 30),
			}}), nil
		case "files.read.get":
			return resultOf(map[string]any{"success": false, "code": "TOO_LARGE", "reason": "too big"}), nil
		case "files.read.url":
			return resultOf(map[string]any{"success": true, "payload": map[string]any{"url": objects.URL + "/get", "method": "GET"}}), nil
		}
		return &wamp.Result{}, nil
	}
	data, err := f.Files().Get(bg, "big.bin")
	if err != nil || len(data) != 100 {
		t.Fatalf("Get: %d bytes, %v", len(data), err)
	}
	if persistConnGoroutines() == 0 {
		t.Fatal("no pooled connection after the direct download")
	}
	if err := f.Stop(bg); err != nil {
		t.Fatal(err)
	}
	eventually(t, "the pooled connections to close", func() bool { return persistConnGoroutines() == 0 })
}

// The connection reads the per-app credential from the device agent's
// /data/env mirror on every attempt, and the agent makes it readable by root
// only: in a container that runs as another user Start says so, once, on the
// app's logger.
func TestStartWarnsWhenTheCredentialMirrorIsUnreadable(t *testing.T) {
	if runtime.GOOS == "windows" || os.Geteuid() == 0 {
		t.Skip("file permissions do not apply to this process")
	}
	env.ResetWarning()
	t.Cleanup(env.ResetWarning)
	f := unstartedFlock(t)
	unreadable := filepath.Join(os.Getenv("IRONFLOCK_ENV_DIR"), "APP_AUTH_SECRET.txt")
	if err := os.WriteFile(unreadable, []byte("rotated"), 0); err != nil {
		t.Fatal(err)
	}
	f.start(t)
	if n := strings.Count(f.logs.String(), "level=WARN"); n != 1 {
		t.Fatalf("%d warnings, want 1:\n%s", n, f.logs)
	}
	mustContain(t, f.logs.String(), "APP_AUTH_SECRET.txt: permission denied", "using the environment the container started with")

	// An explicit credential does not come from the mirror.
	env.ResetWarning()
	explicit := newTestFlock(t, WithCredentials("id", "secret"))
	explicit.start(t)
	if strings.Contains(explicit.logs.String(), "level=WARN") {
		t.Errorf("warned with explicit credentials:\n%s", explicit.logs)
	}
}

// Stop recognizes main's context per instance: when another IronFlock's Run
// runs inside main, Stop with the inner main's context — which the outer
// Stop cancels too — still completes its shutdown.
func TestRunNestedStopWithTheInnerMainsContext(t *testing.T) {
	checkGoroutines(t)
	outer := unstartedFlock(t)
	inner := newTestFlock(t)
	var stopErr error
	release := make(chan struct{})
	outer.own.stopFn = func(ctx context.Context) error {
		<-release // the router's GOODBYE
		return ctx.Err()
	}
	err := outer.Run(context.Background(), func(octx context.Context) error {
		return inner.Run(octx, func(ictx context.Context) error {
			go func() {
				<-ictx.Done() // the outer Stop cancels the inner main's context too
				close(release)
			}()
			stopErr = outer.Stop(ictx)
			return nil
		})
	})
	if err != nil {
		t.Fatalf("Run = %v", err)
	}
	if stopErr != nil {
		t.Errorf("outer.Stop(inner main's context) = %v, want a complete shutdown", stopErr)
	}
	if outer.own.stopCount() != 1 || inner.own.stopCount() != 1 {
		t.Errorf("stopped outer %d, inner %d times", outer.own.stopCount(), inner.own.stopCount())
	}
}

// main's context inherits the deadline of Run's context and is cancelled by
// Stop: a Stop with main's context is bounded only by the run stop timeout,
// like the Stop Run performs after main, also once that deadline has passed.
func TestRunMainStopsAfterTheRunDeadline(t *testing.T) {
	checkGoroutines(t)
	f := unstartedFlock(t)
	var stopCtxErr error
	f.own.stopFn = func(ctx context.Context) error {
		stopCtxErr = ctx.Err()
		if _, ok := ctx.Deadline(); !ok {
			return errors.New("Stop is not bounded")
		}
		return ctx.Err()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	var stopErr error
	err := f.Run(ctx, func(ctx context.Context) error {
		<-ctx.Done()
		stopErr = f.Stop(ctx)
		return nil
	})
	if err != nil {
		t.Fatalf("Run = %v", err)
	}
	if stopErr != nil || stopCtxErr != nil {
		t.Errorf("Stop(main's context after its deadline) = %v, its context %v; want a live context", stopErr, stopCtxErr)
	}

	// A deadline main sets itself does not cut the shutdown short either.
	g := unstartedFlock(t)
	var deadline time.Time
	g.own.stopFn = func(ctx context.Context) error {
		deadline, _ = ctx.Deadline()
		return ctx.Err()
	}
	err = g.Run(context.Background(), func(ctx context.Context) error {
		short, cancel := context.WithTimeout(ctx, time.Millisecond)
		defer cancel()
		<-short.Done()
		return g.Stop(short)
	})
	if err != nil {
		t.Fatalf("Run = %v", err)
	}
	if left := time.Until(deadline); left < g.runStopTimeout/2 {
		t.Errorf("Stop was bounded by main's deadline: %v left", left)
	}
}
