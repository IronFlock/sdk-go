package ironflock

import (
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ironflock/sdk-go/wamp"
)

var bg = context.Background()

// deviceMetadata is the metadata the standard test identity adds to kwargs.
func deviceMetadata() map[string]any {
	return map[string]any{
		"DEVICE_SERIAL_NUMBER": "test-serial-123",
		"DEVICE_KEY":           "12",
		"DEVICE_NAME":          "test-device",
	}
}

func withMetadata(kw map[string]any) map[string]any {
	out := deviceMetadata()
	for k, v := range kw {
		out[k] = v
	}
	return out
}

func TestPublishIsAcknowledgedAndCarriesDeviceMetadata(t *testing.T) {
	f := flock(t)
	if err := f.Publish(bg, "test.topic", 42, "two"); err != nil {
		t.Fatal(err)
	}
	got := f.own.lastPublish(t)
	want := fakePublish{
		Topic:  "test.topic",
		Args:   []any{int64(42), "two"},
		Kwargs: deviceMetadata(),
		Opts:   &wamp.PublishOptions{Acknowledge: true},
		Window: DefaultReconnectWindow, // a publish waits the reconnect window for a session
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("published %#v\nwant %#v", got, want)
	}
}

func TestPublishUserKwargsWinOverMetadata(t *testing.T) {
	f := flock(t)
	if err := f.Publish(bg, "test.topic", Kwargs{"foo": "bar", "DEVICE_KEY": "mine"}); err != nil {
		t.Fatal(err)
	}
	got := f.own.lastPublish(t)
	want := withMetadata(map[string]any{"foo": "bar", "DEVICE_KEY": "mine"})
	if !reflect.DeepEqual(got.Kwargs, want) {
		t.Errorf("kwargs %#v, want %#v", got.Kwargs, want)
	}
	if got.Args != nil {
		t.Errorf("args %#v, want none", got.Args)
	}
}

func TestPublishSendsUnknownMetadataAsNil(t *testing.T) {
	setIdentityEnv(t)
	unsetEnv(t, "DEVICE_KEY", "DEVICE_NAME")
	f := newTestFlock(t)
	f.start(t)
	if err := f.Publish(bg, "test.topic"); err != nil {
		t.Fatal(err)
	}
	want := map[string]any{"DEVICE_SERIAL_NUMBER": "test-serial-123", "DEVICE_KEY": nil, "DEVICE_NAME": nil}
	if got := f.own.lastPublish(t).Kwargs; !reflect.DeepEqual(got, want) {
		t.Errorf("kwargs %#v, want %#v", got, want)
	}
}

func TestPublishRejectsInvalidParameters(t *testing.T) {
	f := flock(t)
	cases := map[string][]any{
		"":       {1},
		"   ":    {1},
		" x":     {1},
		"x\n":    {1},
		"ok":     {func() {}},
		"ok.opt": {CallOptions{}},
	}
	for topic, args := range cases {
		err := f.Publish(bg, topic, args...)
		if !errors.Is(err, ErrInvalidArgument) {
			t.Errorf("Publish(%q): %v, want ErrInvalidArgument", topic, err)
			continue
		}
		mustContain(t, err.Error(), "Invalid publish parameters")
	}
	if n := len(f.own.allPublishes()); n != 0 {
		t.Errorf("%d invalid publications were sent", n)
	}
}

func TestPublishFailuresNameTheTopicAndCause(t *testing.T) {
	f := flock(t)
	f.own.publishFn = func(fakePublish) error { return wampErr("wamp.error.not_authorized", "denied") }
	err := f.Publish(bg, "test.topic", 1)
	var oerr *OperationError
	if !errors.As(err, &oerr) {
		t.Fatalf("err = %T %v", err, err)
	}
	if want := `Publish to topic 'test.topic' failed with WAMP error 'wamp.error.not_authorized' — ["denied"]`; err.Error() != want {
		t.Errorf("message %q\nwant %q", err.Error(), want)
	}
	if WampURI(err) != "wamp.error.not_authorized" {
		t.Errorf("WampURI = %q", WampURI(err))
	}

	f.own.publishFn = func(fakePublish) error { return errors.New("not connected") }
	err = f.Publish(bg, "test.topic", 1)
	if want := "Publish to topic 'test.topic' failed: not connected"; err == nil || err.Error() != want {
		t.Errorf("message %v, want %q", err, want)
	}

	f.own.publishFn = func(p fakePublish) error { return context.Canceled }
	if err := f.Publish(bg, "test.topic"); !errors.Is(err, context.Canceled) {
		t.Errorf("context error not kept: %v", err)
	}
}

// PublishToTable is AppendToTable: the row survives a data backend restart
// and its refusals reach the app (a publish to the table topic lost both).
func TestPublishToTableCallsTheAppendProcedure(t *testing.T) {
	f := flock(t)
	f.own.callFn = func(fakeCall) (*wamp.Result, error) { return resultOf(map[string]any{"success": true}), nil }
	if err := f.PublishToTable(bg, "sensordata", Row{"temp": 22}, CallOptions{Timeout: time.Second}); err != nil {
		t.Fatal(err)
	}
	got := f.own.lastCall(t)
	want := fakeCall{
		Procedure: "append.10.20.sensordata",
		Args:      []any{map[string]any{"temp": int64(22)}},
		Kwargs:    deviceMetadata(),
		Opts:      &CallOptions{Timeout: time.Second},
		Window:    DefaultReconnectWindow,
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("call %#v\nwant %#v", got, want)
	}

	// Kwargs-only rows and a trimmed table name.
	if err := f.PublishToTable(bg, " inspections ", Kwargs{"part_id": "1"}); err != nil {
		t.Fatal(err)
	}
	got = f.own.lastCall(t)
	if got.Procedure != "append.10.20.inspections" || !reflect.DeepEqual(got.Kwargs, withMetadata(map[string]any{"part_id": "1"})) {
		t.Errorf("call %#v", got)
	}
	if n := len(f.own.allPublishes()); n != 0 {
		t.Errorf("%d publications, want none", n)
	}

	// Its errors name the publish.
	f.own.callFn = func(fakeCall) (*wamp.Result, error) { return nil, wampErr(wamp.URINoSuchProcedure) }
	err := f.PublishToTable(bg, "sensordata", Row{})
	if want := "Publish to table 'sensordata' failed with WAMP error 'wamp.error.no_such_procedure'"; err == nil || err.Error() != want {
		t.Errorf("error %v, want %q", err, want)
	}
}

func TestTableNameValidation(t *testing.T) {
	f := flock(t)
	valid := []string{"sensordata", "error-logs", "sensor_data-2", "tëmperatur", strings.Repeat("a", 100), "-a-"}
	for _, name := range valid {
		if err := f.PublishToTable(bg, name, Row{}); err != nil {
			t.Errorf("%q: %v", name, err)
		}
	}
	invalid := []string{"", "   ", "bad name!", "a.b", "a/b", "___", "--", strings.Repeat("a", 101)}
	for _, name := range invalid {
		err := f.PublishToTable(bg, name, Row{})
		if !errors.Is(err, ErrInvalidArgument) {
			t.Errorf("%q: %v, want ErrInvalidArgument", name, err)
			continue
		}
		mustContain(t, err.Error(), "Invalid table parameters: Table name")
		if _, err := f.AppendToTable(bg, name, Row{}); !errors.Is(err, ErrInvalidArgument) {
			t.Errorf("AppendToTable(%q): %v", name, err)
		}
		if err := f.PublishRowsToTable(bg, name, []Row{{}}); !errors.Is(err, ErrInvalidArgument) {
			t.Errorf("PublishRowsToTable(%q): %v", name, err)
		}
	}
	if n := len(f.own.allCalls()); n != len(valid) {
		t.Errorf("%d calls, want %d", n, len(valid))
	}
}

func TestTableWritesNeedSwarmAndAppKey(t *testing.T) {
	for _, missing := range []string{"SWARM_KEY", "APP_KEY"} {
		t.Run(missing, func(t *testing.T) {
			setIdentityEnv(t)
			unsetEnv(t, missing)
			f := newTestFlock(t)
			want := missing + " not set in environment variables!"
			ops := map[string]func() error{
				"PublishToTable": func() error { return f.PublishToTable(bg, "t", Row{}) },
				"AppendToTable": func() error {
					_, err := f.AppendToTable(bg, "t", Row{})
					return err
				},
				"PublishRowsToTable": func() error { return f.PublishRowsToTable(bg, "t", []Row{{}}) },
				"AppendRowsToTable": func() error {
					_, err := f.AppendRowsToTable(bg, "t", []Row{{}})
					return err
				},
				"ReportError": func() error {
					_, err := f.ReportError(bg, "boom")
					return err
				},
				"SubscribeToTable": func() error {
					_, err := f.SubscribeToTable(bg, "t", func(*Event) {})
					return err
				},
			}
			for name, op := range ops {
				err := op()
				if !errors.Is(err, ErrMissingConfig) || errors.Is(err, ErrInvalidArgument) {
					t.Errorf("%s: %v, want only ErrMissingConfig", name, err)
					continue
				}
				if !strings.HasSuffix(err.Error(), want) {
					t.Errorf("%s: %q, want %q", name, err, want)
				}
			}
			if len(f.own.allPublishes())+len(f.own.allCalls())+len(f.own.allSubs()) != 0 {
				t.Error("an operation reached the connection")
			}
			// Parameters are validated first.
			if err := f.PublishToTable(bg, "bad name", Row{}); !errors.Is(err, ErrInvalidArgument) {
				t.Errorf("validation order: %v", err)
			}
		})
	}
}

func TestAppendToTableCallsTheAppendProcedure(t *testing.T) {
	f := flock(t)
	f.own.callFn = func(fakeCall) (*wamp.Result, error) { return resultOf(map[string]any{"success": true}), nil }
	res, err := f.AppendToTable(bg, "sensordata", Row{"temperature": 21.5}, Kwargs{"batch": "b1"}, CallOptions{Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(res.Value(), map[string]any{"success": true}) {
		t.Errorf("result %#v", res.Value())
	}
	got := f.own.lastCall(t)
	want := fakeCall{
		Procedure: "append.10.20.sensordata",
		Args:      []any{map[string]any{"temperature": 21.5}},
		Kwargs:    withMetadata(map[string]any{"batch": "b1"}),
		Opts:      &CallOptions{Timeout: time.Second},
		Window:    DefaultReconnectWindow,
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("call %#v\nwant %#v", got, want)
	}

	f.own.callFn = func(fakeCall) (*wamp.Result, error) { return nil, wampErr(wamp.URINoSuchProcedure) }
	_, err = f.AppendToTable(bg, "sensordata", Row{})
	if want := "Append to table 'sensordata' failed with WAMP error 'wamp.error.no_such_procedure'"; err == nil || err.Error() != want {
		t.Errorf("error %v, want %q", err, want)
	}
}

// The table operations use the reconnect window as their retry window, and
// Publish, Subscribe, SubscribeToTable and RegisterDeviceFunction as their
// wait for a session; the other operations wait the default and are not
// retried.
func TestWhichOperationsUseTheReconnectWindow(t *testing.T) {
	for _, window := range []time.Duration{DefaultReconnectWindow, 0, 5 * time.Second} {
		f := flock(t, WithReconnectWindow(window))
		f.own.callFn = func(fakeCall) (*wamp.Result, error) { return &wamp.Result{}, nil }
		_ = f.PublishToTable(bg, "t", Row{})
		_ = f.PublishRowsToTable(bg, "t", []Row{{}})
		_, _ = f.AppendToTable(bg, "t", Row{})
		_, _ = f.AppendRowsToTable(bg, "t", []Row{{}})
		_, _ = f.ReportError(bg, "x")
		_, _ = f.ReportError(bg, "x", ReportErrorOptions{Append: true})
		_, _ = f.GetHistory(bg, "t", nil)
		_, _ = f.GetSeriesHistory(bg, "t", SeriesQueryParams{Metrics: []SeriesMetric{{"v", MethodAvg}}, Limit: 1,
			TimeRange: &TimeRange{Start: int64(0)}})
		_, _ = f.RevealSecrets(bg, "t", nil)
		_, _ = f.VerifySecret(bg, "t", "c", "x", nil)
		if n := len(f.own.allPublishes()); n != 0 {
			t.Errorf("%d table operations published", n)
		}
		for _, c := range f.own.allCalls() {
			if c.Window != window {
				t.Errorf("call %s: window %v, want %v", c.Procedure, c.Window, window)
			}
		}

		// These wait as long for a session, and are not retried.
		_ = f.Publish(bg, "plain.topic")
		_, _ = f.Subscribe(bg, "plain.topic", func(*Event) {})
		_, _ = f.SubscribeToTable(bg, "t", func(*Event) {})
		_, _ = f.RegisterDeviceFunction(bg, "fn", func(context.Context, *Invocation) (any, error) { return nil, nil })
		if w := f.own.lastPublish(t).Window; w != window {
			t.Errorf("Publish window %v, want %v", w, window)
		}
		for _, sub := range f.own.allSubs() {
			if sub.Window != window {
				t.Errorf("subscription of %s: window %v, want %v", sub.Topic, sub.Window, window)
			}
		}
		for _, reg := range f.own.allRegs() {
			if reg.Window != window {
				t.Errorf("registration of %s: window %v, want %v", reg.Procedure, reg.Window, window)
			}
		}

		// The others never ride out a restart.
		_, _ = f.Call(bg, "plain.proc")
		_, _ = f.CallDeviceFunction(bg, 42, "fn")
		_, _ = f.SetDeviceLocation(bg, 1, 2)
		for _, procedure := range []string{"plain.proc", "10.42.20.DEV.fn", uriLocationUpdate} {
			calls := f.own.callsTo(procedure)
			if len(calls) != 1 || calls[0].Window != 0 {
				t.Errorf("calls of %s: %#v, want one without a window", procedure, calls)
			}
		}
	}
}

// waitingOp is an operation that waits for a session within the reconnect
// window: sent reports whether it reached the connection, once.
type waitingOp struct {
	name string
	do   func(f *testFlock) error
	sent func(c *fakeConn) bool
}

// windowedOps are the operations that wait within the reconnect window
// without being retried.
func windowedOps() []waitingOp {
	noop := func(context.Context, *Invocation) (any, error) { return nil, nil }
	return []waitingOp{
		{"Publish", func(f *testFlock) error { return f.Publish(bg, "com.example.status", 1) },
			func(c *fakeConn) bool { return len(c.allPublishes()) == 1 }},
		{"Subscribe", func(f *testFlock) error {
			_, err := f.Subscribe(bg, "com.example.status", func(*Event) {})
			return err
		}, func(c *fakeConn) bool { return len(c.allSubs()) == 1 }},
		{"SubscribeToTable", func(f *testFlock) error {
			_, err := f.SubscribeToTable(bg, "sensordata", func(*Event) {})
			return err
		}, func(c *fakeConn) bool { return len(c.allSubs()) == 2 }},
		{"RegisterDeviceFunction", func(f *testFlock) error {
			_, err := f.RegisterDeviceFunction(bg, "toggle", noop)
			return err
		}, func(c *fakeConn) bool { return len(c.allRegs()) == 1 }},
	}
}

// tablePublishes are the table writes that used to publish.
func tablePublishes() []waitingOp {
	appended := func(procedure string) func(c *fakeConn) bool {
		return func(c *fakeConn) bool { return len(c.callsTo(procedure)) == 1 && len(c.allPublishes()) == 0 }
	}
	return []waitingOp{
		{"PublishToTable", func(f *testFlock) error { return f.PublishToTable(bg, "sensordata", Row{"tsp": int64(1)}) },
			appended("append.10.20.sensordata")},
		{"PublishRowsToTable", func(f *testFlock) error {
			return f.PublishRowsToTable(bg, "sensordata", []Row{{"tsp": int64(1)}})
		}, appended("appendBulk.10.20.sensordata")},
		{"ReportError", func(f *testFlock) error {
			_, err := f.ReportError(bg, "boom")
			return err
		}, appended("append.10.20.error-logs")},
	}
}

// setSessionWait sets the fake's default wait for a session.
func setSessionWait(c *fakeConn, d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sessionWait = d
}

// Publish, Subscribe, SubscribeToTable and RegisterDeviceFunction wait for a
// session for the whole reconnect window, not just the default wait, and go
// out once the session is back. Without a window they wait the default.
func TestPublishSubscribeAndRegisterWaitForTheWholeWindow(t *testing.T) {
	for _, op := range windowedOps() {
		t.Run(op.name, func(t *testing.T) {
			f := flock(t, WithReconnectWindow(5*time.Second))
			setSessionWait(f.own, 10*time.Millisecond) // the default wait alone would give up
			f.own.drop()
			done := make(chan error, 1)
			go func() { done <- op.do(f) }()
			time.Sleep(60 * time.Millisecond)
			f.own.reconnect()
			if err := recv(t, done, op.name); err != nil {
				t.Fatalf("%s across a 60ms outage with a 5s window: %v", op.name, err)
			}
			if !op.sent(f.own) {
				t.Errorf("%s did not reach the connection once", op.name)
			}

			g := flock(t, WithReconnectWindow(0))
			const wait = 30 * time.Millisecond
			setSessionWait(g.own, wait)
			g.own.drop()
			began := time.Now()
			err := op.do(g)
			if !errors.Is(err, wamp.ErrNotConnected) {
				t.Errorf("%s without a window while down = %v, want ErrNotConnected", op.name, err)
			}
			if took := time.Since(began); took < wait {
				t.Errorf("%s without a window gave up after %v, before the default wait %v", op.name, took, wait)
			}
		})
	}
}

// Without a reconnect window a table publish waits the default wait for a
// session, and the connection gets no retry window: it is not retried
// (TestWithoutAReconnectWindowTableCallsAreNotRetried shows that with a real
// connection). Once the default wait has passed, it gives up.
func TestWithoutAWindowATablePublishWaitsTheDefaultWait(t *testing.T) {
	for _, op := range tablePublishes() {
		t.Run(op.name, func(t *testing.T) {
			f := flock(t, WithReconnectWindow(0))
			setSessionWait(f.own, time.Second) // stands in for the 10s
			f.own.drop()
			done := make(chan error, 1)
			go func() { done <- op.do(f) }()
			time.Sleep(60 * time.Millisecond)
			f.own.reconnect()
			if err := recv(t, done, op.name); err != nil {
				t.Fatalf("%s across a 60ms outage: %v", op.name, err)
			}
			if !op.sent(f.own) {
				t.Fatalf("%s: calls %#v, publications %#v", op.name, f.own.allCalls(), f.own.allPublishes())
			}
			if w := f.own.lastCall(t).Window; w != 0 {
				t.Errorf("%s retries for %v without a window", op.name, w)
			}

			const wait = 30 * time.Millisecond
			setSessionWait(f.own, wait)
			f.own.drop()
			began := time.Now()
			if err := op.do(f); !errors.Is(err, wamp.ErrNotConnected) {
				t.Errorf("%s while down = %v, want ErrNotConnected", op.name, err)
			}
			if took := time.Since(began); took < wait {
				t.Errorf("%s gave up after %v, before the default wait %v", op.name, took, wait)
			}
		})
	}
}

type sensorReading struct {
	Temperature float64   `json:"temperature"`
	At          time.Time `json:"tsp"`
}

func TestBulkWritesSendTheWholeBatchAsOneArgument(t *testing.T) {
	f := flock(t)
	f.own.callFn = func(fakeCall) (*wamp.Result, error) {
		return resultOf(map[string]any{"success": true, "count": int64(2)}), nil
	}
	rows := []Row{{"temp": 22}, {"temp": 23}}
	wantRows := []any{map[string]any{"temp": int64(22)}, map[string]any{"temp": int64(23)}}

	// PublishRowsToTable is AppendRowsToTable: the batch survives a data
	// backend restart and its refusals reach the app (a publish to bulk.*
	// lost both).
	if err := f.PublishRowsToTable(bg, "sensordata", rows, Kwargs{"source": "plc"}); err != nil {
		t.Fatal(err)
	}
	published := f.own.lastCall(t)
	wantPublished := fakeCall{
		Procedure: "appendBulk.10.20.sensordata",
		Args:      []any{wantRows},
		Kwargs:    withMetadata(map[string]any{"source": "plc"}),
		Window:    DefaultReconnectWindow,
	}
	if !reflect.DeepEqual(published, wantPublished) {
		t.Errorf("call %#v\nwant %#v", published, wantPublished)
	}
	if n := len(f.own.allPublishes()); n != 0 {
		t.Errorf("%d publications, want none", n)
	}

	res, err := f.AppendRowsToTable(bg, "sensordata", rows)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(res.Value(), map[string]any{"success": true, "count": int64(2)}) {
		t.Errorf("result %#v", res.Value())
	}
	call := f.own.lastCall(t)
	wantCall := fakeCall{
		Procedure: "appendBulk.10.20.sensordata",
		Args:      []any{wantRows},
		Kwargs:    deviceMetadata(),
		Window:    DefaultReconnectWindow,
	}
	if !reflect.DeepEqual(call, wantCall) {
		t.Errorf("call %#v\nwant %#v", call, wantCall)
	}

	// Structs are encoded through their json tags.
	at := time.Date(2026, 1, 15, 10, 30, 0, 0, time.UTC)
	if err := f.PublishRowsToTable(bg, "sensordata", []sensorReading{{Temperature: 22.5, At: at}}); err != nil {
		t.Fatal(err)
	}
	want := []any{[]any{map[string]any{"temperature": 22.5, "tsp": "2026-01-15T10:30:00Z"}}}
	if got := f.own.lastCall(t).Args; !reflect.DeepEqual(got, want) {
		t.Errorf("struct rows %#v", got)
	}

	f.own.callFn = func(fakeCall) (*wamp.Result, error) {
		return nil, wampErr("wamp.error.invalid_argument", "bad row")
	}
	_, err = f.AppendRowsToTable(bg, "sensordata", rows)
	if want := `Bulk append of 2 row(s) to table 'sensordata' failed with WAMP error 'wamp.error.invalid_argument' — ["bad row"]`; err == nil || err.Error() != want {
		t.Errorf("error %v\nwant %q", err, want)
	}
	err = f.PublishRowsToTable(bg, "sensordata", rows)
	if want := `Bulk publish of 2 row(s) to table 'sensordata' failed with WAMP error 'wamp.error.invalid_argument' — ["bad row"]`; err == nil || err.Error() != want {
		t.Errorf("error %v\nwant %q", err, want)
	}
}

func TestBulkWritesRejectInvalidBatches(t *testing.T) {
	f := flock(t)
	for _, rows := range []any{nil, []Row{}, []int{1}, "x", Row{"a": 1}, []any{Row{"a": 1}, "not-a-row"}} {
		err := f.PublishRowsToTable(bg, "sensordata", rows)
		if !errors.Is(err, ErrInvalidArgument) {
			t.Errorf("PublishRowsToTable(%#v): %v", rows, err)
			continue
		}
		mustContain(t, err.Error(), "Invalid bulk table parameters")
		if _, err := f.AppendRowsToTable(bg, "sensordata", rows); !errors.Is(err, ErrInvalidArgument) {
			t.Errorf("AppendRowsToTable(%#v): %v", rows, err)
		}
	}
	if len(f.own.allPublishes())+len(f.own.allCalls()) != 0 {
		t.Error("an invalid batch reached the connection")
	}
}

type tracedError struct{ msg string }

func (e tracedError) Error() string { return e.msg }

// Format adds a stack trace for %+v, like github.com/pkg/errors does.
func (e tracedError) Format(s fmt.State, verb rune) {
	// A Formatter has no way to report a failed write.
	if verb == 'v' && s.Flag('+') {
		_, _ = fmt.Fprintf(s, "%s\nmain.main\n\t/app/main.go:12", e.msg)
		return
	}
	_, _ = fmt.Fprint(s, e.msg)
}

type stringer struct{}

func (stringer) String() string { return "from String()" }

func TestReportErrorAppendsAnAppTaggedRow(t *testing.T) {
	f := flock(t)
	f.own.callFn = func(fakeCall) (*wamp.Result, error) { return resultOf(map[string]any{"success": true}), nil }
	res, err := f.ReportError(bg, "boom", ReportErrorOptions{Level: LevelWarning, Tsp: "2026-01-01T00:00:00+00:00"})
	if err != nil {
		t.Fatal(err)
	}
	if res == nil || !reflect.DeepEqual(res.Value(), map[string]any{"success": true}) {
		t.Errorf("result %#v", res)
	}
	got := f.own.lastCall(t)
	if got.Procedure != "append.10.20.error-logs" || got.Window != DefaultReconnectWindow {
		t.Errorf("called %s (window %v)", got.Procedure, got.Window)
	}
	wantRow := map[string]any{
		"tsp":          "2026-01-01T00:00:00+00:00",
		"msg":          "boom",
		"user_message": "boom", // defaults to msg
		"source":       "app",
		"level":        "warning",
	}
	if !reflect.DeepEqual(got.Args, []any{wantRow}) {
		t.Errorf("row %#v\nwant %#v", got.Args, wantRow)
	}
	if !reflect.DeepEqual(got.Kwargs, deviceMetadata()) {
		t.Errorf("kwargs %#v", got.Kwargs)
	}
	if len(f.own.allPublishes()) != 0 {
		t.Error("nothing must be published")
	}
}

// The board's toast knows success, error, info and warning: a row with "warn"
// showed as info.
func TestReportErrorSendsTheOldWarnSpellingAsWarning(t *testing.T) {
	for _, level := range []ErrorLevel{LevelWarn, "warn"} {
		f := flock(t)
		if _, err := f.ReportError(bg, "boom", ReportErrorOptions{Level: level}); err != nil {
			t.Fatal(err)
		}
		row := f.own.lastCall(t).Args[0].(map[string]any)
		if row["level"] != "warning" {
			t.Errorf("level %q went out as %q", level, row["level"])
		}
	}
}

func TestReportErrorAppendUsesTheAppendProcedure(t *testing.T) {
	f := flock(t)
	f.own.callFn = func(fakeCall) (*wamp.Result, error) { return resultOf(map[string]any{"success": true}), nil }
	res, err := f.ReportError(bg, "kaboom", ReportErrorOptions{Append: true, Tsp: "2026-01-01T00:00:00.000Z"})
	if err != nil {
		t.Fatal(err)
	}
	if res == nil || !reflect.DeepEqual(res.Value(), map[string]any{"success": true}) {
		t.Errorf("result %#v", res)
	}
	call := f.own.lastCall(t)
	wantRow := map[string]any{
		"tsp": "2026-01-01T00:00:00.000Z", "msg": "kaboom", "user_message": "kaboom", "source": "app", "level": "error",
	}
	if call.Procedure != "append.10.20.error-logs" || !reflect.DeepEqual(call.Args, []any{wantRow}) {
		t.Errorf("call %#v", call)
	}
	if len(f.own.allPublishes()) != 0 {
		t.Error("nothing must be published")
	}

	// Append only picks the operation the error names: both modes append.
	f.own.callFn = func(fakeCall) (*wamp.Result, error) { return nil, wampErr(wamp.URIRuntimeError, map[string]any{}) }
	for _, mode := range []struct {
		append bool
		want   string
	}{{true, "Append to table 'error-logs' failed"}, {false, "Publish to table 'error-logs' failed"}} {
		_, err := f.ReportError(bg, "kaboom", ReportErrorOptions{Append: mode.append})
		if err == nil || !strings.HasPrefix(err.Error(), mode.want) || !strings.Contains(err.Error(), "missing tsp") {
			t.Errorf("Append %v: %v, want %q with the tsp hint", mode.append, err, mode.want)
		}
	}
	if n := len(f.own.callsTo("append.10.20.error-logs")); n != 3 {
		t.Errorf("%d appends, want 3", n)
	}
}

func TestReportErrorMessages(t *testing.T) {
	f := flock(t)
	row := func(errOrMsg any, opts ...ReportErrorOptions) map[string]any {
		t.Helper()
		if _, err := f.ReportError(bg, errOrMsg, opts...); err != nil {
			t.Fatal(err)
		}
		return f.own.lastCall(t).Args[0].(map[string]any)
	}

	if r := row(errors.New("explode")); r["msg"] != "explode" || r["user_message"] != "explode" {
		t.Errorf("error row %#v", r)
	}
	r := row(tracedError{"pump failed"})
	if r["msg"] != "pump failed\nmain.main\n\t/app/main.go:12" {
		t.Errorf("%%+v not used: %q", r["msg"])
	}
	if got := row(fmt.Errorf("wrapped: %w", tracedError{"inner"}))["msg"]; got != "wrapped: inner" {
		t.Errorf("wrapped error msg %q", got)
	}
	if got := row(stringer{})["msg"]; got != "from String()" {
		t.Errorf("Stringer msg %q", got)
	}
	if got := row(42)["msg"]; got != "42" {
		t.Errorf("value msg %q", got)
	}

	r = row("ConnectionError: [Errno 111] refused", ReportErrorOptions{UserMessage: "Pump 1 is not responding"})
	if r["msg"] != "ConnectionError: [Errno 111] refused" || r["user_message"] != "Pump 1 is not responding" {
		t.Errorf("user message row %#v", r)
	}

	// The default timestamp is now, in UTC, with milliseconds.
	before := time.Now().Add(-time.Second)
	tsp := row("x")["tsp"].(string)
	parsed, err := time.Parse("2006-01-02T15:04:05.000Z07:00", tsp)
	if err != nil || !strings.HasSuffix(tsp, "Z") || parsed.Before(before) || parsed.After(time.Now().Add(time.Second)) {
		t.Errorf("default tsp %q (%v)", tsp, err)
	}

	if _, err := f.ReportError(bg, nil); !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("nil: %v", err)
	}
}

func TestSubscribeToTableSubscribesBothFeeds(t *testing.T) {
	f := flock(t)
	var mu sync.Mutex
	var got []*Event
	handler := func(ev *Event) {
		mu.Lock()
		defer mu.Unlock()
		got = append(got, ev)
	}
	opts := SubscribeOptions{Extra: map[string]any{"custom_option": true}}
	ts, err := f.SubscribeToTable(bg, "sensordata", handler, opts)
	if err != nil {
		t.Fatal(err)
	}
	subs := f.own.allSubs()
	if len(subs) != 2 || subs[0].Topic != "transformed.sensordata" || subs[1].Topic != "transformed.bulk.sensordata" {
		t.Fatalf("subscriptions %#v", subs)
	}
	// Both feeds deliver through one DeliveryGroup, with the caller's
	// options otherwise; the caller's options are left alone.
	for _, s := range subs {
		if s.Opts == nil || !reflect.DeepEqual(s.Opts.Extra, opts.Extra) || s.Opts.Match != opts.Match {
			t.Errorf("%s: options %#v", s.Topic, s.Opts)
		}
	}
	if subs[0].Opts.Group == nil || subs[0].Opts.Group != subs[1].Opts.Group {
		t.Errorf("delivery groups %p and %p, want one", subs[0].Opts.Group, subs[1].Opts.Group)
	}
	if opts.Group != nil {
		t.Error("the caller's options were changed")
	}
	if ts.Rows != subs[0].Sub || ts.Bulk != subs[1].Sub {
		t.Error("TableSubscription does not hold both subscriptions")
	}

	// A single-row event reaches the handler as it is.
	single := &Event{Topic: "transformed.sensordata", Args: []any{map[string]any{"temp": int64(1)}}}
	f.own.fire("transformed.sensordata", single)

	// The data backend republishes a bulk insert as one event carrying the batch.
	f.own.fire("transformed.bulk.sensordata", &Event{
		Topic:   "transformed.bulk.sensordata",
		Args:    []any{[]any{map[string]any{"temp": int64(2)}, nil, map[string]any{"temp": int64(3)}}},
		Kwargs:  map[string]any{"numSub": int64(1)},
		Details: map[string]any{"topic": "x"},
	})
	f.own.fire("transformed.bulk.sensordata", &Event{Args: []any{nil}})                              // no rows
	f.own.fire("transformed.bulk.sensordata", &Event{})                                              // no args
	f.own.fire("transformed.bulk.sensordata", &Event{Args: []any{map[string]any{"temp": int64(4)}}}) // one row, not a list

	mu.Lock()
	defer mu.Unlock()
	if len(got) != 4 {
		t.Fatalf("%d events, want 4: %#v", len(got), got)
	}
	if got[0] != single {
		t.Error("a single-row event must be delivered unchanged")
	}
	for i, want := range []int64{2, 3} {
		ev := got[i+1]
		if !reflect.DeepEqual(ev.Args, []any{map[string]any{"temp": want}}) ||
			ev.Topic != "transformed.bulk.sensordata" ||
			!reflect.DeepEqual(ev.Kwargs, map[string]any{"numSub": int64(1)}) ||
			!reflect.DeepEqual(ev.Details, map[string]any{"topic": "x"}) {
			t.Errorf("bulk row %d: %#v", i, ev)
		}
		if ev.Row()["temp"] != want {
			t.Errorf("Event.Row() = %#v", ev.Row())
		}
	}
	if !reflect.DeepEqual(got[3].Args, []any{map[string]any{"temp": int64(4)}}) {
		t.Errorf("non-list bulk payload %#v", got[3].Args)
	}

	if err := ts.Unsubscribe(bg); err != nil {
		t.Fatal(err)
	}
	for _, s := range f.own.allSubs() {
		if !s.Removed {
			t.Errorf("%s still subscribed", s.Topic)
		}
	}
}

func TestBulkHandlerPanicDoesNotCostTheRestOfTheBatch(t *testing.T) {
	f := flock(t)
	var delivered []any
	_, err := f.SubscribeToTable(bg, "sensordata", func(ev *Event) {
		delivered = append(delivered, ev.Row()["n"])
		if ev.Row()["n"] == int64(1) {
			panic("handler bug")
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	f.own.fire("transformed.bulk.sensordata", &Event{Topic: "transformed.bulk.sensordata", Args: []any{[]any{
		map[string]any{"n": int64(1)}, map[string]any{"n": int64(2)},
	}}})
	if !reflect.DeepEqual(delivered, []any{int64(1), int64(2)}) {
		t.Errorf("delivered %#v", delivered)
	}
	mustContain(t, f.logs.String(), "panicked", "handler bug")
}

func TestSubscribeToTableRemovesTheFirstSubscriptionWhenTheSecondFails(t *testing.T) {
	f := flock(t)
	f.own.subscribeFn = func(topic string) error {
		if strings.HasPrefix(topic, "transformed.bulk.") {
			return wampErr("wamp.error.not_authorized")
		}
		return nil
	}
	_, err := f.SubscribeToTable(bg, "sensordata", func(*Event) {})
	if want := "Subscription to topic 'transformed.bulk.sensordata' failed with WAMP error 'wamp.error.not_authorized'"; err == nil || err.Error() != want {
		t.Fatalf("error %v, want %q", err, want)
	}
	subs := f.own.allSubs()
	if len(subs) != 1 || !subs[0].Removed {
		t.Errorf("subscriptions %#v: the row feed must be removed again", subs)
	}
}

func TestSubscribeToTableValidation(t *testing.T) {
	f := flock(t)
	for _, table := range []string{"", " ", " x"} {
		if _, err := f.SubscribeToTable(bg, table, func(*Event) {}); !errors.Is(err, ErrInvalidArgument) {
			t.Errorf("%q: %v", table, err)
		}
	}
	if _, err := f.SubscribeToTable(bg, "t", nil); !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("nil handler: %v", err)
	}
	if len(f.own.allSubs()) != 0 {
		t.Error("an invalid subscription reached the connection")
	}
	var ts *TableSubscription
	if err := ts.Unsubscribe(bg); err != nil {
		t.Errorf("nil TableSubscription: %v", err)
	}
}

// IronFlock.Unsubscribe takes every subscription handle and does what the
// handle's own Unsubscribe does: it removes a Subscription, and both feeds of
// a TableSubscription — of the own app on the own connection, of a consumed
// app on the consumed app's — and stops their catch-up. A nil handle of
// either type is a no-op, as is a second call. Table names the table.
func TestUnsubscribeTakesEveryHandle(t *testing.T) {
	c := newConsumer(t)
	sub, err := c.Subscribe(bg, "com.example", func(*Event) {})
	if err != nil {
		t.Fatal(err)
	}
	own, err := c.SubscribeToTable(bg, "sensordata", func(*Event) {})
	if err != nil {
		t.Fatal(err)
	}
	app, err := c.ConnectToApp(bg, "weatherstation")
	if err != nil {
		t.Fatal(err)
	}
	consumed, err := app.SubscribeToTable(bg, "readings", func(*Event) {})
	if err != nil {
		t.Fatal(err)
	}
	conn := c.factory.all()[0]
	if own.Table() != "sensordata" || consumed.Table() != "readings" {
		t.Errorf("Table() = %q and %q, want the subscribed tables", own.Table(), consumed.Table())
	}
	removed := func(c *fakeConn) (n int) {
		for _, s := range c.allSubs() {
			if s.Removed {
				n++
			}
		}
		return n
	}

	for _, h := range []Unsubscriber{sub, own, consumed} {
		if err := c.Unsubscribe(bg, h); err != nil {
			t.Errorf("Unsubscribe(%T): %v", h, err)
		}
	}
	if n, all := removed(c.own), len(c.own.allSubs()); n != 3 || all != 3 {
		t.Errorf("%d of the own connection's %d subscriptions removed, want the Subscription and both feeds", n, all)
	}
	if n, all := removed(conn), len(conn.allSubs()); n != 2 || all != 2 {
		t.Errorf("%d of the consumed connection's %d subscriptions removed, want both feeds", n, all)
	}
	if !own.follower.stopped.Load() || !consumed.follower.stopped.Load() {
		t.Error("a table subscription still catches up after Unsubscribe")
	}
	if n, m := c.own.joins.Hooks(), conn.joins.Hooks(); n != 0 || m != 0 {
		t.Errorf("%d and %d join hooks left", n, m)
	}

	// A second call is a no-op, and so is a nil handle.
	for _, h := range []Unsubscriber{sub, own, consumed, nil, (*Subscription)(nil), (*TableSubscription)(nil)} {
		if err := c.Unsubscribe(bg, h); err != nil {
			t.Errorf("Unsubscribe(%T %v): %v", h, h, err)
		}
	}
	var none *TableSubscription
	if none.Table() != "" || none.Active() {
		t.Errorf("a nil TableSubscription: Table() %q, Active() %v", none.Table(), none.Active())
	}

	// A refusal is an operation error, the removal stands.
	c.own.mu.Lock()
	c.own.unsubscribeErr = wampErr("wamp.error.not_authorized")
	c.own.mu.Unlock()
	// (The fake's subscriptions have no topic for the message to name.)
	const refused = "failed with WAMP error 'wamp.error.not_authorized'"
	sub, _ = c.Subscribe(bg, "com.refused", func(*Event) {})
	err = c.Unsubscribe(bg, sub)
	var oerr *OperationError
	if !errors.As(err, &oerr) || WampURI(err) != "wamp.error.not_authorized" ||
		!strings.HasPrefix(err.Error(), "Unsubscribe from topic '") || !strings.HasSuffix(err.Error(), refused) {
		t.Errorf("refused Unsubscribe(*Subscription) = %v", err)
	}
	own, _ = c.SubscribeToTable(bg, "refused", func(*Event) {})
	err = c.Unsubscribe(bg, own)
	if !errors.As(err, &oerr) || WampURI(err) != "wamp.error.not_authorized" || strings.Count(fmt.Sprint(err), refused) != 2 {
		t.Errorf("refused Unsubscribe(*TableSubscription) = %v, want the refusals of both feeds", err)
	}
	if n, all := removed(c.own), len(c.own.allSubs()); n != all {
		t.Errorf("%d of %d subscriptions removed: a refusal undid the removal", n, all)
	}
}

func TestSubscribe(t *testing.T) {
	f := flock(t)
	opts := SubscribeOptions{Match: "prefix"}
	sub, err := f.Subscribe(bg, "com.example", func(*Event) {}, opts)
	if err != nil {
		t.Fatal(err)
	}
	subs := f.own.allSubs()
	if len(subs) != 1 || subs[0].Topic != "com.example" || !reflect.DeepEqual(subs[0].Opts, &opts) || subs[0].Sub != sub {
		t.Errorf("subscriptions %#v", subs)
	}
	if _, err := f.Subscribe(bg, "com.other", func(*Event) {}); err != nil {
		t.Fatal(err)
	}
	if f.own.allSubs()[1].Opts != nil {
		t.Error("no options must be sent as nil")
	}
	if err := f.Unsubscribe(bg, sub); err != nil || !f.own.allSubs()[0].Removed {
		t.Errorf("Unsubscribe: %v", err)
	}
	if err := f.Unsubscribe(bg, nil); err != nil {
		t.Errorf("Unsubscribe(nil): %v", err)
	}

	f.own.subscribeFn = func(string) error { return wampErr("wamp.error.not_authorized") }
	_, err = f.Subscribe(bg, "com.denied", func(*Event) {})
	if want := "Subscription to topic 'com.denied' failed with WAMP error 'wamp.error.not_authorized'"; err == nil || err.Error() != want {
		t.Errorf("error %v", err)
	}
	for _, topic := range []string{"", " t"} {
		if _, err := f.Subscribe(bg, topic, func(*Event) {}); !errors.Is(err, ErrInvalidArgument) {
			t.Errorf("%q: %v", topic, err)
		}
	}
	if _, err := f.Subscribe(bg, "t", nil); !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("nil handler: %v", err)
	}
}

// A DeliveryGroup the caller passes is the one both feeds deliver through.
func TestSubscribeToTableUsesTheCallersDeliveryGroup(t *testing.T) {
	f := flock(t)
	group := wamp.NewDeliveryGroup()
	if _, err := f.SubscribeToTable(bg, "sensordata", func(*Event) {}, SubscribeOptions{Group: group}); err != nil {
		t.Fatal(err)
	}
	for _, s := range f.own.allSubs() {
		if s.Opts.Group != group {
			t.Errorf("%s delivers through %p, want the caller's %p", s.Topic, s.Opts.Group, group)
		}
	}
}

// Once Unsubscribe has begun, the handler is called no more: not for the
// rest of a bulk event being delivered, nor for an event that was queued
// before the subscriptions were removed.
func TestTableSubscriptionDeliversNothingOnceUnsubscribed(t *testing.T) {
	f := flock(t)
	var ts *TableSubscription
	var got []any
	ts, err := f.SubscribeToTable(bg, "sensordata", func(ev *Event) {
		got = append(got, ev.Row()["n"])
		if len(got) == 1 {
			if err := ts.Unsubscribe(bg); err != nil {
				t.Error(err)
			}
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	subs := f.own.allSubs() // the handlers, as the connection holds them
	subs[1].Handler(&Event{Args: []any{[]any{map[string]any{"n": int64(1)}, map[string]any{"n": int64(2)}}}})
	subs[0].Handler(&Event{Args: []any{map[string]any{"n": int64(3)}}})
	subs[1].Handler(&Event{Args: []any{[]any{map[string]any{"n": int64(4)}}}})
	if !reflect.DeepEqual(got, []any{int64(1)}) {
		t.Errorf("delivered %v, want only the row before Unsubscribe", got)
	}
}

// NaN and ±Inf — say, a failed sensor read — are sent alike whether the
// row is a Row or a struct, as the Python and JavaScript SDKs send them
// (msgpack float64).
func TestPublishToTableSendsNonFiniteFloatsOfRowsAndStructsAlike(t *testing.T) {
	type reading struct {
		Temperature float64 `json:"temperature"`
	}
	f := flock(t)
	for _, x := range []float64{math.NaN(), math.Inf(1), math.Inf(-1)} {
		if err := f.PublishToTable(bg, "sensordata", Row{"temperature": x}); err != nil {
			t.Fatalf("Row with %v: %v", x, err)
		}
		if err := f.PublishToTable(bg, "sensordata", reading{Temperature: x}); err != nil {
			t.Fatalf("struct with %v: %v", x, err)
		}
		calls := f.own.allCalls()
		fromRow, fromStruct := calls[len(calls)-2], calls[len(calls)-1]
		for _, c := range []fakeCall{fromRow, fromStruct} {
			row, ok := c.Args[0].(map[string]any)
			if !ok || len(row) != 1 {
				t.Fatalf("%v: sent %#v", x, c.Args)
			}
			if got, ok := row["temperature"].(float64); !ok || !(got == x || math.IsNaN(got) && math.IsNaN(x)) {
				t.Errorf("%v: sent %#v", x, row["temperature"])
			}
		}
		if !reflect.DeepEqual(fromRow.Kwargs, fromStruct.Kwargs) || fromRow.Procedure != fromStruct.Procedure {
			t.Errorf("%v: %#v and %#v", x, fromRow, fromStruct)
		}
	}
}

// fleetdb refuses a row it cannot store — a row without tsp, above all — with
// a plain JavaScript Error on an append, which reaches the caller as
// wamp.error.runtime_error without a reason (args [{}]): the error says what
// to check.
func TestAppendRefusedWithoutAReasonHintsAtTsp(t *testing.T) {
	f := flock(t)
	for _, args := range [][]any{{map[string]any{}}, {}, nil} {
		f.own.callFn = func(fakeCall) (*wamp.Result, error) { return nil, wampErr(wamp.URIRuntimeError, args...) }
		detail := ""
		if len(args) > 0 {
			detail = " — [{}]"
		}
		_, err := f.AppendToTable(bg, "sensordata", Row{"temperature": 23})
		want := "Append to table 'sensordata' failed with WAMP error 'wamp.error.runtime_error'" + detail +
			": the data backend refused the row without a reason — a missing tsp is the usual cause; see the error-logs table"
		if err == nil || err.Error() != want {
			t.Errorf("args %#v: %v\nwant %s", args, err, want)
		}
		if WampURI(err) != wamp.URIRuntimeError {
			t.Errorf("the WAMP error is not reachable: %v", err)
		}
		_, err = f.AppendRowsToTable(bg, "sensordata", []Row{{"temperature": 1}, {"temperature": 2}})
		want = "Bulk append of 2 row(s) to table 'sensordata' failed with WAMP error 'wamp.error.runtime_error'" + detail +
			": the data backend refused the rows without a reason — a row without tsp is the usual cause; see the error-logs table"
		if err == nil || err.Error() != want {
			t.Errorf("bulk, args %#v: %v\nwant %s", args, err, want)
		}
		// The table publishes are appends: they are refused alike, and their
		// errors name the publish.
		err = f.PublishToTable(bg, "sensordata", Row{"temperature": 23})
		want = "Publish to table 'sensordata' failed with WAMP error 'wamp.error.runtime_error'" + detail +
			": the data backend refused the row without a reason — a missing tsp is the usual cause; see the error-logs table"
		if err == nil || err.Error() != want {
			t.Errorf("publish, args %#v: %v\nwant %s", args, err, want)
		}
		err = f.PublishRowsToTable(bg, "sensordata", []Row{{"temperature": 1}})
		want = "Bulk publish of 1 row(s) to table 'sensordata' failed with WAMP error 'wamp.error.runtime_error'" + detail +
			": the data backend refused the rows without a reason — a row without tsp is the usual cause; see the error-logs table"
		if err == nil || err.Error() != want {
			t.Errorf("bulk publish, args %#v: %v\nwant %s", args, err, want)
		}
	}
	// Refusals that give a reason, or another URI, are shown as they are.
	for _, werr := range []*WampError{
		wampErr(wamp.URIRuntimeError, "boom"),
		wampErr(wamp.URIRuntimeError, map[string]any{"name": "error", "code": "23505"}),
		{URI: wamp.URIRuntimeError, Kwargs: map[string]any{"why": "x"}},
		wampErr("sys.dataservice.error.storage_full", map[string]any{}),
	} {
		f.own.callFn = func(fakeCall) (*wamp.Result, error) { return nil, werr }
		_, err := f.AppendToTable(bg, "sensordata", Row{"tsp": "2026-01-01T00:00:00Z"})
		if err == nil || strings.Contains(err.Error(), "tsp") {
			t.Errorf("%#v: %v", werr, err)
		}
	}
}
