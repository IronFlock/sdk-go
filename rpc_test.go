package ironflock

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ironflock/sdk-go/wamp"
)

func TestCallSendsArgumentsWithoutMetadata(t *testing.T) {
	f := flock(t)
	f.own.callFn = func(fakeCall) (*wamp.Result, error) { return resultOf("call-result"), nil }
	res, err := f.Call(bg, "some.full.topic", 1, 2, Kwargs{"scale": 10}, CallOptions{DiscloseMe: true})
	if err != nil {
		t.Fatal(err)
	}
	if res.Value() != "call-result" {
		t.Errorf("result %#v", res.Value())
	}
	want := fakeCall{
		Procedure: "some.full.topic",
		Args:      []any{int64(1), int64(2)},
		Kwargs:    map[string]any{"scale": int64(10)},
		Opts:      &CallOptions{DiscloseMe: true},
	}
	if got := f.own.lastCall(t); !reflect.DeepEqual(got, want) {
		t.Errorf("call %#v\nwant %#v", got, want)
	}

	if _, err := f.Call(bg, "no.args"); err != nil {
		t.Fatal(err)
	}
	if got := f.own.lastCall(t); got.Args != nil || got.Kwargs != nil || got.Opts != nil {
		t.Errorf("call without arguments %#v", got)
	}
}

func TestCallValidationAndErrors(t *testing.T) {
	f := flock(t)
	for _, topic := range []string{"", "   ", " a.b"} {
		_, err := f.Call(bg, topic)
		if !errors.Is(err, ErrInvalidArgument) {
			t.Errorf("%q: %v", topic, err)
		}
	}
	_, err := f.Call(bg, "")
	mustContain(t, err.Error(), "topic must be a non-empty string")
	if len(f.own.allCalls()) != 0 {
		t.Fatal("an invalid call reached the connection")
	}

	f.own.callFn = func(fakeCall) (*wamp.Result, error) { return nil, wampErr("wamp.error.runtime_error", "boom") }
	_, err = f.Call(bg, "test.procedure")
	if want := `Call of procedure 'test.procedure' failed with WAMP error 'wamp.error.runtime_error' — ["boom"]`; err == nil || err.Error() != want {
		t.Errorf("error %v\nwant %q", err, want)
	}
	var werr *WampError
	if !errors.As(err, &werr) || werr.URI != "wamp.error.runtime_error" {
		t.Errorf("WampError not reachable: %v", err)
	}

	f.own.callFn = func(fakeCall) (*wamp.Result, error) {
		return nil, errors.New("Not connected to the IronFlock router")
	}
	_, err = f.Call(bg, "test.procedure")
	mustContain(t, err.Error(), "Call of procedure 'test.procedure' failed: Not connected")
}

func TestDeviceFunctionURIsCarryTheRealmStage(t *testing.T) {
	cases := []struct {
		env   string
		stage string
	}{
		{"DEV", "DEV"}, {"PROD", "PROD"}, {"", "DEV"}, {"dev", "DEV"}, {"prod", "PROD"},
		{"Prod", "PROD"}, {"production", "DEV"}, {"staging", "DEV"},
	}
	for _, tc := range cases {
		setIdentityEnv(t)
		t.Setenv("ENV", tc.env)
		f := newTestFlock(t)
		f.start(t)
		if _, err := f.RegisterDeviceFunction(bg, "com.myapp.add", func(context.Context, *Invocation) (any, error) { return nil, nil }); err != nil {
			t.Fatal(err)
		}
		if _, err := f.CallDeviceFunction(bg, 13, "com.myapp.add", 1, 2); err != nil {
			t.Fatal(err)
		}
		if got, want := f.own.allRegs()[0].Procedure, "10.12.20."+tc.stage+".com.myapp.add"; got != want {
			t.Errorf("ENV=%q: registered %q, want %q", tc.env, got, want)
		}
		call := f.own.lastCall(t)
		if want := "10.13.20." + tc.stage + ".com.myapp.add"; call.Procedure != want {
			t.Errorf("ENV=%q: called %q, want %q", tc.env, call.Procedure, want)
		}
		// Device function calls carry no device metadata and no window.
		if !reflect.DeepEqual(call.Args, []any{int64(1), int64(2)}) || call.Kwargs != nil || call.Window != 0 {
			t.Errorf("call %#v", call)
		}
		if f.Stage().Lower() != strings.ToLower(tc.stage) {
			t.Errorf("realm stage %q", f.Stage())
		}
	}
}

func TestRegisterDeviceFunction(t *testing.T) {
	f := flock(t)
	force := false
	opts := RegisterOptions{ForceReregister: &force}
	handler := func(context.Context, *Invocation) (any, error) { return "ok", nil }
	reg, err := f.RegisterDeviceFunction(bg, "my.func", handler, opts)
	if err != nil {
		t.Fatal(err)
	}
	regs := f.own.allRegs()
	if len(regs) != 1 || regs[0].Procedure != "10.12.20.DEV.my.func" || regs[0].Reg != reg {
		t.Fatalf("registrations %#v", regs)
	}
	if !reflect.DeepEqual(regs[0].Opts, &opts) || *opts.ForceReregister {
		t.Errorf("options %#v (caller's options must not change)", regs[0].Opts)
	}
	mustContain(t, f.logs.String(), "Function registered for IronFlock topic 'my.func'. (Full WAMP topic: '10.12.20.DEV.my.func')")

	// Without options the connection applies its default (force_reregister).
	if _, err := f.Register(bg, "other", handler); err != nil {
		t.Fatal(err)
	}
	if f.own.allRegs()[1].Opts != nil {
		t.Error("no options must be sent as nil")
	}
	if _, err := f.RegisterFunction(bg, "legacy", handler); err != nil {
		t.Fatal(err)
	}
	if got := f.own.allRegs()[2].Procedure; got != "10.12.20.DEV.legacy" {
		t.Errorf("RegisterFunction registered %q", got)
	}
	if err := f.Unregister(bg, reg); err != nil || !f.own.allRegs()[0].Removed {
		t.Errorf("Unregister: %v", err)
	}
	if err := f.Unregister(bg, nil); err != nil {
		t.Errorf("Unregister(nil): %v", err)
	}

	f.own.registerFn = func(string) error { return wampErr(wamp.URIProcedureAlreadyExists) }
	_, err = f.RegisterDeviceFunction(bg, "taken", handler)
	if want := "Registration of procedure '10.12.20.DEV.taken' failed with WAMP error 'wamp.error.procedure_already_exists'"; err == nil || err.Error() != want {
		t.Errorf("error %v", err)
	}
	if _, err := f.RegisterDeviceFunction(bg, "nil.handler", nil); !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("nil handler: %v", err)
	}
}

func TestCallDeviceFunctionErrorsNameTheDevice(t *testing.T) {
	f := flock(t)
	f.own.callFn = func(fakeCall) (*wamp.Result, error) { return nil, wampErr(wamp.URINoSuchProcedure) }
	_, err := f.CallDeviceFunction(bg, 42, "my.procedure")
	want := "Call of procedure 'my.procedure' on device '42' (full WAMP topic '10.42.20.DEV.my.procedure') failed with WAMP error 'wamp.error.no_such_procedure'"
	if err == nil || err.Error() != want {
		t.Errorf("error %v\nwant %q", err, want)
	}
	if _, err := f.CallFunction(bg, 42, "my.procedure"); err == nil {
		t.Error("CallFunction must behave like CallDeviceFunction")
	}
	if got := f.own.lastCall(t).Procedure; got != "10.42.20.DEV.my.procedure" {
		t.Errorf("CallFunction called %q", got)
	}
}

const agentHint = "The IronFlock device agent injects SWARM_KEY, DEVICE_KEY and APP_KEY into every app container; " +
	"set them yourself when running the app elsewhere."

func TestRegisterWithoutDeviceKeyFailsFast(t *testing.T) {
	for _, key := range []string{"", " ", "0", " 0 "} {
		setIdentityEnv(t)
		t.Setenv("DEVICE_KEY", key)
		f := newTestFlock(t)
		f.start(t)
		handler := func(context.Context, *Invocation) (any, error) { return nil, nil }
		_, err := f.RegisterDeviceFunction(bg, "toggle_lamp", handler)
		if !errors.Is(err, ErrMissingConfig) || errors.Is(err, ErrInvalidArgument) {
			t.Fatalf("DEVICE_KEY=%q: %v", key, err)
		}
		want := "ironflock: missing configuration: Cannot address device function 'toggle_lamp': DEVICE_KEY not set. " + agentHint
		if err.Error() != want {
			t.Errorf("message %q\nwant %q", err, want)
		}
		if _, err := f.Register(bg, "toggle_lamp", handler); !errors.Is(err, ErrMissingConfig) {
			t.Errorf("Register: %v", err)
		}
		if len(f.own.allRegs()) != 0 {
			t.Error("a registration reached the connection")
		}
		// Calling another device's function does not need this device's key.
		if _, err := f.CallDeviceFunction(bg, 13, "toggle_lamp"); err != nil {
			t.Fatal(err)
		}
		if got := f.own.lastCall(t).Procedure; got != "10.13.20.DEV.toggle_lamp" {
			t.Errorf("called %q", got)
		}
	}
}

func TestCallWithoutTargetDeviceKeyFailsFast(t *testing.T) {
	f := flock(t)
	for _, key := range []int{0, -1} {
		_, err := f.CallDeviceFunction(bg, key, "toggle_lamp")
		if !errors.Is(err, ErrMissingConfig) || !errors.Is(err, ErrInvalidArgument) {
			t.Errorf("deviceKey %d: %v, want ErrMissingConfig and ErrInvalidArgument", key, err)
			continue
		}
		mustContain(t, err.Error(), "Cannot address device function 'toggle_lamp': device_key not set.")
	}
	if len(f.own.allCalls()) != 0 {
		t.Error("a call reached the connection")
	}
}

func TestDeviceFunctionsNeedSwarmAndAppKey(t *testing.T) {
	handler := func(context.Context, *Invocation) (any, error) { return nil, nil }
	for _, missing := range []string{"SWARM_KEY", "APP_KEY"} {
		setIdentityEnv(t)
		unsetEnv(t, missing)
		f := newTestFlock(t)
		if _, err := f.RegisterDeviceFunction(bg, "toggle_lamp", handler); !errors.Is(err, ErrMissingConfig) ||
			!strings.Contains(err.Error(), "'toggle_lamp': "+missing+" not set") {
			t.Errorf("register without %s: %v", missing, err)
		}
		_, err := f.CallDeviceFunction(bg, 13, "toggle_lamp")
		if !errors.Is(err, ErrMissingConfig) || errors.Is(err, ErrInvalidArgument) ||
			!strings.Contains(err.Error(), "'toggle_lamp': "+missing+" not set") {
			t.Errorf("call without %s: %v", missing, err)
		}
		if len(f.own.allRegs())+len(f.own.allCalls()) != 0 {
			t.Error("an operation reached the connection")
		}
	}

	// Every missing key is named, in URI order.
	setIdentityEnv(t)
	unsetEnv(t, "SWARM_KEY", "DEVICE_KEY", "APP_KEY")
	f := newTestFlock(t)
	_, err := f.RegisterDeviceFunction(bg, "toggle_lamp", handler)
	mustContain(t, err.Error(), "Cannot address device function 'toggle_lamp': SWARM_KEY, DEVICE_KEY, APP_KEY not set. "+agentHint)
	_, err = f.CallDeviceFunction(bg, 0, "toggle_lamp")
	mustContain(t, err.Error(), "SWARM_KEY, device_key, APP_KEY not set.")
}

func TestDeviceFunctionsRejectEmptyNames(t *testing.T) {
	f := flock(t)
	handler := func(context.Context, *Invocation) (any, error) { return nil, nil }
	for _, topic := range []string{"", "   "} {
		_, err := f.RegisterDeviceFunction(bg, topic, handler)
		if !errors.Is(err, ErrInvalidArgument) || !strings.HasSuffix(err.Error(), "topic must be a non-empty string") {
			t.Errorf("register %q: %v", topic, err)
		}
		_, err = f.CallDeviceFunction(bg, 13, topic)
		if !errors.Is(err, ErrInvalidArgument) || !strings.HasSuffix(err.Error(), "topic must be a non-empty string") {
			t.Errorf("call %q: %v", topic, err)
		}
	}
	if _, err := f.CallDeviceFunction(bg, 13, " padded"); !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("padded name: %v", err)
	}
	if len(f.own.allRegs())+len(f.own.allCalls()) != 0 {
		t.Error("an operation reached the connection")
	}
}

func TestSetDeviceLocation(t *testing.T) {
	f := flock(t)
	if _, err := f.SetDeviceLocation(bg, 11.5, 48.1); err != nil {
		t.Fatal(err)
	}
	want := fakeCall{
		Procedure: "ironflock.location_service.update",
		Args:      []any{map[string]any{"long": 11.5, "lat": 48.1}},
		Kwargs:    deviceMetadata(),
	}
	if got := f.own.lastCall(t); !reflect.DeepEqual(got, want) {
		t.Errorf("call %#v\nwant %#v", got, want)
	}

	for _, c := range [][2]float64{{200, 48}, {-181, 0}, {0, 91}, {0, -90.5}, {math.NaN(), 0}, {0, math.Inf(1)}} {
		_, err := f.SetDeviceLocation(bg, c[0], c[1])
		if !errors.Is(err, ErrInvalidArgument) || !strings.Contains(err.Error(), "Invalid location parameters") {
			t.Errorf("%v: %v", c, err)
		}
	}
	for _, c := range [][2]float64{{180, 90}, {-180, -90}} {
		if _, err := f.SetDeviceLocation(bg, c[0], c[1]); err != nil {
			t.Errorf("%v: %v", c, err)
		}
	}

	f.own.callFn = func(fakeCall) (*wamp.Result, error) { return nil, wampErr(wamp.URINoSuchProcedure) }
	_, err := f.SetDeviceLocation(bg, 1, 2)
	if want := "Device location update failed with WAMP error 'wamp.error.no_such_procedure'"; err == nil || err.Error() != want {
		t.Errorf("error %v", err)
	}
}

// rpcEnum is encoded through its MarshalText method.
type rpcEnum int

func (e rpcEnum) MarshalText() ([]byte, error) { return []byte(fmt.Sprintf("level-%d", int(e))), nil }

// rpcStatus has fields encoding/json converts: a ",string" option, a
// MarshalText type, a json.Number and a time.Time in another zone.
type rpcStatus struct {
	Count int         `json:"count,string"`
	Level rpcEnum     `json:"level"`
	Num   json.Number `json:"num"`
	When  time.Time   `json:"when"`
}

// A device function's result and error payloads are sent as every other
// payload of the package: normalized (see normalize). The WAMP client's codec
// would send them as they are — a map with integer keys the router cannot
// decode, so the caller never gets an answer; a time.Time as a msgpack
// timestamp; a string with invalid UTF-8 that makes a Python caller drop its
// session.
func TestRegisterDeviceFunctionNormalizesWhatTheHandlerReturns(t *testing.T) {
	f := flock(t)
	cest := time.FixedZone("CEST", 2*3600)
	when := time.Date(2026, 10, 8, 14, 0, 0, 123456789, cest)
	status := rpcStatus{Count: 7, Level: 2, Num: "12", When: when}
	statusWire := map[string]any{"count": "7", "level": "level-2", "num": int64(12), "when": "2026-10-08T12:00:00.123456789Z"}
	cases := []struct {
		name      string
		value     any
		err       error
		wantValue any
		wantErr   *WampError
	}{
		{"nil", nil, nil, nil, nil},
		{"time", when, nil, "2026-10-08T12:00:00.123456789Z", nil},
		{"integer keys", map[int]string{1: "a"}, nil, map[string]any{"1": "a"}, nil},
		{"struct", status, nil, statusWire, nil},
		{"invalid UTF-8", "sensor\xff\xfeline", nil, "sensor��line", nil},
		{"Result", Result{Args: []any{when}, Kwargs: map[string]any{"m": map[int]int{1: 2}}}, nil,
			&Result{Args: []any{"2026-10-08T12:00:00.123456789Z"}, Kwargs: map[string]any{"m": map[string]any{"1": int64(2)}}}, nil},
		{"*Result", &Result{Args: []any{status}, Details: map[string]any{"x": 1}}, nil, &Result{Args: []any{statusWire}}, nil},
		{"nil *Result", (*Result)(nil), nil, nil, nil},
		{"WampError", nil, fmt.Errorf("wrapped: %w", &WampError{URI: "app.error.bad", Args: []any{map[int]string{1: "a"}},
			Kwargs: map[string]any{"at": when}}), nil,
			&WampError{URI: "app.error.bad", Args: []any{map[string]any{"1": "a"}}, Kwargs: map[string]any{"at": "2026-10-08T12:00:00.123456789Z"}}},
		{"plain error", nil, errors.New("bad frame \xff"), nil,
			&WampError{URI: wamp.URIRuntimeError, Args: []any{"bad frame �"}}},
		{"error without a URI", nil, &WampError{Args: []any{1}}, nil,
			&WampError{URI: wamp.URIRuntimeError, Args: []any{(&WampError{Args: []any{1}}).Error()}}},
		{"unsendable result", complex(1, 2), nil, nil, &WampError{URI: wamp.URIRuntimeError, Args: []any{
			"the result of procedure '10.12.20.DEV.fn' cannot be sent: unsupported type complex128"}}},
		{"unsendable Result kwargs", &Result{Kwargs: map[string]any{"cb": func() {}}}, nil, nil, &WampError{URI: wamp.URIRuntimeError,
			Args: []any{"the result of procedure '10.12.20.DEV.fn' cannot be sent: its kwargs: cb: unsupported type func()"}}},
		{"unsendable error payload", nil, &WampError{URI: "app.error.bad", Args: []any{make(chan int)}}, nil,
			&WampError{URI: wamp.URIRuntimeError, Args: []any{
				"the result of procedure '10.12.20.DEV.fn' cannot be sent: the args of its error app.error.bad: [0]: unsupported type chan int"}}},
	}
	for i, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			name := fmt.Sprintf("fn%d", i)
			if _, err := f.RegisterDeviceFunction(bg, name, func(context.Context, *Invocation) (any, error) { return c.value, c.err }); err != nil {
				t.Fatal(err)
			}
			regs := f.own.allRegs()
			value, err := regs[len(regs)-1].Handler(bg, &Invocation{Procedure: "10.12.20.DEV." + name})
			if !reflect.DeepEqual(value, c.wantValue) {
				t.Errorf("value %#v\nwant  %#v", value, c.wantValue)
			}
			want := c.wantErr
			if want != nil && strings.Contains(fmt.Sprint(want.Args...), "'10.12.20.DEV.fn'") {
				want = &WampError{URI: want.URI, Args: []any{strings.Replace(want.Args[0].(string), "DEV.fn'", "DEV."+name+"'", 1)}}
			}
			var werr *WampError
			switch {
			case want == nil && err != nil:
				t.Errorf("error %v", err)
			case want != nil && (!errors.As(err, &werr) || !reflect.DeepEqual(werr, want)):
				t.Errorf("error %#v\nwant  %#v", err, want)
			}
		})
	}
	if n := strings.Count(f.logs.String(), "cannot be sent"); n != 3 {
		t.Errorf("%d unsendable results logged, want 3:\n%s", n, f.logs)
	}
}
