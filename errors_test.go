package ironflock

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/ironflock/sdk-go/wamp"
)

func TestOperationErrorMessages(t *testing.T) {
	cases := []struct {
		err  error
		want string
	}{
		{&OperationError{Op: "Publish to topic 't'", Err: wampErr("wamp.error.not_authorized")},
			"Publish to topic 't' failed with WAMP error 'wamp.error.not_authorized'"},
		{&OperationError{Op: "Call of procedure 'p'", Err: wampErr("wamp.error.runtime_error", "boom", map[string]any{"a": 1})},
			`Call of procedure 'p' failed with WAMP error 'wamp.error.runtime_error' — ["boom",{"a":1}]`},
		{&OperationError{Op: "Call of procedure 'p'", Err: fmt.Errorf("wrapped: %w", wampErr("wamp.error.canceled"))},
			"Call of procedure 'p' failed with WAMP error 'wamp.error.canceled'"},
		{&OperationError{Op: "Device location update", Err: errors.New("not connected")},
			"Device location update failed: not connected"},
		{&OperationError{Op: "getHistory('t')", Err: errors.New("x"), hint: "the hint"},
			"getHistory('t') failed: the hint"},
		// The detail is compact JSON as JavaScript's JSON.stringify and
		// Python's json.dumps write it: <, > and & are not escaped.
		{&OperationError{Op: "Publish to topic 'x'", Err: wampErr("wamp.error.invalid_argument", "a<b && c>d")},
			`Publish to topic 'x' failed with WAMP error 'wamp.error.invalid_argument' — ["a<b && c>d"]`},
		{&OperationError{Op: "Call of procedure 'p'", Err: wampErr("app.error", map[string]any{"q": "x<1"})},
			`Call of procedure 'p' failed with WAMP error 'app.error' — [{"q":"x<1"}]`},
	}
	for _, tc := range cases {
		if got := tc.err.Error(); got != tc.want {
			t.Errorf("got  %q\nwant %q", got, tc.want)
		}
	}
}

func TestErrorPassThrough(t *testing.T) {
	client := []error{invalidf("x"), missingConfigf("y")}
	for _, err := range client {
		if operationFailed("op", err) != err || historyFailed("op", "t", err, false) != err {
			t.Errorf("%v was wrapped", err)
		}
	}
	cerr := &CrossAppAccessError{Code: CodeNoGrant}
	if operationFailed("op", cerr) != error(cerr) {
		t.Error("a CrossAppAccessError was wrapped")
	}
	if operationFailed("op", nil) != nil || historyFailed("op", "t", nil, true) != nil {
		t.Error("nil must stay nil")
	}
	if err := operationFailed("op", context.DeadlineExceeded); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("context error lost: %v", err)
	}
}

func TestSentinelErrors(t *testing.T) {
	err := invalidf("bad %s", "thing")
	if err.Error() != "ironflock: invalid argument: bad thing" || !errors.Is(err, ErrInvalidArgument) || errors.Is(err, ErrMissingConfig) {
		t.Errorf("invalidf: %v", err)
	}
	err = missingConfigf("SWARM_KEY not set in environment variables!")
	if err.Error() != "ironflock: missing configuration: SWARM_KEY not set in environment variables!" ||
		!errors.Is(err, ErrMissingConfig) || errors.Is(err, ErrInvalidArgument) {
		t.Errorf("missingConfigf: %v", err)
	}
	both := &sdkError{msg: "m", kinds: []error{ErrMissingConfig, ErrInvalidArgument}}
	if !errors.Is(both, ErrMissingConfig) || !errors.Is(both, ErrInvalidArgument) {
		t.Error("an error of two kinds must match both")
	}
	if prefixed := invalidParams("query", both); !errors.Is(prefixed, ErrMissingConfig) || prefixed.Error() != "ironflock: missing configuration: Invalid query parameters: m" {
		t.Errorf("invalidParams: %v", prefixed)
	}
	plain := errors.New("plain")
	if invalidParams("query", plain) != plain {
		t.Error("invalidParams must leave other errors alone")
	}
}

func TestMapCrossAppError(t *testing.T) {
	cases := []struct {
		err  error
		code string
		msg  string
	}{
		{wampErr("sys.appaccess.error.no_grant"), CodeNoGrant, "sys.appaccess.error.no_grant"},
		{wampErr("sys.appaccess.error.unknown_app", "nope"), CodeUnknownApp, `sys.appaccess.error.unknown_app: "nope"`},
		{wampErr("wamp.error.authorization_failed", map[string]any{"a": int64(1)}, "x"), CodeNotAuthorized, `wamp.error.authorization_failed: {"a":1}`},
		{fmt.Errorf("ctx: %w", wampErr("sys.appaccess.error.provider_not_installed")), CodeProviderNotInstalled, "sys.appaccess.error.provider_not_installed"},
		// No HTML escaping, as in the Python and JavaScript SDKs.
		{wampErr("sys.appaccess.error.no_grant", "app <weather> & co"), CodeNoGrant, `sys.appaccess.error.no_grant: "app <weather> & co"`},
		{wampErr("wamp.error.not_authorized", map[string]any{"q": "x<1"}), CodeNotAuthorized, `wamp.error.not_authorized: {"q":"x<1"}`},
	}
	for _, tc := range cases {
		got := mapCrossAppError(tc.err)
		if got == nil || got.Code != tc.code || got.Message != tc.msg || !errors.Is(got, tc.err) {
			t.Errorf("%v: %#v", tc.err, got)
		}
	}
	for _, err := range []error{errors.New("boom"), wampErr("wamp.error.runtime_error"), wampErr(wamp.URINoAuthMethod)} {
		if got := mapCrossAppError(err); got != nil {
			t.Errorf("%v mapped to %v", err, got)
		}
	}
	if got := (&CrossAppAccessError{Code: CodeNoGrant}).Error(); got != "NO_GRANT" {
		t.Errorf("message-less error %q", got)
	}
	if got := (&CrossAppAccessError{Code: CodeNoGrant, Message: "m"}).Error(); got != "NO_GRANT: m" {
		t.Errorf("error %q", got)
	}
}

func TestUnknownFilterOperatorsAreLoggedNotRejected(t *testing.T) {
	f := flock(t)
	if _, err := f.GetHistory(bg, "t", &TableQueryParams{Limit: 1, FilterAnd: []Filter{Where("name", "REGEXP", "^a")}}); err != nil {
		t.Fatal(err)
	}
	mustContain(t, f.logs.String(), "Operator 'REGEXP' is not in standard list")
	if got := f.own.lastCall(t).Args[0].(map[string]any)["filterAnd"]; fmt.Sprint(got) != "[map[column:name operator:REGEXP value:^a]]" {
		t.Errorf("filter %v", got)
	}
}

// The data backend's typed refusals keep their URI through the operation
// error, and are not retried. The constants are fleetdb v1.4.0's literals
// (DataBackend.ts, storageGuard.ts).
func TestDataBackendRefusals(t *testing.T) {
	for got, want := range map[string]string{
		URIRateLimited:                "sys.dataservice.error.rate_limited",
		URIResultTooLarge:             "sys.dataservice.error.result_too_large",
		URIInvalidLimit:               "sys.dataservice.error.invalid_limit",
		URIInvalidTimeRange:           "sys.dataservice.error.invalid_time_range",
		URIInvalidMetric:              "sys.dataservice.error.invalid_metric",
		URIInvalidGroupBy:             "sys.dataservice.error.invalid_group_by",
		URISeriesTooManyGroups:        "sys.dataservice.error.series_too_many_groups",
		URISecretColumn:               "sys.dataservice.error.secret_column",
		URINotASecretColumn:           "sys.dataservice.error.not_a_secret_column",
		URIStorageFull:                "sys.dataservice.error.storage_full",
		URIStorageOverusage:           "sys.dataservice.error.storage_overusage",
		URIEntityKeyConflict:          "sys.dataservice.error.entity_key_conflict",
		URISecretSentinelUnresolvable: "sys.dataservice.error.secret_sentinel_unresolvable",
		URISecretCiphertextRejected:   "sys.dataservice.error.secret_ciphertext_rejected",
	} {
		if got != want {
			t.Errorf("%s, want %s", got, want)
		}
	}

	f := flock(t)
	fail(f.own, wampErr(URIRateLimited, "secret.verify is limited to 120 calls per minute"))
	_, err := f.VerifySecret(bg, "credentials", "api_key", "x", nil)
	if WampURI(err) != URIRateLimited {
		t.Errorf("VerifySecret: %v", err)
	}
	if want := `verifySecret('credentials') failed with WAMP error 'sys.dataservice.error.rate_limited' — ["secret.verify is limited to 120 calls per minute"]`; err.Error() != want {
		t.Errorf("message %q", err)
	}
	fail(f.own, wampErr(URIStorageFull, "appliance storage full"))
	calls := len(f.own.allCalls())
	_, err = f.AppendToTable(bg, "sensordata", Row{"tsp": "2026-01-01T00:00:00Z"})
	var werr *WampError
	if !errors.As(err, &werr) || werr.URI != URIStorageFull || len(f.own.allCalls()) != calls+1 {
		t.Errorf("AppendToTable: %v after %d calls", err, len(f.own.allCalls())-calls)
	}
}
