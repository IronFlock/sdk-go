package ironflock

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/ironflock/sdk-go/wamp"
)

func TestRefreshTransform(t *testing.T) {
	f := flock(t)
	answer(f.own, map[string]any{"success": true, "materialized": true, "refreshed": true, "skipped": false,
		"published": true, "rows": int64(42)})
	res, err := f.RefreshTransform(bg, "hourly_avg")
	if err != nil {
		t.Fatal(err)
	}
	want := TransformRefreshResult{Success: true, Materialized: true, Refreshed: true, Published: true, Rows: 42}
	if *res != want {
		t.Errorf("result %#v, want %#v", *res, want)
	}
	// One positional empty object, [{}], as the Python and JavaScript SDKs
	// send it; no kwargs, no call options, the reconnect window as the
	// retry window. (DeepEqual tells a nil map, which would go out as null,
	// from the empty one.)
	wantCall := fakeCall{
		Procedure: "transform.refresh.hourly_avg",
		Args:      []any{map[string]any{}},
		Window:    DefaultReconnectWindow,
	}
	if got := f.own.lastCall(t); !reflect.DeepEqual(got, wantCall) {
		t.Errorf("call %#v\nwant %#v", got, wantCall)
	}
	if arg, ok := f.own.lastCall(t).Args[0].(map[string]any); !ok || arg == nil {
		t.Errorf("argument %#v, want a non-nil empty object", f.own.lastCall(t).Args[0])
	}
}

func TestRefreshTransformDecodesDefensively(t *testing.T) {
	f := flock(t)
	cases := []struct {
		answer any
		want   TransformRefreshResult
	}{
		// A plain view with no subscriber: nothing refreshed, nothing
		// published, no rows field.
		{map[string]any{"success": true, "materialized": false, "refreshed": false, "skipped": false, "published": false},
			TransformRefreshResult{Success: true}},
		{map[string]any{"success": true, "materialized": true, "refreshed": true, "skipped": true, "published": true, "rows": 3.0},
			TransformRefreshResult{Success: true, Materialized: true, Refreshed: true, Skipped: true, Published: true, Rows: 3}},
		{map[string]any{"success": true, "published": true, "rows": uint64(7)},
			TransformRefreshResult{Success: true, Published: true, Rows: 7}},
		{map[string]any{"success": true, "published": true, "rows": int64(-1)},
			TransformRefreshResult{Success: true, Published: true}},
		{map[string]any{"success": true, "published": true, "rows": -2.0},
			TransformRefreshResult{Success: true, Published: true}},
		{map[string]any{"success": true, "published": true, "rows": "3"},
			TransformRefreshResult{Success: true, Published: true}},
		// Strings and numbers are not booleans.
		{map[string]any{"success": "true", "materialized": "true", "refreshed": int64(1), "skipped": "false", "published": "true"},
			TransformRefreshResult{}},
		{"ok", TransformRefreshResult{}},
		{[]any{map[string]any{"success": true}}, TransformRefreshResult{}},
		{nil, TransformRefreshResult{}},
	}
	for _, tc := range cases {
		answer(f.own, tc.answer)
		res, err := f.RefreshTransform(bg, "hourly_avg")
		if err != nil {
			t.Fatal(err)
		}
		if *res != tc.want {
			t.Errorf("answer %#v: %#v, want %#v", tc.answer, *res, tc.want)
		}
	}
}

func TestRefreshTransformValidationAndErrors(t *testing.T) {
	f := flock(t)
	for _, name := range []string{"", "   ", " hourly_avg", "hourly_avg\n"} {
		res, err := f.RefreshTransform(bg, name)
		if !errors.Is(err, ErrInvalidArgument) || res != nil {
			t.Errorf("name %q: %v, %v", name, res, err)
		}
	}
	if len(f.own.allCalls()) != 0 {
		t.Error("an invalid name reached the connection")
	}

	fail(f.own, wampErr(wamp.URINoSuchProcedure))
	_, err := f.RefreshTransform(bg, "hourly_avg")
	want := "refreshTransform('hourly_avg') failed: procedure 'transform.refresh.hourly_avg' is not registered. " +
		"The data backend registers it only for a transform declared in the app's data-template: " +
		"the name is not a transform's `tablename` (a table has no refresh procedure; check `transforms:` in data-template.yml), " +
		"the app's data backend is not running, or it predates on-demand transform refresh (update it)."
	if err == nil || err.Error() != want {
		t.Errorf("error %v\nwant %q", err, want)
	}
	if WampURI(err) != wamp.URINoSuchProcedure {
		t.Errorf("the hint hides the refusal: %v", err)
	}

	// A refusal keeps its URI and reason, and gets no hint.
	fail(f.own, wampErr(URIRefreshFailed, "could not refresh hourly_avg: permission denied for materialized view"))
	_, err = f.RefreshTransform(bg, "hourly_avg")
	if WampURI(err) != URIRefreshFailed {
		t.Errorf("refusal: %v", err)
	}
	if want := `refreshTransform('hourly_avg') failed with WAMP error 'sys.dataservice.error.refresh_failed' — ` +
		`["could not refresh hourly_avg: permission denied for materialized view"]`; err.Error() != want {
		t.Errorf("message %q\nwant %q", err, want)
	}
	for _, uri := range []string{URIRefreshInProgress, URIStorageFull, wamp.URINotAuthorized} {
		fail(f.own, wampErr(uri, "nope"))
		_, err = f.RefreshTransform(bg, "hourly_avg")
		var oerr *OperationError
		if WampURI(err) != uri || !errors.As(err, &oerr) || strings.Contains(err.Error(), "not registered") {
			t.Errorf("%s: %v", uri, err)
		}
	}
}

// Only the owning app may spend its data backend's time and push to its
// boards: a consumed app has no refresh.
func TestConsumedAppHasNoRefreshTransform(t *testing.T) {
	if _, found := reflect.TypeOf(&ConsumedApp{}).MethodByName("RefreshTransform"); found {
		t.Error("ConsumedApp must not offer RefreshTransform")
	}
}
