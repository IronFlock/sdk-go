//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	ironflock "github.com/ironflock/sdk-go"
	"github.com/ironflock/sdk-go/filestore"
)

// stepOut is what one scenario step produced: the payloads the fake platform
// recorded, the SDK's return value, and the error class, if any.
type stepOut struct {
	Recorded any `json:"recorded"`
	Result   any `json:"result"`
	Error    any `json:"error"`
}

// documentedSteps are the steps where the Go SDK deliberately differs from
// the reference SDK (scenario_divergences.json lists them, with the reason):
// what the Go run must record and its error class instead of the
// reference's. Every step the list names needs an entry here, and every
// entry here must be on the list. None today: the reference records what the
// Go run records on every step.
var documentedSteps = map[string]stepOut{}

// TestCrossSDKScenario runs the conformance scenario (the same steps as
// py_scenario.py and js_scenario.mjs) and compares the recorded wire
// payloads and error classes with the Python SDK's reference output; the
// steps scenario_divergences.json lists are compared with documentedSteps
// instead.
func TestCrossSDKScenario(t *testing.T) {
	url := platformURL(t)
	deviceEnv(t)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	ifl, err := ironflock.New(ironflock.WithURL(url), ironflock.WithReconnectWindow(5*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if err := ifl.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := ifl.Stop(context.Background()); err != nil {
			t.Errorf("Stop: %v", err)
		}
	}()

	out := map[string]stepOut{}
	var steps []string
	step := func(name string, fn func() (any, error)) {
		t.Helper()
		if _, err := ifl.Call(ctx, "test.reset"); err != nil {
			t.Fatalf("%s: reset: %v", name, err)
		}
		result, err := fn()
		var errClass any
		if err != nil {
			errClass = errorClass(err)
		}
		time.Sleep(400 * time.Millisecond)
		rec, rerr := ifl.Call(ctx, "test.recorded")
		if rerr != nil {
			t.Fatalf("%s: recorded: %v", name, rerr)
		}
		out[name] = stepOut{Recorded: rec.Value(), Result: jsonable(result), Error: errClass}
		steps = append(steps, name)
	}
	clearRecorded := func() {
		if _, err := ifl.Call(ctx, "test.clear_recorded"); err != nil {
			t.Fatal(err)
		}
	}
	files := ifl.Files()
	keepFiles := func(fn func() (any, error)) func() (any, error) {
		return func() (any, error) {
			// test.reset empties the store: put the object again, then
			// clear only the recordings.
			if _, err := files.Put(ctx, "a/b c.txt", []byte("hello"), filestore.ContentType("text/plain")); err != nil {
				return nil, err
			}
			clearRecorded()
			return fn()
		}
	}
	none := func(err error) (any, error) { return nil, err }

	// Every row carries tsp: fleetdb refuses an append without one and drops
	// such a publish. A kwargs-only publish reaches fleetdb without args[0],
	// so a table with the default column paths drops it too.
	step("publish_to_table_row", func() (any, error) {
		return none(ifl.PublishToTable(ctx, "sensordata", ironflock.Row{"tsp": "2026-01-01T00:00:01Z", "temperature": 22.5, "n": 3}))
	})
	step("publish_to_table_kwargs", func() (any, error) {
		return none(ifl.PublishToTable(ctx, "sensordata", ironflock.Kwargs{"temperature": 1.5}))
	})
	step("append_to_table", func() (any, error) {
		r, err := ifl.AppendToTable(ctx, "sensordata", ironflock.Row{"tsp": "2026-01-01T00:00:02Z", "temperature": 23})
		return r.Value(), err
	})
	step("append_without_tsp", func() (any, error) {
		r, err := ifl.AppendToTable(ctx, "sensordata", ironflock.Row{"temperature": 23})
		return r.Value(), err
	})
	step("publish_rows_to_table", func() (any, error) {
		return none(ifl.PublishRowsToTable(ctx, "sensordata",
			[]ironflock.Row{{"tsp": "2026-01-01T00:00:03Z", "temperature": 1}, {"tsp": "2026-01-01T00:00:04Z", "temperature": 2}},
			ironflock.Kwargs{"batch": "b1"}))
	})
	step("append_rows_to_table", func() (any, error) {
		r, err := ifl.AppendRowsToTable(ctx, "sensordata", []ironflock.Row{{"tsp": "2026-01-01T00:00:05Z", "temperature": 3}})
		return r.Value(), err
	})
	step("report_error_publish", func() (any, error) {
		return ifl.ReportError(ctx, "Sensor timed out", ironflock.ReportErrorOptions{Level: ironflock.LevelWarning, Tsp: "2026-01-01T00:00:00Z"})
	})
	step("report_error_append", func() (any, error) {
		r, err := ifl.ReportError(ctx, "Calibration failed", ironflock.ReportErrorOptions{
			Append: true, Tsp: "2026-01-01T00:00:00Z", UserMessage: "Please recalibrate"})
		return r.Value(), err
	})
	step("get_history_full", func() (any, error) {
		return ifl.GetHistory(ctx, "sensordata", &ironflock.TableQueryParams{
			Limit: 5, Offset: 2,
			TimeRange: &ironflock.TimeRange{Start: "2026-01-01T00:00:00Z"},
			FilterAnd: []ironflock.Filter{ironflock.Where("temperature", ">", 20), ironflock.Latest()},
			Columns:   []string{"temperature"},
		})
	})
	step("get_history_default", func() (any, error) { return ifl.GetHistory(ctx, "sensordata", nil) })
	step("get_series_history", func() (any, error) {
		return ifl.GetSeriesHistory(ctx, "sensordata", ironflock.SeriesQueryParams{
			Metrics: []ironflock.SeriesMetric{{Ref: "temperature", Method: ironflock.MethodAvg}}, Limit: 100,
			TimeRange: &ironflock.TimeRange{Start: "2026-01-01T00:00:00Z", End: "2026-02-01T00:00:00Z"},
			GroupBy:   []string{"device_key"},
		})
	})
	step("reveal_secrets", func() (any, error) {
		return ifl.RevealSecrets(ctx, "credentials", &ironflock.TableQueryParams{Limit: 1, FilterAnd: []ironflock.Filter{ironflock.Latest()}})
	})
	step("verify_secret_match", func() (any, error) {
		r, err := ifl.VerifySecret(ctx, "credentials", "api_key", "right", nil)
		if err != nil {
			return nil, err
		}
		return r.Match, nil
	})
	step("verify_secret_limit", func() (any, error) {
		r, err := ifl.VerifySecret(ctx, "credentials", "api_key", "wrong", &ironflock.TableQueryParams{Limit: 3})
		if err != nil {
			return nil, err
		}
		return r.Match, nil
	})
	step("files_put_inline", func() (any, error) {
		return files.Put(ctx, "a/b c.txt", []byte("hello"), filestore.ContentType("text/plain"))
	})
	step("files_get_inline", keepFiles(func() (any, error) { return files.Get(ctx, "a/b c.txt") }))
	// fleetfiles lists folder-style and refuses a prefix ending in "/" (INTERNAL).
	step("files_list", keepFiles(func() (any, error) { return files.List(ctx, filestore.Prefix("a/")) }))
	step("files_list_root", func() (any, error) {
		if _, err := files.Put(ctx, "top.txt", []byte("top"), filestore.ContentType("text/plain")); err != nil {
			return nil, err
		}
		return keepFiles(func() (any, error) { return files.List(ctx) })()
	})
	step("files_stat", keepFiles(func() (any, error) { return files.Stat(ctx, "a/b c.txt") }))
	step("files_exists_missing", func() (any, error) { return files.Exists(ctx, "missing.txt") })
	step("files_copy", keepFiles(func() (any, error) {
		return files.Copy(ctx, "a/b c.txt", "copy.txt", filestore.ToNamespace("default"))
	}))
	step("files_move", keepFiles(func() (any, error) { return files.Move(ctx, "a/b c.txt", "moved.txt") }))
	step("files_put_direct", func() (any, error) {
		return files.Put(ctx, "big.bin", []byte(strings.Repeat("x", 5000)), filestore.ContentType("application/octet-stream"))
	})
	step("files_get_direct", func() (any, error) {
		if _, err := files.Put(ctx, "big.bin", []byte(strings.Repeat("y", 5000))); err != nil {
			return nil, err
		}
		clearRecorded()
		return files.Get(ctx, "big.bin")
	})
	step("files_usage_detail", func() (any, error) { return files.Usage(ctx, filestore.Detail()) })
	// files.read.url stats the object before it mints a URL.
	step("files_share_url", keepFiles(func() (any, error) {
		return files.ShareURL(ctx, "a/b c.txt", filestore.TTL(120*time.Second))
	}))
	step("files_upload_url", func() (any, error) {
		return files.UploadURL(ctx, "u.bin", filestore.TTL(60*time.Second),
			filestore.ContentType("application/octet-stream"), filestore.Size(10))
	})
	step("files_delete", keepFiles(func() (any, error) { return none(files.Delete(ctx, "a/b c.txt")) }))
	step("files_catalog", func() (any, error) { return files.RefreshCatalog(ctx) })
	step("connect_to_app", func() (any, error) { return ifl.ConnectToApp(ctx, "weather") })
	step("consumed_get_history", func() (any, error) {
		app, err := ifl.ConnectToApp(ctx, "weather")
		if err != nil {
			return nil, err
		}
		return app.GetHistory(ctx, "readings", &ironflock.TableQueryParams{Limit: 2, FilterAnd: []ironflock.Filter{ironflock.Latest()}})
	})
	step("consumed_get_series", func() (any, error) {
		app, err := ifl.ConnectToApp(ctx, "weather")
		if err != nil {
			return nil, err
		}
		return app.GetSeriesHistory(ctx, "readings", ironflock.SeriesQueryParams{
			Metrics: []ironflock.SeriesMetric{{Ref: "temp", Method: ironflock.MethodMax}}, Limit: 10,
			TimeRange: &ironflock.TimeRange{Start: int64(1767225600000)},
		})
	})
	step("connect_to_app_no_grant", func() (any, error) { return ifl.ConnectToApp(ctx, "nogrant") })
	step("list_consumable_apps", func() (any, error) { return ifl.ListConsumableApps(ctx) })

	data, _ := json.MarshalIndent(out, "", " ")
	outPath := os.Getenv("IRONFLOCK_TEST_OUT")
	if outPath == "" {
		outPath = filepath.Join(t.TempDir(), "go_out.json")
	}
	if err := os.WriteFile(outPath, data, 0o644); err != nil {
		t.Fatal(err)
	}

	// The documented steps hold whether or not there is a reference.
	divergent := divergences(t)
	for _, name := range sortedKeys(documentedSteps) {
		got, ran := out[name]
		if !ran {
			t.Errorf("%s: a documented step missing from the Go run", name)
			continue
		}
		want := documentedSteps[name]
		if a, b := canonical(want.Recorded), canonical(got.Recorded); a != b {
			t.Errorf("%s: wire payloads differ from the documented ones\n documented: %s\n go:         %s", name, a, b)
		}
		if fmt.Sprint(want.Error) != fmt.Sprint(got.Error) {
			t.Errorf("%s: error %v, documented %v", name, got.Error, want.Error)
		}
	}

	refPath := os.Getenv("IRONFLOCK_TEST_REFERENCE")
	if refPath == "" {
		t.Logf("wrote %s; set IRONFLOCK_TEST_REFERENCE to compare with a reference SDK run", outPath)
		return
	}
	refData, err := os.ReadFile(refPath)
	if err != nil {
		t.Fatal(err)
	}
	var ref map[string]stepOut
	if err := json.Unmarshal(refData, &ref); err != nil {
		t.Fatal(err)
	}
	var got map[string]stepOut
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	for _, name := range steps {
		if _, ok := ref[name]; !ok {
			t.Errorf("%s: step missing from the reference", name)
		}
	}
	for _, name := range sortedKeys(ref) {
		g, ok := got[name]
		if !ok {
			t.Errorf("%s: step missing from the Go run", name)
			continue
		}
		if reason, ok := divergent[name]; ok {
			t.Logf("%s: compared with its documented payloads, not the reference's (%s)", name, reason)
			continue
		}
		if a, b := canonical(ref[name].Recorded), canonical(g.Recorded); a != b {
			t.Errorf("%s: wire payloads differ\n reference: %s\n go:        %s", name, a, b)
		}
		if (ref[name].Error == nil) != (g.Error == nil) || fmt.Sprint(ref[name].Error) != fmt.Sprint(g.Error) {
			t.Errorf("%s: error differs: reference %v, go %v", name, ref[name].Error, g.Error)
		}
	}
}

// divergences reads scenario_divergences.json, the steps whose Go run
// deliberately differs from the reference SDK's, with the reason, and checks
// that it names exactly the steps of documentedSteps.
func divergences(t *testing.T) map[string]string {
	t.Helper()
	data, err := os.ReadFile("scenario_divergences.json")
	if err != nil {
		t.Fatal(err)
	}
	var list []struct {
		Step   string `json:"step"`
		Reason string `json:"reason"`
	}
	if err := json.Unmarshal(data, &list); err != nil {
		t.Fatalf("scenario_divergences.json: %v", err)
	}
	out := map[string]string{}
	for _, d := range list {
		if d.Step == "" || d.Reason == "" {
			t.Errorf("scenario_divergences.json: an entry without a step or a reason: %+v", d)
		}
		out[d.Step] = d.Reason
		if _, ok := documentedSteps[d.Step]; !ok {
			t.Errorf("%s: listed in scenario_divergences.json, but scenario_test.go documents no payloads for it", d.Step)
		}
	}
	for name := range documentedSteps {
		if _, ok := out[name]; !ok {
			t.Errorf("%s: documented in scenario_test.go, but not listed in scenario_divergences.json", name)
		}
	}
	return out
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// errorClass renders an error the way the reference drivers do:
// "<Type>: <code>" for the typed errors, "WampError: <uri>" for a WAMP
// error from the router or a backend.
func errorClass(err error) string {
	var cerr *ironflock.CrossAppAccessError
	if errors.As(err, &cerr) {
		return "CrossAppAccessError: " + cerr.Code
	}
	var ferr *filestore.Error
	if errors.As(err, &ferr) {
		return "FileStoreError: " + ferr.Code
	}
	if uri := ironflock.WampURI(err); uri != "" {
		return "WampError: " + uri
	}
	return "Error: " + err.Error()
}

// canonical renders v as JSON with sorted keys and numbers normalized, so
// payloads decoded from different SDKs compare equal when they are.
func canonical(v any) string {
	data, _ := json.Marshal(v)
	var x any
	_ = json.Unmarshal(data, &x)
	data, _ = json.Marshal(x)
	return string(data)
}

func jsonable(v any) any {
	switch x := v.(type) {
	case []byte:
		return map[string]any{"$bytes": string(x)}
	case *ironflock.ConsumedApp:
		if x == nil {
			return nil
		}
		return map[string]any{"app": x.App, "stage": x.Stage, "tables": x.Tables, "transforms": x.Transforms}
	}
	return v
}
