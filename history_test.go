package ironflock

import (
	"errors"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ironflock/sdk-go/wamp"
)

// answer makes the fake connection answer every call with v.
func answer(c *fakeConn, v any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.callFn = func(fakeCall) (*wamp.Result, error) { return resultOf(v), nil }
}

// fail makes the fake connection fail every call with err.
func fail(c *fakeConn, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.callFn = func(fakeCall) (*wamp.Result, error) { return nil, err }
}

func TestGetHistoryPayloads(t *testing.T) {
	f := flock(t)
	answer(f.own, []any{map[string]any{"temperature": 21.5}})

	rows, err := f.GetHistory(bg, "sensordata", nil)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(rows, []Row{{"temperature": 21.5}}) {
		t.Errorf("rows %#v", rows)
	}
	want := fakeCall{
		Procedure: "history.transformed.sensordata",
		Args:      []any{map[string]any{"limit": int64(10), "offset": int64(0)}},
		Window:    DefaultReconnectWindow,
	}
	if got := readCall(t, f.own.lastCall(t)); !reflect.DeepEqual(got, want) {
		t.Errorf("call %#v\nwant %#v", got, want)
	}

	q := &TableQueryParams{
		Limit:     100,
		FilterAnd: []Filter{Latest(), Where("device_key", "=", 42), Where("deleted", "=", false)},
		Columns:   []string{"temperature"},
		TimeRange: &TimeRange{Start: "2026-09-14T09:57:43Z", End: nil},
	}
	before := *q
	if _, err := f.GetHistory(bg, "sensordata", q); err != nil {
		t.Fatal(err)
	}
	wantPayload := map[string]any{
		"limit":  int64(100),
		"offset": int64(0),
		"filterAnd": []any{
			map[string]any{"latest": true},
			map[string]any{"column": "device_key", "operator": "=", "value": int64(42)},
			map[string]any{"column": "deleted", "operator": "=", "value": false},
		},
		"columns":   []any{"temperature"},
		"timeRange": []any{"2026-09-14T09:57:43Z", nil},
	}
	if got := f.own.lastCall(t).Args[0]; !reflect.DeepEqual(got, wantPayload) {
		t.Errorf("payload %#v\nwant %#v", got, wantPayload)
	}
	if !reflect.DeepEqual(*q, before) {
		t.Error("the caller's query was modified")
	}
}

func TestGetHistoryResultShapes(t *testing.T) {
	f := flock(t)
	answer(f.own, nil)
	rows, err := f.GetHistory(bg, "t", nil)
	if err != nil || rows == nil || len(rows) != 0 {
		t.Errorf("no result: %#v, %v (want an empty, non-nil slice)", rows, err)
	}
	f.own.callFn = func(fakeCall) (*wamp.Result, error) { return &wamp.Result{}, nil }
	if rows, err := f.GetHistory(bg, "t", nil); err != nil || len(rows) != 0 {
		t.Errorf("no args: %#v, %v", rows, err)
	}
	for _, bad := range []any{"call-result", map[string]any{"rows": 1}, []any{map[string]any{}, "x"}} {
		answer(f.own, bad)
		_, err := f.GetHistory(bg, "t", nil)
		var oerr *OperationError
		if !errors.As(err, &oerr) || !errors.Is(err, errUnexpectedResult) {
			t.Errorf("%#v: %v", bad, err)
			continue
		}
		mustContain(t, err.Error(), "getHistory('t') failed: unexpected result type")
	}
}

func TestGetHistoryValidation(t *testing.T) {
	f := flock(t)
	_, err := f.GetHistory(bg, "", nil)
	if !errors.Is(err, ErrInvalidArgument) || !strings.HasSuffix(err.Error(), "Tablename must not be empty!") {
		t.Errorf("empty table: %v", err)
	}
	_, err = f.GetHistory(bg, "t", &TableQueryParams{Limit: 0})
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("limit 0: %v", err)
	}
	mustContain(t, err.Error(), "Invalid query parameters: limit must be between 1 and 10000")
	_, err = f.GetHistory(bg, "t", &TableQueryParams{Limit: 1, FilterAnd: []Filter{{Column: "temperature"}}})
	if !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("incomplete predicate: %v", err)
	}
	if len(f.own.allCalls()) != 0 {
		t.Error("an invalid query reached the connection")
	}
}

func TestGetHistoryExplainsAMissingProcedure(t *testing.T) {
	f := flock(t)
	fail(f.own, wampErr(wamp.URINoSuchProcedure))
	_, err := f.GetHistory(bg, "missing_table", nil)
	want := "getHistory('missing_table') failed: history procedure 'history.transformed.missing_table' is not registered. " +
		"Check that the table is declared in the app's data-template and that the app's data backend is running."
	if err == nil || err.Error() != want {
		t.Errorf("error %v\nwant %q", err, want)
	}
	if WampURI(err) != wamp.URINoSuchProcedure {
		t.Error("the WAMP error must stay reachable")
	}

	fail(f.own, errors.New("no callee registered for procedure"))
	_, err = f.GetHistory(bg, "missing_table", nil)
	mustContain(t, err.Error(), "history procedure 'history.transformed.missing_table' is not registered")

	fail(f.own, errors.New("boom"))
	_, err = f.GetHistory(bg, "mytable", nil)
	if want := "getHistory('mytable') failed: boom"; err == nil || err.Error() != want {
		t.Errorf("error %v, want %q", err, want)
	}

	fail(f.own, wampErr("wamp.error.not_authorized", "nope"))
	_, err = f.GetHistory(bg, "mytable", nil)
	if want := `getHistory('mytable') failed with WAMP error 'wamp.error.not_authorized' — ["nope"]`; err == nil || err.Error() != want {
		t.Errorf("error %v, want %q", err, want)
	}
}

func TestGetSeriesHistory(t *testing.T) {
	f := flock(t)
	answer(f.own, []any{map[string]any{"tsp": int64(1767225600000), "AVG:temperature": 1.5}})
	q := SeriesQueryParams{
		Metrics:   []SeriesMetric{{Ref: "temperature", Method: MethodAvg}},
		Bucket:    time.Minute,
		Limit:     100,
		TimeRange: &TimeRange{Start: "2026-01-01T00:00:00.000Z"},
		FilterAnd: []Filter{Where("device_key", "=", 42)},
	}
	rows, err := f.GetSeriesHistory(bg, "sensordata", q)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0][q.Metrics[0].Column()] != 1.5 {
		t.Errorf("rows %#v", rows)
	}
	want := fakeCall{
		Procedure: "history.transformed.series.sensordata",
		Args: []any{map[string]any{
			"metrics":   []any{map[string]any{"ref": "temperature", "method": "AVG"}},
			"bucketMs":  int64(60000),
			"limit":     int64(100),
			"timeRange": []any{"2026-01-01T00:00:00.000Z", nil},
			"filterAnd": []any{map[string]any{"column": "device_key", "operator": "=", "value": int64(42)}},
		}},
		Window: DefaultReconnectWindow,
	}
	if got := readCall(t, f.own.lastCall(t)); !reflect.DeepEqual(got, want) {
		t.Errorf("call %#v\nwant %#v", got, want)
	}

	fail(f.own, errors.New("no callee registered for procedure"))
	_, err = f.GetSeriesHistory(bg, "missing_table", q)
	mustContain(t, err.Error(), "getSeriesHistory('missing_table') failed: history procedure 'history.transformed.series.missing_table' is not registered")
}

func TestGetSeriesHistoryValidation(t *testing.T) {
	f := flock(t)
	base := SeriesQueryParams{Metrics: []SeriesMetric{{"t", MethodAvg}}, Limit: 10, TimeRange: &TimeRange{Start: int64(0)}}
	if _, err := f.GetSeriesHistory(bg, "", base); !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("empty table: %v", err)
	}
	for name, mutate := range map[string]func(*SeriesQueryParams){
		"no metrics": func(q *SeriesQueryParams) { q.Metrics = nil },
		"method":     func(q *SeriesQueryParams) { q.Metrics = []SeriesMetric{{"t", "MEDIAN"}} },
		"lower case": func(q *SeriesQueryParams) { q.Metrics = []SeriesMetric{{"t", "avg"}} },
		"limit":      func(q *SeriesQueryParams) { q.Limit = 10001 },
		"time range": func(q *SeriesQueryParams) { q.TimeRange = nil },
		"no start":   func(q *SeriesQueryParams) { q.TimeRange = &TimeRange{} },
		"mixed":      func(q *SeriesQueryParams) { q.TimeRange = &TimeRange{Start: "2026-01-01T00:00:00Z", End: int64(1)} },
		"NaN":        func(q *SeriesQueryParams) { q.TimeRange = &TimeRange{Start: math.NaN()} },
		"bucket":     func(q *SeriesQueryParams) { q.Bucket = time.Millisecond },
	} {
		q := base
		mutate(&q)
		_, err := f.GetSeriesHistory(bg, "t", q)
		if !errors.Is(err, ErrInvalidArgument) || !strings.Contains(err.Error(), "Invalid series query parameters") {
			t.Errorf("%s: %v", name, err)
		}
	}
	for _, filter := range []Filter{Latest(), Where("latest_flag", "=", true), Or(Latest())} {
		q := base
		q.FilterAnd = []Filter{filter}
		_, err := f.GetSeriesHistory(bg, "t", q)
		if !errors.Is(err, ErrInvalidArgument) || !strings.Contains(err.Error(), "not supported in series queries") {
			t.Errorf("latest marker %#v: %v", filter, err)
		}
	}
	if len(f.own.allCalls()) != 0 {
		t.Error("an invalid query reached the connection")
	}
}

func TestRevealSecrets(t *testing.T) {
	f := flock(t)
	answer(f.own, []any{map[string]any{"api_token": "hunter2"}})
	rows, err := f.RevealSecrets(bg, "credentials", &TableQueryParams{Limit: 5, FilterAnd: []Filter{Latest()}})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(rows, []Row{{"api_token": "hunter2"}}) {
		t.Errorf("rows %#v", rows)
	}
	want := fakeCall{
		Procedure: "secret.reveal.credentials",
		Args:      []any{map[string]any{"limit": int64(5), "offset": int64(0), "filterAnd": []any{map[string]any{"latest": true}}}},
		Window:    DefaultReconnectWindow,
	}
	if got := readCall(t, f.own.lastCall(t)); !reflect.DeepEqual(got, want) {
		t.Errorf("call %#v\nwant %#v", got, want)
	}

	if _, err := f.RevealSecrets(bg, "credentials", nil); err != nil {
		t.Fatal(err)
	}
	if got := f.own.lastCall(t).Args[0]; !reflect.DeepEqual(got, map[string]any{"limit": int64(10), "offset": int64(0)}) {
		t.Errorf("default payload %#v", got)
	}

	calls := len(f.own.allCalls())
	_, err = f.RevealSecrets(bg, "credentials", &TableQueryParams{Limit: 101})
	if !errors.Is(err, ErrInvalidArgument) || !strings.Contains(err.Error(), "Invalid query parameters: limit must be between 1 and 100") {
		t.Errorf("limit 101: %v", err)
	}
	if _, err := f.RevealSecrets(bg, "", nil); !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("empty table: %v", err)
	}
	if len(f.own.allCalls()) != calls {
		t.Error("an invalid query reached the connection")
	}

	fail(f.own, errors.New("no callee registered for procedure"))
	_, err = f.RevealSecrets(bg, "credentials", nil)
	// The text the three SDKs share, after the procedure part.
	want2 := "revealSecrets('credentials') failed: procedure 'secret.reveal.credentials' is not registered. " +
		"The data backend registers it only for a table that declares a secret column: " +
		"the table declares no secret column (mark one `secret: true` in the app's data-template), " +
		"the table is not declared in the data-template, or the app's data backend predates secret columns (update it)."
	if err == nil || err.Error() != want2 {
		t.Errorf("error %v\nwant %q", err, want2)
	}
	wantSecretCausesInOrder(t, err)
}

// wantSecretCausesInOrder fails unless err, a secret procedure that is not
// registered, names its causes most likely first: the data backend registers
// secret.reveal and secret.verify only for a table that declares a secret
// column, so a table without one comes first, then a table that is not
// declared, then a data backend older than secret columns.
func wantSecretCausesInOrder(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("no error")
	}
	msg := err.Error()
	last := -1
	for _, cause := range []string{"declares no secret column", "`secret: true`", "is not declared", "predates secret columns"} {
		i := strings.Index(msg, cause)
		if i < 0 {
			t.Errorf("%q does not name %q", msg, cause)
			return
		}
		if i < last {
			t.Errorf("%q names %q out of order", msg, cause)
		}
		last = i
	}
}

func TestVerifySecret(t *testing.T) {
	f := flock(t)
	answer(f.own, map[string]any{"match": true, "checked": int64(1)})
	res, err := f.VerifySecret(bg, "credentials", "api_key", "s3cr3t", nil)
	if err != nil {
		t.Fatal(err)
	}
	if *res != (SecretVerifyResult{Match: true, Checked: 1}) {
		t.Errorf("result %#v", res)
	}
	want := fakeCall{
		Procedure: "secret.verify.credentials",
		Args:      []any{map[string]any{"limit": int64(1), "offset": int64(0), "column": "api_key", "candidate": "s3cr3t"}},
		Window:    DefaultReconnectWindow,
	}
	if got := f.own.lastCall(t); !reflect.DeepEqual(got, want) {
		t.Errorf("call %#v\nwant %#v", got, want)
	}

	q := &TableQueryParams{Limit: 3, FilterAnd: []Filter{Latest()}}
	if _, err := f.VerifySecret(bg, "credentials", "api_key", "", q); err != nil {
		t.Fatal(err)
	}
	wantPayload := map[string]any{
		"limit": int64(3), "offset": int64(0), "filterAnd": []any{map[string]any{"latest": true}},
		"column": "api_key", "candidate": "",
	}
	if got := f.own.lastCall(t).Args[0]; !reflect.DeepEqual(got, wantPayload) {
		t.Errorf("payload %#v", got)
	}
	if q.Limit != 3 || len(q.FilterAnd) != 1 {
		t.Error("the caller's query was modified")
	}
}

func TestVerifySecretFailsClosed(t *testing.T) {
	f := flock(t)
	cases := []struct {
		answer any
		want   SecretVerifyResult
	}{
		{map[string]any{"match": false, "checked": int64(3)}, SecretVerifyResult{Checked: 3}},
		{map[string]any{"match": false, "checked": int64(0)}, SecretVerifyResult{}},
		{map[string]any{"match": true, "checked": 2.0}, SecretVerifyResult{Match: true, Checked: 2}},
		{map[string]any{"match": true}, SecretVerifyResult{Match: true}},
		{map[string]any{"match": "true", "checked": int64(1)}, SecretVerifyResult{Checked: 1}},
		{map[string]any{"match": int64(1)}, SecretVerifyResult{}},
		{map[string]any{"checked": "many"}, SecretVerifyResult{}},
		{"call-result", SecretVerifyResult{}},
		{[]any{map[string]any{"match": true}}, SecretVerifyResult{}},
		{nil, SecretVerifyResult{}},
	}
	for _, tc := range cases {
		answer(f.own, tc.answer)
		res, err := f.VerifySecret(bg, "credentials", "api_key", "x", nil)
		if err != nil {
			t.Fatal(err)
		}
		if *res != tc.want {
			t.Errorf("answer %#v: %#v, want %#v", tc.answer, *res, tc.want)
		}
	}
}

func TestVerifySecretValidationAndErrors(t *testing.T) {
	f := flock(t)
	_, err := f.VerifySecret(bg, "credentials", "", "x", nil)
	if !errors.Is(err, ErrInvalidArgument) || !strings.Contains(err.Error(), "Invalid verify parameters") {
		t.Errorf("empty column: %v", err)
	}
	_, err = f.VerifySecret(bg, "credentials", "api_key", "x", &TableQueryParams{Limit: 500})
	if !errors.Is(err, ErrInvalidArgument) || !strings.Contains(err.Error(), "Invalid verify parameters") {
		t.Errorf("limit 500: %v", err)
	}
	if len(f.own.allCalls()) != 0 {
		t.Error("an invalid query reached the connection")
	}
	fail(f.own, wampErr(wamp.URINoSuchProcedure))
	_, err = f.VerifySecret(bg, "credentials", "api_key", "x", nil)
	mustContain(t, err.Error(), "verifySecret('credentials') failed: procedure 'secret.verify.credentials' is not registered. "+
		"The data backend registers it only for a table that declares a secret column: the table declares no secret column")
	wantSecretCausesInOrder(t, err)
	if WampURI(err) != wamp.URINoSuchProcedure {
		t.Errorf("the hint hides the refusal: %v", err)
	}
}

// row returns a row with the given tsp.
func row(tsp int64) map[string]any { return map[string]any{"tsp": tsp, "v": float64(tsp) / 2} }

// rowList returns the rows with the given tsps, as a payload list.
func rowList(tsps ...int64) []any {
	out := make([]any, len(tsps))
	for i, tsp := range tsps {
		out[i] = row(tsp)
	}
	return out
}

// summary is fleetdb's final result of a chunked read.
func summary(chunkCount, totalRows int64, order any) map[string]any {
	return map[string]any{"chunked": true, "chunkCount": chunkCount, "totalRows": totalRows, "order": order}
}

// chunkedReads are the reads that ask for progressive results and reassemble
// a chunked answer, run against tf (own realm) and app (a consumed app).
func chunkedReads(tf *testFlock, app *ConsumedApp) map[string]func() ([]Row, error) {
	series := SeriesQueryParams{Metrics: []SeriesMetric{{"temperature", MethodAvg}}, Limit: 10, TimeRange: &TimeRange{Start: int64(0)}}
	return map[string]func() ([]Row, error){
		"GetHistory":       func() ([]Row, error) { return tf.GetHistory(bg, "readings", &TableQueryParams{Limit: 100}) },
		"GetSeriesHistory": func() ([]Row, error) { return tf.GetSeriesHistory(bg, "readings", series) },
		"RevealSecrets":    func() ([]Row, error) { return tf.RevealSecrets(bg, "readings", &TableQueryParams{Limit: 100}) },
		"ConsumedApp.GetHistory": func() ([]Row, error) {
			return app.GetHistory(bg, "readings", &TableQueryParams{Limit: 100})
		},
		"ConsumedApp.GetSeriesHistory": func() ([]Row, error) { return app.GetSeriesHistory(bg, "readings", series) },
	}
}

// A result too large for one message of the data backend arrives in chunks
// — progressive results [chunkIndex, rows], the rows newest first — and a
// summary {chunked, chunkCount, totalRows, order}: the reads reassemble the
// rows of a single read, in its order.
func TestReadsReassembleChunkedResults(t *testing.T) {
	c := newConsumer(t)
	app, err := c.ConnectToApp(bg, "weatherstation")
	if err != nil {
		t.Fatal(err)
	}
	provider := c.factory.all()[0]
	cases := []struct {
		name     string
		progress [][]any
		final    any
		want     []any
	}{
		{"ascending", [][]any{{int64(0), rowList(6, 5)}, {int64(1), rowList(4, 3)}, {int64(2), rowList(2, 1)}},
			summary(3, 6, "asc"), rowList(1, 2, 3, 4, 5, 6)},
		{"descending", [][]any{{int64(0), rowList(6, 5)}, {int64(1), rowList(4, 3)}}, summary(2, 4, "desc"), rowList(6, 5, 4, 3)},
		{"chunks in another order", [][]any{{int64(1), rowList(2, 1)}, {int64(0), rowList(4, 3)}}, summary(2, 4, "asc"),
			rowList(1, 2, 3, 4)},
		{"numbers as floats", [][]any{{0.0, rowList(2, 1)}}, map[string]any{"chunked": true, "chunkCount": 1.0,
			"totalRows": uint64(2), "order": "asc"}, rowList(1, 2)},
		{"an empty chunked result", nil, summary(0, 0, "asc"), []any{}},
		{"a plain list", nil, rowList(1, 2), rowList(1, 2)},
		{"no rows", nil, nil, []any{}},
	}
	for _, tc := range cases {
		want := make([]Row, len(tc.want))
		for i, r := range tc.want {
			want[i] = r.(map[string]any)
		}
		for name, read := range chunkedReads(c.testFlock, app) {
			answerProgressively(c.own, tc.progress, tc.final)
			answerProgressively(provider, tc.progress, tc.final)
			rows, err := read()
			if err != nil || !reflect.DeepEqual(rows, want) {
				t.Errorf("%s, %s: %v, %v\nwant %v", tc.name, name, rows, err, want)
			}
		}
	}
}

// A chunked result that does not add up fails the read, rather than
// returning part of the rows.
func TestReadsRefuseBrokenChunkedResults(t *testing.T) {
	c := newConsumer(t)
	app, err := c.ConnectToApp(bg, "weatherstation")
	if err != nil {
		t.Fatal(err)
	}
	provider := c.factory.all()[0]
	cases := []struct {
		name     string
		progress [][]any
		final    any
		want     string
	}{
		{"a missing chunk", [][]any{{int64(0), rowList(3)}, {int64(2), rowList(1)}, {int64(3), rowList(0)}},
			summary(3, 3, "asc"), "chunk 1 of 3 did not arrive"},
		{"fewer chunks", [][]any{{int64(0), rowList(2, 1)}}, summary(2, 2, "asc"), "a chunked result of 2 chunk(s) sent 1"},
		{"more chunks", [][]any{{int64(0), rowList(2)}, {int64(1), rowList(1)}}, summary(1, 2, "asc"),
			"a chunked result of 1 chunk(s) sent 2"},
		{"a duplicate chunk", [][]any{{int64(0), rowList(2)}, {int64(0), rowList(1)}}, summary(1, 2, "asc"), "chunk 0 arrived twice"},
		{"rows miscounted", [][]any{{int64(0), rowList(2, 1)}}, summary(1, 3, "asc"), "a chunked result of 3 row(s) sent 2"},
		{"an unknown order", [][]any{{int64(0), rowList(1)}}, summary(1, 1, "sideways"), `unknown order "sideways"`},
		{"no order", [][]any{{int64(0), rowList(1)}}, summary(1, 1, nil), "unknown order null"},
		{"a malformed summary", [][]any{{int64(0), rowList(1)}}, summary(1, 1, "asc"), "malformed"},
		{"a negative count", nil, summary(-1, 0, "asc"), "malformed"},
		{"not a pair", [][]any{{int64(0)}}, summary(1, 0, "asc"), "a progressive result is not [chunkIndex, rows]: [0]"},
		{"a string index", [][]any{{"0", rowList(1)}}, summary(1, 1, "asc"), `the chunk index "0"`},
		{"a negative index", [][]any{{int64(-1), rowList(1)}}, summary(1, 1, "asc"), "the chunk index -1"},
		{"a fractional index", [][]any{{0.5, rowList(1)}}, summary(1, 1, "asc"), "the chunk index 0.5"},
		{"rows not a list", [][]any{{int64(0), "rows"}}, summary(1, 1, "asc"), "chunk 0 is string, not a list of rows"},
		{"a row not an object", [][]any{{int64(0), []any{"x"}}}, summary(1, 1, "asc"), "a row of chunk 0 is string, not an object"},
		{"a list after chunks", [][]any{{int64(0), rowList(1)}}, rowList(1), "1 chunk(s) of rows arrived, but the result is not their summary"},
		{"nothing after chunks", [][]any{{int64(0), rowList(1)}}, nil, "1 chunk(s) of rows arrived, but the result is not their summary: null"},
	}
	for _, tc := range cases {
		final := tc.final
		if tc.name == "a malformed summary" {
			s := summary(1, 1, "asc")
			s["chunkCount"] = "1"
			final = s
		}
		for name, read := range chunkedReads(c.testFlock, app) {
			answerProgressively(c.own, tc.progress, final)
			answerProgressively(provider, tc.progress, final)
			rows, err := read()
			var oerr *OperationError
			if !errors.As(err, &oerr) || !errors.Is(err, errUnexpectedResult) || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("%s, %s: %v, %v; want an unexpected result: %s", tc.name, name, rows, err, tc.want)
			}
		}
	}
}

// Without progressive results, a large read is refused (as before fleetdb
// streamed them): the reads ask for them, VerifySecret, whose answer is
// small, does not.
func TestReadsAskForProgressiveResults(t *testing.T) {
	c := newConsumer(t)
	app, err := c.ConnectToApp(bg, "weatherstation")
	if err != nil {
		t.Fatal(err)
	}
	provider := c.factory.all()[0]
	for name, read := range chunkedReads(c.testFlock, app) {
		answerProgressively(c.own, [][]any{{int64(0), rowList(1)}}, summary(1, 1, "asc"))
		answerProgressively(provider, [][]any{{int64(0), rowList(1)}}, summary(1, 1, "asc"))
		if rows, err := read(); err != nil || len(rows) != 1 {
			t.Errorf("%s: %v, %v", name, rows, err)
		}
	}
	for _, call := range append(c.own.allCalls(), provider.allCalls()...) {
		if call.Procedure != uriAppAccessResolve {
			readCall(t, call)
		}
	}
	answer(c.own, map[string]any{"match": true, "checked": int64(1)})
	if _, err := c.VerifySecret(bg, "credentials", "api_key", "x", nil); err != nil {
		t.Fatal(err)
	}
	if opts := c.own.lastCall(t).Opts; opts != nil {
		t.Errorf("VerifySecret options %#v", opts)
	}
}

// A bound the data backend would read as an open one cannot be sent: no
// call is made.
func TestReadsRejectNonFiniteTimeRangeBounds(t *testing.T) {
	c := newConsumer(t)
	app, err := c.ConnectToApp(bg, "weatherstation")
	if err != nil {
		t.Fatal(err)
	}
	provider := c.factory.all()[0]
	calls := len(c.own.allCalls())
	nan := &TableQueryParams{Limit: 1, TimeRange: &TimeRange{Start: math.NaN()}}
	series := SeriesQueryParams{Metrics: []SeriesMetric{{"t", MethodAvg}}, Limit: 1, TimeRange: &TimeRange{Start: int64(0), End: math.Inf(1)}}
	for name, err := range map[string]error{
		"GetHistory": func() error { _, err := c.GetHistory(bg, "t", nan); return err }(),
		"GetSeriesHistory": func() error {
			_, err := c.GetSeriesHistory(bg, "t", series)
			return err
		}(),
		"RevealSecrets": func() error { _, err := c.RevealSecrets(bg, "t", nan); return err }(),
		"VerifySecret":  func() error { _, err := c.VerifySecret(bg, "t", "c", "x", nan); return err }(),
		"ConsumedApp.GetHistory": func() error {
			_, err := app.GetHistory(bg, "readings", nan)
			return err
		}(),
		"ConsumedApp.GetSeriesHistory": func() error {
			_, err := app.GetSeriesHistory(bg, "readings", series)
			return err
		}(),
	} {
		if !errors.Is(err, ErrInvalidArgument) || !strings.Contains(err.Error(), "finite epoch-ms numbers") {
			t.Errorf("%s: %v", name, err)
		}
	}
	if len(c.own.allCalls()) != calls || len(provider.allCalls()) != 0 {
		t.Error("a non-finite bound reached the connection")
	}
}
