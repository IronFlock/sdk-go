//go:build integration

package integration

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	ironflock "github.com/ironflock/sdk-go"
	"github.com/ironflock/sdk-go/wamp"
)

// hook calls a test hook of the fake platform and returns its result.
func hook(t *testing.T, ctx context.Context, ifl *ironflock.IronFlock, procedure string, args ...any) any {
	t.Helper()
	res, err := ifl.Call(ctx, procedure, args...)
	if err != nil {
		t.Fatalf("%s: %v", procedure, err)
	}
	return res.Value()
}

// resetPlatform resets the fake platform now and when the test ends (rows,
// chunk budgets).
func resetPlatform(t *testing.T, ctx context.Context, ifl *ironflock.IronFlock) {
	t.Helper()
	hook(t, ctx, ifl, "test.reset")
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, err := ifl.Call(ctx, "test.reset"); err != nil {
			t.Errorf("test.reset: %v", err)
		}
	})
}

// number returns a numeric payload value as a float64.
func number(t *testing.T, v any) float64 {
	t.Helper()
	switch n := v.(type) {
	case int64:
		return float64(n)
	case uint64:
		return float64(n)
	case float64:
		return n
	}
	t.Fatalf("%#v is not a number", v)
	return 0
}

// A read too large for one message of the data backend comes back in chunks
// (progressive results) and is reassembled: the rows equal a single read's,
// in its order. The fake platform chunks like fleetdb; the table "chunked"
// has a chunk budget of 512 bytes, and test.fleetdb.chunk_bytes sets others.
func TestChunkedReadsAreReassembled(t *testing.T) {
	deviceEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	ifl := startIronFlock(t, ctx)
	resetPlatform(t, ctx, ifl)

	const n = 40
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	rows := make([]ironflock.Row, n)
	for i := range rows {
		rows[i] = ironflock.Row{"tsp": start.Add(time.Duration(i) * time.Minute), "seq": i, "payload": strings.Repeat("p", 40)}
	}
	if got := hook(t, ctx, ifl, "test.fleetdb.insert", "chunked", rows); number(t, got) != n {
		t.Fatalf("inserted %v rows", got)
	}

	// The fake streams the read only to a caller that asks for chunks.
	query := []any{map[string]any{"limit": n}}
	var chunks atomic.Int32
	res, err := ifl.Connection().Call(ctx, "history.transformed.chunked", query, nil,
		&wamp.CallOptions{OnProgress: func(*wamp.Result) { chunks.Add(1) }}, 0)
	if err != nil {
		t.Fatal(err)
	}
	summary, _ := res.Value().(map[string]any)
	if summary["chunked"] != true || number(t, summary["chunkCount"]) != float64(chunks.Load()) || chunks.Load() < 2 ||
		number(t, summary["totalRows"]) != n {
		t.Fatalf("not a chunked answer: %v after %d chunks", res.Value(), chunks.Load())
	}
	if _, err := ifl.Connection().Call(ctx, "history.transformed.chunked", query, nil, nil, 0); ironflock.WampURI(err) != ironflock.URIResultTooLarge {
		t.Fatalf("a read without chunks: %v, want %s", err, ironflock.URIResultTooLarge)
	}

	chunked, err := ifl.GetHistory(ctx, "chunked", &ironflock.TableQueryParams{Limit: n})
	if err != nil {
		t.Fatal(err)
	}
	if len(chunked) != n {
		t.Fatalf("%d rows, want %d", len(chunked), n)
	}
	for i, r := range chunked {
		if number(t, r["seq"]) != float64(i) || number(t, r["tsp"]) != float64(start.Add(time.Duration(i)*time.Minute).UnixMilli()) {
			t.Fatalf("row %d is %v: not the rows in ascending order", i, r)
		}
	}
	// A page of the table: the newest 10 rows after the newest 5.
	page, err := ifl.GetHistory(ctx, "chunked", &ironflock.TableQueryParams{Limit: 10, Offset: 5})
	if err != nil || len(page) != 10 || number(t, page[0]["seq"]) != 25 || number(t, page[9]["seq"]) != 34 {
		t.Fatalf("page: %v, %v", page, err)
	}

	series := ironflock.SeriesQueryParams{
		Metrics:   []ironflock.SeriesMetric{{Ref: "seq", Method: ironflock.MethodMax}},
		Bucket:    time.Minute,
		Limit:     n,
		TimeRange: ironflock.Between(start, start.Add(n*time.Minute)),
	}
	hook(t, ctx, ifl, "test.fleetdb.chunk_bytes", "chunked", 64)
	chunkedSeries, err := ifl.GetSeriesHistory(ctx, "chunked", series)
	if err != nil {
		t.Fatal(err)
	}

	// The same reads in one message each.
	hook(t, ctx, ifl, "test.fleetdb.chunk_bytes", "chunked", 1<<30)
	single, err := ifl.GetHistory(ctx, "chunked", &ironflock.TableQueryParams{Limit: n})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(chunked, single) {
		t.Errorf("the reassembled rows differ from a single read:\n%v\n%v", chunked, single)
	}
	singleSeries, err := ifl.GetSeriesHistory(ctx, "chunked", series)
	if err != nil {
		t.Fatal(err)
	}
	if len(chunkedSeries) != n || !reflect.DeepEqual(chunkedSeries, singleSeries) {
		t.Errorf("the reassembled series differs from a single read:\n%v\n%v", chunkedSeries, singleSeries)
	}
	for i, r := range chunkedSeries {
		if number(t, r[series.Metrics[0].Column()]) != float64(i) {
			t.Fatalf("bucket %d is %v", i, r)
		}
	}

	// A consumed app's reads are reassembled as well.
	hook(t, ctx, ifl, "test.fleetdb.chunk_bytes", "readings", 200, ironflock.Kwargs{"provider": true})
	app, err := ifl.ConnectToApp(ctx, "weather")
	if err != nil {
		t.Fatal(err)
	}
	readings, err := app.GetHistory(ctx, "readings", &ironflock.TableQueryParams{Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(readings) != 2 || number(t, readings[0]["tsp"]) != 1767225600000 || number(t, readings[1]["tsp"]) != 1767229200000 {
		t.Fatalf("provider rows %v", readings)
	}
	count, err := app.GetSeriesHistory(ctx, "readings", ironflock.SeriesQueryParams{
		Metrics: []ironflock.SeriesMetric{{Ref: "tsp", Method: ironflock.MethodCount}}, Bucket: time.Hour, Limit: 2,
		TimeRange: &ironflock.TimeRange{Start: int64(1767225600000), End: int64(1767232800000)},
	})
	if err != nil || len(count) != 2 || number(t, count[0]["COUNT:tsp"]) != 1 || number(t, count[1]["COUNT:tsp"]) != 1 {
		t.Fatalf("provider series %v, %v", count, err)
	}
}

// GetSeriesHistory against fleetdb's series contract (the fake platform
// validates the request as fleetdb's typia check does and answers like
// fleetdb): metrics as {ref, method} pairs, buckets, groups, filter groups,
// the "<METHOD>:<ref>" columns, and the typed refusals.
func TestSeriesContract(t *testing.T) {
	deviceEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	ifl := startIronFlock(t, ctx)
	resetPlatform(t, ctx, ifl)

	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	var rows []ironflock.Row
	for i := range 12 { // every 10 minutes for two hours
		rows = append(rows, ironflock.Row{"tsp": start.Add(time.Duration(i) * 10 * time.Minute), "temperature": 20 + i, "n": i % 3})
	}
	hook(t, ctx, ifl, "test.fleetdb.insert", "sensordata", rows)

	avg := ironflock.SeriesMetric{Ref: "temperature", Method: ironflock.MethodAvg}
	maxT := ironflock.SeriesMetric{Ref: "temperature", Method: ironflock.MethodMax}
	count := ironflock.SeriesMetric{Ref: "tsp", Method: ironflock.MethodCount}
	q := ironflock.SeriesQueryParams{
		Metrics:   []ironflock.SeriesMetric{avg, maxT, count},
		Bucket:    time.Hour,
		Limit:     10,
		TimeRange: ironflock.Between(start, start.Add(2*time.Hour)),
		GroupBy:   []string{"device_key"},
		FilterAnd: []ironflock.Filter{ironflock.Or(ironflock.Where("n", "=", 0), ironflock.Where("n", "=", 1))},
	}
	got, err := ifl.GetSeriesHistory(ctx, "sensordata", q)
	if err != nil {
		t.Fatal(err)
	}
	// The first hour holds i = 0..5 (n = 0, 1, 2, 0, 1, 2): the filter keeps
	// 20, 21, 23 and 24; the second i = 6..11: 26, 27, 29 and 30.
	want := []map[string]float64{
		{"tsp": float64(start.UnixMilli()), "device_key": 42, avg.Column(): 22, maxT.Column(): 24, count.Column(): 4},
		{"tsp": float64(start.Add(time.Hour).UnixMilli()), "device_key": 42, avg.Column(): 28, maxT.Column(): 30, count.Column(): 4},
	}
	if len(got) != len(want) {
		t.Fatalf("%d rows, want %d: %v", len(got), len(want), got)
	}
	for i, w := range want {
		if len(got[i]) != len(w) {
			t.Errorf("row %d has the columns %v, want those of %v", i, got[i], w)
		}
		for col, v := range w {
			if number(t, got[i][col]) != v {
				t.Errorf("row %d: %s = %v, want %v", i, col, got[i][col], v)
			}
		}
	}

	// The data backend's typed refusals.
	for _, c := range []struct {
		name string
		edit func(*ironflock.SeriesQueryParams)
		uri  string
	}{
		{"unknown column", func(q *ironflock.SeriesQueryParams) {
			q.Metrics = []ironflock.SeriesMetric{{Ref: "nope", Method: "AVG"}}
		},
			ironflock.URIInvalidMetric},
		{"AVG of a string", func(q *ironflock.SeriesQueryParams) {
			q.Metrics = []ironflock.SeriesMetric{{Ref: "source", Method: "AVG"}}
		},
			ironflock.URIInvalidMetric},
		{"unknown group", func(q *ironflock.SeriesQueryParams) { q.GroupBy = []string{"nope"} }, ironflock.URIInvalidGroupBy},
		{"group and metric", func(q *ironflock.SeriesQueryParams) { q.GroupBy = []string{"temperature"} }, ironflock.URIInvalidGroupBy},
	} {
		bad := q
		c.edit(&bad)
		if _, err := ifl.GetSeriesHistory(ctx, "sensordata", bad); ironflock.WampURI(err) != c.uri {
			t.Errorf("%s: %v, want %s", c.name, err, c.uri)
		}
	}

	// The shape before fleetdb v1.0.58 (sent by ironflock-py and ironflock-js
	// before 1.9.1) is refused.
	_, err = ifl.Call(ctx, "history.transformed.series.sensordata", map[string]any{
		"metrics": []any{"temperature"}, "method": "AVG", "limit": 10, "timeRange": []any{"2026-01-01T00:00:00Z", nil}})
	if ironflock.WampURI(err) != wamp.URIRuntimeError || !strings.Contains(err.Error(), "SeriesMetric") {
		t.Errorf("the old series shape: %v", err)
	}

	// What the data backend would refuse is refused before it is sent.
	hook(t, ctx, ifl, "test.clear_recorded")
	noStart := q
	noStart.TimeRange = &ironflock.TimeRange{End: start}
	if _, err := ifl.GetSeriesHistory(ctx, "sensordata", noStart); !errors.Is(err, ironflock.ErrInvalidArgument) {
		t.Errorf("no start: %v", err)
	}
	if rec := hook(t, ctx, ifl, "test.recorded"); fmt.Sprint(rec) != "[]" {
		t.Errorf("a refused query reached the platform: %v", rec)
	}
}
