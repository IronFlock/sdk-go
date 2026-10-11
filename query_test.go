package ironflock

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestQueryWireMinimal(t *testing.T) {
	got, err := queryWire(&TableQueryParams{Limit: 10}, MaxQueryLimit, nil)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]any{"limit": int64(10), "offset": int64(0)}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %#v, want %#v", got, want)
	}
}

func TestQueryWireFull(t *testing.T) {
	q := &TableQueryParams{
		Limit:     500,
		Offset:    20,
		TimeRange: &TimeRange{Start: "2026-01-01T00:00:00Z", End: nil},
		FilterAnd: []Filter{
			Where("temperature", ">", 20),
			Latest(),
			Or(IsNull("deleted"), Where("deleted", "=", false)),
			Where("id", "IN", []int{1, 2}),
		},
		Columns: []string{"temperature"},
	}
	got, err := queryWire(q, MaxQueryLimit, nil)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]any{
		"limit":     int64(500),
		"offset":    int64(20),
		"timeRange": []any{"2026-01-01T00:00:00Z", nil},
		"filterAnd": []any{
			map[string]any{"column": "temperature", "operator": ">", "value": int64(20)},
			map[string]any{"latest": true},
			map[string]any{"combinator": "OR", "filters": []any{
				map[string]any{"column": "deleted", "operator": "IS NULL"},
				map[string]any{"column": "deleted", "operator": "=", "value": false},
			}},
			map[string]any{"column": "id", "operator": "IN", "value": []any{int64(1), int64(2)}},
		},
		"columns": []any{"temperature"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got  %#v\nwant %#v", got, want)
	}
}

func TestQueryWireRejects(t *testing.T) {
	cases := map[string]*TableQueryParams{
		"zero limit":         {Limit: 0},
		"limit too large":    {Limit: 10001},
		"negative offset":    {Limit: 1, Offset: -1},
		"mixed time range":   {Limit: 1, TimeRange: &TimeRange{Start: "2026-01-01T00:00:00Z", End: 5}},
		"bad iso":            {Limit: 1, TimeRange: &TimeRange{Start: "yesterday"}},
		"empty column":       {Limit: 1, FilterAnd: []Filter{{Operator: "=", Value: 1}}},
		"missing value":      {Limit: 1, FilterAnd: []Filter{Where("a", "=", nil)}},
		"IN needs list":      {Limit: 1, FilterAnd: []Filter{Where("a", "IN", 3)}},
		"list for =":         {Limit: 1, FilterAnd: []Filter{Where("a", "=", []int{1})}},
		"IS NULL with value": {Limit: 1, FilterAnd: []Filter{{Column: "a", Operator: "IS NULL", Value: 1}}},
		"bad combinator":     {Limit: 1, FilterAnd: []Filter{{Combinator: "XOR", Filters: []Filter{IsNull("a")}}}},
		"empty group":        {Limit: 1, FilterAnd: []Filter{Or()}},
	}
	for name, q := range cases {
		if _, err := queryWire(q, MaxQueryLimit, nil); !errors.Is(err, ErrInvalidArgument) {
			t.Errorf("%s: want ErrInvalidArgument, got %v", name, err)
		}
	}
	if _, err := queryWire(&TableQueryParams{Limit: 101}, MaxSecretLimit, nil); !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("secret limit: want ErrInvalidArgument, got %v", err)
	}
}

func TestTimeRangeForms(t *testing.T) {
	start := time.Date(2026, 1, 2, 3, 4, 5, 0, time.FixedZone("x", 3600))
	got, err := timeRangeWire(Since(start))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, []any{"2026-01-02T02:04:05Z", nil}) {
		t.Fatalf("got %#v", got)
	}
	got, err = timeRangeWire(&TimeRange{Start: int64(1700000000000), End: uint64(1700000001000)})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, []any{int64(1700000000000), int64(1700000001000)}) {
		t.Fatalf("got %#v", got)
	}
	for _, s := range []string{"2026-01-01", "2026-01-01T10:00:00", "2026-01-01T10:00:00.123+02:00", "2026-01-01 10:00:00"} {
		if _, err := timeRangeWire(&TimeRange{Start: s}); err != nil {
			t.Errorf("%q: %v", s, err)
		}
	}
}

// isoTimeCase is a TimeRange string the data backend reads, with the time
// value (ms since the epoch) it reads it as.
type isoTimeCase struct {
	s  string
	ms int64
}

// The data backend (fleetdb) turns each string bound of a TimeRange into a
// time with JavaScript's new Date(s).getTime(). It runs on Bun 1.3.13 (the
// oven/bun:1.3.13 image of fleetdb-service's Dockerfile), which reads every
// string below exactly as V8 does: the values were recorded with
// new Date(s).getTime() in the oven/bun:1.3.13, node:20 and node:22 images,
// in the time zone the data backend runs in, UTC.
var (
	// acceptedISOTimes are ISO 8601 dates and date-times in extended format
	// that the data backend reads correctly, with the time value it reads.
	acceptedISOTimes = []isoTimeCase{
		{"2026", 1767225600000},
		{"2026-01", 1767225600000},
		{"2026-02", 1769904000000},
		{"2026-12", 1796083200000},
		{"2026-01-01", 1767225600000},
		{"2026-06-30", 1782777600000},
		{"0000", -62167219200000},
		{"0000-01", -62167219200000},
		{"0000-01-01", -62167219200000},
		{"9999-12-31", 253402214400000},
		{"2026-01-01T10:00", 1767261600000},
		{"2026-01-01T10:00Z", 1767261600000},
		{"2026-01-01T10:00+02:00", 1767254400000},
		{"2026-01-01T10:00+0200", 1767254400000},
		{"2026-01-01T10:00:00", 1767261600000},
		{"2026-01-01T10:00:00Z", 1767261600000},
		{"2026-01-01T00:00:00.000Z", 1767225600000},
		{"1969-12-31T23:59:59.999Z", -1},
		{"1970-01-01T00:00:00.000+00:00", 0},
		{"0000-01-01T00:00:00Z", -62167219200000},
		{"0001-01-01T00:00:00Z", -62135596800000},
		{"9999-12-31T23:59:59Z", 253402300799000},
		// Leap days, in leap years only.
		{"2028-02-29T00:00:00Z", 1835395200000},
		{"2024-02-29T10:00Z", 1709200800000},
		{"2000-02-29T00:00:00Z", 951782400000},
		// Fractions of any length; only milliseconds count.
		{"2026-01-01T10:00:00.5", 1767261600500},
		{"2026-01-01T10:00:00.5Z", 1767261600500},
		{"2026-01-01T10:00:00.1Z", 1767261600100},
		{"2026-01-01T10:00:00.12Z", 1767261600120},
		{"2026-01-01T10:00:00.123Z", 1767261600123},
		{"2026-01-01T10:00:00.1234Z", 1767261600123},
		{"2026-01-01T10:00:00.123456Z", 1767261600123},
		{"2026-01-01T10:00:00.123456789Z", 1767261600123},
		{"2026-01-01T10:00:00.1234567891Z", 1767261600123},
		{"2026-01-01T10:00:00.999999Z", 1767261600999},
		{"2026-01-01T10:00:00.9999999999Z", 1767261600999},
		{"2026-01-01T10:00:00.12345678901234567890Z", 1767261600123},
		{"2026-01-01T10:00:00.12345678901234567890", 1767261600123},
		{"2026-01-01T10:00:00.123456789012345678901234567890123456789012345678901234567890Z", 1767261600123},
		// UTC offsets, with or without a colon.
		{"2026-01-01T10:00:00+02:00", 1767254400000},
		{"2026-01-01T10:00:00-05:30", 1767281400000},
		{"2026-01-01T10:00:00+00:00", 1767261600000},
		{"2026-01-01T10:00:00-00:00", 1767261600000},
		{"2026-01-01T10:00:00+14:00", 1767211200000},
		{"2026-01-01T10:00:00-12:00", 1767304800000},
		{"2026-01-01T10:00:00+23:59", 1767175260000},
		{"2026-01-01T10:00:00.123+02:00", 1767254400123},
		{"2026-01-01T10:00:00.123456789-05:30", 1767281400123},
		{"2026-01-01T10:00:00.5+02:00", 1767254400500},
		{"2026-01-01T10:00:00+0200", 1767254400000},
		{"2026-01-01T10:00:00+0000", 1767261600000},
		{"2026-01-01T10:00:00-0530", 1767281400000},
		{"2026-01-01T10:00:00+0130", 1767256200000},
		{"2026-01-01T10:00:00+2359", 1767175260000},
		{"2026-01-01T10:00:00-2359", 1767347940000},
		{"2026-01-01T10:00:00.123+0200", 1767254400123},
		{"2026-01-01T10:00:00.5+0200", 1767254400500},
		{"2026-01-01T10:00:00.5-0000", 1767261600500},
		// A space or a lower-case t between date and time, a lower-case z.
		{"2026-01-01 10:00", 1767261600000},
		{"2026-01-01 10:00:00", 1767261600000},
		{"2026-01-01 10:00:00Z", 1767261600000},
		{"2026-01-01 10:00:00+02:00", 1767254400000},
		{"2026-01-01 10:00:00+0200", 1767254400000},
		{"2026-01-01 10:00:00.123Z", 1767261600123},
		{"2026-01-01 10:00:00.5", 1767261600500},
		{"2026-01-01 10:00:00.5+02:00", 1767254400500},
		{"2026-01-01 10:00:00z", 1767261600000},
		{"2026-01-01 10:00z", 1767261600000},
		{"2026-01-01t10:00", 1767261600000},
		{"2026-01-01t10:00:00z", 1767261600000},
		{"2026-01-01t10:00:00Z", 1767261600000},
		{"2026-01-01T10:00:00z", 1767261600000},
		{"2026-01-01T10:00z", 1767261600000},
		{"2026-01-01T10:00:00.123z", 1767261600123},
		// 24:00, the end of the day.
		{"2026-01-01T24:00", 1767312000000},
		{"2026-01-01T24:00Z", 1767312000000},
		{"2026-01-01T24:00:00", 1767312000000},
		{"2026-01-01T24:00:00Z", 1767312000000},
		{"2026-01-01T24:00:00.0", 1767312000000},
		{"2026-01-01T24:00:00.000Z", 1767312000000},
		{"2026-01-01 24:00:00Z", 1767312000000},
		{"2026-12-31T24:00:00Z", 1798761600000},
		{"2026-12-31T24:00:00+02:00", 1798754400000},
		{"9999-12-31T24:00:00Z", 253402300800000},
		// Six-digit years with a sign, within JavaScript's Date range
		// (±8.64e15 ms).
		{"+002026", 1767225600000},
		{"+002026-01", 1767225600000},
		{"+002026-01-01", 1767225600000},
		{"+002026-01-01T00:00:00Z", 1767225600000},
		{"+002026-01-01T10:00:00", 1767261600000},
		{"+002026-01-01T10:00:00.5+02:00", 1767254400500},
		{"+000000-01-01T00:00:00Z", -62167219200000},
		{"-000001", -62198755200000},
		{"-000001-01-01T00:00:00Z", -62198755200000},
		{"+010000-01-01T00:00:00Z", 253402300800000},
		{"+275760-09-13", 8640000000000000},
		{"+275760-09-13T00:00:00", 8640000000000000},
		{"+275760-09-13T00:00:00Z", 8640000000000000},
		{"+275760-09-13T00:00:00.0001Z", 8640000000000000},
		{"+275760-09-13T00:00:00.000999Z", 8640000000000000},
		{"+275760-09-12T24:00:00Z", 8640000000000000},
		{"+275760-09-13T01:00:00+02:00", 8639999996400000},
		{"-271821-04-20", -8640000000000000},
		{"-271821-04-20T00:00:00", -8640000000000000},
		{"-271821-04-20T00:00:00Z", -8640000000000000},
		{"-271821-04-19T24:00:00", -8640000000000000},
		{"-271821-04-19T24:00:00Z", -8640000000000000},
		// Years 0-99 with T; a space from the year 100 on, and in negative
		// years.
		{"0099-03-30T23:59:00Z", -59035305660000},
		{"0001-01-01T00:00:00", -62135596800000},
		{"0100-01-01 00:00", -59011459200000},
		{"0100-01-01 00:00Z", -59011459200000},
		{"+000100-01-01 00:00Z", -59011459200000},
		{"-000001-06-15 12:00", -62184456000000},
	}

	// rejectedISOTimes are strings the data backend cannot read (new Date
	// returns an invalid date) or reads as another time than they denote,
	// and strings that are not ISO 8601 in extended format although
	// JavaScript reads them.
	rejectedISOTimes = []string{
		"", "not-a-date", " ", "2026-01-01T", "2026-01-01TZ",
		// Basic format, week and ordinal dates.
		"20260101", "20260101T100000Z", "20260101T10:00:00Z", "2026-01-01T100000Z",
		"2026-W01-1", "2026-001", "2026-032", "2026-060",
		// Hours only, a missing digit, a fraction of a minute or an hour.
		"2026-01-01T10", "2026-01-01T10Z", "2026-01-01 10", "2026-01-01 10Z",
		"2026-01-01T1:00:00Z", "2026-01-01T10:0:00Z", "2026-01-01T10.5", "2026-01-01T10:30.5",
		// Fractions without digits, with a comma, with a sign or other digits.
		"2026-01-01T10:00:00.Z", "2026-01-01T10:00:00,5Z", "2026-01-01T10:00:00.-5Z",
		"2026-01-01T10:00:00.+5Z", "2026-01-01T10:00:00.５Z",
		// Offsets of hours only, with seconds, out of range, malformed.
		"2026-01-01T10:00:00+02", "2026-01-01T10:00:00-05", "2026-01-01T10:00:00+00:00:00",
		"2026-01-01T10:00:00+24:00", "2026-01-01T10:00:00+25:00", "2026-01-01T10:00:00+02:60",
		"2026-01-01T10:00:00+0060", "2026-01-01T10:00:00+2400", "2026-01-01T10:00:00+01:3",
		"2026-01-01T10:00:00+013", "2026-01-01T10:00:00+01300", "2026-01-01T10:00:00+2:00",
		"2026-01-01T10:00:00GMT", "2026-01-01T10:00:00UTC", "2026-01-01T10:00:00+00:00Z",
		"2026-01-01T10:00:00ZZ", "2026-01-01T10:00:00Z+02:00",
		// Out-of-range fields; leap seconds.
		"2026-00", "2026-13", "2026-13-01T00:00:00Z", "2026-00-01T00:00:00Z", "2026-01-00T00:00:00Z",
		"2026-01-32T00:00:00Z", "2026-01-01T25:00:00Z", "2026-01-01T10:60:00Z", "2026-01-01T10:00:60Z",
		"2026-01-01T23:59:60Z", "2026-01-01T24:00:00.001Z", "2026-01-01T24:00:01Z", "2026-01-01T24:01:00Z",
		// Days a month does not have: JavaScript moves them into the next
		// month (2026-02-30 reads as 2026-03-02).
		"2026-02-29", "2026-02-29T10:00Z", "2026-02-29T00:00:00Z", "2026-02-30T00:00:00Z",
		"2026-02-30 00:00:00Z", "2026-04-31T00:00:00Z", "2026-06-31", "2100-02-29T00:00:00Z",
		"1900-02-29T00:00:00Z",
		// Not ISO 8601, although JavaScript reads them: an offset without a
		// time, a time after a reduced date, missing zeros, other spaces and
		// separators, other digits.
		"2026-01-01Z", "2026-01Z", "2026Z", "2026-01-01+02:00", "2026-01T10:00Z", "2026T10:00Z",
		"2026-01T10:00", "2026-1-1", "2026-01-1", "2026-01-01  10:00:00Z", "2026-01-01_10:00:00Z",
		"2026-01-01x10:00:00Z", " 2026-01-01T10:00:00Z", "2026-01-01T10:00:00Z ", "2026-01-01T10:00:00Z\n",
		"2026-01-01T10:00:00\tZ", "2026-01-01T10:00:00 Z", "2026-01-01T10:00:00 +02:00",
		"2026-01-01T10:00:00+02:00 ", "２０２６-01-01",
		// Years: four digits, or six with a sign (never -000000), within
		// JavaScript's Date range. Go's formatting of years beyond 9999 or
		// before 0000 is not ISO 8601 either.
		"-000000-01-01T00:00:00Z", "+2026-01-01T00:00:00Z", "002026-01-01T00:00:00Z",
		"+0002026-01-01T00:00:00Z", "10000-01-01T00:00:00Z", "-0001-01-01T00:00:00Z",
		"+275760-09-13T00:00:00.001Z", "+275760-09-13T00:00:00-00:01", "+275760-09-14",
		"+275760-09-12T24:00:00.5Z", "-271821-04-19T23:59:59.999Z", "-271821-04-20T00:00:00+00:01",
		"-271821-04-19",
		// A space between date and time sends Date to its legacy parser,
		// which reads the years 0-99 as 1950-2049 (13-31: not at all): a zero
		// time.Time formatted with time.DateTime would read as 2001.
		"0001-01-01 00:00:00", "0000-06-15 12:00Z", "0001-10-28 12:00:30.123+14:00", "0012-12-31 00:00",
		"0020-01-01 00:00", "0050-06-01 00:00", "0099-03-30 23:59:00Z", "+000000-01-01 00:00",
		"+000001-06-15 12:00", "+000099-12-31 23:59Z",
	}
)

// A string bound of a TimeRange is accepted exactly when the data backend
// reads it correctly, and sent as given, byte for byte (as the Python and
// JavaScript SDKs send it).
func TestTimeRangeStringsAreThoseTheDataBackendReads(t *testing.T) {
	for _, c := range acceptedISOTimes {
		got, err := timeRangeWire(&TimeRange{Start: c.s, End: c.s})
		if err != nil {
			t.Errorf("%q: %v", c.s, err)
			continue
		}
		if got[0] != any(c.s) || got[1] != any(c.s) {
			t.Errorf("%q sent as %#v", c.s, got)
		}
	}
	for _, s := range rejectedISOTimes {
		_, err := timeRangeWire(&TimeRange{Start: "2026-01-01T00:00:00Z", End: s})
		if !errors.Is(err, ErrInvalidArgument) {
			t.Errorf("%q: %v, want ErrInvalidArgument", s, err)
			continue
		}
		if want := "ironflock: invalid argument: Invalid ISO datetime format: " + s; err.Error() != want {
			t.Errorf("%q: message %q, want %q", s, err, want)
		}
	}
}

// isoTime reads every accepted string as the time JavaScript's Date reads it
// as, and rejects the others.
func TestISOTimeAgreesWithJavaScriptDate(t *testing.T) {
	for _, c := range acceptedISOTimes {
		if ms, ok := isoTime(c.s); !ok || ms != c.ms {
			t.Errorf("isoTime(%q) = %d, %v; Date reads %d", c.s, ms, ok, c.ms)
		}
	}
	for _, s := range rejectedISOTimes {
		if ms, ok := isoTime(s); ok {
			t.Errorf("isoTime(%q) = %d, want rejected", s, ms)
		}
	}
}

// daysFromCivil agrees with Go's proleptic Gregorian calendar over the whole
// range of JavaScript's Date, and isoTime with time.Time's arithmetic.
func TestISOTimeArithmetic(t *testing.T) {
	for y := int64(minDateYear); y <= maxDateYear; y += 997 {
		for _, md := range [][2]int64{{1, 1}, {2, 28}, {2, 29}, {3, 1}, {12, 31}} {
			if md[1] > daysIn(y, md[0]) {
				continue
			}
			want := time.Date(int(y), time.Month(md[0]), int(md[1]), 0, 0, 0, 0, time.UTC).Unix() / 86400
			if got := daysFromCivil(y, md[0], md[1]); got != want {
				t.Fatalf("daysFromCivil(%d, %d, %d) = %d, want %d", y, md[0], md[1], got, want)
			}
		}
	}
	for y, want := range map[int64]int64{-400: 29, -100: 28, -4: 29, -1: 28, 0: 29, 4: 29, 100: 28, 1900: 28,
		2000: 29, 2024: 29, 2026: 28, 2100: 28, 2400: 29} {
		if got := daysIn(y, 2); got != want {
			t.Errorf("daysIn(%d, 2) = %d, want %d", y, got, want)
		}
	}
	at := time.Date(-12345, 6, 7, 8, 9, 10, 987654321, time.FixedZone("", -(3*3600+30*60)))
	ms, ok := isoTime("-012345-06-07T08:09:10.987654321-03:30")
	if !ok || ms != at.UnixMilli() {
		t.Errorf("isoTime = %d, %v; want %d", ms, ok, at.UnixMilli())
	}
}

// time.Time bounds are sent as RFC 3339 in UTC; outside the years 0000-9999
// with the six-digit signed year JavaScript reads (as Date.toISOString writes
// it). A time beyond JavaScript's Date range cannot be sent.
func TestTimeRangeTimeBoundsTheDataBackendReads(t *testing.T) {
	east := time.FixedZone("east", 2*3600)
	cases := []struct {
		t    time.Time
		want string
	}{
		{time.Date(2026, 1, 1, 2, 0, 0, 500, east), "2026-01-01T00:00:00.0000005Z"},
		{time.Date(9999, 12, 31, 23, 59, 59, 999e6, time.UTC), "9999-12-31T23:59:59.999Z"},
		{time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC), "0000-01-01T00:00:00Z"},
		{time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC), "+010000-01-01T00:00:00Z"},
		{time.Date(10000, 1, 1, 1, 0, 0, 0, east), "9999-12-31T23:00:00Z"}, // the year in UTC counts
		{time.Date(-1, 1, 1, 0, 0, 0, 0, time.UTC), "-000001-01-01T00:00:00Z"},
		{time.Date(-1, 6, 15, 12, 30, 0, 25e7, time.UTC), "-000001-06-15T12:30:00.25Z"},
		{time.Date(275760, 9, 13, 0, 0, 0, 0, time.UTC), "+275760-09-13T00:00:00Z"},
		{time.Date(275760, 9, 13, 0, 0, 0, 999999, time.UTC), "+275760-09-13T00:00:00.000999999Z"},
		{time.Date(-271821, 4, 20, 0, 0, 0, 0, time.UTC), "-271821-04-20T00:00:00Z"},
	}
	for _, c := range cases {
		got, err := timeRangeWire(&TimeRange{Start: c.t, End: &c.t})
		if err != nil {
			t.Errorf("%v: %v", c.t, err)
			continue
		}
		if got[0] != any(c.want) || got[1] != any(c.want) {
			t.Errorf("%v sent as %#v, want %q", c.t, got, c.want)
		}
		// The data backend reads it as the same time (to the millisecond).
		if ms, ok := isoTime(c.want); !ok || ms != c.t.UnixMilli() {
			t.Errorf("%q reads as %d, %v; want %d", c.want, ms, ok, c.t.UnixMilli())
		}
	}
	for _, tt := range []time.Time{
		time.Date(275760, 9, 13, 0, 0, 0, 1e6, time.UTC),
		time.Date(-271821, 4, 19, 23, 59, 59, 999999999, time.UTC),
		time.Date(300000, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Unix(1<<63-62135596801, 999999999), // the largest time.Time
		time.Unix(-1<<63, 0),
	} {
		if _, err := timeRangeWire(&TimeRange{Start: tt}); !errors.Is(err, ErrInvalidArgument) {
			t.Errorf("%v: %v, want ErrInvalidArgument", tt, err)
		}
	}
}

// GetSeriesHistory sends fleetdb's series contract (SeriesQueryArgs since
// v1.0.58): the metrics as {ref, method} pairs, no top-level method, the
// bucket width in whole milliseconds; filter groups are allowed.
func TestSeriesWire(t *testing.T) {
	got, err := seriesWire(&SeriesQueryParams{
		Metrics: []SeriesMetric{
			{Ref: "temperature", Method: MethodAvg}, {Ref: "temperature", Method: MethodMax},
			{Ref: "json_data.a.b", Method: MethodSum}, {Ref: "tsp", Method: MethodCount},
		},
		Bucket:    time.Hour,
		Limit:     500,
		TimeRange: &TimeRange{Start: "2026-01-01T00:00:00Z", End: "2026-03-01T00:00:00Z"},
		GroupBy:   []string{"device_key"},
		FilterAnd: []Filter{Or(Where("device_key", "=", 1), Where("device_key", "=", 2)), Where("temperature", "is not null", nil)},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]any{
		"metrics": []any{
			map[string]any{"ref": "temperature", "method": "AVG"},
			map[string]any{"ref": "temperature", "method": "MAX"},
			map[string]any{"ref": "json_data.a.b", "method": "SUM"},
			map[string]any{"ref": "tsp", "method": "COUNT"},
		},
		"bucketMs":  int64(3_600_000),
		"limit":     int64(500),
		"timeRange": []any{"2026-01-01T00:00:00Z", "2026-03-01T00:00:00Z"},
		"groupBy":   []any{"device_key"},
		"filterAnd": []any{
			map[string]any{"combinator": "OR", "filters": []any{
				map[string]any{"column": "device_key", "operator": "=", "value": int64(1)},
				map[string]any{"column": "device_key", "operator": "=", "value": int64(2)},
			}},
			map[string]any{"column": "temperature", "operator": "IS NOT NULL"},
		},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got  %#v\nwant %#v", got, want)
	}

	// The minimal query: an automatic bucket, an open end; optional parts are
	// left out.
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	got, err = seriesWire(&SeriesQueryParams{Metrics: []SeriesMetric{{"tsp", MethodCount}}, Limit: 1, TimeRange: Since(start)}, nil)
	if err != nil {
		t.Fatal(err)
	}
	want = map[string]any{
		"metrics":   []any{map[string]any{"ref": "tsp", "method": "COUNT"}},
		"limit":     int64(1),
		"timeRange": []any{"2026-01-01T00:00:00Z", nil},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("minimal: got %#v", got)
	}
	// A metric's column name may take up to 63 bytes, a bucket a whole
	// number of milliseconds.
	if _, err := seriesWire(&SeriesQueryParams{Metrics: []SeriesMetric{{strings.Repeat("x", 59), MethodAvg}}, Limit: 1,
		Bucket: 1500 * time.Millisecond, TimeRange: &TimeRange{Start: int64(0)}}, nil); err != nil {
		t.Errorf("63 bytes: %v", err)
	}

	end := start.Add(time.Hour)
	ok := SeriesQueryParams{Metrics: []SeriesMetric{{Ref: "a", Method: MethodAvg}}, Limit: 1, TimeRange: &TimeRange{Start: int64(0)}}
	bad := map[string]func(*SeriesQueryParams){
		"no metrics":        func(p *SeriesQueryParams) { p.Metrics = nil },
		"empty metrics":     func(p *SeriesQueryParams) { p.Metrics = []SeriesMetric{} },
		"blank ref":         func(p *SeriesQueryParams) { p.Metrics = []SeriesMetric{{" ", MethodAvg}} },
		"unknown method":    func(p *SeriesQueryParams) { p.Metrics = []SeriesMetric{{"a", "MEDIAN"}} },
		"lower-case method": func(p *SeriesQueryParams) { p.Metrics = []SeriesMetric{{"a", "avg"}} },
		"no method":         func(p *SeriesQueryParams) { p.Metrics = []SeriesMetric{{"a", MethodAvg}, {Ref: "b"}} },
		"64-byte column":    func(p *SeriesQueryParams) { p.Metrics = []SeriesMetric{{strings.Repeat("x", 60), MethodAvg}} },
		"limit 0":           func(p *SeriesQueryParams) { p.Limit = 0 },
		"limit 10001":       func(p *SeriesQueryParams) { p.Limit = 10001 },
		"no time range":     func(p *SeriesQueryParams) { p.TimeRange = nil },
		"no start":          func(p *SeriesQueryParams) { p.TimeRange = &TimeRange{End: int64(5)} },
		"nil time start":    func(p *SeriesQueryParams) { p.TimeRange = &TimeRange{Start: (*time.Time)(nil), End: end} },
		"end before start":  func(p *SeriesQueryParams) { p.TimeRange = Between(end, start) },
		"end at start":      func(p *SeriesQueryParams) { p.TimeRange = &TimeRange{Start: int64(5), End: 5.0} },
		"end in the same ms": func(p *SeriesQueryParams) {
			p.TimeRange = &TimeRange{Start: "2026-01-01T00:00:00.0001Z", End: "2026-01-01T00:00:00.0009Z"}
		},
		"mixed time range":  func(p *SeriesQueryParams) { p.TimeRange = &TimeRange{Start: "2026-01-01T00:00:00Z", End: int64(1)} },
		"NaN start":         func(p *SeriesQueryParams) { p.TimeRange = &TimeRange{Start: math.NaN()} },
		"bucket under 1s":   func(p *SeriesQueryParams) { p.Bucket = 999 * time.Millisecond },
		"negative bucket":   func(p *SeriesQueryParams) { p.Bucket = -time.Second },
		"sub-ms bucket":     func(p *SeriesQueryParams) { p.Bucket = time.Second + 500*time.Microsecond },
		"latest":            func(p *SeriesQueryParams) { p.FilterAnd = []Filter{Latest()} },
		"latest in a group": func(p *SeriesQueryParams) { p.FilterAnd = []Filter{Or(Where("a", "=", 1), Latest())} },
		"latest_flag":       func(p *SeriesQueryParams) { p.FilterAnd = []Filter{Or(Where("latest_flag", "=", true))} },
		"empty group":       func(p *SeriesQueryParams) { p.FilterAnd = []Filter{And()} },
	}
	for name, mutate := range bad {
		p := ok
		mutate(&p)
		if _, err := seriesWire(&p, nil); !errors.Is(err, ErrInvalidArgument) {
			t.Errorf("%s: want ErrInvalidArgument, got %v", name, err)
		}
	}
	if _, err := seriesWire(&ok, nil); err != nil {
		t.Fatalf("the base query: %v", err)
	}
}

// SeriesMetric.Column names a metric's column in the result rows.
func TestSeriesMetricColumn(t *testing.T) {
	if c := (SeriesMetric{Ref: "temperature", Method: MethodAvg}).Column(); c != "AVG:temperature" {
		t.Errorf("Column() = %q", c)
	}
	if c := (SeriesMetric{Ref: "json_data['k']", Method: MethodLast}).Column(); c != "LAST:json_data['k']" {
		t.Errorf("Column() = %q", c)
	}
}

// ColumnPaths are sent as columnPaths; without them the key is left out.
func TestQueryWireColumnPaths(t *testing.T) {
	q := &TableQueryParams{Limit: 5, Columns: []string{"json_data"}, ColumnPaths: []string{"json_data.a.b", "json_data['k.x']"}}
	got, err := queryWire(q, MaxQueryLimit, nil)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]any{"limit": int64(5), "offset": int64(0), "columns": []any{"json_data"},
		"columnPaths": []any{"json_data.a.b", "json_data['k.x']"}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %#v", got)
	}
	got, err = queryWire(&TableQueryParams{Limit: 5, Columns: []string{"json_data"}}, MaxSecretLimit, nil)
	if _, sent := got["columnPaths"]; err != nil || sent {
		t.Errorf("without paths: %#v, %v", got, err)
	}
	if got, err := queryWire(&TableQueryParams{Limit: 5, ColumnPaths: []string{}}, MaxQueryLimit, nil); err != nil ||
		!reflect.DeepEqual(got["columnPaths"], []any{}) {
		t.Errorf("empty paths: %#v, %v", got, err)
	}
	if _, err := queryWire(&TableQueryParams{Limit: 5, ColumnPaths: []string{"json_data.a", " "}}, MaxQueryLimit, nil); !errors.Is(err, ErrInvalidArgument) ||
		!strings.Contains(err.Error(), "columnPaths[1] must be a non-empty string") {
		t.Errorf("blank path: %v", err)
	}
}

type sensorRow struct {
	Temperature float64   `json:"temperature"`
	Count       int       `json:"count"`
	At          time.Time `json:"at"`
	Skip        string    `json:"-"`
}

func TestSplitArgsAndNormalize(t *testing.T) {
	at := time.Date(2026, 1, 1, 2, 0, 0, 0, time.FixedZone("EET", 2*3600))
	pos, kw, opts, err := splitArgs([]any{
		sensorRow{Temperature: 22, Count: 3, At: at},
		7,
		Kwargs{"a": 1, "c": 1.0},
		CallOptions{DiscloseMe: true},
		Kwargs{"b": []string{"x"}, "c": sensorRow{Temperature: 1.5}},
	})
	if err != nil {
		t.Fatal(err)
	}
	wantPos := []any{
		map[string]any{"temperature": float64(22), "count": int64(3), "at": "2026-01-01T00:00:00Z"},
		int64(7),
	}
	if !reflect.DeepEqual(pos, wantPos) {
		t.Fatalf("pos %#v", pos)
	}
	wantKw := map[string]any{
		"a": int64(1),
		"b": []any{"x"},
		"c": map[string]any{"temperature": 1.5, "count": int64(0), "at": "0001-01-01T00:00:00Z"},
	}
	if !reflect.DeepEqual(kw, wantKw) {
		t.Fatalf("kw %#v", kw)
	}
	if opts == nil || !opts.DiscloseMe {
		t.Fatalf("opts %#v", opts)
	}
	// An empty Kwargs still sends (empty) keyword arguments.
	if _, kw, _, err := splitArgs([]any{Kwargs{}}); err != nil || kw == nil || len(kw) != 0 {
		t.Fatalf("empty kwargs %#v, %v", kw, err)
	}
}

func TestNormalizeRows(t *testing.T) {
	at := time.Date(2026, 1, 1, 1, 0, 0, 0, time.FixedZone("CET", 3600))
	rows, err := normalizeRows([]sensorRow{{Temperature: 1, At: at}, {Temperature: 2, Count: 5, Skip: "x"}})
	if err != nil {
		t.Fatal(err)
	}
	// A float field stays a float (2.0 is not sent as the integer 2), times
	// are sent in UTC, and the "-" field is left out.
	want := []any{
		map[string]any{"temperature": float64(1), "count": int64(0), "at": "2026-01-01T00:00:00Z"},
		map[string]any{"temperature": float64(2), "count": int64(5), "at": "0001-01-01T00:00:00Z"},
	}
	if !reflect.DeepEqual(rows, want) {
		t.Fatalf("rows %#v\nwant %#v", rows, want)
	}
	if rows, err := normalizeRows([]map[string]any{{"a": 1}}); err != nil || !reflect.DeepEqual(rows, []any{map[string]any{"a": int64(1)}}) {
		t.Fatalf("map rows %#v, %v", rows, err)
	}
	for _, c := range []struct {
		rows any
		err  string // "" if the batch is valid
	}{
		{"x", "rows must be a non-empty list of rows, got string"},
		{1, "rows must be a non-empty list of rows, got int"},
		{&[]Row{{"a": 1}}, "rows must be a non-empty list of rows, got *[]map[string]interface {}"},
		{&sensorRow{}, "rows must be a non-empty list of rows, got *ironflock.sensorRow"},
		{[1]int{1}, "row at index 0 must be a map or struct, got int"},
		{[]any{Row{"a": 1}, "x"}, "row at index 1 must be a map or struct, got string"},
		{[]any{Row{"a": 1}, nil}, "row at index 1 must be a map or struct, got nil"},
		{[]Row{nil}, "row at index 0 must be a map or struct, got a nil map[string]interface {}"},
		{[]*sensorRow{nil}, "row at index 0 must be a map or struct, got a nil *ironflock.sensorRow"},
		{[]Row{{"cb": func() {}}}, "row at index 0: cb: unsupported type func()"},
		{[][]float64{{1}}, "row at index 0 must be a map or struct, got []float64"},
		{[]map[bool]string{{true: ""}}, "row at index 0: unsupported map key type bool"},
		{[]map[bool]string{{}}, ""},       // no key to encode: an empty row, as for encoding/json
		{[]map[int]string{{1: "a"}}, ""},  // integer keys become strings: a row
		{[]map[float64]int{{1.5: 1}}, ""}, // so do float keys
		{[2]*sensorRow{{}, {}}, ""},
	} {
		_, err := normalizeRows(c.rows)
		if c.err == "" {
			if err != nil {
				t.Errorf("%#v: %v", c.rows, err)
			}
		} else if err == nil || err.Error() != c.err {
			t.Errorf("%#v: %v\nwant %s", c.rows, err, c.err)
		}
	}
	for _, empty := range []any{nil, []Row{}, [0]Row{}, []any(nil)} {
		if _, err := normalizeRows(empty); err == nil || err.Error() != "rows must be a non-empty list of rows" {
			t.Errorf("%#v: %v", empty, err)
		}
	}
}

func TestTimeRangeHelpers(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	end := start.Add(time.Hour)
	cases := []struct {
		tr   *TimeRange
		want []any
	}{
		{Between(start, end), []any{"2026-01-01T00:00:00Z", "2026-01-01T01:00:00Z"}},
		{Since(start), []any{"2026-01-01T00:00:00Z", nil}},
		{Until(end), []any{nil, "2026-01-01T01:00:00Z"}},
		{&TimeRange{Start: &start, End: (*time.Time)(nil)}, []any{"2026-01-01T00:00:00Z", nil}},
		{&TimeRange{}, []any{nil, nil}},
	}
	for i, tc := range cases {
		got, err := timeRangeWire(tc.tr)
		if err != nil || !reflect.DeepEqual(got, tc.want) {
			t.Errorf("case %d: %#v, %v", i, got, err)
		}
	}
	for _, bad := range []any{true, []string{"2026-01-01"}, struct{}{}} {
		if _, err := timeRangeWire(&TimeRange{Start: bad}); !errors.Is(err, ErrInvalidArgument) {
			t.Errorf("%#v: %v", bad, err)
		}
	}
}

// SQLOperators hands out a copy: changing it cannot change which operators
// the SDK treats as known.
func TestSQLOperatorsIsACopy(t *testing.T) {
	want := []string{
		"=", "!=", "<>", ">", "<", ">=", "<=",
		"LIKE", "ILIKE", "NOT LIKE", "NOT ILIKE",
		"IN", "NOT IN", "IS NULL", "IS NOT NULL",
	}
	ops := SQLOperators()
	if !reflect.DeepEqual(ops, want) {
		t.Fatalf("SQLOperators() = %q, want %q", ops, want)
	}
	ops[0] = "REGEXP"
	if got := SQLOperators(); !reflect.DeepEqual(got, want) {
		t.Fatalf("the vocabulary changed through a returned slice: %q", got)
	}
	log, logs := newTestLogger()
	q := &TableQueryParams{Limit: 1, FilterAnd: []Filter{Where("a", "REGEXP", "x"), Where("b", "=", 1)}}
	if _, err := queryWire(q, MaxQueryLimit, log); err != nil {
		t.Fatal(err)
	}
	if n := strings.Count(logs.String(), "is not in standard list"); n != 1 {
		t.Fatalf("%d operator warnings, want 1 (for REGEXP):\n%s", n, logs)
	}
}

// Known operators go on the wire in their canonical form, whatever their
// case and surrounding spaces: fleetdb matches an operator exactly (a typia
// literal union), so "like" would fail the whole read. An unknown operator is
// sent as given, with a warning.
func TestFilterOperatorsAreSentCanonical(t *testing.T) {
	cases := []struct {
		op, want string
		value    any
		wire     any // nil: no value key
	}{
		{"like", "LIKE", "%a%", "%a%"},
		{"ilike", "ILIKE", "%a%", "%a%"},
		{"not ilike", "NOT ILIKE", "%a%", "%a%"},
		{"LIKE ", "LIKE", "%a%", "%a%"},
		{" in ", "IN", []int{1, 2}, []any{int64(1), int64(2)}},
		{"Not In", "NOT IN", []string{"a"}, []any{"a"}},
		{"is null", "IS NULL", nil, nil},
		{"is not null", "IS NOT NULL", nil, nil},
		{" = ", "=", 1, int64(1)},
		{"<> ", "<>", 1, int64(1)},
	}
	for _, c := range cases {
		log, logs := newTestLogger()
		f := Where("a", c.op, c.value)
		got, err := queryWire(&TableQueryParams{Limit: 1, FilterAnd: []Filter{f, Or(f)}}, MaxQueryLimit, log)
		if err != nil {
			t.Errorf("%q: %v", c.op, err)
			continue
		}
		want := map[string]any{"column": "a", "operator": c.want}
		if c.wire != nil {
			want["value"] = c.wire
		}
		wantFilters := []any{want, map[string]any{"combinator": "OR", "filters": []any{want}}}
		if !reflect.DeepEqual(got["filterAnd"], wantFilters) {
			t.Errorf("%q sent as %#v\nwant %#v", c.op, got["filterAnd"], wantFilters)
		}
		if logs.String() != "" {
			t.Errorf("%q logged %s", c.op, logs)
		}
	}

	log, logs := newTestLogger()
	got, err := queryWire(&TableQueryParams{Limit: 1, FilterAnd: []Filter{Where("a", "regexp", "x")}}, MaxQueryLimit, log)
	if err != nil {
		t.Fatal(err)
	}
	if op := got["filterAnd"].([]any)[0].(map[string]any)["operator"]; op != "regexp" {
		t.Errorf("an unknown operator was sent as %q", op)
	}
	if n := strings.Count(logs.String(), "is not in standard list"); n != 1 {
		t.Errorf("%d warnings, want 1:\n%s", n, logs)
	}
}

// IN and NOT IN take a set. A nil slice — what collecting nothing into a
// var []T gives — is the empty set, as an empty one is (fleetdb: IN matches
// no row, NOT IN every row); and set elements may be booleans.
func TestFilterSetsTakeNilSlicesAndBooleans(t *testing.T) {
	var ids []string
	cases := []struct {
		f    Filter
		want any
	}{
		{Where("id", "IN", ids), []any{}},
		{Where("id", "NOT IN", []any(nil)), []any{}},
		{Where("id", "in", []int(nil)), []any{}},
		{Where("id", "IN", []string{}), []any{}},
		{Where("id", "IN", [0]string{}), []any{}},
		{Where("active", "IN", []bool{true, false}), []any{true, false}},
		{Where("x", "NOT IN", []any{"a", int64(1), 2.5, true}), []any{"a", int64(1), 2.5, true}},
	}
	for _, c := range cases {
		got, err := queryWire(&TableQueryParams{Limit: 1, FilterAnd: []Filter{c.f, Or(c.f)}}, MaxQueryLimit, nil)
		if err != nil {
			t.Errorf("%#v: %v", c.f, err)
			continue
		}
		filters := got["filterAnd"].([]any)
		inGroup := filters[1].(map[string]any)["filters"].([]any)[0]
		for _, w := range []any{filters[0], inGroup} {
			if v := w.(map[string]any)["value"]; !reflect.DeepEqual(v, c.want) {
				t.Errorf("%#v: value %#v, want %#v", c.f, v, c.want)
			}
		}
	}
	for _, f := range []Filter{
		Where("id", "IN", []any{nil}),
		Where("id", "IN", []*string{nil}),
		Where("id", "IN", []any{[]any{1}}),
		Where("id", "IN", []map[string]any{{}}),
		Where("id", "IN", 3),
		Where("id", "IN", true),
		Where("id", "=", []string(nil)),
		Where("id", "=", (*int)(nil)),
	} {
		if _, err := queryWire(&TableQueryParams{Limit: 1, FilterAnd: []Filter{f}}, MaxQueryLimit, nil); !errors.Is(err, ErrInvalidArgument) {
			t.Errorf("%#v: %v, want ErrInvalidArgument", f, err)
		}
	}
	_, err := queryWire(&TableQueryParams{Limit: 1, FilterAnd: []Filter{Where("id", "=", (*int)(nil))}}, MaxQueryLimit, nil)
	if err == nil || !strings.Contains(err.Error(), `predicate on column "id" needs a value`) {
		t.Errorf("a nil pointer: %v", err)
	}
}

// queryUUID has the shape of github.com/google/uuid.UUID: an array encoded
// through a value-receiver MarshalText.
type queryUUID [16]byte

func (u queryUUID) MarshalText() ([]byte, error) {
	return []byte(fmt.Sprintf("%x-%x-%x-%x-%x", u[0:4], u[4:6], u[6:8], u[8:10], u[10:])), nil
}

// queryTag is a named string type.
type queryTag string

// listMarshaler encodes as a JSON array.
type listMarshaler struct{}

func (listMarshaler) MarshalJSON() ([]byte, error) { return []byte("[1,2]"), nil }

// A filter value is converted like any payload value before its shape is
// checked: a type that encodes as a string (uuid.UUID, net.IP,
// json.RawMessage) is a scalar, whatever its Go kind, and a value that
// encodes as a JSON array is a list, which only IN and NOT IN take.
func TestFilterValuesAreConvertedBeforeTheirShapeIsChecked(t *testing.T) {
	id := queryUUID{1, 2, 3}
	const idText = "01020300-0000-0000-0000-000000000000"
	accepted := []struct {
		f    Filter
		want any
	}{
		{Where("id", "=", id), idText},
		{Where("id", "!=", id), idText},
		{Where("id", "LIKE", id), idText},
		{Where("id", "IN", []queryUUID{id, {}}), []any{idText, "00000000-0000-0000-0000-000000000000"}},
		{Where("ip", "=", net.ParseIP("10.0.0.1")), "10.0.0.1"},
		{Where("x", "=", json.RawMessage(`"x"`)), "x"},
		{Where("n", ">", json.RawMessage(`5`)), int64(5)},
		{Where("tag", "IN", queryTag("a,b")), "a,b"},
		{Where("ids", "IN", &[]string{"a", "b"}), []any{"a", "b"}},
	}
	for _, c := range accepted {
		got, err := queryWire(&TableQueryParams{Limit: 1, FilterAnd: []Filter{c.f, Or(c.f)}}, MaxQueryLimit, nil)
		if err != nil {
			t.Errorf("%#v: %v", c.f.Value, err)
			continue
		}
		filters := got["filterAnd"].([]any)
		inGroup := filters[1].(map[string]any)["filters"].([]any)[0]
		for _, w := range []any{filters[0], inGroup} {
			if v := w.(map[string]any)["value"]; !reflect.DeepEqual(v, c.want) {
				t.Errorf("%#v: value %#v, want %#v", c.f.Value, v, c.want)
			}
		}
	}
	for _, f := range []Filter{
		Where("ids", "=", &[]string{"a"}),
		Where("x", "=", listMarshaler{}),
		Where("x", "=", json.RawMessage(`[1,2]`)),
		Where("x", "=", []byte("ab")),
		Where("x", "IN", []byte("ab")),
	} {
		if _, err := queryWire(&TableQueryParams{Limit: 1, FilterAnd: []Filter{f}}, MaxQueryLimit, nil); !errors.Is(err, ErrInvalidArgument) {
			t.Errorf("%#v: %v, want ErrInvalidArgument", f.Value, err)
		}
	}
}

// A NaN or infinite epoch-ms bound cannot be sent: on its way to the data
// backend it becomes null, an open bound, which would drop the time filter
// silently.
func TestTimeRangeRejectsNonFiniteBounds(t *testing.T) {
	for _, tr := range []TimeRange{
		{Start: math.NaN()}, {Start: math.Inf(1)}, {End: math.Inf(-1)}, {Start: math.Inf(-1)},
		{Start: float32(math.NaN())}, {End: float32(math.Inf(1))}, {Start: math.NaN(), End: 1.7e12},
	} {
		if _, err := timeRangeWire(&tr); !errors.Is(err, ErrInvalidArgument) ||
			!strings.Contains(err.Error(), "finite epoch-ms numbers") {
			t.Errorf("%v: %v", tr, err)
		}
		for _, limit := range []int{MaxQueryLimit, MaxSecretLimit} {
			if _, err := queryWire(&TableQueryParams{Limit: 1, TimeRange: &tr}, limit, nil); !errors.Is(err, ErrInvalidArgument) {
				t.Errorf("queryWire %v: %v", tr, err)
			}
		}
	}
	for _, tr := range []TimeRange{{Start: 1.7e12, End: 1.8e12}, {Start: 0.5}, {Start: float32(0)}, {Start: -1.0}, {Start: int64(0)}} {
		if _, err := timeRangeWire(&tr); err != nil {
			t.Errorf("%v: %v", tr, err)
		}
	}
}
