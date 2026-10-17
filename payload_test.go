package ironflock

import (
	"bytes"
	"encoding"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/json/jsontext"
	jsonv2 "encoding/json/v2"
	"errors"
	"fmt"
	"math"
	"math/big"
	"math/rand/v2"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"
	"unsafe"

	"github.com/ironflock/nexus/v3/transport/serialize"
)

func TestNormalizeIntegers(t *testing.T) {
	cases := []struct {
		in   any
		want any
	}{
		{-7, int64(-7)},
		{int8(-3), int64(-3)},
		{int16(-3), int64(-3)},
		{int32(-3), int64(-3)},
		{uint8(5), int64(5)},
		{uint16(5), int64(5)},
		{uint32(5), int64(5)},
		{uint(5), int64(5)},
		{uint64(5), int64(5)},
		{uintptr(5), int64(5)},
		{pjInt(-9), int64(-9)},
		{pjUint(math.MaxUint64), uint64(math.MaxUint64)},
		{uint64(math.MaxInt64), int64(math.MaxInt64)},
		{uint64(math.MaxUint64), uint64(math.MaxUint64)}, // only above MaxInt64
		{float32(1.5), 1.5},
		{json.Number("12"), int64(12)},
		{json.Number("18446744073709551615"), uint64(math.MaxUint64)},
		{json.Number("1.25"), 1.25},
		{json.Number("2.0"), 2.0}, // a number with a fraction is a float
		{json.Number(""), int64(0)},
	}
	for _, tc := range cases {
		got, err := normalize(tc.in)
		if err != nil || got != tc.want {
			t.Errorf("normalize(%T %v) = %T %v, %v; want %T %v", tc.in, tc.in, got, got, err, tc.want, tc.want)
		}
	}
}

func TestNormalizeKeepsLargeIntegers(t *testing.T) {
	type counter struct {
		Big   uint64 `json:"big"`
		Small uint8  `json:"small"`
		Neg   int64  `json:"neg"`
	}
	got, err := normalize(counter{Big: math.MaxUint64, Small: 7, Neg: math.MinInt64})
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]any{"big": uint64(math.MaxUint64), "small": int64(7), "neg": int64(math.MinInt64)}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %#v, want %#v", got, want)
	}
}

func TestNormalizeNestedValues(t *testing.T) {
	type inner struct {
		N int `json:"n"`
	}
	var nilPtr *inner
	got, err := normalize(map[string]any{
		"list":   []any{inner{N: 1}, nilPtr, []string{"a"}},
		"kwargs": Kwargs{"k": uint8(1)},
		"bytes":  []byte("raw"),
	})
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]any{
		"list":   []any{map[string]any{"n": int64(1)}, nil, []any{"a"}},
		"kwargs": map[string]any{"k": int64(1)},
		"bytes":  []byte("raw"),
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %#v\nwant %#v", got, want)
	}
	for _, bad := range []any{func() {}, make(chan int), complex(1, 2), map[string]any{"x": func() {}}} {
		if _, err := normalize(bad); err == nil {
			t.Errorf("normalize(%T) succeeded", bad)
		}
	}
}

func TestSplitArgsErrorsAreInvalidArguments(t *testing.T) {
	_, _, _, err := splitArgs([]any{Kwargs{"cb": func() {}}})
	if !errors.Is(err, ErrInvalidArgument) || !strings.Contains(err.Error(), `kwarg "cb"`) {
		t.Errorf("kwarg: %v", err)
	}
	_, _, _, err = splitArgs([]any{1, make(chan int)})
	if !errors.Is(err, ErrInvalidArgument) || !strings.Contains(err.Error(), "argument 1") {
		t.Errorf("argument: %v", err)
	}
	if got := invalidParams("publish", err).Error(); !strings.HasPrefix(got, "ironflock: invalid argument: Invalid publish parameters: argument 1") {
		t.Errorf("invalidParams: %q", got)
	}
	// Several Kwargs merge, later keys win; a *CallOptions is taken as is.
	opts := &CallOptions{DiscloseMe: true}
	pos, kw, co, err := splitArgs([]any{Kwargs{"a": 1, "b": 1}, Kwargs{"b": 2}, opts})
	if err != nil || pos != nil || co != opts || !reflect.DeepEqual(kw, map[string]any{"a": int64(1), "b": int64(2)}) {
		t.Errorf("pos %#v kw %#v opts %v err %v", pos, kw, co, err)
	}
}

func TestDecodeHelpers(t *testing.T) {
	type reading struct {
		Temp float64 `json:"temp"`
	}
	rows, err := DecodeRows[reading]([]Row{{"temp": 21.5}, {"temp": int64(3)}})
	if err != nil || !reflect.DeepEqual(rows, []reading{{21.5}, {3}}) {
		t.Errorf("DecodeRows: %#v %v", rows, err)
	}
	if _, err := DecodeRows[reading]([]Row{{"temp": "hot"}}); err == nil {
		t.Error("DecodeRows accepted a mistyped column")
	}
	r, err := Decode[reading](map[string]any{"temp": 1.5})
	if err != nil || r.Temp != 1.5 {
		t.Errorf("Decode: %#v %v", r, err)
	}
	// What normalize sends decodes back into the same struct.
	type full struct {
		F  float64           `json:"f"`
		I  int64             `json:"i"`
		U  uint64            `json:"u"`
		B  []byte            `json:"b"`
		T  time.Time         `json:"t"`
		S  []string          `json:"s"`
		M  map[string]string `json:"m"`
		P  *float64          `json:"p"`
		Ok bool              `json:"ok"`
	}
	half := 0.5
	in := full{F: 2, I: -3, U: math.MaxUint64, B: []byte{0, 255}, T: time.Date(2026, 1, 2, 3, 4, 5, 6, time.UTC),
		S: []string{"x"}, M: map[string]string{"k": "v"}, P: &half, Ok: true}
	sent, err := normalize(in)
	if err != nil {
		t.Fatal(err)
	}
	back, err := Decode[full](sent)
	if err != nil || !reflect.DeepEqual(back, in) {
		t.Errorf("round trip: %+v, %v\nwant %+v", back, err, in)
	}
}

// ---- findings 11 and 30, and the other differences from encoding/json ----

// sameFloat reports whether got is a float64 equal to want, NaN matching NaN.
func sameFloat(got any, want float64) bool {
	f, ok := got.(float64)
	if !ok {
		return false
	}
	if math.IsNaN(want) {
		return math.IsNaN(f)
	}
	return f == want
}

// Finding 11: NaN and ±Inf travel as float64 however the value is built — a
// Row, a struct, a typed map or slice — as the Python SDK sends them.
func TestNormalizeNonFiniteFloatsEverywhere(t *testing.T) {
	type reading struct {
		T float64 `json:"t"`
	}
	type reading32 struct {
		T float32 `json:"t"`
	}
	type readingPtr struct {
		T *float64 `json:"t"`
	}
	type readingAny struct {
		T any `json:"t"`
	}
	for _, x := range []float64{math.NaN(), math.Inf(1), math.Inf(-1)} {
		x := x
		objects := map[string]any{
			"Row":                Row{"t": x},
			"Kwargs":             Kwargs{"t": x},
			"map[string]float64": map[string]float64{"t": x},
			"map[string]float32": map[string]float32{"t": float32(x)},
			"struct":             reading{T: x},
			"*struct":            &reading{T: x},
			"float32 field":      reading32{T: float32(x)},
			"*float64 field":     readingPtr{T: &x},
			"any field":          readingAny{T: x},
		}
		for name, in := range objects {
			got, err := normalize(in)
			if err != nil {
				t.Errorf("%v %s: %v", x, name, err)
				continue
			}
			if m, ok := got.(map[string]any); !ok || !sameFloat(m["t"], x) {
				t.Errorf("%v %s: got %#v", x, name, got)
			}
		}
		lists := map[string]any{
			"[]any":      []any{x},
			"[]float64":  []float64{x},
			"[1]float64": [1]float64{x},
			"[]float32":  []float32{float32(x)},
		}
		for name, in := range lists {
			got, err := normalize(in)
			if err != nil {
				t.Errorf("%v %s: %v", x, name, err)
				continue
			}
			if l, ok := got.([]any); !ok || len(l) != 1 || !sameFloat(l[0], x) {
				t.Errorf("%v %s: got %#v", x, name, got)
			}
		}
		// A batch with one non-finite reading is sent whole.
		rows, err := normalizeRows([]reading{{T: 1}, {T: x}})
		if err != nil || len(rows) != 2 || !sameFloat(rows[1].(map[string]any)["t"], x) {
			t.Errorf("%v normalizeRows: %#v, %v", x, rows, err)
		}
		pos, kw, _, err := splitArgs([]any{reading{T: x}, Kwargs{"r": reading{T: x}}})
		if err != nil || !sameFloat(pos[0].(map[string]any)["t"], x) || !sameFloat(kw["r"].(map[string]any)["t"], x) {
			t.Errorf("%v splitArgs: %#v %#v, %v", x, pos, kw, err)
		}
	}
}

// Finding 11 through the public API: a struct batch with a NaN reading is
// sent whole, as the equivalent []Row batch is.
func TestBulkWritesSendNonFiniteReadings(t *testing.T) {
	type reading struct {
		Temperature float64 `json:"temperature"`
	}
	f := flock(t)
	if err := f.PublishRowsToTable(bg, "sensordata", []reading{{Temperature: 1}, {Temperature: math.NaN()}}); err != nil {
		t.Fatal(err)
	}
	if err := f.PublishToTable(bg, "sensordata", reading{Temperature: math.Inf(1)}); err != nil {
		t.Fatal(err)
	}
	calls := f.own.allCalls()
	if len(calls) != 2 {
		t.Fatalf("%d calls, want 2", len(calls))
	}
	batch := calls[0].Args[0].([]any)
	if len(batch) != 2 || !sameFloat(batch[0].(map[string]any)["temperature"], 1) ||
		!sameFloat(batch[1].(map[string]any)["temperature"], math.NaN()) {
		t.Errorf("batch %#v", batch)
	}
	if !sameFloat(calls[1].Args[0].(map[string]any)["temperature"], math.Inf(1)) {
		t.Errorf("row %#v", calls[1].Args)
	}
}

// Finding 11, receiving side: a non-finite value (as a Python app sends it)
// decodes as JSON null instead of failing the whole result.
func TestDecodeNonFiniteFloatsAsNull(t *testing.T) {
	type reading struct {
		Temperature float64  `json:"temperature"`
		Humidity    *float64 `json:"humidity"`
	}
	rows := []Row{
		{"temperature": 21.5, "humidity": 40.0},
		{"temperature": math.NaN(), "humidity": math.Inf(1)},
		{"temperature": math.Inf(-1), "humidity": 41.0},
	}
	got, err := DecodeRows[reading](rows)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 || got[0].Temperature != 21.5 || got[0].Humidity == nil || *got[0].Humidity != 40 ||
		got[1].Temperature != 0 || got[1].Humidity != nil ||
		got[2].Temperature != 0 || got[2].Humidity == nil || *got[2].Humidity != 41 {
		t.Errorf("DecodeRows: %+v", got)
	}
	if !math.IsNaN(rows[1]["temperature"].(float64)) {
		t.Error("DecodeRows modified its input")
	}
	r, err := Decode[reading](map[string]any{"temperature": math.NaN(), "humidity": 3.0})
	if err != nil || r.Temperature != 0 || r.Humidity == nil || *r.Humidity != 3 {
		t.Errorf("Decode: %+v, %v", r, err)
	}
	list, err := Decode[[]*float64]([]any{1.0, math.NaN()})
	if err != nil || len(list) != 2 || list[0] == nil || *list[0] != 1 || list[1] != nil {
		t.Errorf("Decode list: %v, %v", list, err)
	}
}

// strings returns every string in a normalized value, in no particular order.
func stringsIn(v any) []string {
	switch x := v.(type) {
	case string:
		return []string{x}
	case []any:
		var out []string
		for _, e := range x {
			out = append(out, stringsIn(e)...)
		}
		return out
	case map[string]any:
		var out []string
		for _, e := range x {
			out = append(out, stringsIn(e)...)
		}
		return out
	}
	return nil
}

// Finding 30: a time.Time is sent as an RFC 3339 string in UTC wherever it
// is, not only at the top level.
func TestNormalizeTimeIsUTCEverywhere(t *testing.T) {
	at := time.Date(2026, 10, 8, 14, 0, 0, 123e6, time.FixedZone("CEST", 2*3600))
	const want = "2026-10-08T12:00:00.123Z"
	type withTime struct {
		Tsp time.Time `json:"tsp"`
	}
	type withPtr struct {
		Tsp *time.Time `json:"tsp"`
	}
	type nested struct {
		Inner withTime `json:"inner"`
	}
	type withAny struct {
		Tsp any `json:"tsp"`
	}
	type withMarshaler struct {
		Tsp json.Marshaler `json:"tsp"`
	}
	type withText struct {
		Tsp encoding.TextMarshaler `json:"tsp"`
	}
	cases := map[string]any{
		"time.Time":            at,
		"*time.Time":           &at,
		"Row":                  Row{"tsp": at},
		"struct":               withTime{Tsp: at},
		"*struct":              &withTime{Tsp: at},
		"*time.Time field":     withPtr{Tsp: &at},
		"nested struct":        nested{Inner: withTime{Tsp: at}},
		"any field":            withAny{Tsp: at},
		"json.Marshaler field": withMarshaler{Tsp: at},
		"TextMarshaler field":  withText{Tsp: &at},
		"map[string]time.Time": map[string]time.Time{"tsp": at},
		"[]time.Time":          []time.Time{at},
		"[1]time.Time":         [1]time.Time{at},
		"map holding a struct": map[string]any{"r": withTime{Tsp: at}},
		"[]*time.Time":         []*time.Time{&at},
	}
	for name, in := range cases {
		got, err := normalize(in)
		if err != nil {
			t.Errorf("%s: %v", name, err)
			continue
		}
		if s := stringsIn(got); len(s) != 1 || s[0] != want {
			t.Errorf("%s: %#v, want %q", name, got, want)
		}
	}
	rows, err := normalizeRows([]withTime{{Tsp: at}})
	if err != nil || !reflect.DeepEqual(rows, []any{map[string]any{"tsp": want}}) {
		t.Errorf("normalizeRows: %#v, %v", rows, err)
	}
	pos, kw, _, err := splitArgs([]any{withTime{Tsp: at}, Kwargs{"r": withTime{Tsp: at}}})
	if err != nil || !reflect.DeepEqual(pos, []any{map[string]any{"tsp": want}}) ||
		!reflect.DeepEqual(kw, map[string]any{"r": map[string]any{"tsp": want}}) {
		t.Errorf("splitArgs: %#v %#v, %v", pos, kw, err)
	}
	got, err := normalize(withPtr{})
	if err != nil || !reflect.DeepEqual(got, map[string]any{"tsp": nil}) {
		t.Errorf("nil *time.Time: %#v, %v", got, err)
	}
}

// Floats stay floats: a float field holding 2.0 is sent as the float 2.0,
// as a Row holding it is and as Python sends it, not as the integer 2.
func TestNormalizeFloatsStayFloats(t *testing.T) {
	type mixed struct {
		F   float64 `json:"f"`
		G   float32 `json:"g"`
		I   int     `json:"i"`
		Neg float64 `json:"neg"`
	}
	got, err := normalize(mixed{F: 2, G: 3, I: 4, Neg: -5})
	want := map[string]any{"f": float64(2), "g": float64(3), "i": int64(4), "neg": float64(-5)}
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Errorf("struct: %#v, %v", got, err)
	}
	for _, in := range []any{[]float64{2}, [1]float64{2}, []any{2.0}, []float32{2}} {
		if got, err := normalize(in); err != nil || !reflect.DeepEqual(got, []any{float64(2)}) {
			t.Errorf("%T: %#v, %v", in, got, err)
		}
	}
	if got, err := normalize(map[string]float64{"f": 2}); err != nil || !reflect.DeepEqual(got, map[string]any{"f": float64(2)}) {
		t.Errorf("typed map: %#v, %v", got, err)
	}
	// A float32 is widened through its shortest decimal form, the value
	// encoding/json and fmt show for it.
	if got, err := normalize(float32(0.1)); err != nil || got != 0.1 {
		t.Errorf("float32(0.1): %#v, %v", got, err)
	}
	if got, err := normalize(map[string]float32{"g": 22.3}); err != nil || !reflect.DeepEqual(got, map[string]any{"g": 22.3}) {
		t.Errorf("float32 map: %#v, %v", got, err)
	}
}

// []byte stays []byte (msgpack bin, like Python bytes) wherever it is,
// instead of encoding/json's base64 string.
func TestNormalizeBytesStayBytes(t *testing.T) {
	type blob []byte
	type withBytes struct {
		Data  []byte `json:"data"`
		Named blob   `json:"named"`
		None  []byte `json:"none"`
	}
	data := []byte("raw\x00\xff")
	got, err := normalize(withBytes{Data: data, Named: blob("b")})
	want := map[string]any{"data": []byte("raw\x00\xff"), "named": []byte("b"), "none": nil}
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("got %#v, %v", got, err)
	}
	// The result shares no memory with the caller's slice.
	data[0] = 'X'
	if got.(map[string]any)["data"].([]byte)[0] != 'r' {
		t.Error("normalize returned the caller's byte slice")
	}
	for _, in := range []any{Row{"data": []byte("b")}, map[string][]byte{"data": []byte("b")}} {
		if got, err := normalize(in); err != nil || !reflect.DeepEqual(got, map[string]any{"data": []byte("b")}) {
			t.Errorf("%T: %#v, %v", in, got, err)
		}
	}
}

// Strings are sent as valid UTF-8, as encoding/json makes them: each invalid
// byte becomes U+FFFD. Python's msgpack decoder refuses a str that is not
// UTF-8 and drops the whole message.
func TestNormalizeCoercesStringsToUTF8(t *testing.T) {
	type text struct {
		S string `json:"s"`
	}
	for _, in := range []any{Row{"s": "a\xffb"}, text{S: "a\xffb"}, map[string]string{"s": "a\xffb"}} {
		if got, err := normalize(in); err != nil || !reflect.DeepEqual(got, map[string]any{"s": "a�b"}) {
			t.Errorf("%T: %#v, %v", in, got, err)
		}
	}
	if got, err := normalize(Row{"k\xfe": 1}); err != nil || !reflect.DeepEqual(got, map[string]any{"k�": int64(1)}) {
		t.Errorf("key: %#v, %v", got, err)
	}
	_, kw, _, err := splitArgs([]any{Kwargs{"k\xfe": "v\xfe"}})
	if err != nil || !reflect.DeepEqual(kw, map[string]any{"k�": "v�"}) {
		t.Errorf("kwargs: %#v, %v", kw, err)
	}
}

// A cyclic value is an error, as it is for encoding/json — never a stack
// overflow, which would kill the process.
func TestNormalizeRejectsCycles(t *testing.T) {
	type node struct {
		Next *node `json:"next"`
	}
	n := &node{}
	n.Next = n
	m := map[string]any{}
	m["self"] = m
	s := []any{nil}
	s[0] = s
	type holder struct {
		M map[string]any `json:"m"`
	}
	// encoding/json recurses on this one until the stack overflows: it
	// detects cycles only among objects and arrays.
	var self any
	self = &self
	for name, in := range map[string]any{
		"pointer":                            n,
		"map[string]any":                     m,
		"[]any":                              s,
		"struct with map":                    holder{M: m},
		"Row inside []any":                   []any{Row{"m": m}},
		"pointer to an interface holding it": &self,
		"map key pointing to itself":         map[pjSelfKey]int{pjSelfKeyLoop(): 1},
	} {
		_, err := normalize(in)
		if err == nil || !strings.Contains(err.Error(), "cycle") {
			t.Errorf("%s: %v, want a cycle error", name, err)
		}
	}
}

// Errors say where in the value the problem is.
func TestNormalizeErrorsSayWhere(t *testing.T) {
	type leaf struct {
		Callback func() `json:"callback"`
	}
	type mid struct {
		Readings []leaf `json:"readings"`
	}
	type quoted struct {
		F float64 `json:"f,string"`
	}
	type node struct {
		Next *node `json:"next"`
	}
	loop := &node{}
	loop.Next = loop
	cases := []struct {
		in   any
		want string
	}{
		{mid{Readings: []leaf{{}}}, "readings[0].callback: unsupported type func()"},
		{Row{"a b": []any{1, make(chan int)}}, `["a b"][1]: unsupported type chan int`},
		{[]any{Row{"ok": 1, "bad": complex(1, 2)}}, "[0].bad: unsupported type complex128"},
		{map[string]pjFailJSON{"x": {}}, "x: error calling MarshalJSON for type ironflock.pjFailJSON: boom"},
		{pjRawJSON("{"), "error calling MarshalJSON for type ironflock.pjRawJSON: unexpected end of JSON input"},
		{pjRawJSON("1 2"), "error calling MarshalJSON for type ironflock.pjRawJSON: invalid character '2' after top-level value"},
		{[]pjFailText{{}}, "[0]: error calling MarshalText for type ironflock.pjFailText: text boom"},
		{map[pjFailKey]int{1: 1}, "error calling MarshalText for a map key of type ironflock.pjFailKey: key boom"},
		{map[bool]int{true: 1}, "unsupported map key type bool"},
		{Row{"m": map[*int]int{nil: 1}}, "m: unsupported map key: a nil *int"},
		{map[any]int{1.5: 1}, "unsupported map key 1.5: a float64 in an interface {} key"},
		{map[float32]int{float32(math.Inf(1)): 1}, "unsupported map key +Inf"},
		{[]pjFormat{{}}, `[0]: encoding/json does not support the "format" tag option of field T of ironflock.pjFormat`},
		{pjNested(maxNesting + 1), "[0][0][0][0][0][0][0][0]…[0][0][0][0][0][0][0][0]: objects and arrays are nested more than 10000 deep"},
		{quoted{F: math.NaN()}, `f: NaN cannot be sent in a field with the ",string" option`},
		{quoted{F: math.Inf(-1)}, `f: -Inf cannot be sent in a field with the ",string" option`},
		{json.Number("abc"), `invalid number literal "abc"`},
		{map[string]any{"n": json.Number("1e400")}, `n: invalid number "1e400"`},
		{unsafe.Pointer(nil), "unsupported type unsafe.Pointer"},
		{loop, "next.next.next.next.next.next.next.next…next.next.next.next.next.next.next.next: encountered a cycle via *ironflock.node"},
	}
	for _, c := range cases {
		_, err := normalize(c.in)
		if err == nil || err.Error() != c.want {
			t.Errorf("normalize(%T): %v\nwant %s", c.in, err, c.want)
		}
	}
	_, _, _, err := splitArgs([]any{mid{Readings: []leaf{{}}}})
	if err == nil || err.Error() != "ironflock: invalid argument: argument 0: readings[0].callback: unsupported type func()" {
		t.Errorf("splitArgs: %v", err)
	}
}

// pjAmbiguousJSON embeds two unexported structs with a MarshalJSON method
// under tag names: neither method is promoted (they conflict at the same
// depth), and a method cannot be called through an unexported field.
// encoding/json leaves both fields out (before Go 1.27 it panicked);
// normalize refuses the struct rather than drop their data.
type pjAmbiguousJSON struct {
	pjHiddenJSON1 `json:"a"`
	pjHiddenJSON2 `json:"b"`
}

type pjHiddenJSON1 struct{ N int }

func (pjHiddenJSON1) MarshalJSON() ([]byte, error) { return []byte(`"hidden"`), nil }

type pjHiddenJSON2 struct{ N int }

func (pjHiddenJSON2) MarshalJSON() ([]byte, error) { return []byte(`"hidden"`), nil }

// pjAmbiguousText is pjAmbiguousJSON with MarshalText.
type pjAmbiguousText struct {
	pjHiddenText1 `json:"a"`
	pjHiddenText2 `json:"b"`
}

type pjHiddenText1 struct{ N int }

func (pjHiddenText1) MarshalText() ([]byte, error) { return []byte("hidden"), nil }

type pjHiddenText2 struct{ N int }

func (pjHiddenText2) MarshalText() ([]byte, error) { return []byte("hidden"), nil }

// pjAmbiguousZero is pjAmbiguousJSON with IsZero, for omitzero.
type pjAmbiguousZero struct {
	pjHiddenZero1 `json:"a,omitzero"`
	pjHiddenZero2 `json:"b,omitzero"`
}

type pjHiddenZero1 struct{ N int }

func (pjHiddenZero1) IsZero() bool { return false }

type pjHiddenZero2 struct{ N int }

func (pjHiddenZero2) IsZero() bool { return false }

// encoding/json leaves out a tagged unexported embedded struct whose
// methods (marshal or unmarshal, its own or its pointer's, or IsZero with
// omitzero) it cannot call on it, dropping its data; normalize refuses the
// value with an error naming the field.
func TestNormalizeRefusesFieldsEncodingJSONLeavesOut(t *testing.T) {
	cases := []struct {
		in       any
		json     string // what encoding/json makes of it
		field    string
		method   string
		embedder string
	}{
		{pjAmbiguousJSON{}, `{}`, "pjHiddenJSON1", "MarshalJSON", "ironflock.pjAmbiguousJSON"},
		{&pjAmbiguousJSON{}, `{}`, "pjHiddenJSON1", "MarshalJSON", "ironflock.pjAmbiguousJSON"},
		{pjAmbiguousText{}, `{}`, "pjHiddenText1", "MarshalText", "ironflock.pjAmbiguousText"},
		{pjAmbiguousZero{}, `{}`, "pjHiddenZero1", "IsZero", "ironflock.pjAmbiguousZero"},
		{pjHiddenMethods{pjHiddenUnmarshal{1}, pjHiddenPtrText{2}, pjhidden{3, 4}}, `{"h":{"Visible":3}}`,
			"pjHiddenUnmarshal", "UnmarshalJSON", "ironflock.pjHiddenMethods"},
		{Row{"r": []any{pjAmbiguousText{}}}, `{"r":[{}]}`, "pjHiddenText1", "MarshalText", "ironflock.pjAmbiguousText"},
	}
	for _, c := range cases {
		if b, err := json.Marshal(c.in); err != nil || string(b) != c.json {
			t.Errorf("%#v: encoding/json gives %s, %v; want %s", c.in, b, err, c.json)
		}
		_, err := normalize(c.in)
		want := fmt.Sprintf("field %s of %s is an unexported embedded struct with a %s method, which cannot be called on it: "+
			"encoding/json would leave it out, dropping its data", c.field, c.embedder, c.method)
		if !errors.Is(err, errLeftOut) || !strings.HasSuffix(err.Error(), want) {
			t.Errorf("%#v: %v\nwant …%s", c.in, err, want)
		}
	}
	// Through a pointer receiver promoted to *pjHiddenMethods, the struct is
	// encoded through MarshalText, not field by field: nothing is left out.
	if got, err := normalize([]pjHiddenMethods{{}}); err != nil || !reflect.DeepEqual(got, []any{"hidden"}) {
		t.Errorf("addressable: %#v, %v", got, err)
	}
}

// pjCustomMarshaler is an interface type with MarshalJSON that is not
// json.Marshaler: encoding/json panics on a nil pointer held in it.
type pjCustomMarshaler interface {
	json.Marshaler
	Name() string
}

type pjCustom struct{}

func (*pjCustom) MarshalJSON() ([]byte, error) { return []byte(`"custom"`), nil }
func (*pjCustom) Name() string                 { return "custom" }

// pjHiddenMethods embeds unexported structs under tag names whose methods
// encoding/json would not call on them (an unmarshal method, a marshal
// method of the pointer): it leaves them out all the same. The one without
// methods is a field. (Their methods are promoted to *pjHiddenMethods, which
// encoding/json encodes through MarshalText.)
type pjHiddenMethods struct {
	pjHiddenUnmarshal `json:"u"`
	pjHiddenPtrText   `json:"p"`
	pjhidden          `json:"h"`
}

type pjHiddenUnmarshal struct{ N int }

func (*pjHiddenUnmarshal) UnmarshalJSON([]byte) error { return nil }

type pjHiddenPtrText struct{ N int }

func (*pjHiddenPtrText) MarshalText() ([]byte, error) { return []byte("hidden"), nil }

// normalize does not panic where encoding/json does: it returns nil for a nil
// pointer inside a json.Marshaler interface whose method has a value
// receiver, which encoding/json calls on the nil pointer (see also
// TestNormalizeNilReceiversWhereEncodingJSONPanics), and for a nil pointer
// in an interface with a marshal method that is none of json.Marshaler,
// encoding.TextAppender and encoding.TextMarshaler.
func TestNormalizeWhereEncodingJSONPanics(t *testing.T) {
	type marshalerField struct {
		M json.Marshaler `json:"m"`
	}
	type customField struct {
		C pjCustomMarshaler `json:"c"`
	}
	panics := func(v any) (panicked bool) {
		defer func() { panicked = recover() != nil }()
		_, _ = json.Marshal(v)
		return false
	}
	cases := []struct {
		name string
		in   any
		want any
	}{
		{"nil pointer in a json.Marshaler", marshalerField{M: (*pjJSONVal)(nil)}, map[string]any{"m": nil}},
		{"nil pointer in another marshaler interface", customField{C: (*pjCustom)(nil)}, map[string]any{"c": nil}},
		{"nil pointer key in another marshaler interface", map[pjCustomMarshaler]int{(*pjCustom)(nil): 1}, map[string]any{"": int64(1)}},
	}
	for _, c := range cases {
		if !panics(c.in) {
			t.Errorf("%s: encoding/json no longer panics; add it to the differential corpus", c.name)
		}
		if got, err := normalizeNoPanic(c.in); err != nil || !reflect.DeepEqual(got, c.want) {
			t.Errorf("%s: %#v, %v; want %#v", c.name, got, err, c.want)
		}
	}
	// Not nil, the pointer in such an interface is encoded as any value.
	checkLikeJSON(t, "pointer in another marshaler interface", customField{C: &pjCustom{}}, false)
}

// normalizeNoPanic is normalize, reporting a panic as an error.
func normalizeNoPanic(v any) (out any, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic: %v", r)
		}
	}()
	return normalize(v)
}

// Where encoding/json calls a MarshalJSON or MarshalText method on a nil
// pointer — held in an interface of that method's type, or embedded and
// promoting the method — and the method fails with a run-time error on it (a
// value receiver, or a pointer receiver that dereferences it), encoding/json
// panics; normalize sends nil, or "" for a map key, as for a nil pointer
// elsewhere.
func TestNormalizeNilReceiversWhereEncodingJSONPanics(t *testing.T) {
	type marshalerField struct {
		M json.Marshaler `json:"m"`
	}
	type textField struct {
		T encoding.TextMarshaler `json:"t"`
	}
	panics := func(v any) (panicked bool) {
		defer func() { panicked = recover() != nil }()
		_, _ = json.Marshal(v)
		return false
	}
	cases := []struct {
		name string
		in   any
		want any
	}{
		{"value receiver in a json.Marshaler", marshalerField{M: (*pjJSONVal)(nil)}, map[string]any{"m": nil}},
		{"*time.Time in a json.Marshaler", marshalerField{M: (*time.Time)(nil)}, map[string]any{"m": nil}},
		{"dereferencing pointer receiver in a TextMarshaler", textField{T: (*pjTextPtr)(nil)}, map[string]any{"t": nil}},
		{"value receiver in a TextMarshaler element", []encoding.TextMarshaler{(*pjTextVal)(nil)}, []any{nil}},
		{"nil embedded *time.Time", pjEmbedsTimePtr{N: 7}, nil},
		{"nil embedded *time.Time, in a Row", Row{"tsp": pjEmbedsTimePtr{N: 7}}, map[string]any{"tsp": nil}},
		{"value receiver through a nil embedded pointer", struct {
			*pjJSONVal
			N int
		}{nil, 1}, nil},
		{"dereferencing pointer receiver through a nil embedded pointer", []struct{ *pjJSONPtr }{{}}, []any{nil}},
		{"nil embedded pointer two levels down", struct{ PjStamped2 }{}, nil},
		{"nil embedded interface", struct{ json.Marshaler }{}, nil},
		{"embedded interface holding a dereferencing nil pointer", struct {
			encoding.TextMarshaler
		}{(*pjTextPtr)(nil)}, nil},
		{"*time.Time in an interface key", map[encoding.TextMarshaler]int{(*time.Time)(nil): 1}, map[string]any{"": int64(1)}},
		{"dereferencing pointer receiver in an interface key", map[encoding.TextMarshaler]int{(*pjPtrKey)(nil): 1}, map[string]any{"": int64(1)}},
		{"key promoting through a nil embedded *time.Time", map[pjEmbedsTimePtr]int{{N: 1}: 1}, map[string]any{"": int64(1)}},
	}
	for _, c := range cases {
		if !panics(c.in) {
			t.Errorf("%s: encoding/json no longer panics; add it to the differential corpus", c.name)
		}
		if got, err := normalizeNoPanic(c.in); err != nil || !reflect.DeepEqual(got, c.want) {
			t.Errorf("%s: %#v, %v; want %#v", c.name, got, err, c.want)
		}
	}
	// A panic that is not a run-time error — the method's own — propagates,
	// as with encoding/json.
	for _, in := range []any{marshalerField{M: (*pjPanicky)(nil)}, marshalerField{M: &pjPanicky{}}} {
		func() {
			defer func() {
				if r := recover(); r != "pjPanicky: refused" {
					t.Errorf("%#v: recovered %v, want the method's panic", in, r)
				}
			}()
			_, _ = normalize(in)
		}()
	}
}

// PjStamped2 embeds a pointer to a struct that embeds time.Time: a nil
// pointer there makes time.Time's promoted methods panic.
type PjStamped2 struct{ *PjStamp }

// Round-3 finding 47: a nil pointer held in an interface whose type is a
// json.Marshaler or encoding.TextMarshaler (a field of type json.Marshaler,
// say) is encoded as encoding/json encodes it: through the method, called on
// the nil pointer. What a method that handles a nil receiver returns is
// sent; it is not replaced by nil.
func TestNormalizeCallsMethodsOnNilPointersAsEncodingJSON(t *testing.T) {
	type fields struct {
		M json.Marshaler         `json:"m"`
		T encoding.TextMarshaler `json:"t"`
	}
	cases := []struct {
		in, want any
	}{
		{fields{M: (*pjOptional)(nil), T: (*big.Float)(nil)}, map[string]any{"m": "unset", "t": "<nil>"}},
		{fields{M: (*big.Int)(nil), T: (*big.Int)(nil)}, map[string]any{"m": nil, "t": "<nil>"}},
		{fields{M: (*pjMixed)(nil)}, map[string]any{"m": "mixed-json", "t": nil}},
		{[]json.Marshaler{(*pjOptional)(nil), &pjOptional{V: 3}}, []any{"unset", int64(3)}},
		{map[string]encoding.TextMarshaler{"x": (*big.Float)(nil)}, map[string]any{"x": "<nil>"}},
		{Row{"x": []json.Marshaler{(*pjOptional)(nil)}}, map[string]any{"x": []any{"unset"}}},
		{map[encoding.TextMarshaler]int{(*big.Float)(nil): 1}, map[string]any{"<nil>": int64(1)}},
		// Not held in such an interface, a nil pointer is nil, as for encoding/json.
		{Row{"x": (*pjOptional)(nil)}, map[string]any{"x": nil}},
		{struct{ X *pjOptional }{}, map[string]any{"X": nil}},
	}
	for _, c := range cases {
		got, err := normalizeNoPanic(c.in)
		if err != nil || !reflect.DeepEqual(got, c.want) {
			t.Errorf("%#v: %#v, %v; want %#v", c.in, got, err, c.want)
		}
		checkLikeJSON(t, fmt.Sprintf("%#v", c.in), c.in, false)
	}
}

// Round-3 finding 46: a time.Time map key is sent in UTC, as a time.Time
// value is — also behind a pointer, in an interface or embedded in the key
// type — instead of keeping its zone offset.
func TestNormalizeTimeKeysAreUTC(t *testing.T) {
	at := time.Date(2026, 10, 8, 14, 0, 0, 123e6, time.FixedZone("CEST", 2*3600))
	const want = "2026-10-08T12:00:00.123Z"
	cases := map[string]any{
		"map[time.Time]":                        map[time.Time]float64{at: 21.5},
		"map[*time.Time]":                       map[*time.Time]float64{&at: 21.5},
		"time.Time in map[TextMarshaler]":       map[encoding.TextMarshaler]float64{at: 21.5},
		"*time.Time in map[TextMarshaler]":      map[encoding.TextMarshaler]float64{&at: 21.5},
		"key type embedding time.Time":          map[pjEmbedsTime]float64{{at, "note"}: 21.5},
		"key type embedding *time.Time":         map[pjEmbedsTimePtr]float64{{&at, 1}: 21.5},
		"key type embedding it two levels down": map[pjStamped]float64{{PjStamp{at}, 1}: 21.5},
		"key type with its own MarshalJSON":     map[pjEpoch]float64{{at}: 21.5}, // keys use MarshalText
	}
	for name, in := range cases {
		got, err := normalizeNoPanic(in)
		if err != nil || !reflect.DeepEqual(got, map[string]any{want: 21.5}) {
			t.Errorf("%s: %#v, %v", name, got, err)
		}
	}
	// A key and a value holding the same instant are the same string.
	nested := Row{"readings": map[time.Time]float64{at: 21.5}, "tsp": at}
	wantNested := map[string]any{"readings": map[string]any{want: 21.5}, "tsp": want}
	if got, err := normalize(nested); err != nil || !reflect.DeepEqual(got, wantNested) {
		t.Errorf("Row: %#v, %v", got, err)
	}
	pos, kw, _, err := splitArgs([]any{map[time.Time]float64{at: 21.5}, Kwargs{"r": nested}})
	if err != nil || !reflect.DeepEqual(pos, []any{map[string]any{want: 21.5}}) || !reflect.DeepEqual(kw, map[string]any{"r": wantNested}) {
		t.Errorf("splitArgs: %#v %#v, %v", pos, kw, err)
	}
	if rows, err := normalizeRows([]Row{nested}); err != nil || !reflect.DeepEqual(rows, []any{wantNested}) {
		t.Errorf("normalizeRows: %#v, %v", rows, err)
	}
	// Outside the years 0000-9999 a key is formatted as a value is
	// (encoding/json fails on such a key).
	far := time.Date(10000, 1, 1, 0, 30, 0, 0, time.FixedZone("x", 2*3600))
	if got, err := normalize(far); err != nil || got != "9999-12-31T22:30:00Z" {
		t.Errorf("far value: %#v, %v", got, err)
	}
	if got, err := normalize(map[time.Time]int{far: 1}); err != nil || !reflect.DeepEqual(got, map[string]any{"9999-12-31T22:30:00Z": int64(1)}) {
		t.Errorf("far key: %#v, %v", got, err)
	}
	// Keys naming the same instant in different zones become one entry:
	// that of the key encoding/json would have written last (the greatest
	// with its zone offset), whatever the order of map iteration.
	same := map[time.Time]string{at: "cest", at.UTC(): "utc", at.In(time.FixedZone("x", -5*3600)): "x"}
	for range 200 {
		if got, err := normalize(same); err != nil || !reflect.DeepEqual(got, map[string]any{want: "cest"}) {
			t.Fatalf("same instant: %#v, %v", got, err)
		}
	}
	// A nil pointer key is "", as for encoding/json.
	if got, err := normalize(map[*time.Time]int{nil: 1, &at: 2}); err != nil || !reflect.DeepEqual(got, map[string]any{"": int64(1), want: int64(2)}) {
		t.Errorf("nil key: %#v, %v", got, err)
	}
	// Strings are sent as they are, however much they look like times.
	const s = "2026-10-08T14:00:00.123+02:00"
	got, err := normalize(Row{s: s, "m": map[string]string{s: s}, "k": map[pjString]int{s: 1}})
	if want := map[string]any{s: s, "m": map[string]any{s: s}, "k": map[string]any{s: int64(1)}}; err != nil || !reflect.DeepEqual(got, want) {
		t.Errorf("strings: %#v, %v", got, err)
	}
}

// Round-3 finding 46: a struct that embeds a time.Time is encoded through
// time.Time's promoted MarshalJSON, as encoding/json does — as that time, its
// other fields dropped — and so in UTC like any time.Time. A MarshalJSON of
// its own wins, as for encoding/json.
func TestNormalizeEmbeddedTimeIsUTC(t *testing.T) {
	at := time.Date(2026, 10, 8, 14, 0, 0, 123e6, time.FixedZone("CEST", 2*3600))
	const want = "2026-10-08T12:00:00.123Z"
	type withMarshaler struct {
		Tsp json.Marshaler `json:"tsp"`
	}
	type withText struct {
		Tsp encoding.TextMarshaler `json:"tsp"`
	}
	cases := map[string]any{
		"embedded time.Time":                    pjEmbedsTime{at, "note"},
		"pointer to it":                         &pjEmbedsTime{at, "note"},
		"embedded *time.Time":                   pjEmbedsTimePtr{&at, 1},
		"two levels down":                       pjStamped{PjStamp{at}, 2},
		"two levels down, through a pointer":    PjStamped2{&PjStamp{at}},
		"through an unexported embedded struct": pjHiddenStamped{pjhiddenStamp{at}, 3},
		"in a json.Marshaler field":             withMarshaler{Tsp: pjEmbedsTime{at, ""}},
		"in a TextMarshaler field":              withText{Tsp: &pjEmbedsTime{at, ""}},
		"in a Row":                              Row{"tsp": pjEmbedsTime{at, ""}},
		"in a typed slice":                      []pjEmbedsTime{{at, ""}},
		"in a typed map":                        map[string]PjStamp{"tsp": {at}},
		"its own MarshalJSON sends the time":    pjNullTime{at},
	}
	for name, in := range cases {
		got, err := normalizeNoPanic(in)
		if err != nil {
			t.Errorf("%s: %v", name, err)
			continue
		}
		if s := stringsIn(got); len(s) != 1 || s[0] != want {
			t.Errorf("%s: %#v, want %q", name, got, want)
		}
	}
	if rows, err := normalizeRows([]Row{{"tsp": pjStamped{PjStamp{at}, 2}}}); err != nil || !reflect.DeepEqual(rows, []any{map[string]any{"tsp": want}}) {
		t.Errorf("normalizeRows: %#v, %v", rows, err)
	}
	// A MarshalJSON of the type's own that does not send the time is used
	// as it is.
	if got, err := normalize(pjEpoch{at}); err != nil || got != at.UnixMilli() {
		t.Errorf("own MarshalJSON: %#v, %v", got, err)
	}
	if got, err := normalize(pjNullTime{}); err != nil || got != nil {
		t.Errorf("own MarshalJSON, zero: %#v, %v", got, err)
	}
	shallower, err := normalize(pjJSONVal{1})
	if got, err2 := normalize(pjShallowerJSON{pjJSONVal{1}, PjStamp{at}}); err != nil || err2 != nil || !reflect.DeepEqual(got, shallower) {
		t.Errorf("shallower MarshalJSON: %#v, %v; want %#v, %v", got, err2, shallower, err)
	}
	// Two time.Time at the same depth promote no method: a struct, or what a
	// MarshalJSON of the type's own sends, as it is.
	if got, err := normalize(pjTwoStamps{PjStamp{at}, pjhiddenStamp{at}}); err != nil || !reflect.DeepEqual(got, map[string]any{}) {
		t.Errorf("two embedded: %#v, %v", got, err)
	}
	if got, err := normalize(pjTwoStampsJSON{PjStamp{at}, pjhiddenStamp{at}}); err != nil || got != "2026-10-08T14:00:00.123+02:00" {
		t.Errorf("two embedded, own MarshalJSON: %#v, %v", got, err)
	}
	// Outside the years 0000-9999 it is formatted as the time.Time is
	// (encoding/json fails on it).
	far := time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)
	if got, err := normalize(pjEmbedsTime{far, ""}); err != nil || got != "10000-01-01T00:00:00Z" {
		t.Errorf("far: %#v, %v", got, err)
	}
}

// Round-3 finding 45: a bulk row is encoded exactly as PublishToTable and
// AppendToTable encode the same row: taken as a value, so a MarshalJSON or
// MarshalText method with a pointer receiver applies to neither (a []*T
// batch, or &row, uses it), not as json.Marshal([]T) would encode it.
func TestBulkRowsEncodeLikeSingleRows(t *testing.T) {
	var counter pjCounter
	counter.Count.SetInt64(42)
	addressable := pjAddressable{J: pjJSONPtr{1}, T: pjTextPtr{"t"}, M: pjMixed{2}, Z: pjZeroPtr{1}, BP: big.NewInt(-42)}
	rows := []any{
		pjRenamed{Temp: 21.5},
		pjMachine{State: 1, Temp: 60},
		counter,
		pjTextRow{A: 7},
		addressable,
		pjJSONVal{3},
		Row{"a": 1},
		&pjRenamed{Temp: 1.5},
	}
	single := func(r any) any {
		t.Helper()
		pos, _, _, err := splitArgs([]any{r})
		if err != nil {
			t.Fatalf("%T: %v", r, err)
		}
		return pos[0]
	}
	// sliceOf returns a []T holding r, whose element is addressable as in
	// any []T.
	sliceOf := func(r any) any {
		return reflect.Append(reflect.MakeSlice(reflect.SliceOf(reflect.TypeOf(r)), 0, 1), reflect.ValueOf(r)).Interface()
	}
	for _, r := range rows {
		want := single(r)
		array := reflect.New(reflect.ArrayOf(1, reflect.TypeOf(r))).Elem()
		array.Index(0).Set(reflect.ValueOf(r))
		for _, batch := range []any{sliceOf(r), array.Interface(), []any{r}} {
			got, err := normalizeRows(batch)
			if err != nil || len(got) != 1 || !reflect.DeepEqual(got[0], want) {
				t.Errorf("%T: %#v, %v\nPublishToTable sends %#v", batch, got, err, want)
			}
		}
	}
	if got := single(pjRenamed{Temp: 21.5}); !reflect.DeepEqual(got, map[string]any{"temperature": 21.5}) {
		t.Errorf("value row: %#v", got)
	}
	// The way to use a pointer-receiver method: a []*T batch, or &row.
	if got := single(&pjRenamed{Temp: 21.5}); !reflect.DeepEqual(got, map[string]any{"temp_c": 21.5}) {
		t.Errorf("pointer row: %#v", got)
	}
	if got, err := normalizeRows([]*pjRenamed{{Temp: 21.5}}); err != nil || !reflect.DeepEqual(got, []any{map[string]any{"temp_c": 21.5}}) {
		t.Errorf("[]*T: %#v, %v", got, err)
	}

	// Through the public API, published and appended.
	f := flock(t)
	for _, r := range rows {
		if err := f.PublishToTable(bg, "sensordata", r); err != nil {
			t.Fatal(err)
		}
		one := f.own.lastCall(t).Args[0]
		if err := f.PublishRowsToTable(bg, "sensordata", sliceOf(r)); err != nil {
			t.Errorf("PublishRowsToTable([]%T): %v", r, err)
		} else if bulk := f.own.lastCall(t).Args[0].([]any)[0]; !reflect.DeepEqual(bulk, one) {
			t.Errorf("PublishRowsToTable([]%T) sends %#v; PublishToTable %#v", r, bulk, one)
		}
		if _, err := f.AppendToTable(bg, "sensordata", r); err != nil {
			t.Fatal(err)
		}
		one = f.own.lastCall(t).Args[0]
		if _, err := f.AppendRowsToTable(bg, "sensordata", sliceOf(r)); err != nil {
			t.Errorf("AppendRowsToTable([]%T): %v", r, err)
		} else if bulk := f.own.lastCall(t).Args[0].([]any)[0]; !reflect.DeepEqual(bulk, one) {
			t.Errorf("AppendRowsToTable([]%T) sends %#v; AppendToTable %#v", r, bulk, one)
		}
	}
}

// A bulk row that its own MarshalJSON or MarshalText method encodes as
// something other than an object is refused with a message that says so.
func TestBulkRowEncodedAsNoObject(t *testing.T) {
	cases := []struct {
		rows any
		want string
	}{
		{[]pjTextVal{{"v"}}, "row at index 0 must be a map or struct, got ironflock.pjTextVal, which its MarshalText method encodes as a string"},
		{[]any{Row{}, pjRawJSON("[1]")}, "row at index 1 must be a map or struct, got ironflock.pjRawJSON, which its MarshalJSON method encodes as a list"},
		{[]pjNullTime{{}}, "row at index 0 must be a map or struct, got ironflock.pjNullTime, which its MarshalJSON method encodes as null"},
		{[]*pjEpoch{{}}, "row at index 0 must be a map or struct, got *ironflock.pjEpoch, which its MarshalJSON method encodes as a number"},
	}
	for _, c := range cases {
		if _, err := normalizeRows(c.rows); err == nil || err.Error() != c.want {
			t.Errorf("%#v: %v\nwant %s", c.rows, err, c.want)
		}
	}
}

// ---- normalize against encoding/json ----

// viaJSON is the reference normalize is checked against: the JSON
// encoding/json makes of v, decoded with integral numbers as int64 (uint64
// above math.MaxInt64) and other numbers as float64.
func viaJSON(v any) (any, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	var out any
	if err := dec.Decode(&out); err != nil {
		return nil, err
	}
	return refNumbers(out)
}

func refNumbers(v any) (any, error) {
	switch x := v.(type) {
	case json.Number:
		if i, err := strconv.ParseInt(x.String(), 10, 64); err == nil {
			return i, nil
		}
		if u, err := strconv.ParseUint(x.String(), 10, 64); err == nil {
			return u, nil
		}
		f, err := strconv.ParseFloat(x.String(), 64)
		if err != nil {
			return nil, err
		}
		return f, nil
	case []any:
		for i, e := range x {
			n, err := refNumbers(e)
			if err != nil {
				return nil, err
			}
			x[i] = n
		}
	case map[string]any:
		for k, e := range x {
			n, err := refNumbers(e)
			if err != nil {
				return nil, err
			}
			x[k] = n
		}
	}
	return v, nil
}

// diffJSON describes how got, normalize's result for a value, differs from
// want, viaJSON's for the same value, allowing for normalize's documented
// differences, which the SDK keeps on purpose so that it sends what the
// Python and JavaScript SDKs send: []byte stays bytes (msgpack bin) instead
// of a base64 string; a time.Time is sent in UTC, as a value and as a map
// key (encoding/json keeps its zone offset), so keys naming the same instant
// merge into one, holding the value of the greatest original key; and a
// float stays a float — where encoding/json wrote an integral float as an
// integer, lenient accepts the float, otherwise the caller must use
// non-integral floats only. It returns "" when they agree.
func diffJSON(got, want any, lenient bool) string {
	return diffAt("", got, want, lenient)
}

func diffAt(path string, got, want any, lenient bool) string {
	bad := func() string { return fmt.Sprintf("at %q: got %#v (%T), want %#v (%T)", path, got, got, want, want) }
	switch w := want.(type) {
	case nil:
		if got != nil {
			return bad()
		}
	case bool:
		if g, ok := got.(bool); !ok || g != w {
			return bad()
		}
	case string:
		switch g := got.(type) {
		case string:
			if g != w {
				// A time.Time: normalize sends the same instant in UTC.
				t, err := time.Parse(time.RFC3339Nano, w)
				if err != nil || g != t.UTC().Format(time.RFC3339Nano) {
					return bad()
				}
			}
		case []byte:
			if base64.StdEncoding.EncodeToString(g) != w {
				return bad()
			}
		default:
			return bad()
		}
	case int64:
		switch g := got.(type) {
		case int64:
			if g != w {
				return bad()
			}
		case float64:
			// encoding/json writes an integral float as its shortest decimal
			// form: the float nearest to that integer is the float itself.
			if !lenient || float64(w) != g {
				return bad()
			}
		default:
			return bad()
		}
	case uint64:
		switch g := got.(type) {
		case uint64:
			if g != w {
				return bad()
			}
		case float64:
			if !lenient || float64(w) != g {
				return bad()
			}
		default:
			return bad()
		}
	case float64:
		if g, ok := got.(float64); !ok || g != w {
			return bad()
		}
	case []any:
		g, ok := got.([]any)
		if !ok || len(g) != len(w) {
			return bad()
		}
		for i := range w {
			if d := diffAt(fmt.Sprintf("%s[%d]", path, i), g[i], w[i], lenient); d != "" {
				return d
			}
		}
	case map[string]any:
		g, ok := got.(map[string]any)
		if !ok {
			return bad()
		}
		// The key of got each key of want is sent as: itself, or a time.Time
		// key in UTC. Of keys of want sent as the same key, the greatest wins.
		wins := make(map[string]string, len(w)) // got's key → want's key
		for k := range w {
			gk := k
			if _, ok := g[k]; !ok {
				if t, err := time.Parse(time.RFC3339Nano, k); err == nil {
					gk = t.UTC().Format(time.RFC3339Nano)
				}
			}
			if prev, ok := wins[gk]; !ok || k > prev {
				wins[gk] = k
			}
		}
		if len(g) != len(wins) {
			return bad()
		}
		for gk, k := range wins {
			gv, ok := g[gk]
			if !ok {
				return fmt.Sprintf("at %q: key %q missing from %#v", path, gk, got)
			}
			if d := diffAt(path+"."+k, gv, w[k], lenient); d != "" {
				return d
			}
		}
	default:
		return fmt.Sprintf("at %q: unexpected reference value %#v", path, want)
	}
	return ""
}

// checkLikeJSON fails the test unless normalize(v) and viaJSON(v) both fail,
// or agree (see diffJSON). It also checks the shape of normalize's result.
func checkLikeJSON(t *testing.T, name string, v any, lenient bool) {
	t.Helper()
	got, err := normalize(v)
	want, wantErr := viaJSON(v)
	switch {
	case err != nil && wantErr != nil:
	case wantErr != nil:
		t.Errorf("%s: normalize gave %#v; encoding/json fails: %v", name, got, wantErr)
	case errors.Is(err, errLeftOut):
		// A documented difference, kept on purpose: encoding/json leaves out
		// a tagged unexported embedded struct whose methods it cannot call,
		// silently dropping its data; normalize refuses the value instead
		// (TestNormalizeRefusesFieldsEncodingJSONLeavesOut checks both sides).
	case err != nil:
		t.Errorf("%s: normalize failed: %v; encoding/json gives %#v", name, err, want)
	default:
		if d := diffJSON(got, want, lenient); d != "" {
			t.Errorf("%s: %s\n got  %#v\n want %#v", name, d, got, want)
		}
		if d := checkShape(got); d != "" {
			t.Errorf("%s: %s", name, d)
		}
	}
}

// checkShape describes where v is not made of the types normalize promises:
// nil, bool, valid UTF-8 strings, int64, uint64 above math.MaxInt64,
// float64, non-nil []byte, []any and map[string]any with valid UTF-8 keys.
func checkShape(v any) string {
	switch x := v.(type) {
	case nil, bool, int64, float64:
	case string:
		if !utf8.ValidString(x) {
			return fmt.Sprintf("invalid UTF-8 string %q", x)
		}
	case uint64:
		if x <= math.MaxInt64 {
			return fmt.Sprintf("uint64 %d fits an int64", x)
		}
	case []byte:
		if x == nil {
			return "typed nil []byte"
		}
	case []any:
		if x == nil {
			return "typed nil []any"
		}
		for _, e := range x {
			if d := checkShape(e); d != "" {
				return d
			}
		}
	case map[string]any:
		if x == nil {
			return "typed nil map"
		}
		for k, e := range x {
			if !utf8.ValidString(k) {
				return fmt.Sprintf("invalid UTF-8 key %q", k)
			}
			if d := checkShape(e); d != "" {
				return d
			}
		}
	default:
		return fmt.Sprintf("value of type %T", v)
	}
	return ""
}

// Types of the differential corpus.
type (
	pjInt     int
	pjString  string
	pjFloat   float64
	pjFloat32 float32
	pjBool    bool
	pjByte    byte
	pjBytes   []byte
	pjUint    uint64
	pjMap     map[string]int
	pjSlice   []string
	pjPtr     *int
	PjEmbInt  int
)

// pjJSONVal encodes itself (value receiver) as padded JSON holding numbers
// of every kind.
type pjJSONVal struct{ N int }

func (v pjJSONVal) MarshalJSON() ([]byte, error) {
	return fmt.Appendf(nil, ` {"n": %d, "two": 2.0, "half": 1.5, "exp": 1e3, "neg": -0,
		"list": [1, -2.5e-3, "s<>&", null, true, {}, []],
		"big": 18446744073709551615, "huge": 123456789012345678901234567890} `, v.N), nil
}

// pjJSONPtr encodes itself with a pointer receiver: encoding/json calls the
// method only on addressable values.
type pjJSONPtr struct{ N int }

func (v *pjJSONPtr) MarshalJSON() ([]byte, error) { return fmt.Appendf(nil, `"ptr-%d"`, v.N), nil }

type pjTextVal struct{ S string }

func (v pjTextVal) MarshalText() ([]byte, error) { return []byte("text:" + v.S), nil }

type pjTextPtr struct{ S string }

func (v *pjTextPtr) MarshalText() ([]byte, error) { return []byte("ptext:" + v.S), nil }

// pjBoth has both methods: MarshalJSON wins.
type pjBoth struct{ N int }

func (pjBoth) MarshalJSON() ([]byte, error) { return []byte(`"json"`), nil }
func (pjBoth) MarshalText() ([]byte, error) { return []byte("text"), nil }

// pjMixed has MarshalJSON on its pointer and MarshalText on its value: an
// addressable value uses the first, any other the second.
type pjMixed struct{ N int }

func (*pjMixed) MarshalJSON() ([]byte, error) { return []byte(`"mixed-json"`), nil }
func (pjMixed) MarshalText() ([]byte, error)  { return []byte("mixed-text"), nil }

// pjList is a slice type that encodes itself.
type pjList []int

func (l pjList) MarshalJSON() ([]byte, error) { return fmt.Appendf(nil, `{"len": %d}`, len(l)), nil }

// pjByteText is a byte type whose pointer has MarshalText: a []pjByteText is
// encoded element by element, not as bytes.
type pjByteText byte

func (b *pjByteText) MarshalText() ([]byte, error) { return fmt.Appendf(nil, "b%d", *b), nil }

// pjRawJSON returns itself as its JSON, valid or not.
type pjRawJSON string

func (r pjRawJSON) MarshalJSON() ([]byte, error) { return []byte(r), nil }

// pjKey is a struct map key with MarshalText.
type pjKey struct{ A, B uint8 }

func (k pjKey) MarshalText() ([]byte, error) { return fmt.Appendf(nil, "%d-%d", k.A, k.B), nil }

// pjIntKey is an integer map key with MarshalText, which wins over the number.
type pjIntKey int

func (k pjIntKey) MarshalText() ([]byte, error) { return fmt.Appendf(nil, "k%d", int(k)), nil }

// pjStringKey is a string map key with MarshalText, which is not used:
// string keys are taken as they are.
type pjStringKey string

func (pjStringKey) MarshalText() ([]byte, error) { return []byte("never"), nil }

// pjPtrKey is a pointer map key with MarshalText; a nil key becomes "".
type pjPtrKey struct{ N int }

func (k *pjPtrKey) MarshalText() ([]byte, error) { return fmt.Appendf(nil, "p%d", k.N), nil }

// pjZeroVal and pjZeroPtr are zero when N <= 0: omitzero asks their IsZero.
type pjZeroVal struct{ N int }

func (z pjZeroVal) IsZero() bool { return z.N <= 0 }

type pjZeroPtr struct{ N int }

func (z *pjZeroPtr) IsZero() bool { return z.N <= 0 }

type pjFailJSON struct{}

func (pjFailJSON) MarshalJSON() ([]byte, error) { return nil, errors.New("boom") }

type pjFailText struct{}

func (pjFailText) MarshalText() ([]byte, error) { return nil, errors.New("text boom") }

type pjFailKey int

func (pjFailKey) MarshalText() ([]byte, error) { return nil, errors.New("key boom") }

// pjTags has every kind of tag, malformed ones included.
//
//nolint:staticcheck // SA5008: the odd tags are what is tested.
type pjTags struct {
	Plain       int
	Named       int    `json:"named"`
	Dash        int    `json:"-"`
	DashComma   int    `json:"-,"`
	OnlyOptions int    `json:",omitempty"`
	Punct       string `json:"a-b.c:d#e"`
	Unicode     string `json:"ünï"`
	Invalid     string `json:"in\"valid"`
	Space       string `json:"with space"`
	TrailComma  string `json:"tc,"`
	Upper       int    `json:"KEY"`
	Lower       int    `json:"key"`
	Unknown     int    `json:"unknown,nosuchoption"`
	unexported  int
}

type pjOmitEmpty struct {
	Bool    bool           `json:",omitempty"`
	Int     int            `json:",omitempty"`
	Int8    int8           `json:",omitempty"`
	Uint    uint           `json:",omitempty"`
	Uintptr uintptr        `json:",omitempty"`
	Float   float64        `json:",omitempty"`
	Float32 float32        `json:",omitempty"`
	String  string         `json:",omitempty"`
	Bytes   []byte         `json:",omitempty"`
	Slice   []int          `json:",omitempty"`
	Map     map[string]int `json:",omitempty"`
	Array   [2]int         `json:",omitempty"`
	Array0  [0]int         `json:",omitempty"`
	Ptr     *int           `json:",omitempty"`
	Iface   any            `json:",omitempty"`
	Struct  struct{}       `json:",omitempty"`
	Time    time.Time      `json:",omitempty"`
	Number  json.Number    `json:",omitempty"`
	Named   pjInt          `json:",omitempty"`
	Marsh   pjJSONVal      `json:",omitempty"`
	MarshP  *pjJSONVal     `json:",omitempty"`
	Text    pjTextVal      `json:",omitempty"`
}

type pjOmitZero struct {
	Int     int                        `json:",omitzero"`
	Float   float64                    `json:",omitzero"`
	String  string                     `json:",omitzero"`
	Slice   []int                      `json:",omitzero"`
	Map     map[string]int             `json:",omitzero"`
	Array   [2]int                     `json:",omitzero"`
	Struct  struct{ A int }            `json:",omitzero"`
	Time    time.Time                  `json:",omitzero"`
	TimePtr *time.Time                 `json:",omitzero"`
	ZeroVal pjZeroVal                  `json:",omitzero"`
	ZeroPtr pjZeroPtr                  `json:",omitzero"`
	ZeroPP  *pjZeroPtr                 `json:",omitzero"`
	ZeroIf  interface{ IsZero() bool } `json:",omitzero"`
	Both    []int                      `json:",omitempty,omitzero"`
	Iface   any                        `json:",omitzero"`
}

// pjQuoted has the ",string" option on fields of every kind, also those it
// does not apply to.
//
//nolint:staticcheck // SA5008: the option on other kinds is what is tested.
type pjQuoted struct {
	Int     int         `json:",string"`
	Int8    int8        `json:",string"`
	Uint    uint64      `json:",string"`
	Float   float64     `json:",string"`
	Float32 float32     `json:",string"`
	Bool    bool        `json:",string"`
	String  string      `json:",string"`
	Ptr     *int        `json:",string"`
	NilPtr  *int        `json:",string"`
	PtrPtr  **int       `json:",string"`
	Named   pjInt       `json:",string"`
	NamedS  pjString    `json:",string"`
	NamedP  pjPtr       `json:",string"`
	Number  json.Number `json:",string"`
	Slice   []int       `json:",string"`
	Iface   any         `json:",string"`
	Marsh   pjJSONVal   `json:",string"`
	Text    pjTextVal   `json:",string"`
	Both    int         `json:"both,omitempty,string"`
}

type PjEmbA struct {
	A      int
	Shared string
}

type PjEmbB struct {
	B      int
	Shared string
}

// Shared is untagged at the same depth in both: it is left out.
type pjTwoEmbedded struct {
	PjEmbA
	PjEmbB
}

type PjTagged struct {
	X int `json:"Shared"`
}

// A tagged field wins over an untagged one at the same depth.
type pjTaggedWins struct {
	PjEmbA
	PjTagged
}

// A less nested field wins over embedded ones.
type pjOuterWins struct {
	PjEmbA
	PjEmbB
	Shared float64
}

// Embedded pointers, nil or not; the tagged one is a named field.
type pjEmbPointers struct {
	*PjEmbA
	*PjEmbB `json:"b"`
}

type pjhidden struct {
	Visible int
	hidden  int
}

type pjhiddenPtr struct{ ViaPointer int }

// Fields promoted through an unexported embedded struct and a pointer to one.
type pjUnexportedEmbedded struct {
	pjhidden
	*pjhiddenPtr
	Own int
}

// An unexported embedded struct named by its tag is a field of that name.
type pjUnexportedTagged struct {
	pjhidden `json:"hidden"`
}

// An embedded exported non-struct type is a field named after the type; an
// unexported one is left out; an embedded interface is a field.
type pjEmbeddedNonStruct struct {
	PjEmbInt
	pjInt
	fmt.Stringer
}

// PjCommon is embedded twice at the same depth (through PjLevelA and
// PjLevelB): its fields are ambiguous and left out.
type PjCommon struct {
	C int
	D int
}

type PjLevelA struct {
	PjCommon
	LA int
}

type PjLevelB struct {
	PjCommon
	LB int
}

type pjDiamond struct {
	PjLevelA
	PjLevelB
}

type PjDeep3 struct {
	X int `json:"x"`
	Y int
	Z int
}

type PjDeep2 struct {
	PjDeep3
	Y int `json:"Y"`
	W int
}

// Fields at three depths: of each name, the least nested one wins.
type pjDeep1 struct {
	PjDeep2
	Z string
}

type pjNode struct {
	Name     string    `json:"name"`
	Children []*pjNode `json:"children,omitempty"`
	Parent   *pjNode   `json:"-"`
}

// pjEmbedsJSON gets pjJSONVal's MarshalJSON promoted: it is encoded through it.
type pjEmbedsJSON struct {
	pjJSONVal
	Other int
}

// pjEmbedsTime gets time.Time's MarshalJSON promoted: it is encoded through
// it, zone and all — it is not a time.Time.
type pjEmbedsTime struct {
	time.Time
	Note string
}

// pjEmbedsPtrJSON gets MarshalJSON promoted to its pointer type only.
type pjEmbedsPtrJSON struct {
	pjJSONPtr
	Other int
}

// pjAddressable holds types whose methods have pointer receivers:
// encoding/json uses them only where the value is addressable.
type pjAddressable struct {
	J  pjJSONPtr
	T  pjTextPtr
	M  pjMixed
	A  [2]pjJSONPtr
	S  []pjJSONPtr
	MV map[string]pjJSONPtr
	Z  pjZeroPtr `json:",omitzero"`
	B  big.Int
	BP *big.Int
}

type pjInterfaces struct {
	M json.Marshaler
	T encoding.TextMarshaler
	S fmt.Stringer
	E error
	A any
}

type pjSpecial struct {
	Raw    json.RawMessage
	RawNil json.RawMessage
	RawPtr *json.RawMessage
	Num    json.Number
	Nums   []json.Number
	Time   time.Time
	TimeP  *time.Time
	Times  map[string]time.Time
	Dur    time.Duration
	Month  time.Month
	Big    *big.Int
	BigF   *big.Float
}

// pjEmbedsTimePtr gets time.Time's methods promoted through a pointer.
type pjEmbedsTimePtr struct {
	*time.Time
	N int
}

// PjStamp embeds time.Time, and pjStamped a PjStamp: time.Time's methods
// are promoted two levels up.
type PjStamp struct{ time.Time }

type pjStamped struct {
	PjStamp
	V int
}

// pjhiddenStamp is an unexported PjStamp: methods are promoted through it
// all the same.
type pjhiddenStamp struct{ time.Time }

type pjHiddenStamped struct {
	pjhiddenStamp
	V int
}

// pjTwoStamps embeds two time.Time at the same depth: their methods
// conflict, so neither is promoted and it is encoded as a struct.
type pjTwoStamps struct {
	PjStamp
	pjhiddenStamp
}

// pjTwoStampsJSON is pjTwoStamps with a MarshalJSON of its own that sends
// one of the two times as it encodes itself.
type pjTwoStampsJSON struct {
	PjStamp
	pjhiddenStamp
}

func (s pjTwoStampsJSON) MarshalJSON() ([]byte, error) { return s.pjhiddenStamp.MarshalJSON() }

// pjShallowerJSON has a MarshalJSON one level up from a time.Time's, which
// wins.
type pjShallowerJSON struct {
	pjJSONVal
	PjStamp
}

// pjEpoch embeds time.Time but declares its own MarshalJSON, which wins: it
// is sent as epoch milliseconds, not as the time.Time it embeds.
type pjEpoch struct{ time.Time }

func (e pjEpoch) MarshalJSON() ([]byte, error) { return strconv.AppendInt(nil, e.UnixMilli(), 10), nil }

// pjNullTime embeds time.Time and declares a MarshalJSON that sends the
// zero time as null and any other as the time.Time encodes itself.
type pjNullTime struct{ time.Time }

func (n pjNullTime) MarshalJSON() ([]byte, error) {
	if n.IsZero() {
		return []byte("null"), nil
	}
	return n.Time.MarshalJSON()
}

// pjRecursive embeds a pointer to itself, through which its own MarshalText
// would be promoted were it not declared on it.
type pjRecursive struct {
	*pjRecursive
	N int
}

func (r pjRecursive) MarshalText() ([]byte, error) { return fmt.Appendf(nil, "rec-%d", r.N), nil }

// pjOptional has a MarshalJSON for a nil receiver too.
type pjOptional struct{ V int }

func (o *pjOptional) MarshalJSON() ([]byte, error) {
	if o == nil {
		return []byte(`"unset"`), nil
	}
	return strconv.AppendInt(nil, int64(o.V), 10), nil
}

// pjPanicky's MarshalJSON panics with a value of its own, not a run-time
// error.
type pjPanicky struct{}

func (*pjPanicky) MarshalJSON() ([]byte, error) { panic("pjPanicky: refused") }

// pjRenamed maps its field onto another column with a MarshalJSON method
// that has a pointer receiver.
type pjRenamed struct {
	Temp float64 `json:"temperature"`
}

func (r *pjRenamed) MarshalJSON() ([]byte, error) {
	return fmt.Appendf(nil, `{"temp_c": %g}`, r.Temp), nil
}

// pjState is an enum whose MarshalText has a pointer receiver.
type pjState int

func (s *pjState) MarshalText() ([]byte, error) { return []byte([]string{"idle", "running"}[*s]), nil }

type pjMachine struct {
	State pjState `json:"state"`
	Temp  float64 `json:"temperature"`
}

// pjCounter holds a big.Int, whose methods have pointer receivers, by value.
type pjCounter struct {
	Count big.Int `json:"count"`
}

// pjTextRow is a row type whose MarshalText has a pointer receiver.
type pjTextRow struct {
	A int `json:"a"`
}

func (r *pjTextRow) MarshalText() ([]byte, error) { return fmt.Appendf(nil, "text-%d", r.A), nil }

// pjSelfKey is a pointer type that can point to itself.
type pjSelfKey *pjSelfKey

func pjSelfKeyLoop() pjSelfKey {
	p := new(pjSelfKey)
	*p = p
	return p
}

// pjFormat has the "format" tag option, which encoding/json refuses (only
// encoding/json/v2 supports it).
type pjFormat struct {
	T time.Time `json:",format:unix"`
}

// pjNested returns n nested non-empty lists around 1.
func pjNested(n int) any { return pjNestedAround(n, 1) }

// pjNestedAround returns n nested non-empty lists around v.
func pjNestedAround(n int, v any) any {
	for range n {
		v = []any{v}
	}
	return v
}

// pjAppender has AppendText with a pointer receiver (for a nil receiver
// too) and MarshalText with a value receiver: encoding/json prefers
// AppendText where it can call it.
type pjAppender struct{ N int }

func (a *pjAppender) AppendText(b []byte) ([]byte, error) {
	if a == nil {
		return append(b, "append-nil"...), nil
	}
	return fmt.Appendf(b, "append-%d", a.N), nil
}

func (a pjAppender) MarshalText() ([]byte, error) { return fmt.Appendf(nil, "text-%d", a.N), nil }

// pjByteApp is a byte type with AppendText: a []pjByteApp is encoded element
// by element.
type pjByteApp byte

func (b pjByteApp) AppendText(dst []byte) ([]byte, error) { return fmt.Appendf(dst, "a%d", b), nil }

// pjJSONTo encodes itself with json/v2's MarshalJSONTo, which encoding/json
// calls first.
type pjJSONTo struct{ N int }

func (v pjJSONTo) MarshalJSONTo(enc *jsontext.Encoder) error {
	return enc.WriteValue(jsontext.Value(fmt.Appendf(nil, `{"to": %d, "f": 2.0, "s": "a\u00ffb"}`, v.N)))
}

// pjJSONToPtr has MarshalJSONTo with a pointer receiver and MarshalJSON with
// a value receiver: the first where the value is addressable.
type pjJSONToPtr struct{ N int }

func (v *pjJSONToPtr) MarshalJSONTo(enc *jsontext.Encoder) error {
	if v == nil {
		return enc.WriteToken(jsontext.String("to-nil"))
	}
	return enc.WriteToken(jsontext.String(fmt.Sprint("to-ptr-", v.N)))
}

func (v pjJSONToPtr) MarshalJSON() ([]byte, error) { return fmt.Appendf(nil, `"json-%d"`, v.N), nil }

// pjEmbed has fields with the ",embed" option: a struct's fields join the
// struct's own; a map's or a jsontext.Value's members are added after them.
type pjEmbed struct {
	A     int
	In    PjEmbB         `json:",embed"`
	Named PjEmbA         `json:"named,embed"` // a named field after all
	P     *PjTagged      `json:",embed"`      // its tagged Shared wins over In's
	Int   int            `json:",embed"`      // not a struct: a field
	Extra map[string]any `json:",embed"`
}

type pjEmbedRaw struct {
	A   int
	Raw jsontext.Value `json:",embed"`
}

// pjTwoFallbacks has two embedded fallbacks at the same depth: neither is
// encoded.
type pjTwoFallbacks struct {
	M1 map[string]int `json:",embed"`
	M2 map[string]int `json:",embed"`
	N  int
}

// pjMethodKeyFallback's map has a key type with methods: it is a field.
type pjMethodKeyFallback struct {
	M map[pjStringKey]int `json:",embed"`
}

// pjOddTags has tags whose names end early or are not names at all.
//
//nolint:staticcheck // SA5008: the odd tags are what is tested.
type pjOddTags struct {
	Quoted   int `json:"'q'"`
	Escaped  int `json:"a b\\c"`
	Mid      int `json:"ab-c\"d"`
	Leading  int `json:"\"x"`
	Bad      int `json:"\xff"`
	Digit    int `json:"9lives"`
	Case     int `json:"c,case:ignore"`
	CaseOnly int `json:",case:strict"`
	BadOpt   int `json:",omitEmpty"`
	Format   int `json:",format"`
	Embedded int `json:"e,embed"`
}

// pjQuotedPointers has the ",string" option through pointers, named or
// not, and on types it does not change.
//
//nolint:staticcheck // SA5008: the option on other kinds is what is tested.
type pjQuotedPointers struct {
	PNamedP *pjPtr         `json:",string"`
	Dur     time.Duration  `json:",string"`
	PDur    *time.Duration `json:",string"`
	Num     *json.Number   `json:",string"`
	PBool   *bool          `json:",string"`
	PStr    *string        `json:",string"`
	PFloat  *float32       `json:",string"`
	Time    time.Time      `json:",string"`
	Bytes   []byte         `json:",string"`
	State   pjState        `json:",string"`
	Map     map[string]int `json:",string"`
	Arr     [1]int         `json:",string"`
	Appd    pjAppender     `json:",string"`
}

// pjCorpus returns the values normalize must encode as encoding/json does
// (up to the documented differences): every struct-tag rule, embedding,
// map keys, nil values, marshalers, special types, and values both refuse.
func pjCorpus() []struct {
	name string
	v    any
} {
	n, m := 7, 0
	pn := &n
	at := time.Date(2026, 10, 8, 14, 0, 0, 123456789, time.FixedZone("CEST", 2*3600))
	atUTC := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	tree := &pjNode{Name: "root", Children: []*pjNode{{Name: "a"}, {Name: "b", Children: []*pjNode{{Name: "c"}}}}}
	tree.Children[0].Parent = tree
	addressable := pjAddressable{
		J: pjJSONPtr{1}, T: pjTextPtr{"t"}, M: pjMixed{2}, A: [2]pjJSONPtr{{3}, {4}},
		S: []pjJSONPtr{{5}}, MV: map[string]pjJSONPtr{"k": {6}}, Z: pjZeroPtr{1}, BP: big.NewInt(-42),
	}
	addressable.B.SetString("123456789012345678901234567890", 10)
	raw := json.RawMessage(`{"z": [1, 2.0, "s"]}`)
	bigF, _ := new(big.Float).SetString("1.5e100")
	cyclicMap := map[string]any{}
	cyclicMap["self"] = cyclicMap
	cyclicSlice := []any{nil}
	cyclicSlice[0] = cyclicSlice
	type pjCycle struct {
		Next *pjCycle
	}
	cyclicPtr := &pjCycle{}
	cyclicPtr.Next = cyclicPtr
	str, state, half := "s", pjState(1), float32(0.5)
	dur, yes := time.Second, true
	num := json.Number("1.5")

	type c = struct {
		name string
		v    any
	}
	return []c{
		// Scalars.
		{"nil", nil},
		{"bools", []any{true, false, pjBool(true)}},
		{"strings", []string{"", "plain", "html <a href='x'>&amp;</a>", "  ", "quote \" backslash \\",
			"ctl \x00\x01\x1f\n\t", "héllo 世界 🎉", "a\xffb\xc0", "\xed\xa0\x80", "�", "2026-01-01T00:00:00+01:00"}},
		{"ints", []any{0, -1, int64(math.MinInt64), int64(math.MaxInt64), int8(-128), int16(1000), int32(-7), uint8(255),
			uint16(65535), uint32(math.MaxUint32), uint64(math.MaxUint64), uint64(1 << 63), uintptr(42), pjInt(3), pjUint(9)}},
		{"floats", []any{0.0, math.Copysign(0, -1), 1.5, -2.25, 2.0, 1e20, 1e21, 1e-7, 123456789.125, math.MaxFloat64,
			math.SmallestNonzeroFloat64, float64(1 << 63), float64(1 << 64), -float64(1 << 63), float64(1<<53 + 1), pjFloat(4)}},
		{"float32s", []any{float32(0.1), float32(1.1), float32(16777217), float32(3.4e38), float32(1e-7),
			float32(math.SmallestNonzeroFloat32), pjFloat32(22.3), float32(math.Copysign(0, -1))}},
		{"numbers", []json.Number{"12", "-7", "1.5", "1e3", "18446744073709551615", "", "0.000001", "-0", "2.0"}},
		{"times", []any{atUTC, at, time.Time{}, &at, (*time.Time)(nil), time.Unix(0, 1).In(time.FixedZone("x", -5*3600-1800))}},
		// Named and composite types.
		{"named", []any{pjString("s"), pjBytes("b"), pjMap{"a": 1}, pjSlice{"x"}, pjPtr(pn), pjPtr(nil), PjEmbInt(5)}},
		{"slices", []any{[]int{}, []int(nil), []string{"a"}, [][]int{nil, {}, {1}}, []*int{nil, pn}, []any{1, "a", nil, 2.5, []any{}, map[string]any{}}}},
		{"arrays", []any{[3]int{1, 2, 3}, [0]int{}, [2][]int{{1}, nil}, [4]byte{1, 2, 3, 255}, [2]*int{nil, pn}}},
		{"bytes", []any{[]byte(nil), []byte{}, []byte("bin\x00\xff"), [][]byte{[]byte("a"), nil}, []pjByte{1, 2}, pjBytes(nil)}},
		{"byte elements with marshal methods", []pjByteText{1, 2}},
		{"pointers", []any{pn, &pn, (*int)(nil), &[]int{1}, &map[string]int{"a": 1}, &PjEmbA{A: 1}}},
		{"maps", []any{
			map[string]int{"b": 2, "a": 1},
			map[string]int(nil),
			map[int]string{-5: "x", 10: "y"},
			map[uint8]bool{1: true},
			map[int64]float64{math.MinInt64: 1.5},
			map[uint64]int{math.MaxUint64: 1},
			map[uintptr]int{7: 1},
			map[pjString]int{"s": 1},
			map[pjKey]string{{1, 2}: "a", {3, 4}: "b"},
			map[pjIntKey]int{3: 1, -4: 2},
			map[pjStringKey]int{"s": 1},
			map[*pjPtrKey]int{nil: 1, {N: 2}: 2},
			map[string][]byte{"b": []byte("x")},
			map[string]map[string]any{"m": {"n": nil}},
			map[string]func(){},
		}},
		{"map keys of other types", []any{
			map[float64]string{1.5: "a", 1e21: "b", math.Copysign(0, -1): "c", 2: "d", 1e-7: "e"},
			map[float32]string{0.1: "a", 16777216: "b"},
			map[*int]string{pn: "p"},
			map[*string]int{&str: 1},
			map[**int]int{&pn: 1},
			map[any]int{"a": 1, 2: 2, int8(-3): 3, float32(1.5): 4, uint(5): 5},
			map[any]int{pjKey{1, 2}: 1, &pjPtrKey{N: 6}: 2, at: 3, &atUTC: 4, pjAppender{1}: 5, &pjAppender{2}: 6},
			map[fmt.Stringer]int{pjStringer("s"): 1, time.Duration(7): 2},
			map[time.Duration]int{time.Second: 1},
			map[json.Number]int{"1.50": 1},
			map[*big.Int]int{big.NewInt(-7): 1},
			map[pjAppender]int{{1}: 1},
			map[*pjAppender]int{{2}: 1, nil: 2},
			map[pjState]int{1: 1},
			map[*pjState]int{&state: 1},
			map[encoding.TextAppender]int{&pjAppender{3}: 1, (*pjAppender)(nil): 2, at: 3},
			map[*[]byte]int{{1, 2, 3}: 1},
		}},
		{"empty maps with keys that are no strings", []any{
			map[[2]int]int{}, map[bool]int{}, map[struct{ A int }]int{}, map[chan int]int{}, map[pjJSONVal]int{},
			map[complex64]int{}, map[any]int{},
		}},
		{"map keys that coerce to the same string", []any{
			map[string]int{"\xff": 1, "�": 2, "\xfe": 3, "ok": 4},
			map[string]int{"�": 2, "\xef": 5},
			Row{"a\xffb": "first", "a�b": "second"},
		}},
		{"rows", []any{
			Row{"rows": []Row{{"a": 1}, {"b": []any{Row{"c": pjTags{Plain: 1}}, nil}}}, "deep": Row{"x": Row{"y": []Row{nil, {}}}}},
			[]Row{{"temperature": 21.5, "tsp": at}, {"temperature": 2.0, "ok": true}},
			map[string]Row{"r": {"k": Kwargs{"x": 1}}},
			Kwargs{"kw": []Kwargs{{"a": 1}}},
		}},
		// Struct tags.
		{"tags", pjTags{Plain: 1, Named: 2, Dash: 3, DashComma: 4, OnlyOptions: 5, Punct: "p", Unicode: "u",
			Invalid: "i", Space: "s", TrailComma: "t", Upper: 6, Lower: 7, Unknown: 8, unexported: 9}},
		{"tags, zero", pjTags{}},
		{"omitempty, zero", pjOmitEmpty{}},
		{"omitempty, empty but not nil", pjOmitEmpty{Bytes: []byte{}, Slice: []int{}, Map: map[string]int{}, Iface: (*int)(nil)}},
		{"omitempty, set", pjOmitEmpty{Bool: true, Int: -1, Int8: 1, Uint: 1, Uintptr: 1, Float: 0.5, Float32: 1.5,
			String: "s", Bytes: []byte("b"), Slice: []int{1}, Map: map[string]int{"a": 1}, Array: [2]int{1, 0},
			Ptr: &m, Iface: 0, Time: at, Number: "1", Named: 1, MarshP: &pjJSONVal{1}}},
		{"omitzero, zero", pjOmitZero{}},
		{"omitzero, zero by IsZero", pjOmitZero{ZeroVal: pjZeroVal{-1}, ZeroPtr: pjZeroPtr{-1}, ZeroPP: &pjZeroPtr{}, ZeroIf: pjZeroVal{}}},
		{"omitzero, set", pjOmitZero{Int: 1, Float: 0.5, String: "s", Slice: []int{}, Map: map[string]int{}, Array: [2]int{0, 1},
			Struct: struct{ A int }{1}, Time: at, TimePtr: &time.Time{}, ZeroVal: pjZeroVal{1}, ZeroPtr: pjZeroPtr{1},
			ZeroPP: &pjZeroPtr{1}, ZeroIf: &pjZeroPtr{1}, Both: []int{}, Iface: false}},
		{"omitzero, addressable", &pjOmitZero{ZeroPtr: pjZeroPtr{-1}}},
		{"omitzero, nil pointer in an IsZero interface", pjOmitZero{ZeroIf: (*pjZeroPtr)(nil)}},
		{"string option", pjQuoted{Int: -1, Int8: 2, Uint: math.MaxUint64, Float: 1e21, Float32: 0.1, Bool: true,
			String: "a<b>&\"c\xff", Ptr: &n, PtrPtr: &pn, Named: 3, NamedS: "n", NamedP: pn, Number: "12",
			Slice: []int{1}, Iface: 1, Both: 4}},
		{"string option, zero", pjQuoted{}},
		{"string option, small float", pjQuoted{Float: 1e-7, Float32: 1e-7}},
		{"string option through pointers", pjQuotedPointers{PNamedP: (*pjPtr)(&pn), Dur: dur, PDur: &dur, Num: &num,
			PBool: &yes, PStr: &str, PFloat: &half, Time: at, Bytes: []byte("b"), State: 1, Map: map[string]int{"a": 1},
			Arr: [1]int{2}, Appd: pjAppender{3}}},
		{"string option through pointers, zero", pjQuotedPointers{}},
		{"string option through pointers, addressable", &pjQuotedPointers{State: 1, Num: new(json.Number)}},
		{"odd tags", pjOddTags{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}},
		{"embed option", []any{
			pjEmbed{A: 1, In: PjEmbB{2, "b"}, Named: PjEmbA{3, "a"}, P: &PjTagged{4}, Int: 5, Extra: map[string]any{"A": "extra", "z": 1.5, "\xff": 1, "\xfe": 2}},
			pjEmbed{A: 1, In: PjEmbB{2, "b"}},
			&pjEmbed{Extra: map[string]any{"B": nil}},
			pjEmbedRaw{A: 1, Raw: jsontext.Value(` {"A": 9, "r": [1, 2.0, {"x": null}], "A": 10} `)},
			pjEmbedRaw{A: 2},
			pjTwoFallbacks{M1: map[string]int{"a": 1}, M2: map[string]int{"b": 2}, N: 3},
			pjMethodKeyFallback{M: map[pjStringKey]int{"k": 1}},
		}},
		// Embedding.
		{"conflict at the same depth", pjTwoEmbedded{PjEmbA{1, "a"}, PjEmbB{2, "b"}}},
		{"tagged wins", pjTaggedWins{PjEmbA{1, "a"}, PjTagged{2}}},
		{"less nested wins", pjOuterWins{PjEmbA{1, "a"}, PjEmbB{2, "b"}, 3.5}},
		{"embedded pointers", pjEmbPointers{&PjEmbA{1, "a"}, &PjEmbB{2, "b"}}},
		{"nil embedded pointers", pjEmbPointers{}},
		{"through unexported embedded structs", pjUnexportedEmbedded{pjhidden{1, 2}, &pjhiddenPtr{3}, 4}},
		{"through a nil unexported embedded pointer", pjUnexportedEmbedded{pjhidden: pjhidden{1, 2}}},
		{"unexported embedded struct with a tag", pjUnexportedTagged{pjhidden{1, 2}}},
		{"embedded non-structs", pjEmbeddedNonStruct{PjEmbInt(1), pjInt(2), time.Duration(3)}},
		{"embedded twice", pjDiamond{PjLevelA{PjCommon{1, 2}, 3}, PjLevelB{PjCommon{4, 5}, 6}}},
		{"three depths", pjDeep1{PjDeep2{PjDeep3{1, 2, 3}, 4, 5}, "z"}},
		{"recursive type", tree},
		// Marshalers.
		{"MarshalJSON", []any{pjJSONVal{1}, &pjJSONVal{2}, (*pjJSONVal)(nil), pjList{1, 2}, pjList(nil), pjBoth{}}},
		{"MarshalJSON output", []pjRawJSON{` [1, 2.0] `, `"a<b"`, "\"a\xffb\"", `null`, `{"a": {"b": [true]}}`, `-0.0`, `1E2`}},
		{"MarshalText", []any{pjTextVal{"v"}, &pjTextVal{"p"}, &pjTextPtr{"q"}, pjMixed{1}, &pjMixed{2}, pjTextVal{"\xff"}}},
		{"AppendText", []any{pjAppender{1}, &pjAppender{2}, []pjAppender{{3}}, [1]pjAppender{{4}}, map[string]pjAppender{"m": {5}},
			struct{ A pjAppender }{pjAppender{6}}, &struct{ A pjAppender }{pjAppender{7}}, []pjByteApp{1, 2}, big.NewInt(8)}},
		{"MarshalJSONTo", []any{pjJSONTo{1}, &pjJSONTo{2}, pjJSONToPtr{3}, &pjJSONToPtr{4}, []pjJSONToPtr{{5}},
			struct{ T pjJSONTo }{pjJSONTo{6}}, json.Number("7"), map[string]pjJSONToPtr{"m": {8}}}},
		{"nil pointers in interfaces of each marshal method", []any{
			struct{ T encoding.TextAppender }{(*pjAppender)(nil)},
			struct{ T jsonv2.MarshalerTo }{(*pjJSONToPtr)(nil)},
			struct{ T encoding.TextAppender }{&pjAppender{9}},
			struct{ T jsonv2.MarshalerTo }{&pjJSONToPtr{10}},
			struct{ T jsonv2.MarshalerTo }{pjJSONTo{11}},
		}},
		{"unexported embedded structs with methods, named by tags", []any{pjAmbiguousJSON{}, &pjAmbiguousJSON{}, pjAmbiguousText{}, pjAmbiguousZero{},
			pjHiddenMethods{pjHiddenUnmarshal{1}, pjHiddenPtrText{2}, pjhidden{3, 4}}}},
		{"pointer receivers, not addressable", addressable},
		{"pointer receivers, addressable", &addressable},
		{"pointer receivers in a slice", []pjAddressable{addressable}},
		{"pointer receivers in a map", map[string]pjAddressable{"a": addressable}},
		{"pointer receivers in an array", [1]pjAddressable{addressable}},
		{"pointer receivers in an addressable array", &[1]pjAddressable{addressable}},
		{"embedded MarshalJSON", pjEmbedsJSON{pjJSONVal{1}, 2}},
		{"embedded time.Time", pjEmbedsTime{at, "note"}},
		{"embedded time.Time, other ways", []any{pjEmbedsTimePtr{&at, 1}, &pjEmbedsTimePtr{&at, 2}, pjStamped{PjStamp{at}, 3},
			PjStamped2{&PjStamp{at}}, pjHiddenStamped{pjhiddenStamp{at}, 4}, pjTwoStamps{PjStamp{at}, pjhiddenStamp{atUTC}},
			pjShallowerJSON{pjJSONVal{1}, PjStamp{at}}, pjEpoch{at}, pjNullTime{at}, pjNullTime{}}},
		{"time map keys", []any{
			map[time.Time]int{at: 1, atUTC: 2, {}: 3},
			map[*time.Time]string{&at: "a", nil: "nil"},
			map[encoding.TextMarshaler]int{at: 1, &atUTC: 2, pjTextVal{"v"}: 3, (*big.Int)(nil): 4},
			map[pjEmbedsTime]int{{at, "note"}: 1},
			map[pjEpoch]int{{at}: 1},
			map[PjStamp]int{{atUTC}: 1},
			map[string]map[time.Time]float64{"r": {at: 21.5}},
		}},
		{"time map keys naming the same instant", map[time.Time]string{at: "cest", at.UTC(): "utc", at.In(time.FixedZone("x", -5*3600)): "x"}},
		{"embedded pointer MarshalJSON", []any{pjEmbedsPtrJSON{pjJSONPtr{1}, 2}, &pjEmbedsPtrJSON{pjJSONPtr{3}, 4}}},
		{"interfaces", pjInterfaces{M: pjJSONVal{1}, T: pjBoth{}, S: time.Duration(1500), E: errors.New("x"), A: &pjJSONPtr{2}}},
		{"interfaces holding pointers", pjInterfaces{M: &pjMixed{}, T: &pjTextPtr{"p"}, S: tree.Children[1].Name2(), A: pjMixed{}}},
		{"nil interfaces", pjInterfaces{}},
		{"interfaces holding nil pointers with nil-safe methods", []any{
			pjInterfaces{M: (*pjMixed)(nil), T: (*big.Float)(nil)},
			pjInterfaces{M: (*big.Int)(nil), T: (*big.Int)(nil)},
			pjInterfaces{M: (*pjOptional)(nil)},
		}},
		{"marshaler elements and map values holding nil pointers", []any{
			[]encoding.TextMarshaler{(*big.Float)(nil), (*big.Int)(nil), nil},
			[]json.Marshaler{(*pjOptional)(nil), (*pjMixed)(nil), nil, &pjOptional{V: 1}},
			map[string]json.Marshaler{"m": (*pjMixed)(nil), "o": (*pjOptional)(nil), "n": nil},
			map[encoding.TextMarshaler]int{(*big.Int)(nil): 1},
		}},
		{"methods promoted through embedded pointers and interfaces", []any{
			struct{ *pjOptional }{}, struct{ *pjOptional }{&pjOptional{V: 2}}, struct{ *big.Int }{},
			struct{ json.Marshaler }{pjJSONVal{1}}, struct{ json.Marshaler }{(*pjOptional)(nil)},
			struct{ encoding.TextMarshaler }{(*big.Float)(nil)}, pjRecursive{nil, 1}, pjRecursive{&pjRecursive{N: 2}, 3},
		}},
		{"special types", pjSpecial{Raw: raw, RawPtr: &raw, Num: "-1.5e3", Nums: []json.Number{"1", "2.5"}, Time: at,
			TimeP: &at, Times: map[string]time.Time{"t": at}, Dur: time.Second, Month: time.March, Big: big.NewInt(7), BigF: bigF}},
		{"special types, zero", pjSpecial{}},
		{"json.RawMessage", []any{raw, json.RawMessage(nil), json.RawMessage("null"), []json.RawMessage{raw, nil}}},
		{"big numbers", []any{big.NewInt(-5), new(big.Int).Lsh(big.NewInt(1), 80), *big.NewInt(3), big.NewFloat(0.25)}},
		// Values both refuse.
		{"channel", make(chan int)},
		{"nil channel field", struct{ C chan int }{}},
		{"function", func() {}},
		{"complex", complex64(1)},
		{"unsafe pointer", unsafe.Pointer(&n)},
		{"bool map key", map[bool]int{true: 1}},
		{"NaN map key", map[float64]int{math.NaN(): 1}},
		{"float64 in an interface map key", map[any]int{1.5: 1}},
		{"bool in an interface map key", map[any]int{true: 1}},
		{"nil interface map key", map[any]int{nil: 1}},
		{"nil pointer map key", map[*int]int{nil: 1}},
		{"array map key", map[[2]int]int{{1, 2}: 1}},
		{"struct map key", map[struct{ A int }]int{{1}: 1}},
		{"map key with MarshalJSON only", map[pjJSONVal]int{{1}: 1}},
		{"map key with MarshalJSON only, in an interface", map[any]int{pjJSONVal{1}: 1}},
		{"map key with MarshalText with a pointer receiver, in an interface", map[any]int{pjPtrKey{1}: 1}},
		{"nil pointer in a json.Marshaler map key", map[json.Marshaler]int{(*pjOptional)(nil): 1}},
		{"format tag option", pjFormat{}},
		{"embedded JSON that is not an object", pjEmbedRaw{Raw: jsontext.Value("[1]")}},
		{"embedded JSON that is not valid", pjEmbedRaw{Raw: jsontext.Value("{")}},
		{"func in a slice", []any{1, func() {}}},
		{"empty slice of funcs", []func(){}},
		{"cyclic map", cyclicMap},
		{"cyclic slice", cyclicSlice},
		{"cyclic pointer", cyclicPtr},
		{"cyclic map in a struct", struct{ M map[string]any }{cyclicMap}},
		{"MarshalJSON fails", pjFailJSON{}},
		{"MarshalJSON returns invalid JSON", []pjRawJSON{"{"}},
		{"MarshalJSON returns trailing data", pjRawJSON("1 2")},
		{"MarshalJSON returns nothing", pjRawJSON("")},
		{"MarshalText fails", pjFailText{}},
		{"MarshalText of a key fails", map[pjFailKey]int{1: 1}},
		{"invalid number", json.Number("abc")},
		{"number with a space", json.Number(" 1")},
		{"number with a leading zero", json.Number("01")},
		{"number out of range", json.Number("1e400")},
		{"string option on NaN", pjQuoted{Float: math.NaN()}},
	}
}

// Name2 returns the name, as an fmt.Stringer.
func (n *pjNode) Name2() fmt.Stringer { return pjStringer(n.Name) }

type pjStringer string

func (s pjStringer) String() string { return string(s) }

// normalize encodes values as encoding/json does — every struct-tag rule,
// embedding, map keys, marshalers — except for its documented differences.
func TestNormalizeMatchesEncodingJSON(t *testing.T) {
	for _, c := range pjCorpus() {
		checkLikeJSON(t, c.name, c.v, true)
		// The same value at the top level and inside other values: in an
		// interface, a struct field, a map, a slice (where it is
		// addressable) and through a pointer.
		holder := struct{ V any }{c.v}
		checkLikeJSON(t, c.name+" in a struct", holder, true)
		checkLikeJSON(t, c.name+" in a Row", Row{"v": c.v}, true)
		checkLikeJSON(t, c.name+" in a list", []any{c.v}, true)
		checkLikeJSON(t, c.name+" through a pointer", &holder, true)
		if c.v != nil {
			slice := reflect.MakeSlice(reflect.SliceOf(reflect.TypeOf(c.v)), 1, 1)
			slice.Index(0).Set(reflect.ValueOf(c.v))
			checkLikeJSON(t, c.name+" in a typed slice", slice.Interface(), true)
		}
	}
}

// Objects and arrays nested more than 10000 deep fail, counted as
// json.Marshal counts them: it writes an empty slice or map without counting
// it, but not an empty array or struct, and counts the objects and arrays in
// what a MarshalJSON method returns.
func TestNormalizeNestingLimit(t *testing.T) {
	cases := []struct {
		name string
		v    any
		ok   bool
	}{
		{"10000 lists", pjNested(maxNesting), true},
		{"10001 lists", pjNested(maxNesting + 1), false},
		{"10001 lists in a struct", struct{ L any }{pjNested(maxNesting)}, false},
		{"10000 lists around an empty one", pjNestedAround(maxNesting, []any{}), true},
		{"10000 lists around an empty typed slice", pjNestedAround(maxNesting, []int{}), true},
		{"10000 lists around an empty map", pjNestedAround(maxNesting, map[string]int{}), true},
		{"10000 lists around an empty Row", pjNestedAround(maxNesting, Row{}), true},
		{"10000 lists around an empty array", pjNestedAround(maxNesting, [0]int{}), false},
		{"10000 lists around an empty struct", pjNestedAround(maxNesting, struct{}{}), false},
		{"MarshalJSON output 10000 deep", pjNestedAround(maxNesting-2, pjRawJSON("[[]]")), true},
		{"MarshalJSON output 10001 deep", pjNestedAround(maxNesting-1, pjRawJSON("[[]]")), false},
		{"MarshalJSON output 10001 deep, the innermost empty", pjNestedAround(maxNesting-1, pjRawJSON("[{}]")), false},
	}
	for _, c := range cases {
		if _, err := json.Marshal(c.v); (err == nil) != c.ok {
			t.Errorf("%s: encoding/json: %v", c.name, err)
		}
		if _, err := normalize(c.v); (err == nil) != c.ok {
			t.Errorf("%s: %v", c.name, err)
		}
	}
}

// ---- properties over random types and values ----

// pjRandom builds random types — struct types made with reflect.StructOf,
// with random field names, tags and embedding, inside slices, arrays, maps
// and pointers — and random values of them.
type pjRandom struct {
	types, values *rand.Rand
	// strict makes every float non-integral, so that comparing with
	// encoding/json needs no leniency to tell floats from integers.
	strict bool
	// quoted allows the ",string" option in tags.
	quoted bool
	// special is put in about every fourth float.
	special float64
}

// pjSentinel is the special float of finite values: exact in float32 and
// float64, not integral, and in no float pool.
const pjSentinel = 1234.5

var (
	pjLeafTypes = []reflect.Type{
		reflect.TypeFor[bool](), reflect.TypeFor[int](), reflect.TypeFor[int8](), reflect.TypeFor[int16](),
		reflect.TypeFor[int32](), reflect.TypeFor[int64](), reflect.TypeFor[uint](), reflect.TypeFor[uint8](),
		reflect.TypeFor[uint16](), reflect.TypeFor[uint32](), reflect.TypeFor[uint64](), reflect.TypeFor[uintptr](),
		reflect.TypeFor[float32](), reflect.TypeFor[float64](), reflect.TypeFor[string](), reflect.TypeFor[[]byte](),
		reflect.TypeFor[pjInt](), reflect.TypeFor[pjString](), reflect.TypeFor[pjFloat](), reflect.TypeFor[pjFloat32](),
		reflect.TypeFor[pjBytes](), reflect.TypeFor[pjByte](), reflect.TypeFor[time.Time](), reflect.TypeFor[json.Number](),
		reflect.TypeFor[any](), reflect.TypeFor[json.RawMessage](), reflect.TypeFor[pjJSONVal](), reflect.TypeFor[pjJSONPtr](),
		reflect.TypeFor[pjTextVal](), reflect.TypeFor[pjTextPtr](), reflect.TypeFor[pjMixed](), reflect.TypeFor[pjBoth](),
		reflect.TypeFor[pjZeroVal](), reflect.TypeFor[pjZeroPtr](), reflect.TypeFor[pjList](), reflect.TypeFor[pjByteText](),
		reflect.TypeFor[pjEmbedsTime](), reflect.TypeFor[json.Marshaler](), reflect.TypeFor[encoding.TextMarshaler](),
		reflect.TypeFor[encoding.TextAppender](), reflect.TypeFor[pjPtr](), reflect.TypeFor[time.Duration](),
		reflect.TypeFor[pjAppender](), reflect.TypeFor[pjJSONTo](), reflect.TypeFor[pjJSONToPtr](), reflect.TypeFor[pjByteApp](),
		reflect.TypeFor[pjState](),
	}
	pjKeyTypes = []reflect.Type{
		reflect.TypeFor[string](), reflect.TypeFor[pjString](), reflect.TypeFor[int](), reflect.TypeFor[uint8](),
		reflect.TypeFor[pjKey](), reflect.TypeFor[pjIntKey](), reflect.TypeFor[time.Time](), reflect.TypeFor[float64](),
		reflect.TypeFor[pjFloat32](), reflect.TypeFor[time.Duration](), reflect.TypeFor[pjAppender](), reflect.TypeFor[pjState](),
		reflect.TypeFor[json.Number](),
		// No pointer keys: two pointers to equal values make the same key,
		// and which entry encoding/json keeps then is up to chance.
	}
	// pjAnyTypes are the types of the values put in an interface.
	pjAnyTypes = []reflect.Type{
		reflect.TypeFor[int](), reflect.TypeFor[float64](), reflect.TypeFor[float32](), reflect.TypeFor[string](),
		reflect.TypeFor[[]any](), reflect.TypeFor[map[string]any](), reflect.TypeFor[[]int](), reflect.TypeFor[[]byte](),
		reflect.TypeFor[time.Time](), reflect.TypeFor[*time.Time](), reflect.TypeFor[pjJSONVal](), reflect.TypeFor[*pjJSONPtr](),
		reflect.TypeFor[pjJSONPtr](), reflect.TypeFor[pjTextVal](), reflect.TypeFor[pjMixed](), reflect.TypeFor[*pjMixed](),
		reflect.TypeFor[pjTags](), reflect.TypeFor[pjOmitEmpty](), reflect.TypeFor[json.Number](), reflect.TypeFor[pjByteText](),
		reflect.TypeFor[pjEmbedsTime](), reflect.TypeFor[pjAppender](), reflect.TypeFor[*pjAppender](), reflect.TypeFor[pjJSONTo](),
		reflect.TypeFor[pjJSONToPtr](), reflect.TypeFor[*pjJSONToPtr](),
	}
	// pjMarshalerTypes and pjTextMarshalerTypes are the types of the values
	// put in a json.Marshaler and in an encoding.TextMarshaler. encoding/json
	// calls the interface's method on a nil pointer in it too, so such a
	// pointer is nil only where pjNilSafe says the method handles that.
	pjMarshalerTypes = []reflect.Type{
		reflect.TypeFor[pjJSONVal](), reflect.TypeFor[*pjJSONPtr](), reflect.TypeFor[*pjMixed](), reflect.TypeFor[pjBoth](),
		reflect.TypeFor[pjList](), reflect.TypeFor[time.Time](), reflect.TypeFor[*time.Time](), reflect.TypeFor[*big.Int](),
		reflect.TypeFor[pjEmbedsTime](), reflect.TypeFor[*pjOptional](), reflect.TypeFor[json.RawMessage](),
		reflect.TypeFor[pjJSONToPtr](), reflect.TypeFor[*pjJSONToPtr](),
	}
	pjTextMarshalerTypes = []reflect.Type{
		reflect.TypeFor[pjTextVal](), reflect.TypeFor[*pjTextPtr](), reflect.TypeFor[pjMixed](), reflect.TypeFor[*pjMixed](),
		reflect.TypeFor[pjBoth](), reflect.TypeFor[time.Time](), reflect.TypeFor[*time.Time](), reflect.TypeFor[*big.Int](),
		reflect.TypeFor[*big.Float](), reflect.TypeFor[pjEmbedsTime](), reflect.TypeFor[pjKey](), reflect.TypeFor[pjAppender](),
		reflect.TypeFor[*pjAppender](),
	}
	pjTextAppenderTypes = []reflect.Type{
		reflect.TypeFor[*pjAppender](), reflect.TypeFor[time.Time](), reflect.TypeFor[*big.Int](), reflect.TypeFor[pjByteApp](),
	}
	pjNilSafe = map[[2]reflect.Type]bool{ // {interface, pointer type}
		{marshalerType, reflect.TypeFor[*pjMixed]()}:       true,
		{marshalerType, reflect.TypeFor[*big.Int]()}:       true,
		{marshalerType, reflect.TypeFor[*pjOptional]()}:    true,
		{textMarshalerType, reflect.TypeFor[*big.Int]()}:   true,
		{textMarshalerType, reflect.TypeFor[*big.Float]()}: true,
		{textAppenderType, reflect.TypeFor[*pjAppender]()}: true,
		{textAppenderType, reflect.TypeFor[*big.Int]()}:    true,
	}
	pjFieldNames = []string{"A", "B", "Shared", "X", "Y"}
	pjTagNames   = []string{"a", "b", "B", "Shared", "shared", "x", "A", "-", "a-b", "ü", `in\"valid`, "'q'", "9x", `x\\y`}
	pjInts       = []int64{0, 1, -1, 42, -128, 127, 255, 1 << 31, math.MaxInt64, math.MinInt64, 1<<53 + 1}
	pjUints      = []uint64{0, 1, 255, 1 << 16, math.MaxUint32, 1 << 63, math.MaxUint64}
	pjFloats     = []float64{0, math.Copysign(0, -1), 1, -1, 2, 0.5, -2.25, 0.1, 1.1, 22.3, 1e20, 1e21, 1.5e300,
		1e-7, 5e-324, 123456789.125, 1 << 63, 1 << 64, -(1 << 63), 1<<53 + 1}
	pjFractions = []float64{0.5, -2.25, 0.1, 1.1, 22.3, 1e-7, 123.375, -0.001, 3.4e-30, 6.02e-5} // also as float32
	pjStrings   = []string{"", "a", "Shared", "<html> & co", "\u2028", "quote\"back\\slash", "ctl\x00\x1f\n",
		"ünï", "🎉", "a\xffb", "\xfe", "\uFFFD", "2026-01-01T00:00:00+01:00"}
	pjNumbers = []string{"0", "-1", "12", "1.5", "2.0", "1e3", "18446744073709551615", "-9223372036854775808", ""}
	pjZones   = []*time.Location{time.UTC, time.FixedZone("CEST", 2*3600), time.FixedZone("x", -9*3600-1800)}
	pjTimes   = []time.Time{{}, time.Date(2026, 10, 8, 14, 0, 0, 123456789, time.UTC), time.Date(1999, 12, 31, 23, 59, 59, 999999999, time.UTC)}
)

// pjRandomValue returns a random value of a random type, both chosen by
// seed. Values of the same seed and options differ only where nan is set:
// NaN is where the sentinel would be.
func pjRandomValue(seed uint64, strict, quoted, nan bool) any {
	g := &pjRandom{
		types:   rand.New(rand.NewPCG(seed, 1)),
		values:  rand.New(rand.NewPCG(seed, 2)),
		strict:  strict,
		quoted:  quoted,
		special: pjSentinel,
	}
	if nan {
		g.special = math.NaN()
	}
	t := g.typ(0)
	if seed%4 != 0 {
		t = g.structType(1) // mostly a struct at the top
	}
	v := reflect.New(t).Elem()
	g.fill(v, 0)
	if seed%3 == 0 {
		return v.Addr().Interface() // addressable inside
	}
	return v.Interface()
}

func (g *pjRandom) typ(depth int) reflect.Type {
	r := g.types
	if depth < 4 {
		switch r.IntN(12) {
		case 0:
			return reflect.SliceOf(g.typ(depth + 1))
		case 1:
			return reflect.ArrayOf(r.IntN(3), g.typ(depth+1))
		case 2:
			return reflect.MapOf(pjKeyTypes[r.IntN(len(pjKeyTypes))], g.typ(depth+1))
		case 3:
			return reflect.PointerTo(g.typ(depth + 1))
		case 4, 5, 6:
			return g.structType(depth + 1)
		}
	}
	return pjLeafTypes[r.IntN(len(pjLeafTypes))]
}

func (g *pjRandom) structType(depth int) reflect.Type {
	r := g.types
	n := r.IntN(6)
	fields := make([]reflect.StructField, 0, n)
	used := map[string]bool{}
	for i := range n {
		var f reflect.StructField
		switch k := r.IntN(10); {
		case k < 2 && depth < 4: // an embedded struct, or a pointer to one
			et := g.structType(depth + 1)
			if r.IntN(3) == 0 {
				et = reflect.PointerTo(et)
			}
			f = reflect.StructField{Name: fmt.Sprintf("E%d", i), Type: et, Anonymous: true}
		case k == 2: // unexported
			f = reflect.StructField{Name: fmt.Sprintf("u%d", i), PkgPath: "github.com/ironflock/sdk-go", Type: g.typ(depth + 1)}
		default:
			name := pjFieldNames[r.IntN(len(pjFieldNames))]
			if used[name] {
				name = fmt.Sprintf("%s%d", name, i)
			}
			f = reflect.StructField{Name: name, Type: g.typ(depth + 1)}
		}
		used[f.Name] = true
		f.Tag = g.tag(f.Anonymous)
		fields = append(fields, f)
	}
	return reflect.StructOf(fields)
}

func (g *pjRandom) tag(embedded bool) reflect.StructTag {
	r := g.types
	name := pjTagNames[r.IntN(len(pjTagNames))]
	var options []string
	if embedded {
		options = []string{"", "", "", name, "-", ",omitempty"}
	} else {
		options = []string{"", "", "", "", name, "-", "-,", ",omitempty", ",omitzero", name + ",omitempty", name + ",omitempty,omitzero",
			",embed", name + ",embed", ",embed,omitempty"}
		if g.quoted {
			options = append(options, ",string", name+",string", name+",omitzero,string")
		}
	}
	tag := options[r.IntN(len(options))]
	if tag == "" {
		return ""
	}
	return reflect.StructTag(`json:"` + tag + `"`)
}

func (g *pjRandom) fill(v reflect.Value, depth int) {
	r := g.values
	t := v.Type()
	switch t {
	case timeType:
		v.Set(reflect.ValueOf(pjTimes[r.IntN(len(pjTimes))].In(pjZones[r.IntN(len(pjZones))])))
		return
	case numberType:
		v.SetString(pjNumbers[r.IntN(len(pjNumbers))])
		return
	case reflect.TypeFor[pjState]():
		v.SetInt(int64(r.IntN(2))) // its MarshalText knows two states
		return
	case reflect.TypeFor[json.RawMessage]():
		if raw := []string{"", "null", `{"a": [1, 2.5]}`, `"s"`, "7"}[r.IntN(5)]; raw != "" {
			v.SetBytes([]byte(raw))
		}
		return
	}
	switch v.Kind() {
	case reflect.Bool:
		v.SetBool(r.IntN(2) == 0)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		for {
			if x := pjInts[r.IntN(len(pjInts))]; !v.OverflowInt(x) {
				v.SetInt(x)
				return
			}
		}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		for {
			if x := pjUints[r.IntN(len(pjUints))]; !v.OverflowUint(x) {
				v.SetUint(x)
				return
			}
		}
	case reflect.Float32, reflect.Float64:
		if r.IntN(4) == 0 {
			v.SetFloat(g.special)
			return
		}
		pool := pjFloats
		if g.strict {
			pool = pjFractions
		}
		for {
			if x := pool[r.IntN(len(pool))]; !v.OverflowFloat(x) {
				v.SetFloat(x)
				return
			}
		}
	case reflect.String:
		v.SetString(pjStrings[r.IntN(len(pjStrings))])
	case reflect.Slice:
		switch k := r.IntN(5); k {
		case 0: // nil
		case 1:
			v.Set(reflect.MakeSlice(t, 0, 0))
		default:
			s := reflect.MakeSlice(t, k-1, k-1)
			for i := range s.Len() {
				g.fill(s.Index(i), depth+1)
			}
			v.Set(s)
		}
	case reflect.Array:
		for i := range v.Len() {
			g.fill(v.Index(i), depth+1)
		}
	case reflect.Map:
		if k := r.IntN(5); k > 0 {
			m := reflect.MakeMap(t)
			for range k - 1 {
				// A float key is the special float only where it is finite:
				// NaN cannot be a key.
				key := reflect.New(t.Key()).Elem()
				special := g.special
				g.special = pjSentinel
				g.fill(key, depth+1)
				g.special = special
				val := reflect.New(t.Elem()).Elem()
				g.fill(val, depth+1)
				m.SetMapIndex(key, val)
			}
			v.Set(m)
		}
	case reflect.Pointer:
		if r.IntN(3) > 0 {
			p := reflect.New(t.Elem())
			g.fill(p.Elem(), depth+1)
			v.Set(p.Convert(t)) // t may be a named pointer type
		}
	case reflect.Interface:
		if r.IntN(3) > 0 && depth < 6 {
			types := pjAnyTypes
			switch t {
			case marshalerType:
				types = pjMarshalerTypes
			case textMarshalerType:
				types = pjTextMarshalerTypes
			case textAppenderType:
				types = pjTextAppenderTypes
			}
			e := reflect.New(types[r.IntN(len(types))]).Elem()
			g.fill(e, depth+1)
			if t != reflect.TypeFor[any]() && e.Kind() == reflect.Pointer && e.IsNil() && !pjNilSafe[[2]reflect.Type{t, e.Type()}] {
				e.Set(reflect.New(e.Type().Elem())) // encoding/json would panic on the nil pointer
				g.fill(e.Elem(), depth+1)
			}
			v.Set(e)
		}
	case reflect.Struct:
		for i := range v.NumField() {
			if f := v.Field(i); f.CanSet() {
				g.fill(f, depth+1)
			}
		}
	}
}

// pjRuns is how many random values each property is checked on.
func pjRuns() int {
	if testing.Short() {
		return 300
	}
	return 3000
}

// Random struct types (made with reflect.StructOf: conflicting names, every
// tag option, embedding at several depths, unexported fields) and random
// values of them encode as encoding/json encodes them.
func TestNormalizeMatchesEncodingJSONOnRandomTypes(t *testing.T) {
	for seed := range uint64(pjRuns()) {
		strict := seed%2 == 0
		v := pjRandomValue(seed, strict, true, false)
		checkLikeJSON(t, fmt.Sprintf("seed %d, %T", seed, v), v, !strict)
		if t.Failed() {
			t.Fatalf("seed %d: value %#v", seed, v)
		}
	}
}

// FuzzNormalizeMatchesEncodingJSON is TestNormalizeMatchesEncodingJSONOnRandomTypes
// for go test -fuzz.
func FuzzNormalizeMatchesEncodingJSON(f *testing.F) {
	for _, seed := range []uint64{0, 1, 2, 3, 1 << 40} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, seed uint64) {
		strict := seed%2 == 0
		v := pjRandomValue(seed, strict, true, false)
		checkLikeJSON(t, fmt.Sprintf("seed %d, %T", seed, v), v, !strict)
	})
}

// NaN is encoded like any other float: a value holding NaN where an
// otherwise equal value holds a finite float normalizes the same, with NaN
// in that place.
func TestNormalizeNonFiniteFloatsLikeFinite(t *testing.T) {
	for seed := range uint64(pjRuns()) {
		withNaN := pjRandomValue(seed, false, false, true)
		finite := pjRandomValue(seed, false, false, false)
		got, err := normalize(withNaN)
		want, err2 := normalize(finite)
		if err != nil || err2 != nil {
			// A value encoding/json refuses too (an embedded JSON field
			// holding no object, say) fails the same either way.
			if err == nil || err2 == nil || err.Error() != err2.Error() {
				t.Fatalf("seed %d: %v; finite: %v", seed, err, err2)
			}
			continue
		}
		if !pjEqual(got, want, true) {
			t.Fatalf("seed %d:\n got  %#v\n want %#v (NaN for %v)", seed, got, want, pjSentinel)
		}
	}
}

// pjEqual reports whether a and b, normalized values, are equal; NaN equals
// NaN. With sentinel, a NaN in a also equals pjSentinel in b.
func pjEqual(a, b any, sentinel bool) bool {
	switch x := a.(type) {
	case float64:
		y, ok := b.(float64)
		switch {
		case !ok:
			return false
		case math.IsNaN(x):
			return math.IsNaN(y) || sentinel && y == pjSentinel
		}
		return x == y && math.Signbit(x) == math.Signbit(y)
	case []any:
		y, ok := b.([]any)
		if !ok || len(x) != len(y) || (x == nil) != (y == nil) {
			return false
		}
		for i := range x {
			if !pjEqual(x[i], y[i], sentinel) {
				return false
			}
		}
		return true
	case map[string]any:
		y, ok := b.(map[string]any)
		if !ok || len(x) != len(y) || (x == nil) != (y == nil) {
			return false
		}
		for k, e := range x {
			if f, ok := y[k]; !ok || !pjEqual(e, f, sentinel) {
				return false
			}
		}
		return true
	}
	return reflect.DeepEqual(a, b)
}

// normalize's result is made of the promised types, and normalizing it
// again changes nothing.
func TestNormalizeIsIdempotent(t *testing.T) {
	for seed := range uint64(pjRuns()) {
		v := pjRandomValue(seed, false, true, seed%2 == 0)
		once, err := normalize(v)
		if err != nil {
			continue // a ",string" field holding NaN
		}
		if d := checkShape(once); d != "" {
			t.Fatalf("seed %d: %s in %#v", seed, d, once)
		}
		twice, err := normalize(once)
		if err != nil || !pjEqual(twice, once, false) {
			t.Fatalf("seed %d: normalized again: %#v, %v\nonce: %#v", seed, twice, err, once)
		}
	}
}

// A float is sent the same however it is held: in a struct field, through a
// pointer, in an interface, a Row, a typed map or slice — NaN, ±Inf, -0 and
// subnormals included.
func TestNormalizeRowAndStructAgree(t *testing.T) {
	type reading struct {
		F   float64  `json:"f"`
		G   float32  `json:"g"`
		P   *float64 `json:"p"`
		Any any      `json:"any"`
	}
	r := rand.New(rand.NewPCG(11, 12))
	specials := []float64{math.NaN(), math.Inf(1), math.Inf(-1), 0, math.Copysign(0, -1), math.MaxFloat64,
		math.SmallestNonzeroFloat64, 2, 0.1, math.Float64frombits(0xfff8000000000000)}
	for i := range pjRuns() {
		x := math.Float64frombits(r.Uint64()) // any bit pattern, NaNs included
		if i < len(specials) {
			x = specials[i]
		}
		x32 := float32(x)
		want, err := normalize(reading{F: x, G: x32, P: &x, Any: x})
		if err != nil {
			t.Fatal(err)
		}
		for _, in := range []any{
			&reading{F: x, G: x32, P: &x, Any: x},
			Row{"f": x, "g": x32, "p": &x, "any": x},
			Kwargs{"f": x, "g": x32, "p": &x, "any": x},
			map[string]any{"f": pjFloat(x), "g": pjFloat32(x32), "p": &x, "any": any(x)},
		} {
			if got, err := normalize(in); err != nil || !pjEqual(got, want, false) {
				t.Fatalf("%v: %T gives %#v, %v; the struct gives %#v", x, in, got, err, want)
			}
		}
		f, g := want.(map[string]any)["f"], want.(map[string]any)["g"]
		for _, in := range []any{map[string]float64{"f": x}, []float64{x}, [1]float64{x}, []any{x}} {
			got, err := normalize(in)
			var got0 any
			switch v := got.(type) {
			case map[string]any:
				got0 = v["f"]
			case []any:
				got0 = v[0]
			}
			if err != nil || !pjEqual(got0, f, false) {
				t.Fatalf("%v: %T gives %#v, %v", x, in, got, err)
			}
		}
		if got, err := normalize([]float32{x32}); err != nil || !pjEqual(got.([]any)[0], g, false) {
			t.Fatalf("%v: []float32 gives %#v, %v", x32, got, err)
		}
	}
}

// normalize is safe for concurrent use: its only shared state is the cache
// of type information, filled here from many goroutines at once.
func TestNormalizeConcurrently(t *testing.T) {
	const goroutines = 8
	runs := pjRuns() / 10
	want := make([]any, runs)
	for i := range runs {
		var err error
		if want[i], err = normalize(pjRandomValue(uint64(i), false, true, false)); err != nil {
			want[i] = err.Error()
		}
	}
	typeInfos.Clear()
	var wg sync.WaitGroup
	errs := make(chan string, goroutines)
	for g := range goroutines {
		wg.Go(func() {
			for j := range runs {
				i := (j + g*runs/goroutines) % runs // start at different values
				got, err := normalize(pjRandomValue(uint64(i), false, true, false))
				if err != nil {
					got = err.Error()
				}
				if !pjEqual(got, want[i], false) {
					errs <- fmt.Sprintf("seed %d: %#v, want %#v", i, got, want[i])
					return
				}
			}
		})
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Error(e)
	}
}

// What normalize returns goes on the wire as autobahn-python's msgpack
// serializer sends the equivalent Python values: a float as a msgpack float
// (also when integral), []byte as bin, NaN and ±Inf as floats, text as
// valid UTF-8.
func TestNormalizedPayloadOnTheWire(t *testing.T) {
	type (
		withF struct {
			F float64 `json:"f"`
		}
		withI struct {
			I int `json:"i"`
		}
		withB struct {
			B []byte `json:"b"`
		}
		withT struct {
			T float64 `json:"t"`
		}
		withU struct {
			U uint64 `json:"u"`
		}
		withS struct {
			S string `json:"s"`
		}
	)
	cases := []struct {
		in  any
		hex string // MsgPackObjectSerializer().serialize(python) of autobahn 25.12.2
	}{
		{withF{2}, "81a166cb4000000000000000"},                                              // {'f': 2.0}
		{withI{3}, "81a16903"},                                                              // {'i': 3}
		{withB{[]byte("raw")}, "81a162c403726177"},                                          // {'b': b'raw'}
		{withT{math.Float64frombits(0x7ff8000000000000)}, "81a174cb7ff8000000000000"},       // {'t': float('nan')}
		{withT{math.Inf(1)}, "81a174cb7ff0000000000000"},                                    // {'t': float('inf')}
		{withT{math.Inf(-1)}, "81a174cbfff0000000000000"},                                   // {'t': float('-inf')}
		{withU{math.MaxUint64}, "81a175cfffffffffffffffff"},                                 // {'u': 2**64 - 1}
		{withS{"a\xffb"}, "81a173a561efbfbd62"},                                             // {'s': 'a\ufffdb'}
		{[]any{float32(1.5), 2.0, nil, true}, "94cb3ff8000000000000cb4000000000000000c0c3"}, // [1.5, 2.0, None, True]
	}
	var ser serialize.MessagePackSerializer
	for _, c := range cases {
		v, err := normalize(c.in)
		if err != nil {
			t.Fatal(err)
		}
		data, err := ser.SerializeDataItem(v)
		if err != nil {
			t.Fatal(err)
		}
		if got := hex.EncodeToString(data); got != c.hex {
			t.Errorf("%#v on the wire: %s\nwant %s", c.in, got, c.hex)
		}
	}
}

func BenchmarkNormalize(b *testing.B) {
	type reading struct {
		Temperature float64   `json:"temperature"`
		Humidity    float64   `json:"humidity"`
		Device      string    `json:"device"`
		Count       int       `json:"count"`
		Tsp         time.Time `json:"tsp"`
		Tags        []string  `json:"tags,omitempty"`
	}
	now := time.Now()
	r := reading{21.5, 40, "dev-1", 3, now, []string{"a", "b"}}
	row := Row{"temperature": 21.5, "humidity": 40.0, "device": "dev-1", "count": 3, "tsp": now, "tags": []any{"a", "b"}}
	batch := make([]reading, 100)
	for i := range batch {
		batch[i] = r
	}
	b.Run("struct", func(b *testing.B) {
		for b.Loop() {
			_, _ = normalize(r)
		}
	})
	b.Run("Row", func(b *testing.B) {
		for b.Loop() {
			_, _ = normalize(row)
		}
	})
	rows := make([]Row, 100)
	for i := range rows {
		rows[i] = row
	}
	b.Run("100 Rows", func(b *testing.B) {
		for b.Loop() {
			_, _ = normalizeRows(rows)
		}
	})
	b.Run("100 struct rows", func(b *testing.B) {
		for b.Loop() {
			_, _ = normalizeRows(batch)
		}
	})
	b.Run("struct via encoding/json (before)", func(b *testing.B) {
		for b.Loop() {
			_, _ = viaJSON(r)
		}
	})
}
