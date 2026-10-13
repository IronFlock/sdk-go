package ironflock

import (
	"bytes"
	"cmp"
	"encoding"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/ironflock/sdk-go/internal/finite"
	"github.com/ironflock/sdk-go/internal/jsontext"
)

// Row is one table row: column name to value.
type Row = map[string]any

// Kwargs marks WAMP keyword arguments in a variadic argument list. Positional
// arguments of Publish, PublishToTable, AppendToTable, Call and
// CallDeviceFunction are sent as WAMP args; a Kwargs value among them is sent
// as WAMP kwargs instead (several Kwargs are merged, later keys win):
//
//	ifl.PublishToTable(ctx, "sensordata", ironflock.Row{"tsp": time.Now(), "temperature": 22.5})
//	ifl.Publish(ctx, "com.myapp.status", ironflock.Kwargs{"state": "ready"})
//	ifl.Call(ctx, "proc", 1, 2, ironflock.Kwargs{"scale": 10})
//
// A table write's row is its first positional argument: the data backend
// reads the kwargs only into columns the table's data template maps to them.
type Kwargs map[string]any

// splitArgs separates positional arguments from Kwargs and CallOptions
// markers, and normalizes every value to the JSON-like shapes the platform
// expects (see normalize).
func splitArgs(args []any) (positional []any, kwargs map[string]any, callOpts *CallOptions, err error) {
	for _, a := range args {
		switch v := a.(type) {
		case Kwargs:
			kw, err := normalizeKwargs(v)
			if err != nil {
				return nil, nil, nil, err
			}
			if kwargs == nil {
				kwargs = kw
				continue
			}
			for k, val := range kw {
				kwargs[k] = val
			}
		case CallOptions:
			o := v
			callOpts = &o
		case *CallOptions:
			callOpts = v
		default:
			nv, err := normalize(a)
			if err != nil {
				return nil, nil, nil, invalidf("argument %d: %v", len(positional), err)
			}
			positional = append(positional, nv)
		}
	}
	return positional, kwargs, callOpts, nil
}

// normalizeKwargs normalizes one Kwargs value: its values as normalize does,
// its keys as normalize does map keys.
func normalizeKwargs(kw Kwargs) (map[string]any, error) {
	obj := newObject(len(kw))
	for k, val := range kw {
		nv, err := normalize(val)
		if err != nil {
			return nil, invalidf("kwarg %q: %v", k, err)
		}
		obj.set(k, nv)
	}
	return obj.m, nil
}

// normalize converts a payload value to the JSON-like shapes every IronFlock
// SDK sends: nil, bool, string, int64 (uint64 only above math.MaxInt64),
// float64, []byte, []any and map[string]any. The result shares no mutable
// memory with v.
//
// Structs, maps, slices, arrays, pointers and interfaces are converted by the
// rules of encoding/json as of Go 1.27, which runs it on the json/v2 engine
// with the options that keep v1's behavior, so a value has the shape
// json.Marshal would give it:
//
//   - Struct fields are named by their json tags (a name ends at the first
//     comma, backslash or quote), with the "-", omitempty, omitzero, ",string"
//     and ",embed" options and the rules for embedded structs. An unnamed
//     field tagged ",embed" adds a struct's fields, or the members of a map
//     with string keys or of a json.RawMessage, to the struct's own; such a
//     member named like a field wins, as a decoder keeps the later of the
//     two encoding/json writes. A struct with a field tagged with a
//     "format:" option is refused.
//   - The ",string" option applies to the field's value, also behind one
//     pointer, named or not; not through an interface or a second pointer,
//     nor to the elements of a container.
//   - A map key becomes the object key encoding/json makes of it: a string
//     as it is, an integer or float as its JSON number, a value whose
//     AppendText or MarshalText method encoding/json calls (never
//     MarshalJSON for a key) as its text, a key behind a pointer or in an
//     interface as what it points to or holds, and a nil pointer key of a
//     type with MarshalText or AppendText as "". A key that is no string —
//     a bool, a struct or array without those methods, a nil pointer or
//     interface, a float64 in an interface{} key — fails, so only an empty
//     map of such a key type can be sent.
//   - A nil slice, map or pointer, Row(nil) included, becomes nil (JSON
//     null), not an empty list or object.
//   - A value with a MarshalJSONTo, MarshalJSON, AppendText or MarshalText
//     method is encoded through the first of them encoding/json calls (the
//     JSON a method returns is decoded, a number written as an integer
//     becoming int64, or uint64 above math.MaxInt64, any other float64): a
//     method with a pointer receiver only on an addressable value, and none
//     on a nil pointer, except that a nil pointer held in an interface of
//     type json.Marshaler, encoding.TextAppender, encoding.TextMarshaler or
//     encoding/json/v2's MarshalerTo (a field of that type, say) is encoded
//     through that method.
//   - Strings are coerced to valid UTF-8, each invalid byte becoming U+FFFD.
//   - Objects and arrays nested more than 10000 deep fail, counted as
//     encoding/json counts them (not an empty slice or map).
//
// Unlike encoding/json:
//
//   - Floats stay float64, NaN and ±Inf included, as the Python SDK sends
//     them; an integral float such as 2.0 stays a float. A float32 is widened
//     through its shortest decimal form, so float32(0.1) is sent as 0.1.
//     Only a field with the ",string" option, which asks for the JSON number
//     in a string, cannot hold NaN or ±Inf.
//   - []byte stays []byte (msgpack bin, like Python bytes) instead of
//     becoming a base64 string.
//   - A time.Time becomes an RFC 3339 string in UTC
//     ("2026-01-02T03:04:05.123Z") instead of keeping its zone offset,
//     wherever it is: a value or a map key, behind a pointer, in an
//     interface, or embedded in a struct that is encoded through time.Time's
//     promoted MarshalJSON (as that time, its other fields dropped, as
//     encoding/json encodes it). Map keys naming the same instant become one
//     entry, the one encoding/json would write last. The Python and
//     JavaScript SDKs send their timestamps in UTC too.
//   - A struct with an unexported embedded struct named by its tag whose
//     marshal or unmarshal method (or IsZero, with omitzero), its own or its
//     pointer's, cannot be called on it is refused with an error naming the
//     field: encoding/json leaves such a field out, silently dropping its
//     data.
//
// Like encoding/json, normalize fails on channels, functions, complex
// numbers and values that contain themselves. Where encoding/json panics,
// normalize does not: it sends nil (as a map key, "") where a method called
// on a nil pointer, or promoted through a nil embedded pointer or interface,
// fails with a run-time error on it, and for a nil pointer held in an
// interface of any other type with a marshal method; and it fails on a value
// that contains itself through pointers and interfaces only (encoding/json
// recurses until the stack overflows).
func normalize(v any) (any, error) {
	var e encoder
	return e.anyValue(v)
}

// trackDepth is the nesting depth (pointers, maps and slices) from which the
// walk remembers the containers on its current path, to fail on a value that
// contains itself instead of recursing until the stack overflows. It is
// encoding/json's startDetectingCyclesAfter.
const trackDepth = 1000

// maxNesting is how deeply encoding/json nests objects and arrays.
const maxNesting = 10000

// encoder carries the state of one normalize walk.
type encoder struct {
	depth  int              // pointers, maps and slices entered
	path   map[any]struct{} // those on the current path, once deeper than trackDepth
	nested int              // objects and arrays open, as encoding/json counts them
}

// anyValue converts v, a value held in an interface: common payload types
// directly, any other type by reflection.
func (e *encoder) anyValue(v any) (any, error) {
	switch x := v.(type) {
	case nil:
		return nil, nil
	case string:
		return jsontext.ValidUTF8(x), nil
	case bool, int64, float64:
		return x, nil
	case int:
		return int64(x), nil
	case int8:
		return int64(x), nil
	case int16:
		return int64(x), nil
	case int32:
		return int64(x), nil
	case uint:
		return unsigned(uint64(x)), nil
	case uint8:
		return int64(x), nil
	case uint16:
		return int64(x), nil
	case uint32:
		return int64(x), nil
	case uint64:
		return unsigned(x), nil
	case float32:
		return widen(x), nil
	case []byte:
		if x == nil {
			return nil, nil
		}
		return bytes.Clone(x), nil
	case time.Time:
		return utc(x), nil
	case map[string]any:
		return e.stringMap(x)
	case []any:
		return e.list(x)
	}
	// A value held in an interface is not addressable: as for encoding/json,
	// a method with a pointer receiver is not used for it.
	return e.value(reflect.ValueOf(v), false)
}

// value converts v by encoding/json's rules (with normalize's exceptions).
// quoted is the ",string" option of the struct field v is (or is pointed to
// by).
func (e *encoder) value(v reflect.Value, quoted bool) (any, error) {
	switch v.Kind() {
	case reflect.Invalid:
		return nil, nil
	case reflect.Pointer:
		return e.pointer(v, quoted)
	case reflect.Interface:
		return e.iface(v)
	}
	t := v.Type()
	switch t {
	case timeType:
		return timeString(v)
	case numberType: // its MarshalJSONTo method writes the number
		return numberValue(v.String(), quoted)
	case anyMapType: // e.g. the rows of a []Row: no reflection needed
		if v.CanInterface() {
			return e.stringMap(v.Interface().(map[string]any))
		}
	case anySliceType:
		if v.CanInterface() {
			return e.list(v.Interface().([]any))
		}
	}
	ti := infoOf(t)
	if kind, how := ti.use(v.CanAddr(), false); kind != noKind {
		return e.marshal(v, kind, how)
	}
	switch v.Kind() {
	case reflect.Bool:
		if quoted {
			return strconv.FormatBool(v.Bool()), nil
		}
		return v.Bool(), nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if quoted {
			return strconv.FormatInt(v.Int(), 10), nil
		}
		return v.Int(), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		if quoted {
			return strconv.FormatUint(v.Uint(), 10), nil
		}
		return unsigned(v.Uint()), nil
	case reflect.Float32, reflect.Float64:
		return floatValue(v.Float(), t.Bits(), quoted)
	case reflect.String:
		if quoted {
			// The JSON encoding of the string, as a string.
			b, _ := json.Marshal(v.String())
			return string(b), nil
		}
		return jsontext.ValidUTF8(v.String()), nil
	case reflect.Struct:
		return e.structValue(v, ti)
	case reflect.Map:
		return e.mapValue(v, ti)
	case reflect.Slice:
		if v.IsNil() {
			return nil, nil
		}
		if ti.bytes {
			return bytes.Clone(v.Bytes()), nil
		}
		key, err := e.enter(v)
		if err != nil {
			return nil, err
		}
		out, err := e.elems(v, v.Len() > 0)
		e.leave(key)
		return out, err
	case reflect.Array:
		return e.elems(v, true)
	}
	return nil, failf("unsupported type %s", t)
}

// pointer converts the pointer v: nil, or the value it points to, which is
// addressable. The ",string" option reaches through one pointer only.
func (e *encoder) pointer(v reflect.Value, quoted bool) (any, error) {
	if quoted && v.Type().Elem().Kind() == reflect.Pointer {
		quoted = false
	}
	if v.IsNil() {
		return nil, nil
	}
	key, err := e.enter(v)
	if err != nil {
		return nil, err
	}
	out, err := e.value(v.Elem(), quoted)
	e.leave(key)
	return out, err
}

// iface converts v, a value of interface type: what it holds, taken as a
// value that is not addressable. A nil pointer in an interface whose type
// has a marshal method is encoded as encoding/json encodes it.
func (e *encoder) iface(v reflect.Value) (any, error) {
	if v.IsNil() {
		return nil, nil
	}
	if elem := v.Elem(); elem.Kind() == reflect.Pointer && elem.IsNil() {
		switch kind := wrapperOf(v.Type()); kind {
		case noKind: // a nil pointer, as anywhere else
		case kindJSONTo:
			return e.marshalNilTo(v)
		case kindJSON, kindAppend, kindText:
			// encoding/json calls the interface's method on the nil pointer.
			return e.marshal(v, kind, valueMethod)
		default:
			// The interface type has marshal methods but is none of those:
			// encoding/json panics.
			return nil, nil
		}
	}
	if !v.CanInterface() {
		return e.value(v.Elem(), false)
	}
	return e.anyValue(v.Elem().Interface())
}

// structValue converts the struct v, of a type described by ti.
func (e *encoder) structValue(v reflect.Value, ti *typeInfo) (any, error) {
	switch {
	case ti.leftOut != "":
		return nil, failf("%s: %w", ti.leftOut, errLeftOut)
	case ti.refused != "":
		return nil, failf("%s", ti.refused)
	}
	if err := e.open(); err != nil {
		return nil, err
	}
	out := make(map[string]any, len(ti.fields))
	for i := range ti.fields {
		f := &ti.fields[i]
		fv, ok := fieldByIndex(v, f.index)
		if !ok {
			continue
		}
		if f.omitEmpty && isEmptyValue(fv) {
			continue
		}
		if f.omitZero {
			zero := false
			if f.isZero == nil {
				zero = fv.IsZero()
			} else {
				var err error
				if zero, err = f.isZero(fv); err != nil {
					return nil, at(err, keySegment(f.name))
				}
			}
			if zero {
				continue
			}
		}
		val, err := e.value(fv, f.quoted)
		if err != nil {
			return nil, at(err, keySegment(f.name))
		}
		out[f.name] = val
	}
	if ti.fallback != nil {
		if err := e.fallback(v, ti.fallback, out); err != nil {
			return nil, err
		}
	}
	e.close()
	return out, nil
}

// fallback adds the members of the struct v's embedded fallback field f (a
// map with string keys or a jsontext.Value tagged ",embed") to out, the
// struct's object. encoding/json writes them after the fields, so a member
// named like a field is the one a decoder keeps.
func (e *encoder) fallback(v reflect.Value, f *structField, out map[string]any) error {
	fv, ok := fieldByIndex(v, f.index)
	if !ok {
		return nil
	}
	if fv.Kind() == reflect.Pointer {
		if fv.IsNil() {
			return nil
		}
		fv = fv.Elem()
	}
	if f.raw {
		b := fv.Bytes()
		if len(b) == 0 {
			return nil
		}
		raw, depth, err := fromJSONText(b)
		if err != nil {
			return failf("invalid embedded JSON: %v", err)
		}
		members, ok := raw.(map[string]any)
		if !ok {
			return failf("embedded JSON is %s, not an object", shape(raw))
		}
		if e.nested+depth-1 > maxNesting {
			return errNesting()
		}
		for k, val := range members {
			out[k] = val
		}
		return nil
	}
	obj := newObject(fv.Len())
	for it := fv.MapRange(); it.Next(); {
		k := it.Key().String()
		val, err := e.value(it.Value(), false)
		if err != nil {
			return at(err, keySegment(k))
		}
		obj.set(k, val)
	}
	for k, val := range obj.m {
		out[k] = val
	}
	return nil
}

// mapValue converts the map v, of a type described by ti.
func (e *encoder) mapValue(v reflect.Value, ti *typeInfo) (any, error) {
	if v.IsNil() {
		return nil, nil
	}
	key, err := e.enter(v)
	if err != nil {
		return nil, err
	}
	n := v.Len()
	if n > 0 {
		if err := e.open(); err != nil {
			return nil, err
		}
	}
	obj := newObject(n)
	for it := v.MapRange(); it.Next(); {
		k, orig, err := e.key(it.Key(), ti.nilKey)
		if err != nil {
			return nil, err
		}
		val, err := e.value(it.Value(), false)
		if err != nil {
			return nil, at(err, keySegment(k))
		}
		obj.setAs(orig, jsontext.ValidUTF8(k), val)
	}
	if n > 0 {
		e.close()
	}
	e.leave(key)
	return obj.m, nil
}

// stringMap is mapValue for a map[string]any, without reflection.
func (e *encoder) stringMap(m map[string]any) (any, error) {
	if m == nil {
		return nil, nil
	}
	var key any
	if e.depth++; e.depth > trackDepth {
		var err error
		if key, err = e.track(reflect.ValueOf(m)); err != nil {
			return nil, err
		}
	}
	if len(m) > 0 {
		if err := e.open(); err != nil {
			return nil, err
		}
	}
	obj := newObject(len(m))
	for k, x := range m {
		val, err := e.anyValue(x)
		if err != nil {
			return nil, at(err, keySegment(k))
		}
		obj.set(k, val)
	}
	if len(m) > 0 {
		e.close()
	}
	e.leave(key)
	return obj.m, nil
}

// list is the slice conversion for a []any, without reflection.
func (e *encoder) list(s []any) (any, error) {
	if s == nil {
		return nil, nil
	}
	var key any
	if e.depth++; e.depth > trackDepth {
		var err error
		if key, err = e.track(reflect.ValueOf(s)); err != nil {
			return nil, err
		}
	}
	if len(s) > 0 {
		if err := e.open(); err != nil {
			return nil, err
		}
	}
	out := make([]any, len(s))
	for i, x := range s {
		val, err := e.anyValue(x)
		if err != nil {
			return nil, at(err, indexSegment(i))
		}
		out[i] = val
	}
	if len(s) > 0 {
		e.close()
	}
	e.leave(key)
	return out, nil
}

// elems converts the elements of the slice or array v. open says whether
// encoding/json counts the array it writes for them against maxNesting (it
// does not for an empty slice).
func (e *encoder) elems(v reflect.Value, open bool) ([]any, error) {
	if open {
		if err := e.open(); err != nil {
			return nil, err
		}
	}
	out := make([]any, v.Len())
	for i := range out {
		val, err := e.value(v.Index(i), false)
		if err != nil {
			return nil, at(err, indexSegment(i))
		}
		out[i] = val
	}
	if open {
		e.close()
	}
	return out, nil
}

// open is called before converting an object or array encoding/json counts
// against maxNesting; close after it. open fails if the object or array is
// nested too deeply.
func (e *encoder) open() error {
	if e.nested >= maxNesting {
		return errNesting()
	}
	e.nested++
	return nil
}

func (e *encoder) close() { e.nested-- }

func errNesting() error {
	return failf("objects and arrays are nested more than %d deep", maxNesting)
}

// enter is called before descending into the pointer, map or slice v; the
// returned key is passed to leave. Deeper than trackDepth, it records v on
// the current path and fails if v is on it already.
func (e *encoder) enter(v reflect.Value) (any, error) {
	if e.depth++; e.depth <= trackDepth {
		return nil, nil
	}
	return e.track(v)
}

// track records the pointer, map or slice v on the current path, failing if
// v is on it already: the value contains itself. It identifies v as
// encoding/json does.
func (e *encoder) track(v reflect.Value) (any, error) {
	var key any
	switch v.Kind() {
	case reflect.Map:
		key = v.UnsafePointer()
	case reflect.Slice:
		key = sliceKey{v.UnsafePointer(), v.Len()}
	default:
		key = pointerKey{v.UnsafePointer(), v.Type()}
	}
	if _, seen := e.path[key]; seen {
		return nil, failf("encountered a cycle via %s", v.Type())
	}
	if e.path == nil {
		e.path = make(map[any]struct{})
	}
	e.path[key] = struct{}{}
	return key, nil
}

// leave undoes enter.
func (e *encoder) leave(key any) {
	if key != nil {
		delete(e.path, key)
	}
	e.depth--
}

// sliceKey identifies a slice: its first element and its length.
type sliceKey struct {
	data any // an unsafe.Pointer
	n    int
}

// pointerKey identifies a pointer: its address and type.
type pointerKey struct {
	addr any // an unsafe.Pointer
	typ  reflect.Type
}

// fieldByIndex returns the field of the struct v at index, following
// embedded pointers. It reports false when one of them is nil: encoding/json
// then leaves the field out.
func fieldByIndex(v reflect.Value, index []int) (reflect.Value, bool) {
	for _, i := range index {
		if v.Kind() == reflect.Pointer {
			if v.IsNil() {
				return reflect.Value{}, false
			}
			v = v.Elem()
		}
		v = v.Field(i)
	}
	return v, true
}

// key returns the object key the map key k is sent as: the key encoding/json
// makes of it, except that a time.Time key — also behind a pointer, in an
// interface, or embedded in the key type (see callMarshaler) — is in UTC, as
// a time.Time value is. orig is the key encoding/json makes of it, which
// decides between keys that come out the same. nilKey says that a nil key is
// "": encoding/json writes that for a nil key of a pointer type with a
// MarshalText or AppendText method.
func (e *encoder) key(k reflect.Value, nilKey bool) (key, orig string, err error) {
	switch k.Kind() {
	case reflect.Pointer:
		if k.IsNil() {
			if nilKey {
				return "", "", nil
			}
			return "", "", failf("unsupported map key: a nil %s", k.Type())
		}
		ek, err := e.enter(k)
		if err != nil {
			return "", "", err
		}
		key, orig, err = e.key(k.Elem(), false) // addressable
		e.leave(ek)
		return key, orig, err
	case reflect.Interface:
		return e.ifaceKey(k)
	}
	t := k.Type()
	if t == timeType {
		if !k.CanInterface() {
			return "", "", unreachableMethod(t, "MarshalText")
		}
		tm := k.Interface().(time.Time)
		return utc(tm), tm.Format(time.RFC3339Nano), nil
	}
	ti := infoOf(t)
	if k.Kind() != reflect.String { // a string key is taken as it is
		if kind, how := ti.use(k.CanAddr(), true); kind != noKind {
			return keyText(k, kind, how)
		}
	}
	switch k.Kind() {
	case reflect.String:
		return k.String(), k.String(), nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		s := strconv.FormatInt(k.Int(), 10)
		return s, s, nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		s := strconv.FormatUint(k.Uint(), 10)
		return s, s, nil
	case reflect.Float32, reflect.Float64:
		f := k.Float()
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return "", "", failf("unsupported map key %s", strconv.FormatFloat(f, 'g', -1, t.Bits()))
		}
		// The JSON number encoding/json writes for the float.
		var b []byte
		if t.Bits() == 32 {
			b, _ = json.Marshal(float32(f))
		} else {
			b, _ = json.Marshal(f)
		}
		return string(b), string(b), nil
	case reflect.Slice: // only reached through a pointer key
		if ti.bytes {
			if k.IsNil() {
				return "", "", failf("unsupported map key: a nil %s", t)
			}
			s := base64.StdEncoding.EncodeToString(k.Bytes())
			return s, s, nil
		}
	}
	return "", "", failf("unsupported map key type %s", t)
}

// ifaceKey is key for a map key of interface type.
func (e *encoder) ifaceKey(k reflect.Value) (key, orig string, err error) {
	if k.IsNil() {
		return "", "", failf("unsupported map key: a nil %s", k.Type())
	}
	elem := k.Elem()
	if elem.Kind() == reflect.Pointer && elem.IsNil() {
		switch kind := wrapperOf(k.Type()); kind {
		case noKind:
		case kindAppend, kindText:
			return keyText(k, kind, valueMethod) // called on the nil pointer
		case kindJSONTo, kindJSON:
			// The struct encoding/json wraps the pointer in to call the method
			// (which it never calls for a key) is no string.
			return "", "", failf("unsupported map key: a nil %s in a %s", elem.Type(), k.Type())
		default:
			return "", "", nil // encoding/json panics
		}
	}
	if k.Type() == anyType && elem.Type() == float64Type {
		// encoding/json writes a float64 in an interface{} as a JSON number,
		// which cannot be a key.
		return "", "", failf("unsupported map key %v: a float64 in an %s key", elem.Float(), k.Type())
	}
	return e.key(elem, false)
}

// keyText returns the key the map key k is sent as, and the one
// encoding/json makes of it, through its AppendText or MarshalText method;
// how says whether the method is called on k or on k's address.
func keyText(k reflect.Value, kind methodKind, how method) (key, orig string, err error) {
	name := methodNames[kind]
	if !k.CanInterface() {
		return "", "", unreachableMethod(k.Type(), name)
	}
	rcv := k
	if how == pointerMethod {
		rcv = k.Addr()
	}
	m, err := callMarshaler(rcv.Interface(), kind)
	switch {
	case err != nil:
		return "", "", failf("error calling %s for a map key of type %s: %v", name, k.Type(), err)
	case m.isNil:
		return "", "", nil // as for a nil pointer key
	case m.isTime:
		return utc(m.t), m.t.Format(time.RFC3339Nano), nil
	}
	return string(m.b), string(m.b), nil
}

// object builds the map[string]any a map converts to. Its keys are coerced
// to valid UTF-8 as strings are, and a time.Time key is in UTC. When two keys
// come out the same, the entry encoding/json would keep wins: it writes the
// keys in sorted order, and a decoder keeps the last of duplicate keys. So
// the result does not depend on the order of map iteration (unless the keys
// encoding/json makes are the same too, as for two pointer keys to equal
// values; then neither does encoding/json's).
type object struct {
	m    map[string]any
	orig map[string]string // the original key of each entry whose key differs from it
}

func newObject(n int) object { return object{m: make(map[string]any, n)} }

// set stores val under key, coerced to valid UTF-8.
func (o *object) set(key string, val any) { o.setAs(key, jsontext.ValidUTF8(key), val) }

// setAs stores val under key, which the original key orig is sent as.
func (o *object) setAs(orig, key string, val any) {
	if key == orig && o.orig == nil {
		// No key differs from its original, so no two keys can be the same.
		o.m[key] = val
		return
	}
	if _, dup := o.m[key]; dup {
		prev := key
		if p, ok := o.orig[key]; ok {
			prev = p
		}
		if orig < prev {
			return
		}
	}
	o.m[key] = val
	if key != orig {
		if o.orig == nil {
			o.orig = make(map[string]string)
		}
		o.orig[key] = orig
	} else {
		delete(o.orig, key)
	}
}

// utc returns t as an RFC 3339 string in UTC, with as many fractional
// digits as it needs.
func utc(t time.Time) string { return t.UTC().Format(time.RFC3339Nano) }

// timeString converts v, a time.Time.
func timeString(v reflect.Value) (any, error) {
	if !v.CanInterface() {
		return nil, unreachableMethod(v.Type(), "MarshalJSON")
	}
	return utc(v.Interface().(time.Time)), nil
}

// floatValue converts a float of the given bit size.
func floatValue(f float64, bits int, quoted bool) (any, error) {
	if quoted {
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return nil, failf("%s cannot be sent in a field with the \",string\" option", strconv.FormatFloat(f, 'g', -1, bits))
		}
		// The JSON number, as a string.
		var b []byte
		if bits == 32 {
			b, _ = json.Marshal(float32(f))
		} else {
			b, _ = json.Marshal(f)
		}
		return string(b), nil
	}
	if bits == 32 {
		return widen(float32(f)), nil
	}
	return f, nil
}

// widen converts f to the float64 nearest to f's shortest decimal form, the
// value encoding/json writes and fmt prints for it: float32(0.1) becomes 0.1,
// not 0.10000000149011612. NaN and ±Inf stay what they are.
func widen(f float32) float64 {
	w := float64(f)
	if math.IsNaN(w) || math.IsInf(w, 0) {
		return w
	}
	w, _ = strconv.ParseFloat(strconv.FormatFloat(w, 'g', -1, 32), 64)
	return w
}

// numberValue converts a json.Number as encoding/json encodes it.
func numberValue(s string, quoted bool) (any, error) {
	if s == "" {
		s = "0" // encoding/json writes the zero Number as 0
	}
	if !isNumberLiteral(s) {
		return nil, failf("invalid number literal %q", s)
	}
	if quoted {
		return s, nil
	}
	n, err := jsonNumber(json.Number(s))
	if err != nil {
		return nil, failf("%v", err)
	}
	return n, nil
}

// isNumberLiteral reports whether s is a JSON number. A JSON value that
// starts with '-' or a digit and ends with a digit is a number: json.Valid
// checks the rest of the grammar.
func isNumberLiteral(s string) bool {
	return s != "" && (s[0] == '-' || '0' <= s[0] && s[0] <= '9') &&
		'0' <= s[len(s)-1] && s[len(s)-1] <= '9' && json.Valid([]byte(s))
}

// marshal encodes v through the marshal method kind, as encoding/json does
// (see callMarshaler); how says whether the method is called on v or on v's
// address. For v of interface type, it is the method of the value inside.
func (e *encoder) marshal(v reflect.Value, kind methodKind, how method) (any, error) {
	name := methodNames[kind]
	if !v.CanInterface() {
		return nil, unreachableMethod(v.Type(), name)
	}
	rcv := v
	if how == pointerMethod {
		rcv = v.Addr()
	}
	m, err := callMarshaler(rcv.Interface(), kind)
	if err != nil {
		return nil, failf("error calling %s for type %s: %v", name, v.Type(), err)
	}
	return e.marshaled(m, kind, v.Type())
}

// marshalNilTo encodes v, an interface of type json/v2's MarshalerTo holding
// a nil pointer, as encoding/json does: through the method, called on the
// nil pointer. json.Marshal calls it on a field of that type.
func (e *encoder) marshalNilTo(v reflect.Value) (any, error) {
	holder := reflect.New(reflect.StructOf([]reflect.StructField{{Name: "V", Type: v.Type()}})).Elem()
	holder.Field(0).Set(v)
	b, isNil, err := call(holder.Interface(), kindJSONTo, func() bool { return true })
	if isNil {
		return nil, nil
	}
	if err == nil {
		var fields struct{ V json.RawMessage }
		if err = json.Unmarshal(b, &fields); err == nil {
			b = fields.V
		}
	}
	if err != nil {
		return nil, failf("error calling MarshalJSONTo for type %s: %v", v.Type(), err)
	}
	return e.marshaled(marshaled{b: b}, kindJSONTo, v.Type())
}

// marshaled converts what a marshal method of kind encoded a value of type
// t as.
func (e *encoder) marshaled(m marshaled, kind methodKind, t reflect.Type) (any, error) {
	switch {
	case m.isNil:
		return nil, nil
	case m.isTime:
		return utc(m.t), nil
	case kind == kindAppend || kind == kindText:
		return jsontext.ValidUTF8(string(m.b)), nil
	}
	out, depth, err := fromJSONText(m.b)
	if err != nil {
		return nil, failf("error calling %s for type %s: %v", methodNames[kind], t, err)
	}
	if e.nested+depth > maxNesting {
		return nil, errNesting()
	}
	return out, nil
}

// marshaled is what callMarshaler encodes a value as: the output of its
// method, a time.Time, or nil.
type marshaled struct {
	b      []byte
	t      time.Time
	isTime bool
	isNil  bool
}

// callMarshaler calls the marshal method kind of x as encoding/json calls
// it, except that:
//
//   - A time.Time or *time.Time comes back as the time, to be sent in UTC.
//     So does a value whose method returns just what the method of the
//     time.Time it embeds returns, as time.Time's promoted method does:
//     encoding/json encodes such a value as that time.
//   - Where encoding/json calls the method on a nil pointer — x is one, or
//     the method is promoted through a nil embedded pointer or interface —
//     and the method fails on it with a run-time error, as a value receiver
//     or a dereferencing pointer receiver does, the value comes back as nil
//     instead of panicking (fmt prints "<nil>" for such a value). As reflect
//     does not say whether a method is promoted, a run-time error while an
//     embedded pointer or interface that has the method is nil is taken as
//     one. Any other panic is passed on, as encoding/json passes it on.
func callMarshaler(x any, kind methodKind) (marshaled, error) {
	switch t := x.(type) {
	case time.Time:
		return marshaled{t: t, isTime: true}, nil
	case *time.Time:
		if t == nil {
			return marshaled{isNil: true}, nil // its methods have value receivers
		}
		return marshaled{t: *t, isTime: true}, nil
	}
	if x == nil || methodOf(reflect.TypeOf(x), kind) != valueMethod {
		return marshaled{isNil: true}, nil // encoding/json writes null
	}
	var nilRcv func() bool // whether a run-time error is the method failing on a nil pointer
	v := reflect.ValueOf(x)
	if v.Kind() == reflect.Pointer {
		if v.IsNil() {
			if methodOf(v.Type().Elem(), kind) == valueMethod {
				// A method of the type pointed to: called on the nil pointer,
				// it panics whatever it does.
				return marshaled{isNil: true}, nil
			}
			nilRcv = func() bool { return true }
		} else {
			v = v.Elem()
		}
	}
	var ti *typeInfo
	if v.Kind() == reflect.Struct {
		ti = infoOf(v.Type())
		if len(ti.nilable) > 0 {
			nilRcv = func() bool { return nilOn(v, ti.nilable) }
		}
	}
	b, isNil, err := call(x, kind, nilRcv)
	if isNil {
		return marshaled{isNil: true}, nil
	}
	if ti != nil && ti.timeIndex != nil && kind != kindJSONTo {
		if t, ok := timeAt(v, ti.timeIndex); ok {
			tb, _, terr := call(t, kind, nil)
			if bytes.Equal(b, tb) && sameError(err, terr) {
				return marshaled{t: t, isTime: true}, nil
			}
		}
	}
	return marshaled{b: b}, err
}

// call calls the marshal method kind of x. With nilRcv, a run-time error the
// method panics with, if nilRcv says a nil pointer is involved, makes it
// report isNil instead; any other panic is passed on.
func call(x any, kind methodKind, nilRcv func() bool) (b []byte, isNil bool, err error) {
	if nilRcv != nil {
		defer func() {
			if r := recover(); r != nil {
				if _, ok := r.(runtime.Error); !ok || !nilRcv() {
					panic(r)
				}
				b, isNil, err = nil, true, nil
			}
		}()
	}
	switch kind {
	case kindJSONTo:
		// The method takes a jsontext.Encoder: let json.Marshal call it.
		b, err = json.Marshal(x)
		if me, ok := errors.AsType[*json.MarshalerError](err); ok {
			err = me.Err
		}
	case kindJSON:
		b, err = x.(json.Marshaler).MarshalJSON()
	case kindAppend:
		b, err = x.(encoding.TextAppender).AppendText(nil)
	default:
		b, err = x.(encoding.TextMarshaler).MarshalText()
	}
	return b, false, err
}

// sameError reports whether a and b are both nil, or say the same.
func sameError(a, b error) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.Error() == b.Error()
}

// timeAt returns the time.Time (or *time.Time) at index in the struct v,
// following embedded pointers; false when a nil pointer is on the way.
func timeAt(v reflect.Value, index []int) (time.Time, bool) {
	f, ok := fieldByIndex(v, index)
	if !ok {
		return time.Time{}, false
	}
	if f.Kind() == reflect.Pointer {
		if f.IsNil() {
			return time.Time{}, false
		}
		f = f.Elem()
	}
	if !f.CanInterface() {
		return time.Time{}, false
	}
	t, ok := f.Interface().(time.Time)
	return t, ok
}

// nilOn reports whether one of the embedded pointers and interfaces at
// indexes in the struct v is nil, holds a nil pointer, or lies behind a nil
// pointer.
func nilOn(v reflect.Value, indexes [][]int) bool {
	for _, index := range indexes {
		f, ok := fieldByIndex(v, index)
		if !ok {
			return true
		}
		if f.Kind() == reflect.Interface && !f.IsNil() {
			f = f.Elem()
		}
		if (f.Kind() == reflect.Pointer || f.Kind() == reflect.Interface) && f.IsNil() {
			return true
		}
	}
	return false
}

// fromJSONText decodes the JSON text b, converting numbers as fromJSON does.
// depth is how deeply b nests objects and arrays.
func fromJSONText(b []byte) (out any, depth int, err error) {
	if !json.Valid(b) {
		var discard any
		if err := json.Unmarshal(b, &discard); err != nil {
			return nil, 0, err // says what is wrong where
		}
		return nil, 0, errors.New("invalid JSON")
	}
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	if err := dec.Decode(&out); err != nil {
		return nil, 0, err
	}
	return fromJSON(out)
}

// unsigned returns u as int64 when it fits, as every integer travels, and as
// uint64 only above math.MaxInt64.
func unsigned(u uint64) any {
	if u <= math.MaxInt64 {
		return int64(u)
	}
	return u
}

// fromJSON converts a value decoded with json.Decoder.UseNumber, turning
// numbers into int64 when integral and float64 otherwise. depth is how
// deeply v nests objects and arrays.
func fromJSON(v any) (out any, depth int, err error) {
	switch x := v.(type) {
	case json.Number:
		n, err := jsonNumber(x)
		return n, 0, err
	case []any:
		for i, e := range x {
			n, d, err := fromJSON(e)
			if err != nil {
				return nil, 0, err
			}
			x[i], depth = n, max(depth, d)
		}
		return x, depth + 1, nil
	case map[string]any:
		for k, e := range x {
			n, d, err := fromJSON(e)
			if err != nil {
				return nil, 0, err
			}
			x[k], depth = n, max(depth, d)
		}
		return x, depth + 1, nil
	default:
		return x, 0, nil
	}
}

// jsonNumber converts a JSON number to int64 when it is an integer that fits,
// to uint64 for a larger unsigned integer (so it keeps its precision), and to
// float64 otherwise.
func jsonNumber(n json.Number) (any, error) {
	if i, err := n.Int64(); err == nil {
		return i, nil
	}
	if u, err := strconv.ParseUint(n.String(), 10, 64); err == nil {
		return u, nil
	}
	f, err := n.Float64()
	if err != nil {
		return nil, fmt.Errorf("invalid number %q", n.String())
	}
	return f, nil
}

var (
	anyType           = reflect.TypeFor[any]()
	float64Type       = reflect.TypeFor[float64]()
	errorType         = reflect.TypeFor[error]()
	marshalerType     = reflect.TypeFor[json.Marshaler]()
	textAppenderType  = reflect.TypeFor[encoding.TextAppender]()
	textMarshalerType = reflect.TypeFor[encoding.TextMarshaler]()
	isZeroerType      = reflect.TypeFor[interface{ IsZero() bool }]()
	timeType          = reflect.TypeFor[time.Time]()
	numberType        = reflect.TypeFor[json.Number]()
	anyMapType        = reflect.TypeFor[map[string]any]()
	anySliceType      = reflect.TypeFor[[]any]()
	// The other interfaces of encoding/json's marshal and unmarshal methods.
	otherMethodTypes = []reflect.Type{
		reflect.TypeFor[json.Unmarshaler](),
		reflect.TypeFor[encoding.TextUnmarshaler](),
	}
)

// methodKind names a marshal method encoding/json calls.
type methodKind uint8

const (
	noKind     methodKind = iota
	kindJSONTo            // MarshalJSONTo(*jsontext.Encoder) error, of encoding/json/v2
	kindJSON              // MarshalJSON
	kindAppend            // AppendText
	kindText              // MarshalText
	kindOther             // of an interface type with one of them that is not their interface
)

// methodNames are the names of the methods of each kind, in the order
// encoding/json prefers them.
var methodNames = [...]string{kindJSONTo: "MarshalJSONTo", kindJSON: "MarshalJSON", kindAppend: "AppendText", kindText: "MarshalText"}

// method says how a type has the method of an interface.
type method uint8

const (
	noMethod      method = iota
	valueMethod          // the type implements the interface
	pointerMethod        // only the pointer type does: usable on an addressable value
)

// methodOf says how the type t (not a pointer type) has the marshal method
// kind.
func methodOf(t reflect.Type, kind methodKind) method {
	switch kind {
	case kindJSONTo:
		return methodSig(t, "MarshalJSONTo", "Encoder")
	case kindJSON:
		return implementation(t, marshalerType)
	case kindAppend:
		return implementation(t, textAppenderType)
	case kindText:
		return implementation(t, textMarshalerType)
	}
	return noMethod
}

// implementation says how the type t has the method of the interface type
// iface.
func implementation(t, iface reflect.Type) method {
	switch {
	case t.Implements(iface):
		return valueMethod
	case t.Kind() != reflect.Pointer && t.Kind() != reflect.Interface && reflect.PointerTo(t).Implements(iface):
		return pointerMethod
	}
	return noMethod
}

// methodSig is implementation for an interface of encoding/json/v2, which
// this package does not import (it exists only in builds with the jsonv2
// experiment, on by default): the method name(*jsontext.<param>) error.
func methodSig(t reflect.Type, name, param string) method {
	has := func(t reflect.Type) bool {
		m, ok := t.MethodByName(name)
		if !ok {
			return false
		}
		in := 1 // the receiver
		if t.Kind() == reflect.Interface {
			in = 0
		}
		ft := m.Type
		if ft.NumIn() != in+1 || ft.NumOut() != 1 || ft.Out(0) != errorType {
			return false
		}
		p := ft.In(in)
		return p.Kind() == reflect.Pointer && p.Elem().PkgPath() == "encoding/json/jsontext" && p.Elem().Name() == param
	}
	switch {
	case has(t):
		return valueMethod
	case t.Kind() != reflect.Pointer && t.Kind() != reflect.Interface && has(reflect.PointerTo(t)):
		return pointerMethod
	}
	return noMethod
}

// hasMarshalMethod reports whether t or *t has one of the marshal methods.
func hasMarshalMethod(t reflect.Type) bool {
	for kind := kindJSONTo; kind <= kindText; kind++ {
		if methodOf(t, kind) != noMethod {
			return true
		}
	}
	return false
}

// hasAnyMethod reports whether t or *t has one of encoding/json's marshal
// or unmarshal methods.
func hasAnyMethod(t reflect.Type) bool { return anyMethod(t) != "" }

// anyMethod returns the name of one of encoding/json's marshal or unmarshal
// methods t or *t has, "" if it has none.
func anyMethod(t reflect.Type) string {
	for kind := kindJSONTo; kind <= kindText; kind++ {
		if methodOf(t, kind) != noMethod {
			return methodNames[kind]
		}
	}
	if methodSig(t, "UnmarshalJSONFrom", "Decoder") != noMethod {
		return "UnmarshalJSONFrom"
	}
	for _, iface := range otherMethodTypes {
		if implementation(t, iface) != noMethod {
			return iface.Method(0).Name
		}
	}
	return ""
}

// errLeftOut is the error for a struct field encoding/json leaves out
// although it holds data: an unexported embedded struct named by its tag
// whose methods it cannot call. normalize refuses the struct instead.
var errLeftOut = errors.New("encoding/json would leave it out, dropping its data")

// wrapperOf says, for the interface type t, through which method
// encoding/json encodes a nil pointer the interface holds: the method of t
// when t is the interface of one, kindOther when t has a marshal method but
// is none of them (encoding/json panics), and noKind when t has none (the
// pointer is null).
func wrapperOf(t reflect.Type) methodKind {
	switch {
	case t == marshalerType:
		return kindJSON
	case t == textAppenderType:
		return kindAppend
	case t == textMarshalerType:
		return kindText
	case t.PkgPath() == "encoding/json/v2" && t.Name() == "MarshalerTo":
		return kindJSONTo
	case hasMarshalMethod(t):
		return kindOther
	}
	return noKind
}

// typeInfo is what the walk needs to know about a type (other than a pointer
// or interface type), worked out once per type.
type typeInfo struct {
	methods [kindText + 1]method // how the type has each marshal method
	bytes   bool                 // a slice type sent as []byte
	nilKey  bool                 // a map type whose nil pointer keys encoding/json writes as ""
	// Of a struct type:
	fields   []structField // the fields that are encoded
	fallback *structField  // the embedded fallback field, if any
	refused  string        // why encoding/json refuses the type, if it does
	leftOut  string        // a field encoding/json leaves out but normalize does not (see structFields)
	// Of a struct type with marshal methods (see promotedFrom):
	timeIndex []int   // the embedded time.Time that may have promoted them
	nilable   [][]int // the embedded pointers and interfaces they may be promoted through
}

// use returns the marshal method encoding/json encodes a value of the type
// through, noKind if none, and how the type has it: the first method of
// methodNames it has, with a pointer receiver only if addressable, and for a
// map key (key) no MarshalJSONTo or MarshalJSON.
func (ti *typeInfo) use(addressable, key bool) (methodKind, method) {
	first := kindJSONTo
	if key {
		first = kindAppend
	}
	for kind := first; kind <= kindText; kind++ {
		if m := ti.methods[kind]; m == valueMethod || m == pointerMethod && addressable {
			return kind, m
		}
	}
	return noKind, noMethod
}

var typeInfos sync.Map // reflect.Type → *typeInfo

func infoOf(t reflect.Type) *typeInfo {
	if ti, ok := typeInfos.Load(t); ok {
		return ti.(*typeInfo)
	}
	ti, _ := typeInfos.LoadOrStore(t, newTypeInfo(t))
	return ti.(*typeInfo)
}

func newTypeInfo(t reflect.Type) *typeInfo {
	ti := &typeInfo{}
	if t.Kind() != reflect.Pointer && t.Kind() != reflect.Interface {
		for kind := kindJSONTo; kind <= kindText; kind++ {
			ti.methods[kind] = methodOf(t, kind)
		}
	}
	switch t.Kind() {
	case reflect.Slice:
		// A []byte, or a slice of a byte type without marshal methods.
		ti.bytes = t.Elem().Kind() == reflect.Uint8 && !hasMarshalMethod(t.Elem())
	case reflect.Map:
		k := t.Key()
		ti.nilKey = k.Kind() == reflect.Pointer && (k.Implements(textMarshalerType) || k.Implements(textAppenderType))
	case reflect.Struct:
		ti.fields, ti.fallback, ti.refused, ti.leftOut = structFields(t)
		if ti.methods != [len(ti.methods)]method{} {
			ti.timeIndex, ti.nilable = promotedFrom(t)
		}
	}
	return ti
}

// promotedFrom works out where the struct type t may have its marshal
// methods from, by Go's rules for promoted methods. A method is promoted
// from the shallowest depth at which t embeds a type that has it — directly,
// or through embedded structs — if only one type has it there; a method t
// declares itself comes first. reflect does not say where a method comes
// from, so the result is what can be told from the fields:
//
//   - timeIndex is the index sequence of the time.Time (or *time.Time) t
//     embeds at the shallowest depth it embeds one at, if it embeds only one
//     there: time.Time's methods are promoted to t from it unless t, or a
//     shallower embedded type, has its own.
//   - nilable are the index sequences of the embedded pointers and
//     interfaces (down to that depth) that have one of the methods: a method
//     promoted through one of them that is nil panics.
func promotedFrom(t reflect.Type) (timeIndex []int, nilable [][]int) {
	type embedded struct {
		typ   reflect.Type
		index []int
	}
	var (
		current, next    = []embedded{}, []embedded{{typ: t}}
		count, nextCount map[reflect.Type]int // how often each struct is embedded at the current and next depth
		visited          = map[reflect.Type]bool{}
		times            int // how often a time.Time is embedded at the current depth
	)
	for len(next) > 0 && times == 0 {
		current, next = next, current[:0]
		count, nextCount = nextCount, map[reflect.Type]int{}
		for _, s := range current {
			if visited[s.typ] {
				continue
			}
			visited[s.typ] = true
			for i := range s.typ.NumField() {
				sf := s.typ.Field(i)
				if !sf.Anonymous {
					continue
				}
				index := append(slices.Clip(s.index), i)
				ft := sf.Type
				switch ft.Kind() {
				case reflect.Pointer, reflect.Interface:
					if hasMarshalMethod(ft) {
						nilable = append(nilable, index)
					}
				}
				if ft.Kind() == reflect.Pointer {
					ft = ft.Elem()
				}
				switch {
				case ft == timeType:
					times += max(count[s.typ], 1)
					timeIndex = index
				case ft.Kind() == reflect.Struct:
					if nextCount[ft]++; nextCount[ft] == 1 {
						next = append(next, embedded{typ: ft, index: index})
					}
				}
			}
		}
	}
	if times != 1 {
		timeIndex = nil
	}
	return timeIndex, nilable
}

// fieldOptions are the options of a struct field's json tag, read as
// encoding/json reads them.
type fieldOptions struct {
	name      string // the name of the object member
	hasName   bool   // the name comes from the tag
	embed     bool   // the field's members are the struct's own (embedding, or the "embed" option)
	omitEmpty bool
	omitZero  bool
	quoted    bool   // the "string" option
	format    string // the value of a "format:" option
	casing    bool   // a "case:" option, which marshaling ignores
}

// structField is a struct field the walk encodes.
type structField struct {
	fieldOptions
	index []int // the field's index sequence, through embedded structs
	raw   bool  // an embedded fallback of type jsontext.Value
	// isZero is the omitzero test through the field type's IsZero method,
	// nil for a type without one (reflect.Value.IsZero is used then).
	isZero func(reflect.Value) (bool, error)
}

// structFields returns the fields encoding/json encodes for the struct type
// t, in field order, its embedded fallback field (a map with string keys or
// a jsontext.Value tagged ",embed", whose members are added to the struct's),
// and why encoding/json refuses t ("" if it does not). It mirrors
// encoding/json/v2's makeStructFields, a breadth-first walk through embedded
// structs (also unexported ones, and pointers to them) that are not named by
// a tag: the exported fields, named by their json tag or else their Go name,
// and tagged unexported embedded structs without marshal or unmarshal
// methods; fields tagged "-" are left out. leftOut describes a tagged
// unexported embedded struct with such a method (or IsZero, with omitzero),
// which encoding/json leaves out and normalize refuses. Of several fields with the same
// name, the least nested one wins; at the same depth a tagged one wins over
// untagged ones; when that does not single one out, all of them are left out.
func structFields(root reflect.Type) (fields []structField, fallback *structField, refused, leftOut string) {
	type queued struct {
		typ           reflect.Type
		index         []int
		visitChildren bool // whether to queue the structs this one embeds
	}
	queue := []queued{{root, nil, true}}
	seen := map[reflect.Type]bool{root: true}
	var all, fallbacks []structField
	for qi := 0; qi < len(queue); qi++ {
		q := queue[qi]
		for i := range q.typ.NumField() {
			sf := q.typ.Field(i)
			opts, ignored := parseFieldTag(sf)
			if ignored {
				continue
			}
			f := structField{fieldOptions: opts, index: append(slices.Clip(q.index), i)}
			tf := sf.Type
			if tf.Kind() == reflect.Pointer && tf.Name() == "" {
				tf = tf.Elem()
			}
			if sf.Anonymous && !f.hasName && tf.Kind() == reflect.Struct {
				f.embed = true
			}
			// field adds f as a field of its own.
			field := func() {
				if !sf.IsExported() {
					if !sf.Anonymous || tf.Kind() != reflect.Struct {
						return
					}
					// Methods cannot be called on an unexported field:
					// encoding/json leaves it out, and normalize refuses the
					// struct rather than drop the field's data.
					name := anyMethod(tf)
					if name == "" && f.omitZero && implementation(tf, isZeroerType) != noMethod {
						name = "IsZero"
					}
					if name != "" {
						if leftOut == "" {
							leftOut = fmt.Sprintf("field %s of %s is an unexported embedded struct with a %s method, which cannot be called on it", sf.Name, q.typ, name)
						}
						return
					}
				}
				if f.omitZero {
					f.isZero = zeroFunc(sf.Type)
				}
				all = append(all, f)
				if f.format != "" && refused == "" {
					refused = fmt.Sprintf("encoding/json does not support the \"format\" tag option of field %s of %s", sf.Name, q.typ)
				}
			}
			if !f.embed {
				field()
				continue
			}
			if f.hasName || f.omitEmpty || f.omitZero || f.quoted || f.format != "" || f.casing {
				if f.hasName { // a named field after all
					field()
					continue
				}
				f.fieldOptions = fieldOptions{name: f.name, embed: true} // the other options are ignored
			}
			switch {
			case tf.Kind() == reflect.Struct:
				if q.visitChildren {
					queue = append(queue, queued{tf, f.index, !seen[tf]})
				}
				seen[tf] = true
				continue
			case !sf.IsExported():
				continue
			case tf.PkgPath() == "encoding/json/jsontext" && tf.Name() == "Value":
				f.raw = true
			case tf.Kind() == reflect.Map && tf.Key().Kind() == reflect.String && !hasAnyMethod(tf.Key()):
			default:
				field()
				continue
			}
			fallbacks = append(fallbacks, f)
		}
	}
	// Sort by name, then least nested first, then tagged first.
	slices.SortStableFunc(all, func(a, b structField) int {
		return cmp.Or(strings.Compare(a.name, b.name), cmp.Compare(len(a.index), len(b.index)), boolsCompare(!a.hasName, !b.hasName))
	})
	for len(all) > 0 {
		n := 1
		for n < len(all) && all[n].name == all[0].name {
			n++
		}
		// all[0] wins its name unless all[1] is as nested and as tagged.
		if n == 1 || len(all[0].index) != len(all[1].index) || all[0].hasName != all[1].hasName {
			fields = append(fields, all[0])
		}
		all = all[n:]
	}
	slices.SortFunc(fields, func(a, b structField) int { return slices.Compare(a.index, b.index) })
	if n := len(fallbacks); n == 1 || n > 1 && len(fallbacks[0].index) != len(fallbacks[1].index) {
		fallback = &fallbacks[0]
	}
	return fields, fallback, refused, leftOut
}

// boolsCompare orders false before true.
func boolsCompare(a, b bool) int {
	switch {
	case !a && b:
		return -1
	case a && !b:
		return 1
	}
	return 0
}

// parseFieldTag reads the json tag of the struct field sf as encoding/json
// reads it (its parseFieldOptions, without the errors it ignores for v1).
// ignored reports a field that is not encoded: tagged "-", or unexported and
// not embedded.
func parseFieldTag(sf reflect.StructField) (o fieldOptions, ignored bool) {
	tag := sf.Tag.Get("json")
	if tag == "-" || !sf.IsExported() && !sf.Anonymous {
		return o, true
	}
	o.name = sf.Name
	if tag != "" && tag[0] != ',' {
		// The name runs up to a comma, backslash or quote; if one of the
		// latter follows it, the name is the identifier it starts with.
		n := len(tag) - len(strings.TrimLeftFunc(tag, func(r rune) bool { return !strings.ContainsRune(",\\'\"`", r) }))
		name, ok := tag[:n], true
		if !strings.HasPrefix(tag[n:], ",") && n != len(tag) {
			name, n, ok = tagOption(tag, false)
		}
		if ok {
			o.name, o.hasName = string([]rune(name)), true // invalid UTF-8 becomes U+FFFD
		}
		tag = tag[n:]
	}
	for tag != "" {
		if tag[0] == ',' {
			if tag = tag[1:]; tag == "" {
				break
			}
		}
		opt, n, _ := tagOption(tag, false)
		tag = tag[n:]
		switch opt {
		case "case":
			if !strings.HasPrefix(tag, ":") {
				break
			}
			tag = tag[1:]
			value, n, ok := tagOption(tag, false)
			if !ok {
				break
			}
			tag = tag[n:]
			o.casing = o.casing || value == "ignore" || value == "strict"
		case "embed":
			o.embed = true
		case "omitzero":
			o.omitZero = true
		case "omitempty":
			o.omitEmpty = true
		case "string":
			o.quoted = true
		case "format":
			if !strings.HasPrefix(tag, ":") {
				break
			}
			tag = tag[1:]
			value, n, ok := tagOption(tag, true)
			if !ok || value == "" {
				break
			}
			tag = tag[n:]
			o.format = value
		}
	}
	return o, false
}

// tagOption consumes the next tag option at the start of in: a Go
// identifier or, if quoted is set, a single-quoted string. n is how much of
// in it takes; if the option is invalid, ok is false and it takes all of in
// up to the next comma.
func tagOption(in string, quoted bool) (opt string, n int, ok bool) {
	i := strings.IndexByte(in, ',')
	if i < 0 {
		i = len(in)
	}
	switch r, _ := utf8.DecodeRuneInString(in); {
	case r == '_' || unicode.IsLetter(r):
		n := len(in) - len(strings.TrimLeftFunc(in, isLetterOrDigit))
		return in[:n], n, true
	case r == '\'' && quoted:
		// Like a double-quoted Go string literal, in single quotes.
		var escaped bool
		b := []byte{'"'}
		n := len(`'`)
		for len(in) > n {
			r, size := utf8.DecodeRuneInString(in[n:])
			switch {
			case escaped:
				if r == '\'' {
					b = b[:len(b)-1] // `\'` is `'`
				}
				escaped = false
			case r == '\\':
				escaped = true
			case r == '"':
				b = append(b, '\\')
			case r == '\'':
				b = append(b, '"')
				n += len(`'`)
				out, err := strconv.Unquote(string(b))
				if err != nil {
					return in[:i], i, false
				}
				return out, n, true
			}
			b = append(b, in[n:][:size]...)
			n += size
		}
	}
	return in[:i], i, false
}

func isLetterOrDigit(r rune) bool {
	return r == '_' || unicode.IsLetter(r) || unicode.IsNumber(r)
}

// isEmptyValue is encoding/json's omitempty test.
func isEmptyValue(v reflect.Value) bool {
	switch v.Kind() {
	case reflect.Array, reflect.Map, reflect.Slice, reflect.String:
		return v.Len() == 0
	case reflect.Bool,
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr,
		reflect.Float32, reflect.Float64,
		reflect.Interface, reflect.Pointer:
		return v.IsZero()
	}
	return false
}

// zeroFunc returns encoding/json's omitzero test for a field of type t that
// has an IsZero method, or nil when t has none.
func zeroFunc(t reflect.Type) func(reflect.Value) (bool, error) {
	switch {
	case t.Kind() == reflect.Interface && t.Implements(isZeroerType):
		return func(v reflect.Value) (bool, error) {
			// Not called on a nil interface, or a nil pointer in one.
			if v.IsNil() || v.Elem().Kind() == reflect.Pointer && v.Elem().IsNil() {
				return true, nil
			}
			return callIsZero(v)
		}
	case t.Kind() == reflect.Pointer && t.Implements(isZeroerType):
		return func(v reflect.Value) (bool, error) {
			if v.IsNil() {
				return true, nil
			}
			return callIsZero(v)
		}
	case t.Implements(isZeroerType):
		return callIsZero
	case reflect.PointerTo(t).Implements(isZeroerType):
		return func(v reflect.Value) (bool, error) {
			if !v.CanAddr() {
				if !v.CanInterface() {
					return false, unreachableMethod(v.Type(), "IsZero")
				}
				// Copy the value to call the method on a pointer to it.
				c := reflect.New(v.Type()).Elem()
				c.Set(v)
				v = c
			}
			return callIsZero(v.Addr())
		}
	}
	return nil
}

func callIsZero(v reflect.Value) (bool, error) {
	if !v.CanInterface() {
		return false, unreachableMethod(v.Type(), "IsZero")
	}
	return v.Interface().(interface{ IsZero() bool }).IsZero(), nil
}

// payloadError reports a part of a payload value that cannot be sent, and
// where in the value it is.
type payloadError struct {
	path []string // field, key and index segments, innermost first
	err  error
}

// maxPathSegments bounds the path an error message shows: a value that
// contains itself fails a thousand levels down.
const maxPathSegments = 16

// Error implements error: "<path>: <problem>", e.g.
// `readings[2].callback: unsupported type func()`. A longer path than
// maxPathSegments is shortened in the middle.
func (e *payloadError) Error() string {
	n := len(e.path)
	if n == 0 {
		return e.err.Error()
	}
	var b strings.Builder
	for j := 0; j < n; j++ { // outermost segment first
		seg := e.path[n-1-j]
		if n > maxPathSegments && j == maxPathSegments/2 {
			j = n - maxPathSegments/2
			seg = "…" + strings.TrimPrefix(e.path[n-1-j], ".")
		}
		b.WriteString(seg)
	}
	return strings.TrimPrefix(b.String(), ".") + ": " + e.err.Error()
}

func (e *payloadError) Unwrap() error { return e.err }

func failf(format string, args ...any) error {
	return &payloadError{err: fmt.Errorf(format, args...)}
}

// at returns err located in the field, key or index seg of a value.
func at(err error, seg string) error {
	var pe *payloadError
	if !errors.As(err, &pe) {
		pe = &payloadError{err: err}
	}
	pe.path = append(pe.path, seg)
	return pe
}

// keySegment is the path segment of a field or map key: .name, or ["name"]
// when the name is not an ASCII identifier.
func keySegment(name string) string {
	if isIdentifier(name) {
		return "." + name
	}
	return "[" + strconv.Quote(name) + "]"
}

func isIdentifier(s string) bool {
	for i, c := range s {
		switch {
		case c == '_', 'a' <= c && c <= 'z', 'A' <= c && c <= 'Z':
		case i > 0 && '0' <= c && c <= '9':
		default:
			return false
		}
	}
	return s != ""
}

func indexSegment(i int) string { return "[" + strconv.Itoa(i) + "]" }

// unreachableMethod is the error for a method encoding/json would call (or a
// time.Time it would read) on a value reflection reaches only through an
// unexported field and cannot call it on. encoding/json leaves such fields
// out, so the walk is not expected to meet one.
func unreachableMethod(t reflect.Type, name string) error {
	return failf("cannot call %s of %s: it is reached through an unexported field", name, t)
}

// normalizeRows validates a bulk batch — a non-empty slice or array whose
// elements are rows (maps with string keys, or structs) — and returns it as
// []any of map[string]any, each row normalized as PublishToTable normalizes
// a row passed to it.
func normalizeRows(rows any) ([]any, error) {
	if rows == nil {
		return nil, fmt.Errorf("rows must be a non-empty list of rows")
	}
	rv := reflect.ValueOf(rows)
	if rv.Kind() != reflect.Slice && rv.Kind() != reflect.Array {
		return nil, fmt.Errorf("rows must be a non-empty list of rows, got %T", rows)
	}
	if rv.Len() == 0 {
		return nil, fmt.Errorf("rows must be a non-empty list of rows")
	}
	var e encoder
	out := make([]any, rv.Len())
	for i := range out {
		// Each row is taken as a value, as PublishToTable takes one: not
		// addressable, so a MarshalJSON or MarshalText method with a pointer
		// receiver is not used for it (as for json.Marshal(row), not
		// json.Marshal(rows)), and a row is sent the same alone and in a
		// batch.
		row := rv.Index(i).Interface()
		n, err := e.anyValue(row)
		if err != nil {
			return nil, fmt.Errorf("row at index %d: %v", i, err)
		}
		m, ok := n.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("row at index %d must be a map or struct, got %s", i, describeRow(row, n))
		}
		out[i] = m
	}
	return out, nil
}

// describeRow says what row is, a bulk row that normalized to n, which is
// not a map.
func describeRow(row, n any) string {
	if row == nil {
		return "nil"
	}
	v := reflect.ValueOf(row)
	if name := marshalMethod(v); name != "" {
		return fmt.Sprintf("%s, which its %s method encodes as %s", v.Type(), name, shape(n))
	}
	if n == nil {
		return "a nil " + v.Type().String()
	}
	return v.Type().String()
}

// marshalMethod returns the name of the method normalize encodes v through
// (MarshalJSONTo, MarshalJSON, AppendText or MarshalText), or "" if it uses
// none.
func marshalMethod(v reflect.Value) string {
	addressable := false
	for v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return ""
		}
		v, addressable = v.Elem(), true
	}
	switch v.Type() {
	case timeType:
		return "MarshalJSON"
	case numberType:
		return "MarshalJSONTo"
	}
	kind, _ := infoOf(v.Type()).use(addressable, false)
	return methodNames[kind]
}

// shape names what kind of value n, a normalized value, is.
func shape(n any) string {
	switch n.(type) {
	case nil:
		return "null"
	case string:
		return "a string"
	case bool:
		return "a boolean"
	case []any:
		return "a list"
	case []byte:
		return "binary data"
	}
	return "a number"
}

// DecodeRows converts rows (as returned by GetHistory and friends) into
// typed values through encoding/json, matching columns to json tags. The
// data backend sends timestamp columns, tsp included, as epoch milliseconds:
// decode them into an int64 (time.UnixMilli converts it).
//
// NaN and ±Inf (which the Python SDK sends as they are, e.g. for a failed
// sensor read) decode as JSON null does: the field keeps its zero value, or
// is nil when it is a pointer — declare it as *float64 to tell such a value
// from 0.
func DecodeRows[T any](rows []Row) ([]T, error) {
	data, err := json.Marshal(finite.OrNil(rows))
	if err != nil {
		return nil, fmt.Errorf("decode rows: %w", err)
	}
	var out []T
	if err := json.Unmarshal(data, &out); err != nil {
		return nil, fmt.Errorf("decode rows: %w", err)
	}
	return out, nil
}

// Decode converts a payload value (an event argument, a call result, a row)
// into T through encoding/json, decoding NaN and ±Inf as DecodeRows does.
func Decode[T any](v any) (T, error) {
	var out T
	data, err := json.Marshal(finite.OrNil(v))
	if err != nil {
		return out, fmt.Errorf("decode: %w", err)
	}
	if err := json.Unmarshal(data, &out); err != nil {
		return out, fmt.Errorf("decode: %w", err)
	}
	return out, nil
}
