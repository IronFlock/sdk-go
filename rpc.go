package ironflock

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/ironflock/sdk-go/internal/jsontext"
	"github.com/ironflock/sdk-go/wamp"
)

// Procedure of the (not yet served) device location service.
const uriLocationUpdate = "ironflock.location_service.update"

// validateTopic checks a topic or procedure URI: non-empty, without leading
// or trailing whitespace. group names the parameters in the error.
func validateTopic(group, topic string) error {
	if strings.TrimSpace(topic) == "" {
		return invalidf("Invalid %s parameters: topic must be a non-empty string", group)
	}
	if strings.TrimSpace(topic) != topic {
		return invalidf("Invalid %s parameters: topic %q cannot have leading/trailing whitespace", group, topic)
	}
	return nil
}

// Subscribe subscribes handler to topic. The subscription is restored after
// every reconnect.
func (f *IronFlock) Subscribe(ctx context.Context, topic string, handler EventHandler, opts ...SubscribeOptions) (*Subscription, error) {
	if err := validateTopic("subscription", topic); err != nil {
		return nil, err
	}
	if handler == nil {
		return nil, invalidf("Invalid subscription parameters: handler must not be nil")
	}
	if _, err := f.gate(ctx, 0); err != nil {
		return nil, operationFailed(fmt.Sprintf("Subscription to topic '%s'", topic), err)
	}
	sub, err := f.conn.Subscribe(ctx, topic, handler, firstSubscribeOptions(opts))
	if err != nil {
		return nil, operationFailed(fmt.Sprintf("Subscription to topic '%s'", topic), err)
	}
	return sub, nil
}

// Unsubscribe removes a subscription as wamp.Connection.Unsubscribe does: at
// once, the handler is called for no further event.
func (f *IronFlock) Unsubscribe(ctx context.Context, sub *Subscription) error {
	if sub == nil {
		return nil
	}
	return operationFailed(fmt.Sprintf("Unsubscribe from topic '%s'", sub.Topic()), f.conn.Unsubscribe(ctx, sub))
}

// Call calls a remote procedure by its full WAMP URI. Positional args are
// sent as WAMP args; a Kwargs value among them as WAMP kwargs, and a
// CallOptions value configures the call.
func (f *IronFlock) Call(ctx context.Context, topic string, args ...any) (*Result, error) {
	if err := validateTopic("call", topic); err != nil {
		return nil, err
	}
	pos, kw, callOpts, err := splitArgs(args)
	if err != nil {
		return nil, invalidParams("call", err)
	}
	res, err := f.call(ctx, topic, pos, kw, callOpts, 0)
	if err != nil {
		return nil, operationFailed(fmt.Sprintf("Call of procedure '%s'", topic), err)
	}
	return res, nil
}

// CallDeviceFunction calls a function another device of this app registered
// with RegisterDeviceFunction: <SWARM_KEY>.<deviceKey>.<APP_KEY>.<STAGE>.<topic>.
// Arguments as for Call.
func (f *IronFlock) CallDeviceFunction(ctx context.Context, deviceKey int, topic string, args ...any) (*Result, error) {
	key := ""
	if deviceKey > 0 {
		key = strconv.Itoa(deviceKey)
	}
	full, err := f.deviceFunctionURI(topic, key, "device_key")
	if err != nil {
		return nil, err
	}
	pos, kw, callOpts, err := splitArgs(args)
	if err != nil {
		return nil, invalidParams("call", err)
	}
	f.log.Debug(fmt.Sprintf("Calling function '%s' on device '%d'. (Full WAMP topic: '%s')", topic, deviceKey, full))
	res, err := f.call(ctx, full, pos, kw, callOpts, 0)
	if err != nil {
		return nil, operationFailed(
			fmt.Sprintf("Call of procedure '%s' on device '%d' (full WAMP topic '%s')", topic, deviceKey, full), err)
	}
	return res, nil
}

// CallFunction is the former name of CallDeviceFunction.
//
// Deprecated: Use CallDeviceFunction.
func (f *IronFlock) CallFunction(ctx context.Context, deviceKey int, topic string, args ...any) (*Result, error) {
	return f.CallDeviceFunction(ctx, deviceKey, topic, args...)
}

// RegisterDeviceFunction registers handler as a function other devices of
// this app (and dashboard widget actions) can call:
// <SWARM_KEY>.<DEVICE_KEY>.<APP_KEY>.<STAGE>.<topic>. The router accepts
// only this shape and single registrations. The registration is restored
// after every reconnect.
//
// What handler returns is sent as the package sends every payload (see
// Kwargs): its result value, the Args and Kwargs of a Result (or *Result) it
// returns, and those of a *WampError it fails with, are converted like
// Publish arguments — a struct by its json tags, a time.Time to an RFC 3339
// string in UTC, map keys to strings, strings to valid UTF-8 — and any other
// error is answered with wamp.error.runtime_error and the error's text. A
// result that cannot be converted (a channel, a function, a complex number)
// is answered with wamp.error.runtime_error naming the reason, and logged as
// an error here.
func (f *IronFlock) RegisterDeviceFunction(ctx context.Context, topic string, handler InvocationHandler, opts ...RegisterOptions) (*Registration, error) {
	full, err := f.deviceFunctionURI(topic, f.deviceKey, "DEVICE_KEY")
	if err != nil {
		return nil, err
	}
	if handler == nil {
		return nil, invalidf("Invalid registration parameters: handler must not be nil")
	}
	var ro *RegisterOptions
	if len(opts) > 0 {
		o := opts[0]
		ro = &o
	}
	op := fmt.Sprintf("Registration of procedure '%s'", full)
	if _, err := f.gate(ctx, 0); err != nil {
		return nil, operationFailed(op, err)
	}
	reg, err := f.conn.Register(ctx, full, f.normalizedHandler(full, handler), ro)
	if err != nil {
		return nil, operationFailed(op, err)
	}
	f.log.Info(fmt.Sprintf("Function registered for IronFlock topic '%s'. (Full WAMP topic: '%s')", topic, full))
	return reg, nil
}

// normalizedHandler wraps handler, the handler of procedure, so that what it
// returns is sent as normalize converts payloads (wamp.Connection.Register
// sends a handler's values as they are; see RegisterDeviceFunction).
func (f *IronFlock) normalizedHandler(procedure string, handler InvocationHandler) InvocationHandler {
	return func(ctx context.Context, inv *Invocation) (any, error) {
		value, err := handler(ctx, inv)
		value, err, unsendable := normalizeReturn(value, err)
		if unsendable != nil {
			msg := fmt.Sprintf("the result of procedure '%s' cannot be sent: %v", procedure, unsendable)
			f.log.Error("Device function failed: " + msg)
			return nil, &WampError{URI: wamp.URIRuntimeError, Args: []any{msg}}
		}
		return value, err
	}
}

// normalizeReturn converts what an InvocationHandler returned to what is
// sent: the result value normalized, a Result as a new *Result with its Args
// and Kwargs normalized, a *WampError found in err (with a URI) as a new one
// with its Args and Kwargs normalized, and any other error as
// wamp.error.runtime_error with the error's text, coerced to valid UTF-8 —
// what wamp.Connection sends for a plain error. unsendable reports a value
// that cannot be converted.
func normalizeReturn(value any, err error) (any, error, error) {
	if err != nil {
		var werr *WampError
		if !errors.As(err, &werr) || werr == nil || werr.URI == "" {
			return nil, &WampError{URI: wamp.URIRuntimeError, Args: []any{jsontext.ValidUTF8(err.Error())}}, nil
		}
		args, kwargs, part, nerr := normalizeArgsKwargs(werr.Args, werr.Kwargs)
		if nerr != nil {
			return nil, nil, fmt.Errorf("the %s of its error %s: %w", part, werr.URI, nerr)
		}
		return nil, &WampError{URI: werr.URI, Args: args, Kwargs: kwargs}, nil
	}
	var res *Result
	switch v := value.(type) {
	case nil:
		return nil, nil, nil
	case Result:
		res = &v
	case *Result:
		if v == nil {
			return nil, nil, nil
		}
		res = v
	default:
		n, nerr := normalize(value)
		return n, nil, nerr
	}
	args, kwargs, part, nerr := normalizeArgsKwargs(res.Args, res.Kwargs)
	if nerr != nil {
		return nil, nil, fmt.Errorf("its %s: %w", part, nerr)
	}
	return &Result{Args: args, Kwargs: kwargs}, nil, nil
}

// normalizeArgsKwargs normalizes a WAMP payload; nil stays nil. When it
// fails, part names the part that failed: "args" or "kwargs".
func normalizeArgsKwargs(args []any, kwargs map[string]any) (outArgs []any, outKwargs map[string]any, part string, err error) {
	if args != nil {
		n, err := normalize(args)
		if err != nil {
			return nil, nil, "args", err
		}
		outArgs = n.([]any)
	}
	if kwargs != nil {
		n, err := normalize(kwargs)
		if err != nil {
			return nil, nil, "kwargs", err
		}
		outKwargs = n.(map[string]any)
	}
	return outArgs, outKwargs, "", nil
}

// RegisterFunction is the former name of RegisterDeviceFunction.
//
// Deprecated: Use RegisterDeviceFunction.
func (f *IronFlock) RegisterFunction(ctx context.Context, topic string, handler InvocationHandler, opts ...RegisterOptions) (*Registration, error) {
	return f.RegisterDeviceFunction(ctx, topic, handler, opts...)
}

// Register is an alias of RegisterDeviceFunction.
func (f *IronFlock) Register(ctx context.Context, topic string, handler InvocationHandler, opts ...RegisterOptions) (*Registration, error) {
	return f.RegisterDeviceFunction(ctx, topic, handler, opts...)
}

// Unregister removes a registration as wamp.Connection.Unregister does: at
// once, the handler is called no more.
func (f *IronFlock) Unregister(ctx context.Context, reg *Registration) error {
	if reg == nil {
		return nil
	}
	return operationFailed(fmt.Sprintf("Unregistration of procedure '%s'", reg.Procedure()), f.conn.Unregister(ctx, reg))
}

// deviceFunctionURI returns the URI of a device function,
// <SWARM_KEY>.<deviceKey>.<APP_KEY>.<DEV|PROD>.<topic>, the stage being the
// stage of the realm the app joins. It refuses to build one with an empty
// name or a missing key segment, which nobody could call: a key is missing
// when it is empty, blank or 0 (keys are database serials starting at 1).
// deviceKeyName names the device key in the error: "DEVICE_KEY" for this
// device's own key, "device_key" for a CallDeviceFunction argument (whose
// error then also wraps ErrInvalidArgument).
func (f *IronFlock) deviceFunctionURI(topic, deviceKey, deviceKeyName string) (string, error) {
	if strings.TrimSpace(topic) == "" {
		return "", invalidf("topic must be a non-empty string")
	}
	if strings.TrimSpace(topic) != topic {
		return "", invalidf("topic %q cannot have leading/trailing whitespace", topic)
	}
	deviceKey = strings.TrimSpace(deviceKey)
	deviceKeyMissing := deviceKey == "" || deviceKey == "0"
	var missing []string
	if f.swarmKey <= 0 {
		missing = append(missing, "SWARM_KEY")
	}
	if deviceKeyMissing {
		missing = append(missing, deviceKeyName)
	}
	if f.appKey <= 0 {
		missing = append(missing, "APP_KEY")
	}
	if len(missing) > 0 {
		err := &sdkError{
			msg: fmt.Sprintf("Cannot address device function '%s': %s not set. "+
				"The IronFlock device agent injects SWARM_KEY, DEVICE_KEY and APP_KEY into every app container; "+
				"set them yourself when running the app elsewhere.", topic, strings.Join(missing, ", ")),
			kinds: []error{ErrMissingConfig},
		}
		if deviceKeyMissing && deviceKeyName == "device_key" {
			err.kinds = append(err.kinds, ErrInvalidArgument)
		}
		return "", err
	}
	return fmt.Sprintf("%d.%s.%d.%s.%s", f.swarmKey, deviceKey, f.appKey, f.stage, topic), nil
}

// SetDeviceLocation asks the platform to update the device's location.
//
// Not served yet: no platform service registers
// ironflock.location_service.update on the app's realm, so it currently
// fails with wamp.error.no_such_procedure. Keep locations in a table of your
// own meanwhile.
func (f *IronFlock) SetDeviceLocation(ctx context.Context, long, lat float64) (*Result, error) {
	// Written as negations so NaN is rejected too.
	if !(long >= -180 && long <= 180) {
		return nil, invalidf("Invalid location parameters: longitude must be between -180 and 180, got %v", long)
	}
	if !(lat >= -90 && lat <= 90) {
		return nil, invalidf("Invalid location parameters: latitude must be between -90 and 90, got %v", lat)
	}
	payload := map[string]any{"long": long, "lat": lat}
	res, err := f.call(ctx, uriLocationUpdate, []any{payload}, f.withDeviceMetadata(nil), nil, 0)
	if err != nil {
		return nil, operationFailed("Device location update", err)
	}
	return res, nil
}
