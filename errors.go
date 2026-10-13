package ironflock

import (
	"errors"
	"fmt"
	"strings"

	"github.com/ironflock/sdk-go/internal/jsontext"
	"github.com/ironflock/sdk-go/wamp"
)

// ErrInvalidArgument is wrapped by every error about invalid parameters
// (the Python SDK's ValueError): test with errors.Is.
var ErrInvalidArgument = errors.New("ironflock: invalid argument")

// ErrMissingConfig is wrapped by every error about identity an operation
// needs but the app was not given: no device serial number, or SWARM_KEY,
// APP_KEY or a device key unset (or 0). The device agent injects all of them
// into every app container; outside one, set them yourself (environment or
// the With… options). Test with errors.Is.
var ErrMissingConfig = errors.New("ironflock: missing configuration")

// ErrAlreadyStarted is returned by Start, and by Run, on an IronFlock that is
// started already or whose Start is still running.
var ErrAlreadyStarted = errors.New("ironflock: Start called while already started")

// sdkError is an error the SDK raises itself, before any router traffic: a
// message classified by one or more of the sentinel errors above.
type sdkError struct {
	msg   string
	kinds []error
}

// Error implements error: "<first sentinel>: <msg>".
func (e *sdkError) Error() string { return e.kinds[0].Error() + ": " + e.msg }

// Unwrap returns the sentinel errors classifying e, for errors.Is.
func (e *sdkError) Unwrap() []error { return e.kinds }

// invalidf returns an error wrapping ErrInvalidArgument.
func invalidf(format string, args ...any) error {
	return &sdkError{msg: fmt.Sprintf(format, args...), kinds: []error{ErrInvalidArgument}}
}

// missingConfigf returns an error wrapping ErrMissingConfig.
func missingConfigf(format string, args ...any) error {
	return &sdkError{msg: fmt.Sprintf(format, args...), kinds: []error{ErrMissingConfig}}
}

// invalidParams prefixes a validation error with the parameter group it is
// about ("Invalid query parameters: …"), as the Python and JavaScript SDKs
// word them. Other errors are returned unchanged.
func invalidParams(group string, err error) error {
	var serr *sdkError
	if errors.As(err, &serr) && errors.Is(serr, ErrInvalidArgument) {
		return &sdkError{msg: fmt.Sprintf("Invalid %s parameters: %s", group, serr.msg), kinds: serr.kinds}
	}
	return err
}

// isClientError reports whether err is an error the SDK raised itself
// (invalid argument or missing configuration). Such errors are returned as
// they are rather than wrapped in an OperationError.
func isClientError(err error) bool {
	return errors.Is(err, ErrInvalidArgument) || errors.Is(err, ErrMissingConfig)
}

// OperationError reports a failed SDK operation. Op names the operation and
// its target (e.g. "Publish to topic 'x'"); Err is the cause — a
// *wamp.Error (WampError) when the router or callee refused, which
// errors.As finds through it.
type OperationError struct {
	Op  string
	Err error
	// hint replaces the generic cause in the message when set.
	hint string
	// note follows the message when set.
	note string
}

// Error implements error:
//
//	<Op> failed with WAMP error '<uri>'[ — <json args>][: <note>]
//	<Op> failed: <hint or cause>
//
// The args are compact JSON as JavaScript's JSON.stringify writes it,
// without escaping <, > and & (args JSON cannot encode, such as NaN, are
// printed as Go values).
func (e *OperationError) Error() string {
	if e.hint != "" {
		return fmt.Sprintf("%s failed: %s", e.Op, e.hint)
	}
	var msg string
	var werr *wamp.Error
	if errors.As(e.Err, &werr) {
		detail := ""
		if len(werr.Args) > 0 {
			detail = " — " + jsontext.Compact(werr.Args)
		}
		msg = fmt.Sprintf("%s failed with WAMP error '%s'%s", e.Op, werr.URI, detail)
	} else {
		msg = fmt.Sprintf("%s failed: %v", e.Op, e.Err)
	}
	if e.note != "" {
		msg += ": " + e.note
	}
	return msg
}

// Unwrap returns the cause.
func (e *OperationError) Unwrap() error { return e.Err }

// WampURI returns the WAMP error URI behind err, or "" when err is not a
// router or callee refusal.
func WampURI(err error) string {
	var werr *wamp.Error
	if errors.As(err, &werr) {
		return werr.URI
	}
	return ""
}

// Refusals of the data backend (fleetdb). An operation it refuses fails with
// an *OperationError around a *WampError carrying one of these URIs (and the
// reason as its first argument): branch on WampURI(err). The SDK retries
// none of them.
const (
	// URIRateLimited: RevealSecrets beyond 30, or VerifySecret beyond 120,
	// calls a minute, counted per app credential on a device (and per data
	// backend process) in a fixed one-minute window. Reveal a secret once
	// and keep it rather than on every use; cache the outcome of a
	// per-request VerifySecret briefly.
	URIRateLimited = "sys.dataservice.error.rate_limited"
	// URIResultTooLarge: a read whose result cannot be transported: a single
	// row over the data backend's 8 MiB message budget, or a result over its
	// runaway guard (1 GiB). (Larger results arrive in chunks, which the
	// read methods reassemble.) Its Kwargs carry bytes, rows, limitBytes and
	// topColumns: lower Limit, narrow TimeRange, select fewer Columns, or
	// prune json columns with ColumnPaths.
	URIResultTooLarge = "sys.dataservice.error.result_too_large"
	// URIInvalidLimit: a read of a transform with a Limit over 3000.
	URIInvalidLimit = "sys.dataservice.error.invalid_limit"
	// URIInvalidTimeRange: a series query whose time range has no start, or
	// an end that is not after its start.
	URIInvalidTimeRange = "sys.dataservice.error.invalid_time_range"
	// URIInvalidMetric: a series metric the table cannot aggregate: a ref
	// that is no column of the table (or a path into a column that is not
	// json), a secret column, a method the column's type does not take, tsp
	// with another method than COUNT, or a column name over 63 bytes.
	URIInvalidMetric = "sys.dataservice.error.invalid_metric"
	// URIInvalidGroupBy: a series GroupBy column the table cannot group by:
	// no column of the table, a secret column, tsp, a column that is also a
	// metric, or a name over 63 bytes.
	URIInvalidGroupBy = "sys.dataservice.error.invalid_group_by"
	// URISeriesTooManyGroups: a series result over 50,000 rows (buckets times
	// groups): fewer buckets, a narrower time range, or a GroupBy of fewer
	// distinct values.
	URISeriesTooManyGroups = "sys.dataservice.error.series_too_many_groups"
	// URISecretColumn: a read (GetHistory, GetSeriesHistory, RevealSecrets,
	// VerifySecret) that filters on a secret column, whose stored values no
	// predicate can match. Use VerifySecret to test a value.
	URISecretColumn = "sys.dataservice.error.secret_column"
	// URINotASecretColumn: VerifySecret with a column that is not a secret
	// column, or RevealSecrets of a table that has no secret column.
	URINotASecretColumn = "sys.dataservice.error.not_a_secret_column"
	// URIStorageFull: an append refused because the appliance's disk is
	// nearly full (below about 3 GiB free; writes are accepted again from
	// 4 GiB). Temporary, nothing stored is lost: hold the rows and append
	// them later. A published row refused for this is dropped without a
	// word to the publisher; only appends report it.
	URIStorageFull = "sys.dataservice.error.storage_full"
	// URIStorageOverusage: an append refused because the account's storage
	// allowance is used up; retrying soon does not help. Published rows are
	// dropped likewise without a word.
	URIStorageOverusage = "sys.dataservice.error.storage_overusage"
	// URIEntityKeyConflict: a write of a row whose entity key
	// (maintainLatestFlagFor) and tsp another row has already — in the same
	// batch (one stamped with one tsp, say) or stored before. Give every row
	// its own tsp.
	URIEntityKeyConflict = "sys.dataservice.error.entity_key_conflict"
	// URISecretSentinelUnresolvable: a write that keeps a secret column's
	// previous value (it sends SecretPlaceholder) to a table without an entity
	// key to find that value by, or without that key in the row.
	URISecretSentinelUnresolvable = "sys.dataservice.error.secret_sentinel_unresolvable"
	// URISecretCiphertextRejected: a write of a value that is already
	// encrypted (read back raw, say) to a secret column: send the plaintext.
	URISecretCiphertextRejected = "sys.dataservice.error.secret_ciphertext_rejected"
)

// operationFailed wraps err for operation op, leaving client-side errors
// (invalid argument, missing configuration) and cross-app errors as they are.
func operationFailed(op string, err error) error {
	if err == nil {
		return nil
	}
	if isClientError(err) {
		return err
	}
	var cerr *CrossAppAccessError
	if errors.As(err, &cerr) {
		return err
	}
	return &OperationError{Op: op, Err: err}
}

// historyHint is the explanation for the most common history failure: the
// procedure is not registered because the table is not declared in the
// data-template or the app's data backend is not running.
const historyHint = "Check that the table is declared in the app's data-template and that the app's data backend is running."

// secretRPCHint replaces historyHint for the secret-column procedures.
const secretRPCHint = "The app's data backend does not support secret columns yet — update it, then declare the column with `secret: true` in the app's data-template."

// historyFailed wraps a failed history-style RPC, spelling out the
// not-registered case. secret selects the secret-column hint.
func historyFailed(op, topic string, err error, secret bool) error {
	if err == nil {
		return nil
	}
	if isClientError(err) {
		return err
	}
	text := err.Error()
	notRegistered := strings.Contains(WampURI(err), "no_such_procedure") ||
		strings.Contains(text, "no_such_procedure") ||
		strings.Contains(text, "no callee registered")
	if notRegistered {
		if secret {
			return &OperationError{Op: op, Err: err,
				hint: fmt.Sprintf("procedure '%s' is not registered. %s", topic, secretRPCHint)}
		}
		return &OperationError{Op: op, Err: err,
			hint: fmt.Sprintf("history procedure '%s' is not registered. %s", topic, historyHint)}
	}
	return &OperationError{Op: op, Err: err}
}

// Cross-app access error codes (CrossAppAccessError.Code).
const (
	CodeNoGrant              = "NO_GRANT"
	CodeProviderNotInstalled = "PROVIDER_NOT_INSTALLED"
	CodeUnknownApp           = "UNKNOWN_APP"
	CodePrivateTable         = "PRIVATE_TABLE"
	CodeSecretColumn         = "SECRET_COLUMN"
	CodeNotAuthorized        = "NOT_AUTHORIZED"
)

// CrossAppAccessError reports that reading another app's data backend was
// declined or misused. Branch on Code.
type CrossAppAccessError struct {
	// Code is one of NO_GRANT, PROVIDER_NOT_INSTALLED, UNKNOWN_APP,
	// PRIVATE_TABLE, SECRET_COLUMN or NOT_AUTHORIZED.
	Code    string
	Message string
	// Err is the underlying cause, if any.
	Err error
}

func (e *CrossAppAccessError) Error() string {
	if e.Message == "" {
		return e.Code
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

// Unwrap returns the underlying cause.
func (e *CrossAppAccessError) Unwrap() error { return e.Err }

// crossAppCodes maps WAMP error URIs to cross-app access codes.
var crossAppCodes = map[string]string{
	"sys.appaccess.error.no_grant":               CodeNoGrant,
	"sys.appaccess.error.provider_not_installed": CodeProviderNotInstalled,
	"sys.appaccess.error.unknown_app":            CodeUnknownApp,
	wamp.URINotAuthorized:                        CodeNotAuthorized,
	wamp.URIAuthorizationFailed:                  CodeNotAuthorized,
	wamp.URIAuthenticationFailed:                 CodeNotAuthorized,
}

// mapCrossAppError maps a WAMP refusal to a *CrossAppAccessError, or returns
// nil when the URI is not a cross-app access condition. The message is the
// URI followed by ": <json of the first error argument>", if any, in compact
// JSON without HTML escaping, as the Python and JavaScript SDKs write it.
func mapCrossAppError(err error) *CrossAppAccessError {
	var werr *wamp.Error
	if !errors.As(err, &werr) {
		return nil
	}
	code, ok := crossAppCodes[werr.URI]
	if !ok {
		return nil
	}
	msg := werr.URI
	if len(werr.Args) > 0 {
		msg += ": " + jsontext.Compact(werr.Args[0])
	}
	return &CrossAppAccessError{Code: code, Message: msg, Err: err}
}
