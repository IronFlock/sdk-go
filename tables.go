package ironflock

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"strings"
	"sync/atomic"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/ironflock/sdk-go/wamp"
)

// maxTableNameLength is the longest table name the SDK accepts.
const maxTableNameLength = 100

// validateTableName checks a table name the app writes to and returns it
// trimmed: non-empty, at most 100 characters, only letters, digits, hyphens
// and underscores (and at least one letter or digit).
func validateTableName(name string) (string, error) {
	t := strings.TrimSpace(name)
	if t == "" {
		return "", invalidf("Table name cannot be empty or just whitespace")
	}
	alnum := false
	for _, r := range t {
		switch {
		case r == '_' || r == '-':
		case unicode.IsLetter(r) || unicode.IsNumber(r):
			alnum = true
		default:
			return "", invalidTableName(name)
		}
	}
	if !alnum {
		return "", invalidTableName(name)
	}
	if utf8.RuneCountInString(t) > maxTableNameLength {
		return "", invalidf("Table name too long (max %d characters)", maxTableNameLength)
	}
	return t, nil
}

func invalidTableName(name string) error {
	return invalidf("Table name should contain only alphanumeric characters, hyphens, and underscores, got %q", name)
}

// mergeKwargs merges and normalizes the optional Kwargs of a bulk operation.
func mergeKwargs(kwargs []Kwargs) (map[string]any, error) {
	args := make([]any, len(kwargs))
	for i, kw := range kwargs {
		args[i] = kw
	}
	_, kw, _, err := splitArgs(args)
	return kw, err
}

// Publish publishes an event to topic. Positional args are sent as WAMP
// args; a Kwargs value among them as WAMP kwargs. The device metadata
// (DEVICE_SERIAL_NUMBER, DEVICE_KEY, DEVICE_NAME) is added to the kwargs,
// user keys winning. The publish is acknowledged: a refusal by the router is
// returned as an error.
func (f *IronFlock) Publish(ctx context.Context, topic string, args ...any) error {
	if err := validateTopic("publish", topic); err != nil {
		return err
	}
	return f.publish(ctx, "publish", topic, args, 0)
}

// publish sends an acknowledged publication with the device metadata merged
// into its kwargs, waiting up to window for a session. group names the
// parameters in validation errors.
func (f *IronFlock) publish(ctx context.Context, group, topic string, args []any, window time.Duration) error {
	pos, kw, callOpts, err := splitArgs(args)
	if err != nil {
		return invalidParams(group, err)
	}
	if callOpts != nil {
		return invalidf("Invalid %s parameters: CallOptions apply to calls, not to publications", group)
	}
	return f.publishMessage(ctx, topic, pos, kw, window)
}

// publishMessage sends an acknowledged publication with the device metadata
// merged into its kwargs, waiting up to window for a session (and, issued
// before Start, for Start).
func (f *IronFlock) publishMessage(ctx context.Context, topic string, args []any, kwargs map[string]any, window time.Duration) error {
	window, err := f.gate(ctx, window)
	if err == nil {
		err = f.conn.Publish(ctx, topic, args, f.withDeviceMetadata(kwargs),
			&wamp.PublishOptions{Acknowledge: true}, window)
	}
	return operationFailed(fmt.Sprintf("Publish to topic '%s'", topic), err)
}

// PublishToTable publishes a row to a fleet table: the table's write topic
// <SWARM_KEY>.<APP_KEY>.<table>. Fire-and-forget: the acknowledgement
// confirms delivery to the router, not the database insert.
//
//	ifl.PublishToTable(ctx, "sensordata", ironflock.Row{"tsp": time.Now(), "temperature": 22.5})
//
// The data backend reads the row from the first positional argument (unless
// the table's data template maps its columns elsewhere), and every table
// has a mandatory tsp column: a row without tsp is dropped, with nothing but
// an entry in the app's error-logs table to show for it. A row the data
// backend refuses for lack of storage is dropped without even that (the
// error-logs table notes only when the appliance's disk runs full).
// AppendToTable reports such refusals.
func (f *IronFlock) PublishToTable(ctx context.Context, table string, args ...any) error {
	t, err := validateTableName(table)
	if err != nil {
		return invalidParams("table", err)
	}
	if err := f.requireKeys(); err != nil {
		return err
	}
	return f.publish(ctx, "table", fmt.Sprintf("%d.%d.%s", f.swarmKey, f.appKey, t), args, f.reconnectWindow)
}

// AppendToTable appends a row to a fleet table by calling its append
// procedure append.<SWARM_KEY>.<APP_KEY>.<table>, and returns the insert
// outcome.
//
// The row needs a tsp, as for PublishToTable: the data backend refuses a row
// without one with wamp.error.runtime_error and no reason ([{}]), and the
// error then says so. Typed refusals: URIStorageFull (temporary: append the
// row later), URIStorageOverusage, URIEntityKeyConflict, and for secret
// columns URISecretSentinelUnresolvable and URISecretCiphertextRejected.
func (f *IronFlock) AppendToTable(ctx context.Context, table string, args ...any) (*Result, error) {
	t, err := validateTableName(table)
	if err != nil {
		return nil, invalidParams("table", err)
	}
	pos, kw, callOpts, err := splitArgs(args)
	if err != nil {
		return nil, invalidParams("table", err)
	}
	if err := f.requireKeys(); err != nil {
		return nil, err
	}
	topic := fmt.Sprintf("append.%d.%d.%s", f.swarmKey, f.appKey, t)
	res, err := f.call(ctx, topic, pos, f.withDeviceMetadata(kw), callOpts, f.reconnectWindow)
	if err != nil {
		return nil, appendFailed(fmt.Sprintf("Append to table '%s'", t), err,
			"the data backend refused the row without a reason — a missing tsp is the usual cause; see the error-logs table")
	}
	return res, nil
}

// appendFailed wraps the failure of an append, as operationFailed does. The
// data backend refuses a row it cannot store because of the row itself — a
// missing tsp, above all — with a plain JavaScript Error, which reaches the
// caller as wamp.error.runtime_error without a reason (args [{}]); such a
// refusal gets note, which says what to check.
func appendFailed(op string, err error, note string) error {
	var werr *wamp.Error
	if errors.As(err, &werr) && werr.URI == wamp.URIRuntimeError && len(werr.Kwargs) == 0 && noReason(werr.Args) {
		return &OperationError{Op: op, Err: err, note: note}
	}
	return operationFailed(op, err)
}

// noReason reports whether args, the payload of an error, says nothing: no
// arguments, or one empty object (what JSON.stringify makes of an Error).
func noReason(args []any) bool {
	switch len(args) {
	case 0:
		return true
	case 1:
		m, ok := args[0].(map[string]any)
		return ok && len(m) == 0
	}
	return false
}

// bulkParams validates the parameters shared by the bulk operations.
func bulkParams(table string, rows any, kwargs []Kwargs) (string, []any, map[string]any, error) {
	t, err := validateTableName(table)
	if err != nil {
		return "", nil, nil, invalidParams("bulk table", err)
	}
	batch, err := normalizeRows(rows)
	if err != nil {
		return "", nil, nil, invalidf("Invalid bulk table parameters: %v", err)
	}
	kw, err := mergeKwargs(kwargs)
	if err != nil {
		return "", nil, nil, invalidParams("bulk table", err)
	}
	return t, batch, kw, nil
}

// PublishRowsToTable publishes many rows in a single message (bulk insert)
// to bulk.<SWARM_KEY>.<APP_KEY>.<table>; the platform inserts the batch
// atomically. rows is a non-empty slice of rows (Row / map[string]any, or
// structs encoded via their json tags). kwargs are shared by the batch.
// Every row needs its own tsp (see PublishToTable): a batch the data backend
// refuses is dropped without a word to the publisher.
//
// Each row is encoded exactly as PublishToTable encodes it: taken as a value,
// so marshal methods (MarshalJSON, MarshalText, ...) with pointer receivers
// are not used; pass a []*T to use them.
func (f *IronFlock) PublishRowsToTable(ctx context.Context, table string, rows any, kwargs ...Kwargs) error {
	t, batch, kw, err := bulkParams(table, rows, kwargs)
	if err != nil {
		return err
	}
	if err := f.requireKeys(); err != nil {
		return err
	}
	// The whole batch is the single positional argument: the data backend
	// reads args[0] as the array of rows.
	topic := fmt.Sprintf("bulk.%d.%d.%s", f.swarmKey, f.appKey, t)
	return f.publishMessage(ctx, topic, []any{batch}, kw, f.reconnectWindow)
}

// AppendRowsToTable appends many rows in a single call (bulk insert) to
// appendBulk.<SWARM_KEY>.<APP_KEY>.<table> and returns the outcome (e.g.
// {"success": true, "count": N}). All-or-nothing: if any row is invalid,
// nothing is persisted. Every row needs its own tsp; the refusals are those
// of AppendToTable. Rows are encoded as PublishRowsToTable encodes them.
func (f *IronFlock) AppendRowsToTable(ctx context.Context, table string, rows any, kwargs ...Kwargs) (*Result, error) {
	t, batch, kw, err := bulkParams(table, rows, kwargs)
	if err != nil {
		return nil, err
	}
	if err := f.requireKeys(); err != nil {
		return nil, err
	}
	topic := fmt.Sprintf("appendBulk.%d.%d.%s", f.swarmKey, f.appKey, t)
	res, err := f.call(ctx, topic, []any{batch}, f.withDeviceMetadata(kw), nil, f.reconnectWindow)
	if err != nil {
		return nil, appendFailed(fmt.Sprintf("Bulk append of %d row(s) to table '%s'", len(batch), t), err,
			"the data backend refused the rows without a reason — a row without tsp is the usual cause; see the error-logs table")
	}
	return res, nil
}

// ErrorLevel is the severity of a reported error.
type ErrorLevel string

// Error levels.
const (
	LevelError ErrorLevel = "error"
	LevelWarn  ErrorLevel = "warn"
	LevelInfo  ErrorLevel = "info"
	LevelDebug ErrorLevel = "debug"
)

// ReportErrorOptions configures ReportError.
type ReportErrorOptions struct {
	// Level defaults to LevelError.
	Level ErrorLevel
	// Append uses the append procedure and returns the insert outcome
	// instead of a fire-and-forget publish.
	Append bool
	// Tsp overrides the timestamp (default: now, RFC 3339 UTC).
	Tsp string
	// UserMessage is the operator-facing text boards show (default: msg).
	UserMessage string
}

// errorTimestampLayout is the default tsp of ReportError: RFC 3339 in UTC
// with milliseconds, as JavaScript's Date.toISOString writes it.
const errorTimestampLayout = "2006-01-02T15:04:05.000Z07:00"

// ReportError writes an application error into the data backend's
// error-logs table, stamped source "app" — queryable with GetHistory and
// streamed on transformed.error-logs, without firing the platform's
// system-error toast. errOrMsg is an error (recorded with fmt's %+v, so
// errors that carry a stack trace include it) or a message string. The
// Result is nil unless opts.Append is set.
//
// The row is {"tsp", "msg", "user_message", "source": "app", "level"}. Only
// the first opts value is used.
func (f *IronFlock) ReportError(ctx context.Context, errOrMsg any, opts ...ReportErrorOptions) (*Result, error) {
	if errOrMsg == nil {
		return nil, invalidf("ReportError needs an error or a message")
	}
	var o ReportErrorOptions
	if len(opts) > 0 {
		o = opts[0]
	}
	msg := errorText(errOrMsg)
	row := Row{
		"tsp":          o.Tsp,
		"msg":          msg,
		"user_message": o.UserMessage,
		"source":       "app",
		"level":        string(o.Level),
	}
	if o.Tsp == "" {
		row["tsp"] = time.Now().UTC().Format(errorTimestampLayout)
	}
	if o.UserMessage == "" {
		row["user_message"] = msg
	}
	if o.Level == "" {
		row["level"] = string(LevelError)
	}
	if o.Append {
		return f.AppendToTable(ctx, ErrorLogsTable, row)
	}
	return nil, f.PublishToTable(ctx, ErrorLogsTable, row)
}

// errorText is the msg column of a reported error.
func errorText(v any) string {
	switch x := v.(type) {
	case error:
		return fmt.Sprintf("%+v", x)
	case string:
		return x
	case fmt.Stringer:
		return x.String()
	default:
		return fmt.Sprint(x)
	}
}

// TableSubscription is the pair of subscriptions behind SubscribeToTable:
// the table's realtime feed and its bulk counterpart.
type TableSubscription struct {
	Rows *Subscription
	Bulk *Subscription
	conn wampConn
	// closed is set by Unsubscribe: no handler call starts after it.
	closed atomic.Bool
}

// Unsubscribe removes both subscriptions. It takes effect at once: the
// handler is called for no further event — neither one still queued for
// either feed nor a remaining row of a bulk event being delivered — though a
// call already under way is not waited for (the handler may call
// Unsubscribe itself). The subscriptions are then removed as
// wamp.Connection.Unsubscribe removes them, within ctx.
func (t *TableSubscription) Unsubscribe(ctx context.Context) error {
	if t == nil || t.conn == nil {
		return nil
	}
	t.closed.Store(true)
	var errs []error
	for _, sub := range []*Subscription{t.Rows, t.Bulk} {
		if sub == nil {
			continue
		}
		if err := t.conn.Unsubscribe(ctx, sub); err != nil {
			errs = append(errs, operationFailed(fmt.Sprintf("Unsubscribe from topic '%s'", sub.Topic()), err))
		}
	}
	return errors.Join(errs...)
}

// SubscribeToTable subscribes handler to the stored rows of a table: the
// data backend's realtime feed transformed.<table> and its bulk counterpart
// transformed.bulk.<table>. Each event carries one row as stored — typed to
// the data-template columns, secret columns masked — in Args[0] (see
// Event.Row); rows of a bulk insert are delivered one event per row.
//
// A transform (a data template's SQL view) has no rows of its own: on every
// tick of its schedule the data backend publishes the whole view — at most
// 3000 rows, in the view's order reversed — as one event whose Args[0] is
// the list of rows. Read it with Event.Rows; Event.Row returns nil for it.
//
// handler is called one event at a time, in the order the events arrive on
// both feeds, as one subscription calls its handler (see EventHandler): it
// never runs concurrently with itself. The two feeds deliver through one
// wamp.DeliveryGroup — opts' Group if it sets one, else a group of their own.
func (f *IronFlock) SubscribeToTable(ctx context.Context, table string, handler EventHandler, opts ...SubscribeOptions) (*TableSubscription, error) {
	if err := validateTopic("subscription", table); err != nil {
		return nil, err
	}
	if err := f.requireKeys(); err != nil {
		return nil, err
	}
	if handler == nil {
		return nil, invalidf("Invalid subscription parameters: handler must not be nil")
	}
	if _, err := f.gate(ctx, 0); err != nil {
		return nil, operationFailed(fmt.Sprintf("Subscription to topic 'transformed.%s'", table), err)
	}
	return subscribeTable(ctx, f.conn, f.log, f.cleanupTimeout, table, handler, opts)
}

// subscribeTable subscribes handler to transformed.<table> and, unrolled to
// one event per row, to transformed.bulk.<table>, both delivering through
// one DeliveryGroup. When the second subscription fails the first is
// removed again.
func subscribeTable(ctx context.Context, conn wampConn, log *slog.Logger, cleanup time.Duration,
	table string, handler EventHandler, opts []SubscribeOptions) (*TableSubscription, error) {
	if handler == nil {
		return nil, invalidf("Invalid subscription parameters: handler must not be nil")
	}
	so := firstSubscribeOptions(opts)
	if so == nil {
		so = &SubscribeOptions{}
	}
	if so.Group == nil {
		so.Group = wamp.NewDeliveryGroup()
	}
	ts := &TableSubscription{conn: conn}
	deliver := func(ev *Event) {
		if !ts.closed.Load() {
			handler(ev)
		}
	}

	rowsTopic := "transformed." + table
	rows, err := conn.Subscribe(ctx, rowsTopic, deliver, so)
	if err != nil {
		return nil, operationFailed(fmt.Sprintf("Subscription to topic '%s'", rowsTopic), err)
	}

	bulkTopic := "transformed.bulk." + table
	bulk, err := conn.Subscribe(ctx, bulkTopic, unrollBulk(deliver, log), so)
	if err != nil {
		cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cleanup)
		defer cancel()
		if uerr := conn.Unsubscribe(cctx, rows); uerr != nil {
			log.Warn(fmt.Sprintf("Failed to remove the subscription to '%s' after the bulk subscription failed: %v", rowsTopic, uerr))
		}
		return nil, operationFailed(fmt.Sprintf("Subscription to topic '%s'", bulkTopic), err)
	}
	ts.Rows, ts.Bulk = rows, bulk
	return ts, nil
}

// unrollBulk wraps handler for a bulk feed, whose events carry the whole
// batch as Args[0]: handler receives one event per row, Args = [row], with the
// batch event's topic, kwargs and details, one after another. A nil batch
// delivers nothing, a value that is not a list is delivered as a single row,
// and nil rows are skipped. A panicking handler is recovered and logged per
// row, so it does not cost the remaining rows of the batch.
func unrollBulk(handler EventHandler, log *slog.Logger) EventHandler {
	deliver := func(ev *Event, row any) {
		defer func() {
			if r := recover(); r != nil {
				log.Error(fmt.Sprintf("Table event handler for '%s' panicked: %v", ev.Topic, r))
			}
		}()
		handler(&Event{
			Topic:   ev.Topic,
			Args:    []any{row},
			Kwargs:  maps.Clone(ev.Kwargs),
			Details: maps.Clone(ev.Details),
		})
	}
	return func(ev *Event) {
		if ev == nil || len(ev.Args) == 0 {
			return
		}
		switch batch := ev.Args[0].(type) {
		case nil:
		case []any:
			for _, row := range batch {
				if row != nil {
					deliver(ev, row)
				}
			}
		default:
			deliver(ev, batch)
		}
	}
}

func firstSubscribeOptions(opts []SubscribeOptions) *SubscribeOptions {
	if len(opts) == 0 {
		return nil
	}
	o := opts[0]
	return &o
}
