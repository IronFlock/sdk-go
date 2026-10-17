package ironflock

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"strings"
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
//
// Publish waits up to the reconnect window (see WithReconnectWindow) for a
// connection — issued before Start, for Start as well — so it rides out a
// short outage, and is then sent once, never retried; without a window it
// waits 10s.
func (f *IronFlock) Publish(ctx context.Context, topic string, args ...any) error {
	if err := validateTopic("publish", topic); err != nil {
		return err
	}
	pos, kw, callOpts, err := splitArgs(args)
	if err != nil {
		return invalidParams("publish", err)
	}
	if callOpts != nil {
		return invalidf("Invalid publish parameters: CallOptions apply to calls, not to publications")
	}
	window, err := f.gate(ctx, f.reconnectWindow)
	if err == nil {
		err = f.conn.Publish(ctx, topic, pos, f.withDeviceMetadata(kw),
			&wamp.PublishOptions{Acknowledge: true}, window)
	}
	return operationFailed(fmt.Sprintf("Publish to topic '%s'", topic), err)
}

// PublishToTable writes a row to a fleet table. It is AppendToTable without
// the outcome: it calls the table's append procedure
// append.<SWARM_KEY>.<APP_KEY>.<table>, with the same arguments, so it rides
// out a platform restart (it waits up to the reconnect window for the
// connection and retries while the data backend does not serve the table
// yet) and returns the data backend's refusals — a row without tsp, say —
// instead of losing the row. Its errors begin "Publish to table".
//
//	ifl.PublishToTable(ctx, "sensordata", ironflock.Row{"tsp": time.Now(), "temperature": 22.5})
//
// The data backend reads the row from the first positional argument (unless
// the table's data template maps its columns elsewhere), and every table
// has a mandatory tsp column.
//
// ironflock-go 1.9.1 and earlier published the row to the topic
// <SWARM_KEY>.<APP_KEY>.<table>: a row published while the data backend
// restarted was lost, and a refused one dropped without a word.
func (f *IronFlock) PublishToTable(ctx context.Context, table string, args ...any) error {
	_, err := f.appendRow(ctx, "Publish to", table, args)
	return err
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
	return f.appendRow(ctx, "Append to", table, args)
}

// appendRow is AppendToTable and PublishToTable: verb begins the error
// message ("Append to", "Publish to").
func (f *IronFlock) appendRow(ctx context.Context, verb, table string, args []any) (*Result, error) {
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
		return nil, appendFailed(fmt.Sprintf("%s table '%s'", verb, t), err,
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

// PublishRowsToTable writes many rows in a single message (bulk insert); the
// platform inserts the batch atomically. rows is a non-empty slice of rows
// (Row / map[string]any, or structs encoded via their json tags). kwargs are
// shared by the batch. Every row needs its own tsp (see PublishToTable).
//
// It is AppendRowsToTable without the outcome: it calls the bulk procedure
// appendBulk.<SWARM_KEY>.<APP_KEY>.<table>, rides out a platform restart as
// that does, and returns the data backend's refusals. Its errors begin
// "Bulk publish of <n> row(s) to table". ironflock-go 1.9.1 and earlier
// published the batch to the topic bulk.<SWARM_KEY>.<APP_KEY>.<table>: a
// batch published while the data backend restarted was lost, and a refused
// one dropped without a word.
//
// Each row is encoded exactly as PublishToTable encodes it: taken as a value,
// so marshal methods (MarshalJSON, MarshalText, ...) with pointer receivers
// are not used; pass a []*T to use them.
func (f *IronFlock) PublishRowsToTable(ctx context.Context, table string, rows any, kwargs ...Kwargs) error {
	_, err := f.appendRows(ctx, "Bulk publish", table, rows, kwargs)
	return err
}

// AppendRowsToTable appends many rows in a single call (bulk insert) to
// appendBulk.<SWARM_KEY>.<APP_KEY>.<table> and returns the outcome (e.g.
// {"success": true, "count": N}). All-or-nothing: if any row is invalid,
// nothing is persisted. Every row needs its own tsp; the refusals are those
// of AppendToTable. Rows are encoded as PublishRowsToTable encodes them.
func (f *IronFlock) AppendRowsToTable(ctx context.Context, table string, rows any, kwargs ...Kwargs) (*Result, error) {
	return f.appendRows(ctx, "Bulk append", table, rows, kwargs)
}

// appendRows is AppendRowsToTable and PublishRowsToTable: verb begins the
// error message ("Bulk append", "Bulk publish").
func (f *IronFlock) appendRows(ctx context.Context, verb, table string, rows any, kwargs []Kwargs) (*Result, error) {
	t, batch, kw, err := bulkParams(table, rows, kwargs)
	if err != nil {
		return nil, err
	}
	if err := f.requireKeys(); err != nil {
		return nil, err
	}
	// The whole batch is the single positional argument: the data backend
	// reads args[0] as the array of rows.
	topic := fmt.Sprintf("appendBulk.%d.%d.%s", f.swarmKey, f.appKey, t)
	res, err := f.call(ctx, topic, []any{batch}, f.withDeviceMetadata(kw), nil, f.reconnectWindow)
	if err != nil {
		return nil, appendFailed(fmt.Sprintf("%s of %d row(s) to table '%s'", verb, len(batch), t), err,
			"the data backend refused the rows without a reason — a row without tsp is the usual cause; see the error-logs table")
	}
	return res, nil
}

// ErrorLevel is the severity of a reported error.
type ErrorLevel string

// Error levels: the types the board's toast widget shows.
const (
	LevelError   ErrorLevel = "error"
	LevelWarning ErrorLevel = "warning"
	LevelInfo    ErrorLevel = "info"
	LevelDebug   ErrorLevel = "debug"

	// Deprecated: LevelWarn is the old name of LevelWarning. It, and the raw
	// level "warn" that ironflock-go 1.9.1 and earlier sent, go out as
	// "warning".
	LevelWarn = LevelWarning
)

// ReportErrorOptions configures ReportError.
type ReportErrorOptions struct {
	// Level defaults to LevelError.
	Level ErrorLevel
	// Append is kept for compatibility: the row is appended either way, as
	// PublishToTable and AppendToTable both append. It only picks which of
	// the two the error message names ("Append to table" with it,
	// "Publish to table" without).
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
// errors that carry a stack trace include it) or a message string. It
// appends the row as AppendToTable does — riding out a platform restart for
// up to the reconnect window — and returns the insert outcome, with or
// without opts.Append (ironflock-go 1.9.1 and earlier published the row
// without Append, and returned a nil Result).
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
	switch o.Level {
	case "":
		row["level"] = string(LevelError)
	case "warn":
		row["level"] = string(LevelWarning)
	}
	verb := "Publish to"
	if o.Append {
		verb = "Append to"
	}
	return f.appendRow(ctx, verb, ErrorLogsTable, []any{row})
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

// TableSubscription is the handle SubscribeToTable returns, for a table of
// the app's own and of a consumed app: the pair of subscriptions behind it,
// the table's realtime feed and its bulk counterpart, which deliver to one
// handler. Unsubscribe removes both (as IronFlock.Unsubscribe does when
// given the handle), Table names the table, and Active reports whether both
// feeds are live.
type TableSubscription struct {
	// Rows and Bulk are the subscriptions of transformed.<table> and
	// transformed.bulk.<table>. Remove them only through Unsubscribe, which
	// also ends the catch-up after a reconnect.
	Rows *Subscription
	Bulk *Subscription

	table string
	conn  wampConn
	// follower delivers to the handler; Unsubscribe stops it, and no
	// handler call starts after that.
	follower *tableFollower
}

// Table returns the name of the subscribed table or transform ("" for a nil
// handle).
func (t *TableSubscription) Table() string {
	if t == nil {
		return ""
	}
	return t.table
}

// Active reports whether both feeds are live at the router (see
// Subscription.Active): false while the connection is down, after a failed
// restore of either feed until a retry succeeds, and after Unsubscribe.
func (t *TableSubscription) Active() bool {
	return t != nil && t.Rows.Active() && t.Bulk.Active()
}

// Unsubscribe removes both subscriptions. It takes effect at once: the
// handler is called for no further event — neither one still queued for
// either feed, nor a remaining row of a bulk event being delivered, nor a
// row stored while the connection was down — though a call already under way
// is not waited for (the handler may call Unsubscribe itself). The
// subscriptions are then removed as wamp.Connection.Unsubscribe removes
// them, within ctx, and are never restored after a reconnect.
//
// Unsubscribe does not wait for a connection: while it is down, the
// subscriptions are removed locally and Unsubscribe returns. While
// connected, it returns once the router has answered; a refusal is returned
// as an *OperationError, and the local removal stands. A second call is a
// no-op, as is a call after Stop or Close, and so is a nil handle. Other
// handles on the same table — another SubscribeToTable, or a Subscribe of
// one of its feeds — keep their subscriptions.
func (t *TableSubscription) Unsubscribe(ctx context.Context) error {
	if t == nil || t.conn == nil {
		return nil
	}
	if t.follower != nil {
		t.follower.stop()
	}
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
// Rows stored while the connection was down arrive after the reconnect. The
// router does not replay what was published meanwhile, so after every
// reconnect the handler first gets the rows stored meanwhile, oldest first,
// and then the rows that arrived live since: every row whose tsp is later
// than the newest row the handler got (before the first one, than the newest
// row stored when it subscribed). A row written meanwhile with an older tsp
// is not caught up on. A SubscribeToTable issued while there is no session —
// before the first join, or in a reconnect gap — waits for the session (up
// to the reconnect window, see WithReconnectWindow; 10s without one) and
// subscribes then, not retried: the rows stored before are not caught up on.
// One catch-up delivers at most the newest 10000 rows, and logs a warning
// when it read that many. Its read waits for the data backend like any
// history read, up to the reconnect window; when it fails, those rows are not
// delivered and a warning is logged. A caught-up row's
// event has the topic transformed.<table> and no kwargs or details.
//
// A transform (a data template's SQL view) has no rows of its own: on every
// tick of its schedule the data backend publishes the whole view — at most
// 3000 rows, in the view's order reversed — as one event whose Args[0] is
// the list of rows. Read it with Event.Rows; Event.Row returns nil for it.
// Its next tick after a reconnect delivers the whole view again.
//
// handler is called one event at a time, in the order the events arrive on
// both feeds, as one subscription calls its handler (see EventHandler): it
// never runs concurrently with itself. The two feeds deliver through one
// wamp.DeliveryGroup — opts' Group if it sets one, else a group of their own
// — and so do the rows a catch-up delivers.
//
// The returned TableSubscription removes both feeds with its Unsubscribe, or
// with IronFlock.Unsubscribe; each SubscribeToTable is a handle of its own,
// also on a table subscribed already.
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
	window, err := f.gate(ctx, f.reconnectWindow)
	if err != nil {
		return nil, operationFailed(fmt.Sprintf("Subscription to topic 'transformed.%s'", table), err)
	}
	return subscribeTable(ctx, tableOrigin{conn: f.conn, log: f.log, cleanup: f.cleanupTimeout,
		lifetime: f.lifetime, read: f.GetHistory, wait: window}, table, kindUnknown, handler, opts)
}

// tableOrigin is where a table subscription lives: the connection of the app
// or of a consumed app, and how to read the table there.
type tableOrigin struct {
	conn wampConn
	log  *slog.Logger
	// cleanup bounds removing a half-made subscription.
	cleanup time.Duration
	// lifetime ends when the app stops, or the consumed app closes.
	lifetime context.Context
	read     readFunc
	// wait is how long each step of subscribing waits for a session (0: the
	// connection's default).
	wait time.Duration
}

// subscribeTable subscribes handler, through a tableFollower, to
// transformed.<table> and, unrolled to one event per row, to
// transformed.bulk.<table>, both delivering through one DeliveryGroup. kind
// is what the table is, when the caller knows. When the second subscription
// fails the first is removed again.
func subscribeTable(ctx context.Context, o tableOrigin, table string, kind tableKind,
	handler EventHandler, opts []SubscribeOptions) (*TableSubscription, error) {
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
	lifetime := o.lifetime
	if lifetime == nil {
		lifetime = context.Background()
	}
	rowsTopic := "transformed." + table
	// The catch-up after a reconnect starts at the newest row stored when
	// the subscription is made at the router: one issued while there is no
	// session — before the first join, or in a reconnect gap — waits for the
	// session first, and its follower starts from there. Rows stored before
	// are history, not caught up on.
	if !o.conn.IsOpen() {
		if err := o.conn.WaitSession(ctx, o.wait); err != nil {
			return nil, operationFailed(fmt.Sprintf("Subscription to topic '%s'", rowsTopic), err)
		}
	}
	// The follower holds back live rows after a reconnect until it has
	// delivered the ones stored while the connection was down. It follows
	// the joins from before the subscription exists, so a rejoin while
	// subscribing is caught up on too.
	fl := newTableFollower(lifetime, o.conn, o.log, o.read, table, kind, handler, so.Group)
	fl.attach()

	rows, err := o.conn.SubscribeWithin(ctx, rowsTopic, fl.deliverLive, so, o.wait)
	if err != nil {
		fl.stop()
		return nil, operationFailed(fmt.Sprintf("Subscription to topic '%s'", rowsTopic), err)
	}
	fl.activate()

	bulkTopic := "transformed.bulk." + table
	bulk, err := o.conn.SubscribeWithin(ctx, bulkTopic, unrollBulk(fl.deliverLive, o.log), so, o.wait)
	if err != nil {
		fl.stop()
		cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), o.cleanup)
		defer cancel()
		if uerr := o.conn.Unsubscribe(cctx, rows); uerr != nil {
			o.log.Warn(fmt.Sprintf("Failed to remove the subscription to '%s' after the bulk subscription failed: %v", rowsTopic, uerr))
		}
		return nil, operationFailed(fmt.Sprintf("Subscription to topic '%s'", bulkTopic), err)
	}
	return &TableSubscription{Rows: rows, Bulk: bulk, table: table, conn: o.conn, follower: fl}, nil
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
