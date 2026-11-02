package ironflock

import (
	"context"
	"fmt"
	"math"
)

// refreshProcedurePrefix starts the procedure that refreshes a transform on
// demand, transform.refresh.<transform>, on the app's own data realm. The
// data backend registers it once per transform of the data-template (a table
// has none).
const refreshProcedurePrefix = "transform.refresh."

// refreshProcedure returns the procedure that refreshes transform.
func refreshProcedure(transform string) string { return refreshProcedurePrefix + transform }

// TransformRefreshResult is the outcome of RefreshTransform: one tick of a
// transform, as the data backend runs it on the transform's schedule.
type TransformRefreshResult struct {
	// Success is true when the data backend ran the tick. (A tick it could
	// not run is an error of RefreshTransform instead.)
	Success bool
	// Materialized is true when the transform is a materialized view
	// (materialize: true in the data-template), which the tick refreshes
	// before reading it.
	Materialized bool
	// Refreshed is true when the tick ran REFRESH MATERIALIZED VIEW.
	Refreshed bool
	// Skipped is true when the refresh was skipped because no source table
	// of the view was written since the last refresh: the view is already
	// current. Its rows still go out to the boards.
	Skipped bool
	// Published is true when the view's rows went out on
	// transformed.<transform>. False — a normal outcome — when no board is
	// subscribed to the transform (or the event is too large for the data
	// backend's message budget).
	Published bool
	// Rows is how many rows were published, at most 3000; 0 when Published
	// is false.
	Rows int
}

// RefreshTransform refreshes transform now and pushes its rows to the open
// boards — the tick the data backend runs on the transform's schedule, run
// on demand (transform.refresh.<transform>): a materialized transform gets a
// REFRESH MATERIALIZED VIEW (Skipped is true when no source table was
// written since the last refresh: the view is already current), then the
// whole view — at most 3000 rows — goes out on transformed.<transform> when
// a board is subscribed (Published is false otherwise, a normal outcome;
// Rows is then 0). Call it after a write, a batch import or a change of
// configuration, so boards show the result at once instead of on the next
// tick.
//
// A call during a running refresh waits for the one queued behind it, so
// its writes are in the snapshot; the data backend starts at most one
// refresh a second per transform. Table writes (PublishToTable,
// AppendToTable, AppendRowsToTable and the other table methods) are
// committed before they return, so a refresh right after one is never
// skipped. Only the app's own containers may call it
// (wamp.error.not_authorized otherwise); there is no refresh of a consumed
// app's transforms.
//
// The data backend registers the procedure only for a transform of the
// data-template: on any other name the call fails as not registered, once
// the reconnect window has passed, with an *OperationError whose message
// names the causes — the name is not a transform's tablename (a table has
// no refresh procedure), the app's data backend is not running, or it
// predates on-demand transform refresh. Refusals carry URIRefreshInProgress
// (the view is held by a refresh outside the queue: call again later),
// URIRefreshFailed (the database refused the refresh or the read after it;
// the message carries its reason) or URIStorageFull. A long refresh is
// bounded only by ctx: pass a deadline.
func (f *IronFlock) RefreshTransform(ctx context.Context, transform string) (*TransformRefreshResult, error) {
	if err := validateReadTable(transform); err != nil {
		return nil, err
	}
	op := fmt.Sprintf("refreshTransform('%s')", transform)
	topic := refreshProcedure(transform)
	// One positional empty options object, [{}], as the Python and
	// JavaScript SDKs send it: a nil map would serialize as null.
	res, err := f.call(ctx, topic, []any{map[string]any{}}, nil, nil, f.reconnectWindow)
	if err != nil {
		return nil, historyFailed(op, topic, err, transformRefreshHint)
	}
	return refreshResult(res), nil
}

// refreshResult reads the {success, materialized, refreshed, skipped,
// published, rows} answer of transform.refresh. It fails closed: a flag is
// set only by the boolean true, Rows only by a non-negative number (the
// data backend sends none when nothing was published).
func refreshResult(res *Result) *TransformRefreshResult {
	out := &TransformRefreshResult{}
	m, ok := res.Value().(map[string]any)
	if !ok {
		return out
	}
	out.Success = m["success"] == true
	out.Materialized = m["materialized"] == true
	out.Refreshed = m["refreshed"] == true
	out.Skipped = m["skipped"] == true
	out.Published = m["published"] == true
	switch n := m["rows"].(type) {
	case int64:
		if n >= 0 && n <= math.MaxInt {
			out.Rows = int(n)
		}
	case uint64:
		out.Rows = int(min(n, uint64(math.MaxInt)))
	case float64:
		if n >= 0 && n <= math.MaxInt {
			out.Rows = int(n)
		}
	}
	return out
}
