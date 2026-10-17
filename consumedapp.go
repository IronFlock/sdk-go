package ironflock

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/ironflock/sdk-go/wamp"
)

// Cross-app procedures on the app's own realm.
const (
	uriAppAccessResolve = "sys.appaccess.resolve"
	uriAppAccessList    = "sys.appaccess.list"
)

// TableInfo is a table or transform a provider app shares.
type TableInfo struct {
	Tablename   string `json:"tablename"`
	Description string `json:"description,omitempty"`
	// Columns are the column definitions as declared in the provider's
	// data template (e.g. {"id": "temperature", "dataType": "numeric"}).
	Columns []map[string]any `json:"columns,omitempty"`
}

// StageCatalog is what a provider shares on one stage.
type StageCatalog struct {
	Tables     []TableInfo `json:"tables"`
	Transforms []TableInfo `json:"transforms"`
}

// ConsumedAppInfo describes a provider app (the result of
// sys.appaccess.resolve and the elements of ListConsumableApps). A stage is
// present only when the provider has a data backend for it.
type ConsumedAppInfo struct {
	App            string `json:"app"`
	ProviderAppKey int    `json:"provider_app_key"`
	Stages         struct {
		Dev  *StageCatalog `json:"dev,omitempty"`
		Prod *StageCatalog `json:"prod,omitempty"`
	} `json:"stages"`
}

// Catalog returns the catalog of stage ("dev" or "prod"), or nil.
func (i *ConsumedAppInfo) Catalog(stage string) *StageCatalog {
	switch stage {
	case "dev":
		return i.Stages.Dev
	case "prod":
		return i.Stages.Prod
	}
	return nil
}

// ConnectToAppOptions configures ConnectToApp.
type ConnectToAppOptions struct {
	// Stage is the provider stage, "dev" or "prod" in any case (default:
	// this app's own stage, IronFlock.Stage().Lower()).
	Stage string
	// OnError is called (on its own goroutine) when the connection is
	// denied for good AFTER ConnectToApp returned — e.g. the grant was
	// revoked, and the reconnects are refused. A refusal counts as for good
	// only once it persists: 3 refusals or more since the connection was
	// last up — the router closing the session for an auth reason counts as
	// one, as does each refused reconnect — the first one at least 60s ago
	// (see wamp.Config.FailOnAuthError), because the platform refuses the
	// same way while it cannot verify access for a moment;
	// until then the connection keeps reconnecting, and the handle recovers
	// if a reconnect succeeds. The handle is then evicted (the next
	// ConnectToApp opens a fresh connection) and its connection released.
	// Before ConnectToApp returned, a denial is returned by ConnectToApp at
	// once.
	//
	// OnError is bound to the cached handle when it is created: a call that
	// returns an existing handle, or shares an open already in flight, does
	// not register its own OnError. A panic in OnError is recovered and
	// logged.
	OnError func(err *CrossAppAccessError)
}

// ConnectToAllAppsOptions configures ConnectToAllApps.
type ConnectToAllAppsOptions struct {
	// Stage is the provider stage, "dev" or "prod" in any case (default:
	// this app's own stage, IronFlock.Stage().Lower()).
	Stage string
	// OnError is called with the failure of each provider that could not be
	// opened (unless StopOnError is set), and with a *CrossAppAccessError
	// when an opened provider connection is later denied for good (when a
	// denial counts as for good: see ConnectToAppOptions.OnError).
	//
	// Open failures are reported on the calling goroutine, in the order of
	// the provider list, before ConnectToAllApps returns. Like
	// ConnectToAppOptions.OnError, it is bound only to handles this call
	// creates, and a panic in it is recovered and logged.
	OnError func(err error)
	// StopOnError makes the first provider that fails to open fail the
	// whole call. By default such a provider is reported to OnError and
	// left out of the result.
	StopOnError bool
}

// consumedEntry is one cached consumed-app connection (or attempt). done is
// closed when the attempt finishes; app and err are valid afterwards.
type consumedEntry struct {
	done chan struct{}
	app  *ConsumedApp
	err  error
}

// wait returns the outcome of the attempt, or ctx.Err() when ctx is done
// first. Giving up waiting does not cancel the attempt, which other callers
// may share.
func (e *consumedEntry) wait(ctx context.Context) (*ConsumedApp, error) {
	select {
	case <-e.done:
		return e.app, e.err
	default:
	}
	select {
	case <-e.done:
		return e.app, e.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// ConsumedApp is a read-only handle on another app's data backend, in the
// same project and fleet. It wraps a dedicated connection to the provider's
// realm, where the router allows the per-app credential only reading shared
// tables and transforms (see ConnectToApp for the legacy credential).
type ConsumedApp struct {
	// App is the provider app's name.
	App string
	// Stage is the provider stage this handle is connected to, in lower
	// case: "dev" or "prod". (IronFlock.Stage returns a Stage, "DEV" or
	// "PROD"; Stage.Lower converts it.)
	Stage string
	// Tables and Transforms are what the provider shares on Stage.
	Tables     []TableInfo
	Transforms []TableInfo

	conn            wampConn
	reconnectWindow time.Duration
	onClosed        func()
	closeOnce       sync.Once
	log             *slog.Logger
	cleanupTimeout  time.Duration
	// lifetime ends when the handle is closed or denied for good, and when
	// the IronFlock stops: its table subscriptions stop catching up then.
	lifetime    context.Context
	endLifetime context.CancelFunc
}

// Connection returns the underlying connection to the provider's realm.
func (a *ConsumedApp) Connection() *wamp.Connection {
	c, _ := a.conn.(*wamp.Connection)
	return c
}

// IsConnected reports whether the connection to the provider is open.
func (a *ConsumedApp) IsConnected() bool { return a.conn.IsOpen() }

// assertInCatalog fails with PRIVATE_TABLE for a name outside the shared
// catalog. A private table is not an error at the router — its publications
// are just filtered out — so without this check a caller would silently
// receive nothing.
func (a *ConsumedApp) assertInCatalog(table string) error {
	if strings.TrimSpace(table) == "" {
		return invalidf("Tablename must not be empty!")
	}
	all := slices.Concat(a.Tables, a.Transforms)
	if slices.ContainsFunc(all, func(t TableInfo) bool { return t.Tablename == table }) {
		return nil
	}
	return &CrossAppAccessError{
		Code: CodePrivateTable,
		Message: fmt.Sprintf("'%s' is not shared by app '%s' (%s). Available tables/transforms: %s",
			table, a.App, a.Stage, availableNames(all)),
	}
}

// availableNames lists the names of tables, or "none".
func availableNames(tables []TableInfo) string {
	var names []string
	for _, t := range tables {
		if t.Tablename != "" {
			names = append(names, t.Tablename)
		}
	}
	if len(names) == 0 {
		return "none"
	}
	return strings.Join(names, ", ")
}

// secretColumns returns the ids of the columns the provider's catalog marks
// secret: true on table (the first catalog entry of that name).
func (a *ConsumedApp) secretColumns(table string) map[string]bool {
	for _, t := range slices.Concat(a.Tables, a.Transforms) {
		if t.Tablename != table {
			continue
		}
		secret := make(map[string]bool)
		for _, col := range t.Columns {
			if id, ok := col["id"].(string); ok && col["secret"] == true {
				secret[id] = true
			}
		}
		return secret
	}
	return nil
}

// assertNoSecretColumnUse fails with SECRET_COLUMN when q filters on, or
// selects, a column the provider marks secret. Secret values are stored
// encrypted with a random IV, so no predicate over one can ever match, and
// only the owning app can decrypt them, so selecting one returns the
// placeholder: both would quietly return a wrong answer.
func (a *ConsumedApp) assertNoSecretColumnUse(table string, q *TableQueryParams) error {
	secret := a.secretColumns(table)
	if len(secret) == 0 {
		return nil
	}
	filtered := make(map[string]bool)
	collectFilterColumns(q.FilterAnd, secret, filtered)
	if cols := sortedKeys(filtered); len(cols) > 0 {
		return &CrossAppAccessError{
			Code: CodeSecretColumn,
			Message: fmt.Sprintf("Cannot filter on secret column(s) %s of '%s' (app '%s'): the stored values are "+
				"encrypted with a random IV, so no comparison can ever match. Filter by the columns that "+
				"identify the row instead.", strings.Join(cols, ", "), table, a.App),
		}
	}
	selected := make(map[string]bool)
	for _, c := range q.Columns {
		if secret[c] {
			selected[c] = true
		}
	}
	if cols := sortedKeys(selected); len(cols) > 0 {
		return &CrossAppAccessError{
			Code: CodeSecretColumn,
			Message: fmt.Sprintf("Cannot read secret column(s) %s of '%s' (app '%s'): secret values are readable "+
				"only by the owning app's own containers, so a cross-app read returns the '%s' placeholder. "+
				"Drop them from 'columns'.", strings.Join(cols, ", "), table, a.App, SecretPlaceholder),
		}
	}
	return nil
}

// collectFilterColumns adds to out every column of secret that filters
// predicate on, at any depth of filter groups. It mirrors how filterWire
// reads a Filter: the latest marker first, then a group, then a predicate.
func collectFilterColumns(filters []Filter, secret, out map[string]bool) {
	for _, f := range filters {
		switch {
		case f.Latest:
		case f.Combinator != "" || f.Filters != nil:
			collectFilterColumns(f.Filters, secret, out)
		case secret[f.Column]:
			out[f.Column] = true
		}
	}
}

func sortedKeys(m map[string]bool) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}

// callFailed maps a failed call on the provider's realm to a
// *CrossAppAccessError when it is an access condition, and wraps it
// otherwise.
func callFailed(op string, err error) error {
	if cerr := mapCrossAppError(err); cerr != nil {
		return cerr
	}
	return operationFailed(op, err)
}

// GetHistory reads rows of a shared table or transform (the provider's
// history.transformed.<table>) as IronFlock.GetHistory does — a large result
// reassembled from chunks, a transform with the semantics TableQueryParams
// describes (Limit at most 3000; TimeRange, Columns, ColumnPaths and the
// Latest marker ignored). A nil q reads the 10 newest rows of a table.
//
// Errors: *CrossAppAccessError with PRIVATE_TABLE for a name outside the
// shared catalog, SECRET_COLUMN when q filters on or selects a column the
// provider marks secret, NOT_AUTHORIZED when the provider denies access; the
// data backend's refusals as for IronFlock.GetHistory.
func (a *ConsumedApp) GetHistory(ctx context.Context, table string, q *TableQueryParams) ([]Row, error) {
	if err := a.assertInCatalog(table); err != nil {
		return nil, err
	}
	if q == nil {
		q = &TableQueryParams{Limit: 10}
	}
	wire, err := queryWire(q, MaxQueryLimit, a.log)
	if err != nil {
		return nil, invalidParams("query", err)
	}
	if err := a.assertNoSecretColumnUse(table, q); err != nil {
		return nil, err
	}
	op := fmt.Sprintf("getHistory('%s') on app '%s' (%s)", table, a.App, a.Stage)
	read := new(chunkedRead)
	res, err := a.conn.Call(ctx, "history.transformed."+table, []any{wire}, nil, read.options(), a.reconnectWindow)
	if err != nil {
		return nil, callFailed(op, err)
	}
	return read.rows(op, res)
}

// GetSeriesHistory reads down-sampled series of a shared table (tables
// only: there is no series procedure for transforms), with the query and
// the rows of IronFlock.GetSeriesHistory.
func (a *ConsumedApp) GetSeriesHistory(ctx context.Context, table string, q SeriesQueryParams) ([]Row, error) {
	if strings.TrimSpace(table) == "" {
		return nil, invalidf("Tablename must not be empty!")
	}
	if !slices.ContainsFunc(a.Tables, func(t TableInfo) bool { return t.Tablename == table }) {
		// A transform passes the catalog guard but has no series procedure:
		// point at GetHistory instead of a raw no_such_procedure.
		if slices.ContainsFunc(a.Transforms, func(t TableInfo) bool { return t.Tablename == table }) {
			return nil, &CrossAppAccessError{
				Code: CodePrivateTable,
				Message: fmt.Sprintf("'%s' is a transform of app '%s' (%s); series history is available for "+
					"tables only — use GetHistory instead.", table, a.App, a.Stage),
			}
		}
		return nil, &CrossAppAccessError{
			Code: CodePrivateTable,
			Message: fmt.Sprintf("'%s' is not a shared table of app '%s' (%s). Available tables: %s",
				table, a.App, a.Stage, availableNames(a.Tables)),
		}
	}
	wire, err := seriesWire(&q, a.log)
	if err != nil {
		return nil, invalidParams("series query", err)
	}
	op := fmt.Sprintf("getSeriesHistory('%s') on app '%s' (%s)", table, a.App, a.Stage)
	read := new(chunkedRead)
	res, err := a.conn.Call(ctx, "history.transformed.series."+table, []any{wire}, nil, read.options(), a.reconnectWindow)
	if err != nil {
		return nil, callFailed(op, err)
	}
	return read.rows(op, res)
}

// SubscribeToTable subscribes handler to realtime rows of a shared table or
// transform, exactly like IronFlock.SubscribeToTable — a transform's events
// carry the whole view (see Event.Rows).
//
// A table's rows stored while the connection was down arrive after the
// reconnect: the handler first gets them, oldest first, as with
// IronFlock.SubscribeToTable. It waits for a session as that does, up to the
// reconnect window, and is not retried.
//
// The returned TableSubscription is the same handle as the app's own:
// its Unsubscribe, or IronFlock.Unsubscribe, removes both feeds; Close
// ends every subscription of the consumed app.
//
// Errors: *CrossAppAccessError with PRIVATE_TABLE for a name outside the
// shared catalog, and NOT_AUTHORIZED when the provider's realm refuses the
// subscription (wamp.error.not_authorized, authorization_failed or
// authentication_failed), as for the reads.
func (a *ConsumedApp) SubscribeToTable(ctx context.Context, table string, handler EventHandler, opts ...SubscribeOptions) (*TableSubscription, error) {
	if err := a.assertInCatalog(table); err != nil {
		return nil, err
	}
	kind := kindTable
	if slices.ContainsFunc(a.Transforms, func(t TableInfo) bool { return t.Tablename == table }) {
		kind = kindTransform
	}
	ts, err := subscribeTable(ctx, tableOrigin{conn: a.conn, log: a.log, cleanup: a.cleanupTimeout,
		lifetime: a.lifetime, read: a.GetHistory, wait: a.reconnectWindow}, table, kind, handler, opts)
	if cerr := mapCrossAppError(err); cerr != nil {
		return nil, cerr
	}
	return ts, err
}

// Close closes the connection to the provider. IronFlock.Stop closes all
// consumed apps as well.
//
// Close first drops the handle from the cache, so a ConnectToApp from then
// on opens a fresh connection (one that ran before Close may still have
// returned this handle). It is safe to call more than once.
func (a *ConsumedApp) Close(ctx context.Context) error {
	if err := a.close(ctx); err != nil {
		return &OperationError{Op: fmt.Sprintf("Close of the connection to app '%s' (%s)", a.App, a.Stage), Err: err}
	}
	return nil
}

// close drops the handle from the cache and then stops the connection: a
// ConnectToApp that comes in while the connection is still saying goodbye
// opens a fresh one instead of getting this one.
func (a *ConsumedApp) close(ctx context.Context) error {
	a.closeOnce.Do(func() {
		if a.onClosed != nil {
			a.onClosed()
		}
		if a.endLifetime != nil {
			a.endLifetime()
		}
	})
	return a.conn.Stop(ctx)
}

// consumedStage resolves a cross-app stage option: empty selects the app's
// own stage; otherwise "dev" or "prod" in any case.
func (f *IronFlock) consumedStage(stage string) (string, error) {
	s := strings.ToLower(strings.TrimSpace(stage))
	if s == "" {
		s = f.stage.Lower()
	}
	if s != "dev" && s != "prod" {
		return "", invalidf("stage must be 'dev' or 'prod'")
	}
	return s, nil
}

// ConnectToApp opens a read-only connection to another app's data backend in
// the same project and returns a handle on it. This app must declare the
// provider in its own data-template consumes: section (or hold the wildcard
// consumes: [{app: "*"}]), and the project user must have granted access.
// Handles are cached per app and stage: a second call returns the same
// handle, and concurrent calls share one attempt.
//
// The connection needs the per-app credential the device agent injects
// (APP_AUTH_ID and APP_AUTH_SECRET), or one passed with WithCredentials. With
// the legacy device credential (the serial-number fallback, e.g. under an
// older agent) the platform refuses the provider's realm (NOT_AUTHORIZED,
// whose message then says so) — or, when the provider app runs on this same
// device, admits it with that app's own full rights rather than read-only.
//
// Errors: *CrossAppAccessError with NO_GRANT, PROVIDER_NOT_INSTALLED,
// UNKNOWN_APP or NOT_AUTHORIZED.
//
// The attempt runs independently of ctx, which bounds only this caller's
// wait: a caller that gives up does not fail the attempt for others sharing
// it, and a completed attempt is cached for the next call. Stop aborts
// attempts in flight; they fail with an error wrapping wamp.ErrStopped.
func (f *IronFlock) ConnectToApp(ctx context.Context, appName string, opts ...ConnectToAppOptions) (*ConsumedApp, error) {
	if strings.TrimSpace(appName) == "" {
		return nil, invalidf("appName must not be empty!")
	}
	var o ConnectToAppOptions
	if len(opts) > 0 {
		o = opts[0]
	}
	stage, err := f.consumedStage(o.Stage)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	entry, err := f.cachedOpen(appName, stage, func(ctx context.Context, evict func()) (*ConsumedApp, error) {
		return f.openConsumedApp(ctx, appName, stage, evict, o.OnError)
	})
	if err != nil {
		return nil, err
	}
	return entry.wait(ctx)
}

// ListConsumableApps lists every non-private provider of this project, with
// its per-stage catalog, without opening connections (for wildcard
// consumers: consumes: [{app: "*"}]).
func (f *IronFlock) ListConsumableApps(ctx context.Context) ([]ConsumedAppInfo, error) {
	// The platform derives the consumer from the realm the call arrives on
	// and ignores the arguments.
	res, err := f.call(ctx, uriAppAccessList, []any{}, nil, nil, 0)
	if err != nil {
		return nil, callFailed(fmt.Sprintf("Call of procedure '%s'", uriAppAccessList), err)
	}
	infos := []ConsumedAppInfo{}
	switch v := res.Value().(type) {
	case nil:
	case []any:
		for i, e := range v {
			m, ok := e.(map[string]any)
			if !ok {
				f.log.Warn(fmt.Sprintf("Skipping entry %d of %s: not an object (%T)", i, uriAppAccessList, e))
				continue
			}
			info, err := decodeAppInfo(m)
			if err != nil {
				f.log.Warn(fmt.Sprintf("Skipping entry %d of %s: %v", i, uriAppAccessList, err))
				continue
			}
			infos = append(infos, *info)
		}
	default:
		return nil, &OperationError{
			Op:  fmt.Sprintf("Call of procedure '%s'", uriAppAccessList),
			Err: fmt.Errorf("%w %T, want a list of apps", errUnexpectedResult, v),
		}
	}
	return infos, nil
}

// decodeAppInfo decodes a provider info object. An empty stage catalog
// object counts as no data backend for that stage.
func decodeAppInfo(m map[string]any) (*ConsumedAppInfo, error) {
	info, err := Decode[ConsumedAppInfo](m)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", errUnexpectedResult, err)
	}
	stages, _ := m["stages"].(map[string]any)
	if c, _ := stages["dev"].(map[string]any); len(c) == 0 {
		info.Stages.Dev = nil
	}
	if c, _ := stages["prod"].(map[string]any); len(c) == 0 {
		info.Stages.Prod = nil
	}
	return &info, nil
}

// ConnectToAllApps opens read-only connections to every non-private
// provider that has a data backend for the stage (wildcard consumers only).
// Handles share ConnectToApp's cache.
func (f *IronFlock) ConnectToAllApps(ctx context.Context, opts ...ConnectToAllAppsOptions) ([]*ConsumedApp, error) {
	var o ConnectToAllAppsOptions
	if len(opts) > 0 {
		o = opts[0]
	}
	stage, err := f.consumedStage(o.Stage)
	if err != nil {
		return nil, err
	}
	infos, err := f.ListConsumableApps(ctx)
	if err != nil {
		return nil, err
	}

	var onDenied func(*CrossAppAccessError)
	if o.OnError != nil {
		onDenied = func(err *CrossAppAccessError) { o.OnError(err) }
	}
	var entries []*consumedEntry
	seen := make(map[*consumedEntry]bool)
	for i := range infos {
		info := &infos[i]
		if info.Catalog(stage) == nil {
			continue // no data backend for this stage
		}
		if info.App == "" {
			f.log.Warn(fmt.Sprintf("Skipping a provider without an app name (provider_app_key %d)", info.ProviderAppKey))
			continue
		}
		entry, err := f.cachedOpen(info.App, stage,
			func(ctx context.Context, evict func()) (*ConsumedApp, error) {
				return f.openFromInfo(ctx, info, stage, evict, onDenied)
			})
		if err != nil {
			return nil, err
		}
		if !seen[entry] {
			seen[entry] = true
			entries = append(entries, entry)
		}
	}

	// Every open settles before any failure is reported or returned.
	for _, e := range entries {
		select {
		case <-e.done:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	opened := make([]*ConsumedApp, 0, len(entries))
	for _, e := range entries {
		if e.err == nil {
			opened = append(opened, e.app)
			continue
		}
		if o.StopOnError {
			return nil, e.err
		}
		if o.OnError != nil {
			f.safeCallback("ConnectToAllApps OnError", func() { o.OnError(e.err) })
		}
	}
	return opened, nil
}

// cachedOpen returns the cache entry of app on stage, starting open in the
// background when there is none. open runs on a context Stop cancels and
// receives the function that evicts this entry (and only this entry) from
// the cache.
func (f *IronFlock) cachedOpen(app, stage string, open func(ctx context.Context, evict func()) (*ConsumedApp, error)) (*consumedEntry, error) {
	// Lower-cased so that ConnectToApp and ConnectToAllApps share entries:
	// the platform returns app names in lower case.
	key := strings.ToLower(app) + ":" + stage
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.stopped {
		return nil, fmt.Errorf("ironflock: cannot connect to another app after Stop: %w", wamp.ErrStopped)
	}
	if e, ok := f.consumed[key]; ok {
		return e, nil
	}
	e := &consumedEntry{done: make(chan struct{})}
	f.consumed[key] = e
	go f.runOpen(e, app, stage, func() { f.evict(key, e) }, open)
	return e, nil
}

// runOpen runs one attempt to open app on stage and publishes its outcome
// on e. A failed attempt is evicted before its waiters wake, so one that
// retries at once starts a fresh attempt.
func (f *IronFlock) runOpen(e *consumedEntry, app, stage string, evict func(), open func(ctx context.Context, evict func()) (*ConsumedApp, error)) {
	a, err := open(f.lifetime, evict)
	if err == nil {
		// Decide under the lock Stop takes its snapshot of the cache under:
		// either the snapshot finds the outcome published, and Stop closes
		// the connection, or this attempt finds Stop and closes it itself.
		f.mu.Lock()
		if !f.stopped {
			if h := f.beforeOpenPublished; h != nil {
				h()
			}
			e.app = a
			close(e.done) // waiters do not take f.mu
			if h := f.afterOpenPublished; h != nil {
				h()
			}
			f.mu.Unlock()
			return
		}
		f.mu.Unlock()
		// Do not hand out a connection nobody would close.
		ctx, cancel := context.WithTimeout(context.Background(), f.cleanupTimeout)
		if cerr := a.close(ctx); cerr != nil {
			f.log.Warn(fmt.Sprintf("Failed to close consumed app '%s': %v", a.App, cerr))
		}
		cancel()
		err = fmt.Errorf("ironflock: connection to app '%s' (%s) closed by Stop: %w", a.App, a.Stage, wamp.ErrStopped)
	} else if abortedByStop(f.lifetime, err) {
		// Reported as what it is: the caller's context was not cancelled.
		err = fmt.Errorf("ironflock: connection to app '%s' (%s) aborted by Stop: %w", app, stage, wamp.ErrStopped)
	}
	evict()
	e.err = err
	close(e.done)
}

// abortedByStop reports whether err is the failure of an open that Stop
// aborted: a cancellation (lifetime is the open's context) or a stopped
// connection once Stop has begun. A cross-app access denial keeps its code,
// even when it races Stop.
func abortedByStop(lifetime context.Context, err error) bool {
	var cerr *CrossAppAccessError
	return lifetime.Err() != nil && (errors.Is(err, context.Canceled) || errors.Is(err, wamp.ErrStopped)) &&
		!errors.As(err, &cerr)
}

// evict removes e from the cache, if key still maps to it.
func (f *IronFlock) evict(key string, e *consumedEntry) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.consumed[key] == e {
		delete(f.consumed, key)
	}
}

// openConsumedApp resolves a provider on the app's own realm and opens the
// connection to its realm.
func (f *IronFlock) openConsumedApp(ctx context.Context, appName, stage string, evict func(), onDenied func(*CrossAppAccessError)) (*ConsumedApp, error) {
	res, err := f.call(ctx, uriAppAccessResolve, []any{map[string]any{"app": appName}}, nil, nil, 0)
	if err != nil {
		return nil, callFailed(fmt.Sprintf("Call of procedure '%s'", uriAppAccessResolve), err)
	}
	m, ok := res.Value().(map[string]any)
	if !ok || len(m) == 0 {
		return nil, &CrossAppAccessError{
			Code:    CodeProviderNotInstalled,
			Message: fmt.Sprintf("App '%s' has no %s data backend in this project", appName, stage),
		}
	}
	info, err := decodeAppInfo(m)
	if err != nil {
		return nil, &OperationError{Op: fmt.Sprintf("Call of procedure '%s'", uriAppAccessResolve), Err: err}
	}
	if info.App == "" {
		info.App = appName
	}
	return f.openFromInfo(ctx, info, stage, evict, onDenied)
}

// openFromInfo opens the connection to a provider's realm from its resolved
// info: realm-<SWARM_KEY>-<provider_app_key>-<stage>, with this app's own
// credential (the platform checks the grant against the connecting app).
//
// The connection treats an authentication or authorization denial as final
// (wamp.Config.FailOnAuthError). A denial while opening fails the open at once
// and is returned as NOT_AUTHORIZED. After the open a denial counts only once
// it persists (the router also refuses while it cannot verify access); then
// the handle is evicted, its connection released, and the denial reported to
// onDenied. The connection is always torn down when the open fails.
func (f *IronFlock) openFromInfo(ctx context.Context, info *ConsumedAppInfo, stage string, evict func(), onDenied func(*CrossAppAccessError)) (*ConsumedApp, error) {
	catalog := info.Catalog(stage)
	if catalog == nil {
		return nil, &CrossAppAccessError{
			Code:    CodeProviderNotInstalled,
			Message: fmt.Sprintf("App '%s' has no %s data backend in this project", info.App, stage),
		}
	}
	if info.ProviderAppKey <= 0 {
		return nil, &CrossAppAccessError{
			Code: CodeProviderNotInstalled,
			Message: fmt.Sprintf("App '%s' has no %s data backend in this project (the platform sent no provider_app_key)",
				info.App, stage),
		}
	}
	url := f.conn.URL()
	if url == "" {
		var err error
		if url, err = f.routerURL(); err != nil {
			return nil, err
		}
	}
	appStage := wamp.StageDevelopment
	if stage == "prod" {
		appStage = wamp.StageProduction
	}
	realm := wamp.RealmName(f.swarmKey, info.ProviderAppKey, appStage)
	op := fmt.Sprintf("Connection to app '%s' (%s)", info.App, stage)
	denial := func(reason string, cause error) *CrossAppAccessError {
		why := "The grant may have been revoked."
		if f.usesLegacyCredential() {
			why = legacyCredentialDenial
		}
		return &CrossAppAccessError{
			Code:    CodeNotAuthorized,
			Message: fmt.Sprintf("Access to app '%s' (%s) denied: %s. %s", info.App, stage, reason, why),
			Err:     cause,
		}
	}

	conn := f.newConn()
	var (
		mu     sync.Mutex
		opened bool
		denied *CrossAppAccessError
	)
	// The handle's lifetime (see ConsumedApp): ended here unless the open
	// hands it over.
	lifetime, endLifetime := context.WithCancel(f.lifetime)
	handedOver := false
	defer func() {
		if !handedOver {
			endLifetime()
		}
	}()
	cfg := wamp.Config{
		SwarmKey:        f.swarmKey,
		AppKey:          info.ProviderAppKey,
		Stage:           appStage,
		URL:             url,
		SerialNumber:    f.serialNumber,
		AuthID:          f.authID,
		AuthSecret:      f.authSecret,
		FailOnAuthError: true,
		Logger:          f.log,
		OnAuthFailure: func(reason string) {
			err := denial(reason, &wamp.AuthError{Realm: realm, Reason: reason})
			mu.Lock()
			denied = err
			wasOpened := opened
			mu.Unlock()
			// A later ConnectToApp (e.g. once the grant is restored) must
			// open a fresh connection rather than get this dead one.
			evict()
			if !wasOpened {
				return // the pending open returns the denial itself
			}
			endLifetime() // the handle is dead: its table subscriptions stop
			f.log.Warn(err.Error())
			go func() {
				// The connection gave up for good; release it.
				ctx, cancel := context.WithTimeout(context.Background(), f.cleanupTimeout)
				defer cancel()
				_ = conn.Stop(ctx)
			}()
			if onDenied != nil {
				f.safeCallback("ConnectToApp OnError", func() { onDenied(err) })
			}
		},
	}
	if err := conn.Configure(cfg); err != nil {
		cctx, cancel := f.cleanupContext(ctx)
		_ = conn.Stop(cctx)
		cancel()
		return nil, &OperationError{Op: op, Err: err}
	}
	if err := conn.Start(ctx); err != nil {
		cctx, cancel := f.cleanupContext(ctx)
		_ = conn.Stop(cctx)
		cancel()
		var aerr *wamp.AuthError
		if errors.As(err, &aerr) {
			return nil, denial(aerr.Reason, err)
		}
		mu.Lock()
		d := denied
		mu.Unlock()
		if d != nil {
			return nil, d
		}
		return nil, &OperationError{Op: op, Err: err}
	}
	mu.Lock()
	opened = true
	d := denied
	mu.Unlock()
	if d != nil {
		// Denied between the join and now: the connection is already dead.
		cctx, cancel := f.cleanupContext(ctx)
		_ = conn.Stop(cctx)
		cancel()
		return nil, d
	}

	handedOver = true
	return &ConsumedApp{
		App:             info.App,
		Stage:           stage,
		Tables:          catalog.Tables,
		Transforms:      catalog.Transforms,
		conn:            conn,
		reconnectWindow: f.reconnectWindow,
		onClosed:        evict,
		log:             f.log,
		cleanupTimeout:  f.cleanupTimeout,
		lifetime:        lifetime,
		endLifetime:     endLifetime,
	}, nil
}

// legacyCredentialDenial explains a cross-app denial of a connection that
// presented the legacy device credential.
const legacyCredentialDenial = "Cross-app access needs the per-app credential the device agent injects " +
	"(APP_AUTH_ID and APP_AUTH_SECRET); this app has none (an older device agent does not inject it), so it " +
	"presented the legacy device credential, which another app's realm does not admit."

// usesLegacyCredential reports whether the app's connections present the
// legacy device credential (serial, serial): no WithCredentials pair and no
// per-app credential injected.
func (f *IronFlock) usesLegacyCredential() bool {
	if f.authID != "" {
		return false
	}
	id, secret := wamp.AppCredentials(f.serialNumber)
	return id == f.serialNumber && secret == f.serialNumber
}

// safeCallback runs a user callback, recovering and logging a panic.
func (f *IronFlock) safeCallback(name string, fn func()) {
	defer func() {
		if r := recover(); r != nil {
			f.log.Error(fmt.Sprintf("%s callback panicked: %v", name, r))
		}
	}()
	fn()
}
