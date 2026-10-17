package ironflock

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ironflock/sdk-go/internal/finite"
	"github.com/ironflock/sdk-go/internal/wampx"
	"github.com/ironflock/sdk-go/wamp"
)

// Keeping a table subscription complete across reconnects.
//
// The connection restores every subscription after a reconnect, but the
// router does not replay what was published while the session was down: a
// row the board saved while a device was offline never reached the device. A
// tableFollower closes that gap: after a reconnect it delivers the rows stored
// meanwhile, so app code sees one subscription and never has to know about
// sessions.
//
// How: every live row is tagged with the connection's join count. A row of a
// join the follower has not caught up on yet is held back. After each rejoin
// the follower reads the rows stored since the newest one its handler got,
// delivers them oldest first, and then releases the held rows, skipping those
// the read delivered already. A transform needs none of this: its next tick
// publishes the whole view again.
//
// The data backend publishes a stored row's live event on its own schedule,
// after the insert: the read can return a row whose live event reaches the
// app only after the catch-up is done, when it is no longer held. So the
// follower remembers the rows a catch-up delivered (see caughtRows), and drops
// one live event per remembered row — the held ones it releases as well as
// the later ones. The next catch-up's turn replaces the memory: no event of
// the session before can be delivered after it.

// catchUpLimit is the most rows one catch-up delivers: the newest ones stored
// while the connection was down. It is the highest limit a table read takes.
const catchUpLimit = MaxQueryLimit

// readFunc reads rows of a table: IronFlock.GetHistory, or a consumed app's.
type readFunc func(ctx context.Context, table string, q *TableQueryParams) ([]Row, error)

// tableKind is what a followed table is, once known: the caller may know it
// (a consumed app's catalog), and otherwise the first event or the first
// catch-up tells.
type tableKind int

const (
	kindUnknown tableKind = iota
	kindTable
	kindTransform
)

// stamp is a row's tsp in epoch milliseconds, the form the data backend
// sends: ms to compare, wire to send back as the start of a read.
type stamp struct {
	ms   float64
	wire any
}

// tspOf returns the tsp of row, a row map, when it is a finite number.
func tspOf(row any) (stamp, bool) {
	m, _ := row.(map[string]any)
	switch v := m["tsp"].(type) {
	case int64:
		return stamp{float64(v), v}, true
	case uint64:
		return stamp{float64(v), v}, true
	case float64:
		if !math.IsNaN(v) && !math.IsInf(v, 0) {
			return stamp{v, v}, true
		}
	}
	return stamp{}, false
}

// fingerprint identifies a row in a read and in a live event alike. The two
// can differ in a column that is null — a read returns it, an event may
// leave it out — which the fingerprint therefore leaves out, and in how a
// whole number is typed, which JSON does not tell apart: int64(22) and
// float64(22) both encode as 22. Keys are sorted at every depth, as
// encoding/json sorts them.
func fingerprint(row any) string {
	if m, ok := row.(map[string]any); ok {
		kept := make(map[string]any, len(m))
		for k, v := range m {
			if v != nil {
				kept[k] = v
			}
		}
		row = kept
	}
	data, err := json.Marshal(finite.OrNil(row))
	if err != nil {
		return fmt.Sprintf("%#v", row) // decoded payloads always encode; fmt sorts map keys too
	}
	return string(data)
}

// caughtRows is the memory of the rows a catch-up delivered whose live
// events may still arrive: a count per fingerprint (two rows a catch-up
// delivered can be alike), kept as the SHA-256 of the fingerprint, so that one
// catch-up of at most catchUpLimit rows costs little memory. The zero value
// is empty.
type caughtRows struct {
	count map[[sha256.Size]byte]int
	// newest is the latest tsp among the rows that have one: a row with a
	// later tsp is none of them (the fingerprint holds the tsp), so it is not
	// hashed — the live rows after a catch-up mostly are such rows.
	newest float64
}

// add remembers row.
func (c *caughtRows) add(row any) {
	if c.count == nil {
		c.count = make(map[[sha256.Size]byte]int)
		c.newest = math.Inf(-1)
	}
	if t, ok := tspOf(row); ok {
		c.newest = max(c.newest, t.ms)
	}
	c.count[sha256.Sum256([]byte(fingerprint(row)))]++
}

// take reports whether row is one of the remembered rows, and forgets it
// then: each remembered row is taken at most once.
func (c *caughtRows) take(row any) bool {
	if len(c.count) == 0 {
		return false
	}
	if t, ok := tspOf(row); ok && t.ms > c.newest {
		return false
	}
	key := sha256.Sum256([]byte(fingerprint(row)))
	switch n := c.count[key]; n {
	case 0:
		return false
	case 1:
		delete(c.count, key)
	default:
		c.count[key] = n - 1
	}
	return true
}

// len returns how many rows are remembered.
func (c *caughtRows) len() int {
	n := 0
	for _, k := range c.count {
		n += k
	}
	return n
}

// firstArg returns the first positional argument of ev, or nil.
func firstArg(ev *Event) any {
	if ev == nil || len(ev.Args) == 0 {
		return nil
	}
	return ev.Args[0]
}

// heldEvent is a live event held back until the follower has caught up on
// join, the connection's join count when it arrived.
type heldEvent struct {
	join int64
	ev   *Event
}

// tableFollower delivers the rows of a table subscription to its handler,
// also those stored while the connection was down.
//
// Lifecycle: attach before subscribing deliverLive (directly, and unrolled
// for the bulk feed), then activate once the row feed is subscribed — or
// stop when subscribing failed. stop also ends it when the subscription is
// unsubscribed.
//
// Every handler call runs on the subscription's DeliveryGroup and under
// callMu: the live ones as the group delivers the events, the rows of a
// catch-up and the released ones in a turn of their own in the group (see
// inTurn). So the handler never runs concurrently with itself, nor with the
// handler of another subscription of the group, and a live event delivered
// after a catch-up's turn sees the follower caught up. callMu keeps the
// handler calls one at a time, and in that order, also for events that
// reach deliverLive outside the group (as a test fake fires them).
type tableFollower struct {
	table   string
	topic   string // transformed.<table>, the topic of the rows a catch-up delivers
	handler EventHandler
	read    readFunc
	joins   *wampx.Joins
	group   *wamp.DeliveryGroup
	log     *slog.Logger
	limit   int // catchUpLimit; tests lower it
	// ctx ends when the follower is stopped, and when the app stops or the
	// consumed app closes: reads are abandoned, and no handler call starts.
	ctx    context.Context
	cancel context.CancelFunc
	// tasks counts the baseline read and the catch-ups under way.
	tasks sync.WaitGroup

	stopped atomic.Bool
	callMu  sync.Mutex // held for every handler call and the decision before it
	catchMu sync.Mutex // serializes the catch-ups

	mu   sync.Mutex // guards the fields below
	kind tableKind
	// last is the newest tsp the handler got (hasLast), and before it,
	// baseline is the newest tsp stored when it subscribed (!hasBaseline: the
	// table was empty). Until the read that finds the latter returns, or when
	// it fails or is answered only after a later join (see findBaseline), the
	// time of subscribing stands in for it. A catch-up takes the baseline as
	// it was at its join (see onJoin).
	last, baseline       stamp
	hasLast, hasBaseline bool
	// subscribed is the join the follower was made in: of the session the
	// subscription is made on.
	subscribed int64
	// caughtUp is the join the handler has every row of; held are the live
	// rows of later joins, or queued behind those, in arrival order.
	caughtUp int64
	held     []heldEvent
	// caught are the rows the last catch-up delivered whose live events have
	// not been seen yet: such an event is dropped, once per row.
	caught caughtRows
	unhook func()
}

// newTableFollower returns the follower of table for handler. conn is the
// connection the subscription lives on, read reads the table there, group is
// the DeliveryGroup of its feeds, and lifetime ends when the app stops or
// the consumed app closes.
func newTableFollower(lifetime context.Context, conn wampConn, log *slog.Logger, read readFunc,
	table string, kind tableKind, handler EventHandler, group *wamp.DeliveryGroup) *tableFollower {
	joins := joinsOf(conn)
	if joins == nil {
		joins = new(wampx.Joins) // a connection that never rejoins: nothing to catch up on
	}
	ctx, cancel := context.WithCancel(lifetime)
	now := time.Now().UnixMilli()
	join := joins.Count()
	return &tableFollower{
		table:       table,
		topic:       "transformed." + table,
		handler:     handler,
		read:        read,
		joins:       joins,
		group:       group,
		log:         log,
		limit:       catchUpLimit,
		ctx:         ctx,
		cancel:      cancel,
		kind:        kind,
		baseline:    stamp{float64(now), now},
		hasBaseline: true,
		subscribed:  join,
		caughtUp:    join,
	}
}

// joinsOf returns the join counter of conn: a *wamp.Connection's (see package
// wampx), or the one a test fake has. nil when conn has none.
func joinsOf(conn wampConn) *wampx.Joins {
	if c, ok := conn.(interface{ joinCounter() *wampx.Joins }); ok {
		return c.joinCounter()
	}
	return wampx.JoinsOf(conn)
}

// attach follows the connection's joins: from before the subscription
// exists, so that a rejoin while subscribing is caught up on too.
func (fl *tableFollower) attach() {
	unhook := fl.joins.OnRestored(fl.onJoin)
	fl.mu.Lock()
	fl.unhook = unhook
	fl.mu.Unlock()
}

// activate starts following the subscription, which now exists: it finds
// the newest row stored now, unless the table is a transform.
func (fl *tableFollower) activate() {
	fl.mu.Lock()
	transform := fl.kind == kindTransform
	fl.mu.Unlock()
	if !transform {
		fl.spawn(fl.findBaseline)
	}
}

// stop ends the follower for good: no handler call starts after it, it
// follows the joins no more, and its reads are abandoned.
func (fl *tableFollower) stop() {
	fl.stopped.Store(true)
	fl.cancel()
	fl.detach()
}

// detach stops following the joins and drops the held rows and the memory
// of the caught-up ones.
func (fl *tableFollower) detach() {
	fl.mu.Lock()
	unhook := fl.unhook
	fl.unhook = nil
	clear(fl.held)
	fl.held = nil
	fl.caught = caughtRows{}
	fl.mu.Unlock()
	if unhook != nil {
		unhook()
	}
}

func (fl *tableFollower) spawn(task func()) {
	fl.tasks.Add(1)
	go func() {
		defer fl.tasks.Done()
		task()
	}()
}

// findBaseline reads the newest row stored now: before the handler has got a
// row, a catch-up delivers the rows stored after it. An answer that arrives
// after a later join is not taken.
func (fl *tableFollower) findBaseline() {
	rows, err := fl.read(fl.ctx, fl.table, &TableQueryParams{Limit: 1})
	if err != nil {
		// The time of subscribing stays the baseline.
		if fl.ctx.Err() == nil {
			fl.log.Debug(fmt.Sprintf("Could not read the newest row of table '%s': %v", fl.table, err))
		}
		return
	}
	fl.mu.Lock()
	defer fl.mu.Unlock()
	if fl.joins.Count() != fl.subscribed {
		// Answered on a session that joined after the subscription was
		// made — a read waits for the connection, and retries, across a
		// reconnect: the newest row then may be one stored while the
		// connection was down, which a catch-up must deliver, not start
		// after. The time of subscribing stays the baseline. (A session
		// counts its join before it can carry any call, so an answer of a
		// later session always sees the later count here.)
		return
	}
	if len(rows) == 0 {
		fl.hasBaseline = false // empty: a catch-up reads the table from its start
		return
	}
	fl.baseline, fl.hasBaseline = tspOf(rows[len(rows)-1])
}

// -- live events ---------------------------------------------------------

// deliverLive is the handler of the table's feeds: an event of the row feed,
// one row of a bulk batch (see unrollBulk), or a transform's tick.
func (fl *tableFollower) deliverLive(ev *Event) {
	fl.callMu.Lock()
	defer fl.callMu.Unlock()
	if _, tick := firstArg(ev).([]any); tick {
		// A transform's tick: the whole view, delivered as it came.
		fl.mu.Lock()
		fl.kind = kindTransform
		fl.mu.Unlock()
		fl.call(ev)
		return
	}
	fl.mu.Lock()
	// The join is read under mu, as a catch-up takes its start under mu: a
	// row delivered here is either one the read then starts after, or held.
	join := fl.joins.Count()
	if join > fl.caughtUp || len(fl.held) > 0 {
		if !fl.stopped.Load() {
			fl.held = append(fl.held, heldEvent{join, ev})
		}
		fl.mu.Unlock()
		return
	}
	if fl.caught.take(firstArg(ev)) {
		fl.mu.Unlock()
		return // the late live event of a row the catch-up delivered
	}
	fl.noteLocked(ev)
	fl.mu.Unlock()
	fl.call(ev)
}

// noteLocked records the tsp of ev's row, which the handler gets next: a
// catch-up delivers the rows after the newest one. fl.mu must be held.
func (fl *tableFollower) noteLocked(ev *Event) {
	if t, ok := tspOf(firstArg(ev)); ok && (!fl.hasLast || t.ms > fl.last.ms) {
		fl.last, fl.hasLast = t, true
	}
}

// call calls the handler, unless the follower has ended.
func (fl *tableFollower) call(ev *Event) {
	if fl.stopped.Load() || fl.ctx.Err() != nil {
		return
	}
	fl.handler(ev)
}

// callLogged is call for the rows a catch-up delivers and the held ones it
// releases: outside the event dispatch nothing logs a panicking handler, and
// one bad row must not hold up the rest.
func (fl *tableFollower) callLogged(ev *Event) {
	defer func() {
		if r := recover(); r != nil {
			topic := fl.topic
			if ev != nil {
				topic = ev.Topic
			}
			fl.log.Error(fmt.Sprintf("Table event handler for '%s' panicked: %v", topic, r))
		}
	}()
	fl.call(ev)
}

// -- catching up ---------------------------------------------------------

// onJoin is the join hook: the connection calls it, on its supervisor
// goroutine, once a join has restored the subscriptions.
//
// The catch-up starts at the baseline as it is now: a join while
// subscribing, or before the baseline read has returned, is caught up on
// from the time of subscribing, even when that read returns first — it
// would skip the rows stored while the connection was down.
func (fl *tableFollower) onJoin(join int64) {
	if fl.stopped.Load() {
		fl.detach() // unsubscribed: nothing left to follow
		return
	}
	fl.mu.Lock()
	baseline, hasBaseline := fl.baseline, fl.hasBaseline
	fl.mu.Unlock()
	fl.spawn(func() { fl.catchUp(join, baseline, hasBaseline) })
}

// catchUp delivers the rows stored while the connection was down before
// join, oldest first, and then the live rows held back meanwhile. It starts
// after the newest row the handler got, or, before the first one, at
// baseline (see onJoin).
func (fl *tableFollower) catchUp(join int64, baseline stamp, hasBaseline bool) {
	fl.catchMu.Lock()
	defer fl.catchMu.Unlock()
	fl.mu.Lock()
	if join <= fl.caughtUp || fl.stopped.Load() {
		fl.mu.Unlock()
		return
	}
	if fl.kind == kindTransform {
		fl.mu.Unlock()
		fl.inTurn(func() { fl.release(join, caughtRows{}) })
		return
	}
	since, hasSince := fl.last, fl.hasLast
	if !hasSince {
		since, hasSince = baseline, hasBaseline
	}
	fl.mu.Unlock()

	q := &TableQueryParams{Limit: fl.limit}
	if hasSince {
		q.TimeRange = &TimeRange{Start: since.wire}
	}
	rows, err := fl.read(fl.ctx, fl.table, q)
	if fl.ctx.Err() != nil {
		return // stopped
	}
	if err != nil {
		switch {
		case WampURI(err) == URIInvalidLimit:
			// Only a transform refuses this limit; its next tick brings the
			// whole view.
			fl.mu.Lock()
			fl.kind = kindTransform
			fl.mu.Unlock()
		case fl.joins.Count() != join:
			return // The connection went down again: the next join catches up.
		default:
			fl.log.Warn(fmt.Sprintf("Could not read the rows of table '%s' stored while the connection "+
				"was down; they are not delivered: %v", fl.table, err))
		}
		fl.inTurn(func() { fl.release(join, caughtRows{}) })
		return
	}

	if len(rows) >= fl.limit {
		fl.log.Warn(fmt.Sprintf("More than %d rows of table '%s' were stored while the connection was "+
			"down; the newest %d are delivered", fl.limit, fl.table, fl.limit))
	}
	var events []*Event
	var caught caughtRows
	for _, row := range rows {
		// The read starts at since, inclusive: those rows the handler has.
		if hasSince {
			if t, ok := tspOf(row); !ok || t.ms <= since.ms {
				continue
			}
		}
		caught.add(row)
		events = append(events, &Event{Topic: fl.topic, Args: []any{row}})
	}
	fl.inTurn(func() {
		for _, ev := range events {
			fl.mu.Lock()
			fl.noteLocked(ev)
			fl.mu.Unlock()
			fl.callLogged(ev)
		}
		fl.release(join, caught)
	})
}

// release delivers the held rows of the joins up to join, in arrival order,
// except the live events of rows a catch-up delivered already — of caught,
// the rows this catch-up delivered, and of the previous catch-up's rows
// whose events had not been seen: an event of the session before may still
// have been queued when the connection dropped. Each such row drops one
// event. caught, less what the held rows took, then replaces the memory of
// the caught-up rows: no event of the session before can be delivered after
// this turn. release runs in a turn of its own (see inTurn).
func (fl *tableFollower) release(join int64, caught caughtRows) {
	fl.mu.Lock()
	previous := fl.caught
	fl.caught = caught
	fl.caughtUp = max(fl.caughtUp, join)
	for len(fl.held) > 0 && fl.held[0].join <= fl.caughtUp {
		h := fl.held[0]
		fl.held[0] = heldEvent{}
		fl.held = fl.held[1:]
		if row := firstArg(h.ev); fl.caught.take(row) || previous.take(row) {
			continue
		}
		fl.noteLocked(h.ev)
		fl.mu.Unlock()
		fl.callLogged(h.ev)
		fl.mu.Lock()
	}
	fl.mu.Unlock()
}

// inTurn runs deliver, which calls the handler, in a turn of its own on the
// subscription's DeliveryGroup — after the events queued there already, so
// those of a join not caught up on yet are held when it runs — and under
// callMu. It waits until deliver has run, or the follower has ended.
func (fl *tableFollower) inTurn(deliver func()) {
	done := make(chan struct{})
	turn := func() {
		defer close(done)
		defer func() {
			if r := recover(); r != nil {
				fl.log.Error(fmt.Sprintf("Catching up on table '%s' panicked: %v", fl.table, r),
					"stack", string(debug.Stack()))
			}
		}()
		fl.callMu.Lock()
		defer fl.callMu.Unlock()
		deliver()
	}
	if !wampx.RunInGroup(fl.group, turn) {
		turn() // no DeliveryGroup: callMu alone keeps the order
		return
	}
	select {
	case <-done:
	case <-fl.ctx.Done():
	}
}
