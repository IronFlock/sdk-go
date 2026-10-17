package ironflock

// Table subscriptions deliver the rows stored while the connection was down.
//
// The router does not replay events published while a session is gone, so
// after a reconnect the tableFollower (follow.go) reads the rows stored
// meanwhile and delivers them, oldest first, before the live rows that
// arrived since. These tests drive it with a fake connection and a fake
// history store that reads like the data backend: tsp >= start of the time
// range, the newest limit rows, oldest first, and a transform refusing a
// limit over 3000. Live events reach the follower as wamp.Connection delivers
// them: one at a time on the subscription's DeliveryGroup.

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ironflock/nexus/v3/client"
	nxwamp "github.com/ironflock/nexus/v3/wamp"

	"github.com/ironflock/sdk-go/internal/wampx"
	"github.com/ironflock/sdk-go/wamp"
)

// historyStore answers table reads as the data backend does.
type historyStore struct {
	mu        sync.Mutex
	rows      []Row
	transform bool
	reads     []TableQueryParams
	fail      error         // returned by the next read
	gate      chan struct{} // the next catch-up read (limit > 1) waits for it to close
	arrived   chan struct{} // closed once that read has arrived
	returned  atomic.Int32  // the reads answered so far
}

func (s *historyStore) add(rows ...Row) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rows = append(s.rows, rows...)
}

// failNext makes the next read fail with err.
func (s *historyStore) failNext(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.fail = err
}

// holdNextCatchUp makes the next catch-up read wait until open is called.
// arrived is closed once that read has arrived.
func (s *historyStore) holdNextCatchUp() (arrived <-chan struct{}, open func()) {
	s.mu.Lock()
	defer s.mu.Unlock()
	gate := make(chan struct{})
	s.gate, s.arrived = gate, make(chan struct{})
	var once sync.Once
	return s.arrived, func() { once.Do(func() { close(gate) }) }
}

func (s *historyStore) allReads() []TableQueryParams {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.reads)
}

func (s *historyStore) lastRead(t *testing.T) TableQueryParams {
	t.Helper()
	reads := s.allReads()
	if len(reads) == 0 {
		t.Fatal("nothing was read")
	}
	return reads[len(reads)-1]
}

func (s *historyStore) read(ctx context.Context, _ string, q *TableQueryParams) ([]Row, error) {
	s.mu.Lock()
	s.reads = append(s.reads, *q)
	var gate chan struct{}
	if s.gate != nil && q.Limit > 1 {
		gate = s.gate
		s.gate = nil
		close(s.arrived)
	}
	s.mu.Unlock()
	if gate != nil {
		select {
		case <-gate:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	defer s.returned.Add(1)
	if err := s.fail; err != nil {
		s.fail = nil
		return nil, err
	}
	if s.transform && q.Limit > 3000 {
		return nil, &OperationError{Op: "getHistory('t')", Err: wampErr(URIInvalidLimit, "limit must be at most 3000")}
	}
	var out []Row
	for _, r := range s.rows {
		if q.TimeRange == nil || q.TimeRange.Start == nil || msOf(r["tsp"]) >= msOf(q.TimeRange.Start) {
			out = append(out, r)
		}
	}
	slices.SortStableFunc(out, func(a, b Row) int { return cmp.Compare(msOf(a["tsp"]), msOf(b["tsp"])) })
	return out[max(len(out)-q.Limit, 0):], nil
}

// answer answers a recorded call of history.transformed.<table> on a fake
// connection from s, reading its wire payload as the data backend does.
func (s *historyStore) answer(call fakeCall) (*wamp.Result, error) {
	wire, _ := call.Args[0].(map[string]any)
	limit, _ := wire["limit"].(int64)
	q := &TableQueryParams{Limit: int(limit)}
	if tr, ok := wire["timeRange"].([]any); ok {
		q.TimeRange = &TimeRange{Start: tr[0], End: tr[1]}
	}
	rows, err := s.read(context.Background(), "", q)
	if err != nil {
		return nil, err
	}
	list := make([]any, len(rows))
	for i, r := range rows {
		list[i] = r
	}
	return resultOf(list), nil
}

// msOf reads a tsp or time-range bound in epoch milliseconds.
func msOf(v any) float64 {
	switch n := v.(type) {
	case int64:
		return float64(n)
	case float64:
		return n
	}
	panic(fmt.Sprintf("not epoch milliseconds: %#v", v))
}

// trow is a row of table t with tsp; key/value pairs follow: value defaults
// to tsp.
func trow(tsp int64, kv ...any) Row {
	r := Row{"tsp": tsp, "value": tsp, "device_key": int64(7)}
	for i := 0; i+1 < len(kv); i += 2 {
		r[kv[i].(string)] = kv[i+1]
	}
	return r
}

// received records the events a handler gets.
type received struct {
	mu  sync.Mutex
	evs []*Event
}

func (r *received) handle(ev *Event) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.evs = append(r.evs, ev)
}

func (r *received) events() []*Event {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.evs)
}

// args returns the first positional argument of every event.
func (r *received) args() []any {
	var out []any
	for _, ev := range r.events() {
		out = append(out, ev.Args[0])
	}
	return out
}

// tsps returns the tsp of every row received.
func (r *received) tsps() []int64 {
	var out []int64
	for _, ev := range r.events() {
		tsp, _ := ev.Row()["tsp"].(int64)
		out = append(out, tsp)
	}
	return out
}

func wantTsps(t *testing.T, r *received, want ...int64) {
	t.Helper()
	if got := r.tsps(); !slices.Equal(got, want) {
		t.Fatalf("rows with tsp %v, want %v", got, want)
	}
}

// followed is a tableFollower of table "t" on a fake connection.
type followed struct {
	t     *testing.T
	conn  *fakeConn // its join counter is the connection's
	store *historyStore
	fl    *tableFollower
	group *wamp.DeliveryGroup
	got   *received
	logs  *logBuffer
}

// follow returns a follower subscribed on a fake connection that has joined
// once, its baseline read done: the state SubscribeToTable leaves it in.
// handler, if not nil, replaces the recorder; setup runs before the follower
// is attached.
func follow(t *testing.T, s *historyStore, kind tableKind, handler EventHandler, setup ...func(*tableFollower)) *followed {
	t.Helper()
	conn := &fakeConn{}
	conn.joins.Begin()
	log, logs := newTestLogger()
	got := &received{}
	if handler == nil {
		handler = got.handle
	}
	group := wamp.NewDeliveryGroup()
	fl := newTableFollower(context.Background(), conn, log, s.read, "t", kind, handler, group)
	for _, f := range setup {
		f(fl)
	}
	t.Cleanup(fl.stop)
	fl.attach()
	fl.activate()
	fw := &followed{t: t, conn: conn, store: s, fl: fl, group: group, got: got, logs: logs}
	fw.settle()
	return fw
}

// settle waits until the follower's reads and catch-ups are done.
func (fw *followed) settle() { fw.fl.tasks.Wait() }

// rejoin is a reconnect: a new session joins and, its restore done, the
// join hooks run. The catch-up runs in the background (see settle).
func (fw *followed) rejoin() { fw.conn.rejoin() }

// live delivers an event of the row feed whose first argument is arg.
func (fw *followed) live(arg any) {
	fw.inGroup(func() { fw.fl.deliverLive(&Event{Topic: "transformed.t", Args: []any{arg}}) })
}

// bulk delivers an event of the bulk feed carrying rows.
func (fw *followed) bulk(rows ...any) {
	unrolled := unrollBulk(fw.fl.deliverLive, discardLogger())
	fw.inGroup(func() { unrolled(&Event{Topic: "transformed.bulk.t", Args: []any{rows}}) })
}

// inGroup runs deliver as the connection delivers an event: in turn on the
// subscription's DeliveryGroup, a panic recovered. It waits until it has run.
func (fw *followed) inGroup(deliver func()) {
	fw.t.Helper()
	done := make(chan struct{})
	if !wampx.RunInGroup(fw.group, func() {
		defer close(done)
		defer func() { _ = recover() }()
		deliver()
	}) {
		fw.t.Fatal("the group did not take the event")
	}
	recv(fw.t, done, "the event to be delivered")
}

func discardLogger() *slog.Logger {
	log, _ := newTestLogger()
	return log
}

func TestLiveRowsReachTheHandlerWhileConnected(t *testing.T) {
	fw := follow(t, &historyStore{}, kindUnknown, nil)
	fw.live(trow(100))
	fw.bulk(trow(101), trow(102))
	wantTsps(t, fw.got, 100, 101, 102)
}

func TestRowsStoredWhileDownArriveAfterTheRejoinOldestFirst(t *testing.T) {
	s := &historyStore{rows: []Row{trow(50)}}
	fw := follow(t, s, kindUnknown, nil)
	fw.live(trow(100))
	s.add(trow(100), trow(120), trow(110)) // stored while the connection was down

	fw.rejoin()
	fw.settle()

	wantTsps(t, fw.got, 100, 110, 120)
	if q := s.lastRead(t); !reflect.DeepEqual(q, TableQueryParams{Limit: catchUpLimit, TimeRange: &TimeRange{Start: int64(100)}}) {
		t.Errorf("catch-up read %+v (time range %+v)", q, q.TimeRange)
	}
}

func TestLiveRowsOfTheNewSessionWaitForTheRowsStoredBeforeThem(t *testing.T) {
	s := &historyStore{}
	fw := follow(t, s, kindUnknown, nil)
	fw.live(trow(100))
	s.add(trow(100), trow(110))
	arrived, open := s.holdNextCatchUp()

	fw.rejoin()
	recv(t, arrived, "the catch-up read")
	// The catch-up read is under way; rows of the new session arrive meanwhile.
	s.add(trow(130))
	fw.live(trow(130))
	fw.bulk(trow(140))
	wantTsps(t, fw.got, 100)

	open()
	fw.settle()
	wantTsps(t, fw.got, 100, 110, 130, 140) // 130 once: read and live
}

// caughtLen returns how many caught-up rows fl remembers.
func (fl *tableFollower) caughtLen() int {
	fl.mu.Lock()
	defer fl.mu.Unlock()
	return fl.caught.len()
}

// The data backend publishes a stored row's live event after the insert, on
// its own schedule: the catch-up read can return a row whose live event
// arrives only after the catch-up is done, when it is no longer held. That
// event is dropped, so the handler gets the row once. A live row that is no
// caught-up row — a newer one, or one of the same tsp with other values — is
// delivered.
func TestALateLiveEventOfACaughtUpRowIsDropped(t *testing.T) {
	s := &historyStore{}
	fw := follow(t, s, kindUnknown, nil)
	fw.live(trow(100))
	s.add(trow(110), trow(120)) // 120 is stored as the catch-up reads: its live event is on its way
	fw.rejoin()
	fw.settle()
	wantTsps(t, fw.got, 100, 110, 120)

	fw.bulk(trow(120, "value", int64(7))) // another row of the same tsp
	fw.live(trow(120))                    // the live event of the caught-up row 120, after the catch-up
	fw.live(trow(130))
	wantTsps(t, fw.got, 100, 110, 120, 120, 130)
	if got := fw.got.events()[3].Row()["value"]; got != int64(7) {
		t.Errorf("the second row of tsp 120 has value %v, want the other row's 7", got)
	}
	if n := fw.fl.caughtLen(); n != 1 {
		t.Errorf("%d caught-up rows remembered, want 110 alone: its live event may still come", n)
	}
}

// A caught-up row drops one live event at most: the same row published once
// more is delivered, and two rows alike in the catch-up drop two events.
func TestACaughtUpRowDropsOneLiveEventAtMost(t *testing.T) {
	s := &historyStore{}
	fw := follow(t, s, kindUnknown, nil)
	fw.live(trow(100))
	s.add(trow(110), trow(120), trow(120)) // two rows alike, both stored
	fw.rejoin()
	fw.settle()
	wantTsps(t, fw.got, 100, 110, 120, 120)

	fw.live(trow(110)) // its live event: dropped
	fw.live(trow(110)) // the same row once more: delivered
	fw.live(trow(120))
	fw.bulk(trow(120)) // the live events of the two rows alike: dropped
	fw.live(trow(120)) // a third: delivered
	wantTsps(t, fw.got, 100, 110, 120, 120, 110, 120)
}

// A held row that the catch-up delivered too is dropped when the catch-up
// releases it, as before, and that uses up the caught-up row: the same row
// live once more after the catch-up is delivered.
func TestAHeldRowUsesUpItsCaughtUpRow(t *testing.T) {
	s := &historyStore{}
	fw := follow(t, s, kindUnknown, nil)
	fw.live(trow(100))
	s.add(trow(110))
	arrived, open := s.holdNextCatchUp()
	fw.rejoin()
	recv(t, arrived, "the catch-up read")
	fw.live(trow(110)) // its live event, during the catch-up: held
	open()
	fw.settle()
	wantTsps(t, fw.got, 100, 110)

	fw.live(trow(110))
	wantTsps(t, fw.got, 100, 110, 110)
}

// The next catch-up's turn replaces the memory. The held rows it releases
// are checked against the previous catch-up's rows too — a live event of the
// session before may have been queued still when the connection dropped
// again — and after that turn only the latest catch-up's rows count.
func TestTheNextCatchUpReplacesTheMemory(t *testing.T) {
	s := &historyStore{}
	fw := follow(t, s, kindUnknown, nil)
	fw.live(trow(100))
	s.add(trow(110), trow(120))
	fw.rejoin()
	fw.settle() // the first catch-up remembers 110 and 120
	wantTsps(t, fw.got, 100, 110, 120)

	s.add(trow(130))
	arrived, open := s.holdNextCatchUp()
	fw.rejoin()
	recv(t, arrived, "the second catch-up read")
	fw.live(trow(110)) // a late event of the session before, delivered only now: held
	open()
	fw.settle() // the second catch-up delivers 130 and drops the held 110
	wantTsps(t, fw.got, 100, 110, 120, 130)

	fw.live(trow(120)) // a row of the first catch-up, which the second replaced: delivered
	fw.live(trow(130)) // the second catch-up's row: dropped
	wantTsps(t, fw.got, 100, 110, 120, 130, 120)
	if n := fw.fl.caughtLen(); n != 0 {
		t.Errorf("%d caught-up rows still remembered", n)
	}
}

// Unsubscribe frees the memory of the caught-up rows.
func TestUnsubscribeFreesTheCaughtUpRows(t *testing.T) {
	f := flock(t)
	s := &historyStore{}
	f.own.callFn = s.answer
	got := &received{}
	ts, err := f.SubscribeToTable(bg, "t", got.handle)
	if err != nil {
		t.Fatal(err)
	}
	ts.follower.tasks.Wait()
	f.own.fire("transformed.t", &Event{Topic: "transformed.t", Args: []any{trow(100)}})
	s.add(trow(110), trow(120))
	f.own.rejoin()
	ts.follower.tasks.Wait()
	wantTsps(t, got, 100, 110, 120)
	if n := ts.follower.caughtLen(); n != 2 {
		t.Fatalf("%d caught-up rows remembered, want 2", n)
	}
	if err := ts.Unsubscribe(bg); err != nil {
		t.Fatal(err)
	}
	if n := ts.follower.caughtLen(); n != 0 {
		t.Errorf("%d caught-up rows remembered after Unsubscribe", n)
	}
}

// caughtRows takes a row alike in every value to one it remembers, as often
// as it was added: rows alike as the fingerprint sees them (a whole number
// typed either way, a null column left out), and rows without an epoch-ms
// tsp. A row with other values, or a later tsp than every row, is not taken.
func TestCaughtRowsTakesEachRowAsOftenAsItWasAdded(t *testing.T) {
	var c caughtRows
	if c.take(trow(1)) || c.len() != 0 {
		t.Fatal("an empty memory took a row")
	}
	textTsp := Row{"tsp": "2026-10-17T10:00:00Z", "value": int64(1)}
	c.add(trow(10))
	c.add(trow(10))
	c.add(Row{"tsp": float64(20), "value": float64(20), "device_key": int64(7), "unset": nil})
	c.add(textTsp)
	if n := c.len(); n != 4 {
		t.Fatalf("%d rows remembered, want 4", n)
	}
	for i, tc := range []struct {
		row  Row
		want bool
	}{
		{trow(20), true},  // the same row as the one added with float64 numbers and a null column
		{trow(20), false}, // taken already
		{trow(10, "value", int64(11)), false},
		{trow(10), true},
		{trow(10), true}, // added twice
		{trow(10), false},
		{trow(30), false}, // later than every row
		{Row{"tsp": "2026-10-17T10:00:00Z", "value": int64(2)}, false},
		{textTsp, true},
	} {
		if got := c.take(tc.row); got != tc.want {
			t.Errorf("take #%d %v = %v, want %v", i, tc.row, got, tc.want)
		}
	}
	if n := c.len(); n != 0 {
		t.Errorf("%d rows remembered, want none", n)
	}
}

func TestBeforeTheFirstRowTheNewestRowStoredWhenSubscribingIsTheStart(t *testing.T) {
	s := &historyStore{rows: []Row{trow(90, "value", "saved before the device started")}}
	fw := follow(t, s, kindUnknown, nil)
	s.add(trow(95, "value", "saved while the device was offline"))

	fw.rejoin()
	fw.settle()

	var values []any
	for _, ev := range fw.got.events() {
		values = append(values, ev.Row()["value"])
	}
	if !reflect.DeepEqual(values, []any{"saved while the device was offline"}) {
		t.Errorf("delivered %v", values)
	}
	if q := s.lastRead(t); q.TimeRange == nil || q.TimeRange.Start != int64(90) || q.TimeRange.End != nil {
		t.Errorf("catch-up read %+v", q)
	}
}

func TestATableEmptyWhenSubscribingIsReadInFull(t *testing.T) {
	s := &historyStore{}
	fw := follow(t, s, kindUnknown, nil)
	s.add(trow(1), trow(2))

	fw.rejoin()
	fw.settle()

	wantTsps(t, fw.got, 1, 2)
	if q := s.lastRead(t); q.TimeRange != nil || q.Limit != catchUpLimit {
		t.Errorf("catch-up read %+v, want the table without a time range", q)
	}
}

func TestWithoutTheNewestStoredRowTheTimeOfSubscribingIsTheStart(t *testing.T) {
	s := &historyStore{fail: errors.New("no history yet")} // the baseline read
	before := time.Now().UnixMilli()
	fw := follow(t, s, kindUnknown, nil)
	after := time.Now().UnixMilli()
	fw.fl.mu.Lock()
	subscribed, ok := fw.fl.baseline.wire.(int64), fw.fl.hasBaseline
	fw.fl.mu.Unlock()
	if !ok || subscribed < before || subscribed > after {
		t.Fatalf("baseline %v (%v), want the time of subscribing", subscribed, ok)
	}

	fw.rejoin()
	fw.settle()
	if q := s.lastRead(t); q.TimeRange == nil || q.TimeRange.Start != subscribed || q.TimeRange.End != nil {
		t.Errorf("catch-up read %+v, want it to start at %d", q, subscribed)
	}
}

// A baseline read answered only after a later session joined — it waited for
// the connection, or retried, across the reconnect — is not taken: the newest
// row then may be one stored while the connection was down, which a catch-up
// must deliver rather than start after. The time of subscribing stays the
// baseline, whether the answer comes before the join's catch-up begins or
// after it (a failed one, here: the next catch-up would start at the answer).
func TestABaselineAnsweredAfterALaterJoinIsNotTaken(t *testing.T) {
	// subscribe makes a follower whose baseline read waits until answer is
	// closed, and returns once the read has been issued.
	subscribe := func(t *testing.T, s *historyStore) (fl *tableFollower, conn *fakeConn, got *received, at int64, answer chan struct{}) {
		t.Helper()
		asked := make(chan struct{})
		answer = make(chan struct{})
		read := func(ctx context.Context, table string, q *TableQueryParams) ([]Row, error) {
			if q.Limit == 1 { // the baseline read
				close(asked)
				select {
				case <-answer:
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}
			return s.read(ctx, table, q)
		}
		conn = &fakeConn{}
		conn.joins.Begin()
		got = &received{}
		at = time.Now().UnixMilli()
		fl = newTableFollower(context.Background(), conn, discardLogger(), read, "t", kindUnknown, got.handle, wamp.NewDeliveryGroup())
		t.Cleanup(fl.stop)
		fl.attach()
		fl.activate()
		recv(t, asked, "the baseline read")
		return fl, conn, got, at, answer
	}
	wantSubscribingTime := func(t *testing.T, fl *tableFollower, at int64) {
		t.Helper()
		fl.mu.Lock()
		baseline := fl.baseline.wire
		fl.mu.Unlock()
		if b, ok := baseline.(int64); !ok || b < at || b >= at+1_000 {
			t.Errorf("baseline %v, want the time of subscribing", baseline)
		}
	}

	t.Run("before the join's catch-up", func(t *testing.T) {
		s := &historyStore{}
		fl, conn, got, at, answer := subscribe(t, s)
		// The connection drops; rows are stored while it is down; the read is
		// answered on the new session before its join hook has run.
		s.add(trow(at+1_000), trow(at+2_000))
		join := conn.joins.Begin()
		close(answer)
		fl.tasks.Wait() // the baseline read has returned
		conn.joins.Restored(join, nil)
		fl.tasks.Wait()
		wantTsps(t, got, at+1_000, at+2_000)
		wantSubscribingTime(t, fl, at)
	})

	t.Run("after a failed catch-up", func(t *testing.T) {
		s := &historyStore{}
		fl, conn, got, at, answer := subscribe(t, s)
		s.add(trow(at+1_000), trow(at+2_000))
		s.failNext(errors.New("data backend restarting")) // the catch-up's read
		conn.rejoin()
		eventually(t, "the failed catch-up", func() bool { return s.returned.Load() == 1 })
		close(answer) // the baseline read is answered after it
		fl.tasks.Wait()
		wantSubscribingTime(t, fl, at)
		conn.rejoin() // the next catch-up starts at the time of subscribing
		fl.tasks.Wait()
		wantTsps(t, got, at+1_000, at+2_000)
	})
}

func TestATransformIsNotReadItsNextTickBringsTheView(t *testing.T) {
	s := &historyStore{transform: true}
	fw := follow(t, s, kindTransform, nil)
	fw.live([]any{trow(1), trow(2)})
	fw.rejoin()
	fw.settle()
	fw.live([]any{trow(1), trow(2), trow(3)})

	if reads := s.allReads(); len(reads) != 0 {
		t.Errorf("reads %+v: no baseline, no catch-up", reads)
	}
	want := []any{[]any{trow(1), trow(2)}, []any{trow(1), trow(2), trow(3)}}
	if got := fw.got.args(); !reflect.DeepEqual(got, want) {
		t.Errorf("delivered %v, want %v", got, want)
	}
}

func TestAnEventWithAListMarksATransform(t *testing.T) {
	s := &historyStore{transform: true}
	fw := follow(t, s, kindUnknown, nil)
	fw.live([]any{trow(1)})
	reads := len(s.allReads())

	fw.rejoin()
	fw.settle()
	if n := len(s.allReads()); n != reads {
		t.Errorf("%d reads after the rejoin, want %d", n, reads)
	}
}

func TestATransformFoundByItsRefusalIsNotReadAgain(t *testing.T) {
	s := &historyStore{transform: true}
	fw := follow(t, s, kindUnknown, nil)

	fw.rejoin()
	fw.settle()
	fw.live([]any{trow(1)})
	fw.rejoin()
	fw.settle()

	var limits []int
	for _, q := range s.allReads() {
		limits = append(limits, q.Limit)
	}
	if !slices.Equal(limits, []int{1, catchUpLimit}) {
		t.Errorf("read limits %v", limits)
	}
	if got := fw.got.args(); !reflect.DeepEqual(got, []any{[]any{trow(1)}}) {
		t.Errorf("delivered %v", got)
	}
}

func TestAFailedReadOnALiveSessionReleasesTheHeldRows(t *testing.T) {
	s := &historyStore{}
	fw := follow(t, s, kindUnknown, nil)
	fw.live(trow(100))
	arrived, open := s.holdNextCatchUp()

	fw.rejoin()
	recv(t, arrived, "the catch-up read")
	fw.live(trow(150)) // held behind the catch-up
	s.failNext(errors.New("history refused"))
	open()
	fw.settle()
	fw.live(trow(200))

	wantTsps(t, fw.got, 100, 150, 200)
	mustContain(t, fw.logs.String(), "Could not read the rows of table 't' stored while the connection was down; "+
		"they are not delivered: history refused")
}

func TestAReadCutOffByTheNextDropLeavesTheCatchUpToTheNextJoin(t *testing.T) {
	s := &historyStore{}
	fw := follow(t, s, kindUnknown, nil)
	fw.live(trow(100))
	s.add(trow(100), trow(110))
	arrived, open := s.holdNextCatchUp()

	fw.rejoin()
	recv(t, arrived, "the catch-up read")
	fw.live(trow(120))            // arrives on the session that drops next
	next := fw.conn.joins.Begin() // dropped and rejoined; this join's restore is still running
	s.failNext(errors.New("TransportLost"))
	open() // the read in flight fails
	fw.settle()
	wantTsps(t, fw.got, 100)
	if strings.Contains(fw.logs.String(), "not delivered") {
		t.Error("a read the next join catches up on was reported as lost")
	}

	s.add(trow(120))
	fw.conn.joins.Restored(next, nil)
	fw.settle()
	wantTsps(t, fw.got, 100, 110, 120)
}

func TestAPanickingHandlerDoesNotStopTheCatchUp(t *testing.T) {
	s := &historyStore{}
	got := &received{}
	fw := follow(t, s, kindUnknown, func(ev *Event) {
		if ev.Row()["tsp"] == int64(110) {
			panic("bad row")
		}
		got.handle(ev)
	})
	s.add(trow(110), trow(120))

	fw.rejoin()
	fw.settle()

	wantTsps(t, got, 120)
	mustContain(t, fw.logs.String(), "Table event handler for 'transformed.t' panicked: bad row")
}

func TestMoreRowsThanOneCatchUpReadsAreCutToTheNewest(t *testing.T) {
	s := &historyStore{}
	fw := follow(t, s, kindUnknown, nil, func(fl *tableFollower) { fl.limit = 3 })
	for tsp := range int64(5) {
		s.add(trow(tsp + 1))
	}

	fw.rejoin()
	fw.settle()

	wantTsps(t, fw.got, 3, 4, 5)
	mustContain(t, fw.logs.String(), "More than 3 rows of table 't' were stored while the connection was down; "+
		"the newest 3 are delivered")
}

func TestAnUnsubscribedTableIsNoLongerFollowed(t *testing.T) {
	s := &historyStore{}
	fw := follow(t, s, kindUnknown, nil)
	fw.live(trow(1))
	arrived, open := s.holdNextCatchUp()
	fw.rejoin()
	recv(t, arrived, "the catch-up read")
	fw.live(trow(2)) // held

	fw.fl.stop() // what TableSubscription.Unsubscribe does
	open()
	fw.settle()
	fw.rejoin()
	fw.settle()
	fw.live(trow(3))

	if n := len(s.allReads()); n != 2 {
		t.Errorf("%d reads, want the baseline and the catch-up under way only", n)
	}
	if n := fw.conn.joins.Hooks(); n != 0 {
		t.Errorf("%d join hooks left", n)
	}
	wantTsps(t, fw.got, 1)
}

// A row read and the same row from a live event fingerprint alike: the read
// returns null columns an event may leave out, and JSON does not tell an
// int64 from a float64 holding the same whole number.
func TestFingerprintTellsRowsApartByTheirValuesOnly(t *testing.T) {
	read := Row{"tsp": int64(5), "temp": float64(22), "note": nil,
		"json": map[string]any{"b": int64(1), "a": []any{float64(2), "x"}}}
	live := Row{"json": map[string]any{"a": []any{int64(2), "x"}, "b": float64(1)}, "temp": int64(22), "tsp": float64(5)}
	if fingerprint(read) != fingerprint(live) {
		t.Errorf("%s\n!= %s", fingerprint(read), fingerprint(live))
	}
	for _, other := range []Row{
		{"tsp": int64(5), "temp": 22.5, "json": live["json"]},
		{"tsp": int64(6), "temp": int64(22), "json": live["json"]},
		{"tsp": int64(5), "temp": int64(22), "json": live["json"], "note": "set"},
	} {
		if fingerprint(other) == fingerprint(live) {
			t.Errorf("%v fingerprints like %v", other, live)
		}
	}
}

// The rows of a catch-up are delivered in a turn of their own on the
// subscription's DeliveryGroup: never while another handler of the group
// runs, and before the events the group gets after the read returned.
func TestCaughtUpRowsAreDeliveredInTurnOnTheDeliveryGroup(t *testing.T) {
	s := &historyStore{}
	fw := follow(t, s, kindUnknown, nil)
	s.add(trow(10))
	arrived, open := s.holdNextCatchUp()
	fw.rejoin()
	recv(t, arrived, "the catch-up read")

	// Another subscription of the group is busy when the read returns.
	busy, done := make(chan struct{}), make(chan struct{})
	wampx.RunInGroup(fw.group, func() {
		close(busy)
		<-done
	})
	recv(t, busy, "the other handler")
	open()
	eventually(t, "the catch-up read to return", func() bool { return s.returned.Load() == 2 })
	time.Sleep(20 * time.Millisecond) // a delivery outside the group would happen meanwhile
	if n := len(fw.got.events()); n != 0 {
		t.Fatalf("%d rows delivered while another handler of the group ran", n)
	}
	close(done)
	fw.settle()
	wantTsps(t, fw.got, 10)
}

// The handler never runs concurrently with itself — not while a catch-up
// delivers, and not when events reach the follower outside its
// DeliveryGroup, as the fake connection fires them.
func TestTheHandlerNeverRunsConcurrentlyWithItself(t *testing.T) {
	s := &historyStore{}
	var active, overlaps atomic.Int32
	seen := map[int64]bool{} // written without a lock: -race reports overlapping calls
	fw := follow(t, s, kindUnknown, func(ev *Event) {
		if active.Add(1) > 1 {
			overlaps.Add(1)
		}
		defer active.Add(-1)
		seen[ev.Row()["tsp"].(int64)] = true
		runtime.Gosched()
	})
	for tsp := range int64(500) {
		s.add(trow(tsp + 1))
	}
	// fire delivers 4 x 50 rows from four goroutines at once.
	fire := func(base int64) {
		var wg sync.WaitGroup
		for i := range int64(4) {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for n := range int64(50) {
					fw.fl.deliverLive(&Event{Topic: "transformed.t", Args: []any{trow(base + i*100 + n)}})
				}
			}()
		}
		wg.Wait()
	}
	fw.rejoin()
	fire(1000) // while the catch-up delivers: held, or delivered once it is done
	fw.settle()
	fire(2000) // caught up: delivered at once
	if n := overlaps.Load(); n != 0 {
		t.Errorf("the handler ran concurrently with itself %d times", n)
	}
	if len(seen) != 900 {
		t.Errorf("%d distinct rows delivered, want 900", len(seen))
	}
}

// --- wiring: SubscribeToTable on the fake connection --------------------

func TestSubscribeToTableCatchesUpAfterARejoin(t *testing.T) {
	f := flock(t)
	s := &historyStore{rows: []Row{trow(50)}}
	f.own.callFn = s.answer
	got := &received{}
	ts, err := f.SubscribeToTable(bg, "t", got.handle)
	if err != nil {
		t.Fatal(err)
	}
	ts.follower.tasks.Wait()
	subs := f.own.allSubs()
	if len(subs) != 2 || subs[0].Topic != "transformed.t" || subs[1].Topic != "transformed.bulk.t" {
		t.Fatalf("subscriptions %#v", subs)
	}
	// The baseline: the newest row stored when subscribing.
	calls := f.own.callsTo("history.transformed.t")
	if len(calls) != 1 {
		t.Fatalf("%d reads, want the baseline read", len(calls))
	}
	if c := readCall(t, calls[0]); !reflect.DeepEqual(c.Args, []any{map[string]any{"limit": int64(1), "offset": int64(0)}}) ||
		c.Window != DefaultReconnectWindow {
		t.Errorf("baseline read %#v", c)
	}

	f.own.fire("transformed.t", &Event{Topic: "transformed.t", Args: []any{trow(60)}})
	s.add(trow(60), trow(70))
	f.own.rejoin()
	ts.follower.tasks.Wait()

	wantTsps(t, got, 60, 70)
	calls = f.own.callsTo("history.transformed.t")
	if len(calls) != 2 {
		t.Fatalf("%d reads, want the baseline and the catch-up", len(calls))
	}
	want := map[string]any{"limit": int64(catchUpLimit), "offset": int64(0), "timeRange": []any{int64(60), nil}}
	if c := readCall(t, calls[1]); !reflect.DeepEqual(c.Args, []any{want}) {
		t.Errorf("catch-up read %#v, want %#v", c.Args, want)
	}
	if ev := got.events()[1]; ev.Topic != "transformed.t" || !reflect.DeepEqual(ev.Args, []any{trow(70)}) ||
		ev.Kwargs != nil || ev.Details != nil {
		t.Errorf("caught-up event %#v", ev)
	}

	// Unsubscribe stops the following: the next rejoin reads nothing.
	if err := ts.Unsubscribe(bg); err != nil {
		t.Fatal(err)
	}
	if n := f.own.joins.Hooks(); n != 0 {
		t.Errorf("%d join hooks after Unsubscribe", n)
	}
	s.add(trow(80))
	f.own.rejoin()
	ts.follower.tasks.Wait()
	if n := len(f.own.callsTo("history.transformed.t")); n != 2 {
		t.Errorf("%d reads after Unsubscribe and a rejoin, want none more", n)
	}
	wantTsps(t, got, 60, 70)
}

// A rejoin while SubscribeToTable waits for its subscription is caught up
// on: the follower follows the joins from before it subscribes.
func TestARejoinWhileSubscribingIsCaughtUpOn(t *testing.T) {
	f := flock(t)
	s := &historyStore{}
	f.own.callFn = s.answer
	f.own.subscribeFn = func(topic string) error {
		if topic == "transformed.t" {
			// The connection drops and rejoins while the SUBSCRIBE is under
			// way; meanwhile a row is stored.
			s.add(trow(time.Now().UnixMilli()+1000, "value", "saved while subscribing"))
			f.own.rejoin()
		}
		return nil
	}
	got := &received{}
	ts, err := f.SubscribeToTable(bg, "t", got.handle)
	if err != nil {
		t.Fatal(err)
	}
	ts.follower.tasks.Wait()
	if evs := got.events(); len(evs) != 1 || evs[0].Row()["value"] != "saved while subscribing" {
		t.Errorf("delivered %v", got.args())
	}
}

// A subscription that fails leaves no follower behind.
func TestAFailedSubscribeToTableStopsItsFollower(t *testing.T) {
	for _, failing := range []string{"transformed.t", "transformed.bulk.t"} {
		t.Run(failing, func(t *testing.T) {
			f := flock(t)
			f.own.subscribeFn = func(topic string) error {
				if topic == failing {
					return wampErr("wamp.error.not_authorized")
				}
				return nil
			}
			if _, err := f.SubscribeToTable(bg, "t", func(*Event) { t.Error("handler called") }); err == nil {
				t.Fatal("SubscribeToTable succeeded")
			}
			if n := f.own.joins.Hooks(); n != 0 {
				t.Errorf("%d join hooks left", n)
			}
			f.own.rejoin()
			time.Sleep(20 * time.Millisecond) // a catch-up would read meanwhile
			for _, c := range f.own.callsTo("history.transformed.t") {
				if wire, _ := c.Args[0].(map[string]any); wire["limit"] != int64(1) {
					t.Errorf("a catch-up read %#v after the failed subscription", wire)
				}
			}
		})
	}
}

func TestConsumedAppSubscribeToTableCatchesUpAfterARejoin(t *testing.T) {
	c := newConsumer(t)
	app, err := c.ConnectToApp(bg, "weatherstation")
	if err != nil {
		t.Fatal(err)
	}
	conn := c.factory.all()[0]
	s := &historyStore{rows: []Row{trow(50)}}
	conn.callFn = s.answer
	got := &received{}
	ts, err := app.SubscribeToTable(bg, "readings", got.handle)
	if err != nil {
		t.Fatal(err)
	}
	ts.follower.tasks.Wait()
	conn.fire("transformed.readings", &Event{Topic: "transformed.readings", Args: []any{trow(60)}})
	s.add(trow(60), trow(70))
	conn.rejoin()
	ts.follower.tasks.Wait()
	wantTsps(t, got, 60, 70)
	calls := conn.callsTo("history.transformed.readings")
	if len(calls) != 2 || !reflect.DeepEqual(readCall(t, calls[1]).Args, []any{map[string]any{
		"limit": int64(catchUpLimit), "offset": int64(0), "timeRange": []any{int64(60), nil}}}) {
		t.Errorf("reads on the provider's realm %#v", calls)
	}
	if len(c.own.callsTo("history.transformed.readings")) != 0 {
		t.Error("read on the own realm")
	}

	// The catalog says what is a transform: it is never read.
	transform, err := app.SubscribeToTable(bg, "hourly_agg", got.handle)
	if err != nil {
		t.Fatal(err)
	}
	conn.rejoin()
	transform.follower.tasks.Wait()
	if n := len(conn.callsTo("history.transformed.hourly_agg")); n != 0 {
		t.Errorf("%d reads of a transform", n)
	}

	// Closing the app ends the following.
	if err := app.Close(bg); err != nil {
		t.Fatal(err)
	}
	if ts.follower.ctx.Err() == nil || transform.follower.ctx.Err() == nil {
		t.Error("the followers outlive Close")
	}
}

// A table subscription issued while there is no session — here while Start
// waits for the first join — waits for the session and is made then, and its
// follower starts there: a row stored after the call but before the join is
// not caught up on (the baseline read finds it), and the rows stored after
// the subscription are, after the next reconnect.
func TestSubscribeToTableWithoutASessionWaitsForIt(t *testing.T) {
	f := unstartedFlock(t)
	s := &historyStore{}
	join := make(chan struct{})
	f.own.callFn = s.answer
	f.own.startFn = func(ctx context.Context, _ wamp.Config) error {
		select {
		case <-join:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	started := make(chan error, 1)
	go func() { started <- f.Start(bg) }()
	eventually(t, "Start to configure the connection", func() bool {
		select {
		case <-f.ready:
			return true
		default:
			return false
		}
	})

	got := &received{}
	var ts *TableSubscription
	subscribed := make(chan error, 1)
	go func() {
		var err error
		ts, err = f.SubscribeToTable(bg, "t", got.handle)
		subscribed <- err
	}()
	time.Sleep(50 * time.Millisecond) // the subscription waits for the session
	if len(subscribed) != 0 || len(f.own.allSubs()) != 0 {
		t.Fatal("subscribed without a session")
	}
	now := time.Now().UnixMilli()
	s.add(trow(now + 60_000)) // stored after the call, before the subscription is made
	close(join)
	if err := recv(t, started, "Start"); err != nil {
		t.Fatal(err)
	}
	if err := recv(t, subscribed, "SubscribeToTable"); err != nil {
		t.Fatal(err)
	}
	ts.follower.tasks.Wait()
	if n := len(got.events()); n != 0 {
		t.Fatalf("caught up on %d row(s) stored before the subscription was made", n)
	}
	s.add(trow(now + 120_000))
	f.own.rejoin()
	ts.follower.tasks.Wait()
	wantTsps(t, got, now+120_000)
}

// So does a consumed app's table subscription issued in a reconnect gap of its
// connection.
func TestConsumedAppSubscribeToTableWithoutASessionWaitsForIt(t *testing.T) {
	c := newConsumer(t)
	app, err := c.ConnectToApp(bg, "weatherstation")
	if err != nil {
		t.Fatal(err)
	}
	conn := c.factory.all()[0]
	s := &historyStore{}
	conn.mu.Lock()
	conn.callFn = s.answer
	conn.mu.Unlock()
	conn.drop()

	got := &received{}
	var ts *TableSubscription
	subscribed := make(chan error, 1)
	go func() {
		var err error
		ts, err = app.SubscribeToTable(bg, "readings", got.handle)
		subscribed <- err
	}()
	time.Sleep(50 * time.Millisecond) // the subscription waits for the session
	if len(subscribed) != 0 || len(conn.allSubs()) != 0 {
		t.Fatal("subscribed without a session")
	}
	s.add(trow(time.Now().UnixMilli() + 60_000)) // stored after the call, before the subscription is made
	conn.reconnect()
	if err := recv(t, subscribed, "SubscribeToTable"); err != nil {
		t.Fatal(err)
	}
	ts.follower.tasks.Wait()
	if n := len(got.events()); n != 0 {
		t.Errorf("caught up on %d row(s) stored before the subscription was made", n)
	}
}

// --- wiring: a real connection and router -------------------------------

// fakeTable is a table of a data backend on the test router: it answers
// history.transformed.<table> from store, as the data backend reads, and
// publishes the rows it stores on transformed.<table>.
type fakeTable struct {
	t       *testing.T
	cli     *client.Client
	topic   string
	store   historyStore
	mu      sync.Mutex
	queries []map[string]any // the wire payload of every read
}

func newFakeTable(t *testing.T, tr *testRouter, table string) *fakeTable {
	t.Helper()
	ft := &fakeTable{t: t, cli: tr.local(t), topic: "transformed." + table}
	err := ft.cli.Register("history.transformed."+table, func(ctx context.Context, inv *nxwamp.Invocation) client.InvokeResult {
		wire, _ := nxwamp.AsDict(inv.Arguments[0])
		ft.mu.Lock()
		ft.queries = append(ft.queries, wire)
		ft.mu.Unlock()
		limit, _ := nxwamp.AsInt64(wire["limit"])
		q := &TableQueryParams{Limit: int(limit)}
		if bounds, _ := nxwamp.AsList(wire["timeRange"]); len(bounds) == 2 {
			start, _ := nxwamp.AsInt64(bounds[0])
			q.TimeRange = &TimeRange{Start: start}
		}
		rows, err := ft.store.read(ctx, table, q)
		if err != nil {
			return client.InvokeResult{Err: "test.error.read", Args: nxwamp.List{err.Error()}}
		}
		list := make(nxwamp.List, len(rows))
		for i, r := range rows {
			list[i] = map[string]any(r)
		}
		return client.InvokeResult{Args: nxwamp.List{list}}
	}, nil)
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	return ft
}

// put stores rows and publishes each on the table's realtime feed.
func (ft *fakeTable) put(rows ...Row) {
	for _, r := range rows {
		ft.store.add(r)
		publish(ft.t, ft.cli, ft.topic, map[string]any(r))
	}
}

func (ft *fakeTable) reads() []map[string]any {
	ft.mu.Lock()
	defer ft.mu.Unlock()
	return slices.Clone(ft.queries)
}

// wireInts reads the integers of a read's wire payload as the router decoded
// them.
func wireInts(t *testing.T, wire map[string]any, keys ...string) []int64 {
	t.Helper()
	var out []int64
	for _, k := range keys {
		n, ok := nxwamp.AsInt64(wire[k])
		if !ok {
			t.Fatalf("%s of %v is not an integer", k, wire)
		}
		out = append(out, n)
	}
	return out
}

// Through a real connection and router: rows stored while the connection
// was down reach the handler after the reconnect, oldest first; a row of the
// new session that arrives while they are read waits for them and is
// delivered once, although the read returns it too.
func TestSubscribeToTableCatchesUpAfterARealReconnect(t *testing.T) {
	tr := newTestRouter(t, true)
	ft := newFakeTable(t, tr, "t")
	ft.store.add(trow(50))
	f := startedRealFlock(t, tr)
	ctx := timeout(t, 30*time.Second)

	group := wamp.NewDeliveryGroup()
	got := &received{}
	ts, err := f.SubscribeToTable(ctx, "t", got.handle, SubscribeOptions{Group: group})
	if err != nil {
		t.Fatal(err)
	}
	// The barrier's event comes behind every event published before it, in
	// the same group: once it is handled, they have been.
	barrier := make(chan struct{}, 4)
	if _, err := f.Subscribe(ctx, "test.barrier", func(*Event) { barrier <- struct{}{} }, SubscribeOptions{Group: group}); err != nil {
		t.Fatal(err)
	}
	eventually(t, "the baseline read", func() bool { return len(ft.reads()) == 1 })
	ft.put(trow(60))
	eventually(t, "the live row", func() bool { return len(got.events()) == 1 })

	// The connection drops, and rows are stored while it is down.
	tr.ln.drop(true)
	eventually(t, "the connection to drop", func() bool { return !f.IsConnected() })
	ft.put(trow(70), trow(80))
	arrived, open := ft.store.holdNextCatchUp()
	tr.ln.resume()
	recv(t, arrived, "the catch-up read after the reconnect")

	// A row of the new session arrives while the catch-up read is under way.
	ft.put(trow(90))
	publish(t, ft.cli, "test.barrier")
	recv(t, barrier, "the barrier")
	wantTsps(t, got, 60)

	open()
	eventually(t, "the rows stored while down", func() bool { return len(got.events()) == 4 })
	ft.put(trow(100))
	eventually(t, "the next live row", func() bool { return len(got.events()) == 5 })
	wantTsps(t, got, 60, 70, 80, 90, 100) // 90 once: read and live

	reads := ft.reads()
	if len(reads) != 2 {
		t.Fatalf("reads %v, want the baseline and the catch-up", reads)
	}
	if n := wireInts(t, reads[0], "limit", "offset"); !slices.Equal(n, []int64{1, 0}) || len(reads[0]) != 2 {
		t.Errorf("baseline read %v", reads[0])
	}
	bounds, _ := nxwamp.AsList(reads[1]["timeRange"])
	if n := wireInts(t, reads[1], "limit", "offset"); !slices.Equal(n, []int64{catchUpLimit, 0}) ||
		len(bounds) != 2 || bounds[1] != nil {
		t.Errorf("catch-up read %v", reads[1])
	} else if start, _ := nxwamp.AsInt64(bounds[0]); start != 60 {
		t.Errorf("catch-up read starts at %v, want 60", bounds[0])
	}
	if err := ts.Unsubscribe(ctx); err != nil {
		t.Fatal(err)
	}
}

// Through a real connection and router: a row the catch-up read returns, and
// whose live event the data backend publishes only after the catch-up is
// done, reaches the handler once.
func TestALateLiveEventOfACaughtUpRowIsDroppedAfterARealReconnect(t *testing.T) {
	tr := newTestRouter(t, true)
	ft := newFakeTable(t, tr, "t")
	f := startedRealFlock(t, tr)
	ctx := timeout(t, 30*time.Second)
	group := wamp.NewDeliveryGroup()
	got := &received{}
	ts, err := f.SubscribeToTable(ctx, "t", got.handle, SubscribeOptions{Group: group})
	if err != nil {
		t.Fatal(err)
	}
	barrier := make(chan struct{}, 4) // see TestSubscribeToTableCatchesUpAfterARealReconnect
	if _, err := f.Subscribe(ctx, "test.barrier", func(*Event) { barrier <- struct{}{} }, SubscribeOptions{Group: group}); err != nil {
		t.Fatal(err)
	}
	eventually(t, "the baseline read", func() bool { return ft.store.returned.Load() == 1 })
	ft.put(trow(100))
	eventually(t, "the live row", func() bool { return len(got.events()) == 1 })

	tr.ln.drop(true)
	eventuallyWithin(t, 5*time.Second, "the connection to drop", func() bool { return !f.IsConnected() })
	// Stored while the connection is down, and as the catch-up reads: the
	// read returns both, and the live event of the second is still to come.
	ft.store.add(trow(110), trow(120))
	tr.ln.resume()
	eventuallyWithin(t, 10*time.Second, "the catch-up", func() bool { return len(got.events()) == 3 })

	publish(t, ft.cli, ft.topic, map[string]any(trow(120))) // the late live event
	ft.put(trow(130))
	publish(t, ft.cli, "test.barrier")
	recv(t, barrier, "the barrier")
	wantTsps(t, got, 100, 110, 120, 130)
	if err := ts.Unsubscribe(ctx); err != nil {
		t.Fatal(err)
	}
}

// Through a real connection and router: a table subscription issued in a
// reconnect gap waits for the session and is made then. A row stored after
// the call but before the reconnect is not caught up on; the rows stored
// after the subscription are delivered, live or after the next reconnect.
func TestSubscribeToTableInAReconnectGapWaitsForTheSession(t *testing.T) {
	tr := newTestRouter(t, true)
	ft := newFakeTable(t, tr, "t")
	f := startedRealFlock(t, tr)
	ctx := timeout(t, 30*time.Second)
	tr.ln.drop(true)
	eventuallyWithin(t, 5*time.Second, "the connection to drop", func() bool { return !f.IsConnected() })

	group := wamp.NewDeliveryGroup()
	got := &received{}
	var ts *TableSubscription
	subscribed := make(chan error, 1)
	go func() {
		var err error
		ts, err = f.SubscribeToTable(ctx, "t", got.handle, SubscribeOptions{Group: group})
		subscribed <- err
	}()
	time.Sleep(100 * time.Millisecond) // the subscription waits for the session
	now := time.Now().UnixMilli()
	ft.store.add(trow(now + 60_000)) // stored after the call, before the subscription is made
	tr.ln.resume()
	if err := recvWithin(t, subscribed, 15*time.Second, "SubscribeToTable"); err != nil {
		t.Fatal(err)
	}
	ts.follower.tasks.Wait()          // the baseline read
	barrier := make(chan struct{}, 4) // see TestSubscribeToTableCatchesUpAfterARealReconnect
	if _, err := f.Subscribe(ctx, "test.barrier", func(*Event) { barrier <- struct{}{} }, SubscribeOptions{Group: group}); err != nil {
		t.Fatal(err)
	}
	ft.put(trow(now + 90_000))
	publish(t, ft.cli, "test.barrier")
	recv(t, barrier, "the barrier")
	wantTsps(t, got, now+90_000)

	// The next reconnect catches up from there.
	tr.ln.drop(true)
	eventuallyWithin(t, 5*time.Second, "the connection to drop again", func() bool { return !f.IsConnected() })
	ft.store.add(trow(now + 120_000))
	tr.ln.resume()
	eventuallyWithin(t, 10*time.Second, "the catch-up", func() bool { return len(got.events()) == 2 })
	wantTsps(t, got, now+90_000, now+120_000)
	if err := ts.Unsubscribe(ctx); err != nil {
		t.Fatal(err)
	}
}
