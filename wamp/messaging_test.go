package wamp

import (
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ironflock/nexus/v3/client"
	nxwamp "github.com/ironflock/nexus/v3/wamp"
)

func eventCollector(buffer int) (EventHandler, chan *Event) {
	ch := make(chan *Event, buffer)
	return func(ev *Event) { ch <- ev }, ch
}

func echoHandler(prefix string) InvocationHandler {
	return func(_ context.Context, inv *Invocation) (any, error) {
		return prefix + fmt.Sprint(inv.Args...), nil
	}
}

// localCall calls from a trusted local client.
func localCall(t *testing.T, cli *client.Client, proc string, args ...any) (*nxwamp.Result, error) {
	t.Helper()
	return cli.Call(ctxTimeout(t, 5*time.Second), proc, nil, args, nil, nil)
}

func TestSubscribePublishRoundTrip(t *testing.T) {
	tr := newTestRouter(t, true)
	c, _ := startTestConn(t, tr)
	ctx := ctxTimeout(t, 5*time.Second)

	handler, events := eventCollector(4)
	sub, err := c.Subscribe(ctx, "app.data", handler, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !sub.Active() || sub.Topic() != "app.data" || sub.Match() != "" {
		t.Fatalf("subscription: active=%v topic=%q match=%q", sub.Active(), sub.Topic(), sub.Match())
	}
	f := false
	err = c.Publish(ctx, "app.data", []any{1, "two"}, map[string]any{"k": true},
		&PublishOptions{Acknowledge: true, ExcludeMe: &f}, 0)
	if err != nil {
		t.Fatal(err)
	}
	ev := recv(t, events, "own event")
	if ev.Topic != "app.data" || !reflect.DeepEqual(ev.Args, []any{int64(1), "two"}) ||
		!reflect.DeepEqual(ev.Kwargs, map[string]any{"k": true}) {
		t.Fatalf("event = %+v", ev)
	}

	// By default the publisher is excluded.
	if err := c.Publish(ctx, "app.data", []any{2}, nil, &PublishOptions{Acknowledge: true}, 0); err != nil {
		t.Fatal(err)
	}
	noRecv(t, events, "excluded own event")
}

func TestPatternSubscriptionEventTopic(t *testing.T) {
	tr := newTestRouter(t, true)
	c, _ := startTestConn(t, tr)
	handler, events := eventCollector(4)
	sub, err := c.Subscribe(ctxTimeout(t, 5*time.Second), "app.sensors", handler, &SubscribeOptions{Match: "prefix"})
	if err != nil {
		t.Fatal(err)
	}
	if sub.Match() != "prefix" {
		t.Fatalf("Match() = %q", sub.Match())
	}
	localPublish(t, tr.local(t), "app.sensors.temp", nxwamp.List{21.5}, nil)
	ev := recv(t, events, "pattern event")
	if ev.Topic != "app.sensors.temp" {
		t.Fatalf("Event.Topic = %q, want the concrete topic", ev.Topic)
	}
	if ev.Details["topic"] != "app.sensors.temp" {
		t.Fatalf("details = %v", ev.Details)
	}

	_, err = c.Subscribe(context.Background(), "app.sensors", func(*Event) {}, nil)
	if err == nil || !strings.Contains(err.Error(), "match policy") {
		t.Fatalf("conflicting match policy = %v", err)
	}
}

func TestMultipleHandlersShareOneSubscription(t *testing.T) {
	tr := newTestRouter(t, true)
	c, _ := startTestConn(t, tr)
	pub := tr.local(t)
	ctx := ctxTimeout(t, 5*time.Second)

	h1, ev1 := eventCollector(8)
	h2, ev2 := eventCollector(8)
	s1, err := c.Subscribe(ctx, "shared.topic", h1, nil)
	if err != nil {
		t.Fatal(err)
	}
	s2, err := c.Subscribe(ctx, "shared.topic", h2, nil)
	if err != nil {
		t.Fatal(err)
	}
	wampSubID := func() (nxwamp.ID, bool) { return c.currentSession().cli.SubscriptionID("shared.topic") }
	id, ok := wampSubID()
	if !ok {
		t.Fatal("no WAMP subscription")
	}

	localPublish(t, pub, "shared.topic", nxwamp.List{1}, nil)
	recv(t, ev1, "event for handler 1")
	recv(t, ev2, "event for handler 2")

	// Unsubscribing one handler keeps the other and the WAMP subscription.
	if err := s1.Unsubscribe(ctx); err != nil {
		t.Fatal(err)
	}
	if s1.Active() || !s2.Active() {
		t.Fatalf("active: s1=%v s2=%v", s1.Active(), s2.Active())
	}
	if id2, ok := wampSubID(); !ok || id2 != id {
		t.Fatal("WAMP subscription changed")
	}
	localPublish(t, pub, "shared.topic", nxwamp.List{2}, nil)
	if got := recv(t, ev2, "event for handler 2").Args[0]; got != int64(2) {
		t.Fatalf("got %v", got)
	}
	noRecv(t, ev1, "event for the removed handler")

	// Stale handle: a no-op.
	if err := c.Unsubscribe(ctx, s1); err != nil {
		t.Fatalf("second Unsubscribe = %v", err)
	}

	// The last handler drops the WAMP subscription.
	if err := c.Unsubscribe(ctx, s2); err != nil {
		t.Fatal(err)
	}
	if _, ok := wampSubID(); ok {
		t.Fatal("WAMP subscription kept after the last handler left")
	}
	localPublish(t, pub, "shared.topic", nxwamp.List{3}, nil)
	noRecv(t, ev2, "event after unsubscribe")

	// UnsubscribeTopic removes every handler of a topic.
	s3, _ := c.Subscribe(ctx, "other.topic", h1, nil)
	s4, _ := c.Subscribe(ctx, "other.topic", h2, nil)
	if err := c.UnsubscribeTopic(ctx, "other.topic"); err != nil {
		t.Fatal(err)
	}
	if s3.Active() || s4.Active() {
		t.Fatal("still active after UnsubscribeTopic")
	}
	if err := c.UnsubscribeTopic(ctx, "never.subscribed"); err != nil {
		t.Fatal(err)
	}
	if err := c.Unsubscribe(ctx, nil); err != nil {
		t.Fatal(err)
	}
}

func TestEventHandlerCanCallWithoutDeadlock(t *testing.T) {
	tr := newTestRouter(t, true)
	c, _ := startTestConn(t, tr)
	callee := tr.local(t)
	if err := callee.Register("svc.double", func(_ context.Context, inv *nxwamp.Invocation) client.InvokeResult {
		n, _ := nxwamp.AsInt64(inv.Arguments[0])
		return client.InvokeResult{Args: nxwamp.List{2 * n}}
	}, nil); err != nil {
		t.Fatal(err)
	}

	results := make(chan any, 1)
	_, err := c.Subscribe(ctxTimeout(t, 5*time.Second), "trigger", func(ev *Event) {
		res, err := c.Call(context.Background(), "svc.double", ev.Args, nil, nil, 0)
		if err != nil {
			results <- err
			return
		}
		// And subscribe from inside a handler, too.
		if _, err := c.Subscribe(context.Background(), "nested.topic", func(*Event) {}, nil); err != nil {
			results <- err
			return
		}
		results <- res.Value()
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	localPublish(t, callee, "trigger", nxwamp.List{21}, nil)
	if got := recv(t, results, "handler result"); got != int64(42) {
		t.Fatalf("handler got %v", got)
	}
}

// A panicking invocation handler is answered with wamp.error.runtime_error
// and a message naming the panic, in valid UTF-8 even when the panic value is
// not: autobahn-python (the Python SDK) drops its session over an invalid
// string.
func TestHandlerPanicMessageIsValidUTF8(t *testing.T) {
	tr := newTestRouter(t, true)
	c, _ := startTestConn(t, tr)
	caller := tr.local(t)
	if _, err := c.Register(ctxTimeout(t, 5*time.Second), "panicky", func(context.Context, *Invocation) (any, error) {
		panic("bad frame \xff\xfe")
	}, nil); err != nil {
		t.Fatal(err)
	}
	_, err := localCall(t, caller, "panicky")
	var rpcErr client.RPCError
	if !errors.As(err, &rpcErr) || string(rpcErr.Err.Error) != URIRuntimeError {
		t.Fatalf("Call = %v, want %s", err, URIRuntimeError)
	}
	want := nxwamp.List{"procedure 'panicky' panicked: bad frame ��"}
	if !reflect.DeepEqual(rpcErr.Err.Arguments, want) {
		t.Fatalf("error args = %+q, want %+q", rpcErr.Err.Arguments, want)
	}
}

func TestEventsDeliveredInOrderAndPanicsRecovered(t *testing.T) {
	tr := newTestRouter(t, true)
	c, logs := startTestConn(t, tr)
	pub := tr.local(t)
	const n = 200

	var mu sync.Mutex
	var got []int64
	done := make(chan struct{})
	_, err := c.Subscribe(ctxTimeout(t, 5*time.Second), "ordered", func(ev *Event) {
		v := ev.Args[0].(int64)
		if v == 0 {
			panic("handler boom")
		}
		time.Sleep(time.Duration(v%3) * 100 * time.Microsecond) // slow handler: events queue up
		mu.Lock()
		got = append(got, v)
		if len(got) == n-1 {
			close(done)
		}
		mu.Unlock()
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	for i := range n {
		if err := pub.Publish("ordered", nil, nxwamp.List{i}, nil); err != nil {
			t.Fatal(err)
		}
	}
	recv(t, done, "all events")
	mu.Lock()
	for i, v := range got {
		if v != int64(i+1) {
			t.Fatalf("event %d out of order: %v", i, got[:i+1])
		}
	}
	mu.Unlock()
	if !strings.Contains(logs.String(), "handler boom") {
		t.Fatal("panic not logged")
	}
	if !c.IsOpen() {
		t.Fatal("connection closed by a panicking handler")
	}
}

func TestReconnectRestoresSubscriptionsAndRegistrations(t *testing.T) {
	tr := newTestRouter(t, true)
	connects := make(chan struct{}, 8)
	c, logs := startTestConn(t, tr, func(cfg *Config, _ *Connection) {
		cfg.OnConnect = func() { connects <- struct{}{} }
	})
	recv(t, connects, "OnConnect")
	ctx := ctxTimeout(t, 10*time.Second)

	hA, evA := eventCollector(8)
	hB, evB := eventCollector(8)
	subA, err := c.Subscribe(ctx, "restore.a", hA, nil)
	if err != nil {
		t.Fatal(err)
	}
	subB, err := c.Subscribe(ctx, "restore.b", hB, &SubscribeOptions{Match: "prefix"})
	if err != nil {
		t.Fatal(err)
	}
	reg1, err := c.Register(ctx, "restore.p1", echoHandler("p1:"), nil)
	if err != nil {
		t.Fatal(err)
	}
	reg2, err := c.Register(ctx, "restore.p2", echoHandler("p2:"), nil)
	if err != nil {
		t.Fatal(err)
	}

	for round := range 2 {
		if round == 0 {
			tr.ln.dropAll() // transport loss
		} else {
			tr.removeRealm() // router-side GOODBYE, then no_such_realm until it is back
			eventually(t, 2*time.Second, "session down", func() bool { return !c.IsOpen() })
			if subA.Active() || reg1.Active() {
				t.Fatal("handles active while the connection is down")
			}
			tr.addRealm()
		}
		recv(t, connects, "OnConnect after reconnect")
		for _, a := range []interface{ Active() bool }{subA, subB, reg1, reg2} {
			if !a.Active() {
				t.Fatalf("round %d: not restored: %+v", round, a)
			}
		}
		other := tr.local(t)
		localPublish(t, other, "restore.a", nxwamp.List{round}, nil)
		localPublish(t, other, "restore.b.x", nxwamp.List{round}, nil)
		if ev := recv(t, evA, "event a"); ev.Args[0] != int64(round) {
			t.Fatalf("event a = %v", ev.Args)
		}
		if ev := recv(t, evB, "event b"); ev.Topic != "restore.b.x" {
			t.Fatalf("event b = %+v", ev)
		}
		for _, p := range []string{"restore.p1", "restore.p2"} {
			res, err := localCall(t, other, p, round)
			if err != nil {
				t.Fatalf("round %d: call %s: %v", round, p, err)
			}
			if want := fmt.Sprintf("%s:%d", p[len("restore."):], round); res.Arguments[0] != want {
				t.Fatalf("call %s = %v, want %v", p, res.Arguments, want)
			}
		}
	}
	if !strings.Contains(logs.String(), "Resubscribing") || !strings.Contains(logs.String(), "Re-registering") {
		t.Fatalf("restore not logged:\n%s", logs)
	}

	// Handles stay valid across reconnects.
	if err := subA.Unsubscribe(ctx); err != nil {
		t.Fatal(err)
	}
	if err := reg1.Unregister(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := localCall(t, tr.local(t), "restore.p1", 1); err == nil {
		t.Fatal("unregistered procedure still callable")
	}
	tr.ln.dropAll()
	recv(t, connects, "OnConnect")
	if _, ok := c.currentSession().cli.SubscriptionID("restore.a"); ok {
		t.Fatal("removed subscription restored")
	}
	if _, ok := c.currentSession().cli.RegistrationID("restore.p1"); ok {
		t.Fatal("removed registration restored")
	}
}

// A failed restore is retried after the next reconnect, too (here before any
// retry while the session lasts).
func TestFailedRestoreIsRetriedNextReconnect(t *testing.T) {
	tr := newTestRouter(t, true)
	connects := make(chan struct{}, 8)
	c, logs := startTestConn(t, tr, restoreRetry(time.Hour, time.Hour), func(cfg *Config, _ *Connection) {
		cfg.OnConnect = func() { connects <- struct{}{} }
	})
	recv(t, connects, "OnConnect")
	ctx := ctxTimeout(t, 10*time.Second)

	hF, evF := eventCollector(8)
	flaky, err := c.Subscribe(ctx, "flaky.topic", hF, nil)
	if err != nil {
		t.Fatal(err)
	}
	reg, err := c.Register(ctx, "flaky.proc", echoHandler(""), nil)
	if err != nil {
		t.Fatal(err)
	}
	steady, err := c.Subscribe(ctx, "steady.topic", func(*Event) {}, nil)
	if err != nil {
		t.Fatal(err)
	}

	tr.authz.setDeny("subscribe:flaky.topic", true)
	tr.authz.setDeny("register:flaky.proc", true)
	tr.ln.dropAll()
	recv(t, connects, "OnConnect")
	if flaky.Active() || reg.Active() || !steady.Active() {
		t.Fatalf("after refused restore: flaky=%v reg=%v steady=%v", flaky.Active(), reg.Active(), steady.Active())
	}
	if !strings.Contains(logs.String(), "Failed to restore subscription") ||
		!strings.Contains(logs.String(), "Failed to restore registration") {
		t.Fatalf("failed restore not logged:\n%s", logs)
	}

	tr.authz.setDeny("subscribe:flaky.topic", false)
	tr.authz.setDeny("register:flaky.proc", false)
	tr.ln.dropAll()
	recv(t, connects, "OnConnect")
	if !flaky.Active() || !reg.Active() {
		t.Fatal("entries not restored on the next reconnect")
	}
	pub := tr.local(t)
	localPublish(t, pub, "flaky.topic", nxwamp.List{"back"}, nil)
	recv(t, evF, "event after the retried restore")
	if _, err := localCall(t, pub, "flaky.proc", "x"); err != nil {
		t.Fatal(err)
	}
}

// restoreRetry sets the backoff of the retries of a failed restore.
func restoreRetry(first, maxDelay time.Duration) connOption {
	return func(_ *Config, c *Connection) {
		c.t.restoreRetryFirstDelay, c.t.restoreRetryMaxDelay = first, maxDelay
	}
}

// failedRestore subscribes topic and registers proc, then makes their
// restore after a reconnect fail: the router refuses them until the test
// allows them again.
func failedRestore(t *testing.T, tr *testRouter, c *Connection, connects <-chan struct{}, topic, proc string) (*Subscription, chan *Event, *Registration) {
	t.Helper()
	ctx := ctxTimeout(t, 10*time.Second)
	handler, events := eventCollector(8)
	sub, err := c.Subscribe(ctx, topic, handler, nil)
	if err != nil {
		t.Fatal(err)
	}
	reg, err := c.Register(ctx, proc, echoHandler("ok:"), nil)
	if err != nil {
		t.Fatal(err)
	}
	tr.authz.setDeny("subscribe:"+topic, true)
	tr.authz.setDeny("register:"+proc, true)
	tr.ln.dropAll()
	recv(t, connects, "OnConnect after the reconnect")
	if sub.Active() || reg.Active() {
		t.Fatalf("restored although refused: subscription %v, registration %v", sub.Active(), reg.Active())
	}
	return sub, events, reg
}

// A restore the router refuses is retried while the session stays up:
// the router refuses a REGISTER of a device function, for one, while
// its identity check cannot reach the authorizer (it fails closed), and an
// entry refused at restore was accepted before (finding 40).
func TestFailedRestoreIsRetriedWhileTheSessionLasts(t *testing.T) {
	tr := newTestRouter(t, true)
	connects := make(chan struct{}, 8)
	c, logs := startTestConn(t, tr, restoreRetry(20*time.Millisecond, 80*time.Millisecond), func(cfg *Config, _ *Connection) {
		cfg.OnConnect = func() { connects <- struct{}{} }
	})
	recv(t, connects, "OnConnect")
	sub, events, reg := failedRestore(t, tr, c, connects, "retry.topic", "retry.proc")

	// Refused again, at least once each.
	eventually(t, 5*time.Second, "retries", func() bool {
		return tr.authz.count("subscribe:retry.topic") >= 3 && tr.authz.count("register:retry.proc") >= 3
	})
	tr.authz.setDeny("subscribe:retry.topic", false)
	tr.authz.setDeny("register:retry.proc", false)
	eventually(t, 5*time.Second, "the entries to be restored", func() bool { return sub.Active() && reg.Active() })

	pub := tr.local(t)
	localPublish(t, pub, "retry.topic", nxwamp.List{"back"}, nil)
	recv(t, events, "an event on the restored subscription")
	if res, err := localCall(t, pub, "retry.proc", 1); err != nil || res.Arguments[0] != "ok:1" {
		t.Fatalf("call of the restored registration = %v, %v", res, err)
	}
	noRecv(t, connects, "a reconnect")
	out := logs.String()
	if n := strings.Count(out, "level=WARN msg=\"Failed to restore subscription"); n != 1 {
		t.Fatalf("refused subscription restore warned %d times, want once:\n%s", n, out)
	}
	if n := strings.Count(out, "level=WARN msg=\"Failed to restore registration"); n != 1 {
		t.Fatalf("refused registration restore warned %d times, want once:\n%s", n, out)
	}
	if !strings.Contains(out, "Subscription restored") || !strings.Contains(out, "Registration restored") {
		t.Fatalf("the late restores not logged:\n%s", out)
	}
}

// What is removed meanwhile is not retried, and once nothing is left the
// retries end.
func TestRestoreRetriesSkipRemovedEntries(t *testing.T) {
	tr := newTestRouter(t, true)
	connects := make(chan struct{}, 8)
	c, _ := startTestConn(t, tr, restoreRetry(20*time.Millisecond, 40*time.Millisecond), func(cfg *Config, _ *Connection) {
		cfg.OnConnect = func() { connects <- struct{}{} }
	})
	recv(t, connects, "OnConnect")
	sub, _, reg := failedRestore(t, tr, c, connects, "removed.topic", "removed.proc")
	eventually(t, 5*time.Second, "a retry", func() bool {
		return tr.authz.count("subscribe:removed.topic") >= 3 && tr.authz.count("register:removed.proc") >= 3
	})

	ctx := ctxTimeout(t, 5*time.Second)
	if err := sub.Unsubscribe(ctx); err != nil {
		t.Fatal(err)
	}
	if err := reg.Unregister(ctx); err != nil {
		t.Fatal(err)
	}
	subscribes, registers := tr.authz.count("subscribe:removed.topic"), tr.authz.count("register:removed.proc")
	time.Sleep(200 * time.Millisecond) // several retry periods
	if n, m := tr.authz.count("subscribe:removed.topic"), tr.authz.count("register:removed.proc"); n != subscribes || m != registers {
		t.Fatalf("retried after the removal: %d SUBSCRIBE, %d REGISTER", n-subscribes, m-registers)
	}
	eventually(t, 5*time.Second, "the retries to end", func() bool { return countGoroutines("(*Connection).retryRestore") == 0 })
}

// The retries end with their session — the next join's restore takes over —
// and with Stop, even while they wait for their next attempt.
func TestRestoreRetriesEndWithTheSession(t *testing.T) {
	tr := newTestRouter(t, true)
	connects := make(chan struct{}, 8)
	c, _ := startTestConn(t, tr, restoreRetry(time.Hour, time.Hour), func(cfg *Config, _ *Connection) {
		cfg.OnConnect = func() { connects <- struct{}{} }
	})
	recv(t, connects, "OnConnect")
	sub, _, reg := failedRestore(t, tr, c, connects, "ended.topic", "ended.proc")
	eventually(t, 5*time.Second, "the retries to wait", func() bool { return countGoroutines("(*Connection).retryRestore") == 1 })

	// The next session restores everything: no retries.
	tr.authz.setDeny("subscribe:ended.topic", false)
	tr.authz.setDeny("register:ended.proc", false)
	tr.ln.dropAll()
	recv(t, connects, "OnConnect after the reconnect")
	if !sub.Active() || !reg.Active() {
		t.Fatal("not restored by the next join")
	}
	eventually(t, 5*time.Second, "the retries to end with their session", func() bool {
		return countGoroutines("(*Connection).retryRestore") == 0
	})

	// Refused again, and stopped.
	tr.authz.setDeny("subscribe:ended.topic", true)
	tr.ln.dropAll()
	recv(t, connects, "OnConnect after the second reconnect")
	eventually(t, 5*time.Second, "the retries to wait", func() bool { return countGoroutines("(*Connection).retryRestore") == 1 })
	begin := time.Now()
	if err := c.Stop(ctxTimeout(t, 5*time.Second)); err != nil {
		t.Fatal(err)
	}
	if el := time.Since(begin); el > time.Second {
		t.Fatalf("Stop took %v", el)
	}
	eventually(t, 5*time.Second, "the retries to end with Stop", func() bool {
		return countGoroutines("(*Connection).retryRestore") == 0
	})
}

// A registration another session took over (force_reregister) is not a
// failed restore: the retries leave it to the next reconnect, so two app
// instances do not keep taking the procedure from each other.
func TestRestoreRetriesLeaveTakenOverRegistrations(t *testing.T) {
	tr := newTestRouter(t, true)
	connects := make(chan struct{}, 8)
	first, _ := startTestConn(t, tr, restoreRetry(20*time.Millisecond, 40*time.Millisecond), func(cfg *Config, _ *Connection) {
		cfg.OnConnect = func() { connects <- struct{}{} }
	})
	recv(t, connects, "OnConnect")
	ctx := ctxTimeout(t, 10*time.Second)
	taken, err := first.Register(ctx, "taken.proc", echoHandler("first:"), nil)
	if err != nil {
		t.Fatal(err)
	}
	failedRestore(t, tr, first, connects, "other.topic", "other.proc") // the retries run
	second, _ := startTestConn(t, tr)
	if _, err := second.Register(ctx, "taken.proc", echoHandler("second:"), nil); err != nil {
		t.Fatal(err)
	}
	eventually(t, 5*time.Second, "the taken-over registration to turn inactive", func() bool { return !taken.Active() })
	registers := tr.authz.count("register:taken.proc")
	retries := tr.authz.count("register:other.proc")
	eventually(t, 5*time.Second, "several retries", func() bool { return tr.authz.count("register:other.proc") >= retries+3 })
	if n := tr.authz.count("register:taken.proc"); n != registers {
		t.Fatalf("the taken-over registration was registered again %d times", n-registers)
	}
	if res, err := localCall(t, tr.local(t), "taken.proc", 1); err != nil || res.Arguments[0] != "second:1" {
		t.Fatalf("call = %v, %v", res, err)
	}
}

func TestSubscribeAndRegisterWhileInactiveRetries(t *testing.T) {
	tr := newTestRouter(t, true)
	connects := make(chan struct{}, 8)
	c, _ := startTestConn(t, tr, restoreRetry(time.Hour, time.Hour), func(cfg *Config, _ *Connection) {
		cfg.OnConnect = func() { connects <- struct{}{} }
	})
	recv(t, connects, "OnConnect")
	ctx := ctxTimeout(t, 10*time.Second)
	h1, ev1 := eventCollector(4)
	s1, err := c.Subscribe(ctx, "inactive.topic", h1, nil)
	if err != nil {
		t.Fatal(err)
	}
	tr.authz.setDeny("subscribe:inactive.topic", true)
	tr.ln.dropAll()
	recv(t, connects, "OnConnect")
	if s1.Active() {
		t.Fatal("restore should have failed")
	}
	// A new handler on the inactive group re-subscribes it for everyone.
	tr.authz.setDeny("subscribe:inactive.topic", false)
	h2, ev2 := eventCollector(4)
	s2, err := c.Subscribe(ctx, "inactive.topic", h2, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !s1.Active() || !s2.Active() {
		t.Fatal("group not reactivated")
	}
	localPublish(t, tr.local(t), "inactive.topic", nil, nil)
	recv(t, ev1, "event 1")
	recv(t, ev2, "event 2")
}

func TestOperationsWaitForReconnect(t *testing.T) {
	tr := newTestRouter(t, true)
	c, _ := startTestConn(t, tr)
	tr.removeRealm()
	eventually(t, 2*time.Second, "session down", func() bool { return !c.IsOpen() })

	done := make(chan error, 1)
	go func() {
		done <- c.Publish(context.Background(), "wait.topic", nil, nil, &PublishOptions{Acknowledge: true}, 5*time.Second)
	}()
	time.Sleep(50 * time.Millisecond)
	tr.addRealm()
	if err := recv(t, done, "publish"); err != nil {
		t.Fatalf("Publish across the outage: %v", err)
	}

	// With no window the default session wait applies.
	tr.removeRealm()
	eventually(t, 2*time.Second, "session down", func() bool { return !c.IsOpen() })
	c.mu.Lock()
	c.cfg.SessionWaitTimeout = 30 * time.Millisecond
	c.mu.Unlock()
	err := c.Publish(context.Background(), "wait.topic", nil, nil, nil, 0)
	if !errors.Is(err, ErrNotConnected) || !strings.Contains(err.Error(), "no session after 0.03s") {
		t.Fatalf("Publish while down = %v", err)
	}
	// So it does for a call without a retry window.
	_, err = c.Call(context.Background(), "wait.proc", nil, nil, nil, 0)
	if !errors.Is(err, ErrNotConnected) || !strings.Contains(err.Error(), "no session after 0.03s") {
		t.Fatalf("Call while down = %v", err)
	}
	// And for Subscribe and Register.
	if _, err := c.Subscribe(context.Background(), "wait.topic", func(*Event) {}, nil); !errors.Is(err, ErrNotConnected) ||
		!strings.Contains(err.Error(), "no session after 0.03s") {
		t.Fatalf("Subscribe while down = %v", err)
	}
	if _, err := c.Register(context.Background(), "wait.proc", echoHandler("ok:"), nil); !errors.Is(err, ErrNotConnected) ||
		!strings.Contains(err.Error(), "no session after 0.03s") {
		t.Fatalf("Register while down = %v", err)
	}
	tr.addRealm()
}

// SubscribeWithin and RegisterWithin wait for a session for their whole
// window, beyond the default session wait, and then subscribe and register
// once; without a window they wait the default.
func TestSubscribeAndRegisterWithinWaitForTheirWindow(t *testing.T) {
	tr := newTestRouter(t, true)
	c, _ := startTestConn(t, tr, func(cfg *Config, _ *Connection) { cfg.SessionWaitTimeout = 30 * time.Millisecond })
	tr.removeRealm()
	eventually(t, 2*time.Second, "session down", func() bool { return !c.IsOpen() })

	handler, events := eventCollector(4)
	subscribed := make(chan error, 1)
	go func() {
		_, err := c.SubscribeWithin(context.Background(), "within.topic", handler, nil, 5*time.Second)
		subscribed <- err
	}()
	registered := make(chan error, 1)
	go func() {
		_, err := c.RegisterWithin(context.Background(), "within.proc", echoHandler("ok:"), nil, 5*time.Second)
		registered <- err
	}()
	time.Sleep(200 * time.Millisecond) // well beyond the default wait
	tr.addRealm()
	if err := recv(t, subscribed, "SubscribeWithin"); err != nil {
		t.Fatalf("SubscribeWithin across the outage: %v", err)
	}
	if err := recv(t, registered, "RegisterWithin"); err != nil {
		t.Fatalf("RegisterWithin across the outage: %v", err)
	}
	pub := tr.local(t)
	localPublish(t, pub, "within.topic", nxwamp.List{1}, nil)
	recv(t, events, "the event")
	if _, err := localCall(t, pub, "within.proc"); err != nil {
		t.Errorf("the procedure registered across the outage: %v", err)
	}

	tr.removeRealm()
	eventually(t, 2*time.Second, "session down", func() bool { return !c.IsOpen() })
	if _, err := c.SubscribeWithin(context.Background(), "other.topic", handler, nil, 0); !errors.Is(err, ErrNotConnected) ||
		!strings.Contains(err.Error(), "no session after 0.03s") {
		t.Errorf("SubscribeWithin without a window = %v", err)
	}
	if _, err := c.RegisterWithin(context.Background(), "other.proc", echoHandler("ok:"), nil, 0); !errors.Is(err, ErrNotConnected) ||
		!strings.Contains(err.Error(), "no session after 0.03s") {
		t.Errorf("RegisterWithin without a window = %v", err)
	}
	tr.addRealm()
}

func TestCallRetryWindow(t *testing.T) {
	tr := newTestRouter(t, true)
	c, logs := startTestConn(t, tr)
	callee := tr.local(t)

	t.Run("no window fails at once", func(t *testing.T) {
		start := time.Now()
		_, err := c.Call(context.Background(), "late.proc", nil, nil, nil, 0)
		var werr *Error
		if !errors.As(err, &werr) || werr.URI != URINoSuchProcedure {
			t.Fatalf("Call = %v, want *Error no_such_procedure", err)
		}
		if time.Since(start) > 500*time.Millisecond {
			t.Fatal("retried without a window")
		}
	})

	t.Run("late registration is reached", func(t *testing.T) {
		go func() {
			time.Sleep(150 * time.Millisecond)
			_ = callee.Register("late.proc", func(context.Context, *nxwamp.Invocation) client.InvokeResult {
				return client.InvokeResult{Args: nxwamp.List{"ok"}}
			}, nil)
		}()
		start := time.Now()
		res, err := c.Call(context.Background(), "late.proc", nil, nil, nil, 3*time.Second)
		if err != nil {
			t.Fatalf("Call: %v\n%s", err, logs)
		}
		if res.Value() != "ok" || time.Since(start) < 150*time.Millisecond {
			t.Fatalf("result %v after %v", res.Value(), time.Since(start))
		}
		if !strings.Contains(logs.String(), "Procedure not registered yet; retrying") {
			t.Fatal("retry not logged")
		}
	})

	t.Run("other errors are not retried", func(t *testing.T) {
		var calls atomic.Int32
		if err := callee.Register("failing.proc", func(context.Context, *nxwamp.Invocation) client.InvokeResult {
			calls.Add(1)
			return client.InvokeResult{Err: "app.error.bad_input",
				Args: nxwamp.List{uint8(7), map[string]int{"limit": 10}}, Kwargs: nxwamp.Dict{"field": "x"}}
		}, nil); err != nil {
			t.Fatal(err)
		}
		_, err := c.Call(context.Background(), "failing.proc", nil, nil, nil, 2*time.Second)
		var werr *Error
		if !errors.As(err, &werr) {
			t.Fatalf("Call = %T %v", err, err)
		}
		want := &Error{URI: "app.error.bad_input", Args: []any{int64(7), map[string]any{"limit": int64(10)}},
			Kwargs: map[string]any{"field": "x"}}
		if !reflect.DeepEqual(werr, want) {
			t.Fatalf("Error = %#v, want %#v", werr, want)
		}
		if n := calls.Load(); n != 1 {
			t.Fatalf("callee invoked %d times", n)
		}
	})

	t.Run("window closes", func(t *testing.T) {
		start := time.Now()
		_, err := c.Call(context.Background(), "never.proc", nil, nil, nil, 100*time.Millisecond)
		var werr *Error
		if !errors.As(err, &werr) || werr.URI != URINoSuchProcedure {
			t.Fatalf("Call = %v", err)
		}
		if el := time.Since(start); el < 50*time.Millisecond || el > 2*time.Second {
			t.Fatalf("gave up after %v", el)
		}
	})

	t.Run("context cancels the retry", func(t *testing.T) {
		_, err := c.Call(ctxTimeout(t, 50*time.Millisecond), "never.proc", nil, nil, nil, 10*time.Second)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Call = %v", err)
		}
	})

	t.Run("window waits for a session", func(t *testing.T) {
		tr.removeRealm()
		eventually(t, 2*time.Second, "session down", func() bool { return !c.IsOpen() })
		go func() {
			time.Sleep(100 * time.Millisecond)
			tr.addRealm()
			time.Sleep(50 * time.Millisecond)
			cli := tr.local(t)
			_ = cli.Register("after.restart", func(context.Context, *nxwamp.Invocation) client.InvokeResult {
				return client.InvokeResult{Args: nxwamp.List{"served"}}
			}, nil)
		}()
		res, err := c.Call(context.Background(), "after.restart", nil, nil, nil, 5*time.Second)
		if err != nil || res.Value() != "served" {
			t.Fatalf("Call = %v, %v", res, err)
		}
	})
}

func TestCallCancellationReachesCallee(t *testing.T) {
	tr := newTestRouter(t, true)
	c, _ := startTestConn(t, tr)
	cancelled := make(chan struct{})
	_, err := c.Register(ctxTimeout(t, 5*time.Second), "slow.proc", func(ctx context.Context, _ *Invocation) (any, error) {
		<-ctx.Done()
		close(cancelled)
		return nil, ctx.Err()
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	caller := tr.local(t)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := caller.Call(ctx, "slow.proc", nil, nil, nil, nil); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Call = %v", err)
	}
	recv(t, cancelled, "handler ctx cancelled")

	// And from our side.
	_, err = c.Call(ctxTimeout(t, 50*time.Millisecond), "slow.proc", nil, nil, &CallOptions{Timeout: time.Minute}, 0)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("own Call = %v", err)
	}
}

func TestRegisterResultMapping(t *testing.T) {
	tr := newTestRouter(t, true)
	c, logs := startTestConn(t, tr)
	caller := tr.local(t)
	ctx := ctxTimeout(t, 10*time.Second)

	tests := []struct {
		name     string
		handler  InvocationHandler
		wantArgs nxwamp.List
		wantKw   nxwamp.Dict
		wantErr  string
		errArgs  nxwamp.List
	}{
		{name: "value", handler: func(context.Context, *Invocation) (any, error) { return 42, nil },
			wantArgs: nxwamp.List{int64(42)}},
		{name: "map value", handler: func(context.Context, *Invocation) (any, error) {
			return map[string]any{"a": []any{1, "b"}}, nil
		}, wantArgs: nxwamp.List{map[string]any{"a": []any{int64(1), "b"}}}},
		{name: "Result", handler: func(context.Context, *Invocation) (any, error) {
			return Result{Args: []any{1, 2}, Kwargs: map[string]any{"k": "v"}}, nil
		}, wantArgs: nxwamp.List{int64(1), int64(2)}, wantKw: nxwamp.Dict{"k": "v"}},
		{name: "*Result", handler: func(context.Context, *Invocation) (any, error) {
			return &Result{Kwargs: map[string]any{"only": true}}, nil
		}, wantKw: nxwamp.Dict{"only": true}},
		{name: "nil", handler: func(context.Context, *Invocation) (any, error) { return nil, nil }},
		{name: "*Error", handler: func(context.Context, *Invocation) (any, error) {
			return nil, &Error{URI: "app.error.custom", Args: []any{"detail"}}
		}, wantErr: "app.error.custom", errArgs: nxwamp.List{"detail"}},
		{name: "plain error", handler: func(context.Context, *Invocation) (any, error) {
			return nil, errors.New("boom")
		}, wantErr: URIRuntimeError, errArgs: nxwamp.List{"boom"}},
		{name: "panic", handler: func(context.Context, *Invocation) (any, error) {
			panic("kaboom")
		}, wantErr: URIRuntimeError},
	}
	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			proc := fmt.Sprintf("map.proc%d", i)
			reg, err := c.Register(ctx, proc, tt.handler, nil)
			if err != nil {
				t.Fatal(err)
			}
			if reg.Procedure() != proc || !reg.Active() {
				t.Fatal("registration handle")
			}
			res, err := localCall(t, caller, proc)
			if tt.wantErr != "" {
				var rpcErr client.RPCError
				if !errors.As(err, &rpcErr) || string(rpcErr.Err.Error) != tt.wantErr {
					t.Fatalf("Call = %v, want %s", err, tt.wantErr)
				}
				if tt.errArgs != nil && !reflect.DeepEqual(rpcErr.Err.Arguments, tt.errArgs) {
					t.Fatalf("error args = %#v", rpcErr.Err.Arguments)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			// The local caller sees exactly what the callee sent, decoded
			// from msgpack.
			if len(res.Arguments) != len(tt.wantArgs) || len(res.ArgumentsKw) != len(tt.wantKw) {
				t.Fatalf("result = %#v / %#v", res.Arguments, res.ArgumentsKw)
			}
			for j := range tt.wantArgs {
				if !reflect.DeepEqual(normalizeValue(res.Arguments[j]), normalizeValue(tt.wantArgs[j])) {
					t.Fatalf("arg %d = %#v, want %#v", j, res.Arguments[j], tt.wantArgs[j])
				}
			}
			if !reflect.DeepEqual(normalizeDict(res.ArgumentsKw), normalizeDict(tt.wantKw)) {
				t.Fatalf("kwargs = %#v", res.ArgumentsKw)
			}
		})
	}
	if !strings.Contains(logs.String(), "kaboom") {
		t.Fatal("handler panic not logged")
	}
	if !c.IsOpen() {
		t.Fatal("connection lost")
	}
}

func TestRegisterOptionsAndInvocation(t *testing.T) {
	tr := newTestRouter(t, true)
	c, _ := startTestConn(t, tr)
	ctx := ctxTimeout(t, 5*time.Second)
	invs := make(chan *Invocation, 1)
	handler := func(_ context.Context, inv *Invocation) (any, error) { invs <- inv; return nil, nil }

	if _, err := c.Register(ctx, "opts.default", handler, nil); err != nil {
		t.Fatal(err)
	}
	if got := tr.authz.registered("opts.default")["force_reregister"]; got != true {
		t.Fatalf("force_reregister = %v, want true by default", got)
	}
	f := false
	opts := &RegisterOptions{ForceReregister: &f}
	if _, err := c.Register(ctx, "opts.explicit", handler, opts); err != nil {
		t.Fatal(err)
	}
	if got := tr.authz.registered("opts.explicit")["force_reregister"]; got != false {
		t.Fatalf("force_reregister = %v, want false", got)
	}
	if *opts.ForceReregister || opts.Extra != nil {
		t.Fatal("caller's options modified")
	}

	// A second registration of a procedure on the same connection.
	_, err := c.Register(ctx, "opts.default", handler, nil)
	var werr *Error
	if !errors.As(err, &werr) || werr.URI != URIProcedureAlreadyExists {
		t.Fatalf("duplicate Register = %v", err)
	}

	// Payload normalization on the callee side.
	caller := tr.local(t)
	_, err = caller.Call(ctx, "opts.default", nil,
		nxwamp.List{uint16(1), map[string]any{"n": []int{1, 2}}}, nxwamp.Dict{"f": float32(0.5)}, nil)
	if err != nil {
		t.Fatal(err)
	}
	inv := recv(t, invs, "invocation")
	if inv.Procedure != "opts.default" ||
		!reflect.DeepEqual(inv.Args, []any{int64(1), map[string]any{"n": []any{int64(1), int64(2)}}}) ||
		!reflect.DeepEqual(inv.Kwargs, map[string]any{"f": 0.5}) {
		t.Fatalf("invocation = %+v", inv)
	}

	// Pattern registration: Procedure is the concrete URI the router discloses.
	if _, err := c.Register(ctx, "opts.pattern", handler, &RegisterOptions{Match: "prefix"}); err != nil {
		t.Fatal(err)
	}
	if _, err := caller.Call(ctx, "opts.pattern.leaf", nil, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	if inv := recv(t, invs, "pattern invocation"); inv.Procedure != "opts.pattern.leaf" {
		t.Fatalf("Procedure = %q", inv.Procedure)
	}
}

func TestPayloadNormalizationFromMsgpack(t *testing.T) {
	tr := newTestRouter(t, true)
	c, _ := startTestConn(t, tr)
	handler, events := eventCollector(1)
	if _, err := c.Subscribe(ctxTimeout(t, 5*time.Second), "norm.topic", handler, nil); err != nil {
		t.Fatal(err)
	}
	pub := tr.local(t)
	big := uint64(math.MaxInt64) + 10
	localPublish(t, pub, "norm.topic",
		nxwamp.List{int8(-1), uint8(200), 70000, int64(1) << 40, big, 2.5, float32(1.5), "s", true, nil, []byte("raw")},
		nxwamp.Dict{
			"nested": map[string]any{
				"list": []any{1, map[string]int{"deep": 3}, nil},
				"dict": nxwamp.Dict{"x": []string{"a", "b"}},
			},
		})
	ev := recv(t, events, "event")
	wantArgs := []any{int64(-1), int64(200), int64(70000), int64(1) << 40, big, 2.5, 1.5, "s", true, nil, []byte("raw")}
	if !reflect.DeepEqual(ev.Args, wantArgs) {
		t.Fatalf("args = %#v\nwant   %#v", ev.Args, wantArgs)
	}
	wantKw := map[string]any{"nested": map[string]any{
		"list": []any{int64(1), map[string]any{"deep": int64(3)}, nil},
		"dict": map[string]any{"x": []any{"a", "b"}},
	}}
	if !reflect.DeepEqual(ev.Kwargs, wantKw) {
		t.Fatalf("kwargs = %#v\nwant     %#v", ev.Kwargs, wantKw)
	}
	if _, ok := ev.Kwargs["nested"].(map[string]any); !ok {
		t.Fatalf("nested map has type %T", ev.Kwargs["nested"])
	}

	// Call results are normalized the same way.
	if err := pub.Register("norm.proc", func(context.Context, *nxwamp.Invocation) client.InvokeResult {
		return client.InvokeResult{Args: nxwamp.List{uint32(5), nxwamp.Dict{"k": []uint{1}}}, Kwargs: nxwamp.Dict{"z": int16(-2)}}
	}, nil); err != nil {
		t.Fatal(err)
	}
	res, err := c.Call(context.Background(), "norm.proc", nil, nil, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(res.Args, []any{int64(5), map[string]any{"k": []any{int64(1)}}}) ||
		!reflect.DeepEqual(res.Kwargs, map[string]any{"z": int64(-2)}) {
		t.Fatalf("result = %#v %#v", res.Args, res.Kwargs)
	}
	var decoded struct {
		K []int `json:"k"`
	}
	if err := (&Result{Args: res.Args[1:]}).Decode(&decoded); err != nil || len(decoded.K) != 1 {
		t.Fatalf("Decode = %+v, %v", decoded, err)
	}
}

func TestRouterRefusalsAreErrors(t *testing.T) {
	tr := newTestRouter(t, true)
	c, _ := startTestConn(t, tr)
	ctx := ctxTimeout(t, 5*time.Second)
	tr.authz.setDeny("publish:denied.topic", true)
	tr.authz.setDeny("subscribe:denied.topic", true)
	tr.authz.setDeny("register:denied.proc", true)

	check := func(op string, err error) {
		t.Helper()
		var werr *Error
		if !errors.As(err, &werr) || werr.URI != URINotAuthorized {
			t.Fatalf("%s = %T %v, want *Error not_authorized", op, err, err)
		}
	}
	check("acknowledged Publish", c.Publish(ctx, "denied.topic", []any{1}, nil, &PublishOptions{Acknowledge: true}, 0))
	_, err := c.Subscribe(ctx, "denied.topic", func(*Event) {}, nil)
	check("Subscribe", err)
	_, err = c.Register(ctx, "denied.proc", echoHandler(""), nil)
	check("Register", err)

	// Unacknowledged publishes cannot report a refusal.
	if err := c.Publish(ctx, "denied.topic", []any{1}, nil, nil, 0); err != nil {
		t.Fatalf("fire-and-forget Publish = %v", err)
	}
	// Refused entries are not tracked.
	if groups, regs := trackedCounts(t, c); groups != 0 || regs != 0 {
		t.Fatalf("refused entries tracked: %d groups, %d regs", groups, regs)
	}
	// Invalid arguments never reach the router.
	if err := c.Publish(ctx, " bad", nil, nil, nil, 0); err == nil {
		t.Fatal("blank-padded topic accepted")
	}
	if _, err := c.Subscribe(ctx, "a.b", nil, nil); err == nil {
		t.Fatal("nil handler accepted")
	}
	if _, err := c.Subscribe(ctx, "a.b", func(*Event) {}, &SubscribeOptions{Match: "regex"}); err == nil {
		t.Fatal("bad match accepted")
	}
	if _, err := c.Call(ctx, "", nil, nil, nil, 0); err == nil {
		t.Fatal("empty procedure accepted")
	}
}

func TestNoGoroutineLeaks(t *testing.T) {
	before := interestingGoroutines()

	func() {
		tr := newTestRouter(t, true)
		connects := make(chan struct{}, 4)
		c, _ := newTestConn(t, tr, func(cfg *Config, _ *Connection) {
			cfg.OnConnect = func() { connects <- struct{}{} }
		})
		if err := c.Start(ctxTimeout(t, 5*time.Second)); err != nil {
			t.Fatal(err)
		}
		recv(t, connects, "OnConnect")
		ctx := ctxTimeout(t, 5*time.Second)
		handler, events := eventCollector(16)
		if _, err := c.Subscribe(ctx, "leak.topic", handler, nil); err != nil {
			t.Fatal(err)
		}
		grouped, err := c.Subscribe(ctx, "leak.grouped", handler, &SubscribeOptions{Group: NewDeliveryGroup()})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := c.Register(ctx, "leak.proc", echoHandler(""), nil); err != nil {
			t.Fatal(err)
		}
		f := false
		for range 5 {
			for _, topic := range []string{"leak.topic", "leak.grouped"} {
				if err := c.Publish(ctx, topic, []any{1}, nil, &PublishOptions{Acknowledge: true, ExcludeMe: &f}, 0); err != nil {
					t.Fatal(err)
				}
				recv(t, events, "event")
			}
		}
		// A removal whose context has ended completes in the background.
		cancelled, cancel := context.WithCancel(context.Background())
		cancel()
		if err := grouped.Unsubscribe(cancelled); !errors.Is(err, context.Canceled) {
			t.Fatalf("Unsubscribe with a cancelled context = %v", err)
		}
		if _, err := c.Call(ctx, "leak.proc", []any{"x"}, nil, nil, 0); err != nil {
			t.Fatal(err)
		}
		tr.ln.dropAll()
		recv(t, connects, "OnConnect after reconnect")
		if err := c.Stop(ctxTimeout(t, 5*time.Second)); err != nil {
			t.Fatal(err)
		}
		// A connection that never joined.
		c2, _ := newTestConn(t, tr, func(cfg *Config, _ *Connection) { cfg.AuthSecret = "wrong" })
		_ = c2.Start(ctxTimeout(t, 100*time.Millisecond))
		tr.close()
	}()

	var leaked []string
	deadline := time.Now().Add(5 * time.Second)
	for {
		leaked = leaked[:0]
		for id, stack := range interestingGoroutines() {
			if _, ok := before[id]; !ok {
				leaked = append(leaked, stack)
			}
		}
		if len(leaked) == 0 || time.Now().After(deadline) {
			break
		}

		time.Sleep(20 * time.Millisecond)
	}
	if len(leaked) > 0 {
		t.Fatalf("%d goroutines leaked:\n\n%s", len(leaked), strings.Join(leaked, "\n\n"))
	}
}

func TestForceReregisterEviction(t *testing.T) {
	tr := newTestRouter(t, true)
	first, logs := startTestConn(t, tr)
	second, _ := startTestConn(t, tr)
	ctx := ctxTimeout(t, 5*time.Second)

	reg1, err := first.Register(ctx, "takeover.proc", echoHandler("first:"), nil)
	if err != nil {
		t.Fatal(err)
	}
	reg2, err := second.Register(ctx, "takeover.proc", echoHandler("second:"), nil)
	if err != nil {
		t.Fatalf("force_reregister: %v", err)
	}
	eventually(t, 5*time.Second, "the evicted registration to turn inactive", func() bool { return !reg1.Active() })
	if !reg2.Active() {
		t.Fatal("new registration inactive")
	}
	eventually(t, 5*time.Second, "eviction log", func() bool {
		return strings.Contains(logs.String(), "taken over by another session") &&
			strings.Contains(logs.String(), "procedure=takeover.proc")
	})
	res, err := localCall(t, tr.local(t), "takeover.proc", 1)
	if err != nil || res.Arguments[0] != "second:1" {
		t.Fatalf("call = %v, %v", res, err)
	}
	// Unregistering the evicted handle is not an error.
	if err := reg1.Unregister(ctx); err != nil {
		t.Fatalf("Unregister of an evicted registration = %v", err)
	}
	if !reg2.Active() {
		t.Fatal("unregistering the evicted handle affected the new owner")
	}
}

// Concurrent subscription churn, publishes and calls while the transport is
// dropped again and again: no deadlock, no race, and the tracked state ends
// up exactly as the callers left it.
func TestConcurrentOperationsAcrossReconnects(t *testing.T) {
	tr := newTestRouter(t, true)
	c, logs := startTestConn(t, tr)
	stop := make(chan struct{})
	var wg sync.WaitGroup

	wg.Add(1)
	go func() { // chaos
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			case <-time.After(40 * time.Millisecond):
				tr.ln.dropAll()
			}
		}
	}()

	var ops atomic.Int64
	for w := range 6 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			f := false
			topic := fmt.Sprintf("stress.topic.%d", w%3) // shared by two workers
			proc := fmt.Sprintf("stress.proc.%d", w)
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				sub, err := c.Subscribe(ctx, topic, func(ev *Event) {
					_, _ = c.Call(context.Background(), proc, nil, nil, nil, 0)
				}, nil)
				reg, rerr := c.Register(ctx, proc, echoHandler(""), nil)
				_ = c.Publish(ctx, topic, []any{i}, nil, &PublishOptions{Acknowledge: true, ExcludeMe: &f}, 0)
				_, _ = c.Call(ctx, proc, []any{i}, nil, nil, 100*time.Millisecond)
				if err == nil {
					_ = sub.Unsubscribe(ctx)
				}
				if rerr == nil {
					_ = reg.Unregister(ctx)
				}
				cancel()
				ops.Add(1)
			}
		}()
	}

	time.Sleep(1500 * time.Millisecond)
	close(stop)
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(20 * time.Second):
		buf := make([]byte, 1<<22)
		buf = buf[:runtime.Stack(buf, true)]
		t.Fatalf("workers deadlocked\n%s\n\nlog tail:\n%s", buf, tail(logs.String(), 4000))
	}
	if ops.Load() < 6 {
		t.Fatalf("only %d iterations completed", ops.Load())
	}

	// A removal whose context ended while it waited for a restore finishes
	// in the background.
	eventually(t, 5*time.Second, "nothing tracked after every caller unsubscribed", func() bool {
		groups, regs := trackedCounts(t, c)
		return groups == 0 && regs == 0
	})
	eventually(t, 5*time.Second, "reconnect", c.IsOpen)
	sess := c.currentSession()
	for w := range 6 {
		if _, ok := sess.cli.SubscriptionID(fmt.Sprintf("stress.topic.%d", w%3)); ok {
			t.Fatalf("WAMP subscription left behind for worker %d", w)
		}
		if _, ok := sess.cli.RegistrationID(fmt.Sprintf("stress.proc.%d", w)); ok {
			t.Fatalf("WAMP registration left behind for worker %d", w)
		}
	}
	t.Logf("%d iterations", ops.Load())
}

func tail(s string, n int) string {
	if len(s) > n {
		return s[len(s)-n:]
	}
	return s
}

// While the connection is down, Unsubscribe and Unregister do not wait for a
// session: they remove the handle locally and return at once. What they
// removed is not restored after the reconnect; another handler of the same
// topic is, and keeps its events.
func TestRemovalWhileDisconnectedDoesNotWaitForASession(t *testing.T) {
	tr := newTestRouter(t, true)
	c, _ := startTestConn(t, tr, func(cfg *Config, _ *Connection) { cfg.SessionWaitTimeout = time.Minute })
	ctx := ctxTimeout(t, 10*time.Second)
	h1, ev1 := eventCollector(8)
	h2, ev2 := eventCollector(8)
	h3, ev3 := eventCollector(8)
	gone, err := c.Subscribe(ctx, "shared.topic", h1, nil)
	if err != nil {
		t.Fatal(err)
	}
	kept, err := c.Subscribe(ctx, "shared.topic", h2, nil)
	if err != nil {
		t.Fatal(err)
	}
	solo, err := c.Subscribe(ctx, "solo.topic", h3, nil)
	if err != nil {
		t.Fatal(err)
	}
	reg, err := c.Register(ctx, "removed.proc", echoHandler("ok:"), nil)
	if err != nil {
		t.Fatal(err)
	}

	tr.removeRealm() // the session ends, and every reconnect is refused until the realm is back
	eventually(t, 2*time.Second, "session down", func() bool { return !c.IsOpen() })
	for _, removal := range []struct {
		name   string
		remove func(context.Context) error
	}{
		{"Unsubscribe of one handler of a topic", func(ctx context.Context) error { return c.Unsubscribe(ctx, gone) }},
		{"Subscription.Unsubscribe of a topic's only handler", solo.Unsubscribe},
		{"Unregister", func(ctx context.Context) error { return c.Unregister(ctx, reg) }},
	} {
		began := time.Now()
		if err := removal.remove(ctxTimeout(t, 3*time.Second)); err != nil {
			t.Errorf("%s while disconnected = %v", removal.name, err)
		}
		if el := time.Since(began); el > 500*time.Millisecond {
			t.Errorf("%s took %v while disconnected: it waited for a session", removal.name, el)
		}
	}
	if c.IsOpen() {
		t.Fatal("reconnected during the removals: they were not made while disconnected")
	}
	if gone.Active() || solo.Active() || reg.Active() {
		t.Fatal("a removed handle is active")
	}
	if groups, regs := trackedCounts(t, c); groups != 1 || regs != 0 {
		t.Fatalf("tracked %d topics and %d procedures, want only shared.topic", groups, regs)
	}

	tr.addRealm()
	eventually(t, 5*time.Second, "the reconnect to restore the kept handler", kept.Active)
	if gone.Active() || solo.Active() || reg.Active() {
		t.Fatal("a handle removed while disconnected was restored after the reconnect")
	}
	if tr.lookup(t, nxwamp.MetaProcSubLookup, "solo.topic") != 0 || tr.lookup(t, nxwamp.MetaProcRegLookup, "removed.proc") != 0 {
		t.Fatal("the router has a subscription or registration removed while disconnected")
	}
	pub := tr.local(t)
	localPublish(t, pub, "shared.topic", nxwamp.List{1}, nil)
	localPublish(t, pub, "solo.topic", nxwamp.List{2}, nil)
	if got := recv(t, ev2, "the kept handler's event").Args[0]; got != int64(1) {
		t.Fatalf("got %v", got)
	}
	noRecv(t, ev1, "an event for the handler removed while disconnected")
	noRecv(t, ev3, "an event of the topic removed while disconnected")
	if _, err := localCall(t, pub, "removed.proc"); err == nil {
		t.Error("the procedure removed while disconnected answered")
	}
}

// A removal the router refuses is returned as the router's error, and the
// local removal stands: the handler is called no more, and the subscription
// or registration is not restored after a reconnect.
func TestARefusedRemovalStandsLocally(t *testing.T) {
	tr := newTestRouter(t, true)
	connects := make(chan struct{}, 8)
	c, _ := startTestConn(t, tr, func(cfg *Config, _ *Connection) { cfg.OnConnect = func() { connects <- struct{}{} } })
	recv(t, connects, "OnConnect")
	ctx := ctxTimeout(t, 10*time.Second)
	handler, events := eventCollector(8)
	sub, err := c.Subscribe(ctx, "refused.topic", handler, nil)
	if err != nil {
		t.Fatal(err)
	}
	invoked := make(chan struct{}, 4)
	reg, err := c.Register(ctx, "refused.proc", func(context.Context, *Invocation) (any, error) {
		invoked <- struct{}{}
		return "served", nil
	}, nil)
	if err != nil {
		t.Fatal(err)
	}

	tr.authz.setDeny("UNSUBSCRIBE", true)
	tr.authz.setDeny("UNREGISTER", true)
	var werr *Error
	if err := c.Unsubscribe(ctx, sub); !errors.As(err, &werr) || werr.URI != URINotAuthorized {
		t.Fatalf("refused Unsubscribe = %v, want the router's %s", err, URINotAuthorized)
	}
	if err := c.Unregister(ctx, reg); !errors.As(err, &werr) || werr.URI != URINotAuthorized {
		t.Fatalf("refused Unregister = %v, want the router's %s", err, URINotAuthorized)
	}
	if sub.Active() || reg.Active() {
		t.Fatal("active after a refused removal")
	}
	if groups, regs := trackedCounts(t, c); groups != 0 || regs != 0 {
		t.Fatalf("still tracked after a refused removal: %d topics, %d procedures", groups, regs)
	}
	// The router keeps them, but nothing reaches the handlers.
	pub := tr.local(t)
	localPublish(t, pub, "refused.topic", nxwamp.List{1}, nil)
	noRecv(t, events, "an event after the refused Unsubscribe")
	if _, err := localCall(t, pub, "refused.proc"); err == nil {
		t.Error("the procedure answered after the refused Unregister")
	}
	noRecv(t, invoked, "a call of the handler after the refused Unregister")

	// Nor are they restored after a reconnect.
	tr.ln.dropAll()
	recv(t, connects, "OnConnect after the reconnect")
	if tr.lookup(t, nxwamp.MetaProcSubLookup, "refused.topic") != 0 || tr.lookup(t, nxwamp.MetaProcRegLookup, "refused.proc") != 0 {
		t.Fatal("restored after the reconnect")
	}
	localPublish(t, pub, "refused.topic", nxwamp.List{2}, nil)
	noRecv(t, events, "an event after the reconnect")
}
