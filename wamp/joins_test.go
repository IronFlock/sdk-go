package wamp

import (
	"sync/atomic"
	"testing"
	"time"
)

// Every join is counted before its restore, and the join hooks (the ones the
// ironflock package's table subscriptions add to catch up after a reconnect)
// run once the restore is done — only for a session that is still up, and
// each recovered when it panics.
func TestJoinHooksRunOnceTheRestoreIsDone(t *testing.T) {
	tr := newTestRouter(t, true)
	c, logs := newTestConn(t, tr)
	type seen struct {
		join, count  int64
		open, active bool
	}
	hooks := make(chan seen, 8)
	var sub atomic.Pointer[Subscription]
	c.joins.OnRestored(func(int64) { panic("hook boom") })
	c.joins.OnRestored(func(join int64) {
		hooks <- seen{join, c.joins.Count(), c.IsOpen(), sub.Load().Active()}
	})
	if err := c.Start(ctxTimeout(t, 5*time.Second)); err != nil {
		t.Fatal(err)
	}
	if got := recv(t, hooks, "the first join's hooks"); got != (seen{1, 1, true, false}) {
		t.Fatalf("hook saw %+v", got)
	}
	eventually(t, 2*time.Second, "the panic to be logged", func() bool {
		return logs.count("Handling join 1 failed: hook boom") == 1
	})
	s, err := c.Subscribe(ctxTimeout(t, 5*time.Second), "com.example.t", func(*Event) {}, nil)
	if err != nil {
		t.Fatal(err)
	}
	sub.Store(s)

	// The next session's restore waits at the router: the join is counted
	// already, and its hooks wait for the restore.
	release := tr.authz.hold(t, "subscribe:com.example.t")
	tr.ln.dropAll()
	if key := recv(t, tr.authz.held, "the restore's SUBSCRIBE"); key != "subscribe:com.example.t" {
		t.Fatalf("held %s", key)
	}
	if n := c.joins.Count(); n != 2 {
		t.Fatalf("join count %d during the second restore, want 2", n)
	}
	noRecv(t, hooks, "a hook before the restore is done")
	release()
	if got := recv(t, hooks, "the second join's hooks"); got != (seen{2, 2, true, true}) {
		t.Fatalf("hook saw %+v, want the subscription restored", got)
	}

	// A session lost while it restores runs no hooks; the next join's do.
	release = tr.authz.hold(t, "subscribe:com.example.t")
	tr.ln.dropAll()
	recv(t, tr.authz.held, "the third session's SUBSCRIBE")
	tr.ln.dropAll()
	release()
	got := recv(t, hooks, "the next join's hooks")
	if got.join < 4 || got.count != got.join || !got.open || !got.active {
		t.Fatalf("hook saw %+v, want a join after the lost one, restored", got)
	}
	if !c.IsOpen() {
		t.Fatal("the panicking hook broke the connection")
	}
}
