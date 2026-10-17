// Package wampx is what package ironflock needs of package wamp beyond its
// public API: the join counter of a wamp.Connection, and running a function
// in turn with the events of a wamp.DeliveryGroup. Table subscriptions use
// both to catch up on the rows stored while the connection was down.
//
// Package wamp keeps a Joins in every Connection and binds the lookups (see
// Bind) when it is initialized. Nothing here is public API.
package wampx

import (
	"fmt"
	"log/slog"
	"runtime/debug"
	"slices"
	"sync"
	"sync/atomic"
)

// Joins counts the sessions a connection has joined and calls hooks once a
// join has restored the connection's subscriptions and registrations. The
// zero value is ready to use.
//
// The connection counts a join before its restore: an event of the new
// session, which can arrive only once its subscription is restored, is
// handled under the new count already. It calls the hooks after the restore,
// on its supervisor goroutine, only while the session is still up, in the
// order they were added. A hook must not block.
type Joins struct {
	count atomic.Int64

	mu    sync.Mutex
	hooks []*joinHook
}

type joinHook struct{ fn func(join int64) }

// Count returns how many sessions have joined so far.
func (j *Joins) Count() int64 { return j.count.Load() }

// Begin counts a join and returns its number. The connection calls it as a
// session joins, before its restore.
func (j *Joins) Begin() int64 { return j.count.Add(1) }

// Restored calls the hooks with join, the number Begin returned. The
// connection calls it once the join's restore is done and its session is
// still the current one. A panicking hook is recovered and logged to log: it
// costs neither the other hooks nor the join.
func (j *Joins) Restored(join int64, log *slog.Logger) {
	j.mu.Lock()
	hooks := slices.Clone(j.hooks)
	j.mu.Unlock()
	for _, h := range hooks {
		h.call(join, log)
	}
}

func (h *joinHook) call(join int64, log *slog.Logger) {
	defer func() {
		if p := recover(); p != nil && log != nil {
			log.Warn(fmt.Sprintf("Handling join %d failed: %v", join, p), "stack", string(debug.Stack()))
		}
	}()
	h.fn(join)
}

// Hooks returns how many hooks are added.
func (j *Joins) Hooks() int {
	j.mu.Lock()
	defer j.mu.Unlock()
	return len(j.hooks)
}

// OnRestored adds hook, called with the number of every join from now on
// once its restore is done (see Restored). It returns the function that
// removes the hook again.
func (j *Joins) OnRestored(hook func(join int64)) (remove func()) {
	h := &joinHook{fn: hook}
	j.mu.Lock()
	j.hooks = append(j.hooks, h)
	j.mu.Unlock()
	return func() {
		j.mu.Lock()
		defer j.mu.Unlock()
		j.hooks = slices.DeleteFunc(j.hooks, func(x *joinHook) bool { return x == h })
	}
}

// Binding is what package wamp binds.
type Binding struct {
	// Joins returns the Joins of conn, a *wamp.Connection; nil for anything
	// else.
	Joins func(conn any) *Joins
	// RunInGroup queues fn on group, a *wamp.DeliveryGroup, and reports
	// whether it did (false for anything else).
	RunInGroup func(group any, fn func()) bool
}

var bound atomic.Pointer[Binding]

// Bind installs package wamp's binding. Package wamp calls it in its init.
func Bind(b Binding) { bound.Store(&b) }

// JoinsOf returns the join counter of conn, a *wamp.Connection, or nil for
// any other value.
func JoinsOf(conn any) *Joins {
	if b := bound.Load(); b != nil && b.Joins != nil {
		return b.Joins(conn)
	}
	return nil
}

// RunInGroup queues fn to run on group, a *wamp.DeliveryGroup, in turn with
// the events the group delivers: after those queued already, before those
// that arrive later, and never while one of the group's handlers runs. fn
// must not panic: nothing recovers it there. RunInGroup reports false, and
// does not run fn, when group is not a DeliveryGroup.
func RunInGroup(group any, fn func()) bool {
	if b := bound.Load(); b != nil && b.RunInGroup != nil {
		return b.RunInGroup(group, fn)
	}
	return false
}
