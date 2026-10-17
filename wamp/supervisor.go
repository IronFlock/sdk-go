package wamp

import (
	"context"
	"fmt"
	"math/rand/v2"
	"net"
	"net/url"
	"slices"
	"time"

	"github.com/ironflock/nexus/v3/client"
	"github.com/ironflock/nexus/v3/transport"
	"github.com/ironflock/nexus/v3/transport/serialize"
	nxwamp "github.com/ironflock/nexus/v3/wamp"
	"github.com/ironflock/nexus/v3/wamp/crsign"
)

// tunables are the timing knobs of a Connection. They default to the
// exported constants; tests shorten them.
type tunables struct {
	initialRetryDelay        time.Duration
	baseMaxRetryDelay        time.Duration
	noSuchRealmBackoffAfter  time.Duration
	noSuchRealmMaxRetryDelay time.Duration
	retryDelayGrowth         float64
	retryDelayJitter         float64

	retryFirstDelay time.Duration
	retryMaxDelay   time.Duration

	// A fatal auth refusal of a connection that has been established is
	// final once it persists: authDenialAttempts refused attempts or more,
	// the first one at least authDenialGrace ago (see
	// Config.FailOnAuthError).
	authDenialAttempts int
	authDenialGrace    time.Duration

	// The retries of a failed restore while the session lasts (see
	// Connection.retryRestore) wait restoreRetryFirstDelay, doubling up to
	// restoreRetryMaxDelay, with relative random jitter restoreRetryJitter.
	restoreRetryFirstDelay time.Duration
	restoreRetryMaxDelay   time.Duration
	restoreRetryJitter     float64

	// connectTimeout bounds the TCP/TLS/WebSocket handshake of an attempt;
	// responseTimeout bounds each step of the WAMP join, every router
	// answer to SUBSCRIBE, REGISTER and acknowledged PUBLISH, the GOODBYE
	// exchange of Stop, and how long the client may leave a received
	// message untaken (see observedPeer.deliver).
	connectTimeout  time.Duration
	responseTimeout time.Duration
	// startTeardown bounds how long a failed Start waits for the supervisor
	// to exit before closing the connection forcibly.
	startTeardown time.Duration
	// clientGrace is how long a session's nexus client gets to end once its
	// connection is gone (or its GOODBYE exchange is over) before the
	// session is given up and the client abandoned.
	clientGrace time.Duration

	now  func() time.Time
	rand func() float64 // in [0, 1)

	// onRetry, if set, observes every scheduled reconnect (tests).
	onRetry func(reason string, delay time.Duration)
	// onDial, if set, observes every TCP dial of a connection attempt
	// (tests). Dials run on the supervisor goroutine.
	onDial func()
	// beforePublish, if set, runs in establish while it holds c.mu, right
	// before it publishes the restored session (tests).
	beforePublish func()
	// afterSubscribe, if set, runs in subscribeOn right after the router
	// accepted its SUBSCRIBE (tests).
	afterSubscribe func()
}

func defaultTunables() tunables {
	return tunables{
		initialRetryDelay:        InitialRetryDelay,
		baseMaxRetryDelay:        BaseMaxRetryDelay,
		noSuchRealmBackoffAfter:  NoSuchRealmBackoffAfter,
		noSuchRealmMaxRetryDelay: NoSuchRealmMaxRetryDelay,
		retryDelayGrowth:         RetryDelayGrowth,
		retryDelayJitter:         RetryDelayJitter,
		retryFirstDelay:          RetryFirstDelay,
		retryMaxDelay:            RetryMaxDelay,
		authDenialAttempts:       3,
		authDenialGrace:          60 * time.Second,
		restoreRetryFirstDelay:   1 * time.Second,
		restoreRetryMaxDelay:     30 * time.Second,
		restoreRetryJitter:       0.15,
		connectTimeout:           15 * time.Second,
		responseTimeout:          DefaultSessionWaitTimeout,
		startTeardown:            5 * time.Second,
		clientGrace:              2 * time.Second,
		now:                      time.Now,
		rand:                     rand.Float64,
	}
}

// backoff is the reconnect delay policy: exponential growth from the initial
// delay with random jitter, capped at baseMaxRetryDelay — or at
// noSuchRealmMaxRetryDelay once the realm has been missing for
// noSuchRealmBackoffAfter. The cap applies after the jitter, as in autobahn
// (Python SDK) and autobahn-js: at the cap the jitter only shortens the
// delay. Only the supervisor goroutine uses it.
type backoff struct {
	t *tunables

	delay time.Duration // base of the next delay, before jitter

	// The current streak of no_such_realm refusals.
	realmMissingSince time.Time
	realmMissing      bool
	slowed            bool
}

func newBackoff(t *tunables) *backoff {
	return &backoff{t: t, delay: t.initialRetryDelay}
}

// joined resets the policy after a successful join.
func (b *backoff) joined() {
	b.delay = b.t.initialRetryDelay
	b.resetStreak()
}

func (b *backoff) resetStreak() {
	b.realmMissing = false
	b.slowed = false
}

// observe records why the last attempt or session ended. It reports true
// exactly when the realm has now been missing long enough to slow down.
func (b *backoff) observe(reason string) bool {
	if reason != URINoSuchRealm {
		b.resetStreak()
		return false
	}
	now := b.t.now()
	if !b.realmMissing {
		b.realmMissing = true
		b.realmMissingSince = now
		return false
	}
	if b.slowed || now.Sub(b.realmMissingSince) < b.t.noSuchRealmBackoffAfter {
		return false
	}
	b.slowed = true
	return true
}

func (b *backoff) maxDelay() time.Duration {
	if b.slowed {
		return b.t.noSuchRealmMaxRetryDelay
	}
	return b.t.baseMaxRetryDelay
}

// next returns the delay before the next attempt and grows the base. The
// base itself carries no jitter.
func (b *backoff) next() time.Duration {
	ceiling := b.maxDelay()
	d := min(b.delay, ceiling)
	b.delay = min(time.Duration(float64(d)*b.t.retryDelayGrowth), ceiling)
	if j := b.t.retryDelayJitter; j > 0 {
		d = min(time.Duration(float64(d)*(1+j*(2*b.t.rand()-1))), ceiling)
	}
	return d
}

// authDenial is the streak of fatal auth refusals (IsFatalAuthReason) since
// the last join: a refused attempt, or a session the router closed with
// such a reason. Only the supervisor goroutine uses it.
type authDenial struct {
	t     *tunables
	count int
	since time.Time // of the first refusal
}

func (d *authDenial) reset() { d.count = 0 }

// refused records a refusal. It reports whether the refusal persists —
// authDenialAttempts refusals or more, the first one authDenialGrace ago or
// longer — and whether it is the first of the streak.
func (d *authDenial) refused() (persists, first bool) {
	now := d.t.now()
	if d.count == 0 {
		d.since = now
	}
	d.count++
	return d.count >= d.t.authDenialAttempts && now.Sub(d.since) >= d.t.authDenialGrace, d.count == 1
}

// supervise is the connection's only connect/retry loop. It runs from Start
// until ctx is done — the connection was stopped, or Start failed — or the
// connection fails for good, and then closes done. Everything it does is
// bounded once ctx is done (see Connection.forceClose for the slowest case).
func (c *Connection) supervise(ctx context.Context, done chan<- struct{}) {
	defer close(done)
	bo := newBackoff(&c.t)
	denial := authDenial{t: &c.t}
	for ctx.Err() == nil {
		s, reason, err := c.dialAndJoin(ctx)
		if err == nil {
			bo.joined()
			denial.reset()
			reason = c.runSession(ctx, s)
		}
		if ctx.Err() != nil {
			return
		}
		if c.cfg.FailOnAuthError && IsFatalAuthReason(reason) && c.deniedForGood(&denial, reason) {
			c.failAuth(reason)
			return
		}
		if bo.observe(reason) {
			c.log.Warn(fmt.Sprintf("Realm %s still does not exist after %ss; slowing reconnect attempts to "+
				"at most one per %ss until it appears", c.cfg.Realm,
				formatSeconds(c.t.noSuchRealmBackoffAfter), formatSeconds(c.t.noSuchRealmMaxRetryDelay)))
		}
		delay := bo.next()
		if err != nil {
			c.log.Info("Connection attempt failed; retrying", "reason", reason, "error", err, "retry_in", delay)
		} else {
			c.log.Info("Reconnecting", "retry_in", delay)
		}
		if c.t.onRetry != nil {
			c.t.onRetry(reason, delay)
		}
		timer := time.NewTimer(delay)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return
		}
	}
}

// dialAndJoin makes one connection attempt, abandoned as soon as ctx (the
// supervisor's) is done. On failure it returns the router's close reason,
// if it sent one.
func (c *Connection) dialAndJoin(ctx context.Context) (*session, string, error) {
	authID, secret := c.credentials()

	attempt, cancel := context.WithTimeout(ctx, c.t.connectTimeout)
	defer cancel()
	logger := nexusLogger{c.log}
	dead := newSignal()
	// Once TCP is connected, gorilla/websocket honours only the attempt's
	// deadline, not its cancellation: writing the upgrade request and reading
	// the response (also of an HTTP proxy's CONNECT) end only at the connect
	// timeout. So the socket is closed as soon as ctx is done, until the
	// upgrade is over. The hook is registered on ctx, not on attempt, whose
	// cancel above also runs after a successful attempt. Dial runs on this
	// goroutine, inside ConnectWebsocketPeer.
	var unhook []func() bool
	var conn *watchedConn // the last one dialed carries the WebSocket
	wsCfg := transport.WebsocketConfig{
		KeepAlive: c.keepAlive(),
		Dial: func(network, addr string) (net.Conn, error) {
			if c.t.onDial != nil {
				c.t.onDial()
			}
			var d net.Dialer
			nc, err := d.DialContext(attempt, network, addr)
			if err != nil {
				return nil, err
			}
			wc := &watchedConn{Conn: nc, dead: dead}
			unhook = append(unhook, context.AfterFunc(ctx, func() { _ = wc.Close() }))
			conn = wc
			return wc, nil
		},
	}
	inner, err := transport.ConnectWebsocketPeer(attempt, c.dialURL, serialize.MSGPACK, c.cfg.TLSConfig, logger, &wsCfg)
	for _, stop := range unhook {
		stop()
	}
	if err != nil {
		if ctx.Err() != nil {
			return nil, "", ctx.Err()
		}
		return nil, "", fmt.Errorf("connect to %s: %w", c.cfg.URL, err)
	}
	if ctx.Err() != nil {
		inner.Close() // ctx ended as the upgrade finished; the hook may have closed the socket
		return nil, "", ctx.Err()
	}

	p := newObservedPeer(inner, dead, peerOptions{
		conn:       conn,
		onEvicted:  c.registrationEvicted,
		stallAfter: c.t.responseTimeout,
		onStall:    c.clientStalled,
		holdLimit:  c.t.responseTimeout,
	})
	c.setPeer(p)
	// Stop must not wait for a join that is still waiting for the router.
	stopJoin := context.AfterFunc(ctx, p.closeNow)
	cli, err := client.NewClient(p, client.Config{
		Realm:        c.cfg.Realm,
		HelloDetails: nxwamp.Dict{"authid": authID},
		AuthHandlers: map[string]client.AuthFunc{
			"wampcra": func(ch *nxwamp.Challenge) (string, nxwamp.Dict) {
				c.log.Debug("WAMP-CRA challenge received", "authmethod", ch.AuthMethod)
				return crsign.RespondChallenge(secret, ch, nil), nxwamp.Dict{}
			},
		},
		ResponseTimeout: c.t.responseTimeout,
		Serialization:   client.MSGPACK,
		Logger:          logger,
	})
	interrupted := !stopJoin()
	if err != nil {
		p.closeNow() // NewClient closed it already on most paths; closing is idempotent
		c.setPeer(nil)
		if ctx.Err() != nil {
			return nil, "", ctx.Err()
		}
		reason, msg := p.abortReason()
		if reason != "" {
			if msg != "" {
				return nil, reason, fmt.Errorf("join refused: %s: %s", reason, msg)
			}
			return nil, reason, fmt.Errorf("join refused: %s", reason)
		}
		return nil, "", fmt.Errorf("join: %w", err)
	}
	s := newSession(cli, p)
	if interrupted {
		c.teardown(s, true)
		return nil, "", ctx.Err()
	}
	c.log.Debug("Joined realm", "authid", authID, "session", cli.ID())
	return s, "", nil
}

// runSession restores the tracked subscriptions and registrations on a
// freshly joined session, announces it, and blocks until it ends or ctx is
// done. It returns the close reason ("" when ctx is done).
//
// The join is counted before the restore, so that the events of s, which
// arrive once its subscriptions are restored, are handled under its count;
// the join hooks (see wampx.Joins) are called once the restore is done,
// while s is still up.
func (c *Connection) runSession(ctx context.Context, s *session) string {
	join := c.joins.Begin()
	failed, ok := c.establish(ctx, s)
	if !ok {
		c.markDown(s)
		c.teardown(s, s.alive())
		if ctx.Err() != nil {
			return ""
		}
		reason := s.closeReason()
		c.log.Warn("Connection lost while restoring subscriptions and registrations", "reason", reason)
		return reason
	}
	c.log.Info("Connection to IronFlock app realm established", "url", c.cfg.URL)
	if cb := c.cfg.OnConnect; cb != nil {
		c.runCallback("OnConnect", cb)
	}
	if !failed.empty() {
		go c.retryRestore(ctx, s, failed)
	}
	// s is the current session (only this goroutine withdraws it); it may
	// have ended already, and then the next join's hooks take over.
	if s.alive() {
		c.joins.Restored(join, c.log)
	}

	if !c.awaitEnd(ctx, s) {
		c.markDown(s)
		c.teardown(s, true)
		c.log.Info("Connection to IronFlock app realm closed by client")
		return ""
	}
	reason := s.closeReason()
	c.markDown(s)
	if ctx.Err() != nil {
		c.teardown(s, false)
		return ""
	}
	c.log.Warn("Connection to IronFlock app realm closed", "reason", reason)
	if cb := c.cfg.OnDisconnect; cb != nil {
		c.runCallback("OnDisconnect", func() { cb(reason) })
	}
	c.teardown(s, false)
	return reason
}

// awaitEnd blocks until the session s has ended (true) or ctx is done
// (false).
//
// A session ends with its client's receive loop, which exits moments after
// the connection is gone, once it has processed what was received before.
// But a nexus client's loop can wedge for good: a reply handed over just as
// its waiter timed out parks it, and nothing wakes it (a nexus bug). Its
// session would then never end, and nothing would reconnect. So a session
// whose connection has been gone for clientGrace while its client still runs
// has ended too; teardown abandons such a client. (A wedged client on a
// connection that stays up is caught by the forwarder: see
// observedPeer.deliver.)
func (c *Connection) awaitEnd(ctx context.Context, s *session) bool {
	select {
	case <-s.cli.Done():
		return true
	case <-ctx.Done():
		return false
	case <-s.peer.Done():
	}
	timer := time.NewTimer(c.t.clientGrace)
	defer timer.Stop()
	select {
	case <-s.cli.Done():
	case <-ctx.Done():
		return false
	case <-timer.C:
		c.log.Debug("The WAMP client has not ended with its connection", "grace", c.t.clientGrace)
	}
	return true
}

// establish restores every tracked subscription and registration onto s
// and then publishes s as the current session, unless ctx is done. Holding
// c.state throughout keeps restores and subscription changes from
// interleaving, and no operation sees the session before its restore is
// complete. It returns what failed to restore.
func (c *Connection) establish(ctx context.Context, s *session) (failed restoreList, ok bool) {
	if err := c.state.lock(ctx); err != nil {
		return restoreList{}, false
	}
	defer c.state.unlock()
	failed = c.restore(ctx, s)

	c.mu.Lock()
	defer c.mu.Unlock()
	if ctx.Err() != nil || !s.alive() {
		return restoreList{}, false
	}
	if hook := c.t.beforePublish; hook != nil {
		hook()
	}
	c.sess = s
	c.established = true
	close(c.upCh)
	return failed, true
}

// restoreList lists subscription groups and registrations to restore.
type restoreList struct {
	groups []*subGroup
	regs   []*Registration
}

func (l restoreList) empty() bool { return len(l.groups) == 0 && len(l.regs) == 0 }

// restore re-subscribes and re-registers every tracked entry on s, and
// returns the entries that failed: they stay tracked, inactive, and are
// retried while s lasts (see retryRestore) and after the next join. Entries
// removed while their removal waits for c.state are skipped. c.state must
// be held.
func (c *Connection) restore(ctx context.Context, s *session) (failed restoreList) {
	groups := c.groupList()
	if n := len(groups); n > 0 {
		c.log.Info("Resubscribing", "subscriptions", n)
	}
	for _, g := range groups {
		if !s.alive() || ctx.Err() != nil {
			return restoreList{}
		}
		if !g.wanted() {
			g.sess.Store(nil)
			continue
		}
		if err := c.restoreGroup(s, g); err != nil {
			c.log.Warn("Failed to restore subscription; retrying", "topic", g.topic, "error", err)
			failed.groups = append(failed.groups, g)
		}
	}
	if n := len(c.regs); n > 0 {
		c.log.Info("Re-registering", "procedures", n)
	}
	for _, r := range c.regs {
		if !s.alive() || ctx.Err() != nil {
			return restoreList{}
		}
		if r.removed.Load() {
			r.sess.Store(nil)
			continue
		}
		if err := c.restoreRegistration(s, r); err != nil {
			c.log.Warn("Failed to restore registration; retrying", "procedure", r.procedure, "error", err)
			failed.regs = append(failed.regs, r)
		}
	}
	return failed
}

// restoreGroup subscribes g's WAMP subscription on s. c.state must be held.
func (c *Connection) restoreGroup(s *session, g *subGroup) error {
	if err := s.subscribe(g.topic, g.onEvent, cloneDict(g.options)); err != nil {
		g.sess.Store(nil)
		return c.requestError("subscribe to topic '"+g.topic+"'", subscribePrefix(g.topic), err)
	}
	g.sess.Store(s)
	return nil
}

// restoreRegistration registers r on s. c.state must be held.
func (c *Connection) restoreRegistration(s *session, r *Registration) error {
	if err := s.register(r.procedure, r.invokeOn(s), cloneDict(r.options)); err != nil {
		r.sess.Store(nil)
		return c.requestError("register procedure '"+r.procedure+"'", registerPrefix(r.procedure), err)
	}
	r.sess.Store(s)
	return nil
}

// retryRestore retries, while the session s lasts, the entries its restore
// failed to restore. A router refuses such a restore only for a passing
// reason — the entries were accepted before — such as the router's
// identity check of a device function failing closed while its authorizer
// is slow or unavailable; without retries the entry would stay inactive
// until the next reconnect, which a stable link may never bring. Attempts
// follow restoreRetryFirstDelay, doubling up to restoreRetryMaxDelay, with
// jitter (the devices of a fleet reconnect together after a platform event).
// Each attempt holds c.state and goes on only while s is the current
// session; it skips the entries removed meanwhile, or restored otherwise (a
// new handler re-subscribes its group). A registration another session took
// over (force_reregister) is never retried here: it did not fail to restore.
// The retries end once nothing is left, or when s or ctx ends; the next
// join's restore takes over then.
func (c *Connection) retryRestore(ctx context.Context, s *session, pending restoreList) {
	delay := c.t.restoreRetryFirstDelay
	for {
		wait := delay
		if j := c.t.restoreRetryJitter; j > 0 {
			wait = time.Duration(float64(delay) * (1 + j*(2*c.t.rand()-1)))
		}
		timer := time.NewTimer(wait)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return
		case <-s.down:
			timer.Stop()
			return
		case <-s.peer.Done():
			timer.Stop()
			return
		}
		if !c.restoreAgain(ctx, s, &pending) {
			return
		}
		delay = min(2*delay, c.t.restoreRetryMaxDelay)
	}
}

// restoreAgain makes one attempt of retryRestore. It reports whether
// anything is left to retry on s.
func (c *Connection) restoreAgain(ctx context.Context, s *session, pending *restoreList) bool {
	if err := c.state.lock(ctx); err != nil {
		return false
	}
	defer c.state.unlock()
	if c.currentSession() != s || !s.alive() {
		return false
	}
	tracked := c.groupList()
	var groups []*subGroup
	for _, g := range pending.groups {
		if !slices.Contains(tracked, g) || !g.wanted() || g.sess.Load() == s {
			continue
		}
		if err := c.restoreGroup(s, g); err != nil {
			if !s.alive() {
				return false
			}
			c.log.Debug("Failed to restore subscription; retrying", "topic", g.topic, "error", err)
			groups = append(groups, g)
			continue
		}
		c.log.Info("Subscription restored", "topic", g.topic)
	}
	var regs []*Registration
	for _, r := range pending.regs {
		if !slices.Contains(c.regs, r) || r.removed.Load() || r.sess.Load() == s {
			continue
		}
		if err := c.restoreRegistration(s, r); err != nil {
			if !s.alive() {
				return false
			}
			c.log.Debug("Failed to restore registration; retrying", "procedure", r.procedure, "error", err)
			regs = append(regs, r)
			continue
		}
		c.log.Info("Registration restored", "procedure", r.procedure)
	}
	*pending = restoreList{groups: groups, regs: regs}
	return !pending.empty()
}

// markDown withdraws s as the current session.
func (c *Connection) markDown(s *session) {
	c.mu.Lock()
	if c.sess == s {
		c.sess = nil
		c.upCh = make(chan struct{})
	}
	c.mu.Unlock()
	s.markDown()
}

// teardown ends the session and closes its client and WebSocket, within
// bounds: with graceful set it first says GOODBYE (see leave). Calls still
// waiting on the session are ended, and invocation handlers still running
// see their ctx cancelled (see session.over).
func (c *Connection) teardown(s *session, graceful bool) {
	if !graceful || !c.leave(s) {
		s.peer.closeNow() // lost, or no GOODBYE: nothing more to say
	}
	// The receive loop ends once the WebSocket is closed or the router
	// answered the GOODBYE; Close then neither sends another GOODBYE nor
	// waits for an answer, it only waits for running invocation goroutines.
	// A wedged loop (see awaitEnd) never ends, and Close would wait for it
	// forever: such a client is abandoned. Its connection is closed already.
	timer := time.NewTimer(c.t.clientGrace)
	select {
	case <-s.cli.Done():
		timer.Stop()
		_ = s.cli.Close()
	case <-timer.C:
		c.log.Warn("The WAMP client did not shut down; abandoning it", "session", s.cli.ID())
	}
	s.endOver()
	c.setPeer(nil)
}

// leave sends GOODBYE and waits for the router's answer, each within the
// response timeout. It reports whether the session ended. nexus' own Close
// does the same but keeps waiting to hand the GOODBYE to a writer that has
// already died with its connection (Stop racing a dropped socket), for twice
// the response timeout. No answer can come once the connection is gone
// (forceClose closed it, for one).
func (c *Connection) leave(s *session) bool {
	timer := time.NewTimer(c.t.responseTimeout)
	defer timer.Stop()
	select {
	case s.peer.Send() <- &nxwamp.Goodbye{Reason: nxwamp.CloseRealm, Details: nxwamp.Dict{}}:
	case <-s.cli.Done():
		return true
	case <-s.peer.Done():
		return false
	case <-timer.C:
		return false
	}
	select {
	case <-s.cli.Done():
		return true
	case <-s.peer.Done():
		return false
	case <-timer.C:
		c.log.Debug("No GOODBYE from the router; closing the connection")
		return false
	}
}

// clientStalled reports a session whose client has stopped taking messages
// (see observedPeer.deliver).
func (c *Connection) clientStalled(after time.Duration) {
	c.log.Warn("The WAMP client stopped taking messages; closing the connection", "stalled_for", after)
}

// registrationEvicted logs a registration the router revoked because
// another session took its procedure over (force_reregister). The entry stays
// tracked, inactive, and is registered again after the next reconnect — the
// Python SDK's behaviour. It runs on the receive path, so the lookup of the
// procedure, which needs c.state, happens on a goroutine of its own.
func (c *Connection) registrationEvicted(id nxwamp.ID) {
	go func() {
		_ = c.state.lock(context.Background()) // never fails
		s := c.currentSession()
		procedure := ""
		for _, r := range c.regs {
			if s != nil && r.sess.Load() == s {
				if rid, ok := s.cli.RegistrationID(r.procedure); ok && rid == id {
					procedure = r.procedure
				}
			}
		}
		c.state.unlock()
		c.log.Warn("Registration taken over by another session (force_reregister); "+
			"it is registered again after the next reconnect", "procedure", procedure, "registration", id)
	}()
}

// deniedForGood records a fatal auth refusal of a FailOnAuthError connection
// and reports whether it is final: at once while the connection has never
// been established, so that Start fails fast; afterwards once the refusal
// persists (see Config.FailOnAuthError).
func (c *Connection) deniedForGood(denial *authDenial, reason string) bool {
	c.mu.Lock()
	established := c.established
	c.mu.Unlock()
	if !established {
		return true
	}
	persists, first := denial.refused()
	if first && !persists {
		c.log.Warn("Access refused; reconnecting, as the router also refuses while it cannot verify access: "+
			"the refusal is final only once it persists", "reason", reason,
			"attempts", c.t.authDenialAttempts, "for", c.t.authDenialGrace)
	}
	return persists
}

// failAuth stops the connection for good after a fatal auth denial.
func (c *Connection) failAuth(reason string) {
	c.mu.Lock()
	c.fatal = &AuthError{Realm: c.cfg.Realm, Reason: reason}
	c.stopped = true
	c.stopFlag.Store(true)
	c.cancelRun()
	c.mu.Unlock()
	c.log.Error("Authentication denied; not reconnecting", "reason", reason)
	if cb := c.cfg.OnAuthFailure; cb != nil {
		c.runCallback("OnAuthFailure", func() { cb(reason) })
	}
}

// credentials returns the WAMP-CRA credential of the next attempt: the
// explicit pair, or the injected per-app pair (re-read every attempt, so a
// rotated credential is picked up), or the legacy (serial, serial).
func (c *Connection) credentials() (authID, secret string) {
	if c.cfg.AuthID != "" && c.cfg.AuthSecret != "" {
		return c.cfg.AuthID, c.cfg.AuthSecret
	}
	return AppCredentials(c.cfg.SerialNumber)
}

func (c *Connection) keepAlive() time.Duration {
	switch {
	case c.cfg.KeepAlive < 0:
		return 0
	case c.cfg.KeepAlive == 0:
		return DefaultKeepAlive
	}
	return c.cfg.KeepAlive
}

func (c *Connection) setPeer(p *observedPeer) {
	c.mu.Lock()
	c.peer = p
	c.mu.Unlock()
}

// forceClose closes the connection of the current session (or of a join in
// progress) without waiting for the router or the network: Stop past its
// deadline, or a failed Start whose supervisor is slow to exit. It is
// called once the supervisor's context is done, which already closed the
// socket of an attempt still in its WebSocket handshake (see dialAndJoin).
// After it, the supervisor exits within at most a response timeout (a
// restore request on a wedged client) plus clientGrace.
func (c *Connection) forceClose() {
	c.mu.Lock()
	p := c.peer
	c.mu.Unlock()
	if p != nil {
		p.closeNow()
	}
}

// toDialURL converts the configured router URL to the ws:// or wss:// form
// the WebSocket dialer accepts.
func toDialURL(raw string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", fmt.Errorf("wamp: invalid router URL %q: %w", raw, err)
	}
	switch u.Scheme {
	case "ws", "wss":
	case "http":
		u.Scheme = "ws"
	case "https":
		u.Scheme = "wss"
	default:
		return "", fmt.Errorf("wamp: invalid router URL %q: scheme must be ws, wss, http or https", raw)
	}
	if u.Host == "" {
		return "", fmt.Errorf("wamp: invalid router URL %q: missing host", raw)
	}
	return u.String(), nil
}

func subscribePrefix(topic string) string    { return "subscribing to topic '" + topic + "': " }
func registerPrefix(procedure string) string { return "registering procedure '" + procedure + "': " }
func unsubscribePrefix(topic string) string  { return "unsubscribing to '" + topic + "': " }
func unregisterPrefix(procedure string) string {
	return "unregistering procedure '" + procedure + "': "
}

const publishPrefix = "waiting for published message: "
