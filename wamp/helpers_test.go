package wamp

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ironflock/nexus/v3/client"
	"github.com/ironflock/nexus/v3/router"
	"github.com/ironflock/nexus/v3/router/auth"
	nxwamp "github.com/ironflock/nexus/v3/wamp"
)

const (
	testRealm  = "realm-1-2-dev"
	testAuthID = "app-auth-id"
	testSecret = "app-secret"
	testSerial = "test-device"
)

// keyStore is a static WAMP-CRA key store for the test router.
type keyStore struct {
	mu    sync.Mutex
	users map[string]string // authid -> secret
}

func (k *keyStore) set(authID, secret string) {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.users[authID] = secret
}

func (k *keyStore) AuthKey(authid, _ string) ([]byte, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	secret, ok := k.users[authid]
	if !ok {
		return nil, errors.New("no such user")
	}
	return []byte(secret), nil
}

func (k *keyStore) PasswordInfo(string) (string, int, int) { return "", 0, 0 }
func (k *keyStore) AuthRole(string) (string, error)        { return "device", nil }
func (k *keyStore) Provider() string                       { return "static" }

// recordingAuth records the authid of every HELLO before delegating to the
// WAMP-CRA authenticator.
type recordingAuth struct {
	auth.Authenticator
	mu      sync.Mutex
	authIDs []string
}

func (a *recordingAuth) Authenticate(sid nxwamp.ID, details nxwamp.Dict, peer nxwamp.Peer) (*nxwamp.Welcome, error) {
	id, _ := nxwamp.AsString(details["authid"])
	a.mu.Lock()
	a.authIDs = append(a.authIDs, id)
	a.mu.Unlock()
	return a.Authenticator.Authenticate(sid, details, peer)
}

func (a *recordingAuth) attempts() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]string(nil), a.authIDs...)
}

// testAuthorizer denies selected operations, records REGISTER options and the
// messages it sees, and holds selected messages until the test releases them.
// The router authorizes a session's messages one after another, so a held
// message holds every later message of its session, but not what the router
// sends to the session.
type testAuthorizer struct {
	mu           sync.Mutex
	deny         map[string]bool // keys as authzKey makes them
	registerOpts map[string]nxwamp.Dict
	holds        map[string]chan struct{} // held until closed
	held         chan string              // the key of every message being held
	seen         []string                 // the key of every message, in order
}

func newTestAuthorizer() *testAuthorizer {
	return &testAuthorizer{
		deny:         map[string]bool{},
		registerOpts: map[string]nxwamp.Dict{},
		holds:        map[string]chan struct{}{},
		held:         make(chan string, 16),
	}
}

// authzKey names a message: "publish:<topic>", "subscribe:<topic>",
// "register:<procedure>", "call:<procedure>", or the message type
// ("UNSUBSCRIBE", "UNREGISTER", ...).
func authzKey(msg nxwamp.Message) string {
	switch m := msg.(type) {
	case *nxwamp.Publish:
		return "publish:" + string(m.Topic)
	case *nxwamp.Subscribe:
		return "subscribe:" + string(m.Topic)
	case *nxwamp.Register:
		return "register:" + string(m.Procedure)
	case *nxwamp.Call:
		return "call:" + string(m.Procedure)
	}
	return msg.MessageType().String()
}

func (a *testAuthorizer) setDeny(key string, deny bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.deny[key] = deny
}

func (a *testAuthorizer) registered(proc string) nxwamp.Dict {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.registerOpts[proc]
}

// hold makes the router hold the messages with key until release is called,
// at the latest at the end of the test. a.held receives the key of each.
func (a *testAuthorizer) hold(t *testing.T, key string) (release func()) {
	t.Helper()
	ch := make(chan struct{})
	a.mu.Lock()
	a.holds[key] = ch
	a.mu.Unlock()
	var once sync.Once
	release = func() {
		once.Do(func() {
			a.mu.Lock()
			delete(a.holds, key)
			a.mu.Unlock()
			close(ch)
		})
	}
	t.Cleanup(release)
	return release
}

// count returns how many messages with key the router has seen.
func (a *testAuthorizer) count(key string) int {
	a.mu.Lock()
	defer a.mu.Unlock()
	n := 0
	for _, k := range a.seen {
		if k == key {
			n++
		}
	}
	return n
}

func (a *testAuthorizer) Authorize(_ *nxwamp.Session, msg nxwamp.Message) (bool, error) {
	key := authzKey(msg)
	a.mu.Lock()
	a.seen = append(a.seen, key)
	if m, ok := msg.(*nxwamp.Register); ok {
		opts := make(nxwamp.Dict, len(m.Options))
		for k, v := range m.Options {
			opts[k] = v
		}
		a.registerOpts[string(m.Procedure)] = opts
	}
	deny, hold := a.deny[key], a.holds[key]
	a.mu.Unlock()
	if hold != nil {
		select {
		case a.held <- key:
		default:
		}
		<-hold
	}
	return !deny, nil
}

// trackingListener remembers accepted connections so a test can drop them:
// httptest does not close hijacked (WebSocket) connections.
type trackingListener struct {
	net.Listener
	mu    sync.Mutex
	conns []net.Conn
}

func (l *trackingListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err == nil {
		l.mu.Lock()
		l.conns = append(l.conns, conn)
		l.mu.Unlock()
	}
	return conn, err
}

func (l *trackingListener) dropAll() {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, c := range l.conns {
		_ = c.Close()
	}
	l.conns = nil
}

// testRouter is an in-process nexus router behind a WebSocket server.
type testRouter struct {
	t        *testing.T
	r        router.Router
	srv      *httptest.Server
	ln       *trackingListener
	url      string
	keys     *keyStore
	auth     *recordingAuth
	authz    *testAuthorizer
	protoMu  sync.Mutex
	protocol []string // Sec-WebSocket-Protocol of every upgrade request
	handlers sync.WaitGroup
	closed   atomic.Bool
	inject   atomic.Pointer[injectFunc] // see injectAfter
}

// injectFunc returns the messages to send a client right behind msg.
type injectFunc func(msg nxwamp.Message) []nxwamp.Message

// newTestRouter starts a router; withRealm controls whether testRealm exists.
func newTestRouter(t *testing.T, withRealm bool) *testRouter {
	t.Helper()
	keys := &keyStore{users: map[string]string{testAuthID: testSecret}}
	tr := &testRouter{
		t:     t,
		keys:  keys,
		auth:  &recordingAuth{Authenticator: auth.NewCRAuthenticator(keys, time.Second)},
		authz: newTestAuthorizer(),
	}
	cfg := &router.Config{}
	if withRealm {
		cfg.RealmConfigs = []*router.RealmConfig{tr.realmConfig()}
	}
	r, err := router.NewRouter(cfg, log.New(io.Discard, "", 0))
	if err != nil {
		t.Fatalf("router: %v", err)
	}
	tr.r = r
	wss := router.NewWebsocketServer(&hookedRouter{Router: r, tr: tr})
	handler := http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		// ServeHTTP returns once the session is attached (or refused);
		// httptest stops tracking the request when it is hijacked.
		tr.handlers.Add(1)
		defer tr.handlers.Done()
		tr.protoMu.Lock()
		tr.protocol = append(tr.protocol, req.Header.Get("Sec-Websocket-Protocol"))
		tr.protoMu.Unlock()
		wss.ServeHTTP(w, req)
	})
	tr.srv = httptest.NewUnstartedServer(handler)
	tr.ln = &trackingListener{Listener: tr.srv.Listener}
	tr.srv.Listener = tr.ln
	tr.srv.Start()
	tr.url = "ws" + strings.TrimPrefix(tr.srv.URL, "http") + "/ws-ua-usr"
	t.Cleanup(tr.close)
	return tr
}

func (tr *testRouter) realmConfig() *router.RealmConfig {
	return &router.RealmConfig{
		URI:            testRealm,
		Authenticators: []auth.Authenticator{tr.auth},
		Authorizer:     tr.authz,
		// A held message must not hold the broker (see testAuthorizer); the
		// authorizer never alters session details.
		AuthorizeUnlocked: true,
		EnableMetaKill:    true,
	}
}

// injectAfter makes the router send every WebSocket client that connects
// from now on the messages f returns for a message, right behind it.
func (tr *testRouter) injectAfter(f injectFunc) { tr.inject.Store(&f) }

// hookedRouter attaches WebSocket clients through an injectingPeer while the
// test router has an inject hook.
type hookedRouter struct {
	router.Router
	tr *testRouter
}

func (h *hookedRouter) AttachClient(p nxwamp.Peer, details nxwamp.Dict) error {
	if f := h.tr.inject.Load(); f != nil {
		p = newInjectingPeer(p, *f)
	}
	return h.Router.AttachClient(p, details)
}

// injectingPeer is the router's end of a client connection that writes the
// client extra messages right behind the ones its inject function picks, as
// a router does when a message follows its reply at once. Like the router's
// WebSocket peer it queues what the router sends: the router drops a
// message rather than wait for a full queue.
type injectingPeer struct {
	nxwamp.Peer
	out    chan nxwamp.Message
	inject injectFunc
}

func newInjectingPeer(p nxwamp.Peer, inject injectFunc) *injectingPeer {
	ip := &injectingPeer{Peer: p, out: make(chan nxwamp.Message, 256), inject: inject}
	go ip.forward()
	return ip
}

func (p *injectingPeer) Send() chan<- nxwamp.Message { return p.out }

func (p *injectingPeer) forward() {
	to := p.Peer.Send()
	for {
		select {
		case msg := <-p.out:
			for _, m := range append([]nxwamp.Message{msg}, p.inject(msg)...) {
				select {
				case to <- m:
				case <-p.Done():
					return
				}
			}
		case <-p.Done():
			return
		}
	}
}

// lookup asks the router for the subscription ID of topic (meta procedure
// "wamp.subscription.lookup") or the registration ID of procedure
// ("wamp.registration.lookup"); 0 means there is none.
func (tr *testRouter) lookup(t *testing.T, meta nxwamp.URI, uri string) nxwamp.ID {
	t.Helper()
	res, err := tr.local(t).Call(ctxTimeout(t, 5*time.Second), string(meta), nil, nxwamp.List{uri}, nil, nil)
	if err != nil {
		t.Fatalf("%s(%s): %v", meta, uri, err)
	}
	if len(res.Arguments) == 0 {
		return 0
	}
	id, _ := nxwamp.AsID(res.Arguments[0])
	return id
}

func (tr *testRouter) addRealm() {
	tr.t.Helper()
	if err := tr.r.AddRealm(tr.realmConfig()); err != nil {
		tr.t.Fatalf("AddRealm: %v", err)
	}
}

func (tr *testRouter) removeRealm() { tr.r.RemoveRealm(testRealm) }

func (tr *testRouter) protocols() []string {
	tr.protoMu.Lock()
	defer tr.protoMu.Unlock()
	return append([]string(nil), tr.protocol...)
}

// close stops the server and the router. Connections must be stopped first,
// and no join may still be in progress: the nexus router races (and panics
// on a closed channel) when it is closed while attaching a session.
func (tr *testRouter) close() {
	if !tr.closed.CompareAndSwap(false, true) {
		return
	}
	tr.srv.Close()
	tr.ln.dropAll()
	tr.handlers.Wait()
	tr.r.Close()
}

// local returns a trusted in-process client on testRealm.
func (tr *testRouter) local(t *testing.T) *client.Client {
	t.Helper()
	cli, err := client.ConnectLocal(tr.r, client.Config{
		Realm:  testRealm,
		Logger: log.New(io.Discard, "", 0),
	})
	if err != nil {
		t.Fatalf("local client: %v", err)
	}
	t.Cleanup(func() { _ = cli.Close() })
	return cli
}

// logBuffer captures slog output for assertions.
type logBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *logBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *logBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func (b *logBuffer) count(substr string) int {
	return strings.Count(b.String(), substr)
}

// fastTunables shortens every delay for tests and disables jitter.
func fastTunables(c *Connection) {
	c.t.initialRetryDelay = 10 * time.Millisecond
	c.t.baseMaxRetryDelay = 20 * time.Millisecond
	c.t.retryDelayJitter = 0
	c.t.retryFirstDelay = 10 * time.Millisecond
	c.t.retryMaxDelay = 40 * time.Millisecond
	c.t.responseTimeout = 2 * time.Second
	c.t.connectTimeout = 2 * time.Second
}

type connOption func(*Config, *Connection)

// newTestConn returns a configured (not started) connection to tr. Stop is
// registered as cleanup.
func newTestConn(t *testing.T, tr *testRouter, opts ...connOption) (*Connection, *logBuffer) {
	t.Helper()
	logs := &logBuffer{}
	c := NewConnection()
	fastTunables(c)
	cfg := Config{
		SwarmKey:     1,
		AppKey:       2,
		Stage:        StageDevelopment,
		URL:          tr.url,
		SerialNumber: testSerial,
		AuthID:       testAuthID,
		AuthSecret:   testSecret,
		Logger:       slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
	}
	for _, o := range opts {
		o(&cfg, c)
	}
	if err := c.Configure(cfg); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := c.Stop(ctx); err != nil {
			t.Errorf("Stop: %v", err)
		}
	})
	return c, logs
}

// startTestConn returns a started connection.
func startTestConn(t *testing.T, tr *testRouter, opts ...connOption) (*Connection, *logBuffer) {
	t.Helper()
	c, logs := newTestConn(t, tr, opts...)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.Start(ctx); err != nil {
		t.Fatalf("Start: %v\nlogs:\n%s", err, logs)
	}
	return c, logs
}

// eventually polls cond until it holds or timeout passes.
func eventually(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// recv waits for a value from ch.
func recv[T any](t *testing.T, ch <-chan T, what string) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
	var zero T
	return zero
}

// noRecv asserts nothing arrives on ch for a short while.
func noRecv[T any](t *testing.T, ch <-chan T, what string) {
	t.Helper()
	select {
	case v := <-ch:
		t.Fatalf("unexpected %s: %v", what, v)
	case <-time.After(100 * time.Millisecond):
	}
}

func ctxTimeout(t *testing.T, d time.Duration) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	t.Cleanup(cancel)
	return ctx
}

// localPublish publishes from a trusted local client.
func localPublish(t *testing.T, cli *client.Client, topic string, args nxwamp.List, kwargs nxwamp.Dict) {
	t.Helper()
	if err := cli.Publish(topic, nxwamp.Dict{nxwamp.OptAcknowledge: true}, args, kwargs); err != nil {
		t.Fatalf("publish %s: %v", topic, err)
	}
}

var goroutineHeader = regexp.MustCompile(`^goroutine (\d+) `)

// interestingGoroutines returns the stacks of goroutines running SDK or
// nexus code (not test code), keyed by goroutine ID.
func interestingGoroutines() map[string]string {
	buf := make([]byte, 1<<20)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			buf = buf[:n]
			break
		}
		buf = make([]byte, 2*len(buf))
	}
	out := map[string]string{}
	for _, g := range strings.Split(string(buf), "\n\n") {
		m := goroutineHeader.FindStringSubmatch(g)
		if m == nil {
			continue
		}
		relevant := false
		for _, line := range strings.Split(g, "\n") {
			line = strings.TrimSpace(line)
			if strings.HasPrefix(line, "github.com/ironflock/nexus/") ||
				(strings.HasPrefix(line, "github.com/ironflock/sdk-go/wamp.") &&
					!strings.Contains(line, "wamp.Test") && !strings.Contains(line, "_test.go")) {
				relevant = true
			}
		}
		if relevant && !strings.Contains(g, "_test.go") {
			out[m[1]] = g
		}
	}
	return out
}
