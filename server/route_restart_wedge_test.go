// Copyright 2026 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package server

// Reproduction test for a production wedge observed on a live 3-node
// JetStream cluster (nats-server 2.14.7 / 2.15.0 / 2.15.1):
//
//   * After ANY member restarts (fly.io machine stop: peers get no clean
//     FIN/RST, so they keep the dead member's route connections registered
//     until the route ping timer reaps them), exactly ONE unordered node
//     pair stops exchanging subscription interest in BOTH directions while
//     their route TCP connection(s) stay up with traffic. A message
//     published on A for a plain subject subscribed only on B never
//     arrives; the wedge persists for many minutes and only heals when
//     another node is restarted.
//   * Decisive live measurement on a wedged pair (A=286 / B=683): B created
//     a NEW unique subscription; A's /routez?subs=1 entry for B kept
//     `subscriptions` frozen at the stale pre-wedge value (~107) while
//     `in_msgs` on the same connection kept growing. Both directions behave
//     identically (symmetric wedge), and each side also holds a second,
//     fully idle connection to the other (subs=0, in/out=0) - the dedicated
//     $SYS route that a default pooled config creates.
//
// Route pooling is ON by default (DEFAULT_ROUTE_POOL_SIZE = 3, applied when
// a config file sets no pool_size; see opts.go setClusterDefaults). For the
// global account, RS+ interest incrementals for a pair flow over the single
// pool slot chosen by fnv32a(account)%pool on BOTH sides, so damaged slot
// registration on either side severs new-interest propagation for the pair.
//
// The restart must mimic a machine stop: peers must NOT receive clean FINs,
// otherwise they reap the dead member instantly and the re-join race never
// happens. A local SIGKILL/SIGSTOP of an exec'd process cannot reproduce that
// (the kernel still closes or signals the sockets; verified empirically on
// darwin - peers reap within milliseconds of a SIGSTOP). So this test
// blackholes the victim's route connections from inside: it swaps each route
// client's nc for one end of a loopback TCP connection whose peer is never
// read. The victim's read loops keep draining peer traffic while every write
// vanishes, so peers see exactly the production shape - an ESTABLISHED
// connection that stops answering PINGs - and reap it only at the route ping
// timeout. The victim is then shut down and re-run; its successor re-joins
// while peers still hold the dead generation registered, which is the race
// that wedges a pair.
//
// Two tests live in this file:
//
//   - TestRoutePoolInterestAfterMemberRestart runs the natural repro: a
//     bounded loop of machine-stop restarts with JetStream enabled and
//     subscription churn during the re-join window, checking fresh
//     all-pairs interest after every cycle. It also verifies the pool-slot
//     mirror invariant each cycle. This is the test CI should keep; on the
//     unpatched tree it passes, which is itself a data point (see below).
//
//   - TestRoutePoolInterestDeadSlotInjection constructs the wedged steady
//     state directly - a registered-but-dead route connection at the global
//     account's pool slot of one pair, on both ends - and verifies that new
//     interest freezes in both directions while pre-existing interest keeps
//     delivering, and that a forced re-join does not heal it. This is the
//     deterministic RED reproduction; its header maps every step to the
//     production evidence.
//
// The natural loop did not go red within 100 single-restart cycles plus
// overlapping-restart (cascade) cycles on an unpatched v2.15.1 tree: the
// connect-time full dump (sendSubsToRoute) re-syncs any interest lost while
// a slot is momentarily dead, so the wedge only persists when a pair's slot
// ends up occupied by a never-reaped dead registration. The injection test
// builds exactly that end state.
//
// Environment knobs:
//
//	WEDGE_RESTART_STYLE   "machinestop" (default), "cascade" (two
//	                      overlapping machine stops per cycle) or "graceful"
//	WEDGE_RESTART_CYCLES  restart cycles per run (default 20)
//	WEDGE_DEBUG           dump per-server route state on failure

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

const (
	wedgeNumServers = 3
	wedgePoolSize   = 3 // DEFAULT_ROUTE_POOL_SIZE, implicit in the conf
)

type wedgeCluster struct {
	t        *testing.T
	confs    map[string]string
	servers  map[string]*Server
	order    []string // A, B, C - restart victim rotates through this
	replaced []net.Conn
	bhEnds   []net.Conn
	stashed  []*client        // deregistered-but-alive route clients; kept live on purpose
	injected map[*client]bool // dead-slot route clients; never torn down
}

// wedgeBlackhole replaces every route connection's nc with one end of a
// UNIX socketpair (the other end is held by the test and never read). All
// victim writes now vanish silently; reads keep draining the real socket, so
// the peer sees an ESTABLISHED connection that stops answering - a machine
// stop. The real connections are stashed and closed only after the cycle's
// checks, so no FIN/RST can leak into the race window.
func (wc *wedgeCluster) blackholeRoutes(victim string) {
	v := wc.servers[victim]
	stash := func(c *client) {
		c.mu.Lock()
		if c.kind == ROUTER && c.nc != nil {
			bh := wc.blackholeConn()
			wc.replaced = append(wc.replaced, c.nc)
			wc.bhEnds = append(wc.bhEnds, bh)
			c.nc = bh
		}
		c.mu.Unlock()
	}
	v.mu.RLock()
	v.forEachRoute(stash)
	v.mu.RUnlock()
}

func (wc *wedgeCluster) releaseBlackholes() {
	for _, nc := range wc.replaced {
		nc.Close()
	}
	for _, nc := range wc.bhEnds {
		nc.Close()
	}
	wc.replaced = nil
	wc.bhEnds = nil
}

// wedgeBlackholeConn returns a loopback TCP connection whose peer is never
// read: writes buffer silently in the kernel (a machine stop from the
// peer's point of view), reads block forever. The peer end is stashed in
// wc.bhEnds and closed at cycle end.
func (wc *wedgeCluster) blackholeConn() net.Conn {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		wc.t.Fatalf("blackhole listen: %v", err)
	}
	defer l.Close()
	type res struct {
		c   net.Conn
		err error
	}
	ch := make(chan res, 1)
	go func() {
		c, err := l.Accept()
		ch <- res{c, err}
	}()
	d := net.Dialer{Timeout: 2 * time.Second}
	cli, err := d.Dial("tcp", l.Addr().String())
	if err != nil {
		wc.t.Fatalf("blackhole dial: %v", err)
	}
	srv := <-ch
	if srv.err != nil {
		wc.t.Fatalf("blackhole accept: %v", srv.err)
	}
	// srv is never read: victim writes vanish. cli is handed to the client.
	wc.bhEnds = append(wc.bhEnds, srv.c)
	return cli
}

func (wc *wedgeCluster) restartServer(name string) {
	old := wc.servers[name]
	done := make(chan struct{})
	go func() {
		old.Shutdown()
		old.WaitForShutdown()
		close(done)
	}()
	// A blackholed route readLoop only unblocks when the peer reaps and FINs
	// the connection; guard against a harness deadlock masking the test.
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		wc.t.Fatalf("shutdown of %s timed out (leaked blackholed route conn?)", name)
	}
	s, _ := RunServerWithConfig(wc.confs[name])
	wc.servers[name] = s
}

// churnSubscriptions models the JetStream re-join burst: while the victim is
// down and the mesh is re-forming, every live server creates and removes
// batches of unique plain and queue subscriptions, so interest incrementals
// race the reconnecting routes' full dumps. Returns a stop func that drains
// and unsubscribes everything.
func (wc *wedgeCluster) churnSubscription(cycle int, names []string) func() {
	var mu sync.Mutex
	var subs []*nats.Subscription
	var conns []*nats.Conn
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for _, n := range names {
		nc := natsConnect(wc.t, wc.servers[n].ClientURL())
		mu.Lock()
		conns = append(conns, nc)
		mu.Unlock()
		wg.Add(1)
		go func(nc *nats.Conn, server string) {
			defer wg.Done()
			i := 0
			for {
				select {
				case <-stop:
					return
				default:
				}
				base := fmt.Sprintf("_churn.c%d.%s.%d", cycle, server, i)
				s, err := nc.Subscribe(base, func(*nats.Msg) {})
				if err == nil {
					mu.Lock()
					subs = append(subs, s)
					mu.Unlock()
					nc.Flush()
				}
				q, err := nc.QueueSubscribe(base+".q", "wq", func(*nats.Msg) {})
				if err == nil {
					mu.Lock()
					subs = append(subs, q)
					mu.Unlock()
					nc.Flush()
				}
				i++
				time.Sleep(5 * time.Millisecond)
			}
		}(nc, n)
	}
	return func() {
		close(stop)
		wg.Wait()
		mu.Lock()
		for _, s := range subs {
			_ = s.Unsubscribe()
		}
		for _, nc := range conns {
			nc.Close()
		}
		mu.Unlock()
	}
}

func wedgeClusterConf(t *testing.T, name, clusterPort string, routePorts []string) string {
	t.Helper()
	var routes string
	for _, p := range routePorts {
		routes += fmt.Sprintf("nats://127.0.0.1:%s, ", p)
	}
	// JetStream is part of the production trigger: a re-joining member
	// starts meta-group traffic and consumer recovery, which bursts
	// ephemeral internal subscriptions across all nodes exactly while the
	// dead generation's routes are still registered at the peers.
	conf := fmt.Sprintf(`
		server_name: %s
		host: 127.0.0.1
		port: -1
		jetstream: true
		jetstream {
			store_dir: %s
			max_mem_store: 100MB
			max_file_store: 200MB
		}
		cluster {
			name: wedge
			host: 127.0.0.1
			port: %s
			routes: [%s]
		}
		ping_interval: 500ms
		ping_max: 2
	`, name, filepath.Join(t.TempDir(), name), clusterPort, routes)
	return createConfFile(t, []byte(conf))
}

// meshFormed mirrors checkClusterFormed's accounting (3 pooled + 1 $SYS
// route per pair) and additionally requires that no route to a dead
// generation lingers.
func (wc *wedgeCluster) meshFormed() error {
	live := make(map[*Server]string)
	for _, n := range wc.order {
		live[wc.servers[n]] = wc.servers[n].ID()
	}
	for _, n := range wc.order {
		s := wc.servers[n]
		perRemote := make(map[string]int)
		deadGen := false
		s.mu.RLock()
		s.forEachRoute(func(r *client) {
			r.mu.Lock()
			rid := r.route.remoteID
			r.mu.Unlock()
			if _, ok := liveID(live, rid); !ok {
				deadGen = true
			}
			perRemote[rid]++
		})
		s.mu.RUnlock()
		if deadGen {
			return fmt.Errorf("%s still holds a route to a dead generation", n)
		}
		for _, other := range wc.order {
			if other == n {
				continue
			}
			// Production pairs settle at anywhere from 1 pool conn + the
			// dedicated $SYS route up to the full pool: the refill chain
			// (startNewRoute after the first PONG) only runs on the
			// soliciting side and is not retried when its link is
			// duplicate-closed mid-churn. Do not force the full pool here -
			// the wedge state production hits includes the partial pool.
			if nRoutes := perRemote[live[wc.servers[other]]]; nRoutes < 2 {
				return fmt.Errorf("%s has %d routes to %s, want at least 2 (1 pool + $SYS)",
					n, nRoutes, other)
			}
		}
	}
	return nil
}

func liveID(live map[*Server]string, id string) (string, bool) {
	for _, v := range live {
		if v == id {
			return v, true
		}
	}
	return "", false
}

type wedgeRouteEnd struct {
	name   string
	id     string // remote server ID
	slot   int
	local  string
	remote string
	subs   int
	closed bool
}

func (wc *wedgeCluster) routeEnds(n string) []wedgeRouteEnd {
	s := wc.servers[n]
	var ends []wedgeRouteEnd
	s.mu.RLock()
	s.forEachRoute(func(r *client) {
		r.mu.Lock()
		e := wedgeRouteEnd{
			name:   n,
			id:     r.route.remoteID,
			slot:   r.route.poolIdx,
			subs:   len(r.subs),
			closed: r.isClosed(),
		}
		if r.nc != nil {
			e.local = r.nc.LocalAddr().String()
			e.remote = r.nc.RemoteAddr().String()
		}
		r.mu.Unlock()
		ends = append(ends, e)
	})
	s.mu.RUnlock()
	return ends
}

// mirrorIntact verifies the invariant TestRoutePoolRouteStoredSameIndexBothSides
// checks by address: the two ends of every route TCP connection must be
// registered at the same pool slot. A broken mirror after re-join is the
// wedge precursor: interest incrementals for an account flow over the slot
// chosen on the sender's side, which then lands on a different (or no live)
// connection at the receiver.
func (wc *wedgeCluster) mirrorIntact() error {
	for _, n := range wc.order {
		ends := wc.routeEnds(n)
		for _, m := range wc.order {
			if m == n {
				continue
			}
			other := wc.routeEnds(m)
			for _, e := range ends {
				if e.id != wc.servers[m].ID() || e.slot < 0 {
					continue
				}
				found := false
				for _, o := range other {
					if o.local == e.remote && o.remote == e.local {
						found = true
						if o.slot != e.slot {
							return fmt.Errorf("mirror broken: %s slot %d <-> %s slot %d (%s <-> %s)",
								n, e.slot, m, o.slot, e.local, e.remote)
						}
						break
					}
				}
				if !found {
					return fmt.Errorf("%s has registered route end %s (slot %d) with no counterpart at %s",
						n, e.remote, e.slot, m)
				}
			}
		}
	}
	return nil
}

// interestFlows creates a fresh unique subscription on the subscriber's
// server, then requires the subject to show up in the publisher server's
// sublist (checkSubInterest, the in-package equivalent of the production
// /routez measurement) and to deliver end-to-end. It fails the test on
// timeout because in the natural restart loop interest MUST flow.
func (wc *wedgeCluster) interestFlows(pubName, subName, subject string) error {
	wc.t.Helper()
	if err := wc.interestProbe(pubName, subName, subject, 3*time.Second); err != nil {
		wc.t.Fatal(err)
	}
	return nil
}

// interestProbe is the non-fatal variant of interestFlows used by the
// injection test: it returns an error instead of failing the test, so the
// caller can report the wedge and continue dumping state.
func (wc *wedgeCluster) interestProbe(pubName, subName, subject string, timeout time.Duration) error {
	wc.t.Helper()
	pub, sub := wc.servers[pubName], wc.servers[subName]

	ncSub := natsConnect(wc.t, sub.ClientURL())
	defer ncSub.Close()
	ch := make(chan *nats.Msg, 16)
	if _, err := ncSub.ChanSubscribe(subject, ch); err != nil {
		return fmt.Errorf("subscribe on %s: %v", subName, err)
	}
	natsFlush(wc.t, ncSub)

	acc, err := pub.LookupAccount(globalAccountName)
	if err != nil {
		return fmt.Errorf("lookup account on %s: %v", pubName, err)
	}
	deadline := time.Now().Add(timeout)
	for !acc.SubscriptionInterest(subject) {
		if time.Now().After(deadline) {
			return fmt.Errorf("%s never learned %q (subscribed on %s): new route interest wedged",
				pubName, subject, subName)
		}
		time.Sleep(20 * time.Millisecond)
	}

	ncPub := natsConnect(wc.t, pub.ClientURL())
	defer ncPub.Close()
	for i := 0; i < 3; i++ {
		natsPub(wc.t, ncPub, subject, []byte("wedge"))
	}
	for i := 0; i < 3; i++ {
		select {
		case m := <-ch:
			if string(m.Data) != "wedge" {
				return fmt.Errorf("unexpected payload %q", m.Data)
			}
		case <-time.After(2 * time.Second):
			return fmt.Errorf("message %d on %q published on %s never reached subscriber on %s (route delivery wedged)",
				i, subject, pubName, subName)
		}
	}
	return nil
}

func (wc *wedgeCluster) dumpRoutes(context string) {
	if os.Getenv("WEDGE_DEBUG") == "" {
		return
	}
	wc.t.Logf("--- route state at %s", context)
	for _, n := range wc.order {
		s := wc.servers[n]
		s.mu.RLock()
		var lines []string
		s.forEachRoute(func(r *client) {
			r.mu.Lock()
			lines = append(lines, fmt.Sprintf("remote=%.8s idx=%d acc=%q subs=%d closed=%v",
				r.route.remoteID, r.route.poolIdx, r.route.accName, len(r.subs), r.isClosed()))
			r.mu.Unlock()
		})
		s.mu.RUnlock()
		wc.t.Logf("  %s(%.8s): %v", n, s.ID(), lines)
	}
}

func TestRoutePoolInterestAfterMemberRestart(t *testing.T) {
	style := os.Getenv("WEDGE_RESTART_STYLE")
	if style == "" {
		style = "machinestop"
	}
	graceful := style == "graceful"
	cycles := 20
	if v := os.Getenv("WEDGE_RESTART_CYCLES"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			cycles = n
		}
	}

	wc := newWedgeCluster(t)
	wc.order = []string{"A", "B", "C"}

	// Fixed cluster ports per name so a restarted member re-joins through
	// the same addresses, like production.
	clusterPorts := map[string]string{}
	for i, n := range wc.order {
		clusterPorts[n] = fmt.Sprintf("%d", 22331+i)
	}
	for _, n := range wc.order {
		var peers []string
		for _, m := range wc.order {
			if m != n {
				peers = append(peers, clusterPorts[m])
			}
		}
		wc.confs[n] = wedgeClusterConf(t, n, clusterPorts[n], peers)
	}

	for _, n := range wc.order {
		s, _ := RunServerWithConfig(wc.confs[n])
		wc.servers[n] = s
		defer wc.servers[n].Shutdown()
	}

	checkFor(t, 30*time.Second, 50*time.Millisecond, wc.meshFormed)

	// Sanity: interest propagates all-pairs before any restart.
	for _, pub := range wc.order {
		for _, sub := range wc.order {
			if pub == sub {
				continue
			}
			if err := wc.interestFlows(pub, sub, fmt.Sprintf("_sanity.%s.%s", pub, sub)); err != nil {
				t.Fatalf("pre-restart sanity failed: %v", err)
			}
		}
	}
	t.Logf("sanity: all-pairs interest OK")

	for i := 0; i < cycles; i++ {
		victim := wc.order[i%wedgeNumServers]
		t.Logf("cycle %d: restarting %s (style %s)", i, victim, style)

		peers := make([]string, 0, wedgeNumServers-1)
		for _, n := range wc.order {
			if n != victim {
				peers = append(peers, n)
			}
		}
		stopChurn := wc.churnSubscription(i, peers)
		if graceful || style == "machinestop" {
			if graceful {
				wc.restartServer(victim)
			} else {
				// Machine stop: silence the victim's route writes first, so
				// the shutdown that follows cannot deliver clean FINs to
				// peers.
				wc.blackholeRoutes(victim)
				wc.restartServer(victim)
			}
		} else if style == "cascade" {
			// Two machine stops in the same zombie window: the second
			// victim is frozen while the peers still hold the first
			// victim's dead generation, so route registration churn
			// overlaps across generations.
			wc.blackholeRoutes(victim)
			wc.restartServer(victim)
			second := wc.order[(i+1)%wedgeNumServers]
			t.Logf("cycle %d: cascading restart of %s", i, second)
			wc.blackholeRoutes(second)
			// Unblock the first victim's leaked route sockets before the
			// second shutdown waits on them; its peers have long since
			// reaped or will reap via the FINs.
			wc.releaseBlackholes()
			wc.restartServer(second)
		} else {
			t.Fatalf("unknown WEDGE_RESTART_STYLE %q", style)
		}

		checkFor(t, 30*time.Second, 50*time.Millisecond, wc.meshFormed)
		wc.releaseBlackholes()
		stopChurn()
		if err := wc.mirrorIntact(); err != nil {
			wc.dumpRoutes(fmt.Sprintf("cycle %d mirror break", i))
			t.Fatalf("route pool mirror broken after restart of %s: %v", victim, err)
		}

		// Fresh unique interest for EVERY ordered pair: on unpatched code
		// the re-join leaves one or more ordered pairs unable to learn new
		// interest in either direction.
		var failures []string
		for _, pub := range wc.order {
			for _, sub := range wc.order {
				if pub == sub {
					continue
				}
				subject := fmt.Sprintf("_w.c%d.%s.to.%s.%d", i, pub, sub, time.Now().UnixNano())
				if err := wc.interestFlows(pub, sub, subject); err != nil {
					failures = append(failures, fmt.Sprintf("cycle %d (restarted %s): %v", i, victim, err))
				}
			}
		}
		if len(failures) > 0 {
			wc.dumpRoutes(fmt.Sprintf("cycle %d wedge", i))
			t.Fatalf("route subscription interest wedged after member restart:\n%s", strings.Join(failures, "\n"))
		}
	}
	t.Logf("no wedge in %d restart cycles", cycles)
}

// TestRoutePoolInterestDeadSlotInjection deterministically constructs the
// wedged steady state that the restart churn leaves behind on the live
// cluster (see the file header for how it maps to the production evidence)
// and verifies that new subscription interest for the pair freezes in both
// directions while pre-existing interest keeps delivering.
//
// Construction, per server X in the pair and the pair's global-account pool
// slot h:
//
//   - a registered-but-dead route connection Z is placed at X.routes[peer][h]:
//     kind ROUTER, remoteID = peer's live ID, nc = a never-read blackhole.
//     X keeps pinging nothing on it, so nothing ever reaps it, and every
//     RS+ incremental X sends for $G (updateRouteSubscriptionMap gathers
//     conns[h]) is accepted by enqueueProto and vanishes into Z - while
//     lws[key] is recorded as sent, so the incremental is never re-sent.
//     No addRoute ever fires for Z, so the connect-time full dump
//     (sendSubsToRoute) never re-flows the lost interest either.
//   - X's live connection that used to occupy h is moved to another slot
//     (the crossed registration the duplicate-resolution churn leaves
//     behind), so the pair keeps exactly the production shape: old interest
//     delivers over the live crossed connection, new interest dies at slot h.
//
// Production mapping: Z is the dead-generation connection the peers kept
// registered after the machine stop (their /routez showed a second, fully
// idle connection with subs=0 and in/out=0 - this blackholed registration);
// the frozen ~107 subscriptions on the traffic connection are the pre-wedge
// RS+ entries on the live (crossed) connection, which no new RS+ reach;
// in_msgs keeps growing because PING/PONG and old-subject RMSGs still flow
// over the live connections.
func TestRoutePoolInterestDeadSlotInjection(t *testing.T) {
	wc := newWedgeCluster(t)
	wc.order = []string{"A", "B", "C"}
	clusterPorts := map[string]string{}
	for i, n := range wc.order {
		clusterPorts[n] = fmt.Sprintf("%d", 22441+i)
	}
	for _, n := range wc.order {
		var peers []string
		for _, m := range wc.order {
			if m != n {
				peers = append(peers, clusterPorts[m])
			}
		}
		wc.confs[n] = wedgeClusterConf(t, n, clusterPorts[n], peers)
	}
	for _, n := range wc.order {
		s, _ := RunServerWithConfig(wc.confs[n])
		wc.servers[n] = s
		// The injected dead-slot client is intentionally not fully wired
		// for a clean shutdown (nothing in production reaps it either), so
		// don't let the deferred teardown block the test result.
		defer func(s *Server) { go s.Shutdown() }(s)
	}
	checkFor(t, 30*time.Second, 50*time.Millisecond, wc.meshFormed)

	const pubName, subName = "A", "B"

	// Persistent baseline subscribers on both sides, held open for the
	// whole test: pre-existing interest that must keep delivering after
	// the injection (production: old subjects flow on the frozen
	// connection, so its in_msgs keep growing).
	ncBaseSub := natsConnect(t, wc.servers[subName].ClientURL())
	defer ncBaseSub.Close()
	baseCh := make(chan *nats.Msg, 64)
	if _, err := ncBaseSub.ChanSubscribe("_inj.baseline", baseCh); err != nil {
		t.Fatalf("baseline subscribe on %s: %v", subName, err)
	}
	ncBasePub := natsConnect(t, wc.servers[pubName].ClientURL())
	defer ncBasePub.Close()
	natsFlush(t, ncBaseSub)
	checkFor(t, 5*time.Second, 20*time.Millisecond, func() error {
		acc, err := wc.servers[pubName].LookupAccount(globalAccountName)
		if err != nil {
			return err
		}
		if acc.SubscriptionInterest("_inj.baseline") {
			return nil
		}
		return fmt.Errorf("%s has no baseline interest yet", pubName)
	})

	// Inject the dead slot at both ends of the pair, on the global
	// account's pool slot.
	h := wc.globalPoolIdx(subName)
	t.Logf("global account pool slot for the pair: %d", h)
	injectDeadSlot(t, wc.servers[pubName], wc.servers[subName], h, wc)
	injectDeadSlot(t, wc.servers[subName], wc.servers[pubName], h, wc)
	wc.dumpRoutes("after dead-slot injection")

	// Production signature: pre-existing interest keeps delivering.
	natsPub(t, ncBasePub, "_inj.baseline", []byte("old"))
	select {
	case m := <-baseCh:
		if string(m.Data) != "old" {
			t.Fatalf("unexpected baseline payload %q", m.Data)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("pre-existing baseline delivery broke immediately after injection")
	}

	// Brand-new interest must freeze in both directions.
	var failures []string
	for _, dir := range [][2]string{{pubName, subName}, {subName, pubName}} {
		subject := fmt.Sprintf("_inj.fresh.%s.to.%s.%d", dir[0], dir[1], time.Now().UnixNano())
		if err := wc.interestProbe(dir[0], dir[1], subject, 3*time.Second); err != nil {
			failures = append(failures, err.Error())
		}
	}
	wedged := len(failures) > 0
	if wedged {
		wc.dumpRoutes("fresh interest after injection")
		for _, f := range failures {
			t.Errorf("fresh interest after injection: %v", f)
		}
	}

	// Persistence: tear the pair's live connections down and let the
	// reconnect machinery re-run against the injected state, like the next
	// restart in production. The pair reforms around the dead slot (a
	// partial pool, like production's idle second connection) and the
	// wedge persists: the connect-time full dump routes accounts to their
	// pool slot only, so the dead slot also swallows the re-sync.
	wc.closeRoutesBetween(pubName, subName)
	checkFor(t, 30*time.Second, 50*time.Millisecond, func() error {
		if err := wc.meshFormed(); err != nil {
			return err
		}
		return wc.routesBetweenAtLeast(pubName, subName, 2)
	})
	wc.dumpRoutes("after forced re-join")
	for _, dir := range [][2]string{{pubName, subName}, {subName, pubName}} {
		subject := fmt.Sprintf("_inj.postjoin.%s.to.%s.%d", dir[0], dir[1], time.Now().UnixNano())
		if err := wc.interestProbe(dir[0], dir[1], subject, 3*time.Second); err != nil {
			t.Errorf("wedge persisted across re-join, direction %s->%s: %v", dir[0], dir[1], err)
			wedged = true
		}
	}
	if wedged {
		t.Fatalf("dead slot %d wedged new interest for the pair (production wedge reproduced; see failures above)", h)
	}
}

// globalPoolIdx returns the account's route pool slot on the given server.
func (wc *wedgeCluster) globalPoolIdx(name string) int {
	acc, err := wc.servers[name].LookupAccount(globalAccountName)
	if err != nil {
		wc.t.Fatalf("lookup global account on %s: %v", name, err)
	}
	acc.mu.RLock()
	idx := acc.routePoolIdx
	acc.mu.RUnlock()
	return idx
}

// injectDeadSlot places a registered-but-dead route connection at x's pool
// slot `slot` for remote y: every RS+ x sends for an account mapping to that
// slot is accepted into its outbound buffer and vanishes (the nc is a
// never-read blackhole), nothing ever pings or reaps it, and no connect dump
// re-flows the lost interest. x's live connection previously at that slot is
// moved to a free slot - the crossed registration left behind by the churn.
func injectDeadSlot(t *testing.T, x, y *Server, slot int, wc *wedgeCluster) {
	// The dead registration models the un-reaped dead generation production
	// peers keep after a machine stop: it is old (so the slot-trust checks
	// silence horizon flags it), its writes vanish into a blackhole, and it
	// carries the displaced connection's dial identity so that tearing it
	// down re-runs the standard reconnect-and-dump refill, exactly like a
	// reaped solicited route connection.
	// Old enough to exceed the slot-trust silence horizon for this test's
	// ping config (ping_interval 500ms, ping_max 2 -> horizon 1.5s), so the
	// dead registration is detected at the next interest gather.
	age := 7 * time.Second
	dead := &client{srv: x, nc: wc.blackholeConn(), opts: ClientOpts{}, kind: ROUTER, msubs: -1, mpay: -1, start: time.Now().Add(-age)}
	dead.mu.Lock()
	dead.initClient()
	dead.route = &route{remoteID: y.ID(), poolIdx: slot, didSolicit: true, routeType: Explicit}
	dead.mu.Unlock()

	x.mu.Lock()
	conns, ok := x.routes[y.ID()]
	if !ok {
		x.mu.Unlock()
		t.Fatalf("%s has no route slice for %s", x, y)
	}
	free := -1
	for i, c := range conns {
		if c == nil {
			free = i
			break
		}
	}
	live := conns[slot]
	conns[slot] = dead
	if live != nil && free >= 0 {
		conns[free] = live
	}
	x.mu.Unlock()
	// Copy the dial identity from the displaced live connection and evict
	// it, so the slot holds exactly one registration (the dead one). Do
	// not hold the server lock while taking client locks: closeConnection
	// takes client lock -> server lock, so the inverse ordering here would
	// deadlock.
	if live != nil {
		live.mu.Lock()
		dead.mu.Lock()
		dead.route.url = live.route.url
		if dead.route.url == nil {
			// Fall back to any configured route URL pointing at the peer.
			for _, u := range x.getOpts().Routes {
				dead.route.url = u
			}
		}
		dead.route.routeType = live.route.routeType
		dead.route.didSolicit = live.route.didSolicit || dead.route.didSolicit
		dead.lastIn = time.Now().Add(-age)
		live.mu.Unlock()
		dead.mu.Unlock()
		if free >= 0 {
			live.mu.Lock()
			live.route.poolIdx = free
			live.mu.Unlock()
		} else {
			// No free slot: drop the live registration so the slice holds
			// the dead connection at `slot` only. Its connection object
			// stays alive (mirroring the crossed leftover connection in
			// production); closeRoutesBetween skips injected entries, and
			// the test's servers are torn down at the end.
		}
	}
	wc.injected[dead] = true
	t.Logf("injected dead route conn at slot %d on %s for remote %s (live conn moved to slot %d)",
		slot, x, y, free)
}

// closeRoutesBetween closes the live route connections between the two
// servers so the reconnect machinery re-runs against the injected state.
func (wc *wedgeCluster) closeRoutesBetween(a, b string) {
	closeTo := func(x, y *Server) {
		x.mu.RLock()
		conns := x.routes[y.ID()]
		var live []*client
		for _, c := range conns {
			if c != nil && !wc.injected[c] {
				live = append(live, c)
			}
		}
		x.mu.RUnlock()
		for _, c := range live {
			c.mu.Lock()
			if c.nc != nil {
				c.nc.Close()
			}
			c.mu.Unlock()
		}
	}
	closeTo(wc.servers[a], wc.servers[b])
	closeTo(wc.servers[b], wc.servers[a])
}

func (wc *wedgeCluster) routesBetweenAtLeast(a, b string, n int) error {
	count := func(x, y *Server) int {
		x.mu.RLock()
		defer x.mu.RUnlock()
		cnt := 0
		for _, c := range x.routes[y.ID()] {
			if c != nil {
				cnt++
			}
		}
		return cnt
	}
	if c := count(wc.servers[a], wc.servers[b]); c < n {
		return fmt.Errorf("%s has %d pool routes to %s, want >= %d", a, c, b, n)
	}
	if c := count(wc.servers[b], wc.servers[a]); c < n {
		return fmt.Errorf("%s has %d pool routes to %s, want >= %d", b, c, a, n)
	}
	return nil
}

func newWedgeCluster(t *testing.T) *wedgeCluster {
	return &wedgeCluster{t: t, confs: map[string]string{}, servers: map[string]*Server{}, injected: map[*client]bool{}}
}

// TestRoutePoolInterestEmptySlotRefill reproduces the live wedge observed on
// the patched fleet: the pair's pool is only partially filled - the slot
// that the global account hashes to stayed EMPTY at both ends after the
// restart - while the two existing connections are live (one carries all
// routed traffic, the other is protocol-idle; routez message counters do not
// show control protos, so both look plausible). The pool refill chain
// (route.startNewRoute, consumed in processPong) stops permanently when the
// dial it triggers is rejected as a duplicate/pool-full
// (handleDuplicateRoute(nil, c, true) sets noReconnect, so closeConnection
// does not retry), so nothing ever fills the slot again.
//
// In that state updateRouteSubscriptionMap's gather finds
// conns[routePoolIdx] == nil, sends nothing and involves no connection -
// which is why the dead-connection repair does not fire and the connection
// identities stay stable. Old interest keeps delivering (it lives in the
// account sublist with live client connections); only fresh interest for
// accounts mapped to the empty slot freezes, in both directions.
func TestRoutePoolInterestEmptySlotRefill(t *testing.T) {
	wc := newWedgeCluster(t)
	wc.order = []string{"A", "B", "C"}
	clusterPorts := map[string]string{}
	for i, n := range wc.order {
		clusterPorts[n] = fmt.Sprintf("%d", 22551+i)
	}
	for _, n := range wc.order {
		var peers []string
		for _, m := range wc.order {
			if m != n {
				peers = append(peers, clusterPorts[m])
			}
		}
		wc.confs[n] = wedgeClusterConf(t, n, clusterPorts[n], peers)
	}
	for _, n := range wc.order {
		s, _ := RunServerWithConfig(wc.confs[n])
		wc.servers[n] = s
		defer func(s *Server) { go s.Shutdown() }(s)
	}
	checkFor(t, 30*time.Second, 50*time.Millisecond, wc.meshFormed)

	const pubName, subName = "A", "B"

	// Pre-wedge baseline interest on the pair; must keep flowing.
	ncBaseSub := natsConnect(t, wc.servers[subName].ClientURL())
	defer ncBaseSub.Close()
	baseCh := make(chan *nats.Msg, 64)
	if _, err := ncBaseSub.ChanSubscribe("_es.baseline", baseCh); err != nil {
		t.Fatalf("baseline subscribe on %s: %v", subName, err)
	}
	ncBasePub := natsConnect(t, wc.servers[pubName].ClientURL())
	defer ncBasePub.Close()
	natsFlush(t, ncBaseSub)
	checkFor(t, 5*time.Second, 20*time.Millisecond, func() error {
		acc, err := wc.servers[pubName].LookupAccount(globalAccountName)
		if err != nil {
			return err
		}
		if acc.SubscriptionInterest("_es.baseline") {
			return nil
		}
		return fmt.Errorf("%s has no baseline interest yet", pubName)
	})

	// Surgically empty the global account's pool slot for the pair at both
	// ends, keeping the connection objects (and their sockets and read
	// loops) alive - exactly the production state, where the missing slot
	// and the protocol-idle sibling persist indefinitely.
	h := wc.globalPoolIdx(subName)
	var stashed []*client
	empty := func(x, y *Server) {
		x.mu.Lock()
		conns, ok := x.routes[y.ID()]
		if !ok || h >= len(conns) {
			x.mu.Unlock()
			t.Fatalf("%s has no pool slice/slot for %s", x, y)
		}
		if conns[h] == nil {
			x.mu.Unlock()
			t.Fatalf("slot %d for %s -> %s already empty", h, x, y)
		}
		stashed = append(stashed, conns[h])
		conns[h] = nil
		x.mu.Unlock()
	}
	empty(wc.servers[pubName], wc.servers[subName])
	empty(wc.servers[subName], wc.servers[pubName])
	wc.stashed = append(wc.stashed, stashed...)
	t.Logf("emptied pool slot %d for the pair at both ends (connections left alive)", h)

	// Pre-existing interest keeps delivering.
	natsPub(t, ncBasePub, "_es.baseline", []byte("old"))
	select {
	case m := <-baseCh:
		if string(m.Data) != "old" {
			t.Fatalf("unexpected baseline payload %q", m.Data)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("pre-existing baseline delivery broke after emptying the slot")
	}

	// Fresh interest in BOTH directions must propagate. On unpatched code
	// (and with only the dead-connection repair) the gather finds an empty
	// slot, sends nothing, and nothing ever refills it: both probes wedge.
	var failures []string
	for _, dir := range [][2]string{{pubName, subName}, {subName, pubName}} {
		subject := fmt.Sprintf("_es.fresh.%s.to.%s.%d", dir[0], dir[1], time.Now().UnixNano())
		if err := wc.interestProbe(dir[0], dir[1], subject, 8*time.Second); err != nil {
			failures = append(failures, err.Error())
		}
	}
	if len(failures) > 0 {
		wc.dumpRoutes("fresh interest with empty pool slot")
		t.Fatalf("empty pool slot %d wedged fresh interest for the pair:\n%s", h, strings.Join(failures, "\n"))
	}

	// The refill must not overfill: each pair ends with exactly the pool
	// plus the $SYS route, and stays there quietly.
	checkFor(t, 10*time.Second, 50*time.Millisecond, func() error {
		for _, n := range wc.order {
			s := wc.servers[n]
			perRemote := make(map[string]int)
			s.mu.RLock()
			s.forEachRoute(func(r *client) {
				r.mu.Lock()
				perRemote[r.route.remoteID]++
				r.mu.Unlock()
			})
			s.mu.RUnlock()
			for _, other := range wc.order {
				if other == n {
					continue
				}
				if c := perRemote[wc.servers[other].ID()]; c != 4 {
					return fmt.Errorf("%s has %d routes to %s, want 4 (3 pool + $SYS)", n, c, other)
				}
			}
		}
		return nil
	})
}
