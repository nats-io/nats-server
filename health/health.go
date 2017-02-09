package health

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/go-nats"
)

// sysMemberPrefix creates a namespace
// for system cluster membership communication.
// This prefix aims to avoid collisions
// with user-level topics. Only system
// processes / internal clients should
// write to these topics, but everyone
// is welcome to listen on them.
//
// note: `_nats` is for now, can easily
// changed to be `_SYS` later once
// we're sure everything is working.
//
const sysMemberPrefix = "_nats.cluster.members."

// Membership tracks the nats server cluster
// membership, issuing health checks and
// choosing a leader.
type Membership struct {
	Cfg MembershipCfg

	// the pongCollector holds
	// all the pongs received in
	// response to allcall pings
	// in the most recent heartbeat
	// session.
	pc *pongCollector

	// actually elected leaders, should
	// change only after a lease term.
	elec  *leadHolder
	nc    *nats.Conn
	myLoc AgentLoc
	pid   int

	subjAllCall     string
	subjAllReply    string
	subjMemberLost  string
	subjMemberAdded string
	subjMembership  string

	halt     *halter
	mu       sync.Mutex
	stopping bool

	needReconnect chan bool
}

func (m *Membership) trace(f string, arg ...interface{}) {
	m.Cfg.Log.Tracef(fmt.Sprintf("my.Port:%v. ", m.myLoc.Port)+f, arg...)
}

func (m *Membership) dlog(f string, arg ...interface{}) {
	m.Cfg.Log.Debugf(fmt.Sprintf("my.Port:%v. ", m.myLoc.Port)+f, arg...)
}

func (m *Membership) getMyLocWithAnyLease() AgentLoc {
	m.mu.Lock()
	myLoc := m.myLoc
	m.mu.Unlock()

	lead := m.elec.getLeader()
	if slocEqualIgnoreLease(&lead, &myLoc) {
		myLoc.LeaseExpires = lead.LeaseExpires
		myLoc.IsLeader = true
	}
	return myLoc
}

func (m *Membership) getMyLocWithZeroLease() AgentLoc {
	m.mu.Lock()
	myLoc := m.myLoc
	m.mu.Unlock()
	myLoc.LeaseExpires = time.Time{}
	myLoc.IsLeader = false
	return myLoc
}

// deaf means we don't ping or pong.
// It is used to simulate network
// partition and healing.
//
func (m *Membership) deaf() bool {
	v := atomic.LoadInt64(&m.Cfg.deaf)
	return v == deafTrue
}

func (m *Membership) setDeaf() {
	atomic.StoreInt64(&m.Cfg.deaf, deafTrue)
}

func (m *Membership) unDeaf() {
	atomic.StoreInt64(&m.Cfg.deaf, deafFalse)
}

// NewMembership creates a new Membership.
func NewMembership(cfg *MembershipCfg) *Membership {
	m := &Membership{
		Cfg:  *cfg,
		halt: newHalter(),
		pid:  os.Getpid(),
		// needReconnect should be sent on, not closed.
		needReconnect: make(chan bool),
	}
	m.pc = m.newPongCollector()
	m.elec = m.newLeadHolder(cfg.historyCount)
	return m
}

// leadHolder holds who is the current leader,
// and what their lease is. Used to synchronize
// access between various goroutines.
type leadHolder struct {
	mu   sync.Mutex
	sloc AgentLoc

	myID            string
	myRank          int
	myLocHasBeenSet bool

	history *ringBuf
	histsz  int

	m *Membership
}

func (m *Membership) newLeadHolder(histsz int) *leadHolder {
	if histsz == 0 {
		histsz = 100
	}
	return &leadHolder{
		history: newRingBuf(histsz),
		histsz:  histsz,
		m:       m,
	}
}

func (e *leadHolder) setMyLoc(myLoc *AgentLoc) {
	e.mu.Lock()
	if e.myLocHasBeenSet {
		panic("no double set!")
	}
	e.myLocHasBeenSet = true
	e.myID = myLoc.ID
	e.myRank = myLoc.Rank
	e.mu.Unlock()
}

// getLeader retreives the stored e.sloc value.
func (e *leadHolder) getLeader() AgentLoc {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.sloc
}

// setLeader aims to copy sloc and store it
// for future getLeader() calls to access.
//
// However we reject any attempt to replace
// a leader with a one that doesn't rank lower, where rank
// includes the LeaseExpires time
// (see the AgentLocLessThan() function).
//
// If we accept sloc
// we return slocWon true. If we reject sloc then
// we return slocWon false. In short, we will only
// accept sloc if AgentLocLessThan(sloc, e.sloc),
// and we return AgentLocLessThan(sloc, e.sloc).
//
// If we return slocWon false, alt contains the
// value we favored, which is the current value
// of our retained e.sloc. If we return true,
// then alt contains a copy of sloc. We
// return a value in alt to avoid data races.
//
func (e *leadHolder) setLeader(sloc AgentLoc, now time.Time) (slocWon bool, alt AgentLoc) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if sloc.ID == "" {
		e.m.trace(`setLeader returning false because sloc==nil or sloc.ID==""`)
		return false, e.sloc
	}

	// check on expired leases: any new leader must
	// have a lease that is not expired.
	nowu := now.UnixNano()
	cure := e.sloc.LeaseExpires.UnixNano()
	newe := sloc.LeaseExpires.UnixNano()

	curExpired := cure <= nowu
	newExpired := newe <= nowu
	bothExpired := curExpired && newExpired
	neitherExpired := !curExpired && !newExpired

	var newWon, oldWon bool

	switch {
	case bothExpired:
		e.m.trace("22222 setLeader finds both expired")
		return false, e.sloc

	case neitherExpired:
		newWon = AgentLocLessThan(&sloc, &e.sloc)
		oldWon = AgentLocLessThan(&e.sloc, &sloc)

	case newExpired:
		e.m.trace("55555 setLeader is returning false because new has expired lease.")
		return false, e.sloc

	case curExpired:
		newWon = true
		oldWon = false
		e.m.trace("44444 setLeader finds old lease expired")
	}

	switch {
	case !newWon && !oldWon:
		// they are equal, pick the longer lease
		// so we allow lease renewal
		if sloc.LeaseExpires.After(e.sloc.LeaseExpires) {
			slocWon = true
			alt = sloc
			e.sloc = sloc

			e.m.trace("999999 setLeader: same leader, > lease, renewing lease for '%s'", &e.sloc)
		} else {
			slocWon = false
			alt = e.sloc

			e.m.trace("000000 setLeader is failing to update the leader, rejecting the new contendor. sloc='%s' >= prev:'%s'", &sloc, &e.sloc)
		}
	case newWon:
		slocWon = true
		alt = sloc
		e.sloc = sloc

		e.m.trace("11111 setLeader updated the leader, accepting new proposal. sloc='%s' < prev:'%s'", &sloc, &e.sloc)

	default:
		//oldWon
		slocWon = false
		alt = e.sloc
	}

	// update history
	if slocWon {
		histcp := sloc
		e.history.Append(&histcp)
	}

	return
}

func (e *leadHolder) copyLeadHistory() *ringBuf {
	e.mu.Lock()
	r := e.history.clone()
	e.mu.Unlock()
	return r
}

func (e *leadHolder) getLeaderAsBytes() []byte {
	lead := e.getLeader()
	by, err := json.Marshal(&lead)
	panicOn(err)
	return by
}

// Stop blocks until the Membership goroutine
// acknowledges the shutdown request.
func (m *Membership) Stop() {
	m.mu.Lock()
	if m.stopping {
		m.mu.Unlock()
		return
	}
	m.stopping = true
	m.mu.Unlock()
	m.halt.ReqStop.Close()
	<-m.halt.Done.Chan
}

// Start launches the Membership goroutine.
func (m *Membership) Start() error {

	m.Cfg.SetDefaults()

	err := m.setupNatsClient()
	if err != nil {
		m.halt.Done.Close()
		return err
	}
	go m.start()
	return nil
}

func (m *Membership) start() {

	nc := m.nc
	pc := m.pc

	defer func() {
		m.halt.Done.Close()
	}()

	m.Cfg.Log.Debugf("health-agent: Listening on [%s]\n", m.subjAllCall)

	prevCount, curCount := 0, 0
	prevMember := newMembers()
	var curMember *members
	var curLead AgentLoc

	var err error
	var now time.Time
	var expired bool
	var prevLead AgentLoc
	var nextLeadReportTm time.Time

	k := -1
	for {
		k++
		// NB: replies to an
		// allcall will only update
		// the pongCollectors.from set,
		// and won't change
		// what the current leader
		// is in elec.
		m.trace("issuing k-th (k=%v) allcall", k)
		err = m.allcall()
		if err != nil {
			// err could be: "write on closed buffer"
			// typically means we are shutting down.

			m.trace("health-agent: "+
				"error on allcall, "+
				"shutting down the "+
				"health-agent: %s",
				err)
			return
		}

		m.trace("SLEEPING for a heartbeat of %v", m.Cfg.BeatDur)
		select {
		case <-time.After(m.Cfg.BeatDur):
			// continue below, latest heartbeat session done.
		case <-m.needReconnect:
			err := m.setupNatsClient()
			if err != nil {
				m.Cfg.Log.Debugf("health-agent: "+
					"fatal error: could not reconnect to, "+
					"our url '%s', error: %s",
					m.Cfg.NatsURL, err)

				m.halt.ReqStop.Close()
				return
			}
		case <-m.halt.ReqStop.Chan:
			return
		}
		lastSeenLead := m.elec.getLeader()

		// cur responses should be back by now
		// and we can compare prev and cur.
		curCount, curMember = pc.getSetAndClear()
		now = time.Now().UTC()
		m.trace("k-th (k=%v) before doing leaderLeaseCheck, curMember='%s'", k, curMember)

		expired, curLead = curMember.leaderLeaseCheck(
			now,
			m.Cfg.LeaseTime,
			lastSeenLead,
			m.Cfg.MaxClockSkew,
			m,
		)

		if expired {
			// record in our history
			won, alt := m.elec.setLeader(curLead, now)
			if !won {
				m.trace("k-th (k=%v) round "+
					"conclusion of trying to "+
					"setLeader: rejected '%s'"+
					" in favor of '%s'",
					k, curLead, &alt)
				curLead = alt
			} else {
				m.trace("k-th (k=%v) round "+
					"conclusion of trying to "+
					"setLeader: accepted as "+
					"new lead '%s'",
					k, curLead)
			}
		}

		// logging
		loc, _ := m.getNatsServerLocation()
		if loc != nil {
			if loc.ID == curLead.ID &&
				curLead.Pid == m.pid {

				if now.After(nextLeadReportTm) ||
					prevLead.ID == "" ||
					prevLead.ID != curLead.ID {

					left := curLead.LeaseExpires.Sub(now)
					m.dlog("health-agent: "+
						"I am LEAD, my ID: '%s', "+
						"rank %v port %v host %v "+
						"pid %v. lease expires "+
						"in %s",
						loc.ID,
						loc.Rank,
						loc.Port,
						loc.Host,
						m.pid,
						left)

					nextLeadReportTm = now.Add(left).Add(m.Cfg.MaxClockSkew)
				}
			} else {
				if prevLead.ID == loc.ID &&
					prevLead.Pid == m.pid {

					m.dlog("health-agent: "+
						"I am no longer lead, "+
						"new LEAD is '%s', rank %v. "+
						"port %v host %v pid %v lease expires in %s",
						curLead.ID,
						curLead.Rank,
						curLead.Port,
						curLead.Host,
						curLead.Pid,
						curLead.LeaseExpires.Sub(now))

				} else {
					if nextLeadReportTm.IsZero() ||
						now.After(nextLeadReportTm) {

						left := curLead.LeaseExpires.Sub(now)
						if curLead.ID == "" {
							m.dlog("health-agent: "+
								"I am '%s'/rank=%v. "+
								"port %v. lead is unknown.",
								m.myLoc.ID,
								m.myLoc.Rank,
								m.myLoc.Port)

						} else {
							m.dlog("health-agent: "+
								"I am not lead. lead is '%s', "+
								"rank %v host %v port %v pid %v for %v",
								curLead.ID,
								curLead.Rank,
								curLead.Host,
								curLead.Port,
								curLead.Pid,
								left)

						}
						nextLeadReportTm = now.Add(left).Add(m.Cfg.MaxClockSkew)
					}
				}
			}
		}

		lost := setDiff(prevMember, curMember)
		gained := setDiff(curMember, prevMember)
		same := setsEqual(prevMember, curMember)

		if same {
			// nothing more to do.
			// This is the common case when nothing changes.
		} else {
			lostBytes := lost.mustJSONBytes()
			if !lost.setEmpty() {
				if !m.deaf() {
					nc.Publish(m.subjMemberLost, lostBytes)
					// ignore errors on purpose;
					// don't crash mid-health-report
					// if at all possible.
				}
			}
			gainedBytes := gained.mustJSONBytes()
			if !gained.setEmpty() {
				if !m.deaf() {
					nc.Publish(m.subjMemberAdded, gainedBytes)
					// same error approach as above.
				}
			}
		}
		if curCount < prevCount {
			m.dlog("health-agent: ---- "+
				"PAGE PAGE PAGE!! we went "+
				"down a server, from %v -> %v. "+
				"lost: '%s'",
				prevCount,
				curCount,
				lost)

		} else if curCount > prevCount && prevCount > 0 {
			m.dlog("health-agent: ++++  "+
				"MORE ROBUSTNESS GAINED; "+
				"we went from %v -> %v. "+
				"gained: '%s'",
				prevCount,
				curCount,
				gained)

		}

		if expired {
			curBytes := curMember.mustJSONBytes()
			if !m.deaf() {
				nc.Publish(m.subjMembership, curBytes)
			}
		}

		// done with compare, now loop
		prevCount = curCount
		prevMember = curMember
		prevLead = curLead
	}
}

func pong(nc *nats.Conn, subj string, msg []byte) {
	nc.Publish(subj, msg)
	nc.Flush()
	// ignore errors, probably shutting down.
	// e.g. "nats: connection closed on shutdown."
	// or "nats: connection closed"
}

// allcall sends out a health ping on the
// subjAllCall topic.
//
// The ping consists of sending the AgentLoc
// for the current leader, which provides lease
// and full contact info for the leader.
//
// This gives a round-trip connectivity check.
//
func (m *Membership) allcall() error {
	lead := m.elec.getLeader()
	m.trace("ISSUING ALLCALL on '%s' with leader '%s'\n", m.subjAllCall, &lead)

	leadby, err := json.Marshal(&lead)
	panicOn(err)

	// allcall broadcasts the current leader + lease
	return m.nc.PublishRequest(m.subjAllCall, m.subjAllReply, leadby)
}

// pongCollector collects the responses
// from an allcall request.
type pongCollector struct {
	replies int

	fromNoTime *members

	mu    sync.Mutex
	mship *Membership
}

func (m *Membership) newPongCollector() *pongCollector {
	return &pongCollector{
		fromNoTime: newMembers(),
		mship:      m,
	}
}

func (pc *pongCollector) insert(sloc AgentLoc) {
	// insert into both our trees, one
	// keeping the lease time, the other not.
	cp := sloc
	cp.LeaseExpires = time.Time{}
	cp.IsLeader = false
	pc.fromNoTime.insert(cp)
}

// acumulate pong responses
func (pc *pongCollector) receivePong(msg *nats.Msg) {
	pc.mu.Lock()

	pc.replies++

	var loc AgentLoc
	err := loc.fromBytes(msg.Data)
	if err == nil {
		pc.insert(loc)
	} else {
		panic(err)
	}
	pc.mship.trace("PONG COLLECTOR RECEIVED ALLCALL REPLY '%s'", &loc)

	pc.mu.Unlock()
}

func (pc *pongCollector) clear() {
	pc.mu.Lock()
	pc.fromNoTime.clear()
	pc.mu.Unlock()
}

// getSet returns the count and set so far, then
// clears the set, emptying it, and then adding
// back just myLoc
func (pc *pongCollector) getSetAndClear() (int, *members) {

	mem := pc.fromNoTime.clone()
	pc.clear()

	// we don't necessarily need to seed,
	// since we'll hear our own allcall.
	// But we may want to seed in case
	// the connection to gnatsd is down
	// This happens under test when we
	// are deaf. Hence we seed so we
	// can elect ourselves when the
	// connection to gnatsd is not available.
	//
	// add myLoc to pc.from as a part of the reset:
	myLoc := pc.mship.getMyLocWithZeroLease()
	pc.insert(myLoc)

	pc.mship.trace("in getSetAndClear, here are the contents of mem.DedupTree: '%s'", mem.DedupTree)

	// return the old member set
	return mem.DedupTree.Len(), mem
}

// leaderLeaseCheck evaluates the lease as of now,
// and returns the leader or best candiate. Returns
// expired == true if any prior leader lease has
// lapsed. In this case we return the best new
// leader with its IsLeader bit set and its
// LeaseExpires set to now + lease.
//
// If expired == false then the we return
// the current leader in lead.
//
// PRE: there are only 0 or 1 leaders in m.DedupTree
//      who have a non-zero LeaseExpires field.
//
// If m.DedupTree is empty, we return (true, nil).
//
// This method is where the actual "election"
// happens. See the AgentLocLessThan()
// function below for exactly how
// we rank candidates.
//
func (mems *members) leaderLeaseCheck(
	now time.Time,
	leaseLen time.Duration,
	prevLead AgentLoc,
	maxClockSkew time.Duration,
	m *Membership,

) (expired bool, lead AgentLoc) {

	if prevLead.LeaseExpires.Add(maxClockSkew).After(now) {
		// honor the leases until they expire
		m.trace("leaderLeaseCheck: honoring outstanding lease")
		return false, prevLead
	}

	if mems.DedupTree.Len() == 0 {
		m.trace("leaderLeaseCheck: m.DedupTree.Len is 0")
		return false, prevLead
	}

	// INVAR: any lease has expired.
	expired = true
	lead = mems.DedupTree.minrank()
	lead.IsLeader = true
	lead.LeaseExpires = now.Add(leaseLen).UTC()

	return
}

type byRankThenID struct {
	s   []*AgentLoc
	now time.Time
}

func (p byRankThenID) Len() int      { return len(p.s) }
func (p byRankThenID) Swap(i, j int) { p.s[i], p.s[j] = p.s[j], p.s[i] }

// Less must be stable and computable locally yet
// applicable globally: it is how we choose a leader
// in a stable fashion.
func (p byRankThenID) Less(i, j int) bool {
	return AgentLocLessThan(p.s[i], p.s[j])
}

// AgentLocLessThan returns true iff i < j, in
// terms of leader preference where lowest is
// more electable/preferred as leader.
func AgentLocLessThan(i, j *AgentLoc) bool {

	// recognize empty AgentLoc and sort them high, not low.
	iempt := i.ID == ""
	jempt := j.ID == ""
	if iempt && jempt {
		return false // "" == ""
	}
	if jempt {
		return true // "123" < ""
	}
	if iempt {
		return false // "" > "123"
	}

	if i.Rank != j.Rank {
		return i.Rank < j.Rank
	}
	if i.ID != j.ID {
		return lessThanString(i.ID, j.ID)
	}
	if i.Host != j.Host {
		return lessThanString(i.Host, j.Host)
	}
	if i.Port != j.Port {
		return i.Port < j.Port
	}
	if i.Pid != j.Pid {
		return i.Pid < j.Pid
	}
	itm := i.LeaseExpires.UnixNano()
	jtm := j.LeaseExpires.UnixNano()
	return itm > jtm // want the later expiration to have priority
}

// return i < j where empty strings are big not small.
func lessThanString(i, j string) bool {
	iempt := i == ""
	jempt := j == ""
	if iempt || jempt {
		if jempt {
			return true // "123" < ""
		}
		return false
	}
	return i < j
}

func (m *Membership) setupNatsClient() error {
	pc := m.pc

	discon := func(nc *nats.Conn) {
		select {
		case m.needReconnect <- true:
		case <-m.halt.ReqStop.Chan:
			return
		}
	}
	optdis := nats.DisconnectHandler(discon)
	norand := nats.DontRandomize()

	// We don't want to get connected to
	// some different server in the pool,
	// so any reconnect, if needed, will
	// need to be handled manually by us by
	// attempting to contact the
	// exact same address as we are
	// configured with; see the m.needReconnect
	// channel.
	// Otherwise we are monitoring
	// the health of the wrong server.
	//
	optrecon := nats.NoReconnect()

	opts := []nats.Option{optdis, optrecon, norand}
	if m.Cfg.CliConn != nil {
		opts = append(opts, nats.Dialer(&m.Cfg))
	}

	nc, err := nats.Connect(m.Cfg.NatsURL, opts...)
	if err != nil {
		msg := fmt.Errorf("Can't connect to "+
			"nats on url '%s': %v",
			m.Cfg.NatsURL,
			err)
		m.Cfg.Log.Errorf(msg.Error())
		return msg
	}
	m.nc = nc
	loc, err := m.getNatsServerLocation()
	if err != nil {
		return err
	}
	m.setLoc(loc)
	m.Cfg.Log.Debugf("health-agent: HELLOWORLD: "+
		"I am '%s' at '%v:%v'. "+
		"rank %v",
		m.myLoc.ID,
		m.myLoc.Host,
		m.myLoc.Port,
		m.myLoc.Rank)

	m.subjAllCall = sysMemberPrefix + "allcall"
	m.subjAllReply = sysMemberPrefix + "allreply"
	m.subjMemberLost = sysMemberPrefix + "lost"
	m.subjMemberAdded = sysMemberPrefix + "added"
	m.subjMembership = sysMemberPrefix + "list"

	nc.Subscribe(m.subjAllReply, func(msg *nats.Msg) {
		if m.deaf() {
			return
		}
		pc.receivePong(msg)
	})

	// allcall says: "who is out there? Are you a lead?"
	nc.Subscribe(m.subjAllCall, func(msg *nats.Msg) {
		m.trace("ALLCALL RECEIVED. msg:'%s'", string(msg.Data))
		if m.deaf() {
			return
		}

		// sanity check that we haven't moved.
		loc, err := m.getNatsServerLocation()
		if err != nil {
			return // try again next time.
		}

		// did we accidentally change
		// server locacations?
		// Yikes, we don't want to do that!
		// We are supposed to be monitoring
		// just our own server.
		if m.locDifferent(loc) {
			panic(fmt.Sprintf("\n very bad! health-agent "+
				"changed locations! "+
				"first: '%s',\n\nvs\n now:'%s'\n",
				&m.myLoc,
				loc))
		}
		// Done with sanity check.
		// INVAR: we haven't moved, and
		// loc matches m.myLoc.

		locWithLease := m.getMyLocWithAnyLease()

		hp, err := json.Marshal(&locWithLease)
		panicOn(err)
		if !m.deaf() {
			m.trace("REPLYING TO ALLCALL on '%s' with my details: '%s'", msg.Reply, &locWithLease)
			pong(nc, msg.Reply, hp)
		}
	})

	// reporting
	nc.Subscribe(m.subjMemberLost, func(msg *nats.Msg) {
		if m.deaf() {
			return
		}
		m.Cfg.Log.Tracef("health-agent: "+
			"Received on [%s]: '%s'",
			msg.Subject,
			string(msg.Data))
	})

	// reporting
	nc.Subscribe(m.subjMemberAdded, func(msg *nats.Msg) {
		if m.deaf() {
			return
		}
		m.Cfg.Log.Tracef("health-agent: Received on [%s]: '%s'",
			msg.Subject, string(msg.Data))
	})

	// reporting
	nc.Subscribe(m.subjMembership, func(msg *nats.Msg) {
		if m.deaf() {
			return
		}
		m.Cfg.Log.Tracef("health-agent: "+
			"Received on [%s]: '%s'",
			msg.Subject,
			string(msg.Data))
	})

	return nil
}

func (m *Membership) locDifferent(b *nats.ServerLoc) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if b.ID != m.myLoc.ID {
		return true
	}
	if b.Rank != m.myLoc.Rank {
		return true
	}
	if b.Host != m.myLoc.Host {
		return true
	}
	if b.Port != m.myLoc.Port {
		return true
	}
	return false
}

func (m *Membership) setLoc(b *nats.ServerLoc) {
	m.mu.Lock()
	m.myLoc.ID = b.ID
	m.myLoc.Rank = b.Rank
	m.myLoc.Host = b.Host
	m.myLoc.Port = b.Port
	m.myLoc.Pid = os.Getpid()
	m.mu.Unlock()
	m.elec.setMyLoc(&m.myLoc)
}

func (m *Membership) getNatsServerLocation() (*nats.ServerLoc, error) {
	loc, err := m.nc.ServerLocation()
	if err != nil {
		return nil, err
	}
	// fill in the rank because server
	// doesn't have the rank correct under
	// various test scenarios where we
	// spin up an embedded gnatsd.
	//
	// This is still correct in non-test,
	// since the health-agent will
	// have read from the command line
	// -rank options and then
	// configured Cfg.MyRank when running
	// embedded as an internal client.
	loc.Rank = m.Cfg.MyRank
	return loc, nil
}
