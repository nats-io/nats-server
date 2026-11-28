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

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/nats-io/nkeys"
)

// JSLeaderBalanceOpts configures opt-in consumer Raft leader balancing.
type JSLeaderBalanceOpts struct {
	Enabled   bool
	Threshold int
	Interval  time.Duration
}

func (o JSLeaderBalanceOpts) validate() error {
	if o.Threshold < 0 || o.Threshold > 100 {
		return errors.New("jetstream leader_balancing threshold must be between 0 and 100")
	}
	if o.Interval != 0 && o.Interval < time.Minute {
		return errors.New("jetstream leader_balancing interval must be at least one minute")
	}
	return nil
}

func parseJSLeaderBalance(v any, tk token, opts *Options) error {
	m, ok := v.(map[string]any)
	if !ok {
		return &configErr{tk, "leader_balancing must be a map"}
	}
	o := JSLeaderBalanceOpts{Threshold: 30, Interval: time.Hour}
	for key, value := range m {
		var lt token
		t, val := unwrapValue(value, &lt)
		switch strings.ToLower(key) {
		case "enabled":
			b, ok := val.(bool)
			if !ok {
				return &configErr{t, "leader_balancing enabled must be a boolean"}
			}
			o.Enabled = b
		case "threshold":
			i, ok := val.(int64)
			if !ok || i < 0 || i > 100 {
				return &configErr{t, "leader_balancing threshold must be an integer between 0 and 100"}
			}
			o.Threshold = int(i)
		case "interval":
			d, ok := val.(string)
			if !ok {
				return &configErr{t, "leader_balancing interval must be a duration"}
			}
			var err error
			if o.Interval, err = time.ParseDuration(d); err != nil || o.Interval < time.Minute {
				return &configErr{t, "leader_balancing interval must be at least one minute"}
			}
		default:
			return &configErr{t, fmt.Sprintf("unknown leader_balancing field %q", key)}
		}
	}
	opts.JetStreamLeaderBalance = o
	return nil
}

const (
	leaderBalanceSubject        = "$JSC.LB.%s"
	leaderBalanceBatch          = 64
	leaderBalanceMinAge         = 30 * time.Second
	leaderBalanceBudget         = 100
	leaderBalanceSpacing        = time.Second
	leaderBalanceRequestTimeout = 2 * time.Second
	leaderBalanceInventoryTTL   = 30 * time.Second
	leaderBalanceRefreshMoves   = 10
)

type leaderBalanceGroup struct {
	Account, Stream, Consumer, Group, Cluster string
	Created                                   time.Time
	Voters                                    []string
	Movable                                   bool
}

type leaderBalanceStatus struct {
	Leader      string
	Term        uint64
	Ready       bool
	LeaderReady bool
	Applied     uint64
	Candidates  []string
	Voters      []string
}

type leaderBalanceMove struct {
	Group, Source, Target string
}

type leaderBalanceRequest struct {
	Meta      string
	Recipient string
	Reply     string
	Term      uint64
	MetaIndex uint64
	Expires   time.Time
	Groups    []string
	Transfer  *leaderBalanceTransfer
}

type leaderBalanceTransfer struct {
	Group          leaderBalanceGroup
	Source, Target string
	Term           uint64
	TargetProof    *leaderBalanceEnvelope
}

type leaderBalanceResponse struct {
	Node      string
	Reply     string
	Meta      string
	MetaTerm  uint64
	MetaIndex uint64
	Expires   time.Time
	Accepted  bool
	Status    map[string]leaderBalanceStatus
	proof     *leaderBalanceEnvelope
}

// leaderBalanceImbalanced checks the percentage spread relative to the maximum.
func leaderBalanceImbalanced(counts map[string]int, threshold int) bool {
	if len(counts) < 2 {
		return false
	}
	lo, hi := int(^uint(0)>>1), 0
	for _, c := range counts {
		lo, hi = min(lo, c), max(hi, c)
	}
	return hi > 0 && int64(hi-lo)*100 > int64(hi)*int64(threshold)
}

// selectLeaderBalanceMove maximizes the decrease in the sum of squared counts.
// The caller provides only currently eligible candidates. Tie-breaking is stable.
func selectLeaderBalanceMove(counts map[string]int, groups map[string]leaderBalanceStatus) (leaderBalanceMove, bool) {
	var best leaderBalanceMove
	gain := 0
	for group, st := range groups {
		src, ok := counts[st.Leader]
		if !ok {
			continue
		}
		for _, target := range st.Candidates {
			dst, ok := counts[target]
			if !ok || target == st.Leader {
				continue
			}
			d := src - dst - 1
			if d > gain || (d == gain && d > 0 && (group < best.Group || (group == best.Group && target < best.Target))) {
				best = leaderBalanceMove{group, st.Leader, target}
				gain = d
			}
		}
	}
	return best, gain > 0
}

func (n *raft) leaderBalanceStatus() leaderBalanceStatus {
	n.RLock()
	defer n.RUnlock()
	st := leaderBalanceStatus{Leader: n.leader, Term: n.term, Voters: n.peerNames(), Applied: min(n.applied, n.processed)}
	slices.Sort(st.Voters)
	switch n.State() {
	case Leader:
		st.LeaderReady = n.leaderState.Load()
		since := n.leaderSince.Load()
		st.Ready = n.balanceTransferReadyLocked() && since != nil && time.Since(*since) >= leaderBalanceMinAge
		if st.Ready {
			for _, id := range st.Voters {
				if n.balanceCandidateLocked(id) {
					st.Candidates = append(st.Candidates, id)
				}
			}
		}
	case Follower:
		st.Ready = n.leader != noLeader && n.peers[n.id] != nil && n.membChange == nil &&
			n.rescue == nil && !n.observer && !n.scaleUp && !n.paused && !n.quorumPaused &&
			n.catchup == nil && n.werr == nil && n.commit > 0 && n.applied >= n.commit &&
			n.processed >= n.commit && n.peers[n.leader] != nil && withinLiveWindow(n.peers[n.leader].ts)
	}
	return st
}

// Only stable replicated consumers may be moved. Unmovable replicated groups
// still count toward the distribution.
func leaderBalanceAssignment(account string, sa *streamAssignment, ca *consumerAssignment) (leaderBalanceGroup, bool) {
	if sa == nil || ca == nil || ca.Group == nil || len(ca.Group.Peers) < 2 {
		return leaderBalanceGroup{}, false
	}
	g := ca.Group
	voters := slices.Clone(g.Peers)
	slices.Sort(voters)
	// Assignment recovering flags describe historical replay, not current Raft health.
	// Live recovery is checked in the replica reports and the transfer primitive.
	stable := sa.Group != nil && sa.Group.Desired == nil && !sa.Group.ScaleUp && len(sa.Group.Excluded) == 0 &&
		!sa.reassigning && !sa.resetting && sa.err == nil && sa.unsupported == nil &&
		ca.err == nil && ca.unsupported == nil &&
		g.Desired == nil && !g.ScaleUp && len(g.Excluded) == 0 && ca.targetReplicas(sa.Config) == len(voters)
	return leaderBalanceGroup{account, ca.Stream, ca.Name, g.Name, g.Cluster, ca.Created, voters, stable}, true
}

func (js *jetStream) leaderBalanceInventory(meta RaftNode, term uint64) (map[string]leaderBalanceGroup, bool) {
	js.mu.RLock()
	defer js.mu.RUnlock()
	cc := js.cluster
	if cc == nil || cc.meta != meta || cc.term != term || js.metaRecovering || js.shuttingDown || cc.werr != nil || !meta.Leader() || len(cc.inflightStreams) != 0 || len(cc.inflightConsumers) != 0 {
		return nil, false
	}
	groups := make(map[string]leaderBalanceGroup)
	for account, streams := range cc.streams {
		for _, sa := range streams {
			for _, ca := range sa.consumers {
				if g, ok := leaderBalanceAssignment(account, sa, ca); ok {
					if _, exists := groups[g.Group]; exists {
						return nil, false
					}
					groups[g.Group] = g
				}
			}
		}
	}
	return groups, true
}

// Meta observations are a best-effort fence, not a distributed lease.
// The Raft lock provides a consistent view of the term and leader.
func (n *raft) leaderBalanceMetaMatches(leader string, term, index uint64) bool {
	n.RLock()
	defer n.RUnlock()
	if leader == noLeader || term == 0 || n.term != term || n.leader != leader ||
		n.observer || n.paused || n.quorumPaused || n.scaleUp || n.rescue != nil ||
		n.membChange != nil || n.werr != nil || n.catchup != nil || n.processed < n.commit ||
		n.peers[n.id] == nil || n.peers[leader] == nil || (index != 0 && n.commit != index) {
		return false
	}
	if leader == n.id {
		if n.State() != Leader || !n.leaderState.Load() {
			return false
		}
		live := 1
		for id, ps := range n.peers {
			if id != n.id && withinLiveWindow(ps.ts) {
				live++
			}
		}
		return live >= n.qn
	}
	return n.State() == Follower && withinLiveWindow(n.peers[leader].ts)
}

// Lock should be held.
func (js *jetStream) leaderBalanceMetaMatches(req *leaderBalanceRequest) bool {
	cc := js.cluster
	if cc == nil || js.shuttingDown || js.metaRecovering || cc.werr != nil ||
		!time.Now().Before(req.Expires) {
		return false
	}
	meta, ok := cc.meta.(*raft)
	return ok && req.MetaIndex != 0 && meta.leaderBalanceMetaMatches(req.Meta, req.Term, req.MetaIndex)
}

// The control messages are signed using the existing server NKey identity.
// Verification uses the server ID learned through normal peer discovery; the
// system account and peer discovery remain the existing administrative trust
// boundary. Recipient and inbox binding prevent cross-request replay.
type leaderBalanceEnvelope struct {
	Payload   json.RawMessage
	Signature []byte
}

func (s *Server) signLeaderBalance(v any) (*leaderBalanceEnvelope, error) {
	payload, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	sig, err := s.kp.Sign(payload)
	if err != nil {
		return nil, err
	}
	return &leaderBalanceEnvelope{payload, sig}, nil
}

func (s *Server) verifyLeaderBalance(node string, envelope *leaderBalanceEnvelope) bool {
	v, ok := s.nodeToInfo.Load(node)
	if !ok || v == nil {
		return false
	}
	ni := v.(nodeInfo)
	if ni.offline || !ni.js || !nkeys.IsValidPublicServerKey(ni.id) {
		return false
	}
	kp, err := nkeys.FromPublicKey(ni.id)
	return err == nil && kp.Verify(envelope.Payload, envelope.Signature) == nil
}

// Lock should be held.
func (js *jetStream) leaderBalanceGroupCurrent(g leaderBalanceGroup) (*consumerAssignment, bool) {
	cc := js.cluster
	if cc == nil || len(cc.inflightStreams) != 0 || len(cc.inflightConsumers) != 0 {
		return nil, false
	}
	sa := js.streamAssignment(g.Account, g.Stream)
	if sa == nil {
		return nil, false
	}
	ca := sa.consumers[g.Consumer]
	current, ok := leaderBalanceAssignment(g.Account, sa, ca)
	return ca, ok && current.Movable && current.Group == g.Group &&
		current.Created.Equal(g.Created) && current.Cluster == g.Cluster && slices.Equal(current.Voters, g.Voters)
}

// This is a privileged system-account operation, like other JetStream internal
// requests. The metadata and consumer terms fence stale requests.
// Validate target-side readiness at the source, not just at the coordinator.
// The signed proof is bound to a particular target, metadata epoch and group.
func (s *Server) leaderBalanceTargetProof(req *leaderBalanceRequest) (uint64, time.Time, bool) {
	tr := req.Transfer
	if tr == nil || tr.TargetProof == nil || !s.verifyLeaderBalance(tr.Target, tr.TargetProof) {
		return 0, time.Time{}, false
	}
	var resp leaderBalanceResponse
	if json.Unmarshal(tr.TargetProof.Payload, &resp) != nil || !resp.Accepted ||
		resp.Node != tr.Target || resp.Meta != req.Meta || resp.MetaTerm != req.Term || resp.MetaIndex != req.MetaIndex ||
		!time.Now().Before(resp.Expires) || time.Until(resp.Expires) > leaderBalanceRequestTimeout*2 ||
		len(resp.Status) != 1 {
		return 0, time.Time{}, false
	}
	st, ok := resp.Status[tr.Group.Group]
	if !ok || !st.Ready || st.LeaderReady || st.Leader != tr.Source || st.Term != tr.Term ||
		!slices.Equal(st.Voters, tr.Group.Voters) {
		return 0, time.Time{}, false
	}
	expires := resp.Expires
	if req.Expires.Before(expires) {
		expires = req.Expires
	}
	return st.Applied, expires, true
}

func (js *jetStream) processLeaderBalanceRequest(_ *subscription, _ *client, _ *Account, _, reply string, msg []byte) {
	s := js.srv
	if reply == _EMPTY_ || !s.getOpts().JetStreamLeaderBalance.Enabled || s.isShuttingDown() || s.isLameDuckMode() {
		return
	}
	var envelope leaderBalanceEnvelope
	var req leaderBalanceRequest
	if json.Unmarshal(msg, &envelope) != nil || json.Unmarshal(envelope.Payload, &req) != nil ||
		req.Recipient != s.NodeName() || req.Reply != reply || !strings.HasPrefix(reply, InboxPrefix) || !s.verifyLeaderBalance(req.Meta, &envelope) || len(req.Groups) > leaderBalanceBatch ||
		time.Until(req.Expires) > leaderBalanceRequestTimeout*2 || (req.Transfer != nil && len(req.Groups) != 0) {
		return
	}
	var targetApplied uint64
	var expires time.Time
	if req.Transfer != nil {
		var ok bool
		targetApplied, expires, ok = s.leaderBalanceTargetProof(&req)
		if !ok {
			return
		}
	}
	js.mu.RLock()
	if !js.leaderBalanceMetaMatches(&req) {
		js.mu.RUnlock()
		return
	}
	resp := leaderBalanceResponse{Node: js.cluster.meta.ID(), Reply: reply, Meta: req.Meta, MetaTerm: req.Term, MetaIndex: req.MetaIndex, Expires: req.Expires}
	if tr := req.Transfer; tr != nil {
		g := tr.Group
		ca, ok := js.leaderBalanceGroupCurrent(g)
		if ok && tr.Source == resp.Node && ca.Group.node != nil {
			if n, ok := ca.Group.node.(*raft); ok && n.ID() == tr.Source {
				// Recheck local opt-in and expiry before the Raft operation.
				if s.getOpts().JetStreamLeaderBalance.Enabled && !s.isShuttingDown() && !s.isLameDuckMode() && js.leaderBalanceMetaMatches(&req) {
					resp.Accepted = n.tryBalanceTransfer(tr.Target, tr.Term, g.Voters, leaderBalanceMinAge, expires, targetApplied) == nil
				}
			}
		}
		js.mu.RUnlock()
	} else {
		js.mu.RUnlock()
		resp.Accepted = true
		resp.Status = make(map[string]leaderBalanceStatus, len(req.Groups))
		for _, group := range req.Groups {
			if n, ok := s.lookupRaftNode(group).(*raft); ok {
				resp.Status[group] = n.leaderBalanceStatus()
			}
		}
	}
	// Send via the JetStream client so a local coordinator also receives replies.
	js.mu.RLock()
	if js.cluster != nil && js.cluster.c != nil {
		if signed, err := s.signLeaderBalance(&resp); err == nil {
			js.cluster.c.sendInternalMsg(reply, _EMPTY_, nil, signed)
		}
	}
	js.mu.RUnlock()
}

func (js *jetStream) requestLeaderBalance(ctx context.Context, node string, req leaderBalanceRequest) (leaderBalanceResponse, bool) {
	var zero leaderBalanceResponse
	if ctx.Err() != nil {
		return zero, false
	}
	s := js.srv
	if req.MetaIndex == 0 {
		if meta := js.getMetaGroup(); meta != nil {
			_, req.MetaIndex, _ = meta.Progress()
		}
		if req.MetaIndex == 0 {
			return zero, false
		}
	}
	req.Expires = time.Now().Add(leaderBalanceRequestTimeout)
	if deadline, ok := ctx.Deadline(); ok && deadline.Before(req.Expires) {
		req.Expires = deadline
	}
	s.mu.Lock()
	if s.sys == nil || s.sys.replies == nil {
		s.mu.Unlock()
		return zero, false
	}
	inbox := s.newRespInbox()
	req.Recipient, req.Reply = node, inbox
	ch := make(chan leaderBalanceResponse, 1)
	s.sys.replies[inbox] = func(_ *subscription, _ *client, _ *Account, _, _ string, msg []byte) {
		var envelope leaderBalanceEnvelope
		var resp leaderBalanceResponse
		if json.Unmarshal(msg, &envelope) == nil && json.Unmarshal(envelope.Payload, &resp) == nil &&
			resp.Node == node && resp.Reply == inbox && resp.Meta == req.Meta && resp.MetaTerm == req.Term && resp.MetaIndex == req.MetaIndex &&
			resp.Expires.Equal(req.Expires) && time.Now().Before(resp.Expires) && s.verifyLeaderBalance(node, &envelope) {
			resp.proof = &envelope
			select {
			case ch <- resp:
			default:
			}
		}
	}
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		if s.sys != nil && s.sys.replies != nil {
			delete(s.sys.replies, inbox)
		}
		s.mu.Unlock()
	}()
	signed, err := s.signLeaderBalance(&req)
	if err != nil || ctx.Err() != nil {
		return zero, false
	}
	s.sendInternalMsgLocked(fmt.Sprintf(leaderBalanceSubject, node), inbox, nil, signed)
	timer := time.NewTimer(leaderBalanceRequestTimeout)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return zero, false
	case <-s.quitCh:
		return zero, false
	case <-timer.C:
		return zero, false
	case resp := <-ch:
		return resp, resp.Accepted && ctx.Err() == nil
	}
}

// Reports are collected in bounded batches from every assigned voter. A missing
// or inconsistent report aborts the pass rather than undercounting leaders.
func (js *jetStream) collectLeaderBalance(ctx context.Context, req leaderBalanceRequest, groups map[string]leaderBalanceGroup, peers []string) (map[string]leaderBalanceStatus, bool) {
	if req.MetaIndex == 0 {
		if meta := js.getMetaGroup(); meta != nil {
			_, req.MetaIndex, _ = meta.Progress()
		}
		if req.MetaIndex == 0 {
			return nil, false
		}
	}
	byPeer := make(map[string][]string, len(peers))
	for _, peer := range peers {
		byPeer[peer] = nil
	}
	for _, g := range groups {
		for _, p := range g.Voters {
			if _, ok := byPeer[p]; !ok {
				return nil, false
			}
			byPeer[p] = append(byPeer[p], g.Group)
		}
	}
	reports := make(map[string]map[string]leaderBalanceStatus, len(peers))
	for _, peer := range peers {
		names := byPeer[peer]
		slices.Sort(names)
		reports[peer] = make(map[string]leaderBalanceStatus, len(names))
		for start := 0; start < len(names) || start == 0; start += leaderBalanceBatch {
			if ctx.Err() != nil || !js.srv.getOpts().JetStreamLeaderBalance.Enabled {
				return nil, false
			}
			req.Groups = names[start:min(start+leaderBalanceBatch, len(names))]
			resp, ok := js.requestLeaderBalance(ctx, peer, req)
			if !ok {
				return nil, false
			}
			for _, name := range req.Groups {
				st, ok := resp.Status[name]
				if !ok {
					return nil, false
				}
				reports[peer][name] = st
			}
		}
	}
	result := make(map[string]leaderBalanceStatus, len(groups))
	for name, g := range groups {
		if len(g.Voters) == 0 {
			return nil, false
		}
		first := reports[g.Voters[0]][name]
		if !slices.Contains(g.Voters, first.Leader) || !reports[first.Leader][name].LeaderReady {
			return nil, false
		}
		for _, p := range g.Voters {
			st := reports[p][name]
			if st.Leader != first.Leader || st.Term != first.Term || !slices.Equal(st.Voters, g.Voters) {
				return nil, false
			}
		}
		st := reports[first.Leader][name]
		st.Candidates = slices.DeleteFunc(st.Candidates, func(p string) bool {
			return !g.Movable || !st.Ready || !slices.Contains(g.Voters, p) || !reports[p][name].Ready
		})
		result[name] = st
	}
	return result, true
}

func (js *jetStream) leaderBalanceCanRun(meta RaftNode, term uint64, opts JSLeaderBalanceOpts) bool {
	s := js.srv
	n, ok := meta.(*raft)
	return ok && opts.Enabled && s.getOpts().JetStreamLeaderBalance == opts && !s.isShuttingDown() && !s.isLameDuckMode() &&
		n.leaderBalanceMetaMatches(n.ID(), term, 0) && !js.isMetaRecovering() && !js.isShuttingDown()
}

func (js *jetStream) runLeaderBalance(ctx context.Context, meta RaftNode, term uint64, opts JSLeaderBalanceOpts) {
	attempted := make(map[string]struct{})
	for len(attempted) < leaderBalanceBudget && ctx.Err() == nil {
		if !js.runLeaderBalanceBatch(ctx, meta, term, opts, attempted) {
			return
		}
	}
}

// Refresh the complete inventory periodically instead of trusting a long-lived
// plan. The attempted set is scoped to this pass, including across refreshes.
func (js *jetStream) runLeaderBalanceBatch(ctx context.Context, meta RaftNode, term uint64, opts JSLeaderBalanceOpts, attempted map[string]struct{}) (refresh bool) {
	s := js.srv
	if !js.leaderBalanceCanRun(meta, term, opts) {
		return
	}
	// Bind all reports and transfers in this batch to one applied metadata
	// revision. A concurrent assignment update invalidates the plan.
	_, revision, _ := meta.Progress()
	groups, ok := js.leaderBalanceInventory(meta, term)
	if !ok || len(groups) == 0 || revision == 0 || !meta.(*raft).leaderBalanceMetaMatches(meta.ID(), term, revision) {
		return
	}
	peers := meta.PeerNames()
	slices.Sort(peers)
	clusters := make(map[string]map[string]int)
	peerCluster := make(map[string]string, len(peers))
	for _, p := range peers {
		v, ok := s.nodeToInfo.Load(p)
		if !ok || v == nil {
			return
		}
		ni := v.(nodeInfo)
		if ni.offline || !ni.js || ni.cluster == _EMPTY_ || ni.domain != s.getOpts().JetStreamDomain {
			return
		}
		peerCluster[p] = ni.cluster
		if clusters[ni.cluster] == nil {
			clusters[ni.cluster] = make(map[string]int)
		}
		clusters[ni.cluster][p] = 0
	}
	req := leaderBalanceRequest{Meta: meta.ID(), Term: term, MetaIndex: revision}
	inventoryStart := time.Now()
	inventoryCtx, cancel := context.WithTimeout(ctx, leaderBalanceInventoryTTL)
	status, ok := js.collectLeaderBalance(inventoryCtx, req, groups, peers)
	cancel()
	if !ok {
		return
	}
	byCluster := make(map[string]map[string]leaderBalanceStatus)
	for name, st := range status {
		g := groups[name]
		cluster := peerCluster[st.Leader]
		if cluster == _EMPTY_ || g.Cluster != cluster {
			return
		}
		for _, p := range g.Voters {
			if peerCluster[p] != cluster {
				return
			}
		}
		clusters[cluster][st.Leader]++
		if byCluster[cluster] == nil {
			byCluster[cluster] = make(map[string]leaderBalanceStatus)
		}
		if _, alreadyAttempted := attempted[name]; !alreadyAttempted {
			byCluster[cluster][name] = st
		}
	}
	names := make([]string, 0, len(byCluster))
	for name := range byCluster {
		names = append(names, name)
	}
	slices.Sort(names)
	moved := 0
	for _, cluster := range names {
		counts, candidates := clusters[cluster], byCluster[cluster]
		for len(attempted) < leaderBalanceBudget && leaderBalanceImbalanced(counts, opts.Threshold) {
			if ctx.Err() != nil || !js.leaderBalanceCanRun(meta, term, opts) || !meta.(*raft).leaderBalanceMetaMatches(meta.ID(), term, revision) {
				return
			}
			if time.Since(inventoryStart) > leaderBalanceInventoryTTL {
				return moved > 0
			}
			move, ok := selectLeaderBalanceMove(counts, candidates)
			if !ok {
				break
			}
			st := candidates[move.Group]
			g := groups[move.Group]
			// Fresh target-side evidence, followed by strict leader-side validation.
			probe := req
			probe.Groups = []string{move.Group}
			r, ok := js.requestLeaderBalance(ctx, move.Target, probe)
			target := r.Status[move.Group]
			if !ok || !target.Ready || target.Term != st.Term || target.Leader != move.Source || !slices.Equal(target.Voters, g.Voters) {
				return
			}
			if ctx.Err() != nil || !js.leaderBalanceCanRun(meta, term, opts) || !meta.(*raft).leaderBalanceMetaMatches(meta.ID(), term, revision) {
				return
			}
			js.mu.RLock()
			_, current := js.leaderBalanceGroupCurrent(g)
			js.mu.RUnlock()
			if !current {
				return
			}
			transfer := req
			transfer.Transfer = &leaderBalanceTransfer{g, move.Source, move.Target, st.Term, r.proof}
			attempted[move.Group] = struct{}{}
			if _, ok := js.requestLeaderBalance(ctx, move.Source, transfer); !ok {
				return
			}
			// Confirm actual leadership before accounting for the move.
			confirmCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			confirmed := js.confirmLeaderBalance(confirmCtx, meta, term, opts, move, g, st, probe)
			cancel()
			if !confirmed {
				return
			}
			moved++
			counts[move.Source]--
			counts[move.Target]++
			delete(candidates, move.Group)
			s.Debugf("JetStream consumer leader balance: %s transferred from %s to %s", move.Group, move.Source, move.Target)
			select {
			case <-ctx.Done():
				return
			case <-time.After(leaderBalanceSpacing):
			}
			if moved >= leaderBalanceRefreshMoves {
				return true
			}
		}
	}
	return false
}

func (js *jetStream) confirmLeaderBalance(ctx context.Context, meta RaftNode, term uint64, opts JSLeaderBalanceOpts, move leaderBalanceMove, g leaderBalanceGroup, st leaderBalanceStatus, probe leaderBalanceRequest) bool {
	for ctx.Err() == nil && js.leaderBalanceCanRun(meta, term, opts) {
		r, ok := js.requestLeaderBalance(ctx, move.Target, probe)
		newStatus := r.Status[move.Group]
		if ok && newStatus.LeaderReady && newStatus.Leader == move.Target && newStatus.Term > st.Term && slices.Equal(newStatus.Voters, g.Voters) {
			return true
		}
		// A newer term with an unexpected leader invalidates the whole plan.
		if ok && newStatus.Term > st.Term && newStatus.Leader != noLeader && newStatus.Leader != move.Target {
			return false
		}
		select {
		case <-ctx.Done():
			return false
		case <-time.After(100 * time.Millisecond):
		}
	}
	return false
}
