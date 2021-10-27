// Copyright 2021-2021 The NATS Authors
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
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"net/textproto"
	"strings"
	"time"
)

// Structs used for json serialization of a trace
type TraceMsg struct {
	TrcRef  string `json:"trc_ref"`
	CtxName string `json:"ctx_name,omitempty"`
	// contains a value corresponding to a TraceSubscription or TraceStore entry with ref set.
	// When not present or empty, the message originated from a client.
	MsgRef            string               `json:"msg_ref,omitempty"`
	StreamRef         string               `json:"stream_ref,omitempty"`
	StreamType        string               `json:"stream_type,omitempty"`
	ConsumerRef       string               `json:"consumer_ref,omitempty"`
	Acc               string               `json:"acc"`
	SubjectPreMapping string               `json:"subject_pre_mapping,omitempty"`
	SubjectMatched    string               `json:"subject_matched,omitempty"`
	Reply             string               `json:"reply_subject,omitempty"`
	Client            *TraceClient         `json:"client,omitempty"`
	Subscriptions     []*TraceSubscription `json:"subscriptions,omitempty"`
	Stores            []*TraceStore        `json:"stores,omitempty"`
	ProcessingStart   string               `json:"processing_start,omitempty"`
	ProcessingDur     string               `json:"processing_duration,omitempty"`
	// Contains Info objects for probes
	AssociatedInfo *AssociatedTraceInfo `json:"associated_info,omitempty"`
}

type TraceClient struct {
	Type     string `json:"type,omitempty"`
	Kind     string `json:"kind,omitempty"`
	Name     string `json:"name,omitempty"`
	ClientId uint64 `json:"client_id,omitempty"`
	ServerId string `json:"server_id,omitempty"`
}

type TraceSubscription struct {
	Sub        string       `json:"subscription"`
	ImportTo   string       `json:"import_to,omitempty"`
	ImportSub  string       `json:"import_subscription,omitempty"`
	SubClient  *TraceClient `json:"client,omitempty"`
	Acc        string       `json:"acc"`
	Queue      string       `json:"queue,omitempty"` //todo queue sub across gateway
	PubSubject string       `json:"subject,omitempty"`
	Reply      string       `json:"reply_subject,omitempty"`
	Ref        string       `json:"ref,omitempty"`
	// When present, contains the reason why the send to the subscription did NOT happen
	NoSendReason string `json:"no_send_reason,omitempty"`
}

type TraceStore struct {
	StreamSubs []string `json:"stream_subjects"`
	Acc        string   `json:"acc"`
	PubSubject string   `json:"subject,omitempty"`
	Ref        string   `json:"ref,omitempty"`
	// under certain circumstances, this may reference the message that TraceStore is in.
	// Specifically in the same document, it is possible for a TraceSubscription to exist that is referenced by this.
	MsgRef     string `json:"msg_ref,omitempty"`
	StreamRef  string `json:"stream_ref,omitempty"`
	StreamType string `json:"stream_type,omitempty"`
	StreamRepl int    `json:"stream_repl,omitempty"`
	JsMsgId    string `json:"js_msg_id,omitempty"`
	DidStore   bool   `json:"did_store,omitempty"`
	// When present, contains the reason why the store did NOT happen
	NoStoreReason string `json:"no_store_reason,omitempty"`
}

type AssociatedTraceInfo struct {
	ConnInfo    []*ConnInfo    `json:"connz,omitempty"`
	AccountInfo []*AccountInfo `json:"accountz,omitempty"`
	Varz        *Varz          `json:"varz,omitempty"`
	Msg         []byte         `json:"msg,omitempty"`
	Header      http.Header    `json:"header,omitempty"`
}

// Header used for tracing
// When present, the message will not actually be delivered to subscriber/JetStream or across accounts.
// This header only requires presence, but can contain a list of comma separated probe names.
// The message body can contain a comma separated list of server ids to filter which server respond.
// (use if your setup limits response messages)
const TracePing = "Nats-Ping"

// Tracing System Header.
const TraceHeader = "Nats-Trace"

// Content of TraceHeader
type traceHeader struct {
	Ref    string           `json:"msg_ref,omitempty"`
	Traces []*selectedTrace `json:"traces,omitempty"`
}

// Tracing rule that fired.
type selectedTrace struct {
	Ref  string `json:"ref"`
	Name string `json:"name"`
	// include always. not all policies may be present in every server.
	TrcSubj  string `json:"trc_subj"`
	LocalAcc string `json:"local_acc,omitempty"`
	probes   `json:"probes,omitempty"`
	sendAcc  *Account
}

func (trc *selectedTrace) IsOperatorTrace() bool {
	return trc.LocalAcc == _EMPTY_
}

// multiple selected traces that share the actual trace message but vary on the details
type traceGroup struct {
	traces   []*selectedTrace
	traceMsg TraceMsg
}

// A tracing context is per receive loop. Do not access concurrently!!!
type traceCtx struct {
	hasTrace      bool
	trcHdrSet     bool
	srv           *Server
	rnd           *rand.Rand
	traceStart    time.Time
	ctxName       string
	global        traceGroup // no matter how many traces match, the message will always be the same
	pub           traceGroup
	hdr           traceGroup // temporary trace group for header
	pubAcc        *Account
	importedStrms map[*streamImport]*traceGroup
	exportedSrvcs map[*serviceImport]*traceGroup

	liftedTraces []*selectedTrace
	msgAcc       *Account

	stream   *stream
	consumer *consumer
	Msg      []byte
	Header   http.Header
}

func newTraceCtx(loopName string, srv *Server) *traceCtx {
	return &traceCtx{
		srv:     srv,
		rnd:     rand.New(rand.NewSource(time.Now().UnixNano())),
		ctxName: loopName,
		global:  traceGroup{traces: make([]*selectedTrace, 0, 10)},
		pub:     traceGroup{traces: make([]*selectedTrace, 0, 10)},
	}
}

// config structs and functions
type Trace struct {
	trigger
	probes
	TrcSubject string // Where to send a trace to.
}

type trigger struct {
	Subject    string
	NotSubject string
	SampleFreq float64
}

type probes struct {
	Connz    bool `json:"trc_connz,omitempty"`
	Accountz bool `json:"trc_accountz,omitempty"`
	Varz     bool `json:"trc_varz,omitempty"`
	Msg      bool `json:"trc_msg,omitempty"`
}

func (t trigger) hasTrigger() bool {
	return t == trigger{}
}

func (trc *probes) fillTraceProbes(items []string, errOnUnknown, privilegedProbes bool) error {
	for _, v := range items {
		switch strings.ToLower(v) {
		case "all":
			*trc = probes{true, true, privilegedProbes, !privilegedProbes}
		case "connz":
			trc.Connz = true
		case "accountz":
			trc.Accountz = true
		case "varz":
			if !privilegedProbes {
				return fmt.Errorf("varz only valid for operator")
			}
			trc.Varz = true
		case "msg":
			if privilegedProbes {
				return fmt.Errorf("msg only valid for account")
			}
			trc.Msg = true
		default:
			if errOnUnknown {
				return fmt.Errorf("unknown trace item")
			}
		}
	}
	return nil
}

// collect info applicable to a particular trace
func (tCtx *traceCtx) traceInfo(t *selectedTrace, tMsg *TraceMsg, accInfoCache map[string]*AccountInfo, connInfoCache map[uint64]*ConnInfo, now time.Time) *AssociatedTraceInfo {
	if !t.Accountz && !t.Connz && !t.Varz && !t.Msg {
		return nil
	}
	var err error
	var ok bool
	var accInfo []*AccountInfo
	if t.Accountz {
		accInfo = make([]*AccountInfo, 0, 1+len(tMsg.Subscriptions))
		accSet := make(map[string]struct{})

		var ai *AccountInfo
		if ai, ok = accInfoCache[tMsg.Acc]; !ok {
			ai, err = tCtx.srv.accountInfoByName(tMsg.Acc)
			if err != nil {
				tCtx.srv.Errorf("Unexpected error during trace account info gathering: %s", err)
			}
		}
		if ai != nil {
			accInfoCache[tMsg.Acc] = ai
			accSet[tMsg.Acc] = struct{}{}
			accInfo = append(accInfo, ai)
		}
		for _, s := range tMsg.Subscriptions {
			if _, ok := accSet[s.Acc]; ok {
				continue
			}
			if ai, ok = accInfoCache[s.Acc]; !ok {
				ai, err = tCtx.srv.accountInfoByName(s.Acc)
				if err != nil {
					tCtx.srv.Errorf("Unexpected error during trace account info gathering: %s", err)
					continue
				}
				accInfoCache[s.Acc] = ai
			}
			accSet[tMsg.Acc] = struct{}{}
			accInfo = append(accInfo, ai)
		}
	}
	var connInfo []*ConnInfo
	if t.Connz {
		connInfo = make([]*ConnInfo, 0, 1+len(tMsg.Subscriptions))
		connSet := make(map[uint64]struct{})

		var ci *ConnInfo
		if tMsg.Client != nil {
			if ci, ok = connInfoCache[tMsg.Client.ClientId]; !ok {
				var c *client
				if tMsg.Client.Kind == "Leafnode" {
					c = tCtx.srv.GetLeafNode(tMsg.Client.ClientId)
				} else if tMsg.Client.Kind == "Client" {
					c = tCtx.srv.getClient(tMsg.Client.ClientId)
				} else if tMsg.Client.Kind == "Router" {
					tCtx.srv.mu.Lock()
					c = tCtx.srv.routes[tMsg.Client.ClientId]
					tCtx.srv.mu.Unlock()
				}
				if !(tMsg.Client.Kind == "JetStream" || tMsg.Client.Kind == "Account") {
					if c == nil {
						tCtx.srv.Errorf("%s Client not found during trace conn info gathering", tMsg.Client.Kind)
					} else {
						ci = &ConnInfo{}
						c.mu.Lock()
						ci.fill(c, c.nc, now, true)
						c.mu.Unlock()
						connInfoCache[tMsg.Client.ClientId] = ci
					}
				}
			}
		}
		if ci != nil {
			connInfoCache[tMsg.Client.ClientId] = ci
			connSet[tMsg.Client.ClientId] = struct{}{}
			connInfo = append(connInfo, ci)
		}
		for _, s := range tMsg.Subscriptions {
			if s.SubClient == nil {
				continue
			}
			if _, ok := connSet[s.SubClient.ClientId]; ok {
				continue
			}
			if ci, ok = connInfoCache[s.SubClient.ClientId]; !ok {
				var c *client
				if s.SubClient.Kind == "Leafnode" {
					c = tCtx.srv.GetLeafNode(s.SubClient.ClientId)
				} else if s.SubClient.Kind == "Client" {
					c = tCtx.srv.getClient(s.SubClient.ClientId)
				} else if s.SubClient.Kind == "Router" {
					tCtx.srv.mu.Lock()
					c = tCtx.srv.routes[s.SubClient.ClientId]
					tCtx.srv.mu.Unlock()
				}
				if !(s.SubClient.Kind == "JetStream" || s.SubClient.Kind == "Account") {
					if c == nil {
						tCtx.srv.Errorf("%s Client not found during subscriber trace conn info gathering", s.SubClient.Kind)
						continue
					} else {
						ci = &ConnInfo{}
						c.mu.Lock()
						ci.fill(c, c.nc, now, true)
						c.mu.Unlock()
						connInfoCache[s.SubClient.ClientId] = ci
					}
				}
			}
			if ci != nil {
				connSet[s.SubClient.ClientId] = struct{}{}
				connInfo = append(connInfo, ci)
			}
		}
	}
	var varInfo *Varz
	if t.Varz {
		varInfo, err = tCtx.srv.Varz(nil)
		if err != nil {
			tCtx.srv.Errorf("Unexpected error during trace varz info gathering: %s", err)
		}
	}
	var msg []byte
	var hdr http.Header
	if t.Msg {
		msg = tCtx.Msg
		hdr = tCtx.Header
	}
	return &AssociatedTraceInfo{connInfo, accInfo, varInfo, msg, hdr}
}

// Send to all traces sharing the same trace message but possibly differ in trace level
func (tCtx *traceCtx) traceGroupSend(group *traceGroup, accInfoSet map[string]*AccountInfo, connInfoSet map[uint64]*ConnInfo, now time.Time, start time.Time, ctxName string) {
	if len(group.traces) == 0 {
		return
	}
	for _, t := range group.traces {
		tMsg := group.traceMsg
		tMsg.CtxName = ctxName
		tMsg.TrcRef = t.Ref
		tMsg.ProcessingStart = start.String()
		tMsg.ProcessingDur = now.Sub(start).String()
		tMsg.AssociatedInfo = tCtx.traceInfo(t, &tMsg, accInfoSet, connInfoSet, now)
		si := &ServerInfo{}
		tCtx.srv.sendInternalAccountMsgWithSi(t.sendAcc, t.TrcSubj, si, &ServerAPIResponse{Server: si, Data: &tMsg})
	}
}

// Completes an ongoing trace by sending all trace messages
func (tCtx *traceCtx) traceMsgCompletion(send bool) {
	if !tCtx.hasTrace {
		tCtx.consumer = nil
		tCtx.stream = nil
		tCtx.trcHdrSet = false
		tCtx.msgAcc = nil
		return
	}
	tCtx.srv.Debugf("Trace Complete: global (%d), header (%d), pub (%d), imported (%d), exported (%d)",
		len(tCtx.global.traces), len(tCtx.hdr.traces), len(tCtx.pub.traces), len(tCtx.importedStrms), len(tCtx.exportedSrvcs))
	now := time.Now().UTC()
	connInfoSet := map[uint64]*ConnInfo{}
	accInfoSet := map[string]*AccountInfo{}
	if len(tCtx.global.traces) > 0 {
		if send {
			tCtx.traceGroupSend(&tCtx.global, accInfoSet, connInfoSet, now, tCtx.traceStart, tCtx.ctxName)
		}
		tCtx.global.traces = []*selectedTrace{}
		tCtx.global.traceMsg = TraceMsg{}
	}
	if len(tCtx.hdr.traces) > 0 {
		if send {
			tCtx.hdr.traceMsg = tCtx.pub.traceMsg
			tCtx.traceGroupSend(&tCtx.hdr, accInfoSet, connInfoSet, now, tCtx.traceStart, tCtx.ctxName)
		}
		tCtx.hdr.traces = []*selectedTrace{}
		tCtx.hdr.traceMsg = TraceMsg{}
	}
	if len(tCtx.pub.traces) > 0 {
		if send {
			tCtx.traceGroupSend(&tCtx.pub, accInfoSet, connInfoSet, now, tCtx.traceStart, tCtx.ctxName)
		}
		tCtx.pub.traces = []*selectedTrace{}
		tCtx.pubAcc = nil
		tCtx.pub.traceMsg = TraceMsg{}
	}
	for k, tGrp := range tCtx.importedStrms {
		if send {
			tCtx.traceGroupSend(tGrp, accInfoSet, connInfoSet, now, tCtx.traceStart, tCtx.ctxName)
		}
		tGrp.traces = []*selectedTrace{}
		delete(tCtx.importedStrms, k)
	}
	for k, tGrp := range tCtx.exportedSrvcs {
		if send {
			tCtx.traceGroupSend(tGrp, accInfoSet, connInfoSet, now, tCtx.traceStart, tCtx.ctxName)
		}
		tGrp.traces = []*selectedTrace{}
		delete(tCtx.exportedSrvcs, k)
	}
	tCtx.liftedTraces = []*selectedTrace{}
	tCtx.hasTrace = false
	tCtx.trcHdrSet = false
	tCtx.consumer = nil
	tCtx.stream = nil
	tCtx.Msg = nil
	tCtx.Header = nil
	tCtx.msgAcc = nil
	tCtx.traceStart = time.Time{}
}

func (tCtx *traceCtx) traceCtxMarshal(msgRef string, filterAcc *Account, filterSvc *serviceImport, filterStrm *streamImport) ([]byte, error) {
	if !tCtx.hasTrace {
		return nil, nil
	}
	var _iad [30]*selectedTrace
	hdr := &traceHeader{Ref: msgRef, Traces: _iad[:0]}
	hdr.Traces = append(hdr.Traces, tCtx.global.traces...)
	if tg, ok := tCtx.importedStrms[filterStrm]; ok {
		hdr.Traces = append(hdr.Traces, tg.traces...)
	} else if tg, ok := tCtx.exportedSrvcs[filterSvc]; ok {
		hdr.Traces = append(hdr.Traces, tg.traces...)
	} else if tCtx.pubAcc == filterAcc {
		hdr.Traces = append(hdr.Traces, tCtx.pub.traces...)
	}
	if len(hdr.Traces) > 0 || hdr.Ref != _EMPTY_ {
		return json.Marshal(hdr)
	} else {
		return nil, nil
	}
}

func (c *client) traceSetHeader(tCtx *traceCtx, dmsg []byte, msgRef string, acc *Account, rc *client, filterSvc *serviceImport, filterStrm *streamImport) ([]byte, bool) {
	if !tCtx.hasTrace {
		return dmsg, false
	}
	hset := false
	if cnt, _ := tCtx.traceCtxMarshal(msgRef, acc, filterSvc, filterStrm); cnt != nil {
		if rc.kind == ROUTER || rc.kind == GATEWAY || rc.kind == JETSTREAM || rc.kind == SYSTEM {
			dmsg = c.setHeader(TraceHeader, string(cnt), dmsg)
			hset = true
		}
	}
	if !hset && msgRef != _EMPTY_ {
		dmsg = c.setHeader(TraceHeader, fmt.Sprintf(`{"msg_ref":"%s"}`, msgRef), dmsg)
		hset = true
	}
	return dmsg, hset
}

func (tCtx *traceCtx) evalTrace(name string, policy Trace, filterAcc string, pubSubject string) string {
	if policy.SampleFreq != 0 && tCtx.rnd.Float64() > policy.SampleFreq {
		return _EMPTY_
	}
	// optimize for not being smaller scope. Say: Subject ">" and NotSubject "_INBOX_.>"
	if policy.NotSubject != _EMPTY_ && subjectIsSubsetMatch(pubSubject, policy.NotSubject) {
		return _EMPTY_
	}
	if policy.Subject != _EMPTY_ && !subjectIsSubsetMatch(pubSubject, policy.Subject) {
		return _EMPTY_
	}
	subj := policy.TrcSubject
	if subj == _EMPTY_ {
		if filterAcc == _EMPTY_ {
			subj = fmt.Sprintf(serverTrcEvent, tCtx.srv.ID(), name)
		} else {
			subj = fmt.Sprintf(accTrcEvent, filterAcc, name)
		}
	}
	// Do not trace messages sent to this trace's trace subject
	if pubSubject == subj {
		subj = _EMPTY_
	}
	return subj
}

// evaluates which traces apply to a message and if so, if they were added or carried over.
// In other words, where traces requesting the message content, added or where already added by another server?
func (tCtx *traceCtx) evalTraces(acc *Account, accName, pubSubject string, msgTrcPolicies map[string]Trace, tracePolicies []*selectedTrace) (bool, []*selectedTrace) {
	if len(msgTrcPolicies) == 0 {
		return false, tracePolicies
	}
	firstMsg := false
FOR_ACCOUNT_POLICY:
	for name, policy := range msgTrcPolicies {
		for _, v := range tracePolicies {
			if v.LocalAcc == accName && name == v.Name {
				v.sendAcc = acc
				// skip evaluating and just carry over. Also prioritize tracing setting at point of origin
				continue FOR_ACCOUNT_POLICY
			}
		}
		// We may be here because this is the point of origin, or because subsequent server have additional tracing
		if subj := tCtx.evalTrace(name, policy, accName, pubSubject); subj != _EMPTY_ {
			var b [8]byte
			rn := rand.Int63()
			for i, l := 0, rn; i < len(b); i++ {
				b[i] = digits[l%base]
				l /= base
			}
			trcRef := string(b[:])
			firstMsg = firstMsg || policy.Msg
			tracePolicies = append(tracePolicies, &selectedTrace{
				Name:     name,
				probes:   policy.probes,
				TrcSubj:  subj,
				LocalAcc: accName,
				sendAcc:  acc,
				Ref:      trcRef})
		}
	}
	return firstMsg, tracePolicies
}

func (tCtx *traceCtx) evalHdrTraces(acc *Account, trcSubj string, trcLvel []string, msgBody []byte) []*selectedTrace {
	if acc.pingProbes == nil || trcSubj == _EMPTY_ {
		return nil
	}
	// Messages with TracePing set have a comma separated list of server id's as message body.
	if body := strings.Trim(string(msgBody), "\r\n\t "); len(body) > 0 {
		// When present, only server listed will emit a trace message.
		found := false
		filterIds := strings.Split(body, ",")
		for _, id := range filterIds {
			if strings.TrimSpace(strings.ToLower(id)) == strings.ToLower(tCtx.srv.ID()) {
				found = true
				break
			}
			// TODO deal with hop cnt
		}
		if !found && len(filterIds) > 0 {
			return nil
		}
	}
	connz := acc.pingProbes.Connz
	accountz := acc.pingProbes.Accountz
	varz := acc.pingProbes.Varz
	msg := acc.pingProbes.Msg
	tmpPol := &Trace{}
	if err := tmpPol.fillTraceProbes(trcLvel, true, false); err != nil {
		tCtx.srv.Warnf("Trace Subject warn level Parsing error: %s", err)
		return nil
	}
	return []*selectedTrace{{TrcSubj: trcSubj, sendAcc: acc, probes: probes{
		Connz:    connz && tmpPol.Connz,
		Accountz: accountz && tmpPol.Accountz,
		Varz:     varz && tmpPol.Varz,
		Msg:      msg && tmpPol.Msg}}}
}

// Gather JS traces
// This does NOT evaluate rules, it simply records JS data for Messages already traced
// This call must be matched by a call to traceMsgCompletion.
func (tCtx *traceCtx) traceJsMsgStore(msg []byte, hdr []byte, subj, reply, msgId string, msgC *client, msgCAcc *Account, mset *stream) (string, []*TraceStore, bool) {
	if len(hdr) == 0 {
		return _EMPTY_, nil, false
	}
	if isHeaderSet(TracePing, hdr) {
		msgC.Errorf("Header %s not expected to appear here", TracePing)
		return _EMPTY_, nil, false
	}
	var _ia1 [8]*selectedTrace
	trcHeader := traceHeader{Traces: _ia1[:0]}
	// Read and Enforce (system) policy header rules
	if policies := getHeader(TraceHeader, hdr); len(policies) > 0 {
		if err := json.Unmarshal(policies, &trcHeader); err != nil {
			msgC.Errorf("Badly formatted header: %s", TraceHeader)
		}
	}
	// skip if already happened
	canComplete := false
	if !tCtx.hasTrace {
		tCtx.tracePub(msgC, msgCAcc, subj, reply, msg, trcHeader.Traces, false, "", trcHeader.Ref, nil)
		canComplete = true
	}
	// TODO evaluate JetStream specific queries, like trace all messages stored in a stream
	if !tCtx.hasTrace {
		return _EMPTY_, nil, false
	}
	if mset.acc == tCtx.srv.SystemAccount() {
		return _EMPTY_, nil, false
	}

	var b [8]byte
	rn := rand.Int63()
	for i, l := 0, rn; i < len(b); i++ {
		b[i] = digits[l%base]
		l /= base
	}
	msgRef := string(b[:])

	var _ia [8]*TraceStore
	subs := _ia[:0]
	if len(tCtx.global.traces) > 0 {
		s := &TraceStore{
			Acc:        mset.acc.Name,
			StreamRef:  mset.cfg.Name,
			StreamType: mset.store.Type().String(),
			StreamRepl: mset.cfg.Replicas,
			PubSubject: subj,
			MsgRef:     trcHeader.Ref,
			Ref:        msgRef,
			StreamSubs: mset.cfg.Subjects,
			JsMsgId:    msgId,
		}
		tCtx.global.traceMsg.Stores = append(tCtx.global.traceMsg.Stores, s)
		subs = append(subs, s)
	}
	if len(tCtx.pub.traces) > 0 || len(tCtx.hdr.traces) > 0 {
		s := &TraceStore{
			Acc:        mset.acc.Name,
			StreamRef:  mset.cfg.Name,
			StreamType: mset.store.Type().String(),
			StreamRepl: mset.cfg.Replicas,
			PubSubject: subj,
			MsgRef:     trcHeader.Ref,
			Ref:        msgRef,
			StreamSubs: mset.cfg.Subjects,
			JsMsgId:    msgId,
		}
		tCtx.pub.traceMsg.Stores = append(tCtx.pub.traceMsg.Stores, s)
		subs = append(subs, s)
	}
	return msgRef, subs, canComplete
}

// conditionally capture header and message content
func condCaptureMsg(firstMsg bool, hdrStart int, msg []byte) (http.Header, []byte) {
	if !firstMsg {
		return nil, nil
	}
	var h http.Header
	if hdrStart < 0 {
		hdrStart = 0
	} else {
		tp := textproto.NewReader(bufio.NewReader(bytes.NewReader(msg[:hdrStart])))
		tp.ReadLine() // skip over first line, contains version
		if mimeHeader, err := tp.ReadMIMEHeader(); err == nil {
			h = http.Header(mimeHeader)
		}
	}
	return h, msg[hdrStart : len(msg)-2] // strip \r\n
}

// Gather publisher traces
func (tCtx *traceCtx) tracePub(msgC *client, msgAcc *Account, pubSubject, reply string, msg []byte, tracePolicies []*selectedTrace, trcHdrSet bool, trcSubj, trcRef string, trcLvel []string) {
	srv := tCtx.srv
	if srv == nil {
		return
	}
	tCtx.msgAcc = msgAcc
	tCtx.trcHdrSet = trcHdrSet
	// In case storage in JetStream needs to be monitored in detail, only return if msgC.kind == Client
	if msgAcc == srv.SystemAccount() {
		return
	}
	opts := srv.getOpts()

	msgAccName := msgAcc.Name

	var _ia1 [4]*selectedTrace
	var _ia2 [4]*selectedTrace
	var _ia3 [4]*selectedTrace
	globalTracePolicies := _ia1[:0]
	pubAccTracePolicies := _ia2[:0]
	liftedTraces := _ia3[:0]

	for _, v := range tracePolicies {
		if v.LocalAcc == _EMPTY_ {
			v.sendAcc = srv.SystemAccount()
			globalTracePolicies = append(globalTracePolicies, v)
		} else if v.LocalAcc == msgAccName {
			v.sendAcc = msgAcc
			pubAccTracePolicies = append(pubAccTracePolicies, v)
		} else if v.LocalAcc != _EMPTY_ && msgC.kind != CLIENT {
			if msgAcc == srv.SystemAccount() {
				if a, err := srv.lookupAccount(v.LocalAcc); err == nil {
					v.sendAcc = a
				}
			}
			if v.sendAcc == nil {
				v.LocalAcc = msgAccName
				v.sendAcc = msgAcc
			}
			pubAccTracePolicies = append(pubAccTracePolicies, v)
		} else {
			liftedTraces = append(liftedTraces, v)
		}
	}
	tCtx.liftedTraces = liftedTraces
	_, globalTracePolicies = tCtx.evalTraces(srv.SystemAccount(), "", pubSubject, opts.MsgTracePolicy, globalTracePolicies)
	firstMsg := false
	firstMsg, pubAccTracePolicies = tCtx.evalTraces(msgAcc, msgAccName, pubSubject, msgAcc.traces, pubAccTracePolicies)

	// Turn this into an eval Hdr Traces call
	_, msgBody := msgC.msgParts(msg)
	hdrTracePolicies := tCtx.evalHdrTraces(msgAcc, trcSubj, trcLvel, msgBody)

	refServerId := _EMPTY_
	if len(globalTracePolicies) > 0 || len(pubAccTracePolicies) > 0 || len(hdrTracePolicies) > 0 {
		if tCtx.traceStart.IsZero() {
			tCtx.traceStart = time.Now().UTC()
		}
		switch {
		case msgC.leaf != nil:
			refServerId = msgC.leaf.remoteId
		case msgC.route != nil:
			refServerId = msgC.route.remoteID
		case msgC.gw != nil:
			refServerId = msgC.opts.Name
		}
		if msgC.kind == JETSTREAM && refServerId == _EMPTY_ {
			refServerId = srv.ID()
		}
	}
	if len(globalTracePolicies) > 0 {
		tCtx.global.traces = globalTracePolicies
		tCtx.global.traceMsg = TraceMsg{
			Client: &TraceClient{
				Type:     msgC.clientTypeString(),
				Kind:     msgC.kindString(),
				Name:     msgC.GetName(),
				ClientId: msgC.cid,
				ServerId: refServerId,
			},
			Acc:               msgAccName,
			SubjectMatched:    pubSubject,
			SubjectPreMapping: string(msgC.pa.mapped),
			Subscriptions:     make([]*TraceSubscription, 0, 4),
			MsgRef:            trcRef,
			Reply:             reply,
		}
		tCtx.hasTrace = true
		if tCtx.stream != nil {
			tCtx.global.traceMsg.StreamType = tCtx.stream.store.Type().String()
			tCtx.global.traceMsg.StreamRef = tCtx.stream.name()
		}
		if tCtx.consumer != nil {
			tCtx.global.traceMsg.ConsumerRef = tCtx.consumer.name
		}
	}
	if len(pubAccTracePolicies) > 0 || len(hdrTracePolicies) > 0 {
		tCtx.hdr.traces = hdrTracePolicies
		tCtx.pub.traces = pubAccTracePolicies
		tCtx.pub.traceMsg = TraceMsg{
			Client: &TraceClient{
				Type:     msgC.clientTypeString(),
				Kind:     msgC.kindString(),
				Name:     msgC.GetName(),
				ClientId: msgC.cid,
				ServerId: refServerId,
			},
			Acc:               msgAccName,
			SubjectMatched:    pubSubject,
			SubjectPreMapping: string(msgC.pa.mapped),
			Subscriptions:     make([]*TraceSubscription, 0, 4),
			MsgRef:            trcRef,
			Reply:             reply,
		}
		tCtx.pubAcc = msgAcc
		tCtx.hasTrace = true
		if tCtx.stream != nil {
			tCtx.pub.traceMsg.StreamType = tCtx.stream.store.Type().String()
			tCtx.pub.traceMsg.StreamRef = tCtx.stream.name()
		}
		if tCtx.consumer != nil {
			tCtx.pub.traceMsg.ConsumerRef = tCtx.consumer.name
		}
		tCtx.Header, tCtx.Msg = condCaptureMsg(firstMsg, msgC.pa.hdr, msg)
	}
}

// Gathers subscriber traces
// When necessary, returns a message id to include in TraceHeader
func (tCtx *traceCtx) traceSub(c *client, sub *subscription, acc *Account, subject, reply, msg []byte) (string, []*TraceSubscription) {
	if sub.client.kind == ACCOUNT || tCtx.srv == nil {
		return _EMPTY_, nil
	}
	// In case storage in JetStream needs to be monitored in detail, only return if msgC.kind == Client
	if acc == tCtx.srv.SystemAccount() {
		return _EMPTY_, nil
	}
	var trc *traceGroup
	var importFound bool
	subAccName := acc.Name
	subAcc := acc
	subTo := _EMPTY_
	subImp := _EMPTY_
	pubSubj := string(subject)
	subscription := string(sub.subject)
	if sub.im != nil {
		subTo = sub.im.to
		subImp = sub.im.from
		if sub.im.tr != nil {
			subImp, subTo = sub.im.tr.src, sub.im.tr.dest
		}
		subAcc = sub.client.acc
		subAccName = subAcc.Name
		// Determine if import tracing is needed
		trc, importFound = tCtx.importedStrms[sub.im]
		if !importFound {
			firstMsg, tracePolicies := tCtx.evalTraces(subAcc, subAccName, pubSubj, sub.client.acc.traces, tCtx.liftedTraces)
			if len(tracePolicies) > 0 {
				tCtx.Header, tCtx.Msg = condCaptureMsg(firstMsg, c.pa.hdr, msg)
				trc = &traceGroup{
					traces:   tracePolicies,
					traceMsg: TraceMsg{Acc: tCtx.msgAcc.Name, Reply: string(reply)},
				}
				if tCtx.importedStrms == nil {
					tCtx.importedStrms = make(map[*streamImport]*traceGroup)
				}
				tCtx.importedStrms[sub.im] = trc
				tCtx.hasTrace = true
			}
		}
	} else if c.pa.psi != nil {
		subTo = c.pa.psi.to
		subImp = c.pa.psi.from
		if c.pa.psi.tr != nil {
			subImp, subTo = c.pa.psi.tr.src, c.pa.psi.tr.dest
		}
		subAcc = acc //sub.client.acc
		subAccName = subAcc.Name
		// Determine if import tracing is needed
		trc, importFound = tCtx.exportedSrvcs[c.pa.psi]
		if !importFound {
			firstMsg, tracePolicies := tCtx.evalTraces(subAcc, subAccName, pubSubj, subAcc.traces, tCtx.liftedTraces)
			if len(tracePolicies) > 0 {
				tCtx.Header, tCtx.Msg = condCaptureMsg(firstMsg, c.pa.hdr, msg)
				trc = &traceGroup{
					traces:   tracePolicies,
					traceMsg: TraceMsg{Acc: tCtx.msgAcc.Name, Reply: string(reply)},
				}
				if tCtx.exportedSrvcs == nil {
					tCtx.exportedSrvcs = make(map[*serviceImport]*traceGroup)
				}
				tCtx.exportedSrvcs[c.pa.psi] = trc
				tCtx.hasTrace = true
			}
		}
	}
	var _ia [2]*TraceSubscription
	var tSubs []*TraceSubscription
	msgRef := _EMPTY_
	refServerId := _EMPTY_
	if tCtx.hasTrace {
		if tCtx.traceStart.IsZero() {
			tCtx.traceStart = time.Now().UTC()
		}
		switch {
		case sub.client.leaf != nil:
			refServerId = sub.client.leaf.remoteId
		case sub.client.route != nil:
			refServerId = sub.client.route.remoteID
		case sub.client.gw != nil:
			refServerId = sub.client.opts.Name
		}
		// JetStream means reference to self
		if (sub.client.kind == JETSTREAM || sub.client.kind == SYSTEM) && refServerId == _EMPTY_ {
			refServerId = c.srv.ID()
		}
		if refServerId != _EMPTY_ {
			var b [8]byte
			rn := rand.Int63()
			for i, l := 0, rn; i < len(b); i++ {
				b[i] = digits[l%base]
				l /= base
			}
			msgRef = string(b[:])
		}
		tSubs = _ia[:0]
	}
	if len(tCtx.global.traces) > 0 || (trc != nil && len(trc.traces) > 0) {
		trcSub := &TraceSubscription{
			Acc: subAccName,
			SubClient: &TraceClient{
				Type:     sub.client.clientTypeString(),
				Kind:     sub.client.kindString(),
				Name:     sub.client.GetName(),
				ClientId: sub.client.cid,
				ServerId: refServerId,
			},
			Queue:      string(sub.queue),
			Sub:        subscription,
			PubSubject: pubSubj,
			ImportTo:   subTo,
			ImportSub:  subImp,
			Ref:        msgRef,
			Reply:      string(reply),
		}
		tSubs = append(tSubs, trcSub)
		if len(tCtx.global.traces) > 0 {
			tCtx.global.traceMsg.Subscriptions = append(tCtx.global.traceMsg.Subscriptions, trcSub)
		}
		if trc != nil && len(trc.traces) > 0 {
			trc.traceMsg.Subscriptions = append(trc.traceMsg.Subscriptions, trcSub)
		}
		tCtx.hasTrace = true
	}
	if len(tCtx.pub.traces) > 0 || len(tCtx.hdr.traces) > 0 {
		var trcSub *TraceSubscription
		if tCtx.pubAcc == subAcc {
			trcSub = &TraceSubscription{
				Acc: subAccName,
				SubClient: &TraceClient{
					Type:     sub.client.clientTypeString(),
					Kind:     sub.client.kindString(),
					Name:     sub.client.GetName(),
					ClientId: sub.client.cid,
					ServerId: refServerId,
				},
				Queue:      string(sub.queue),
				Sub:        subscription,
				PubSubject: pubSubj,
				ImportTo:   subTo,
				ImportSub:  subImp,
				Ref:        msgRef,
				Reply:      string(reply),
			}
		} else {
			var tc *TraceClient
			if refServerId != _EMPTY_ {
				tc = &TraceClient{ServerId: refServerId}
			}
			trcSub = &TraceSubscription{
				Acc:        subAccName,
				ImportSub:  subImp,
				Sub:        subImp, // For easier handling, set subscription to what the import would be
				SubClient:  tc,
				Ref:        msgRef,
				PubSubject: pubSubj,
				Reply:      string(reply),
			}
		}
		tSubs = append(tSubs, trcSub)
		tCtx.pub.traceMsg.Subscriptions = append(tCtx.pub.traceMsg.Subscriptions, trcSub)
		tCtx.hasTrace = true
	}
	return msgRef, tSubs
}

// Gathers pub traces for the message. This call must be matched by a call to traceMsgCompletion.
// This call also checks and enforces tracing header. When an error happens, msgC may be altered and/or closed.
// returns (possibly modified) msg
func (tCtx *traceCtx) traceMsg(msg []byte, subj, reply string, msgC *client, msgCAcc *Account) []byte {
	var _ia1 [8]*selectedTrace
	trcHeader := traceHeader{Traces: _ia1[:0]}
	trcHdrSet := false
	trcSubj := _EMPTY_
	trcLvl := []string{}
	srv := msgC.srv
	if tCtx == nil {
		srv.Errorf("Trace context is required")
		return msg
	}
	// Check if tracing is enabled on the message and additional sanity checks
	if msgC.pa.hdr > 0 {
		if val := getHeader(TracePing, msg[:msgC.pa.hdr]); len(val) > 0 || isHeaderSet(TracePing, msg[:msgC.pa.hdr]) {
			trcSubj = reply
			trcHdrSet = true
			if m := strings.TrimSpace(string(val)); len(m) > 0 && len(trcSubj) > 0 {
				trcLvl = strings.Split(m, ",")
			}
		}
		// Read and Enforce (system) policy header rules
		if policies := getHeader(TraceHeader, msg[:msgC.pa.hdr]); len(policies) > 0 {
			// TODO if we where to allow clients to present the trace header, we can monitor responses across connections
			if msgC.kind == CLIENT {
				msgC.Errorf("Not allowed to set header: %s", TraceHeader)
				msgC.closeConnection(ProtocolViolation)
				return msg
			}
			if err := json.Unmarshal(policies, &trcHeader); err != nil {
				msgC.Errorf("Badly formatted header: %s", TraceHeader)
				msg = msgC.setHeader(TraceHeader, "", msg)
				trcSubj = _EMPTY_
			}
			//TODO only do this if the system account is not shared.
			if msgC.kind == LEAF && len(trcHeader.Traces) != 0 {
				msgC.Errorf("Leaf nodes only allowed to set header without traces: %s", TraceHeader)
				msgC.closeConnection(ProtocolViolation)
				return msg
			}
		}
	}
	tCtx.tracePub(msgC, msgCAcc, subj, reply, msg, trcHeader.Traces, trcHdrSet, trcSubj, trcHeader.Ref, trcLvl)
	return msg
}
