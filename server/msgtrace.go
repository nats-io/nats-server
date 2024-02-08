// Copyright 2024 The NATS Authors
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
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"time"
)

const (
	MsgTraceSendTo        = "Nats-Trace-Dest"
	MsgTraceHop           = "Nats-Trace-Hop"
	MsgTraceOriginAccount = "Nats-Trace-Origin-Account"
	MsgTraceOnly          = "Nats-Trace-Only"
)

type MsgTraceType string

// Type of message trace events in the MsgTraceEvents list.
// This is needed to unmarshal the list.
const (
	MsgTraceIngressType        = "in"
	MsgTraceSubjectMappingType = "sm"
	MsgTraceStreamExportType   = "se"
	MsgTraceServiceImportType  = "si"
	MsgTraceJetStreamType      = "js"
	MsgTraceEgressType         = "eg"
)

type MsgTraceEvent struct {
	Server  ServerInfo      `json:"server"`
	Request MsgTraceRequest `json:"request"`
	Hops    int             `json:"hops,omitempty"`
	Events  MsgTraceEvents  `json:"events"`
}

type MsgTraceRequest struct {
	Header  http.Header `json:"header,omitempty"`
	MsgSize int         `json:"msgsize,omitempty"`
}

type MsgTraceEvents []MsgTrace

type MsgTrace interface {
	new() MsgTrace
	typ() MsgTraceType
}

type MsgTraceBase struct {
	Type      MsgTraceType `json:"type"`
	Timestamp time.Time    `json:"ts"`
}

type MsgTraceIngress struct {
	MsgTraceBase
	Kind    int    `json:"kind"`
	CID     uint64 `json:"cid"`
	Name    string `json:"name,omitempty"`
	Account string `json:"acc"`
	Subject string `json:"subj"`
	Error   string `json:"error,omitempty"`
}

type MsgTraceSubjectMapping struct {
	MsgTraceBase
	MappedTo string `json:"to"`
}

type MsgTraceStreamExport struct {
	MsgTraceBase
	Account string `json:"acc"`
	To      string `json:"to"`
}

type MsgTraceServiceImport struct {
	MsgTraceBase
	Account string `json:"acc"`
	From    string `json:"from"`
	To      string `json:"to"`
}

type MsgTraceJetStream struct {
	MsgTraceBase
	Stream     string `json:"stream"`
	Subject    string `json:"subject,omitempty"`
	NoInterest bool   `json:"nointerest,omitempty"`
	Error      string `json:"error,omitempty"`
}

type MsgTraceEgress struct {
	MsgTraceBase
	Kind         int    `json:"kind"`
	CID          uint64 `json:"cid"`
	Name         string `json:"name,omitempty"`
	Hop          string `json:"hop,omitempty"`
	Account      string `json:"acc,omitempty"`
	Subscription string `json:"sub,omitempty"`
	Queue        string `json:"queue,omitempty"`
	Error        string `json:"error,omitempty"`

	// This is for applications that unmarshal the trace events
	// and want to link an egress to route/leaf/gateway with
	// the MsgTraceEvent from that server.
	Link *MsgTraceEvent `json:"-"`
}

// -------------------------------------------------------------

func (t MsgTraceBase) typ() MsgTraceType       { return t.Type }
func (_ MsgTraceIngress) new() MsgTrace        { return &MsgTraceIngress{} }
func (_ MsgTraceSubjectMapping) new() MsgTrace { return &MsgTraceSubjectMapping{} }
func (_ MsgTraceStreamExport) new() MsgTrace   { return &MsgTraceStreamExport{} }
func (_ MsgTraceServiceImport) new() MsgTrace  { return &MsgTraceServiceImport{} }
func (_ MsgTraceJetStream) new() MsgTrace      { return &MsgTraceJetStream{} }
func (_ MsgTraceEgress) new() MsgTrace         { return &MsgTraceEgress{} }

var msgTraceInterfaces = map[MsgTraceType]MsgTrace{
	MsgTraceIngressType:        MsgTraceIngress{},
	MsgTraceSubjectMappingType: MsgTraceSubjectMapping{},
	MsgTraceStreamExportType:   MsgTraceStreamExport{},
	MsgTraceServiceImportType:  MsgTraceServiceImport{},
	MsgTraceJetStreamType:      MsgTraceJetStream{},
	MsgTraceEgressType:         MsgTraceEgress{},
}

func (t *MsgTraceEvents) UnmarshalJSON(data []byte) error {
	var raw []json.RawMessage
	err := json.Unmarshal(data, &raw)
	if err != nil {
		return err
	}
	*t = make(MsgTraceEvents, len(raw))
	var tt MsgTraceBase
	for i, r := range raw {
		if err = json.Unmarshal(r, &tt); err != nil {
			return err
		}
		tr, ok := msgTraceInterfaces[tt.Type]
		if !ok {
			return fmt.Errorf("Unknown trace type %v", tt.Type)
		}
		te := tr.new()
		if err := json.Unmarshal(r, te); err != nil {
			return err
		}
		(*t)[i] = te
	}
	return nil
}

func getTraceAs[T MsgTrace](e any) *T {
	v, ok := e.(*T)
	if ok {
		return v
	}
	return nil
}

func (t *MsgTraceEvent) Ingress() *MsgTraceIngress {
	if len(t.Events) < 1 {
		return nil
	}
	return getTraceAs[MsgTraceIngress](t.Events[0])
}

func (t *MsgTraceEvent) SubjectMapping() *MsgTraceSubjectMapping {
	for _, e := range t.Events {
		if e.typ() == MsgTraceSubjectMappingType {
			return getTraceAs[MsgTraceSubjectMapping](e)
		}
	}
	return nil
}

func (t *MsgTraceEvent) StreamExports() []*MsgTraceStreamExport {
	var se []*MsgTraceStreamExport
	for _, e := range t.Events {
		if e.typ() == MsgTraceStreamExportType {
			se = append(se, getTraceAs[MsgTraceStreamExport](e))
		}
	}
	return se
}

func (t *MsgTraceEvent) ServiceImports() []*MsgTraceServiceImport {
	var si []*MsgTraceServiceImport
	for _, e := range t.Events {
		if e.typ() == MsgTraceServiceImportType {
			si = append(si, getTraceAs[MsgTraceServiceImport](e))
		}
	}
	return si
}

func (t *MsgTraceEvent) JetStream() *MsgTraceJetStream {
	for _, e := range t.Events {
		if e.typ() == MsgTraceJetStreamType {
			return getTraceAs[MsgTraceJetStream](e)
		}
	}
	return nil
}

func (t *MsgTraceEvent) Egresses() []*MsgTraceEgress {
	var eg []*MsgTraceEgress
	for _, e := range t.Events {
		if e.typ() == MsgTraceEgressType {
			eg = append(eg, getTraceAs[MsgTraceEgress](e))
		}
	}
	return eg
}

const (
	errMsgTraceOnlyNoSupport   = "Not delivered because remote does not support message tracing"
	errMsgTraceNoSupport       = "Message delivered but remote does not support message tracing so no trace event generated from there"
	errMsgTraceNoEcho          = "Not delivered because of no echo"
	errMsgTracePubViolation    = "Not delivered because publish denied for this subject"
	errMsgTraceSubDeny         = "Not delivered because subscription denies this subject"
	errMsgTraceSubClosed       = "Not delivered because subscription is closed"
	errMsgTraceClientClosed    = "Not delivered because client is closed"
	errMsgTraceAutoSubExceeded = "Not delivered because auto-unsubscribe exceeded"
)

type msgTrace struct {
	ready int32
	srv   *Server
	acc   *Account
	// Origin account name, set only if acc is nil when acc lookup failed.
	oan   string
	dest  string
	event *MsgTraceEvent
	js    *MsgTraceJetStream
	hop   string
	nhop  string
	tonly bool // Will only trace the message, not do delivery.
	ct    compressionType
}

// This will be false outside of the tests, so when building the server binary,
// any code where you see `if msgTraceRunInTests` statement will be compiled
// out, so this will have no performance penalty.
var (
	msgTraceRunInTests   bool
	msgTraceCheckSupport bool
)

// Returns the message trace object, if message is being traced,
// and `true` if we want to only trace, not actually deliver the message.
func (c *client) isMsgTraceEnabled() (*msgTrace, bool) {
	t := c.pa.trace
	if t == nil {
		return nil, false
	}
	return t, t.tonly
}

// For LEAF/ROUTER/GATEWAY, return false if the remote does not support
// message tracing (important if the tracing requests trace-only).
func (c *client) msgTraceSupport() bool {
	// Exclude client connection from the protocol check.
	return c.kind == CLIENT || c.opts.Protocol >= MsgTraceProto
}

func getConnName(c *client) string {
	switch c.kind {
	case ROUTER:
		if n := c.route.remoteName; n != _EMPTY_ {
			return n
		}
	case GATEWAY:
		if n := c.gw.remoteName; n != _EMPTY_ {
			return n
		}
	case LEAF:
		if n := c.leaf.remoteServer; n != _EMPTY_ {
			return n
		}
	}
	return c.opts.Name
}

func getCompressionType(cts string) compressionType {
	if cts == _EMPTY_ {
		return noCompression
	}
	cts = strings.ToLower(cts)
	if strings.Contains(cts, "snappy") || strings.Contains(cts, "s2") {
		return snappyCompression
	}
	if strings.Contains(cts, "gzip") {
		return gzipCompression
	}
	return unsupportedCompression
}

func (c *client) initMsgTrace() *msgTrace {
	// The code in the "if" statement is only running in test mode.
	if msgTraceRunInTests {
		// Check the type of client that tries to initialize a trace struct.
		if !(c.kind == CLIENT || c.kind == ROUTER || c.kind == GATEWAY || c.kind == LEAF) {
			panic(fmt.Errorf("Unexpected client type %q trying to initialize msgTrace", c.kindString()))
		}
		// In some tests, we want to make a server behave like an old server
		// and so even if a trace header is received, we want the server to
		// simply ignore it.
		if msgTraceCheckSupport {
			if c.srv == nil || c.srv.getServerProto() < MsgTraceProto {
				return nil
			}
		}
	}
	if c.pa.hdr <= 0 {
		return nil
	}
	hdr := c.msgBuf[:c.pa.hdr]
	// Do not call c.parseState.getHeader() yet for performance reasons.
	// We first do a "manual" search of the "send-to" destination's header.
	// If not present, no need to lift the message headers.
	td := getHeader(MsgTraceSendTo, hdr)
	if len(td) <= 0 {
		return nil
	}
	// Now we know that this is a message that requested tracing, we
	// will lift the headers since we also need to transfer them to
	// the produced trace message.
	headers := c.parseState.getHeader()
	if headers == nil {
		return nil
	}
	ct := getCompressionType(headers.Get(acceptEncodingHeader))
	var traceOnly bool
	if to := headers.Get(MsgTraceOnly); to != _EMPTY_ {
		tos := strings.ToLower(to)
		switch tos {
		case "1", "true", "on":
			traceOnly = true
		}
	}
	var (
		// Account to use when sending the trace event
		acc *Account
		// Ingress' account name
		ian string
		// Origin account name
		oan string
		// The hop "id", taken from headers only when not from CLIENT
		hop string
	)
	if c.kind == ROUTER || c.kind == GATEWAY || c.kind == LEAF {
		// The ingress account name will always be c.pa.account, but `acc` may
		// be different if we have an origin account header.
		if c.kind == LEAF {
			ian = c.acc.GetName()
		} else {
			ian = string(c.pa.account)
		}
		// The remote will have set the origin account header only if the
		// message changed account (think of service imports).
		oan = headers.Get(MsgTraceOriginAccount)
		if oan == _EMPTY_ {
			// For LEAF or ROUTER with pinned-account, we can use the c.acc.
			if c.kind == LEAF || (c.kind == ROUTER && len(c.route.accName) > 0) {
				acc = c.acc
			} else {
				// We will lookup account with c.pa.account (or ian).
				oan = ian
			}
		}
		// Unless we already got the account, we need to look it up.
		if acc == nil {
			// We don't want to do account resolving here, and we have to return
			// a msgTrace object because if we don't and if the user wanted to do
			// trace-only, the message would end-up being delivered.
			if acci, ok := c.srv.accounts.Load(oan); ok {
				acc = acci.(*Account)
				// Since we have looked-up the account, we don't need oan, so
				// clear it in case it was set.
				oan = _EMPTY_
			} else {
				c.Errorf("Account %q was not found, won't be able to trace events", oan)
			}
		}
		// Check the hop header
		hop = headers.Get(MsgTraceHop)
	} else {
		acc = c.acc
		ian = acc.GetName()
	}
	c.pa.trace = &msgTrace{
		srv:  c.srv,
		acc:  acc,
		oan:  oan,
		dest: string(td),
		ct:   ct,
		hop:  hop,
		event: &MsgTraceEvent{
			Request: MsgTraceRequest{
				Header:  headers,
				MsgSize: c.pa.size,
			},
			Events: append(MsgTraceEvents(nil), &MsgTraceIngress{
				MsgTraceBase: MsgTraceBase{
					Type:      MsgTraceIngressType,
					Timestamp: time.Now(),
				},
				Kind:    c.kind,
				CID:     c.cid,
				Name:    getConnName(c),
				Account: ian,
				Subject: string(c.pa.subject),
			}),
		},
		tonly: traceOnly,
	}
	return c.pa.trace
}

// Special case where we create a trace event before parsing the message.
// This is for cases where the connection will be closed when detecting
// an error during early message processing (for instance max payload).
func (c *client) initAndSendIngressErrEvent(hdr []byte, dest string, ingressError error) {
	if ingressError == nil {
		return
	}
	ct := getAcceptEncoding(hdr)
	t := &msgTrace{
		srv:  c.srv,
		acc:  c.acc,
		dest: dest,
		ct:   ct,
		event: &MsgTraceEvent{
			Request: MsgTraceRequest{MsgSize: c.pa.size},
			Events: append(MsgTraceEvents(nil), &MsgTraceIngress{
				MsgTraceBase: MsgTraceBase{
					Type:      MsgTraceIngressType,
					Timestamp: time.Now(),
				},
				Kind:  c.kind,
				CID:   c.cid,
				Name:  getConnName(c),
				Error: ingressError.Error(),
			}),
		},
	}
	t.sendEvent()
}

// Returns `true` if message tracing is enabled and we are tracing only,
// that is, we are not going to deliver the inbound message, returns
// `false` otherwise (no tracing, or tracing and message delivery).
func (t *msgTrace) traceOnly() bool {
	return t != nil && t.tonly
}

func (t *msgTrace) setOriginAccountHeaderIfNeeded(c *client, acc *Account, msg []byte) []byte {
	var oan string
	// If t.acc is set, only check that, not t.oan.
	if t.acc != nil {
		if t.acc != acc {
			oan = t.acc.GetName()
		}
	} else if t.oan != acc.GetName() {
		oan = t.oan
	}
	if oan != _EMPTY_ {
		msg = c.setHeader(MsgTraceOriginAccount, oan, msg)
	}
	return msg
}

func (t *msgTrace) setHopHeader(c *client, msg []byte) []byte {
	e := t.event
	e.Hops++
	if len(t.hop) > 0 {
		t.nhop = fmt.Sprintf("%s.%d", t.hop, e.Hops)
	} else {
		t.nhop = fmt.Sprintf("%d", e.Hops)
	}
	return c.setHeader(MsgTraceHop, t.nhop, msg)
}

// Will look for the MsgTraceSendTo header and change the first character
// to an 'X' so that if this message is sent to a remote, the remote will
// not initialize tracing since it won't find the actual MsgTraceSendTo
// header. The function returns the position of the header so it can
// efficiently be re-enabled by calling enableTraceHeader.
// Note that if `msg` can be either the header alone or the full message
// (header and payload). This function will use c.pa.hdr to limit the
// search to the header section alone.
func (t *msgTrace) disableTraceHeader(c *client, msg []byte) int {
	// Code largely copied from getHeader(), except that we don't need the value
	if c.pa.hdr <= 0 {
		return -1
	}
	hdr := msg[:c.pa.hdr]
	key := stringToBytes(MsgTraceSendTo)
	pos := bytes.Index(hdr, key)
	if pos < 0 {
		return -1
	}
	// Make sure this key does not have additional prefix.
	if pos < 2 || hdr[pos-1] != '\n' || hdr[pos-2] != '\r' {
		return -1
	}
	index := pos + len(key)
	if index >= len(hdr) {
		return -1
	}
	if hdr[index] != ':' {
		return -1
	}
	// Disable the trace by altering the first character of the header
	hdr[pos] = 'X'
	// Return the position of that character so we can re-enable it.
	return pos
}

// Changes back the character at the given position `pos` in the `msg`
// byte slice to the first character of the MsgTraceSendTo header.
func (t *msgTrace) enableTraceHeader(c *client, msg []byte, pos int) {
	if pos <= 0 {
		return
	}
	msg[pos] = MsgTraceSendTo[0]
}

func (t *msgTrace) setIngressError(err string) {
	if i := t.event.Ingress(); i != nil {
		i.Error = err
	}
}

func (t *msgTrace) addSubjectMappingEvent(subj []byte) {
	if t == nil {
		return
	}
	t.event.Events = append(t.event.Events, &MsgTraceSubjectMapping{
		MsgTraceBase: MsgTraceBase{
			Type:      MsgTraceSubjectMappingType,
			Timestamp: time.Now(),
		},
		MappedTo: string(subj),
	})
}

func (t *msgTrace) addEgressEvent(dc *client, sub *subscription, err string) {
	if t == nil {
		return
	}
	e := &MsgTraceEgress{
		MsgTraceBase: MsgTraceBase{
			Type:      MsgTraceEgressType,
			Timestamp: time.Now(),
		},
		Kind:  dc.kind,
		CID:   dc.cid,
		Name:  getConnName(dc),
		Hop:   t.nhop,
		Error: err,
	}
	t.nhop = _EMPTY_
	// Specific to CLIENT connections...
	if dc.kind == CLIENT {
		// Set the subscription's subject and possibly queue name.
		e.Subscription = string(sub.subject)
		if len(sub.queue) > 0 {
			e.Queue = string(sub.queue)
		}
	}
	if dc.kind == CLIENT || dc.kind == LEAF {
		if i := t.event.Ingress(); i != nil {
			// If the Ingress' account is different from the destination's
			// account, add the account name into the Egress trace event.
			// This would happen with service imports.
			if dcAccName := dc.acc.GetName(); dcAccName != i.Account {
				e.Account = dcAccName
			}
		}
	}
	t.event.Events = append(t.event.Events, e)
}

func (t *msgTrace) addStreamExportEvent(dc *client, to []byte) {
	if t == nil {
		return
	}
	dc.mu.Lock()
	accName := dc.acc.GetName()
	dc.mu.Unlock()
	t.event.Events = append(t.event.Events, &MsgTraceStreamExport{
		MsgTraceBase: MsgTraceBase{
			Type:      MsgTraceStreamExportType,
			Timestamp: time.Now(),
		},
		Account: accName,
		To:      string(to),
	})
}

func (t *msgTrace) addServiceImportEvent(accName, from, to string) {
	if t == nil {
		return
	}
	t.event.Events = append(t.event.Events, &MsgTraceServiceImport{
		MsgTraceBase: MsgTraceBase{
			Type:      MsgTraceServiceImportType,
			Timestamp: time.Now(),
		},
		Account: accName,
		From:    from,
		To:      to,
	})
}

func (t *msgTrace) addJetStreamEvent(streamName string) {
	if t == nil {
		return
	}
	t.js = &MsgTraceJetStream{
		MsgTraceBase: MsgTraceBase{
			Type:      MsgTraceJetStreamType,
			Timestamp: time.Now(),
		},
		Stream: streamName,
	}
	t.event.Events = append(t.event.Events, t.js)
}

func (t *msgTrace) updateJetStreamEvent(subject string, noInterest bool) {
	if t == nil {
		return
	}
	// JetStream event should have been created in addJetStreamEvent
	if t.js == nil {
		return
	}
	t.js.Subject = subject
	t.js.NoInterest = noInterest
}

func (t *msgTrace) sendEventFromJetStream(err error) {
	if t == nil {
		return
	}
	// JetStream event should have been created in addJetStreamEvent
	if t.js == nil {
		return
	}
	if err != nil {
		t.js.Error = err.Error()
	}
	t.sendEvent()
}

func (t *msgTrace) sendEvent() {
	if t == nil {
		return
	}
	if t.js != nil {
		ready := atomic.AddInt32(&t.ready, 1) == 2
		if !ready {
			return
		}
	}
	t.srv.sendInternalAccountSysMsg(t.acc, t.dest, &t.event.Server, t.event, t.ct)
}
