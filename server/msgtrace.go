// Copyright 2024-2026 The NATS Authors
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
	"math/rand/v2"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	MsgTraceDest          = "Nats-Trace-Dest"
	MsgTraceDestDisabled  = "trace disabled" // This must be an invalid NATS subject
	MsgTraceHop           = "Nats-Trace-Hop"
	MsgTraceOriginAccount = "Nats-Trace-Origin-Account"
	MsgTraceOnly          = "Nats-Trace-Only"

	// External trace header. Note that this header is normally in lower
	// case (https://www.w3.org/TR/trace-context/#header-name). Vendors
	// MUST expect the header in any case (upper, lower, mixed), and
	// SHOULD send the header name in lowercase. We used to change it
	// to lower case, but no longer do that in 2.14.
	traceParentHdr = "traceparent"
)

var (
	traceDestHdrAsBytes      = stringToBytes(MsgTraceDest)
	traceDestDisabledAsBytes = stringToBytes(MsgTraceDestDisabled)
	traceParentHdrAsBytes    = stringToBytes(traceParentHdr)
	crLFAsBytes              = stringToBytes(CR_LF)
	dashAsBytes              = stringToBytes("-")
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
	// We are not making this an http.Header so that header name case is preserved.
	Header  map[string][]string `json:"header,omitempty"`
	MsgSize int                 `json:"msgsize,omitempty"`
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

func (t MsgTraceBase) typ() MsgTraceType     { return t.Type }
func (MsgTraceIngress) new() MsgTrace        { return &MsgTraceIngress{} }
func (MsgTraceSubjectMapping) new() MsgTrace { return &MsgTraceSubjectMapping{} }
func (MsgTraceStreamExport) new() MsgTrace   { return &MsgTraceStreamExport{} }
func (MsgTraceServiceImport) new() MsgTrace  { return &MsgTraceServiceImport{} }
func (MsgTraceJetStream) new() MsgTrace      { return &MsgTraceJetStream{} }
func (MsgTraceEgress) new() MsgTrace         { return &MsgTraceEgress{} }

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
			return fmt.Errorf("unknown trace type %v", tt.Type)
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
	errMsgTraceFastProdNoStall = "Not delivered because fast producer not stalled and consumer is slow"
)

type msgTrace struct {
	kind  int
	ready int32
	srv   *Server
	acc   *Account
	dest  string
	event *MsgTraceEvent
	js    *MsgTraceJetStream
	siMu  sync.RWMutex
	si    []*msgTraceServiceImport
	rsi   *msgTraceRespServiceImport
	hop   string
	nhop  string
	tonly bool // Will only trace the message, not do delivery.
	ct    compressionType
}

type msgTraceServiceImport struct {
	si  *serviceImport
	acc *Account
}

type msgTraceRespServiceImport struct {
	hops     int
	received int
	tree     map[string]*msgTraceRespServiceImport
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

// Possibly initialize message tracing if appropriate headers are found in the
// given `hdr` byte slice.
// If there is an error and the header `MsgTraceOnly` is set to `true` (indicating
// that it should be a "trace only" message, without message delivery), then this
// function will return `true, nil`, so that the caller can skip message processing.
func (c *client) initMsgTrace(hdr []byte, ingressError error) (bool, *msgTrace) {
	// The code in the "if" statement is only running in test mode.
	if msgTraceRunInTests {
		// Check the type of client that tries to initialize a trace struct.
		if !(c.kind == CLIENT || c.kind == ROUTER || c.kind == GATEWAY || c.kind == LEAF) {
			panic(fmt.Sprintf("Unexpected client type %q trying to initialize msgTrace", c.kindString()))
		}
		// In some tests, we want to make a server behave like an old server
		// and so even if a trace header is received, we want the server to
		// simply ignore it.
		if msgTraceCheckSupport {
			if c.srv == nil || c.srv.getServerProto() < MsgTraceProto {
				return false, nil
			}
		}
	}
	// Get the headers if we find `Nats-Trace-Dest` or `traceparent` header.
	// For `traceparent`, `external` will be true indicating that we need
	// to get the destination and sampling from the account.
	headers, external := genHeaderMapIfTraceHeadersPresent(hdr)
	if len(headers) == 0 {
		return false, nil
	}
	// Little helper to give us the first value of a given header, or _EMPTY_
	// if key is not present.
	getHdrVal := func(key string) string {
		vv, ok := headers[key]
		if !ok {
			return _EMPTY_
		}
		return vv[0]
	}
	var (
		dest      string   // The destination the trace message should be sent to
		traceOnly bool     // True if this is "trace only" and no message delivery should occur
		hop       string   // The hop "id", taken from headers only when not from CLIENT
		kind      = c.kind // The type of connection this originates from
	)
	// Check for traceOnly only if not external.
	if !external {
		if to := getHdrVal(MsgTraceOnly); to != _EMPTY_ {
			tos := strings.ToLower(to)
			switch tos {
			case "1", "true", "on":
				traceOnly = true
			}
		}
		dest = getHdrVal(MsgTraceDest)
		// If no dest, bail out.
		if dest == _EMPTY_ {
			return traceOnly, nil
		}
	}
	// Now that we know if there is `traceOnly` set or not, if we were to fail
	// here by returning `nil` for `*msgTrace`, we will return `true, nil` to
	// indicate to the caller that it should skip message processing (so that
	// the failed trace message is not delivered to regular subscriptions).

	// First, disable tracing if the message contains the old MsgTraceOriginAccount header.
	if v := getHdrVal(MsgTraceOriginAccount); v != _EMPTY_ {
		c.Errorf("Message tracing disabled because of header %q with value=%q",
			MsgTraceOriginAccount, v)
		// Skip this if we are invoked with ingressError, or if we are going to skip
		// message processing anyway (if `traceOnly` is true).
		if ingressError == nil && !traceOnly {
			// Disable tracing by changing the `MsgTraceDest` header to `MsgTraceDestDisabled`.
			c.msgBuf = c.setHeader(MsgTraceDest, MsgTraceDestDisabled, c.msgBuf)
		}
		// If `traceOnly` is false, the message will be delivered and possibly routed,
		// but it will not be traced. If it is `true`, then there will be no tracing
		// and the message will not be processed (so no chance to be routed).
		return traceOnly, nil
	}

	// Get the account and server.
	c.mu.Lock()
	acc := c.acc
	srv := c.srv
	c.mu.Unlock()
	// There should always be a server object, and for CLIENT and LEAF, the
	// account should be available.
	if srv == nil || (acc == nil && (kind == CLIENT || kind == LEAF)) {
		// Make the caller skip message procesing if `traceOnly` is true.
		return traceOnly, nil
	}
	// For non CLIENT connection, get the account and hop header.
	if kind != CLIENT {
		hop = getHdrVal(MsgTraceHop)
		if kind != LEAF {
			// Lookup the account, if not present, bail out.
			if acci, ok := c.srv.accounts.Load(string(c.pa.account)); ok {
				acc = acci.(*Account)
			} else {
				c.Errorf("Account %q was not found, won't be able to trace events", c.pa.account)
				return traceOnly, nil
			}
		}
	}
	// If external, we need to have the account's trace destination set,
	// otherwise, we are not enabling tracing.
	if external {
		var sampling int
		dest, sampling = acc.getTraceDestAndSampling()
		if dest == _EMPTY_ {
			// No account destination, no tracing for external trace headers.
			// This is not an error, so return false so that we don't skip
			// message processing.
			return false, nil
		}
		// Check sampling, but only from origin server.
		if kind == CLIENT && !sample(sampling) {
			// Need to disable tracing so that if the message is routed, it won't
			// trigger a trace there. Skip this if we have an ingress error.
			if ingressError == nil {
				c.msgBuf = c.setHeader(MsgTraceDest, MsgTraceDestDisabled, c.msgBuf)
			}
			return false, nil
		}
	}
	// Check the destination to see if this is a valid publish subject.
	if !IsValidPublishSubject(dest) {
		c.Errorf("Destination %q is not valid, won't be able to trace events", dest)
		// Return `traceOnly` to cause the caller to skip message processing if
		// this was supposed to be a trace only message.
		return traceOnly, nil
	}
	// Now that we have a valid `dest`, make sure this connection is allowed
	// to publish to it.
	if !c.allowedToPublishOnMsgTraceDest(srv, acc, dest) {
		// Send the error back only if CLIENT or LEAF, otherwise just log.
		if kind == CLIENT || kind == LEAF {
			c.pubPermissionViolation(stringToBytes(dest))
		} else {
			c.Errorf("Publish Violation - Subject %q", dest)
		}
		// Return `true, nil` to force skipping of message processing.
		return true, nil
	}
	c.pa.trace = &msgTrace{
		kind: kind,
		srv:  c.srv,
		acc:  acc,
		dest: dest,
		ct:   getCompressionType(getHdrVal(acceptEncodingHeader)),
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
				Kind:    kind,
				CID:     c.cid,
				Name:    getConnName(c),
				Account: acc.GetName(),
				Subject: string(c.pa.subject),
			}),
		},
		tonly: traceOnly,
	}
	// If we are invoked with an ingress error, set it now.
	if ingressError != nil {
		c.pa.trace.event.Events[0].(*MsgTraceIngress).Error = ingressError.Error()
	}
	return false, c.pa.trace
}

func (c *client) allowedToPublishOnMsgTraceDest(s *Server, acc *Account, dest string) bool {
	td := stringToBytes(dest)
	if hasGWRoutedReplyPrefix(td) {
		return false
	}
	if bytes.HasPrefix(td, clientNRGPrefix) && acc != s.SystemAccount() {
		return false
	}
	allowed := true
	c.mu.Lock()
	if c.kind == LEAF {
		if c.isSpokeLeafNode() {
			allowed = c.leafReceiveAllowed(td)
		} else {
			allowed = c.leafSendAllowed(td)
		}
	} else if c.perms != nil && (c.perms.pub.allow != nil || c.perms.pub.deny != nil) && !c.pubAllowedFullCheck(dest, false, true) {
		allowed = false
	}
	c.mu.Unlock()
	return allowed
}

func sample(sampling int) bool {
	// Option parsing should ensure that sampling is [1..100], but consider
	// any value outside of this range to be 100%.
	if sampling <= 0 || sampling >= 100 {
		return true
	}
	return rand.Int32N(100) <= int32(sampling)
}

// This function will return the header as a map (instead of http.Header because
// we want to preserve the header names' case) and a boolean that indicates if
// the headers have been lifted due to the presence of the external trace header
// only.
// Note that because of the traceParentHdr, the search is done in a case
// insensitive way. We used to rewrite it in lower case but no longer do since v2.14.
func genHeaderMapIfTraceHeadersPresent(hdr []byte) (map[string][]string, bool) {

	var (
		_keys               = [64][]byte{}
		_vals               = [64][]byte{}
		m                   map[string][]string
		traceDestHdrFound   bool
		traceParentHdrFound bool
	)
	// Skip the hdrLine
	if !bytes.HasPrefix(hdr, stringToBytes(hdrLine)) {
		return nil, false
	}

	keys := _keys[:0]
	vals := _vals[:0]

	for i := len(hdrLine); i < len(hdr); {
		// Search for key/val delimiter
		del := bytes.IndexByte(hdr[i:], ':')
		if del < 0 {
			break
		}
		keyStart := i
		key := hdr[keyStart : keyStart+del]
		i += del + 1
		for i < len(hdr) && (hdr[i] == ' ' || hdr[i] == '\t') {
			i++
		}
		valStart := i
		nl := bytes.Index(hdr[valStart:], crLFAsBytes)
		if nl < 0 {
			break
		}
		valEnd := valStart + nl
		for valEnd > valStart && (hdr[valEnd-1] == ' ' || hdr[valEnd-1] == '\t') {
			valEnd--
		}
		val := hdr[valStart:valEnd]
		if len(key) > 0 && len(val) > 0 {
			vals = append(vals, val)

			// We search for our special keys only if not already found.

			// Check for the external trace header.
			// Search needs to be case insensitive.
			if !traceParentHdrFound && bytes.EqualFold(key, traceParentHdrAsBytes) {
				// We will now check if the value has sampling or not.
				// TODO(ik): Not sure if this header can have multiple values
				// or not, and if so, what would be the rule to check for
				// sampling. What is done here is to check them all until we
				// found one with sampling.
				tk := bytes.Split(val, dashAsBytes)
				if len(tk) == 4 && len([]byte(tk[3])) == 2 {
					if hexVal, err := strconv.ParseInt(bytesToString(tk[3]), 16, 8); err == nil {
						if hexVal&0x1 == 0x1 {
							traceParentHdrFound = true
						}
					}
				}
			} else if !traceDestHdrFound && bytes.Equal(key, traceDestHdrAsBytes) {
				// This is the Nats-Trace-Dest header, check the value to see
				// if it indicates that the trace was disabled.
				if bytes.Equal(val, traceDestDisabledAsBytes) {
					return nil, false
				}
				traceDestHdrFound = true
			}
			// Add to the keys and preserve the key's case
			keys = append(keys, key)
		}
		i += nl + 2
	}
	if !traceDestHdrFound && !traceParentHdrFound {
		return nil, false
	}
	m = make(map[string][]string, len(keys))
	for i, k := range keys {
		hname := string(k)
		m[hname] = append(m[hname], string(vals[i]))
	}
	return m, !traceDestHdrFound && traceParentHdrFound
}

// Special case where we create a trace event before parsing the message.
// This is for cases where the connection will be closed when detecting
// an error during early message processing (for instance max payload).
func (c *client) sendMsgTraceIngressErrEvent(hdr []byte, ingressError error) {
	if ingressError == nil {
		return
	}
	if _, t := c.initMsgTrace(hdr, ingressError); t != nil {
		t.sendEvent()
	}
}

// Returns `true` if message tracing is enabled and we are tracing only,
// that is, we are not going to deliver the inbound message, returns
// `false` otherwise (no tracing, or tracing and message delivery).
func (t *msgTrace) traceOnly() bool {
	return t != nil && t.tonly
}

func (t *msgTrace) setHopHeader(c *client, msg []byte) []byte {
	e := t.event
	e.Hops++
	if len(t.hop) > 0 {
		t.nhop = fmt.Sprintf("%s.%d", t.hop, e.Hops)
	} else {
		t.nhop = fmt.Sprintf("%d", e.Hops)
	}
	if t.kind == CLIENT && strings.HasPrefix(t.dest, replyPrefix) {
		t.siMu.Lock()
		if t.rsi == nil {
			t.rsi = &msgTraceRespServiceImport{}
		}
		t.rsi.hops++
		t.siMu.Unlock()
	}
	return c.setHeader(MsgTraceHop, t.nhop, msg)
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
	// Update the timestamp since this is more accurate than when it
	// was first added in addJetStreamEvent().
	t.js.Timestamp = time.Now()
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

func (t *msgTrace) setupResponseServiceImport(c *client, acc *Account, si *serviceImport, msg []byte) (*serviceImport, []byte) {
	rsi := si.acc.addRespServiceImport(acc, t.dest, si, false, nil, t)
	t.dest = rsi.from
	t.siMu.Lock()
	t.si = append(t.si, &msgTraceServiceImport{rsi, si.acc})
	t.siMu.Unlock()
	return rsi, c.setHeader(MsgTraceDest, t.dest, msg)
}

func (t *msgTrace) handleRespServiceImport(e *MsgTraceEvent) {
	t.siMu.Lock()
	defer t.siMu.Unlock()

	// If response service import were created and routed for message traces,
	// we should have t.rsi created. If it is not, we are done. Note that
	// if for any reason we bail out because we are not in a state that we
	// expect, the response service imports will be cleaned-up on a timer based.
	if t.rsi == nil {
		return
	}
	t.updateRespServiceImport(e)

	if t.allRespServiceImportReceived(t.rsi) {
		for _, rsi := range t.si {
			rsi.acc.removeRespServiceImport(rsi.si, rsiOk)
		}
		t.rsi, t.si = nil, nil
	}
}

// For a given trace message event, update the tree of response service import
// trace messages.
// Lock is held on entry.
func (t *msgTrace) updateRespServiceImport(e *MsgTraceEvent) {
	// Check for the hop header.
	hop, ok := e.Request.Header[MsgTraceHop]
	if !ok || len(hop) != 1 {
		return
	}
	hops := strings.Split(hop[0], ".")
	prsi := t.rsi
	for i, h := range hops {
		rsi := prsi.tree[h]
		if rsi == nil {
			if prsi.tree == nil {
				prsi.tree = make(map[string]*msgTraceRespServiceImport)
			}
			// Create an entry and initializes the hops count to -1. It will be set
			// when receiving the corresponding trace message.
			rsi = &msgTraceRespServiceImport{hops: -1}
			// Bind it to the parent tree for this "hop" id.
			prsi.tree[h] = rsi
		}
		// When dealing with the last section of the `hop` string, we set the
		// expected hops count based on the event's `Hops` field and bump the number
		// of received trace messages on the parent's node. We do this only once
		// per event (use rsi.hops == -1 as the indicator).
		if rsi.hops == -1 && i == len(hops)-1 {
			rsi.hops = e.Hops
			prsi.received++
		}
		prsi = rsi
	}
}

// Determine if all response service import traces have been received.
// Lock held on entry.
func (t *msgTrace) allRespServiceImportReceived(prsi *msgTraceRespServiceImport) bool {
	if prsi.hops != prsi.received {
		return false
	}
	for _, rsi := range prsi.tree {
		if !t.allRespServiceImportReceived(rsi) {
			return false
		}
	}
	return true
}
