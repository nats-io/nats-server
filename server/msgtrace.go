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
	"math/rand"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

const (
	MsgTraceDest          = "Nats-Trace-Dest"
	MsgTraceHop           = "Nats-Trace-Hop"
	MsgTraceOriginAccount = "Nats-Trace-Origin-Account"
	MsgTraceOnly          = "Nats-Trace-Only"

	// External trace header. Note that this header is normally in lower
	// case (https://www.w3.org/TR/trace-context/#header-name). Vendors
	// MUST expect the header in any case (upper, lower, mixed), and
	// SHOULD send the header name in lowercase.
	traceParentHdr = "traceparent"
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
			panic(fmt.Sprintf("Unexpected client type %q trying to initialize msgTrace", c.kindString()))
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
	headers, external := genHeaderMapIfTraceHeadersPresent(hdr)
	if len(headers) == 0 {
		return nil
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
	ct := getCompressionType(getHdrVal(acceptEncodingHeader))
	var (
		dest      string
		traceOnly bool
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
		// Check the destination to see if this is a valid public subject.
		if !IsValidPublishSubject(dest) {
			// We still have to return a msgTrace object (if traceOnly is set)
			// because if we don't, the message will end-up being delivered to
			// applications, which may break them. We report the error in any case.
			c.Errorf("Destination %q is not valid, won't be able to trace events", dest)
			if !traceOnly {
				// We can bail, tracing will be disabled for this message.
				return nil
			}
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
		oan = getHdrVal(MsgTraceOriginAccount)
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
			// We don't want to do account resolving here.
			if acci, ok := c.srv.accounts.Load(oan); ok {
				acc = acci.(*Account)
				// Since we have looked-up the account, we don't need oan, so
				// clear it in case it was set.
				oan = _EMPTY_
			} else {
				// We still have to return a msgTrace object (if traceOnly is set)
				// because if we don't, the message will end-up being delivered to
				// applications, which may break them. We report the error in any case.
				c.Errorf("Account %q was not found, won't be able to trace events", oan)
				if !traceOnly {
					// We can bail, tracing will be disabled for this message.
					return nil
				}
			}
		}
		// Check the hop header
		hop = getHdrVal(MsgTraceHop)
	} else {
		acc = c.acc
		ian = acc.GetName()
	}
	// If external, we need to have the account's trace destination set,
	// otherwise, we are not enabling tracing.
	if external {
		var sampling int
		if acc != nil {
			dest, sampling = acc.getTraceDestAndSampling()
		}
		if dest == _EMPTY_ {
			// No account destination, no tracing for external trace headers.
			return nil
		}
		// Check sampling, but only from origin server.
		if c.kind == CLIENT && !sample(sampling) {
			// Need to desactivate the traceParentHdr so that if the message
			// is routed, it does possibly trigger a trace there.
			disableTraceHeaders(c, hdr)
			return nil
		}
	}
	c.pa.trace = &msgTrace{
		srv:  c.srv,
		acc:  acc,
		oan:  oan,
		dest: dest,
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

func sample(sampling int) bool {
	// Option parsing should ensure that sampling is [1..100], but consider
	// any value outside of this range to be 100%.
	if sampling <= 0 || sampling >= 100 {
		return true
	}
	return rand.Int31n(100) <= int32(sampling)
}

// This function will return the header as a map (instead of http.Header because
// we want to preserve the header names' case) and a boolean that indicates if
// the headers have been lifted due to the presence of the external trace header
// only.
// Note that because of the traceParentHdr, the search is done in a case
// insensitive way, but if the header is found, it is rewritten in lower case
// as suggested by the spec, but also to make it easier to disable the header
// when needed.
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

	traceDestHdrAsBytes := stringToBytes(MsgTraceDest)
	traceParentHdrAsBytes := stringToBytes(traceParentHdr)
	crLFAsBytes := stringToBytes(CR_LF)
	dashAsBytes := stringToBytes("-")

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
		valStart := i
		nl := bytes.Index(hdr[valStart:], crLFAsBytes)
		if nl < 0 {
			break
		}
		if len(key) > 0 {
			val := bytes.Trim(hdr[valStart:valStart+nl], " \t")
			vals = append(vals, val)

			// Check for the external trace header.
			if bytes.EqualFold(key, traceParentHdrAsBytes) {
				// Rewrite the header using lower case if needed.
				if !bytes.Equal(key, traceParentHdrAsBytes) {
					copy(hdr[keyStart:], traceParentHdrAsBytes)
				}
				// We will now check if the value has sampling or not.
				// TODO(ik): Not sure if this header can have multiple values
				// or not, and if so, what would be the rule to check for
				// sampling. What is done here is to check them all until we
				// found one with sampling.
				if !traceParentHdrFound {
					tk := bytes.Split(val, dashAsBytes)
					if len(tk) == 4 && len([]byte(tk[3])) == 2 {
						if hexVal, err := strconv.ParseInt(bytesToString(tk[3]), 16, 8); err == nil {
							if hexVal&0x1 == 0x1 {
								traceParentHdrFound = true
							}
						}
					}
				}
				// Add to the keys with the external trace header in lower case.
				keys = append(keys, traceParentHdrAsBytes)
			} else {
				// Is the key the Nats-Trace-Dest header?
				if bytes.EqualFold(key, traceDestHdrAsBytes) {
					traceDestHdrFound = true
				}
				// Add to the keys and preserve the key's case
				keys = append(keys, key)
			}
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

// Will look for the MsgTraceSendTo and traceParentHdr headers and change the first
// character to an 'X' so that if this message is sent to a remote, the remote
// will not initialize tracing since it won't find the actual trace headers.
// The function returns the position of the headers so it can efficiently be
// re-enabled by calling enableTraceHeaders.
// Note that if `msg` can be either the header alone or the full message
// (header and payload). This function will use c.pa.hdr to limit the
// search to the header section alone.
func disableTraceHeaders(c *client, msg []byte) []int {
	// Code largely copied from getHeader(), except that we don't need the value
	if c.pa.hdr <= 0 {
		return []int{-1, -1}
	}
	hdr := msg[:c.pa.hdr]
	headers := [2]string{MsgTraceDest, traceParentHdr}
	positions := [2]int{-1, -1}
	for i := 0; i < 2; i++ {
		key := stringToBytes(headers[i])
		pos := bytes.Index(hdr, key)
		if pos < 0 {
			continue
		}
		// Make sure this key does not have additional prefix.
		if pos < 2 || hdr[pos-1] != '\n' || hdr[pos-2] != '\r' {
			continue
		}
		index := pos + len(key)
		if index >= len(hdr) {
			continue
		}
		if hdr[index] != ':' {
			continue
		}
		// Disable the trace by altering the first character of the header
		hdr[pos] = 'X'
		positions[i] = pos
	}
	// Return the positions of those characters so we can re-enable the headers.
	return positions[:2]
}

// Changes back the character at the given position `pos` in the `msg`
// byte slice to the first character of the MsgTraceSendTo header.
func enableTraceHeaders(msg []byte, positions []int) {
	firstChar := [2]byte{MsgTraceDest[0], traceParentHdr[0]}
	for i, pos := range positions {
		if pos == -1 {
			continue
		}
		msg[pos] = firstChar[i]
	}
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
