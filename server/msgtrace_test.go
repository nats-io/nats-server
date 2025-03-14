// Copyright 2024-2025 The NATS Authors
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

//go:build !skip_msgtrace_tests

package server

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/klauspost/compress/s2"
	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
)

func init() {
	msgTraceRunInTests = true
}

func TestMsgTraceConnName(t *testing.T) {
	c := &client{kind: ROUTER, route: &route{remoteName: "somename"}}
	c.opts.Name = "someid"

	// If route.remoteName is set, it will take precedence.
	val := getConnName(c)
	require_Equal[string](t, val, "somename")
	// When not set, we revert to c.opts.Name
	c.route.remoteName = _EMPTY_
	val = getConnName(c)
	require_Equal[string](t, val, "someid")

	// Now same for GW.
	c.route = nil
	c.gw = &gateway{remoteName: "somename"}
	c.kind = GATEWAY
	val = getConnName(c)
	require_Equal[string](t, val, "somename")
	// Revert to c.opts.Name
	c.gw.remoteName = _EMPTY_
	val = getConnName(c)
	require_Equal[string](t, val, "someid")

	// For LeafNode now
	c.gw = nil
	c.leaf = &leaf{remoteServer: "somename"}
	c.kind = LEAF
	val = getConnName(c)
	require_Equal[string](t, val, "somename")
	// But if not set...
	c.leaf.remoteServer = _EMPTY_
	val = getConnName(c)
	require_Equal[string](t, val, "someid")

	c.leaf = nil
	c.kind = CLIENT
	val = getConnName(c)
	require_Equal[string](t, val, "someid")
}

func TestMsgTraceGenHeaderMap(t *testing.T) {
	for _, test := range []struct {
		name     string
		header   []byte
		expected map[string][]string
		external bool
	}{
		{"missing header line", []byte("Nats-Trace-Dest: val\r\n"), nil, false},
		{"no trace header present", []byte(hdrLine + "Header1: val1\r\nHeader2: val2\r\n"), nil, false},
		{"trace header with some prefix", []byte(hdrLine + "Some-Prefix-" + MsgTraceDest + ": some value\r\n"), nil, false},
		{"trace header with some suffix", []byte(hdrLine + MsgTraceDest + "-Some-Suffix: some value\r\n"), nil, false},
		{"trace header with space before colon", []byte(hdrLine + MsgTraceDest + " : some value\r\n"), nil, false},
		{"trace header with missing cr_lf for value", []byte(hdrLine + MsgTraceDest + " : bogus"), nil, false},
		{"external trace header with some prefix", []byte(hdrLine + "Some-Prefix-" + traceParentHdr + ": 00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01\r\n"), nil, false},
		{"external trace header with some suffix", []byte(hdrLine + traceParentHdr + "-Some-Suffix: 00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01\r\n"), nil, false},
		{"external header with space before colon", []byte(hdrLine + traceParentHdr + " : 00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01\r\n"), nil, false},
		{"external header with missing cr_lf for value", []byte(hdrLine + traceParentHdr + " : 00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"), nil, false},
		{"trace header first", []byte(hdrLine + MsgTraceDest + ": some.dest\r\nSome-Header: some value\r\n"),
			map[string][]string{"Some-Header": {"some value"}, MsgTraceDest: {"some.dest"}}, false},
		{"trace header last", []byte(hdrLine + "Some-Header: some value\r\n" + MsgTraceDest + ": some.dest\r\n"),
			map[string][]string{"Some-Header": {"some value"}, MsgTraceDest: {"some.dest"}}, false},
		{"trace header multiple values", []byte(hdrLine + MsgTraceDest + ": some.dest\r\nSome-Header: some value\r\n" + MsgTraceDest + ": some.dest.2"),
			map[string][]string{"Some-Header": {"some value"}, MsgTraceDest: {"some.dest", "some.dest.2"}}, false},
		{"trace header and some empty key", []byte(hdrLine + MsgTraceDest + ": some.dest\r\n: bogus\r\nSome-Header: some value\r\n"),
			map[string][]string{"Some-Header": {"some value"}, MsgTraceDest: {"some.dest"}}, false},
		{"trace header and some header missing cr_lf for value", []byte(hdrLine + MsgTraceDest + ": some.dest\r\nSome-Header: bogus"),
			map[string][]string{MsgTraceDest: {"some.dest"}}, false},
		{"trace header and external after", []byte(hdrLine + MsgTraceDest + ": some.dest\r\n" + traceParentHdr + ": 00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01\r\nSome-Header: some value\r\n"),
			map[string][]string{"Some-Header": {"some value"}, MsgTraceDest: {"some.dest"}, traceParentHdr: {"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"}}, false},
		{"trace header and external before", []byte(hdrLine + traceParentHdr + ": 00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01\r\n" + MsgTraceDest + ": some.dest\r\nSome-Header: some value\r\n"),
			map[string][]string{"Some-Header": {"some value"}, MsgTraceDest: {"some.dest"}, traceParentHdr: {"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"}}, false},
		{"external malformed", []byte(hdrLine + traceParentHdr + ": 00-4bf92f3577b34da6a3ce929d0e0e4736-01\r\n"), nil, false},
		{"external first and sampling", []byte(hdrLine + traceParentHdr + ": 00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01\r\nSome-Header: some value\r\n"),
			map[string][]string{"Some-Header": {"some value"}, traceParentHdr: {"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"}}, true},
		{"external middle and sampling", []byte(hdrLine + "Some-Header: some value1\r\n" + traceParentHdr + ": 00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01\r\nSome-Header: some value2\r\n"),
			map[string][]string{"Some-Header": {"some value1", "some value2"}, traceParentHdr: {"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"}}, true},
		{"external last and sampling", []byte(hdrLine + "Some-Header: some value\r\n" + traceParentHdr + ": 00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01\r\n"),
			map[string][]string{"Some-Header": {"some value"}, traceParentHdr: {"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"}}, true},
		{"external sampling with not just 01", []byte(hdrLine + traceParentHdr + ": 00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-27\r\nSome-Header: some value\r\n"),
			map[string][]string{"Some-Header": {"some value"}, traceParentHdr: {"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-27"}}, true},
		{"external with different case and sampling", []byte(hdrLine + "TrAcEpArEnT: 00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01\r\nSome-Header: some value\r\n"),
			map[string][]string{"Some-Header": {"some value"}, traceParentHdr: {"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"}}, true},
		{"external first and not sampling", []byte(hdrLine + traceParentHdr + ": 00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00\r\nSome-Header: some value\r\n"), nil, false},
		{"external middle and not sampling", []byte(hdrLine + "Some-Header: some value1\r\n" + traceParentHdr + ": 00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00\r\nSome-Header: some value2\r\n"), nil, false},
		{"external last and not sampling", []byte(hdrLine + "Some-Header: some value\r\n" + traceParentHdr + ": 00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00\r\n"), nil, false},
		{"external not sampling with not just 00", []byte(hdrLine + traceParentHdr + ": 00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-22\r\nSome-Header: some value\r\n"), nil, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			m, ext := genHeaderMapIfTraceHeadersPresent(test.header)
			if test.external != ext {
				t.Fatalf("Expected external to be %v, got %v", test.external, ext)
			}
			if len(test.expected) != len(m) {
				t.Fatalf("Expected map to be of size %v, got %v", len(test.expected), len(m))
			}
			// If external, we should find traceParentHdr
			if test.external {
				if _, ok := m[traceParentHdr]; !ok {
					t.Fatalf("Expected traceparent header to be present, it was not: %+v", m)
				}
				// Header should have been rewritten, so we should find it in original header.
				if !bytes.Contains(test.header, []byte(traceParentHdr)) {
					t.Fatalf("Header should have been rewritten to have the traceparent in lower case: %s", test.header)
				}
			}
			for k, vv := range m {
				evv, ok := test.expected[k]
				if !ok {
					t.Fatalf("Did not find header %q in resulting map: %+v", k, m)
				}
				for i, v := range vv {
					if evv[i] != v {
						t.Fatalf("Expected value %v of key %q to be %q, got %q", i, k, evv[i], v)
					}
				}
			}
		})
	}
}

func TestMsgTraceBasic(t *testing.T) {
	conf := createConfFile(t, []byte(`
		listen: 127.0.0.1:-1
		mappings = {
			foo: bar
		}
	`))
	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	nc := natsConnect(t, s.ClientURL())
	defer nc.Close()
	cid, err := nc.GetClientID()
	require_NoError(t, err)

	traceSub := natsSubSync(t, nc, "my.trace.subj")
	natsFlush(t, nc)

	// Send trace message to a dummy subject to check that resulting trace's
	// SubjectMapping and Egress are nil.
	msg := nats.NewMsg("dummy")
	msg.Header.Set(MsgTraceDest, traceSub.Subject)
	msg.Header.Set(MsgTraceOnly, "true")
	msg.Data = []byte("hello!")
	err = nc.PublishMsg(msg)
	require_NoError(t, err)

	traceMsg := natsNexMsg(t, traceSub, time.Second)
	var e MsgTraceEvent
	json.Unmarshal(traceMsg.Data, &e)
	require_Equal[string](t, e.Server.Name, s.Name())
	// We don't remove the headers, so we will find the tracing header there.
	require_True(t, e.Request.Header != nil)
	require_Equal[int](t, len(e.Request.Header), 2)
	// The message size is 6 + whatever size for the 2 trace headers.
	// Let's just make sure that size is > 20...
	require_True(t, e.Request.MsgSize > 20)
	ingress := e.Ingress()
	require_True(t, ingress != nil)
	require_True(t, ingress.Kind == CLIENT)
	require_True(t, ingress.Timestamp != time.Time{})
	require_Equal[uint64](t, ingress.CID, cid)
	require_Equal[string](t, ingress.Name, _EMPTY_)
	require_Equal[string](t, ingress.Account, globalAccountName)
	require_Equal[string](t, ingress.Subject, "dummy")
	require_Equal[string](t, ingress.Error, _EMPTY_)
	require_True(t, e.SubjectMapping() == nil)
	require_True(t, e.StreamExports() == nil)
	require_True(t, e.ServiceImports() == nil)
	require_True(t, e.JetStream() == nil)
	require_True(t, e.Egresses() == nil)

	// Now setup subscriptions that generate interest on the subject.
	nc2 := natsConnect(t, s.ClientURL(), nats.Name("sub1And2"))
	defer nc2.Close()
	sub1 := natsSubSync(t, nc2, "bar")
	sub2 := natsSubSync(t, nc2, "bar")
	natsFlush(t, nc2)
	nc2CID, _ := nc2.GetClientID()

	nc3 := natsConnect(t, s.ClientURL())
	defer nc3.Close()
	sub3 := natsSubSync(t, nc3, "*")
	natsFlush(t, nc3)

	for _, test := range []struct {
		name       string
		deliverMsg bool
	}{
		{"just trace", false},
		{"deliver msg", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			msg = nats.NewMsg("foo")
			msg.Header.Set("Some-App-Header", "some value")
			msg.Header.Set(MsgTraceDest, traceSub.Subject)
			if !test.deliverMsg {
				msg.Header.Set(MsgTraceOnly, "true")
			}
			msg.Data = []byte("hello!")
			err = nc.PublishMsg(msg)
			require_NoError(t, err)

			checkAppMsg := func(sub *nats.Subscription, expected bool) {
				if expected {
					appMsg := natsNexMsg(t, sub, time.Second)
					require_Equal[string](t, string(appMsg.Data), "hello!")
					// We don't remove message trace header, so we should have
					// 2 headers (the app + trace destination)
					require_True(t, len(appMsg.Header) == 2)
					require_Equal[string](t, appMsg.Header.Get("Some-App-Header"), "some value")
					require_Equal[string](t, appMsg.Header.Get(MsgTraceDest), traceSub.Subject)
				}
				// Check that no (more) messages are received.
				if msg, err := sub.NextMsg(100 * time.Millisecond); err != nats.ErrTimeout {
					t.Fatalf("Did not expect application message, got %s", msg.Data)
				}
			}
			for _, sub := range []*nats.Subscription{sub1, sub2, sub3} {
				checkAppMsg(sub, test.deliverMsg)
			}

			traceMsg = natsNexMsg(t, traceSub, time.Second)
			e = MsgTraceEvent{}
			json.Unmarshal(traceMsg.Data, &e)
			require_Equal[string](t, e.Server.Name, s.Name())
			require_True(t, e.Request.Header != nil)
			// We should have the app header and the trace header(s) too.
			expected := 2
			if !test.deliverMsg {
				// The "trace-only" header is added.
				expected++
			}
			require_Equal[int](t, len(e.Request.Header), expected)
			require_Equal[string](t, e.Request.Header["Some-App-Header"][0], "some value")
			// The message size is 6 + whatever size for the 2 trace headers.
			// Let's just make sure that size is > 20...
			require_True(t, e.Request.MsgSize > 20)
			ingress := e.Ingress()
			require_True(t, ingress.Kind == CLIENT)
			require_True(t, ingress.Timestamp != time.Time{})
			require_Equal[string](t, ingress.Account, globalAccountName)
			require_Equal[string](t, ingress.Subject, "foo")
			sm := e.SubjectMapping()
			require_True(t, sm != nil)
			require_True(t, sm.Timestamp != time.Time{})
			require_Equal[string](t, sm.MappedTo, "bar")
			egress := e.Egresses()
			require_Equal[int](t, len(egress), 3)
			var sub1And2 int
			for _, eg := range egress {
				// All Egress should be clients
				require_True(t, eg.Kind == CLIENT)
				require_True(t, eg.Timestamp != time.Time{})
				// For nc2CID, we should have two egress
				if eg.CID == nc2CID {
					// Check name
					require_Equal[string](t, eg.Name, "sub1And2")
					require_Equal[string](t, eg.Subscription, "bar")
					sub1And2++
				} else {
					// No name set
					require_Equal[string](t, eg.Name, _EMPTY_)
					require_Equal[string](t, eg.Subscription, "*")
				}
			}
			require_Equal[int](t, sub1And2, 2)
		})
	}
}

func TestMsgTraceIngressMaxPayloadError(t *testing.T) {
	o := DefaultOptions()
	o.MaxPayload = 1024
	s := RunServer(o)
	defer s.Shutdown()

	nc := natsConnect(t, s.ClientURL())
	defer nc.Close()

	traceSub := natsSubSync(t, nc, "my.trace.subj")
	natsSub(t, nc, "foo", func(_ *nats.Msg) {})
	natsFlush(t, nc)

	// Ensure the subscription is known by the server we're connected to.
	checkSubInterest(t, s, globalAccountName, "my.trace.subj", time.Second)
	checkSubInterest(t, s, globalAccountName, "foo", time.Second)

	for _, test := range []struct {
		name       string
		deliverMsg bool
	}{
		{"just trace", false},
		{"deliver msg", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			nc2, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", o.Port))
			require_NoError(t, err)
			defer nc2.Close()

			nc2.Write([]byte("CONNECT {\"protocol\":1,\"headers\":true,\"no_responders\":true}\r\n"))

			var traceOnlyHdr string
			if !test.deliverMsg {
				traceOnlyHdr = fmt.Sprintf("%s:true\r\n", MsgTraceOnly)
			}
			hdr := fmt.Sprintf("%s%s:%s\r\n%s\r\n", hdrLine, MsgTraceDest, traceSub.Subject, traceOnlyHdr)
			hPub := fmt.Sprintf("HPUB foo %d 2048\r\n%sAAAAAAAAAAAAAAAAAA...", len(hdr), hdr)
			nc2.Write([]byte(hPub))

			traceMsg := natsNexMsg(t, traceSub, time.Second)
			var e MsgTraceEvent
			json.Unmarshal(traceMsg.Data, &e)
			require_Equal[string](t, e.Server.Name, s.Name())
			require_True(t, e.Request.Header == nil)
			require_True(t, e.Ingress() != nil)
			require_Contains(t, e.Ingress().Error, ErrMaxPayload.Error())
			require_True(t, e.Egresses() == nil)
		})
	}
}

func TestMsgTraceIngressErrors(t *testing.T) {
	conf := createConfFile(t, []byte(`
		port: -1
		accounts {
			A {
				users: [
					{
						user: a
						password: pwd
						permissions {
							subscribe: ["my.trace.subj", "foo"]
							publish {
								allow: ["foo", "bar.>"]
								deny: ["bar.baz"]
							}
						}
					}
				]
			}
		}
	`))
	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	nc := natsConnect(t, s.ClientURL(), nats.UserInfo("a", "pwd"))
	defer nc.Close()

	traceSub := natsSubSync(t, nc, "my.trace.subj")
	natsSub(t, nc, "foo", func(_ *nats.Msg) {})
	natsFlush(t, nc)

	// Ensure the subscription is known by the server we're connected to.
	checkSubInterest(t, s, "A", "my.trace.subj", time.Second)
	checkSubInterest(t, s, "A", "foo", time.Second)

	for _, test := range []struct {
		name       string
		deliverMsg bool
	}{
		{"just trace", false},
		{"deliver msg", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			nc2 := natsConnect(t, s.ClientURL(), nats.UserInfo("a", "pwd"))
			defer nc2.Close()

			sendMsg := func(subj, reply, errTxt string) {
				msg := nats.NewMsg(subj)
				msg.Header.Set(MsgTraceDest, traceSub.Subject)
				if !test.deliverMsg {
					msg.Header.Set(MsgTraceOnly, "true")
				}
				msg.Reply = reply
				msg.Data = []byte("hello")
				nc2.PublishMsg(msg)

				traceMsg := natsNexMsg(t, traceSub, time.Second)
				var e MsgTraceEvent
				json.Unmarshal(traceMsg.Data, &e)
				require_Equal[string](t, e.Server.Name, s.Name())
				require_True(t, e.Request.Header != nil)
				require_Contains(t, e.Ingress().Error, errTxt)
				require_True(t, e.Egresses() == nil)
			}

			// Send to a subject that causes permission violation
			sendMsg("bar.baz", _EMPTY_, "Permissions Violation for Publish to")

			// Send to a subject that is reserved for GW replies
			sendMsg(gwReplyPrefix+"foo", _EMPTY_, "Permissions Violation for Publish to")

			// Send with a Reply that is reserved
			sendMsg("foo", replyPrefix+"bar", "Permissions Violation for Publish with Reply of")
		})
	}
}

func TestMsgTraceEgressErrors(t *testing.T) {
	conf := createConfFile(t, []byte(`
		port: -1
		accounts {
			A {
				users: [
					{
						user: a
						password: pwd
						permissions {
							subscribe: {
								allow: ["my.trace.subj", "foo", "bar.>"]
								deny: "bar.bat"
							}
							publish {
								allow: ["foo", "bar.>"]
								deny: ["bar.baz"]
							}
						}
					}
				]
			}
		}
	`))
	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	nc := natsConnect(t, s.ClientURL(), nats.UserInfo("a", "pwd"))
	defer nc.Close()

	traceSub := natsSubSync(t, nc, "my.trace.subj")
	natsFlush(t, nc)

	// Ensure the subscription is known by the server we're connected to.
	checkSubInterest(t, s, "A", "my.trace.subj", time.Second)

	for _, test := range []struct {
		name       string
		deliverMsg bool
	}{
		{"just trace", false},
		{"deliver msg", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			sendMsg := func(pubc *nats.Conn, subj, errTxt string) {
				t.Helper()

				msg := nats.NewMsg(subj)
				msg.Header.Set(MsgTraceDest, traceSub.Subject)
				if !test.deliverMsg {
					msg.Header.Set(MsgTraceOnly, "true")
				}
				msg.Data = []byte("hello")
				pubc.PublishMsg(msg)

				traceMsg := natsNexMsg(t, traceSub, time.Second)
				var e MsgTraceEvent
				json.Unmarshal(traceMsg.Data, &e)
				require_Equal[string](t, e.Server.Name, s.Name())
				egress := e.Egresses()
				require_Equal[int](t, len(egress), 1)
				require_Contains(t, egress[0].Error, errTxt)
			}

			// Test no-echo.
			nc2 := natsConnect(t, s.ClientURL(), nats.UserInfo("a", "pwd"), nats.NoEcho())
			defer nc2.Close()
			natsSubSync(t, nc2, "foo")
			sendMsg(nc2, "foo", errMsgTraceNoEcho)
			nc2.Close()

			// Test deny sub.
			nc2 = natsConnect(t, s.ClientURL(), nats.UserInfo("a", "pwd"))
			defer nc2.Close()
			natsSubSync(t, nc2, "bar.>")
			sendMsg(nc2, "bar.bat", errMsgTraceSubDeny)
			nc2.Close()

			// Test sub closed
			nc2 = natsConnect(t, s.ClientURL(), nats.UserInfo("a", "pwd"))
			defer nc2.Close()
			natsSubSync(t, nc2, "bar.>")
			natsFlush(t, nc2)
			// Aritifially change the closed status of the subscription
			cid, err := nc2.GetClientID()
			require_NoError(t, err)
			c := s.GetClient(cid)
			c.mu.Lock()
			for _, sub := range c.subs {
				if string(sub.subject) == "bar.>" {
					sub.close()
				}
			}
			c.mu.Unlock()
			sendMsg(nc2, "bar.bar", errMsgTraceSubClosed)
			nc2.Close()

			// The following applies only when doing delivery.
			if test.deliverMsg {
				// Test auto-unsub exceeded
				nc2 = natsConnect(t, s.ClientURL(), nats.UserInfo("a", "pwd"))
				defer nc2.Close()
				sub := natsSubSync(t, nc2, "bar.>")
				err := sub.AutoUnsubscribe(10)
				require_NoError(t, err)
				natsFlush(t, nc2)

				// Modify sub.nm to be already over the 10 limit
				cid, err := nc2.GetClientID()
				require_NoError(t, err)
				c := s.GetClient(cid)
				c.mu.Lock()
				for _, sub := range c.subs {
					if string(sub.subject) == "bar.>" {
						sub.nm = 20
					}
				}
				c.mu.Unlock()

				sendMsg(nc2, "bar.bar", errMsgTraceAutoSubExceeded)
				nc2.Close()

				// Test client closed
				nc2 = natsConnect(t, s.ClientURL(), nats.UserInfo("a", "pwd"))
				defer nc2.Close()
				natsSubSync(t, nc2, "bar.>")
				cid, err = nc2.GetClientID()
				require_NoError(t, err)
				c = s.GetClient(cid)
				c.mu.Lock()
				c.out.stc = make(chan struct{})
				c.mu.Unlock()
				msg := nats.NewMsg("bar.bar")
				msg.Header.Set(MsgTraceDest, traceSub.Subject)
				if !test.deliverMsg {
					msg.Header.Set(MsgTraceOnly, "true")
				}
				msg.Data = []byte("hello")
				nc2.PublishMsg(msg)
				// This needs to be less than default stall time, which now is 2ms.
				time.Sleep(1 * time.Millisecond)
				c.mu.Lock()
				c.flags.set(closeConnection)
				c.mu.Unlock()

				traceMsg := natsNexMsg(t, traceSub, time.Second)
				var e MsgTraceEvent
				json.Unmarshal(traceMsg.Data, &e)
				require_Equal[string](t, e.Server.Name, s.Name())
				egress := e.Egresses()
				require_Equal[int](t, len(egress), 1)
				require_Contains(t, egress[0].Error, errMsgTraceClientClosed)
				c.mu.Lock()
				c.flags.clear(closeConnection)
				c.mu.Unlock()
				nc2.Close()
			}
		})
	}
}

func TestMsgTraceWithQueueSub(t *testing.T) {
	o := DefaultOptions()
	s := RunServer(o)
	defer s.Shutdown()

	nc := natsConnect(t, s.ClientURL())
	defer nc.Close()

	traceSub := natsSubSync(t, nc, "my.trace.subj")
	natsFlush(t, nc)

	nc2 := natsConnect(t, s.ClientURL(), nats.Name("sub1"))
	defer nc2.Close()
	sub1 := natsQueueSubSync(t, nc2, "foo", "bar")
	natsFlush(t, nc2)

	nc3 := natsConnect(t, s.ClientURL(), nats.Name("sub2"))
	defer nc3.Close()
	sub2 := natsQueueSubSync(t, nc3, "foo", "bar")
	sub3 := natsQueueSubSync(t, nc3, "*", "baz")
	natsFlush(t, nc3)

	// Ensure the subscription is known by the server we're connected to.
	checkSubInterest(t, s, globalAccountName, "my.trace.subj", time.Second)
	checkSubInterest(t, s, globalAccountName, "foo", time.Second)

	for _, test := range []struct {
		name       string
		deliverMsg bool
	}{
		{"just trace", false},
		{"deliver msg", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			msg := nats.NewMsg("foo")
			msg.Header.Set(MsgTraceDest, traceSub.Subject)
			if !test.deliverMsg {
				msg.Header.Set(MsgTraceOnly, "true")
			}
			if !test.deliverMsg {
				msg.Data = []byte("hello1")
			} else {
				msg.Data = []byte("hello2")
			}
			err := nc.PublishMsg(msg)
			require_NoError(t, err)

			if test.deliverMsg {
				// Only one should have got the message...
				msg1, err1 := sub1.NextMsg(100 * time.Millisecond)
				msg2, err2 := sub2.NextMsg(100 * time.Millisecond)
				if err1 == nil && err2 == nil {
					t.Fatalf("Only one message should have been received")
				}
				var val string
				if msg1 != nil {
					val = string(msg1.Data)
				} else {
					val = string(msg2.Data)
				}
				require_Equal[string](t, val, "hello2")
				// Queue baz should also have received the message
				msg := natsNexMsg(t, sub3, time.Second)
				require_Equal[string](t, string(msg.Data), "hello2")
			}
			// Check that no (more) messages are received.
			for _, sub := range []*nats.Subscription{sub1, sub2, sub3} {
				if msg, err := sub.NextMsg(100 * time.Millisecond); err != nats.ErrTimeout {
					t.Fatalf("Expected no message, got %s", msg.Data)
				}
			}

			traceMsg := natsNexMsg(t, traceSub, time.Second)
			var e MsgTraceEvent
			json.Unmarshal(traceMsg.Data, &e)
			require_Equal[string](t, e.Server.Name, s.Name())
			ingress := e.Ingress()
			require_True(t, ingress != nil)
			require_True(t, ingress.Kind == CLIENT)
			require_Equal[string](t, ingress.Subject, "foo")
			egress := e.Egresses()
			require_Equal[int](t, len(egress), 2)
			var qbar, qbaz int
			for _, eg := range egress {
				switch eg.Queue {
				case "bar":
					require_Equal[string](t, eg.Subscription, "foo")
					qbar++
				case "baz":
					require_Equal[string](t, eg.Subscription, "*")
					qbaz++
				default:
					t.Fatalf("Wrong queue name: %q", eg.Queue)
				}
			}
			require_Equal[int](t, qbar, 1)
			require_Equal[int](t, qbaz, 1)
		})
	}
}

func TestMsgTraceWithRoutes(t *testing.T) {
	tmpl := `
		port: -1
		accounts {
			A { users: [{user:A, password: pwd}] }
			B { users: [{user:B, password: pwd}] }
		}
		cluster {
			name: "local"
			port: -1
			accounts: ["A"]
			%s
		}
	`
	conf1 := createConfFile(t, []byte(fmt.Sprintf(tmpl, _EMPTY_)))
	s1, o1 := RunServerWithConfig(conf1)
	defer s1.Shutdown()

	conf2 := createConfFile(t, []byte(fmt.Sprintf(tmpl, fmt.Sprintf("routes: [\"nats://127.0.0.1:%d\"]", o1.Cluster.Port))))
	s2, _ := RunServerWithConfig(conf2)
	defer s2.Shutdown()

	checkClusterFormed(t, s1, s2)

	checkDummy := func(user string) {
		nc := natsConnect(t, s1.ClientURL(), nats.UserInfo(user, "pwd"))
		defer nc.Close()

		traceSub := natsSubSync(t, nc, "my.trace.subj")
		natsFlush(t, nc)

		// Send trace message to a dummy subject to check that resulting trace
		// is as expected.
		msg := nats.NewMsg("dummy")
		msg.Header.Set(MsgTraceDest, traceSub.Subject)
		msg.Header.Set(MsgTraceOnly, "true")
		msg.Data = []byte("hello!")
		err := nc.PublishMsg(msg)
		require_NoError(t, err)

		traceMsg := natsNexMsg(t, traceSub, time.Second)
		var e MsgTraceEvent
		json.Unmarshal(traceMsg.Data, &e)
		require_Equal[string](t, e.Server.Name, s1.Name())
		ingress := e.Ingress()
		require_True(t, ingress != nil)
		require_True(t, ingress.Kind == CLIENT)
		// "user" is same than account name in this test.
		require_Equal[string](t, ingress.Account, user)
		require_Equal[string](t, ingress.Subject, "dummy")
		require_True(t, e.SubjectMapping() == nil)
		require_True(t, e.Egresses() == nil)

		// We should also not get an event from the remote server.
		if msg, err := traceSub.NextMsg(100 * time.Millisecond); err != nats.ErrTimeout {
			t.Fatalf("Expected no message, got %s", msg.Data)
		}
	}
	checkDummy("A")
	checkDummy("B")

	for _, test := range []struct {
		name string
		acc  string
	}{
		{"pinned account", "A"},
		{"reg account", "B"},
	} {
		t.Run(test.name, func(t *testing.T) {
			acc := test.acc
			// Now create subscriptions on both s1 and s2
			nc2 := natsConnect(t, s2.ClientURL(), nats.UserInfo(acc, "pwd"), nats.Name("sub2"))
			defer nc2.Close()
			sub2 := natsQueueSubSync(t, nc2, "foo.*", "my_queue")

			nc3 := natsConnect(t, s2.ClientURL(), nats.UserInfo(acc, "pwd"), nats.Name("sub3"))
			defer nc3.Close()
			sub3 := natsQueueSubSync(t, nc3, "*.*", "my_queue_2")

			checkSubInterest(t, s1, acc, "foo.bar", time.Second)

			nc1 := natsConnect(t, s1.ClientURL(), nats.UserInfo(acc, "pwd"), nats.Name("sub1"))
			defer nc1.Close()
			sub1 := natsSubSync(t, nc1, "*.bar")

			nct := natsConnect(t, s1.ClientURL(), nats.UserInfo(acc, "pwd"), nats.Name("tracer"))
			defer nct.Close()
			traceSub := natsSubSync(t, nct, "my.trace.subj")

			for _, test := range []struct {
				name       string
				deliverMsg bool
			}{
				{"just trace", false},
				{"deliver msg", true},
			} {
				t.Run(test.name, func(t *testing.T) {
					msg := nats.NewMsg("foo.bar")
					msg.Header.Set(MsgTraceDest, traceSub.Subject)
					if !test.deliverMsg {
						msg.Header.Set(MsgTraceOnly, "true")
					}
					msg.Data = []byte("hello!")
					err := nct.PublishMsg(msg)
					require_NoError(t, err)

					checkAppMsg := func(sub *nats.Subscription, expected bool) {
						if expected {
							appMsg := natsNexMsg(t, sub, time.Second)
							require_Equal[string](t, string(appMsg.Data), "hello!")
						}
						// Check that no (more) messages are received.
						if msg, err := sub.NextMsg(100 * time.Millisecond); err != nats.ErrTimeout {
							t.Fatalf("Did not expect application message, got %s", msg.Data)
						}
					}
					for _, sub := range []*nats.Subscription{sub1, sub2, sub3} {
						checkAppMsg(sub, test.deliverMsg)
					}

					check := func() {
						traceMsg := natsNexMsg(t, traceSub, time.Second)
						var e MsgTraceEvent
						json.Unmarshal(traceMsg.Data, &e)
						ingress := e.Ingress()
						require_True(t, ingress != nil)
						switch ingress.Kind {
						case CLIENT:
							require_Equal[string](t, e.Server.Name, s1.Name())
							require_Equal[string](t, ingress.Account, acc)
							require_Equal[string](t, ingress.Subject, "foo.bar")
							egress := e.Egresses()
							require_Equal[int](t, len(egress), 2)
							for _, eg := range egress {
								if eg.Kind == CLIENT {
									require_Equal[string](t, eg.Name, "sub1")
									require_Equal[string](t, eg.Subscription, "*.bar")
									require_Equal[string](t, eg.Queue, _EMPTY_)
								} else {
									require_True(t, eg.Kind == ROUTER)
									require_Equal[string](t, eg.Name, s2.Name())
									require_Equal[string](t, eg.Subscription, _EMPTY_)
									require_Equal[string](t, eg.Queue, _EMPTY_)
								}
							}
						case ROUTER:
							require_Equal[string](t, e.Server.Name, s2.Name())
							require_Equal[string](t, ingress.Account, acc)
							require_Equal[string](t, ingress.Subject, "foo.bar")
							egress := e.Egresses()
							require_Equal[int](t, len(egress), 2)
							var gotSub2, gotSub3 int
							for _, eg := range egress {
								require_True(t, eg.Kind == CLIENT)
								switch eg.Name {
								case "sub2":
									require_Equal[string](t, eg.Subscription, "foo.*")
									require_Equal[string](t, eg.Queue, "my_queue")
									gotSub2++
								case "sub3":
									require_Equal[string](t, eg.Subscription, "*.*")
									require_Equal[string](t, eg.Queue, "my_queue_2")
									gotSub3++
								default:
									t.Fatalf("Unexpected egress name: %+v", eg)
								}
							}
							require_Equal[int](t, gotSub2, 1)
							require_Equal[int](t, gotSub3, 1)
						default:
							t.Fatalf("Unexpected ingress: %+v", ingress)
						}
					}
					// We should get 2 events. Order is not guaranteed.
					check()
					check()
					// Make sure we are not receiving more traces
					if tm, err := traceSub.NextMsg(250 * time.Millisecond); err == nil {
						t.Fatalf("Should not have received trace message: %s", tm.Data)
					}
				})
			}
		})
	}
}

func TestMsgTraceWithRouteToOldServer(t *testing.T) {
	msgTraceCheckSupport = true
	defer func() { msgTraceCheckSupport = false }()
	tmpl := `
		port: -1
		cluster {
			name: "local"
			port: -1
			pool_size: -1
			%s
		}
	`
	conf1 := createConfFile(t, []byte(fmt.Sprintf(tmpl, _EMPTY_)))
	s1, o1 := RunServerWithConfig(conf1)
	defer s1.Shutdown()

	conf2 := createConfFile(t, []byte(fmt.Sprintf(tmpl, fmt.Sprintf("routes: [\"nats://127.0.0.1:%d\"]", o1.Cluster.Port))))
	o2 := LoadConfig(conf2)
	// Make this server behave like an older server
	o2.overrideProto = setServerProtoForTest(MsgTraceProto - 1)
	s2 := RunServer(o2)
	defer s2.Shutdown()

	checkClusterFormed(t, s1, s2)

	// Now create subscriptions on both s1 and s2
	nc2 := natsConnect(t, s2.ClientURL(), nats.Name("sub2"))
	defer nc2.Close()
	sub2 := natsSubSync(t, nc2, "foo")

	checkSubInterest(t, s1, globalAccountName, "foo", time.Second)

	nc1 := natsConnect(t, s1.ClientURL(), nats.Name("sub1"))
	defer nc1.Close()
	sub1 := natsSubSync(t, nc1, "foo")

	nct := natsConnect(t, s1.ClientURL(), nats.Name("tracer"))
	defer nct.Close()
	traceSub := natsSubSync(t, nct, "my.trace.subj")

	// Ensure the subscription is known by the server we're connected to.
	checkSubInterest(t, s1, globalAccountName, "my.trace.subj", time.Second)
	checkSubInterest(t, s2, globalAccountName, "my.trace.subj", time.Second)

	for _, test := range []struct {
		name       string
		deliverMsg bool
	}{
		{"just trace", false},
		{"deliver msg", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			msg := nats.NewMsg("foo")
			msg.Header.Set(MsgTraceDest, traceSub.Subject)
			if !test.deliverMsg {
				msg.Header.Set(MsgTraceOnly, "true")
			}
			msg.Data = []byte("hello!")
			err := nct.PublishMsg(msg)
			require_NoError(t, err)

			checkAppMsg := func(sub *nats.Subscription, expected bool) {
				if expected {
					appMsg := natsNexMsg(t, sub, time.Second)
					require_Equal[string](t, string(appMsg.Data), "hello!")
				}
				// Check that no (more) messages are received.
				if msg, err := sub.NextMsg(100 * time.Millisecond); err != nats.ErrTimeout {
					t.Fatalf("Did not expect application message, got %s", msg.Data)
				}
			}
			// Even if a server does not support tracing, as long as the header
			// TraceOnly is not set, the message should be forwarded to the remote.
			for _, sub := range []*nats.Subscription{sub1, sub2} {
				checkAppMsg(sub, test.deliverMsg)
			}

			traceMsg := natsNexMsg(t, traceSub, time.Second)
			var e MsgTraceEvent
			json.Unmarshal(traceMsg.Data, &e)
			ingress := e.Ingress()
			require_True(t, ingress != nil)
			require_True(t, ingress.Kind == CLIENT)
			require_Equal[string](t, e.Server.Name, s1.Name())
			egress := e.Egresses()
			require_Equal[int](t, len(egress), 2)
			for _, ci := range egress {
				switch ci.Kind {
				case CLIENT:
					require_Equal[string](t, ci.Name, "sub1")
				case ROUTER:
					require_Equal[string](t, ci.Name, s2.Name())
					if test.deliverMsg {
						require_Contains(t, ci.Error, errMsgTraceNoSupport)
					} else {
						require_Contains(t, ci.Error, errMsgTraceOnlyNoSupport)
					}
				default:
					t.Fatalf("Unexpected egress: %+v", ci)
				}
			}
			// We should not get a second trace
			if msg, err := traceSub.NextMsg(100 * time.Millisecond); err != nats.ErrTimeout {
				t.Fatalf("Did not expect other trace, got %s", msg.Data)
			}
		})
	}
}

func TestMsgTraceWithLeafNode(t *testing.T) {
	for _, mainTest := range []struct {
		name            string
		fromHub         bool
		leafUseLocalAcc bool
	}{
		{"from hub", true, false},
		{"from leaf", false, false},
		{"from hub with local account", true, true},
		{"from leaf with local account", false, true},
	} {
		t.Run(mainTest.name, func(t *testing.T) {
			confHub := createConfFile(t, []byte(`
				port: -1
				server_name: "A"
				accounts {
					A { users: [{user: "a", password: "pwd"}]}
					B { users: [{user: "b", password: "pwd"}]}
				}
				leafnodes {
					port: -1
				}
			`))
			hub, ohub := RunServerWithConfig(confHub)
			defer hub.Shutdown()

			var accs string
			var lacc string
			if mainTest.leafUseLocalAcc {
				accs = `accounts { B { users: [{user: "b", password: "pwd"}]} }`
				lacc = `account: B`
			}
			confLeaf := createConfFile(t, []byte(fmt.Sprintf(`
				port: -1
				server_name: "B"
				%s
				leafnodes {
					remotes [
						{
							url: "nats://a:pwd@127.0.0.1:%d"
							%s
						}
					]
				}
				`, accs, ohub.LeafNode.Port, lacc)))
			leaf, _ := RunServerWithConfig(confLeaf)
			defer leaf.Shutdown()

			checkLeafNodeConnected(t, hub)
			checkLeafNodeConnected(t, leaf)

			var s1, s2 *Server
			if mainTest.fromHub {
				s1, s2 = hub, leaf
			} else {
				s1, s2 = leaf, hub
			}
			// Now create subscriptions on both s1 and s2
			opts := []nats.Option{nats.Name("sub2")}
			var user string
			// If fromHub, then it means that s2 is the leaf.
			if mainTest.fromHub {
				if mainTest.leafUseLocalAcc {
					user = "b"
				}
			} else {
				// s2 is the hub, always connect with user "a'"
				user = "a"
			}
			if user != _EMPTY_ {
				opts = append(opts, nats.UserInfo(user, "pwd"))
			}
			nc2 := natsConnect(t, s2.ClientURL(), opts...)
			defer nc2.Close()
			sub2 := natsSubSync(t, nc2, "foo")

			if mainTest.fromHub {
				checkSubInterest(t, s1, "A", "foo", time.Second)
			} else if mainTest.leafUseLocalAcc {
				checkSubInterest(t, s1, "B", "foo", time.Second)
			} else {
				checkSubInterest(t, s1, globalAccountName, "foo", time.Second)
			}

			user = _EMPTY_
			opts = []nats.Option{nats.Name("sub1")}
			if mainTest.fromHub {
				// s1 is the hub, so we need user "a"
				user = "a"
			} else if mainTest.leafUseLocalAcc {
				// s1 is the leaf, we need user "b" if leafUseLocalAcc
				user = "b"
			}
			if user != _EMPTY_ {
				opts = append(opts, nats.UserInfo(user, "pwd"))
			}
			nc1 := natsConnect(t, s1.ClientURL(), opts...)
			defer nc1.Close()
			sub1 := natsSubSync(t, nc1, "foo")

			opts = []nats.Option{nats.Name("tracer")}
			if user != _EMPTY_ {
				opts = append(opts, nats.UserInfo(user, "pwd"))
			}
			nct := natsConnect(t, s1.ClientURL(), opts...)
			defer nct.Close()
			traceSub := natsSubSync(t, nct, "my.trace.subj")

			for _, test := range []struct {
				name       string
				deliverMsg bool
			}{
				{"just trace", false},
				{"deliver msg", true},
			} {
				t.Run(test.name, func(t *testing.T) {
					msg := nats.NewMsg("foo")
					msg.Header.Set(MsgTraceDest, traceSub.Subject)
					if !test.deliverMsg {
						msg.Header.Set(MsgTraceOnly, "true")
					}
					msg.Data = []byte("hello!")
					err := nct.PublishMsg(msg)
					require_NoError(t, err)

					checkAppMsg := func(sub *nats.Subscription, expected bool) {
						if expected {
							appMsg := natsNexMsg(t, sub, time.Second)
							require_Equal[string](t, string(appMsg.Data), "hello!")
						}
						// Check that no (more) messages are received.
						if msg, err := sub.NextMsg(100 * time.Millisecond); err != nats.ErrTimeout {
							t.Fatalf("Did not expect application message, got %s", msg.Data)
						}
					}
					for _, sub := range []*nats.Subscription{sub1, sub2} {
						checkAppMsg(sub, test.deliverMsg)
					}

					check := func() {
						traceMsg := natsNexMsg(t, traceSub, time.Second)
						var e MsgTraceEvent
						json.Unmarshal(traceMsg.Data, &e)
						ingress := e.Ingress()
						require_True(t, ingress != nil)
						switch ingress.Kind {
						case CLIENT:
							require_Equal[string](t, e.Server.Name, s1.Name())
							egress := e.Egresses()
							require_Equal[int](t, len(egress), 2)
							for _, eg := range egress {
								switch eg.Kind {
								case CLIENT:
									require_Equal[string](t, eg.Name, "sub1")
								case LEAF:
									require_Equal[string](t, eg.Name, s2.Name())
									require_Equal[string](t, eg.Error, _EMPTY_)
								default:
									t.Fatalf("Unexpected egress: %+v", eg)
								}
							}
						case LEAF:
							require_Equal[string](t, e.Server.Name, s2.Name())
							require_True(t, ingress.Kind == LEAF)
							require_Equal(t, ingress.Name, s1.Name())
							egress := e.Egresses()
							require_Equal[int](t, len(egress), 1)
							eg := egress[0]
							require_True(t, eg.Kind == CLIENT)
							require_Equal[string](t, eg.Name, "sub2")
						default:
							t.Fatalf("Unexpected ingress: %+v", ingress)
						}
					}
					check()
					check()
					// Make sure we are not receiving more traces
					if tm, err := traceSub.NextMsg(250 * time.Millisecond); err == nil {
						t.Fatalf("Should not have received trace message: %s", tm.Data)
					}
				})
			}
		})
	}
}

func TestMsgTraceWithLeafNodeToOldServer(t *testing.T) {
	msgTraceCheckSupport = true
	defer func() { msgTraceCheckSupport = false }()
	for _, mainTest := range []struct {
		name    string
		fromHub bool
	}{
		{"from hub", true},
		{"from leaf", false},
	} {
		t.Run(mainTest.name, func(t *testing.T) {
			confHub := createConfFile(t, []byte(`
				port: -1
				server_name: "A"
				leafnodes {
					port: -1
				}
			`))
			ohub := LoadConfig(confHub)
			if !mainTest.fromHub {
				// Make this server behave like an older server
				ohub.overrideProto = setServerProtoForTest(MsgTraceProto - 1)
			}
			hub := RunServer(ohub)
			defer hub.Shutdown()

			confLeaf := createConfFile(t, []byte(fmt.Sprintf(`
				port: -1
				server_name: "B"
				leafnodes {
					remotes [{url: "nats://127.0.0.1:%d"}]
				}
				`, ohub.LeafNode.Port)))
			oleaf := LoadConfig(confLeaf)
			if mainTest.fromHub {
				// Make this server behave like an older server
				oleaf.overrideProto = setServerProtoForTest(MsgTraceProto - 1)
			}
			leaf := RunServer(oleaf)
			defer leaf.Shutdown()

			checkLeafNodeConnected(t, hub)
			checkLeafNodeConnected(t, leaf)

			var s1, s2 *Server
			if mainTest.fromHub {
				s1, s2 = hub, leaf
			} else {
				s1, s2 = leaf, hub
			}

			// Now create subscriptions on both s1 and s2
			nc2 := natsConnect(t, s2.ClientURL(), nats.Name("sub2"))
			defer nc2.Close()
			sub2 := natsSubSync(t, nc2, "foo")

			checkSubInterest(t, s1, globalAccountName, "foo", time.Second)

			nc1 := natsConnect(t, s1.ClientURL(), nats.Name("sub1"))
			defer nc1.Close()
			sub1 := natsSubSync(t, nc1, "foo")

			nct := natsConnect(t, s1.ClientURL(), nats.Name("tracer"))
			defer nct.Close()
			traceSub := natsSubSync(t, nct, "my.trace.subj")

			for _, test := range []struct {
				name       string
				deliverMsg bool
			}{
				{"just trace", false},
				{"deliver msg", true},
			} {
				t.Run(test.name, func(t *testing.T) {
					msg := nats.NewMsg("foo")
					msg.Header.Set(MsgTraceDest, traceSub.Subject)
					if !test.deliverMsg {
						msg.Header.Set(MsgTraceOnly, "true")
					}
					msg.Data = []byte("hello!")
					err := nct.PublishMsg(msg)
					require_NoError(t, err)

					checkAppMsg := func(sub *nats.Subscription, expected bool) {
						if expected {
							appMsg := natsNexMsg(t, sub, time.Second)
							require_Equal[string](t, string(appMsg.Data), "hello!")
						}
						// Check that no (more) messages are received.
						if msg, err := sub.NextMsg(100 * time.Millisecond); err != nats.ErrTimeout {
							t.Fatalf("Did not expect application message, got %s", msg.Data)
						}
					}
					// Even if a server does not support tracing, as long as the header
					// TraceOnly is not set, the message should be forwarded to the remote.
					for _, sub := range []*nats.Subscription{sub1, sub2} {
						checkAppMsg(sub, test.deliverMsg)
					}

					traceMsg := natsNexMsg(t, traceSub, time.Second)
					var e MsgTraceEvent
					json.Unmarshal(traceMsg.Data, &e)
					ingress := e.Ingress()
					require_True(t, ingress != nil)
					require_True(t, ingress.Kind == CLIENT)
					require_Equal[string](t, e.Server.Name, s1.Name())
					egress := e.Egresses()
					require_Equal[int](t, len(egress), 2)
					for _, ci := range egress {
						switch ci.Kind {
						case CLIENT:
							require_Equal[string](t, ci.Name, "sub1")
						case LEAF:
							require_Equal[string](t, ci.Name, s2.Name())
							if test.deliverMsg {
								require_Contains(t, ci.Error, errMsgTraceNoSupport)
							} else {
								require_Contains(t, ci.Error, errMsgTraceOnlyNoSupport)
							}
						default:
							t.Fatalf("Unexpected egress: %+v", ci)
						}
					}
					// We should not get a second trace
					if msg, err := traceSub.NextMsg(100 * time.Millisecond); err != nats.ErrTimeout {
						t.Fatalf("Did not expect other trace, got %s", msg.Data)
					}
				})
			}
		})
	}
}

func TestMsgTraceWithLeafNodeDaisyChain(t *testing.T) {
	confHub := createConfFile(t, []byte(`
		port: -1
		server_name: "A"
		accounts {
			A { users: [{user: "a", password: "pwd"}]}
		}
		leafnodes {
			port: -1
		}
	`))
	hub, ohub := RunServerWithConfig(confHub)
	defer hub.Shutdown()

	confLeaf1 := createConfFile(t, []byte(fmt.Sprintf(`
		port: -1
		server_name: "B"
		accounts {
			B { users: [{user: "b", password: "pwd"}]}
		}
		leafnodes {
			port: -1
			remotes [{url: "nats://a:pwd@127.0.0.1:%d", account: B}]
		}
	`, ohub.LeafNode.Port)))
	leaf1, oleaf1 := RunServerWithConfig(confLeaf1)
	defer leaf1.Shutdown()

	confLeaf2 := createConfFile(t, []byte(fmt.Sprintf(`
		port: -1
		server_name: "C"
		accounts {
			C { users: [{user: "c", password: "pwd"}]}
		}
		leafnodes {
			remotes [{url: "nats://b:pwd@127.0.0.1:%d", account: C}]
		}
	`, oleaf1.LeafNode.Port)))
	leaf2, _ := RunServerWithConfig(confLeaf2)
	defer leaf2.Shutdown()

	checkLeafNodeConnected(t, hub)
	checkLeafNodeConnectedCount(t, leaf1, 2)
	checkLeafNodeConnected(t, leaf2)

	nct := natsConnect(t, hub.ClientURL(), nats.UserInfo("a", "pwd"), nats.Name("Tracer"))
	defer nct.Close()
	traceSub := natsSubSync(t, nct, "my.trace.subj")
	natsFlush(t, nct)
	// Make sure that subject interest travels down to leaf2
	checkSubInterest(t, leaf2, "C", traceSub.Subject, time.Second)

	nc1 := natsConnect(t, leaf1.ClientURL(), nats.UserInfo("b", "pwd"), nats.Name("sub1"))
	defer nc1.Close()

	nc2 := natsConnect(t, leaf2.ClientURL(), nats.UserInfo("c", "pwd"), nats.Name("sub2"))
	defer nc2.Close()
	sub2 := natsQueueSubSync(t, nc2, "foo.bar", "my_queue")
	natsFlush(t, nc2)

	// Check the subject interest makes it to leaf1
	checkSubInterest(t, leaf1, "B", "foo.bar", time.Second)

	// Now create the sub on leaf1
	sub1 := natsSubSync(t, nc1, "foo.*")
	natsFlush(t, nc1)

	// Check that subject interest registered on "hub"
	checkSubInterest(t, hub, "A", "foo.bar", time.Second)

	for _, test := range []struct {
		name       string
		deliverMsg bool
	}{
		{"just trace", false},
		{"deliver msg", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			msg := nats.NewMsg("foo.bar")
			msg.Header.Set(MsgTraceDest, traceSub.Subject)
			if !test.deliverMsg {
				msg.Header.Set(MsgTraceOnly, "true")
			}
			msg.Data = []byte("hello!")
			err := nct.PublishMsg(msg)
			require_NoError(t, err)

			checkAppMsg := func(sub *nats.Subscription, expected bool) {
				if expected {
					appMsg := natsNexMsg(t, sub, time.Second)
					require_Equal[string](t, string(appMsg.Data), "hello!")
				}
				// Check that no (more) messages are received.
				if msg, err := sub.NextMsg(100 * time.Millisecond); err != nats.ErrTimeout {
					t.Fatalf("Did not expect application message, got %s", msg.Data)
				}
			}
			for _, sub := range []*nats.Subscription{sub1, sub2} {
				checkAppMsg(sub, test.deliverMsg)
			}

			check := func() {
				traceMsg := natsNexMsg(t, traceSub, time.Second)
				var e MsgTraceEvent
				json.Unmarshal(traceMsg.Data, &e)

				ingress := e.Ingress()
				require_True(t, ingress != nil)
				switch ingress.Kind {
				case CLIENT:
					require_Equal[string](t, e.Server.Name, hub.Name())
					require_Equal[string](t, ingress.Name, "Tracer")
					require_Equal[string](t, ingress.Account, "A")
					require_Equal[string](t, ingress.Subject, "foo.bar")
					egress := e.Egresses()
					require_Equal[int](t, len(egress), 1)
					eg := egress[0]
					require_True(t, eg.Kind == LEAF)
					require_Equal[string](t, eg.Name, leaf1.Name())
					require_Equal[string](t, eg.Account, _EMPTY_)
					require_Equal[string](t, eg.Subscription, _EMPTY_)
				case LEAF:
					switch e.Server.Name {
					case leaf1.Name():
						require_Equal(t, ingress.Name, hub.Name())
						require_Equal(t, ingress.Account, "B")
						require_Equal[string](t, ingress.Subject, "foo.bar")
						egress := e.Egresses()
						require_Equal[int](t, len(egress), 2)
						for _, eg := range egress {
							switch eg.Kind {
							case CLIENT:
								require_Equal[string](t, eg.Name, "sub1")
								require_Equal[string](t, eg.Subscription, "foo.*")
								require_Equal[string](t, eg.Queue, _EMPTY_)
							case LEAF:
								require_Equal[string](t, eg.Name, leaf2.Name())
								require_Equal[string](t, eg.Account, _EMPTY_)
								require_Equal[string](t, eg.Subscription, _EMPTY_)
								require_Equal[string](t, eg.Queue, _EMPTY_)
							default:
								t.Fatalf("Unexpected egress: %+v", eg)
							}
						}
					case leaf2.Name():
						require_Equal(t, ingress.Name, leaf1.Name())
						require_Equal(t, ingress.Account, "C")
						require_Equal[string](t, ingress.Subject, "foo.bar")
						egress := e.Egresses()
						require_Equal[int](t, len(egress), 1)
						eg := egress[0]
						require_True(t, eg.Kind == CLIENT)
						require_Equal[string](t, eg.Name, "sub2")
						require_Equal[string](t, eg.Subscription, "foo.bar")
						require_Equal[string](t, eg.Queue, "my_queue")
					default:
						t.Fatalf("Unexpected ingress: %+v", ingress)
					}
				default:
					t.Fatalf("Unexpected ingress: %+v", ingress)
				}
			}
			check()
			check()
			check()
			// Make sure we are not receiving more traces
			if tm, err := traceSub.NextMsg(250 * time.Millisecond); err == nil {
				t.Fatalf("Should not have received trace message: %s", tm.Data)
			}
		})
	}
}

func TestMsgTraceWithGateways(t *testing.T) {
	o2 := testDefaultOptionsForGateway("B")
	o2.NoSystemAccount = false
	s2 := runGatewayServer(o2)
	defer s2.Shutdown()

	o1 := testGatewayOptionsFromToWithServers(t, "A", "B", s2)
	o1.NoSystemAccount = false
	s1 := runGatewayServer(o1)
	defer s1.Shutdown()

	waitForOutboundGateways(t, s1, 1, time.Second)
	waitForInboundGateways(t, s2, 1, time.Second)
	waitForOutboundGateways(t, s2, 1, time.Second)

	nc2 := natsConnect(t, s2.ClientURL(), nats.Name("sub2"))
	defer nc2.Close()
	sub2 := natsQueueSubSync(t, nc2, "foo.*", "my_queue")

	nc3 := natsConnect(t, s2.ClientURL(), nats.Name("sub3"))
	defer nc3.Close()
	sub3 := natsQueueSubSync(t, nc3, "*.*", "my_queue_2")

	nc1 := natsConnect(t, s1.ClientURL(), nats.Name("sub1"))
	defer nc1.Close()
	sub1 := natsSubSync(t, nc1, "*.bar")

	nct := natsConnect(t, s1.ClientURL(), nats.Name("tracer"))
	defer nct.Close()
	traceSub := natsSubSync(t, nct, "my.trace.subj")

	// Ensure the subscription is known by the server we're connected to.
	require_NoError(t, nct.Flush())
	time.Sleep(100 * time.Millisecond)

	for _, test := range []struct {
		name       string
		deliverMsg bool
	}{
		{"just trace", false},
		{"deliver msg", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			msg := nats.NewMsg("foo.bar")
			msg.Header.Set(MsgTraceDest, traceSub.Subject)
			if !test.deliverMsg {
				msg.Header.Set(MsgTraceOnly, "true")
			}
			msg.Data = []byte("hello!")
			err := nct.PublishMsg(msg)
			require_NoError(t, err)

			checkAppMsg := func(sub *nats.Subscription, expected bool) {
				if expected {
					appMsg := natsNexMsg(t, sub, time.Second)
					require_Equal[string](t, string(appMsg.Data), "hello!")
				}
				// Check that no (more) messages are received.
				if msg, err := sub.NextMsg(100 * time.Millisecond); err != nats.ErrTimeout {
					t.Fatalf("Did not expect application message, got %s", msg.Data)
				}
			}
			for _, sub := range []*nats.Subscription{sub1, sub2, sub3} {
				checkAppMsg(sub, test.deliverMsg)
			}

			check := func() {
				traceMsg := natsNexMsg(t, traceSub, time.Second)
				var e MsgTraceEvent
				json.Unmarshal(traceMsg.Data, &e)

				ingress := e.Ingress()
				require_True(t, ingress != nil)
				switch ingress.Kind {
				case CLIENT:
					require_Equal[string](t, e.Server.Name, s1.Name())
					require_Equal[string](t, ingress.Account, globalAccountName)
					require_Equal[string](t, ingress.Subject, "foo.bar")
					egress := e.Egresses()
					require_Equal[int](t, len(egress), 2)
					for _, eg := range egress {
						switch eg.Kind {
						case CLIENT:
							require_Equal[string](t, eg.Name, "sub1")
							require_Equal[string](t, eg.Subscription, "*.bar")
							require_Equal[string](t, eg.Queue, _EMPTY_)
						case GATEWAY:
							require_Equal[string](t, eg.Name, s2.Name())
							require_Equal[string](t, eg.Error, _EMPTY_)
							require_Equal[string](t, eg.Subscription, _EMPTY_)
							require_Equal[string](t, eg.Queue, _EMPTY_)
						default:
							t.Fatalf("Unexpected egress: %+v", eg)
						}
					}
				case GATEWAY:
					require_Equal[string](t, e.Server.Name, s2.Name())
					require_Equal[string](t, ingress.Account, globalAccountName)
					require_Equal[string](t, ingress.Subject, "foo.bar")
					egress := e.Egresses()
					require_Equal[int](t, len(egress), 2)
					var gotSub2, gotSub3 int
					for _, eg := range egress {
						require_True(t, eg.Kind == CLIENT)
						switch eg.Name {
						case "sub2":
							require_Equal[string](t, eg.Subscription, "foo.*")
							require_Equal[string](t, eg.Queue, "my_queue")
							gotSub2++
						case "sub3":
							require_Equal[string](t, eg.Subscription, "*.*")
							require_Equal[string](t, eg.Queue, "my_queue_2")
							gotSub3++
						default:
							t.Fatalf("Unexpected egress name: %+v", eg)
						}
					}
					require_Equal[int](t, gotSub2, 1)
					require_Equal[int](t, gotSub3, 1)

				default:
					t.Fatalf("Unexpected ingress: %+v", ingress)
				}
			}
			// We should get 2 events
			check()
			check()
			// Make sure we are not receiving more traces
			if tm, err := traceSub.NextMsg(250 * time.Millisecond); err == nil {
				t.Fatalf("Should not have received trace message: %s", tm.Data)
			}
		})
	}
}

func TestMsgTraceWithGatewayToOldServer(t *testing.T) {
	msgTraceCheckSupport = true
	defer func() { msgTraceCheckSupport = false }()

	o2 := testDefaultOptionsForGateway("B")
	o2.NoSystemAccount = false
	// Make this server behave like an older server
	o2.overrideProto = setServerProtoForTest(MsgTraceProto - 1)
	s2 := runGatewayServer(o2)
	defer s2.Shutdown()

	o1 := testGatewayOptionsFromToWithServers(t, "A", "B", s2)
	o1.NoSystemAccount = false
	s1 := runGatewayServer(o1)
	defer s1.Shutdown()

	waitForOutboundGateways(t, s1, 1, time.Second)
	waitForInboundGateways(t, s2, 1, time.Second)
	waitForOutboundGateways(t, s2, 1, time.Second)

	nc2 := natsConnect(t, s2.ClientURL(), nats.Name("sub2"))
	defer nc2.Close()
	sub2 := natsSubSync(t, nc2, "foo")

	nc1 := natsConnect(t, s1.ClientURL(), nats.Name("sub1"))
	defer nc1.Close()
	sub1 := natsSubSync(t, nc1, "foo")

	nct := natsConnect(t, s1.ClientURL(), nats.Name("tracer"))
	defer nct.Close()
	traceSub := natsSubSync(t, nct, "my.trace.subj")

	// Ensure the subscription is known by the server we're connected to.
	require_NoError(t, nct.Flush())
	time.Sleep(100 * time.Millisecond)

	for _, test := range []struct {
		name       string
		deliverMsg bool
	}{
		{"just trace", false},
		{"deliver msg", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			msg := nats.NewMsg("foo")
			msg.Header.Set(MsgTraceDest, traceSub.Subject)
			if !test.deliverMsg {
				msg.Header.Set(MsgTraceOnly, "true")
			}
			msg.Data = []byte("hello!")
			err := nct.PublishMsg(msg)
			require_NoError(t, err)

			checkAppMsg := func(sub *nats.Subscription, expected bool) {
				if expected {
					appMsg := natsNexMsg(t, sub, time.Second)
					require_Equal[string](t, string(appMsg.Data), "hello!")
				}
				// Check that no (more) messages are received.
				if msg, err := sub.NextMsg(100 * time.Millisecond); err != nats.ErrTimeout {
					t.Fatalf("Did not expect application message, got %s", msg.Data)
				}
			}
			// Even if a server does not support tracing, as long as the header
			// TraceOnly is not set, the message should be forwarded to the remote.
			for _, sub := range []*nats.Subscription{sub1, sub2} {
				checkAppMsg(sub, test.deliverMsg)
			}

			traceMsg := natsNexMsg(t, traceSub, time.Second)
			var e MsgTraceEvent
			json.Unmarshal(traceMsg.Data, &e)
			ingress := e.Ingress()
			require_True(t, ingress != nil)
			switch ingress.Kind {
			case CLIENT:
				require_Equal[string](t, e.Server.Name, s1.Name())
				egress := e.Egresses()
				require_Equal[int](t, len(egress), 2)
				for _, ci := range egress {
					switch ci.Kind {
					case CLIENT:
						require_Equal[string](t, ci.Name, "sub1")
					case GATEWAY:
						require_Equal[string](t, ci.Name, s2.Name())
						if test.deliverMsg {
							require_Contains(t, ci.Error, errMsgTraceNoSupport)
						} else {
							require_Contains(t, ci.Error, errMsgTraceOnlyNoSupport)
						}
					default:
						t.Fatalf("Unexpected egress: %+v", ci)
					}
				}
			default:
				t.Fatalf("Unexpected ingress: %+v", ingress)
			}
			// We should not get a second trace
			if msg, err := traceSub.NextMsg(100 * time.Millisecond); err != nats.ErrTimeout {
				t.Fatalf("Did not expect other trace, got %s", msg.Data)
			}
		})
	}
}

func TestMsgTraceServiceImport(t *testing.T) {
	tmpl := `
		listen: 127.0.0.1:-1
		accounts {
			A {
				users: [{user: a, password: pwd}]
				exports: [ { service: ">", allow_trace: %v} ]
				mappings = {
					bar: bozo
				}
			}
			B {
				users: [{user: b, password: pwd}]
				imports: [ { service: {account: "A", subject:">"} } ]
				exports: [ { service: ">", allow_trace: %v} ]
			}
			C {
				users: [{user: c, password: pwd}]
				exports: [ { service: ">", allow_trace: %v } ]
			}
			D {
				users: [{user: d, password: pwd}]
				imports: [
					{ service: {account: "B", subject:"bar"}, to: baz }
					{ service: {account: "C", subject:">"} }
				]
				mappings = {
						bat: baz
				}
			}
		}
	`
	conf := createConfFile(t, []byte(fmt.Sprintf(tmpl, false, false, false)))
	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	nc := natsConnect(t, s.ClientURL(), nats.UserInfo("d", "pwd"), nats.Name("Requestor"))
	defer nc.Close()

	traceSub := natsSubSync(t, nc, "my.trace.subj")
	sub := natsSubSync(t, nc, "my.service.response.inbox")

	nc2 := natsConnect(t, s.ClientURL(), nats.UserInfo("a", "pwd"), nats.Name("ServiceA"))
	defer nc2.Close()
	recv := int32(0)
	natsQueueSub(t, nc2, "*", "my_queue", func(m *nats.Msg) {
		atomic.AddInt32(&recv, 1)
		m.Respond(m.Data)
	})
	natsFlush(t, nc2)

	nc3 := natsConnect(t, s.ClientURL(), nats.UserInfo("c", "pwd"), nats.Name("ServiceC"))
	defer nc3.Close()
	natsSub(t, nc3, "baz", func(m *nats.Msg) {
		atomic.AddInt32(&recv, 1)
		m.Respond(m.Data)
	})
	natsFlush(t, nc3)

	for mainIter, mainTest := range []struct {
		name  string
		allow bool
	}{
		{"not allowed", false},
		{"allowed", true},
		{"not allowed again", false},
	} {
		atomic.StoreInt32(&recv, 0)
		t.Run(mainTest.name, func(t *testing.T) {
			for _, test := range []struct {
				name       string
				deliverMsg bool
			}{
				{"just trace", false},
				{"deliver msg", true},
			} {
				t.Run(test.name, func(t *testing.T) {
					msg := nats.NewMsg("bat")
					msg.Header.Set(MsgTraceDest, traceSub.Subject)
					if !test.deliverMsg {
						msg.Header.Set(MsgTraceOnly, "true")
					}
					if !test.deliverMsg {
						msg.Data = []byte("request1")
					} else {
						msg.Data = []byte("request2")
					}
					msg.Reply = sub.Subject

					err := nc.PublishMsg(msg)
					require_NoError(t, err)

					if test.deliverMsg {
						for i := 0; i < 2; i++ {
							appMsg := natsNexMsg(t, sub, time.Second)
							require_Equal[string](t, string(appMsg.Data), "request2")
						}
					}
					// Check that no (more) messages are received.
					if msg, err := sub.NextMsg(100 * time.Millisecond); msg != nil || (err != nats.ErrTimeout && err != nats.ErrNoResponders) {
						t.Fatalf("Did not expect application message, got msg=%v err=%v", msg, err)
					}
					if !test.deliverMsg {
						// Just to make sure that message was not delivered to service
						// responders, wait a bit and check the recv value.
						time.Sleep(50 * time.Millisecond)
						if n := atomic.LoadInt32(&recv); n != 0 {
							t.Fatalf("Expected no message to be received, but service callback fired %d times", n)
						}
					}

					traceMsg := natsNexMsg(t, traceSub, time.Second)
					var e MsgTraceEvent
					json.Unmarshal(traceMsg.Data, &e)

					require_Equal[string](t, e.Server.Name, s.Name())
					ingress := e.Ingress()
					require_True(t, ingress != nil)
					require_True(t, ingress.Kind == CLIENT)
					require_Equal[string](t, ingress.Account, "D")
					require_Equal[string](t, ingress.Subject, "bat")
					sm := e.SubjectMapping()
					require_True(t, sm != nil)
					require_Equal[string](t, sm.MappedTo, "baz")
					simps := e.ServiceImports()
					require_True(t, simps != nil)
					var expectedServices int
					if mainTest.allow {
						expectedServices = 3
					} else {
						expectedServices = 2
					}
					require_Equal[int](t, len(simps), expectedServices)
					for _, si := range simps {
						require_True(t, si.Timestamp != time.Time{})
						switch si.Account {
						case "C":
							require_Equal[string](t, si.From, "baz")
							require_Equal[string](t, si.To, "baz")
						case "B":
							require_Equal[string](t, si.From, "baz")
							require_Equal[string](t, si.To, "bar")
						case "A":
							if !mainTest.allow {
								t.Fatalf("Without allow_trace, we should not see service for account A")
							}
							require_Equal[string](t, si.From, "bar")
							require_Equal[string](t, si.To, "bozo")
						default:
							t.Fatalf("Unexpected account name: %s", si.Account)
						}
					}
					egress := e.Egresses()
					if !mainTest.allow {
						require_Equal[int](t, len(egress), 0)
					} else {
						require_Equal[int](t, len(egress), 2)
						var gotA, gotC bool
						for _, eg := range egress {
							// All Egress should be clients
							require_True(t, eg.Kind == CLIENT)
							// We should have one for ServiceA and one for ServiceC
							if eg.Name == "ServiceA" {
								require_Equal[string](t, eg.Account, "A")
								require_Equal[string](t, eg.Subscription, "*")
								require_Equal[string](t, eg.Queue, "my_queue")
								gotA = true
							} else if eg.Name == "ServiceC" {
								require_Equal[string](t, eg.Account, "C")
								require_Equal[string](t, eg.Queue, _EMPTY_)
								gotC = true
							}
						}
						if !gotA {
							t.Fatalf("Did not get Egress for serviceA: %+v", egress)
						}
						if !gotC {
							t.Fatalf("Did not get Egress for serviceC: %+v", egress)
						}
					}

					// Make sure we properly remove the responses.
					checkResp := func(an string) {
						acc, err := s.lookupAccount(an)
						require_NoError(t, err)
						checkFor(t, time.Second, 15*time.Millisecond, func() error {
							if n := acc.NumPendingAllResponses(); n != 0 {
								return fmt.Errorf("Still %d responses pending for account %q on server %s", n, acc, s)
							}
							return nil
						})
					}
					for _, acc := range []string{"A", "B", "C", "D"} {
						checkResp(acc)
					}
				})
			}
			switch mainIter {
			case 0:
				reloadUpdateConfig(t, s, conf, fmt.Sprintf(tmpl, true, true, true))
			case 1:
				reloadUpdateConfig(t, s, conf, fmt.Sprintf(tmpl, false, false, false))
			}
		})
	}
}

func TestMsgTraceServiceImportWithSuperCluster(t *testing.T) {
	for _, mainTest := range []struct {
		name     string
		allowStr string
		allow    bool
	}{
		{"allowed", "true", true},
		{"not allowed", "false", false},
	} {
		t.Run(mainTest.name, func(t *testing.T) {
			tmpl := `
				listen: 127.0.0.1:-1
				server_name: %s
				jetstream: {max_mem_store: 256MB, max_file_store: 2GB, store_dir: '%s'}

				cluster {
					name: %s
					listen: 127.0.0.1:%d
					routes = [%s]
				}
				accounts {
					A {
						users: [{user: a, password: pwd}]
						exports: [ { service: ">", allow_trace: ` + mainTest.allowStr + ` } ]
						mappings = {
							bar: bozo
						}
						trace_dest: "a.trace.subj"
					}
					B {
						users: [{user: b, password: pwd}]
						imports: [ { service: {account: "A", subject:">"} } ]
						exports: [ { service: ">" , allow_trace: ` + mainTest.allowStr + ` } ]
						trace_dest: "b.trace.subj"
					}
					C {
						users: [{user: c, password: pwd}]
						exports: [ { service: ">" , allow_trace: ` + mainTest.allowStr + ` } ]
						trace_dest: "c.trace.subj"
					}
					D {
						users: [{user: d, password: pwd}]
						imports: [
							{ service: {account: "B", subject:"bar"}, to: baz }
							{ service: {account: "C", subject:">"} }
						]
						mappings = {
								bat: baz
						}
						trace_dest: "d.trace.subj"
					}
					$SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] }
				}
			`
			sc := createJetStreamSuperClusterWithTemplate(t, tmpl, 3, 2)
			defer sc.shutdown()

			sfornc := sc.clusters[0].servers[0]
			nc := natsConnect(t, sfornc.ClientURL(), nats.UserInfo("d", "pwd"), nats.Name("Requestor"))
			defer nc.Close()

			traceSub := natsSubSync(t, nc, "my.trace.subj")
			sub := natsSubSync(t, nc, "my.service.response.inbox")

			sfornc2 := sc.clusters[0].servers[1]
			nc2 := natsConnect(t, sfornc2.ClientURL(), nats.UserInfo("a", "pwd"), nats.Name("ServiceA"))
			defer nc2.Close()
			subSvcA := natsQueueSubSync(t, nc2, "*", "my_queue")
			natsFlush(t, nc2)

			sfornc3 := sc.clusters[1].servers[0]
			nc3 := natsConnect(t, sfornc3.ClientURL(), nats.UserInfo("c", "pwd"), nats.Name("ServiceC"))
			defer nc3.Close()
			subSvcC := natsSubSync(t, nc3, "baz")
			natsFlush(t, nc3)

			// Create a subscription for each account trace destination to make
			// sure that we are not sending it there.
			var accSubs []*nats.Subscription
			for _, user := range []string{"a", "b", "c", "d"} {
				nc := natsConnect(t, sfornc.ClientURL(), nats.UserInfo(user, "pwd"))
				defer nc.Close()
				accSubs = append(accSubs, natsSubSync(t, nc, user+".trace.subj"))
			}

			for _, test := range []struct {
				name       string
				deliverMsg bool
			}{
				{"just trace", false},
				{"deliver msg", true},
			} {
				t.Run(test.name, func(t *testing.T) {
					msg := nats.NewMsg("bat")
					msg.Header.Set(MsgTraceDest, traceSub.Subject)
					if !test.deliverMsg {
						msg.Header.Set(MsgTraceOnly, "true")
					}
					// We add the traceParentHdr header to make sure that it is
					// deactivated in addition to the Nats-Trace-Dest header too
					// when needed.
					traceParentHdrVal := "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
					msg.Header.Set(traceParentHdr, traceParentHdrVal)
					if !test.deliverMsg {
						msg.Data = []byte("request1")
					} else {
						msg.Data = []byte("request2")
					}
					msg.Reply = sub.Subject

					err := nc.PublishMsg(msg)
					require_NoError(t, err)

					if test.deliverMsg {
						processSvc := func(sub *nats.Subscription) {
							t.Helper()
							appMsg := natsNexMsg(t, sub, time.Second)
							// This test causes a message to be routed to the
							// service responders. When not allowing, we need
							// to make sure that the trace header has been
							// disabled. Not receiving the trace event from
							// the remote is not enough to verify since the
							// trace would not reach the origin server because
							// the origin account header will not be present.
							if mainTest.allow {
								if hv := appMsg.Header.Get(MsgTraceDest); hv != traceSub.Subject {
									t.Fatalf("Expecting header with %q, but got %q", traceSub.Subject, hv)
								}
								if hv := appMsg.Header.Get(traceParentHdr); hv != traceParentHdrVal {
									t.Fatalf("Expecting header with %q, but got %q", traceParentHdrVal, hv)
								}
							} else {
								if hv := appMsg.Header.Get(MsgTraceDest); hv != _EMPTY_ {
									t.Fatalf("Expecting no header, but header was present with value: %q", hv)
								}
								if hv := appMsg.Header.Get(traceParentHdr); hv != _EMPTY_ {
									t.Fatalf("Expecting no header, but header was present with value: %q", hv)
								}
								// We don't really need to check that, but we
								// should see the header with the first letter
								// being an `X`.
								hnb := []byte(MsgTraceDest)
								hnb[0] = 'X'
								hn := string(hnb)
								if hv := appMsg.Header.Get(hn); hv != traceSub.Subject {
									t.Fatalf("Expected header %q to be %q, got %q", hn, traceSub.Subject, hv)
								}
								hnb = []byte(traceParentHdr)
								hnb[0] = 'X'
								hn = string(hnb)
								if hv := appMsg.Header.Get(hn); hv != traceParentHdrVal {
									t.Fatalf("Expected header %q to be %q, got %q", hn, traceParentHdrVal, hv)
								}
							}
							appMsg.Respond(appMsg.Data)
						}
						processSvc(subSvcA)
						processSvc(subSvcC)

						for i := 0; i < 2; i++ {
							appMsg := natsNexMsg(t, sub, time.Second)
							require_Equal[string](t, string(appMsg.Data), "request2")
						}
					}
					// Check that no (more) messages are received.
					if msg, err := sub.NextMsg(100 * time.Millisecond); msg != nil || (err != nats.ErrTimeout && err != nats.ErrNoResponders) {
						t.Fatalf("Did not expect application message, got msg=%v err=%v", msg, err)
					}
					if !test.deliverMsg {
						// Just to make sure that message was not delivered to service
						// responders, wait a bit and check the recv value.
						time.Sleep(50 * time.Millisecond)
						for _, sub := range []*nats.Subscription{subSvcA, subSvcC} {
							if msg, err := sub.NextMsg(250 * time.Millisecond); err == nil {
								t.Fatalf("Expected no message to be received, but service subscription got %s", msg.Data)
							}
						}
					}

					check := func() {
						traceMsg := natsNexMsg(t, traceSub, time.Second)
						var e MsgTraceEvent
						json.Unmarshal(traceMsg.Data, &e)

						ingress := e.Ingress()
						require_True(t, ingress != nil)
						switch ingress.Kind {
						case CLIENT:
							require_Equal[string](t, e.Server.Name, sfornc.Name())
							require_Equal[string](t, ingress.Account, "D")
							require_Equal[string](t, ingress.Subject, "bat")
							sm := e.SubjectMapping()
							require_True(t, sm != nil)
							require_Equal[string](t, sm.MappedTo, "baz")
							simps := e.ServiceImports()
							require_True(t, simps != nil)
							var expectedServices int
							if mainTest.allow {
								expectedServices = 3
							} else {
								expectedServices = 2
							}
							require_Equal[int](t, len(simps), expectedServices)
							for _, si := range simps {
								switch si.Account {
								case "C":
									require_Equal[string](t, si.From, "baz")
									require_Equal[string](t, si.To, "baz")
								case "B":
									require_Equal[string](t, si.From, "baz")
									require_Equal[string](t, si.To, "bar")
								case "A":
									if !mainTest.allow {
										t.Fatalf("Without allow_trace, we should not see service for account A")
									}
									require_Equal[string](t, si.From, "bar")
									require_Equal[string](t, si.To, "bozo")
								default:
									t.Fatalf("Unexpected account name: %s", si.Account)
								}
							}
							egress := e.Egresses()
							if !mainTest.allow {
								require_Equal[int](t, len(egress), 0)
							} else {
								require_Equal[int](t, len(egress), 2)
								for _, eg := range egress {
									switch eg.Kind {
									case ROUTER:
										require_Equal[string](t, eg.Name, sfornc2.Name())
										require_Equal[string](t, eg.Account, _EMPTY_)
									case GATEWAY:
										require_Equal[string](t, eg.Name, sfornc3.Name())
										require_Equal[string](t, eg.Account, _EMPTY_)
									}
								}
							}
						case ROUTER:
							require_Equal[string](t, e.Server.Name, sfornc2.Name())
							require_Equal[string](t, ingress.Account, "A")
							require_Equal[string](t, ingress.Subject, "bozo")
							egress := e.Egresses()
							require_Equal[int](t, len(egress), 1)
							eg := egress[0]
							require_True(t, eg.Kind == CLIENT)
							require_Equal[string](t, eg.Name, "ServiceA")
							require_Equal[string](t, eg.Account, _EMPTY_)
							require_Equal[string](t, eg.Subscription, "*")
							require_Equal[string](t, eg.Queue, "my_queue")
						case GATEWAY:
							require_Equal[string](t, e.Server.Name, sfornc3.Name())
							require_Equal[string](t, ingress.Account, "C")
							require_Equal[string](t, ingress.Subject, "baz")
							egress := e.Egresses()
							require_Equal[int](t, len(egress), 1)
							eg := egress[0]
							require_True(t, eg.Kind == CLIENT)
							require_Equal[string](t, eg.Name, "ServiceC")
							require_Equal[string](t, eg.Account, _EMPTY_)
							require_Equal[string](t, eg.Subscription, "baz")
							require_Equal[string](t, eg.Queue, _EMPTY_)
						default:
							t.Fatalf("Unexpected ingress: %+v", ingress)
						}
					}
					// We should receive 3 events when allowed, a single when not.
					check()
					if mainTest.allow {
						check()
						check()
					}
					// Make sure we are not receiving more traces
					if tm, err := traceSub.NextMsg(250 * time.Millisecond); err == nil {
						t.Fatalf("Should not have received trace message: %s", tm.Data)
					}
					// Make sure that we never receive on any of the account
					// trace destination's sub.
					for _, sub := range accSubs {
						if tm, err := sub.NextMsg(100 * time.Millisecond); err == nil {
							t.Fatalf("Should not have received trace message on account's trace sub, got %s", tm.Data)
						}
					}
					// Make sure we properly remove the responses.
					checkResp := func(an string) {
						for _, s := range []*Server{sfornc, sfornc2, sfornc3} {
							acc, err := s.lookupAccount(an)
							require_NoError(t, err)
							checkFor(t, time.Second, 15*time.Millisecond, func() error {
								if n := acc.NumPendingAllResponses(); n != 0 {
									return fmt.Errorf("Still %d responses pending for account %q on server %s", n, acc, s)
								}
								return nil
							})
						}
					}
					for _, acc := range []string{"A", "B", "C", "D"} {
						checkResp(acc)
					}
				})
			}
		})
	}
}

func TestMsgTraceServiceImportWithLeafNodeHub(t *testing.T) {
	confHub := createConfFile(t, []byte(`
		listen: 127.0.0.1:-1
		server_name: "S1"
		accounts {
			A {
				users: [{user: a, password: pwd}]
				exports: [ { service: ">", allow_trace: true } ]
				mappings = {
					bar: bozo
				}
			}
			B {
				users: [{user: b, password: pwd}]
				imports: [ { service: {account: "A", subject:">"} } ]
				exports: [ { service: ">", allow_trace: true } ]
			}
			C {
				users: [{user: c, password: pwd}]
				exports: [ { service: ">", allow_trace: true } ]
			}
			D {
				users: [{user: d, password: pwd}]
				imports: [
					{ service: {account: "B", subject:"bar"}, to: baz }
					{ service: {account: "C", subject:">"} }
				]
				mappings = {
						bat: baz
				}
			}
			$SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] }
		}
		leafnodes {
			port: -1
		}
	`))
	hub, ohub := RunServerWithConfig(confHub)
	defer hub.Shutdown()

	confLeaf := createConfFile(t, []byte(fmt.Sprintf(`
		listen: 127.0.0.1:-1
		server_name: "S2"
		leafnodes {
			remotes [{url: "nats://d:pwd@127.0.0.1:%d"}]
		}
	`, ohub.LeafNode.Port)))
	leaf, _ := RunServerWithConfig(confLeaf)
	defer leaf.Shutdown()

	checkLeafNodeConnectedCount(t, hub, 1)
	checkLeafNodeConnectedCount(t, leaf, 1)

	nc2 := natsConnect(t, hub.ClientURL(), nats.UserInfo("a", "pwd"), nats.Name("ServiceA"))
	defer nc2.Close()
	recv := int32(0)
	natsQueueSub(t, nc2, "*", "my_queue", func(m *nats.Msg) {
		atomic.AddInt32(&recv, 1)
		m.Respond(m.Data)
	})
	natsFlush(t, nc2)

	nc3 := natsConnect(t, hub.ClientURL(), nats.UserInfo("c", "pwd"), nats.Name("ServiceC"))
	defer nc3.Close()
	natsSub(t, nc3, "baz", func(m *nats.Msg) {
		atomic.AddInt32(&recv, 1)
		m.Respond(m.Data)
	})
	natsFlush(t, nc3)

	nc := natsConnect(t, leaf.ClientURL(), nats.Name("Requestor"))
	defer nc.Close()

	traceSub := natsSubSync(t, nc, "my.trace.subj")
	sub := natsSubSync(t, nc, "my.service.response.inbox")

	checkSubInterest(t, leaf, globalAccountName, "bat", time.Second)

	for _, test := range []struct {
		name       string
		deliverMsg bool
	}{
		{"just trace", false},
		{"deliver msg", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			msg := nats.NewMsg("bat")
			msg.Header.Set(MsgTraceDest, traceSub.Subject)
			if !test.deliverMsg {
				msg.Header.Set(MsgTraceOnly, "true")
			}
			if !test.deliverMsg {
				msg.Data = []byte("request1")
			} else {
				msg.Data = []byte("request2")
			}
			msg.Reply = sub.Subject

			err := nc.PublishMsg(msg)
			require_NoError(t, err)

			if test.deliverMsg {
				for i := 0; i < 2; i++ {
					appMsg := natsNexMsg(t, sub, time.Second)
					require_Equal[string](t, string(appMsg.Data), "request2")
				}
			}
			// Check that no (more) messages are received.
			if msg, err := sub.NextMsg(100 * time.Millisecond); msg != nil || err != nats.ErrTimeout {
				t.Fatalf("Did not expect application message, got msg=%v err=%v", msg, err)
			}
			if !test.deliverMsg {
				// Just to make sure that message was not delivered to service
				// responders, wait a bit and check the recv value.
				time.Sleep(50 * time.Millisecond)
				if n := atomic.LoadInt32(&recv); n != 0 {
					t.Fatalf("Expected no message to be received, but service callback fired %d times", n)
				}
			}

			check := func() {
				traceMsg := natsNexMsg(t, traceSub, time.Second)
				var e MsgTraceEvent
				json.Unmarshal(traceMsg.Data, &e)

				ingress := e.Ingress()
				require_True(t, ingress != nil)
				switch ingress.Kind {
				case CLIENT:
					require_Equal[string](t, e.Server.Name, "S2")
					require_Equal[string](t, ingress.Account, globalAccountName)
					require_Equal[string](t, ingress.Subject, "bat")
					require_True(t, e.SubjectMapping() == nil)
					egress := e.Egresses()
					require_Equal[int](t, len(egress), 1)
					eg := egress[0]
					require_True(t, eg.Kind == LEAF)
					require_Equal[string](t, eg.Name, "S1")
					require_Equal[string](t, eg.Account, _EMPTY_)
				case LEAF:
					require_Equal[string](t, e.Server.Name, hub.Name())
					require_Equal[string](t, ingress.Name, leaf.Name())
					require_Equal[string](t, ingress.Account, "D")
					require_Equal[string](t, ingress.Subject, "bat")
					sm := e.SubjectMapping()
					require_True(t, sm != nil)
					require_Equal[string](t, sm.MappedTo, "baz")
					simps := e.ServiceImports()
					require_True(t, simps != nil)
					require_Equal[int](t, len(simps), 3)
					for _, si := range simps {
						switch si.Account {
						case "C":
							require_Equal[string](t, si.From, "baz")
							require_Equal[string](t, si.To, "baz")
						case "B":
							require_Equal[string](t, si.From, "baz")
							require_Equal[string](t, si.To, "bar")
						case "A":
							require_Equal[string](t, si.From, "bar")
							require_Equal[string](t, si.To, "bozo")
						default:
							t.Fatalf("Unexpected account name: %s", si.Account)
						}
					}
					egress := e.Egresses()
					require_Equal[int](t, len(egress), 2)
					for _, eg := range egress {
						require_True(t, eg.Kind == CLIENT)
						switch eg.Account {
						case "C":
							require_Equal[string](t, eg.Name, "ServiceC")
							require_Equal[string](t, eg.Subscription, "baz")
							require_Equal[string](t, eg.Queue, _EMPTY_)
						case "A":
							require_Equal[string](t, eg.Name, "ServiceA")
							require_Equal[string](t, eg.Subscription, "*")
							require_Equal[string](t, eg.Queue, "my_queue")
						default:
							t.Fatalf("Unexpected egress: %+v", eg)
						}
					}
				default:
					t.Fatalf("Unexpected ingress: %+v", ingress)
				}
			}
			// We should receive 2 events.
			check()
			check()
			// Make sure we are not receiving more traces
			if tm, err := traceSub.NextMsg(250 * time.Millisecond); err == nil {
				t.Fatalf("Should not have received trace message: %s", tm.Data)
			}

			// Make sure we properly remove the responses.
			checkResp := func(an string) {
				acc, err := hub.lookupAccount(an)
				require_NoError(t, err)
				checkFor(t, time.Second, 15*time.Millisecond, func() error {
					if n := acc.NumPendingAllResponses(); n != 0 {
						return fmt.Errorf("Still %d responses for account %q pending on %s", n, an, hub)
					}
					return nil
				})
			}
			for _, acc := range []string{"A", "B", "C", "D"} {
				checkResp(acc)
			}
		})
	}
}

func TestMsgTraceServiceImportWithLeafNodeLeaf(t *testing.T) {
	confHub := createConfFile(t, []byte(`
		listen: 127.0.0.1:-1
		server_name: "S1"
		accounts {
			A {
				users: [{user: a, password: pwd}]
				exports: [ { service: "bar", allow_trace: true } ]
			}
			B {
				users: [{user: b, password: pwd}]
				imports: [{ service: {account: "A", subject:"bar"}, to: baz }]
			}
			$SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] }
		}
		leafnodes {
			port: -1
		}
	`))
	hub, ohub := RunServerWithConfig(confHub)
	defer hub.Shutdown()

	confLeaf := createConfFile(t, []byte(fmt.Sprintf(`
		listen: 127.0.0.1:-1
		server_name: "S2"
		accounts {
			A {
				users: [{user: a, password: pwd}]
				exports: [ { service: "bar"} ]
			}
			B { users: [{user: b, password: pwd}] }
			$SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] }
		}
		leafnodes {
			remotes [
				{
					url: "nats://a:pwd@127.0.0.1:%d"
					account: A
				}
				{
					url: "nats://b:pwd@127.0.0.1:%d"
					account: B
				}
			]
		}
	`, ohub.LeafNode.Port, ohub.LeafNode.Port)))
	leaf, _ := RunServerWithConfig(confLeaf)
	defer leaf.Shutdown()

	checkLeafNodeConnectedCount(t, hub, 2)
	checkLeafNodeConnectedCount(t, leaf, 2)

	nc2 := natsConnect(t, leaf.ClientURL(), nats.UserInfo("a", "pwd"), nats.Name("ServiceA"))
	defer nc2.Close()
	recv := int32(0)
	natsQueueSub(t, nc2, "*", "my_queue", func(m *nats.Msg) {
		atomic.AddInt32(&recv, 1)
		m.Respond(m.Data)
	})
	natsFlush(t, nc2)

	nc := natsConnect(t, hub.ClientURL(), nats.UserInfo("b", "pwd"), nats.Name("Requestor"))
	defer nc.Close()

	traceSub := natsSubSync(t, nc, "my.trace.subj")
	sub := natsSubSync(t, nc, "my.service.response.inbox")

	// Check that hub has a subscription interest on "baz"
	checkSubInterest(t, hub, "A", "baz", time.Second)
	// And check that the leaf has the sub interest on the trace subject
	checkSubInterest(t, leaf, "B", traceSub.Subject, time.Second)

	for _, test := range []struct {
		name       string
		deliverMsg bool
	}{
		{"just trace", false},
		{"deliver msg", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			msg := nats.NewMsg("baz")
			msg.Header.Set(MsgTraceDest, traceSub.Subject)
			if !test.deliverMsg {
				msg.Header.Set(MsgTraceOnly, "true")
			}
			if !test.deliverMsg {
				msg.Data = []byte("request1")
			} else {
				msg.Data = []byte("request2")
			}
			msg.Reply = sub.Subject

			err := nc.PublishMsg(msg)
			require_NoError(t, err)

			if test.deliverMsg {
				appMsg := natsNexMsg(t, sub, time.Second)
				require_Equal[string](t, string(appMsg.Data), "request2")
			}
			// Check that no (more) messages are received.
			if msg, err := sub.NextMsg(100 * time.Millisecond); msg != nil || (err != nats.ErrTimeout && err != nats.ErrNoResponders) {
				t.Fatalf("Did not expect application message, got msg=%v err=%v", msg, err)
			}
			if !test.deliverMsg {
				// Just to make sure that message was not delivered to service
				// responders, wait a bit and check the recv value.
				time.Sleep(50 * time.Millisecond)
				if n := atomic.LoadInt32(&recv); n != 0 {
					t.Fatalf("Expected no message to be received, but service callback fired %d times", n)
				}
			}

			check := func() {
				traceMsg := natsNexMsg(t, traceSub, time.Second)
				var e MsgTraceEvent
				json.Unmarshal(traceMsg.Data, &e)

				ingress := e.Ingress()
				require_True(t, ingress != nil)

				switch ingress.Kind {
				case CLIENT:
					require_Equal[string](t, e.Server.Name, "S1")
					require_Equal[string](t, ingress.Name, "Requestor")
					require_Equal[string](t, ingress.Account, "B")
					require_Equal[string](t, ingress.Subject, "baz")
					require_True(t, e.SubjectMapping() == nil)
					simps := e.ServiceImports()
					require_True(t, simps != nil)
					require_Equal[int](t, len(simps), 1)
					si := simps[0]
					require_Equal[string](t, si.Account, "A")
					require_Equal[string](t, si.From, "baz")
					require_Equal[string](t, si.To, "bar")
					egress := e.Egresses()
					require_Equal[int](t, len(egress), 1)
					eg := egress[0]
					require_True(t, eg.Kind == LEAF)
					require_Equal[string](t, eg.Name, "S2")
					require_Equal[string](t, eg.Account, "A")
					require_Equal[string](t, eg.Subscription, _EMPTY_)
				case LEAF:
					require_Equal[string](t, e.Server.Name, leaf.Name())
					require_Equal[string](t, ingress.Name, hub.Name())
					require_Equal[string](t, ingress.Account, "A")
					require_Equal[string](t, ingress.Subject, "bar")
					require_True(t, e.SubjectMapping() == nil)
					require_True(t, e.ServiceImports() == nil)
					egress := e.Egresses()
					require_Equal[int](t, len(egress), 1)
					eg := egress[0]
					require_True(t, eg.Kind == CLIENT)
					require_Equal[string](t, eg.Name, "ServiceA")
					require_Equal[string](t, eg.Subscription, "*")
					require_Equal[string](t, eg.Queue, "my_queue")
				default:
					t.Fatalf("Unexpected ingress: %+v", ingress)
				}
			}
			// We should receive 2 events.
			check()
			check()
			// Make sure we are not receiving more traces
			if tm, err := traceSub.NextMsg(250 * time.Millisecond); err == nil {
				t.Fatalf("Should not have received trace message: %s", tm.Data)
			}

			// Make sure we properly remove the responses.
			checkResp := func(an string) {
				acc, err := leaf.lookupAccount(an)
				require_NoError(t, err)
				checkFor(t, time.Second, 15*time.Millisecond, func() error {
					if n := acc.NumPendingAllResponses(); n != 0 {
						return fmt.Errorf("Still %d responses for account %q pending on %s", n, an, leaf)
					}
					return nil
				})
			}
			for _, acc := range []string{"A", "B"} {
				checkResp(acc)
			}
		})
	}
}

func TestMsgTraceStreamExport(t *testing.T) {
	tmpl := `
		listen: 127.0.0.1:-1
		accounts {
			A {
				users: [{user: a, password: pwd}]
				exports: [
					{ stream: "info.*.*.>"}
				]
			}
			B {
				users: [{user: b, password: pwd}]
				imports: [ { stream: {account: "A", subject:"info.*.*.>"}, to: "B.info.$2.$1.>", allow_trace: %v } ]
			}
			C {
				users: [{user: c, password: pwd}]
				imports: [ { stream: {account: "A", subject:"info.*.*.>"}, to: "C.info.$1.$2.>", allow_trace: %v } ]
			}
		}
	`
	conf := createConfFile(t, []byte(fmt.Sprintf(tmpl, false, false)))
	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	nc := natsConnect(t, s.ClientURL(), nats.UserInfo("a", "pwd"), nats.Name("Tracer"))
	defer nc.Close()
	traceSub := natsSubSync(t, nc, "my.trace.subj")

	nc2 := natsConnect(t, s.ClientURL(), nats.UserInfo("b", "pwd"), nats.Name("sub1"))
	defer nc2.Close()
	sub1 := natsSubSync(t, nc2, "B.info.*.*.>")
	natsFlush(t, nc2)

	nc3 := natsConnect(t, s.ClientURL(), nats.UserInfo("c", "pwd"), nats.Name("sub2"))
	defer nc3.Close()
	sub2 := natsQueueSubSync(t, nc3, "C.info.>", "my_queue")
	natsFlush(t, nc3)

	for mainIter, mainTest := range []struct {
		name  string
		allow bool
	}{
		{"not allowed", false},
		{"allowed", true},
		{"not allowed again", false},
	} {
		t.Run(mainTest.name, func(t *testing.T) {
			for _, test := range []struct {
				name       string
				deliverMsg bool
			}{
				{"just trace", false},
				{"deliver msg", true},
			} {
				t.Run(test.name, func(t *testing.T) {
					msg := nats.NewMsg("info.11.22.bar")
					msg.Header.Set(MsgTraceDest, traceSub.Subject)
					if !test.deliverMsg {
						msg.Header.Set(MsgTraceOnly, "true")
					}
					msg.Data = []byte("hello")

					err := nc.PublishMsg(msg)
					require_NoError(t, err)

					if test.deliverMsg {
						appMsg := natsNexMsg(t, sub1, time.Second)
						require_Equal[string](t, appMsg.Subject, "B.info.22.11.bar")
						appMsg = natsNexMsg(t, sub2, time.Second)
						require_Equal[string](t, appMsg.Subject, "C.info.11.22.bar")
					}
					// Check that no (more) messages are received.
					for _, sub := range []*nats.Subscription{sub1, sub2} {
						if msg, err := sub.NextMsg(100 * time.Millisecond); msg != nil || err != nats.ErrTimeout {
							t.Fatalf("Did not expect application message, got msg=%v err=%v", msg, err)
						}
					}

					traceMsg := natsNexMsg(t, traceSub, time.Second)
					var e MsgTraceEvent
					json.Unmarshal(traceMsg.Data, &e)

					require_Equal[string](t, e.Server.Name, s.Name())
					ingress := e.Ingress()
					require_True(t, ingress != nil)
					require_True(t, ingress.Kind == CLIENT)
					require_Equal[string](t, ingress.Account, "A")
					require_Equal[string](t, ingress.Subject, "info.11.22.bar")
					require_True(t, e.SubjectMapping() == nil)
					require_True(t, e.ServiceImports() == nil)
					stexps := e.StreamExports()
					require_True(t, stexps != nil)
					require_Equal[int](t, len(stexps), 2)
					for _, se := range stexps {
						require_True(t, se.Timestamp != time.Time{})
						switch se.Account {
						case "B":
							require_Equal[string](t, se.To, "B.info.22.11.bar")
						case "C":
							require_Equal[string](t, se.To, "C.info.11.22.bar")
						default:
							t.Fatalf("Unexpected stream export: %+v", se)
						}
					}
					egress := e.Egresses()
					if mainTest.allow {
						require_Equal[int](t, len(egress), 2)
						for _, eg := range egress {
							require_True(t, eg.Kind == CLIENT)
							switch eg.Account {
							case "B":
								require_Equal[string](t, eg.Name, "sub1")
								require_Equal[string](t, eg.Subscription, "info.*.*.>")
								require_Equal[string](t, eg.Queue, _EMPTY_)
							case "C":
								require_Equal[string](t, eg.Name, "sub2")
								require_Equal[string](t, eg.Subscription, "info.*.*.>")
								require_Equal[string](t, eg.Queue, "my_queue")
							default:
								t.Fatalf("Unexpected egress: %+v", eg)
							}
						}
					} else {
						require_Equal[int](t, len(egress), 0)
					}
				})
			}
			switch mainIter {
			case 0:
				reloadUpdateConfig(t, s, conf, fmt.Sprintf(tmpl, true, true))
			case 1:
				reloadUpdateConfig(t, s, conf, fmt.Sprintf(tmpl, false, false))
			}
		})
	}
}

func TestMsgTraceStreamExportWithSuperCluster(t *testing.T) {
	for _, mainTest := range []struct {
		name     string
		allowStr string
		allow    bool
	}{
		{"allowed", "true", true},
		{"not allowed", "false", false},
	} {
		t.Run(mainTest.name, func(t *testing.T) {
			tmpl := `
				listen: 127.0.0.1:-1
				server_name: %s
				jetstream: {max_mem_store: 256MB, max_file_store: 2GB, store_dir: '%s'}

				cluster {
					name: %s
					listen: 127.0.0.1:%d
					routes = [%s]
				}
				accounts {
					A {
						users: [{user: a, password: pwd}]
						exports: [
							{ stream: "info.*.*.>"}
						]
					}
					B {
						users: [{user: b, password: pwd}]
						imports: [ { stream: {account: "A", subject:"info.*.*.>"}, to: "B.info.$2.$1.>", allow_trace: ` + mainTest.allowStr + ` } ]
					}
					C {
						users: [{user: c, password: pwd}]
						imports: [ { stream: {account: "A", subject:"info.*.*.>"}, to: "C.info.$1.$2.>", allow_trace: ` + mainTest.allowStr + ` } ]
					}
					$SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] }
				}
			`
			sc := createJetStreamSuperClusterWithTemplate(t, tmpl, 2, 2)
			defer sc.shutdown()

			sfornc := sc.clusters[0].servers[0]
			nc := natsConnect(t, sfornc.ClientURL(), nats.UserInfo("a", "pwd"), nats.Name("Tracer"))
			defer nc.Close()
			traceSub := natsSubSync(t, nc, "my.trace.subj")

			sfornc2 := sc.clusters[0].servers[1]
			nc2 := natsConnect(t, sfornc2.ClientURL(), nats.UserInfo("b", "pwd"), nats.Name("sub1"))
			defer nc2.Close()
			sub1 := natsSubSync(t, nc2, "B.info.*.*.>")
			natsFlush(t, nc2)
			checkSubInterest(t, sfornc2, "A", traceSub.Subject, time.Second)

			sfornc3 := sc.clusters[1].servers[0]
			nc3 := natsConnect(t, sfornc3.ClientURL(), nats.UserInfo("c", "pwd"), nats.Name("sub2"))
			defer nc3.Close()
			sub2 := natsQueueSubSync(t, nc3, "C.info.>", "my_queue")
			natsFlush(t, nc3)

			checkSubInterest(t, sfornc, "A", "info.1.2.3.4", time.Second)
			for _, s := range sc.clusters[0].servers {
				checkForRegisteredQSubInterest(t, s, "C2", "A", "info.1.2.3", 1, time.Second)
			}

			for _, test := range []struct {
				name       string
				deliverMsg bool
			}{
				{"just trace", false},
				{"deliver msg", true},
			} {
				t.Run(test.name, func(t *testing.T) {
					msg := nats.NewMsg("info.11.22.bar")
					msg.Header.Set(MsgTraceDest, traceSub.Subject)
					if !test.deliverMsg {
						msg.Header.Set(MsgTraceOnly, "true")
					}
					msg.Data = []byte("hello")

					err := nc.PublishMsg(msg)
					require_NoError(t, err)

					if test.deliverMsg {
						appMsg := natsNexMsg(t, sub1, time.Second)
						require_Equal[string](t, appMsg.Subject, "B.info.22.11.bar")
						appMsg = natsNexMsg(t, sub2, time.Second)
						require_Equal[string](t, appMsg.Subject, "C.info.11.22.bar")
					}
					// Check that no (more) messages are received.
					for _, sub := range []*nats.Subscription{sub1, sub2} {
						if msg, err := sub.NextMsg(100 * time.Millisecond); msg != nil || err != nats.ErrTimeout {
							t.Fatalf("Did not expect application message, got msg=%v err=%v", msg, err)
						}
					}

					check := func() {
						traceMsg := natsNexMsg(t, traceSub, time.Second)
						var e MsgTraceEvent
						json.Unmarshal(traceMsg.Data, &e)

						ingress := e.Ingress()
						require_True(t, ingress != nil)
						switch ingress.Kind {
						case CLIENT:
							require_Equal[string](t, e.Server.Name, sfornc.Name())
							require_Equal[string](t, ingress.Name, "Tracer")
							require_Equal[string](t, ingress.Account, "A")
							require_Equal[string](t, ingress.Subject, "info.11.22.bar")
							require_True(t, e.SubjectMapping() == nil)
							require_True(t, e.ServiceImports() == nil)
							require_True(t, e.StreamExports() == nil)
							egress := e.Egresses()
							require_Equal[int](t, len(egress), 2)
							for _, eg := range egress {
								switch eg.Kind {
								case ROUTER:
									require_Equal[string](t, eg.Name, sfornc2.Name())
									require_Equal[string](t, eg.Account, _EMPTY_)
								case GATEWAY:
									require_Equal[string](t, eg.Name, sfornc3.Name())
									require_Equal[string](t, eg.Account, _EMPTY_)
								default:
									t.Fatalf("Unexpected egress: %+v", eg)
								}
							}
						case ROUTER:
							require_Equal[string](t, e.Server.Name, sfornc2.Name())
							require_Equal[string](t, ingress.Name, sfornc.Name())
							require_Equal[string](t, ingress.Account, "A")
							require_Equal[string](t, ingress.Subject, "info.11.22.bar")
							require_True(t, e.SubjectMapping() == nil)
							require_True(t, e.ServiceImports() == nil)
							stexps := e.StreamExports()
							require_True(t, stexps != nil)
							require_Equal[int](t, len(stexps), 1)
							se := stexps[0]
							require_Equal[string](t, se.Account, "B")
							require_Equal[string](t, se.To, "B.info.22.11.bar")
							egress := e.Egresses()
							if mainTest.allow {
								require_Equal[int](t, len(egress), 1)
								eg := egress[0]
								require_True(t, eg.Kind == CLIENT)
								require_Equal[string](t, eg.Name, "sub1")
								require_Equal[string](t, eg.Subscription, "info.*.*.>")
								require_Equal[string](t, eg.Queue, _EMPTY_)
							} else {
								require_Equal[int](t, len(egress), 0)
							}
						case GATEWAY:
							require_Equal[string](t, e.Server.Name, sfornc3.Name())
							require_Equal[string](t, ingress.Name, sfornc.Name())
							require_Equal[string](t, ingress.Account, "A")
							require_Equal[string](t, ingress.Subject, "info.11.22.bar")
							require_True(t, e.SubjectMapping() == nil)
							require_True(t, e.ServiceImports() == nil)
							stexps := e.StreamExports()
							require_True(t, stexps != nil)
							require_Equal[int](t, len(stexps), 1)
							se := stexps[0]
							require_Equal[string](t, se.Account, "C")
							require_Equal[string](t, se.To, "C.info.11.22.bar")
							egress := e.Egresses()
							if mainTest.allow {
								require_Equal[int](t, len(egress), 1)
								eg := egress[0]
								require_True(t, eg.Kind == CLIENT)
								require_Equal[string](t, eg.Name, "sub2")
								require_Equal[string](t, eg.Subscription, "info.*.*.>")
								require_Equal[string](t, eg.Queue, "my_queue")
							} else {
								require_Equal[int](t, len(egress), 0)
							}
						default:
							t.Fatalf("Unexpected ingress: %+v", ingress)
						}
					}
					// We expect 3 events
					check()
					check()
					check()
					// Make sure we are not receiving more traces
					if tm, err := traceSub.NextMsg(250 * time.Millisecond); err == nil {
						t.Fatalf("Should not have received trace message: %s", tm.Data)
					}
				})
			}
		})
	}
}

func TestMsgTraceStreamExportWithLeafNode_Hub(t *testing.T) {
	confHub := createConfFile(t, []byte(`
		listen: 127.0.0.1:-1
		server_name: "S1"
		accounts {
			A {
				users: [{user: a, password: pwd}]
				exports: [
					{ stream: "info.*.*.>"}
				]
			}
			B {
				users: [{user: b, password: pwd}]
				imports: [ { stream: {account: "A", subject:"info.*.*.>"}, to: "B.info.$2.$1.>", allow_trace: true } ]
			}
			C {
				users: [{user: c, password: pwd}]
				imports: [ { stream: {account: "A", subject:"info.*.*.>"}, to: "C.info.$1.$2.>", allow_trace: true } ]
			}
		}
		leafnodes {
			port: -1
		}
	`))
	hub, ohub := RunServerWithConfig(confHub)
	defer hub.Shutdown()

	confLeaf := createConfFile(t, []byte(fmt.Sprintf(`
		listen: 127.0.0.1:-1
		server_name: "S2"
		accounts {
			LEAF { users: [{user: leaf, password: pwd}] }
		}
		leafnodes {
			remotes [
				{ url: "nats://a:pwd@127.0.0.1:%d", account: "LEAF" }
			]
		}
	`, ohub.LeafNode.Port)))
	leaf, _ := RunServerWithConfig(confLeaf)
	defer leaf.Shutdown()

	checkLeafNodeConnectedCount(t, hub, 1)
	checkLeafNodeConnectedCount(t, leaf, 1)

	nc := natsConnect(t, leaf.ClientURL(), nats.UserInfo("leaf", "pwd"), nats.Name("Tracer"))
	defer nc.Close()
	traceSub := natsSubSync(t, nc, "my.trace.subj")

	checkSubInterest(t, hub, "A", traceSub.Subject, time.Second)

	nc2 := natsConnect(t, hub.ClientURL(), nats.UserInfo("b", "pwd"), nats.Name("sub1"))
	defer nc2.Close()
	sub1 := natsSubSync(t, nc2, "B.info.*.*.>")
	natsFlush(t, nc2)

	nc3 := natsConnect(t, hub.ClientURL(), nats.UserInfo("c", "pwd"), nats.Name("sub2"))
	defer nc3.Close()
	sub2 := natsQueueSubSync(t, nc3, "C.info.>", "my_queue")
	natsFlush(t, nc3)

	acc, err := leaf.LookupAccount("LEAF")
	require_NoError(t, err)
	checkFor(t, time.Second, 50*time.Millisecond, func() error {
		acc.mu.RLock()
		sl := acc.sl
		acc.mu.RUnlock()
		r := sl.Match("info.1.2.3")
		ok := len(r.psubs) > 0
		if ok && (len(r.qsubs) == 0 || len(r.qsubs[0]) == 0) {
			ok = false
		}
		if !ok {
			return fmt.Errorf("Subscription interest not yet propagated")
		}
		return nil
	})

	for _, test := range []struct {
		name       string
		deliverMsg bool
	}{

		{"just trace", false},
		{"deliver msg", true},
	} {

		t.Run(test.name, func(t *testing.T) {
			msg := nats.NewMsg("info.11.22.bar")
			msg.Header.Set(MsgTraceDest, traceSub.Subject)
			if !test.deliverMsg {
				msg.Header.Set(MsgTraceOnly, "true")
			}
			msg.Data = []byte("hello")

			err := nc.PublishMsg(msg)
			require_NoError(t, err)

			if test.deliverMsg {
				appMsg := natsNexMsg(t, sub1, time.Second)
				require_Equal[string](t, appMsg.Subject, "B.info.22.11.bar")
				appMsg = natsNexMsg(t, sub2, time.Second)
				require_Equal[string](t, appMsg.Subject, "C.info.11.22.bar")
			}
			// Check that no (more) messages are received.
			for _, sub := range []*nats.Subscription{sub1, sub2} {
				if msg, err := sub.NextMsg(100 * time.Millisecond); msg != nil || err != nats.ErrTimeout {
					t.Fatalf("Did not expect application message, got msg=%v err=%v", msg, err)
				}
			}
			check := func() {
				traceMsg := natsNexMsg(t, traceSub, time.Second)
				var e MsgTraceEvent
				json.Unmarshal(traceMsg.Data, &e)

				ingress := e.Ingress()
				require_True(t, ingress != nil)

				switch ingress.Kind {
				case CLIENT:
					require_Equal[string](t, e.Server.Name, leaf.Name())
					require_Equal[string](t, ingress.Name, "Tracer")
					require_Equal[string](t, ingress.Account, "LEAF")
					require_Equal[string](t, ingress.Subject, "info.11.22.bar")
					require_True(t, e.SubjectMapping() == nil)
					require_True(t, e.ServiceImports() == nil)
					require_True(t, e.StreamExports() == nil)
					egress := e.Egresses()
					require_Equal[int](t, len(egress), 1)
					eg := egress[0]
					require_True(t, eg.Kind == LEAF)
					require_Equal[string](t, eg.Name, hub.Name())
					require_Equal[string](t, eg.Account, _EMPTY_)
					require_Equal[string](t, eg.Subscription, _EMPTY_)
					require_Equal[string](t, eg.Queue, _EMPTY_)
				case LEAF:
					require_Equal[string](t, e.Server.Name, hub.Name())
					require_Equal[string](t, ingress.Name, leaf.Name())
					require_Equal[string](t, ingress.Account, "A")
					require_Equal[string](t, ingress.Subject, "info.11.22.bar")
					require_True(t, e.SubjectMapping() == nil)
					require_True(t, e.ServiceImports() == nil)
					stexps := e.StreamExports()
					require_True(t, stexps != nil)
					require_Equal[int](t, len(stexps), 2)
					for _, se := range stexps {
						switch se.Account {
						case "B":
							require_Equal[string](t, se.To, "B.info.22.11.bar")
						case "C":
							require_Equal[string](t, se.To, "C.info.11.22.bar")
						default:
							t.Fatalf("Unexpected stream export: %+v", se)
						}
					}
					egress := e.Egresses()
					require_Equal[int](t, len(egress), 2)
					for _, eg := range egress {
						require_True(t, eg.Kind == CLIENT)
						switch eg.Account {
						case "B":
							require_Equal[string](t, eg.Name, "sub1")
							require_Equal[string](t, eg.Subscription, "info.*.*.>")
							require_Equal[string](t, eg.Queue, _EMPTY_)
						case "C":
							require_Equal[string](t, eg.Name, "sub2")
							require_Equal[string](t, eg.Subscription, "info.*.*.>")
							require_Equal[string](t, eg.Queue, "my_queue")
						default:
							t.Fatalf("Unexpected egress: %+v", eg)
						}
					}
				default:
					t.Fatalf("Unexpected ingress: %+v", ingress)
				}
			}
			// We expect 2 events
			check()
			check()
			// Make sure we are not receiving more traces
			if tm, err := traceSub.NextMsg(250 * time.Millisecond); err == nil {
				t.Fatalf("Should not have received trace message: %s", tm.Data)
			}
		})
	}
}

func TestMsgTraceStreamExportWithLeafNode_Leaf(t *testing.T) {
	confHub := createConfFile(t, []byte(`
		listen: 127.0.0.1:-1
		server_name: "S1"
		accounts {
			HUB { users: [{user: hub, password: pwd}] }
		}
		leafnodes {
			port: -1
		}
	`))
	hub, ohub := RunServerWithConfig(confHub)
	defer hub.Shutdown()

	confLeaf := createConfFile(t, []byte(fmt.Sprintf(`
		listen: 127.0.0.1:-1
		server_name: "S2"
		accounts {
			A {
				users: [{user: a, password: pwd}]
				exports: [
					{ stream: "info.*.*.>"}
				]
			}
			B {
				users: [{user: b, password: pwd}]
				imports: [ { stream: {account: "A", subject:"info.*.*.>"}, to: "B.info.$2.$1.>", allow_trace: true } ]
			}
			C {
				users: [{user: c, password: pwd}]
				imports: [ { stream: {account: "A", subject:"info.*.*.>"}, to: "C.info.$1.$2.>", allow_trace: true } ]
			}
		}
		leafnodes {
			remotes [
				{ url: "nats://hub:pwd@127.0.0.1:%d", account: "A" }
			]
		}
	`, ohub.LeafNode.Port)))
	leaf, _ := RunServerWithConfig(confLeaf)
	defer leaf.Shutdown()

	checkLeafNodeConnectedCount(t, hub, 1)
	checkLeafNodeConnectedCount(t, leaf, 1)

	nc := natsConnect(t, hub.ClientURL(), nats.UserInfo("hub", "pwd"), nats.Name("Tracer"))
	defer nc.Close()
	traceSub := natsSubSync(t, nc, "my.trace.subj")

	checkSubInterest(t, leaf, "A", traceSub.Subject, time.Second)

	nc2 := natsConnect(t, leaf.ClientURL(), nats.UserInfo("b", "pwd"), nats.Name("sub1"))
	defer nc2.Close()
	sub1 := natsSubSync(t, nc2, "B.info.*.*.>")
	natsFlush(t, nc2)

	nc3 := natsConnect(t, leaf.ClientURL(), nats.UserInfo("c", "pwd"), nats.Name("sub2"))
	defer nc3.Close()
	sub2 := natsQueueSubSync(t, nc3, "C.info.>", "my_queue")
	natsFlush(t, nc3)

	acc, err := hub.LookupAccount("HUB")
	require_NoError(t, err)
	checkFor(t, time.Second, 50*time.Millisecond, func() error {
		acc.mu.RLock()
		sl := acc.sl
		acc.mu.RUnlock()
		r := sl.Match("info.1.2.3")
		ok := len(r.psubs) > 0
		if ok && (len(r.qsubs) == 0 || len(r.qsubs[0]) == 0) {
			ok = false
		}
		if !ok {
			return fmt.Errorf("Subscription interest not yet propagated")
		}
		return nil
	})

	for _, test := range []struct {
		name       string
		deliverMsg bool
	}{

		{"just trace", false},
		{"deliver msg", true},
	} {

		t.Run(test.name, func(t *testing.T) {
			msg := nats.NewMsg("info.11.22.bar")
			msg.Header.Set(MsgTraceDest, traceSub.Subject)
			if !test.deliverMsg {
				msg.Header.Set(MsgTraceOnly, "true")
			}
			msg.Data = []byte("hello")

			err := nc.PublishMsg(msg)
			require_NoError(t, err)

			if test.deliverMsg {
				appMsg := natsNexMsg(t, sub1, time.Second)
				require_Equal[string](t, appMsg.Subject, "B.info.22.11.bar")
				appMsg = natsNexMsg(t, sub2, time.Second)
				require_Equal[string](t, appMsg.Subject, "C.info.11.22.bar")
			}
			// Check that no (more) messages are received.
			for _, sub := range []*nats.Subscription{sub1, sub2} {
				if msg, err := sub.NextMsg(100 * time.Millisecond); msg != nil || err != nats.ErrTimeout {
					t.Fatalf("Did not expect application message, got msg=%v err=%v", msg, err)
				}
			}
			check := func() {
				traceMsg := natsNexMsg(t, traceSub, time.Second)
				var e MsgTraceEvent
				json.Unmarshal(traceMsg.Data, &e)

				ingress := e.Ingress()
				require_True(t, ingress != nil)

				switch ingress.Kind {
				case CLIENT:
					require_Equal[string](t, e.Server.Name, hub.Name())
					require_Equal[string](t, ingress.Name, "Tracer")
					require_Equal[string](t, ingress.Account, "HUB")
					require_Equal[string](t, ingress.Subject, "info.11.22.bar")
					require_True(t, e.SubjectMapping() == nil)
					require_True(t, e.ServiceImports() == nil)
					require_True(t, e.StreamExports() == nil)
					egress := e.Egresses()
					require_Equal[int](t, len(egress), 1)
					eg := egress[0]
					require_True(t, eg.Kind == LEAF)
					require_Equal[string](t, eg.Name, leaf.Name())
					require_Equal[string](t, eg.Account, _EMPTY_)
					require_Equal[string](t, eg.Subscription, _EMPTY_)
					require_Equal[string](t, eg.Queue, _EMPTY_)
				case LEAF:
					require_Equal[string](t, e.Server.Name, leaf.Name())
					require_Equal[string](t, ingress.Name, hub.Name())
					require_Equal[string](t, ingress.Account, "A")
					require_Equal[string](t, ingress.Subject, "info.11.22.bar")
					require_True(t, e.SubjectMapping() == nil)
					require_True(t, e.ServiceImports() == nil)
					stexps := e.StreamExports()
					require_True(t, stexps != nil)
					require_Equal[int](t, len(stexps), 2)
					for _, se := range stexps {
						switch se.Account {
						case "B":
							require_Equal[string](t, se.To, "B.info.22.11.bar")
						case "C":
							require_Equal[string](t, se.To, "C.info.11.22.bar")
						default:
							t.Fatalf("Unexpected stream export: %+v", se)
						}
					}
					egress := e.Egresses()
					require_Equal[int](t, len(egress), 2)
					for _, eg := range egress {
						require_True(t, eg.Kind == CLIENT)
						switch eg.Account {
						case "B":
							require_Equal[string](t, eg.Name, "sub1")
							require_Equal[string](t, eg.Subscription, "info.*.*.>")
							require_Equal[string](t, eg.Queue, _EMPTY_)
						case "C":
							require_Equal[string](t, eg.Name, "sub2")
							require_Equal[string](t, eg.Subscription, "info.*.*.>")
							require_Equal[string](t, eg.Queue, "my_queue")
						default:
							t.Fatalf("Unexpected egress: %+v", eg)
						}
					}
				default:
					t.Fatalf("Unexpected ingress: %+v", ingress)
				}
			}
			// We expect 2 events
			check()
			check()
			// Make sure we are not receiving more traces
			if tm, err := traceSub.NextMsg(250 * time.Millisecond); err == nil {
				t.Fatalf("Should not have received trace message: %s", tm.Data)
			}
		})
	}
}

func TestMsgTraceJetStream(t *testing.T) {
	opts := DefaultTestOptions
	opts.Port = -1
	opts.JetStream = true
	opts.JetStreamMaxMemory = 270
	opts.StoreDir = t.TempDir()
	s := RunServer(&opts)
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	cfg := &nats.StreamConfig{
		Name:        "TEST",
		Storage:     nats.MemoryStorage,
		Subjects:    []string{"foo"},
		Replicas:    1,
		AllowRollup: true,
		SubjectTransform: &nats.SubjectTransformConfig{
			Source:      "foo",
			Destination: "bar",
		},
	}
	_, err := js.AddStream(cfg)
	require_NoError(t, err)

	nct := natsConnect(t, s.ClientURL(), nats.Name("Tracer"))
	defer nct.Close()

	traceSub := natsSubSync(t, nct, "my.trace.subj")
	natsFlush(t, nct)

	msg := nats.NewMsg("foo")
	msg.Header.Set(JSMsgId, "MyId")
	msg.Data = make([]byte, 50)
	_, err = js.PublishMsg(msg)
	require_NoError(t, err)

	checkStream := func(t *testing.T, expected int) {
		t.Helper()
		checkFor(t, time.Second, 15*time.Millisecond, func() error {
			si, err := js.StreamInfo("TEST")
			if err != nil {
				return err
			}
			if n := si.State.Msgs; int(n) != expected {
				return fmt.Errorf("Expected %d messages, got %v", expected, n)
			}
			return nil
		})
	}
	checkStream(t, 1)

	for _, test := range []struct {
		name       string
		deliverMsg bool
	}{
		{"just trace", false},
		{"deliver msg", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			msg := nats.NewMsg("foo")
			msg.Header.Set(MsgTraceDest, traceSub.Subject)
			if !test.deliverMsg {
				msg.Header.Set(MsgTraceOnly, "true")
			}
			msg.Data = make([]byte, 50)
			err = nct.PublishMsg(msg)
			require_NoError(t, err)

			// Wait a bit and check if message should be in the stream or not.
			time.Sleep(50 * time.Millisecond)
			if test.deliverMsg {
				checkStream(t, 2)
			} else {
				checkStream(t, 1)
			}

			traceMsg := natsNexMsg(t, traceSub, time.Second)
			var e MsgTraceEvent
			json.Unmarshal(traceMsg.Data, &e)
			require_Equal[string](t, e.Server.Name, s.Name())
			ingress := e.Ingress()
			require_True(t, ingress != nil)
			require_True(t, ingress.Kind == CLIENT)
			require_Equal[string](t, ingress.Name, "Tracer")
			require_Equal[int](t, len(e.Egresses()), 0)
			js := e.JetStream()
			require_True(t, js != nil)
			require_True(t, js.Timestamp != time.Time{})
			require_Equal[string](t, js.Stream, "TEST")
			require_Equal[string](t, js.Subject, "bar")
			require_False(t, js.NoInterest)
			require_Equal[string](t, js.Error, _EMPTY_)
		})
	}

	jst, err := nct.JetStream()
	require_NoError(t, err)

	mset, err := s.globalAccount().lookupStream("TEST")
	require_NoError(t, err)

	// Now we will not ask for delivery and use headers that will fail checks
	// and make sure that message is not added, that the stream's clfs is not
	// increased, and that the JS trace shows the error.
	newMsg := func() *nats.Msg {
		msg = nats.NewMsg("foo")
		msg.Header.Set(MsgTraceDest, traceSub.Subject)
		msg.Header.Set(MsgTraceOnly, "true")
		msg.Data = []byte("hello")
		return msg
	}

	msgCount := 2
	for _, test := range []struct {
		name        string
		headerName  string
		headerVal   string
		expectedErr string
		special     int
	}{
		{"unexpected stream name", JSExpectedStream, "WRONG", "expected stream does not match", 0},
		{"duplicate id", JSMsgId, "MyId", "duplicate", 0},
		{"last seq by subject mismatch", JSExpectedLastSubjSeq, "10", "last sequence by subject mismatch", 0},
		{"last seq mismatch", JSExpectedLastSeq, "10", "last sequence mismatch", 0},
		{"last msgid mismatch", JSExpectedLastMsgId, "MyId3", "last msgid mismatch", 0},
		{"invalid rollup command", JSMsgRollup, "wrong", "rollup value invalid: \"wrong\"", 0},
		{"rollup not permitted", JSMsgRollup, JSMsgRollupSubject, "rollup not permitted", 1},
		{"max msg size", _EMPTY_, _EMPTY_, ErrMaxPayload.Error(), 2},
		{"normal message ok", _EMPTY_, _EMPTY_, _EMPTY_, 3},
		{"insufficient resources", _EMPTY_, _EMPTY_, NewJSInsufficientResourcesError().Error(), 0},
		{"stream sealed", _EMPTY_, _EMPTY_, NewJSStreamSealedError().Error(), 4},
	} {
		t.Run(test.name, func(t *testing.T) {
			msg = newMsg()
			if test.headerName != _EMPTY_ {
				msg.Header.Set(test.headerName, test.headerVal)
			}
			switch test.special {
			case 1:
				// Update stream to prevent rollups, and set a max size.
				cfg.AllowRollup = false
				cfg.MaxMsgSize = 100
				_, err = js.UpdateStream(cfg)
				require_NoError(t, err)
			case 2:
				msg.Data = make([]byte, 200)
			case 3:
				pa, err := jst.Publish("foo", make([]byte, 100))
				require_NoError(t, err)
				msgCount++
				checkStream(t, msgCount)
				require_Equal[uint64](t, pa.Sequence, 3)
				return
			case 4:
				cfg.Sealed = true
				_, err = js.UpdateStream(cfg)
				require_NoError(t, err)
			default:
			}
			jst.PublishMsg(msg)

			// Message count should not have increased and stay at 2.
			checkStream(t, msgCount)
			// Check that clfs does not increase
			mset.mu.RLock()
			clfs := mset.getCLFS()
			mset.mu.RUnlock()
			if clfs != 0 {
				t.Fatalf("Stream's clfs was expected to be 0, is %d", clfs)
			}
			traceMsg := natsNexMsg(t, traceSub, time.Second)
			var e MsgTraceEvent
			json.Unmarshal(traceMsg.Data, &e)
			require_Equal[string](t, e.Server.Name, s.Name())
			ingress := e.Ingress()
			require_True(t, ingress != nil)
			require_True(t, ingress.Kind == CLIENT)
			require_Equal[string](t, ingress.Name, "Tracer")
			require_Equal[int](t, len(e.Egresses()), 0)
			js := e.JetStream()
			require_True(t, js != nil)
			require_Equal[string](t, js.Stream, "TEST")
			require_Equal[string](t, js.Subject, _EMPTY_)
			require_False(t, js.NoInterest)
			if et := js.Error; !strings.Contains(et, test.expectedErr) {
				t.Fatalf("Expected JS error to contain %q, got %q", test.expectedErr, et)
			}
		})
	}

	// Create a stream with interest retention policy
	_, err = js.AddStream(&nats.StreamConfig{
		Name:      "NO_INTEREST",
		Subjects:  []string{"baz"},
		Retention: nats.InterestPolicy,
	})
	require_NoError(t, err)
	msg = nats.NewMsg("baz")
	msg.Header.Set(MsgTraceDest, traceSub.Subject)
	msg.Header.Set(MsgTraceOnly, "true")
	msg.Data = []byte("hello")
	err = nct.PublishMsg(msg)
	require_NoError(t, err)

	traceMsg := natsNexMsg(t, traceSub, time.Second)
	var e MsgTraceEvent
	json.Unmarshal(traceMsg.Data, &e)
	require_Equal[string](t, e.Server.Name, s.Name())
	ingress := e.Ingress()
	require_True(t, ingress != nil)
	require_True(t, ingress.Kind == CLIENT)
	require_Equal[string](t, ingress.Name, "Tracer")
	require_Equal[int](t, len(e.Egresses()), 0)
	ejs := e.JetStream()
	require_True(t, js != nil)
	require_Equal[string](t, ejs.Stream, "NO_INTEREST")
	require_Equal[string](t, ejs.Subject, "baz")
	require_True(t, ejs.NoInterest)
	require_Equal[string](t, ejs.Error, _EMPTY_)
}

func TestMsgTraceJetStreamWithSuperCluster(t *testing.T) {
	sc := createJetStreamSuperCluster(t, 3, 2)
	defer sc.shutdown()

	traceDest := "my.trace.subj"

	// Hack to set the trace destination for the global account in order
	// to make sure that the traceParentHdr header is disabled when a message
	// is stored in JetStream, which will prevent emitting a trace
	// when such message is retrieved and traverses a route.
	// Without the account destination set, the trace would not be
	// triggered, but that does not mean that we would have been
	// doing the right thing of disabling the header.
	for _, cl := range sc.clusters {
		for _, s := range cl.servers {
			acc, err := s.LookupAccount(globalAccountName)
			require_NoError(t, err)
			acc.setTraceDest(traceDest)
		}
	}

	c1 := sc.clusters[0]
	c2 := sc.clusters[1]
	nc, js := jsClientConnect(t, c1.randomServer())
	defer nc.Close()

	checkStream := func(t *testing.T, stream string, expected int) {
		t.Helper()
		checkFor(t, 5*time.Second, 15*time.Millisecond, func() error {
			si, err := js.StreamInfo(stream)
			if err != nil {
				return err
			}
			if n := si.State.Msgs; int(n) != expected {
				return fmt.Errorf("Expected %d messages for stream %q, got %v", expected, stream, n)
			}
			return nil
		})
	}

	for mainIter, mainTest := range []struct {
		name   string
		stream string
	}{
		{"from stream leader", "TEST1"},
		{"from non stream leader", "TEST2"},
		{"from other cluster", "TEST3"},
	} {
		t.Run(mainTest.name, func(t *testing.T) {
			cfg := &nats.StreamConfig{
				Name:        mainTest.stream,
				Replicas:    3,
				AllowRollup: true,
			}
			_, err := js.AddStream(cfg)
			require_NoError(t, err)
			sc.waitOnStreamLeader(globalAccountName, mainTest.stream)

			// The streams are created from c1 cluster.
			slSrv := c1.streamLeader(globalAccountName, mainTest.stream)

			// Store some messages
			payload := make([]byte, 50)
			for i := 0; i < 5; i++ {
				_, err = js.Publish(mainTest.stream, payload)
				require_NoError(t, err)
			}

			// We will connect the app that sends the trace message to a server
			// that is either the stream leader, a random server in c1, or
			// a server in c2 (to go through a GW).
			var s *Server
			switch mainIter {
			case 0:
				s = slSrv
			case 1:
				s = c1.randomNonStreamLeader(globalAccountName, mainTest.stream)
			case 2:
				s = c2.randomServer()
			}

			nct := natsConnect(t, s.ClientURL(), nats.Name("Tracer"))
			defer nct.Close()

			traceSub := natsSubSync(t, nct, traceDest)
			natsFlush(t, nct)

			for _, test := range []struct {
				name       string
				deliverMsg bool
			}{
				{"just trace", false},
				{"deliver msg", true},
			} {
				t.Run(test.name, func(t *testing.T) {
					msg := nats.NewMsg(mainTest.stream)
					msg.Header.Set(MsgTraceDest, traceSub.Subject)
					if !test.deliverMsg {
						msg.Header.Set(MsgTraceOnly, "true")
					}
					// We add the traceParentHdr header to make sure that it
					// is deactivated in addition to the Nats-Trace-Dest
					// header too when needed.
					msg.Header.Set(traceParentHdr, "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
					msg.Header.Set(JSMsgId, "MyId")
					msg.Data = payload
					err = nct.PublishMsg(msg)
					require_NoError(t, err)

					if test.deliverMsg {
						checkStream(t, mainTest.stream, 6)
					} else {
						checkStream(t, mainTest.stream, 5)
					}

					var (
						clientOK  bool
						gatewayOK bool
						routeOK   bool
					)

					check := func() {
						traceMsg := natsNexMsg(t, traceSub, time.Second)
						var e MsgTraceEvent
						json.Unmarshal(traceMsg.Data, &e)

						checkJS := func() {
							t.Helper()
							js := e.JetStream()
							require_True(t, js != nil)
							require_Equal[string](t, js.Stream, mainTest.stream)
							require_Equal[string](t, js.Subject, mainTest.stream)
							require_False(t, js.NoInterest)
							require_Equal[string](t, js.Error, _EMPTY_)
						}

						ingress := e.Ingress()
						require_True(t, ingress != nil)
						switch mainIter {
						case 0:
							require_Equal[string](t, e.Server.Name, s.Name())
							require_True(t, ingress.Kind == CLIENT)
							require_Equal[string](t, ingress.Name, "Tracer")
							require_Equal[int](t, len(e.Egresses()), 0)
							checkJS()
						case 1:
							switch ingress.Kind {
							case CLIENT:
								require_Equal[string](t, e.Server.Name, s.Name())
								require_Equal[string](t, ingress.Name, "Tracer")
								egress := e.Egresses()
								require_Equal[int](t, len(egress), 1)
								ci := egress[0]
								require_True(t, ci.Kind == ROUTER)
								require_Equal[string](t, ci.Name, slSrv.Name())
							case ROUTER:
								require_Equal[string](t, e.Server.Name, slSrv.Name())
								require_Equal[int](t, len(e.Egresses()), 0)
								checkJS()
							default:
								t.Fatalf("Unexpected ingress: %+v", ingress)
							}
						case 2:
							switch ingress.Kind {
							case CLIENT:
								require_Equal[string](t, e.Server.Name, s.Name())
								require_Equal[string](t, ingress.Name, "Tracer")
								egress := e.Egresses()
								require_Equal[int](t, len(egress), 1)
								ci := egress[0]
								require_True(t, ci.Kind == GATEWAY)
								// It could have gone to any server in the C1 cluster.
								// If it is not the stream leader, it should be
								// routed to it.
								clientOK = true
							case GATEWAY:
								require_Equal[string](t, ingress.Name, s.Name())
								// If the server that emitted this event is the
								// stream leader, then we should have the stream,
								// otherwise, it should be routed.
								if e.Server.Name == slSrv.Name() {
									require_Equal[int](t, len(e.Egresses()), 0)
									checkJS()
									// Set this so that we know that we don't expect
									// to have the route receive it.
									routeOK = true
								} else {
									egress := e.Egresses()
									require_Equal[int](t, len(egress), 1)
									ci := egress[0]
									require_True(t, ci.Kind == ROUTER)
									require_Equal[string](t, ci.Name, slSrv.Name())
								}
								gatewayOK = true
							case ROUTER:
								require_Equal[string](t, e.Server.Name, slSrv.Name())
								require_Equal[int](t, len(e.Egresses()), 0)
								checkJS()
								routeOK = true
							default:
								t.Fatalf("Unexpected ingress: %+v", ingress)
							}
						}
					}
					check()
					if mainIter > 0 {
						// There will be at least 2 events
						check()
						// For the last test, there may be a 3rd.
						if mainIter == 2 && !(clientOK && gatewayOK && routeOK) {
							check()
						}
					}
					// Make sure we are not receiving more traces
					if tm, err := traceSub.NextMsg(250 * time.Millisecond); err == nil {
						t.Fatalf("Should not have received trace message: %s", tm.Data)
					}
				})
			}

			jst, err := nct.JetStream()
			require_NoError(t, err)

			newMsg := func() *nats.Msg {
				msg := nats.NewMsg(mainTest.stream)
				msg.Header.Set(MsgTraceDest, traceSub.Subject)
				msg.Header.Set(MsgTraceOnly, "true")
				msg.Header.Set(traceParentHdr, "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
				msg.Data = []byte("hello")
				return msg
			}

			msgCount := 6
			for _, subtest := range []struct {
				name        string
				headerName  string
				headerVal   string
				expectedErr string
				special     int
			}{
				{"unexpected stream name", JSExpectedStream, "WRONG", "expected stream does not match", 0},
				{"duplicate id", JSMsgId, "MyId", "duplicate", 0},
				{"last seq by subject mismatch", JSExpectedLastSubjSeq, "3", "last sequence by subject mismatch", 0},
				{"last seq mismatch", JSExpectedLastSeq, "10", "last sequence mismatch", 0},
				{"last msgid mismatch", JSExpectedLastMsgId, "MyId3", "last msgid mismatch", 0},
				{"invalid rollup command", JSMsgRollup, "wrong", "rollup value invalid: \"wrong\"", 0},
				{"rollup not permitted", JSMsgRollup, JSMsgRollupSubject, "rollup not permitted", 1},
				{"max msg size", _EMPTY_, _EMPTY_, ErrMaxPayload.Error(), 2},
				{"new message ok", _EMPTY_, _EMPTY_, _EMPTY_, 3},
				{"stream sealed", _EMPTY_, _EMPTY_, NewJSStreamSealedError().Error(), 4},
			} {
				t.Run(subtest.name, func(t *testing.T) {
					msg := newMsg()
					if subtest.headerName != _EMPTY_ {
						msg.Header.Set(subtest.headerName, subtest.headerVal)
					}
					switch subtest.special {
					case 1:
						// Update stream to prevent rollups, and set a max size.
						cfg.AllowRollup = false
						cfg.MaxMsgSize = 100
						_, err = js.UpdateStream(cfg)
						require_NoError(t, err)
					case 2:
						msg.Data = make([]byte, 200)
					case 3:
						pa, err := jst.Publish(mainTest.stream, []byte("hello"))
						require_NoError(t, err)
						msgCount++
						checkStream(t, mainTest.stream, msgCount)
						require_Equal[uint64](t, pa.Sequence, 7)
						return
					case 4:
						cfg.Sealed = true
						_, err = js.UpdateStream(cfg)
						require_NoError(t, err)
					default:
					}
					jst.PublishMsg(msg)
					checkStream(t, mainTest.stream, msgCount)

					var (
						clientOK  bool
						gatewayOK bool
						routeOK   bool
					)

					checkJSTrace := func() {
						traceMsg := natsNexMsg(t, traceSub, time.Second)
						var e MsgTraceEvent
						json.Unmarshal(traceMsg.Data, &e)

						checkJS := func() {
							t.Helper()
							js := e.JetStream()
							require_True(t, e.JetStream() != nil)
							require_Equal[string](t, js.Stream, mainTest.stream)
							require_Equal[string](t, js.Subject, _EMPTY_)
							require_False(t, js.NoInterest)
							if et := js.Error; !strings.Contains(et, subtest.expectedErr) {
								t.Fatalf("Expected JS error to contain %q, got %q", subtest.expectedErr, et)
							}
						}

						ingress := e.Ingress()
						require_True(t, ingress != nil)
						// We will focus only on the trace message that
						// includes the JetStream event.
						switch mainIter {
						case 0:
							require_Equal[string](t, e.Server.Name, s.Name())
							require_True(t, ingress.Kind == CLIENT)
							require_Equal[string](t, ingress.Name, "Tracer")
							require_Equal[int](t, len(e.Egresses()), 0)
							checkJS()
						case 1:
							if ingress.Kind == ROUTER {
								require_Equal[string](t, e.Server.Name, slSrv.Name())
								require_Equal[int](t, len(e.Egresses()), 0)
								require_True(t, e.JetStream() != nil)
								checkJS()
							}
						case 2:
							switch ingress.Kind {
							case CLIENT:
								clientOK = true
							case GATEWAY:
								require_Equal[string](t, ingress.Name, s.Name())
								// If the server that emitted this event is the
								// stream leader, then we should have the stream,
								// otherwise, it should be routed.
								if e.Server.Name == slSrv.Name() {
									require_Equal[int](t, len(e.Egresses()), 0)
									checkJS()
									// We don't expect the route event
									routeOK = true
								}
								gatewayOK = true
							case ROUTER:
								require_Equal[string](t, e.Server.Name, slSrv.Name())
								require_Equal[int](t, len(e.Egresses()), 0)
								checkJS()
								routeOK = true
							}
						}
					}
					checkJSTrace()
					if mainIter > 0 {
						// There will be at least 2 events
						checkJSTrace()
						// For the last test, there may be a 3rd.
						if mainIter == 2 && !(clientOK && gatewayOK && routeOK) {
							checkJSTrace()
						}
					}
				})
			}
		})
	}

	// Now cause a step-down, and verify count is as expected.
	for _, stream := range []string{"TEST1", "TEST2", "TEST3"} {
		_, err := nc.Request(fmt.Sprintf(JSApiStreamLeaderStepDownT, stream), nil, time.Second)
		require_NoError(t, err)
		sc.waitOnStreamLeader(globalAccountName, stream)
		checkStream(t, stream, 7)
	}

	s := c1.randomNonStreamLeader(globalAccountName, "TEST1")
	// Try to get a message that will come from a route and make sure that
	// this does not trigger a trace message, that is, that headers have
	// been properly removed so that they don't trigger it.
	nct := natsConnect(t, s.ClientURL(), nats.Name("Tracer"))
	defer nct.Close()
	traceSub := natsSubSync(t, nct, traceDest)
	natsFlush(t, nct)

	jct, err := nct.JetStream()
	require_NoError(t, err)

	sub, err := jct.SubscribeSync("TEST1")
	require_NoError(t, err)
	for i := 0; i < 7; i++ {
		jmsg, err := sub.NextMsg(time.Second)
		require_NoError(t, err)
		require_Equal[string](t, jmsg.Header.Get(MsgTraceDest), _EMPTY_)
	}

	msg, err := traceSub.NextMsg(250 * time.Millisecond)
	if err != nats.ErrTimeout {
		if msg != nil {
			t.Fatalf("Expected timeout, got msg headers=%+v data=%s", msg.Header, msg.Data)
		}
		t.Fatalf("Expected timeout, got err=%v", err)
	}
}

func TestMsgTraceWithCompression(t *testing.T) {
	o := DefaultOptions()
	s := RunServer(o)
	defer s.Shutdown()

	nc := natsConnect(t, s.ClientURL())
	defer nc.Close()

	traceSub := natsSubSync(t, nc, "my.trace.subj")
	natsFlush(t, nc)

	for _, test := range []struct {
		compressAlgo string
		expectedHdr  string
		unsupported  bool
	}{
		{"gzip", "gzip", false},
		{"snappy", "snappy", false},
		{"s2", "snappy", false},
		{"bad one", "identity", true},
	} {
		t.Run(test.compressAlgo, func(t *testing.T) {
			msg := nats.NewMsg("foo")
			msg.Header.Set(MsgTraceDest, traceSub.Subject)
			msg.Header.Set(acceptEncodingHeader, test.compressAlgo)
			msg.Data = []byte("hello!")
			err := nc.PublishMsg(msg)
			require_NoError(t, err)

			traceMsg := natsNexMsg(t, traceSub, time.Second)
			data := traceMsg.Data
			eh := traceMsg.Header.Get(contentEncodingHeader)
			require_Equal[string](t, eh, test.expectedHdr)
			if test.unsupported {
				// We should be able to unmarshal directly
			} else {
				switch test.expectedHdr {
				case "gzip":
					zr, err := gzip.NewReader(bytes.NewReader(data))
					require_NoError(t, err)
					data, err = io.ReadAll(zr)
					if err != nil && err != io.ErrUnexpectedEOF {
						t.Fatalf("Unexpected error: %v", err)
					}
					err = zr.Close()
					require_NoError(t, err)
				case "snappy":
					sr := s2.NewReader(bytes.NewReader(data))
					data, err = io.ReadAll(sr)
					if err != nil && err != io.ErrUnexpectedEOF {
						t.Fatalf("Unexpected error: %v", err)
					}
				}
			}
			var e MsgTraceEvent
			err = json.Unmarshal(data, &e)
			require_NoError(t, err)
			ingress := e.Ingress()
			require_True(t, ingress != nil)
			require_Equal[string](t, e.Server.Name, s.Name())
			require_Equal[string](t, ingress.Subject, "foo")
		})
	}
}

func TestMsgTraceHops(t *testing.T) {
	// Will have a test with following toplogy
	//
	// ===================                       ===================
	// =   C1 cluster    =                       =   C2 cluster    =
	// ===================   <--- Gateway --->   ===================
	// = C1-S1 <-> C1-S2 =                       =      C2-S1      =
	// ===================                       ===================
	//    ^          ^                                    ^
	//    | Leafnode |                                    | Leafnode
	//    |          |                                    |
	// ===================                       ===================
	// =    C3 cluster   =                       =    C4 cluster   =
	// ===================                       ===================
	// = C3-S1 <-> C3-S2 =                       =       C4-S1     =
	// ===================                       ===================
	//                ^
	//                | Leafnode
	//            |-------|
	//       ===================
	//       =    C5 cluster   =
	//       ===================
	//       = C5-S1 <-> C5-S2 =
	//       ===================
	//
	// And a subscription on "foo" attached to all servers, and the subscription
	// on the trace subject attached to c1-s1 (and where the trace message will
	// be sent from).
	//
	commonTmpl := `
		port: -1
		server_name: "%s-%s"
		accounts {
			A { users: [{user:"a", pass: "pwd"}] }
			$SYS { users: [{user:"admin", pass: "s3cr3t!"}] }
		}
		system_account: "$SYS"
		cluster {
			port: -1
			name: "%s"
			%s
		}
	`
	genCommon := func(cname, sname string, routePort int) string {
		var routes string
		if routePort > 0 {
			routes = fmt.Sprintf(`routes: ["nats://127.0.0.1:%d"]`, routePort)
		}
		return fmt.Sprintf(commonTmpl, cname, sname, cname, routes)
	}
	c1s1conf := createConfFile(t, []byte(fmt.Sprintf(`
		%s
		gateway {
			port: -1
			name: "C1"
		}
		leafnodes {
			port: -1
		}
	`, genCommon("C1", "S1", 0))))
	c1s1, oc1s1 := RunServerWithConfig(c1s1conf)
	defer c1s1.Shutdown()

	c1s2conf := createConfFile(t, []byte(fmt.Sprintf(`
		%s
		gateway {
			port: -1
			name: "C1"
		}
		leafnodes {
			port: -1
		}
	`, genCommon("C1", "S2", oc1s1.Cluster.Port))))
	c1s2, oc1s2 := RunServerWithConfig(c1s2conf)
	defer c1s2.Shutdown()

	checkClusterFormed(t, c1s1, c1s2)

	c2s1conf := createConfFile(t, []byte(fmt.Sprintf(`
		%s
		gateway {
			port: -1
			name: "C2"
			gateways [
				{
					name: "C1"
					url: "nats://a:pwd@127.0.0.1:%d"
				}
			]
		}
		leafnodes {
			port: -1
		}
	`, genCommon("C2", "S1", 0), oc1s1.Gateway.Port)))
	c2s1, oc2s1 := RunServerWithConfig(c2s1conf)
	defer c2s1.Shutdown()

	c4s1conf := createConfFile(t, []byte(fmt.Sprintf(`
		%s
		leafnodes {
			remotes [{url: "nats://a:pwd@127.0.0.1:%d", account: "A"}]
		}
	`, genCommon("C4", "S1", 0), oc2s1.LeafNode.Port)))
	c4s1, _ := RunServerWithConfig(c4s1conf)
	defer c4s1.Shutdown()

	for _, s := range []*Server{c1s1, c1s2, c2s1} {
		waitForOutboundGateways(t, s, 1, time.Second)
	}
	waitForInboundGateways(t, c2s1, 2, time.Second)

	c3s1conf := createConfFile(t, []byte(fmt.Sprintf(`
		%s
		leafnodes {
			port: -1
			remotes [{url: "nats://a:pwd@127.0.0.1:%d", account: "A"}]
		}
	`, genCommon("C3", "S1", 0), oc1s1.LeafNode.Port)))
	c3s1, oc3s1 := RunServerWithConfig(c3s1conf)
	defer c3s1.Shutdown()

	c3s2conf := createConfFile(t, []byte(fmt.Sprintf(`
		%s
		leafnodes {
			port: -1
			remotes [{url: "nats://a:pwd@127.0.0.1:%d", account: "A"}]
		}
		system_account: "$SYS"
	`, genCommon("C3", "S2", oc3s1.Cluster.Port), oc1s2.LeafNode.Port)))
	c3s2, oc3s2 := RunServerWithConfig(c3s2conf)
	defer c3s2.Shutdown()

	checkClusterFormed(t, c3s1, c3s2)
	checkLeafNodeConnected(t, c1s1)
	checkLeafNodeConnected(t, c1s2)
	checkLeafNodeConnected(t, c3s1)
	checkLeafNodeConnected(t, c3s2)

	c5s1conf := createConfFile(t, []byte(fmt.Sprintf(`
		%s
		leafnodes {
			remotes [{url: "nats://a:pwd@127.0.0.1:%d", account: "A"}]
		}
	`, genCommon("C5", "S1", 0), oc3s2.LeafNode.Port)))
	c5s1, oc5s1 := RunServerWithConfig(c5s1conf)
	defer c5s1.Shutdown()

	c5s2conf := createConfFile(t, []byte(fmt.Sprintf(`
		%s
		leafnodes {
			remotes [{url: "nats://a:pwd@127.0.0.1:%d", account: "A"}]
		}
	`, genCommon("C5", "S2", oc5s1.Cluster.Port), oc3s2.LeafNode.Port)))
	c5s2, _ := RunServerWithConfig(c5s2conf)
	defer c5s2.Shutdown()

	checkLeafNodeConnected(t, c5s1)
	checkLeafNodeConnected(t, c5s2)
	checkLeafNodeConnectedCount(t, c3s2, 3)

	nct := natsConnect(t, c1s1.ClientURL(), nats.UserInfo("a", "pwd"), nats.Name("Tracer"))
	defer nct.Close()
	traceSub := natsSubSync(t, nct, "my.trace.subj")
	natsFlush(t, nct)

	allServers := []*Server{c1s1, c1s2, c2s1, c3s1, c3s2, c4s1, c5s1, c5s2}
	// Check that the subscription interest on the trace subject reaches all servers.
	for _, s := range allServers {
		if s == c2s1 {
			// Gateway needs to be checked differently.
			checkGWInterestOnlyModeInterestOn(t, c2s1, "C1", "A", traceSub.Subject)
			continue
		}
		checkSubInterest(t, s, "A", traceSub.Subject, time.Second)
	}

	var subs []*nats.Subscription
	// Now create a subscription on "foo" on all servers (do in reverse order).
	for i := len(allServers) - 1; i >= 0; i-- {
		s := allServers[i]
		nc := natsConnect(t, s.ClientURL(), nats.UserInfo("a", "pwd"), nats.Name(fmt.Sprintf("sub%d", i+1)))
		defer nc.Close()
		subs = append(subs, natsSubSync(t, nc, "foo"))
		natsFlush(t, nc)
	}

	// Check sub interest on "foo" on all servers.
	for _, s := range allServers {
		checkSubInterest(t, s, "A", "foo", time.Second)
	}

	// Now send a trace message from c1s1
	msg := nats.NewMsg("foo")
	msg.Header.Set(MsgTraceDest, traceSub.Subject)
	msg.Data = []byte("hello!")
	err := nct.PublishMsg(msg)
	require_NoError(t, err)

	// Check that all subscriptions received the message
	for i, sub := range subs {
		appMsg, err := sub.NextMsg(time.Second)
		if err != nil {
			t.Fatalf("Error getting app message for server %q", allServers[i])
		}
		require_Equal[string](t, string(appMsg.Data), "hello!")
		// Check that no (more) messages are received.
		if msg, err := sub.NextMsg(100 * time.Millisecond); err != nats.ErrTimeout {
			t.Fatalf("Did not expect application message, got %s", msg.Data)
		}
	}

	events := make(map[string]*MsgTraceEvent, 8)
	// We expect 8 events
	for i := 0; i < 8; i++ {
		traceMsg := natsNexMsg(t, traceSub, time.Second)
		var e MsgTraceEvent
		json.Unmarshal(traceMsg.Data, &e)

		var hop string
		if hopVals := e.Request.Header[MsgTraceHop]; len(hopVals) > 0 {
			hop = hopVals[0]
		}
		events[hop] = &e
	}
	// Make sure we are not receiving more traces
	if tm, err := traceSub.NextMsg(250 * time.Millisecond); err == nil {
		t.Fatalf("Should not have received trace message: %s", tm.Data)
	}

	checkIngress := func(e *MsgTraceEvent, kind int, name, hop string) *MsgTraceIngress {
		t.Helper()
		ingress := e.Ingress()
		require_True(t, ingress != nil)
		require_True(t, ingress.Kind == kind)
		require_Equal[string](t, ingress.Account, "A")
		require_Equal[string](t, ingress.Subject, "foo")
		require_Equal[string](t, ingress.Name, name)
		var hhop string
		if hopVals := e.Request.Header[MsgTraceHop]; len(hopVals) > 0 {
			hhop = hopVals[0]
		}
		require_Equal[string](t, hhop, hop)
		return ingress
	}

	checkEgressClient := func(eg *MsgTraceEgress, name string) {
		t.Helper()
		require_True(t, eg.Kind == CLIENT)
		require_Equal[string](t, eg.Name, name)
		require_Equal[string](t, eg.Hop, _EMPTY_)
		require_Equal[string](t, eg.Subscription, "foo")
		require_Equal[string](t, eg.Queue, _EMPTY_)
	}

	// First, we should have an event without a "hop" header, that is the
	// ingress from the client.
	e, ok := events[_EMPTY_]
	require_True(t, ok)
	checkIngress(e, CLIENT, "Tracer", _EMPTY_)
	require_Equal[int](t, e.Hops, 3)
	egress := e.Egresses()
	require_Equal[int](t, len(egress), 4)
	var (
		leafC3S1Hop  string
		leafC3S2Hop  string
		leafC4S1Hop  string
		leafC5S1Hop  string
		leafC5S2Hop  string
		routeC1S2Hop string
		gwC2S1Hop    string
	)
	for _, eg := range egress {
		switch eg.Kind {
		case CLIENT:
			checkEgressClient(eg, "sub1")
		case ROUTER:
			require_Equal[string](t, eg.Name, c1s2.Name())
			routeC1S2Hop = eg.Hop
		case LEAF:
			require_Equal[string](t, eg.Name, c3s1.Name())
			leafC3S1Hop = eg.Hop
		case GATEWAY:
			require_Equal[string](t, eg.Name, c2s1.Name())
			gwC2S1Hop = eg.Hop
		default:
			t.Fatalf("Unexpected egress: %+v", eg)
		}
	}
	// All "hop" ids should be not empty and different from each other
	require_True(t, leafC3S1Hop != _EMPTY_ && routeC1S2Hop != _EMPTY_ && gwC2S1Hop != _EMPTY_)
	require_True(t, leafC3S1Hop != routeC1S2Hop && leafC3S1Hop != gwC2S1Hop && routeC1S2Hop != gwC2S1Hop)

	// Now check the routed server in C1 (c1s2)
	e, ok = events[routeC1S2Hop]
	require_True(t, ok)
	checkIngress(e, ROUTER, c1s1.Name(), routeC1S2Hop)
	require_Equal[int](t, e.Hops, 1)
	egress = e.Egresses()
	require_Equal[int](t, len(egress), 2)
	for _, eg := range egress {
		switch eg.Kind {
		case CLIENT:
			checkEgressClient(eg, "sub2")
		case LEAF:
			require_Equal[string](t, eg.Name, c3s2.Name())
			require_Equal[string](t, eg.Hop, routeC1S2Hop+".1")
			leafC3S2Hop = eg.Hop
		default:
			t.Fatalf("Unexpected egress: %+v", eg)
		}
	}
	require_True(t, leafC3S2Hop != _EMPTY_)

	// Let's check the gateway server
	e, ok = events[gwC2S1Hop]
	require_True(t, ok)
	checkIngress(e, GATEWAY, c1s1.Name(), gwC2S1Hop)
	require_Equal[int](t, e.Hops, 1)
	egress = e.Egresses()
	require_Equal[int](t, len(egress), 2)
	for _, eg := range egress {
		switch eg.Kind {
		case CLIENT:
			checkEgressClient(eg, "sub3")
		case LEAF:
			require_Equal[string](t, eg.Name, c4s1.Name())
			require_Equal[string](t, eg.Hop, gwC2S1Hop+".1")
			leafC4S1Hop = eg.Hop
		default:
			t.Fatalf("Unexpected egress: %+v", eg)
		}
	}
	require_True(t, leafC4S1Hop != _EMPTY_)

	// Let's check the C3 cluster, starting at C3-S1
	e, ok = events[leafC3S1Hop]
	require_True(t, ok)
	checkIngress(e, LEAF, c1s1.Name(), leafC3S1Hop)
	require_Equal[int](t, e.Hops, 0)
	egress = e.Egresses()
	require_Equal[int](t, len(egress), 1)
	checkEgressClient(egress[0], "sub4")

	// Now C3-S2
	e, ok = events[leafC3S2Hop]
	require_True(t, ok)
	checkIngress(e, LEAF, c1s2.Name(), leafC3S2Hop)
	require_Equal[int](t, e.Hops, 2)
	egress = e.Egresses()
	require_Equal[int](t, len(egress), 3)
	for _, eg := range egress {
		switch eg.Kind {
		case CLIENT:
			checkEgressClient(eg, "sub5")
		case LEAF:
			require_True(t, eg.Name == c5s1.Name() || eg.Name == c5s2.Name())
			require_True(t, eg.Hop == leafC3S2Hop+".1" || eg.Hop == leafC3S2Hop+".2")
			if eg.Name == c5s1.Name() {
				leafC5S1Hop = eg.Hop
			} else {
				leafC5S2Hop = eg.Hop
			}
		default:
			t.Fatalf("Unexpected egress: %+v", eg)
		}
	}
	// The leafC5SxHop must be different and not empty
	require_True(t, leafC5S1Hop != _EMPTY_ && leafC5S1Hop != leafC5S2Hop && leafC5S2Hop != _EMPTY_)

	// Check the C4 cluster
	e, ok = events[leafC4S1Hop]
	require_True(t, ok)
	checkIngress(e, LEAF, c2s1.Name(), leafC4S1Hop)
	require_Equal[int](t, e.Hops, 0)
	egress = e.Egresses()
	require_Equal[int](t, len(egress), 1)
	checkEgressClient(egress[0], "sub6")

	// Finally, the C5 cluster, starting with C5-S1
	e, ok = events[leafC5S1Hop]
	require_True(t, ok)
	checkIngress(e, LEAF, c3s2.Name(), leafC5S1Hop)
	require_Equal[int](t, e.Hops, 0)
	egress = e.Egresses()
	require_Equal[int](t, len(egress), 1)
	checkEgressClient(egress[0], "sub7")

	// Then C5-S2
	e, ok = events[leafC5S2Hop]
	require_True(t, ok)
	checkIngress(e, LEAF, c3s2.Name(), leafC5S2Hop)
	require_Equal[int](t, e.Hops, 0)
	egress = e.Egresses()
	require_Equal[int](t, len(egress), 1)
	checkEgressClient(egress[0], "sub8")
}

func TestMsgTraceTriggeredByExternalHeader(t *testing.T) {
	tmpl := `
		port: -1
		server_name: "%s"
		accounts {
			A {
				users: [{user:A, password: pwd}]
				trace_dest: "acc.trace.dest"
			}
			B {
				users: [{user:B, password: pwd}]
				%s
			}
		}
		cluster {
			name: "local"
			port: -1
			%s
		}
	`
	conf1 := createConfFile(t, []byte(fmt.Sprintf(tmpl, "A", _EMPTY_, _EMPTY_)))
	s1, o1 := RunServerWithConfig(conf1)
	defer s1.Shutdown()

	conf2 := createConfFile(t, []byte(fmt.Sprintf(tmpl, "B", _EMPTY_, fmt.Sprintf(`routes: ["nats://127.0.0.1:%d"]`, o1.Cluster.Port))))
	s2, _ := RunServerWithConfig(conf2)
	defer s2.Shutdown()

	checkClusterFormed(t, s1, s2)

	nc2 := natsConnect(t, s2.ClientURL(), nats.UserInfo("A", "pwd"))
	defer nc2.Close()
	appSub := natsSubSync(t, nc2, "foo")
	natsFlush(t, nc2)

	checkSubInterest(t, s1, "A", "foo", time.Second)

	nc1 := natsConnect(t, s1.ClientURL(), nats.UserInfo("A", "pwd"))
	defer nc1.Close()

	traceSub := natsSubSync(t, nc1, "trace.dest")
	accTraceSub := natsSubSync(t, nc1, "acc.trace.dest")
	natsFlush(t, nc1)

	checkSubInterest(t, s1, "A", traceSub.Subject, time.Second)
	checkSubInterest(t, s1, "A", accTraceSub.Subject, time.Second)

	var msgCount int
	for _, test := range []struct {
		name           string
		setHeaders     func(h nats.Header)
		traceTriggered bool
		traceOnly      bool
		expectedAccSub bool
	}{
		// Tests with external header only (no Nats-Trace-Dest). In this case, the
		// trace is triggered based on sampling (last token is `-01`). The presence
		// of Nats-Trace-Only has no effect and message should always be delivered
		// to the application.
		{"only external header sampling",
			func(h nats.Header) {
				h.Set(traceParentHdr, "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
			},
			true,
			false,
			true},
		{"only external header with different case and sampling",
			func(h nats.Header) {
				h.Set("TraceParent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
			},
			true,
			false,
			true},
		{"only external header sampling but not simply 01",
			func(h nats.Header) {
				h.Set(traceParentHdr, "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-25")
			},
			true,
			false,
			true},
		{"only external header no sampling",
			func(h nats.Header) {
				h.Set(traceParentHdr, "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00")
			},
			false,
			false,
			false},
		{"external header sampling and trace only",
			func(h nats.Header) {
				h.Set(traceParentHdr, "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
				h.Set(MsgTraceOnly, "true")
			},
			true,
			false,
			true},
		{"external header no sampling and trace only",
			func(h nats.Header) {
				h.Set(traceParentHdr, "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00")
				h.Set(MsgTraceOnly, "true")
			},
			false,
			false,
			false},
		// Tests where Nats-Trace-Dest is present, so ignore external header and
		// always deliver to the Nats-Trace-Dest, not the account.
		{"trace dest and external header sampling",
			func(h nats.Header) {
				h.Set(MsgTraceDest, traceSub.Subject)
				h.Set(traceParentHdr, "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
			},
			true,
			false,
			false},
		{"trace dest and external header no sampling",
			func(h nats.Header) {
				h.Set(MsgTraceDest, traceSub.Subject)
				h.Set(traceParentHdr, "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00")
			},
			true,
			false,
			false},
		{"trace dest with trace only and external header sampling",
			func(h nats.Header) {
				h.Set(MsgTraceDest, traceSub.Subject)
				h.Set(MsgTraceOnly, "true")
				h.Set(traceParentHdr, "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
			},
			true,
			true,
			false},
		{"trace dest with trace only and external header no sampling",
			func(h nats.Header) {
				h.Set(MsgTraceDest, traceSub.Subject)
				h.Set(MsgTraceOnly, "true")
				h.Set(traceParentHdr, "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00")
			},
			true,
			true,
			false},
	} {
		t.Run(test.name, func(t *testing.T) {
			msg := nats.NewMsg("foo")
			test.setHeaders(msg.Header)
			msgCount++
			msgPayload := fmt.Sprintf("msg%d", msgCount)
			msg.Data = []byte(msgPayload)
			err := nc1.PublishMsg(msg)
			require_NoError(t, err)

			if !test.traceOnly {
				appMsg := natsNexMsg(t, appSub, time.Second)
				require_Equal[string](t, string(appMsg.Data), msgPayload)
			}
			// Make sure we don't receive more (or not if trace only)
			if appMsg, err := appSub.NextMsg(100 * time.Millisecond); err != nats.ErrTimeout {
				t.Fatalf("Expected no app message, got %q", appMsg.Data)
			}

			checkTrace := func(sub *nats.Subscription) {
				// We should receive 2 traces, 1 per server.
				for i := 0; i < 2; i++ {
					tm := natsNexMsg(t, sub, time.Second)
					var e MsgTraceEvent
					err := json.Unmarshal(tm.Data, &e)
					require_NoError(t, err)
				}
			}

			if test.traceTriggered {
				if test.expectedAccSub {
					checkTrace(accTraceSub)
				} else {
					checkTrace(traceSub)
				}
			}
			// Make sure no trace is received in the other trace sub
			// or no trace received at all.
			for _, sub := range []*nats.Subscription{accTraceSub, traceSub} {
				if tm, err := sub.NextMsg(100 * time.Millisecond); err != nats.ErrTimeout {
					t.Fatalf("Expected no trace for the trace sub on %q, got %q", sub.Subject, tm.Data)
				}
			}
		})
	}

	nc1.Close()
	nc2.Close()

	// Now replace connections and subs for account "B"
	nc2 = natsConnect(t, s2.ClientURL(), nats.UserInfo("B", "pwd"))
	defer nc2.Close()
	appSub = natsSubSync(t, nc2, "foo")
	natsFlush(t, nc2)

	checkSubInterest(t, s1, "B", "foo", time.Second)

	nc1 = natsConnect(t, s1.ClientURL(), nats.UserInfo("B", "pwd"))
	defer nc1.Close()

	traceSub = natsSubSync(t, nc1, "trace.dest")
	accTraceSub = natsSubSync(t, nc1, "acc.trace.dest")
	natsFlush(t, nc1)

	checkSubInterest(t, s1, "B", traceSub.Subject, time.Second)
	checkSubInterest(t, s1, "B", accTraceSub.Subject, time.Second)

	for _, test := range []struct {
		name   string
		reload bool
	}{
		{"external header but no account destination", true},
		{"external header with account destination added through config reload", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			msg := nats.NewMsg("foo")
			msg.Header.Set(traceParentHdr, "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
			msg.Data = []byte("hello")
			err := nc1.PublishMsg(msg)
			require_NoError(t, err)

			// Application should receive the message
			appMsg := natsNexMsg(t, appSub, time.Second)
			require_Equal[string](t, string(appMsg.Data), "hello")
			// Only once...
			if appMsg, err := appSub.NextMsg(100 * time.Millisecond); err != nats.ErrTimeout {
				t.Fatalf("Expected no app message, got %q", appMsg.Data)
			}
			if !test.reload {
				// We should receive the traces (1 per server) on the account destination
				for i := 0; i < 2; i++ {
					tm := natsNexMsg(t, accTraceSub, time.Second)
					var e MsgTraceEvent
					err := json.Unmarshal(tm.Data, &e)
					require_NoError(t, err)
				}
			}
			// No (or no more) trace message should be received.
			for _, sub := range []*nats.Subscription{accTraceSub, traceSub} {
				if tm, err := sub.NextMsg(100 * time.Millisecond); err != nats.ErrTimeout {
					t.Fatalf("Expected no trace for the trace sub on %q, got %q", sub.Subject, tm.Data)
				}
			}
			// Do the config reload and we will repeat the test and now
			// we should receive the trace message into the account
			// destination trace.
			if test.reload {
				reloadUpdateConfig(t, s1, conf1, fmt.Sprintf(tmpl, "A", `trace_dest: "acc.trace.dest"`, _EMPTY_))
				reloadUpdateConfig(t, s2, conf2, fmt.Sprintf(tmpl, "B", `trace_dest: "acc.trace.dest"`, fmt.Sprintf(`routes: ["nats://127.0.0.1:%d"]`, o1.Cluster.Port)))
			}
		})
	}
}

func TestMsgTraceAccountTraceDestJWTUpdate(t *testing.T) {
	// create system account
	sysKp, _ := nkeys.CreateAccount()
	sysPub, _ := sysKp.PublicKey()
	sysCreds := newUser(t, sysKp)
	// create account A
	akp, _ := nkeys.CreateAccount()
	aPub, _ := akp.PublicKey()
	claim := jwt.NewAccountClaims(aPub)
	aJwt, err := claim.Encode(oKp)
	require_NoError(t, err)

	dir := t.TempDir()
	conf := createConfFile(t, []byte(fmt.Sprintf(`
		listen: -1
		operator: %s
		resolver: {
			type: full
			dir: '%s'
		}
		system_account: %s
    `, ojwt, dir, sysPub)))
	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()
	updateJwt(t, s.ClientURL(), sysCreds, aJwt, 1)

	nc := natsConnect(t, s.ClientURL(), createUserCreds(t, nil, akp))
	defer nc.Close()

	sub := natsSubSync(t, nc, "acc.trace.dest")
	natsFlush(t, nc)

	for i, test := range []struct {
		name           string
		traceTriggered bool
	}{
		{"no acc dest", false},
		{"adding trace dest", true},
		{"removing trace dest", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			msg := nats.NewMsg("foo")
			msg.Header.Set(traceParentHdr, "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
			msg.Data = []byte("hello")
			err = nc.PublishMsg(msg)
			require_NoError(t, err)

			if test.traceTriggered {
				tm := natsNexMsg(t, sub, time.Second)
				var e MsgTraceEvent
				err = json.Unmarshal(tm.Data, &e)
				require_NoError(t, err)
				// Simple check
				require_Equal[string](t, e.Server.Name, s.Name())
			}
			// No (more) trace message expected.
			tm, err := sub.NextMsg(250 * time.Millisecond)
			if err != nats.ErrTimeout {
				t.Fatalf("Expected no trace message, got %s", tm.Data)
			}
			if i < 2 {
				if i == 0 {
					claim.Trace = &jwt.MsgTrace{Destination: "acc.trace.dest"}
				} else {
					claim.Trace = nil
				}
				aJwt, err = claim.Encode(oKp)
				require_NoError(t, err)
				updateJwt(t, s.ClientURL(), sysCreds, aJwt, 1)
			}
		})
	}
}

func TestMsgTraceServiceJWTUpdate(t *testing.T) {
	// create system account
	sysKp, _ := nkeys.CreateAccount()
	sysPub, _ := sysKp.PublicKey()
	sysCreds := newUser(t, sysKp)
	// create account A
	akp, _ := nkeys.CreateAccount()
	aPub, _ := akp.PublicKey()
	aClaim := jwt.NewAccountClaims(aPub)
	serviceExport := &jwt.Export{Subject: "req", Type: jwt.Service}
	aClaim.Exports.Add(serviceExport)
	aJwt, err := aClaim.Encode(oKp)
	require_NoError(t, err)
	// create account B
	bkp, _ := nkeys.CreateAccount()
	bPub, _ := bkp.PublicKey()
	bClaim := jwt.NewAccountClaims(bPub)
	serviceImport := &jwt.Import{Account: aPub, Subject: "req", Type: jwt.Service}
	bClaim.Imports.Add(serviceImport)
	bJwt, err := bClaim.Encode(oKp)
	require_NoError(t, err)

	dir := t.TempDir()
	conf := createConfFile(t, []byte(fmt.Sprintf(`
		listen: -1
		operator: %s
		resolver: {
			type: full
			dir: '%s'
		}
		system_account: %s
	`, ojwt, dir, sysPub)))
	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()
	updateJwt(t, s.ClientURL(), sysCreds, aJwt, 1)
	updateJwt(t, s.ClientURL(), sysCreds, bJwt, 1)

	ncA := natsConnect(t, s.ClientURL(), createUserCreds(t, nil, akp), nats.Name("Service"))
	defer ncA.Close()

	natsSub(t, ncA, "req", func(m *nats.Msg) {
		m.Respond([]byte("resp"))
	})
	natsFlush(t, ncA)

	ncB := natsConnect(t, s.ClientURL(), createUserCreds(t, nil, bkp))
	defer ncB.Close()

	sub := natsSubSync(t, ncB, "trace.dest")
	natsFlush(t, ncB)

	for i, test := range []struct {
		name       string
		allowTrace bool
	}{
		{"trace not allowed", false},
		{"trace allowed", true},
		{"trace not allowed again", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			msg := nats.NewMsg("req")
			msg.Header.Set(MsgTraceDest, sub.Subject)
			msg.Data = []byte("request")
			reply, err := ncB.RequestMsg(msg, time.Second)
			require_NoError(t, err)
			require_Equal[string](t, string(reply.Data), "resp")

			tm := natsNexMsg(t, sub, time.Second)
			var e MsgTraceEvent
			err = json.Unmarshal(tm.Data, &e)
			require_NoError(t, err)
			require_Equal[string](t, e.Server.Name, s.Name())
			require_Equal[string](t, e.Ingress().Account, bPub)
			sis := e.ServiceImports()
			require_Equal[int](t, len(sis), 1)
			si := sis[0]
			require_Equal[string](t, si.Account, aPub)
			egresses := e.Egresses()
			if !test.allowTrace {
				require_Equal[int](t, len(egresses), 0)
			} else {
				require_Equal[int](t, len(egresses), 1)
				eg := egresses[0]
				require_Equal[string](t, eg.Name, "Service")
				require_Equal[string](t, eg.Account, aPub)
				require_Equal[string](t, eg.Subscription, "req")
			}
			// No (more) trace message expected.
			tm, err = sub.NextMsg(250 * time.Millisecond)
			if err != nats.ErrTimeout {
				t.Fatalf("Expected no trace message, got %s", tm.Data)
			}
			if i < 2 {
				// Set AllowTrace to true at the first iteration, then
				// false at the second.
				aClaim.Exports[0].AllowTrace = (i == 0)
				aJwt, err = aClaim.Encode(oKp)
				require_NoError(t, err)
				updateJwt(t, s.ClientURL(), sysCreds, aJwt, 1)
			}
		})
	}
}

func TestMsgTraceStreamJWTUpdate(t *testing.T) {
	// create system account
	sysKp, _ := nkeys.CreateAccount()
	sysPub, _ := sysKp.PublicKey()
	sysCreds := newUser(t, sysKp)
	// create account A
	akp, _ := nkeys.CreateAccount()
	aPub, _ := akp.PublicKey()
	aClaim := jwt.NewAccountClaims(aPub)
	streamExport := &jwt.Export{Subject: "info", Type: jwt.Stream}
	aClaim.Exports.Add(streamExport)
	aJwt, err := aClaim.Encode(oKp)
	require_NoError(t, err)
	// create account B
	bkp, _ := nkeys.CreateAccount()
	bPub, _ := bkp.PublicKey()
	bClaim := jwt.NewAccountClaims(bPub)
	streamImport := &jwt.Import{Account: aPub, Subject: "info", To: "b", Type: jwt.Stream}
	bClaim.Imports.Add(streamImport)
	bJwt, err := bClaim.Encode(oKp)
	require_NoError(t, err)

	dir := t.TempDir()
	conf := createConfFile(t, []byte(fmt.Sprintf(`
		listen: -1
		operator: %s
		resolver: {
			type: full
			dir: '%s'
		}
		system_account: %s
	`, ojwt, dir, sysPub)))
	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()
	updateJwt(t, s.ClientURL(), sysCreds, aJwt, 1)
	updateJwt(t, s.ClientURL(), sysCreds, bJwt, 1)

	ncA := natsConnect(t, s.ClientURL(), createUserCreds(t, nil, akp))
	defer ncA.Close()

	traceSub := natsSubSync(t, ncA, "trace.dest")
	natsFlush(t, ncA)

	ncB := natsConnect(t, s.ClientURL(), createUserCreds(t, nil, bkp), nats.Name("BInfo"))
	defer ncB.Close()

	appSub := natsSubSync(t, ncB, "b.info")
	natsFlush(t, ncB)

	for i, test := range []struct {
		name       string
		allowTrace bool
	}{
		{"trace not allowed", false},
		{"trace allowed", true},
		{"trace not allowed again", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			msg := nats.NewMsg("info")
			msg.Header.Set(MsgTraceDest, traceSub.Subject)
			msg.Data = []byte("some info")
			err = ncA.PublishMsg(msg)
			require_NoError(t, err)

			appMsg := natsNexMsg(t, appSub, time.Second)
			require_Equal[string](t, string(appMsg.Data), "some info")

			tm := natsNexMsg(t, traceSub, time.Second)
			var e MsgTraceEvent
			err = json.Unmarshal(tm.Data, &e)
			require_NoError(t, err)
			require_Equal[string](t, e.Server.Name, s.Name())
			ses := e.StreamExports()
			require_Equal[int](t, len(ses), 1)
			se := ses[0]
			require_Equal[string](t, se.Account, bPub)
			require_Equal[string](t, se.To, "b.info")
			egresses := e.Egresses()
			if !test.allowTrace {
				require_Equal[int](t, len(egresses), 0)
			} else {
				require_Equal[int](t, len(egresses), 1)
				eg := egresses[0]
				require_Equal[string](t, eg.Name, "BInfo")
				require_Equal[string](t, eg.Account, bPub)
				require_Equal[string](t, eg.Subscription, "info")
			}
			// No (more) trace message expected.
			tm, err = traceSub.NextMsg(250 * time.Millisecond)
			if err != nats.ErrTimeout {
				t.Fatalf("Expected no trace message, got %s", tm.Data)
			}
			if i < 2 {
				// Set AllowTrace to true at the first iteration, then
				// false at the second.
				bClaim.Imports[0].AllowTrace = (i == 0)
				bJwt, err = bClaim.Encode(oKp)
				require_NoError(t, err)
				updateJwt(t, s.ClientURL(), sysCreds, bJwt, 1)
			}
		})
	}
}

func TestMsgTraceParseAccountDestWithSampling(t *testing.T) {
	tmpl := `
		port: -1
		accounts {
			A {
				users: [{user: a, password: pwd}]
				%s
			}
		}
	`
	for _, test := range []struct {
		name        string
		samplingStr string
		want        int
	}{
		{"trace sampling no dest", `msg_trace: {sampling: 50}`, 0},
		{"trace dest only", `msg_trace: {dest: foo}`, 100},
		{"trace dest with number only", `msg_trace: {dest: foo, sampling: 20}`, 20},
		{"trace dest with percentage", `msg_trace: {dest: foo, sampling: 50%}`, 50},
	} {
		t.Run(test.name, func(t *testing.T) {
			conf := createConfFile(t, []byte(fmt.Sprintf(tmpl, test.samplingStr)))
			o := LoadConfig(conf)
			_, sampling := o.Accounts[0].getTraceDestAndSampling()
			require_Equal[int](t, test.want, sampling)
		})
	}
}

func TestMsgTraceAccountDestWithSampling(t *testing.T) {
	tmpl := `
		port: -1
		server_name: %s
		accounts {
			A {
				users: [{user: a, password:pwd}]
				msg_trace: {dest: "acc.dest"%s}
			}
		}
		cluster {
			port: -1
			%s
		}
	`
	conf1 := createConfFile(t, []byte(fmt.Sprintf(tmpl, "A", _EMPTY_, _EMPTY_)))
	s1, o1 := RunServerWithConfig(conf1)
	defer s1.Shutdown()

	routes := fmt.Sprintf("routes: [\"nats://127.0.0.1:%d\"]", o1.Cluster.Port)
	conf2 := createConfFile(t, []byte(fmt.Sprintf(tmpl, "B", _EMPTY_, routes)))
	s2, _ := RunServerWithConfig(conf2)
	defer s2.Shutdown()

	checkClusterFormed(t, s1, s2)

	nc2 := natsConnect(t, s2.ClientURL(), nats.UserInfo("a", "pwd"))
	defer nc2.Close()
	natsSub(t, nc2, "foo", func(_ *nats.Msg) {})
	natsFlush(t, nc2)

	nc1 := natsConnect(t, s1.ClientURL(), nats.UserInfo("a", "pwd"))
	defer nc1.Close()
	sub := natsSubSync(t, nc1, "acc.dest")
	natsFlush(t, nc1)

	checkSubInterest(t, s1, "A", "foo", time.Second)
	checkSubInterest(t, s2, "A", "acc.dest", time.Second)

	for iter, test := range []struct {
		name        string
		samplingStr string
		sampling    int
	}{
		// Sampling is considered 100% if not specified or <=0 or >= 100.
		// To disable sampling, the account destination should not be specified.
		{"no sampling specified", _EMPTY_, 100},
		{"sampling specified", ", sampling: \"25%\"", 25},
		{"no sampling again", _EMPTY_, 100},
	} {
		t.Run(test.name, func(t *testing.T) {
			if iter > 0 {
				reloadUpdateConfig(t, s1, conf1, fmt.Sprintf(tmpl, "A", test.samplingStr, _EMPTY_))
				reloadUpdateConfig(t, s2, conf2, fmt.Sprintf(tmpl, "B", test.samplingStr, routes))
			}
			msg := nats.NewMsg("foo")
			msg.Header.Set(traceParentHdr, "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
			total := 400
			for i := 0; i < total; i++ {
				err := nc1.PublishMsg(msg)
				require_NoError(t, err)
			}
			// Wait a bit to make sure that we received all traces that should
			// have been received.
			time.Sleep(500 * time.Millisecond)
			n, _, err := sub.Pending()
			require_NoError(t, err)
			fromClient := 0
			fromRoute := 0
			for i := 0; i < n; i++ {
				msg = natsNexMsg(t, sub, time.Second)
				var e MsgTraceEvent
				err = json.Unmarshal(msg.Data, &e)
				require_NoError(t, err)
				ingress := e.Ingress()
				require_True(t, ingress != nil)
				switch ingress.Kind {
				case CLIENT:
					fromClient++
				case ROUTER:
					fromRoute++
				default:
					t.Fatalf("Unexpected ingress: %+v", ingress)
				}
			}
			// There should be as many messages coming from the origin server
			// and the routed server. This checks that if sampling is not 100%
			// then when a message is routed, the header is properly deactivated.
			require_Equal[int](t, fromClient, fromRoute)
			// Now check that if sampling was 100%, we have the total number
			// of published messages.
			if test.sampling == 100 {
				require_Equal[int](t, fromClient, total)
			} else {
				// Otherwise, we should have no more (but let's be conservative)
				// than the sampling number.
				require_LessThan[int](t, fromClient, int(float64(test.sampling*total/100)*1.35))
			}
		})
	}
}

func TestMsgTraceAccDestWithSamplingJWTUpdate(t *testing.T) {
	// create system account
	sysKp, _ := nkeys.CreateAccount()
	sysPub, _ := sysKp.PublicKey()
	sysCreds := newUser(t, sysKp)
	// create account A
	akp, _ := nkeys.CreateAccount()
	aPub, _ := akp.PublicKey()
	claim := jwt.NewAccountClaims(aPub)
	claim.Trace = &jwt.MsgTrace{Destination: "acc.trace.dest"}
	aJwt, err := claim.Encode(oKp)
	require_NoError(t, err)

	dir := t.TempDir()
	conf := createConfFile(t, []byte(fmt.Sprintf(`
			listen: -1
			operator: %s
			resolver: {
				type: full
				dir: '%s'
			}
			system_account: %s
		`, ojwt, dir, sysPub)))
	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()
	updateJwt(t, s.ClientURL(), sysCreds, aJwt, 1)

	nc := natsConnect(t, s.ClientURL(), createUserCreds(t, nil, akp))
	defer nc.Close()

	sub := natsSubSync(t, nc, "acc.trace.dest")
	natsFlush(t, nc)

	for iter, test := range []struct {
		name     string
		sampling int
	}{
		{"no sampling specified", 100},
		{"sampling", 25},
		{"set back sampling to 0", 100},
	} {
		t.Run(test.name, func(t *testing.T) {
			if iter > 0 {
				claim.Trace = &jwt.MsgTrace{Destination: "acc.trace.dest", Sampling: test.sampling}
				aJwt, err = claim.Encode(oKp)
				require_NoError(t, err)
				updateJwt(t, s.ClientURL(), sysCreds, aJwt, 1)
			}

			msg := nats.NewMsg("foo")
			msg.Header.Set(traceParentHdr, "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
			msg.Data = []byte("hello")

			total := 400
			for i := 0; i < total; i++ {
				err := nc.PublishMsg(msg)
				require_NoError(t, err)
			}
			// Wait a bit to make sure that we received all traces that should
			// have been received.
			time.Sleep(500 * time.Millisecond)
			n, _, err := sub.Pending()
			require_NoError(t, err)
			for i := 0; i < n; i++ {
				msg = natsNexMsg(t, sub, time.Second)
				var e MsgTraceEvent
				err = json.Unmarshal(msg.Data, &e)
				require_NoError(t, err)
			}
			// Now check that if sampling was 100%, we have the total number
			// of published messages.
			if test.sampling == 100 {
				require_Equal[int](t, n, total)
			} else {
				// Otherwise, we should have no more (but let's be conservative)
				// than the sampling number.
				require_LessThan[int](t, n, int(float64(test.sampling*total/100)*1.35))
			}
		})
	}
}
