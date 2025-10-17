// Copyright 2025 The NATS Authors
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
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net/url"
	"regexp"
	"strconv"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func init() {
	msgTraceRunInTests = true
}

// setupGatewayClusterABC creates 3 gateway clusters (A, B, C) where A connects to both B and C.
func setupGatewayClusterABC(t *testing.T) (sa, sb, sc *Server) {
	ob := testDefaultOptionsForGateway("B")
	ob.NoSystemAccount = false
	sb = runGatewayServer(ob)

	oc := testDefaultOptionsForGateway("C")
	oc.NoSystemAccount = false
	sc = runGatewayServer(oc)

	oa := testGatewayOptionsFromToWithServers(t, "A", "B", sb)
	urlC, err := url.Parse(fmt.Sprintf("nats://127.0.0.1:%d", sc.GatewayAddr().Port))
	if err != nil {
		t.Fatalf("Error parsing url: %v", err)
	}
	oa.Gateway.Gateways = append(oa.Gateway.Gateways, &RemoteGatewayOpts{
		Name: "C",
		URLs: []*url.URL{urlC},
	})
	oa.NoSystemAccount = false
	sa = runGatewayServer(oa)

	waitForOutboundGateways(t, sa, 2, 2*time.Second)
	waitForOutboundGateways(t, sb, 2, 2*time.Second)
	waitForOutboundGateways(t, sc, 2, 2*time.Second)
	waitForInboundGateways(t, sa, 2, 2*time.Second)
	waitForInboundGateways(t, sb, 2, 2*time.Second)
	waitForInboundGateways(t, sc, 2, 2*time.Second)

	return sa, sb, sc
}

// TestGatewayMsgTraceDuplicateHopHeader verifies that when a traced message
// is sent to multiple gateways, all gateways receive the same hop count.
func TestGatewayMsgTraceDuplicateHopHeader(t *testing.T) {
	sa, sb, sc := setupGatewayClusterABC(t)
	defer sa.Shutdown()
	defer sb.Shutdown()
	defer sc.Shutdown()

	ncb := natsConnect(t, sb.ClientURL())
	defer ncb.Close()
	ncc := natsConnect(t, sc.ClientURL())
	defer ncc.Close()

	msgReceivedB := make(chan *nats.Msg, 1)
	msgReceivedC := make(chan *nats.Msg, 1)

	_, err := ncb.Subscribe("test.subject", func(msg *nats.Msg) {
		msgReceivedB <- msg
	})
	require_NoError(t, err)

	_, err = ncc.Subscribe("test.subject", func(msg *nats.Msg) {
		msgReceivedC <- msg
	})
	require_NoError(t, err)

	require_NoError(t, ncb.Flush())
	require_NoError(t, ncc.Flush())
	time.Sleep(200 * time.Millisecond)

	nca := natsConnect(t, sa.ClientURL())
	defer nca.Close()

	msg := nats.NewMsg("test.subject")
	msg.Header.Set(MsgTraceDest, "trace.dest")
	msg.Data = []byte("test payload")
	require_NoError(t, nca.PublishMsg(msg))

	var msgB, msgC *nats.Msg
	select {
	case msgB = <-msgReceivedB:
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for message on gateway B")
	}

	select {
	case msgC = <-msgReceivedC:
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for message on gateway C")
	}

	hopB := msgB.Header.Get(MsgTraceHop)
	hopC := msgC.Header.Get(MsgTraceHop)

	if hopB != hopC {
		t.Fatalf("Gateway B received hop '%s' but Gateway C received hop '%s', expected same hop count", hopB, hopC)
	}
	if hopB != "1" {
		t.Fatalf("Expected hop count '1', got '%s'", hopB)
	}
}

// TestGatewayMsgTraceDuplicateHopHeaderRawProtocol verifies hop count consistency
// at the raw HMSG protocol level.
func TestGatewayMsgTraceDuplicateHopHeaderRawProtocol(t *testing.T) {
	sa, sb, sc := setupGatewayClusterABC(t)
	defer sa.Shutdown()
	defer sb.Shutdown()
	defer sc.Shutdown()

	clientB, readerB, _ := newClientForServer(sb)
	defer clientB.close()
	clientC, readerC, _ := newClientForServer(sc)
	defer clientC.close()

	clientB.parseAsync("CONNECT {\"headers\":true,\"verbose\":false}\r\nSUB test.raw 1\r\nPING\r\n")
	clientC.parseAsync("CONNECT {\"headers\":true,\"verbose\":false}\r\nSUB test.raw 2\r\nPING\r\n")

	readerB.ReadString('\n')
	readerC.ReadString('\n')
	time.Sleep(200 * time.Millisecond)

	ncPub := natsConnect(t, sa.ClientURL())
	defer ncPub.Close()

	msg := nats.NewMsg("test.raw")
	msg.Header.Set(MsgTraceDest, "trace.dest")
	msg.Data = []byte("payload")
	require_NoError(t, ncPub.PublishMsg(msg))
	require_NoError(t, ncPub.Flush())

	hmsgPat := regexp.MustCompile(`HMSG\s+([^\s]+)\s+([^\s]+)\s+(\d+)\s+(\d+)\r\n`)

	readHopValue := func(reader *bufio.Reader) string {
		line, err := reader.ReadString('\n')
		require_NoError(t, err)

		matches := hmsgPat.FindStringSubmatch(line)
		if len(matches) != 5 {
			t.Fatalf("HMSG line doesn't match pattern: %q", line)
		}

		hdrSize, _ := strconv.Atoi(matches[3])
		totalSize, _ := strconv.Atoi(matches[4])

		headerBuf := make([]byte, hdrSize)
		_, err = io.ReadFull(reader, headerBuf)
		require_NoError(t, err)

		payloadBuf := make([]byte, totalSize-hdrSize)
		if len(payloadBuf) > 0 {
			io.ReadFull(reader, payloadBuf)
		}

		hopPrefix := []byte("Nats-Trace-Hop:")
		idx := bytes.Index(headerBuf, hopPrefix)
		if idx < 0 {
			return ""
		}

		valueStart := idx + len(hopPrefix)
		for valueStart < len(headerBuf) && (headerBuf[valueStart] == ' ' || headerBuf[valueStart] == '\t') {
			valueStart++
		}
		valueEnd := valueStart
		for valueEnd < len(headerBuf) && headerBuf[valueEnd] != '\r' && headerBuf[valueEnd] != '\n' {
			valueEnd++
		}
		return string(headerBuf[valueStart:valueEnd])
	}

	hopB := readHopValue(readerB)
	hopC := readHopValue(readerC)

	if hopB != hopC {
		t.Fatalf("Gateway B has hop '%s' but Gateway C has hop '%s', expected same hop count", hopB, hopC)
	}
	if hopB != "1" {
		t.Fatalf("Expected hop value '1', got '%s'", hopB)
	}
}
