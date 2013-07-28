// Copyright 2012 Apcera Inc. All rights reserved.

package test

import (
	"encoding/json"
	"fmt"
	"net"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/apcera/gnatsd/server"
)

func runRouteServer(t *testing.T) (*server.Server, *server.Options) {
	opts, err := server.ProcessConfigFile("./configs/cluster.conf")

	// Override for running in Go routine.
	opts.NoSigs = true
	//	opts.Debug  = true
		opts.Trace  = true
//	opts.NoLog = true

	if err != nil {
		t.Fatalf("Error parsing config file: %v\n", err)
	}
	return RunServer(opts), opts
}

func TestRouterListeningSocket(t *testing.T) {
	s, opts := runRouteServer(t)
	defer s.Shutdown()

	// Check that the cluster socket is able to be connected.
	addr := fmt.Sprintf("%s:%d", opts.ClusterHost, opts.ClusterPort)
	end := time.Now().Add(2 * time.Second)
	for time.Now().Before(end) {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			time.Sleep(50 * time.Millisecond)
			// Retry
			continue
		}
		conn.Close()
		return
	}
	t.Fatalf("Failed to connect to the cluster port: %q", addr)
}

func TestRouteGoServerShutdown(t *testing.T) {
	base := runtime.NumGoroutine()
	s, _ := runRouteServer(t)
	s.Shutdown()
	time.Sleep(10 * time.Millisecond)
	delta := (runtime.NumGoroutine() - base)
	if delta > 1 {
		t.Fatalf("%d Go routines still exist post Shutdown()", delta)
	}
}

func TestSendRouteInfoOnConnect(t *testing.T) {
	s, opts := runRouteServer(t)
	defer s.Shutdown()
	rc := createRouteConn(t, opts.ClusterHost, opts.ClusterPort)
	buf := expectResult(t, rc, infoRe)

	info := server.Info{}
	if err := json.Unmarshal(buf[4:], &info); err != nil {
		t.Fatalf("Could not unmarshal route info: %v", err)
	}
	if info.Port != opts.ClusterPort {
		t.Fatalf("Received wrong information for port, expected %d, got %d",
			info.Port, opts.ClusterPort)
	}
}

func TestSendRouteSubAndUnsub(t *testing.T) {
	s, opts := runRouteServer(t)
	defer s.Shutdown()

	c := createClientConn(t, opts.Host, opts.Port)
	defer c.Close()

	send, _ := setupConn(t, c)

	rc := createRouteConn(t, opts.ClusterHost, opts.ClusterPort)
	doDefaultConnect(t, rc)

	// Send SUB via client connection
	send("SUB foo 22\r\n")

	// Make sure the SUB is broadcast via the route
	buf := expectResult(t, rc, subRe)
	matches := subRe.FindAllSubmatch(buf, -1)
	rsid := string(matches[0][5])
	fmt.Printf("Received rsid for SUB of %s\n", rsid)
	if !strings.HasPrefix(rsid, "RSID:") {
		t.Fatalf("Got wrong RSID: %s\n", rsid)
	}

	// Send UNSUB via client connection
	send("UNSUB 22\r\n")

	// Make sure the SUB is broadcast via the route
	buf = expectResult(t, rc, unsubRe)
	matches = unsubRe.FindAllSubmatch(buf, -1)
	rsid2 := string(matches[0][1])

	if rsid2 != rsid {
		t.Fatalf("Expected rsid's to match. %q vs %q\n", rsid, rsid2)
	}
}

