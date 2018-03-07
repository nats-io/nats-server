// Copyright 2012-2013 Apcera Inc. All rights reserved.

package test

import (
	"net"
	"runtime"
	"testing"
	"time"
)

func TestSimpleGoServerShutdown(t *testing.T) {
	base := runtime.NumGoroutine()
	opts := DefaultTestOptions
	opts.Port = -1
	s := RunServer(&opts)
	s.Shutdown()
	time.Sleep(100 * time.Millisecond)
	delta := (runtime.NumGoroutine() - base)
	if delta > 1 {
		t.Fatalf("%d Go routines still exist post Shutdown()", delta)
	}
}

func TestGoServerShutdownWithClients(t *testing.T) {
	base := runtime.NumGoroutine()
	opts := DefaultTestOptions
	opts.Port = -1
	s := RunServer(&opts)
	addr := s.Addr().(*net.TCPAddr)
	for i := 0; i < 50; i++ {
		createClientConn(t, "localhost", addr.Port)
	}
	s.Shutdown()
	// Wait longer for client connections
	time.Sleep(1 * time.Second)
	delta := (runtime.NumGoroutine() - base)
	// There may be some finalizers or IO, but in general more than
	// 2 as a delta represents a problem.
	if delta > 2 {
		t.Fatalf("%d Go routines still exist post Shutdown()", delta)
	}
}

func TestGoServerMultiShutdown(t *testing.T) {
	opts := DefaultTestOptions
	opts.Port = -1
	s := RunServer(&opts)
	s.Shutdown()
	s.Shutdown()
}
