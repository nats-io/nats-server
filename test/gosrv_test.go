// Copyright 2012-2013 Apcera Inc. All rights reserved.

package test

import (
	"runtime"
	"testing"
	"time"
)

func TestSimpleGoServerShutdown(t *testing.T) {
	base := runtime.NumGoroutine()
	s := runDefaultServer()
	s.Shutdown()
	time.Sleep(10 * time.Millisecond)
	delta := (runtime.NumGoroutine() - base)
	if delta > 1 {
		t.Fatalf("%d Go routines still exist post Shutdown()", delta)
	}
}

func TestGoServerShutdownWithClients(t *testing.T) {
	base := runtime.NumGoroutine()
	s := runDefaultServer()
	for i := 0; i < 10; i++ {
		createClientConn(t, "localhost", 4222)
	}
	s.Shutdown()
	// Wait longer for client connections
	time.Sleep(50 * time.Millisecond)
	delta := (runtime.NumGoroutine() - base)
	if delta > 1 {
		t.Fatalf("%d Go routines still exist post Shutdown()", delta)
	}
}

func TestGoServerMultiShutdown(t *testing.T) {
	s := runDefaultServer()
	s.Shutdown()
	s.Shutdown()
}
