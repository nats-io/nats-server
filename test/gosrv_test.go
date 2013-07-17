// Copyright 2012 Apcera Inc. All rights reserved.

package test

import (
	"runtime"
	"testing"
	"time"

	. "github.com/apcera/gnatsd/test/unittest"
)

func TestSimpleGoServerShutdown(t *testing.T) {
	StartTest(t)
	defer FinishTest(t)

	s := runDefaultServer()
	base := runtime.NumGoroutine()
	s.Shutdown()
	time.Sleep(10 * time.Millisecond)
	delta := (runtime.NumGoroutine() - base)
	if delta > 0 {
		t.Fatalf("%d Go routines still exist post Shutdown()", delta)
	}
}

func TestGoServerShutdownWithClients(t *testing.T) {
	StartTest(t)
	defer FinishTest(t)

	s := runDefaultServer()
	for i := 0; i < 10; i++ {
		createClientConn(t, "localhost", 4222)
	}
	base := runtime.NumGoroutine()
	s.Shutdown()
	time.Sleep(10 * time.Millisecond)
	delta := (runtime.NumGoroutine() - base)
	if delta > 0 {
		t.Fatalf("%d Go routines still exist post Shutdown()", delta)
	}
}
