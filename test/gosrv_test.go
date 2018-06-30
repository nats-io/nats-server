// Copyright 2012-2018 The NATS Authors
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
		createClientConn(t, "127.0.0.1", addr.Port)
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
