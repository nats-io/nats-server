// Copyright 2026 The NATS Authors
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

package ats_test

import (
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server/ats"
)

func TestConcurrentLifecycle(t *testing.T) {
	const workers, cycles = 8, 100
	start := make(chan struct{})
	var clients sync.WaitGroup
	clients.Add(workers)
	for range workers {
		go func() {
			defer clients.Done()
			<-start
			for range cycles {
				ats.Register()
				if ats.AccessTime() == 0 {
					t.Error("Expected non-zero access time while registered")
				}
				ats.Unregister()
			}
		}()
	}
	close(start)
	finished := make(chan struct{})
	go func() {
		clients.Wait()
		close(finished)
	}()
	select {
	case <-finished:
	case <-time.After(10 * time.Second):
		t.Fatal("Concurrent ATS lifecycle calls did not finish")
	}

	at := ats.AccessTime()
	if at == 0 {
		t.Fatal("Expected non-zero access time after registering")
	}
	time.Sleep(2 * ats.TickInterval)
	if ats.AccessTime() != at {
		t.Fatal("Access time changed after all clients unregistered")
	}
}
