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

// ats controls the go routines for the access time service.
// This allows more efficient unixnano operations for cache access times.
// We will have one per binary (usually per server).
package ats

import (
	"sync"
	"sync/atomic"
	"time"
)

// Update every 100ms for gathering access time in unix nano.
const TickInterval = 100 * time.Millisecond

var (
	// Serializes Register/Unregister so the lifetime of the time-keeping Go
	// routine (and its WaitGroup) is never manipulated concurrently. Stores can
	// be created and stopped from different goroutines, so the refs 0<->1
	// transitions that start and stop the routine must not overlap.
	mu sync.Mutex
	// Our unix nano time.
	utime atomic.Int64
	// How may registered users do we have, controls lifetime of Go routine.
	refs atomic.Int64
	// To signal the shutdown of the Go routine.
	done chan struct{}
	// To wait for the Go routine to actually exit on shutdown, so that
	// Unregister is synchronous and does not leak a goroutine past its return.
	wg sync.WaitGroup
)

func init() {
	// Initialize our done chan.
	done = make(chan struct{}, 1)
}

// Register usage. This will happen on filestore creation.
func Register() {
	mu.Lock()
	defer mu.Unlock()
	if v := refs.Add(1); v == 1 {
		// This is the first to register (could also go up and down),
		// so spin up Go routine and grab initial time.
		utime.Store(time.Now().UnixNano())

		wg.Add(1)
		go func() {
			defer wg.Done()
			ticker := time.NewTicker(TickInterval)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					utime.Store(time.Now().UnixNano())
				case <-done:
					return
				}
			}
		}()
	}
}

// Unregister usage. We will shutdown the go routine if no more registered users.
func Unregister() {
	mu.Lock()
	defer mu.Unlock()
	if v := refs.Add(-1); v == 0 {
		done <- struct{}{}
		// Wait for the Go routine to fully exit before returning so that,
		// once Unregister returns, the access time service is completely
		// shut down and no goroutine lingers into a subsequent Register.
		wg.Wait()
	} else if v < 0 {
		refs.Store(0)
		panic("unbalanced unregister for access time state")
	}
}

// Will load the access time from an atomic.
// If no one has registered this will return 0 or stale data.
// It is the responsibility of the user to properly register and unregister.
func AccessTime() int64 {
	// Return last updated time.
	v := utime.Load()
	if v == 0 {
		// Always register a time, the worst case is a stale time.
		// On startup, we can register in parallel and could previously panic.
		v = time.Now().UnixNano()
		utime.Store(v)
	}
	return v
}
