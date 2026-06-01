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

package server

import (
	"sync/atomic"
	"time"
)

// Used to limit number of disk IO calls in flight since they could all be blocking an OS thread.
// https://github.com/nats-io/nats-server/issues/2742
type diskIOSemaphore struct {
	ch           chan struct{}
	waiters      atomic.Int64
	waits        atomic.Uint64
	waitNanos    atomic.Uint64
	maxWaitNanos atomic.Uint64
}

func newDiskIOSemaphore(n int) *diskIOSemaphore {
	d := &diskIOSemaphore{ch: make(chan struct{}, n)}
	for range n {
		d.ch <- struct{}{}
	}
	return d
}

func defaultDiskIOSemaphore() *diskIOSemaphore {
	// The disk IO semaphore used to be sized based on the number
	// of CPU cores. That policy led to poor use of devices that
	// can handle many requests in parallel, so simply cap the
	// number of concurrent IO requests handed to the Go runtime.
	return newDiskIOSemaphore(512)
}

func (d *diskIOSemaphore) acquire() {
	select {
	case <-d.ch:
		return
	default:
		// No slot available, count this
		// waiter before blocking.
		d.waiters.Add(1)
		start := time.Now()
		<-d.ch
		waited := time.Since(start)
		d.waiters.Add(-1)
		d.countWait(uint64(waited.Nanoseconds()))
	}
}

func (d *diskIOSemaphore) countWait(ns uint64) {
	d.waits.Add(1)
	d.waitNanos.Add(ns)
	for {
		cur := d.maxWaitNanos.Load()
		if ns <= cur || d.maxWaitNanos.CompareAndSwap(cur, ns) {
			return
		}
	}
}

func (d *diskIOSemaphore) release() {
	d.ch <- struct{}{}
}

func (d *diskIOSemaphore) cap() int {
	return cap(d.ch)
}
