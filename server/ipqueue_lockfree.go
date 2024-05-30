// Copyright 2021-2023 The NATS Authors
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

//go:build !lockfreeipq

package server

import (
	"sync"
	"sync/atomic"
)

const ipQueueDefaultMaxRecycleSize = 4 * 1024

// This is a lock-free implementation of a generic intra-process queue.
type ipQueue[T any] struct {
	ch         chan struct{}                     // Used to signal waiting listeners
	head       atomic.Pointer[ipQueueElement[T]] // First element in the queue
	tail       atomic.Pointer[ipQueueElement[T]] // Last element in the queue
	sz         atomic.Int64                      // Current size of the IP queue
	inprogress atomic.Int64                      // How many elements have been pulled off?
	pool       *sync.Pool                        // Elements are recycled here
	mrs        int                               // Max recycle size
	name       string                            // Name of this queue
	m          *sync.Map                         // Map to all registered IP queues
}

type ipQueueElement[T any] struct {
	v    atomic.Pointer[T]
	next atomic.Pointer[ipQueueElement[T]]
}

type ipQueueOpts struct {
	maxRecycleSize int
}

type ipQueueOpt func(*ipQueueOpts)

func newIPQueue[T any](s *Server, name string, opts ...ipQueueOpt) *ipQueue[T] {
	qo := ipQueueOpts{maxRecycleSize: ipQueueDefaultMaxRecycleSize}
	for _, o := range opts {
		o(&qo)
	}
	q := &ipQueue[T]{
		ch:   make(chan struct{}, 1),
		mrs:  qo.maxRecycleSize,
		pool: &sync.Pool{},
		m:    &s.ipQueues,
	}
	s.ipQueues.Store(name, q)
	return q
}

func (q *ipQueue[T]) Lock()   {} // Gets inlined away but satisfies callsites
func (q *ipQueue[T]) Unlock() {} // Gets inlined away but satisfies callsites

// Add the element `e` to the queue, notifying the queue channel's `ch` if the
// entry is the first to be added, and returns the length of the queue after
// this element is added.
func (q *ipQueue[T]) push(e T) int {
	new := &ipQueueElement[T]{}
	new.v.Store(&e)
	for {
		tail := q.tail.Load()
		if !q.tail.CompareAndSwap(tail, new) {
			continue
		}
		if tail != nil {
			tail.next.CompareAndSwap(nil, new)
		}
		if q.head.CompareAndSwap(nil, new) {
			select {
			case q.ch <- struct{}{}:
			default:
			}
		}
		return int(q.sz.Add(1))
	}
}

// Returns the whole list of elements currently present in the queue,
// emptying the queue. This should be called after receiving a notification
// from the queue's `ch` notification channel that indicates that there
// is something in the queue.
// However, in cases where `drain()` may be called from another go
// routine, it is possible that a routine is notified that there is
// something, but by the time it calls `pop()`, the drain() would have
// emptied the queue. So the caller should never assume that pop() will
// return a slice of 1 or more, it could return `nil`.
func (q *ipQueue[T]) pop() []T {
	if q == nil {
		return nil
	}
	for {
		head := q.head.Load()
		if head == nil {
			return nil
		}
		tail := q.tail.Load()
		if !q.head.CompareAndSwap(head, nil) {
			continue
		}
		q.tail.CompareAndSwap(tail, nil)
		var elts []T
		if eltsi := q.pool.Get(); eltsi != nil {
			// Reason we use pointer to slice instead of slice is explained
			// here: https://staticcheck.io/docs/checks#SA6002
			elts = (*(eltsi.(*[]T)))[:0]
		}
		if cap(elts) == 0 {
			elts = make([]T, 0, 32)
		}
		for ptr := head; ptr != nil; ptr = ptr.next.Load() {
			if v := ptr.v.Load(); v != nil {
				elts = append(elts, *ptr.v.Load())
			}
		}
		c := int64(len(elts))
		q.inprogress.Add(c)
		q.sz.Add(-c)
		return elts
	}
}

// Returns the first element from the queue, if any. See comment above
// regarding calling after being notified that there is something and
// the use of drain(). In short, the caller should always check the
// boolean return value to ensure that the value is genuine and not a
// default empty value.
func (q *ipQueue[T]) popOne() (T, bool) {
	var elt T
	for {
		head := q.head.Load()
		if head == nil {
			return elt, false
		}
		next := head.next.Load()
		if q.head.CompareAndSwap(head, next) {
			if v := head.v.Load(); v != nil {
				elt = *v
			}
			q.tail.CompareAndSwap(head, nil)
			q.sz.Add(-1)
			return elt, true
		}
	}
}

// After a pop(), the slice can be recycled for the next push() when
// a first element is added to the queue.
// This will also decrement the "in progress" count with the length
// of the slice.
// Reason we use pointer to slice instead of slice is explained
// here: https://staticcheck.io/docs/checks#SA6002
func (q *ipQueue[T]) recycle(elts *[]T) {
	// If invoked with a nil list, nothing to do.
	if elts == nil || *elts == nil {
		return
	}
	c := int64(len(*elts))
	q.inprogress.Add(-c)
	// We also don't want to recycle huge slices, so check against the max.
	// q.mrs is normally immutable but can be changed, in a safe way, in some tests.
	if cap(*elts) > q.mrs {
		return
	}
	q.pool.Put(elts)
}

// Returns the current length of the queue.
func (q *ipQueue[T]) len() int {
	return int(q.sz.Load())
}

// Empty the queue and consumes the notification signal if present.
// Note that this could cause a reader go routine that has been
// notified that there is something in the queue (reading from queue's `ch`)
// may then get nothing if `drain()` is invoked before the `pop()` or `popOne()`.
func (q *ipQueue[T]) drain() {
	if q == nil {
		return
	}
	for {
		if head := q.head.Load(); q.head.CompareAndSwap(head, nil) {
			q.tail.Store(nil)
			q.sz.Store(0)
			// Consume the signal if it was present to reduce the chance of a reader
			// routine to be think that there is something in the queue...
			select {
			case <-q.ch:
			default:
			}
			return
		}
	}
}

// Since the length of the queue goes to 0 after a pop(), it is good to
// have an insight on how many elements are yet to be processed after a pop().
// For that reason, the queue maintains a count of elements returned through
// the pop() API. When the caller will call q.recycle(), this count will
// be reduced by the size of the slice returned by pop().
func (q *ipQueue[T]) inProgress() int64 {
	return q.inprogress.Load()
}

// Remove this queue from the server's map of ipQueues.
// All ipQueue operations (such as push/pop/etc..) are still possible.
func (q *ipQueue[T]) unregister() {
	if q == nil {
		return
	}
	q.m.Delete(q.name)
}
