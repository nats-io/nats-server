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

package server

import (
	"errors"
	"sync"
	"sync/atomic"
)

const ipQueueDefaultMaxRecycleSize = 4 * 1024

// This is a generic intra-process queue.
type ipQueue[T any] struct {
	inprogress int64
	sync.Mutex
	ch   chan struct{}
	elts []T
	pos  int
	pool *sync.Pool
	sz   uint64 // Calculated size (only if calc != nil)
	name string
	m    *sync.Map
	ipQueueOpts[T]
}

type ipQueueOpts[T any] struct {
	mrs  int              // Max recycle size
	calc func(e T) uint64 // Calc function for tracking size
	msz  uint64           // Limit by total calculated size
	mlen int              // Limit by number of entries
}

type ipQueueOpt[T any] func(*ipQueueOpts[T])

// This option allows to set the maximum recycle size when attempting
// to put back a slice to the pool.
func ipqMaxRecycleSize[T any](max int) ipQueueOpt[T] {
	return func(o *ipQueueOpts[T]) {
		o.mrs = max
	}
}

// This option enables total queue size counting by passing in a function
// that evaluates the size of each entry as it is pushed/popped. This option
// enables the size() function.
func ipqSizeCalculation[T any](calc func(e T) uint64) ipQueueOpt[T] {
	return func(o *ipQueueOpts[T]) {
		o.calc = calc
	}
}

// This option allows setting the maximum queue size. Once the limit is
// reached, then push() will stop returning true and no more entries will
// be stored until some more are popped. The ipQueue_SizeCalculation must
// be provided for this to work.
func ipqLimitBySize[T any](max uint64) ipQueueOpt[T] {
	return func(o *ipQueueOpts[T]) {
		o.msz = max
	}
}

// This option allows setting the maximum queue length. Once the limit is
// reached, then push() will stop returning true and no more entries will
// be stored until some more are popped.
func ipqLimitByLen[T any](max int) ipQueueOpt[T] {
	return func(o *ipQueueOpts[T]) {
		o.mlen = max
	}
}

var errIPQLenLimitReached = errors.New("IPQ len limit reached")
var errIPQSizeLimitReached = errors.New("IPQ size limit reached")

func newIPQueue[T any](s *Server, name string, opts ...ipQueueOpt[T]) *ipQueue[T] {
	q := &ipQueue[T]{
		ch:   make(chan struct{}, 1),
		pool: &sync.Pool{},
		name: name,
		m:    &s.ipQueues,
		ipQueueOpts: ipQueueOpts[T]{
			mrs: ipQueueDefaultMaxRecycleSize,
		},
	}
	for _, o := range opts {
		o(&q.ipQueueOpts)
	}
	s.ipQueues.Store(name, q)
	return q
}

// Add the element `e` to the queue, notifying the queue channel's `ch` if the
// entry is the first to be added, and returns the length of the queue after
// this element is added.
func (q *ipQueue[T]) push(e T) (int, error) {
	var signal bool
	q.Lock()
	l := len(q.elts) - q.pos
	if q.mlen > 0 && l == q.mlen {
		q.Unlock()
		return l, errIPQLenLimitReached
	}
	if l == 0 {
		signal = true
		eltsi := q.pool.Get()
		if eltsi != nil {
			// Reason we use pointer to slice instead of slice is explained
			// here: https://staticcheck.io/docs/checks#SA6002
			q.elts = (*(eltsi.(*[]T)))[:0]
		}
		if cap(q.elts) == 0 {
			q.elts = make([]T, 0, 32)
		}
	}
	if q.calc != nil {
		sz := q.calc(e)
		if q.msz > 0 && q.sz+sz > q.msz {
			q.Unlock()
			return l, errIPQSizeLimitReached
		}
		q.sz += sz
	}
	q.elts = append(q.elts, e)
	q.Unlock()
	if signal {
		select {
		case q.ch <- struct{}{}:
		default:
		}
	}
	return l + 1, nil
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
	var elts []T
	q.Lock()
	if q.pos == 0 {
		elts = q.elts
	} else {
		elts = q.elts[q.pos:]
	}
	q.elts, q.pos = nil, 0
	atomic.AddInt64(&q.inprogress, int64(len(elts)))
	if q.calc != nil {
		for _, e := range elts {
			q.sz -= q.calc(e)
		}
	}
	q.Unlock()
	return elts
}

func (q *ipQueue[T]) resetAndReturnToPool(elts *[]T) {
	(*elts) = (*elts)[:0]
	q.pool.Put(elts)
}

// Returns the first element from the queue, if any. See comment above
// regarding calling after being notified that there is something and
// the use of drain(). In short, the caller should always check the
// boolean return value to ensure that the value is genuine and not a
// default empty value.
func (q *ipQueue[T]) popOne() (T, bool) {
	q.Lock()
	l := len(q.elts) - q.pos
	if l < 1 {
		q.Unlock()
		var empty T
		return empty, false
	}
	e := q.elts[q.pos]
	q.pos++
	if q.calc != nil {
		q.sz -= q.calc(e)
	}
	l--
	if l > 0 {
		// We need to re-signal
		select {
		case q.ch <- struct{}{}:
		default:
		}
	} else {
		// We have just emptied the queue, so we can recycle now.
		q.resetAndReturnToPool(&q.elts)
		q.elts, q.pos = nil, 0
	}
	q.Unlock()
	return e, true
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
	// Update the in progress count.
	if len(*elts) > 0 {
		if atomic.AddInt64(&q.inprogress, int64(-(len(*elts)))) < 0 {
			atomic.StoreInt64(&q.inprogress, 0)
		}
	}
	// We also don't want to recycle huge slices, so check against the max.
	// q.mrs is normally immutable but can be changed, in a safe way, in some tests.
	if cap(*elts) > q.mrs {
		return
	}
	q.resetAndReturnToPool(elts)
}

// Returns the current length of the queue.
func (q *ipQueue[T]) len() int {
	q.Lock()
	defer q.Unlock()
	return len(q.elts) - q.pos
}

// Returns the calculated size of the queue (if ipQueue_SizeCalculation has been
// passed in), otherwise returns zero.
func (q *ipQueue[T]) size() uint64 {
	q.Lock()
	defer q.Unlock()
	return q.sz
}

// Empty the queue and consumes the notification signal if present.
// Note that this could cause a reader go routine that has been
// notified that there is something in the queue (reading from queue's `ch`)
// may then get nothing if `drain()` is invoked before the `pop()` or `popOne()`.
func (q *ipQueue[T]) drain() {
	if q == nil {
		return
	}
	q.Lock()
	if q.elts != nil {
		q.resetAndReturnToPool(&q.elts)
		q.elts, q.pos = nil, 0
	}
	q.sz = 0
	// Consume the signal if it was present to reduce the chance of a reader
	// routine to be think that there is something in the queue...
	select {
	case <-q.ch:
	default:
	}
	q.Unlock()
}

// Since the length of the queue goes to 0 after a pop(), it is good to
// have an insight on how many elements are yet to be processed after a pop().
// For that reason, the queue maintains a count of elements returned through
// the pop() API. When the caller will call q.recycle(), this count will
// be reduced by the size of the slice returned by pop().
func (q *ipQueue[T]) inProgress() int64 {
	return atomic.LoadInt64(&q.inprogress)
}

// Remove this queue from the server's map of ipQueues.
// All ipQueue operations (such as push/pop/etc..) are still possible.
func (q *ipQueue[T]) unregister() {
	if q == nil {
		return
	}
	q.m.Delete(q.name)
}
