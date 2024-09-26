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

const (
	ipQueueDefaultMaxRecycleSize = 4 * 1024
	ipQueueMaxPoolSize           = 5
)

// This is a generic intra-process queue.
// Fields are in specific order based on benchmark results.
type ipQueue[T any] struct {
	inprogress   int64
	inprogressSz uint64
	sync.Mutex
	ipQueueOpts[T]
	ch   chan struct{}
	elts []T
	pos  int
	sz   uint64 // Calculated size (only if calc != nil)
	psMu sync.Mutex
	pool [][]T
	tcps int
	name string
	m    *sync.Map
}

type ipQueueOpts[T any] struct {
	mrs  int              // Max recycle size
	calc func(e T) uint64 // Calc function for tracking size
	msz  uint64           // Limit by total calculated size
	mlen int              // Limit by number of elements
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
// that evaluates the size of each element as it is pushed/popped. This option
// enables the size() function.
// NOTE: The callback MUST NOT acquire any lock since it is invoked
// in some cases under the queue's lock, which could lead to lock inversion
// or queue lock's contention by slowing it down.
func ipqSizeCalculation[T any](calc func(e T) uint64) ipQueueOpt[T] {
	return func(o *ipQueueOpts[T]) {
		o.calc = calc
	}
}

// This option allows setting the maximum queue size. Once the limit is
// reached, then push() will return `errIPQSizeLimitReached` and no more
// elements will be stored until some are popped. The `ipQueue_SizeCalculation`
// must be provided for this to work.
func ipqLimitBySize[T any](max uint64) ipQueueOpt[T] {
	return func(o *ipQueueOpts[T]) {
		o.msz = max
	}
}

// This option allows setting the maximum queue length. Once the limit is
// reached, then push() will return `errIPQLenLimitReached` and no more
// elements will be stored until some more are popped.
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
// element is the first to be added, and returns the length and size of the queue
// after this element is added.
// If the queue has limits, this function checks against what is currently in
// the queue in addition to the "in progress" elements (the ones that were
// returned by `pop()`). It will return `errIPQLenLimitReached` or
// `errIPQSizeLimitReached` if adding the element would have gone over the length
// or size limit respectively.
func (q *ipQueue[T]) push(e T) (int, uint64, error) {
	var sz uint64
	if q.calc != nil {
		sz = q.calc(e)
	}
	q.Lock()
	l := len(q.elts) - q.pos
	qsz := q.sz
	// If max length is specified, check the current length + "in progress"
	// count to see if we are going to go over the limit.
	if q.mlen > 0 && l+int(atomic.LoadInt64(&q.inprogress))+1 > q.mlen {
		q.Unlock()
		return l, qsz, errIPQLenLimitReached
	}
	if sz > 0 {
		// Take into account the "in progress" size.
		if q.msz > 0 && q.sz+atomic.LoadUint64(&q.inprogressSz)+sz > q.msz {
			q.Unlock()
			return l, qsz, errIPQSizeLimitReached
		}
		q.sz += sz
	}
	if q.elts == nil {
		q.psMu.Lock()
		if pl := len(q.pool); pl > 0 {
			q.elts = q.pool[pl-1]
			q.tcps -= cap(q.elts)
			q.pool = q.pool[:pl-1]
		}
		q.psMu.Unlock()
		if q.elts == nil {
			q.elts = make([]T, 0, 32)
		}
	}
	q.elts = append(q.elts, e)
	q.Unlock()
	// Signal if the queue was empty before adding this element.
	if l == 0 {
		select {
		case q.ch <- struct{}{}:
		default:
		}
	}
	return l + 1, qsz + sz, nil
}

// Returns the whole list of elements currently present in the queue,
// emptying the queue. It also returns the length and size (if applicable)
// of the returned list. These two should be passed to `processed()` when
// dealing with individual elements of a queue with limites, and ultimately
// to `recycle()` when done with the `pop()` so that the "in progress" count
// and size numbers can be updated.
//
// This should be called after receiving a notification from the queue's `ch`
// notification channel that indicates that there is something in the queue.
// However, in cases where `drain()` may be called from another go routine, it
// is possible that a routine is notified that there is something, but by the
// time it calls `pop()`, the drain() would have emptied the queue. So the
// caller should never assume that pop() will return a slice of 1 or more.
func (q *ipQueue[T]) pop() ([]T, int64, uint64) {
	if q == nil {
		return nil, 0, 0
	}
	var elts []T
	q.Lock()
	if len(q.elts)-q.pos == 0 {
		q.Unlock()
		return nil, 0, 0
	}
	if q.pos == 0 {
		elts = q.elts
	} else {
		// This should not happen since normally callers won't mix use
		// of q.popOne() and q.pop() from the same queue.
		elts = q.elts[q.pos:]
	}
	qsz := q.sz
	q.elts, q.pos, q.sz = nil, 0, 0
	// Although we are using atomics, this needs to be done inside the lock
	// because otherwise it would defeat the purpose of the check in push()
	// that takes into account the number/size of "in progress" elements
	// to check for limits. The use of atomics is because the count/size
	// are decremented in processed() and/or recycle() without the queue's lock.
	if qsz > 0 {
		atomic.AddUint64(&q.inprogressSz, qsz)
	}
	atomic.AddInt64(&q.inprogress, int64(len(elts)))
	q.Unlock()
	return elts, int64(len(elts)), qsz
}

// Returns the first element from the queue, if any. See comment above
// regarding calling after being notified that there is something and
// the use of drain(). In short, the caller should always check the
// boolean return value to ensure that the value is genuine and not a
// default empty value.
func (q *ipQueue[T]) popOne() (T, bool) {
	q.Lock()
	l := len(q.elts) - q.pos
	if l == 0 {
		q.Unlock()
		var empty T
		return empty, false
	}
	e := q.elts[q.pos]
	l--
	if l > 0 {
		q.pos++
		if q.calc != nil {
			q.sz -= q.calc(e)
		}
		// We need to re-signal
		select {
		case q.ch <- struct{}{}:
		default:
		}
	} else {
		// We have just emptied the queue, so we can recycle now.
		if cap(q.elts) > q.mrs {
			q.elts = nil
		} else {
			q.elts = q.elts[:0]
		}
		q.pos, q.sz = 0, 0
	}
	q.Unlock()
	return e, true
}

// After a `pop()`, the slice can be recycled for the next `push()` when
// a first element is added to the queue.
// This will also decrement the "in progress" count/size with the given
// `ql` and `qsz` which were returned by `pop()` (and possibly updated
// if calling `processed()`).
func (q *ipQueue[T]) recycle(elts []T, ql int64, qsz uint64) {
	if q == nil || elts == nil {
		return
	}
	if qsz > 0 {
		atomic.AddUint64(&q.inprogressSz, ^(qsz - 1))
	}
	if ql > 0 {
		atomic.AddInt64(&q.inprogress, -ql)
	}
	// We also don't want to recycle huge slices, so check against the max.
	// q.mrs is normally immutable but can be changed, in a safe way, in some tests.
	c := cap(elts)
	if c > q.mrs {
		return
	}
	q.psMu.Lock()
	if len(q.pool) < ipQueueMaxPoolSize && q.tcps+c <= q.mrs {
		elts = elts[:0]
		q.pool = append(q.pool, elts)
		q.tcps += c
	}
	q.psMu.Unlock()
}

// When a queue has limits specified, `push()` will take into account the number
// and size of "in progress" elements, that is, the ones removed from a `pop()` call.
// When processing those elements, the caller should call `q.processed()` on
// individual elements to decrement the number and size of "in progress" elements.
func (q *ipQueue[T]) processed(e T, ql *int64, qsz *uint64) {
	if calc := q.calc; calc != nil {
		if sz := calc(e); sz > 0 {
			atomic.AddUint64(&q.inprogressSz, ^(sz - 1))
			*qsz -= sz
		}
	}
	atomic.AddInt64(&q.inprogress, -1)
	*ql--
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
		q.elts, q.pos, q.sz = nil, 0, 0
	}
	// Consume the signal if it was present to reduce the chance of a reader
	// routine to be think that there is something in the queue...
	select {
	case <-q.ch:
	default:
	}
	q.Unlock()
}

// Since the length of the queue goes to 0 after a `pop()`, it is good to
// have an insight on how many elements are yet to be processed.
// For that reason, the queue maintains a count of elements returned through
// the `pop()` API. This number will be reduced by the use of `processed()`
// and or `recycle()`.
func (q *ipQueue[T]) inProgress() int64 {
	return atomic.LoadInt64(&q.inprogress)
}

// Since the size of the queue goes to 0 after a `pop()`, it is good to
// have an insight on the size of the elements that are yet to be processed.
// For that reason, the queue maintains the size of elements returned through
// the `pop()` API. This size will be reduced by the use of `processed()`
// and or `recycle()`.
func (q *ipQueue[T]) inProgressSize() uint64 {
	return atomic.LoadUint64(&q.inprogressSz)
}

// Remove this queue from the server's map of ipQueues.
// All ipQueue operations (such as push/pop/etc..) are still possible.
func (q *ipQueue[T]) unregister() {
	if q == nil {
		return
	}
	q.m.Delete(q.name)
}
