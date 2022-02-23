// Copyright 2021 The NATS Authors
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
	"sync"
)

const ipQueueDefaultMaxRecycleSize = 4 * 1024
const ipQueueDefaultWarnThreshold = 32 * 1024

type ipQueueLogger interface {
	// The ipQueue will invoke this function with the queue's name and the number
	// of pending elements. This call CANNOT block. It is ok to drop the logging
	// if desired, but not block.
	log(name string, pending int)
}

// This is a generic intra-process queue.
type ipQueue struct {
	sync.RWMutex
	ch     chan struct{}
	elts   []interface{}
	pos    int
	pool   *sync.Pool
	mrs    int
	name   string
	logger ipQueueLogger
	lt     int
}

type ipQueueOpts struct {
	maxRecycleSize int
	name           string
	logger         ipQueueLogger
}

type ipQueueOpt func(*ipQueueOpts)

// This option allows to set the maximum recycle size when attempting
// to put back a slice to the pool.
func ipQueue_MaxRecycleSize(max int) ipQueueOpt {
	return func(o *ipQueueOpts) {
		o.maxRecycleSize = max
	}
}

// This option provides the logger to be used by this queue to log
// when the number of pending elements reaches a certain threshold.
func ipQueue_Logger(name string, l ipQueueLogger) ipQueueOpt {
	return func(o *ipQueueOpts) {
		o.name, o.logger = name, l
	}
}

func newIPQueue(opts ...ipQueueOpt) *ipQueue {
	qo := ipQueueOpts{maxRecycleSize: ipQueueDefaultMaxRecycleSize}
	for _, o := range opts {
		o(&qo)
	}
	q := &ipQueue{
		ch:     make(chan struct{}, 1),
		mrs:    qo.maxRecycleSize,
		pool:   &sync.Pool{},
		name:   qo.name,
		logger: qo.logger,
		lt:     ipQueueDefaultWarnThreshold,
	}
	return q
}

// Add the element `e` to the queue, notifying the queue channel's `ch` if the
// entry is the first to be added, and returns the length of the queue after
// this element is added.
func (q *ipQueue) push(e interface{}) int {
	var signal bool
	q.Lock()
	l := len(q.elts) - q.pos
	if l == 0 {
		signal = true
		eltsi := q.pool.Get()
		if eltsi != nil {
			// Reason we use pointer to slice instead of slice is explained
			// here: https://staticcheck.io/docs/checks#SA6002
			q.elts = (*(eltsi.(*[]interface{})))[:0]
		}
		if cap(q.elts) == 0 {
			q.elts = make([]interface{}, 0, 32)
		}
	}
	q.elts = append(q.elts, e)
	l++
	if l >= q.lt && q.logger != nil && (l <= q.lt+10 || q.lt%10000 == 0) {
		q.logger.log(q.name, l)
	}
	q.Unlock()
	if signal {
		select {
		case q.ch <- struct{}{}:
		default:
		}
	}
	return l
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
func (q *ipQueue) pop() []interface{} {
	var elts []interface{}
	q.Lock()
	if q.pos == 0 {
		elts = q.elts
	} else {
		elts = q.elts[q.pos:]
	}
	q.elts, q.pos = nil, 0
	q.Unlock()
	return elts
}

func (q *ipQueue) resetAndReturnToPool(elts *[]interface{}) {
	for i, l := 0, len(*elts); i < l; i++ {
		(*elts)[i] = nil
	}
	q.pool.Put(elts)
}

// Returns the first element from the queue, if any. See comment above
// regarding calling after being notified that there is something and
// the use of drain(). In short, the caller should always expect that
// pop() or popOne() may return `nil`.
func (q *ipQueue) popOne() interface{} {
	q.Lock()
	l := len(q.elts) - q.pos
	if l < 1 {
		q.Unlock()
		return nil
	}
	e := q.elts[q.pos]
	q.pos++
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
	return e
}

// After a pop(), the slice can be recycled for the next push() when
// a first element is added to the queue.
// Reason we use pointer to slice instead of slice is explained
// here: https://staticcheck.io/docs/checks#SA6002
func (q *ipQueue) recycle(elts *[]interface{}) {
	// If invoked with an nil list, don't recyle.
	// We also don't want to recycle huge slices, so check against the max.
	// q.mrs is normally immutable but can be changed, in a safe way, in some tests.
	if elts == nil || *elts == nil || cap(*elts) > q.mrs {
		return
	}
	q.resetAndReturnToPool(elts)
}

// Returns the current length of the queue.
func (q *ipQueue) len() int {
	q.RLock()
	l := len(q.elts) - q.pos
	q.RUnlock()
	return l
}

// Empty the queue and consumes the notification signal if present.
// Note that this could cause a reader go routine that has been
// notified that there is something in the queue (reading from queue's `ch`)
// may then get nothing if `drain()` is invoked before the `pop()` or `popOne()`.
func (q *ipQueue) drain() {
	q.Lock()
	if q.elts != nil {
		q.resetAndReturnToPool(&q.elts)
		q.elts, q.pos = nil, 0
	}
	// Consume the signal if it was present to reduce the chance of a reader
	// routine to be think that there is something in the queue...
	select {
	case <-q.ch:
	default:
	}
	q.Unlock()
}
