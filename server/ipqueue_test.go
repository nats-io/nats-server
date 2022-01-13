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
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestIPQueueBasic(t *testing.T) {
	q := newIPQueue()
	// Check that pool has been created
	if q.pool == nil {
		t.Fatal("Expected pool to have been created")
	}
	// Check for the default mrs
	if q.mrs != ipQueueDefaultMaxRecycleSize {
		t.Fatalf("Expected default max recycle size to be %v, got %v",
			ipQueueDefaultMaxRecycleSize, q.mrs)
	}
	select {
	case <-q.ch:
		t.Fatalf("Should not have been notified")
	default:
		// OK!
	}
	if l := q.len(); l != 0 {
		t.Fatalf("Expected len to be 0, got %v", l)
	}

	// Try to change the max recycle size
	q = newIPQueue(ipQueue_MaxRecycleSize(10))
	if q.mrs != 10 {
		t.Fatalf("Expected max recycle size to be 10, got %v", q.mrs)
	}
}

func TestIPQueuePush(t *testing.T) {
	q := newIPQueue()
	q.push(1)
	if l := q.len(); l != 1 {
		t.Fatalf("Expected len to be 1, got %v", l)
	}
	select {
	case <-q.ch:
		// OK
	default:
		t.Fatalf("Should have been notified of addition")
	}
	// Push a new element, we should not be notified.
	q.push(2)
	if l := q.len(); l != 2 {
		t.Fatalf("Expected len to be 2, got %v", l)
	}
	select {
	case <-q.ch:
		t.Fatalf("Should not have been notified of addition")
	default:
		// OK
	}
}

func TestIPQueuePop(t *testing.T) {
	q := newIPQueue()
	q.push(1)
	<-q.ch
	elts := q.pop()
	if l := len(elts); l != 1 {
		t.Fatalf("Expected 1 elt, got %v", l)
	}
	if l := q.len(); l != 0 {
		t.Fatalf("Expected len to be 0, got %v", l)
	}
	// The channel notification should be empty
	select {
	case <-q.ch:
		t.Fatalf("Should not have been notified of addition")
	default:
		// OK
	}
	// If we call pop() now, we should get an empty list.
	if elts = q.pop(); elts != nil {
		t.Fatalf("Expected nil, got %v", elts)
	}
}

func TestIPQueuePopOne(t *testing.T) {
	q := newIPQueue()
	q.push(1)
	<-q.ch
	e := q.popOne()
	if e == nil {
		t.Fatal("Got nil")
	}
	if i := e.(int); i != 1 {
		t.Fatalf("Expected 1, got %v", i)
	}
	if l := q.len(); l != 0 {
		t.Fatalf("Expected len to be 0, got %v", l)
	}
	select {
	case <-q.ch:
		t.Fatalf("Should not have been notified of addition")
	default:
		// OK
	}
	q.push(2)
	q.push(3)
	e = q.popOne()
	if e == nil {
		t.Fatal("Got nil")
	}
	if i := e.(int); i != 2 {
		t.Fatalf("Expected 2, got %v", i)
	}
	if l := q.len(); l != 1 {
		t.Fatalf("Expected len to be 1, got %v", l)
	}
	select {
	case <-q.ch:
		// OK
	default:
		t.Fatalf("Should have been notified that there is more")
	}
	e = q.popOne()
	if e == nil {
		t.Fatal("Got nil")
	}
	if i := e.(int); i != 3 {
		t.Fatalf("Expected 3, got %v", i)
	}
	if l := q.len(); l != 0 {
		t.Fatalf("Expected len to be 0, got %v", l)
	}
	select {
	case <-q.ch:
		t.Fatalf("Should not have been notified that there is more")
	default:
		// OK
	}
	// Calling it again now that we know there is nothing, we
	// should get nil.
	if e = q.popOne(); e != nil {
		t.Fatalf("Expected nil, got %v", e)
	}

	q = newIPQueue()
	q.push(1)
	q.push(2)
	// Capture current capacity
	q.RLock()
	c := cap(q.elts)
	q.RUnlock()
	e = q.popOne()
	if e == nil || e.(int) != 1 {
		t.Fatalf("Invalid value: %v", e)
	}
	if l := q.len(); l != 1 {
		t.Fatalf("Expected len to be 1, got %v", l)
	}
	values := q.pop()
	if len(values) != 1 || values[0].(int) != 2 {
		t.Fatalf("Unexpected values: %v", values)
	}
	if cap(values) != c-1 {
		t.Fatalf("Unexpected capacity: %v vs %v", cap(values), c-1)
	}
	if l := q.len(); l != 0 {
		t.Fatalf("Expected len to be 0, got %v", l)
	}
	// Just make sure that this is ok...
	q.recycle(&values)
}

func TestIPQueueMultiProducers(t *testing.T) {
	q := newIPQueue()

	wg := sync.WaitGroup{}
	wg.Add(3)
	send := func(start, end int) {
		defer wg.Done()

		for i := start; i <= end; i++ {
			q.push(i)
		}
	}
	go send(1, 100)
	go send(101, 200)
	go send(201, 300)

	tm := time.NewTimer(2 * time.Second)
	m := make(map[int]struct{})
	for done := false; !done; {
		select {
		case <-q.ch:
			values := q.pop()
			for _, v := range values {
				m[v.(int)] = struct{}{}
			}
			q.recycle(&values)
			done = len(m) == 300
		case <-tm.C:
			t.Fatalf("Did not receive all elements: %v", m)
		}
	}
	wg.Wait()
}

func TestIPQueueRecycle(t *testing.T) {
	q := newIPQueue()
	total := 1000
	for iter := 0; iter < 5; iter++ {
		var sz int
		for i := 0; i < total; i++ {
			sz = q.push(i)
		}
		if sz != total {
			t.Fatalf("Expected size to be %v, got %v", total, sz)
		}
		values := q.pop()
		preRecycleCap := cap(values)
		q.recycle(&values)
		sz = q.push(1001)
		if sz != 1 {
			t.Fatalf("Expected size to be %v, got %v", 1, sz)
		}
		values = q.pop()
		if l := len(values); l != 1 {
			t.Fatalf("Len should be 1, got %v", l)
		}
		if c := cap(values); c == preRecycleCap {
			break
		} else if iter == 4 {
			// We can't fail the test since there is no guarantee that the slice
			// is still present in the pool when we do a Get(), but let's log that
			// recycling did not occur even after all iterations..
			t.Logf("Seem like the previous slice was not recycled, old cap=%v new cap=%v",
				preRecycleCap, c)
		}
	}

	q = newIPQueue(ipQueue_MaxRecycleSize(10))
	for i := 0; i < 100; i++ {
		q.push(i)
	}
	values := q.pop()
	preRecycleCap := cap(values)
	q.recycle(&values)
	q.push(1001)
	values = q.pop()
	if l := len(values); l != 1 {
		t.Fatalf("Len should be 1, got %v", l)
	}
	// This time, we should not have recycled it, so the new cap should
	// be 1 for the new element added. In case Go creates a slice of
	// cap more than 1 in some future release, just check that the
	// cap is lower than the pre recycle cap.
	if c := cap(values); c >= preRecycleCap {
		t.Fatalf("The slice should not have been put back in the pool, got cap of %v", c)
	}

	// Also check that if we mistakenly pop a queue that was not
	// notified (pop() will return nil), and we try to recycle,
	// recycle() will ignore the call.
	values = q.pop()
	q.recycle(&values)
	q.push(1002)
	q.RLock()
	recycled := &q.elts == &values
	q.RUnlock()
	if recycled {
		t.Fatalf("Unexpected recycled slice")
	}
	// Check that we don't crash when recycling a nil or empty slice
	values = q.pop()
	q.recycle(&values)
	q.recycle(nil)
}

func TestIPQueueDrain(t *testing.T) {
	q := newIPQueue()
	for iter, recycled := 0, false; iter < 5 && !recycled; iter++ {
		for i := 0; i < 100; i++ {
			q.push(i + 1)
		}
		q.drain()
		// Try to get something from the pool right away
		s := q.pool.Get()
		recycled := s != nil
		if !recycled {
			// We can't fail the test, since we have no guarantee it will be recycled
			// especially when running with `-race` flag...
			if iter == 4 {
				t.Log("nothing was recycled")
			}
		}
		// Check that we have consumed the signal...
		select {
		case <-q.ch:
			t.Fatal("Signal should have been consumed by drain")
		default:
			// OK!
		}
		// Check len
		if l := q.len(); l != 0 {
			t.Fatalf("Expected len to be 0, got %v", l)
		}
		if recycled {
			break
		}
	}
}

type testIPQLog struct {
	msgs []string
}

func (l *testIPQLog) log(name string, pending int) {
	l.msgs = append(l.msgs, fmt.Sprintf("%s: %d pending", name, pending))
}

func TestIPQueueLogger(t *testing.T) {
	l := &testIPQLog{}
	q := newIPQueue(ipQueue_Logger("test_logger", l))
	q.lt = 2
	q.push(1)
	q.push(2)
	if len(l.msgs) != 1 {
		t.Fatalf("Unexpected logging: %v", l.msgs)
	}
	if l.msgs[0] != "test_logger: 2 pending" {
		t.Fatalf("Unexpected content: %v", l.msgs[0])
	}
	l.msgs = nil
	q.push(3)
	if len(l.msgs) != 1 {
		t.Fatalf("Unexpected logging: %v", l.msgs)
	}
	if l.msgs[0] != "test_logger: 3 pending" {
		t.Fatalf("Unexpected content: %v", l.msgs[0])
	}
	l.msgs = nil
	q.popOne()
	q.push(4)
	if len(l.msgs) != 1 {
		t.Fatalf("Unexpected logging: %v", l.msgs)
	}
	if l.msgs[0] != "test_logger: 3 pending" {
		t.Fatalf("Unexpected content: %v", l.msgs[0])
	}
	l.msgs = nil
	q.pop()
	q.push(5)
	if len(l.msgs) != 0 {
		t.Fatalf("Unexpected logging: %v", l.msgs)
	}
	q.push(6)
	if len(l.msgs) != 1 {
		t.Fatalf("Unexpected logging: %v", l.msgs)
	}
	if l.msgs[0] != "test_logger: 2 pending" {
		t.Fatalf("Unexpected content: %v", l.msgs[0])
	}
}
