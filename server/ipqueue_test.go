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
	"testing"
	"time"
)

func TestIPQueueBasic(t *testing.T) {
	s := &Server{}
	q := s.newIPQueue("test")
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
	q2 := s.newIPQueue("test2", ipQueue_MaxRecycleSize(10))
	if q2.mrs != 10 {
		t.Fatalf("Expected max recycle size to be 10, got %v", q2.mrs)
	}

	// Check that those 2 queues are registered
	var gotFirst bool
	var gotSecond bool
	s.ipQueues.Range(func(k, v interface{}) bool {
		switch k.(string) {
		case "test":
			gotFirst = true
		case "test2":
			gotSecond = true
		default:
			t.Fatalf("Unknown queue: %q", k.(string))
		}
		return true
	})
	if !gotFirst {
		t.Fatalf("Did not find queue %q", "test")
	}
	if !gotSecond {
		t.Fatalf("Did not find queue %q", "test2")
	}
	// Unregister them
	q.unregister()
	q2.unregister()
	// They should have been removed from the map
	s.ipQueues.Range(func(k, v interface{}) bool {
		t.Fatalf("Got queue %q", k.(string))
		return false
	})
	// But verify that we can still push/pop
	q.push(1)
	elts := q.pop()
	if len(elts) != 1 {
		t.Fatalf("Should have gotten 1 element, got %v", len(elts))
	}
	q2.push(2)
	if e := q2.popOne(); e.(int) != 2 {
		t.Fatalf("popOne failed: %+v", e)
	}
}

func TestIPQueuePush(t *testing.T) {
	s := &Server{}
	q := s.newIPQueue("test")
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
	s := &Server{}
	q := s.newIPQueue("test")
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
	// Since pop() brings the number of pending to 0, we keep track of the
	// number of "in progress" elements. Check that the value is 1 here.
	if n := q.inProgress(); n != 1 {
		t.Fatalf("Expected count to be 1, got %v", n)
	}
	// Recycling will bring it down to 0.
	q.recycle(&elts)
	if n := q.inProgress(); n != 0 {
		t.Fatalf("Expected count to be 0, got %v", n)
	}
	// If we call pop() now, we should get an empty list.
	if elts = q.pop(); elts != nil {
		t.Fatalf("Expected nil, got %v", elts)
	}
	// The in progress count should still be 0
	if n := q.inProgress(); n != 0 {
		t.Fatalf("Expected count to be 0, got %v", n)
	}
}

func TestIPQueuePopOne(t *testing.T) {
	s := &Server{}
	q := s.newIPQueue("test")
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
	// That does not affect the number of notProcessed
	if n := q.inProgress(); n != 0 {
		t.Fatalf("Expected count to be 0, got %v", n)
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

	q = s.newIPQueue("test2")
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
	s := &Server{}
	q := s.newIPQueue("test")

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
			if n := q.inProgress(); n != 0 {
				t.Fatalf("Expected count to be 0, got %v", n)
			}
			done = len(m) == 300
		case <-tm.C:
			t.Fatalf("Did not receive all elements: %v", m)
		}
	}
	wg.Wait()
}

func TestIPQueueRecycle(t *testing.T) {
	s := &Server{}
	q := s.newIPQueue("test")
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

	q = s.newIPQueue("test2", ipQueue_MaxRecycleSize(10))
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
	s := &Server{}
	q := s.newIPQueue("test")
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
