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
	"sync/atomic"
	"testing"
	"time"
)

func TestIPQueueBasic(t *testing.T) {
	s := &Server{}
	q := newIPQueue[int](s, "test")
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
	q2 := newIPQueue[int](s, "test2", ipqMaxRecycleSize[int](10))
	if q2.mrs != 10 {
		t.Fatalf("Expected max recycle size to be 10, got %v", q2.mrs)
	}

	// Check that those 2 queues are registered
	var gotFirst bool
	var gotSecond bool
	s.ipQueues.Range(func(k, v any) bool {
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
	s.ipQueues.Range(func(k, v any) bool {
		t.Fatalf("Got queue %q", k.(string))
		return false
	})
	// But verify that we can still push/pop
	q.push(1)
	elts, ql, qsz := q.pop()
	require_Equal(t, len(elts), 1)
	require_Equal(t, ql, 1)
	// Since there is no calculation function, size should be 0
	require_Equal(t, qsz, 0)
	q.recycle(elts, ql, qsz)
	q2.push(2)
	if e, ok := q2.popOne(); !ok || e != 2 {
		t.Fatalf("popOne failed: %+v", e)
	}
}

func TestIPQueuePush(t *testing.T) {
	s := &Server{}
	q := newIPQueue[int](s, "test")
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
	// Use a calculation function that returns the value of the element as its size
	q := newIPQueue[int](s, "test", ipqSizeCalculation[int](func(e int) uint64 { return uint64(e) }))
	q.push(1)
	q.push(2)
	q.push(3)
	// Make sure that we get a signal
	<-q.ch
	// pop() will make the length/size go to 0, but the in progress count/size will go up.
	elts, ql, qsz := q.pop()
	require_Equal(t, len(elts), 3)
	require_Equal(t, ql, 3)
	require_Equal(t, qsz, 1+2+3)
	require_Equal(t, q.len(), 0)
	require_Equal(t, q.size(), 0)
	require_Equal(t, q.inProgress(), 3)
	require_Equal(t, q.inProgressSize(), 1+2+3)
	// recylce() will bring the in progress numbers down to 0.
	q.recycle(elts, ql, qsz)
	require_Equal(t, q.len(), 0)
	require_Equal(t, q.size(), 0)
	require_Equal(t, q.inProgress(), 0)
	require_Equal(t, q.inProgressSize(), 0)

	// Put more elements now and we will check the ability to indicate progress.
	q.push(4)
	q.push(5)
	q.push(6)
	q.push(7)
	// Check the expected length/size
	require_Equal(t, q.len(), 4)
	require_Equal(t, q.size(), 4+5+6+7)
	require_Equal(t, q.inProgress(), 0)
	require_Equal(t, q.inProgressSize(), 0)

	// pop() will make the length/size go to 0, but the in progress count/size will go up.
	elts, ql, qsz = q.pop()
	require_Equal(t, len(elts), 4)
	require_Equal(t, ql, 4)
	require_Equal(t, qsz, 4+5+6+7)
	require_Equal(t, q.len(), 0)
	require_Equal(t, q.size(), 0)
	require_Equal(t, q.inProgress(), 4)
	require_Equal(t, q.inProgressSize(), 4+5+6+7)

	// Now indicate progress
	q.processed(elts[0], &ql, &qsz)
	// Should have reduced ql and qsz
	require_Equal(t, ql, 3)
	require_Equal(t, qsz, 5+6+7)
	require_Equal(t, q.len(), 0)
	require_Equal(t, q.size(), 0)
	require_Equal(t, q.inProgress(), 3)
	require_Equal(t, q.inProgressSize(), 5+6+7)

	// More progress
	q.processed(elts[1], &ql, &qsz)
	require_Equal(t, ql, 2)
	require_Equal(t, qsz, 6+7)
	require_Equal(t, q.len(), 0)
	require_Equal(t, q.size(), 0)
	require_Equal(t, q.inProgress(), 2)
	require_Equal(t, q.inProgressSize(), 6+7)

	// Finish with the recycle.
	q.recycle(elts, ql, qsz)
	require_Equal(t, q.len(), 0)
	require_Equal(t, q.size(), 0)
	require_Equal(t, q.inProgress(), 0)
	require_Equal(t, q.inProgressSize(), 0)

	// If we call pop() now, we should get nil, 0, 0.
	if elts, ql, qsz = q.pop(); elts != nil || ql != 0 || qsz != 0 {
		t.Fatalf("Expected nil, 0, 0, got %v, %v, %v", elts, ql, qsz)
	}
	// The in progress numbers should still be 0
	require_Equal(t, q.inProgress(), 0)
	require_Equal(t, q.inProgressSize(), 0)
}

func TestIPQueuePopOne(t *testing.T) {
	s := &Server{}
	q := newIPQueue[int](s, "test")
	q.push(1)
	<-q.ch
	e, ok := q.popOne()
	if !ok {
		t.Fatal("Got nil")
	}
	if i := e; i != 1 {
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
	e, ok = q.popOne()
	if !ok {
		t.Fatal("Got nil")
	}
	if i := e; i != 2 {
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
	e, ok = q.popOne()
	if !ok {
		t.Fatal("Got nil")
	}
	if i := e; i != 3 {
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
	if e, ok = q.popOne(); ok {
		t.Fatalf("Expected nil, got %v", e)
	}

	q = newIPQueue[int](s, "test2")
	q.push(1)
	q.push(2)
	// Capture current capacity
	q.Lock()
	c := cap(q.elts)
	q.Unlock()
	e, ok = q.popOne()
	if !ok || e != 1 {
		t.Fatalf("Invalid value: %v", e)
	}
	if l := q.len(); l != 1 {
		t.Fatalf("Expected len to be 1, got %v", l)
	}
	values, ql, qsz := q.pop()
	if len(values) != 1 || (values)[0] != 2 {
		t.Fatalf("Unexpected values: %v", values)
	}
	if cap(values) != c-1 {
		t.Fatalf("Unexpected capacity: %v vs %v", cap(values), c-1)
	}
	if l := q.len(); l != 0 {
		t.Fatalf("Expected len to be 0, got %v", l)
	}
	// Just make sure that this is ok...
	q.recycle(values, ql, qsz)
	// Check pool
	var l []int
	q.psMu.Lock()
	tcps := q.tcps
	if len(q.pool) > 0 {
		l = q.pool[len(q.pool)-1]
	}
	q.psMu.Unlock()
	require_Equal(t, tcps, c-1)
	require_Equal(t, cap(l), c-1)
}

func TestIPQueueMultiProducers(t *testing.T) {
	s := &Server{}
	q := newIPQueue[int](s, "test")

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
			values, ql, qsz := q.pop()
			for _, v := range values {
				m[v] = struct{}{}
			}
			q.recycle(values, ql, qsz)
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
	q := newIPQueue[int](s, "test")
	total := 1000
	for iter := 0; iter < 5; iter++ {
		var sz int
		for i := 0; i < total; i++ {
			q.push(i)
		}
		if q.len() != total {
			t.Fatalf("Expected size to be %v, got %v", total, sz)
		}
		values, ql, qsz := q.pop()
		preRecycleCap := cap(values)
		q.recycle(values, ql, qsz)
		q.push(1001)
		if q.len() != 1 {
			t.Fatalf("Expected size to be %v, got %v", 1, sz)
		}
		values, ql, qsz = q.pop()
		require_Equal(t, len(values), 1)
		require_Equal(t, ql, 1)
		require_Equal(t, qsz, 0)
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

	q = newIPQueue[int](s, "test2", ipqMaxRecycleSize[int](10))
	for i := 0; i < 100; i++ {
		q.push(i)
	}
	values, ql, qsz := q.pop()
	require_Equal(t, len(values), 100)
	require_Equal(t, ql, 100)
	preRecycleCap := cap(values)
	q.recycle(values, ql, qsz)
	q.push(101)
	values, ql, qsz = q.pop()
	require_Equal(t, len(values), 1)
	require_Equal(t, ql, 1)
	// This time, we should not have recycled it, so the cap of the slice
	// created for q.push(101) should be the default (32) but clearly less
	// than the preRecycleCap.
	if c := cap(values); c >= preRecycleCap {
		t.Fatalf("The slice should not have been put back in the pool, got cap of %v", c)
	}
	q.recycle(values, ql, qsz)

	// Also check that if we mistakenly pop a queue that was not
	// notified (pop() will return nil), and we try to recycle,
	// recycle() will ignore the call.
	values, ql, qsz = q.pop()
	q.recycle(values, ql, qsz)
	// Check that we don't crash when recycling a nil or empty slice
	q.recycle(nil, 0, 0)
	q.recycle([]int{}, 0, 0)

	// There was a bug that would cause the following situation to fail.
	// We have this test now to make sure that if we change in the future,
	// this may catch possible issues.
	var (
		elts     []int
		expected int64
		sum      int64
	)
	q2 := newIPQueue[int](s, "test3")
	for i := 0; i < 1000; i++ {
		expected += int64(i + 1)
		q2.push(i + 1)

		elts, ql, qsz = q2.pop()
		for _, v := range elts {
			sum += int64(v)
		}
		q2.recycle(elts, ql, qsz)

		expected += int64(i + 2)
		q2.push(i + 2)

		elts, ql, qsz = q2.pop()
		expected += int64(i + 3)
		q2.push(i + 3)
		for _, v := range elts {
			sum += int64(v)
		}
		q2.recycle(elts, ql, qsz)

		elts, ql, qsz = q2.pop()
		for _, v := range elts {
			sum += int64(v)
		}
		q2.recycle(elts, ql, qsz)
	}
	if sum != expected {
		t.Fatalf("Expected sum to be %v, got %v", expected, sum)
	}
}

func TestIPQueueDrain(t *testing.T) {
	s := &Server{}
	q := newIPQueue[int](s, "test")
	for iter, recycled := 0, false; iter < 5 && !recycled; iter++ {
		for i := 0; i < 100; i++ {
			q.push(i + 1)
		}
		q.drain()
		// We expect the queue length to be 0 and q.elts set to nil.
		require_Equal(t, q.len(), 0)
		q.Lock()
		elts := q.elts
		q.Unlock()
		require_True(t, elts == nil)
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

func TestIPQueueSizeCalculation(t *testing.T) {
	type testType = [16]byte
	var testValue testType

	calc := ipqSizeCalculation[testType](func(e testType) uint64 {
		return uint64(len(e))
	})
	s := &Server{}
	q := newIPQueue[testType](s, "test", calc)

	for i := 0; i < 10; i++ {
		q.push(testValue)
		require_Equal(t, q.len(), i+1)
		require_Equal(t, q.size(), uint64(i+1)*uint64(len(testValue)))
	}

	for i := 10; i > 5; i-- {
		q.popOne()
		require_Equal(t, q.len(), i-1)
		require_Equal(t, q.size(), uint64(i-1)*uint64(len(testValue)))
	}

	elts, ql, qsz := q.pop()
	require_Equal(t, q.len(), 0)
	require_Equal(t, q.size(), 0)
	q.recycle(elts, ql, qsz)
}

func TestIPQueueSizeCalculationWithLimits(t *testing.T) {
	type testType = [16]byte
	var testValue testType

	calc := ipqSizeCalculation[testType](func(e testType) uint64 {
		return uint64(len(e))
	})
	s := &Server{}

	t.Run("LimitByLen", func(t *testing.T) {
		q := newIPQueue[testType](s, "test", calc, ipqLimitByLen[testType](5))
		for i := 0; i < 10; i++ {
			_, _, err := q.push(testValue)
			if i >= 5 {
				require_Error(t, err, errIPQLenLimitReached)
			} else {
				require_NoError(t, err)
			}
			require_LessThan(t, q.len(), 6)
		}
		// Verify that even after a pop() and until messages are processed, the push() fails.
		values, ql, qsz := q.pop()
		_, _, err := q.push(testValue)
		require_Error(t, err, errIPQLenLimitReached)
		q.recycle(values, ql, qsz)
		_, _, err = q.push(testValue)
		require_NoError(t, err)
	})

	t.Run("LimitBySize", func(t *testing.T) {
		q := newIPQueue[testType](s, "test", calc, ipqLimitBySize[testType](16*5))
		for i := 0; i < 10; i++ {
			_, _, err := q.push(testValue)
			if i >= 5 {
				require_Error(t, err, errIPQSizeLimitReached)
			} else {
				require_NoError(t, err)
			}
			require_LessThan(t, q.len(), 6)
		}
		// Verify that even after a pop() and until messages are processed, the push() fails.
		values, ql, qsz := q.pop()
		_, _, err := q.push(testValue)
		require_Error(t, err, errIPQSizeLimitReached)
		q.recycle(values, ql, qsz)
		_, _, err = q.push(testValue)
		require_NoError(t, err)
	})
}

func Benchmark_IPQueueSizeCalculation(b *testing.B) {
	type testType = [16]byte
	var testValue testType

	s := &Server{}

	runPopOne := func(b *testing.B, q *ipQueue[testType]) {
		b.SetBytes(16)
		for i := 0; i < b.N; i++ {
			q.push(testValue)
		}
		for i := b.N; i > 0; i-- {
			q.popOne()
		}
	}
	runPopAll := func(b *testing.B, q *ipQueue[testType]) {
		b.SetBytes(16)
		for i := 0; i < b.N; i++ {
			q.push(testValue)
		}
		elts, ql, qsz := q.pop()
		for _, v := range elts {
			_ = v
		}
		q.recycle(elts, ql, qsz)
	}
	for _, test := range []struct {
		name string
		run  func(b *testing.B, q *ipQueue[testType])
	}{
		{"pop one", runPopOne},
		{"pop all", runPopAll},
	} {
		b.Run(test.name, func(b *testing.B) {
			// Measures without calculation function overheads.
			b.Run("WithoutCalc", func(b *testing.B) {
				test.run(b, newIPQueue[testType](s, "test"))
			})

			// Measures the raw overhead of having a calculation function.
			b.Run("WithEmptyCalc", func(b *testing.B) {
				calc := ipqSizeCalculation[testType](func(e testType) uint64 {
					return 0
				})
				test.run(b, newIPQueue[testType](s, "test", calc))
			})

			// Measures the overhead of having a calculation function that
			// actually measures something useful.
			b.Run("WithLenCalc", func(b *testing.B) {
				calc := ipqSizeCalculation[testType](func(e testType) uint64 {
					return uint64(len(e))
				})
				test.run(b, newIPQueue[testType](s, "test", calc))
			})
		})
	}
}

func Benchmark_IPQueue1Prod1ConsPopOne(b *testing.B) {
	benchIPQueue(b, 1, 1, true, false)
}

func Benchmark_IPQueue5Prod1ConsPopOne(b *testing.B) {
	benchIPQueue(b, 5, 1, true, false)
}

func Benchmark_IPQueue1Prod5ConsPopOne(b *testing.B) {
	benchIPQueue(b, 1, 5, true, false)
}

func Benchmark_IPQueue5Prod5ConsPopOne(b *testing.B) {
	benchIPQueue(b, 5, 5, true, false)
}

func Benchmark_IPQueue1Prod1ConsPopAll(b *testing.B) {
	benchIPQueue(b, 1, 1, false, false)
}

func Benchmark_IPQueue5Prod1ConsPopAll(b *testing.B) {
	benchIPQueue(b, 5, 1, false, false)
}

func Benchmark_IPQueue1Prod5ConsPopAll(b *testing.B) {
	benchIPQueue(b, 1, 5, false, false)
}

func Benchmark_IPQueue5Prod5ConsPopAll(b *testing.B) {
	benchIPQueue(b, 5, 5, false, false)
}

func Benchmark_IPQueueWithLim1Prod1ConsPopOne(b *testing.B) {
	benchIPQueue(b, 1, 1, true, true)
}

func Benchmark_IPQueueWithLim5Prod1ConsPopOne(b *testing.B) {
	benchIPQueue(b, 5, 1, true, true)
}

func Benchmark_IPQueueWithLim1Prod5ConsPopOne(b *testing.B) {
	benchIPQueue(b, 1, 5, true, true)
}

func Benchmark_IPQueueWithLim5Prod5ConsPopOne(b *testing.B) {
	benchIPQueue(b, 5, 5, true, true)
}

func Benchmark_IPQueueWithLim1Prod1ConsPopAll(b *testing.B) {
	benchIPQueue(b, 1, 1, false, true)
}

func Benchmark_IPQueueWithLim5Prod1ConsPopAll(b *testing.B) {
	benchIPQueue(b, 5, 1, false, true)
}

func Benchmark_IPQueueWithLim1Prod5ConsPopAll(b *testing.B) {
	benchIPQueue(b, 1, 5, false, true)
}

func Benchmark_IPQueueWithLim5Prod5ConsPopAll(b *testing.B) {
	benchIPQueue(b, 5, 5, false, true)
}

func benchIPQueue(b *testing.B, numProd, numCons int, popOne, withLimits bool) {
	var count atomic.Int64
	var sum atomic.Int64

	perProd := (b.N / numProd) + 1
	total := int64(perProd * numProd)

	var opts []ipQueueOpt[int]
	if withLimits {
		opts = append(opts, ipqLimitByLen[int](int(total+100)))
		opts = append(opts, ipqLimitBySize[int](uint64((total+100)*5)))
		opts = append(opts, ipqSizeCalculation[int](func(e int) uint64 { return uint64(5) }))
	}
	queue := newIPQueue[int](&Server{}, "bench", opts...)

	cdone := make(chan struct{}, numCons)
	for i := 0; i < numCons; i++ {
		go func() {
			for range queue.ch {
				if popOne {
					v, ok := queue.popOne()
					if !ok {
						continue
					}
					sum.Add(int64(v))
					if count.Add(1) >= total {
						cdone <- struct{}{}
						return
					}
				} else {
					elts, ql, qsz := queue.pop()
					for _, v := range elts {
						sum.Add(int64(v))
						if count.Add(1) >= total {
							queue.recycle(elts, ql, qsz)
							cdone <- struct{}{}
							return
						}
						if withLimits {
							queue.processed(v, &ql, &qsz)
						}
					}
					queue.recycle(elts, ql, qsz)
				}
			}
		}()
	}

	pwg := sync.WaitGroup{}
	pwg.Add(numProd)
	var expected atomic.Int64
	for i := 0; i < numProd; i++ {
		go func() {
			defer pwg.Done()
			for i := 0; i < perProd; i++ {
				expected.Add(int64(i + 1))
				queue.push(i + 1)
			}
		}()
	}

	pwg.Wait()
	<-cdone
	b.StopTimer()

	for i := 1; i < numCons; i++ {
		queue.push(0)
		<-cdone
	}
	if sum.Load() != expected.Load() {
		b.Fatalf("Invalid sum, expected %v, got %v", expected.Load(), sum.Load())
	}
}

func Benchmark_IPQueuePushAndPopOne(b *testing.B) {
	s := &Server{}
	q := newIPQueue[int](s, "test")
	for i := 0; i < b.N; i++ {
		q.push(i + 1)
	}
	require_Equal[int](b, q.len(), b.N)
	for i := 0; i < b.N; i++ {
		v, ok := q.popOne()
		require_True(b, ok)
		require_Equal[int](b, v, i+1)
	}
	require_Equal[int](b, q.len(), 0)
	require_Equal(b, q.inProgress(), 0)
}

func Benchmark_IPQueuePushAndPopAll(b *testing.B) {
	s := &Server{}
	q := newIPQueue[int](s, "test")
	expected := 0
	for i := 0; i < b.N; i++ {
		q.push(i + 1)
		expected += i + 1
	}
	require_Equal(b, q.len(), b.N)
	<-q.ch

	elts, ql, qsz := q.pop()
	require_Equal(b, len(elts), b.N)
	require_Equal(b, ql, int64(b.N))
	require_Equal(b, q.len(), 0)
	require_Equal(b, q.inProgress(), int64(b.N))
	sum := 0
	for _, v := range elts {
		sum += v
	}
	q.recycle(elts, ql, qsz)
	require_Equal(b, expected, sum)
	require_Equal(b, q.len(), 0)
	require_Equal(b, q.inProgress(), 0)
}
