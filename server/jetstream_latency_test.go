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
	"sync"
	"testing"
	"time"
)

func TestJetStreamAPILatencyTrackerBasics(t *testing.T) {
	tr := newJSAPILatencyTracker()
	if s := tr.snapshot(); s != nil {
		t.Fatalf("expected nil snapshot with no records, got %+v", s)
	}
	// Record 100 durations from 100µs to 10ms.
	for i := 1; i <= 100; i++ {
		tr.record(time.Duration(i) * 100 * time.Microsecond)
	}
	s := tr.snapshot()
	if s == nil {
		t.Fatal("expected non-nil snapshot")
	}
	if s.Count != 100 {
		t.Fatalf("expected count 100, got %d", s.Count)
	}
	if s.Min != 100*time.Microsecond {
		t.Fatalf("expected min 100µs, got %v", s.Min)
	}
	if s.Max != 10*time.Millisecond {
		t.Fatalf("expected max 10ms, got %v", s.Max)
	}
	expectedTotal := time.Duration(5050) * 100 * time.Microsecond
	if s.Total != expectedTotal {
		t.Fatalf("expected total %v, got %v", expectedTotal, s.Total)
	}
	if s.Avg != expectedTotal/100 {
		t.Fatalf("expected avg %v, got %v", expectedTotal/100, s.Avg)
	}
	// Buckets are cumulative at 100µs:1, 200µs:2, 500µs:5, 1ms:10,
	// 2ms:20, 5ms:50, 10ms:100, so percentiles resolve to bucket bounds.
	if s.P50 != 5*time.Millisecond {
		t.Fatalf("expected p50 5ms, got %v", s.P50)
	}
	if s.P75 != 10*time.Millisecond || s.P90 != 10*time.Millisecond || s.P95 != 10*time.Millisecond || s.P99 != 10*time.Millisecond {
		t.Fatalf("expected p75-p99 10ms, got %v %v %v %v", s.P75, s.P90, s.P95, s.P99)
	}
	if s.P999 != 10*time.Millisecond {
		t.Fatalf("expected p999 10ms, got %v", s.P999)
	}
}

func TestJetStreamAPILatencyTrackerPercentileClamp(t *testing.T) {
	tr := newJSAPILatencyTracker()
	tr.record(500 * time.Microsecond)
	tr.record(60 * time.Second) // Lands in the overflow bucket.
	s := tr.snapshot()
	if s == nil {
		t.Fatal("expected non-nil snapshot")
	}
	// The overflow bucket bound is math.MaxInt64, so without clamping
	// percentiles above the 50th would report ~292 years.
	if s.P50 != 500*time.Microsecond {
		t.Fatalf("expected p50 500µs, got %v", s.P50)
	}
	for name, p := range map[string]time.Duration{"p75": s.P75, "p90": s.P90, "p95": s.P95, "p99": s.P99, "p999": s.P999} {
		if p != 60*time.Second {
			t.Fatalf("expected %s clamped to max 60s, got %v", name, p)
		}
	}
	if !(s.P50 <= s.P75 && s.P75 <= s.P90 && s.P90 <= s.P95 && s.P95 <= s.P99 && s.P99 <= s.P999) {
		t.Fatalf("expected monotonic percentiles, got %+v", s)
	}
}

func TestJetStreamAPILatencyTrackerZeroAndNegative(t *testing.T) {
	tr := newJSAPILatencyTracker()
	tr.record(-time.Second) // Must be ignored.
	if s := tr.snapshot(); s != nil {
		t.Fatalf("expected nil snapshot after negative duration, got %+v", s)
	}
	tr.record(0)
	s := tr.snapshot()
	if s == nil {
		t.Fatal("expected non-nil snapshot")
	}
	if s.Count != 1 || s.Min != 0 || s.Max != 0 || s.Total != 0 || s.Avg != 0 {
		t.Fatalf("unexpected stats for zero duration: %+v", s)
	}
	for name, p := range map[string]time.Duration{"p50": s.P50, "p75": s.P75, "p90": s.P90, "p95": s.P95, "p99": s.P99, "p999": s.P999} {
		if p != 0 {
			t.Fatalf("expected %s 0 for zero duration, got %v", name, p)
		}
	}
}

func TestJetStreamAPILatencyTrackerConcurrent(t *testing.T) {
	tr := newJSAPILatencyTracker()
	const goroutines = 8
	const perGoroutine = 1000
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				tr.record(time.Duration(i%100+1) * time.Microsecond)
			}
		}()
	}
	wg.Wait()
	s := tr.snapshot()
	if s == nil {
		t.Fatal("expected non-nil snapshot")
	}
	if s.Count != goroutines*perGoroutine {
		t.Fatalf("expected count %d, got %d", goroutines*perGoroutine, s.Count)
	}
	// Each goroutine records sum(i%100+1)µs = 10 * 5050µs.
	expectedTotal := time.Duration(goroutines) * 10 * 5050 * time.Microsecond
	if s.Total != expectedTotal {
		t.Fatalf("expected total %v, got %v", expectedTotal, s.Total)
	}
	if s.Avg != expectedTotal/(goroutines*perGoroutine) {
		t.Fatalf("expected avg %v, got %v", expectedTotal/(goroutines*perGoroutine), s.Avg)
	}
	if s.Min != time.Microsecond || s.Max != 100*time.Microsecond {
		t.Fatalf("expected min 1µs and max 100µs, got %v and %v", s.Min, s.Max)
	}
	if !(s.P50 <= s.P75 && s.P75 <= s.P90 && s.P90 <= s.P95 && s.P95 <= s.P99 && s.P99 <= s.P999) {
		t.Fatalf("expected monotonic percentiles, got %+v", s)
	}
}

func TestJetStreamRecordAPILatencyFastPathNoAlloc(t *testing.T) {
	js := &jetStream{}
	js.recordAPILatency("$JS.API.STREAM.INFO.*", time.Millisecond)
	js.recordAPILatency("$JS.API.STREAM.CREATE.*", time.Millisecond)
	allocs := testing.AllocsPerRun(100, func() {
		js.recordAPILatency("$JS.API.STREAM.INFO.*", time.Millisecond)
	})
	if allocs != 0 {
		t.Fatalf("expected no allocations on the fast path, got %v", allocs)
	}
	stats := js.apiLatencyStats()
	if len(stats) != 2 {
		t.Fatalf("expected stats for 2 subjects, got %+v", stats)
	}
	s, ok := stats["$JS.API.STREAM.INFO.*"]
	// 1 warm up call above, plus AllocsPerRun's own warm up and 100 runs.
	if !ok || s.Count != 102 {
		t.Fatalf("expected count 102 for stream info, got %+v", s)
	}
}
