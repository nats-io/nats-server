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
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func benchmarkWriteTermVote(b *testing.B, workers int) {
	root := b.TempDir()
	dirs := make([]string, workers)
	for i := range dirs {
		dir := filepath.Join(root, fmt.Sprintf("raft-%d", i))
		if err := os.MkdirAll(dir, defaultDirPerms); err != nil {
			b.Fatal(err)
		}
		dirs[i] = dir
	}

	wtvs := make([][]byte, workers)
	for i := range wtvs {
		wtv := make([]byte, termVoteLen)
		copy(wtv[termLen:], fmt.Sprintf("%08d", i))
		wtvs[i] = wtv
	}

	var next atomic.Uint64
	var wg sync.WaitGroup
	type latencyStats struct {
		count uint64
		total time.Duration
		min   time.Duration
		max   time.Duration
	}
	stats := make([]latencyStats, workers)
	wg.Add(workers)

	b.ResetTimer()
	for i := range workers {
		go func(worker int) {
			defer wg.Done()
			dir := dirs[worker]
			wtv := wtvs[worker]
			stat := &stats[worker]
			for {
				n := next.Add(1)
				if n > uint64(b.N) {
					return
				}
				binary.LittleEndian.PutUint64(wtv[:termLen], n)
				start := time.Now()
				if err := writeTermVote(dir, wtv); err != nil {
					b.Errorf("writeTermVote failed: %v", err)
					return
				}
				elapsed := time.Since(start)
				stat.count++
				stat.total += elapsed
				if stat.min == 0 || elapsed < stat.min {
					stat.min = elapsed
				}
				if elapsed > stat.max {
					stat.max = elapsed
				}
			}
		}(i)
	}
	wg.Wait()
	b.StopTimer()

	var count uint64
	var total, minLatency, maxLatency time.Duration
	for i := range stats {
		stat := stats[i]
		if stat.count == 0 {
			continue
		}
		count += stat.count
		total += stat.total
		if minLatency == 0 || stat.min < minLatency {
			minLatency = stat.min
		}
		if stat.max > maxLatency {
			maxLatency = stat.max
		}
	}
	if count > 0 {
		avgLatency := time.Duration(total.Nanoseconds() / int64(count))
		b.ReportMetric(float64(minLatency.Microseconds()), "min-latency-us")
		b.ReportMetric(float64(avgLatency.Microseconds()), "avg-latency-us")
		b.ReportMetric(float64(maxLatency.Microseconds()), "max-latency-us")
		b.ReportMetric(0, "ns/op")
	}
	if elapsed := b.Elapsed(); elapsed > 0 {
		b.ReportMetric(float64(b.N)/elapsed.Seconds(), "writes/s")
	}
}

func BenchmarkWriteTermVote(b *testing.B) {
	for _, workers := range []int{1, 16, 128, 512} {
		b.Run(fmt.Sprintf("workers=%d", workers), func(b *testing.B) {
			benchmarkWriteTermVote(b, workers)
		})
	}
}
