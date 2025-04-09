// Copyright 2024-2025 The NATS Authors
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

package thw

import (
	"math"
	"testing"
	"time"
)

func TestHashWheelBasics(t *testing.T) {
	hw := NewHashWheel()
	now := time.Now()

	// Add a sequence.
	seq, expires := uint64(1), now.Add(5*time.Second).UnixNano()
	require_NoError(t, hw.Add(seq, expires))
	require_Equal(t, hw.count, 1)

	// Try to remove non-existent sequence.
	require_Error(t, hw.Remove(999, expires), ErrTaskNotFound)
	require_Equal(t, hw.count, 1)

	// Remove the sequence properly.
	require_NoError(t, hw.Remove(seq, expires))
	require_Equal(t, hw.count, 0)

	// Verify it's gone.
	require_Error(t, hw.Remove(seq, expires), ErrTaskNotFound)
	require_Equal(t, hw.count, 0)
}

func TestHashWheelUpdate(t *testing.T) {
	hw := NewHashWheel()
	now := time.Now()
	oldExpires := now.Add(5 * time.Second).UnixNano()
	newExpires := now.Add(10 * time.Second).UnixNano()

	// Add initial sequence.
	require_NoError(t, hw.Add(1, oldExpires))
	require_Equal(t, hw.count, 1)

	// Update expiration.
	require_NoError(t, hw.Update(1, oldExpires, newExpires))
	require_Equal(t, hw.count, 1)

	// Verify old expiration is gone.
	require_Error(t, hw.Remove(1, oldExpires), ErrTaskNotFound)
	require_Equal(t, hw.count, 1)

	// Verify new expiration exists
	require_NoError(t, hw.Remove(1, newExpires))
	require_Equal(t, hw.count, 0)
}

func TestHashWheelExpiration(t *testing.T) {
	hw := NewHashWheel()
	now := time.Now()

	// Add sequences with different expiration times.
	seqs := map[uint64]int64{
		1: now.Add(-1 * time.Second).UnixNano(), // Already expired
		2: now.Add(1 * time.Second).UnixNano(),  // Expires soon
		3: now.Add(10 * time.Second).UnixNano(), // Expires later
		4: now.Add(60 * time.Second).UnixNano(), // Expires much later
	}

	for seq, expires := range seqs {
		require_NoError(t, hw.Add(seq, expires))
	}
	require_Equal(t, hw.count, uint64(len(seqs)))

	// Process expired tasks.
	expired := make(map[uint64]bool)
	hw.ExpireTasks(func(seq uint64, expires int64) bool {
		expired[seq] = true
		return true
	})

	// Verify only sequence 1 expired.
	require_Equal(t, len(expired), 1)
	require_True(t, expired[1])
	require_Equal(t, hw.count, 3)
}

func TestHashWheelManualExpiration(t *testing.T) {
	hw := NewHashWheel()
	now := time.Now().UnixNano()

	for seq := uint64(1); seq <= 4; seq++ {
		require_NoError(t, hw.Add(seq, now))
	}
	require_Equal(t, hw.count, 4)

	// Loop over expired multiple times, but without removing them.
	expired := make(map[uint64]uint64)
	for i := uint64(0); i <= 1; i++ {
		hw.ExpireTasks(func(seq uint64, expires int64) bool {
			expired[seq]++
			return false
		})

		require_Equal(t, len(expired), 4)
		require_Equal(t, expired[1], 1+i)
		require_Equal(t, expired[2], 1+i)
		require_Equal(t, expired[3], 1+i)
		require_Equal(t, expired[4], 1+i)
		require_Equal(t, hw.count, 4)
	}

	// Only remove even sequences.
	for i := uint64(0); i <= 1; i++ {
		hw.ExpireTasks(func(seq uint64, expires int64) bool {
			expired[seq]++
			return seq%2 == 0
		})

		// Verify even sequences are removed.
		require_Equal(t, expired[1], 3+i)
		require_Equal(t, expired[2], 3)
		require_Equal(t, expired[3], 3+i)
		require_Equal(t, expired[4], 3)
		require_Equal(t, hw.count, 2)
	}

	// Manually remove last items.
	require_NoError(t, hw.Remove(1, now))
	require_NoError(t, hw.Remove(3, now))
	require_Equal(t, hw.count, 0)
}

func TestHashWheelNextExpiration(t *testing.T) {
	hw := NewHashWheel()
	now := time.Now()

	// Add sequences with different expiration times.
	seqs := map[uint64]int64{
		1: now.Add(5 * time.Second).UnixNano(),
		2: now.Add(3 * time.Second).UnixNano(), // Earliest
		3: now.Add(10 * time.Second).UnixNano(),
	}

	for seq, expires := range seqs {
		require_NoError(t, hw.Add(seq, expires))
	}
	require_Equal(t, hw.count, uint64(len(seqs)))

	// Test GetNextExpiration.
	nextExternalTick := now.Add(6 * time.Second).UnixNano()
	// Should return sequence 2's expiration
	require_Equal(t, hw.GetNextExpiration(nextExternalTick), seqs[2])

	// Test with empty wheel.
	empty := NewHashWheel()
	require_Equal(t, empty.GetNextExpiration(now.Add(1*time.Second).UnixNano()), math.MaxInt64)
}

func TestHashWheelStress(t *testing.T) {
	hw := NewHashWheel()
	now := time.Now()

	// Add many sequences.
	numSequences := 100_000
	for seq := 0; seq < numSequences; seq++ {
		expires := now.Add(time.Duration(seq) * time.Second).UnixNano()
		require_NoError(t, hw.Add(uint64(seq), expires))
	}

	// Update many sequences.
	for seq := 0; seq < numSequences; seq += 2 { // Update every other sequence
		oldExpires := now.Add(time.Duration(seq) * time.Second).UnixNano()
		newExpires := now.Add(time.Duration(seq+numSequences) * time.Second).UnixNano()
		require_NoError(t, hw.Update(uint64(seq), oldExpires, newExpires))
	}

	// Remove many sequences.
	for seq := 1; seq < numSequences; seq += 2 { // Remove odd-numbered sequences
		expires := now.Add(time.Duration(seq) * time.Second).UnixNano()
		require_NoError(t, hw.Remove(uint64(seq), expires))
	}
}

func TestHashWheelEncodeDecode(t *testing.T) {
	hw := NewHashWheel()
	now := time.Now()

	// Add many sequences.
	numSequences := 100_000
	for seq := 0; seq < numSequences; seq++ {
		expires := now.Add(time.Duration(seq) * time.Second).UnixNano()
		require_NoError(t, hw.Add(uint64(seq), expires))
	}

	b := hw.Encode(12345)
	require_True(t, len(b) > 17) // Bigger than just the header

	nhw := NewHashWheel()
	stamp, err := nhw.Decode(b)
	require_NoError(t, err)
	require_Equal(t, stamp, 12345)
	require_Equal(t, hw.GetNextExpiration(math.MaxInt64), nhw.GetNextExpiration(math.MaxInt64))

	for s, slot := range hw.wheel {
		nslot := nhw.wheel[s]
		require_Equal(t, slot.lowest, nslot.lowest)
		require_Equal(t, len(slot.entries), len(nslot.entries))
		for v, ts := range slot.entries {
			nts, ok := nslot.entries[v]
			require_True(t, ok)
			require_Equal(t, ts, nts)
		}
	}
}

// Benchmarks

func BenchmarkHashWheel_Add(b *testing.B) {
	hw := NewHashWheel()
	now := time.Now()

	// Create different ranges for expires to spread across slots
	expires := make([]int64, b.N)
	for i := 0; i < b.N; i++ {
		expires[i] = now.Add(time.Duration(i%3600) * time.Second).UnixNano()
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		hw.Add(uint64(i), expires[i])
	}
}

func BenchmarkHashWheel_Add_SameSlot(b *testing.B) {
	hw := NewHashWheel()
	expires := time.Now().Add(time.Second).UnixNano()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		hw.Add(uint64(i), expires)
	}
}

func BenchmarkHashWheel_Update(b *testing.B) {
	hw := NewHashWheel()
	now := time.Now()

	// First add N items
	expires := make([]int64, b.N)
	for i := 0; i < b.N; i++ {
		expires[i] = now.Add(time.Duration(i%3600) * time.Second).UnixNano()
		hw.Add(uint64(i), expires[i])
	}

	newExpires := now.Add(2 * time.Hour).UnixNano()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		hw.Update(uint64(i), expires[i], newExpires)
	}
}

func BenchmarkHashWheel_Update_SameSlot(b *testing.B) {
	hw := NewHashWheel()
	now := time.Now()
	oldExpires := now.Add(time.Second).UnixNano()
	newExpires := now.Add(2 * time.Second).UnixNano()

	// First add N items
	for i := 0; i < b.N; i++ {
		hw.Add(uint64(i), oldExpires)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		hw.Update(uint64(i), oldExpires, newExpires)
	}
}

// Benchmark memory allocation
func BenchmarkHashWheel_Add_Memory(b *testing.B) {
	b.ReportAllocs()
	hw := NewHashWheel()
	now := time.Now()

	expires := make([]int64, b.N)
	for i := 0; i < b.N; i++ {
		expires[i] = now.Add(time.Duration(i%3600) * time.Second).UnixNano()
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		hw.Add(uint64(i), expires[i])
	}
}

// Large scale test
func BenchmarkHashWheel_LargeScale(b *testing.B) {
	hw := NewHashWheel()
	now := time.Now()

	// Pre-populate with 100k items
	for i := 0; i < 100_000; i++ {
		expires := now.Add(time.Duration(i%3600) * time.Second).UnixNano()
		hw.Add(uint64(i), expires)
	}

	expires := make([]int64, b.N)
	for i := 0; i < b.N; i++ {
		expires[i] = now.Add(time.Duration(i%3600) * time.Second).UnixNano()
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		hw.Add(uint64(i+100000), expires[i])
	}
}
