// Copyright 2023 The NATS Authors
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

package avl

import (
	"math/rand"
	"testing"
)

func TestSeqSetBasics(t *testing.T) {
	var ss SequenceSet

	seqs := []uint64{22, 222, 2222, 2, 2, 4}
	for _, seq := range seqs {
		ss.Insert(seq)
		require_True(t, ss.Exists(seq))
	}

	require_True(t, ss.Nodes() == 1)
	require_True(t, ss.Size() == len(seqs)-1) // We have one dup in there.
	lh, rh := ss.Heights()
	require_True(t, lh == 0)
	require_True(t, rh == 0)
}

func TestSeqSetLeftLean(t *testing.T) {
	var ss SequenceSet

	for i := uint64(4 * numEntries); i > 0; i-- {
		ss.Insert(i)
	}

	require_True(t, ss.Nodes() == 5)
	require_True(t, ss.Size() == 4*numEntries)
	lh, rh := ss.Heights()
	require_True(t, lh == 2)
	require_True(t, rh == 1)
}

func TestSeqSetRightLean(t *testing.T) {
	var ss SequenceSet

	for i := uint64(0); i < uint64(4*numEntries); i++ {
		ss.Insert(i)
	}

	require_True(t, ss.Nodes() == 4)
	require_True(t, ss.Size() == 4*numEntries)
	lh, rh := ss.Heights()
	require_True(t, lh == 1)
	require_True(t, rh == 2)
}

func TestSeqSetCorrectness(t *testing.T) {
	// Generate 100k sequences across 500k range.
	num := 100_000
	max := 500_000

	set := make(map[uint64]struct{}, num)
	var ss SequenceSet
	for i := 0; i < num; i++ {
		n := uint64(rand.Int63n(int64(max + 1)))
		ss.Insert(n)
		set[n] = struct{}{}
	}

	for i := uint64(0); i <= uint64(max); i++ {
		_, exists := set[i]
		require_True(t, ss.Exists(i) == exists)
	}
}

func TestSeqSetRange(t *testing.T) {
	num := 2*numEntries + 22
	nums := make([]uint64, 0, num)
	for i := 0; i < num; i++ {
		nums = append(nums, uint64(i))
	}
	rand.Shuffle(len(nums), func(i, j int) { nums[i], nums[j] = nums[j], nums[i] })

	var ss SequenceSet
	for _, n := range nums {
		ss.Insert(n)
	}

	nums = nums[:0]
	ss.Range(func(n uint64) bool {
		nums = append(nums, n)
		return true
	})
	require_True(t, len(nums) == num)
	for i := uint64(0); i < uint64(num); i++ {
		require_True(t, nums[i] == i)
	}

	// Test truncating the range call.
	nums = nums[:0]
	ss.Range(func(n uint64) bool {
		if n >= 10 {
			return false
		}
		nums = append(nums, n)
		return true
	})
	require_True(t, len(nums) == 10)
	for i := uint64(0); i < 10; i++ {
		require_True(t, nums[i] == i)
	}
}

func TestSeqSetDelete(t *testing.T) {
	var ss SequenceSet

	// Simple single node.
	seqs := []uint64{22, 222, 2222, 2, 2, 4}
	for _, seq := range seqs {
		ss.Insert(seq)
	}

	for _, seq := range seqs {
		ss.Delete(seq)
		require_True(t, !ss.Exists(seq))
	}
	require_True(t, ss.root == nil)

	num := 22*numEntries + 22
	nums := make([]uint64, 0, num)
	for i := 0; i < num; i++ {
		nums = append(nums, uint64(i))
	}
	rand.Shuffle(len(nums), func(i, j int) { nums[i], nums[j] = nums[j], nums[i] })

	for _, n := range nums {
		ss.Insert(n)
	}

	for _, n := range nums {
		ss.Delete(n)
		require_True(t, !ss.Exists(n))
	}
	require_True(t, ss.root == nil)
}

func TestSeqSetMinMax(t *testing.T) {
	var ss SequenceSet

	// Simple single node.
	seqs := []uint64{22, 222, 2222, 2, 2, 4}
	for _, seq := range seqs {
		ss.Insert(seq)
	}

	min, max := ss.MinMax()
	require_True(t, min == 2 && max == 2222)

	// Multi-node
	ss.Empty()

	num := 22*numEntries + 22
	nums := make([]uint64, 0, num)
	for i := 0; i < num; i++ {
		nums = append(nums, uint64(i))
	}
	rand.Shuffle(len(nums), func(i, j int) { nums[i], nums[j] = nums[j], nums[i] })
	for _, n := range nums {
		ss.Insert(n)
	}

	min, max = ss.MinMax()
	require_True(t, min == 0 && max == uint64(num-1))
}

func TestSeqSetClone(t *testing.T) {
	// Generate 100k sequences across 500k range.
	num := 100_000
	max := 500_000

	var ss SequenceSet
	for i := 0; i < num; i++ {
		ss.Insert(uint64(rand.Int63n(int64(max + 1))))
	}

	ssc := ss.Clone()
	require_True(t, ss.Size() == ssc.Size())
	require_True(t, ss.Nodes() == ssc.Nodes())
}

func TestSeqSetUnion(t *testing.T) {
	var ss1, ss2 SequenceSet

	seqs1 := []uint64{22, 222, 2222, 2, 2, 4}
	for _, seq := range seqs1 {
		ss1.Insert(seq)
	}

	seqs2 := []uint64{33, 333, 3333, 3, 33_333, 333_333}
	for _, seq := range seqs2 {
		ss2.Insert(seq)
	}

	ss := Union(&ss1, &ss2)
	require_True(t, ss.Size() == 11)

	seqs := append(seqs1, seqs2...)
	for _, n := range seqs {
		require_True(t, ss.Exists(n))
	}
}

func require_NoError(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("require no error, but got: %v", err)
	}
}

func require_True(t *testing.T, b bool) {
	t.Helper()
	if !b {
		t.Fatalf("require true")
	}
}
