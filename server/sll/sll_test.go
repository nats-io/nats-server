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

package sll

import (
	"math/rand/v2"
	"slices"
	"testing"
)

func checkList[T comparable](t *testing.T, l *List[T], want []T) {
	t.Helper()
	var got []T
	l.Range(func(value T) bool {
		got = append(got, value)
		if len(got) > len(want) {
			t.Fatal("list contains more nodes than expected")
		}
		return true
	})
	if !slices.Equal(got, want) || l.Len() != len(want) {
		t.Fatalf("got %v (len %d), want %v", got, l.Len(), want)
	}
	if len(want) == 0 {
		if l.head != nil || l.tail != nil {
			t.Fatal("empty list retains nodes")
		}
	} else if l.tail == nil || l.tail.next != nil || l.tail.value != want[len(want)-1] {
		t.Fatal("invalid tail")
	}
}

func TestListQueue(t *testing.T) {
	var l List[int]
	checkList(t, &l, nil)
	if v, ok := l.PopFront(); ok || v != 0 {
		t.Fatalf("empty PopFront returned %d, %v", v, ok)
	}
	// Include the zero value, interleaved operations, and reuse after draining.
	for range 3 {
		l.PushBack(0)
		l.PushBack(1)
		if v, ok := l.PopFront(); !ok || v != 0 {
			t.Fatalf("PopFront returned %d, %v; want 0, true", v, ok)
		}
		l.PushBack(2)
		checkList(t, &l, []int{1, 2})
		for _, want := range []int{1, 2} {
			if v, ok := l.PopFront(); !ok || v != want {
				t.Fatalf("PopFront returned %d, %v; want %d, true", v, ok, want)
			}
		}
		checkList(t, &l, nil)
	}
}

func TestListRemove(t *testing.T) {
	for _, test := range []struct {
		name  string
		items []int
		value int
		want  []int
		found bool
	}{
		{"empty", nil, 1, nil, false},
		{"missing", []int{1, 2, 3}, 4, []int{1, 2, 3}, false},
		{"head", []int{1, 2, 3}, 1, []int{2, 3}, true},
		{"middle", []int{1, 2, 3}, 2, []int{1, 3}, true},
		{"tail", []int{1, 2, 3}, 3, []int{1, 2}, true},
		{"singleton", []int{1}, 1, nil, true},
		{"duplicate", []int{1, 2, 1}, 1, []int{2, 1}, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			var l List[int]
			for _, v := range test.items {
				l.PushBack(v)
			}
			if got := l.Remove(test.value); got != test.found {
				t.Fatalf("Remove returned %v, want %v", got, test.found)
			}
			checkList(t, &l, test.want)
			// In particular, appending must work after removing the tail.
			l.PushBack(9)
			checkList(t, &l, append(slices.Clone(test.want), 9))
		})
	}
}

func TestListRangeAndEmpty(t *testing.T) {
	var l List[*int]
	a, b := 1, 2
	l.PushBack(&a)
	l.PushBack(nil)
	l.PushBack(&b)
	checkList(t, &l, []*int{&a, nil, &b})
	calls := 0
	l.Range(func(value *int) bool {
		calls++
		if value != &a {
			t.Fatal("Range did not start at the head")
		}
		return false
	})
	if calls != 1 {
		t.Fatalf("Range called callback %d times after early stop", calls)
	}
	l.Empty()
	l.Empty()
	checkList(t, &l, nil)
	l.PushBack(&b)
	checkList(t, &l, []*int{&b})
}

func TestListMixedOperations(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	var l List[int]
	var want []int
	for range 10000 {
		value := rng.IntN(20)
		switch rng.IntN(10) {
		case 0, 1, 2, 3, 4:
			l.PushBack(value)
			want = append(want, value)
		case 5, 6:
			got, ok := l.PopFront()
			if len(want) == 0 {
				if ok || got != 0 {
					t.Fatal("PopFront succeeded on an empty list")
				}
			} else {
				if !ok || got != want[0] {
					t.Fatalf("PopFront returned %d, %v; want %d, true", got, ok, want[0])
				}
				want = want[1:]
			}
		case 7, 8:
			i := slices.Index(want, value)
			if l.Remove(value) != (i >= 0) {
				t.Fatal("Remove disagrees with slice model")
			}
			if i >= 0 {
				want = slices.Delete(want, i, i+1)
			}
		case 9:
			l.Empty()
			want = nil
		}
		checkList(t, &l, want)
	}
}
