// Copyright 2023-2024 The NATS Authors
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

package stree

import (
	crand "crypto/rand"
	"encoding/hex"
	"flag"
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"
	"unsafe"
)

// Print Results: go test -v  --args --results
// For some benchmarks.
var runResults = flag.Bool("results", false, "Enable Results Tests")

func TestSubjectTreeBasics(t *testing.T) {
	st := NewSubjectTree[int]()
	require_Equal(t, st.Size(), 0)
	// Single leaf
	old, updated := st.Insert(b("foo.bar.baz"), 22)
	require_True(t, old == nil)
	require_False(t, updated)
	require_Equal(t, st.Size(), 1)
	// Find with single leaf.
	v, found := st.Find(b("foo.bar.baz"))
	require_True(t, found)
	require_Equal(t, *v, 22)
	// Update single leaf
	old, updated = st.Insert(b("foo.bar.baz"), 33)
	require_True(t, old != nil)
	require_Equal(t, *old, 22)
	require_True(t, updated)
	require_Equal(t, st.Size(), 1)
	// Split the tree
	old, updated = st.Insert(b("foo.bar"), 22)
	require_True(t, old == nil)
	require_False(t, updated)
	require_Equal(t, st.Size(), 2)
	// Now we have node4 -> leaf*2
	v, found = st.Find(b("foo.bar"))
	require_True(t, found)
	require_Equal(t, *v, 22)
	// Make sure we can still retrieve the original after the split.
	v, found = st.Find(b("foo.bar.baz"))
	require_True(t, found)
	require_Equal(t, *v, 33)
}

func TestSubjectTreeNodeGrow(t *testing.T) {
	st := NewSubjectTree[int]()
	for i := 0; i < 4; i++ {
		subj := b(fmt.Sprintf("foo.bar.%c", 'A'+i))
		old, updated := st.Insert(subj, 22)
		require_True(t, old == nil)
		require_False(t, updated)
	}
	// We have filled a node4.
	_, ok := st.root.(*node4)
	require_True(t, ok)
	// This one will trigger us to grow.
	old, updated := st.Insert(b("foo.bar.E"), 22)
	require_True(t, old == nil)
	require_False(t, updated)
	_, ok = st.root.(*node16)
	require_True(t, ok)
	// We do not have node48, so once we fill this we should jump to node256.
	for i := 5; i < 16; i++ {
		subj := b(fmt.Sprintf("foo.bar.%c", 'A'+i))
		old, updated := st.Insert(subj, 22)
		require_True(t, old == nil)
		require_False(t, updated)
	}
	// This one will trigger us to grow.
	old, updated = st.Insert(b("foo.bar.Q"), 22)
	require_True(t, old == nil)
	require_False(t, updated)
	_, ok = st.root.(*node256)
	require_True(t, ok)
}

func TestSubjectTreeNodePrefixMismatch(t *testing.T) {
	st := NewSubjectTree[int]()
	st.Insert(b("foo.bar.A"), 11)
	st.Insert(b("foo.bar.B"), 22)
	st.Insert(b("foo.bar.C"), 33)
	// Grab current root. Split below will cause update.
	or := st.root
	// This one will force a split of the node
	st.Insert(b("foo.foo.A"), 44)
	require_True(t, or != st.root)
	// Now make sure we can retrieve correctly.
	v, found := st.Find(b("foo.bar.A"))
	require_True(t, found)
	require_Equal(t, *v, 11)
	v, found = st.Find(b("foo.bar.B"))
	require_True(t, found)
	require_Equal(t, *v, 22)
	v, found = st.Find(b("foo.bar.C"))
	require_True(t, found)
	require_Equal(t, *v, 33)
	v, found = st.Find(b("foo.foo.A"))
	require_True(t, found)
	require_Equal(t, *v, 44)
}

func TestSubjectTreeNodeDelete(t *testing.T) {
	st := NewSubjectTree[int]()
	st.Insert(b("foo.bar.A"), 22)
	v, found := st.Delete(b("foo.bar.A"))
	require_True(t, found)
	require_Equal(t, *v, 22)
	require_Equal(t, st.root, nil)
	v, found = st.Delete(b("foo.bar.A"))
	require_False(t, found)
	require_Equal(t, v, nil)
	v, found = st.Find(b("foo.foo.A"))
	require_False(t, found)
	require_Equal(t, v, nil)
	// Kick to a node4.
	st.Insert(b("foo.bar.A"), 11)
	st.Insert(b("foo.bar.B"), 22)
	st.Insert(b("foo.bar.C"), 33)
	// Make sure we can delete and that we shrink back to leaf.
	v, found = st.Delete(b("foo.bar.C"))
	require_True(t, found)
	require_Equal(t, *v, 33)
	v, found = st.Delete(b("foo.bar.B"))
	require_True(t, found)
	require_Equal(t, *v, 22)
	// We should have shrunk here.
	require_True(t, st.root.isLeaf())
	v, found = st.Delete(b("foo.bar.A"))
	require_True(t, found)
	require_Equal(t, *v, 11)
	require_Equal(t, st.root, nil)
	// Now pop up to a node16 and make sure we can shrink back down.
	for i := 0; i < 5; i++ {
		subj := fmt.Sprintf("foo.bar.%c", 'A'+i)
		st.Insert(b(subj), 22)
	}
	_, ok := st.root.(*node16)
	require_True(t, ok)
	v, found = st.Delete(b("foo.bar.A"))
	require_True(t, found)
	require_Equal(t, *v, 22)
	_, ok = st.root.(*node4)
	require_True(t, ok)
	// Now pop up to node256
	st = NewSubjectTree[int]()
	for i := 0; i < 17; i++ {
		subj := fmt.Sprintf("foo.bar.%c", 'A'+i)
		st.Insert(b(subj), 22)
	}
	_, ok = st.root.(*node256)
	require_True(t, ok)
	v, found = st.Delete(b("foo.bar.A"))
	require_True(t, found)
	require_Equal(t, *v, 22)
	_, ok = st.root.(*node16)
	require_True(t, ok)
	v, found = st.Find(b("foo.bar.B"))
	require_True(t, found)
	require_Equal(t, *v, 22)
}

func TestSubjectTreeNodesAndPaths(t *testing.T) {
	st := NewSubjectTree[int]()
	check := func(subj string) {
		t.Helper()
		v, found := st.Find(b(subj))
		require_True(t, found)
		require_Equal(t, *v, 22)
	}
	st.Insert(b("foo.bar.A"), 22)
	st.Insert(b("foo.bar.B"), 22)
	st.Insert(b("foo.bar.C"), 22)
	st.Insert(b("foo.bar"), 22)
	check("foo.bar.A")
	check("foo.bar.B")
	check("foo.bar.C")
	check("foo.bar")
	// This will do several things in terms of shrinking and pruning,
	// want to make sure it gets prefix correct for new top node4.
	st.Delete(b("foo.bar"))
	check("foo.bar.A")
	check("foo.bar.B")
	check("foo.bar.C")
}

// Check that we are constructing a proper tree with complex insert patterns.
func TestSubjectTreeConstruction(t *testing.T) {
	st := NewSubjectTree[int]()
	st.Insert(b("foo.bar.A"), 1)
	st.Insert(b("foo.bar.B"), 2)
	st.Insert(b("foo.bar.C"), 3)
	st.Insert(b("foo.baz.A"), 11)
	st.Insert(b("foo.baz.B"), 22)
	st.Insert(b("foo.baz.C"), 33)
	st.Insert(b("foo.bar"), 42)

	checkNode := func(an *node, kind string, pors string, numChildren uint16) {
		//		t.Helper()
		require_True(t, an != nil)
		n := *an
		require_True(t, n != nil)
		require_Equal(t, n.kind(), kind)
		require_Equal(t, pors, string(n.path()))
		require_Equal(t, numChildren, n.numChildren())
	}

	checkNode(&st.root, "NODE4", "foo.ba", 2)
	nn := st.root.findChild('r')
	checkNode(nn, "NODE4", "r", 2)
	checkNode((*nn).findChild(0), "LEAF", "", 0)
	rnn := (*nn).findChild('.')
	checkNode(rnn, "NODE4", ".", 3)
	checkNode((*rnn).findChild('A'), "LEAF", "A", 0)
	checkNode((*rnn).findChild('B'), "LEAF", "B", 0)
	checkNode((*rnn).findChild('C'), "LEAF", "C", 0)
	znn := st.root.findChild('z')
	checkNode(znn, "NODE4", "z.", 3)
	checkNode((*znn).findChild('A'), "LEAF", "A", 0)
	checkNode((*znn).findChild('B'), "LEAF", "B", 0)
	checkNode((*znn).findChild('C'), "LEAF", "C", 0)
	// Use st.Dump() if you want a tree print out.

	// Now delete "foo.bar" and make sure put ourselves back together properly.
	v, found := st.Delete(b("foo.bar"))
	require_True(t, found)
	require_Equal(t, *v, 42)

	checkNode(&st.root, "NODE4", "foo.ba", 2)
	nn = st.root.findChild('r')
	checkNode(nn, "NODE4", "r.", 3)
	checkNode((*nn).findChild('A'), "LEAF", "A", 0)
	checkNode((*nn).findChild('B'), "LEAF", "B", 0)
	checkNode((*nn).findChild('C'), "LEAF", "C", 0)
	znn = st.root.findChild('z')
	checkNode(znn, "NODE4", "z.", 3)
	checkNode((*znn).findChild('A'), "LEAF", "A", 0)
	checkNode((*znn).findChild('B'), "LEAF", "B", 0)
	checkNode((*znn).findChild('C'), "LEAF", "C", 0)
}

func match(t *testing.T, st *SubjectTree[int], filter string, expected int) {
	t.Helper()
	var matches []int
	st.Match(b(filter), func(_ []byte, v *int) {
		matches = append(matches, *v)
	})
	require_Equal(t, expected, len(matches))
}

func TestSubjectTreeMatchLeafOnly(t *testing.T) {
	st := NewSubjectTree[int]()
	st.Insert(b("foo.bar.baz.A"), 1)

	// Check all placements of pwc in token space.
	match(t, st, "foo.bar.*.A", 1)
	match(t, st, "foo.*.baz.A", 1)
	match(t, st, "foo.*.*.A", 1)
	match(t, st, "foo.*.*.*", 1)
	match(t, st, "*.*.*.*", 1)
	// Now check fwc.
	match(t, st, ">", 1)
	match(t, st, "foo.>", 1)
	match(t, st, "foo.*.>", 1)
	match(t, st, "foo.bar.>", 1)
	match(t, st, "foo.bar.*.>", 1)

	// Check partials so they do not trigger on leafs.
	match(t, st, "foo.bar.baz", 0)
}

func TestSubjectTreeMatchNodes(t *testing.T) {
	st := NewSubjectTree[int]()
	st.Insert(b("foo.bar.A"), 1)
	st.Insert(b("foo.bar.B"), 2)
	st.Insert(b("foo.bar.C"), 3)
	st.Insert(b("foo.baz.A"), 11)
	st.Insert(b("foo.baz.B"), 22)
	st.Insert(b("foo.baz.C"), 33)

	// Test literals.
	match(t, st, "foo.bar.A", 1)
	match(t, st, "foo.baz.A", 1)
	match(t, st, "foo.bar", 0)
	// Test internal pwc
	match(t, st, "foo.*.A", 2)
	// Test terminal pwc
	match(t, st, "foo.bar.*", 3)
	match(t, st, "foo.baz.*", 3)
	// Check fwc
	match(t, st, ">", 6)
	match(t, st, "foo.>", 6)
	match(t, st, "foo.bar.>", 3)
	match(t, st, "foo.baz.>", 3)
	// Make sure we do not have false positives on prefix matches.
	match(t, st, "foo.ba", 0)

	// Now add in "foo.bar" to make a more complex tree construction
	// and re-test.
	st.Insert(b("foo.bar"), 42)

	// Test literals.
	match(t, st, "foo.bar.A", 1)
	match(t, st, "foo.baz.A", 1)
	match(t, st, "foo.bar", 1)
	// Test internal pwc
	match(t, st, "foo.*.A", 2)
	// Test terminal pwc
	match(t, st, "foo.bar.*", 3)
	match(t, st, "foo.baz.*", 3)
	// Check fwc
	match(t, st, ">", 7)
	match(t, st, "foo.>", 7)
	match(t, st, "foo.bar.>", 3)
	match(t, st, "foo.baz.>", 3)
}

func TestSubjectTreeNoPrefix(t *testing.T) {
	st := NewSubjectTree[int]()
	for i := 0; i < 26; i++ {
		subj := b(fmt.Sprintf("%c", 'A'+i))
		old, updated := st.Insert(subj, 22)
		require_True(t, old == nil)
		require_False(t, updated)
	}
	n, ok := st.root.(*node256)
	require_True(t, ok)
	require_Equal(t, n.numChildren(), 26)
	v, found := st.Delete(b("B"))
	require_True(t, found)
	require_Equal(t, *v, 22)
	require_Equal(t, n.numChildren(), 25)
	v, found = st.Delete(b("Z"))
	require_True(t, found)
	require_Equal(t, *v, 22)
	require_Equal(t, n.numChildren(), 24)
}

func TestSubjectTreePartialTerminalWildcardBugMatch(t *testing.T) {
	st := NewSubjectTree[int]()
	st.Insert(b("STATE.GLOBAL.CELL1.7PDSGAALXNN000010.PROPERTY-A"), 5)
	st.Insert(b("STATE.GLOBAL.CELL1.7PDSGAALXNN000010.PROPERTY-B"), 1)
	st.Insert(b("STATE.GLOBAL.CELL1.7PDSGAALXNN000010.PROPERTY-C"), 2)
	match(t, st, "STATE.GLOBAL.CELL1.7PDSGAALXNN000010.*", 3)
}

func TestSubjectTreeMaxPrefix(t *testing.T) {
	st := NewSubjectTree[int]()
	longPre := strings.Repeat("Z", maxPrefixLen+2)
	for i := 0; i < 2; i++ {
		subj := b(fmt.Sprintf("%s.%c", longPre, 'A'+i))
		old, updated := st.Insert(subj, 22)
		require_True(t, old == nil)
		require_False(t, updated)
	}
	v, found := st.Find(b(fmt.Sprintf("%s.B", longPre)))
	require_True(t, found)
	require_Equal(t, *v, 22)
	v, found = st.Delete(b(fmt.Sprintf("%s.A", longPre)))
	require_True(t, found)
	require_Equal(t, *v, 22)
	require_Equal(t, st.root.numChildren(), 1)
}

func TestSubjectTreeMatchSubjectParam(t *testing.T) {
	st := NewSubjectTree[int]()
	st.Insert(b("foo.bar.A"), 1)
	st.Insert(b("foo.bar.B"), 2)
	st.Insert(b("foo.bar.C"), 3)
	st.Insert(b("foo.baz.A"), 11)
	st.Insert(b("foo.baz.B"), 22)
	st.Insert(b("foo.baz.C"), 33)
	st.Insert(b("foo.bar"), 42)

	checkValMap := map[string]int{
		"foo.bar.A": 1,
		"foo.bar.B": 2,
		"foo.bar.C": 3,
		"foo.baz.A": 11,
		"foo.baz.B": 22,
		"foo.baz.C": 33,
		"foo.bar":   42,
	}
	// Make sure we get a proper subject parameter and it matches our value properly.
	st.Match([]byte(">"), func(subject []byte, v *int) {
		if expected, ok := checkValMap[string(subject)]; !ok {
			t.Fatalf("Unexpected subject parameter: %q", subject)
		} else if expected != *v {
			t.Fatalf("Expected %q to have value of %d, but got %d", subject, expected, *v)
		}
	})
}

func TestSubjectTreeMatchRandomDoublePWC(t *testing.T) {
	st := NewSubjectTree[int]()
	for i := 1; i <= 10_000; i++ {
		subj := fmt.Sprintf("foo.%d.%d", rand.Intn(20)+1, i)
		st.Insert(b(subj), 42)
	}
	match(t, st, "foo.*.*", 10_000)

	// Check with pwc and short interior token.
	seen, verified := 0, 0
	st.Match(b("*.2.*"), func(_ []byte, _ *int) {
		seen++
	})
	// Now check via walk to make sure we are right.
	st.Iter(func(subject []byte, v *int) bool {
		tokens := strings.Split(string(subject), ".")
		require_Equal(t, len(tokens), 3)
		if tokens[1] == "2" {
			verified++
		}
		return true
	})
	require_Equal(t, seen, verified)

	seen, verified = 0, 0
	st.Match(b("*.*.222"), func(_ []byte, _ *int) {
		seen++
	})
	st.Iter(func(subject []byte, v *int) bool {
		tokens := strings.Split(string(subject), ".")
		require_Equal(t, len(tokens), 3)
		if tokens[2] == "222" {
			verified++
		}
		return true
	})
	require_Equal(t, seen, verified)
}

func TestSubjectTreeIter(t *testing.T) {
	st := NewSubjectTree[int]()
	st.Insert(b("foo.bar.A"), 1)
	st.Insert(b("foo.bar.B"), 2)
	st.Insert(b("foo.bar.C"), 3)
	st.Insert(b("foo.baz.A"), 11)
	st.Insert(b("foo.baz.B"), 22)
	st.Insert(b("foo.baz.C"), 33)
	st.Insert(b("foo.bar"), 42)

	checkValMap := map[string]int{
		"foo.bar.A": 1,
		"foo.bar.B": 2,
		"foo.bar.C": 3,
		"foo.baz.A": 11,
		"foo.baz.B": 22,
		"foo.baz.C": 33,
		"foo.bar":   42,
	}
	checkOrder := []string{
		"foo.bar",
		"foo.bar.A",
		"foo.bar.B",
		"foo.bar.C",
		"foo.baz.A",
		"foo.baz.B",
		"foo.baz.C",
	}
	var received int
	walk := func(subject []byte, v *int) bool {
		if expected := checkOrder[received]; expected != string(subject) {
			t.Fatalf("Expected %q for %d item returned, got %q", expected, received, subject)
		}
		received++
		require_True(t, v != nil)
		if expected := checkValMap[string(subject)]; expected != *v {
			t.Fatalf("Expected %q to have value of %d, but got %d", subject, expected, *v)
		}
		return true
	}
	// Kick in the iter.
	st.Iter(walk)
	require_Equal(t, received, len(checkOrder))

	// Make sure we can terminate properly.
	received = 0
	st.Iter(func(subject []byte, v *int) bool {
		received++
		return received != 4
	})
	require_Equal(t, received, 4)
}

func TestSubjectTreeInsertSamePivotBug(t *testing.T) {
	testSubjects := [][]byte{
		[]byte("0d00.2abbb82c1d.6e16.fa7f85470e.3e46"),
		[]byte("534b12.3486c17249.4dde0666"),
		[]byte("6f26aabd.920ee3.d4d3.5ffc69f6"),
		[]byte("8850.ade3b74c31.aa533f77.9f59.a4bd8415.b3ed7b4111"),
		[]byte("5a75047dcb.5548e845b6.76024a34.14d5b3.80c426.51db871c3a"),
		[]byte("825fa8acfc.5331.00caf8bbbd.107c4b.c291.126d1d010e"),
	}
	st := NewSubjectTree[int]()
	for _, subj := range testSubjects {
		old, updated := st.Insert(subj, 22)
		require_True(t, old == nil)
		require_False(t, updated)
		if _, found := st.Find(subj); !found {
			t.Fatalf("Could not find subject %q which should be findable", subj)
		}
	}
}

func TestSubjectTreeRandomTrackEntries(t *testing.T) {
	st := NewSubjectTree[int]()
	smap := make(map[string]struct{}, 1000)

	// Make sure all added items can be found.
	check := func() {
		t.Helper()
		for subj := range smap {
			if _, found := st.Find(b(subj)); !found {
				t.Fatalf("Could not find subject %q which should be findable", subj)
			}
		}
	}

	buf := make([]byte, 10)
	for i := 0; i < 1000; i++ {
		var sb strings.Builder
		// 1-6 tokens.
		numTokens := rand.Intn(6) + 1
		for i := 0; i < numTokens; i++ {
			tlen := rand.Intn(4) + 2
			tok := buf[:tlen]
			crand.Read(tok)
			sb.WriteString(hex.EncodeToString(tok))
			if i != numTokens-1 {
				sb.WriteString(".")
			}
		}
		subj := sb.String()
		// Avoid dupes since will cause check to fail after we delete messages.
		if _, ok := smap[subj]; ok {
			continue
		}
		smap[subj] = struct{}{}
		old, updated := st.Insert(b(subj), 22)
		require_True(t, old == nil)
		require_False(t, updated)
		require_Equal(t, st.Size(), len(smap))
		check()
	}
}

func TestSubjectTreeMetaSize(t *testing.T) {
	var base meta
	require_Equal(t, unsafe.Sizeof(base), 64)
}

func b(s string) []byte {
	return []byte(s)
}

func TestSubjectTreeMatchAllPerf(t *testing.T) {
	if !*runResults {
		t.Skip()
	}
	st := NewSubjectTree[int]()

	for i := 0; i < 1_000_000; i++ {
		subj := fmt.Sprintf("subj.%d.%d", rand.Intn(100)+1, i)
		st.Insert(b(subj), 22)
	}

	for _, f := range [][]byte{
		[]byte(">"),
		[]byte("subj.>"),
		[]byte("subj.*.*"),
		[]byte("*.*.*"),
		[]byte("subj.1.*"),
		[]byte("subj.1.>"),
		[]byte("subj.*.1"),
		[]byte("*.*.1"),
	} {
		start := time.Now()
		count := 0
		st.Match(f, func(_ []byte, _ *int) {
			count++
		})
		t.Logf("Match %q took %s and matched %d entries", f, time.Since(start), count)
	}
}
