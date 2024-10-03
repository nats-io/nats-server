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
	"bytes"
	crand "crypto/rand"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"slices"
	"strings"
	"testing"
	"time"
)

// Print Results: go test -v  --args --results
// For some benchmarks.
var runResults = flag.Bool("results", false, "Enable Results Tests")

func TestSubjectTreeBasics(t *testing.T) {
	st := NewSubjectTree[int]()
	testSubjectTreeBasics(t, st)
}

func testSubjectTreeBasics(t *testing.T, st *SubjectTree[int]) {
	require_Equal(t, st.Size(), 0)
	// Single leaf
	old, updated := st.Insert(b("foo.bar.baz"), 22)
	require_True(t, old == nil)
	require_False(t, updated)
	require_Equal(t, st.Size(), 1)
	// Find shouldn't work with a wildcard.
	_, found := st.Find(b("foo.bar.*"))
	require_False(t, found)
	// But it should with a literal. Find with single leaf.
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
	_, ok = st.root.(*node48)
	require_True(t, ok)
	// Fill the node48.
	for i := 17; i < 48; i++ {
		subj := b(fmt.Sprintf("foo.bar.%c", 'A'+i))
		old, updated := st.Insert(subj, 22)
		require_True(t, old == nil)
		require_False(t, updated)
	}
	// This one will trigger us to grow.
	subj := b(fmt.Sprintf("foo.bar.%c", 'A'+49))
	old, updated = st.Insert(subj, 22)
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
	// Now pop up to node48
	st = NewSubjectTree[int]()
	for i := 0; i < 17; i++ {
		subj := fmt.Sprintf("foo.bar.%c", 'A'+i)
		st.Insert(b(subj), 22)
	}
	_, ok = st.root.(*node48)
	require_True(t, ok)
	v, found = st.Delete(b("foo.bar.A"))
	require_True(t, found)
	require_Equal(t, *v, 22)
	_, ok = st.root.(*node16)
	require_True(t, ok)
	v, found = st.Find(b("foo.bar.B"))
	require_True(t, found)
	require_Equal(t, *v, 22)
	// Now pop up to node256
	st = NewSubjectTree[int]()
	for i := 0; i < 49; i++ {
		subj := fmt.Sprintf("foo.bar.%c", 'A'+i)
		st.Insert(b(subj), 22)
	}
	_, ok = st.root.(*node256)
	require_True(t, ok)
	v, found = st.Delete(b("foo.bar.A"))
	require_True(t, found)
	require_Equal(t, *v, 22)
	_, ok = st.root.(*node48)
	require_True(t, ok)
	v, found = st.Find(b("foo.bar.B"))
	require_True(t, found)
	require_Equal(t, *v, 22)
}

func TestSubjectTreeNodesAndPaths(t *testing.T) {
	st := NewSubjectTree[int]()
	testSubjectTreeNodesAndPaths(t, st)
}

func testSubjectTreeNodesAndPaths(t *testing.T, st *SubjectTree[int]) {
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
	testSubjectTreeMatchLeafOnly(t , st)
}

func testSubjectTreeMatchLeafOnly(t *testing.T, st *SubjectTree[int]) {
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
	testSubjectTreeMatchNodes(t, st)
}

func testSubjectTreeMatchNodes(t *testing.T, st *SubjectTree[int]) {
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
	n, ok := st.root.(*node48)
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
	testSubjectTreePartialTerminalWildcardBugMatch(t, st)
}

func testSubjectTreePartialTerminalWildcardBugMatch(t *testing.T, st *SubjectTree[int]) {
	st.Insert(b("STATE.GLOBAL.CELL1.7PDSGAALXNN000010.PROPERTY-A"), 5)
	st.Insert(b("STATE.GLOBAL.CELL1.7PDSGAALXNN000010.PROPERTY-B"), 1)
	st.Insert(b("STATE.GLOBAL.CELL1.7PDSGAALXNN000010.PROPERTY-C"), 2)
	match(t, st, "STATE.GLOBAL.CELL1.7PDSGAALXNN000010.*", 3)
}

func TestSubjectTreeMatchSubjectParam(t *testing.T) {
	st := NewSubjectTree[int]()
	testSubjectTreeMatchSubjectParam(t, st)
}

func testSubjectTreeMatchSubjectParam(t *testing.T, st *SubjectTree[int]) {
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
	testSubjectTreeMatchRandomDoublePWC(t, st)
}

func testSubjectTreeMatchRandomDoublePWC(t *testing.T, st *SubjectTree[int]) {
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
	testSubjectTreeIter(t , st)
}

func testSubjectTreeIter(t *testing.T, st *SubjectTree[int]) {
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
	st := NewSubjectTree[int]()
	testSubjectTreeInsertSamePivotBug(t, st)
}

func testSubjectTreeInsertSamePivotBug(t *testing.T, st *SubjectTree[int]) {
	testSubjects := [][]byte{
		[]byte("0d00.2abbb82c1d.6e16.fa7f85470e.3e46"),
		[]byte("534b12.3486c17249.4dde0666"),
		[]byte("6f26aabd.920ee3.d4d3.5ffc69f6"),
		[]byte("8850.ade3b74c31.aa533f77.9f59.a4bd8415.b3ed7b4111"),
		[]byte("5a75047dcb.5548e845b6.76024a34.14d5b3.80c426.51db871c3a"),
		[]byte("825fa8acfc.5331.00caf8bbbd.107c4b.c291.126d1d010e"),
	}
	for _, subj := range testSubjects {
		old, updated := st.Insert(subj, 22)
		require_True(t, old == nil)
		require_False(t, updated)
		if _, found := st.Find(subj); !found {
			t.Fatalf("Could not find subject %q which should be findable", subj)
		}
	}
}

func TestSubjectTreeMatchTsepSecondThenPartialPartBug(t *testing.T) {
	st := NewSubjectTree[int]()
	testSubjectTreeMatchTsepSecondThenPartialPartBug(t, st)
}

func testSubjectTreeMatchTsepSecondThenPartialPartBug(t *testing.T, st *SubjectTree[int]) {
	st.Insert(b("foo.xxxxx.foo1234.zz"), 22)
	st.Insert(b("foo.yyy.foo123.zz"), 22)
	st.Insert(b("foo.yyybar789.zz"), 22)
	st.Insert(b("foo.yyy.foo12345.zz"), 22)
	st.Insert(b("foo.yyy.foo12345.yy"), 22)
	st.Insert(b("foo.yyy.foo123456789.zz"), 22)
	match(t, st, "foo.*.foo123456789.*", 1)
	match(t, st, "foo.*.*.zzz.foo.>", 0)
}

func TestSubjectTreeMatchMultipleWildcardBasic(t *testing.T) {
	st := NewSubjectTree[int]()
	testSubjectTreeMatchMultipleWildcardBasic(t, st)
}

func testSubjectTreeMatchMultipleWildcardBasic(t *testing.T, st *SubjectTree[int]) {
	st.Insert(b("A.B.C.D.0.G.H.I.0"), 22)
	st.Insert(b("A.B.C.D.1.G.H.I.0"), 22)
	match(t, st, "A.B.*.D.1.*.*.I.0", 1)
}

func TestSubjectTreeMatchInvalidWildcard(t *testing.T) {
	st := NewSubjectTree[int]()
	testSubjectTreeMatchInvalidWildcard(t, st)
}

func testSubjectTreeMatchInvalidWildcard(t *testing.T, st *SubjectTree[int]) {
	st.Insert(b("foo.123"), 22)
	st.Insert(b("one.two.three.four.five"), 22)
	st.Insert(b("'*.123"), 22)
	match(t, st, "invalid.>", 0)
	match(t, st, ">", 3)
	match(t, st, `'*.*`, 1)
	match(t, st, `'*.*.*'`, 0)
	// None of these should match.
	match(t, st, "`>`", 0)
	match(t, st, `">"`, 0)
	match(t, st, `'>'`, 0)
	match(t, st, `'*.>'`, 0)
	match(t, st, `'*.>.`, 0)
	match(t, st, "`invalid.>`", 0)
	match(t, st, `'*.*'`, 0)
}

func TestSubjectTreeRandomTrackEntries(t *testing.T) {
	st := NewSubjectTree[int]()
	testSubjectTreeRandomTrackEntries(t, st)
}

func testSubjectTreeRandomTrackEntries(t *testing.T, st *SubjectTree[int]) {
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

// Needs to be longer then internal node prefix, which currently is 24.
func TestSubjectTreeLongTokens(t *testing.T) {
	st := NewSubjectTree[int]()
	testSubjectTreeLongTokens(t , st)
}

func testSubjectTreeLongTokens(t *testing.T, st *SubjectTree[int]) {
	st.Insert(b("a1.aaaaaaaaaaaaaaaaaaaaaa0"), 1)
	st.Insert(b("a2.0"), 2)
	st.Insert(b("a1.aaaaaaaaaaaaaaaaaaaaaa1"), 3)
	st.Insert(b("a2.1"), 4)
	// Simulate purge of a2.>
	// This required to show bug.
	st.Delete(b("a2.0"))
	st.Delete(b("a2.1"))
	require_Equal(t, st.Size(), 2)
	v, found := st.Find(b("a1.aaaaaaaaaaaaaaaaaaaaaa0"))
	require_True(t, found)
	require_Equal(t, *v, 1)
	v, found = st.Find(b("a1.aaaaaaaaaaaaaaaaaaaaaa1"))
	require_True(t, found)
	require_Equal(t, *v, 3)
}

func b(s string) []byte {
	return []byte(s)
}

func TestSubjectTreeMatchAllPerf(t *testing.T) {
	if !*runResults {
		t.Skip()
	}
	st := NewSubjectTree[int]()
	testSubjectTreeMatchAllPerf(t, st)
}

func testSubjectTreeMatchAllPerf(t *testing.T, st *SubjectTree[int]) {
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

func TestSubjectTreeIterPerf(t *testing.T) {
	if !*runResults {
		t.Skip()
	}
	st := NewSubjectTree[int]()
	testSubjectTreeIterPerf(t, st)
}

func testSubjectTreeIterPerf(t *testing.T, st *SubjectTree[int]) {
	for i := 0; i < 1_000_000; i++ {
		subj := fmt.Sprintf("subj.%d.%d", rand.Intn(100)+1, i)
		st.Insert(b(subj), 22)
	}

	start := time.Now()
	count := 0
	st.Iter(func(_ []byte, _ *int) bool {
		count++
		return true
	})
	t.Logf("Iter took %s and matched %d entries", time.Since(start), count)
}

func TestSubjectTreeNode48(t *testing.T) {
	var a, b, c leaf[int]
	var n node48

	n.addChild('A', &a)
	require_Equal(t, n.key['A'], 1)
	require_True(t, n.child[0] != nil)
	require_Equal(t, n.child[0].(*leaf[int]), &a)
	require_Equal(t, len(n.children()), 1)

	child := n.findChild('A')
	require_True(t, child != nil)
	require_Equal(t, (*child).(*leaf[int]), &a)

	n.addChild('B', &b)
	require_Equal(t, n.key['B'], 2)
	require_True(t, n.child[1] != nil)
	require_Equal(t, n.child[1].(*leaf[int]), &b)
	require_Equal(t, len(n.children()), 2)

	child = n.findChild('B')
	require_True(t, child != nil)
	require_Equal(t, (*child).(*leaf[int]), &b)

	n.addChild('C', &c)
	require_Equal(t, n.key['C'], 3)
	require_True(t, n.child[2] != nil)
	require_Equal(t, n.child[2].(*leaf[int]), &c)
	require_Equal(t, len(n.children()), 3)

	child = n.findChild('C')
	require_True(t, child != nil)
	require_Equal(t, (*child).(*leaf[int]), &c)

	n.deleteChild('A')
	require_Equal(t, len(n.children()), 2)
	require_Equal(t, n.key['A'], 0) // Now deleted
	require_Equal(t, n.key['B'], 2) // Untouched
	require_Equal(t, n.key['C'], 1) // Where A was

	child = n.findChild('A')
	require_Equal(t, child, nil)
	require_True(t, n.child[0] != nil)
	require_Equal(t, n.child[0].(*leaf[int]), &c)

	child = n.findChild('B')
	require_True(t, child != nil)
	require_Equal(t, (*child).(*leaf[int]), &b)
	require_True(t, n.child[1] != nil)
	require_Equal(t, n.child[1].(*leaf[int]), &b)

	child = n.findChild('C')
	require_True(t, child != nil)
	require_Equal(t, (*child).(*leaf[int]), &c)
	require_True(t, n.child[2] == nil)

	var gotB, gotC bool
	var iterations int
	n.iter(func(n node) bool {
		iterations++
		if gb, ok := n.(*leaf[int]); ok && &b == gb {
			gotB = true
		}
		if gc, ok := n.(*leaf[int]); ok && &c == gc {
			gotC = true
		}
		return true
	})
	require_Equal(t, iterations, 2)
	require_True(t, gotB)
	require_True(t, gotC)

	// Check for off-by-one on byte 255 as found by staticcheck, see
	// https://github.com/nats-io/nats-server/pull/5826.
	n.addChild(255, &c)
	require_Equal(t, n.key[255], 3)
	grown := n.grow().(*node256)
	require_True(t, grown.findChild(255) != nil)
	shrunk := n.shrink().(*node16)
	require_True(t, shrunk.findChild(255) != nil)
}

func TestSubjectTreeMatchNoCallbackDupe(t *testing.T) {
	st := NewSubjectTree[int]()
	testSubjectTreeMatchNoCallbackDupe(t, st)
}

func testSubjectTreeMatchNoCallbackDupe(t *testing.T, st *SubjectTree[int]) {
	st.Insert(b("foo.bar.A"), 1)
	st.Insert(b("foo.bar.B"), 1)
	st.Insert(b("foo.bar.C"), 1)
	st.Insert(b("foo.bar.>"), 1)

	for _, f := range [][]byte{
		[]byte(">"),
		[]byte("foo.>"),
		[]byte("foo.bar.>"),
	} {
		seen := map[string]struct{}{}
		st.Match(f, func(bsubj []byte, _ *int) {
			subj := string(bsubj)
			if _, ok := seen[subj]; ok {
				t.Logf("Match callback was called twice for %q", subj)
			}
			seen[subj] = struct{}{}
		})
	}
}

func TestSubjectTreeNilNoPanic(t *testing.T) {
	var st *SubjectTree[int]
	st.Match([]byte("foo"), func(_ []byte, _ *int) {})
	_, found := st.Find([]byte("foo"))
	require_False(t, found)
	_, found = st.Delete([]byte("foo"))
	require_False(t, found)
	_, found = st.Insert([]byte("foo"), 22)
	require_False(t, found)
}

func TestSqlSubjectTreeBasics(t *testing.T) {
	st := NewSqlSubjectTree[int](nil)
	testSubjectTreeBasics(t, st)
}

func TestSqlSubjectTreeNodesAndPaths(t *testing.T) {
	st := NewSqlSubjectTree[int](nil)
	testSubjectTreeNodesAndPaths(t, st)
}

func TestSqlSubjectTreeMatchLeafOnly(t *testing.T) {
	st := NewSqlSubjectTree[int](nil)
	testSubjectTreeMatchLeafOnly(t , st)
}

func TestSqlSubjectTreeMatchNodes(t *testing.T) {
	st := NewSqlSubjectTree[int](nil)
	testSubjectTreeMatchNodes(t, st)
}

func TestSqlSubjectTreePartialTerminalWildcardBugMatch(t *testing.T) {
	st := NewSqlSubjectTree[int](nil)
	testSubjectTreePartialTerminalWildcardBugMatch(t, st)
}

func TestSqlSubjectTreeMatchSubjectParam(t *testing.T) {
	st := NewSqlSubjectTree[int](nil)
	testSubjectTreeMatchSubjectParam(t, st)
}

func TestSqlSubjectTreeMatchRandomDoublePWC(t *testing.T) {
	st := NewSqlSubjectTree[int](nil)
	testSubjectTreeMatchRandomDoublePWC(t, st)
}

func TestSqlSubjectTreeIter(t *testing.T) {
	st := NewSqlSubjectTree[int](nil)
	testSubjectTreeIter(t , st)
}

func TestSqlSubjectTreeInsertSamePivotBug(t *testing.T) {
	st := NewSqlSubjectTree[int](nil)
	testSubjectTreeInsertSamePivotBug(t, st)
}

func TestSqlSubjectTreeMatchTsepSecondThenPartialPartBug(t *testing.T) {
	st := NewSqlSubjectTree[int](nil)
	testSubjectTreeMatchTsepSecondThenPartialPartBug(t, st)
}

func TestSqlSubjectTreeMatchMultipleWildcardBasic(t *testing.T) {
	st := NewSqlSubjectTree[int](nil)
	testSubjectTreeMatchMultipleWildcardBasic(t, st)
}

func TestSqlSubjectTreeMatchInvalidWildcard(t *testing.T) {
	st := NewSqlSubjectTree[int](nil)
	testSubjectTreeMatchInvalidWildcard(t, st)
}

func TestSqlSubjectTreeRandomTrackEntries(t *testing.T) {
	st := NewSqlSubjectTree[int](nil)
	testSubjectTreeRandomTrackEntries(t, st)
}

func TestSqlSubjectTreeLongTokens(t *testing.T) {
	st := NewSqlSubjectTree[int](nil)
	testSubjectTreeLongTokens(t , st)
}

func TestSqlSubjectTreeMatchAllPerf(t *testing.T) {
	if !*runResults {
		t.Skip()
	}
	st := NewSqlSubjectTree[int](nil)
	testSubjectTreeMatchAllPerf(t, st)
}

func TestSqlSubjectTreeIterPerf(t *testing.T) {
	if !*runResults {
		t.Skip()
	}
	st := NewSqlSubjectTree[int](nil)
	testSubjectTreeIterPerf(t, st)
}

func TestSqlSubjectTreeMatchNoCallbackDupe(t *testing.T) {
	st := NewSqlSubjectTree[int](nil)
	testSubjectTreeMatchNoCallbackDupe(t, st)
}

var strChoice []byte

func init() {
	seq := func(m, n byte) {
		for m <= n {
			strChoice = append(strChoice, m)
			m++
		}
	}
	// no riff-raff
	seq('a', 'z')
	seq('A', 'Z')
	seq('-', '-')
	seq('0', '9')
}

func randStr(m, n int) string {
	if n > m {
		m += rand.Intn(n-m)
	}
	b := make([]byte, 1, m)
	b[0] = strChoice[rand.Intn(len(strChoice) - 11)]
	for range m-1 {
		b = append(b, strChoice[rand.Intn(len(strChoice))])
	}
	return string(b)
}

func print(format string, args ...any) {
	if testing.Verbose() {
		fmt.Printf(format, args...)
	}
}

func Sl[T any](e ...T) []T {
	return e
}

func TestSqlSubjectTree(t *testing.T) {
	dbPath := "stree-test.db"
	for _, sfx := range Sl("", "-wal", "-shm") {
		if err := os.Remove(dbPath+sfx); err != nil && !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
	}
	st := NewSqlSubjectTree[string](&Config{
		DBPath: dbPath,
	})
	var _idx [16]int
	var idx []int
	for op := range []int{opStore, opFind} {
		idx = st.subjectToIdx(op, b("foo"), _idx[:0])
		require_True(t, slices.Equal(idx, Sl(1)))
		idx = st.subjectToIdx(op, b("foobar.foobat"), _idx[:0])
		require_True(t, slices.Equal(idx, Sl(2, 3)))
		idx = st.subjectToIdx(op, b("foo.plugh.foobat.xyzzy"), _idx[:0])
		require_True(t, slices.Equal(idx, Sl(1, 4, 3, 5)))
	}
	idx = st.subjectToIdx(opFind, b("foobar.missing.foo"), _idx[:0])
	require_True(t, idx == nil)
	require_True(t, slices.Equal(_idx[:3], Sl(2, -1, 1)))

	var _tokens [16]string
	var tokens []string
	tokens = st.sqlIdxToStr(Sl(4, 99, 1), _tokens[:0])
	require_True(t, slices.Equal(tokens, Sl("plugh", "", "foo")))

	idx = st.subjectToIdx(opMatch, b("xyzzy.*.foo.plugh.>"), _idx[:0])
	print("idx=%v\n", idx)
	require_True(t, slices.Equal(idx, Sl(5, 0, 1, 4, 0)))
	idx = st.subjectToIdx(opMatch, b("xyzzy.*.foo.qwerty.>"), _idx[:0])
	print("idx=%v\n", idx)
	require_True(t, idx == nil)

	subj := b("xyzzy.tora.tora.tora")
	idx = st.subjectToIdx(opStore, subj, _idx[:0])
	require_True(t, slices.Equal(idx, Sl(5, 6, 6, 6)))
	print("%s -> idx=%v\n", string(subj), idx)

	for k := range 6 {
		s := getQuerySubjectFull(k+1)
		print("%s\n", s)
	}
	for k := range 6 {
		s := getDeleteSubject(k+1)
		print("%s\n", s)
	}
	for k := range 6 {
		s := getQuerySubjectWild(k+1)
		print("%s\n", s)
	}
	for k := range 6 {
		s := getInsertSubject(k+1)
		print("%s\n", s)
	}
	for k := range 6 {
		s := st._ensureSubject(k+1)
		print("%s\n", s)
	}

	subj = b("plugh.foobar.x123")
	old, deleted := st.Delete(subj)
	require_True(t, old == nil)
	require_False(t, deleted)

	old, updated := st.Insert(subj, "rhubarb")
	require_True(t, old == nil)
	require_False(t, updated)

	old, deleted = st.Delete(subj)
	require_True(t, old != nil && *old == "rhubarb")
	require_True(t, deleted)

	subjs := [][]byte{subj, b("plugh.foo.x123"), b("plugh.foobat.x123")}
	vals := []string{"rhubarb", "mumbo-jumbo", "hoopla"}
	svmap := make(map[string]string)
	for k, subj := range subjs {
		svmap[string(subj)] = vals[k]
		old, updated := st.Insert(subj, vals[k])
		require_True(t, old == nil)
		require_False(t, updated)
	}
	for k, subj := range subjs {
		pV, found := st.Find(subj)
		require_True(t, pV != nil && *pV == vals[k])
		require_True(t, found)
	}

	require_Equal(t, st.Size(), 3)
	old, updated = st.Insert(subj, "rhubarb1")
	require_True(t, old != nil && *old == "rhubarb")
	require_True(t, updated)

	require_Equal(t, st.Size(), 3)
	old, updated = st.Insert(subj, "rhubarb")
	require_True(t, old != nil && *old == "rhubarb1")
	require_True(t, updated)
	require_Equal(t, st.Size(), 3)

	count := 0
	st.Match(b("plugh.*.x123"), func(subject []byte, val *string) {
		count++
		print("%s -> %s\n", string(subject), *val)
		require_True(t, svmap[string(subject)] == *val)
	})
	require_Equal(t, count, len(svmap))

	count = 0
	st.Match(b("plugh.>"), func(subject []byte, val *string) {
		count++
		require_True(t, svmap[string(subject)] == *val)
	})
	require_Equal(t, count, len(svmap))

	count = 0
	st.Match(b("*.*.x123"), func(subject []byte, val *string) {
		count++
		require_True(t, svmap[string(subject)] == *val)
	})
	require_Equal(t, count, len(svmap))

	print("%s\n", st.sqlIterQuery())
	st.Iter(func(subject []byte, val *string) bool {
		print("%s -> %s\n", string(subject), *val)
		return true
	})

	st.Empty()
	for range 20 {
		var sb strings.Builder
		pfx := ""
		for range rand.Intn(6)+1 {
			sb.WriteString(pfx)
			pfx = "."
			sb.WriteString(randStr(1, 8))
		}
		subj := sb.String()
		val := randStr(17, 23)
		//print("%s : %s\n", subj, val)
		old, updated := st.Insert(b(subj), val)
		require_True(t, old == nil)
		require_False(t, updated)
	}
	st.Iter(func(subject []byte, val *string) bool {
		print("%s -> %s\n", string(subject), *val)
		return true
	})
}

type GobSmack struct {
	Index   int
	Notes   string
	Payload []byte
}

func (gb *GobSmack) Equal(other *GobSmack) bool {
	return gb.Index == other.Index &&
		gb.Notes == other.Notes &&
		bytes.Equal(gb.Payload, other.Payload)
}

func TestSqlSubjectTreeGob(t *testing.T) {
	dbPath := "stree-gob-test.db"
	for _, sfx := range Sl("", "-wal", "-shm") {
		if err := os.Remove(dbPath+sfx); err != nil && !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
	}
	st := NewSqlSubjectTree[GobSmack](&Config{
		DBPath: dbPath,
	})
	print("len(desc) = %d\n", len(st.conn.vElemGob.Desc()))
	print("desc = % 02x\n", st.conn.vElemGob.Desc())

	datum := GobSmack{
		Index:   137,
		Notes:   "almost 1/\u03b1",
		Payload: b("https://physics.nist.gov/cuu/Constants/alpha.html"),
	}
	subj := b("consts.alpha")
	old, updated := st.Insert(subj, datum)
	require_True(t, old == nil)
	require_False(t, updated)

	pV, found := st.Find(subj)
	require_True(t, pV != nil && datum.Equal(pV))
	require_True(t, found)

	err := st.Close()
	if err != nil {
		print("Close: err=%v\n", err)
	}
	require_True(t, err == nil)

	st = NewSqlSubjectTree[GobSmack](&Config{
		DBPath: dbPath,
	})
	print("read: len(desc) = %d\n", len(st.conn.vElemGob.Desc()))
	print("read: desc = % 02x\n", st.conn.vElemGob.Desc())

	got_it := false
	st.Match(subj, func(subj []byte, pV *GobSmack) {
		got_it = true
		require_True(t, pV != nil && datum.Equal(pV))
	})
	require_True(t, got_it)

	err = st.Close()
	require_True(t, err == nil)
}
