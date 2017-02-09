package health

import (
	"bytes"
	"fmt"
	"sync"

	"github.com/google/btree"
)

// ranktree is an in-memory, sorted,
// balanced tree that is implemented
// as a left-leaning red-black tree.
// It holds AgentLoc
// from candidate servers in the cluster,
// sorting them based on
// AgentLocLessThan() so they are in
// priority order and deduplicated.
type ranktree struct {
	*btree.BTree
	tex sync.Mutex
}

// Less is how the agents are sorted.
// The Less method is expected by the btree.
// See the AgentLocLessThan for standalone
// implementation.
func (a AgentLoc) Less(than btree.Item) bool {
	b := than.(AgentLoc)
	return AgentLocLessThan(&a, &b)
}

// insert is idemopotent so it is safe
// to insert the same sloc multiple times and
// duplicates will be ignored.
func (t *ranktree) insert(j AgentLoc) {
	t.tex.Lock()
	t.ReplaceOrInsert(j)
	t.tex.Unlock()
}

// present locks, Has does not.
func (t *ranktree) present(j AgentLoc) bool {
	t.tex.Lock()
	b := t.Has(j)
	t.tex.Unlock()
	return b
}

func (t *ranktree) minrank() (min AgentLoc) {
	t.tex.Lock()
	min = t.Min().(AgentLoc)
	t.tex.Unlock()

	return
}

func (t *ranktree) deleteSloc(j AgentLoc) {
	t.tex.Lock()
	t.Delete(j)
	t.tex.Unlock()
}

func newRanktree() *ranktree {
	return &ranktree{
		BTree: btree.New(2),
	}
}

func (t *ranktree) String() string {
	t.tex.Lock()

	s := "["
	t.AscendLessThan(AgentLoc{}, func(item btree.Item) bool {
		cur := item.(AgentLoc)
		s += cur.String() + ","
		return true
	})
	t.tex.Unlock()

	// replace last comma with matching bracket
	n := len(s)
	if n > 1 {
		s = s[:n-1]
	}
	return s + "]"
}

func (t *ranktree) clone() *ranktree {
	r := newRanktree()
	t.tex.Lock()

	t.AscendLessThan(AgentLoc{}, func(item btree.Item) bool {
		cur := item.(AgentLoc)
		r.insert(cur)
		return true
	})
	t.tex.Unlock()
	return r
}

func (t *ranktree) MarshalJSON() ([]byte, error) {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "%s", t)
	return buf.Bytes(), nil
}

// Len() is inherted from the btree,
// but isn't protected by mutex. Use
// size to avoid races.
func (t *ranktree) size() int {
	t.tex.Lock()
	n := t.Len()
	t.tex.Unlock()
	return n
}

// return a minus b, where a and b are sets.
func setDiff(a, b *members) *members {

	res := a.DedupTree.clone()
	a.DedupTree.tex.Lock()
	b.DedupTree.tex.Lock()

	b.DedupTree.AscendLessThan(AgentLoc{}, func(item btree.Item) bool {
		v := item.(AgentLoc)
		res.deleteSloc(v)
		return true // keep iterating
	})

	b.DedupTree.tex.Unlock()
	a.DedupTree.tex.Unlock()
	return &members{DedupTree: res}
}

func setsEqual(a, b *members) bool {
	a.DedupTree.tex.Lock()
	b.DedupTree.tex.Lock()
	defer b.DedupTree.tex.Unlock()
	defer a.DedupTree.tex.Unlock()

	alen := a.DedupTree.Len()
	if alen != b.DedupTree.Len() {
		return false
	}
	// INVAR: len(a) == len(b)
	if alen == 0 {
		return true
	}

	missing := false
	a.DedupTree.AscendLessThan(AgentLoc{}, func(item btree.Item) bool {
		v := item.(AgentLoc)
		if !b.DedupTree.Has(v) {
			missing = true
			return false // stop iterating
		}
		return true // keep iterating
	})
	return !missing
}
