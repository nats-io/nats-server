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
	"sort"
)

// SubjectTree is an adaptive radix trie (ART) for storing subject information on literal subjects.
// Will use dynamic nodes, path compression and lazy expansion.
// The reason this exists is to not only save some memory in our filestore but to greatly optimize matching
// a wildcard subject to certain members, e.g. consumer NumPending calculations.
type SubjectTree[T any] struct {
	root node
	size int
}

// NewSubjectTree creates a new SubjectTree with values T.
func NewSubjectTree[T any]() *SubjectTree[T] {
	return &SubjectTree[T]{}
}

// Size returns the number of elements stored.
func (t *SubjectTree[T]) Size() int {
	if t == nil {
		return 0
	}
	return t.size
}

// Will empty out the tree, or if tree is nil create a new one.
func (t *SubjectTree[T]) Empty() *SubjectTree[T] {
	if t == nil {
		return NewSubjectTree[T]()
	}
	t.root, t.size = nil, 0
	return t
}

// Insert a value into the tree. Will return if the value was updated and if so the old value.
func (t *SubjectTree[T]) Insert(subject []byte, value T) (*T, bool) {
	old, updated := t.insert(&t.root, subject, value, 0)
	if !updated {
		t.size++
	}
	return old, updated
}

// Find will find the value and return it or false if it was not found.
func (t *SubjectTree[T]) Find(subject []byte) (*T, bool) {
	var si uint16
	for n := t.root; n != nil; {
		if n.isLeaf() {
			if ln := n.(*leaf[T]); ln.match(subject[si:]) {
				return &ln.value, true
			}
			return nil, false
		}
		// We are a node type here, grab meta portion.
		if bn := n.base(); bn.prefixLen > 0 {
			end := min(int(si+bn.prefixLen), len(subject))
			if !bytes.Equal(subject[si:end], bn.prefix[:bn.prefixLen]) {
				return nil, false
			}
			// Increment our subject index.
			si += bn.prefixLen
		}
		if an := n.findChild(pivot(subject, si)); an != nil {
			n = *an
		} else {
			return nil, false
		}
	}
	return nil, false
}

// Delete will delete the item and return its value, or not found if it did not exist.
func (t *SubjectTree[T]) Delete(subject []byte) (*T, bool) {
	val, deleted := t.delete(&t.root, subject, 0)
	if deleted {
		t.size--
	}
	return val, deleted
}

// Match will match against a subject that can have wildcards and invoke the callback func for each matched value.
func (t *SubjectTree[T]) Match(filter []byte, cb func(subject []byte, val *T)) {
	if len(filter) == 0 || cb == nil {
		return
	}
	// We need to break this up into chunks based on wildcards, either pwc '*' or fwc '>'.
	var raw [16][]byte
	parts := genParts(filter, raw[:0])
	t.match(t.root, parts, nil, cb)
}

// Iter will walk all entries in the SubjectTree lexographically. The callback can return false to terminate the walk.
func (t *SubjectTree[T]) Iter(cb func(subject []byte, val *T) bool) {
	if t == nil || t.root == nil {
		return
	}
	t.iter(t.root, nil, cb)
}

// Internal methods

// Internal call to insert that can be recursive.
func (t *SubjectTree[T]) insert(np *node, subject []byte, value T, si int) (*T, bool) {
	n := *np
	if n == nil {
		*np = newLeaf(subject, value)
		return nil, false
	}
	if n.isLeaf() {
		ln := n.(*leaf[T])
		if ln.match(subject[si:]) {
			// Replace with new value.
			old := ln.value
			ln.value = value
			return &old, true
		}
		// Here we need to split this leaf.
		cpi := commonPrefixLen(ln.suffix, subject[si:])
		nn := newNode4(subject[si : si+cpi])
		ln.setSuffix(ln.suffix[cpi:])
		si += cpi
		// Make sure we have different pivot, normally this will be the case unless we have overflowing prefixes.
		if p := pivot(ln.suffix, 0); si < len(subject) && p == subject[si] {
			// We need to split the original leaf. Recursively call into insert.
			t.insert(np, subject, value, si)
			// Now add the update version of *np as a child to the new node4.
			nn.addChild(p, *np)
		} else {
			// Can just add this new leaf as a sibling.
			nl := newLeaf(subject[si:], value)
			nn.addChild(pivot(nl.suffix, 0), nl)
			// Add back original.
			nn.addChild(pivot(ln.suffix, 0), ln)
		}
		*np = nn
		return nil, false
	}

	// Non-leaf nodes.
	bn := n.base()
	if bn.prefixLen > 0 {
		cpi := commonPrefixLen(bn.prefix[:bn.prefixLen], subject[si:])
		if pli := int(bn.prefixLen); cpi >= pli {
			// Move past this node. We look for an existing child node to recurse into.
			// If one does not exist we can create a new leaf node.
			si += pli
			if nn := n.findChild(pivot(subject, si)); nn != nil {
				return t.insert(nn, subject, value, si)
			}
			if n.isFull() {
				n = n.grow()
				*np = n
			}
			n.addChild(pivot(subject, si), newLeaf(subject[si:], value))
			return nil, false
		} else {
			// We did not match the prefix completely here.
			// Calculate new prefix for this node.
			prefix := subject[si : si+cpi]
			si += len(prefix)
			// We will insert a new node4 and attach our current node below after adjusting prefix.
			nn := newNode4(prefix)
			// Shift the prefix for our original node.
			n.setPrefix(bn.prefix[cpi:bn.prefixLen])
			nn.addChild(pivot(bn.prefix[:], 0), n)
			// Add in our new leaf.
			nn.addChild(pivot(subject[si:], 0), newLeaf(subject[si:], value))
			// Update our node reference.
			*np = nn
		}
	} else {
		if nn := n.findChild(pivot(subject, si)); nn != nil {
			return t.insert(nn, subject, value, si)
		}
		// No prefix and no matched child, so add in new leafnode as needed.
		if n.isFull() {
			n = n.grow()
			*np = n
		}
		n.addChild(pivot(subject, si), newLeaf(subject[si:], value))
	}

	return nil, false
}

// internal function to recursively find the leaf to delete. Will do compaction if the item is found and removed.
func (t *SubjectTree[T]) delete(np *node, subject []byte, si uint16) (*T, bool) {
	if t == nil || np == nil || *np == nil || len(subject) == 0 {
		return nil, false
	}
	n := *np
	if n.isLeaf() {
		ln := n.(*leaf[T])
		if ln.match(subject[si:]) {
			*np = nil
			return &ln.value, true
		}
		return nil, false
	}
	// Not a leaf node.
	if bn := n.base(); bn.prefixLen > 0 {
		if !bytes.Equal(subject[si:si+bn.prefixLen], bn.prefix[:bn.prefixLen]) {
			return nil, false
		}
		// Increment our subject index.
		si += bn.prefixLen
	}
	p := pivot(subject, si)
	nna := n.findChild(p)
	if nna == nil {
		return nil, false
	}
	nn := *nna
	if nn.isLeaf() {
		ln := nn.(*leaf[T])
		if ln.match(subject[si:]) {
			n.deleteChild(p)

			if sn := n.shrink(); sn != nil {
				bn := n.base()
				// Make sure to set cap so we force an append to copy below.
				pre := bn.prefix[:bn.prefixLen:bn.prefixLen]
				// Need to fix up prefixes/suffixes.
				if sn.isLeaf() {
					ln := sn.(*leaf[T])
					// Make sure to set cap so we force an append to copy.
					ln.suffix = append(pre, ln.suffix...)
				} else {
					// We are a node here, we need to add in the old prefix.
					if len(pre) > 0 {
						bsn := sn.base()
						sn.setPrefix(append(pre, bsn.prefix[:bsn.prefixLen]...))
					}
				}
				*np = sn
			}

			return &ln.value, true
		}
		return nil, false
	}
	return t.delete(nna, subject, si)
}

// Internal function which can be called recursively to match all leaf nodes to a given filter subject which
// once here has been decomposed to parts. These parts only care about wildcards, both pwc and fwc.
func (t *SubjectTree[T]) match(n node, parts [][]byte, pre []byte, cb func(subject []byte, val *T)) {
	// We do not want to use the original pre so copy and append our prefix here.
	// Try to keep this from allocation.
	var _pre [256]byte
	// Updated our pre
	pre = append(_pre[:0], pre...)

	// Capture if we are sitting on a terminal fwc.
	var hasFWC bool
	if lp := len(parts); lp > 0 && parts[lp-1][0] == fwc {
		hasFWC = true
	}

	// Used to copy nparts if we recursively call into ourselves.
	// This is due to the fact that the low level matchParts can
	// update an individual part on a partial match.
	var cparts [64][]byte

	for n != nil {
		nparts, matched := n.matchParts(parts)
		// Check if we did not match.
		if !matched {
			return
		}
		// We have matched here. If we are a leaf and have exhausted all parts or he have a FWC fire callback.
		if n.isLeaf() {
			if len(nparts) == 0 || hasFWC {
				ln := n.(*leaf[T])
				cb(append(pre, ln.suffix...), &ln.value)
			}
			return
		}
		// We have normal nodes here.
		// We need to append our prefix
		bn := n.base()
		if bn.prefixLen > 0 {
			pre = append(pre, bn.prefix[:bn.prefixLen]...)
		}

		// Check our remaining parts.
		if len(nparts) == 0 && !hasFWC {
			// We are a node with no parts left and we are not looking at a fwc.
			// We could have a leafnode with no suffix which would be a match.
			// We could also have a terminal pwc. Check for those here.
			var hasTermPWC bool
			if lp := len(parts); lp > 0 && len(parts[lp-1]) == 1 && parts[lp-1][0] == pwc {
				// If we are sitting on a terminal pwc, put the pwc back and continue.
				nparts = parts[len(parts)-1:]
				hasTermPWC = true
			}
			n.iter(func(cn node) bool {
				if cn.isLeaf() {
					ln := cn.(*leaf[T])
					if len(ln.suffix) == 0 {
						cb(append(pre, ln.suffix...), &ln.value)
					} else if hasTermPWC && bytes.IndexByte(ln.suffix, tsep) < 0 {
						cb(append(pre, ln.suffix...), &ln.value)
					}
				} else if hasTermPWC {
					// We have terminal pwc so call into match again with the child node.
					t.match(cn, append(cparts[:0], nparts...), pre, cb)
				}
				return true
			})
			// Return regardless.
			return
		}
		// If we are sitting on a terminal fwc, put back and continue.
		if hasFWC && len(nparts) == 0 {
			nparts = parts[len(parts)-1:]
		}

		// Here we are a node type with a partial match.
		// Check if the first part is a wildcard.
		fp := nparts[0]
		p := pivot(fp, 0)
		// Check if we have a pwc/fwc part here. This will cause us to iterate.
		if len(fp) == 1 {
			if p == pwc {
				// We need to iterate over all children here for the current node
				// to see if we match further down.
				n.iter(func(cn node) bool {
					t.match(cn, append(cparts[:0], nparts...), pre, cb)
					return true
				})
			} else if p == fwc {
				n.iter(func(cn node) bool {
					t.match(cn, append(cparts[:0], nparts...), pre, cb)
					return true
				})
			}
		}
		// Here we have normal traversal, so find the next child.
		nn := n.findChild(p)
		if nn == nil {
			return
		}
		n, parts = *nn, nparts
	}
}

// Interal iter function to walk nodes in lexigraphical order.
func (t *SubjectTree[T]) iter(n node, pre []byte, cb func(subject []byte, val *T) bool) bool {
	// We do not want to use the original pre so copy and append our prefix here.
	// Try to keep this from allocation.
	var _pre [256]byte
	pre = append(_pre[:0], pre...)

	if n.isLeaf() {
		ln := n.(*leaf[T])
		return cb(append(pre, ln.suffix...), &ln.value)
	}
	// We are normal node here.
	bn := n.base()
	pre = append(pre, bn.prefix[:bn.prefixLen]...)
	// Collect nodes since unsorted.
	nodes := make([]node, 0, n.numChildren())
	n.iter(func(cn node) bool {
		nodes = append(nodes, cn)
		return true
	})
	// Now sort.
	sort.SliceStable(nodes, func(i, j int) bool { return bytes.Compare(nodes[i].path(), nodes[j].path()) < 0 })
	// Now walk the nodes in order and call into next iter.
	for _, cn := range nodes {
		if shouldContinue := t.iter(cn, pre, cb); !shouldContinue {
			return false
		}
	}
	return true
}
