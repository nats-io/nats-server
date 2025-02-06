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

package avl

import (
	"cmp"
	"encoding/binary"
	"errors"
	"math/bits"
	"slices"
)

// SequenceSet is a memory and encoding optimized set for storing unsigned ints.
//
// SequenceSet is ~80-100 times more efficient memory wise than a map[uint64]struct{}.
// SequenceSet is ~1.75 times slower at inserts than the same map.
// SequenceSet is not thread safe.
//
// We use an AVL tree with nodes that hold bitmasks for set membership.
//
// Encoding will convert to a space optimized encoding using bitmasks.
type SequenceSet struct {
	root  *node // root node
	size  int   // number of items
	nodes int   // number of nodes
	// Having this here vs on the stack in Insert/Delete
	// makes a difference in memory usage.
	changed bool
}

// Insert will insert the sequence into the set.
// The tree will be balanced inline.
func (ss *SequenceSet) Insert(seq uint64) {
	if ss.root = ss.root.insert(seq, &ss.changed, &ss.nodes); ss.changed {
		ss.changed = false
		ss.size++
	}
}

// Exists will return true iff the sequence is a member of this set.
func (ss *SequenceSet) Exists(seq uint64) bool {
	for n := ss.root; n != nil; {
		if seq < n.base {
			n = n.l
			continue
		} else if seq >= n.base+numEntries {
			n = n.r
			continue
		}
		return n.exists(seq)
	}
	return false
}

// SetInitialMin should be used to set the initial minimum sequence when known.
// This will more effectively utilize space versus self selecting.
// The set should be empty.
func (ss *SequenceSet) SetInitialMin(min uint64) error {
	if !ss.IsEmpty() {
		return ErrSetNotEmpty
	}
	ss.root, ss.nodes = &node{base: min, h: 1}, 1
	return nil
}

// Delete will remove the sequence from the set.
// Will optionally remove nodes and rebalance.
// Returns where the sequence was set.
func (ss *SequenceSet) Delete(seq uint64) bool {
	if ss == nil || ss.root == nil {
		return false
	}
	ss.root = ss.root.delete(seq, &ss.changed, &ss.nodes)
	if ss.changed {
		ss.changed = false
		ss.size--
		if ss.size == 0 {
			ss.Empty()
		}
		return true
	}
	return false
}

// Size returns the number of items in the set.
func (ss *SequenceSet) Size() int {
	return ss.size
}

// Nodes returns the number of nodes in the tree.
func (ss *SequenceSet) Nodes() int {
	return ss.nodes
}

// Empty will clear all items from a set.
func (ss *SequenceSet) Empty() {
	ss.root = nil
	ss.size = 0
	ss.nodes = 0
}

// IsEmpty is a fast check of the set being empty.
func (ss *SequenceSet) IsEmpty() bool {
	if ss == nil || ss.root == nil {
		return true
	}
	return false
}

// Range will invoke the given function for each item in the set.
// They will range over the set in ascending order.
// If the callback returns false we terminate the iteration.
func (ss *SequenceSet) Range(f func(uint64) bool) {
	ss.root.iter(f)
}

// Heights returns the left and right heights of the tree.
func (ss *SequenceSet) Heights() (l, r int) {
	if ss.root == nil {
		return 0, 0
	}
	if ss.root.l != nil {
		l = ss.root.l.h
	}
	if ss.root.r != nil {
		r = ss.root.r.h
	}
	return l, r
}

// Returns min, max and number of set items.
func (ss *SequenceSet) State() (min, max, num uint64) {
	if ss == nil || ss.root == nil {
		return 0, 0, 0
	}
	min, max = ss.MinMax()
	return min, max, uint64(ss.Size())
}

// MinMax will return the minunum and maximum values in the set.
func (ss *SequenceSet) MinMax() (min, max uint64) {
	if ss.root == nil {
		return 0, 0
	}
	for l := ss.root; l != nil; l = l.l {
		if l.l == nil {
			min = l.min()
		}
	}
	for r := ss.root; r != nil; r = r.r {
		if r.r == nil {
			max = r.max()
		}
	}
	return min, max
}

func clone(src *node, target **node) {
	if src == nil {
		return
	}
	n := &node{base: src.base, bits: src.bits, h: src.h}
	*target = n
	clone(src.l, &n.l)
	clone(src.r, &n.r)
}

// Clone will return a clone of the given SequenceSet.
func (ss *SequenceSet) Clone() *SequenceSet {
	if ss == nil {
		return nil
	}
	css := &SequenceSet{nodes: ss.nodes, size: ss.size}
	clone(ss.root, &css.root)

	return css
}

// Union will union this SequenceSet with ssa.
func (ss *SequenceSet) Union(ssa ...*SequenceSet) {
	for _, sa := range ssa {
		sa.root.nodeIter(func(n *node) {
			for nb, b := range n.bits {
				for pos := uint64(0); b != 0; pos++ {
					if b&1 == 1 {
						seq := n.base + (uint64(nb) * uint64(bitsPerBucket)) + pos
						ss.Insert(seq)
					}
					b >>= 1
				}
			}
		})
	}
}

// Union will return a union of all sets.
func Union(ssa ...*SequenceSet) *SequenceSet {
	if len(ssa) == 0 {
		return nil
	}
	// Sort so we can clone largest.
	slices.SortFunc(ssa, func(i, j *SequenceSet) int { return -cmp.Compare(i.Size(), j.Size()) }) // reverse order
	ss := ssa[0].Clone()

	// Insert the rest through range call.
	for i := 1; i < len(ssa); i++ {
		ssa[i].Range(func(n uint64) bool {
			ss.Insert(n)
			return true
		})
	}
	return ss
}

const (
	// Magic is used to identify the encode binary state..
	magic = uint8(22)
	// Version
	version = uint8(2)
	// hdrLen
	hdrLen = 2
	// minimum length of an encoded SequenceSet.
	minLen = 2 + 8 // magic + version + num nodes + num entries.
)

// EncodeLen returns the bytes needed for encoding.
func (ss SequenceSet) EncodeLen() int {
	return minLen + (ss.Nodes() * ((numBuckets+1)*8 + 2))
}

func (ss SequenceSet) Encode(buf []byte) ([]byte, error) {
	nn, encLen := ss.Nodes(), ss.EncodeLen()

	if cap(buf) < encLen {
		buf = make([]byte, encLen)
	} else {
		buf = buf[:encLen]
	}

	// TODO(dlc) - Go 1.19 introduced Append to not have to keep track.
	// Once 1.20 is out we could change this over.
	// Also binary.Write() is way slower, do not use.

	var le = binary.LittleEndian
	buf[0], buf[1] = magic, version
	i := hdrLen
	le.PutUint32(buf[i:], uint32(nn))
	le.PutUint32(buf[i+4:], uint32(ss.size))
	i += 8
	ss.root.nodeIter(func(n *node) {
		le.PutUint64(buf[i:], n.base)
		i += 8
		for _, b := range n.bits {
			le.PutUint64(buf[i:], b)
			i += 8
		}
		le.PutUint16(buf[i:], uint16(n.h))
		i += 2
	})
	return buf[:i], nil
}

// ErrBadEncoding is returned when we can not decode properly.
var (
	ErrBadEncoding = errors.New("ss: bad encoding")
	ErrBadVersion  = errors.New("ss: bad version")
	ErrSetNotEmpty = errors.New("ss: set not empty")
)

// Decode returns the sequence set and number of bytes read from the buffer on success.
func Decode(buf []byte) (*SequenceSet, int, error) {
	if len(buf) < minLen || buf[0] != magic {
		return nil, -1, ErrBadEncoding
	}

	switch v := buf[1]; v {
	case 1:
		return decodev1(buf)
	case 2:
		return decodev2(buf)
	default:
		return nil, -1, ErrBadVersion
	}
}

// Helper to decode v2.
func decodev2(buf []byte) (*SequenceSet, int, error) {
	var le = binary.LittleEndian
	index := 2
	nn := int(le.Uint32(buf[index:]))
	sz := int(le.Uint32(buf[index+4:]))
	index += 8

	expectedLen := minLen + (nn * ((numBuckets+1)*8 + 2))
	if len(buf) < expectedLen {
		return nil, -1, ErrBadEncoding
	}

	ss, nodes := SequenceSet{size: sz}, make([]node, nn)

	for i := 0; i < nn; i++ {
		n := &nodes[i]
		n.base = le.Uint64(buf[index:])
		index += 8
		for bi := range n.bits {
			n.bits[bi] = le.Uint64(buf[index:])
			index += 8
		}
		n.h = int(le.Uint16(buf[index:]))
		index += 2
		ss.insertNode(n)
	}

	return &ss, index, nil
}

// Helper to decode v1 into v2 which has fixed buckets of 32 vs 64 originally.
func decodev1(buf []byte) (*SequenceSet, int, error) {
	var le = binary.LittleEndian
	index := 2
	nn := int(le.Uint32(buf[index:]))
	sz := int(le.Uint32(buf[index+4:]))
	index += 8

	const v1NumBuckets = 64

	expectedLen := minLen + (nn * ((v1NumBuckets+1)*8 + 2))
	if len(buf) < expectedLen {
		return nil, -1, ErrBadEncoding
	}

	var ss SequenceSet
	for i := 0; i < nn; i++ {
		base := le.Uint64(buf[index:])
		index += 8
		for nb := uint64(0); nb < v1NumBuckets; nb++ {
			n := le.Uint64(buf[index:])
			// Walk all set bits and insert sequences manually for this decode from v1.
			for pos := uint64(0); n != 0; pos++ {
				if n&1 == 1 {
					seq := base + (nb * uint64(bitsPerBucket)) + pos
					ss.Insert(seq)
				}
				n >>= 1
			}
			index += 8
		}
		// Skip over encoded height.
		index += 2
	}

	// Sanity check.
	if ss.Size() != sz {
		return nil, -1, ErrBadEncoding
	}

	return &ss, index, nil

}

// insertNode places a decoded node into the tree.
// These should be done in tree order as defined by Encode()
// This allows us to not have to calculate height or do rebalancing.
// So much better performance this way.
func (ss *SequenceSet) insertNode(n *node) {
	ss.nodes++

	if ss.root == nil {
		ss.root = n
		return
	}
	// Walk our way to the insertion point.
	for p := ss.root; p != nil; {
		if n.base < p.base {
			if p.l == nil {
				p.l = n
				return
			}
			p = p.l
		} else {
			if p.r == nil {
				p.r = n
				return
			}
			p = p.r
		}
	}
}

const (
	bitsPerBucket = 64 // bits in uint64
	numBuckets    = 32
	numEntries    = numBuckets * bitsPerBucket
)

type node struct {
	//v dvalue
	base uint64
	bits [numBuckets]uint64
	l    *node
	r    *node
	h    int
}

// Set the proper bit.
// seq should have already been qualified and inserted should be non nil.
func (n *node) set(seq uint64, inserted *bool) {
	seq -= n.base
	i := seq / bitsPerBucket
	mask := uint64(1) << (seq % bitsPerBucket)
	if (n.bits[i] & mask) == 0 {
		n.bits[i] |= mask
		*inserted = true
	}
}

func (n *node) insert(seq uint64, inserted *bool, nodes *int) *node {
	if n == nil {
		base := (seq / numEntries) * numEntries
		n := &node{base: base, h: 1}
		n.set(seq, inserted)
		*nodes++
		return n
	}

	if seq < n.base {
		n.l = n.l.insert(seq, inserted, nodes)
	} else if seq >= n.base+numEntries {
		n.r = n.r.insert(seq, inserted, nodes)
	} else {
		n.set(seq, inserted)
	}

	n.h = maxH(n) + 1

	// Don't make a function, impacts performance.
	if bf := balanceF(n); bf > 1 {
		// Left unbalanced.
		if balanceF(n.l) < 0 {
			n.l = n.l.rotateL()
		}
		return n.rotateR()
	} else if bf < -1 {
		// Right unbalanced.
		if balanceF(n.r) > 0 {
			n.r = n.r.rotateR()
		}
		return n.rotateL()
	}
	return n
}

func (n *node) rotateL() *node {
	r := n.r
	if r != nil {
		n.r = r.l
		r.l = n
		n.h = maxH(n) + 1
		r.h = maxH(r) + 1
	} else {
		n.r = nil
		n.h = maxH(n) + 1
	}
	return r
}

func (n *node) rotateR() *node {
	l := n.l
	if l != nil {
		n.l = l.r
		l.r = n
		n.h = maxH(n) + 1
		l.h = maxH(l) + 1
	} else {
		n.l = nil
		n.h = maxH(n) + 1
	}
	return l
}

func balanceF(n *node) int {
	if n == nil {
		return 0
	}
	var lh, rh int
	if n.l != nil {
		lh = n.l.h
	}
	if n.r != nil {
		rh = n.r.h
	}
	return lh - rh
}

func maxH(n *node) int {
	if n == nil {
		return 0
	}
	var lh, rh int
	if n.l != nil {
		lh = n.l.h
	}
	if n.r != nil {
		rh = n.r.h
	}
	if lh > rh {
		return lh
	}
	return rh
}

// Clear the proper bit.
// seq should have already been qualified and deleted should be non nil.
// Will return true if this node is now empty.
func (n *node) clear(seq uint64, deleted *bool) bool {
	seq -= n.base
	i := seq / bitsPerBucket
	mask := uint64(1) << (seq % bitsPerBucket)
	if (n.bits[i] & mask) != 0 {
		n.bits[i] &^= mask
		*deleted = true
	}
	for _, b := range n.bits {
		if b != 0 {
			return false
		}
	}
	return true
}

func (n *node) delete(seq uint64, deleted *bool, nodes *int) *node {
	if n == nil {
		return nil
	}

	if seq < n.base {
		n.l = n.l.delete(seq, deleted, nodes)
	} else if seq >= n.base+numEntries {
		n.r = n.r.delete(seq, deleted, nodes)
	} else if empty := n.clear(seq, deleted); empty {
		*nodes--
		if n.l == nil {
			n = n.r
		} else if n.r == nil {
			n = n.l
		} else {
			// We have both children.
			n.r = n.r.insertNodePrev(n.l)
			n = n.r
		}
	}

	if n != nil {
		n.h = maxH(n) + 1
	}

	// Check balance.
	if bf := balanceF(n); bf > 1 {
		// Left unbalanced.
		if balanceF(n.l) < 0 {
			n.l = n.l.rotateL()
		}
		return n.rotateR()
	} else if bf < -1 {
		// right unbalanced.
		if balanceF(n.r) > 0 {
			n.r = n.r.rotateR()
		}
		return n.rotateL()
	}

	return n
}

// Will insert nn into the node assuming it is less than all other nodes in n.
// Will re-calculate height and balance.
func (n *node) insertNodePrev(nn *node) *node {
	if n.l == nil {
		n.l = nn
	} else {
		n.l = n.l.insertNodePrev(nn)
	}
	n.h = maxH(n) + 1

	// Check balance.
	if bf := balanceF(n); bf > 1 {
		// Left unbalanced.
		if balanceF(n.l) < 0 {
			n.l = n.l.rotateL()
		}
		return n.rotateR()
	} else if bf < -1 {
		// right unbalanced.
		if balanceF(n.r) > 0 {
			n.r = n.r.rotateR()
		}
		return n.rotateL()
	}
	return n
}

func (n *node) exists(seq uint64) bool {
	seq -= n.base
	i := seq / bitsPerBucket
	mask := uint64(1) << (seq % bitsPerBucket)
	return n.bits[i]&mask != 0
}

// Return minimum sequence in the set.
// This node can not be empty.
func (n *node) min() uint64 {
	for i, b := range n.bits {
		if b != 0 {
			return n.base +
				uint64(i*bitsPerBucket) +
				uint64(bits.TrailingZeros64(b))
		}
	}
	return 0
}

// Return maximum sequence in the set.
// This node can not be empty.
func (n *node) max() uint64 {
	for i := numBuckets - 1; i >= 0; i-- {
		if b := n.bits[i]; b != 0 {
			return n.base +
				uint64(i*bitsPerBucket) +
				uint64(bitsPerBucket-bits.LeadingZeros64(b>>1))
		}
	}
	return 0
}

// This is done in tree order.
func (n *node) nodeIter(f func(n *node)) {
	if n == nil {
		return
	}
	f(n)
	n.l.nodeIter(f)
	n.r.nodeIter(f)
}

// iter will iterate through the set's items in this node.
// If the supplied function returns false we terminate the iteration.
func (n *node) iter(f func(uint64) bool) bool {
	if n == nil {
		return true
	}

	if ok := n.l.iter(f); !ok {
		return false
	}
	for num := n.base; num < n.base+numEntries; num++ {
		if n.exists(num) {
			if ok := f(num); !ok {
				return false
			}
		}
	}
	if ok := n.r.iter(f); !ok {
		return false
	}

	return true
}
