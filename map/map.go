// Copyright 2012 Apcera Inc. All rights reserved.

// HashMap defines a high performance hashmap based on
// fast hashing and fast key comparison. Simple chaining
// is used, relying on the hashing algorithms for good
// distribution
package hashmap

import (
	"bytes"
	"errors"
	"unsafe"

	"github.com/apcera/gnatsd/hash"
)

// Entry represents what the map is actually storing.
// Uses simple linked list resolution for collisions.
type Entry struct {
	hk   uint32
	key  []byte
	data interface{}
	next *Entry
}

// BucketSize, must be power of 2
const _BSZ = 8

// Constants for multiples of sizeof(WORD)
const (
	_WSZ  = 4         // 4
	_DWSZ = _WSZ << 1 // 8
)

// DefaultHash to be used unless overridden.
var DefaultHash = hash.Jesteress

// HashMap stores Entry items using a given Hash function.
// The Hash function can be overridden.
type HashMap struct {
	Hash func([]byte) uint32
	bkts []*Entry
	msk  uint32
	used uint32
	rsz  bool
}

// Stats are reported on HashMaps
type Stats struct {
	NumElements uint32
	NumSlots    uint32
	NumBuckets  uint32
	LongChain   uint32
	AvgChain    float32
}

// NewWithBkts creates a new HashMap using the bkts slice argument.
// len(bkts) must be a power of 2.
func NewWithBkts(bkts []*Entry) (*HashMap, error) {
	l := len(bkts)
	if l == 0 || (l&(l-1) != 0) {
		return nil, errors.New("Size of buckets must be power of 2")
	}
	h := HashMap{}
	h.msk = uint32(l - 1)
	h.bkts = bkts
	h.Hash = DefaultHash
	h.rsz = true
	return &h, nil
}

// New creates a new HashMap of default size and using the default
// Hashing algorithm.
func New() *HashMap {
	h, _ := NewWithBkts(make([]*Entry, _BSZ))
	return h
}

func (h *HashMap) Set(key []byte, data interface{}) {
	hk := h.Hash(key)
	ne := &Entry{hk: hk, key: key, data: data}
	ne.next = h.bkts[hk&h.msk]
	h.bkts[hk&h.msk] = ne
	h.used += 1
	// Check for resizing
	if h.rsz && (h.used > uint32(len(h.bkts))) {
		h.grow()
	}
}

func (h *HashMap) Get(key []byte) interface{} {
	hk := h.Hash(key)
	e := h.bkts[hk&h.msk]

	// FIXME: Reorder on GET if chained?
	for e != nil && len(key) == len(e.key) {
		// We unroll and optimize the key comparison here.
		klen := len(key)
		for i := 0; klen >= _DWSZ; klen -= _DWSZ {
			k1 := *(*uint64)(unsafe.Pointer(&key[i]))
			k2 := *(*uint64)(unsafe.Pointer(&e.key[i]))
			if k1 != k2 {
				goto next
			}
			i += _DWSZ
		}
		for i := 0; i < klen; i++ {
			if key[i] != e.key[i] {
				goto next
			}
		}
		// Success
		return e.data
	next:
		e = e.next
	}
	return nil
}

func (h *HashMap) Remove(key []byte) {
	hk := h.Hash(key)
	e := &h.bkts[hk&h.msk]
	for *e != nil {
		if len(key) == len((*e).key) && bytes.Equal(key, (*e).key) {
			// Success
			*e = (*e).next
			h.used -= 1
			// Check for resizing
			lbkts := uint32(len(h.bkts))
			if h.rsz && lbkts > _BSZ && (h.used < lbkts/4) {
				h.shrink()
			}
			return
		}
		e = &(*e).next
	}
}

// resize is responsible for reallocating the buckets and
// redistributing the hashmap entries.
func (h *HashMap) resize(nsz uint32) {
	nmsk := nsz - 1
	bkts := make([]*Entry, nsz)
	ents := make([]Entry, h.used)
	var ne *Entry
	var i int
	for _, e := range h.bkts {
		for ; e != nil; e = e.next {
			ne, i = &ents[i], i+1
			*ne = *e
			ne.next = bkts[e.hk&nmsk]
			bkts[e.hk&nmsk] = ne
		}
	}
	h.bkts = bkts
	h.msk = nmsk
}

// grow the HashMap's buckets by 2
func (h *HashMap) grow() {
	h.resize(uint32(2 * len(h.bkts)))
}

// shrink the HashMap's buckets by 2
func (h *HashMap) shrink() {
	h.resize(uint32(len(h.bkts) / 2))
}

// Stats will collect general statistics about the HashMap
func (h *HashMap) Stats() *Stats {
	lc, totalc, slots := 0, 0, 0
	for _, e := range h.bkts {
		if e != nil {
			slots += 1
		}
		i := 0
		for ; e != nil; e = e.next {
			i += 1
			if i > lc {
				lc = i
			}
		}
		totalc += i
	}
	l := uint32(len(h.bkts))
	avg := (float32(totalc) / float32(slots))
	return &Stats{
		NumElements: h.used,
		NumBuckets:  l,
		LongChain:   uint32(lc),
		AvgChain:    avg,
		NumSlots:    uint32(slots)}
}
