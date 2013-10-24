// Copyright 2012-2013 Apcera Inc. All rights reserved.

// Sublist is a subject distribution data structure that can match subjects to
// interested subscribers. Subscribers can have wildcard subjects to match
// multiple published subjects.
package sublist

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apcera/gnatsd/hash"
	"github.com/apcera/gnatsd/hashmap"
)

// A Sublist stores and efficiently retrieves subscriptions. It uses a
// tree structure and an efficient RR cache to achieve quick lookups.
type Sublist struct {
	mu    sync.RWMutex
	root  *level
	count uint32
	cache *hashmap.HashMap
	cmax  int
	stats stats
}

type stats struct {
	inserts   uint64
	removes   uint64
	matches   uint64
	cacheHits uint64
	since     time.Time
}

// A node contains subscriptions and a pointer to the next level.
type node struct {
	next *level
	subs []interface{}
}

// A level represents a group of nodes and special pointers to
// wildcard nodes.
type level struct {
	nodes    *hashmap.HashMap
	pwc, fwc *node
}

// Create a new default node.
func newNode() *node {
	return &node{subs: make([]interface{}, 0, 4)}
}

// Create a new default level. We use FNV1A as the hash
// algortihm for the tokens, which should be short.
func newLevel() *level {
	h := hashmap.New()
	h.Hash = hash.FNV1A
	return &level{nodes: h}
}

// defaultCacheMax is used to bound limit the frontend cache
const defaultCacheMax = 1024

// New will create a default sublist
func New() *Sublist {
	return &Sublist{
		root:  newLevel(),
		cache: hashmap.New(),
		cmax:  defaultCacheMax,
		stats: stats{since: time.Now()},
	}
}

// Common byte variables for wildcards and token separator.
var (
	_PWC = byte('*')
	_FWC = byte('>')
	_SEP = byte('.')
)

var (
	ErrInvalidSubject = errors.New("Invalid Subject")
	ErrNotFound       = errors.New("No Matches Found")
)

// split will split a subject into tokens
func split(subject []byte, tokens [][]byte) [][]byte {
	start := 0
	for i, b := range subject {
		if b == _SEP {
			tokens = append(tokens, subject[start:i])
			start = i + 1
		}
	}
	return append(tokens, subject[start:])
}

func (s *Sublist) Insert(subject []byte, sub interface{}) error {
	tsa := [16][]byte{}
	toks := split(subject, tsa[:0])

	s.mu.Lock()
	sfwc := false
	l := s.root
	var n *node

	for _, t := range toks {
		if len(t) == 0 || sfwc {
			s.mu.Unlock()
			return ErrInvalidSubject
		}
		switch t[0] {
		case _PWC:
			n = l.pwc
		case _FWC:
			n = l.fwc
			sfwc = true
		default:
			if v := l.nodes.Get(t); v == nil {
				n = nil
			} else {
				n = v.(*node)
			}
		}
		if n == nil {
			n = newNode()
			switch t[0] {
			case _PWC:
				l.pwc = n
			case _FWC:
				l.fwc = n
			default:
				l.nodes.Set(t, n)
			}
		}
		if n.next == nil {
			n.next = newLevel()
		}
		l = n.next
	}
	n.subs = append(n.subs, sub)
	s.count++
	s.stats.inserts++
	s.addToCache(subject, sub)
	s.mu.Unlock()
	return nil
}

// addToCache will add the new entry to existing cache
// entries if needed. Assumes write lock is held.
func (s *Sublist) addToCache(subject []byte, sub interface{}) {
	if s.cache.Count() == 0 {
		return
	}

	// FIXME(dlc) avoid allocation?
	all := s.cache.AllKeys()
	for _, k := range all {
		if !matchLiteral(k, subject) {
			continue
		}
		r := s.cache.Get(k)
		if r == nil {
			continue
		}
		res := r.([]interface{})
		res = append(res, sub)
		s.cache.Set(k, res)
	}
}

// removeFromCache will remove the sub from any active cache entries.
// Assumes write lock is held.
func (s *Sublist) removeFromCache(subject []byte, sub interface{}) {
	if s.cache.Count() == 0 {
		return
	}
	all := s.cache.AllKeys()
	for _, k := range all {
		if !matchLiteral(k, subject) {
			continue
		}
		// FIXME(dlc), right now just remove all matching cache
		// entries. This could be smarter and walk small result
		// lists and delete the individual sub.
		s.cache.Remove(k)
	}
}

// Match will match all entries to the literal subject. It will return a
// slice of results.
func (s *Sublist) Match(subject []byte) []interface{} {
	// Fastpath match on cache

	s.mu.RLock()
	atomic.AddUint64(&s.stats.matches, 1)
	r := s.cache.Get(subject)
	s.mu.RUnlock()

	if r != nil {
		atomic.AddUint64(&s.stats.cacheHits, 1)
		return r.([]interface{})
	}

	// Cache miss

	// Process subject into tokens, this is performed
	// unlocked, so can be parallel.
	tsa := [32][]byte{}
	toks := tsa[:0]

	start := 0
	for i, b := range subject {
		if b == _SEP {
			toks = append(toks, subject[start:i])
			start = i + 1
		}
	}
	toks = append(toks, subject[start:])
	results := make([]interface{}, 0, 4)

	// Lock the sublist and lookup and add entry to cache.
	s.mu.Lock()
	matchLevel(s.root, toks, &results)

	// We use random eviction to bound the size of the cache.
	// RR is used for speed purposes here.
	if int(s.cache.Count()) >= s.cmax {
		s.cache.RemoveRandom()
	}
	// Make sure we copy the subject key here
	scopy := make([]byte, len(subject))
	copy(scopy, subject)
	s.cache.Set(scopy, results)
	s.mu.Unlock()

	return results
}

// matchLevel is used to recursively descend into the trie when there
// is a cache miss.
func matchLevel(l *level, toks [][]byte, results *[]interface{}) {
	var pwc, n *node
	for i, t := range toks {
		if l == nil {
			return
		}
		if l.fwc != nil {
			*results = append(*results, l.fwc.subs...)
		}
		if pwc = l.pwc; pwc != nil {
			matchLevel(pwc.next, toks[i+1:], results)
		}
		if v := l.nodes.Get(t); v == nil {
			n = nil
		} else {
			n = v.(*node)
		}
		if n != nil {
			l = n.next
		} else {
			l = nil
		}
	}
	if n != nil {
		*results = append(*results, n.subs...)
	}
	if pwc != nil {
		*results = append(*results, pwc.subs...)
	}
	return
}

// lnt is used to track descent into a removal for pruning.
type lnt struct {
	l *level
	n *node
	t []byte
}

// Remove will remove any item associated with key. It will track descent
// into the trie and prune upon successful removal.
func (s *Sublist) Remove(subject []byte, sub interface{}) error {
	tsa := [16][]byte{}
	toks := split(subject, tsa[:0])

	s.mu.Lock()
	sfwc := false
	l := s.root
	var n *node

	var lnts [32]lnt
	levels := lnts[:0]

	for _, t := range toks {
		if len(t) == 0 || sfwc {
			s.mu.Unlock()
			return ErrInvalidSubject
		}
		if l == nil {
			s.mu.Unlock()
			return ErrNotFound
		}
		switch t[0] {
		case _PWC:
			n = l.pwc
		case _FWC:
			n = l.fwc
			sfwc = true
		default:
			if v := l.nodes.Get(t); v == nil {
				n = nil
			} else {
				n = v.(*node)
			}
		}
		if n != nil {
			levels = append(levels, lnt{l, n, t})
			l = n.next
		} else {
			l = nil
		}
	}
	if !s.removeFromNode(n, sub) {
		s.mu.Unlock()
		return ErrNotFound
	}

	s.count--
	s.stats.removes++

	for i := len(levels) - 1; i >= 0; i-- {
		l, n, t := levels[i].l, levels[i].n, levels[i].t
		if n.isEmpty() {
			l.pruneNode(n, t)
		}
	}
	s.removeFromCache(subject, sub)
	s.mu.Unlock()
	return nil
}

// pruneNode is used to prune and empty node from the tree.
func (l *level) pruneNode(n *node, t []byte) {
	if n == nil {
		return
	}
	if n == l.fwc {
		l.fwc = nil
	} else if n == l.pwc {
		l.pwc = nil
	} else {
		l.nodes.Remove(t)
	}
}

// isEmpty will test if the node has any entries. Used
// in pruning.
func (n *node) isEmpty() bool {
	if len(n.subs) == 0 {
		if n.next == nil || n.next.numNodes() == 0 {
			return true
		}
	}
	return false
}

// Return the number of nodes for the given level.
func (l *level) numNodes() uint32 {
	num := l.nodes.Count()
	if l.pwc != nil {
		num += 1
	}
	if l.fwc != nil {
		num += 1
	}
	return num
}

// Remove the sub for the given node.
func (s *Sublist) removeFromNode(n *node, sub interface{}) bool {
	if n == nil {
		return false
	}
	for i, v := range n.subs {
		if v == sub {
			num := len(n.subs)
			a := n.subs
			copy(a[i:num-1], a[i+1:num])
			n.subs = a[0 : num-1]
			return true
		}
	}
	return false
}

// matchLiteral is used to test literal subjects, those that do not have any
// wildcards, with a target subject. This is used in the cache layer.
func matchLiteral(literal, subject []byte) bool {
	li := 0
	for _, b := range subject {
		if li >= len(literal) {
			return false
		}
		switch b {
		case _PWC:
			// Skip token in literal
			ll := len(literal)
			for {
				if li >= ll || literal[li] == _SEP {
					li -= 1
					break
				}
				li += 1
			}
		case _FWC:
			return true
		default:
			if b != literal[li] {
				return false
			}
		}
		li += 1
	}
	return true
}

func IsValidLiteralSubject(subject []byte) bool {
	tsa := [16][]byte{}
	toks := split(subject, tsa[:0])

	for _, t := range toks {
		if len(t) == 0 {
			return false
		}
		if len(t) > 1 {
			continue
		}
		switch t[0] {
		case _PWC, _FWC:
			return false
		}
	}
	return true
}

// Count return the number of stored items in the HashMap.
func (s *Sublist) Count() uint32 { return s.count }

// Stats for the sublist
type Stats struct {
	NumSubs      uint32
	NumCache     uint32
	NumInserts   uint64
	NumRemoves   uint64
	NumMatches   uint64
	CacheHitRate float64
	MaxFanout    uint32
	AvgFanout    float64
	StatsTime    time.Time
}

// Stats will return a stats structure for the current state.
func (s *Sublist) Stats() *Stats {
	s.mu.Lock()
	defer s.mu.Unlock()

	st := &Stats{}
	st.NumSubs = s.count
	st.NumCache = s.cache.Count()
	st.NumInserts = s.stats.inserts
	st.NumRemoves = s.stats.removes
	st.NumMatches = s.stats.matches
	if s.stats.matches > 0 {
		st.CacheHitRate = float64(s.stats.cacheHits) / float64(s.stats.matches)
	}
	// whip through cache for fanout stats
	// FIXME, creating all each time could be expensive, should do a cb version.
	tot, max := 0, 0
	all := s.cache.All()
	for _, r := range all {
		l := len(r.([]interface{}))
		tot += l
		if l > max {
			max = l
		}
	}
	st.MaxFanout = uint32(max)
	st.AvgFanout = float64(tot) / float64(len(all))
	st.StatsTime = s.stats.since
	return st
}

// ResetStats will clear stats and update StatsTime to time.Now()
func (s *Sublist) ResetStats() {
	s.stats = stats{}
	s.stats.since = time.Now()
}

// numLevels will return the maximum number of levels
// contained in the Sublist tree.
func (s *Sublist) numLevels() int {
	return visitLevel(s.root, 0)
}

// visitLevel is used to descend the Sublist tree structure
// recursively.
func visitLevel(l *level, depth int) int {
	if l == nil || l.numNodes() == 0 {
		return depth
	}

	depth += 1
	maxDepth := depth

	all := l.nodes.All()
	for _, a := range all {
		if a == nil {
			continue
		}
		n := a.(*node)
		newDepth := visitLevel(n.next, depth)
		if newDepth > maxDepth {
			maxDepth = newDepth
		}
	}
	if l.pwc != nil {
		pwcDepth := visitLevel(l.pwc.next, depth)
		if pwcDepth > maxDepth {
			maxDepth = pwcDepth
		}
	}
	if l.fwc != nil {
		fwcDepth := visitLevel(l.fwc.next, depth)
		if fwcDepth > maxDepth {
			maxDepth = fwcDepth
		}
	}
	return maxDepth
}
