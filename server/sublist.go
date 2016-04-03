// Copyright 2016 Apcera Inc. All rights reserved.

// Package sublist is a routing mechanism to handle subject distribution
// and provides a facility to match subjects from published messages to
// interested subscribers. Subscribers can have wildcard subjects to match
// multiple published subjects.
package server

import (
	"errors"
	"strings"
	"sync"
	"sync/atomic"
)

// Common byte variables for wildcards and token separator.
const (
	pwc   = '*'
	fwc   = '>'
	tsep  = "."
	btsep = '.'
)

// Sublist related errors
var (
	ErrInvalidSubject = errors.New("sublist: Invalid Subject")
	ErrNotFound       = errors.New("sublist: No Matches Found")
)

// cacheMax is used to bound limit the frontend cache
const slCacheMax = 1024

// A Sublist stores and efficiently retrieves subscriptions.
type Sublist struct {
	sync.RWMutex
	genid     uint64
	matches   uint64
	cacheHits uint64
	inserts   uint64
	removes   uint64
	cache     map[string][]*subscription
	root      *level
	count     uint32
}

// A node contains subscriptions and a pointer to the next level.
type node struct {
	next *level
	subs []*subscription
}

// A level represents a group of nodes and special pointers to
// wildcard nodes.
type level struct {
	nodes    map[string]*node
	pwc, fwc *node
}

// Create a new default node.
func newNode() *node {
	return &node{subs: make([]*subscription, 0, 4)}
}

// Create a new default level. We use FNV1A as the hash
// algortihm for the tokens, which should be short.
func newLevel() *level {
	return &level{nodes: make(map[string]*node)}
}

// New will create a default sublist
func NewSublist() *Sublist {
	return &Sublist{root: newLevel(), cache: make(map[string][]*subscription)}
}

// Insert adds a subscription into the sublist
func (s *Sublist) Insert(sub *subscription) error {
	// copy the subject since we hold this and this might be part of a large byte slice.
	subject := string(append([]byte(nil), sub.subject...))
	tsa := [32]string{}
	tokens := tsa[:0]
	start := 0
	for i := 0; i < len(subject); i++ {
		if subject[i] == btsep {
			tokens = append(tokens, subject[start:i])
			start = i + 1
		}
	}
	tokens = append(tokens, subject[start:])

	s.Lock()

	sfwc := false
	l := s.root
	var n *node

	for _, t := range tokens {
		if len(t) == 0 || sfwc {
			s.Unlock()
			return ErrInvalidSubject
		}

		switch t[0] {
		case pwc:
			n = l.pwc
		case fwc:
			n = l.fwc
			sfwc = true
		default:
			n = l.nodes[t]
		}
		if n == nil {
			n = newNode()
			switch t[0] {
			case pwc:
				l.pwc = n
			case fwc:
				l.fwc = n
			default:
				l.nodes[t] = n
			}
		}
		if n.next == nil {
			n.next = newLevel()
		}
		l = n.next
	}
	n.subs = append(n.subs, sub)
	s.count++
	s.inserts++

	s.addToCache(subject, sub)
	atomic.AddUint64(&s.genid, 1)

	s.Unlock()
	return nil
}

// addToCache will add the new entry to existing cache
// entries if needed. Assumes write lock is held.
func (s *Sublist) addToCache(subject string, sub *subscription) {
	for k, results := range s.cache {
		if matchLiteral(k, subject) {
			// Copy since others may have a reference.
			nr := make([]*subscription, len(results), len(results)+1)
			copy(nr, results)
			s.cache[k] = append(nr, sub)
		}
	}
}

// removeFromCache will remove the sub from any active cache entries.
// Assumes write lock is held.
func (s *Sublist) removeFromCache(subject string, sub *subscription) {
	for k, _ := range s.cache {
		if !matchLiteral(k, subject) {
			continue
		}
		// Since someone else may be referecing, can't modify the list
		// safely, just let it re-populate.
		delete(s.cache, k)
	}
}

// Match will match all entries to the literal subject.
// It will return a slice of results.
// Note that queue subscribers will only have one member selected
// and returned for each queue group.
func (s *Sublist) Match(subject string) []*subscription {
	s.RLock()
	atomic.AddUint64(&s.matches, 1)
	rc, ok := s.cache[subject]
	s.RUnlock()
	if ok {
		atomic.AddUint64(&s.cacheHits, 1)
		return rc
	}

	tsa := [32]string{}
	tokens := tsa[:0]
	start := 0
	for i := 0; i < len(subject); i++ {
		if subject[i] == btsep {
			tokens = append(tokens, subject[start:i])
			start = i + 1
		}
	}
	tokens = append(tokens, subject[start:])

	// FIXME(dlc) - Make pool?
	results := []*subscription{}

	s.RLock()
	results = matchLevel(s.root, tokens, results)
	s.RUnlock()

	s.Lock()
	// Add to our cache
	s.cache[subject] = results
	// Bound the number of entries to sublistMaxCache
	if len(s.cache) > slCacheMax {
		for k, _ := range s.cache {
			delete(s.cache, k)
			break
		}
	}
	s.Unlock()

	return results
}

// matchLevel is used to recursively descend into the trie.
func matchLevel(l *level, toks []string, results []*subscription) []*subscription {
	var pwc, n *node
	for i, t := range toks {
		if l == nil {
			return results
		}
		if l.fwc != nil {
			results = append(results, l.fwc.subs...)
		}
		if pwc = l.pwc; pwc != nil {
			results = matchLevel(pwc.next, toks[i+1:], results)
		}
		n = l.nodes[t]
		if n != nil {
			l = n.next
		} else {
			l = nil
		}
	}
	if n != nil {
		results = append(results, n.subs...)
	}
	if pwc != nil {
		results = append(results, pwc.subs...)
	}
	return results
}

// lnt is used to track descent into levels for a removal for pruning.
type lnt struct {
	l *level
	n *node
	t string
}

// Remove will remove a subscription.
func (s *Sublist) Remove(sub *subscription) error {
	subject := string(sub.subject)
	tsa := [32]string{}
	tokens := tsa[:0]
	start := 0
	for i := 0; i < len(subject); i++ {
		if subject[i] == btsep {
			tokens = append(tokens, subject[start:i])
			start = i + 1
		}
	}
	tokens = append(tokens, subject[start:])

	s.Lock()
	defer s.Unlock()

	sfwc := false
	l := s.root
	var n *node

	// Track levels for pruning
	var lnts [32]lnt
	levels := lnts[:0]

	for _, t := range tokens {
		if len(t) == 0 || sfwc {
			return ErrInvalidSubject
		}
		if l == nil {
			return ErrNotFound
		}
		switch t[0] {
		case pwc:
			n = l.pwc
		case fwc:
			n = l.fwc
			sfwc = true
		default:
			n = l.nodes[t]
		}
		if n != nil {
			levels = append(levels, lnt{l, n, t})
			l = n.next
		} else {
			l = nil
		}
	}
	if !s.removeFromNode(n, sub) {
		return ErrNotFound
	}

	s.count--
	s.removes++

	for i := len(levels) - 1; i >= 0; i-- {
		l, n, t := levels[i].l, levels[i].n, levels[i].t
		if n.isEmpty() {
			l.pruneNode(n, t)
		}
	}
	s.removeFromCache(subject, sub)
	atomic.AddUint64(&s.genid, 1)

	return nil
}

// pruneNode is used to prune an empty node from the tree.
func (l *level) pruneNode(n *node, t string) {
	if n == nil {
		return
	}
	if n == l.fwc {
		l.fwc = nil
	} else if n == l.pwc {
		l.pwc = nil
	} else {
		delete(l.nodes, t)
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
func (l *level) numNodes() int {
	num := len(l.nodes)
	if l.pwc != nil {
		num++
	}
	if l.fwc != nil {
		num++
	}
	return num
}

// Remove the sub for the given node.
func (s *Sublist) removeFromNode(n *node, sub *subscription) bool {
	if n == nil {
		return false
	}
	sl := n.subs
	for i := 0; i < len(sl); i++ {
		if sl[i] == sub {
			sl[i] = sl[len(sl)-1]
			sl[len(sl)-1] = nil
			sl = sl[:len(sl)-1]
			n.subs = shrinkAsNeeded(sl)
			return true
		}
	}
	return false
}

// Checks if we need to do a resize. This is for very large growth then
// subsequent return to a more normal size from unsubscribe.
func shrinkAsNeeded(sl []*subscription) []*subscription {
	lsl := len(sl)
	csl := cap(sl)
	// Don't bother if list not too big
	if csl <= 8 {
		return sl
	}
	pFree := float32(csl-lsl) / float32(csl)
	if pFree > 0.50 {
		return append([]*subscription(nil), sl...)
	}
	return sl
}

// Count returns the number of subscriptions.
func (s *Sublist) Count() uint32 {
	s.RLock()
	defer s.RUnlock()
	return s.count
}

// CacheCount returns the number of result sets in the cache.
func (s *Sublist) CacheCount() int {
	s.RLock()
	defer s.RUnlock()
	return len(s.cache)
}

// Public stats for the sublist
type SublistStats struct {
	NumSubs      uint32  `json:"num_subscriptions"`
	NumCache     uint32  `json:"num_cache"`
	NumInserts   uint64  `json:"num_inserts"`
	NumRemoves   uint64  `json:"num_removes"`
	NumMatches   uint64  `json:"num_matches"`
	CacheHitRate float64 `json:"cache_hit_rate"`
	MaxFanout    uint32  `json:"max_fanout"`
	AvgFanout    float64 `json:"avg_fanout"`
}

// Stats will return a stats structure for the current state.
func (s *Sublist) Stats() *SublistStats {
	s.Lock()
	defer s.Unlock()

	st := &SublistStats{}
	st.NumSubs = s.count
	st.NumCache = uint32(len(s.cache))
	st.NumInserts = s.inserts
	st.NumRemoves = s.removes
	st.NumMatches = s.matches
	if s.matches > 0 {
		st.CacheHitRate = float64(s.cacheHits) / float64(s.matches)
	}
	// whip through cache for fanout stats
	tot, max := 0, 0
	for _, results := range s.cache {
		l := len(results)
		tot += l
		if l > max {
			max = l
		}
	}
	st.MaxFanout = uint32(max)
	if tot > 0 {
		st.AvgFanout = float64(tot) / float64(len(s.cache))
	}
	return st
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

	depth++
	maxDepth := depth

	for _, n := range l.nodes {
		if n == nil {
			continue
		}
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

// IsValidLiteralSubject returns true if a subject is valid, false otherwise
func IsValidLiteralSubject(subject string) bool {
	tokens := strings.Split(string(subject), tsep)
	for _, t := range tokens {
		if len(t) == 0 {
			return false
		}
		if len(t) > 1 {
			continue
		}
		switch t[0] {
		case pwc, fwc:
			return false
		}
	}
	return true
}

// matchLiteral is used to test literal subjects, those that do not have any
// wildcards, with a target subject. This is used in the cache layer.
func matchLiteral(literal, subject string) bool {
	li := 0
	ll := len(literal)
	for i := 0; i < len(subject); i++ {
		if li >= ll {
			return false
		}
		b := subject[i]
		switch b {
		case pwc:
			// Skip token in literal
			ll := len(literal)
			for {
				if li >= ll || literal[li] == btsep {
					li--
					break
				}
				li++
			}
		case fwc:
			return true
		default:
			if b != literal[li] {
				return false
			}
		}
		li++
	}
	// Make sure we have processed all of the literal's chars..
	if li < ll {
		return false
	}
	return true
}
