// Copyright 2012 Apcera Inc. All rights reserved.

package gnatsd

import (
	"sync"

	"github.com/apcera/gnatsd/hash"
	"github.com/apcera/gnatsd/hashmap"
)

// A Sublist stores and efficiently retrieves subscriptions. It uses a
// trie structure and an efficient LRU cache to achieve quick lookups.
type Sublist struct {
	mu    sync.RWMutex
	root  *level
	count uint32
	cache *hashmap.HashMap
}

type node struct {
	next *level
	subs []interface{}
}

type level struct {
	nodes    *hashmap.HashMap
	pwc, fwc *node
}

func newNode() *node {
	return &node{subs: make([]interface{}, 0, 4)}
}

func newLevel() *level {
	h := hashmap.New()
	h.Hash = hash.FNV1A
	return &level{nodes: h}
}

func New() *Sublist {
	return &Sublist{
		root:  newLevel(),
		cache: hashmap.New(),
	}
}

var (
	_PWC = byte('*')
	_FWC = byte('>')
	_SEP = byte('.')
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

func (s *Sublist) Insert(subject []byte, sub interface{}) {
	tsa := [16][]byte{}
	toks := split(subject, tsa[:0])

	s.mu.Lock()
	l := s.root
	var n *node

	for _, t := range toks {
		switch t[0] {
		case _PWC:
			n = l.pwc
		case _FWC:
			n = l.fwc
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
	s.addToCache(subject, sub)
	s.mu.Unlock()
}

// addToCache will add the new entry to existing cache
// entries if needed.
func (s *Sublist) addToCache(subject []byte, sub interface{}) {
	if s.cache.Count() == 0 {
		return
	}
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

// removeFromCache will remove the sub from any active cache entries
func (s *Sublist) removeFromCache(subject []byte, sub interface{}) {
	if s.cache.Count() == 0 {
		return
	}
	all := s.cache.AllKeys()
	for _, k := range all {
		if !matchLiteral(k, subject) {
			continue
		}
		r := s.cache.Get(k)
		if r == nil {
			continue
		}
		s.cache.Remove(k)
	}
}

// Match will match all entries to the literal subject. It will return a
// slice of results.
func (s *Sublist) Match(subject []byte) []interface{} {

	s.mu.RLock()
	r := s.cache.Get(subject)
	s.mu.RUnlock()

	if r != nil {
		return r.([] interface{})
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

	// Lookup and add entry to hash.
	s.mu.Lock()
	matchLevel(s.root, toks, &results)

	// FIXME: This can overwhelm memory, but can't find a fast enough solution.
	// LRU is too slow, LFU and time.Now() is also too slow. Can try PLRU or
	// possible 2-way, although 2-way also uses time. We could have a go routine
	// for getting time, then on a fetch we just &timeNow in an atomic way.
	s.cache.Set(subject, results)
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
func (s *Sublist) Remove(subject []byte, sub interface{}) {
	tsa := [16][]byte{}
	toks := split(subject, tsa[:0])

	s.mu.Lock()
	l := s.root
	var n *node

	var lnts [32]lnt
	levels := lnts[:0]

	for _, t := range toks {
		if l == nil {
			s.mu.Unlock()
			return
		}
		switch t[0] {
		case _PWC:
			n = l.pwc
		case _FWC:
			n = l.fwc
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
		return
	}
	for i := len(levels) - 1; i >= 0; i-- {
		l, n, t := levels[i].l, levels[i].n, levels[i].t
		if n.isEmpty() {
			l.pruneNode(n, t)
		}
	}
	s.removeFromCache(subject, sub)
	s.mu.Unlock()
}

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
			s.count--
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
	for _, b := range(subject) {
		if li >= len(literal) {
			return false
		}
		switch b {
		case _PWC:
			// Skip token
			for {
				if li >= len(literal) || literal[li] == _SEP {
					break;
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

// Count return the number of stored items in the HashMap.
func (s *Sublist) Count() uint32 { return s.count }

// DebugNumLevels will return the number of levels contained in
// the HashMap.
func (s *Sublist) DebugNumLevels() int {
	return visitLevel(s.root, 0)
}

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
