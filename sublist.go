// Copyright 2012 Apcera Inc. All rights reserved.

package gnatsd

import (
	"sync"

	"github.com/apcera/gnatsd/hashmap"
)

type node struct {
	next *level
	subs []interface{}
}

type level struct {
	nodes    *hashmap.HashMap
	pwc, fwc *node
}

type Sublist struct {
	lck   sync.RWMutex
	root  *level
	count uint32
	cache *hashmap.HashMap
}

func newNode() *node {
	return &node{subs: make([]interface{}, 0, 4)}
}

func newLevel() *level {
	return &level{nodes: hashmap.New()}
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

	s.lck.Lock()
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
	// FIXME: Do something more intelligent here
	s.cache = hashmap.New()
	s.lck.Unlock()
}

func (s *Sublist) Match(subject []byte) []interface{} {

	s.lck.RLock()
	r := s.cache.Get(subject)
	s.lck.RUnlock()

	if r != nil {
		return r.([]interface{})
	}

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

	s.lck.Lock()
	matchLevel(s.root, toks, &results)
	s.cache.Set(subject, results)
	s.lck.Unlock()
	return results
}

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

type lnt struct {
	l *level
	n *node
	t []byte
}

func (s *Sublist) Remove(subject []byte, sub interface{}) {
	tsa := [16][]byte{}
	toks := split(subject, tsa[:0])

	s.lck.Lock()
	l := s.root
	var n *node

	var lnts [32]lnt
	levels := lnts[:0]

	for _, t := range toks {
		if l == nil {
			s.lck.Unlock()
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
		s.lck.Unlock()
		return
	}
	for i := len(levels) - 1; i >= 0; i-- {
		l, n, t := levels[i].l, levels[i].n, levels[i].t
		if n.isEmpty() {
			l.pruneNode(n, t)
		}
	}
	// FIXME: Do something more intelligent here
	s.cache = hashmap.New()
	s.lck.Unlock()
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

func (s *Sublist) Count() uint32 { return s.count }

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
