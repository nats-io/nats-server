// Copyright 2025 The NATS Authors
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

package gsl

import (
	"errors"
	"strings"
	"sync"
)

// Sublist is a routing mechanism to handle subject distribution and
// provides a facility to match subjects from published messages to
// interested subscribers. Subscribers can have wildcard subjects to
// match multiple published subjects.

// Common byte variables for wildcards and token separator.
const (
	pwc     = '*'
	pwcs    = "*"
	fwc     = '>'
	fwcs    = ">"
	tsep    = "."
	btsep   = '.'
	_EMPTY_ = ""
)

// Sublist related errors
var (
	ErrInvalidSubject    = errors.New("gsl: invalid subject")
	ErrNotFound          = errors.New("gsl: no matches found")
	ErrNilChan           = errors.New("gsl: nil channel")
	ErrAlreadyRegistered = errors.New("gsl: notification already registered")
)

// SimpleSublist is an alias type for GenericSublist that takes
// empty values, useful for tracking interest only without any
// unnecessary allocations.
type SimpleSublist = GenericSublist[struct{}]

// NewSimpleSublist will create a simple sublist.
func NewSimpleSublist() *SimpleSublist {
	return &GenericSublist[struct{}]{root: newLevel[struct{}]()}
}

// A GenericSublist stores and efficiently retrieves subscriptions.
type GenericSublist[T comparable] struct {
	sync.RWMutex
	root  *level[T]
	count uint32
}

// A node contains subscriptions and a pointer to the next level.
type node[T comparable] struct {
	next *level[T]
	subs map[T]string // value -> subject
}

// A level represents a group of nodes and special pointers to
// wildcard nodes.
type level[T comparable] struct {
	nodes    map[string]*node[T]
	pwc, fwc *node[T]
}

// Create a new default node.
func newNode[T comparable]() *node[T] {
	return &node[T]{subs: make(map[T]string)}
}

// Create a new default level.
func newLevel[T comparable]() *level[T] {
	return &level[T]{nodes: make(map[string]*node[T])}
}

// NewSublist will create a default sublist with caching enabled per the flag.
func NewSublist[T comparable]() *GenericSublist[T] {
	return &GenericSublist[T]{root: newLevel[T]()}
}

// Insert adds a subscription into the sublist
func (s *GenericSublist[T]) Insert(subject string, value T) error {
	s.Lock()

	var sfwc bool
	var n *node[T]
	l := s.root

	for t := range strings.SplitSeq(subject, tsep) {
		lt := len(t)
		if lt == 0 || sfwc {
			s.Unlock()
			return ErrInvalidSubject
		}

		if lt > 1 {
			n = l.nodes[t]
		} else {
			switch t[0] {
			case pwc:
				n = l.pwc
			case fwc:
				n = l.fwc
				sfwc = true
			default:
				n = l.nodes[t]
			}
		}
		if n == nil {
			n = newNode[T]()
			if lt > 1 {
				l.nodes[t] = n
			} else {
				switch t[0] {
				case pwc:
					l.pwc = n
				case fwc:
					l.fwc = n
				default:
					l.nodes[t] = n
				}
			}
		}
		if n.next == nil {
			n.next = newLevel[T]()
		}
		l = n.next
	}

	n.subs[value] = subject

	s.count++
	s.Unlock()

	return nil
}

// Match will match all entries to the literal subject.
// It will return a set of results for both normal and queue subscribers.
func (s *GenericSublist[T]) Match(subject string, cb func(T)) {
	s.match(subject, cb, true)
}

// MatchBytes will match all entries to the literal subject.
// It will return a set of results for both normal and queue subscribers.
func (s *GenericSublist[T]) MatchBytes(subject []byte, cb func(T)) {
	s.match(string(subject), cb, true)
}

// HasInterest will return whether or not there is any interest in the subject.
// In cases where more detail is not required, this may be faster than Match.
func (s *GenericSublist[T]) HasInterest(subject string) bool {
	return s.hasInterest(subject, true, nil)
}

// NumInterest will return the number of subs interested in the subject.
// In cases where more detail is not required, this may be faster than Match.
func (s *GenericSublist[T]) NumInterest(subject string) (np int) {
	s.hasInterest(subject, true, &np)
	return
}

func (s *GenericSublist[T]) match(subject string, cb func(T), doLock bool) {
	tsa := [32]string{}
	tokens := tsa[:0]
	start := 0
	for i := 0; i < len(subject); i++ {
		if subject[i] == btsep {
			if i-start == 0 {
				return
			}
			tokens = append(tokens, subject[start:i])
			start = i + 1
		}
	}
	if start >= len(subject) {
		return
	}
	tokens = append(tokens, subject[start:])

	if doLock {
		s.RLock()
		defer s.RUnlock()
	}
	matchLevel(s.root, tokens, cb)
}

func (s *GenericSublist[T]) hasInterest(subject string, doLock bool, np *int) bool {
	tsa := [32]string{}
	tokens := tsa[:0]
	start := 0
	for i := 0; i < len(subject); i++ {
		if subject[i] == btsep {
			if i-start == 0 {
				return false
			}
			tokens = append(tokens, subject[start:i])
			start = i + 1
		}
	}
	if start >= len(subject) {
		return false
	}
	tokens = append(tokens, subject[start:])

	if doLock {
		s.RLock()
		defer s.RUnlock()
	}
	return matchLevelForAny(s.root, tokens, np)
}

func matchLevelForAny[T comparable](l *level[T], toks []string, np *int) bool {
	var pwc, n *node[T]
	for i, t := range toks {
		if l == nil {
			return false
		}
		if l.fwc != nil {
			if np != nil {
				*np += len(l.fwc.subs)
			}
			return true
		}
		if pwc = l.pwc; pwc != nil {
			if match := matchLevelForAny(pwc.next, toks[i+1:], np); match {
				return true
			}
		}
		n = l.nodes[t]
		if n != nil {
			l = n.next
		} else {
			l = nil
		}
	}
	if n != nil {
		if np != nil {
			*np += len(n.subs)
		}
		if len(n.subs) > 0 {
			return true
		}
	}
	if pwc != nil {
		if np != nil {
			*np += len(pwc.subs)
		}
		return len(pwc.subs) > 0
	}
	return false
}

// callbacksForResults will make the necessary callbacks for each
// result in this node.
func callbacksForResults[T comparable](n *node[T], cb func(T)) {
	for sub := range n.subs {
		cb(sub)
	}
}

// matchLevel is used to recursively descend into the trie.
func matchLevel[T comparable](l *level[T], toks []string, cb func(T)) {
	var pwc, n *node[T]
	for i, t := range toks {
		if l == nil {
			return
		}
		if l.fwc != nil {
			callbacksForResults(l.fwc, cb)
		}
		if pwc = l.pwc; pwc != nil {
			matchLevel(pwc.next, toks[i+1:], cb)
		}
		n = l.nodes[t]
		if n != nil {
			l = n.next
		} else {
			l = nil
		}
	}
	if n != nil {
		callbacksForResults(n, cb)
	}
	if pwc != nil {
		callbacksForResults(pwc, cb)
	}
}

// lnt is used to track descent into levels for a removal for pruning.
type lnt[T comparable] struct {
	l *level[T]
	n *node[T]
	t string
}

// Raw low level remove, can do batches with lock held outside.
func (s *GenericSublist[T]) remove(subject string, value T, shouldLock bool) error {
	if shouldLock {
		s.Lock()
		defer s.Unlock()
	}

	var sfwc bool
	var n *node[T]
	l := s.root

	// Track levels for pruning
	var lnts [32]lnt[T]
	levels := lnts[:0]

	for t := range strings.SplitSeq(subject, tsep) {
		lt := len(t)
		if lt == 0 || sfwc {
			return ErrInvalidSubject
		}
		if l == nil {
			return ErrNotFound
		}
		if lt > 1 {
			n = l.nodes[t]
		} else {
			switch t[0] {
			case pwc:
				n = l.pwc
			case fwc:
				n = l.fwc
				sfwc = true
			default:
				n = l.nodes[t]
			}
		}
		if n != nil {
			levels = append(levels, lnt[T]{l, n, t})
			l = n.next
		} else {
			l = nil
		}
	}

	if !s.removeFromNode(n, value) {
		return ErrNotFound
	}

	s.count--

	for i := len(levels) - 1; i >= 0; i-- {
		l, n, t := levels[i].l, levels[i].n, levels[i].t
		if n.isEmpty() {
			l.pruneNode(n, t)
		}
	}

	return nil
}

// Remove will remove a subscription.
func (s *GenericSublist[T]) Remove(subject string, value T) error {
	return s.remove(subject, value, true)
}

// HasInterestStartingIn is a helper for subject tree intersection.
func (s *GenericSublist[T]) HasInterestStartingIn(subj string) bool {
	s.RLock()
	defer s.RUnlock()
	var _tokens [64]string
	tokens := tokenizeSubjectIntoSlice(_tokens[:0], subj)
	return hasInterestStartingIn(s.root, tokens)
}

func hasInterestStartingIn[T comparable](l *level[T], tokens []string) bool {
	if l == nil {
		return false
	}
	if len(tokens) == 0 {
		return true
	}
	token := tokens[0]
	if l.fwc != nil {
		return true
	}
	found := false
	if pwc := l.pwc; pwc != nil {
		found = found || hasInterestStartingIn(pwc.next, tokens[1:])
	}
	if n := l.nodes[token]; n != nil {
		found = found || hasInterestStartingIn(n.next, tokens[1:])
	}
	return found
}

// pruneNode is used to prune an empty node from the tree.
func (l *level[T]) pruneNode(n *node[T], t string) {
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
func (n *node[T]) isEmpty() bool {
	return len(n.subs) == 0 && (n.next == nil || n.next.numNodes() == 0)
}

// Return the number of nodes for the given level.
func (l *level[T]) numNodes() int {
	if l == nil {
		return 0
	}
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
func (s *GenericSublist[T]) removeFromNode(n *node[T], value T) (found bool) {
	if n == nil {
		return false
	}
	if _, found = n.subs[value]; found {
		delete(n.subs, value)
	}
	return found
}

// Count returns the number of subscriptions.
func (s *GenericSublist[T]) Count() uint32 {
	s.RLock()
	defer s.RUnlock()
	return s.count
}

// numLevels will return the maximum number of levels
// contained in the Sublist tree.
func (s *GenericSublist[T]) numLevels() int {
	return visitLevel(s.root, 0)
}

// visitLevel is used to descend the Sublist tree structure
// recursively.
func visitLevel[T comparable](l *level[T], depth int) int {
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

// use similar to append. meaning, the updated slice will be returned
func tokenizeSubjectIntoSlice(tts []string, subject string) []string {
	start := 0
	for i := 0; i < len(subject); i++ {
		if subject[i] == btsep {
			tts = append(tts, subject[start:i])
			start = i + 1
		}
	}
	tts = append(tts, subject[start:])
	return tts
}
