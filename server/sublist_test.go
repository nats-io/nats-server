// Copyright 2016-2018 The NATS Authors
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

package server

import (
	"fmt"
	"runtime"
	"strings"
	"sync"
	"testing"

	dbg "runtime/debug"
)

func stackFatalf(t *testing.T, f string, args ...interface{}) {
	lines := make([]string, 0, 32)
	msg := fmt.Sprintf(f, args...)
	lines = append(lines, msg)

	// Generate the Stack of callers: Skip us and verify* frames.
	for i := 2; true; i++ {
		_, file, line, ok := runtime.Caller(i)
		if !ok {
			break
		}
		msg := fmt.Sprintf("%d - %s:%d", i, file, line)
		lines = append(lines, msg)
	}
	t.Fatalf("%s", strings.Join(lines, "\n"))
}

func verifyCount(s *Sublist, count uint32, t *testing.T) {
	if s.Count() != count {
		stackFatalf(t, "Count is %d, should be %d", s.Count(), count)
	}
}

func verifyLen(r []*subscription, l int, t *testing.T) {
	if len(r) != l {
		stackFatalf(t, "Results len is %d, should be %d", len(r), l)
	}
}

func verifyQLen(r [][]*subscription, l int, t *testing.T) {
	if len(r) != l {
		stackFatalf(t, "Queue Results len is %d, should be %d", len(r), l)
	}
}

func verifyNumLevels(s *Sublist, expected int, t *testing.T) {
	dl := s.numLevels()
	if dl != expected {
		stackFatalf(t, "NumLevels is %d, should be %d", dl, expected)
	}
}

func verifyQMember(qsubs [][]*subscription, val *subscription, t *testing.T) {
	verifyMember(qsubs[findQSliceForSub(val, qsubs)], val, t)
}

func verifyMember(r []*subscription, val *subscription, t *testing.T) {
	for _, v := range r {
		if v == nil {
			continue
		}
		if v == val {
			return
		}
	}
	stackFatalf(t, "Subscription (%p) for [%s : %s] not found in results", val, val.subject, val.queue)
}

// Helpers to generate test subscriptions.
func newSub(subject string) *subscription {
	return &subscription{subject: []byte(subject)}
}

func newQSub(subject, queue string) *subscription {
	if queue != "" {
		return &subscription{subject: []byte(subject), queue: []byte(queue)}
	}
	return newSub(subject)
}

func TestSublistInit(t *testing.T) {
	s := NewSublist()
	verifyCount(s, 0, t)
}

func TestSublistInsertCount(t *testing.T) {
	s := NewSublist()
	s.Insert(newSub("foo"))
	s.Insert(newSub("bar"))
	s.Insert(newSub("foo.bar"))
	verifyCount(s, 3, t)
}

func TestSublistSimple(t *testing.T) {
	s := NewSublist()
	subject := "foo"
	sub := newSub(subject)
	s.Insert(sub)
	r := s.Match(subject)
	verifyLen(r.psubs, 1, t)
	verifyMember(r.psubs, sub, t)
}

func TestSublistSimpleMultiTokens(t *testing.T) {
	s := NewSublist()
	subject := "foo.bar.baz"
	sub := newSub(subject)
	s.Insert(sub)
	r := s.Match(subject)
	verifyLen(r.psubs, 1, t)
	verifyMember(r.psubs, sub, t)
}

func TestSublistPartialWildcard(t *testing.T) {
	s := NewSublist()
	lsub := newSub("a.b.c")
	psub := newSub("a.*.c")
	s.Insert(lsub)
	s.Insert(psub)
	r := s.Match("a.b.c")
	verifyLen(r.psubs, 2, t)
	verifyMember(r.psubs, lsub, t)
	verifyMember(r.psubs, psub, t)
}

func TestSublistPartialWildcardAtEnd(t *testing.T) {
	s := NewSublist()
	lsub := newSub("a.b.c")
	psub := newSub("a.b.*")
	s.Insert(lsub)
	s.Insert(psub)
	r := s.Match("a.b.c")
	verifyLen(r.psubs, 2, t)
	verifyMember(r.psubs, lsub, t)
	verifyMember(r.psubs, psub, t)
}

func TestSublistFullWildcard(t *testing.T) {
	s := NewSublist()
	lsub := newSub("a.b.c")
	fsub := newSub("a.>")
	s.Insert(lsub)
	s.Insert(fsub)
	r := s.Match("a.b.c")
	verifyLen(r.psubs, 2, t)
	verifyMember(r.psubs, lsub, t)
	verifyMember(r.psubs, fsub, t)
}

func TestSublistRemove(t *testing.T) {
	s := NewSublist()
	subject := "a.b.c.d"
	sub := newSub(subject)
	s.Insert(sub)
	verifyCount(s, 1, t)
	r := s.Match(subject)
	verifyLen(r.psubs, 1, t)
	s.Remove(newSub("a.b.c"))
	verifyCount(s, 1, t)
	s.Remove(sub)
	verifyCount(s, 0, t)
	r = s.Match(subject)
	verifyLen(r.psubs, 0, t)
}

func TestSublistRemoveWildcard(t *testing.T) {
	s := NewSublist()
	subject := "a.b.c.d"
	sub := newSub(subject)
	psub := newSub("a.b.*.d")
	fsub := newSub("a.b.>")
	s.Insert(sub)
	s.Insert(psub)
	s.Insert(fsub)
	verifyCount(s, 3, t)
	r := s.Match(subject)
	verifyLen(r.psubs, 3, t)
	s.Remove(sub)
	verifyCount(s, 2, t)
	s.Remove(fsub)
	verifyCount(s, 1, t)
	s.Remove(psub)
	verifyCount(s, 0, t)
	r = s.Match(subject)
	verifyLen(r.psubs, 0, t)
}

func TestSublistRemoveCleanup(t *testing.T) {
	s := NewSublist()
	literal := "a.b.c.d.e.f"
	depth := len(strings.Split(literal, tsep))
	sub := newSub(literal)
	verifyNumLevels(s, 0, t)
	s.Insert(sub)
	verifyNumLevels(s, depth, t)
	s.Remove(sub)
	verifyNumLevels(s, 0, t)
}

func TestSublistRemoveCleanupWildcards(t *testing.T) {
	s := NewSublist()
	subject := "a.b.*.d.e.>"
	depth := len(strings.Split(subject, tsep))
	sub := newSub(subject)
	verifyNumLevels(s, 0, t)
	s.Insert(sub)
	verifyNumLevels(s, depth, t)
	s.Remove(sub)
	verifyNumLevels(s, 0, t)
}

func TestSublistRemoveWithLargeSubs(t *testing.T) {
	subject := "foo"
	s := NewSublist()
	for i := 0; i < plistMin*2; i++ {
		sub := newSub(subject)
		s.Insert(sub)
	}
	r := s.Match(subject)
	verifyLen(r.psubs, plistMin*2, t)
	// Remove one that is in the middle
	s.Remove(r.psubs[plistMin])
	// Remove first one
	s.Remove(r.psubs[0])
	// Remove last one
	s.Remove(r.psubs[len(r.psubs)-1])
	// Check len again
	r = s.Match(subject)
	verifyLen(r.psubs, plistMin*2-3, t)
}

func TestSublistInvalidSubjectsInsert(t *testing.T) {
	s := NewSublist()

	// Insert, or subscriptions, can have wildcards, but not empty tokens,
	// and can not have a FWC that is not the terminal token.

	// beginning empty token
	if err := s.Insert(newSub(".foo")); err != ErrInvalidSubject {
		t.Fatal("Expected invalid subject error")
	}

	// trailing empty token
	if err := s.Insert(newSub("foo.")); err != ErrInvalidSubject {
		t.Fatal("Expected invalid subject error")
	}
	// empty middle token
	if err := s.Insert(newSub("foo..bar")); err != ErrInvalidSubject {
		t.Fatal("Expected invalid subject error")
	}
	// empty middle token #2
	if err := s.Insert(newSub("foo.bar..baz")); err != ErrInvalidSubject {
		t.Fatal("Expected invalid subject error")
	}
	// fwc not terminal
	if err := s.Insert(newSub("foo.>.bar")); err != ErrInvalidSubject {
		t.Fatal("Expected invalid subject error")
	}
}

func TestSublistCache(t *testing.T) {
	s := NewSublist()

	// Test add a remove logistics
	subject := "a.b.c.d"
	sub := newSub(subject)
	psub := newSub("a.b.*.d")
	fsub := newSub("a.b.>")
	s.Insert(sub)
	r := s.Match(subject)
	verifyLen(r.psubs, 1, t)
	s.Insert(psub)
	s.Insert(fsub)
	verifyCount(s, 3, t)
	r = s.Match(subject)
	verifyLen(r.psubs, 3, t)
	s.Remove(sub)
	verifyCount(s, 2, t)
	s.Remove(fsub)
	verifyCount(s, 1, t)
	s.Remove(psub)
	verifyCount(s, 0, t)

	// Check that cache is now empty
	if cc := s.CacheCount(); cc != 0 {
		t.Fatalf("Cache should be zero, got %d\n", cc)
	}

	r = s.Match(subject)
	verifyLen(r.psubs, 0, t)

	for i := 0; i < 2*slCacheMax; i++ {
		s.Match(fmt.Sprintf("foo-%d\n", i))
	}

	if cc := s.CacheCount(); cc > slCacheMax {
		t.Fatalf("Cache should be constrained by cacheMax, got %d for current count\n", cc)
	}
}

func TestSublistBasicQueueResults(t *testing.T) {
	s := NewSublist()

	// Test some basics
	subject := "foo"
	sub := newSub(subject)
	sub1 := newQSub(subject, "bar")
	sub2 := newQSub(subject, "baz")

	s.Insert(sub1)
	r := s.Match(subject)
	verifyLen(r.psubs, 0, t)
	verifyQLen(r.qsubs, 1, t)
	verifyLen(r.qsubs[0], 1, t)
	verifyQMember(r.qsubs, sub1, t)

	s.Insert(sub2)
	r = s.Match(subject)
	verifyLen(r.psubs, 0, t)
	verifyQLen(r.qsubs, 2, t)
	verifyLen(r.qsubs[0], 1, t)
	verifyLen(r.qsubs[1], 1, t)
	verifyQMember(r.qsubs, sub1, t)
	verifyQMember(r.qsubs, sub2, t)

	s.Insert(sub)
	r = s.Match(subject)
	verifyLen(r.psubs, 1, t)
	verifyQLen(r.qsubs, 2, t)
	verifyLen(r.qsubs[0], 1, t)
	verifyLen(r.qsubs[1], 1, t)
	verifyQMember(r.qsubs, sub1, t)
	verifyQMember(r.qsubs, sub2, t)
	verifyMember(r.psubs, sub, t)

	sub3 := newQSub(subject, "bar")
	sub4 := newQSub(subject, "baz")

	s.Insert(sub3)
	s.Insert(sub4)

	r = s.Match(subject)
	verifyLen(r.psubs, 1, t)
	verifyQLen(r.qsubs, 2, t)
	verifyLen(r.qsubs[0], 2, t)
	verifyLen(r.qsubs[1], 2, t)
	verifyQMember(r.qsubs, sub1, t)
	verifyQMember(r.qsubs, sub2, t)
	verifyQMember(r.qsubs, sub3, t)
	verifyQMember(r.qsubs, sub4, t)
	verifyMember(r.psubs, sub, t)

	// Now removal
	s.Remove(sub)

	r = s.Match(subject)
	verifyLen(r.psubs, 0, t)
	verifyQLen(r.qsubs, 2, t)
	verifyLen(r.qsubs[0], 2, t)
	verifyLen(r.qsubs[1], 2, t)
	verifyQMember(r.qsubs, sub1, t)
	verifyQMember(r.qsubs, sub2, t)

	s.Remove(sub1)
	r = s.Match(subject)
	verifyLen(r.psubs, 0, t)
	verifyQLen(r.qsubs, 2, t)
	verifyLen(r.qsubs[findQSliceForSub(sub1, r.qsubs)], 1, t)
	verifyLen(r.qsubs[findQSliceForSub(sub2, r.qsubs)], 2, t)
	verifyQMember(r.qsubs, sub2, t)
	verifyQMember(r.qsubs, sub3, t)
	verifyQMember(r.qsubs, sub4, t)

	s.Remove(sub3) // Last one
	r = s.Match(subject)
	verifyLen(r.psubs, 0, t)
	verifyQLen(r.qsubs, 1, t)
	verifyLen(r.qsubs[0], 2, t) // this is sub2/baz now
	verifyQMember(r.qsubs, sub2, t)

	s.Remove(sub2)
	s.Remove(sub4)
	r = s.Match(subject)
	verifyLen(r.psubs, 0, t)
	verifyQLen(r.qsubs, 0, t)
}

func checkBool(b, expected bool, t *testing.T) {
	if b != expected {
		dbg.PrintStack()
		t.Fatalf("Expected %v, but got %v\n", expected, b)
	}
}

func TestSublistValidLiteralSubjects(t *testing.T) {
	checkBool(IsValidLiteralSubject("foo"), true, t)
	checkBool(IsValidLiteralSubject(".foo"), false, t)
	checkBool(IsValidLiteralSubject("foo."), false, t)
	checkBool(IsValidLiteralSubject("foo..bar"), false, t)
	checkBool(IsValidLiteralSubject("foo.bar.*"), false, t)
	checkBool(IsValidLiteralSubject("foo.bar.>"), false, t)
	checkBool(IsValidLiteralSubject("*"), false, t)
	checkBool(IsValidLiteralSubject(">"), false, t)
	// The followings have widlcards characters but are not
	// considered as such because they are not individual tokens.
	checkBool(IsValidLiteralSubject("foo*"), true, t)
	checkBool(IsValidLiteralSubject("foo**"), true, t)
	checkBool(IsValidLiteralSubject("foo.**"), true, t)
	checkBool(IsValidLiteralSubject("foo*bar"), true, t)
	checkBool(IsValidLiteralSubject("foo.*bar"), true, t)
	checkBool(IsValidLiteralSubject("foo*.bar"), true, t)
	checkBool(IsValidLiteralSubject("*bar"), true, t)
	checkBool(IsValidLiteralSubject("foo>"), true, t)
	checkBool(IsValidLiteralSubject("foo>>"), true, t)
	checkBool(IsValidLiteralSubject("foo.>>"), true, t)
	checkBool(IsValidLiteralSubject("foo>bar"), true, t)
	checkBool(IsValidLiteralSubject("foo.>bar"), true, t)
	checkBool(IsValidLiteralSubject("foo>.bar"), true, t)
	checkBool(IsValidLiteralSubject(">bar"), true, t)
}

func TestSublistValidlSubjects(t *testing.T) {
	checkBool(IsValidSubject("."), false, t)
	checkBool(IsValidSubject(".foo"), false, t)
	checkBool(IsValidSubject("foo."), false, t)
	checkBool(IsValidSubject("foo..bar"), false, t)
	checkBool(IsValidSubject(">.bar"), false, t)
	checkBool(IsValidSubject("foo.>.bar"), false, t)
	checkBool(IsValidSubject("foo"), true, t)
	checkBool(IsValidSubject("foo.bar.*"), true, t)
	checkBool(IsValidSubject("foo.bar.>"), true, t)
	checkBool(IsValidSubject("*"), true, t)
	checkBool(IsValidSubject(">"), true, t)
	checkBool(IsValidSubject("foo*"), true, t)
	checkBool(IsValidSubject("foo**"), true, t)
	checkBool(IsValidSubject("foo.**"), true, t)
	checkBool(IsValidSubject("foo*bar"), true, t)
	checkBool(IsValidSubject("foo.*bar"), true, t)
	checkBool(IsValidSubject("foo*.bar"), true, t)
	checkBool(IsValidSubject("*bar"), true, t)
	checkBool(IsValidSubject("foo>"), true, t)
	checkBool(IsValidSubject("foo.>>"), true, t)
	checkBool(IsValidSubject("foo>bar"), true, t)
	checkBool(IsValidSubject("foo.>bar"), true, t)
	checkBool(IsValidSubject("foo>.bar"), true, t)
	checkBool(IsValidSubject(">bar"), true, t)
}

func TestSublistMatchLiterals(t *testing.T) {
	checkBool(matchLiteral("foo", "foo"), true, t)
	checkBool(matchLiteral("foo", "bar"), false, t)
	checkBool(matchLiteral("foo", "*"), true, t)
	checkBool(matchLiteral("foo", ">"), true, t)
	checkBool(matchLiteral("foo.bar", ">"), true, t)
	checkBool(matchLiteral("foo.bar", "foo.>"), true, t)
	checkBool(matchLiteral("foo.bar", "bar.>"), false, t)
	checkBool(matchLiteral("stats.test.22", "stats.>"), true, t)
	checkBool(matchLiteral("stats.test.22", "stats.*.*"), true, t)
	checkBool(matchLiteral("foo.bar", "foo"), false, t)
	checkBool(matchLiteral("stats.test.foos", "stats.test.foos"), true, t)
	checkBool(matchLiteral("stats.test.foos", "stats.test.foo"), false, t)
	checkBool(matchLiteral("stats.test", "stats.test.*"), false, t)
	checkBool(matchLiteral("stats.test.foos", "stats.*"), false, t)
	checkBool(matchLiteral("stats.test.foos", "stats.*.*.foos"), false, t)

	// These are cases where wildcards characters should not be considered
	// wildcards since they do not follow the rules of wildcards.
	checkBool(matchLiteral("*bar", "*bar"), true, t)
	checkBool(matchLiteral("foo*", "foo*"), true, t)
	checkBool(matchLiteral("foo*bar", "foo*bar"), true, t)
	checkBool(matchLiteral("foo.***.bar", "foo.***.bar"), true, t)
	checkBool(matchLiteral(">bar", ">bar"), true, t)
	checkBool(matchLiteral("foo>", "foo>"), true, t)
	checkBool(matchLiteral("foo>bar", "foo>bar"), true, t)
	checkBool(matchLiteral("foo.>>>.bar", "foo.>>>.bar"), true, t)
}

func TestSublistBadSubjectOnRemove(t *testing.T) {
	bad := "a.b..d"
	sub := newSub(bad)

	s := NewSublist()
	if err := s.Insert(sub); err != ErrInvalidSubject {
		t.Fatalf("Expected ErrInvalidSubject, got %v\n", err)
	}

	if err := s.Remove(sub); err != ErrInvalidSubject {
		t.Fatalf("Expected ErrInvalidSubject, got %v\n", err)
	}

	badfwc := "a.>.b"
	if err := s.Remove(newSub(badfwc)); err != ErrInvalidSubject {
		t.Fatalf("Expected ErrInvalidSubject, got %v\n", err)
	}
}

// This is from bug report #18
func TestSublistTwoTokenPubMatchSingleTokenSub(t *testing.T) {
	s := NewSublist()
	sub := newSub("foo")
	s.Insert(sub)
	r := s.Match("foo")
	verifyLen(r.psubs, 1, t)
	verifyMember(r.psubs, sub, t)
	r = s.Match("foo.bar")
	verifyLen(r.psubs, 0, t)
}

func TestSublistInsertWithWildcardsAsLiterals(t *testing.T) {
	s := NewSublist()
	subjects := []string{"foo.*-", "foo.>-"}
	for _, subject := range subjects {
		sub := newSub(subject)
		s.Insert(sub)
		// Should find no match
		r := s.Match("foo.bar")
		verifyLen(r.psubs, 0, t)
		// Should find a match
		r = s.Match(subject)
		verifyLen(r.psubs, 1, t)
	}
}

func TestSublistRemoveWithWildcardsAsLiterals(t *testing.T) {
	s := NewSublist()
	subjects := []string{"foo.*-", "foo.>-"}
	for _, subject := range subjects {
		sub := newSub(subject)
		s.Insert(sub)
		// Should find no match
		rsub := newSub("foo.bar")
		s.Remove(rsub)
		if c := s.Count(); c != 1 {
			t.Fatalf("Expected sublist to still contain sub, got %v", c)
		}
		s.Remove(sub)
		if c := s.Count(); c != 0 {
			t.Fatalf("Expected sublist to be empty, got %v", c)
		}
	}
}

func TestSublistRaceOnRemove(t *testing.T) {
	s := NewSublist()

	var (
		total = 100
		subs  = make(map[int]*subscription, total) // use map for randomness
	)
	for i := 0; i < total; i++ {
		sub := newQSub("foo", "bar")
		subs[i] = sub
	}

	for i := 0; i < 2; i++ {
		for _, sub := range subs {
			s.Insert(sub)
		}
		// Call Match() once or twice, to make sure we get from cache
		if i == 1 {
			s.Match("foo")
		}
		// This will be from cache when i==1
		r := s.Match("foo")
		wg := sync.WaitGroup{}
		wg.Add(1)
		go func() {
			for _, sub := range subs {
				s.Remove(sub)
			}
			wg.Done()
		}()
		for _, qsub := range r.qsubs {
			for i := 0; i < len(qsub); i++ {
				sub := qsub[i]
				if string(sub.queue) != "bar" {
					t.Fatalf("Queue name should be bar, got %s", qsub[i].queue)
				}
			}
		}
		wg.Wait()
	}

	// Repeat tests with regular subs
	for i := 0; i < total; i++ {
		sub := newSub("foo")
		subs[i] = sub
	}

	for i := 0; i < 2; i++ {
		for _, sub := range subs {
			s.Insert(sub)
		}
		// Call Match() once or twice, to make sure we get from cache
		if i == 1 {
			s.Match("foo")
		}
		// This will be from cache when i==1
		r := s.Match("foo")
		wg := sync.WaitGroup{}
		wg.Add(1)
		go func() {
			for _, sub := range subs {
				s.Remove(sub)
			}
			wg.Done()
		}()
		for i := 0; i < len(r.psubs); i++ {
			sub := r.psubs[i]
			if string(sub.subject) != "foo" {
				t.Fatalf("Subject should be foo, got %s", sub.subject)
			}
		}
		wg.Wait()
	}
}

func TestSublistRaceOnInsert(t *testing.T) {
	s := NewSublist()

	var (
		total = 100
		subs  = make(map[int]*subscription, total) // use map for randomness
		wg    sync.WaitGroup
	)
	for i := 0; i < total; i++ {
		sub := newQSub("foo", "bar")
		subs[i] = sub
	}
	wg.Add(1)
	go func() {
		for _, sub := range subs {
			s.Insert(sub)
		}
		wg.Done()
	}()
	for i := 0; i < 1000; i++ {
		r := s.Match("foo")
		for _, qsubs := range r.qsubs {
			for _, qsub := range qsubs {
				if string(qsub.queue) != "bar" {
					t.Fatalf("Expected queue name to be bar, got %v", string(qsub.queue))
				}
			}
		}
	}
	wg.Wait()

	// Repeat the test with plain subs
	for i := 0; i < total; i++ {
		sub := newSub("foo")
		subs[i] = sub
	}
	wg.Add(1)
	go func() {
		for _, sub := range subs {
			s.Insert(sub)
		}
		wg.Done()
	}()
	for i := 0; i < 1000; i++ {
		r := s.Match("foo")
		for _, sub := range r.psubs {
			if string(sub.subject) != "foo" {
				t.Fatalf("Expected subject to be foo, got %v", string(sub.subject))
			}
		}
	}
	wg.Wait()
}

func TestSublistRaceOnMatch(t *testing.T) {
	s := NewSublist()
	s.Insert(newQSub("foo.*", "workers"))
	s.Insert(newQSub("foo.bar", "workers"))
	s.Insert(newSub("foo.*"))
	s.Insert(newSub("foo.bar"))

	wg := sync.WaitGroup{}
	wg.Add(2)
	errCh := make(chan error, 2)
	f := func() {
		defer wg.Done()
		for i := 0; i < 10; i++ {
			r := s.Match("foo.bar")
			for _, sub := range r.psubs {
				if !strings.HasPrefix(string(sub.subject), "foo.") {
					errCh <- fmt.Errorf("Wrong subject: %s", sub.subject)
					return
				}
			}
			for _, qsub := range r.qsubs {
				for _, sub := range qsub {
					if string(sub.queue) != "workers" {
						errCh <- fmt.Errorf("Wrong queue name: %s", sub.queue)
						return
					}
				}
			}
			// Empty cache to maximize chance for race
			s.Lock()
			delete(s.cache, "foo.bar")
			s.Unlock()
		}
	}
	go f()
	go f()
	wg.Wait()
	select {
	case e := <-errCh:
		t.Fatalf(e.Error())
	default:
	}
}

// -- Benchmarks Setup --

var subs []*subscription
var toks = []string{"apcera", "continuum", "component", "router", "api", "imgr", "jmgr", "auth"}
var sl = NewSublist()

func init() {
	subs = make([]*subscription, 0, 256*1024)
	subsInit("")
	for i := 0; i < len(subs); i++ {
		sl.Insert(subs[i])
	}
	addWildcards()
}

func subsInit(pre string) {
	var sub string
	for _, t := range toks {
		if len(pre) > 0 {
			sub = pre + tsep + t
		} else {
			sub = t
		}
		subs = append(subs, newSub(sub))
		if len(strings.Split(sub, tsep)) < 5 {
			subsInit(sub)
		}
	}
}

func addWildcards() {
	sl.Insert(newSub("cloud.>"))
	sl.Insert(newSub("cloud.continuum.component.>"))
	sl.Insert(newSub("cloud.*.*.router.*"))
}

// -- Benchmarks Setup End --

func Benchmark______________________SublistInsert(b *testing.B) {
	s := NewSublist()
	for i, l := 0, len(subs); i < b.N; i++ {
		index := i % l
		s.Insert(subs[index])
	}
}

func Benchmark____________SublistMatchSingleToken(b *testing.B) {
	for i := 0; i < b.N; i++ {
		sl.Match("apcera")
	}
}

func Benchmark______________SublistMatchTwoTokens(b *testing.B) {
	for i := 0; i < b.N; i++ {
		sl.Match("apcera.continuum")
	}
}

func Benchmark____________SublistMatchThreeTokens(b *testing.B) {
	for i := 0; i < b.N; i++ {
		sl.Match("apcera.continuum.component")
	}
}

func Benchmark_____________SublistMatchFourTokens(b *testing.B) {
	for i := 0; i < b.N; i++ {
		sl.Match("apcera.continuum.component.router")
	}
}

func Benchmark_SublistMatchFourTokensSingleResult(b *testing.B) {
	for i := 0; i < b.N; i++ {
		sl.Match("apcera.continuum.component.router")
	}
}

func Benchmark_SublistMatchFourTokensMultiResults(b *testing.B) {
	for i := 0; i < b.N; i++ {
		sl.Match("cloud.continuum.component.router")
	}
}

func Benchmark_______SublistMissOnLastTokenOfFive(b *testing.B) {
	for i := 0; i < b.N; i++ {
		sl.Match("apcera.continuum.component.router.ZZZZ")
	}
}

func multiRead(b *testing.B, num int) {
	b.StopTimer()
	var swg, fwg sync.WaitGroup
	swg.Add(num)
	fwg.Add(num)
	s := "apcera.continuum.component.router"
	for i := 0; i < num; i++ {
		go func() {
			swg.Done()
			swg.Wait()
			for i := 0; i < b.N; i++ {
				sl.Match(s)
			}
			fwg.Done()
		}()
	}
	swg.Wait()
	b.StartTimer()
	fwg.Wait()
}

func Benchmark____________Sublist10XMultipleReads(b *testing.B) {
	multiRead(b, 10)
}

func Benchmark___________Sublist100XMultipleReads(b *testing.B) {
	multiRead(b, 100)
}

func Benchmark__________Sublist1000XMultipleReads(b *testing.B) {
	multiRead(b, 1000)
}

func Benchmark________________SublistMatchLiteral(b *testing.B) {
	b.StopTimer()
	cachedSubj := "foo.foo.foo.foo.foo.foo.foo.foo.foo.foo"
	subjects := []string{
		"foo.foo.foo.foo.foo.foo.foo.foo.foo.foo",
		"foo.foo.foo.foo.foo.foo.foo.foo.foo.>",
		"foo.foo.foo.foo.foo.foo.foo.foo.>",
		"foo.foo.foo.foo.foo.foo.foo.>",
		"foo.foo.foo.foo.foo.foo.>",
		"foo.foo.foo.foo.foo.>",
		"foo.foo.foo.foo.>",
		"foo.foo.foo.>",
		"foo.foo.>",
		"foo.>",
		">",
		"foo.foo.foo.foo.foo.foo.foo.foo.foo.*",
		"foo.foo.foo.foo.foo.foo.foo.foo.*.*",
		"foo.foo.foo.foo.foo.foo.foo.*.*.*",
		"foo.foo.foo.foo.foo.foo.*.*.*.*",
		"foo.foo.foo.foo.foo.*.*.*.*.*",
		"foo.foo.foo.foo.*.*.*.*.*.*",
		"foo.foo.foo.*.*.*.*.*.*.*",
		"foo.foo.*.*.*.*.*.*.*.*",
		"foo.*.*.*.*.*.*.*.*.*",
		"*.*.*.*.*.*.*.*.*.*",
	}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		for _, subject := range subjects {
			if !matchLiteral(cachedSubj, subject) {
				b.Fatalf("Subject %q no match with %q", cachedSubj, subject)
			}
		}
	}
}

func Benchmark_____SublistMatch10kSubsWithNoCache(b *testing.B) {
	var nsubs = 512
	b.StopTimer()
	s := NewSublist()
	subject := "foo"
	for i := 0; i < nsubs; i++ {
		s.Insert(newSub(subject))
	}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		r := s.Match(subject)
		if len(r.psubs) != nsubs {
			b.Fatalf("Results len is %d, should be %d", len(r.psubs), nsubs)
		}
		delete(s.cache, subject)
	}
}

func removeTest(b *testing.B, singleSubject, doBatch bool, qgroup string) {
	b.StopTimer()
	s := NewSublist()
	subject := "foo"

	subs := make([]*subscription, 0, b.N)
	for i := 0; i < b.N; i++ {
		var sub *subscription
		if singleSubject {
			sub = newQSub(subject, qgroup)
		} else {
			sub = newQSub(fmt.Sprintf("%s.%d\n", subject, i), qgroup)
		}
		s.Insert(sub)
		subs = append(subs, sub)
	}

	// Actual test on Remove
	b.StartTimer()
	if doBatch {
		s.RemoveBatch(subs)
	} else {
		for _, sub := range subs {
			s.Remove(sub)
		}
	}
}

func Benchmark__________SublistRemove1TokenSingle(b *testing.B) {
	removeTest(b, true, false, "")
}

func Benchmark___________SublistRemove1TokenBatch(b *testing.B) {
	removeTest(b, true, true, "")
}

func Benchmark_________SublistRemove2TokensSingle(b *testing.B) {
	removeTest(b, false, false, "")
}

func Benchmark__________SublistRemove2TokensBatch(b *testing.B) {
	removeTest(b, false, true, "")
}

func Benchmark________SublistRemove1TokenQGSingle(b *testing.B) {
	removeTest(b, true, false, "bar")
}

func Benchmark_________SublistRemove1TokenQGBatch(b *testing.B) {
	removeTest(b, true, true, "bar")
}

func removeMultiTest(b *testing.B, singleSubject, doBatch bool) {
	b.StopTimer()
	s := NewSublist()
	subject := "foo"
	var swg, fwg sync.WaitGroup
	swg.Add(b.N)
	fwg.Add(b.N)

	// We will have b.N go routines each with 1k subscriptions.
	sc := 1000

	for i := 0; i < b.N; i++ {
		go func() {
			subs := make([]*subscription, 0, sc)
			for n := 0; n < sc; n++ {
				var sub *subscription
				if singleSubject {
					sub = newSub(subject)
				} else {
					sub = newSub(fmt.Sprintf("%s.%d\n", subject, n))
				}
				s.Insert(sub)
				subs = append(subs, sub)
			}
			// Wait to start test
			swg.Done()
			swg.Wait()
			// Actual test on Remove
			if doBatch {
				s.RemoveBatch(subs)
			} else {
				for _, sub := range subs {
					s.Remove(sub)
				}
			}
			fwg.Done()
		}()
	}
	swg.Wait()
	b.StartTimer()
	fwg.Wait()
}

// Check contention rates for remove from multiple Go routines.
// Reason for BatchRemove.
func Benchmark_________SublistRemove1kSingleMulti(b *testing.B) {
	removeMultiTest(b, true, false)
}

// Batch version
func Benchmark__________SublistRemove1kBatchMulti(b *testing.B) {
	removeMultiTest(b, true, true)
}

func Benchmark__SublistRemove1kSingle2TokensMulti(b *testing.B) {
	removeMultiTest(b, false, false)
}

// Batch version
func Benchmark___SublistRemove1kBatch2TokensMulti(b *testing.B) {
	removeMultiTest(b, false, true)
}
