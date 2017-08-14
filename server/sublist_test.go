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

func verifyMember(r []*subscription, val *subscription, t *testing.T) {
	for _, v := range r {
		if v == nil {
			continue
		}
		if v == val {
			return
		}
	}
	stackFatalf(t, "Value '%+v' not found in results", val)
}

// Helpera to generate test subscriptions.
func newSub(subject string) *subscription {
	return &subscription{subject: []byte(subject)}
}

func newQSub(subject, queue string) *subscription {
	return &subscription{subject: []byte(subject), queue: []byte(queue)}
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
	verifyMember(r.qsubs[0], sub1, t)

	s.Insert(sub2)
	r = s.Match(subject)
	verifyLen(r.psubs, 0, t)
	verifyQLen(r.qsubs, 2, t)
	verifyLen(r.qsubs[0], 1, t)
	verifyLen(r.qsubs[1], 1, t)
	verifyMember(r.qsubs[0], sub1, t)
	verifyMember(r.qsubs[1], sub2, t)

	s.Insert(sub)
	r = s.Match(subject)
	verifyLen(r.psubs, 1, t)
	verifyQLen(r.qsubs, 2, t)
	verifyLen(r.qsubs[0], 1, t)
	verifyLen(r.qsubs[1], 1, t)
	verifyMember(r.qsubs[0], sub1, t)
	verifyMember(r.qsubs[1], sub2, t)
	verifyMember(r.psubs, sub, t)

	s.Insert(sub1)
	s.Insert(sub2)

	r = s.Match(subject)
	verifyLen(r.psubs, 1, t)
	verifyQLen(r.qsubs, 2, t)
	verifyLen(r.qsubs[0], 2, t)
	verifyLen(r.qsubs[1], 2, t)
	verifyMember(r.qsubs[0], sub1, t)
	verifyMember(r.qsubs[1], sub2, t)
	verifyMember(r.psubs, sub, t)

	// Now removal
	s.Remove(sub)

	r = s.Match(subject)
	verifyLen(r.psubs, 0, t)
	verifyQLen(r.qsubs, 2, t)
	verifyLen(r.qsubs[0], 2, t)
	verifyLen(r.qsubs[1], 2, t)
	verifyMember(r.qsubs[0], sub1, t)
	verifyMember(r.qsubs[1], sub2, t)

	s.Remove(sub1)
	r = s.Match(subject)
	verifyLen(r.psubs, 0, t)
	verifyQLen(r.qsubs, 2, t)
	verifyLen(r.qsubs[0], 1, t)
	verifyLen(r.qsubs[1], 2, t)
	verifyMember(r.qsubs[0], sub1, t)
	verifyMember(r.qsubs[1], sub2, t)

	s.Remove(sub1) // Last one
	r = s.Match(subject)
	verifyLen(r.psubs, 0, t)
	verifyQLen(r.qsubs, 1, t)
	verifyLen(r.qsubs[0], 2, t) // this is sub2/baz now
	verifyMember(r.qsubs[0], sub2, t)

	s.Remove(sub2)
	s.Remove(sub2)
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

func Benchmark_____________Sublist10XMultipleReads(b *testing.B) {
	multiRead(b, 10)
}

func Benchmark____________Sublist100XMultipleReads(b *testing.B) {
	multiRead(b, 100)
}
