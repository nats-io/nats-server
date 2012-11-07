package gnatsd

import (
	"bytes"
	"fmt"
	"runtime"
	"runtime/debug"
	"strings"
	"testing"
	"time"
)

func verifyCount(s *Sublist, count uint32, t *testing.T) {
	if s.Count() != count {
		t.Errorf("Count is %d, should be %d", s.Count(), count)
	}
}

func verifyLen(r []interface{}, l int, t *testing.T) {
	if len(r) != l {
		t.Errorf("Results len is %d, should be %d", len(r), l)
	}
}

func verifyMember(r []interface{}, val string, t *testing.T) {
	for _, v := range r {
		if v == nil {
			continue
		}
		if v.(string) == val {
			return
		}
	}
	t.Errorf("Value '%s' not found in results", val)
}

func verifyNumLevels(s *Sublist, expected int, t *testing.T) {
	dl := s.numLevels()
	if dl != expected {
		t.Errorf("NumLevels is %d, should be %d", dl, expected)
	}
}

func TestInit(t *testing.T) {
	s := New()
	verifyCount(s, 0, t)
}

func TestInsertCount(t *testing.T) {
	s := New()
	s.Insert([]byte("foo"), "a")
	s.Insert([]byte("bar"), "b")
	s.Insert([]byte("foo.bar"), "b")
	verifyCount(s, 3, t)
}

func TestSimple(t *testing.T) {
	s := New()
	val := "a"
	sub := []byte("foo")
	s.Insert(sub, val)
	r := s.Match(sub)
	verifyLen(r, 1, t)
	verifyMember(r, val, t)
}

func TestSimpleMultiTokens(t *testing.T) {
	s := New()
	val := "a"
	sub := []byte("foo.bar.baz")
	s.Insert(sub, val)
	r := s.Match(sub)
	verifyLen(r, 1, t)
	verifyMember(r, val, t)
}

func TestPartialWildcard(t *testing.T) {
	s := New()
	literal := []byte("a.b.c")
	pwc := []byte("a.*.c")
	a, b := "a", "b"
	s.Insert(literal, a)
	s.Insert(pwc, b)
	r := s.Match(literal)
	verifyLen(r, 2, t)
	verifyMember(r, a, t)
	verifyMember(r, b, t)
}

func TestPartialWildcardAtEnd(t *testing.T) {
	s := New()
	literal := []byte("a.b.c")
	pwc := []byte("a.b.*")
	a, b := "a", "b"
	s.Insert(literal, a)
	s.Insert(pwc, b)
	r := s.Match(literal)
	verifyLen(r, 2, t)
	verifyMember(r, a, t)
	verifyMember(r, b, t)
}

func TestFullWildcard(t *testing.T) {
	s := New()
	literal := []byte("a.b.c")
	fwc := []byte("a.>")
	a, b := "a", "b"
	s.Insert(literal, a)
	s.Insert(fwc, b)
	r := s.Match(literal)
	verifyLen(r, 2, t)
	verifyMember(r, a, t)
	verifyMember(r, b, t)
}

func TestRemove(t *testing.T) {
	s := New()
	literal := []byte("a.b.c.d")
	value := "foo"
	s.Insert(literal, value)
	verifyCount(s, 1, t)
	s.Remove(literal, "bar")
	verifyCount(s, 1, t)
	s.Remove([]byte("a.b.c"), value)
	verifyCount(s, 1, t)
	s.Remove(literal, value)
	verifyCount(s, 0, t)
	r := s.Match(literal)
	verifyLen(r, 0, t)
}

func TestRemoveWildcard(t *testing.T) {
	s := New()
	literal := []byte("a.b.c.d")
	pwc := []byte("a.b.*.d")
	fwc := []byte("a.b.>")
	value := "foo"
	s.Insert(pwc, value)
	s.Insert(fwc, value)
	s.Insert(literal, value)
	verifyCount(s, 3, t)
	r := s.Match(literal)
	verifyLen(r, 3, t)
	s.Remove(literal, value)
	verifyCount(s, 2, t)
	s.Remove(fwc, value)
	verifyCount(s, 1, t)
	s.Remove(pwc, value)
	verifyCount(s, 0, t)
}

func TestRemoveCleanup(t *testing.T) {
	s := New()
	literal := []byte("a.b.c.d.e.f")
	depth := len(bytes.Split(literal, []byte(".")))
	value := "foo"
	verifyNumLevels(s, 0, t)
	s.Insert(literal, value)
	verifyNumLevels(s, depth, t)
	s.Remove(literal, value)
	verifyNumLevels(s, 0, t)
}

func TestRemoveCleanupWildcards(t *testing.T) {
	s := New()
	literal := []byte("a.b.*.d.e.>")
	depth := len(bytes.Split(literal, []byte(".")))
	value := "foo"
	verifyNumLevels(s, 0, t)
	s.Insert(literal, value)
	verifyNumLevels(s, depth, t)
	s.Remove(literal, value)
	verifyNumLevels(s, 0, t)
}

func TestCacheBehavior(t *testing.T) {
	s := New()
	literal := []byte("a.b.c")
	fwc := []byte("a.>")
	a, b := "a", "b"
	s.Insert(literal, a)
	r := s.Match(literal)
	verifyLen(r, 1, t)
	s.Insert(fwc, b)
	r = s.Match(literal)
	verifyLen(r, 2, t)
	verifyMember(r, a, t)
	verifyMember(r, b, t)
	s.Remove(fwc, b)
	r = s.Match(literal)
	verifyLen(r, 1, t)
	verifyMember(r, a, t)
}

func checkBool(b, expected bool, t *testing.T) {
	if b != expected {
		debug.PrintStack()
		t.Fatalf("Expected %v, but got %v\n", expected, b)
	}
}

func TestMatchLiterals(t *testing.T) {
	checkBool(matchLiteral([]byte("foo"), []byte("foo")), true, t)
	checkBool(matchLiteral([]byte("foo"), []byte("bar")), false, t)
	checkBool(matchLiteral([]byte("foo"), []byte("*")), true, t)
	checkBool(matchLiteral([]byte("foo"), []byte(">")), true, t)
	checkBool(matchLiteral([]byte("foo.bar"), []byte(">")), true, t)
	checkBool(matchLiteral([]byte("foo.bar"), []byte("foo.>")), true, t)
	checkBool(matchLiteral([]byte("foo.bar"), []byte("bar.>")), false, t)
}

func TestCacheBounds(t *testing.T) {
	s := New()
	s.Insert([]byte("cache.>"), "foo")

	tmpl := "cache.test.%d"
	loop := s.cmax + 100

	for i := 0; i < loop ; i++ {
		sub := []byte(fmt.Sprintf(tmpl, i))
		s.Match(sub)
	}
	cs := int(s.cache.Count())
	if  cs > s.cmax {
		t.Fatalf("Cache is growing past limit: %d vs %d\n", cs, s.cmax)
	}
}

// -- Benchmarks Setup --

var subs [][]byte
var toks = []string{"apcera", "continuum", "component", "router", "api", "imgr", "jmgr", "auth"}
var sl = New()
var results = make([]interface{}, 0, 64)

func init() {
	subs = make([][]byte, 0, 256*1024)
	subsInit("")
	for i := 0; i < len(subs); i++ {
		sl.Insert(subs[i], subs[i])
	}
	addWildcards()
	println("Sublist holding ", sl.Count(), " subscriptions")
}

func subsInit(pre string) {
	var sub string
	for _, t := range toks {
		if len(pre) > 0 {
			sub = pre + "." + t
		} else {
			sub = t
		}
		subs = append(subs, []byte(sub))
		if len(strings.Split(sub, ".")) < 5 {
			subsInit(sub)
		}
	}
}

func addWildcards() {
	sl.Insert([]byte("cloud.>"), "paas")
	sl.Insert([]byte("cloud.continuum.component.>"), "health")
	sl.Insert([]byte("cloud.*.*.router.*"), "traffic")
}

// -- Benchmarks Setup End --


func Benchmark______________________Insert(b *testing.B) {
	b.SetBytes(1)
	s := New()
	for i, l := 0, len(subs); i < b.N; i++ {
		index := i % l
		s.Insert(subs[index], subs[index])
	}
}

func Benchmark____________MatchSingleToken(b *testing.B) {
	b.SetBytes(1)
	s := []byte("apcera")
	for i := 0; i < b.N; i++ {
		sl.Match(s)
	}
}

func Benchmark______________MatchTwoTokens(b *testing.B) {
	b.SetBytes(1)
	s := []byte("apcera.continuum")
	for i := 0; i < b.N; i++ {
		sl.Match(s)
	}
}

func Benchmark_MatchFourTokensSingleResult(b *testing.B) {
	b.SetBytes(1)
	s := []byte("apcera.continuum.component.router")
	for i := 0; i < b.N; i++ {
		sl.Match(s)
	}
}

func Benchmark_MatchFourTokensMultiResults(b *testing.B) {
	b.SetBytes(1)
	s := []byte("cloud.continuum.component.router")
	for i := 0; i < b.N; i++ {
		sl.Match(s)
	}
}

func Benchmark_______MissOnLastTokenOfFive(b *testing.B) {
	b.SetBytes(1)
	s := []byte("apcera.continuum.component.router.ZZZZ")
	for i := 0; i < b.N; i++ {
		sl.Match(s)
	}
}

func _BenchmarkRSS(b *testing.B) {
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	println("HEAP:", m.HeapObjects)
	println("ALLOC:", m.Alloc)
	println("TOTAL ALLOC:", m.TotalAlloc)
	time.Sleep(30 * 1e9)
}
