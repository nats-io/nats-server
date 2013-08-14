package sublist

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

func TestInvalidSubjectsInsert(t *testing.T) {
	s := New()

	// Insert, or subscribtions, can have wildcards, but not empty tokens,
	// and can not have a FWC that is not terminal

	// beginning empty token
	if err := s.Insert([]byte(".foo"), '@'); err != ErrInvalidSubject {
		t.Fatal("Expected invalid subject error")
	}

	// trailing empty token
	if err := s.Insert([]byte("foo."), '@'); err != ErrInvalidSubject {
		t.Fatal("Expected invalid subject error")
	}
	// empty middle token
	if err := s.Insert([]byte("foo..bar"), '@'); err != ErrInvalidSubject {
		t.Fatal("Expected invalid subject error")
	}
	// empty middle token #2
	if err := s.Insert([]byte("foo.bar..baz"), '@'); err != ErrInvalidSubject {
		t.Fatal("Expected invalid subject error")
	}
	// fwc not terminal
	if err := s.Insert([]byte("foo.>.bar"), '@'); err != ErrInvalidSubject {
		t.Fatal("Expected invalid subject error")
	}
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

func TestValidLiteralSubjects(t *testing.T) {
	checkBool(IsValidLiteralSubject([]byte("foo")), true, t)
	checkBool(IsValidLiteralSubject([]byte(".foo")), false, t)
	checkBool(IsValidLiteralSubject([]byte("foo.")), false, t)
	checkBool(IsValidLiteralSubject([]byte("foo..bar")), false, t)
	checkBool(IsValidLiteralSubject([]byte("foo.bar.*")), false, t)
	checkBool(IsValidLiteralSubject([]byte("foo.bar.>")), false, t)
	checkBool(IsValidLiteralSubject([]byte("*")), false, t)
	checkBool(IsValidLiteralSubject([]byte(">")), false, t)
}

func TestMatchLiterals(t *testing.T) {
	checkBool(matchLiteral([]byte("foo"), []byte("foo")), true, t)
	checkBool(matchLiteral([]byte("foo"), []byte("bar")), false, t)
	checkBool(matchLiteral([]byte("foo"), []byte("*")), true, t)
	checkBool(matchLiteral([]byte("foo"), []byte(">")), true, t)
	checkBool(matchLiteral([]byte("foo.bar"), []byte(">")), true, t)
	checkBool(matchLiteral([]byte("foo.bar"), []byte("foo.>")), true, t)
	checkBool(matchLiteral([]byte("foo.bar"), []byte("bar.>")), false, t)
	checkBool(matchLiteral([]byte("stats.test.22"), []byte("stats.>")), true, t)
	checkBool(matchLiteral([]byte("stats.test.22"), []byte("stats.*.*")), true, t)
}

func TestCacheBounds(t *testing.T) {
	s := New()
	s.Insert([]byte("cache.>"), "foo")

	tmpl := "cache.test.%d"
	loop := s.cmax + 100

	for i := 0; i < loop; i++ {
		sub := []byte(fmt.Sprintf(tmpl, i))
		s.Match(sub)
	}
	cs := int(s.cache.Count())
	if cs > s.cmax {
		t.Fatalf("Cache is growing past limit: %d vs %d\n", cs, s.cmax)
	}
}

func TestStats(t *testing.T) {
	s := New()
	s.Insert([]byte("stats.>"), "fwc")
	tmpl := "stats.test.%d"
	loop := 255
	total := uint32(loop + 1)

	for i := 0; i < loop; i++ {
		sub := []byte(fmt.Sprintf(tmpl, i))
		s.Insert(sub, "l")
	}

	stats := s.Stats()
	if time.Since(stats.StatsTime) > 50*time.Millisecond {
		t.Fatalf("StatsTime seems incorrect: %+v\n", stats.StatsTime)
	}
	if stats.NumSubs != total {
		t.Fatalf("Wrong stats for NumSubs: %d vs %d\n", stats.NumSubs, total)
	}
	if stats.NumInserts != uint64(total) {
		t.Fatalf("Wrong stats for NumInserts: %d vs %d\n", stats.NumInserts, total)
	}
	if stats.NumRemoves != 0 {
		t.Fatalf("Wrong stats for NumRemoves: %d vs %d\n", stats.NumRemoves, 0)
	}
	if stats.NumMatches != 0 {
		t.Fatalf("Wrong stats for NumMatches: %d vs %d\n", stats.NumMatches, 0)
	}

	for i := 0; i < loop; i++ {
		s.Match([]byte("stats.test.22"))
	}
	s.Insert([]byte("stats.*.*"), "pwc")
	s.Match([]byte("stats.test.22"))

	stats = s.Stats()
	if stats.NumMatches != uint64(loop+1) {
		t.Fatalf("Wrong stats for NumMatches: %d vs %d\n", stats.NumMatches, loop+1)
	}
	expectedCacheHitRate := 255.0 / 256.0
	if stats.CacheHitRate != expectedCacheHitRate {
		t.Fatalf("Wrong stats for CacheHitRate: %.3g vs %0.3g\n", stats.CacheHitRate, expectedCacheHitRate)
	}
	if stats.MaxFanout != 3 {
		t.Fatalf("Wrong stats for MaxFanout: %d vs %d\n", stats.MaxFanout, 3)
	}
	if stats.AvgFanout != 3.0 {
		t.Fatalf("Wrong stats for AvgFanout: %g vs %g\n", stats.AvgFanout, 3.0)
	}
	s.ResetStats()
	stats = s.Stats()
	if time.Since(stats.StatsTime) > 50*time.Millisecond {
		t.Fatalf("After Reset: StatsTime seems incorrect: %+v\n", stats.StatsTime)
	}
	if stats.NumInserts != 0 {
		t.Fatalf("After Reset: Wrong stats for NumInserts: %d vs %d\n", stats.NumInserts, 0)
	}
	if stats.NumRemoves != 0 {
		t.Fatalf("After Reset: Wrong stats for NumRemoves: %d vs %d\n", stats.NumRemoves, 0)
	}
	if stats.NumMatches != 0 {
		t.Fatalf("After Reset: Wrong stats for NumMatches: %d vs %d\n", stats.NumMatches, 0)
	}
	if stats.CacheHitRate != 0.0 {
		t.Fatalf("After Reset: Wrong stats for CacheHitRate: %.3g vs %0.3g\n", stats.CacheHitRate, 0.0)
	}
}

func TestResultSetSnapshots(t *testing.T) {
	// Make sure result sets do not change out from underneath of us.

	literal := []byte("a.b.c.d.e.f")
	wc := []byte("a.b.c.>")
	value := "xxx"

	s := New()
	s.Insert(literal, value)

	r := s.Match(literal)
	verifyLen(r, 1, t)

	s.Insert(wc, value)
	s.Insert(wc, value)
	verifyLen(r, 1, t)

	s.Remove(wc, value)
	verifyLen(r, 1, t)
}

func TestBadSubjectOnRemove(t *testing.T) {
	bad := []byte("a.b..d")
	value := "bad"

	s := New()
	if err := s.Insert(bad, value); err != ErrInvalidSubject {
		t.Fatalf("Expected ErrInvalidSubject, got %v\n", err)
	}

	if err := s.Remove(bad, value); err != ErrInvalidSubject {
		t.Fatalf("Expected ErrInvalidSubject, got %v\n", err)
	}

	badfwc := []byte("a.>.b")
	if err := s.Remove(badfwc, value); err != ErrInvalidSubject {
		t.Fatalf("Expected ErrInvalidSubject, got %v\n", err)
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
	//	println("Sublist holding ", sl.Count(), " subscriptions")
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
