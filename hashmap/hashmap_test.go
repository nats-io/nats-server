package hashmap

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"io"
	"testing"
)

func TestMapWithBkts(t *testing.T) {
	bkts := make([]*Entry, 3, 3)
	_, err := NewWithBkts(bkts)
	if err == nil {
		t.Fatalf("Buckets size of %d should have failed\n", len(bkts))
	}
	bkts = make([]*Entry, 8, 8)
	_, err = NewWithBkts(bkts)
	if err != nil {
		t.Fatalf("Buckets size of %d should have succeeded\n", len(bkts))
	}
}

var foo = []byte("foo")
var bar = []byte("bar")
var baz = []byte("baz")
var sub = []byte("apcera.continuum.router.foo.bar.baz")

func TestHashMapBasics(t *testing.T) {
	h := New()

	if h.used != 0 {
		t.Fatalf("Wrong number of entries: %d vs 0\n", h.used)
	}
	h.Set(foo, bar)
	if h.used != 1 {
		t.Fatalf("Wrong number of entries: %d vs 1\n", h.used)
	}
	if v := h.Get(foo).([]byte); !bytes.Equal(v, bar) {
		t.Fatalf("Did not receive correct answer: '%s' vs '%s'\n", bar, v)
	}
	h.Remove(foo)
	if h.used != 0 {
		t.Fatalf("Wrong number of entries: %d vs 0\n", h.used)
	}
	if v := h.Get(foo); v != nil {
		t.Fatal("Did not receive correct answer, should be nil")
	}
}

const (
	INS  = 100
	EXP  = 128
	REM  = 75
	EXP2 = 64
)

func TestGrowing(t *testing.T) {
	h := New()

	if len(h.bkts) != _BSZ {
		t.Fatalf("Initial bucket size is wrong: %d vs %d\n", len(h.bkts), _BSZ)
	}
	// Create _INBOX style end tokens
	var toks [INS][]byte
	for i, _ := range toks {
		u := make([]byte, 13)
		io.ReadFull(rand.Reader, u)
		toks[i] = []byte(hex.EncodeToString(u))
		h.Set(toks[i], toks[i])
		tg := h.Get(toks[i]).([]byte)
		if !bytes.Equal(tg, toks[i]) {
			t.Fatalf("Did not match properly, '%s' vs '%s'\n", tg, toks[i])
		}
	}
	if len(h.bkts) != EXP {
		t.Fatalf("Expanded bucket size is wrong: %d vs %d\n", len(h.bkts), EXP)
	}
}

func TestHashMapCollisions(t *testing.T) {
	h := New()
	h.rsz = false

	// Create _INBOX style end tokens
	var toks [INS][]byte
	for i, _ := range toks {
		u := make([]byte, 13)
		io.ReadFull(rand.Reader, u)
		toks[i] = []byte(hex.EncodeToString(u))
		h.Set(toks[i], toks[i])
		tg := h.Get(toks[i]).([]byte)
		if !bytes.Equal(tg, toks[i]) {
			t.Fatalf("Did not match properly, '%s' vs '%s'\n", tg, toks[i])
		}
	}
	if len(h.bkts) != _BSZ {
		t.Fatalf("Bucket size is wrong: %d vs %d\n", len(h.bkts), _BSZ)
	}
	h.grow()
	if len(h.bkts) != 2*_BSZ {
		t.Fatalf("Bucket size is wrong: %d vs %d\n", len(h.bkts), 2*_BSZ)
	}
	ti := 32
	tg := h.Get(toks[ti]).([]byte)
	if !bytes.Equal(tg, toks[ti]) {
		t.Fatalf("Did not match properly, '%s' vs '%s'\n", tg, toks[ti])
	}

	h.Remove(toks[99])
	rg := h.Get(toks[99])
	if rg != nil {
		t.Fatalf("After remove should have been nil! '%s'\n", rg.([]byte))
	}
}

func TestAll(t *testing.T) {
	h := New()
	h.Set([]byte("1"), 1)
	h.Set([]byte("2"), 1)
	h.Set([]byte("3"), 1)
	all := h.All()
	if len(all) != 3 {
		t.Fatalf("Expected All() to return 3, but got %d\n", len(all))
	}
	allkeys := h.AllKeys()
	if len(allkeys) != 3 {
		t.Fatalf("Expected All() to return 3, but got %d\n", len(allkeys))
	}
}

func TestSetDoesReplaceOnExisting(t *testing.T) {
	h := New()
	k := []byte("key")
	h.Set(k, "foo")
	h.Set(k, "bar")
	all := h.All()
	if len(all) != 1 {
		t.Fatalf("Set should replace, expected 1 vs %d\n", len(all))
	}
	s, ok := all[0].(string)
	if !ok {
		t.Fatalf("Value is incorrect: %v\n", all[0].(string))
	}
	if s != "bar" {
		t.Fatalf("Value is incorrect, expected 'bar' vs '%s'\n", s)
	}
}

func TestCollision(t *testing.T) {
	h := New()
	k1 := []byte("999")
	k2 := []byte("1000")
	h.Set(k1, "foo")
	h.Set(k2, "bar")
	all := h.All()
	if len(all) != 2 {
		t.Fatalf("Expected 2 vs %d\n", len(all))
	}
	if h.Get(k1) == nil {
		t.Fatalf("Failed to get '999'\n")
	}
}

func TestHashMapStats(t *testing.T) {
	h := New()
	h.rsz = false

	// Create _INBOX style end tokens
	var toks [INS][]byte
	for i, _ := range toks {
		u := make([]byte, 13)
		io.ReadFull(rand.Reader, u)
		toks[i] = []byte(hex.EncodeToString(u))
		h.Set(toks[i], toks[i])
		tg := h.Get(toks[i]).([]byte)
		if !bytes.Equal(tg, toks[i]) {
			t.Fatalf("Did not match properly, '%s' vs '%s'\n", tg, toks[i])
		}
	}

	s := h.Stats()
	if s.NumElements != INS {
		t.Fatalf("NumElements incorrect: %d vs %d\n", s.NumElements, INS)
	}
	if s.NumBuckets != _BSZ {
		t.Fatalf("NumBuckets incorrect: %d vs %d\n", s.NumBuckets, _BSZ)
	}
	if s.AvgChain > 13 || s.AvgChain < 12 {
		t.Fatalf("AvgChain out of bounds: %f vs %f\n", s.AvgChain, 12.5)
	}
	if s.LongChain > 25 {
		t.Fatalf("LongChain out of bounds: %d vs %d\n", s.LongChain, 22)
	}
}

func TestShrink(t *testing.T) {
	h := New()

	if len(h.bkts) != _BSZ {
		t.Fatalf("Initial bucket size is wrong: %d vs %d\n", len(h.bkts), _BSZ)
	}
	// Create _INBOX style end tokens
	var toks [INS][]byte
	for i, _ := range toks {
		u := make([]byte, 13)
		io.ReadFull(rand.Reader, u)
		toks[i] = []byte(hex.EncodeToString(u))
		h.Set(toks[i], toks[i])
		tg := h.Get(toks[i]).([]byte)
		if !bytes.Equal(tg, toks[i]) {
			t.Fatalf("Did not match properly, '%s' vs '%s'\n", tg, toks[i])
		}
	}
	if len(h.bkts) != EXP {
		t.Fatalf("Expanded bucket size is wrong: %d vs %d\n", len(h.bkts), EXP)
	}
	for i := 0; i < REM; i++ {
		h.Remove(toks[i])
	}
	if len(h.bkts) != EXP2 {
		t.Fatalf("Shrunk bucket size is wrong: %d vs %d\n", len(h.bkts), EXP2)
	}
}

func TestFalseLookup(t *testing.T) {
	h := New()
	// DW + W
	h.Set([]byte("cache.test.0"), "foo")
	v := h.Get([]byte("cache.test.1"))
	if v != nil {
		t.Fatalf("Had a match when did not expect one!\n")
	}
	// DW + W + 3
	h.Set([]byte("cache.test.1234"), "foo")
	v = h.Get([]byte("cache.test.0000"))
	if v != nil {
		t.Fatalf("Had a match when did not expect one!\n")
	}
}

func Benchmark_GoMap___GetSmallKey(b *testing.B) {
	b.StopTimer()
	b.SetBytes(1)
	m := make(map[string][]byte)
	m["foo"] = bar
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		_ = m["foo"]
	}
}

func Benchmark_HashMap_GetSmallKey(b *testing.B) {
	b.StopTimer()
	b.SetBytes(1)
	m := New()
	m.Set(foo, bar)
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		_ = m.Get(foo)
	}
}

func Benchmark_GoMap____GetMedKey(b *testing.B) {
	b.StopTimer()
	b.SetBytes(1)
	ts := string(sub)
	m := make(map[string][]byte)
	m[ts] = bar
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		_ = m[ts]
	}
}

func Benchmark_HashMap__GetMedKey(b *testing.B) {
	b.StopTimer()
	b.SetBytes(1)
	m := New()
	m.Set(sub, bar)
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		m.Get(sub)
	}
}

func Benchmark_HashMap_______Set(b *testing.B) {
	b.StopTimer()
	b.SetBytes(1)
	m := New()
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		m.Set(foo, bar)
	}
}
