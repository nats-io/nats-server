package gnatsd

import (
	"bytes"
	"testing"
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
	dl := s.DebugNumLevels()
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


