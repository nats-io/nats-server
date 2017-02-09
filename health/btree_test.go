package health

import (
	"testing"
)

func Test201BtreeInsertDisplay(t *testing.T) {
	s1 := AgentLoc{ID: "abc"}
	s2 := AgentLoc{ID: "xyz"}
	r := newRanktree()
	r.insert(s2)
	r.insert(s1)

	sz := r.size()
	if sz != 2 {
		t.Fatalf("expected 2, saw sz=%v", sz)
	}
	s := r.String()
	if s == "[]" {
		t.Fatalf("missing serialization of set elements")
	}
	expect := `[{"serverId":"abc","host":"","port":0,"leader":false,"leaseExpires":"0001-01-01T00:00:00Z","rank":0,"pid":0},{"serverId":"xyz","host":"","port":0,"leader":false,"leaseExpires":"0001-01-01T00:00:00Z","rank":0,"pid":0}]`
	if s != expect {
		t.Fatalf("serial json didn't match expectations.\n expect:'%s'\n\n observe:'%s'", expect, s)
	}
}

func Test202BtreeEqual(t *testing.T) {
	s1 := AgentLoc{ID: "abc"}
	s2 := AgentLoc{ID: "xyz"}
	r := newRanktree()
	r.insert(s2)
	r.insert(s1)

	s := r.clone()
	same := setsEqual(&members{DedupTree: s}, &members{DedupTree: r})
	if !same {
		t.Fatalf("expected setsEqual to be true")
	}
}

func Test203SetDiff(t *testing.T) {
	s1 := AgentLoc{ID: "abc"}
	s2 := AgentLoc{ID: "def"}
	s3 := AgentLoc{ID: "ghi"}
	s4 := AgentLoc{ID: "jkl"}

	r1 := newRanktree()
	r1.insert(s1)
	r1.insert(s2)
	r1.insert(s3)
	r1.insert(s4)

	r2 := newRanktree()
	r2.insert(s1)
	r2.insert(s2)

	diff := setDiff(&members{DedupTree: r1}, &members{DedupTree: r2})
	if diff.DedupTree.size() != 2 {
		t.Fatalf("setdiff was not the right size")
	}
	x := diff.DedupTree.minrank()
	if !alocEqual(&x, &s3) {
		t.Fatalf("setdiff was not the right element")
	}
}
