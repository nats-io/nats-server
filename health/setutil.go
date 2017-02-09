package health

import (
	"fmt"
)

// utilities and sets stuff

// p is a shortcut for a call to fmt.Printf that implicitly starts
// and ends its message with a newline.
func p(format string, stuff ...interface{}) {
	fmt.Printf("\n "+format+"\n", stuff...)
}

func panicOn(err error) {
	if err != nil {
		panic(err)
	}
}

type members struct {
	DedupTree *ranktree `json:"Mem"`
}

func (m *members) insert(s AgentLoc) {
	m.DedupTree.insert(s)
}
func (m *members) clear() {
	m.DedupTree = newRanktree()
}

func (m *members) minrank() AgentLoc {
	return m.DedupTree.minrank()
}

func (m *members) clone() *members {
	cp := newMembers()
	if m.DedupTree == nil {
		return cp
	}
	cp.DedupTree = m.DedupTree.clone()
	return cp
}

func (m *members) setEmpty() bool {
	return m.DedupTree.Len() == 0
}

func (m *members) String() string {
	return string(m.mustJSONBytes())
}

func newMembers() *members {
	return &members{
		DedupTree: newRanktree(),
	}
}

func (m *members) mustJSONBytes() []byte {
	by, err := m.DedupTree.MarshalJSON()
	panicOn(err)
	return by
}
