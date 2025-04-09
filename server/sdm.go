package server

import "time"

// SDMMeta holds pending/proposed data for subject delete markers or message removals.
type SDMMeta struct {
	totals  map[string]uint64
	pending map[uint64]SDMBySeq
}

// SDMBySeq holds data for a message with a specific sequence.
type SDMBySeq struct {
	last bool  // Whether the message for this sequence was the last for this subject.
	ts   int64 // Last timestamp we proposed a removal/sdm.
}

// SDMBySubj holds whether a message for a specific subject and sequence was a subject delete marker or not.
type SDMBySubj struct {
	seq uint64
	sdm bool
}

func newSDMMeta() *SDMMeta {
	return &SDMMeta{
		totals:  make(map[string]uint64, 1),
		pending: make(map[uint64]SDMBySeq, 1),
	}
}

// empty clears all data.
func (sdm *SDMMeta) empty() {
	if sdm == nil {
		return
	}
	clear(sdm.totals)
	clear(sdm.pending)
}

// trackPending caches the given seq and subj and whether it's the last message for that subject.
func (sdm *SDMMeta) trackPending(seq uint64, subj string, last bool) bool {
	if p, ok := sdm.pending[seq]; ok {
		return p.last
	}
	sdm.pending[seq] = SDMBySeq{last, time.Now().UnixNano()}
	sdm.totals[subj]++
	return last
}

// removeSeqAndSubject clears the seq and subj from the cache.
func (sdm *SDMMeta) removeSeqAndSubject(seq uint64, subj string) {
	if sdm == nil {
		return
	}
	if _, ok := sdm.pending[seq]; ok {
		delete(sdm.pending, seq)
		if msgs, ok := sdm.totals[subj]; ok {
			if msgs <= 1 {
				delete(sdm.totals, subj)
			} else {
				sdm.totals[subj] = msgs - 1
			}
		}
	}
}
