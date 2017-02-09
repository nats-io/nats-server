package health

// https://github.com/glycerine/rbuf
// copyright (c) 2014, Jason E. Aten
// license: MIT

import "io"

// ringBuf:
//
//    a fixed-size circular ring buffer. Just what it says.
//
type ringBuf struct {
	A        []interface{}
	N        int // MaxViewInBytes, the size of A
	Beg      int // start of data in A
	Readable int // number of bytes available to read in A
}

// newRingBuf constructs a new ringBuf.
func newRingBuf(maxViewInBytes int) *ringBuf {
	n := maxViewInBytes
	r := &ringBuf{
		N:        n,
		Beg:      0,
		Readable: 0,
	}
	r.A = make([]interface{}, n, n)

	return r
}

// clone makes a copy of b.
func (b *ringBuf) clone() *ringBuf {
	a := &ringBuf{}
	for i := range b.A {
		a.A = append(a.A, b.A[i])
	}
	a.N = b.N
	a.Beg = b.Beg
	a.Readable = b.Readable
	return a
}

// Reset quickly forgets any data stored in the ring buffer. The
// data is still there, but the ring buffer will ignore it and
// overwrite those buffers as new data comes in.
func (b *ringBuf) Reset() {
	b.Beg = 0
	b.Readable = 0
}

// Advance(): non-standard, but better than Next(),
// because we don't have to unwrap our buffer and pay the cpu time
// for the copy that unwrapping may need.
// Useful in conjuction/after ReadWithoutAdvance() above.
func (b *ringBuf) Advance(n int) {
	if n <= 0 {
		return
	}
	if n > b.Readable {
		n = b.Readable
	}
	b.Readable -= n
	b.Beg = (b.Beg + n) % b.N
}

func intMin(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (b *ringBuf) Avail() int {
	return b.Readable
}

// returns the earliest index, or -1 if
// the ring is empty
func (b *ringBuf) First() int {
	if b.Readable == 0 {
		return -1
	}
	return b.Beg
}

// returns the index of the last element,
// or -1 if the ring is empty.
func (b *ringBuf) Last() int {
	if b.Readable == 0 {
		return -1
	}

	last := b.Beg + b.Readable - 1
	if last < b.N {
		// we fit without wrapping
		return last
	}

	return last % b.N
}

// Kth presents the contents of the
// ring as a strictly linear sequence,
// so the user doesn't need to think
// about modular arithmetic. Here k indexes from
// [0, f.Readable-1], assuming f.Avail()
// is greater than 0. Kth() returns an
// actual index where the logical k-th
// element, starting from f.Beg, resides.
// f.Beg itself lives at k = 0. If k is
// out of bounds, or the ring is empty,
// -1 is returned.
func (b *ringBuf) Kth(k int) int {
	if b.Readable == 0 || k < 0 || k >= b.Readable {
		return -1
	}
	return (b.Beg + k) % b.N
}

//
// Append returns an error if there is no more
// space in the ring. Otherwise it returns nil
// and writes p into the ring in last position.
//
func (b *ringBuf) Append(p interface{}) error {
	writeCapacity := b.N - b.Readable
	if writeCapacity <= 0 {
		// we are all full up already.
		return io.ErrShortWrite
	}

	writeStart := (b.Beg + b.Readable) % b.N
	b.A[writeStart] = p

	b.Readable++
	return nil
}
