package stree

import (
	"encoding/gob"
	"io"
)

type Gobber[T any] struct {
	desc   []byte
	enc    *gob.Encoder
	dec    *gob.Decoder
	buf    gobBuffer
	err    error
	failed bool
}

func NewGobber[T any](desc []byte) *Gobber[T] {
	g := new(Gobber[T])
	g.enc = gob.NewEncoder(&g.buf)
	g.dec = gob.NewDecoder(&g.buf)
	var v T
	if desc == nil {
		if g.err = g.enc.Encode(v); g.err == nil {
			g.desc = append(g.desc, g.buf.data...)
			g.err = g.dec.Decode(&v)
		}
	} else {
		g.desc = append(g.desc, desc...)
		g.buf.data = desc
		if g.err = g.dec.Decode(&v); g.err == nil {
			g.buf.data = nil
			g.err = g.enc.Encode(&v)
		}
	}
	g.buf.data = nil
	g.failed = (g.err != nil)
	return g
}

func (g *Gobber[T]) Desc() []byte {
	if g.failed {
		return nil
	}
	return g.desc
}

func (g *Gobber[T]) Error() error {
	return g.err
}

func (g *Gobber[T]) Encode(v *T) []byte {
	if g.failed {
		return nil
	}
	g.buf.data = nil
	g.err = g.enc.Encode(v)
	r := g.buf.data
	g.buf.data = nil
	if g.err == nil {
		return r
	}
	return nil
}

func (g *Gobber[T]) Decode(b []byte) *T {
	if g.failed {
		return nil
	}
	var v T
	g.buf.data = b
	g.err = g.dec.Decode(&v)
	g.buf.data = nil
	if g.err == nil {
		return &v
	}
	return nil
}

type gobBuffer struct {
	data []byte
}

func (gb *gobBuffer) Write(p []byte) (int, error) {
	gb.data = append(gb.data, p...)
	return len(p), nil
}

func (gb *gobBuffer) Read(p []byte) (int, error) {
	if len(gb.data) == 0 {
		return 0, io.EOF
	}
	n := copy(p, gb.data)
	gb.data = gb.data[n:]
	return n, nil
}

func (gb *gobBuffer) ReadByte() (byte, error) {
	if len(gb.data) == 0 {
		return 0, io.EOF
	}
	r := gb.data[0]
	gb.data = gb.data[1:]
	return r, nil
}
