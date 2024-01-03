// Copyright 2020-23 The LevelDB-Go, Pebble and NATS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package fastrand

import (
	"math/rand"
	"sync"
	"testing"
	"time"
)

type defaultRand struct {
	mu  sync.Mutex
	src rand.Source64
}

func newDefaultRand() *defaultRand {
	r := &defaultRand{
		src: rand.New(rand.NewSource(time.Now().UnixNano())),
	}
	return r
}

func (r *defaultRand) Uint32() uint32 {
	r.mu.Lock()
	i := uint32(r.src.Uint64())
	r.mu.Unlock()
	return i
}

func (r *defaultRand) Uint64() uint64 {
	r.mu.Lock()
	i := uint64(r.src.Uint64())
	r.mu.Unlock()
	return i
}

func BenchmarkFastRand32(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			Uint32()
		}
	})
}

func BenchmarkFastRand64(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			Uint64()
		}
	})
}

func BenchmarkDefaultRand32(b *testing.B) {
	r := newDefaultRand()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			r.Uint32()
		}
	})
}

func BenchmarkDefaultRand64(b *testing.B) {
	r := newDefaultRand()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			r.Uint64()
		}
	})
}
