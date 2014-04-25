// Copyright 2014 Apcera Inc. All rights reserved.

package server

import (
	"strconv"
	"testing"
	"sync"
)

func BenchmarkParseInt(b *testing.B) {
	b.SetBytes(1)
	n := "12345678"
	for i := 0; i < b.N; i++ {
		strconv.ParseInt(n, 10, 0)
	}
}

func BenchmarkParseSize(b *testing.B) {
	b.SetBytes(1)
	n := []byte("12345678")
	for i := 0; i < b.N; i++ {
		parseSize(n)
	}
}

func deferUnlock() {
	mu sync.Mutex
	mu.Lock()
	defer mu.Unlock()
}

func BenchmarkDeferMutex(b *testing.B) {
	for i := 0; i < b.N; i++ {
		deferUnlock()
	}
}


