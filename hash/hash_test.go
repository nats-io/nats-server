package hash

import (
	"testing"
)

var foo = []byte("foo")
var bar = []byte("bar")
var baz = []byte("baz")
var sub = []byte("apcera.continuum.router.foo.bar")

func Benchmark_Bernstein_SmallKey(b *testing.B) {
	b.SetBytes(1)
	for i := 0; i < b.N; i++ {
		Bernstein(foo)
	}
}

func Benchmark_Murmur3___SmallKey(b *testing.B) {
	b.SetBytes(1)
	for i := 0; i < b.N; i++ {
		Murmur3(foo, M3Seed)
	}
}

func Benchmark_FNV1A_____SmallKey(b *testing.B) {
	b.SetBytes(1)
	for i := 0; i < b.N; i++ {
		FNV1A(foo)
	}
}

func Benchmark_Meiyan____SmallKey(b *testing.B) {
	b.SetBytes(1)
	for i := 0; i < b.N; i++ {
		Meiyan(foo)
	}
}

func Benchmark_Jesteress_SmallKey(b *testing.B) {
	b.SetBytes(1)
	for i := 0; i < b.N; i++ {
		Jesteress(foo)
	}
}

func Benchmark_Yorikke___SmallKey(b *testing.B) {
	b.SetBytes(1)
	for i := 0; i < b.N; i++ {
		Yorikke(foo)
	}
}

func Benchmark_Bernstein___MedKey(b *testing.B) {
	b.SetBytes(1)
	for i := 0; i < b.N; i++ {
		Bernstein(sub)
	}
}

func Benchmark_Murmur3_____MedKey(b *testing.B) {
	b.SetBytes(1)
	for i := 0; i < b.N; i++ {
		Murmur3(sub, M3Seed)
	}
}

func Benchmark_FNV1A_______MedKey(b *testing.B) {
	b.SetBytes(1)
	for i := 0; i < b.N; i++ {
		FNV1A(sub)
	}
}

func Benchmark_Meiyan______MedKey(b *testing.B) {
	b.SetBytes(1)
	for i := 0; i < b.N; i++ {
		Meiyan(sub)
	}
}

func Benchmark_Jesteress___MedKey(b *testing.B) {
	b.SetBytes(1)
	for i := 0; i < b.N; i++ {
		Jesteress(sub)
	}
}

func Benchmark_Yorikke_____MedKey(b *testing.B) {
	b.SetBytes(1)
	for i := 0; i < b.N; i++ {
		Yorikke(sub)
	}
}
