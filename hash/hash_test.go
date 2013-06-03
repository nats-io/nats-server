package hash

import (
	"testing"
)

// Representative of small and medium subjects.
var smlKey = []byte("foo")
var medKey = []byte("apcera.continuum.router.foo.bar")

func Benchmark_Bernstein_SmallKey(b *testing.B) {
	b.SetBytes(1)
	for i := 0; i < b.N; i++ {
		Bernstein(smlKey)
	}
}

func Benchmark_Murmur3___SmallKey(b *testing.B) {
	b.SetBytes(1)
	for i := 0; i < b.N; i++ {
		Murmur3(smlKey, M3Seed)
	}
}

func Benchmark_FNV1A_____SmallKey(b *testing.B) {
	b.SetBytes(1)
	for i := 0; i < b.N; i++ {
		FNV1A(smlKey)
	}
}

func Benchmark_Meiyan____SmallKey(b *testing.B) {
	b.SetBytes(1)
	for i := 0; i < b.N; i++ {
		Meiyan(smlKey)
	}
}

func Benchmark_Jesteress_SmallKey(b *testing.B) {
	b.SetBytes(1)
	for i := 0; i < b.N; i++ {
		Jesteress(smlKey)
	}
}

func Benchmark_Yorikke___SmallKey(b *testing.B) {
	b.SetBytes(1)
	for i := 0; i < b.N; i++ {
		Yorikke(smlKey)
	}
}

func Benchmark_Bernstein___MedKey(b *testing.B) {
	b.SetBytes(1)
	for i := 0; i < b.N; i++ {
		Bernstein(medKey)
	}
}

func Benchmark_Murmur3_____MedKey(b *testing.B) {
	b.SetBytes(1)
	for i := 0; i < b.N; i++ {
		Murmur3(medKey, M3Seed)
	}
}

func Benchmark_FNV1A_______MedKey(b *testing.B) {
	b.SetBytes(1)
	for i := 0; i < b.N; i++ {
		FNV1A(medKey)
	}
}

func Benchmark_Meiyan______MedKey(b *testing.B) {
	b.SetBytes(1)
	for i := 0; i < b.N; i++ {
		Meiyan(medKey)
	}
}

func Benchmark_Jesteress___MedKey(b *testing.B) {
	b.SetBytes(1)
	for i := 0; i < b.N; i++ {
		Jesteress(medKey)
	}
}

func Benchmark_Yorikke_____MedKey(b *testing.B) {
	b.SetBytes(1)
	for i := 0; i < b.N; i++ {
		Yorikke(medKey)
	}
}
