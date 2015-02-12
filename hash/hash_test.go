package hash

import (
	"testing"
)

var keys = [][]byte{
	[]byte("foo"),
	[]byte("bar"),
	[]byte("apcera.continuum.router.foo.bar"),
	[]byte("apcera.continuum.router.foo.bar.baz"),
}

func TestBernstein(t *testing.T) {
	results := []uint32{193491849, 193487034, 2487287557, 3139297488}
	for i, key := range keys {
		h := Bernstein(key)
		if h != results[i] {
			t.Fatalf("hash is incorrect, expected %d, got %d\n",
				results[i], h)
		}
	}
}

func TestFNV1A(t *testing.T) {
	results := []uint32{2851307223, 1991736602, 1990810707, 1244015104}
	for i, key := range keys {
		h := FNV1A(key)
		if h != results[i] {
			t.Fatalf("hash is incorrect, expected %d, got %d\n",
				results[i], h)
		}
	}
}

func TestJesteress(t *testing.T) {
	results := []uint32{1058908168, 1061739001, 4242539713, 3332038527}
	for i, key := range keys {
		h := Jesteress(key)
		if h != results[i] {
			t.Fatalf("hash is incorrect, expected %d, got %d\n",
				results[i], h)
		}
	}
}

func TestMeiyan(t *testing.T) {
	results := []uint32{1058908168, 1061739001, 2891236487, 3332038527}
	for i, key := range keys {
		h := Meiyan(key)
		if h != results[i] {
			t.Fatalf("hash is incorrect, expected %d, got %d\n",
				results[i], h)
		}
	}
}

func TestYorikke(t *testing.T) {
	results := []uint32{3523423968, 2222334353, 407908456, 359111667}
	for i, key := range keys {
		h := Yorikke(key)
		if h != results[i] {
			t.Fatalf("hash is incorrect, expected %d, got %d\n",
				results[i], h)
		}
	}
}

func TestMurmur3(t *testing.T) {
	results := []uint32{659908353, 522989004, 135963738, 990328005}
	for i, key := range keys {
		h := Murmur3(key, M3Seed)
		if h != results[i] {
			t.Fatalf("hash is incorrect, expected %d, got %d\n",
				results[i], h)
		}
	}
}

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
