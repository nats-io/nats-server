// Copyright 2012 Apcera Inc. All rights reserved.

// Collection of high performance 32-bit hash functions.
package hash

import (
	"unsafe"
)

// Generates a Bernstein Hash.
func Bernstein(data []byte) uint32 {
	hash := uint32(5381)
	for _, b := range data {
		hash = ((hash << 5) + hash) + uint32(b)
	}
	return hash
}

// Constants for FNV1A and derivatives
const (
	_OFF32 = 2166136261
	_P32   = 16777619
	_YP32  = 709607
)

// Generates an FNV1A Hash [http://en.wikipedia.org/wiki/Fowle-Noll-Vo_hash_function]
func FNV1A(data []byte) uint32 {
	var hash uint32 = _OFF32
	for _, c := range data {
		hash ^= uint32(c)
		hash *= _P32
	}
	return hash
}

// Constants for multiples of sizeof(WORD)
const (
	_WSZ    = 4         // 4
	_DWSZ   = _WSZ << 1 // 8
	_DDWSZ  = _WSZ << 2 // 16
	_DDDWSZ = _WSZ << 3 // 32
)

// Jesteress derivative of FNV1A from [http://www.sanmayce.com/Fastest_Hash/]
func Jesteress(data []byte) uint32 {
	h32 := uint32(_OFF32)
	i, dlen := 0, len(data)

	for ; dlen >= _DDWSZ; dlen -= _DDWSZ {
		k1 := *(*uint64)(unsafe.Pointer(&data[i]))
		k2 := *(*uint64)(unsafe.Pointer(&data[i+4]))
		h32 = uint32((uint64(h32) ^ ((k1<<5 | k1>>27) ^ k2)) * _YP32)
		i += _DDWSZ
	}

	// Cases: 0,1,2,3,4,5,6,7
	if (dlen & _DWSZ) > 0 {
		k1 := *(*uint64)(unsafe.Pointer(&data[i]))
		h32 = uint32(uint64(h32) ^ k1) * _YP32
		i += _DWSZ
	}
	if (dlen & _WSZ) > 0 {
		k1 := *(*uint32)(unsafe.Pointer(&data[i]))
		h32 = (h32 ^ k1) * _YP32
		i += _WSZ
	}
	if (dlen & 1) > 0 {
		h32 = (h32 ^ uint32(data[i])) * _YP32
	}
	return h32 ^ (h32 >> 16)
}

// Meiyan derivative of FNV1A from [http://www.sanmayce.com/Fastest_Hash/]
func Meiyan(data []byte) uint32 {
	h32 := uint32(_OFF32)
	i, dlen := 0, len(data)

	for ; dlen >= _DDWSZ; dlen -= _DDWSZ {
		k1 := *(*uint64)(unsafe.Pointer(&data[i]))
		k2 := *(*uint64)(unsafe.Pointer(&data[i+4]))
		h32 = uint32((uint64(h32) ^ ((k1<<5 | k1>>27) ^ k2)) * _YP32)
		i += _DDWSZ
	}

	// Cases: 0,1,2,3,4,5,6,7
	if (dlen & _DWSZ) > 0 {
		k1 := *(*uint64)(unsafe.Pointer(&data[i]))
		h32 = uint32(uint64(h32)^k1) * _YP32
		i += _WSZ
		k1 = *(*uint64)(unsafe.Pointer(&data[i]))
		h32 = uint32(uint64(h32)^k1) * _YP32
		i += _WSZ
	}
	if (dlen & _WSZ) > 0 {
		k1 := *(*uint32)(unsafe.Pointer(&data[i]))
		h32 = (h32 ^ k1) * _YP32
		i += _WSZ
	}
	if (dlen & 1) > 0 {
		h32 = (h32 ^ uint32(data[i])) * _YP32
	}
	return h32 ^ (h32 >> 16)
}

// Yorikke derivative of FNV1A from [http://www.sanmayce.com/Fastest_Hash/]
func Yorikke(data []byte) uint32 {
	h32 := uint32(_OFF32)
	h32b := uint32(_OFF32)
	i, dlen := 0, len(data)

	for ; dlen >= _DDDWSZ; dlen -= _DDDWSZ {
		k1 := *(*uint64)(unsafe.Pointer(&data[i]))
		k2 := *(*uint64)(unsafe.Pointer(&data[i+4]))
		h32 = uint32((uint64(h32) ^ (((k1<<5 | k1>>27)) ^ k2)) * _YP32)
		k1 = *(*uint64)(unsafe.Pointer(&data[i+8]))
		k2 = *(*uint64)(unsafe.Pointer(&data[i+12]))
		h32b = uint32((uint64(h32b) ^ (((k1<<5 | k1>>27)) ^ k2)) * _YP32)
		i += _DDDWSZ
	}
	if (dlen & _DDWSZ) > 0 {
		k1 := *(*uint64)(unsafe.Pointer(&data[i]))
		k2 := *(*uint64)(unsafe.Pointer(&data[i+4]))
		h32 = uint32((uint64(h32) ^ k1) * _YP32)
		h32b = uint32((uint64(h32b) ^ k2) * _YP32)
		i += _DDWSZ
	}
	// Cases: 0,1,2,3,4,5,6,7
	if (dlen & _DWSZ) > 0 {
		k1 := *(*uint32)(unsafe.Pointer(&data[i]))
		k2 := *(*uint32)(unsafe.Pointer(&data[i+2]))
		h32 = (h32 ^ k1) * _YP32
		h32b = (h32b ^ k2) * _YP32
		i += _DWSZ
	}
	if (dlen & _WSZ) > 0 {
		k1 := *(*uint32)(unsafe.Pointer(&data[i]))
		h32 = (h32 ^ k1) * _YP32
		i += _WSZ
	}
	if (dlen & 1) > 0 {
		h32 = (h32 ^ uint32(data[i])) * _YP32
	}
	h32 = (h32 ^ (h32b<<5 | h32b>>27)) * _YP32
	return h32 ^ (h32 >> 16)
}

// Constants defined by the Murmur3 algorithm
const (
	_C1 = uint32(0xcc9e2d51)
	_C2 = uint32(0x1b873593)
	_F1 = uint32(0x85ebca6b)
	_F2 = uint32(0xc2b2ae35)
)

// A default seed for Murmur3
const M3Seed = uint32(0x9747b28c)

// Generates a Murmur3 Hash [http://code.google.com/p/smhasher/wiki/MurmurHash3]
// Does not generate intermediate objects.
func Murmur3(data []byte, seed uint32) uint32 {
	h1 := seed
	ldata := len(data)
	end := ldata - (ldata % 4)
	i := 0

	// Inner
	for ; i < end; i += 4 {
		k1 := *(*uint32)(unsafe.Pointer(&data[i]))
		k1 *= _C1
		k1 = (k1 << 15) | (k1 >> 17)
		k1 *= _C2

		h1 ^= k1
		h1 = (h1 << 13) | (h1 >> 19)
		h1 = h1*5 + 0xe6546b64
	}

	// Tail
	var k1 uint32
	switch ldata - i {
	case 3:
		k1 |= uint32(data[i+2]) << 16
		fallthrough
	case 2:
		k1 |= uint32(data[i+1]) << 8
		fallthrough
	case 1:
		k1 |= uint32(data[i])
		k1 *= _C1
		k1 = (k1 << 15) | (k1 >> 17)
		k1 *= _C2
		h1 ^= k1
	}

	// Finalization
	h1 ^= uint32(ldata)
	h1 ^= (h1 >> 16)
	h1 *= _F1
	h1 ^= (h1 >> 13)
	h1 *= _F2
	h1 ^= (h1 >> 16)

	return h1
}
