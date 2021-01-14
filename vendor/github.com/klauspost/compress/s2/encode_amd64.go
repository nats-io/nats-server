// +build !appengine
// +build !noasm
// +build gc

package s2

// encodeBlock encodes a non-empty src to a guaranteed-large-enough dst. It
// assumes that the varint-encoded length of the decompressed bytes has already
// been written.
//
// It also assumes that:
//	len(dst) >= MaxEncodedLen(len(src)) &&
// 	minNonLiteralBlockSize <= len(src) && len(src) <= maxBlockSize
func encodeBlock(dst, src []byte) (d int) {
	const (
		// Use 12 bit table when less than...
		limit12B = 16 << 10
		// Use 10 bit table when less than...
		limit10B = 4 << 10
		// Use 8 bit table when less than...
		limit8B = 512
	)

	if len(src) >= limit12B {
		return encodeBlockAsm(dst, src)
	}
	if len(src) >= limit10B {
		return encodeBlockAsm12B(dst, src)
	}
	if len(src) >= limit8B {
		return encodeBlockAsm10B(dst, src)
	}
	if len(src) < minNonLiteralBlockSize {
		return 0
	}
	return encodeBlockAsm8B(dst, src)
}

// encodeBlockSnappy encodes a non-empty src to a guaranteed-large-enough dst. It
// assumes that the varint-encoded length of the decompressed bytes has already
// been written.
//
// It also assumes that:
//	len(dst) >= MaxEncodedLen(len(src)) &&
// 	minNonLiteralBlockSize <= len(src) && len(src) <= maxBlockSize
func encodeBlockSnappy(dst, src []byte) (d int) {
	const (
		// Use 12 bit table when less than...
		limit12B = 16 << 10
		// Use 10 bit table when less than...
		limit10B = 4 << 10
		// Use 8 bit table when less than...
		limit8B = 512
	)
	if len(src) >= limit12B {
		return encodeSnappyBlockAsm(dst, src)
	}
	if len(src) >= limit10B {
		return encodeSnappyBlockAsm12B(dst, src)
	}
	if len(src) >= limit8B {
		return encodeSnappyBlockAsm10B(dst, src)
	}
	if len(src) < minNonLiteralBlockSize {
		return 0
	}
	return encodeSnappyBlockAsm8B(dst, src)
}
