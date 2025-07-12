package server

import (
	"bytes"
	"compress/gzip"
	"io"
	"testing"

	"github.com/klauspost/compress/s2"
)

func TestGzipPool_CompressDecompress(t *testing.T) {
	original := []byte("gzip compression pool test")
	var buf bytes.Buffer

	writer := Gzip.GetWriter(&buf)
	if _, err := writer.Write(original); err != nil {
		t.Fatalf("gzip write failed: %v", err)
	}

	Gzip.PutWriter(writer)

	r, err := gzip.NewReader(&buf)
	if err != nil {
		t.Fatalf("gzip.NewReader failed: %v", err)
	}
	defer r.Close()

	result, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("reading gzip decompressed data failed: %v", err)
	}
	if !bytes.Equal(original, result) {
		t.Fatalf("decompressed gzip data does not match original.\nGot: %q\nWant: %q", result, original)
	}
}

func TestSnappyPool_CompressDecompress(t *testing.T) {
	original := []byte("snappy compression pool test")
	var buf bytes.Buffer

	writer := SnappyCompact.GetWriter(&buf)
	if _, err := writer.Write(original); err != nil {
		t.Fatalf("snappy write failed: %v", err)
	}

	SnappyCompact.PutWriter(writer)

	result, err := s2.Decode(nil, buf.Bytes())
	if err != nil {
		t.Fatalf("snappy decode failed: %v", err)
	}
	if !bytes.Equal(original, result) {
		t.Fatalf("decompressed snappy data does not match original.\nGot: %q\nWant: %q", result, original)
	}
}
