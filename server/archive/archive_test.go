// Copyright 2026 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package archive

import (
	"bytes"
	"errors"
	"io"
	"testing"
)

func TestRoundTrip(t *testing.T) {
	var buf bytes.Buffer

	w := NewWriter(&buf)

	want := []struct {
		hdr  Header
		data string
	}{
		{
			hdr: Header{
				Name:        "alpha.txt",
				Timestamp:   100,
				HeaderSize:  4,
				PayloadSize: 7,
			},
			data: "hello alpha",
		},
		{
			hdr: Header{
				Name:        "nested/beta.txt",
				Timestamp:   100,
				Sequence:    42,
				HeaderSize:  3,
				PayloadSize: 9,
			},
			data: "beta payload",
		},
	}

	for i := range want {
		if err := w.WriteHeader(&want[i].hdr); err != nil {
			t.Fatalf("write header %d: %v", i, err)
		}
		if _, err := io.WriteString(w, want[i].data); err != nil {
			t.Fatalf("write payload %d: %v", i, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}

	r := NewReader(bytes.NewReader(buf.Bytes()))

	for i := range want {
		hdr, err := r.Next()
		if err != nil {
			t.Fatalf("next %d: %v", i, err)
		}
		if hdr.Name != want[i].hdr.Name {
			t.Fatalf("header %d name mismatch: got %q want %q", i, hdr.Name, want[i].hdr.Name)
		}
		if hdr.Timestamp != want[i].hdr.Timestamp {
			t.Fatalf("header %d timestamp mismatch: got %d want %d", i, hdr.Timestamp, want[i].hdr.Timestamp)
		}
		if hdr.Sequence != want[i].hdr.Sequence {
			t.Fatalf("header %d sequence mismatch: got %d want %d", i, hdr.Sequence, want[i].hdr.Sequence)
		}
		if hdr.HeaderSize != want[i].hdr.HeaderSize {
			t.Fatalf("header %d header size mismatch: got %d want %d", i, hdr.HeaderSize, want[i].hdr.HeaderSize)
		}
		if hdr.PayloadSize != want[i].hdr.PayloadSize {
			t.Fatalf("header %d payload size mismatch: got %d want %d", i, hdr.PayloadSize, want[i].hdr.PayloadSize)
		}
		got, err := io.ReadAll(r)
		if err != nil {
			t.Fatalf("read payload %d: %v", i, err)
		}
		if string(got) != want[i].data {
			t.Fatalf("payload %d mismatch: got %q want %q", i, got, want[i].data)
		}
	}

	if _, err := r.Next(); !errors.Is(err, io.EOF) {
		t.Fatalf("expected EOF, got %v", err)
	}
}

func TestNextSkipsUnreadPayload(t *testing.T) {
	var buf bytes.Buffer

	w := NewWriter(&buf)
	first := Header{Name: "first", HeaderSize: 0, PayloadSize: int64(len("abcdef"))}
	if err := w.WriteHeader(&first); err != nil {
		t.Fatalf("write first header: %v", err)
	}
	if _, err := io.WriteString(w, "abcdef"); err != nil {
		t.Fatalf("write first payload: %v", err)
	}

	second := Header{Name: "second", HeaderSize: 0, PayloadSize: int64(len("second-body"))}
	if err := w.WriteHeader(&second); err != nil {
		t.Fatalf("write second header: %v", err)
	}
	if _, err := io.WriteString(w, "second-body"); err != nil {
		t.Fatalf("write second payload: %v", err)
	}

	r := NewReader(bytes.NewReader(buf.Bytes()))

	hdr, err := r.Next()
	if err != nil {
		t.Fatalf("next first: %v", err)
	}
	if hdr.Name != "first" {
		t.Fatalf("unexpected first header name %q", hdr.Name)
	}

	buf1 := make([]byte, 3)
	if _, err := io.ReadFull(r, buf1); err != nil {
		t.Fatalf("read partial first payload: %v", err)
	}
	if string(buf1) != "abc" {
		t.Fatalf("unexpected partial payload %q", buf1)
	}

	hdr, err = r.Next()
	if err != nil {
		t.Fatalf("next second: %v", err)
	}
	if hdr.Name != "second" {
		t.Fatalf("unexpected second header name %q", hdr.Name)
	}

	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("read second payload: %v", err)
	}
	if string(got) != "second-body" {
		t.Fatalf("unexpected second payload %q", got)
	}
}

// validArchive returns a well-formed archive holding a single entry whose
// payload is the 6-byte string "abcdef" split as HeaderSize=2, PayloadSize=4.
func validArchive(t *testing.T) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := NewWriter(&buf)
	hdr := Header{Name: "first", Timestamp: 7, Sequence: 3, HeaderSize: 2, PayloadSize: 4}
	if err := w.WriteHeader(&hdr); err != nil {
		t.Fatalf("write header: %v", err)
	}
	if _, err := io.WriteString(w, "abcdef"); err != nil {
		t.Fatalf("write payload: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	return buf.Bytes()
}

func TestEmptyArchiveReturnsEOF(t *testing.T) {
	r := NewReader(bytes.NewReader(nil))
	if _, err := r.Next(); !errors.Is(err, io.EOF) {
		t.Fatalf("expected io.EOF on empty archive, got %v", err)
	}
}

func TestInvalidMagicIsRejected(t *testing.T) {
	r := NewReader(bytes.NewReader([]byte("not-a-valid-archive-stream")))
	if _, err := r.Next(); !errors.Is(err, ErrInvalidArchive) {
		t.Fatalf("expected ErrInvalidArchive, got %v", err)
	}
}

// A clean EOF on the first header field means end-of-stream, but an EOF
// partway through the header means the entry was truncated.
func TestTruncatedHeaderIsInvalid(t *testing.T) {
	full := validArchive(t)
	// Keep the magic plus the single name-length byte, dropping the rest.
	r := NewReader(bytes.NewReader(full[:len(MagicBytes)+1]))
	if _, err := r.Next(); !errors.Is(err, ErrInvalidArchive) {
		t.Fatalf("expected ErrInvalidArchive, got %v", err)
	}
}

func TestTruncatedPayloadIsInvalid(t *testing.T) {
	full := validArchive(t)
	// Drop the last 3 payload bytes; the header still decodes cleanly.
	r := NewReader(bytes.NewReader(full[:len(full)-3]))
	hdr, err := r.Next()
	if err != nil {
		t.Fatalf("next: %v", err)
	}
	if hdr.HeaderSize+hdr.PayloadSize != 6 {
		t.Fatalf("unexpected payload size %d", hdr.HeaderSize+hdr.PayloadSize)
	}
	if _, err := io.ReadAll(r); !errors.Is(err, ErrInvalidArchive) {
		t.Fatalf("expected ErrInvalidArchive reading payload, got %v", err)
	}
}

// The header round-trips when its fields are read directly from the stream,
// including a large Sequence (uvarint) and a negative Timestamp (varint).
func TestHeaderFieldEncoding(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	want := Header{Name: "nested/x.dat", Timestamp: -123, Sequence: 1 << 60, HeaderSize: 0, PayloadSize: 0}
	if err := w.WriteHeader(&want); err != nil {
		t.Fatalf("write header: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	r := NewReader(bytes.NewReader(buf.Bytes()))
	got, err := r.Next()
	if err != nil {
		t.Fatalf("next: %v", err)
	}
	if *got != want {
		t.Fatalf("header round-trip mismatch: got %+v want %+v", *got, want)
	}
	// A zero-length payload yields immediate EOF, then a clean end-of-stream.
	if n, err := r.Read(make([]byte, 4)); n != 0 || !errors.Is(err, io.EOF) {
		t.Fatalf("expected (0, EOF) reading empty payload, got (%d, %v)", n, err)
	}
	if _, err := r.Next(); !errors.Is(err, io.EOF) {
		t.Fatalf("expected io.EOF after last entry, got %v", err)
	}
}
