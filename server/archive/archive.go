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
	"bufio"
	"encoding/binary"
	"errors"
	"io"
)

const MagicBytes = "NATSARC1"

// maxNameLen bounds the entry name length accepted when reading an archive,
// avoiding an unbounded allocation on corrupt or malicious input. The name is
// the only variable-length field read directly from the stream.
const maxNameLen = 1 << 20

var (
	ErrClosed            = errors.New("archive: closed")
	ErrInvalidArchive    = errors.New("archive: invalid archive stream")
	ErrIncompleteEntry   = errors.New("archive: entry not fully written")
	ErrNoActiveEntry     = errors.New("archive: no active entry")
	ErrWriteTooLong      = errors.New("archive: write exceeds declared entry size")
	ErrNilHeader         = errors.New("archive: nil header")
	ErrNegativeEntrySize = errors.New("archive: negative entry size")
)

// Header describes one archive entry.
//
// On the wire each entry is the encoded header fields followed by the payload.
// HeaderSize and PayloadSize describe how that payload is split (e.g. message
// headers vs. body); the payload length is their sum and is not stored
// separately. Sequence is always encoded, with 0 meaning "unset".
type Header struct {
	Name        string
	HeaderSize  int64
	PayloadSize int64
	Timestamp   int64
	Sequence    uint64
}

func (h *Header) clone() *Header {
	if h == nil {
		return nil
	}
	c := *h
	return &c
}

// Writer writes a stream of archive entries.
type Writer struct {
	w          io.Writer
	wroteMagic bool
	header     *Header
	remaining  int64
	closed     bool
}

// NewWriter returns a new stream writer.
func NewWriter(w io.Writer) *Writer {
	return &Writer{w: w}
}

// WriteHeader starts a new archive entry.
func (a *Writer) WriteHeader(hdr *Header) error {
	if a.closed {
		return ErrClosed
	}
	if hdr == nil {
		return ErrNilHeader
	}
	if hdr.HeaderSize < 0 || hdr.PayloadSize < 0 {
		return ErrNegativeEntrySize
	}
	if a.header != nil && a.remaining > 0 {
		return ErrIncompleteEntry
	}
	totalPayloadSize := hdr.HeaderSize + hdr.PayloadSize
	if totalPayloadSize < 0 {
		return ErrNegativeEntrySize
	}
	if !a.wroteMagic {
		if _, err := io.WriteString(a.w, MagicBytes); err != nil {
			return err
		}
		a.wroteMagic = true
	}
	if err := a.writeUvarint(uint64(len(hdr.Name))); err != nil {
		return err
	}
	if err := a.writeVarint(hdr.Timestamp); err != nil {
		return err
	}
	if err := a.writeUvarint(hdr.Sequence); err != nil {
		return err
	}
	if err := a.writeUvarint(uint64(hdr.HeaderSize)); err != nil {
		return err
	}
	if err := a.writeUvarint(uint64(hdr.PayloadSize)); err != nil {
		return err
	}
	if _, err := io.WriteString(a.w, hdr.Name); err != nil {
		return err
	}
	a.header = hdr.clone()
	a.remaining = totalPayloadSize
	if a.remaining == 0 {
		a.header = nil
	}
	return nil
}

// Write writes payload bytes for the current entry.
func (a *Writer) Write(p []byte) (int, error) {
	if a.closed {
		return 0, ErrClosed
	}
	if len(p) == 0 {
		return 0, nil
	}
	if a.header == nil {
		return 0, ErrNoActiveEntry
	}
	wanted := len(p)
	if int64(wanted) > a.remaining {
		wanted = int(a.remaining)
	}
	n, err := a.w.Write(p[:wanted])
	a.remaining -= int64(n)
	if err != nil {
		return n, err
	}
	if n < wanted {
		return n, io.ErrShortWrite
	}
	if len(p) > wanted {
		if a.remaining == 0 {
			a.header = nil
		}
		return n, ErrWriteTooLong
	}
	if a.remaining == 0 {
		a.header = nil
	}
	return n, nil
}

// Close finalizes the stream writer.
func (a *Writer) Close() error {
	if a.closed {
		return nil
	}
	a.closed = true
	if a.header != nil && a.remaining > 0 {
		return ErrIncompleteEntry
	}
	return nil
}

// Flush flushes the underlying writer if it supports flushing.
func (a *Writer) Flush() error {
	if a.closed {
		return ErrClosed
	}
	if f, ok := a.w.(interface{ Flush() error }); ok {
		return f.Flush()
	}
	if f, ok := a.w.(interface{ Flush() }); ok {
		f.Flush()
	}
	return nil
}

// Reader reads a stream of archive entries.
type Reader struct {
	r         *bufio.Reader
	header    *Header
	remaining int64
	seenMagic bool
	err       error
}

// NewReader returns a new stream reader.
func NewReader(r io.Reader) *Reader {
	return &Reader{r: bufio.NewReader(r)}
}

func (u *Reader) discardCurrent() error {
	if u.remaining == 0 {
		u.header = nil
		return nil
	}
	_, err := io.CopyN(io.Discard, u.r, u.remaining)
	u.remaining = 0
	u.header = nil
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return ErrInvalidArchive
	}
	return err
}

// Next advances to the next entry and returns its header.
func (u *Reader) Next() (*Header, error) {
	if u.err != nil {
		return nil, u.err
	}
	if err := u.discardCurrent(); err != nil {
		u.err = err
		return nil, err
	}
	if !u.seenMagic {
		magic := make([]byte, len(MagicBytes))
		if _, err := io.ReadFull(u.r, magic); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				u.err = io.EOF
				return nil, io.EOF
			}
			u.err = err
			return nil, err
		}
		if string(magic) != MagicBytes {
			u.err = ErrInvalidArchive
			return nil, ErrInvalidArchive
		}
		u.seenMagic = true
	}
	// A clean io.EOF on the first field is the normal end of the stream.
	nameLen, err := u.readUvarint()
	if err != nil {
		u.err = err
		return nil, err
	}
	timestamp, err := u.readVarint()
	if err != nil {
		u.err = noEOF(err)
		return nil, u.err
	}
	sequence, err := u.readUvarint()
	if err != nil {
		u.err = noEOF(err)
		return nil, u.err
	}
	headerSize, err := u.readUvarint()
	if err != nil {
		u.err = noEOF(err)
		return nil, u.err
	}
	payloadSize, err := u.readUvarint()
	if err != nil {
		u.err = noEOF(err)
		return nil, u.err
	}
	// HeaderSize/PayloadSize are stored as int64: a value above MaxInt64
	// converts to a negative int64, and an overflowing sum lands negative too,
	// so the sign checks below cover both cases.
	hdrSize, plSize := int64(headerSize), int64(payloadSize)
	total := hdrSize + plSize
	if nameLen > maxNameLen || hdrSize < 0 || plSize < 0 || total < 0 {
		u.err = ErrInvalidArchive
		return nil, ErrInvalidArchive
	}
	name := make([]byte, nameLen)
	if _, err := io.ReadFull(u.r, name); err != nil {
		u.err = ErrInvalidArchive
		return nil, ErrInvalidArchive
	}
	u.header = &Header{
		Name:        string(name),
		Timestamp:   timestamp,
		Sequence:    sequence,
		HeaderSize:  hdrSize,
		PayloadSize: plSize,
	}
	u.remaining = total
	return u.header.clone(), nil
}

// Read reads bytes from the current entry payload.
func (u *Reader) Read(p []byte) (int, error) {
	if u.err != nil {
		return 0, u.err
	}
	if len(p) == 0 {
		return 0, nil
	}
	if u.header == nil || u.remaining == 0 {
		return 0, io.EOF
	}

	if int64(len(p)) > u.remaining {
		p = p[:int(u.remaining)]
	}
	n, err := io.ReadFull(u.r, p)
	u.remaining -= int64(n)
	if err != nil {
		u.err = ErrInvalidArchive
		return n, ErrInvalidArchive
	}
	if u.remaining == 0 {
		u.header = nil
	}
	return n, nil
}

// noEOF converts a clean io.EOF into ErrInvalidArchive. It is used for header
// fields after the first: reaching EOF there means the entry was truncated
// rather than that the stream ended cleanly on an entry boundary.
func noEOF(err error) error {
	if err != nil && errors.Is(err, io.EOF) {
		return ErrInvalidArchive
	}
	return err
}

func (a *Writer) writeVarint(v int64) error {
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutVarint(tmp[:], v)
	_, err := a.w.Write(tmp[:n])
	return err
}

func (a *Writer) writeUvarint(v uint64) error {
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(tmp[:], v)
	_, err := a.w.Write(tmp[:n])
	return err
}

func (u *Reader) readVarint() (int64, error) {
	v, err := binary.ReadVarint(u.r)
	if err != nil && !errors.Is(err, io.EOF) {
		return v, ErrInvalidArchive
	}
	return v, err
}

func (u *Reader) readUvarint() (uint64, error) {
	v, err := binary.ReadUvarint(u.r)
	if err != nil && !errors.Is(err, io.EOF) {
		return v, ErrInvalidArchive
	}
	return v, err
}
