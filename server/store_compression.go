// Copyright 2019-2023 The NATS Authors
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

package server

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"

	"github.com/klauspost/compress/s2"
)

type StoreCompression uint8

const (
	NoCompression StoreCompression = iota
	S2Compression
)

func (alg StoreCompression) String() string {
	switch alg {
	case NoCompression:
		return "None"
	case S2Compression:
		return "S2"
	default:
		return "Unknown StoreCompression"
	}
}

func (alg StoreCompression) MarshalJSON() ([]byte, error) {
	var str string
	switch alg {
	case S2Compression:
		str = "s2"
	case NoCompression:
		str = "none"
	default:
		return nil, fmt.Errorf("unknown compression algorithm")
	}
	return json.Marshal(str)
}

func (alg *StoreCompression) UnmarshalJSON(b []byte) error {
	var str string
	if err := json.Unmarshal(b, &str); err != nil {
		return err
	}
	switch str {
	case "s2":
		*alg = S2Compression
	case "none":
		*alg = NoCompression
	default:
		return fmt.Errorf("unknown compression algorithm")
	}
	return nil
}

type CompressionInfo struct {
	Algorithm    StoreCompression
	OriginalSize uint64
}

func (c *CompressionInfo) MarshalMetadata() []byte {
	b := make([]byte, 14) // 4 + potentially up to 10 for uint64
	b[0], b[1], b[2] = 'c', 'm', 'p'
	b[3] = byte(c.Algorithm)
	n := binary.PutUvarint(b[4:], c.OriginalSize)
	return b[:4+n]
}

func (c *CompressionInfo) UnmarshalMetadata(b []byte) (int, error) {
	c.Algorithm = NoCompression
	c.OriginalSize = 0
	if len(b) < 5 { // 4 + min 1 for uvarint uint64
		return 0, nil
	}
	if b[0] != 'c' || b[1] != 'm' || b[2] != 'p' {
		return 0, nil
	}
	var n int
	c.Algorithm = StoreCompression(b[3])
	c.OriginalSize, n = binary.Uvarint(b[4:])
	if n <= 0 {
		return 0, fmt.Errorf("metadata incomplete")
	}
	return 4 + n, nil
}

func (alg StoreCompression) Compress(buf []byte) ([]byte, error) {
	var output bytes.Buffer
	var writer io.WriteCloser
	switch alg {
	case NoCompression:
		return buf, nil
	case S2Compression:
		writer = s2.NewWriter(&output)
	default:
		return nil, fmt.Errorf("compression algorithm not known")
	}

	// Compress the block content, but don't compress the checksum.
	// We will preserve it at the end of the block as-is.
	if n, err := io.Copy(writer, bytes.NewReader(buf)); err != nil {
		return nil, fmt.Errorf("error writing to compression writer: %w", err)
	} else if bodyLen := len(buf); n != int64(bodyLen) {
		return nil, fmt.Errorf("short write on body (%d != %d)", n, bodyLen)
	}
	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("error closing compression writer: %w", err)
	}

	return output.Bytes(), nil
}

func (alg StoreCompression) Decompress(buf []byte) ([]byte, error) {
	var reader io.ReadCloser
	switch alg {
	case NoCompression:
		return buf, nil
	case S2Compression:
		reader = io.NopCloser(s2.NewReader(bytes.NewReader(buf)))
	default:
		return nil, fmt.Errorf("compression algorithm not known")
	}

	// Decompress the block content. The checksum isn't compressed so
	// we can preserve it from the end of the block as-is.
	output, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("error reading compression reader: %w", err)
	}
	if err := reader.Close(); err != nil {
		return nil, fmt.Errorf("error closing compression reader: %w", err)
	}

	return output, nil
}

// CompressWithChecksum compresses the given buffer, preserving the checksum
// bytes at the end of the buffer.
func (alg StoreCompression) CompressWithChecksum(buf []byte) ([]byte, error) {
	if len(buf) < checksumSize {
		return nil, fmt.Errorf("uncompressed buffer is too short")
	}
	bodyLen := len(buf) - checksumSize
	checksum := buf[bodyLen:]
	compressed, err := alg.Compress(buf[:bodyLen])
	if err != nil {
		return nil, fmt.Errorf("error compressing: %w", err)
	}
	return append(compressed, checksum[:]...), nil
}

// DecompressWithChecksum decompresses the given buffer, preserving the checksum
// bytes at the end of the buffer.
func (alg StoreCompression) DecompressWithChecksum(buf []byte) ([]byte, error) {
	if len(buf) < checksumSize {
		return nil, fmt.Errorf("compressed buffer is too short")
	}
	bodyLen := len(buf) - checksumSize
	checksum := buf[bodyLen:]
	decompressed, err := alg.Decompress(buf[:bodyLen])
	if err != nil {
		return nil, fmt.Errorf("error decompressing: %w", err)
	}
	return append(decompressed, checksum[:]...), nil
}
