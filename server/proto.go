// Copyright 2024 The NATS Authors
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

// Inspired by https://github.com/protocolbuffers/protobuf-go/blob/master/encoding/protowire/wire.go

package server

import (
	"errors"
	"fmt"
	"math"
)

var errProtoInsufficient = errors.New("insufficient data to read a value")
var errProtoOverflow = errors.New("too much data for a value")
var errProtoInvalidFieldNumber = errors.New("invalid field number")

func protoScanField(b []byte) (num, typ, size int, err error) {
	num, typ, sizeTag, err := protoScanTag(b)
	if err != nil {
		return 0, 0, 0, err
	}
	b = b[sizeTag:]

	sizeValue, err := protoScanFieldValue(typ, b)
	if err != nil {
		return 0, 0, 0, err
	}
	return num, typ, sizeTag + sizeValue, nil
}

func protoScanTag(b []byte) (num, typ, size int, err error) {
	tagint, size, err := protoScanVarint(b)
	if err != nil {
		return 0, 0, 0, err
	}

	// NOTE: MessageSet allows for larger field numbers than normal.
	if (tagint >> 3) > uint64(math.MaxInt32) {
		return 0, 0, 0, errProtoInvalidFieldNumber
	}
	num = int(tagint >> 3)
	if num < 1 {
		return 0, 0, 0, errProtoInvalidFieldNumber
	}
	typ = int(tagint & 7)

	return num, typ, size, nil
}

func protoScanFieldValue(typ int, b []byte) (size int, err error) {
	switch typ {
	case 0:
		_, size, err = protoScanVarint(b)
	case 5: // fixed32
		size = 4
	case 1: // fixed64
		size = 8
	case 2: // length-delimited
		size, err = protoScanBytes(b)
	default:
		return 0, fmt.Errorf("unsupported type: %d", typ)
	}
	return size, err
}

func protoScanVarint(b []byte) (v uint64, size int, err error) {
	var y uint64
	if len(b) <= 0 {
		return 0, 0, errProtoInsufficient
	}
	v = uint64(b[0])
	if v < 0x80 {
		return v, 1, nil
	}
	v -= 0x80

	if len(b) <= 1 {
		return 0, 0, errProtoInsufficient
	}
	y = uint64(b[1])
	v += y << 7
	if y < 0x80 {
		return v, 2, nil
	}
	v -= 0x80 << 7

	if len(b) <= 2 {
		return 0, 0, errProtoInsufficient
	}
	y = uint64(b[2])
	v += y << 14
	if y < 0x80 {
		return v, 3, nil
	}
	v -= 0x80 << 14

	if len(b) <= 3 {
		return 0, 0, errProtoInsufficient
	}
	y = uint64(b[3])
	v += y << 21
	if y < 0x80 {
		return v, 4, nil
	}
	v -= 0x80 << 21

	if len(b) <= 4 {
		return 0, 0, errProtoInsufficient
	}
	y = uint64(b[4])
	v += y << 28
	if y < 0x80 {
		return v, 5, nil
	}
	v -= 0x80 << 28

	if len(b) <= 5 {
		return 0, 0, errProtoInsufficient
	}
	y = uint64(b[5])
	v += y << 35
	if y < 0x80 {
		return v, 6, nil
	}
	v -= 0x80 << 35

	if len(b) <= 6 {
		return 0, 0, errProtoInsufficient
	}
	y = uint64(b[6])
	v += y << 42
	if y < 0x80 {
		return v, 7, nil
	}
	v -= 0x80 << 42

	if len(b) <= 7 {
		return 0, 0, errProtoInsufficient
	}
	y = uint64(b[7])
	v += y << 49
	if y < 0x80 {
		return v, 8, nil
	}
	v -= 0x80 << 49

	if len(b) <= 8 {
		return 0, 0, errProtoInsufficient
	}
	y = uint64(b[8])
	v += y << 56
	if y < 0x80 {
		return v, 9, nil
	}
	v -= 0x80 << 56

	if len(b) <= 9 {
		return 0, 0, errProtoInsufficient
	}
	y = uint64(b[9])
	v += y << 63
	if y < 2 {
		return v, 10, nil
	}
	return 0, 0, errProtoOverflow
}

func protoScanBytes(b []byte) (size int, err error) {
	l, lenSize, err := protoScanVarint(b)
	if err != nil {
		return 0, err
	}
	if l > uint64(len(b[lenSize:])) {
		return 0, errProtoInsufficient
	}
	return lenSize + int(l), nil
}

func protoEncodeVarint(v uint64) []byte {
	b := make([]byte, 0, 10)
	switch {
	case v < 1<<7:
		b = append(b, byte(v))
	case v < 1<<14:
		b = append(b,
			byte((v>>0)&0x7f|0x80),
			byte(v>>7))
	case v < 1<<21:
		b = append(b,
			byte((v>>0)&0x7f|0x80),
			byte((v>>7)&0x7f|0x80),
			byte(v>>14))
	case v < 1<<28:
		b = append(b,
			byte((v>>0)&0x7f|0x80),
			byte((v>>7)&0x7f|0x80),
			byte((v>>14)&0x7f|0x80),
			byte(v>>21))
	case v < 1<<35:
		b = append(b,
			byte((v>>0)&0x7f|0x80),
			byte((v>>7)&0x7f|0x80),
			byte((v>>14)&0x7f|0x80),
			byte((v>>21)&0x7f|0x80),
			byte(v>>28))
	case v < 1<<42:
		b = append(b,
			byte((v>>0)&0x7f|0x80),
			byte((v>>7)&0x7f|0x80),
			byte((v>>14)&0x7f|0x80),
			byte((v>>21)&0x7f|0x80),
			byte((v>>28)&0x7f|0x80),
			byte(v>>35))
	case v < 1<<49:
		b = append(b,
			byte((v>>0)&0x7f|0x80),
			byte((v>>7)&0x7f|0x80),
			byte((v>>14)&0x7f|0x80),
			byte((v>>21)&0x7f|0x80),
			byte((v>>28)&0x7f|0x80),
			byte((v>>35)&0x7f|0x80),
			byte(v>>42))
	case v < 1<<56:
		b = append(b,
			byte((v>>0)&0x7f|0x80),
			byte((v>>7)&0x7f|0x80),
			byte((v>>14)&0x7f|0x80),
			byte((v>>21)&0x7f|0x80),
			byte((v>>28)&0x7f|0x80),
			byte((v>>35)&0x7f|0x80),
			byte((v>>42)&0x7f|0x80),
			byte(v>>49))
	case v < 1<<63:
		b = append(b,
			byte((v>>0)&0x7f|0x80),
			byte((v>>7)&0x7f|0x80),
			byte((v>>14)&0x7f|0x80),
			byte((v>>21)&0x7f|0x80),
			byte((v>>28)&0x7f|0x80),
			byte((v>>35)&0x7f|0x80),
			byte((v>>42)&0x7f|0x80),
			byte((v>>49)&0x7f|0x80),
			byte(v>>56))
	default:
		b = append(b,
			byte((v>>0)&0x7f|0x80),
			byte((v>>7)&0x7f|0x80),
			byte((v>>14)&0x7f|0x80),
			byte((v>>21)&0x7f|0x80),
			byte((v>>28)&0x7f|0x80),
			byte((v>>35)&0x7f|0x80),
			byte((v>>42)&0x7f|0x80),
			byte((v>>49)&0x7f|0x80),
			byte((v>>56)&0x7f|0x80),
			1)
	}
	return b
}
