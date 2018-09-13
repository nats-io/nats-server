// Copyright 2018 The NATS Authors
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

package nkeys

import (
	"bytes"
	"encoding/base32"
	"encoding/binary"

	"golang.org/x/crypto/ed25519"
)

// PrefixByte is a lead byte representing the type.
type PrefixByte byte

const (
	//PrefixByteSeed is the version byte used for encoded NATS Seeds
	PrefixByteSeed PrefixByte = 18 << 3 // Base32-encodes to 'S...'

	//PrefixBytePrivate is the version byte used for encoded NATS Private keys
	PrefixBytePrivate PrefixByte = 15 << 3 // Base32-encodes to 'P...'

	//PrefixByteServer is the version byte used for encoded NATS Servers
	PrefixByteServer PrefixByte = 13 << 3 // Base32-encodes to 'N...'

	//PrefixByteCluster is the version byte used for encoded NATS Clusters
	PrefixByteCluster PrefixByte = 2 << 3 // Base32-encodes to 'C...'

	//PrefixByteOperator is the version byte used for encoded NATS Operators
	PrefixByteOperator PrefixByte = 14 << 3 // Base32-encodes to 'O...'

	//PrefixByteAccount is the version byte used for encoded NATS Accounts
	PrefixByteAccount PrefixByte = 0 // Base32-encodes to 'A...'

	//PrefixByteUser is the version byte used for encoded NATS Users
	PrefixByteUser PrefixByte = 20 << 3 // Base32-encodes to 'U...'
)

// Set our encoding to not include padding '=='
var b32Enc = base32.StdEncoding.WithPadding(base32.NoPadding)

// Encode will encode a raw key or seed with the prefix and crc16 and then base32 encoded.
func Encode(prefix PrefixByte, src []byte) (string, error) {
	if err := checkValidPrefixByte(prefix); err != nil {
		return "", err
	}

	var raw bytes.Buffer

	// write prefix byte
	if err := raw.WriteByte(byte(prefix)); err != nil {
		return "", err
	}

	// write payload
	if _, err := raw.Write(src); err != nil {
		return "", err
	}

	// Calculate and write crc16 checksum
	err := binary.Write(&raw, binary.LittleEndian, crc16(raw.Bytes()))
	if err != nil {
		return "", err
	}

	return b32Enc.EncodeToString(raw.Bytes()), nil
}

// EncodeSeed will encode a raw key with the prefix and then seed prefix and crc16 and then base32 encoded.
func EncodeSeed(public PrefixByte, src []byte) (string, error) {
	if err := checkValidPublicPrefixByte(public); err != nil {
		return "", err
	}

	if len(src) != ed25519.SeedSize {
		return "", ErrInvalidSeedLen
	}

	// In order to make this human printable for both bytes, we need to do a little
	// bit manipulation to setup for base32 encoding which takes 5 bits at a time.
	b1 := byte(PrefixByteSeed) | (byte(public) >> 5)
	b2 := (byte(public) & 31) << 3 // 31 = 00011111

	var raw bytes.Buffer

	raw.WriteByte(b1)
	raw.WriteByte(b2)

	// write payload
	if _, err := raw.Write(src); err != nil {
		return "", err
	}

	// Calculate and write crc16 checksum
	err := binary.Write(&raw, binary.LittleEndian, crc16(raw.Bytes()))
	if err != nil {
		return "", err
	}

	return b32Enc.EncodeToString(raw.Bytes()), nil
}

// decode will decode the base32 string and check crc16 and the prefix for validity.
func decode(src string) ([]byte, error) {
	raw, err := b32Enc.DecodeString(src)
	if err != nil {
		return nil, err
	}

	if len(raw) < 4 {
		return nil, ErrInvalidEncoding
	}

	var crc uint16
	checksum := bytes.NewReader(raw[len(raw)-2:])
	if err := binary.Read(checksum, binary.LittleEndian, &crc); err != nil {
		return nil, err
	}

	// ensure checksum is valid
	if err := validate(raw[0:len(raw)-2], crc); err != nil {
		return nil, err
	}

	return raw[0 : len(raw)-2], nil
}

// Decode will decode the base32 string and check crc16 and enforce the prefix is what is expected.
func Decode(expectedPrefix PrefixByte, src string) ([]byte, error) {
	if err := checkValidPrefixByte(expectedPrefix); err != nil {
		return nil, err
	}

	raw, err := decode(src)
	if err != nil {
		return nil, err
	}

	if prefix := PrefixByte(raw[0]); prefix != expectedPrefix {
		return nil, ErrInvalidPrefixByte
	}
	return raw[1:], nil
}

// DecodeSeed will decode the base32 string and check crc16 and enforce the prefix is a seed
// and the subsequent type is a valid type.
func DecodeSeed(src string) (PrefixByte, []byte, error) {
	raw, err := decode(src)
	if err != nil {
		return PrefixByteSeed, nil, err
	}
	// Need to do the reverse here to get back to internal representation.
	b1 := raw[0] & 248                          // 248 = 11111000
	b2 := (raw[0]&7)<<5 | ((raw[1] & 248) >> 3) // 7 = 00000111

	if PrefixByte(b1) != PrefixByteSeed {
		return PrefixByteSeed, nil, ErrInvalidSeed
	}
	if checkValidPublicPrefixByte(PrefixByte(b2)) != nil {
		return PrefixByteSeed, nil, ErrInvalidSeed
	}
	return PrefixByte(b2), raw[2:], nil
}

// IsValidPublicUserKey will decode and verify the string is a valid encoded Public User Key.
func IsValidPublicUserKey(src string) bool {
	_, err := Decode(PrefixByteUser, src)
	return err == nil
}

// IsValidPublicAccountKey will decode and verify the string is a valid encoded Public Account Key.
func IsValidPublicAccountKey(src string) bool {
	_, err := Decode(PrefixByteAccount, src)
	return err == nil
}

// IsValidPublicServerKey will decode and verify the string is a valid encoded Public Server Key.
func IsValidPublicServerKey(src string) bool {
	_, err := Decode(PrefixByteServer, src)
	return err == nil
}

// IsValidPublicClusterKey will decode and verify the string is a valid encoded Public Cluster Key.
func IsValidPublicClusterKey(src string) bool {
	_, err := Decode(PrefixByteCluster, src)
	return err == nil
}

// IsValidPublicOperatorKey will decode and verify the string is a valid encoded Public Operator Key.
func IsValidPublicOperatorKey(src string) bool {
	_, err := Decode(PrefixByteOperator, src)
	return err == nil
}

// checkValidPrefixByte returns an error if the provided value
// is not one of the defined valid prefix byte constants.
func checkValidPrefixByte(prefix PrefixByte) error {
	if prefix == PrefixByteOperator {
		return nil
	}
	if prefix == PrefixByteServer {
		return nil
	}
	if prefix == PrefixByteCluster {
		return nil
	}
	if prefix == PrefixByteAccount {
		return nil
	}
	if prefix == PrefixByteUser {
		return nil
	}
	if prefix == PrefixByteSeed {
		return nil
	}
	if prefix == PrefixBytePrivate {
		return nil
	}
	return ErrInvalidPrefixByte
}

// checkValidPublicPrefixByte returns an error if the provided value
// is not one of the public defined valid prefix byte constants.
func checkValidPublicPrefixByte(prefix PrefixByte) error {
	if prefix == PrefixByteServer {
		return nil
	}
	if prefix == PrefixByteCluster {
		return nil
	}
	if prefix == PrefixByteOperator {
		return nil
	}
	if prefix == PrefixByteAccount {
		return nil
	}
	if prefix == PrefixByteUser {
		return nil
	}
	return ErrInvalidPrefixByte
}
