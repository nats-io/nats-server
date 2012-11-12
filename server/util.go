// Copyright 2012 Apcera Inc. All rights reserved.

package server

import (
	"crypto/rand"
	"encoding/hex"
	"io"
)

func genId() string {
	u := make([]byte, 16)
	io.ReadFull(rand.Reader, u)
	return hex.EncodeToString(u)
}

// Ascii numbers 0-9
const (
	ascii_0 = 48
	ascii_9 = 57
)

// parseSize expects decimal positive numbers. We
// return -1 to signal error
func parseSize(d []byte) (n int) {
	if len(d) == 0 {
		return -1
	}
	for _, dec := range d {
		if dec < ascii_0 || dec > ascii_9 {
			return -1
		}
		n = n*10 + (int(dec) - ascii_0)
	}
	return n
}
