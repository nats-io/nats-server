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

//go:build go1.26

package server

import (
	"crypto/fips140"
	"crypto/sha1"
	"encoding/base64"
)

func wsAllowedFIPS() bool {
	// As SHA-1 is only used for Sec-WebSocket-Key and Sec-WebSocket-Accept, we
	// can continue to allow it in FIPS builds as long as they are built with
	// Go 1.26 or later only.
	return true
}

// Concatenate the key sent by the client with the GUID, then computes the SHA1 hash
// and returns it as a based64 encoded string.
func wsAcceptKey(key string) string {
	var r []byte
	fips140.WithoutEnforcement(func() {
		h := sha1.New()
		h.Write([]byte(key))
		h.Write(wsGUID)
		r = h.Sum(nil)
	})
	return base64.StdEncoding.EncodeToString(r)
}
