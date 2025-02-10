// Copyright 2022-2023 The NATS Authors
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

//go:build !windows

package certstore

import (
	"crypto"
	"crypto/tls"
	"io"
)

var _ = MATCHBYEMPTY

// otherKey implements crypto.Signer and crypto.Decrypter to satisfy linter on platforms that don't implement certstore
type otherKey struct{}

func TLSConfig(_ StoreType, _ MatchByType, _ string, _ []string, _ bool, _ *tls.Config) error {
	return ErrOSNotCompatCertStore
}

// Public always returns nil public key since this is a stub on non-supported platform
func (k otherKey) Public() crypto.PublicKey {
	return nil
}

// Sign always returns a nil signature since this is a stub on non-supported platform
func (k otherKey) Sign(rand io.Reader, digest []byte, opts crypto.SignerOpts) (signature []byte, err error) {
	_, _, _ = rand, digest, opts
	return nil, nil
}

// Verify interface conformance.
var _ credential = &otherKey{}
