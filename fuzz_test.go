//go:build go1.18
// +build go1.18

// Copyright 2012-2026 The NATS Authors
// SPDX-License-Identifier: Apache-2.0

package nats_test

import (
	"testing"

	"github.com/nats-io/nats-server/v2/server/certstore"
)

// FuzzParseCertStore tests certificate store type parsing with
// arbitrary attacker-controlled strings.
//
// NATS is a cloud-native messaging system with 20K+ stars and
// 13 GitHub Security Advisories.
func FuzzParseCertStore(f *testing.F) {
	f.Add("windows")
	f.Add("macos")
	f.Add("")
	f.Add("invalid")
	f.Add(string(make([]byte, 1000)))

	f.Fuzz(func(t *testing.T, certStore string) {
		if len(certStore) > 10000 {
			return
		}
		_, _ = certstore.ParseCertStore(certStore)
	})
}

// FuzzParseCertMatchBy tests certificate match type parsing
// with arbitrary strings.
func FuzzParseCertMatchBy(f *testing.F) {
	f.Add("sha256")
	f.Add("")
	f.Add("invalid")
	f.Add(string(make([]byte, 1000)))

	f.Fuzz(func(t *testing.T, certMatchBy string) {
		if len(certMatchBy) > 10000 {
			return
		}
		_, _ = certstore.ParseCertMatchBy(certMatchBy)
	})
}
