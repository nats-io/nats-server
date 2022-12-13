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

package server

import (
	"fmt"
	"strings"
	"testing"
)

const (
	t1 = "OBYEOZQ46VZMFMNETBAW2H6VGDSOBLP67VUEZJ5LPR3PIZBWWRIY4UI4"
	t2 = "OAHC7NGAHG3YVPTD6QOUFZGPM2OMU6EOS67O2VHBUOA6BJLPTWFHGLKU"
)

func TestStampedTrustedKeys(t *testing.T) {
	opts := DefaultOptions()
	defer func() { trustedKeys = "" }()

	// Set this to a bad key. We require valid operator public keys.
	trustedKeys = "bad"
	if s := New(opts); s != nil {
		s.Shutdown()
		t.Fatalf("Expected a bad trustedKeys to return nil server")
	}

	trustedKeys = t1
	s := New(opts)
	if s == nil {
		t.Fatalf("Expected non-nil server")
	}
	if len(s.trustedKeys) != 1 || s.trustedKeys[0] != t1 {
		t.Fatalf("Trusted Nkeys not setup properly")
	}
	trustedKeys = strings.Join([]string{t1, t2}, " ")
	if s = New(opts); s == nil {
		t.Fatalf("Expected non-nil server")
	}
	if len(s.trustedKeys) != 2 || s.trustedKeys[0] != t1 || s.trustedKeys[1] != t2 {
		t.Fatalf("Trusted Nkeys not setup properly")
	}

	opts.TrustedKeys = []string{"OVERRIDE ME"}
	if s = New(opts); s != nil {
		t.Fatalf("Expected opts.TrustedKeys to return nil server")
	}
}

func TestTrustedKeysOptions(t *testing.T) {
	trustedKeys = ""
	opts := DefaultOptions()
	opts.TrustedKeys = []string{"bad"}
	if s := New(opts); s != nil {
		s.Shutdown()
		t.Fatalf("Expected a bad opts.TrustedKeys to return nil server")
	}
	opts.TrustedKeys = []string{t1}
	s := New(opts)
	if s == nil {
		t.Fatalf("Expected non-nil server")
	}
	if len(s.trustedKeys) != 1 || s.trustedKeys[0] != t1 {
		t.Fatalf("Trusted Nkeys not setup properly via options")
	}
	opts.TrustedKeys = []string{t1, t2}
	if s = New(opts); s == nil {
		t.Fatalf("Expected non-nil server")
	}
	if len(s.trustedKeys) != 2 || s.trustedKeys[0] != t1 || s.trustedKeys[1] != t2 {
		t.Fatalf("Trusted Nkeys not setup properly via options")
	}
}

func TestTrustConfigOption(t *testing.T) {
	confFileName := createConfFile(t, []byte(fmt.Sprintf("trusted = %q", t1)))
	opts, err := ProcessConfigFile(confFileName)
	if err != nil {
		t.Fatalf("Error parsing config: %v", err)
	}
	if l := len(opts.TrustedKeys); l != 1 {
		t.Fatalf("Expected 1 trusted key, got %d", l)
	}
	if opts.TrustedKeys[0] != t1 {
		t.Fatalf("Expected trusted key to be %q, got %q", t1, opts.TrustedKeys[0])
	}

	confFileName = createConfFile(t, []byte(fmt.Sprintf("trusted = [%q, %q]", t1, t2)))
	opts, err = ProcessConfigFile(confFileName)
	if err != nil {
		t.Fatalf("Error parsing config: %v", err)
	}
	if l := len(opts.TrustedKeys); l != 2 {
		t.Fatalf("Expected 2 trusted key, got %d", l)
	}
	if opts.TrustedKeys[0] != t1 {
		t.Fatalf("Expected trusted key to be %q, got %q", t1, opts.TrustedKeys[0])
	}
	if opts.TrustedKeys[1] != t2 {
		t.Fatalf("Expected trusted key to be %q, got %q", t2, opts.TrustedKeys[1])
	}

	// Now do a bad one.
	confFileName = createConfFile(t, []byte(fmt.Sprintf("trusted = [%q, %q]", t1, "bad")))
	_, err = ProcessConfigFile(confFileName)
	if err == nil {
		t.Fatalf("Expected an error parsing trust keys with a bad key")
	}
}
