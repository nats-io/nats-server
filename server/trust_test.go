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
	"os"
	"strings"
	"testing"
)

const (
	t1 = "OBYEOZQ46VZMFMNETBAW2H6VGDSOBLP67VUEZJ5LPR3PIZBWWRIY4UI4"
	t2 = "OAHC7NGAHG3YVPTD6QOUFZGPM2OMU6EOS67O2VHBUOA6BJLPTWFHGLKU"
)

func TestStampedTrustedNkeys(t *testing.T) {
	opts := DefaultOptions()
	defer func() { trustedNkeys = "" }()

	// Set this to a bad key. We require valid operator public keys.
	trustedNkeys = "bad"
	if s := New(opts); s != nil {
		s.Shutdown()
		t.Fatalf("Expected a bad trustedNkeys to return nil server")
	}

	trustedNkeys = t1
	s := New(opts)
	if s == nil {
		t.Fatalf("Expected non-nil server")
	}
	if len(s.trustedNkeys) != 1 || s.trustedNkeys[0] != t1 {
		t.Fatalf("Trusted Nkeys not setup properly")
	}
	trustedNkeys = strings.Join([]string{t1, t2}, " ")
	if s = New(opts); s == nil {
		t.Fatalf("Expected non-nil server")
	}
	if len(s.trustedNkeys) != 2 || s.trustedNkeys[0] != t1 || s.trustedNkeys[1] != t2 {
		t.Fatalf("Trusted Nkeys not setup properly")
	}

	opts.TrustedNkeys = []string{"OVERRIDE ME"}
	if s = New(opts); s != nil {
		t.Fatalf("Expected opts.TrustedNkeys to return nil server")
	}
}

func TestTrustedKeysOptions(t *testing.T) {
	trustedNkeys = ""
	opts := DefaultOptions()
	opts.TrustedNkeys = []string{"bad"}
	if s := New(opts); s != nil {
		s.Shutdown()
		t.Fatalf("Expected a bad opts.TrustedNkeys to return nil server")
	}
	opts.TrustedNkeys = []string{t1}
	s := New(opts)
	if s == nil {
		t.Fatalf("Expected non-nil server")
	}
	if len(s.trustedNkeys) != 1 || s.trustedNkeys[0] != t1 {
		t.Fatalf("Trusted Nkeys not setup properly via options")
	}
	opts.TrustedNkeys = []string{t1, t2}
	if s = New(opts); s == nil {
		t.Fatalf("Expected non-nil server")
	}
	if len(s.trustedNkeys) != 2 || s.trustedNkeys[0] != t1 || s.trustedNkeys[1] != t2 {
		t.Fatalf("Trusted Nkeys not setup properly via options")
	}
}

func TestTrustConfigOption(t *testing.T) {
	confFileName := createConfFile(t, []byte(fmt.Sprintf("trusted = %q", t1)))
	defer os.Remove(confFileName)
	opts, err := ProcessConfigFile(confFileName)
	if err != nil {
		t.Fatalf("Error parsing config: %v", err)
	}
	if l := len(opts.TrustedNkeys); l != 1 {
		t.Fatalf("Expected 1 trusted key, got %d", l)
	}
	if opts.TrustedNkeys[0] != t1 {
		t.Fatalf("Expected trusted key to be %q, got %q", t1, opts.TrustedNkeys[0])
	}

	confFileName = createConfFile(t, []byte(fmt.Sprintf("trusted = [%q, %q]", t1, t2)))
	defer os.Remove(confFileName)
	opts, err = ProcessConfigFile(confFileName)
	if err != nil {
		t.Fatalf("Error parsing config: %v", err)
	}
	if l := len(opts.TrustedNkeys); l != 2 {
		t.Fatalf("Expected 2 trusted key, got %d", l)
	}
	if opts.TrustedNkeys[0] != t1 {
		t.Fatalf("Expected trusted key to be %q, got %q", t1, opts.TrustedNkeys[0])
	}
	if opts.TrustedNkeys[1] != t2 {
		t.Fatalf("Expected trusted key to be %q, got %q", t2, opts.TrustedNkeys[1])
	}

	// Now do a bad one.
	confFileName = createConfFile(t, []byte(fmt.Sprintf("trusted = [%q, %q]", t1, "bad")))
	defer os.Remove(confFileName)
	_, err = ProcessConfigFile(confFileName)
	if err == nil {
		t.Fatalf("Expected an error parsing trust keys with a bad key")
	}
}
