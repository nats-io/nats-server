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

//go:build windows

package server

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

// The tests in this file are not complete, but are a start to test the TPM
// with default settings. For a complete test we would need to use a TPM
// configured with an owner password and a SRK password. This is not
// typically the default or setup in CI/CD environments.

var jsTPMConfigPassword = `
	listen: 127.0.0.1:-1
	jetstream: {
		store_dir: %q
		tpm {
			keys_file: %q
			encryption_password: "s3cr3t!"
		}
	}`

var jsTPMConfigBadPassword = `
    listen: 127.0.0.1:-1
	jetstream: {
		store_dir: %q
		tpm {
			keys_file: %q
			encryption_password: "garbage"
		}
	}`

var jsTPMConfigPasswordPcr = `
	listen: 127.0.0.1:-1
	jetstream: {
		store_dir: %q
		tpm {
			keys_file: %q
			encryption_password: "s3cr3t!"
			pcr: %d
		}
	}`

var jsTPMConfigAllFields = `
	listen: 127.0.0.1:-1
	jetstream: {
		store_dir: %q
		tpm {
			keys_file: %q
			encryption_password: "s3cr3t!"
			pcr: %d
			srk_password: %q
			cipher: "aes"
		}
	}`

var jsTPMConfigInvalidBothOptionsSet = `
	listen: 127.0.0.1:-1
	jetstream: {
		store_dir: %q
		encryption_key: "foo"
		tpm {
			keys_file: "bar.json"
			encryption_password: "s3cr3t!"
		}
	}`

func getTPMFileName(t *testing.T) string {
	return t.TempDir() + "/" + t.Name() + "/jskeys.json"
}

func checkSendMessage(t *testing.T, s *Server) {
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "tpm_test",
		Subjects: []string{"tpm_test"},
	})
	require_NoError(t, err)

	_, err = js.Publish("tpm_test", []byte("hello"))
	require_NoError(t, err)
}

func checkReceiveMessage(t *testing.T, s *Server) {
	// reconnect and get the message
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	// get the message
	sub, err := js.PullSubscribe("tpm_test", "cls")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	m := fetchMsgs(t, sub, 1, 5*time.Second)
	if m == nil {
		t.Fatalf("Did not receive the message")
	}
	if len(m) != 1 {
		t.Fatalf("Expected 1 message, got %d", len(m))
	}
}

func TestJetStreamTPMBasic(t *testing.T) {
	fileName := getTPMFileName(t)
	cf := createConfFile(t, []byte(fmt.Sprintf(jsTPMConfigPassword, t.TempDir(), fileName)))

	s, _ := RunServerWithConfig(cf)
	defer s.Shutdown()

	// Note, actual encryption is tested elsewhere. This just tests that the key is generated.
	key := s.opts.JetStreamKey
	if !strings.HasPrefix(key, "SU") {
		t.Fatalf("expected a TPM key to be generated. Key = %q", key)
	}

	if _, err := os.Stat(fileName); err != nil {
		t.Fatalf("keys file was not created")
	}

	checkSendMessage(t, s)

	// restart the server.
	s.Shutdown()
	s, _ = RunServerWithConfig(cf)
	if s.Running() == false {
		t.Fatalf("Server should be running")
	}
	defer s.Shutdown()

	// double check we're encrypted with the same key.
	if s.opts.JetStreamKey != key {
		t.Fatalf("expected same key")
	}
	checkReceiveMessage(t, s)
}

// Warning: Running this test too frequently will considerably slow down
// these tests and eventually lock out the TPM.
//
// Depending on the environment this varies from 10 failures per day with
// a 24 hour lockout (Dell) to 32 failures per day with a 10
// minute healing time (decrementing one failure every 10 minutes).
func TestJetStreamTPMKeyBadPassword(t *testing.T) {
	fileName := getTPMFileName(t)
	cf := createConfFile(t, []byte(fmt.Sprintf(jsTPMConfigPassword,
		t.TempDir(), fileName)))

	s, _ := RunServerWithConfig(cf)
	defer s.Shutdown()

	checkSendMessage(t, s)
	s.Shutdown()

	// restart the server.
	defer func() {
		r := recover()
		if r == nil {
			t.Fatalf("Expected server panic, unexpected start.")
		}
	}()
	cf2 := createConfFile(t, []byte(fmt.Sprintf(jsTPMConfigBadPassword,
		t.TempDir(), fileName)))
	s, _ = RunServerWithConfig(cf2)
	time.Sleep(3 * time.Second)
	if s.Running() {
		s.Shutdown()
		t.Fatalf("Server should NOT be running")
	}
}

func TestJetStreamTPMKeyWithPCR(t *testing.T) {
	cf := createConfFile(t, []byte(fmt.Sprintf(jsTPMConfigPasswordPcr,
		t.TempDir(), getTPMFileName(t), 18)))

	s, _ := RunServerWithConfig(cf)
	defer s.Shutdown()

	checkSendMessage(t, s)
	s.Shutdown()

	// restart the server
	s, _ = RunServerWithConfig(cf)
	if !s.Running() {
		t.Fatalf("Server should be running")
	}
}

func TestJetStreamTPMAll(t *testing.T) {
	fileName := getTPMFileName(t)

	// TODO: When the CI/CD environment supports updating the TPM,
	// expand this test with the SRK password.
	cf := createConfFile(t, []byte(fmt.Sprintf(jsTPMConfigAllFields,
		getTPMFileName(t), fileName, 19, "")))

	s, _ := RunServerWithConfig(cf)
	defer s.Shutdown()

	checkSendMessage(t, s)
	s.Shutdown()

	// restart the server.
	s, _ = RunServerWithConfig(cf)
	if s.Running() == false {
		t.Fatalf("Server should be running")
	}
	defer s.Shutdown()
	checkReceiveMessage(t, s)
}

func TestJetStreamInvalidConfig(t *testing.T) {
	cf := createConfFile(t, []byte(fmt.Sprintf(jsTPMConfigInvalidBothOptionsSet,
		getTPMFileName(t))))

	defer func() {
		r := recover()
		if r == nil {
			t.Fatalf("Expected server panic, unexpected start.")
		}
	}()
	s, _ := RunServerWithConfig(cf)
	if s.Running() == true {
		s.Shutdown()
		t.Fatalf("Server should NOT be running")
	}
}
