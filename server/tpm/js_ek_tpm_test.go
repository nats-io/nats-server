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

package tpm

import (
	"os"
	"testing"
)

func getTempFile(t *testing.T) string {
	return t.TempDir() + "/jskeys.json"
}

func TestLoadJetStreamEncryptionKeyFromTPM(t *testing.T) {
	testFile := getTempFile(t)
	type args struct {
		srkPassword   string
		jsKeyFile     string
		jsKeyPassword string
		pcr           int
	}
	tests := []struct {
		name    string
		args    args
		clear   bool
		wantErr bool
	}{
		{"TestLoadJetStreamEncryptionKeyFromTPM-Load", args{"", testFile, "password", 22}, true, false},
		{"TestLoadJetStreamEncryptionKeyFromTPM-Read", args{"", testFile, "password", 22}, false, false},
		{"TestLoadJetStreamEncryptionKeyFromTPM-BadPass", args{"", testFile, "badpass", 22}, false, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.clear {
				os.Remove(tt.args.jsKeyFile)
			}
			_, err := LoadJetStreamEncryptionKeyFromTPM(tt.args.srkPassword, tt.args.jsKeyFile, tt.args.jsKeyPassword, tt.args.pcr)
			if (err != nil) != tt.wantErr {
				t.Errorf("LoadJetStreamEncryptionKeyFromTPM() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}

// TestLoadJetStreamEncryptionKeyFromTPMBasic tests the basic functionality.
// The first pass will create the keys and generate the js encryption key.
// the second pass will read the keys from disk, decrypt with the TPM (unseal),
// and return the same key.
func TestLoadJetStreamEncryptionKeyFromTPMBasic(t *testing.T) {
	testFile := getTempFile(t)

	// Create the key file.
	key1, err := LoadJetStreamEncryptionKeyFromTPM("", testFile, "password", 22)
	if err != nil {
		t.Errorf("LoadJetStreamEncryptionKeyFromTPM() failed: %v", err)
	}

	// Now obtain the newly generated key from the file.
	key2, err := LoadJetStreamEncryptionKeyFromTPM("", testFile, "password", 22)
	if err != nil {
		t.Errorf("LoadJetStreamEncryptionKeyFromTPM() failed: %v", err)
	}
	if key1 != key2 {
		t.Errorf("Keys should match")
	}
}
