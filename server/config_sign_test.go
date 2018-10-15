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
	"os"
	"testing"

	"github.com/nats-io/nkeys"
)

func TestSignedConfigSingleFile(t *testing.T) {
	opts := &Options{
		ConfigKey:     "./configs/config.nkey",
		ConfigSigFile: "./configs/single_file_signed.sig",
	}
	err := opts.ProcessConfigFile("./configs/single_file_signed.conf")
	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}
}

func TestSignedConfigSingleFileBadSignature(t *testing.T) {
	opts := &Options{
		ConfigKey:     "./configs/config.nkey",
		ConfigSigFile: "./configs/single_file_signed_bad.sig",
	}
	err := opts.ProcessConfigFile("./configs/single_file_signed.conf")
	if err == nil {
		t.Fatalf("Expected error when using config with wrong signature")
	}
	if err != nkeys.ErrInvalidSignature {
		t.Errorf("Expected invalid signature error, got: %s'", err)
	}
}

func TestSignedConfigIncludes(t *testing.T) {
	opts := &Options{
		ConfigKey:     "./configs/config.nkey",
		ConfigSigFile: "./configs/included_files.sig",
	}
	err := opts.ProcessConfigFile("./configs/included_files.conf")
	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}
}

func TestSignedConfigIncludesBadSignature(t *testing.T) {
	opts := &Options{
		ConfigKey:     "./configs/config.nkey",
		ConfigSigFile: "./configs/included_files_bad.sig",
	}
	err := opts.ProcessConfigFile("./configs/included_files.conf")
	if err != nkeys.ErrInvalidSignature {
		t.Errorf("Expected invalid signature error, got: %s'", err)
	}
}

func TestSignedConfigIncludesWithIncludes(t *testing.T) {
	opts := &Options{
		ConfigKey:     "./configs/config.nkey",
		ConfigSigFile: "./configs/included_files_with_includes.sig",
	}
	err := opts.ProcessConfigFile("./configs/included_files_with_includes.conf")
	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}
}

func TestSignedConfigIncludesWithIncludesBadSignature(t *testing.T) {
	opts := &Options{
		ConfigKey:     "./configs/config.nkey",
		ConfigSigFile: "./configs/included_files_with_includes_bad.sig",
	}
	err := opts.ProcessConfigFile("./configs/included_files_with_includes.conf")
	if err != nkeys.ErrInvalidSignature {
		t.Errorf("Expected invalid signature error, got: %s'", err)
	}
}

func TestSignedConfigIncludesWithMissingSignature(t *testing.T) {
	opts := &Options{
		ConfigKey:     "./configs/config.nkey",
		ConfigSigFile: "./configs/included_files_with_missing_signature.sig",
	}
	err := opts.ProcessConfigFile("./configs/included_files_with_includes.conf")
	if err == nil {
		t.Fatalf("Expected error about file with missing signature")
	}

	got := err.Error()
	expected := `nats: found included file without signature: "configs/cncf_includes.conf"`
	if got != expected {
		t.Errorf("Expected error about missing file, got: %s'", err)
	}
}

func TestSignedConfigIncludesWithInvalidSigfile(t *testing.T) {
	conf := createConfFile(t, []byte("foo bar quux"))
	defer os.Remove(conf)

	opts := &Options{
		ConfigKey:     "./configs/config.nkey",
		ConfigSigFile: conf,
	}
	err := opts.ProcessConfigFile("./configs/single_file_signed.conf")
	if err == nil {
		t.Fatalf("Expected error about file with missing signature")
	}

	got := err.Error()
	expected := `nats: invalid sigfile`
	if got != expected {
		t.Errorf("Expected error about invalid signature, got: %s'", err)
	}
}
