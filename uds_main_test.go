// Copyright 2026 Michael Utech
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

//go:build linux

package main

import (
	"bytes"
	"flag"
	"strings"
	"testing"

	"github.com/nats-io/nats-server/v2/server"
)

func TestUDS_HelpTextSync(t *testing.T) {
	// Ensure main.go usageStr stays in sync with flag definition in opts.go
	defer func() { server.FlagSnapshot = nil }()

	// Get the authoritative usage text from flag definition
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	fs.SetOutput(&bytes.Buffer{})
	server.ConfigureOptions(fs, []string{}, nil, nil, nil)

	f := fs.Lookup("uds")
	if f == nil {
		t.Fatal("--uds flag not registered")
	}
	definedUsage := f.Usage

	// Verify the defined usage text appears in usageStr (main.go)
	if !strings.Contains(usageStr, definedUsage) {
		t.Errorf("usageStr missing flag usage text.\nExpected to find: %q\nusageStr:\n%s", definedUsage, usageStr)
	}
}
