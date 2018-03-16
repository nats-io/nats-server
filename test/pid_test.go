// Copyright 2012-2018 The NATS Authors
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

package test

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"
)

func TestPidFile(t *testing.T) {
	opts := DefaultTestOptions

	tmpDir, err := ioutil.TempDir("", "_gnatsd")
	if err != nil {
		t.Fatal("Could not create tmp dir")
	}
	defer os.RemoveAll(tmpDir)

	file, err := ioutil.TempFile(tmpDir, "gnatsd:pid_")
	if err != nil {
		t.Fatalf("Unable to create temp file: %v", err)
	}
	file.Close()
	opts.PidFile = file.Name()

	s := RunServer(&opts)
	s.Shutdown()

	buf, err := ioutil.ReadFile(opts.PidFile)
	if err != nil {
		t.Fatalf("Could not read pid_file: %v", err)
	}
	if len(buf) <= 0 {
		t.Fatal("Expected a non-zero length pid_file")
	}

	pid := 0
	fmt.Sscanf(string(buf), "%d", &pid)
	if pid != os.Getpid() {
		t.Fatalf("Expected pid to be %d, got %d\n", os.Getpid(), pid)
	}
}
