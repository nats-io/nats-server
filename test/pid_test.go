// Copyright 2012-2013 Apcera Inc. All rights reserved.

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
