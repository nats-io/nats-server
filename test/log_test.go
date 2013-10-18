// Copyright 2012-2013 Apcera Inc. All rights reserved.

package test

import (
	"io/ioutil"
	"os"
	"regexp"
	"testing"
)

var startRe = regexp.MustCompile(`\["Starting gnatsd version\s+([^\s]+)"\]\n`)

func TestLogFile(t *testing.T) {
	opts := DefaultTestOptions
	opts.NoLog = false
	opts.Logtime = false

	tmpDir, err := ioutil.TempDir("", "_gnatsd")
	if err != nil {
		t.Fatal("Could not create tmp dir")
	}
	defer os.RemoveAll(tmpDir)

	file, err := ioutil.TempFile(tmpDir, "gnatsd:log_")
	file.Close()
	opts.LogFile = file.Name()

	s := RunServer(&opts)
	s.Shutdown()

	buf, err := ioutil.ReadFile(opts.LogFile)
	if err != nil {
		t.Fatalf("Could not read logfile: %v", err)
	}
	if len(buf) <= 0 {
		t.Fatal("Expected a non-zero length logfile")
	}
	if !startRe.Match(buf) {
		t.Fatalf("Logfile did not match expected:  \n\tReceived:'%q'\n\tExpected:'%s'\n", buf, startRe)
	}
}
