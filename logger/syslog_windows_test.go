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

// +build windows

package logger

import (
	"os/exec"
	"strings"
	"testing"

	"golang.org/x/sys/windows/svc/eventlog"
)

// Skips testing if we do not have privledges to run this test.
// This lets us skip the tests for general (non admin/system) users.
func checkPrivledges(t *testing.T) {
	src := "NATS-eventlog-testsource"
	defer eventlog.Remove(src)
	if err := eventlog.InstallAsEventCreate(src, eventlog.Info|eventlog.Error|eventlog.Warning); err != nil {
		if strings.Contains(err.Error(), "Access is denied") {
			t.Skip("skipping:  elevated privledges are required.")
		}
		// let the tests report other types of errors
	}
}

// lastLogEntryContains reads the last entry (/c:1 /rd:true) written
// to the event log by the NATS-Server source, returning true if the
// passed text was found, false otherwise.
func lastLogEntryContains(t *testing.T, text string) bool {
	var output []byte
	var err error

	cmd := exec.Command("wevtutil.exe", "qe", "Application", "/q:*[System[Provider[@Name='NATS-Server']]]",
		"/rd:true", "/c:1")
	if output, err = cmd.Output(); err != nil {
		t.Fatalf("Unable to execute command: %v", err)
	}
	return strings.Contains(string(output), text)
}

// TestSysLogger tests event logging on windows
func TestSysLogger(t *testing.T) {
	checkPrivledges(t)
	logger := NewSysLogger(false, false)
	if logger.debug {
		t.Fatalf("Expected %t, received %t\n", false, logger.debug)
	}

	if logger.trace {
		t.Fatalf("Expected %t, received %t\n", false, logger.trace)
	}
	logger.Noticef("%s", "Noticef")
	if !lastLogEntryContains(t, "[NOTICE]: Noticef") {
		t.Fatalf("missing log entry")
	}

	logger.Errorf("%s", "Errorf")
	if !lastLogEntryContains(t, "[ERROR]: Errorf") {
		t.Fatalf("missing log entry")
	}

	logger.Tracef("%s", "Tracef")
	if lastLogEntryContains(t, "Tracef") {
		t.Fatalf("should not contain log entry")
	}

	logger.Debugf("%s", "Debugf")
	if lastLogEntryContains(t, "Debugf") {
		t.Fatalf("should not contain log entry")
	}
}

// TestSysLoggerWithDebugAndTrace tests event logging
func TestSysLoggerWithDebugAndTrace(t *testing.T) {
	checkPrivledges(t)
	logger := NewSysLogger(true, true)
	if !logger.debug {
		t.Fatalf("Expected %t, received %t\n", true, logger.debug)
	}

	if !logger.trace {
		t.Fatalf("Expected %t, received %t\n", true, logger.trace)
	}

	logger.Tracef("%s", "Tracef")
	if !lastLogEntryContains(t, "[TRACE]: Tracef") {
		t.Fatalf("missing log entry")
	}

	logger.Debugf("%s", "Debugf")
	if !lastLogEntryContains(t, "[DEBUG]: Debugf") {
		t.Fatalf("missing log entry")
	}
}

// TestSysLoggerWithDebugAndTrace tests remote event logging
func TestRemoteSysLoggerWithDebugAndTrace(t *testing.T) {
	checkPrivledges(t)
	logger := NewRemoteSysLogger("", true, true)
	if !logger.debug {
		t.Fatalf("Expected %t, received %t\n", true, logger.debug)
	}

	if !logger.trace {
		t.Fatalf("Expected %t, received %t\n", true, logger.trace)
	}
	logger.Tracef("NATS %s", "[TRACE]: Remote Noticef")
	if !lastLogEntryContains(t, "Remote Noticef") {
		t.Fatalf("missing log entry")
	}
}

func TestSysLoggerFatalf(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			if !lastLogEntryContains(t, "[FATAL]: Fatalf") {
				t.Fatalf("missing log entry")
			}
		}
	}()

	checkPrivledges(t)
	logger := NewSysLogger(true, true)
	logger.Fatalf("%s", "Fatalf")
	t.Fatalf("did not panic when expected to")
}
