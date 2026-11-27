// Copyright 2012-2025 The NATS Authors
// Copyright 2026 JEMES contributors
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

package logger

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/internal/filerename"
	"golang.org/x/sys/windows"
	"golang.org/x/sys/windows/svc/eventlog"
)

// Skips testing if we do not have privledges to run this test.
// This lets us skip the tests for general (non admin/system) users.
func checkPrivledges(t *testing.T) {
	src := "NATS-eventlog-testsource"
	defer eventlog.Remove(src)
	if err := eventlog.InstallAsEventCreate(src, eventlog.Info|eventlog.Error|eventlog.Warning); err != nil {
		if strings.Contains(err.Error(), "Access is denied") {
			// Skip this test because elevated privileges are required.
			t.SkipNow()
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

// TestRemoteSysLoggerWithDebugAndTrace tests remote event logging
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

func TestWindowsLogRotationRetainsActiveLog(t *testing.T) {
	name := filepath.Join(t.TempDir(), "nats.log")
	l := NewFileLogger(name, false, false, false, false)
	t.Cleanup(func() { l.Close() })
	if err := l.SetSizeLimit(512); err != nil {
		t.Fatal(err)
	}
	if err := l.SetMaxNumFiles(2); err != nil {
		t.Fatal(err)
	}
	// A failed rotation must not purge the previous backup.
	previous := name + ".2022.01.01.12.00.00.000000000"
	if err := os.WriteFile(previous, []byte("previous"), 0600); err != nil {
		t.Fatal(err)
	}
	extra := name + ".2023.01.01.12.00.00.000000000"
	if err := os.WriteFile(extra, []byte("extra"), 0600); err != nil {
		t.Fatal(err)
	}
	held, err := os.Open(name)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { held.Close() })
	line := bytes.Repeat([]byte("x"), 513)
	n, err := l.fl.Write(line)
	var linkErr *os.LinkError
	if n != len(line) || !errors.As(err, &linkErr) {
		t.Fatalf("rotation error = %v; wrote %d of %d bytes", err, n, len(line))
	}
	l.Noticef("after failed rotation")
	state, err := os.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.HasPrefix(state, line) || !bytes.Contains(state, []byte("after failed rotation")) ||
		bytes.Count(state, []byte("File rename retry:")) != 1 || !bytes.Contains(state, []byte("recovered=false")) ||
		bytes.Contains(state, []byte("backup saved")) || l.fl.limit <= l.fl.out {
		t.Fatalf("unexpected failed rotation, limit=%d size=%d: %s", l.fl.limit, l.fl.out, state)
	}
	if _, err := os.Stat(previous); err != nil {
		t.Fatalf("existing backup was purged on rotation failure: %v", err)
	}
	if err := held.Close(); err != nil {
		t.Fatal(err)
	}
	l.Noticef("%s", bytes.Repeat([]byte("a"), int(l.fl.limit-l.fl.out+1)))
	state, err = os.ReadFile(name)
	if err != nil || !bytes.Contains(state, []byte("backup saved")) || l.fl.limit != l.fl.olimit {
		t.Fatalf("rotation did not recover, limit=%d: %s, %v", l.fl.limit, state, err)
	}
}

func TestRenameRealHandle(t *testing.T) {
	for _, scenario := range []string{"source", "destination", "directory child", "persistent destination", "source removed during retry"} {
		t.Run(scenario, func(t *testing.T) {
			dir := t.TempDir()
			src, dst := filepath.Join(dir, "old"), filepath.Join(dir, "new")
			locked := src
			if scenario == "directory child" {
				if err := os.Mkdir(src, 0700); err != nil {
					t.Fatal(err)
				}
				locked = filepath.Join(src, "child")
			} else if scenario != "source" {
				locked = dst
			}
			if err := os.WriteFile(locked, []byte("existing"), 0600); err != nil {
				t.Fatal(err)
			}
			if locked == dst {
				if err := os.WriteFile(src, []byte("incoming"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			h, err := os.Open(locked)
			if err != nil {
				t.Fatal(err)
			}
			defer h.Close()
			persistent := scenario == "persistent destination"
			var waits []time.Duration
			result, err := filerename.Rename(src, dst, func(d time.Duration) {
				waits = append(waits, d/time.Millisecond)
				if !persistent {
					h.Close()
					if scenario == "source removed during retry" {
						if err := os.Remove(src); err != nil {
							t.Fatal(err)
						}
					}
				}
			})
			t.Logf("real handle %s: Win32=%d attempts=%d err=%v", scenario, result.FirstCode, result.Attempts, err)
			if result.FirstCode != 5 && result.FirstCode != 32 {
				t.Fatal("handle did not exercise the retry path")
			}
			wantWaits := []time.Duration{50}
			if persistent {
				wantWaits = []time.Duration{50, 100, 200, 400}
			}
			if !slices.Equal(waits, wantWaits) {
				t.Fatalf("retry delays (ms) = %v, want %v", waits, wantWaits)
			}
			if persistent {
				var link *os.LinkError
				if result.Attempts != 5 || !errors.As(err, &link) || !errors.Is(err, windows.Errno(result.FirstCode)) {
					t.Fatalf("lost final OS error: %v", err)
				}
				for path, want := range map[string]string{src: "incoming", dst: "existing"} {
					b, e := os.ReadFile(path)
					if e != nil || string(b) != want {
						t.Fatalf("%s: %q, %v", path, b, e)
					}
				}
			} else if scenario == "source removed during retry" {
				if !errors.Is(err, os.ErrNotExist) || result.Attempts != 2 {
					t.Fatalf("unexpected result after source removal: %+v %v", result, err)
				}
			} else if err != nil || result.Attempts != 2 {
				t.Fatalf("failed to recover: %+v %v", result, err)
			}
		})
	}
}

func TestRenameReadOnlyDestination(t *testing.T) {
	dir := t.TempDir()
	src, dst := filepath.Join(dir, "old"), filepath.Join(dir, "new")
	for _, path := range []string{src, dst} {
		if err := os.WriteFile(path, []byte(path), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Chmod(dst, 0400); err != nil {
		t.Fatal(err)
	}
	result, err := filerename.Rename(src, dst, func(time.Duration) {
		if err := os.Chmod(dst, 0600); err != nil {
			t.Fatal(err)
		}
	})
	if err != nil || result.FirstCode != uint32(windows.ERROR_ACCESS_DENIED) || result.Attempts != 2 {
		t.Fatalf("read-only destination: %+v %v", result, err)
	}
	var log string
	result.Report(func(f string, args ...any) { log = fmt.Sprintf(f, args...) }, err)
	if !strings.Contains(log, "recovered=true") {
		t.Fatalf("unexpected retry log: %q", log)
	}
	// Reversing the rename succeeds immediately. Repeating it then has no source.
	for i, want := range []error{nil, os.ErrNotExist} {
		result, err := filerename.Rename(dst, src, func(time.Duration) { t.Fatal("unexpected retry") })
		if !errors.Is(err, want) || result.Attempts != 1 || result.FirstCode != 0 {
			t.Fatalf("rename %d: %+v %v", i, result, err)
		}
		result.Report(func(string, ...any) { t.Fatal("unexpected retry log") }, err)
	}
}
