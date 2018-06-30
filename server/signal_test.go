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

// +build !windows

package server

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/nats-io/gnatsd/logger"
)

func TestSignalToReOpenLogFile(t *testing.T) {
	logFile := "test.log"
	defer os.Remove(logFile)
	defer os.Remove(logFile + ".bak")
	opts := &Options{
		Host:    "127.0.0.1",
		Port:    -1,
		NoSigs:  false,
		LogFile: logFile,
	}
	s := RunServer(opts)
	defer s.SetLogger(nil, false, false)
	defer s.Shutdown()

	// Set the file log
	fileLog := logger.NewFileLogger(s.opts.LogFile, s.opts.Logtime, s.opts.Debug, s.opts.Trace, true)
	s.SetLogger(fileLog, false, false)

	// Add a trace
	expectedStr := "This is a Notice"
	s.Noticef(expectedStr)
	buf, err := ioutil.ReadFile(logFile)
	if err != nil {
		t.Fatalf("Error reading file: %v", err)
	}
	if !strings.Contains(string(buf), expectedStr) {
		t.Fatalf("Expected log to contain %q, got %q", expectedStr, string(buf))
	}
	// Rename the file
	if err := os.Rename(logFile, logFile+".bak"); err != nil {
		t.Fatalf("Unable to rename file: %v", err)
	}
	// This should cause file to be reopened.
	syscall.Kill(syscall.Getpid(), syscall.SIGUSR1)
	// Wait a bit for action to be performed
	time.Sleep(500 * time.Millisecond)
	buf, err = ioutil.ReadFile(logFile)
	if err != nil {
		t.Fatalf("Error reading file: %v", err)
	}
	expectedStr = "File log re-opened"
	if !strings.Contains(string(buf), expectedStr) {
		t.Fatalf("Expected log to contain %q, got %q", expectedStr, string(buf))
	}
}

func TestSignalToReloadConfig(t *testing.T) {
	opts, err := ProcessConfigFile("./configs/reload/basic.conf")
	if err != nil {
		t.Fatalf("Error processing config file: %v", err)
	}
	opts.NoLog = true
	s := RunServer(opts)
	defer s.Shutdown()

	// Repeat test to make sure that server services signals more than once...
	for i := 0; i < 2; i++ {
		loaded := s.ConfigTime()

		// Wait a bit to ensure ConfigTime changes.
		time.Sleep(5 * time.Millisecond)

		// This should cause config to be reloaded.
		syscall.Kill(syscall.Getpid(), syscall.SIGHUP)
		// Wait a bit for action to be performed
		time.Sleep(500 * time.Millisecond)

		if reloaded := s.ConfigTime(); !reloaded.After(loaded) {
			t.Fatalf("ConfigTime is incorrect.\nexpected greater than: %s\ngot: %s", loaded, reloaded)
		}
	}
}

func TestProcessSignalNoProcesses(t *testing.T) {
	pgrepBefore := pgrep
	pgrep = func() ([]byte, error) {
		return nil, &exec.ExitError{}
	}
	defer func() {
		pgrep = pgrepBefore
	}()

	err := ProcessSignal(CommandStop, "")
	if err == nil {
		t.Fatal("Expected error")
	}
	expectedStr := "no gnatsd processes running"
	if err.Error() != expectedStr {
		t.Fatalf("Error is incorrect.\nexpected: %s\ngot: %s", expectedStr, err.Error())
	}
}

func TestProcessSignalMultipleProcesses(t *testing.T) {
	pid := os.Getpid()
	pgrepBefore := pgrep
	pgrep = func() ([]byte, error) {
		return []byte(fmt.Sprintf("123\n456\n%d\n", pid)), nil
	}
	defer func() {
		pgrep = pgrepBefore
	}()

	err := ProcessSignal(CommandStop, "")
	if err == nil {
		t.Fatal("Expected error")
	}
	expectedStr := "multiple gnatsd processes running:\n123\n456"
	if err.Error() != expectedStr {
		t.Fatalf("Error is incorrect.\nexpected: %s\ngot: %s", expectedStr, err.Error())
	}
}

func TestProcessSignalPgrepError(t *testing.T) {
	pgrepBefore := pgrep
	pgrep = func() ([]byte, error) {
		return nil, errors.New("error")
	}
	defer func() {
		pgrep = pgrepBefore
	}()

	err := ProcessSignal(CommandStop, "")
	if err == nil {
		t.Fatal("Expected error")
	}
	expectedStr := "unable to resolve pid, try providing one"
	if err.Error() != expectedStr {
		t.Fatalf("Error is incorrect.\nexpected: %s\ngot: %s", expectedStr, err.Error())
	}
}

func TestProcessSignalPgrepMangled(t *testing.T) {
	pgrepBefore := pgrep
	pgrep = func() ([]byte, error) {
		return []byte("12x"), nil
	}
	defer func() {
		pgrep = pgrepBefore
	}()

	err := ProcessSignal(CommandStop, "")
	if err == nil {
		t.Fatal("Expected error")
	}
	expectedStr := "unable to resolve pid, try providing one"
	if err.Error() != expectedStr {
		t.Fatalf("Error is incorrect.\nexpected: %s\ngot: %s", expectedStr, err.Error())
	}
}

func TestProcessSignalResolveSingleProcess(t *testing.T) {
	pid := os.Getpid()
	pgrepBefore := pgrep
	pgrep = func() ([]byte, error) {
		return []byte(fmt.Sprintf("123\n%d\n", pid)), nil
	}
	defer func() {
		pgrep = pgrepBefore
	}()
	killBefore := kill
	called := false
	kill = func(pid int, signal syscall.Signal) error {
		called = true
		if pid != 123 {
			t.Fatalf("pid is incorrect.\nexpected: 123\ngot: %d", pid)
		}
		if signal != syscall.SIGKILL {
			t.Fatalf("signal is incorrect.\nexpected: killed\ngot: %v", signal)
		}
		return nil
	}
	defer func() {
		kill = killBefore
	}()

	if err := ProcessSignal(CommandStop, ""); err != nil {
		t.Fatalf("ProcessSignal failed: %v", err)
	}

	if !called {
		t.Fatal("Expected kill to be called")
	}
}

func TestProcessSignalInvalidCommand(t *testing.T) {
	err := ProcessSignal(Command("invalid"), "123")
	if err == nil {
		t.Fatal("Expected error")
	}
	expectedStr := "unknown signal \"invalid\""
	if err.Error() != expectedStr {
		t.Fatalf("Error is incorrect.\nexpected: %s\ngot: %s", expectedStr, err.Error())
	}
}

func TestProcessSignalInvalidPid(t *testing.T) {
	err := ProcessSignal(CommandStop, "abc")
	if err == nil {
		t.Fatal("Expected error")
	}
	expectedStr := "invalid pid: abc"
	if err.Error() != expectedStr {
		t.Fatalf("Error is incorrect.\nexpected: %s\ngot: %s", expectedStr, err.Error())
	}
}

func TestProcessSignalQuitProcess(t *testing.T) {
	killBefore := kill
	called := false
	kill = func(pid int, signal syscall.Signal) error {
		called = true
		if pid != 123 {
			t.Fatalf("pid is incorrect.\nexpected: 123\ngot: %d", pid)
		}
		if signal != syscall.SIGINT {
			t.Fatalf("signal is incorrect.\nexpected: interrupt\ngot: %v", signal)
		}
		return nil
	}
	defer func() {
		kill = killBefore
	}()

	if err := ProcessSignal(CommandQuit, "123"); err != nil {
		t.Fatalf("ProcessSignal failed: %v", err)
	}

	if !called {
		t.Fatal("Expected kill to be called")
	}
}

func TestProcessSignalReopenProcess(t *testing.T) {
	killBefore := kill
	called := false
	kill = func(pid int, signal syscall.Signal) error {
		called = true
		if pid != 123 {
			t.Fatalf("pid is incorrect.\nexpected: 123\ngot: %d", pid)
		}
		if signal != syscall.SIGUSR1 {
			t.Fatalf("signal is incorrect.\nexpected: user defined signal 1\ngot: %v", signal)
		}
		return nil
	}
	defer func() {
		kill = killBefore
	}()

	if err := ProcessSignal(CommandReopen, "123"); err != nil {
		t.Fatalf("ProcessSignal failed: %v", err)
	}

	if !called {
		t.Fatal("Expected kill to be called")
	}
}

func TestProcessSignalReloadProcess(t *testing.T) {
	killBefore := kill
	called := false
	kill = func(pid int, signal syscall.Signal) error {
		called = true
		if pid != 123 {
			t.Fatalf("pid is incorrect.\nexpected: 123\ngot: %d", pid)
		}
		if signal != syscall.SIGHUP {
			t.Fatalf("signal is incorrect.\nexpected: hangup\ngot: %v", signal)
		}
		return nil
	}
	defer func() {
		kill = killBefore
	}()

	if err := ProcessSignal(CommandReload, "123"); err != nil {
		t.Fatalf("ProcessSignal failed: %v", err)
	}

	if !called {
		t.Fatal("Expected kill to be called")
	}
}
