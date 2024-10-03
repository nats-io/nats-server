// Copyright 2012-2019 The NATS Authors
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

//go:build !windows
// +build !windows

package server

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/logger"
	"github.com/nats-io/nats.go"
)

func TestSignalToReOpenLogFile(t *testing.T) {
	logFile := filepath.Join(t.TempDir(), "test.log")
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
	fileLog := logger.NewFileLogger(s.opts.LogFile, s.opts.Logtime, s.opts.Debug, s.opts.Trace, true, logger.LogUTC(s.opts.LogtimeUTC))
	s.SetLogger(fileLog, false, false)

	// Add a trace
	expectedStr := "This is a Notice"
	s.Noticef(expectedStr)
	buf, err := os.ReadFile(logFile)
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
	buf, err = os.ReadFile(logFile)
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
	expectedStr := "no nats-server processes running"
	if err.Error() != expectedStr {
		t.Fatalf("Error is incorrect.\nexpected: %s\ngot: %s", expectedStr, err.Error())
	}
}

func TestProcessSignalMultipleProcesses(t *testing.T) {
	sk := NewSignalsKit(2)
	pgrepBefore := pgrep
	pgrep = func() ([]byte, error) {
		return []byte(fmt.Sprintf("%d\n%d\n%d\n", sk.NonPids[0], sk.NonPids[1], sk.SelfPid)), nil
	}
	defer func() {
		pgrep = pgrepBefore
	}()

	err := ProcessSignal(CommandStop, "")
	if err == nil {
		t.Fatal("Expected error")
	}
	expectedStr := fmt.Sprintf("multiple nats-server processes running:\n%d\n%d", sk.NonPids[0], sk.NonPids[1])
	if err.Error() != expectedStr {
		t.Fatalf("Error is incorrect.\nexpected: %s\ngot: %s", expectedStr, err.Error())
	}
}

func TestProcessSignalMultipleProcessesGlob(t *testing.T) {
	sk := NewSignalsKit(2)
	pgrepBefore := pgrep
	pgrep = func() ([]byte, error) {
		return []byte(fmt.Sprintf("%d\n%d\n%d\n", sk.NonPids[0], sk.NonPids[1], sk.SelfPid)), nil
	}
	defer func() {
		pgrep = pgrepBefore
	}()

	err := ProcessSignal(CommandStop, "*")
	if err == nil {
		t.Fatal("Expected error")
	}
	expectedStr := fmt.Sprintf("\nsignal \"stop\" %d: no such process", sk.NonPids[0])
	expectedStr += fmt.Sprintf("\nsignal \"stop\" %d: no such process", sk.NonPids[1])
	if err.Error() != expectedStr {
		t.Fatalf("Error is incorrect.\nexpected: %s\ngot: %s", expectedStr, err.Error())
	}
}

func TestProcessSignalMultipleProcessesGlobPartial(t *testing.T) {
	sk := NewSignalsKit(2)
	pgrepBefore := pgrep
	pgrep = func() ([]byte, error) {
		return []byte(fmt.Sprintf("%d\n%d\n%d\n%d\n", sk.NonPids[0], sk.NonPids[1], sk.NonPats[0], sk.SelfPid)), nil
	}
	defer func() {
		pgrep = pgrepBefore
	}()

	err := ProcessSignal(CommandStop, sk.Pattern)
	if err == nil {
		t.Fatal("Expected error")
	}
	expectedStr := fmt.Sprintf("\nsignal \"stop\" %d: no such process", sk.NonPids[0])
	expectedStr += fmt.Sprintf("\nsignal \"stop\" %d: no such process", sk.NonPids[1])
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

func TestProcessSignalTermProcess(t *testing.T) {
	killBefore := kill
	called := false
	kill = func(pid int, signal syscall.Signal) error {
		called = true
		if pid != 123 {
			t.Fatalf("pid is incorrect.\nexpected: 123\ngot: %d", pid)
		}
		if signal != syscall.SIGTERM {
			t.Fatalf("signal is incorrect.\nexpected: interrupt\ngot: %v", signal)
		}
		return nil
	}
	defer func() {
		kill = killBefore
	}()

	if err := ProcessSignal(commandTerm, "123"); err != nil {
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

func TestProcessSignalLameDuckMode(t *testing.T) {
	killBefore := kill
	called := false
	kill = func(pid int, signal syscall.Signal) error {
		called = true
		if pid != 123 {
			t.Fatalf("pid is incorrect.\nexpected: 123\ngot: %d", pid)
		}
		if signal != syscall.SIGUSR2 {
			t.Fatalf("signal is incorrect.\nexpected: sigusr2\ngot: %v", signal)
		}
		return nil
	}
	defer func() {
		kill = killBefore
	}()

	if err := ProcessSignal(commandLDMode, "123"); err != nil {
		t.Fatalf("ProcessSignal failed: %v", err)
	}

	if !called {
		t.Fatal("Expected kill to be called")
	}
}

func TestProcessSignalTermDuringLameDuckMode(t *testing.T) {
	opts := &Options{
		Host:                "127.0.0.1",
		Port:                -1,
		NoSigs:              false,
		NoLog:               true,
		LameDuckDuration:    2 * time.Second,
		LameDuckGracePeriod: 1 * time.Second,
	}
	s := RunServer(opts)
	defer s.Shutdown()

	// Create single NATS Connection which will cause the server
	// to delay the shutdown.
	doneCh := make(chan struct{})
	nc, err := nats.Connect(fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port),
		nats.DisconnectHandler(func(*nats.Conn) {
			close(doneCh)
		}),
	)
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc.Close()

	// Trigger lame duck based shutdown.
	go s.lameDuckMode()

	// Wait for client to be disconnected.
	select {
	case <-doneCh:
		break
	case <-time.After(3 * time.Second):
		t.Fatalf("Timed out waiting for client to disconnect")
	}

	// Termination signal should not cause server to shutdown
	// while in lame duck mode already.
	syscall.Kill(syscall.Getpid(), syscall.SIGTERM)

	// Wait for server shutdown due to lame duck shutdown.
	timeoutCh := make(chan error)
	timer := time.AfterFunc(3*time.Second, func() {
		timeoutCh <- errors.New("Timed out waiting for server shutdown")
	})
	for range time.NewTicker(1 * time.Millisecond).C {
		select {
		case err := <-timeoutCh:
			t.Fatal(err)
		default:
		}

		if !s.isRunning() {
			timer.Stop()
			break
		}
	}
}

type SignalsKit struct {
	SelfPid int    // us
	Pattern string // pattern that doesn't match us
	NonPids []int  // non-pid's that match pattern
	NonPats []int  // non-pid's that don't match pattern
}

func NewSignalsKit(n int) *SignalsKit {
	sk := SignalsKit{
		SelfPid: os.Getpid(),
	}
	if sk.SelfPid < 10 { // get a real computer
		return nil
	}
	selfKlass := strconv.Itoa(sk.SelfPid)[:2]
	tallies := make(map[string][]int)
	var klass string
	for k := range NonProcsIter {
		if k < 10 {
			continue
		}
		klass = strconv.Itoa(k)[:2]
		if klass == selfKlass {
			continue
		}
		if len(tallies[klass]) < n {
			tallies[klass] = append(tallies[klass], k)
		}
	}
	for k, v := range tallies {
		if sk.Pattern == "" {
			sk.Pattern = k + "*"
			sk.NonPids = v
		} else if sk.NonPats == nil {
			sk.NonPats = v
		} else {
			break
		}
	}
	return &sk
}

func RunningProcs() ([]int, error) {
	list, err := os.ReadDir("/proc")
	if err != nil {
		return nil, err
	}
	pids := make([]int, 0, len(list))
	for _, d := range list {
		if s := d.Name(); s[0] < '0' || s[0] > '9' {
			continue
		} else if pid, err := strconv.Atoi(s); err != nil {
			return nil, err
		} else {
			pids = append(pids, pid)
		}
	}
	slices.Sort(pids)
	return pids, nil
}

func NonProcsIter(yield func(int) bool) {
	pids, err := RunningProcs()
	if err != nil {
		return
	}
	for k := range len(pids)-1 {
		l, u := pids[k], pids[k+1]
		for k := l+1; k < u; k++ {
			if !yield(k) {
				return
			}
		}
	}
}
