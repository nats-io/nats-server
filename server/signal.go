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
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
)

const processName = "gnatsd"

// Signal Handling
func (s *Server) handleSignals() {
	if s.getOpts().NoSigs {
		return
	}
	c := make(chan os.Signal, 1)

	signal.Notify(c, syscall.SIGINT, syscall.SIGUSR1, syscall.SIGHUP)

	s.grWG.Add(1)
	go func() {
		defer s.grWG.Done()
		for {
			select {
			case sig := <-c:
				s.Debugf("Trapped %q signal", sig)
				switch sig {
				case syscall.SIGINT:
					s.Noticef("Server Exiting..")
					os.Exit(0)
				case syscall.SIGUSR1:
					// File log re-open for rotating file logs.
					s.ReOpenLogFile()
				case syscall.SIGHUP:
					// Config reload.
					if err := s.Reload(); err != nil {
						s.Errorf("Failed to reload server configuration: %s", err)
					}
				}
			case <-s.quitCh:
				return
			}
		}
	}()
}

// ProcessSignal sends the given signal command to the given process. If pidStr
// is empty, this will send the signal to the single running instance of
// gnatsd. If multiple instances are running, it returns an error. This returns
// an error if the given process is not running or the command is invalid.
func ProcessSignal(command Command, pidStr string) error {
	var pid int
	if pidStr == "" {
		pids, err := resolvePids()
		if err != nil {
			return err
		}
		if len(pids) == 0 {
			return errors.New("no gnatsd processes running")
		}
		if len(pids) > 1 {
			errStr := "multiple gnatsd processes running:\n"
			prefix := ""
			for _, p := range pids {
				errStr += fmt.Sprintf("%s%d", prefix, p)
				prefix = "\n"
			}
			return errors.New(errStr)
		}
		pid = pids[0]
	} else {
		p, err := strconv.Atoi(pidStr)
		if err != nil {
			return fmt.Errorf("invalid pid: %s", pidStr)
		}
		pid = p
	}

	var err error
	switch command {
	case CommandStop:
		err = kill(pid, syscall.SIGKILL)
	case CommandQuit:
		err = kill(pid, syscall.SIGINT)
	case CommandReopen:
		err = kill(pid, syscall.SIGUSR1)
	case CommandReload:
		err = kill(pid, syscall.SIGHUP)
	default:
		err = fmt.Errorf("unknown signal %q", command)
	}
	return err
}

// resolvePids returns the pids for all running gnatsd processes.
func resolvePids() ([]int, error) {
	// If pgrep isn't available, this will just bail out and the user will be
	// required to specify a pid.
	output, err := pgrep()
	if err != nil {
		switch err.(type) {
		case *exec.ExitError:
			// ExitError indicates non-zero exit code, meaning no processes
			// found.
			break
		default:
			return nil, errors.New("unable to resolve pid, try providing one")
		}
	}
	var (
		myPid   = os.Getpid()
		pidStrs = strings.Split(string(output), "\n")
		pids    = make([]int, 0, len(pidStrs))
	)
	for _, pidStr := range pidStrs {
		if pidStr == "" {
			continue
		}
		pid, err := strconv.Atoi(pidStr)
		if err != nil {
			return nil, errors.New("unable to resolve pid, try providing one")
		}
		// Ignore the current process.
		if pid == myPid {
			continue
		}
		pids = append(pids, pid)
	}
	return pids, nil
}

var kill = func(pid int, signal syscall.Signal) error {
	return syscall.Kill(pid, signal)
}

var pgrep = func() ([]byte, error) {
	return exec.Command("pgrep", processName).Output()
}
