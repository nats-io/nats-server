// +build !windows
// Copyright 2012-2017 Apcera Inc. All rights reserved.

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

	go func() {
		for sig := range c {
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
		}
	}()
}

// ProcessSignal sends the given signal command to the given process. If pid is
// -1, this will send the signal to the single running instance of gnatsd. If
// multiple instances are running, it returns an error.
func ProcessSignal(command string, pid int) (err error) {
	if pid == -1 {
		pids, err := resolvePids()
		if err != nil {
			return err
		}
		if len(pids) == 0 {
			return errors.New("No gnatsd processes running")
		}
		if len(pids) > 1 {
			errStr := "Multiple gnatsd processes running:\n"
			prefix := ""
			for _, p := range pids {
				errStr += fmt.Sprintf("%s%d", prefix, p)
				prefix = "\n"
			}
			return errors.New(errStr)
		}
		pid = pids[0]
	}

	switch command {
	case "stop":
		err = syscall.Kill(pid, syscall.SIGKILL)
	case "quit":
		err = syscall.Kill(pid, syscall.SIGINT)
	case "reopen":
		err = syscall.Kill(pid, syscall.SIGUSR1)
	case "reload":
		err = syscall.Kill(pid, syscall.SIGHUP)
	default:
		err = fmt.Errorf("unknown signal %q", command)
	}
	return
}

// resolvePids returns the pids for all running gnatsd processes.
func resolvePids() ([]int, error) {
	// If pgrep isn't available, this will just bail out and the user will be
	// required to specify a pid.
	output, _ := exec.Command("pgrep", processName).Output()
	pidStrs := strings.Split(string(output), "\n")
	pids := make([]int, 0, len(pidStrs))
	for _, pidStr := range pidStrs {
		if pidStr == "" {
			continue
		}
		pid, err := strconv.Atoi(pidStr)
		if err != nil {
			return nil, errors.New("Unable to resolve pid")
		}
		pids = append(pids, pid)
	}
	return pids, nil
}
