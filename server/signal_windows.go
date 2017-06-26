// Copyright 2012-2016 Apcera Inc. All rights reserved.

package server

import (
	"errors"
	"fmt"
	"os"
	"os/signal"
	"time"

	"golang.org/x/sys/windows/svc"
	"golang.org/x/sys/windows/svc/mgr"
)

// Signal Handling
func (s *Server) handleSignals() {
	if s.getOpts().NoSigs {
		return
	}
	c := make(chan os.Signal, 1)

	signal.Notify(c, os.Interrupt)

	go func() {
		for sig := range c {
			s.Debugf("Trapped %q signal", sig)
			s.Noticef("Server Exiting..")
			os.Exit(0)
		}
	}()
}

// ProcessSignal sends the given signal command to the running gnatsd service.
// If pid is not -1 or if there is no gnatsd service running, it returns an
// error.
func ProcessSignal(command string, pid int) error {
	if pid != -1 {
		return errors.New("cannot signal pid on Windows")
	}

	m, err := mgr.Connect()
	if err != nil {
		return err
	}
	defer m.Disconnect()

	s, err := m.OpenService(serviceName)
	if err != nil {
		return fmt.Errorf("could not access service: %v", err)
	}
	defer s.Close()

	var (
		cmd svc.Cmd
		to  svc.State
	)

	switch command {
	case "stop":
		cmd = svc.Stop
		to = svc.Stopped
	case "quit":
		cmd = svc.Shutdown
		to = svc.Stopped
	case "reopen":
		cmd = svc.Cmd(256)
		to = svc.Running
	case "reload":
		cmd = svc.ParamChange
		to = svc.Running
	default:
		return fmt.Errorf("unknown signal %q", command)
	}

	status, err := s.Control(cmd)
	if err != nil {
		return fmt.Errorf("could not send control=%d: %v", cmd, err)
	}

	timeout := time.Now().Add(10 * time.Second)
	for status.State != to {
		if timeout.Before(time.Now()) {
			return fmt.Errorf("timeout waiting for service to go to state=%d", to)
		}
		time.Sleep(300 * time.Millisecond)
		status, err = s.Query()
		if err != nil {
			return fmt.Errorf("could not retrieve service status: %v", err)
		}
	}

	return nil
}
