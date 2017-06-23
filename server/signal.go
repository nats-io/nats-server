// +build !windows
// Copyright 2012-2017 Apcera Inc. All rights reserved.

package server

import (
	"os"
	"os/signal"
	"syscall"
)

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
