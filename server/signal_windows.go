// Copyright 2012-2016 Apcera Inc. All rights reserved.

package server

import (
	"os"
	"os/signal"
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

// ProcessSignal sends the given signal command to the given process. If pid is
// -1, this will send the signal to the single running instance of gnatsd. If
// multiple instances are running, it returns an error.
func ProcessSignal(command string, pid int) error {
	// TODO
	return errors.New("TODO")
}
