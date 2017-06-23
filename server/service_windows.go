// Copyright 2012-2017 Apcera Inc. All rights reserved.

package server

import (
	"time"

	"golang.org/x/sys/windows/svc"
)

const serviceName = "gnatsd"

// winServiceWrapper implements the svc.Handler interface for implementing
// gnatsd as a Windows service.
type winServiceWrapper struct {
	server *Server
}

// Execute will be called by the package code at the start of
// the service, and the service will exit once Execute completes.
// Inside Execute you must read service change requests from r and
// act accordingly. You must keep service control manager up to date
// about state of your service by writing into s as required.
// args contains service name followed by argument strings passed
// to the service.
// You can provide service exit code in exitCode return parameter,
// with 0 being "no error". You can also indicate if exit code,
// if any, is service specific or not by using svcSpecificEC
// parameter.
func (w *winServiceWrapper) Execute(args []string, r <-chan svc.ChangeRequest,
	s chan<- svc.Status) (bool, uint32) {

	go w.server.Start()

	// Wait for accept loop(s) to be started
	if !w.server.ReadyForConnections(10 * time.Second) {
		// Failed to start.
		return false, 1
	}

	select {
	case s <- svc.Status{
		State:   svc.Running,
		Accepts: svc.AcceptStop | svc.AcceptShutdown,
	}:
	default:
	}

loop:
	for change := range r {
		switch change.Cmd {
		case svc.Stop, svc.Shutdown:
			w.server.Shutdown()
			break loop
		//case svc.ParamChange:
		//	if err := w.server.Reload(); err != nil {
		//		w.server.Errorf("Failed to reload server configuration: %s", err)
		//	}
		default:
			w.server.Debugf("Command not supported by service %s: %v", serviceName, change.Cmd)
		}
	}

	return false, 0
}

// Run starts the NATS server as a Windows service.
func Run(server *Server) {
	svc.Run(serviceName, &winServiceWrapper{server})
}
