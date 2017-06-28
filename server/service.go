// +build !windows
// Copyright 2012-2017 Apcera Inc. All rights reserved.

package server

// Run starts the NATS server. This wrapper function allows Windows to add a
// hook for running NATS as a service.
func Run(server *Server) error {
	server.Start()
	return nil
}

// isWindowsService indicates if NATS is running as a Windows service.
func isWindowsService() bool {
	return false
}
