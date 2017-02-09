package server

import (
	"net"
	"sync"
)

// LocalInternalClient is a trait interface.
// The net.Conn implementations of
// internal clients provided over
// the accept() callback (see Start below)
// should implement it to tell the server to ignore
// TLS and auth for internal clients.
//
type LocalInternalClient interface {
	IsInternal()
}

// iCli tracks the internal
// clients.
//
type iCli struct {
	configured []InternalClient
	mu         sync.Mutex
}

// InternalClient provides
// a plugin-like interface,
// supporting internal clients that live
// in-process with the Server
// on their own goroutines.
//
// An example of an internal client
// is the health monitoring client.
// In order to be effective, its lifetime
// must exactly match that of the
// server it monitors.
//
type InternalClient interface {

	// Name should return a readable
	// human name for the InternalClient;
	// it will be invoked as a part of
	// startup/shutdown/error logging.
	//
	Name() string

	// Start should run the client on
	// a background goroutine.
	//
	// The Server s will invoke Start()
	// as a part of its own init and setup.
	//
	// The info and opts pointers will be
	// viewable from an already locked Server
	// instance, and so can be read without
	// worrying about data races.
	//
	// Any returned error will be logged.
	// This will not prevent the Server
	// from calling Stop() on termination,
	// and Stop() must be expected (and
	// not block) no matter what.
	//
	// By returning an net.Conn the client
	// provides the server with the
	// equivalent of a Listen/Accept created
	// net.Conn for communication with
	// the client.
	//
	// The iclient should log using logger.
	//
	Start(info Info,
		opts Options,
		logger Logger) (net.Conn, error)

	// Stop should shutdown the goroutine(s)
	// of the internal client.
	// The Server will invoke Stop() as a part
	// of its own shutdown process, *even* if
	// Start() failed to start the background
	// goroutine. Authors should take care
	// to allow Stop() to be called even
	// on a failed start.
	//
	// Stop is expected not to block for long.
	//
	Stop()
}
