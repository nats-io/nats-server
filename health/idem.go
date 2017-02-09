package health

import (
	"fmt"
	"sync"
)

// Copyright (c) 2017 Jason E. Aten, Ph.D.
// https://github.com/glycerine/idem
// MIT license.

// idemCloseChan can have Close() called on it
// multiple times, and it will only close
// Chan once.
type idemCloseChan struct {
	Chan   chan bool
	closed bool
	mut    sync.Mutex
}

// Reinit re-allocates the Chan, assinging
// a new channel and reseting the state
// as if brand new.
func (c *idemCloseChan) Reinit() {
	c.mut.Lock()
	defer c.mut.Unlock()
	c.Chan = make(chan bool)
	c.closed = false
}

// newIdemCloseChan makes a new idemCloseChan.
func newIdemCloseChan() *idemCloseChan {
	return &idemCloseChan{
		Chan: make(chan bool),
	}
}

var errAlreadyClosed = fmt.Errorf("Chan already closed")

// Close returns errAlreadyClosed if it has been
// called before. It never closes IdemClose.Chan more
// than once, so it is safe to ignore the returned
// error value. Close() is safe for concurrent access by multiple
// goroutines. Close returns nil after the first time
// it is called.
func (c *idemCloseChan) Close() error {
	c.mut.Lock()
	defer c.mut.Unlock()
	if !c.closed {
		close(c.Chan)
		c.closed = true
		return nil
	}
	return errAlreadyClosed
}

// IsClosed tells you if Chan is already closed or not.
func (c *idemCloseChan) IsClosed() bool {
	c.mut.Lock()
	defer c.mut.Unlock()
	return c.closed
}

// halter helps shutdown a goroutine
type halter struct {
	// The owning goutine should call Done.Close() as its last
	// actual once it has received the ReqStop() signal.
	Done idemCloseChan

	// Other goroutines call ReqStop.Close() in order
	// to request that the owning goroutine stop immediately.
	// The owning goroutine should select on ReqStop.Chan
	// in order to recognize shutdown requests.
	ReqStop idemCloseChan
}

func newHalter() *halter {
	return &halter{
		Done:    *newIdemCloseChan(),
		ReqStop: *newIdemCloseChan(),
	}
}

// RequestStop closes the h.ReqStop channel
// if it has not already done so. Safe for
// multiple goroutine access.
func (h *halter) RequestStop() {
	h.ReqStop.Close()
}

// MarkDone closes the h.Done channel
// if it has not already done so. Safe for
// multiple goroutine access.
func (h *halter) MarkDone() {
	h.Done.Close()
}

// IsStopRequested returns true iff h.ReqStop has been Closed().
func (h *halter) IsStopRequested() bool {
	return h.ReqStop.IsClosed()
}

// IsDone returns true iff h.Done has been Closed().
func (h *halter) IsDone() bool {
	return h.Done.IsClosed()
}
