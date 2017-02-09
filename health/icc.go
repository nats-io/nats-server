package health

import (
	"net"
)

// Icc allows the server to
// detect a net.Conn as
// an internal client connection
// by checking if it implements the
// LocalInternalClient interface.
//
type Icc struct {
	*net.TCPConn
}

// IsInternal satisfy LocalInternalClient interface
func (c *Icc) IsInternal() {}

// NewInternalClientPair constructs a client/server
// pair that wrap tcp endpoints in Icc to let
// the server recognized them as internal.
//
func NewInternalClientPair() (cli, srv *Icc, err error) {

	lsn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return
	}

	srvDone := make(chan struct{})
	go func() {
		s, err2 := lsn.Accept()
		if err2 == nil {
			srv = &Icc{TCPConn: s.(*net.TCPConn)}
		} else {
			err = err2
		}
		lsn.Close()
		close(srvDone)
	}()

	addr := lsn.Addr()
	c, err3 := net.Dial(addr.Network(), addr.String())
	<-srvDone

	if err3 != nil {
		err = err3
		if srv != nil {
			srv.Close()
			srv = nil
		}
		return
	}
	cli = &Icc{TCPConn: c.(*net.TCPConn)}
	// INVAR: cli ok.
	if err != nil {
		cli.Close()
		cli = nil
		return
	}
	// INVAR: srv ok.
	return
}
