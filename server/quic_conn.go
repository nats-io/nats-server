// Copyright 2025 The NATS Authors
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

package server

import (
	"context"
	"net"
	"time"

	"github.com/quic-go/quic-go"
)

// quicConn wraps a QUIC stream to implement net.Conn interface
type quicConn struct {
	stream quic.Stream
	conn   quic.Connection
}

// newQUICConn creates a new QUIC connection wrapper
func newQUICConn(conn quic.Connection, stream quic.Stream) net.Conn {
	return &quicConn{
		stream: stream,
		conn:   conn,
	}
}

// Read reads data from the QUIC stream
func (c *quicConn) Read(b []byte) (n int, err error) {
	return c.stream.Read(b)
}

// Write writes data to the QUIC stream
func (c *quicConn) Write(b []byte) (n int, err error) {
	return c.stream.Write(b)
}

// Close closes the QUIC stream and connection
func (c *quicConn) Close() error {
	streamErr := c.stream.Close()
	connErr := c.conn.CloseWithError(0, "connection closed")
	if streamErr != nil {
		return streamErr
	}
	return connErr
}

// LocalAddr returns the local network address
func (c *quicConn) LocalAddr() net.Addr {
	return c.conn.LocalAddr()
}

// RemoteAddr returns the remote network address
func (c *quicConn) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

// SetDeadline sets the read and write deadlines
func (c *quicConn) SetDeadline(t time.Time) error {
	return c.stream.SetDeadline(t)
}

// SetReadDeadline sets the deadline for future Read calls
func (c *quicConn) SetReadDeadline(t time.Time) error {
	return c.stream.SetReadDeadline(t)
}

// SetWriteDeadline sets the deadline for future Write calls
func (c *quicConn) SetWriteDeadline(t time.Time) error {
	return c.stream.SetWriteDeadline(t)
}

// quicListener wraps a QUIC listener to implement net.Listener interface
type quicListener struct {
	listener *quic.Listener
	ctx      context.Context
	cancel   context.CancelFunc
}

// newQUICListener creates a new QUIC listener wrapper
func newQUICListener(l *quic.Listener) net.Listener {
	ctx, cancel := context.WithCancel(context.Background())
	return &quicListener{
		listener: l,
		ctx:      ctx,
		cancel:   cancel,
	}
}

// Accept waits for and returns the next connection to the listener
func (l *quicListener) Accept() (net.Conn, error) {
	conn, err := l.listener.Accept(l.ctx)
	if err != nil {
		return nil, err
	}

	// Accept a stream from the connection
	stream, err := conn.AcceptStream(l.ctx)
	if err != nil {
		conn.CloseWithError(0, "failed to accept stream")
		return nil, err
	}

	return newQUICConn(conn, stream), nil
}

// Close closes the listener
func (l *quicListener) Close() error {
	l.cancel()
	return l.listener.Close()
}

// Addr returns the listener's network address
func (l *quicListener) Addr() net.Addr {
	return l.listener.Addr()
}
