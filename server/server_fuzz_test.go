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
	"bufio"
	"crypto/tls"
	"fmt"
	"io"
	"math"
	"net"
	"testing"
	"time"
)

const (
	tlsHandshake   byte = 22
	tlsClientHello byte = 1
)

type ClientHelloInjector struct {
	sock       io.ReadWriteCloser
	tlsVersion uint16
	buf        []byte
}

func NewClientHelloInjector(s io.ReadWriteCloser, tlsVer uint16, b []byte) *ClientHelloInjector {
	return &ClientHelloInjector{
		sock:       s,
		tlsVersion: tlsVer,
		buf:        b,
	}
}

func (i *ClientHelloInjector) inject(b []byte) []byte {
	if !(b != nil && b[0] == tlsHandshake) {
		return b
	}

	hsLen := (uint16(b[3]) << 8) + uint16(b[4])

	if !(hsLen > 0 && b[5] == tlsClientHello) {
		return b
	}

	// fuzz tls version in client hello
	b[9] = uint8(i.tlsVersion >> 8)
	b[10] = uint8(i.tlsVersion & 0xFF)

	// Go to begin of random opaque
	offset := 11

	randomOpaque := b[offset : offset+32]

	copy(randomOpaque, i.buf)

	offset += 32

	sessionIDLen := b[offset]

	// Skip session id len + opaque
	offset += 1 + int(sessionIDLen)

	cypherSuiteLen := (uint16(b[offset]) << 8) + uint16(b[offset+1])

	// Skip cypherSuiteLen
	offset += 2

	cupherSuites := b[offset : offset+int(cypherSuiteLen)]

	// Leave unchanged if i.cypherSuites empty
	copy(cupherSuites, i.buf)

	// Skip cypherSuites
	offset += int(cypherSuiteLen)

	// Skip CompressionMethod
	offset += 2

	extensionsLen := (uint16(b[offset]) << 8) + uint16(b[offset+1])

	// Skip extensions length
	offset += 2

	// Extensions slice. Stub for future use
	_ = b[offset : offset+int(extensionsLen)]

	return b
}

func (i *ClientHelloInjector) Write(b []byte) (int, error) {
	return i.sock.Write(i.inject(b))
}

func (i *ClientHelloInjector) Read(b []byte) (int, error) {
	return i.sock.Read(b)
}

func (i *ClientHelloInjector) Close() error {
	return i.sock.Close()
}

type FakeSocket struct {
	sockName string
	buf      []byte
	data     chan []byte
	done     chan struct{}
}

func NewFakeSocket(name string, capacity int) *FakeSocket {
	return &FakeSocket{
		sockName: name,
		data:     make(chan []byte, capacity),
		done:     make(chan struct{}),
	}
}

func (s *FakeSocket) Write(b []byte) (int, error) {
	select {
	case s.data <- b:
		return len(b), nil
	case <-s.done:
		return 0, net.ErrClosed
	}
}

func (s *FakeSocket) readChunk(b []byte) (int, error) {
	n := copy(b, s.buf)
	s.buf = s.buf[n:]
	return n, nil
}

func (s *FakeSocket) Read(b []byte) (int, error) {
	if len(s.buf) > 0 {
		return s.readChunk(b)
	}

	select {
	case buf, ok := <-s.data:
		if !ok {
			return 0, nil
		}
		s.buf = buf
		return s.readChunk(b)
	case <-s.done:
		return 0, nil
	}
}

func (s *FakeSocket) Close() error {
	close(s.done)
	return nil
}

type FakeConn struct {
	local  io.ReadWriteCloser
	remote io.ReadWriteCloser
}

func NewFakeConn(loc io.ReadWriteCloser, rem io.ReadWriteCloser) *FakeConn {
	return &FakeConn{
		local:  loc,
		remote: rem,
	}
}

func (c *FakeConn) Read(b []byte) (int, error) {
	return c.local.Read(b)
}

func (c *FakeConn) Write(b []byte) (int, error) {
	return c.remote.Write(b)
}

func (c *FakeConn) Close() error {
	return c.local.Close()
}

func (c *FakeConn) LocalAddr() net.Addr {
	return &net.TCPAddr{IP: net.IP{127, 0, 0, 1}, Port: 4222, Zone: ""}
}

func (c *FakeConn) RemoteAddr() net.Addr {
	return &net.TCPAddr{IP: net.IP{127, 0, 0, 1}, Port: 4222, Zone: ""}
}

func (c *FakeConn) SetDeadline(t time.Time) error {
	return nil
}

func (c *FakeConn) SetReadDeadline(t time.Time) error {
	return nil
}

func (c *FakeConn) SetWriteDeadline(t time.Time) error {
	return nil
}

type FakeListener struct {
	ch        chan *FakeConn
	acceptErr error
}

func NewFakeListener() *FakeListener {
	return &FakeListener{
		ch:        make(chan *FakeConn),
		acceptErr: nil,
	}
}

func (ln *FakeListener) Accept() (c net.Conn, err error) {
	return <-ln.ch, ln.acceptErr
}

func (ln *FakeListener) Close() error {
	ln.acceptErr = io.EOF
	close(ln.ch)
	return nil
}

func (ln *FakeListener) Addr() net.Addr {
	return &net.TCPAddr{IP: net.IP{127, 0, 0, 1}, Port: 4222, Zone: ""}
}

func getTlsVersion(useTls13 bool) uint16 {
	if useTls13 {
		return tls.VersionTLS13
	}

	return tls.VersionTLS12
}

func corruptCert(crt []byte, i uint16) []byte {
	crt[int(i)%len(crt)] ^= 0xFF
	return crt
}

func runServerWithListener(ln net.Listener, opts *Options) *Server {
	if opts == nil {
		opts = DefaultOptions()
	}
	s, err := NewServer(opts)
	if err != nil || s == nil {
		panic(fmt.Sprintf("No NATS Server object returned: %v", err))
	}

	if !opts.NoLog {
		s.ConfigureLogger()
	}

	s.listener = ln
	s.listenerErr = nil

	// Run server in Go routine.
	s.Start()

	// Wait for accept loop(s) to be started
	if err := s.readyForConnections(10 * time.Second); err != nil {
		panic(err)
	}
	return s
}

type MathRandReader byte

func (m MathRandReader) Read(buf []byte) (int, error) {
	for i := range buf {
		buf[i] = byte(m)
	}
	return len(buf), nil
}

// FuzzServerTLS performs fuzz testing of the NATS server's TLS handshake implementation.
// It verifies the server's ability to handle various TLS connection scenarios, including:
//   - Different TLS versions (1.2 and 1.3)
//   - Malformed/mutated client certificates
//   - Corrupted TLS handshake data
//   - Edge cases in the TLS negotiation process
//
// Test Setup:
//   - Configures a server with mutual TLS authentication using test certificates
//   - Creates a client with configurable TLS parameters
//   - Uses fake network connections to inject test cases
//
// Fuzzing Parameters:
//   - useTls13:       Boolean flag to test TLS 1.3 (true) or TLS 1.2 (false)
//   - tlsVer:         TLS version number to use in ClientHello
//   - buf:            Additional bytes to inject into ClientHello message
//   - corruptCertOffset: Position to corrupt in client certificate (MaxUint16 = no corruption)
//
// Expectations
// The server should either:
//   - Successfully complete the TLS handshake and protocol exchange, or
//   - Cleanly reject invalid connections without crashing
func FuzzServerTLS(f *testing.F) {
	srvTc := &TLSConfigOpts{
		CertFile: "../test/configs/certs/tlsauth/server.pem",
		KeyFile:  "../test/configs/certs/tlsauth/server-key.pem",
		CaFile:   "../test/configs/certs/tlsauth/ca.pem",
		Insecure: false,
		Verify:   true,
	}

	srvTlsCfg, err := GenTLSConfig(srvTc)
	if err != nil {
		f.Fatalf("Error generating server tls config: %v", err)
	}

	opts := &Options{
		Host:              "127.0.0.1",
		Port:              4222,
		NoLog:             true,
		NoSigs:            true,
		Debug:             true,
		Trace:             true,
		TLSHandshakeFirst: true,
		AllowNonTLS:       false,
		JetStream:         false,
		TLSConfig:         srvTlsCfg,
		CheckConfig:       false,
	}

	clientCerts, err := tls.LoadX509KeyPair("../test/configs/certs/tlsauth/client.pem", "../test/configs/certs/tlsauth/client-key.pem")
	if err != nil {
		f.Fatalf("client1 certificate load error: %s", err)
	}

	clientTlsCfg := &tls.Config{
		InsecureSkipVerify: true,
		Rand:               MathRandReader(0),
		Time:               func() time.Time { return time.Date(2025, 1, 1, 1, 1, 1, 1, nil) },
	}

	tlsVer := uint16(0x0303)

	corpuses := []struct {
		useTls13          bool
		clientHelloTlsVer uint16
		buf               []byte
		corruptCertOffset uint16
	}{
		{useTls13: false, clientHelloTlsVer: tlsVer, buf: []byte{}, corruptCertOffset: math.MaxUint16},
		{useTls13: true, clientHelloTlsVer: tlsVer, buf: []byte{}, corruptCertOffset: math.MaxUint16},
	}

	for _, crp := range corpuses {
		f.Add(crp.useTls13, crp.clientHelloTlsVer, crp.buf, crp.corruptCertOffset)
	}

	f.Fuzz(func(t *testing.T, useTls13 bool, tlsVer uint16, buf []byte, corruptCertOffset uint16) {
		ln := NewFakeListener()
		s := runServerWithListener(ln, opts.Clone())
		defer s.Shutdown()

		clientSocket := NewFakeSocket("CLIENT", 8)
		serverSocket := NewClientHelloInjector(NewFakeSocket("SERVER", 8), tlsVer, buf)

		clientConn := NewFakeConn(clientSocket, serverSocket)
		serverConn := NewFakeConn(serverSocket, clientSocket)

		// Connect to server
		ln.ch <- serverConn

		tlsVersion := getTlsVersion(useTls13)

		tlsCfg := clientTlsCfg.Clone()
		tlsCfg.GetClientCertificate = func(info *tls.CertificateRequestInfo) (*tls.Certificate, error) {
			if corruptCertOffset == math.MaxUint16 {
				t.Log("Leave certificate unchanged")
				return &clientCerts, nil
			}

			origCert := clientCerts.Certificate[0]
			newCert := make([]byte, len(origCert))
			copy(newCert, origCert)

			newTlsCerts := clientCerts
			newTlsCerts.Certificate[0] = corruptCert(newCert, corruptCertOffset)

			return &newTlsCerts, nil
		}
		tlsCfg.MaxVersion = tlsVersion
		tlsCfg.MinVersion = tlsVersion

		tlsClientConn := tls.Client(clientConn, tlsCfg)
		defer tlsClientConn.Close()

		if err := tlsClientConn.Handshake(); err != nil {
			t.Logf("Handshake error: %v", err)
			return
		}

		br := bufio.NewReaderSize(tlsClientConn, 128)
		if _, err := br.ReadString('\n'); err != nil {
			t.Logf("Unexpected error reading INFO message: %v", err)
			return
		}
	})
}
