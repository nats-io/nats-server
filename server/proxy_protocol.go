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
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"
)

const (
	proxyV1HeaderPrefix = "PROXY"
	proxyV2HeaderPrefix = "\x0D\x0A\x0D\x0A\x00\x0D\x0A\x51\x55\x49\x54\x0A"
	proxyV2HeaderLen    = 16
	proxyV1MaxLen       = 108
	proxyTimeout        = 5 * time.Second
)

type ProxyProtocolInfo struct {
	SrcIP    net.IP
	DestIP   net.IP
	SrcPort  int
	DestPort int
}

type proxyConn struct {
	net.Conn
	proxyInfo *ProxyProtocolInfo
}

func (pc *proxyConn) RemoteAddr() net.Addr {
	if pc.proxyInfo != nil && pc.proxyInfo.SrcIP != nil {
		return &net.TCPAddr{
			IP:   pc.proxyInfo.SrcIP,
			Port: pc.proxyInfo.SrcPort,
		}
	}
	return pc.Conn.RemoteAddr()
}

func (pc *proxyConn) LocalAddr() net.Addr {
	if pc.proxyInfo != nil && pc.proxyInfo.DestIP != nil {
		return &net.TCPAddr{
			IP:   pc.proxyInfo.DestIP,
			Port: pc.proxyInfo.DestPort,
		}
	}
	return pc.Conn.LocalAddr()
}

func parseProxyProtocol(conn net.Conn) (net.Conn, error) {
	connWithTimeout := &timeoutConn{Conn: conn, timeout: proxyTimeout}
	reader := bufio.NewReader(connWithTimeout)

	// Peek at the first byte to determine protocol version
	firstByte, err := reader.Peek(1)
	if err != nil {
		return nil, fmt.Errorf("failed to peek first byte: %w", err)
	}

	var proxyInfo *ProxyProtocolInfo

	if firstByte[0] == 'P' {
		// PROXY v1 (text-based)
		proxyInfo, _, err = parseProxyV1(reader)
	} else if firstByte[0] == '\x0D' {
		// PROXY v2 (binary)
		proxyInfo, _, err = parseProxyV2(reader)
	} else {
		return nil, fmt.Errorf("invalid PROXY protocol header")
	}

	if err != nil {
		return nil, err
	}

	// Create a buffered reader with any remaining data
	remainingReader := &readerWithUnread{
		Reader: reader,
		conn:   conn,
	}

	// Wrap the connection
	wrappedConn := &bufferedConn{
		Conn:   conn,
		reader: remainingReader,
	}

	return &proxyConn{
		Conn:      wrappedConn,
		proxyInfo: proxyInfo,
	}, nil
}

func parseProxyV1(reader *bufio.Reader) (*ProxyProtocolInfo, int, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return nil, 0, fmt.Errorf("failed to read PROXY v1 line: %w", err)
	}

	line = strings.TrimSpace(line)
	parts := strings.Split(line, " ")

	if len(parts) < 2 || parts[0] != "PROXY" {
		return nil, 0, fmt.Errorf("invalid PROXY v1 header")
	}

	if len(parts) < 6 {
		// Handle UNKNOWN connections or malformed headers
		if len(parts) >= 2 && parts[1] == "UNKNOWN" {
			return &ProxyProtocolInfo{}, len(line) + 1, nil
		}
		return nil, 0, fmt.Errorf("invalid PROXY v1 header")
	}

	protocol := parts[1]
	if protocol != "TCP4" && protocol != "TCP6" {
		// UNKNOWN connection, return empty proxy info
		return &ProxyProtocolInfo{}, len(line) + 1, nil
	}

	srcIP := net.ParseIP(parts[2])
	if srcIP == nil {
		return nil, 0, fmt.Errorf("invalid source IP: %s", parts[2])
	}

	destIP := net.ParseIP(parts[3])
	if destIP == nil {
		return nil, 0, fmt.Errorf("invalid destination IP: %s", parts[3])
	}

	srcPort, err := strconv.Atoi(parts[4])
	if err != nil {
		return nil, 0, fmt.Errorf("invalid source port: %s", parts[4])
	}

	destPort, err := strconv.Atoi(parts[5])
	if err != nil {
		return nil, 0, fmt.Errorf("invalid destination port: %s", parts[5])
	}

	return &ProxyProtocolInfo{
		SrcIP:    srcIP,
		DestIP:   destIP,
		SrcPort:  srcPort,
		DestPort: destPort,
	}, len(line) + 1, nil
}

func parseProxyV2(reader *bufio.Reader) (*ProxyProtocolInfo, int, error) {
	header := make([]byte, proxyV2HeaderLen)
	_, err := io.ReadFull(reader, header)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to read PROXY v2 header: %w", err)
	}

	// Verify signature
	if string(header[:12]) != proxyV2HeaderPrefix {
		return nil, 0, fmt.Errorf("invalid PROXY v2 signature")
	}

	// Parse version and command
	versionCommand := header[12]
	version := (versionCommand & 0xF0) >> 4
	command := versionCommand & 0x0F

	if version != 2 {
		return nil, 0, fmt.Errorf("unsupported PROXY protocol version: %d", version)
	}

	// Parse family and protocol
	familyProtocol := header[13]
	family := (familyProtocol & 0xF0) >> 4
	_ = familyProtocol & 0x0F // protocol (not currently used)

	// Parse length
	length := binary.BigEndian.Uint16(header[14:16])

	// Read the address information
	addressInfo := make([]byte, length)
	if length > 0 {
		_, err = io.ReadFull(reader, addressInfo)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to read PROXY v2 address info: %w", err)
		}
	}

	totalBytes := proxyV2HeaderLen + int(length)

	// Handle LOCAL command (health checks)
	if command == 0x00 {
		return &ProxyProtocolInfo{}, totalBytes, nil
	}

	// Handle PROXY command
	if command != 0x01 {
		return nil, 0, fmt.Errorf("unsupported PROXY v2 command: %d", command)
	}

	// Parse address information based on family
	switch family {
	case 0x01: // IPv4
		if length < 12 {
			return nil, 0, fmt.Errorf("insufficient IPv4 address data")
		}
		return &ProxyProtocolInfo{
			SrcIP:    net.IP(addressInfo[0:4]),
			DestIP:   net.IP(addressInfo[4:8]),
			SrcPort:  int(binary.BigEndian.Uint16(addressInfo[8:10])),
			DestPort: int(binary.BigEndian.Uint16(addressInfo[10:12])),
		}, totalBytes, nil

	case 0x02: // IPv6
		if length < 36 {
			return nil, 0, fmt.Errorf("insufficient IPv6 address data")
		}
		return &ProxyProtocolInfo{
			SrcIP:    net.IP(addressInfo[0:16]),
			DestIP:   net.IP(addressInfo[16:32]),
			SrcPort:  int(binary.BigEndian.Uint16(addressInfo[32:34])),
			DestPort: int(binary.BigEndian.Uint16(addressInfo[34:36])),
		}, totalBytes, nil

	default:
		// UNSPEC or other family, return empty proxy info
		return &ProxyProtocolInfo{}, totalBytes, nil
	}
}

// timeoutConn wraps a connection with a read timeout
type timeoutConn struct {
	net.Conn
	timeout time.Duration
}

func (tc *timeoutConn) Read(b []byte) (int, error) {
	tc.Conn.SetReadDeadline(time.Now().Add(tc.timeout))
	return tc.Conn.Read(b)
}

// readerWithUnread allows reading remaining data from the buffered reader
type readerWithUnread struct {
	io.Reader
	conn net.Conn
}

// bufferedConn wraps a connection with a buffered reader for any unread data
type bufferedConn struct {
	net.Conn
	reader io.Reader
}

func (bc *bufferedConn) Read(b []byte) (int, error) {
	return bc.reader.Read(b)
}
