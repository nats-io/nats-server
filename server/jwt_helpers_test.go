// Copyright 2026 The NATS Authors
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
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
)

func encodeJWTRefreshOptions(t *testing.T, f *jwtRefreshFixture, c *jwtRefreshConnection, claims *jwt.UserClaims, protocol int) string {
	t.Helper()
	encoded, signature := jwtRefreshCredentials(t, f, c, claims)
	options, err := json.Marshal(ClientOpts{
		JWT: encoded, Sig: signature,
		Protocol: protocol, Echo: true, Pedantic: true,
	})
	require_NoError(t, err)
	return "CONNECT " + string(options) + "\r\n"
}

func connectJWTRefreshProtocol(t *testing.T, f *jwtRefreshFixture, claims *jwt.UserClaims, protocol int) *jwtRefreshConnection {
	t.Helper()
	conn, err := net.DialTimeout("tcp", f.server.Addr().String(), time.Second)
	require_NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	c := &jwtRefreshConnection{fixture: f, conn: conn, reader: bufio.NewReader(conn)}
	frame := c.readFrame()
	require_Equal(t, frame.op, "INFO")
	require_NoError(t, json.Unmarshal(frame.payload, &c.info))
	c.requireNoErrors(c.command(encodeJWTRefreshOptions(t, f, c, claims, protocol)))
	c.requireNoErrors(c.command(""))
	return c
}

func jwtRefreshIntegrationMessage(t *testing.T, c *jwtRefreshConnection, frames []jwtRefreshFrame, sid string) jwtRefreshFrame {
	t.Helper()
	for {
		for _, frame := range frames {
			if frame.op == "-ERR" {
				t.Fatalf("unexpected delivery protocol error: %s", frame.error)
			}
			if frame.op == "MSG" || frame.op == "HMSG" {
				require_Equal(t, frame.sid, sid)
				return frame
			}
		}
		frames = []jwtRefreshFrame{c.readFrame()}
	}
}

func require_JWTRefreshExpirationClose(t *testing.T, c *jwtRefreshConnection, deadline time.Time, reason string) {
	t.Helper()
	require_NoError(t, c.conn.SetReadDeadline(deadline.Add(3*time.Second)))
	seenError := false
	for {
		line, err := c.reader.ReadString('\n')
		if err != nil {
			require_Equal(t, err, io.EOF)
			require_True(t, seenError)
			break
		}
		if strings.HasPrefix(line, "-ERR ") {
			require_Contains(t, line, reason)
			seenError = true
		} else if strings.HasPrefix(line, "PING") {
			c.write("PONG\r\n")
		} else if strings.HasPrefix(line, "PONG") {
			t.Fatal("expired connection confirmed a new round trip")
		}
	}
	require_True(t, time.Now().Before(deadline.Add(3*time.Second)))
	checkFor(t, time.Second, 10*time.Millisecond, func() error {
		if c.fixture.server.getClient(c.info.CID) != nil {
			return fmt.Errorf("expired connection remains registered")
		}
		return nil
	})
}

func jwtRefreshConnectProtocol(token, signature string) string {
	return fmt.Sprintf("CONNECT {\"jwt\":%q,\"sig\":%q,\"verbose\":false,\"pedantic\":true,\"echo\":true}\r\n", token, signature)
}

func require_JWTRefreshClosed(t *testing.T, c *jwtRefreshConnection, protocol, expected string) {
	t.Helper()
	target := c.serverClient()
	target.mu.Lock()
	var subjects []string
	for _, sub := range target.subs {
		subjects = append(subjects, string(sub.subject))
	}
	target.mu.Unlock()
	c.write(protocol + "PING\r\n")
	require_NoError(t, c.conn.SetReadDeadline(time.Now().Add(2*time.Second)))
	seenError := false
	for {
		line, err := c.reader.ReadString('\n')
		if err != nil {
			require_Equal(t, err, io.EOF)
			require_True(t, seenError)
			break
		}
		if strings.HasPrefix(line, "-ERR ") {
			require_Contains(t, line, expected)
			seenError = true
		} else if strings.HasPrefix(line, "PONG") {
			t.Fatal("rejected operation produced a successful round trip")
		} else if strings.HasPrefix(line, "PING") {
			c.write("PONG\r\n")
		}
	}
	checkFor(t, time.Second, 10*time.Millisecond, func() error {
		if c.fixture.server.getClient(c.info.CID) != nil {
			return fmt.Errorf("rejected connection remains registered")
		}
		acc, err := c.fixture.server.LookupAccount(c.fixture.accountID)
		if err != nil {
			return err
		}
		if acc.NumConnections() != 0 {
			return fmt.Errorf("rejected connection left account clients")
		}
		for _, subject := range subjects {
			matches := acc.sl.Match(subject)
			for _, sub := range matches.psubs {
				if sub.client == target {
					return fmt.Errorf("rejected connection left subscription interest")
				}
			}
		}
		return nil
	})
}

type jwtRefreshFixture struct {
	t          *testing.T
	server     *Server
	accountKey nkeys.KeyPair
	accountID  string
	systemKey  nkeys.KeyPair
	userKey    nkeys.KeyPair
	userID     string
}

type jwtRefreshConnection struct {
	fixture *jwtRefreshFixture
	conn    net.Conn
	reader  *bufio.Reader
	info    Info
}

type jwtRefreshFrame struct {
	op      string
	subject string
	sid     string
	reply   string
	payload []byte
	headers []byte
	error   string
}

func newJWTRefreshFixture(t *testing.T, configureAccount func(*jwt.AccountClaims)) *jwtRefreshFixture {
	t.Helper()
	return newJWTRefreshFixtureWithOptions(t, configureAccount, nil)
}

func newJWTRefreshFixtureWithOptions(t *testing.T, configureAccount func(*jwt.AccountClaims), configureOptions func(*Options)) *jwtRefreshFixture {
	t.Helper()
	f := &jwtRefreshFixture{t: t}
	var err error
	f.accountKey, err = nkeys.CreateAccount()
	require_NoError(t, err)
	f.accountID, err = f.accountKey.PublicKey()
	require_NoError(t, err)
	f.userKey, err = nkeys.CreateUser()
	require_NoError(t, err)
	f.userID, err = f.userKey.PublicKey()
	require_NoError(t, err)
	account := jwt.NewAccountClaims(f.accountID)
	if configureAccount != nil {
		configureAccount(account)
	}
	accountJWT, err := account.Encode(oKp)
	require_NoError(t, err)
	systemKey, systemID := createKey(t)
	f.systemKey = systemKey
	systemJWT, err := jwt.NewAccountClaims(systemID).Encode(oKp)
	require_NoError(t, err)
	conf := createConfFile(t, []byte(fmt.Sprintf(`
		listen: 127.0.0.1:-1
		operator: %s
		system_account: %s
		resolver: MEM
		resolver_preload: { %s: %s, %s: %s }
	`, ojwt, systemID, f.accountID, accountJWT, systemID, systemJWT)))
	opts, err := ProcessConfigFile(conf)
	require_NoError(t, err)
	opts.NoSigs = true
	opts.NoLog = true
	if configureOptions != nil {
		configureOptions(opts)
	}
	f.server = RunServer(opts)
	t.Cleanup(f.server.Shutdown)
	return f
}

func (f *jwtRefreshFixture) claims(configure func(*jwt.UserClaims)) *jwt.UserClaims {
	claims := jwt.NewUserClaims(f.userID)
	if configure != nil {
		configure(claims)
	}
	return claims
}

func (f *jwtRefreshFixture) connect(claims *jwt.UserClaims) *jwtRefreshConnection {
	f.t.Helper()
	conn, err := net.DialTimeout("tcp", f.server.Addr().String(), time.Second)
	require_NoError(f.t, err)
	f.t.Cleanup(func() { conn.Close() })
	c := &jwtRefreshConnection{fixture: f, conn: conn, reader: bufio.NewReader(conn)}
	frame := c.readFrame()
	require_Equal(f.t, frame.op, "INFO")
	require_NoError(f.t, json.Unmarshal(frame.payload, &c.info))
	c.refresh(claims)
	return c
}

func (c *jwtRefreshConnection) write(protocol string) {
	c.fixture.t.Helper()
	require_NoError(c.fixture.t, c.conn.SetWriteDeadline(time.Now().Add(2*time.Second)))
	_, err := io.WriteString(c.conn, protocol)
	require_NoError(c.fixture.t, err)
}

func (c *jwtRefreshConnection) readFrame() jwtRefreshFrame {
	t := c.fixture.t
	t.Helper()
	require_NoError(t, c.conn.SetReadDeadline(time.Now().Add(2*time.Second)))
	line, err := c.reader.ReadString('\n')
	require_NoError(t, err)
	line = strings.TrimSuffix(strings.TrimSuffix(line, "\n"), "\r")
	parts := strings.Fields(line)
	if len(parts) == 0 {
		t.Fatal("empty protocol frame")
	}
	frame := jwtRefreshFrame{op: parts[0]}
	switch frame.op {
	case "MSG", "HMSG":
		minimum := 4
		if frame.op == "HMSG" {
			minimum++
		}
		if len(parts) != minimum && len(parts) != minimum+1 {
			t.Fatalf("invalid MSG header: %q", line)
		}
		frame.subject, frame.sid = parts[1], parts[2]
		if len(parts) == minimum+1 {
			frame.reply = parts[3]
		}
		size, err := strconv.Atoi(parts[len(parts)-1])
		require_NoError(t, err)
		if size < 0 || size > 1024*1024 {
			t.Fatalf("invalid payload size: %d", size)
		}
		body := make([]byte, size+2)
		_, err = io.ReadFull(c.reader, body)
		require_NoError(t, err)
		require_Equal(t, string(body[size:]), "\r\n")
		frame.payload = body[:size]
		if frame.op == "HMSG" {
			headerSize, err := strconv.Atoi(parts[len(parts)-2])
			require_NoError(t, err)
			if headerSize < 0 || headerSize > size {
				t.Fatal("invalid header size")
			}
			frame.headers, frame.payload = body[:headerSize], body[headerSize:size]
		}
	case "INFO":
		frame.payload = []byte(strings.TrimPrefix(line, "INFO "))
	case "-ERR":
		frame.error = strings.TrimPrefix(line, "-ERR ")
	case "PING":
		c.write("PONG\r\n")
	case "PONG", "+OK":
	default:
		t.Fatalf("unexpected protocol frame: %q", line)
	}
	return frame
}

func (c *jwtRefreshConnection) command(protocol string) []jwtRefreshFrame {
	c.fixture.t.Helper()
	c.write(protocol + "PING\r\n")
	var frames []jwtRefreshFrame
	for {
		frame := c.readFrame()
		if frame.op == "PONG" {
			return frames
		}
		frames = append(frames, frame)
	}
}

func (c *jwtRefreshConnection) refresh(claims *jwt.UserClaims) {
	t := c.fixture.t
	t.Helper()
	encoded, signature := jwtRefreshCredentials(t, c.fixture, c, claims)
	connect, err := json.Marshal(struct {
		JWT      string `json:"jwt"`
		Sig      string `json:"sig"`
		Verbose  bool   `json:"verbose"`
		Pedantic bool   `json:"pedantic"`
		Echo     bool   `json:"echo"`
	}{JWT: encoded, Sig: signature, Pedantic: true, Echo: true})
	require_NoError(t, err)
	c.requireNoErrors(c.command("CONNECT " + string(connect) + "\r\n"))
}

func (c *jwtRefreshConnection) requireNoErrors(frames []jwtRefreshFrame) {
	c.fixture.t.Helper()
	for _, frame := range frames {
		if frame.op == "-ERR" {
			c.fixture.t.Fatalf("unexpected protocol error: %s", frame.error)
		}
	}
}

func (c *jwtRefreshConnection) userInfo() *UserInfo {
	t := c.fixture.t
	t.Helper()
	inbox := nats.NewInbox()
	c.requireNoErrors(c.command("SUB " + inbox + " metadata\r\n"))
	c.write("PUB $SYS.REQ.USER.INFO " + inbox + " 0\r\n\r\n")
	for {
		frame := c.readFrame()
		if frame.op == "-ERR" {
			t.Fatalf("UserInfo request rejected: %s", frame.error)
		}
		if frame.op != "MSG" {
			continue
		}
		require_Equal(t, frame.subject, inbox)
		response := ServerAPIResponse{Data: &UserInfo{}}
		require_NoError(t, json.Unmarshal(frame.payload, &response))
		if response.Error != nil {
			t.Fatalf("UserInfo request failed: %+v", response.Error)
		}
		c.requireNoErrors(c.command("UNSUB metadata\r\n"))
		return response.Data.(*UserInfo)
	}
}

func (c *jwtRefreshConnection) serverClient() *client {
	c.fixture.t.Helper()
	client := c.fixture.server.getClient(c.info.CID)
	if client == nil {
		c.fixture.t.Fatalf("connection %d is no longer registered", c.info.CID)
	}
	return client
}

func (c *jwtRefreshConnection) connectionInfo() *ConnInfo {
	t := c.fixture.t
	t.Helper()
	connections, err := c.fixture.server.Connz(&ConnzOptions{CID: c.info.CID, Username: true, Subscriptions: true})
	require_NoError(t, err)
	require_Equal(t, len(connections.Conns), 1)
	return connections.Conns[0]
}

func (f *jwtRefreshFixture) observer() *nats.Conn {
	f.t.Helper()
	encoded, err := f.claims(nil).Encode(f.accountKey)
	require_NoError(f.t, err)
	nc := natsConnect(f.t, f.server.ClientURL(), nats.UserJWT(func() (string, error) {
		return encoded, nil
	}, f.userKey.Sign), nats.NoReconnect())
	f.t.Cleanup(nc.Close)
	return nc
}

func jwtRefreshPublish(subject, payload string) string {
	return fmt.Sprintf("PUB %s %d\r\n%s\r\n", subject, len(payload), payload)
}

func require_JWTRefreshMessage(t *testing.T, frames []jwtRefreshFrame, sid, payload string) {
	t.Helper()
	for _, frame := range frames {
		if frame.op == "MSG" && frame.sid == sid && string(frame.payload) == payload {
			return
		}
	}
	t.Fatalf("missing message on SID %s with payload %q", sid, payload)
}

func require_JWTRefreshPermissionError(t *testing.T, frames []jwtRefreshFrame, operation, subject string) {
	t.Helper()
	for _, frame := range frames {
		if frame.op == "-ERR" && strings.Contains(frame.error, "Permissions Violation") && strings.Contains(frame.error, operation) && strings.Contains(frame.error, subject) {
			return
		}
	}
	t.Fatalf("missing %s permission error for %s", operation, subject)
}

func jwtRefreshTransportProtocol(t *testing.T, c *jwtRefreshConnection, claims *jwt.UserClaims, configure func(*ClientOpts)) string {
	t.Helper()
	encoded, signature := jwtRefreshCredentials(t, c.fixture, c, claims)
	opts := ClientOpts{JWT: encoded, Sig: signature, Echo: true, Pedantic: true}
	if configure != nil {
		configure(&opts)
	}
	payload, err := json.Marshal(opts)
	require_NoError(t, err)
	return "CONNECT " + string(payload) + "\r\n"
}

func jwtRefreshCredentials(t *testing.T, f *jwtRefreshFixture, c *jwtRefreshConnection, claims *jwt.UserClaims) (string, string) {
	t.Helper()
	encoded, err := claims.Encode(f.accountKey)
	require_NoError(t, err)
	signature, err := f.userKey.Sign([]byte(c.info.Nonce))
	require_NoError(t, err)
	return encoded, base64.RawURLEncoding.EncodeToString(signature)
}
