// Copyright 2020-2023 The NATS Authors
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

//go:build !skip_mqtt_tests
// +build !skip_mqtt_tests

package server

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
	"github.com/nats-io/nuid"
)

var testMQTTTimeout = 4 * time.Second

var jsClusterTemplWithLeafAndMQTT = `
	listen: 127.0.0.1:-1
	server_name: %s
	jetstream: {max_mem_store: 256MB, max_file_store: 2GB, store_dir: '%s'}

	{{leaf}}

	cluster {
		name: %s
		listen: 127.0.0.1:%d
		routes = [%s]
	}

	mqtt {
		listen: 127.0.0.1:-1
	}

	# For access to system account.
	accounts { $SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] } }
`

type mqttWrapAsWs struct {
	net.Conn
	t   testing.TB
	br  *bufio.Reader
	tmp []byte
}

func (c *mqttWrapAsWs) Write(p []byte) (int, error) {
	proto := testWSCreateClientMsg(wsBinaryMessage, 1, true, false, p)
	return c.Conn.Write(proto)
}

func (c *mqttWrapAsWs) Read(p []byte) (int, error) {
	for {
		if n := len(c.tmp); n > 0 {
			if len(p) < n {
				n = len(p)
			}
			copy(p, c.tmp[:n])
			c.tmp = c.tmp[n:]
			return n, nil
		}
		c.tmp = testWSReadFrame(c.t, c.br)
	}
}

func testMQTTReadPacket(t testing.TB, r *mqttReader) (byte, int) {
	t.Helper()

	var b byte
	var pl int
	var err error

	rd := r.reader

	fill := func() []byte {
		t.Helper()
		var buf [512]byte

		n, err := rd.Read(buf[:])
		if err != nil {
			t.Fatalf("Error reading data: %v", err)
		}
		return copyBytes(buf[:n])
	}

	rd.SetReadDeadline(time.Now().Add(testMQTTTimeout))
	for {
		r.pstart = r.pos
		if !r.hasMore() {
			r.reset(fill())
			continue
		}
		b, err = r.readByte("packet type")
		if err != nil {
			t.Fatalf("Error reading packet: %v", err)
		}
		var complete bool
		pl, complete, err = r.readPacketLen()
		if err != nil {
			t.Fatalf("Error reading packet: %v", err)
		}
		if !complete {
			r.reset(fill())
			continue
		}
		break
	}
	rd.SetReadDeadline(time.Time{})
	return b, pl
}

func TestMQTTReader(t *testing.T) {
	r := &mqttReader{}
	r.reset([]byte{0, 2, 'a', 'b'})
	bs, err := r.readBytes("", false)
	if err != nil {
		t.Fatal(err)
	}
	sbs := string(bs)
	if sbs != "ab" {
		t.Fatalf(`expected "ab", got %q`, sbs)
	}

	r.reset([]byte{0, 2, 'a', 'b'})
	bs, err = r.readBytes("", true)
	if err != nil {
		t.Fatal(err)
	}
	bs[0], bs[1] = 'c', 'd'
	if bytes.Equal(bs, r.buf[2:]) {
		t.Fatal("readBytes should have returned a copy")
	}

	r.reset([]byte{'a', 'b'})
	if b, err := r.readByte(""); err != nil || b != 'a' {
		t.Fatalf("Error reading byte: b=%v err=%v", b, err)
	}
	if !r.hasMore() {
		t.Fatal("expected to have more, did not")
	}
	if b, err := r.readByte(""); err != nil || b != 'b' {
		t.Fatalf("Error reading byte: b=%v err=%v", b, err)
	}
	if r.hasMore() {
		t.Fatal("expected to not have more")
	}
	if _, err := r.readByte("test"); err == nil || !strings.Contains(err.Error(), "error reading test") {
		t.Fatalf("unexpected error: %v", err)
	}

	r.reset([]byte{0, 2, 'a', 'b'})
	if s, err := r.readString(""); err != nil || s != "ab" {
		t.Fatalf("Error reading string: s=%q err=%v", s, err)
	}

	r.reset([]byte{10})
	if _, err := r.readUint16("uint16"); err == nil || !strings.Contains(err.Error(), "error reading uint16") {
		t.Fatalf("unexpected error: %v", err)
	}

	r.reset([]byte{0x82, 0xff, 0x3})
	l, _, err := r.readPacketLenWithCheck(false)
	if err != nil {
		t.Fatal("error getting packet len")
	}
	if l != 0xff82 {
		t.Fatalf("expected length 0xff82 got 0x%x", l)
	}
	r.reset([]byte{0xff, 0xff, 0xff, 0xff, 0xff})
	if _, _, err := r.readPacketLenWithCheck(false); err == nil || !strings.Contains(err.Error(), "malformed") {
		t.Fatalf("unexpected error: %v", err)
	}

	r.reset([]byte{0, 2, 'a', 'b', mqttPacketPub, 0x82, 0xff, 0x3})
	r.readString("")
	for i := 0; i < 2; i++ {
		r.pstart = r.pos
		b, err := r.readByte("")
		if err != nil {
			t.Fatalf("Error reading byte: %v", err)
		}
		if pt := b & mqttPacketMask; pt != mqttPacketPub {
			t.Fatalf("Unexpected byte: %v", b)
		}
		pl, complete, err := r.readPacketLen()
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if complete {
			t.Fatal("Expected to be incomplete")
		}
		if pl != 0 {
			t.Fatalf("Expected pl to be 0, got %v", pl)
		}
		if i > 0 {
			break
		}
		if !bytes.Equal(r.pbuf, []byte{mqttPacketPub, 0x82, 0xff, 0x3}) {
			t.Fatalf("Invalid recorded partial: %v", r.pbuf)
		}
		r.reset([]byte{'a', 'b', 'c'})
		if !bytes.Equal(r.buf, []byte{mqttPacketPub, 0x82, 0xff, 0x3, 'a', 'b', 'c'}) {
			t.Fatalf("Invalid buffer: %v", r.buf)
		}
		if r.pbuf != nil {
			t.Fatalf("Partial buffer should have been reset, got %v", r.pbuf)
		}
		if r.pos != 0 {
			t.Fatalf("Pos should have been reset, got %v", r.pos)
		}
		if r.pstart != 0 {
			t.Fatalf("Pstart should have been reset, got %v", r.pstart)
		}
	}
	// On second pass, the pbuf should have been extended with 'abc'
	if !bytes.Equal(r.pbuf, []byte{mqttPacketPub, 0x82, 0xff, 0x3, 'a', 'b', 'c'}) {
		t.Fatalf("Invalid recorded partial: %v", r.pbuf)
	}
}

func TestMQTTWriter(t *testing.T) {
	w := &mqttWriter{}
	w.WriteUint16(1234)

	r := &mqttReader{}
	r.reset(w.Bytes())
	if v, err := r.readUint16(""); err != nil || v != 1234 {
		t.Fatalf("unexpected value: v=%v err=%v", v, err)
	}

	w.Reset()
	w.WriteString("test")
	r.reset(w.Bytes())
	if len(r.buf) != 6 {
		t.Fatalf("Expected 2 bytes size before string, got %v", r.buf)
	}

	w.Reset()
	w.WriteBytes([]byte("test"))
	r.reset(w.Bytes())
	if len(r.buf) != 6 {
		t.Fatalf("Expected 2 bytes size before bytes, got %v", r.buf)
	}

	ints := []int{
		0, 1, 127, 128, 16383, 16384, 2097151, 2097152, 268435455,
	}
	lens := []int{
		1, 1, 1, 2, 2, 3, 3, 4, 4,
	}

	tl := 0
	w.Reset()
	for i, v := range ints {
		w.WriteVarInt(v)
		tl += lens[i]
		if tl != w.Len() {
			t.Fatalf("expected len %d, got %d", tl, w.Len())
		}
	}

	r.reset(w.Bytes())
	for _, v := range ints {
		x, _, _ := r.readPacketLenWithCheck(false)
		if v != x {
			t.Fatalf("expected %d, got %d", v, x)
		}
	}
}

func testMQTTDefaultOptions() *Options {
	o := DefaultOptions()
	o.ServerName = nuid.Next()
	o.Cluster.Port = 0
	o.Gateway.Name = ""
	o.Gateway.Port = 0
	o.LeafNode.Port = 0
	o.Websocket.Port = 0
	o.MQTT.Host = "127.0.0.1"
	o.MQTT.Port = -1
	o.JetStream = true
	return o
}

func testMQTTRunServer(t testing.TB, o *Options) *Server {
	t.Helper()
	o.NoLog = false
	if o.StoreDir == _EMPTY_ {
		o.StoreDir = t.TempDir()
	}
	s, err := NewServer(o)
	if err != nil {
		t.Fatalf("Error creating server: %v", err)
	}
	l := &DummyLogger{}
	s.SetLogger(l, true, true)
	s.Start()
	if err := s.readyForConnections(3 * time.Second); err != nil {
		testMQTTShutdownServer(s)
		t.Fatal(err)
	}
	return s
}

func testMQTTShutdownRestartedServer(s **Server) {
	srv := *s
	testMQTTShutdownServer(srv)
	*s = nil
}

func testMQTTShutdownServer(s *Server) {
	if c := s.JetStreamConfig(); c != nil {
		dir := strings.TrimSuffix(c.StoreDir, JetStreamStoreDir)
		defer os.RemoveAll(dir)
	}
	s.Shutdown()
}

func testMQTTDefaultTLSOptions(t *testing.T, verify bool) *Options {
	t.Helper()
	o := testMQTTDefaultOptions()
	tc := &TLSConfigOpts{
		CertFile: "../test/configs/certs/server-cert.pem",
		KeyFile:  "../test/configs/certs/server-key.pem",
		CaFile:   "../test/configs/certs/ca.pem",
		Verify:   verify,
	}
	var err error
	o.MQTT.TLSConfig, err = GenTLSConfig(tc)
	o.MQTT.TLSTimeout = 2.0
	if err != nil {
		t.Fatalf("Error creating tls config: %v", err)
	}
	return o
}

func TestMQTTServerNameRequired(t *testing.T) {
	conf := createConfFile(t, []byte(`
		mqtt {
			port: -1
		}
	`))
	o, err := ProcessConfigFile(conf)
	if err != nil {
		t.Fatalf("Error processing config file: %v", err)
	}
	if _, err := NewServer(o); err == nil || err.Error() != errMQTTServerNameMustBeSet.Error() {
		t.Fatalf("Expected error about requiring server name to be set, got %v", err)
	}
}

func TestMQTTStandaloneRequiresJetStream(t *testing.T) {
	conf := createConfFile(t, []byte(`
		server_name: mqtt
		mqtt {
			port: -1
			tls {
				cert_file: "./configs/certs/server.pem"
				key_file: "./configs/certs/key.pem"
			}
		}
	`))
	o, err := ProcessConfigFile(conf)
	if err != nil {
		t.Fatalf("Error processing config file: %v", err)
	}
	if _, err := NewServer(o); err == nil || err.Error() != errMQTTStandaloneNeedsJetStream.Error() {
		t.Fatalf("Expected error about requiring JetStream in standalone mode, got %v", err)
	}
}

func TestMQTTConfig(t *testing.T) {
	conf := createConfFile(t, []byte(`
		jetstream: enabled
		server_name: mqtt
		mqtt {
			port: -1
			tls {
				cert_file: "./configs/certs/server.pem"
				key_file: "./configs/certs/key.pem"
			}
		}
	`))
	s, o := RunServerWithConfig(conf)
	defer testMQTTShutdownServer(s)
	if o.MQTT.TLSConfig == nil {
		t.Fatal("expected TLS config to be set")
	}
}

func TestMQTTValidateOptions(t *testing.T) {
	nmqtto := DefaultOptions()
	mqtto := testMQTTDefaultOptions()
	for _, test := range []struct {
		name    string
		getOpts func() *Options
		err     error
	}{
		{"mqtt disabled", func() *Options { return nmqtto.Clone() }, nil},
		{"mqtt username not allowed if users specified", func() *Options {
			o := mqtto.Clone()
			o.Users = []*User{{Username: "abc", Password: "pwd"}}
			o.MQTT.Username = "b"
			o.MQTT.Password = "pwd"
			return o
		}, errMQTTUserMixWithUsersNKeys},
		{"mqtt token not allowed if users specified", func() *Options {
			o := mqtto.Clone()
			o.Nkeys = []*NkeyUser{{Nkey: "abc"}}
			o.MQTT.Token = "mytoken"
			return o
		}, errMQTTTokenMixWIthUsersNKeys},
		{"ack wait should be >=0", func() *Options {
			o := mqtto.Clone()
			o.MQTT.AckWait = -10 * time.Second
			return o
		}, errMQTTAckWaitMustBePositive},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := validateMQTTOptions(test.getOpts())
			if test.err == nil && err != nil {
				t.Fatalf("Unexpected error: %v", err)
			} else if test.err != nil && (err == nil || err.Error() != test.err.Error()) {
				t.Fatalf("Expected error to contain %q, got %v", test.err, err)
			}
		})
	}
}

func TestMQTTParseOptions(t *testing.T) {
	for _, test := range []struct {
		name     string
		content  string
		checkOpt func(*MQTTOpts) error
		err      string
	}{
		// Negative tests
		{"bad type", "mqtt: []", nil, "to be a map"},
		{"bad listen", "mqtt: { listen: [] }", nil, "port or host:port"},
		{"bad port", `mqtt: { port: "abc" }`, nil, "not int64"},
		{"bad host", `mqtt: { host: 123 }`, nil, "not string"},
		{"bad tls", `mqtt: { tls: 123 }`, nil, "not map[string]interface {}"},
		{"unknown field", `mqtt: { this_does_not_exist: 123 }`, nil, "unknown"},
		{"ack wait", `mqtt: {ack_wait: abc}`, nil, "invalid duration"},
		{"max ack pending", `mqtt: {max_ack_pending: abc}`, nil, "not int64"},
		{"max ack pending too high", `mqtt: {max_ack_pending: 12345678}`, nil, "invalid value"},
		// Positive tests
		{"tls gen fails", `
			mqtt {
				tls {
					cert_file: "./configs/certs/server.pem"
				}
			}`, nil, "missing 'key_file'"},
		{"listen port only", `mqtt { listen: 1234 }`, func(o *MQTTOpts) error {
			if o.Port != 1234 {
				return fmt.Errorf("expected 1234, got %v", o.Port)
			}
			return nil
		}, ""},
		{"listen host and port", `mqtt { listen: "localhost:1234" }`, func(o *MQTTOpts) error {
			if o.Host != "localhost" || o.Port != 1234 {
				return fmt.Errorf("expected localhost:1234, got %v:%v", o.Host, o.Port)
			}
			return nil
		}, ""},
		{"host", `mqtt { host: "localhost" }`, func(o *MQTTOpts) error {
			if o.Host != "localhost" {
				return fmt.Errorf("expected localhost, got %v", o.Host)
			}
			return nil
		}, ""},
		{"port", `mqtt { port: 1234 }`, func(o *MQTTOpts) error {
			if o.Port != 1234 {
				return fmt.Errorf("expected 1234, got %v", o.Port)
			}
			return nil
		}, ""},
		{"tls config",
			`
			mqtt {
				tls {
					cert_file: "./configs/certs/server.pem"
					key_file: "./configs/certs/key.pem"
				}
			}
			`, func(o *MQTTOpts) error {
				if o.TLSConfig == nil {
					return fmt.Errorf("TLSConfig should have been set")
				}
				return nil
			}, ""},
		{"no auth user",
			`
			mqtt {
				no_auth_user: "noauthuser"
			}
			`, func(o *MQTTOpts) error {
				if o.NoAuthUser != "noauthuser" {
					return fmt.Errorf("Invalid NoAuthUser value: %q", o.NoAuthUser)
				}
				return nil
			}, ""},
		{"auth block",
			`
			mqtt {
				authorization {
					user: "mqttuser"
					password: "pwd"
					token: "token"
					timeout: 2.0
				}
			}
			`, func(o *MQTTOpts) error {
				if o.Username != "mqttuser" || o.Password != "pwd" || o.Token != "token" || o.AuthTimeout != 2.0 {
					return fmt.Errorf("Invalid auth block: %+v", o)
				}
				return nil
			}, ""},
		{"auth timeout as int",
			`
			mqtt {
				authorization {
					timeout: 2
				}
			}
			`, func(o *MQTTOpts) error {
				if o.AuthTimeout != 2.0 {
					return fmt.Errorf("Invalid auth timeout: %v", o.AuthTimeout)
				}
				return nil
			}, ""},
		{"ack wait",
			`
			mqtt {
				ack_wait: "10s"
			}
			`, func(o *MQTTOpts) error {
				if o.AckWait != 10*time.Second {
					return fmt.Errorf("Invalid ack wait: %v", o.AckWait)
				}
				return nil
			}, ""},
		{"max ack pending",
			`
			mqtt {
				max_ack_pending: 123
			}
			`, func(o *MQTTOpts) error {
				if o.MaxAckPending != 123 {
					return fmt.Errorf("Invalid max ack pending: %v", o.MaxAckPending)
				}
				return nil
			}, ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			conf := createConfFile(t, []byte(test.content))
			o, err := ProcessConfigFile(conf)
			if test.err != _EMPTY_ {
				if err == nil || !strings.Contains(err.Error(), test.err) {
					t.Fatalf("For content: %q, expected error about %q, got %v", test.content, test.err, err)
				}
				return
			} else if err != nil {
				t.Fatalf("Unexpected error for content %q: %v", test.content, err)
			}
			if err := test.checkOpt(&o.MQTT); err != nil {
				t.Fatalf("Incorrect option for content %q: %v", test.content, err.Error())
			}
		})
	}
}

func TestMQTTStart(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	nc, err := net.Dial("tcp", fmt.Sprintf("%s:%d", o.MQTT.Host, o.MQTT.Port))
	if err != nil {
		t.Fatalf("Unable to create tcp connection to mqtt port: %v", err)
	}
	nc.Close()

	// Check failure to start due to port in use
	o2 := testMQTTDefaultOptions()
	o2.MQTT.Port = o.MQTT.Port
	s2, err := NewServer(o2)
	if err != nil {
		t.Fatalf("Error creating server: %v", err)
	}
	defer s2.Shutdown()
	l := &captureFatalLogger{fatalCh: make(chan string, 1)}
	s2.SetLogger(l, false, false)

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		s2.Start()
		wg.Done()
	}()

	select {
	case e := <-l.fatalCh:
		if !strings.Contains(e, "Unable to listen for MQTT connections") {
			t.Fatalf("Unexpected error: %q", e)
		}
	case <-time.After(time.Second):
		t.Fatal("Should have gotten a fatal error")
	}
}

func TestMQTTTLS(t *testing.T) {
	o := testMQTTDefaultTLSOptions(t, false)
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	c, _, err := testMQTTConnectRetryWithError(t, &mqttConnInfo{tls: true}, o.MQTT.Host, o.MQTT.Port, 0)
	if err != nil {
		t.Fatal(err)
	}
	c.Close()
	c = nil
	testMQTTShutdownServer(s)

	// Force client cert verification
	o = testMQTTDefaultTLSOptions(t, true)
	s = testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	c, _, err = testMQTTConnectRetryWithError(t, &mqttConnInfo{tls: true}, o.MQTT.Host, o.MQTT.Port, 0)
	if err == nil || c != nil {
		if c != nil {
			c.Close()
		}
		t.Fatal("Handshake expected to fail since client did not provide cert")
	}

	// Add client cert.
	tc := &TLSConfigOpts{
		CertFile: "../test/configs/certs/client-cert.pem",
		KeyFile:  "../test/configs/certs/client-key.pem",
	}
	tlsc, err := GenTLSConfig(tc)
	if err != nil {
		t.Fatalf("Error generating tls config: %v", err)
	}
	tlsc.InsecureSkipVerify = true
	c, _, err = testMQTTConnectRetryWithError(t, &mqttConnInfo{
		tls:  true,
		tlsc: tlsc,
	}, o.MQTT.Host, o.MQTT.Port, 0)
	if err != nil {
		t.Fatal(err)
	}
	c.Close()
	c = nil
	testMQTTShutdownServer(s)

	// Lower TLS timeout so low that we should fail
	o.MQTT.TLSTimeout = 0.001
	s = testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	nc, err := net.Dial("tcp", fmt.Sprintf("%s:%d", o.MQTT.Host, o.MQTT.Port))
	if err != nil {
		t.Fatalf("Unable to create tcp connection to mqtt port: %v", err)
	}
	defer nc.Close()
	time.Sleep(100 * time.Millisecond)
	tlsConn := tls.Client(nc, tlsc)
	tlsConn.SetDeadline(time.Now().Add(time.Second))
	if err := tlsConn.Handshake(); err == nil {
		t.Fatal("Expected failure, did not get one")
	}
}

type mqttConnInfo struct {
	clientID  string
	cleanSess bool
	keepAlive uint16
	will      *mqttWill
	user      string
	pass      string
	ws        bool
	tls       bool
	tlsc      *tls.Config
}

func testMQTTGetClient(t testing.TB, s *Server, clientID string) *client {
	t.Helper()
	var mc *client
	s.mu.Lock()
	for _, c := range s.clients {
		c.mu.Lock()
		if c.isMqtt() && c.mqtt.cid == clientID {
			mc = c
		}
		c.mu.Unlock()
		if mc != nil {
			break
		}
	}
	s.mu.Unlock()
	if mc == nil {
		t.Fatalf("Did not find client %q", clientID)
	}
	return mc
}

func testMQTTRead(c net.Conn) ([]byte, error) {
	var buf [512]byte
	// Make sure that test does not block
	c.SetReadDeadline(time.Now().Add(testMQTTTimeout))
	n, err := c.Read(buf[:])
	if err != nil {
		return nil, err
	}
	c.SetReadDeadline(time.Time{})
	return copyBytes(buf[:n]), nil
}

func testMQTTWrite(c net.Conn, buf []byte) (int, error) {
	c.SetWriteDeadline(time.Now().Add(testMQTTTimeout))
	n, err := c.Write(buf)
	c.SetWriteDeadline(time.Time{})
	return n, err
}

func testMQTTConnect(t testing.TB, ci *mqttConnInfo, host string, port int) (net.Conn, *mqttReader) {
	t.Helper()
	return testMQTTConnectRetry(t, ci, host, port, 0)
}

func testMQTTConnectRetry(t testing.TB, ci *mqttConnInfo, host string, port int, retryCount int) (net.Conn, *mqttReader) {
	t.Helper()
	c, r, err := testMQTTConnectRetryWithError(t, ci, host, port, retryCount)
	if err != nil {
		t.Fatal(err)
	}
	return c, r
}

func testMQTTConnectRetryWithError(t testing.TB, ci *mqttConnInfo, host string, port int, retryCount int) (net.Conn, *mqttReader, error) {
	retry := func(c net.Conn) bool {
		if c != nil {
			c.Close()
		}
		if retryCount == 0 {
			return false
		}
		time.Sleep(time.Second)
		retryCount--
		return true
	}

	addr := fmt.Sprintf("%s:%d", host, port)
	var c net.Conn
	var err error
RETRY:
	if ci.ws {
		var br *bufio.Reader
		c, br, _, err = testNewWSClientWithError(t, testWSClientOptions{
			host:  host,
			port:  port,
			noTLS: !ci.tls,
			path:  mqttWSPath,
		})
		if err == nil {
			c = &mqttWrapAsWs{Conn: c, t: t, br: br}
		}
	} else {
		c, err = net.Dial("tcp", addr)
		if err == nil && ci.tls {
			tc := ci.tlsc
			if tc == nil {
				tc = &tls.Config{InsecureSkipVerify: true}
			}
			c = tls.Client(c, tc)
			c.SetDeadline(time.Now().Add(time.Second))
			err = c.(*tls.Conn).Handshake()
			c.SetDeadline(time.Time{})
		}
	}
	if err != nil {
		if retry(c) {
			goto RETRY
		}
		return nil, nil, fmt.Errorf("Error creating mqtt connection: %v", err)
	}

	proto := mqttCreateConnectProto(ci)
	if _, err := testMQTTWrite(c, proto); err != nil {
		if retry(c) {
			goto RETRY
		}
		return nil, nil, fmt.Errorf("Error writing connect: %v", err)
	}

	buf, err := testMQTTRead(c)
	if err != nil {
		if retry(c) {
			goto RETRY
		}
		return nil, nil, fmt.Errorf("Error reading: %v", err)
	}
	mr := &mqttReader{reader: c}
	mr.reset(buf)

	return c, mr, nil
}

func mqttCreateConnectProto(ci *mqttConnInfo) []byte {
	flags := byte(0)
	if ci.cleanSess {
		flags |= mqttConnFlagCleanSession
	}
	if ci.will != nil {
		flags |= mqttConnFlagWillFlag | (ci.will.qos << 3)
		if ci.will.retain {
			flags |= mqttConnFlagWillRetain
		}
	}
	if ci.user != _EMPTY_ {
		flags |= mqttConnFlagUsernameFlag
	}
	if ci.pass != _EMPTY_ {
		flags |= mqttConnFlagPasswordFlag
	}

	pkLen := 2 + len(mqttProtoName) +
		1 + // proto level
		1 + // flags
		2 + // keepAlive
		2 + len(ci.clientID)

	if ci.will != nil {
		pkLen += 2 + len(ci.will.topic)
		pkLen += 2 + len(ci.will.message)
	}
	if ci.user != _EMPTY_ {
		pkLen += 2 + len(ci.user)
	}
	if ci.pass != _EMPTY_ {
		pkLen += 2 + len(ci.pass)
	}

	w := &mqttWriter{}
	w.WriteByte(mqttPacketConnect)
	w.WriteVarInt(pkLen)
	w.WriteString(string(mqttProtoName))
	w.WriteByte(0x4)
	w.WriteByte(flags)
	w.WriteUint16(ci.keepAlive)
	w.WriteString(ci.clientID)
	if ci.will != nil {
		w.WriteBytes(ci.will.topic)
		w.WriteBytes(ci.will.message)
	}
	if ci.user != _EMPTY_ {
		w.WriteString(ci.user)
	}
	if ci.pass != _EMPTY_ {
		w.WriteBytes([]byte(ci.pass))
	}
	return w.Bytes()
}

func testMQTTCheckConnAck(t testing.TB, r *mqttReader, rc byte, sessionPresent bool) {
	t.Helper()
	b, pl := testMQTTReadPacket(t, r)
	pt := b & mqttPacketMask
	if pt != mqttPacketConnectAck {
		t.Fatalf("Expected ConnAck (%x), got %x", mqttPacketConnectAck, pt)
	}
	if pl != 2 {
		t.Fatalf("ConnAck packet length should be 2, got %v", pl)
	}
	caf, err := r.readByte("connack flags")
	if err != nil {
		t.Fatalf("Error reading packet length: %v", err)
	}
	if caf&0xfe != 0 {
		t.Fatalf("ConnAck flag bits 7-1 should all be 0, got %x", caf>>1)
	}
	if sp := caf == 1; sp != sessionPresent {
		t.Fatalf("Expected session present flag=%v got %v", sessionPresent, sp)
	}
	carc, err := r.readByte("connack return code")
	if err != nil {
		t.Fatalf("Error reading returned code: %v", err)
	}
	if carc != rc {
		t.Fatalf("Expected return code to be %v, got %v", rc, carc)
	}
}

func TestMQTTRequiresJSEnabled(t *testing.T) {
	o := testMQTTDefaultOptions()
	acc := NewAccount("mqtt")
	o.Accounts = []*Account{acc}
	o.Users = []*User{{Username: "mqtt", Account: acc}}
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	addr := fmt.Sprintf("%s:%d", o.MQTT.Host, o.MQTT.Port)
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Error creating mqtt connection: %v", err)
	}
	defer c.Close()

	proto := mqttCreateConnectProto(&mqttConnInfo{cleanSess: true, user: "mqtt"})
	if _, err := testMQTTWrite(c, proto); err != nil {
		t.Fatalf("Error writing connect: %v", err)
	}
	if _, err := testMQTTRead(c); err == nil {
		t.Fatal("Expected failure, did not get one")
	}
}

func testMQTTEnableJSForAccount(t *testing.T, s *Server, accName string) {
	t.Helper()
	acc, err := s.LookupAccount(accName)
	if err != nil {
		t.Fatalf("Error looking up account: %v", err)
	}
	limits := map[string]JetStreamAccountLimits{
		_EMPTY_: {
			MaxConsumers: -1,
			MaxStreams:   -1,
			MaxMemory:    1024 * 1024,
			MaxStore:     1024 * 1024,
		},
	}
	if err := acc.EnableJetStream(limits); err != nil {
		t.Fatalf("Error enabling JS: %v", err)
	}
}

func TestMQTTTLSVerifyAndMap(t *testing.T) {
	accName := "MyAccount"
	acc := NewAccount(accName)
	certUserName := "CN=example.com,OU=NATS.io"
	users := []*User{{Username: certUserName, Account: acc}}

	for _, test := range []struct {
		name        string
		filtering   bool
		provideCert bool
	}{
		{"no filtering, client provides cert", false, true},
		{"no filtering, client does not provide cert", false, false},
		{"filtering, client provides cert", true, true},
		{"filtering, client does not provide cert", true, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			o := testMQTTDefaultOptions()
			o.Host = "localhost"
			o.Accounts = []*Account{acc}
			o.Users = users
			if test.filtering {
				o.Users[0].AllowedConnectionTypes = testCreateAllowedConnectionTypes([]string{jwt.ConnectionTypeStandard, jwt.ConnectionTypeMqtt})
			}
			tc := &TLSConfigOpts{
				CertFile: "../test/configs/certs/tlsauth/server.pem",
				KeyFile:  "../test/configs/certs/tlsauth/server-key.pem",
				CaFile:   "../test/configs/certs/tlsauth/ca.pem",
				Verify:   true,
			}
			tlsc, err := GenTLSConfig(tc)
			if err != nil {
				t.Fatalf("Error creating tls config: %v", err)
			}
			o.MQTT.TLSConfig = tlsc
			o.MQTT.TLSTimeout = 2.0
			o.MQTT.TLSMap = true
			s := testMQTTRunServer(t, o)
			defer testMQTTShutdownServer(s)

			testMQTTEnableJSForAccount(t, s, accName)

			tlscc := &tls.Config{}
			if test.provideCert {
				tc := &TLSConfigOpts{
					CertFile: "../test/configs/certs/tlsauth/client.pem",
					KeyFile:  "../test/configs/certs/tlsauth/client-key.pem",
				}
				var err error
				tlscc, err = GenTLSConfig(tc)
				if err != nil {
					t.Fatalf("Error generating tls config: %v", err)
				}
			}
			tlscc.InsecureSkipVerify = true
			if test.provideCert {
				tlscc.MinVersion = tls.VersionTLS13
			}
			mc, r, err := testMQTTConnectRetryWithError(t, &mqttConnInfo{
				cleanSess: true,
				tls:       true,
				tlsc:      tlscc,
			}, o.MQTT.Host, o.MQTT.Port, 0)
			if !test.provideCert {
				if err == nil {
					t.Fatal("Expected error, did not get one")
				} else if !strings.Contains(err.Error(), "bad certificate") && !strings.Contains(err.Error(), "certificate required") {
					t.Fatalf("Unexpected error: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Error reading: %v", err)
			}
			defer mc.Close()
			testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

			var c *client
			s.mu.Lock()
			for _, sc := range s.clients {
				sc.mu.Lock()
				if sc.isMqtt() {
					c = sc
				}
				sc.mu.Unlock()
				if c != nil {
					break
				}
			}
			s.mu.Unlock()
			if c == nil {
				t.Fatal("Client not found")
			}

			var uname string
			var accname string
			c.mu.Lock()
			uname = c.opts.Username
			if c.acc != nil {
				accname = c.acc.GetName()
			}
			c.mu.Unlock()
			if uname != certUserName {
				t.Fatalf("Expected username %q, got %q", certUserName, uname)
			}
			if accname != accName {
				t.Fatalf("Expected account %q, got %v", accName, accname)
			}
		})
	}
}

func TestMQTTBasicAuth(t *testing.T) {
	for _, test := range []struct {
		name string
		opts func() *Options
		user string
		pass string
		rc   byte
	}{
		{
			"top level auth, no override, wrong u/p",
			func() *Options {
				o := testMQTTDefaultOptions()
				o.Username = "normal"
				o.Password = "client"
				return o
			},
			"mqtt", "client", mqttConnAckRCNotAuthorized,
		},
		{
			"top level auth, no override, correct u/p",
			func() *Options {
				o := testMQTTDefaultOptions()
				o.Username = "normal"
				o.Password = "client"
				return o
			},
			"normal", "client", mqttConnAckRCConnectionAccepted,
		},
		{
			"no top level auth, mqtt auth, wrong u/p",
			func() *Options {
				o := testMQTTDefaultOptions()
				o.MQTT.Username = "mqtt"
				o.MQTT.Password = "client"
				return o
			},
			"normal", "client", mqttConnAckRCNotAuthorized,
		},
		{
			"no top level auth, mqtt auth, correct u/p",
			func() *Options {
				o := testMQTTDefaultOptions()
				o.MQTT.Username = "mqtt"
				o.MQTT.Password = "client"
				return o
			},
			"mqtt", "client", mqttConnAckRCConnectionAccepted,
		},
		{
			"top level auth, mqtt override, wrong u/p",
			func() *Options {
				o := testMQTTDefaultOptions()
				o.Username = "normal"
				o.Password = "client"
				o.MQTT.Username = "mqtt"
				o.MQTT.Password = "client"
				return o
			},
			"normal", "client", mqttConnAckRCNotAuthorized,
		},
		{
			"top level auth, mqtt override, correct u/p",
			func() *Options {
				o := testMQTTDefaultOptions()
				o.Username = "normal"
				o.Password = "client"
				o.MQTT.Username = "mqtt"
				o.MQTT.Password = "client"
				return o
			},
			"mqtt", "client", mqttConnAckRCConnectionAccepted,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			o := test.opts()
			s := testMQTTRunServer(t, o)
			defer testMQTTShutdownServer(s)

			ci := &mqttConnInfo{
				cleanSess: true,
				user:      test.user,
				pass:      test.pass,
			}
			mc, r := testMQTTConnect(t, ci, o.MQTT.Host, o.MQTT.Port)
			defer mc.Close()
			testMQTTCheckConnAck(t, r, test.rc, false)
		})
	}
}

func TestMQTTAuthTimeout(t *testing.T) {
	for _, test := range []struct {
		name string
		at   float64
		mat  float64
		ok   bool
	}{
		{"use top-level auth timeout", 0.5, 0.0, true},
		{"use mqtt auth timeout", 0.5, 0.05, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			o := testMQTTDefaultOptions()
			o.AuthTimeout = test.at
			o.MQTT.Username = "mqtt"
			o.MQTT.Password = "client"
			o.MQTT.AuthTimeout = test.mat
			s := testMQTTRunServer(t, o)
			defer testMQTTShutdownServer(s)

			mc, err := net.Dial("tcp", fmt.Sprintf("%s:%d", o.MQTT.Host, o.MQTT.Port))
			if err != nil {
				t.Fatalf("Error connecting: %v", err)
			}
			defer mc.Close()

			time.Sleep(100 * time.Millisecond)

			ci := &mqttConnInfo{
				cleanSess: true,
				user:      "mqtt",
				pass:      "client",
			}
			proto := mqttCreateConnectProto(ci)
			if _, err := testMQTTWrite(mc, proto); err != nil {
				if test.ok {
					t.Fatalf("Error sending connect: %v", err)
				}
				// else it is ok since we got disconnected due to auth timeout
				return
			}
			buf, err := testMQTTRead(mc)
			if err != nil {
				if test.ok {
					t.Fatalf("Error reading: %v", err)
				}
				// else it is ok since we got disconnected due to auth timeout
				return
			}
			r := &mqttReader{reader: mc}
			r.reset(buf)
			testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

			time.Sleep(500 * time.Millisecond)
			testMQTTPublish(t, mc, r, 1, false, false, "foo", 1, []byte("msg"))
		})
	}
}

func TestMQTTTokenAuth(t *testing.T) {
	for _, test := range []struct {
		name  string
		opts  func() *Options
		token string
		rc    byte
	}{
		{
			"top level auth, no override, wrong token",
			func() *Options {
				o := testMQTTDefaultOptions()
				o.Authorization = "goodtoken"
				return o
			},
			"badtoken", mqttConnAckRCNotAuthorized,
		},
		{
			"top level auth, no override, correct token",
			func() *Options {
				o := testMQTTDefaultOptions()
				o.Authorization = "goodtoken"
				return o
			},
			"goodtoken", mqttConnAckRCConnectionAccepted,
		},
		{
			"no top level auth, mqtt auth, wrong token",
			func() *Options {
				o := testMQTTDefaultOptions()
				o.MQTT.Token = "goodtoken"
				return o
			},
			"badtoken", mqttConnAckRCNotAuthorized,
		},
		{
			"no top level auth, mqtt auth, correct token",
			func() *Options {
				o := testMQTTDefaultOptions()
				o.MQTT.Token = "goodtoken"
				return o
			},
			"goodtoken", mqttConnAckRCConnectionAccepted,
		},
		{
			"top level auth, mqtt override, wrong token",
			func() *Options {
				o := testMQTTDefaultOptions()
				o.Authorization = "clienttoken"
				o.MQTT.Token = "mqtttoken"
				return o
			},
			"clienttoken", mqttConnAckRCNotAuthorized,
		},
		{
			"top level auth, mqtt override, correct token",
			func() *Options {
				o := testMQTTDefaultOptions()
				o.Authorization = "clienttoken"
				o.MQTT.Token = "mqtttoken"
				return o
			},
			"mqtttoken", mqttConnAckRCConnectionAccepted,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			o := test.opts()
			s := testMQTTRunServer(t, o)
			defer testMQTTShutdownServer(s)

			ci := &mqttConnInfo{
				cleanSess: true,
				user:      "ignore_use_token",
				pass:      test.token,
			}
			mc, r := testMQTTConnect(t, ci, o.MQTT.Host, o.MQTT.Port)
			defer mc.Close()
			testMQTTCheckConnAck(t, r, test.rc, false)
		})
	}
}

func TestMQTTJWTWithAllowedConnectionTypes(t *testing.T) {
	o := testMQTTDefaultOptions()
	// Create System Account
	syskp, _ := nkeys.CreateAccount()
	syspub, _ := syskp.PublicKey()
	sysAc := jwt.NewAccountClaims(syspub)
	sysjwt, err := sysAc.Encode(oKp)
	if err != nil {
		t.Fatalf("Error generating account JWT: %v", err)
	}
	// Create memory resolver and store system account
	mr := &MemAccResolver{}
	mr.Store(syspub, sysjwt)
	if err != nil {
		t.Fatalf("Error saving system account JWT to memory resolver: %v", err)
	}
	// Add system account and memory resolver to server options
	o.SystemAccount = syspub
	o.AccountResolver = mr
	setupAddTrusted(o)

	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	for _, test := range []struct {
		name            string
		connectionTypes []string
		rc              byte
	}{
		{"not allowed", []string{jwt.ConnectionTypeStandard}, mqttConnAckRCNotAuthorized},
		{"allowed", []string{jwt.ConnectionTypeStandard, strings.ToLower(jwt.ConnectionTypeMqtt)}, mqttConnAckRCConnectionAccepted},
		{"allowed with unknown", []string{jwt.ConnectionTypeMqtt, "SomeNewType"}, mqttConnAckRCConnectionAccepted},
		{"not allowed with unknown", []string{"SomeNewType"}, mqttConnAckRCNotAuthorized},
	} {
		t.Run(test.name, func(t *testing.T) {

			nuc := newJWTTestUserClaims()
			nuc.AllowedConnectionTypes = test.connectionTypes
			nuc.BearerToken = true

			okp, _ := nkeys.FromSeed(oSeed)

			akp, _ := nkeys.CreateAccount()
			apub, _ := akp.PublicKey()
			nac := jwt.NewAccountClaims(apub)
			// Enable Jetstream on account with lax limitations
			nac.Limits.JetStreamLimits.Consumer = -1
			nac.Limits.JetStreamLimits.Streams = -1
			nac.Limits.JetStreamLimits.MemoryStorage = 1024 * 1024
			nac.Limits.JetStreamLimits.DiskStorage = 1024 * 1024
			ajwt, err := nac.Encode(okp)
			if err != nil {
				t.Fatalf("Error generating account JWT: %v", err)
			}

			nkp, _ := nkeys.CreateUser()
			pub, _ := nkp.PublicKey()
			nuc.Subject = pub
			jwt, err := nuc.Encode(akp)
			if err != nil {
				t.Fatalf("Error generating user JWT: %v", err)
			}

			addAccountToMemResolver(s, apub, ajwt)

			ci := &mqttConnInfo{
				cleanSess: true,
				user:      "ignore_use_token",
				pass:      jwt,
			}

			mc, r := testMQTTConnect(t, ci, o.MQTT.Host, o.MQTT.Port)
			defer mc.Close()
			testMQTTCheckConnAck(t, r, test.rc, false)
		})
	}
}

func TestMQTTUsersAuth(t *testing.T) {
	users := []*User{{Username: "user", Password: "pwd"}}
	for _, test := range []struct {
		name string
		opts func() *Options
		user string
		pass string
		rc   byte
	}{
		{
			"no filtering, wrong user",
			func() *Options {
				o := testMQTTDefaultOptions()
				o.Users = users
				return o
			},
			"wronguser", "pwd", mqttConnAckRCNotAuthorized,
		},
		{
			"no filtering, correct user",
			func() *Options {
				o := testMQTTDefaultOptions()
				o.Users = users
				return o
			},
			"user", "pwd", mqttConnAckRCConnectionAccepted,
		},
		{
			"filtering, user not allowed",
			func() *Options {
				o := testMQTTDefaultOptions()
				o.Users = users
				// Only allowed for regular clients
				o.Users[0].AllowedConnectionTypes = testCreateAllowedConnectionTypes([]string{jwt.ConnectionTypeStandard})
				return o
			},
			"user", "pwd", mqttConnAckRCNotAuthorized,
		},
		{
			"filtering, user allowed",
			func() *Options {
				o := testMQTTDefaultOptions()
				o.Users = users
				o.Users[0].AllowedConnectionTypes = testCreateAllowedConnectionTypes([]string{jwt.ConnectionTypeStandard, jwt.ConnectionTypeMqtt})
				return o
			},
			"user", "pwd", mqttConnAckRCConnectionAccepted,
		},
		{
			"filtering, wrong password",
			func() *Options {
				o := testMQTTDefaultOptions()
				o.Users = users
				o.Users[0].AllowedConnectionTypes = testCreateAllowedConnectionTypes([]string{jwt.ConnectionTypeStandard, jwt.ConnectionTypeMqtt})
				return o
			},
			"user", "badpassword", mqttConnAckRCNotAuthorized,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			o := test.opts()
			s := testMQTTRunServer(t, o)
			defer testMQTTShutdownServer(s)

			ci := &mqttConnInfo{
				cleanSess: true,
				user:      test.user,
				pass:      test.pass,
			}
			mc, r := testMQTTConnect(t, ci, o.MQTT.Host, o.MQTT.Port)
			defer mc.Close()
			testMQTTCheckConnAck(t, r, test.rc, false)
		})
	}
}

func TestMQTTNoAuthUserValidation(t *testing.T) {
	o := testMQTTDefaultOptions()
	o.Users = []*User{{Username: "user", Password: "pwd"}}
	// Should fail because it is not part of o.Users.
	o.MQTT.NoAuthUser = "notfound"
	if _, err := NewServer(o); err == nil || !strings.Contains(err.Error(), "not present as user") {
		t.Fatalf("Expected error saying not present as user, got %v", err)
	}

	// Set a valid no auth user for global options, but still should fail because
	// of o.MQTT.NoAuthUser
	o.NoAuthUser = "user"
	o.MQTT.NoAuthUser = "notfound"
	if _, err := NewServer(o); err == nil || !strings.Contains(err.Error(), "not present as user") {
		t.Fatalf("Expected error saying not present as user, got %v", err)
	}
}

func TestMQTTNoAuthUser(t *testing.T) {
	for _, test := range []struct {
		name         string
		override     bool
		useAuth      bool
		expectedUser string
		expectedAcc  string
	}{
		{"no override, no user provided", false, false, "noauth", "normal"},
		{"no override, user povided", false, true, "user", "normal"},
		{"override, no user provided", true, false, "mqttnoauth", "mqtt"},
		{"override, user provided", true, true, "mqttuser", "mqtt"},
	} {
		t.Run(test.name, func(t *testing.T) {
			o := testMQTTDefaultOptions()
			normalAcc := NewAccount("normal")
			mqttAcc := NewAccount("mqtt")
			o.Accounts = []*Account{normalAcc, mqttAcc}
			o.Users = []*User{
				{Username: "noauth", Password: "pwd", Account: normalAcc},
				{Username: "user", Password: "pwd", Account: normalAcc},
				{Username: "mqttnoauth", Password: "pwd", Account: mqttAcc},
				{Username: "mqttuser", Password: "pwd", Account: mqttAcc},
			}
			o.NoAuthUser = "noauth"
			if test.override {
				o.MQTT.NoAuthUser = "mqttnoauth"
			}
			s := testMQTTRunServer(t, o)
			defer testMQTTShutdownServer(s)

			testMQTTEnableJSForAccount(t, s, "normal")
			testMQTTEnableJSForAccount(t, s, "mqtt")

			ci := &mqttConnInfo{clientID: "mqtt", cleanSess: true}
			if test.useAuth {
				ci.user = test.expectedUser
				ci.pass = "pwd"
			}
			mc, r := testMQTTConnect(t, ci, o.MQTT.Host, o.MQTT.Port)
			defer mc.Close()
			testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

			c := testMQTTGetClient(t, s, "mqtt")
			c.mu.Lock()
			uname := c.opts.Username
			aname := c.acc.GetName()
			c.mu.Unlock()
			if uname != test.expectedUser {
				t.Fatalf("Expected selected user to be %q, got %q", test.expectedUser, uname)
			}
			if aname != test.expectedAcc {
				t.Fatalf("Expected selected account to be %q, got %q", test.expectedAcc, aname)
			}
		})
	}
}

func TestMQTTConnectNotFirstPacket(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	l := &captureErrorLogger{errCh: make(chan string, 10)}
	s.SetLogger(l, false, false)

	c, err := net.Dial("tcp", fmt.Sprintf("%s:%d", o.MQTT.Host, o.MQTT.Port))
	if err != nil {
		t.Fatalf("Error on dial: %v", err)
	}
	defer c.Close()

	w := &mqttWriter{}
	mqttWritePublish(w, 0, false, false, "foo", 0, []byte("hello"))
	if _, err := testMQTTWrite(c, w.Bytes()); err != nil {
		t.Fatalf("Error publishing: %v", err)
	}
	testMQTTExpectDisconnect(t, c)

	select {
	case err := <-l.errCh:
		if !strings.Contains(err, "should be a CONNECT") {
			t.Fatalf("Expected error about first packet being a CONNECT, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Did not log any error")
	}
}

func TestMQTTSecondConnect(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	mc, r := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mc.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

	proto := mqttCreateConnectProto(&mqttConnInfo{cleanSess: true})
	if _, err := testMQTTWrite(mc, proto); err != nil {
		t.Fatalf("Error writing connect: %v", err)
	}
	testMQTTExpectDisconnect(t, mc)
}

func TestMQTTParseConnect(t *testing.T) {
	for _, test := range []struct {
		name  string
		proto []byte
		pl    int
		err   string
	}{
		{"packet in buffer error", []byte{0}, 10, io.ErrUnexpectedEOF.Error()},
		{"bad proto name", []byte{0, 4, 'B', 'A', 'D'}, 5, "protocol name"},
		{"invalid proto name", []byte{0, 3, 'B', 'A', 'D'}, 5, "expected connect packet with protocol name"},
		{"old proto not supported", []byte{0, 6, 'M', 'Q', 'I', 's', 'd', 'p'}, 8, "older protocol"},
		{"error on protocol level", []byte{0, 4, 'M', 'Q', 'T', 'T'}, 6, "protocol level"},
		{"unacceptable protocol version", []byte{0, 4, 'M', 'Q', 'T', 'T', 10}, 7, "unacceptable protocol version"},
		{"error on flags", []byte{0, 4, 'M', 'Q', 'T', 'T', mqttProtoLevel}, 7, "flags"},
		{"reserved flag", []byte{0, 4, 'M', 'Q', 'T', 'T', mqttProtoLevel, 1}, 8, errMQTTConnFlagReserved.Error()},
		{"will qos without will flag", []byte{0, 4, 'M', 'Q', 'T', 'T', mqttProtoLevel, 1 << 3}, 8, "if Will flag is set to 0, Will QoS must be 0 too"},
		{"will retain without will flag", []byte{0, 4, 'M', 'Q', 'T', 'T', mqttProtoLevel, 1 << 5}, 8, errMQTTWillAndRetainFlag.Error()},
		{"will qos", []byte{0, 4, 'M', 'Q', 'T', 'T', mqttProtoLevel, 3<<3 | 1<<2}, 8, "if Will flag is set to 1, Will QoS can be 0, 1 or 2"},
		{"no user but password", []byte{0, 4, 'M', 'Q', 'T', 'T', mqttProtoLevel, mqttConnFlagPasswordFlag}, 8, errMQTTPasswordFlagAndNoUser.Error()},
		{"missing keep alive", []byte{0, 4, 'M', 'Q', 'T', 'T', mqttProtoLevel, 0}, 8, "keep alive"},
		{"missing client ID", []byte{0, 4, 'M', 'Q', 'T', 'T', mqttProtoLevel, 0, 0, 1}, 10, "client ID"},
		{"empty client ID", []byte{0, 4, 'M', 'Q', 'T', 'T', mqttProtoLevel, 0, 0, 1, 0, 0}, 12, errMQTTCIDEmptyNeedsCleanFlag.Error()},
		{"invalid utf8 client ID", []byte{0, 4, 'M', 'Q', 'T', 'T', mqttProtoLevel, 0, 0, 1, 0, 1, 241}, 13, "invalid utf8 for client ID"},
		{"missing will topic", []byte{0, 4, 'M', 'Q', 'T', 'T', mqttProtoLevel, mqttConnFlagWillFlag | mqttConnFlagCleanSession, 0, 0, 0, 0}, 12, "Will topic"},
		{"empty will topic", []byte{0, 4, 'M', 'Q', 'T', 'T', mqttProtoLevel, mqttConnFlagWillFlag | mqttConnFlagCleanSession, 0, 0, 0, 0, 0, 0}, 14, errMQTTEmptyWillTopic.Error()},
		{"invalid utf8 will topic", []byte{0, 4, 'M', 'Q', 'T', 'T', mqttProtoLevel, mqttConnFlagWillFlag | mqttConnFlagCleanSession, 0, 0, 0, 0, 0, 1, 241}, 15, "invalid utf8 for Will topic"},
		{"invalid wildcard will topic", []byte{0, 4, 'M', 'Q', 'T', 'T', mqttProtoLevel, mqttConnFlagWillFlag | mqttConnFlagCleanSession, 0, 0, 0, 0, 0, 1, '#'}, 15, "wildcards not allowed"},
		{"error on will message", []byte{0, 4, 'M', 'Q', 'T', 'T', mqttProtoLevel, mqttConnFlagWillFlag | mqttConnFlagCleanSession, 0, 0, 0, 0, 0, 1, 'a', 0, 3}, 17, "Will message"},
		{"error on username", []byte{0, 4, 'M', 'Q', 'T', 'T', mqttProtoLevel, mqttConnFlagUsernameFlag | mqttConnFlagCleanSession, 0, 0, 0, 0}, 12, "user name"},
		{"empty username", []byte{0, 4, 'M', 'Q', 'T', 'T', mqttProtoLevel, mqttConnFlagUsernameFlag | mqttConnFlagCleanSession, 0, 0, 0, 0, 0, 0}, 14, errMQTTEmptyUsername.Error()},
		{"invalid utf8 username", []byte{0, 4, 'M', 'Q', 'T', 'T', mqttProtoLevel, mqttConnFlagUsernameFlag | mqttConnFlagCleanSession, 0, 0, 0, 0, 0, 1, 241}, 15, "invalid utf8 for user name"},
		{"error on password", []byte{0, 4, 'M', 'Q', 'T', 'T', mqttProtoLevel, mqttConnFlagUsernameFlag | mqttConnFlagPasswordFlag | mqttConnFlagCleanSession, 0, 0, 0, 0, 0, 1, 'a'}, 15, "password"},
	} {
		t.Run(test.name, func(t *testing.T) {
			r := &mqttReader{}
			r.reset(test.proto)
			mqtt := &mqtt{r: r}
			c := &client{mqtt: mqtt}
			if _, _, err := c.mqttParseConnect(r, test.pl, false); err == nil || !strings.Contains(err.Error(), test.err) {
				t.Fatalf("Expected error %q, got %v", test.err, err)
			}
		})
	}
}

func TestMQTTConnectFailsOnParse(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	addr := fmt.Sprintf("%s:%d", o.MQTT.Host, o.MQTT.Port)
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Error creating mqtt connection: %v", err)
	}

	pkLen := 2 + len(mqttProtoName) +
		1 + // proto level
		1 + // flags
		2 + // keepAlive
		2 + len("mqtt")

	w := &mqttWriter{}
	w.WriteByte(mqttPacketConnect)
	w.WriteVarInt(pkLen)
	w.WriteString(string(mqttProtoName))
	w.WriteByte(0x7)
	w.WriteByte(mqttConnFlagCleanSession)
	w.WriteUint16(0)
	w.WriteString("mqtt")
	c.Write(w.Bytes())

	buf, err := testMQTTRead(c)
	if err != nil {
		t.Fatalf("Error reading: %v", err)
	}
	r := &mqttReader{reader: c}
	r.reset(buf)
	testMQTTCheckConnAck(t, r, mqttConnAckRCUnacceptableProtocolVersion, false)
}

func TestMQTTConnKeepAlive(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	mc, r := testMQTTConnect(t, &mqttConnInfo{cleanSess: true, keepAlive: 1}, o.MQTT.Host, o.MQTT.Port)
	defer mc.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

	testMQTTPublish(t, mc, r, 0, false, false, "foo", 0, []byte("msg"))

	time.Sleep(2 * time.Second)
	testMQTTExpectDisconnect(t, mc)
}

func TestMQTTDontSetPinger(t *testing.T) {
	o := testMQTTDefaultOptions()
	o.PingInterval = 15 * time.Millisecond
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	mc, r := testMQTTConnect(t, &mqttConnInfo{clientID: "mqtt", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mc.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

	c := testMQTTGetClient(t, s, "mqtt")
	c.mu.Lock()
	timerSet := c.ping.tmr != nil
	c.mu.Unlock()
	if timerSet {
		t.Fatalf("Ping timer should not be set for MQTT clients")
	}

	// Wait a bit and expect nothing (and connection should still be valid)
	testMQTTExpectNothing(t, r)
	testMQTTPublish(t, mc, r, 0, false, false, "foo", 0, []byte("msg"))
}

func TestMQTTUnsupportedPackets(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	for _, test := range []struct {
		name       string
		packetType byte
	}{
		{"pubrec", mqttPacketPubRec},
		{"pubrel", mqttPacketPubRel},
		{"pubcomp", mqttPacketPubComp},
	} {
		t.Run(test.name, func(t *testing.T) {
			mc, r := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
			defer mc.Close()
			testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

			w := &mqttWriter{}
			pt := test.packetType
			if test.packetType == mqttPacketPubRel {
				pt |= byte(0x2)
			}
			w.WriteByte(pt)
			w.WriteVarInt(2)
			w.WriteUint16(1)
			mc.Write(w.Bytes())

			testMQTTExpectDisconnect(t, mc)
		})
	}
}

func TestMQTTTopicAndSubjectConversion(t *testing.T) {
	for _, test := range []struct {
		name        string
		mqttTopic   string
		natsSubject string
		err         string
	}{
		{"/", "/", "/./", ""},
		{"//", "//", "/././", ""},
		{"///", "///", "/./././", ""},
		{"////", "////", "/././././", ""},
		{"foo", "foo", "foo", ""},
		{"/foo", "/foo", "/.foo", ""},
		{"//foo", "//foo", "/./.foo", ""},
		{"///foo", "///foo", "/././.foo", ""},
		{"///foo/", "///foo/", "/././.foo./", ""},
		{"///foo//", "///foo//", "/././.foo././", ""},
		{"///foo///", "///foo///", "/././.foo./././", ""},
		{"//.foo.//", "//.foo.//", "/././/foo//././", ""},
		{"foo/bar", "foo/bar", "foo.bar", ""},
		{"/foo/bar", "/foo/bar", "/.foo.bar", ""},
		{"/foo/bar/", "/foo/bar/", "/.foo.bar./", ""},
		{"foo/bar/baz", "foo/bar/baz", "foo.bar.baz", ""},
		{"/foo/bar/baz", "/foo/bar/baz", "/.foo.bar.baz", ""},
		{"/foo/bar/baz/", "/foo/bar/baz/", "/.foo.bar.baz./", ""},
		{"bar", "bar/", "bar./", ""},
		{"bar//", "bar//", "bar././", ""},
		{"bar///", "bar///", "bar./././", ""},
		{"foo//bar", "foo//bar", "foo./.bar", ""},
		{"foo///bar", "foo///bar", "foo././.bar", ""},
		{"foo////bar", "foo////bar", "foo./././.bar", ""},
		{".", ".", "//", ""},
		{"..", "..", "////", ""},
		{"...", "...", "//////", ""},
		{"./", "./", "//./", ""},
		{".//.", ".//.", "//././/", ""},
		{"././.", "././.", "//.//.//", ""},
		{"././/.", "././/.", "//.//././/", ""},
		{".foo", ".foo", "//foo", ""},
		{"foo.", "foo.", "foo//", ""},
		{".foo.", ".foo.", "//foo//", ""},
		{"foo../bar/", "foo../bar/", "foo////.bar./", ""},
		{"foo../bar/.", "foo../bar/.", "foo////.bar.//", ""},
		{"/foo/", "/foo/", "/.foo./", ""},
		{"./foo/.", "./foo/.", "//.foo.//", ""},
		{"foo.bar/baz", "foo.bar/baz", "foo//bar.baz", ""},
		// These should produce errors
		{"foo/+", "foo/+", "", "wildcards not allowed in publish"},
		{"foo/#", "foo/#", "", "wildcards not allowed in publish"},
		{"foo bar", "foo bar", "", "not supported"},
	} {
		t.Run(test.name, func(t *testing.T) {
			res, err := mqttTopicToNATSPubSubject([]byte(test.mqttTopic))
			if test.err != _EMPTY_ {
				if err == nil || !strings.Contains(err.Error(), test.err) {
					t.Fatalf("Expected error %q, got %v", test.err, err)
				}
				return
			}
			toNATS := string(res)
			if toNATS != test.natsSubject {
				t.Fatalf("Expected subject %q got %q", test.natsSubject, toNATS)
			}

			res = natsSubjectToMQTTTopic(string(res))
			backToMQTT := string(res)
			if backToMQTT != test.mqttTopic {
				t.Fatalf("Expected topic %q got %q (NATS conversion was %q)", test.mqttTopic, backToMQTT, toNATS)
			}
		})
	}
}

func TestMQTTFilterConversion(t *testing.T) {
	// Similar to TopicConversion test except that wildcards are OK here.
	// So testing only those.
	for _, test := range []struct {
		name        string
		mqttTopic   string
		natsSubject string
	}{
		{"single level wildcard", "+", "*"},
		{"single level wildcard", "/+", "/.*"},
		{"single level wildcard", "+/", "*./"},
		{"single level wildcard", "/+/", "/.*./"},
		{"single level wildcard", "foo/+", "foo.*"},
		{"single level wildcard", "foo/+/", "foo.*./"},
		{"single level wildcard", "foo/+/bar", "foo.*.bar"},
		{"single level wildcard", "foo/+/+", "foo.*.*"},
		{"single level wildcard", "foo/+/+/", "foo.*.*./"},
		{"single level wildcard", "foo/+/+/bar", "foo.*.*.bar"},
		{"single level wildcard", "foo//+", "foo./.*"},
		{"single level wildcard", "foo//+/", "foo./.*./"},
		{"single level wildcard", "foo//+//", "foo./.*././"},
		{"single level wildcard", "foo//+//bar", "foo./.*./.bar"},
		{"single level wildcard", "foo///+///bar", "foo././.*././.bar"},
		{"single level wildcard", "foo.bar///+///baz", "foo//bar././.*././.baz"},

		{"multi level wildcard", "#", ">"},
		{"multi level wildcard", "/#", "/.>"},
		{"multi level wildcard", "/foo/#", "/.foo.>"},
		{"multi level wildcard", "foo/#", "foo.>"},
		{"multi level wildcard", "foo//#", "foo./.>"},
		{"multi level wildcard", "foo///#", "foo././.>"},
		{"multi level wildcard", "foo/bar/#", "foo.bar.>"},
		{"multi level wildcard", "foo/bar.baz/#", "foo.bar//baz.>"},
	} {
		t.Run(test.name, func(t *testing.T) {
			res, err := mqttFilterToNATSSubject([]byte(test.mqttTopic))
			if err != nil {
				t.Fatalf("Error: %v", err)
			}
			if string(res) != test.natsSubject {
				t.Fatalf("Expected subject %q got %q", test.natsSubject, res)
			}
		})
	}
}

func TestMQTTParseSub(t *testing.T) {
	for _, test := range []struct {
		name  string
		proto []byte
		b     byte
		pl    int
		err   string
	}{
		{"reserved flag", nil, 3, 0, "wrong subscribe reserved flags"},
		{"ensure packet loaded", []byte{1, 2}, mqttSubscribeFlags, 10, io.ErrUnexpectedEOF.Error()},
		{"error reading packet id", []byte{1}, mqttSubscribeFlags, 1, "reading packet identifier"},
		{"missing filters", []byte{0, 1}, mqttSubscribeFlags, 2, "subscribe protocol must contain at least 1 topic filter"},
		{"error reading topic", []byte{0, 1, 0, 2, 'a'}, mqttSubscribeFlags, 5, "topic filter"},
		{"empty topic", []byte{0, 1, 0, 0}, mqttSubscribeFlags, 4, errMQTTTopicFilterCannotBeEmpty.Error()},
		{"invalid utf8 topic", []byte{0, 1, 0, 1, 241}, mqttSubscribeFlags, 5, "invalid utf8 for topic filter"},
		{"missing qos", []byte{0, 1, 0, 1, 'a'}, mqttSubscribeFlags, 5, "QoS"},
		{"invalid qos", []byte{0, 1, 0, 1, 'a', 3}, mqttSubscribeFlags, 6, "subscribe QoS value must be 0, 1 or 2"},
	} {
		t.Run(test.name, func(t *testing.T) {
			r := &mqttReader{}
			r.reset(test.proto)
			mqtt := &mqtt{r: r}
			c := &client{mqtt: mqtt}
			if _, _, err := c.mqttParseSubsOrUnsubs(r, test.b, test.pl, true); err == nil || !strings.Contains(err.Error(), test.err) {
				t.Fatalf("Expected error %q, got %v", test.err, err)
			}
		})
	}
}

func testMQTTSub(t testing.TB, pi uint16, c net.Conn, r *mqttReader, filters []*mqttFilter, expected []byte) {
	t.Helper()
	w := &mqttWriter{}
	pkLen := 2 // for pi
	for i := 0; i < len(filters); i++ {
		f := filters[i]
		pkLen += 2 + len(f.filter) + 1
	}
	w.WriteByte(mqttPacketSub | mqttSubscribeFlags)
	w.WriteVarInt(pkLen)
	w.WriteUint16(pi)
	for i := 0; i < len(filters); i++ {
		f := filters[i]
		w.WriteBytes([]byte(f.filter))
		w.WriteByte(f.qos)
	}
	if _, err := testMQTTWrite(c, w.Bytes()); err != nil {
		t.Fatalf("Error writing SUBSCRIBE protocol: %v", err)
	}
	b, pl := testMQTTReadPacket(t, r)
	if pt := b & mqttPacketMask; pt != mqttPacketSubAck {
		t.Fatalf("Expected SUBACK packet %x, got %x", mqttPacketSubAck, pt)
	}
	rpi, err := r.readUint16("packet identifier")
	if err != nil || rpi != pi {
		t.Fatalf("Error with packet identifier expected=%v got: %v err=%v", pi, rpi, err)
	}
	for i, rem := 0, pl-2; rem > 0; rem-- {
		qos, err := r.readByte("filter qos")
		if err != nil {
			t.Fatal(err)
		}
		if qos != expected[i] {
			t.Fatalf("For topic filter %q expected qos of %v, got %v",
				filters[i].filter, expected[i], qos)
		}
		i++
	}
}

func TestMQTTSubAck(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	mc, r := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mc.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

	subs := []*mqttFilter{
		{filter: "foo", qos: 0},
		{filter: "bar", qos: 1},
		{filter: "baz", qos: 2},       // Since we don't support, we should receive a result of 1
		{filter: "foo/#/bar", qos: 0}, // Invalid sub, so we should receive a result of mqttSubAckFailure
	}
	expected := []byte{
		0,
		1,
		1,
		mqttSubAckFailure,
	}
	testMQTTSub(t, 1, mc, r, subs, expected)
}

func testMQTTFlush(t testing.TB, c net.Conn, bw *bufio.Writer, r *mqttReader) {
	t.Helper()
	w := &mqttWriter{}
	w.WriteByte(mqttPacketPing)
	w.WriteByte(0)
	if bw != nil {
		bw.Write(w.Bytes())
		bw.Flush()
	} else {
		c.Write(w.Bytes())
	}
	ab, l := testMQTTReadPacket(t, r)
	if pt := ab & mqttPacketMask; pt != mqttPacketPingResp {
		t.Fatalf("Expected ping response got %x", pt)
	}
	if l != 0 {
		t.Fatalf("Expected PINGRESP length to be 0, got %v", l)
	}
}

func testMQTTExpectNothing(t testing.TB, r *mqttReader) {
	t.Helper()
	var buf [128]byte
	r.reader.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
	if n, err := r.reader.Read(buf[:]); err == nil {
		t.Fatalf("Expected nothing, got %v", buf[:n])
	}
	r.reader.SetReadDeadline(time.Time{})
}

func testMQTTCheckPubMsg(t testing.TB, c net.Conn, r *mqttReader, topic string, flags byte, payload []byte) {
	t.Helper()
	pflags, pi := testMQTTGetPubMsg(t, c, r, topic, payload)
	if pflags != flags {
		t.Fatalf("Expected flags to be %x, got %x", flags, pflags)
	}
	if pi > 0 {
		testMQTTSendPubAck(t, c, pi)
	}
}

func testMQTTCheckPubMsgNoAck(t testing.TB, c net.Conn, r *mqttReader, topic string, flags byte, payload []byte) uint16 {
	t.Helper()
	pflags, pi := testMQTTGetPubMsg(t, c, r, topic, payload)
	if pflags != flags {
		t.Fatalf("Expected flags to be %x, got %x", flags, pflags)
	}
	return pi
}

func testMQTTGetPubMsg(t testing.TB, c net.Conn, r *mqttReader, topic string, payload []byte) (byte, uint16) {
	flags, pi, _ := testMQTTGetPubMsgEx(t, c, r, topic, payload)
	return flags, pi
}

func testMQTTGetPubMsgEx(t testing.TB, c net.Conn, r *mqttReader, topic string, payload []byte) (byte, uint16, string) {
	t.Helper()
	b, pl := testMQTTReadPacket(t, r)
	if pt := b & mqttPacketMask; pt != mqttPacketPub {
		t.Fatalf("Expected PUBLISH packet %x, got %x", mqttPacketPub, pt)
	}
	pflags := b & mqttPacketFlagMask
	qos := (pflags & mqttPubFlagQoS) >> 1
	start := r.pos
	ptopic, err := r.readString("topic name")
	if err != nil {
		t.Fatal(err)
	}
	if topic != _EMPTY_ && ptopic != topic {
		t.Fatalf("Expected topic %q, got %q", topic, ptopic)
	}
	var pi uint16
	if qos > 0 {
		pi, err = r.readUint16("packet identifier")
		if err != nil {
			t.Fatal(err)
		}
	}
	msgLen := pl - (r.pos - start)
	if r.pos+msgLen > len(r.buf) {
		t.Fatalf("computed message length goes beyond buffer: ml=%v pos=%v lenBuf=%v",
			msgLen, r.pos, len(r.buf))
	}
	ppayload := r.buf[r.pos : r.pos+msgLen]
	if !bytes.Equal(payload, ppayload) {
		t.Fatalf("Expected payload %q, got %q", payload, ppayload)
	}
	r.pos += msgLen
	return pflags, pi, ptopic
}

func testMQTTSendPubAck(t testing.TB, c net.Conn, pi uint16) {
	t.Helper()
	w := &mqttWriter{}
	w.WriteByte(mqttPacketPubAck)
	w.WriteVarInt(2)
	w.WriteUint16(pi)
	if _, err := testMQTTWrite(c, w.Bytes()); err != nil {
		t.Fatalf("Error writing PUBACK: %v", err)
	}
}

func testMQTTPublish(t testing.TB, c net.Conn, r *mqttReader, qos byte, dup, retain bool, topic string, pi uint16, payload []byte) {
	t.Helper()
	w := &mqttWriter{}
	mqttWritePublish(w, qos, dup, retain, topic, pi, payload)
	if _, err := testMQTTWrite(c, w.Bytes()); err != nil {
		t.Fatalf("Error writing PUBLISH proto: %v", err)
	}
	if qos > 0 {
		// Since we don't support QoS 2, we should get disconnected
		if qos == 2 {
			testMQTTExpectDisconnect(t, c)
			return
		}
		b, _ := testMQTTReadPacket(t, r)
		if pt := b & mqttPacketMask; pt != mqttPacketPubAck {
			t.Fatalf("Expected PUBACK packet %x, got %x", mqttPacketPubAck, pt)
		}
		rpi, err := r.readUint16("packet identifier")
		if err != nil || rpi != pi {
			t.Fatalf("Error with packet identifier expected=%v got: %v err=%v", pi, rpi, err)
		}
	}
}

func TestMQTTParsePub(t *testing.T) {
	for _, test := range []struct {
		name  string
		flags byte
		proto []byte
		pl    int
		err   string
	}{
		{"qos not supported", 0x4, nil, 0, "not supported"},
		{"packet in buffer error", 0, nil, 10, io.ErrUnexpectedEOF.Error()},
		{"error on topic", 0, []byte{0, 3, 'f', 'o'}, 4, "topic"},
		{"empty topic", 0, []byte{0, 0}, 2, errMQTTTopicIsEmpty.Error()},
		{"wildcards topic", 0, []byte{0, 1, '#'}, 3, "wildcards not allowed"},
		{"error on packet identifier", mqttPubQos1, []byte{0, 3, 'f', 'o', 'o'}, 5, "packet identifier"},
		{"invalid packet identifier", mqttPubQos1, []byte{0, 3, 'f', 'o', 'o', 0, 0}, 7, errMQTTPacketIdentifierIsZero.Error()},
	} {
		t.Run(test.name, func(t *testing.T) {
			r := &mqttReader{}
			r.reset(test.proto)
			mqtt := &mqtt{r: r}
			c := &client{mqtt: mqtt}
			pp := &mqttPublish{flags: test.flags}
			if err := c.mqttParsePub(r, test.pl, pp, false); err == nil || !strings.Contains(err.Error(), test.err) {
				t.Fatalf("Expected error %q, got %v", test.err, err)
			}
		})
	}
}

func TestMQTTParsePubAck(t *testing.T) {
	for _, test := range []struct {
		name  string
		proto []byte
		pl    int
		err   string
	}{
		{"packet in buffer error", nil, 10, io.ErrUnexpectedEOF.Error()},
		{"error reading packet identifier", []byte{0}, 1, "packet identifier"},
		{"invalid packet identifier", []byte{0, 0}, 2, errMQTTPacketIdentifierIsZero.Error()},
	} {
		t.Run(test.name, func(t *testing.T) {
			r := &mqttReader{}
			r.reset(test.proto)
			if _, err := mqttParsePubAck(r, test.pl); err == nil || !strings.Contains(err.Error(), test.err) {
				t.Fatalf("Expected error %q, got %v", test.err, err)
			}
		})
	}
}

func TestMQTTPublish(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	nc := natsConnect(t, s.ClientURL())
	defer nc.Close()

	mcp, mpr := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mcp.Close()
	testMQTTCheckConnAck(t, mpr, mqttConnAckRCConnectionAccepted, false)

	testMQTTPublish(t, mcp, mpr, 0, false, false, "foo", 0, []byte("msg"))
	testMQTTPublish(t, mcp, mpr, 1, false, false, "foo", 1, []byte("msg"))
	testMQTTPublish(t, mcp, mpr, 2, false, false, "foo", 2, []byte("msg"))
}

func TestMQTTSub(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	nc := natsConnect(t, s.ClientURL())
	defer nc.Close()

	mcp, mpr := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mcp.Close()
	testMQTTCheckConnAck(t, mpr, mqttConnAckRCConnectionAccepted, false)

	for _, test := range []struct {
		name           string
		mqttSubTopic   string
		natsPubSubject string
		mqttPubTopic   string
		ok             bool
	}{
		{"1 level match", "foo", "foo", "foo", true},
		{"1 level no match", "foo", "bar", "bar", false},
		{"2 levels match", "foo/bar", "foo.bar", "foo/bar", true},
		{"2 levels no match", "foo/bar", "foo.baz", "foo/baz", false},
		{"3 levels match", "/foo/bar", "/.foo.bar", "/foo/bar", true},
		{"3 levels no match", "/foo/bar", "/.foo.baz", "/foo/baz", false},

		{"single level wc", "foo/+", "foo.bar.baz", "foo/bar/baz", false},
		{"single level wc", "foo/+", "foo.bar./", "foo/bar/", false},
		{"single level wc", "foo/+", "foo.bar", "foo/bar", true},
		{"single level wc", "foo/+", "foo./", "foo/", true},
		{"single level wc", "foo/+", "foo", "foo", false},
		{"single level wc", "foo/+", "/.foo", "/foo", false},

		{"multiple level wc", "foo/#", "foo.bar.baz./", "foo/bar/baz/", true},
		{"multiple level wc", "foo/#", "foo.bar.baz", "foo/bar/baz", true},
		{"multiple level wc", "foo/#", "foo.bar./", "foo/bar/", true},
		{"multiple level wc", "foo/#", "foo.bar", "foo/bar", true},
		{"multiple level wc", "foo/#", "foo./", "foo/", true},
		{"multiple level wc", "foo/#", "foo", "foo", true},
		{"multiple level wc", "foo/#", "/.foo", "/foo", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			mc, r := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
			defer mc.Close()
			testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

			testMQTTSub(t, 1, mc, r, []*mqttFilter{{filter: test.mqttSubTopic, qos: 0}}, []byte{0})
			testMQTTFlush(t, mc, nil, r)

			natsPub(t, nc, test.natsPubSubject, []byte("msg"))
			if test.ok {
				testMQTTCheckPubMsg(t, mc, r, test.mqttPubTopic, 0, []byte("msg"))
			} else {
				testMQTTExpectNothing(t, r)
			}

			testMQTTPublish(t, mcp, mpr, 0, false, false, test.mqttPubTopic, 0, []byte("msg"))
			if test.ok {
				testMQTTCheckPubMsg(t, mc, r, test.mqttPubTopic, 0, []byte("msg"))
			} else {
				testMQTTExpectNothing(t, r)
			}
		})
	}
}

func TestMQTTSubQoS(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	nc := natsConnect(t, s.ClientURL())
	defer nc.Close()

	mcp, mpr := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mcp.Close()
	testMQTTCheckConnAck(t, mpr, mqttConnAckRCConnectionAccepted, false)

	mc, r := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mc.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

	mqttTopic := "foo/bar"

	// Subscribe with QoS 1
	testMQTTSub(t, 1, mc, r, []*mqttFilter{{filter: "foo/#", qos: 1}}, []byte{1})
	testMQTTSub(t, 1, mc, r, []*mqttFilter{{filter: mqttTopic, qos: 1}}, []byte{1})
	testMQTTFlush(t, mc, nil, r)

	// Publish from NATS, which means QoS 0
	natsPub(t, nc, "foo.bar", []byte("NATS"))
	// Will receive as QoS 0
	testMQTTCheckPubMsg(t, mc, r, mqttTopic, 0, []byte("NATS"))
	testMQTTCheckPubMsg(t, mc, r, mqttTopic, 0, []byte("NATS"))

	// Publish from MQTT with QoS 0
	testMQTTPublish(t, mcp, mpr, 0, false, false, mqttTopic, 0, []byte("msg"))
	// Will receive as QoS 0
	testMQTTCheckPubMsg(t, mc, r, mqttTopic, 0, []byte("msg"))
	testMQTTCheckPubMsg(t, mc, r, mqttTopic, 0, []byte("msg"))

	// Publish from MQTT with QoS 1
	testMQTTPublish(t, mcp, mpr, 1, false, false, mqttTopic, 1, []byte("msg"))
	pflags1, pi1 := testMQTTGetPubMsg(t, mc, r, mqttTopic, []byte("msg"))
	if pflags1 != 0x2 {
		t.Fatalf("Expected flags to be 0x2, got %v", pflags1)
	}
	pflags2, pi2 := testMQTTGetPubMsg(t, mc, r, mqttTopic, []byte("msg"))
	if pflags2 != 0x2 {
		t.Fatalf("Expected flags to be 0x2, got %v", pflags2)
	}
	if pi1 == pi2 {
		t.Fatalf("packet identifier for message 1: %v should be different from message 2", pi1)
	}
	testMQTTSendPubAck(t, mc, pi1)
	testMQTTSendPubAck(t, mc, pi2)
}

func getSubQoS(sub *subscription) int {
	if sub.mqtt != nil {
		return int(sub.mqtt.qos)
	}
	return -1
}

func TestMQTTSubDups(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	mcp, mpr := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mcp.Close()
	testMQTTCheckConnAck(t, mpr, mqttConnAckRCConnectionAccepted, false)

	mc, r := testMQTTConnect(t, &mqttConnInfo{clientID: "sub", user: "sub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mc.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

	// Test with single SUBSCRIBE protocol but multiple filters
	filters := []*mqttFilter{
		{filter: "foo", qos: 1},
		{filter: "foo", qos: 0},
	}
	testMQTTSub(t, 1, mc, r, filters, []byte{1, 0})
	testMQTTFlush(t, mc, nil, r)

	// And also with separate SUBSCRIBE protocols
	testMQTTSub(t, 1, mc, r, []*mqttFilter{{filter: "bar", qos: 0}}, []byte{0})
	// Ask for QoS 2 but server will downgrade to 1
	testMQTTSub(t, 1, mc, r, []*mqttFilter{{filter: "bar", qos: 2}}, []byte{1})
	testMQTTFlush(t, mc, nil, r)

	// Publish and test msg received only once
	testMQTTPublish(t, mcp, r, 0, false, false, "foo", 0, []byte("msg"))
	testMQTTCheckPubMsg(t, mc, r, "foo", 0, []byte("msg"))
	testMQTTExpectNothing(t, r)

	testMQTTPublish(t, mcp, r, 0, false, false, "bar", 0, []byte("msg"))
	testMQTTCheckPubMsg(t, mc, r, "bar", 0, []byte("msg"))
	testMQTTExpectNothing(t, r)

	// Check that the QoS for subscriptions have been updated to the latest received filter
	var err error
	subc := testMQTTGetClient(t, s, "sub")
	subc.mu.Lock()
	if subc.opts.Username != "sub" {
		err = fmt.Errorf("wrong user name")
	}
	if err == nil {
		if sub := subc.subs["foo"]; sub == nil || getSubQoS(sub) != 0 {
			err = fmt.Errorf("subscription foo QoS should be 0, got %v", getSubQoS(sub))
		}
	}
	if err == nil {
		if sub := subc.subs["bar"]; sub == nil || getSubQoS(sub) != 1 {
			err = fmt.Errorf("subscription bar QoS should be 1, got %v", getSubQoS(sub))
		}
	}
	subc.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}

	// Now subscribe on "foo/#" which means that a PUBLISH on "foo" will be received
	// by this subscription and also the one on "foo".
	testMQTTSub(t, 1, mc, r, []*mqttFilter{{filter: "foo/#", qos: 1}}, []byte{1})
	testMQTTFlush(t, mc, nil, r)

	// Publish and test msg received twice
	testMQTTPublish(t, mcp, r, 0, false, false, "foo", 0, []byte("msg"))
	testMQTTCheckPubMsg(t, mc, r, "foo", 0, []byte("msg"))
	testMQTTCheckPubMsg(t, mc, r, "foo", 0, []byte("msg"))

	checkWCSub := func(expectedQoS int) {
		t.Helper()

		subc.mu.Lock()
		defer subc.mu.Unlock()

		// When invoked with expectedQoS==1, we have the following subs:
		// foo (QoS-0), bar (QoS-1), foo.> (QoS-1)
		// which means (since QoS-1 have a JS consumer + sub for delivery
		// and foo.> causes a "foo fwc") that we should have the following
		// number of NATS subs: foo (1), bar (2), foo.> (2) and "foo fwc" (2),
		// so total=7.
		// When invoked with expectedQoS==0, it means that we have replaced
		// foo/# QoS-1 to QoS-0, so we should have 2 less NATS subs,
		// so total=5
		expected := 7
		if expectedQoS == 0 {
			expected = 5
		}
		if lenmap := len(subc.subs); lenmap != expected {
			t.Fatalf("Subs map should have %v entries, got %v", expected, lenmap)
		}
		if sub, ok := subc.subs["foo.>"]; !ok {
			t.Fatal("Expected sub foo.> to be present but was not")
		} else if getSubQoS(sub) != expectedQoS {
			t.Fatalf("Expected sub foo.> QoS to be %v, got %v", expectedQoS, getSubQoS(sub))
		}
		if sub, ok := subc.subs["foo fwc"]; !ok {
			t.Fatal("Expected sub foo fwc to be present but was not")
		} else if getSubQoS(sub) != expectedQoS {
			t.Fatalf("Expected sub foo fwc QoS to be %v, got %v", expectedQoS, getSubQoS(sub))
		}
		// Make sure existing sub on "foo" qos was not changed.
		if sub, ok := subc.subs["foo"]; !ok {
			t.Fatal("Expected sub foo to be present but was not")
		} else if getSubQoS(sub) != 0 {
			t.Fatalf("Expected sub foo QoS to be 0, got %v", getSubQoS(sub))
		}
	}
	checkWCSub(1)

	// Sub again on same subject with lower QoS
	testMQTTSub(t, 1, mc, r, []*mqttFilter{{filter: "foo/#", qos: 0}}, []byte{0})
	testMQTTFlush(t, mc, nil, r)

	// Publish and test msg received twice
	testMQTTPublish(t, mcp, r, 0, false, false, "foo", 0, []byte("msg"))
	testMQTTCheckPubMsg(t, mc, r, "foo", 0, []byte("msg"))
	testMQTTCheckPubMsg(t, mc, r, "foo", 0, []byte("msg"))
	checkWCSub(0)
}

func TestMQTTSubWithSpaces(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	mcp, mpr := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mcp.Close()
	testMQTTCheckConnAck(t, mpr, mqttConnAckRCConnectionAccepted, false)

	mc, r := testMQTTConnect(t, &mqttConnInfo{user: "sub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mc.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

	testMQTTSub(t, 1, mc, r, []*mqttFilter{{filter: "foo bar", qos: 0}}, []byte{mqttSubAckFailure})
}

func TestMQTTSubCaseSensitive(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	mcp, mpr := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mcp.Close()
	testMQTTCheckConnAck(t, mpr, mqttConnAckRCConnectionAccepted, false)

	mc, r := testMQTTConnect(t, &mqttConnInfo{user: "sub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mc.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

	testMQTTSub(t, 1, mc, r, []*mqttFilter{{filter: "Foo/Bar", qos: 0}}, []byte{0})
	testMQTTFlush(t, mc, nil, r)

	testMQTTPublish(t, mcp, r, 0, false, false, "Foo/Bar", 0, []byte("msg"))
	testMQTTCheckPubMsg(t, mc, r, "Foo/Bar", 0, []byte("msg"))

	testMQTTPublish(t, mcp, r, 0, false, false, "foo/bar", 0, []byte("msg"))
	testMQTTExpectNothing(t, r)

	nc := natsConnect(t, s.ClientURL())
	defer nc.Close()

	natsPub(t, nc, "Foo.Bar", []byte("nats"))
	testMQTTCheckPubMsg(t, mc, r, "Foo/Bar", 0, []byte("nats"))

	natsPub(t, nc, "foo.bar", []byte("nats"))
	testMQTTExpectNothing(t, r)
}

func TestMQTTPubSubMatrix(t *testing.T) {
	for _, test := range []struct {
		name        string
		natsPub     bool
		mqttPub     bool
		mqttPubQoS  byte
		natsSub     bool
		mqttSubQoS0 bool
		mqttSubQoS1 bool
	}{
		{"NATS to MQTT sub QoS-0", true, false, 0, false, true, false},
		{"NATS to MQTT sub QoS-1", true, false, 0, false, false, true},
		{"NATS to MQTT sub QoS-0 and QoS-1", true, false, 0, false, true, true},

		{"MQTT QoS-0 to NATS sub", false, true, 0, true, false, false},
		{"MQTT QoS-0 to MQTT sub QoS-0", false, true, 0, false, true, false},
		{"MQTT QoS-0 to MQTT sub QoS-1", false, true, 0, false, false, true},
		{"MQTT QoS-0 to NATS sub and MQTT sub QoS-0", false, true, 0, true, true, false},
		{"MQTT QoS-0 to NATS sub and MQTT sub QoS-1", false, true, 0, true, false, true},
		{"MQTT QoS-0 to all subs", false, true, 0, true, true, true},

		{"MQTT QoS-1 to NATS sub", false, true, 1, true, false, false},
		{"MQTT QoS-1 to MQTT sub QoS-0", false, true, 1, false, true, false},
		{"MQTT QoS-1 to MQTT sub QoS-1", false, true, 1, false, false, true},
		{"MQTT QoS-1 to NATS sub and MQTT sub QoS-0", false, true, 1, true, true, false},
		{"MQTT QoS-1 to NATS sub and MQTT sub QoS-1", false, true, 1, true, false, true},
		{"MQTT QoS-1 to all subs", false, true, 1, true, true, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			o := testMQTTDefaultOptions()
			s := testMQTTRunServer(t, o)
			defer testMQTTShutdownServer(s)

			nc := natsConnect(t, s.ClientURL())
			defer nc.Close()

			mc, r := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
			defer mc.Close()
			testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

			mc1, r1 := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
			defer mc1.Close()
			testMQTTCheckConnAck(t, r1, mqttConnAckRCConnectionAccepted, false)

			mc2, r2 := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
			defer mc2.Close()
			testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, false)

			// First setup subscriptions based on test options.
			var ns *nats.Subscription
			if test.natsSub {
				ns = natsSubSync(t, nc, "foo")
			}
			if test.mqttSubQoS0 {
				testMQTTSub(t, 1, mc1, r1, []*mqttFilter{{filter: "foo", qos: 0}}, []byte{0})
				testMQTTFlush(t, mc1, nil, r1)
			}
			if test.mqttSubQoS1 {
				testMQTTSub(t, 1, mc2, r2, []*mqttFilter{{filter: "foo", qos: 1}}, []byte{1})
				testMQTTFlush(t, mc2, nil, r2)
			}

			// Just as a barrier
			natsFlush(t, nc)

			// Now publish
			if test.natsPub {
				natsPubReq(t, nc, "foo", "", []byte("msg"))
			} else {
				testMQTTPublish(t, mc, r, test.mqttPubQoS, false, false, "foo", 1, []byte("msg"))
			}

			// Check message received
			if test.natsSub {
				natsNexMsg(t, ns, time.Second)
				// Make sure no other is received
				if msg, err := ns.NextMsg(50 * time.Millisecond); err == nil {
					t.Fatalf("Should not have gotten a second message, got %v", msg)
				}
			}
			if test.mqttSubQoS0 {
				testMQTTCheckPubMsg(t, mc1, r1, "foo", 0, []byte("msg"))
				testMQTTExpectNothing(t, r1)
			}
			if test.mqttSubQoS1 {
				var expectedFlag byte
				if test.mqttPubQoS > 0 {
					expectedFlag = test.mqttPubQoS << 1
				}
				testMQTTCheckPubMsg(t, mc2, r2, "foo", expectedFlag, []byte("msg"))
				testMQTTExpectNothing(t, r2)
			}
		})
	}
}

func TestMQTTPreventSubWithMQTTSubPrefix(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	mc, r := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mc.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, mc, r,
		[]*mqttFilter{{filter: strings.ReplaceAll(mqttSubPrefix, ".", "/") + "foo/bar", qos: 1}},
		[]byte{mqttSubAckFailure})
}

func TestMQTTSubWithNATSStream(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	mc, r := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mc.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, mc, r, []*mqttFilter{{filter: "foo/bar", qos: 1}}, []byte{1})
	testMQTTFlush(t, mc, nil, r)

	mcp, rp := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mcp.Close()
	testMQTTCheckConnAck(t, rp, mqttConnAckRCConnectionAccepted, false)
	testMQTTFlush(t, mcp, nil, rp)

	nc := natsConnect(t, s.ClientURL())
	defer nc.Close()

	sc := &StreamConfig{
		Name:      "test",
		Storage:   FileStorage,
		Retention: InterestPolicy,
		Subjects:  []string{"foo.>"},
	}
	mset, err := s.GlobalAccount().addStream(sc)
	if err != nil {
		t.Fatalf("Unable to create stream: %v", err)
	}

	sub := natsSubSync(t, nc, "bar")
	cc := &ConsumerConfig{
		Durable:        "dur",
		AckPolicy:      AckExplicit,
		DeliverSubject: "bar",
	}
	if _, err := mset.addConsumer(cc); err != nil {
		t.Fatalf("Unable to add consumer: %v", err)
	}

	// Now send message from NATS
	resp, err := nc.Request("foo.bar", []byte("nats"), time.Second)
	if err != nil {
		t.Fatalf("Error publishing: %v", err)
	}
	ar := &ApiResponse{}
	if err := json.Unmarshal(resp.Data, ar); err != nil || ar.Error != nil {
		t.Fatalf("Unexpected response: err=%v resp=%+v", err, ar.Error)
	}

	// Check that message is received by both
	checkRecv := func(content string, flags byte) {
		t.Helper()
		if msg := natsNexMsg(t, sub, time.Second); string(msg.Data) != content {
			t.Fatalf("Expected %q, got %q", content, msg.Data)
		}
		testMQTTCheckPubMsg(t, mc, r, "foo/bar", flags, []byte(content))
	}
	checkRecv("nats", 0)

	// Send from MQTT as a QoS0
	testMQTTPublish(t, mcp, rp, 0, false, false, "foo/bar", 0, []byte("qos0"))
	checkRecv("qos0", 0)

	// Send from MQTT as a QoS1
	testMQTTPublish(t, mcp, rp, 1, false, false, "foo/bar", 1, []byte("qos1"))
	checkRecv("qos1", mqttPubQos1)
}

func TestMQTTTrackPendingOverrun(t *testing.T) {
	sess := &mqttSession{pending: make(map[uint16]*mqttPending)}
	sub := &subscription{mqtt: &mqttSub{qos: 1}}

	sess.ppi = 0xFFFF
	pi, _ := sess.trackPending(1, _EMPTY_, sub)
	if pi != 1 {
		t.Fatalf("Expected 1, got %v", pi)
	}

	p := &mqttPending{}
	for i := 1; i <= 0xFFFF; i++ {
		sess.pending[uint16(i)] = p
	}
	pi, _ = sess.trackPending(1, _EMPTY_, sub)
	if pi != 0 {
		t.Fatalf("Expected 0, got %v", pi)
	}

	delete(sess.pending, 1234)
	pi, _ = sess.trackPending(1, _EMPTY_, sub)
	if pi != 1234 {
		t.Fatalf("Expected 1234, got %v", pi)
	}
}

func TestMQTTSubRestart(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	nc := natsConnect(t, s.ClientURL())
	defer nc.Close()

	mc, r := testMQTTConnect(t, &mqttConnInfo{clientID: "sub", cleanSess: false}, o.MQTT.Host, o.MQTT.Port)
	defer mc.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

	// Start an MQTT subscription QoS=1 on "foo"
	testMQTTSub(t, 1, mc, r, []*mqttFilter{{filter: "foo", qos: 1}}, []byte{1})
	testMQTTFlush(t, mc, nil, r)

	// Now start a NATS subscription on ">" (anything that would match the JS consumer delivery subject)
	natsSubSync(t, nc, ">")
	natsFlush(t, nc)

	// Restart the MQTT client
	testMQTTDisconnect(t, mc, nil)

	mc, r = testMQTTConnect(t, &mqttConnInfo{clientID: "sub", cleanSess: false}, o.MQTT.Host, o.MQTT.Port)
	defer mc.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, true)

	// Restart an MQTT subscription QoS=1 on "foo"
	testMQTTSub(t, 1, mc, r, []*mqttFilter{{filter: "foo", qos: 1}}, []byte{1})
	testMQTTFlush(t, mc, nil, r)

	pc, pr := testMQTTConnect(t, &mqttConnInfo{clientID: "pub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer pc.Close()
	testMQTTCheckConnAck(t, pr, mqttConnAckRCConnectionAccepted, false)

	// Publish a message QoS1
	testMQTTPublish(t, pc, pr, 1, false, false, "foo", 1, []byte("msg1"))
	// Make sure we receive it
	testMQTTCheckPubMsg(t, mc, r, "foo", mqttPubQos1, []byte("msg1"))

	// Now "restart" the subscription but as a Qos0
	testMQTTSub(t, 1, mc, r, []*mqttFilter{{filter: "foo", qos: 0}}, []byte{0})
	testMQTTFlush(t, mc, nil, r)

	// Publish a message QoS
	testMQTTPublish(t, pc, pr, 1, false, false, "foo", 1, []byte("msg2"))
	// Make sure we receive but as a QoS0
	testMQTTCheckPubMsg(t, mc, r, "foo", 0, []byte("msg2"))
}

func testMQTTGetClusterTemplaceNoLeaf() string {
	return strings.Replace(jsClusterTemplWithLeafAndMQTT, "{{leaf}}", "", 1)
}

func TestMQTTSubPropagation(t *testing.T) {
	cl := createJetStreamClusterWithTemplate(t, testMQTTGetClusterTemplaceNoLeaf(), "MQTT", 2)
	defer cl.shutdown()

	o := cl.opts[0]
	s2 := cl.servers[1]
	nc := natsConnect(t, s2.ClientURL())
	defer nc.Close()

	mc, r := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mc.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

	testMQTTSub(t, 1, mc, r, []*mqttFilter{{filter: "foo/#", qos: 0}}, []byte{0})
	testMQTTFlush(t, mc, nil, r)

	// Because in MQTT foo/# means foo.> but also foo, check that this is propagated
	checkSubInterest(t, s2, globalAccountName, "foo", time.Second)

	// Publish on foo.bar, foo./ and foo and we should receive them
	natsPub(t, nc, "foo.bar", []byte("hello"))
	testMQTTCheckPubMsg(t, mc, r, "foo/bar", 0, []byte("hello"))

	natsPub(t, nc, "foo./", []byte("from"))
	testMQTTCheckPubMsg(t, mc, r, "foo/", 0, []byte("from"))

	natsPub(t, nc, "foo", []byte("NATS"))
	testMQTTCheckPubMsg(t, mc, r, "foo", 0, []byte("NATS"))
}

func TestMQTTCluster(t *testing.T) {
	cl := createJetStreamClusterWithTemplate(t, testMQTTGetClusterTemplaceNoLeaf(), "MQTT", 2)
	defer cl.shutdown()

	for _, topTest := range []struct {
		name    string
		restart bool
	}{
		{"first_start", true},
		{"restart", false},
	} {
		t.Run(topTest.name, func(t *testing.T) {
			for _, test := range []struct {
				name   string
				subQos byte
			}{
				{"qos_0", 0},
				{"qos_1", 1},
			} {
				t.Run(test.name, func(t *testing.T) {
					clientID := nuid.Next()

					o := cl.opts[0]
					mc, r := testMQTTConnectRetry(t, &mqttConnInfo{clientID: clientID, cleanSess: false}, o.MQTT.Host, o.MQTT.Port, 5)
					defer mc.Close()
					testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

					testMQTTSub(t, 1, mc, r, []*mqttFilter{{filter: "foo/#", qos: test.subQos}}, []byte{test.subQos})
					testMQTTFlush(t, mc, nil, r)

					check := func(mc net.Conn, r *mqttReader, o *Options, s *Server) {
						t.Helper()

						nc := natsConnect(t, s.ClientURL())
						defer nc.Close()

						natsPub(t, nc, "foo.bar", []byte("fromNats"))
						testMQTTCheckPubMsg(t, mc, r, "foo/bar", 0, []byte("fromNats"))

						mpc, pr := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
						defer mpc.Close()
						testMQTTCheckConnAck(t, pr, mqttConnAckRCConnectionAccepted, false)

						testMQTTPublish(t, mpc, pr, 0, false, false, "foo/baz", 0, []byte("mqtt_qos0"))
						testMQTTCheckPubMsg(t, mc, r, "foo/baz", 0, []byte("mqtt_qos0"))

						testMQTTPublish(t, mpc, pr, 1, false, false, "foo/bat", 1, []byte("mqtt_qos1"))
						expectedQoS := byte(0)
						if test.subQos == 1 {
							expectedQoS = mqttPubQos1
						}
						testMQTTCheckPubMsg(t, mc, r, "foo/bat", expectedQoS, []byte("mqtt_qos1"))
						testMQTTDisconnect(t, mpc, nil)
					}
					check(mc, r, cl.opts[0], cl.servers[0])
					check(mc, r, cl.opts[1], cl.servers[1])

					// Start the same subscription from the other server. It should disconnect
					// the one connected in the first server.
					o = cl.opts[1]

					mc2, r2 := testMQTTConnect(t, &mqttConnInfo{clientID: clientID, cleanSess: false}, o.MQTT.Host, o.MQTT.Port)
					defer mc2.Close()
					testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, true)

					// Expect first connection to be closed.
					testMQTTExpectDisconnect(t, mc)

					// Now re-run the checks
					check(mc2, r2, cl.opts[0], cl.servers[0])
					check(mc2, r2, cl.opts[1], cl.servers[1])

					// Disconnect our sub and restart with clean session then disconnect again to clear the state.
					testMQTTDisconnect(t, mc2, nil)
					mc2, r2 = testMQTTConnect(t, &mqttConnInfo{clientID: clientID, cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
					defer mc2.Close()
					testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, false)
					testMQTTFlush(t, mc2, nil, r2)
					testMQTTDisconnect(t, mc2, nil)

					// Remove the session from the flappers so we can restart the test
					// without failure and have to wait for 1sec before being able to reconnect.
					s := cl.servers[0]
					sm := &s.mqtt.sessmgr
					sm.mu.Lock()
					asm := sm.sessions[globalAccountName]
					sm.mu.Unlock()
					if asm != nil {
						asm.mu.Lock()
						delete(asm.flappers, clientID)
						asm.mu.Unlock()
					}
				})
			}
			if !t.Failed() && topTest.restart {
				cl.stopAll()
				cl.restartAll()

				streams := []string{mqttStreamName, mqttRetainedMsgsStreamName, mqttSessStreamName}
				for _, sn := range streams {
					cl.waitOnStreamLeader(globalAccountName, sn)
				}
				cl.waitOnConsumerLeader(globalAccountName, mqttRetainedMsgsStreamName, "$MQTT_rmsgs_esFhDys3")
				cl.waitOnConsumerLeader(globalAccountName, mqttRetainedMsgsStreamName, "$MQTT_rmsgs_z3WIzPtj")
			}
		})
	}
}

func TestMQTTClusterRetainedMsg(t *testing.T) {
	cl := createJetStreamClusterWithTemplate(t, testMQTTGetClusterTemplaceNoLeaf(), "MQTT", 2)
	defer cl.shutdown()

	srv1Opts := cl.opts[0]
	srv2Opts := cl.opts[1]

	// Connect subscription on server 1.
	mc, rc := testMQTTConnectRetry(t, &mqttConnInfo{clientID: "sub", cleanSess: false}, srv1Opts.MQTT.Host, srv1Opts.MQTT.Port, 5)
	defer mc.Close()
	testMQTTCheckConnAck(t, rc, mqttConnAckRCConnectionAccepted, false)

	testMQTTSub(t, 1, mc, rc, []*mqttFilter{{filter: "foo/#", qos: 1}}, []byte{1})
	testMQTTFlush(t, mc, nil, rc)

	// Create a publisher from server 2.
	mp, rp := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, srv2Opts.MQTT.Host, srv2Opts.MQTT.Port)
	defer mp.Close()
	testMQTTCheckConnAck(t, rp, mqttConnAckRCConnectionAccepted, false)

	// Send retained message.
	testMQTTPublish(t, mp, rp, 1, false, true, "foo/bar", 1, []byte("retained"))
	// Check it is received.
	testMQTTCheckPubMsg(t, mc, rc, "foo/bar", mqttPubQos1, []byte("retained"))

	// Start a new subscription on server 1 and make sure we receive the retained message
	mc2, rc2 := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, srv1Opts.MQTT.Host, srv1Opts.MQTT.Port)
	defer mc2.Close()
	testMQTTCheckConnAck(t, rc2, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, mc2, rc2, []*mqttFilter{{filter: "foo/#", qos: 1}}, []byte{1})
	testMQTTCheckPubMsg(t, mc2, rc2, "foo/bar", mqttPubQos1|mqttPubFlagRetain, []byte("retained"))
	testMQTTDisconnect(t, mc2, nil)

	// Send an empty retained message which should remove it from storage, but still be delivered.
	testMQTTPublish(t, mp, rp, 1, false, true, "foo/bar", 1, []byte(""))
	testMQTTCheckPubMsg(t, mc, rc, "foo/bar", mqttPubQos1, []byte(""))

	// Now shutdown the consumer connection
	testMQTTDisconnect(t, mc, nil)
	mc.Close()

	// Reconnect to server where the retained message was published (server 2)
	mc, rc = testMQTTConnect(t, &mqttConnInfo{clientID: "sub", cleanSess: false}, srv2Opts.MQTT.Host, srv2Opts.MQTT.Port)
	defer mc.Close()
	testMQTTCheckConnAck(t, rc, mqttConnAckRCConnectionAccepted, true)
	testMQTTSub(t, 1, mc, rc, []*mqttFilter{{filter: "foo/#", qos: 1}}, []byte{1})
	// The retained message should not be delivered.
	testMQTTExpectNothing(t, rc)
	// Now disconnect and reconnect back to first server
	testMQTTDisconnect(t, mc, nil)
	mc.Close()

	// Now reconnect to the server 1, which is not where the messages were published, and check
	// that we don't receive the message.
	mc, rc = testMQTTConnect(t, &mqttConnInfo{clientID: "sub", cleanSess: false}, srv1Opts.MQTT.Host, srv1Opts.MQTT.Port)
	defer mc.Close()
	testMQTTCheckConnAck(t, rc, mqttConnAckRCConnectionAccepted, true)
	testMQTTSub(t, 1, mc, rc, []*mqttFilter{{filter: "foo/#", qos: 1}}, []byte{1})
	testMQTTExpectNothing(t, rc)
	testMQTTDisconnect(t, mc, nil)
	mc.Close()

	// Will now test network deletes

	// Create a subscription on server 1 and server 2
	mc, rc = testMQTTConnect(t, &mqttConnInfo{clientID: "sub_one", cleanSess: false}, srv1Opts.MQTT.Host, srv1Opts.MQTT.Port)
	defer mc.Close()
	testMQTTCheckConnAck(t, rc, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, mc, rc, []*mqttFilter{{filter: "bar", qos: 1}}, []byte{1})
	testMQTTFlush(t, mc, nil, rc)

	mc2, rc2 = testMQTTConnect(t, &mqttConnInfo{clientID: "sub_two", cleanSess: false}, srv2Opts.MQTT.Host, srv2Opts.MQTT.Port)
	defer mc2.Close()
	testMQTTCheckConnAck(t, rc2, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, mc2, rc2, []*mqttFilter{{filter: "bar", qos: 1}}, []byte{1})
	testMQTTFlush(t, mc2, nil, rc2)

	// Publish 1 retained message (producer is connected to server 2)
	testMQTTPublish(t, mp, rp, 1, false, true, "bar", 1, []byte("msg1"))

	// Make sure messages are received by both
	testMQTTCheckPubMsg(t, mc, rc, "bar", mqttPubQos1, []byte("msg1"))
	testMQTTCheckPubMsg(t, mc2, rc2, "bar", mqttPubQos1, []byte("msg1"))

	// Now send an empty retained message that should delete it. For the one on server 1,
	// this will be a network delete.
	testMQTTPublish(t, mp, rp, 1, false, true, "bar", 1, []byte(""))
	testMQTTCheckPubMsg(t, mc, rc, "bar", mqttPubQos1, []byte(""))
	testMQTTCheckPubMsg(t, mc2, rc2, "bar", mqttPubQos1, []byte(""))

	// Now send a new retained message
	testMQTTPublish(t, mp, rp, 1, false, true, "bar", 1, []byte("msg2"))

	// Again, verify that they all receive it.
	testMQTTCheckPubMsg(t, mc, rc, "bar", mqttPubQos1, []byte("msg2"))
	testMQTTCheckPubMsg(t, mc2, rc2, "bar", mqttPubQos1, []byte("msg2"))

	// But now, restart the consumer that was in the server that processed the
	// original network delete.
	testMQTTDisconnect(t, mc, nil)
	mc.Close()

	mc, rc = testMQTTConnect(t, &mqttConnInfo{clientID: "sub_one", cleanSess: false}, srv1Opts.MQTT.Host, srv1Opts.MQTT.Port)
	defer mc.Close()
	testMQTTCheckConnAck(t, rc, mqttConnAckRCConnectionAccepted, true)
	testMQTTSub(t, 1, mc, rc, []*mqttFilter{{filter: "bar", qos: 1}}, []byte{1})
	// Expect the message to be delivered as retained
	testMQTTCheckPubMsg(t, mc, rc, "bar", mqttPubQos1|mqttPubFlagRetain, []byte("msg2"))
}

func TestMQTTRetainedMsgNetworkUpdates(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	mc, rc := testMQTTConnect(t, &mqttConnInfo{clientID: "sub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mc.Close()
	testMQTTCheckConnAck(t, rc, mqttConnAckRCConnectionAccepted, false)

	c := testMQTTGetClient(t, s, "sub")
	asm := c.mqtt.asm

	// For this test, we are going to simulate updates arriving in a
	// mixed order and verify that we have the expected outcome.
	check := func(t *testing.T, subject string, present bool, current, floor uint64) {
		t.Helper()
		asm.mu.RLock()
		defer asm.mu.RUnlock()
		erm, ok := asm.retmsgs[subject]
		if present && !ok {
			t.Fatalf("Subject %q not present", subject)
		} else if !present && ok {
			t.Fatalf("Subject %q should not be present", subject)
		} else if !present {
			return
		}
		if floor != erm.floor {
			t.Fatalf("Expected floor to be %v, got %v", floor, erm.floor)
		}
		if erm.sseq != current {
			t.Fatalf("Expected current sequence to be %v, got %v", current, erm.sseq)
		}
	}

	type action struct {
		add bool
		seq uint64
	}
	for _, test := range []struct {
		subject string
		order   []action
		seq     uint64
		floor   uint64
	}{
		{"foo.1", []action{{true, 1}, {true, 2}, {true, 3}}, 3, 0},
		{"foo.2", []action{{true, 3}, {true, 1}, {true, 2}}, 3, 0},
		{"foo.3", []action{{true, 1}, {false, 1}, {true, 2}}, 2, 0},
		{"foo.4", []action{{false, 2}, {true, 1}, {true, 3}, {true, 2}}, 3, 0},
		{"foo.5", []action{{false, 2}, {true, 1}, {true, 2}}, 0, 2},
		{"foo.6", []action{{true, 1}, {true, 2}, {false, 2}}, 0, 2},
	} {
		t.Run(test.subject, func(t *testing.T) {
			for _, a := range test.order {
				if a.add {
					rf := &mqttRetainedMsgRef{sseq: a.seq}
					asm.handleRetainedMsg(test.subject, rf)
				} else {
					asm.handleRetainedMsgDel(test.subject, a.seq)
				}
			}
			check(t, test.subject, true, test.seq, test.floor)
		})
	}

	for _, subject := range []string{"foo.5", "foo.6"} {
		t.Run("clear_"+subject, func(t *testing.T) {
			// Now add a new message, which should clear the floor.
			rf := &mqttRetainedMsgRef{sseq: 3}
			asm.handleRetainedMsg(subject, rf)
			check(t, subject, true, 3, 0)
			// Now do a non network delete and make sure it is gone.
			asm.handleRetainedMsgDel(subject, 0)
			check(t, subject, false, 0, 0)
		})
	}
}

func TestMQTTRetainedMsgMigration(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	// Create the retained messages stream to listen on the old subject first.
	// The server will correct this when the migration takes place.
	_, err := js.AddStream(&nats.StreamConfig{
		Name:      mqttRetainedMsgsStreamName,
		Subjects:  []string{`$MQTT.rmsgs`},
		Storage:   nats.FileStorage,
		Retention: nats.LimitsPolicy,
		Replicas:  1,
	})
	require_NoError(t, err)

	// Publish some retained messages on the old "$MQTT.rmsgs" subject.
	for i := 0; i < 100; i++ {
		msg := fmt.Sprintf(
			`{"origin":"b5IQZNtG","subject":"test%d","topic":"test%d","msg":"YmFy","flags":1}`, i, i,
		)
		_, err := js.Publish(`$MQTT.rmsgs`, []byte(msg))
		require_NoError(t, err)
	}

	// Check that the old subject looks right.
	si, err := js.StreamInfo(mqttRetainedMsgsStreamName, &nats.StreamInfoRequest{
		SubjectsFilter: `$MQTT.>`,
	})
	require_NoError(t, err)
	if si.State.NumSubjects != 1 {
		t.Fatalf("expected 1 subject, got %d", si.State.NumSubjects)
	}
	if n := si.State.Subjects[`$MQTT.rmsgs`]; n != 100 {
		t.Fatalf("expected to find 100 messages on the original subject but found %d", n)
	}

	// Create an MQTT client, this will cause a migration to take place.
	mc, rc := testMQTTConnect(t, &mqttConnInfo{clientID: "sub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mc.Close()
	testMQTTCheckConnAck(t, rc, mqttConnAckRCConnectionAccepted, false)

	testMQTTSub(t, 1, mc, rc, []*mqttFilter{{filter: "+", qos: 0}}, []byte{0})
	topics := map[string]struct{}{}
	for i := 0; i < 100; i++ {
		_, _, topic := testMQTTGetPubMsgEx(t, mc, rc, _EMPTY_, []byte("bar"))
		topics[topic] = struct{}{}
	}
	if len(topics) != 100 {
		t.Fatalf("Unexpected topics: %v", topics)
	}

	// Now look at the stream, there should be 100 messages on the new
	// divided subjects and none on the old undivided subject.
	si, err = js.StreamInfo(mqttRetainedMsgsStreamName, &nats.StreamInfoRequest{
		SubjectsFilter: `$MQTT.>`,
	})
	require_NoError(t, err)
	if si.State.NumSubjects != 100 {
		t.Fatalf("expected 100 subjects, got %d", si.State.NumSubjects)
	}
	if n := si.State.Subjects[`$MQTT.rmsgs`]; n > 0 {
		t.Fatalf("expected to find no messages on the original subject but found %d", n)
	}

	// Check that the message counts look right. There should be one
	// retained message per key.
	for i := 0; i < 100; i++ {
		expected := fmt.Sprintf(`$MQTT.rmsgs.test%d`, i)
		n, ok := si.State.Subjects[expected]
		if !ok {
			t.Fatalf("expected to find %q but didn't", expected)
		}
		if n != 1 {
			t.Fatalf("expected %q to have 1 message but had %d", expected, n)
		}
	}
}

func TestMQTTClusterReplicasCount(t *testing.T) {
	for _, test := range []struct {
		size     int
		replicas int
	}{
		{1, 1},
		{2, 2},
		{3, 3},
		{5, 3},
	} {
		t.Run(fmt.Sprintf("size %v", test.size), func(t *testing.T) {
			var s *Server
			var o *Options
			if test.size == 1 {
				o = testMQTTDefaultOptions()
				s = testMQTTRunServer(t, o)
				defer testMQTTShutdownServer(s)
			} else {
				cl := createJetStreamClusterWithTemplate(t, testMQTTGetClusterTemplaceNoLeaf(), "MQTT", test.size)
				defer cl.shutdown()
				o = cl.opts[0]
				s = cl.randomServer()
			}

			mc, rc := testMQTTConnectRetry(t, &mqttConnInfo{clientID: "sub", cleanSess: false}, o.MQTT.Host, o.MQTT.Port, 5)
			defer mc.Close()
			testMQTTCheckConnAck(t, rc, mqttConnAckRCConnectionAccepted, false)
			testMQTTSub(t, 1, mc, rc, []*mqttFilter{{filter: "foo/#", qos: 1}}, []byte{1})
			testMQTTFlush(t, mc, nil, rc)

			nc := natsConnect(t, s.ClientURL())
			defer nc.Close()

			// Check the replicas of all MQTT streams
			js, err := nc.JetStream()
			if err != nil {
				t.Fatalf("Error getting js: %v", err)
			}
			for _, sname := range []string{
				mqttStreamName,
				mqttRetainedMsgsStreamName,
				mqttSessStreamName,
			} {
				t.Run(sname, func(t *testing.T) {
					si, err := js.StreamInfo(sname)
					if err != nil {
						t.Fatalf("Error getting stream info: %v", err)
					}
					if si.Config.Replicas != test.replicas {
						t.Fatalf("Expected %v replicas, got %v", test.replicas, si.Config.Replicas)
					}
				})
			}
		})
	}
}

func TestMQTTClusterCanCreateSessionWithOnServerDown(t *testing.T) {
	cl := createJetStreamClusterWithTemplate(t, testMQTTGetClusterTemplaceNoLeaf(), "MQTT", 3)
	defer cl.shutdown()
	o := cl.opts[0]

	mc, rc := testMQTTConnectRetry(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port, 5)
	defer mc.Close()
	testMQTTCheckConnAck(t, rc, mqttConnAckRCConnectionAccepted, false)
	mc.Close()

	// Shutdown one of the server.
	sd := cl.servers[1].StoreDir()
	defer os.RemoveAll(strings.TrimSuffix(sd, JetStreamStoreDir))
	cl.servers[1].Shutdown()

	// Make sure there is a meta leader
	cl.waitOnPeerCount(2)
	cl.waitOnLeader()

	// Now try to create a new session. Since we use a single stream now for all sessions,
	// this should succeed.
	o = cl.opts[2]
	// We may still get failures because of some JS APIs may timeout while things
	// settle, so try again for a certain amount of times.
	mc, rc = testMQTTConnectRetry(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port, 5)
	defer mc.Close()
	testMQTTCheckConnAck(t, rc, mqttConnAckRCConnectionAccepted, false)
}

func TestMQTTClusterPlacement(t *testing.T) {
	sc := createJetStreamSuperCluster(t, 3, 2)
	defer sc.shutdown()

	c := sc.randomCluster()
	lnc := c.createLeafNodesWithTemplateAndStartPort(jsClusterTemplWithLeafAndMQTT, "SPOKE", 3, 22111)
	defer lnc.shutdown()

	sc.waitOnPeerCount(9)
	sc.waitOnLeader()

	for i := 0; i < 10; i++ {
		mc, rc := testMQTTConnectRetry(t, &mqttConnInfo{cleanSess: true}, lnc.opts[i%3].MQTT.Host, lnc.opts[i%3].MQTT.Port, 5)
		defer mc.Close()
		testMQTTCheckConnAck(t, rc, mqttConnAckRCConnectionAccepted, false)
	}

	// Now check that MQTT assets have been created in the LEAF node's side, not the Hub.
	nc := natsConnect(t, lnc.servers[0].ClientURL())
	defer nc.Close()
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("Unable to get JetStream: %v", err)
	}
	count := 0
	for si := range js.StreamsInfo() {
		if si.Cluster == nil || si.Cluster.Name != "SPOKE" {
			t.Fatalf("Expected asset %q to be placed on spoke cluster, was placed on %+v", si.Config.Name, si.Cluster)
		}
		for _, repl := range si.Cluster.Replicas {
			if !strings.HasPrefix(repl.Name, "SPOKE-") {
				t.Fatalf("Replica on the wrong cluster: %+v", repl)
			}
		}
		if si.State.Consumers > 0 {
			for ci := range js.ConsumersInfo(si.Config.Name) {
				if ci.Cluster == nil || ci.Cluster.Name != "SPOKE" {
					t.Fatalf("Expected asset %q to be placed on spoke cluster, was placed on %+v", ci.Name, si.Cluster)
				}
				for _, repl := range ci.Cluster.Replicas {
					if !strings.HasPrefix(repl.Name, "SPOKE-") {
						t.Fatalf("Replica on the wrong cluster: %+v", repl)
					}
				}
			}
		}
		count++
	}
	if count == 0 {
		t.Fatal("No stream found!")
	}
}

func TestMQTTLeafnodeWithoutJSToClusterWithJSNoSharedSysAcc(t *testing.T) {
	test := func(t *testing.T, resolution int) {
		getClusterOpts := func(name string, i int) *Options {
			o := testMQTTDefaultOptions()
			o.ServerName = name
			o.Cluster.Name = "hub"
			// first two test cases rely on domain not being set in hub
			if resolution > 1 {
				o.JetStreamDomain = "DOMAIN"
			}
			o.Cluster.Host = "127.0.0.1"
			o.Cluster.Port = 2790 + i
			o.Routes = RoutesFromStr("nats://127.0.0.1:2791,nats://127.0.0.1:2792,nats://127.0.0.1:2793")
			o.LeafNode.Host = "127.0.0.1"
			o.LeafNode.Port = -1
			return o
		}
		o1 := getClusterOpts("S1", 1)
		s1 := testMQTTRunServer(t, o1)
		defer testMQTTShutdownServer(s1)

		o2 := getClusterOpts("S2", 2)
		s2 := testMQTTRunServer(t, o2)
		defer testMQTTShutdownServer(s2)

		o3 := getClusterOpts("S3", 3)
		s3 := testMQTTRunServer(t, o3)
		defer testMQTTShutdownServer(s3)

		cluster := []*Server{s1, s2, s3}
		checkClusterFormed(t, cluster...)
		checkFor(t, 10*time.Second, 50*time.Millisecond, func() error {
			for _, s := range cluster {
				if s.JetStreamIsLeader() {
					// Need to wait for usage updates now to propagate to meta leader.
					if len(s.JetStreamClusterPeers()) == len(cluster) {
						time.Sleep(100 * time.Millisecond)
						return nil
					}
				}
			}
			return fmt.Errorf("no leader yet")
		})

		// Now define a leafnode that has mqtt enabled, but no JS. This should still work.
		lno := testMQTTDefaultOptions()
		// Make sure jetstream is not explicitly defined here.
		lno.JetStream = false
		switch resolution {
		case 0:
			// turn off jetstream in $G by adding another account and set mqtt domain option and set account default
			lno.Accounts = append(lno.Accounts, NewAccount("unused-account"))
			fallthrough
		case 1:
			lno.JsAccDefaultDomain = map[string]string{"$G": ""}
		case 2:
			lno.JsAccDefaultDomain = map[string]string{"$G": o1.JetStreamDomain}
		case 3:
			// turn off jetstream in $G by adding another account and set mqtt domain option
			lno.Accounts = append(lno.Accounts, NewAccount("unused-account"))
			fallthrough
		case 4:
			// actual solution
			lno.MQTT.JsDomain = o1.JetStreamDomain
		case 5:
			// set per account overwrite and disable js in $G
			lno.Accounts = append(lno.Accounts, NewAccount("unused-account"))
			lno.JsAccDefaultDomain = map[string]string{
				"$G": o1.JetStreamDomain,
			}
		}
		// Whenever an account was added to disable JS in $G, enable it in the server
		if len(lno.Accounts) > 0 {
			lno.JetStream = true
			lno.JetStreamDomain = "OTHER"
			lno.StoreDir = t.TempDir()
		}

		// Use RoutesFromStr() to make an array of urls
		urls := RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d,nats://127.0.0.1:%d,nats://127.0.0.1:%d",
			o1.LeafNode.Port, o2.LeafNode.Port, o3.LeafNode.Port))
		lno.LeafNode.Remotes = []*RemoteLeafOpts{{URLs: urls}}

		ln := RunServer(lno)
		defer testMQTTShutdownServer(ln)

		checkLeafNodeConnected(t, ln)

		// Now connect to leafnode and subscribe
		mc, rc := testMQTTConnectRetry(t, &mqttConnInfo{clientID: "sub", cleanSess: true}, lno.MQTT.Host, lno.MQTT.Port, 5)
		defer mc.Close()
		testMQTTCheckConnAck(t, rc, mqttConnAckRCConnectionAccepted, false)
		testMQTTSub(t, 1, mc, rc, []*mqttFilter{{filter: "foo", qos: 1}}, []byte{1})
		testMQTTFlush(t, mc, nil, rc)

		connectAndPublish := func(o *Options) {
			mp, rp := testMQTTConnectRetry(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port, 5)
			defer mp.Close()
			testMQTTCheckConnAck(t, rp, mqttConnAckRCConnectionAccepted, false)

			testMQTTPublish(t, mp, rp, 1, false, false, "foo", 1, []byte("msg"))
		}
		// Connect a publisher from leafnode and publish, verify message is received.
		connectAndPublish(lno)
		testMQTTCheckPubMsg(t, mc, rc, "foo", mqttPubQos1, []byte("msg"))

		// Connect from one server in the cluster check it works from there too.
		connectAndPublish(o3)
		testMQTTCheckPubMsg(t, mc, rc, "foo", mqttPubQos1, []byte("msg"))

		// Connect from a server in the hub and subscribe
		mc2, rc2 := testMQTTConnectRetry(t, &mqttConnInfo{clientID: "sub2", cleanSess: true}, o2.MQTT.Host, o2.MQTT.Port, 5)
		defer mc2.Close()
		testMQTTCheckConnAck(t, rc2, mqttConnAckRCConnectionAccepted, false)
		testMQTTSub(t, 1, mc2, rc2, []*mqttFilter{{filter: "foo", qos: 1}}, []byte{1})
		testMQTTFlush(t, mc2, nil, rc2)

		// Connect a publisher from leafnode and publish, verify message is received.
		connectAndPublish(lno)
		testMQTTCheckPubMsg(t, mc2, rc2, "foo", mqttPubQos1, []byte("msg"))

		// Connect from one server in the cluster check it works from there too.
		connectAndPublish(o1)
		testMQTTCheckPubMsg(t, mc2, rc2, "foo", mqttPubQos1, []byte("msg"))
	}
	t.Run("backwards-compatibility-default-js-enabled-in-leaf", func(t *testing.T) {
		test(t, 0) // test with JsAccDefaultDomain set to default (pointing at hub) but jetstream enabled in leaf node too
	})
	t.Run("backwards-compatibility-default-js-disabled-in-leaf", func(t *testing.T) {
		// test with JsAccDefaultDomain set. Checks if it works with backwards compatibility code for empty domain
		test(t, 1)
	})
	t.Run("backwards-compatibility-domain-js-disabled-in-leaf", func(t *testing.T) {
		// test with JsAccDefaultDomain set. Checks if it works with backwards compatibility code for domain set
		test(t, 2) // test with domain set in mqtt client
	})
	t.Run("mqtt-explicit-js-enabled-in-leaf", func(t *testing.T) {
		test(t, 3) // test with domain set in mqtt client (pointing at hub) but jetstream enabled in leaf node too
	})
	t.Run("mqtt-explicit-js-disabled-in-leaf", func(t *testing.T) {
		test(t, 4) // test with domain set in mqtt client
	})
	t.Run("backwards-compatibility-domain-js-enabled-in-leaf", func(t *testing.T) {
		test(t, 5) // test with JsAccDefaultDomain set to domain (pointing at hub) but jetstream enabled in leaf node too
	})
}

func TestMQTTImportExport(t *testing.T) {
	conf := createConfFile(t, []byte(`
		listen: "127.0.0.1:-1"
		server_name: "mqtt"
		jetstream {
			store_dir=org_dir
		}
		accounts {
			org {
				jetstream: enabled
				users: [{user: org, password: pwd}]
				imports = [{stream: {account: "device", subject: "foo"}, prefix: "org"}]
			}
			device {
				users: [{user: device, password: pwd}]
				exports = [{stream: "foo"}]
			}
		}
		mqtt {
			listen: "127.0.0.1:-1"
		}
		no_auth_user: device
	`))
	defer os.Remove(conf)
	defer os.RemoveAll("org_dir")

	s, o := RunServerWithConfig(conf)
	defer s.Shutdown()

	mc1, rc1 := testMQTTConnect(t, &mqttConnInfo{clientID: "sub1", user: "org", pass: "pwd", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mc1.Close()
	testMQTTCheckConnAck(t, rc1, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, mc1, rc1, []*mqttFilter{{filter: "org/foo", qos: 0}}, []byte{0})
	testMQTTFlush(t, mc1, nil, rc1)

	mc2, rc2 := testMQTTConnect(t, &mqttConnInfo{clientID: "sub2", user: "org", pass: "pwd", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mc2.Close()
	testMQTTCheckConnAck(t, rc2, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, mc2, rc2, []*mqttFilter{{filter: "org/foo", qos: 1}}, []byte{1})
	testMQTTFlush(t, mc2, nil, rc2)

	nc := natsConnect(t, s.ClientURL())
	defer nc.Close()
	natsPub(t, nc, "foo", []byte("msg"))

	// Verify message is received on receiver side.
	testMQTTCheckPubMsg(t, mc1, rc1, "org/foo", 0, []byte("msg"))
	testMQTTCheckPubMsg(t, mc2, rc2, "org/foo", 0, []byte("msg"))
}

func TestMQTTSessionMovingDomains(t *testing.T) {
	tmpl := strings.Replace(jsClusterTemplWithLeafAndMQTT, "{{leaf}}", `leafnodes { listen: 127.0.0.1:-1 }`, 1)
	tmpl = strings.Replace(tmpl, "store_dir:", "domain: HUB, store_dir:", 1)
	c := createJetStreamCluster(t, tmpl, "HUB", _EMPTY_, 3, 22020, true)
	defer c.shutdown()
	c.waitOnLeader()

	tmpl = strings.Replace(jsClusterTemplWithLeafAndMQTT, "store_dir:", "domain: SPOKE, store_dir:", 1)
	lnc := c.createLeafNodesWithTemplateAndStartPort(tmpl, "SPOKE", 3, 22111)
	defer lnc.shutdown()
	lnc.waitOnPeerCount(3)

	connectSubAndDisconnect := func(host string, port int, present bool) {
		t.Helper()
		mc, rc := testMQTTConnectRetry(t, &mqttConnInfo{clientID: "sub", cleanSess: false}, host, port, 5)
		defer mc.Close()
		testMQTTCheckConnAck(t, rc, mqttConnAckRCConnectionAccepted, present)
		testMQTTSub(t, 1, mc, rc, []*mqttFilter{{filter: "foo", qos: 1}}, []byte{1})
		testMQTTFlush(t, mc, nil, rc)
		testMQTTDisconnect(t, mc, nil)
	}

	// Create a session on the HUB. Make sure we don't use "clean" session so that
	// it is not removed when the client connection closes.
	for i := 0; i < 7; i++ {
		var present bool
		if i > 0 {
			present = true
		}
		connectSubAndDisconnect(c.opts[0].MQTT.Host, c.opts[0].MQTT.Port, present)
	}

	// Now move to the SPOKE cluster, this is a brand new session there, so should not be present.
	connectSubAndDisconnect(lnc.opts[1].MQTT.Host, lnc.opts[1].MQTT.Port, false)

	// Move back to HUB cluster. Make it interesting by connecting to a different
	// server in that cluster. This should work, and present flag should be true.
	connectSubAndDisconnect(c.opts[2].MQTT.Host, c.opts[2].MQTT.Port, true)
}

type remoteConnSameClientIDLogger struct {
	DummyLogger
	warn chan string
}

func (l *remoteConnSameClientIDLogger) Warnf(format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	if strings.Contains(msg, "remote connection has started with the same client ID") {
		l.warn <- msg
	}
}

func TestMQTTSessionsDifferentDomains(t *testing.T) {
	tmpl := strings.Replace(jsClusterTemplWithLeafAndMQTT, "{{leaf}}", `leafnodes { listen: 127.0.0.1:-1 }`, 1)
	c := createJetStreamCluster(t, tmpl, "HUB", _EMPTY_, 3, 22020, true)
	defer c.shutdown()
	c.waitOnLeader()

	tmpl = strings.Replace(jsClusterTemplWithLeafAndMQTT, "store_dir:", "domain: LEAF1, store_dir:", 1)
	lnc1 := c.createLeafNodesWithTemplateAndStartPort(tmpl, "LEAF1", 2, 22111)
	defer lnc1.shutdown()
	lnc1.waitOnPeerCount(2)

	l := &remoteConnSameClientIDLogger{warn: make(chan string, 10)}
	lnc1.servers[0].SetLogger(l, false, false)

	tmpl = strings.Replace(jsClusterTemplWithLeafAndMQTT, "store_dir:", "domain: LEAF2, store_dir:", 1)
	lnc2 := c.createLeafNodesWithTemplateAndStartPort(tmpl, "LEAF2", 2, 23111)
	defer lnc2.shutdown()
	lnc2.waitOnPeerCount(2)

	o := &(lnc1.opts[0].MQTT)
	mc1, rc1 := testMQTTConnectRetry(t, &mqttConnInfo{clientID: "sub", cleanSess: false}, o.Host, o.Port, 5)
	defer mc1.Close()
	testMQTTCheckConnAck(t, rc1, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, mc1, rc1, []*mqttFilter{{filter: "foo", qos: 1}}, []byte{1})
	testMQTTFlush(t, mc1, nil, rc1)

	o = &(lnc2.opts[0].MQTT)
	connectSubAndDisconnect := func(host string, port int, present bool) {
		t.Helper()
		mc, rc := testMQTTConnectRetry(t, &mqttConnInfo{clientID: "sub", cleanSess: false}, host, port, 5)
		defer mc.Close()
		testMQTTCheckConnAck(t, rc, mqttConnAckRCConnectionAccepted, present)
		testMQTTSub(t, 1, mc, rc, []*mqttFilter{{filter: "foo", qos: 1}}, []byte{1})
		testMQTTFlush(t, mc, nil, rc)
		testMQTTDisconnect(t, mc, nil)
	}

	for i := 0; i < 2; i++ {
		connectSubAndDisconnect(o.Host, o.Port, i > 0)
	}

	select {
	case w := <-l.warn:
		t.Fatalf("Got a warning: %v", w)
	case <-time.After(500 * time.Millisecond):
		// OK
	}
}

func TestMQTTParseUnsub(t *testing.T) {
	for _, test := range []struct {
		name  string
		proto []byte
		b     byte
		pl    int
		err   string
	}{
		{"reserved flag", nil, 3, 0, "wrong unsubscribe reserved flags"},
		{"ensure packet loaded", []byte{1, 2}, mqttUnsubscribeFlags, 10, io.ErrUnexpectedEOF.Error()},
		{"error reading packet id", []byte{1}, mqttUnsubscribeFlags, 1, "reading packet identifier"},
		{"missing filters", []byte{0, 1}, mqttUnsubscribeFlags, 2, "subscribe protocol must contain at least 1 topic filter"},
		{"error reading topic", []byte{0, 1, 0, 2, 'a'}, mqttUnsubscribeFlags, 5, "topic filter"},
		{"empty topic", []byte{0, 1, 0, 0}, mqttUnsubscribeFlags, 4, errMQTTTopicFilterCannotBeEmpty.Error()},
		{"invalid utf8 topic", []byte{0, 1, 0, 1, 241}, mqttUnsubscribeFlags, 5, "invalid utf8 for topic filter"},
	} {
		t.Run(test.name, func(t *testing.T) {
			r := &mqttReader{}
			r.reset(test.proto)
			mqtt := &mqtt{r: r}
			c := &client{mqtt: mqtt}
			if _, _, err := c.mqttParseSubsOrUnsubs(r, test.b, test.pl, false); err == nil || !strings.Contains(err.Error(), test.err) {
				t.Fatalf("Expected error %q, got %v", test.err, err)
			}
		})
	}
}

func testMQTTUnsub(t *testing.T, pi uint16, c net.Conn, r *mqttReader, filters []*mqttFilter) {
	t.Helper()
	w := &mqttWriter{}
	pkLen := 2 // for pi
	for i := 0; i < len(filters); i++ {
		f := filters[i]
		pkLen += 2 + len(f.filter)
	}
	w.WriteByte(mqttPacketUnsub | mqttUnsubscribeFlags)
	w.WriteVarInt(pkLen)
	w.WriteUint16(pi)
	for i := 0; i < len(filters); i++ {
		f := filters[i]
		w.WriteBytes([]byte(f.filter))
	}
	if _, err := testMQTTWrite(c, w.Bytes()); err != nil {
		t.Fatalf("Error writing UNSUBSCRIBE protocol: %v", err)
	}
	b, _ := testMQTTReadPacket(t, r)
	if pt := b & mqttPacketMask; pt != mqttPacketUnsubAck {
		t.Fatalf("Expected UNSUBACK packet %x, got %x", mqttPacketUnsubAck, pt)
	}
	rpi, err := r.readUint16("packet identifier")
	if err != nil || rpi != pi {
		t.Fatalf("Error with packet identifier expected=%v got: %v err=%v", pi, rpi, err)
	}
}

func TestMQTTUnsub(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	mcp, mpr := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mcp.Close()
	testMQTTCheckConnAck(t, mpr, mqttConnAckRCConnectionAccepted, false)

	mc, r := testMQTTConnect(t, &mqttConnInfo{user: "sub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mc.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

	testMQTTSub(t, 1, mc, r, []*mqttFilter{{filter: "foo", qos: 0}}, []byte{0})
	testMQTTFlush(t, mc, nil, r)

	// Publish and test msg received
	testMQTTPublish(t, mcp, r, 0, false, false, "foo", 0, []byte("msg"))
	testMQTTCheckPubMsg(t, mc, r, "foo", 0, []byte("msg"))

	// Unsubscribe
	testMQTTUnsub(t, 1, mc, r, []*mqttFilter{{filter: "foo"}})

	// Publish and test msg not received
	testMQTTPublish(t, mcp, r, 0, false, false, "foo", 0, []byte("msg"))
	testMQTTExpectNothing(t, r)

	// Use of wildcards subs
	filters := []*mqttFilter{
		{filter: "foo/bar", qos: 0},
		{filter: "foo/#", qos: 0},
	}
	testMQTTSub(t, 1, mc, r, filters, []byte{0, 0})
	testMQTTFlush(t, mc, nil, r)

	// Publish and check that message received twice
	testMQTTPublish(t, mcp, r, 0, false, false, "foo/bar", 0, []byte("msg"))
	testMQTTCheckPubMsg(t, mc, r, "foo/bar", 0, []byte("msg"))
	testMQTTCheckPubMsg(t, mc, r, "foo/bar", 0, []byte("msg"))

	// Unsub the wildcard one
	testMQTTUnsub(t, 1, mc, r, []*mqttFilter{{filter: "foo/#"}})
	// Publish and check that message received once
	testMQTTPublish(t, mcp, r, 0, false, false, "foo/bar", 0, []byte("msg"))
	testMQTTCheckPubMsg(t, mc, r, "foo/bar", 0, []byte("msg"))
	testMQTTExpectNothing(t, r)

	// Unsub last
	testMQTTUnsub(t, 1, mc, r, []*mqttFilter{{filter: "foo/bar"}})
	// Publish and test msg not received
	testMQTTPublish(t, mcp, r, 0, false, false, "foo/bar", 0, []byte("msg"))
	testMQTTExpectNothing(t, r)
}

func testMQTTExpectDisconnect(t testing.TB, c net.Conn) {
	t.Helper()
	if buf, err := testMQTTRead(c); err == nil {
		t.Fatalf("Expected connection to be disconnected, got %s", buf)
	}
}

func TestMQTTPublishTopicErrors(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	for _, test := range []struct {
		name  string
		topic string
	}{
		{"empty", ""},
		{"with single level wildcard", "foo/+"},
		{"with multiple level wildcard", "foo/#"},
	} {
		t.Run(test.name, func(t *testing.T) {
			mc, r := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
			defer mc.Close()
			testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

			testMQTTPublish(t, mc, r, 0, false, false, test.topic, 0, []byte("msg"))
			testMQTTExpectDisconnect(t, mc)
		})
	}
}

func testMQTTDisconnect(t testing.TB, c net.Conn, bw *bufio.Writer) {
	t.Helper()
	w := &mqttWriter{}
	w.WriteByte(mqttPacketDisconnect)
	w.WriteByte(0)
	if bw != nil {
		bw.Write(w.Bytes())
		bw.Flush()
	} else {
		c.Write(w.Bytes())
	}
	testMQTTExpectDisconnect(t, c)
}

func TestMQTTWill(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	nc := natsConnect(t, s.ClientURL())
	defer nc.Close()

	sub := natsSubSync(t, nc, "will.topic")
	natsFlush(t, nc)

	willMsg := []byte("bye")

	for _, test := range []struct {
		name         string
		willExpected bool
		willQoS      byte
	}{
		{"will qos 0", true, 0},
		{"will qos 1", true, 1},
		{"proper disconnect no will", false, 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			mcs, rs := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
			defer mcs.Close()
			testMQTTCheckConnAck(t, rs, mqttConnAckRCConnectionAccepted, false)

			testMQTTSub(t, 1, mcs, rs, []*mqttFilter{{filter: "will/#", qos: 1}}, []byte{1})
			testMQTTFlush(t, mcs, nil, rs)

			mc, r := testMQTTConnect(t,
				&mqttConnInfo{
					cleanSess: true,
					will: &mqttWill{
						topic:   []byte("will/topic"),
						message: willMsg,
						qos:     test.willQoS,
					},
				}, o.MQTT.Host, o.MQTT.Port)
			defer mc.Close()
			testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

			if test.willExpected {
				mc.Close()
				testMQTTCheckPubMsg(t, mcs, rs, "will/topic", test.willQoS<<1, willMsg)
				wm := natsNexMsg(t, sub, time.Second)
				if !bytes.Equal(wm.Data, willMsg) {
					t.Fatalf("Expected will message to be %q, got %q", willMsg, wm.Data)
				}
			} else {
				testMQTTDisconnect(t, mc, nil)
				testMQTTExpectNothing(t, rs)
				if wm, err := sub.NextMsg(100 * time.Millisecond); err == nil {
					t.Fatalf("Should not have receive a message, got subj=%q data=%q",
						wm.Subject, wm.Data)
				}
			}
		})
	}
}

func TestMQTTWillRetain(t *testing.T) {
	for _, test := range []struct {
		name   string
		pubQoS byte
		subQoS byte
	}{
		{"pub QoS0 sub QoS0", 0, 0},
		{"pub QoS0 sub QoS1", 0, 1},
		{"pub QoS1 sub QoS0", 1, 0},
		{"pub QoS1 sub QoS1", 1, 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			o := testMQTTDefaultOptions()
			s := testMQTTRunServer(t, o)
			defer testMQTTShutdownServer(s)

			willTopic := []byte("will/topic")
			willMsg := []byte("bye")

			mc, r := testMQTTConnect(t,
				&mqttConnInfo{
					cleanSess: true,
					will: &mqttWill{
						topic:   willTopic,
						message: willMsg,
						qos:     test.pubQoS,
						retain:  true,
					},
				}, o.MQTT.Host, o.MQTT.Port)
			defer mc.Close()
			testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

			// Disconnect the client
			mc.Close()

			// Wait for the server to process the connection close, which will
			// cause the "will" message to be published (and retained).
			checkClientsCount(t, s, 0)

			// Create subscription on will topic and expect will message.
			mcs, rs := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
			defer mcs.Close()
			testMQTTCheckConnAck(t, rs, mqttConnAckRCConnectionAccepted, false)

			testMQTTSub(t, 1, mcs, rs, []*mqttFilter{{filter: "will/#", qos: test.subQoS}}, []byte{test.subQoS})
			pflags, _ := testMQTTGetPubMsg(t, mcs, rs, "will/topic", willMsg)
			if pflags&mqttPubFlagRetain == 0 {
				t.Fatalf("expected retain flag to be set, it was not: %v", pflags)
			}
			// Expected QoS will be the lesser of the pub/sub QoS.
			expectedQoS := test.pubQoS
			if test.subQoS == 0 {
				expectedQoS = 0
			}
			if qos := mqttGetQoS(pflags); qos != expectedQoS {
				t.Fatalf("expected qos to be %v, got %v", expectedQoS, qos)
			}
		})
	}
}

func TestMQTTWillRetainPermViolation(t *testing.T) {
	template := `
		port: -1
		jetstream: enabled
		server_name: mqtt
		authorization {
			mqtt_perms = {
				publish = ["%s"]
				subscribe = ["foo", "bar", "$MQTT.sub.>"]
			}
			users = [
				{user: mqtt, password: pass, permissions: $mqtt_perms}
			]
		}
		mqtt {
			port: -1
		}
	`
	conf := createConfFile(t, []byte(fmt.Sprintf(template, "foo")))

	s, o := RunServerWithConfig(conf)
	defer testMQTTShutdownServer(s)

	ci := &mqttConnInfo{
		cleanSess: true,
		user:      "mqtt",
		pass:      "pass",
	}

	// We create first a connection with the Will topic that the publisher
	// is allowed to publish to.
	ci.will = &mqttWill{
		topic:   []byte("foo"),
		message: []byte("bye"),
		qos:     1,
		retain:  true,
	}
	mc, r := testMQTTConnect(t, ci, o.MQTT.Host, o.MQTT.Port)
	defer mc.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

	// Disconnect, which will cause the Will to be sent with retain flag.
	mc.Close()

	// Wait for the server to process the connection close, which will
	// cause the "will" message to be published (and retained).
	checkClientsCount(t, s, 0)

	// Create a subscription on the Will subject and we should receive it.
	ci.will = nil
	mcs, rs := testMQTTConnect(t, ci, o.MQTT.Host, o.MQTT.Port)
	defer mcs.Close()
	testMQTTCheckConnAck(t, rs, mqttConnAckRCConnectionAccepted, false)

	testMQTTSub(t, 1, mcs, rs, []*mqttFilter{{filter: "foo", qos: 1}}, []byte{1})
	pflags, _ := testMQTTGetPubMsg(t, mcs, rs, "foo", []byte("bye"))
	if pflags&mqttPubFlagRetain == 0 {
		t.Fatalf("expected retain flag to be set, it was not: %v", pflags)
	}
	if qos := mqttGetQoS(pflags); qos != 1 {
		t.Fatalf("expected qos to be 1, got %v", qos)
	}
	testMQTTDisconnect(t, mcs, nil)

	// Now create another connection with a Will that client is not allowed to publish to.
	ci.will = &mqttWill{
		topic:   []byte("bar"),
		message: []byte("bye"),
		qos:     1,
		retain:  true,
	}
	mc, r = testMQTTConnect(t, ci, o.MQTT.Host, o.MQTT.Port)
	defer mc.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

	// Disconnect, to cause Will to be produced, but in that case should not be stored
	// since user not allowed to publish on "bar".
	mc.Close()

	// Wait for the server to process the connection close, which will
	// cause the "will" message to be published (and retained).
	checkClientsCount(t, s, 0)

	// Create sub on "bar" which user is allowed to subscribe to.
	ci.will = nil
	mcs, rs = testMQTTConnect(t, ci, o.MQTT.Host, o.MQTT.Port)
	defer mcs.Close()
	testMQTTCheckConnAck(t, rs, mqttConnAckRCConnectionAccepted, false)

	testMQTTSub(t, 1, mcs, rs, []*mqttFilter{{filter: "bar", qos: 1}}, []byte{1})
	// No Will should be published since it should not have been stored in the first place.
	testMQTTExpectNothing(t, rs)
	testMQTTDisconnect(t, mcs, nil)

	// Now remove permission to publish on "foo" and check that a new subscription
	// on "foo" is now not getting the will message because the original user no
	// longer has permission to do so.
	reloadUpdateConfig(t, s, conf, fmt.Sprintf(template, "baz"))

	mcs, rs = testMQTTConnect(t, ci, o.MQTT.Host, o.MQTT.Port)
	defer mcs.Close()
	testMQTTCheckConnAck(t, rs, mqttConnAckRCConnectionAccepted, false)

	testMQTTSub(t, 1, mcs, rs, []*mqttFilter{{filter: "foo", qos: 1}}, []byte{1})
	testMQTTExpectNothing(t, rs)
	testMQTTDisconnect(t, mcs, nil)
}

func TestMQTTPublishRetain(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	for _, test := range []struct {
		name          string
		retained      bool
		sentValue     string
		expectedValue string
		subGetsIt     bool
	}{
		{"publish retained", true, "retained", "retained", true},
		{"publish not retained", false, "not retained", "retained", true},
		{"remove retained", true, "", "", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			mc1, rs1 := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
			defer mc1.Close()
			testMQTTCheckConnAck(t, rs1, mqttConnAckRCConnectionAccepted, false)
			testMQTTPublish(t, mc1, rs1, 0, false, test.retained, "foo", 0, []byte(test.sentValue))
			testMQTTFlush(t, mc1, nil, rs1)

			mc2, rs2 := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
			defer mc2.Close()
			testMQTTCheckConnAck(t, rs2, mqttConnAckRCConnectionAccepted, false)

			testMQTTSub(t, 1, mc2, rs2, []*mqttFilter{{filter: "foo/#", qos: 1}}, []byte{1})

			if test.subGetsIt {
				pflags, _ := testMQTTGetPubMsg(t, mc2, rs2, "foo", []byte(test.expectedValue))
				if pflags&mqttPubFlagRetain == 0 {
					t.Fatalf("retain flag should have been set, it was not: flags=%v", pflags)
				}
			} else {
				testMQTTExpectNothing(t, rs2)
			}

			testMQTTDisconnect(t, mc1, nil)
			testMQTTDisconnect(t, mc2, nil)
		})
	}
}

func TestMQTTPublishRetainPermViolation(t *testing.T) {
	o := testMQTTDefaultOptions()
	o.Users = []*User{
		{
			Username: "mqtt",
			Password: "pass",
			Permissions: &Permissions{
				Publish:   &SubjectPermission{Allow: []string{"foo"}},
				Subscribe: &SubjectPermission{Allow: []string{"bar", "$MQTT.sub.>"}},
			},
		},
	}
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	ci := &mqttConnInfo{
		cleanSess: true,
		user:      "mqtt",
		pass:      "pass",
	}

	mc1, rs1 := testMQTTConnect(t, ci, o.MQTT.Host, o.MQTT.Port)
	defer mc1.Close()
	testMQTTCheckConnAck(t, rs1, mqttConnAckRCConnectionAccepted, false)
	testMQTTPublish(t, mc1, rs1, 0, false, true, "bar", 0, []byte("retained"))
	testMQTTFlush(t, mc1, nil, rs1)

	mc2, rs2 := testMQTTConnect(t, ci, o.MQTT.Host, o.MQTT.Port)
	defer mc2.Close()
	testMQTTCheckConnAck(t, rs2, mqttConnAckRCConnectionAccepted, false)

	testMQTTSub(t, 1, mc2, rs2, []*mqttFilter{{filter: "bar", qos: 1}}, []byte{1})
	testMQTTExpectNothing(t, rs2)

	testMQTTDisconnect(t, mc1, nil)
	testMQTTDisconnect(t, mc2, nil)
}

func TestMQTTPublishViolation(t *testing.T) {
	o := testMQTTDefaultOptions()
	o.Users = []*User{
		{
			Username: "mqtt",
			Password: "pass",
			Permissions: &Permissions{
				Publish:   &SubjectPermission{Allow: []string{"foo.bar"}},
				Subscribe: &SubjectPermission{Allow: []string{"foo.*", "$MQTT.sub.>"}},
			},
		},
	}
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	ci := &mqttConnInfo{
		user: "mqtt",
		pass: "pass",
	}
	ci.clientID = "sub"
	mc, rc := testMQTTConnect(t, ci, o.MQTT.Host, o.MQTT.Port)
	defer mc.Close()
	testMQTTCheckConnAck(t, rc, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, mc, rc, []*mqttFilter{{filter: "foo/+", qos: 1}}, []byte{1})
	testMQTTFlush(t, mc, nil, rc)

	ci.clientID = "pub"
	ci.cleanSess = true
	mp, rp := testMQTTConnect(t, ci, o.MQTT.Host, o.MQTT.Port)
	defer mp.Close()
	testMQTTCheckConnAck(t, rp, mqttConnAckRCConnectionAccepted, false)

	// These should be received since publisher has the right to publish on foo.bar
	testMQTTPublish(t, mp, rp, 0, false, false, "foo/bar", 0, []byte("msg1"))
	testMQTTCheckPubMsg(t, mc, rc, "foo/bar", 0, []byte("msg1"))
	testMQTTPublish(t, mp, rp, 1, false, false, "foo/bar", 1, []byte("msg2"))
	testMQTTCheckPubMsg(t, mc, rc, "foo/bar", mqttPubQos1, []byte("msg2"))

	// But these should not be cause pub has no permission to publish on foo.baz
	testMQTTPublish(t, mp, rp, 0, false, false, "foo/baz", 0, []byte("msg3"))
	testMQTTExpectNothing(t, rc)
	testMQTTPublish(t, mp, rp, 1, false, false, "foo/baz", 1, []byte("msg4"))
	testMQTTExpectNothing(t, rc)

	// Disconnect publisher
	testMQTTDisconnect(t, mp, nil)
	mp.Close()
	// Disconnect subscriber and restart it to make sure that it does not receive msg3/msg4
	testMQTTDisconnect(t, mc, nil)
	mc.Close()

	ci.cleanSess = false
	ci.clientID = "sub"
	mc, rc = testMQTTConnect(t, ci, o.MQTT.Host, o.MQTT.Port)
	defer mc.Close()
	testMQTTCheckConnAck(t, rc, mqttConnAckRCConnectionAccepted, true)
	testMQTTSub(t, 1, mc, rc, []*mqttFilter{{filter: "foo/+", qos: 1}}, []byte{1})
	testMQTTExpectNothing(t, rc)
}

func TestMQTTCleanSession(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	ci := &mqttConnInfo{
		clientID:  "me",
		cleanSess: false,
	}
	c, r := testMQTTConnect(t, ci, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTDisconnect(t, c, nil)

	c, r = testMQTTConnect(t, ci, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, true)
	testMQTTDisconnect(t, c, nil)

	ci.cleanSess = true
	c, r = testMQTTConnect(t, ci, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTDisconnect(t, c, nil)
}

func TestMQTTDuplicateClientID(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	ci := &mqttConnInfo{
		clientID:  "me",
		cleanSess: false,
	}
	c1, r1 := testMQTTConnect(t, ci, o.MQTT.Host, o.MQTT.Port)
	defer c1.Close()
	testMQTTCheckConnAck(t, r1, mqttConnAckRCConnectionAccepted, false)

	c2, r2 := testMQTTConnect(t, ci, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, true)

	// The old client should be disconnected.
	testMQTTExpectDisconnect(t, c1)
}

func TestMQTTPersistedSession(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownRestartedServer(&s)

	cisub := &mqttConnInfo{clientID: "sub", cleanSess: false}
	cipub := &mqttConnInfo{clientID: "pub", cleanSess: true}

	c, r := testMQTTConnect(t, cisub, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

	testMQTTSub(t, 1, c, r,
		[]*mqttFilter{
			{filter: "foo/#", qos: 1},
			{filter: "bar", qos: 1},
			{filter: "baz", qos: 0},
		},
		[]byte{1, 1, 0})
	testMQTTFlush(t, c, nil, r)

	// Shutdown server, close connection and restart server. It should
	// have restored the session and consumers.
	dir := strings.TrimSuffix(s.JetStreamConfig().StoreDir, JetStreamStoreDir)
	s.Shutdown()
	c.Close()

	o.Port = -1
	o.MQTT.Port = -1
	o.StoreDir = dir
	s = testMQTTRunServer(t, o)
	// There is already the defer for shutdown at top of function

	// Create a publisher that will send qos1 so we verify that messages
	// are stored for the persisted sessions.
	c, r = testMQTTConnect(t, cipub, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

	testMQTTPublish(t, c, r, 1, false, false, "foo/bar", 1, []byte("msg0"))
	testMQTTFlush(t, c, nil, r)
	testMQTTDisconnect(t, c, nil)
	c.Close()

	// Recreate session
	c, r = testMQTTConnect(t, cisub, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, true)

	// Since consumers have been recovered, messages should be received
	// (MQTT does not need client to recreate consumers for a recovered
	// session)

	// Check that qos1 publish message is received.
	testMQTTCheckPubMsg(t, c, r, "foo/bar", mqttPubQos1, []byte("msg0"))

	// Flush to prevent publishes to be done too soon since we are
	// receiving the CONNACK before the subscriptions are restored.
	testMQTTFlush(t, c, nil, r)

	// Now publish some messages to all subscriptions.
	nc := natsConnect(t, s.ClientURL())
	defer nc.Close()

	natsPub(t, nc, "foo.bar", []byte("msg1"))
	testMQTTCheckPubMsg(t, c, r, "foo/bar", 0, []byte("msg1"))

	natsPub(t, nc, "foo", []byte("msg2"))
	testMQTTCheckPubMsg(t, c, r, "foo", 0, []byte("msg2"))

	natsPub(t, nc, "bar", []byte("msg3"))
	testMQTTCheckPubMsg(t, c, r, "bar", 0, []byte("msg3"))

	natsPub(t, nc, "baz", []byte("msg4"))
	testMQTTCheckPubMsg(t, c, r, "baz", 0, []byte("msg4"))

	// Now unsub "bar" and verify that message published on this topic
	// is not received.
	testMQTTUnsub(t, 1, c, r, []*mqttFilter{{filter: "bar"}})
	natsPub(t, nc, "bar", []byte("msg5"))
	testMQTTExpectNothing(t, r)

	nc.Close()
	s.Shutdown()
	c.Close()

	o.Port = -1
	o.MQTT.Port = -1
	o.StoreDir = dir
	s = testMQTTRunServer(t, o)
	// There is already the defer for shutdown at top of function

	// Recreate a client
	c, r = testMQTTConnect(t, cisub, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, true)

	nc = natsConnect(t, s.ClientURL())
	defer nc.Close()

	natsPub(t, nc, "foo.bar", []byte("msg6"))
	testMQTTCheckPubMsg(t, c, r, "foo/bar", 0, []byte("msg6"))

	natsPub(t, nc, "foo", []byte("msg7"))
	testMQTTCheckPubMsg(t, c, r, "foo", 0, []byte("msg7"))

	// Make sure that we did not recover bar.
	natsPub(t, nc, "bar", []byte("msg8"))
	testMQTTExpectNothing(t, r)

	natsPub(t, nc, "baz", []byte("msg9"))
	testMQTTCheckPubMsg(t, c, r, "baz", 0, []byte("msg9"))

	// Have the sub client send a subscription downgrading the qos1 subscription.
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "foo/#", qos: 0}}, []byte{0})
	testMQTTFlush(t, c, nil, r)

	nc.Close()
	s.Shutdown()
	c.Close()

	o.Port = -1
	o.MQTT.Port = -1
	o.StoreDir = dir
	s = testMQTTRunServer(t, o)
	// There is already the defer for shutdown at top of function

	// Recreate the sub client
	c, r = testMQTTConnect(t, cisub, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, true)

	// Publish as a qos1
	c2, r2 := testMQTTConnect(t, cipub, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, false)
	testMQTTPublish(t, c2, r2, 1, false, false, "foo/bar", 1, []byte("msg10"))

	// Verify that it is received as qos0 which is the qos of the subscription.
	testMQTTCheckPubMsg(t, c, r, "foo/bar", 0, []byte("msg10"))

	testMQTTDisconnect(t, c, nil)
	c.Close()
	testMQTTDisconnect(t, c2, nil)
	c2.Close()

	// Finally, recreate the sub with clean session and ensure that all is gone
	cisub.cleanSess = true
	for i := 0; i < 2; i++ {
		c, r = testMQTTConnect(t, cisub, o.MQTT.Host, o.MQTT.Port)
		defer c.Close()
		testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

		nc = natsConnect(t, s.ClientURL())
		defer nc.Close()

		natsPub(t, nc, "foo.bar", []byte("msg11"))
		testMQTTExpectNothing(t, r)

		natsPub(t, nc, "foo", []byte("msg12"))
		testMQTTExpectNothing(t, r)

		// Make sure that we did not recover bar.
		natsPub(t, nc, "bar", []byte("msg13"))
		testMQTTExpectNothing(t, r)

		natsPub(t, nc, "baz", []byte("msg14"))
		testMQTTExpectNothing(t, r)

		testMQTTDisconnect(t, c, nil)
		c.Close()
		nc.Close()

		s.Shutdown()
		o.Port = -1
		o.MQTT.Port = -1
		o.StoreDir = dir
		s = testMQTTRunServer(t, o)
		// There is already the defer for shutdown at top of function
	}
}

func TestMQTTRecoverSessionAndAddNewSub(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownRestartedServer(&s)

	cisub := &mqttConnInfo{clientID: "sub1", cleanSess: false}
	c, r := testMQTTConnect(t, cisub, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTDisconnect(t, c, nil)
	c.Close()

	// Shutdown server, close connection and restart server. It should
	// have restored the session and consumers.
	dir := strings.TrimSuffix(s.JetStreamConfig().StoreDir, JetStreamStoreDir)
	s.Shutdown()
	c.Close()

	o.Port = -1
	o.MQTT.Port = -1
	o.StoreDir = dir
	s = testMQTTRunServer(t, o)
	// No need for defer since it is done top of function

	c, r = testMQTTConnect(t, cisub, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, true)
	// Now add sub and make sure it does not crash
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "foo", qos: 1}}, []byte{1})
	testMQTTFlush(t, c, nil, r)

	// Now repeat with a new client but without server restart.
	cisub2 := &mqttConnInfo{clientID: "sub2", cleanSess: false}
	c2, r2 := testMQTTConnect(t, cisub2, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, false)
	testMQTTDisconnect(t, c2, nil)
	c2.Close()

	c2, r2 = testMQTTConnect(t, cisub2, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, true)
	testMQTTSub(t, 1, c2, r2, []*mqttFilter{{filter: "bar", qos: 1}}, []byte{1})
	testMQTTFlush(t, c2, nil, r2)
}

func TestMQTTRecoverSessionWithSubAndClientResendSub(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownRestartedServer(&s)

	cisub1 := &mqttConnInfo{clientID: "sub1", cleanSess: false}
	c, r := testMQTTConnect(t, cisub1, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

	// Have a client send a SUBSCRIBE protocol for foo, QoS1
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "foo", qos: 1}}, []byte{1})
	testMQTTDisconnect(t, c, nil)
	c.Close()

	// Restart the server now.
	dir := strings.TrimSuffix(s.JetStreamConfig().StoreDir, JetStreamStoreDir)
	s.Shutdown()

	o.Port = -1
	o.MQTT.Port = -1
	o.StoreDir = dir
	s = testMQTTRunServer(t, o)
	// No need for defer since it is done top of function

	// Now restart the client. Since the client was created with cleanSess==false,
	// the server will have recorded the subscriptions for this client.
	c, r = testMQTTConnect(t, cisub1, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, true)
	// At this point, the server has recreated the subscription on foo, QoS1.

	// For applications that restart, it is possible (likely) that they
	// will resend their SUBSCRIBE protocols, so do so now:
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "foo", qos: 1}}, []byte{1})
	testMQTTFlush(t, c, nil, r)

	checkNumSub := func(clientID string) {
		t.Helper()

		// Find the MQTT client...
		mc := testMQTTGetClient(t, s, clientID)

		// Check how many NATS subscriptions are registered.
		var fooSub int
		var otherSub int
		mc.mu.Lock()
		for _, sub := range mc.subs {
			switch string(sub.subject) {
			case "foo":
				fooSub++
			default:
				otherSub++
			}
		}
		mc.mu.Unlock()

		// We should have 2 subscriptions, one on "foo", and one for the JS durable
		// consumer's delivery subject.
		if fooSub != 1 {
			t.Fatalf("Expected 1 sub on 'foo', got %v", fooSub)
		}
		if otherSub != 1 {
			t.Fatalf("Expected 1 subscription for JS durable, got %v", otherSub)
		}
	}
	checkNumSub("sub1")

	c.Close()

	// Now same but without the server restart in-between.
	cisub2 := &mqttConnInfo{clientID: "sub2", cleanSess: false}
	c, r = testMQTTConnect(t, cisub2, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "foo", qos: 1}}, []byte{1})
	testMQTTDisconnect(t, c, nil)
	c.Close()
	// Restart client
	c, r = testMQTTConnect(t, cisub2, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, true)
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "foo", qos: 1}}, []byte{1})
	testMQTTFlush(t, c, nil, r)
	// Check client subs
	checkNumSub("sub2")
}

func TestMQTTFlappingSession(t *testing.T) {
	mqttSessJailDur = 250 * time.Millisecond
	mqttFlapCleanItvl = 350 * time.Millisecond
	defer func() {
		mqttSessJailDur = mqttSessFlappingJailDur
		mqttFlapCleanItvl = mqttSessFlappingCleanupInterval
	}()

	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	ci := &mqttConnInfo{clientID: "flapper", cleanSess: false}
	c, r := testMQTTConnect(t, ci, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

	// Let's get a handle on the asm to check things later.
	cli := testMQTTGetClient(t, s, "flapper")
	asm := cli.mqtt.asm

	// Start a new connection with the same clientID, which should replace
	// the old one and put it in the flappers map.
	c2, r2 := testMQTTConnect(t, ci, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, true)

	// Should be disconnected...
	testMQTTExpectDisconnect(t, c)

	// Now try to reconnect "c" and we should fail. We have to do this manually,
	// since we expect it to fail.
	addr := fmt.Sprintf("%s:%d", o.MQTT.Host, o.MQTT.Port)
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Error creating mqtt connection: %v", err)
	}
	defer c.Close()
	proto := mqttCreateConnectProto(ci)
	if _, err := testMQTTWrite(c, proto); err != nil {
		t.Fatalf("Error writing protocols: %v", err)
	}
	// Misbehave and send a SUB protocol without waiting for the CONNACK
	w := &mqttWriter{}
	pkLen := 2 // for pi
	// Topic "foo"
	pkLen += 2 + 3 + 1
	w.WriteByte(mqttPacketSub | mqttSubscribeFlags)
	w.WriteVarInt(pkLen)
	w.WriteUint16(1)
	w.WriteBytes([]byte("foo"))
	w.WriteByte(1)
	if _, err := testMQTTWrite(c, w.Bytes()); err != nil {
		t.Fatalf("Error writing protocols: %v", err)
	}
	// Now read the CONNACK and we should have been disconnected.
	if _, err := testMQTTRead(c); err == nil {
		t.Fatal("Expected connection to fail")
	}

	// This should be in the flappers map, but after 250ms should be cleared.
	for i := 0; i < 2; i++ {
		asm.mu.RLock()
		_, present := asm.flappers["flapper"]
		asm.mu.RUnlock()
		if i == 0 {
			if !present {
				t.Fatal("Did not find the client ID in the flappers map")
			}
			// Wait for more than the cleanup interval
			time.Sleep(mqttFlapCleanItvl + 100*time.Millisecond)
		} else if present {
			t.Fatal("The client ID should have been cleared from the map")
		}
	}
}

func TestMQTTLockedSession(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	ci := &mqttConnInfo{clientID: "sub", cleanSess: false}
	c, r := testMQTTConnect(t, ci, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

	sm := &s.mqtt.sessmgr
	sm.mu.Lock()
	asm := sm.sessions[globalAccountName]
	sm.mu.Unlock()
	if asm == nil {
		t.Fatalf("account session manager not found")
	}

	// Get the session for "sub"
	cli := testMQTTGetClient(t, s, "sub")
	sess := cli.mqtt.sess

	// Pretend that the session above is locked.
	if err := asm.lockSession(sess, cli); err != nil {
		t.Fatalf("Unable to lock session: %v", err)
	}
	defer asm.unlockSession(sess)

	// Now try to connect another client that wants to use "sub".
	// We can't use testMQTTConnect() because it is going to fail.
	addr := fmt.Sprintf("%s:%d", o.MQTT.Host, o.MQTT.Port)
	c2, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Error creating mqtt connection: %v", err)
	}
	defer c2.Close()
	proto := mqttCreateConnectProto(ci)
	if _, err := testMQTTWrite(c2, proto); err != nil {
		t.Fatalf("Error writing connect: %v", err)
	}
	if _, err := testMQTTRead(c2); err == nil {
		t.Fatal("Expected connection to fail")
	}

	// Now try again, but this time release the session while waiting
	// to connect and it should succeed.
	time.AfterFunc(250*time.Millisecond, func() { asm.unlockSession(sess) })
	c3, r3 := testMQTTConnect(t, ci, o.MQTT.Host, o.MQTT.Port)
	defer c3.Close()
	testMQTTCheckConnAck(t, r3, mqttConnAckRCConnectionAccepted, true)
}

func TestMQTTPersistRetainedMsg(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownRestartedServer(&s)

	dir := strings.TrimSuffix(s.JetStreamConfig().StoreDir, JetStreamStoreDir)

	cipub := &mqttConnInfo{clientID: "pub", cleanSess: true}
	c, r := testMQTTConnect(t, cipub, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

	testMQTTPublish(t, c, r, 1, false, true, "foo", 1, []byte("foo1"))
	testMQTTPublish(t, c, r, 1, false, true, "foo", 1, []byte("foo2"))
	testMQTTPublish(t, c, r, 1, false, true, "bar", 1, []byte("bar1"))
	testMQTTPublish(t, c, r, 0, false, true, "baz", 1, []byte("baz1"))
	// Remove bar
	testMQTTPublish(t, c, r, 1, false, true, "bar", 1, nil)
	testMQTTFlush(t, c, nil, r)
	testMQTTDisconnect(t, c, nil)
	c.Close()

	s.Shutdown()

	o.Port = -1
	o.MQTT.Port = -1
	o.StoreDir = dir
	s = testMQTTRunServer(t, o)
	// There is already the defer for shutdown at top of function

	cisub := &mqttConnInfo{clientID: "sub", cleanSess: false}
	c, r = testMQTTConnect(t, cisub, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "foo", qos: 1}}, []byte{1})
	testMQTTCheckPubMsg(t, c, r, "foo", mqttPubFlagRetain|mqttPubQos1, []byte("foo2"))

	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "baz", qos: 1}}, []byte{1})
	testMQTTCheckPubMsg(t, c, r, "baz", mqttPubFlagRetain, []byte("baz1"))

	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "bar", qos: 1}}, []byte{1})
	testMQTTExpectNothing(t, r)

	testMQTTDisconnect(t, c, nil)
	c.Close()
}

func TestMQTTConnAckFirstPacket(t *testing.T) {
	o := testMQTTDefaultOptions()
	o.NoLog, o.Debug, o.Trace = true, false, false
	s := RunServer(o)
	defer testMQTTShutdownServer(s)

	cisub := &mqttConnInfo{clientID: "sub", cleanSess: false}
	c, r := testMQTTConnect(t, cisub, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "foo", qos: 0}}, []byte{0})
	testMQTTDisconnect(t, c, nil)
	c.Close()

	nc := natsConnect(t, s.ClientURL())
	defer nc.Close()

	wg := sync.WaitGroup{}
	wg.Add(1)
	ch := make(chan struct{}, 1)
	ready := make(chan struct{})
	go func() {
		defer wg.Done()

		close(ready)
		for {
			nc.Publish("foo", []byte("msg"))
			select {
			case <-ch:
				return
			default:
			}
		}
	}()

	<-ready
	for i := 0; i < 100; i++ {
		c, r = testMQTTConnect(t, cisub, o.MQTT.Host, o.MQTT.Port)
		defer c.Close()
		testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, true)
		w := &mqttWriter{}
		w.WriteByte(mqttPacketDisconnect)
		w.WriteByte(0)
		c.Write(w.Bytes())
		// Wait to be disconnected, we can't use testMQTTDisconnect() because
		// it would fail because we may still receive some NATS messages.
		var b [10]byte
		for {
			if _, err := c.Read(b[:]); err != nil {
				break
			}
		}
		c.Close()
	}
	close(ch)
	wg.Wait()
}

func TestMQTTRedeliveryAckWait(t *testing.T) {
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = 250 * time.Millisecond
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	cisub := &mqttConnInfo{clientID: "sub", cleanSess: false}
	c, r := testMQTTConnect(t, cisub, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "foo", qos: 1}}, []byte{1})

	cipub := &mqttConnInfo{clientID: "pub", cleanSess: true}
	cp, rp := testMQTTConnect(t, cipub, o.MQTT.Host, o.MQTT.Port)
	defer cp.Close()
	testMQTTCheckConnAck(t, rp, mqttConnAckRCConnectionAccepted, false)

	testMQTTPublish(t, cp, rp, 1, false, false, "foo", 1, []byte("foo1"))
	testMQTTPublish(t, cp, rp, 1, false, false, "foo", 2, []byte("foo2"))
	testMQTTDisconnect(t, cp, nil)
	cp.Close()

	for i := 0; i < 2; i++ {
		flags := mqttPubQos1
		if i > 0 {
			flags |= mqttPubFlagDup
		}
		pi1 := testMQTTCheckPubMsgNoAck(t, c, r, "foo", flags, []byte("foo1"))
		pi2 := testMQTTCheckPubMsgNoAck(t, c, r, "foo", flags, []byte("foo2"))

		if pi1 != 1 || pi2 != 2 {
			t.Fatalf("Unexpected pi values: %v, %v", pi1, pi2)
		}
	}
	// Ack first message
	testMQTTSendPubAck(t, c, 1)
	// Redelivery should only be for second message now
	for i := 0; i < 2; i++ {
		flags := mqttPubQos1 | mqttPubFlagDup
		pi := testMQTTCheckPubMsgNoAck(t, c, r, "foo", flags, []byte("foo2"))
		if pi != 2 {
			t.Fatalf("Unexpected pi to be 2, got %v", pi)
		}
	}

	// Restart client, should receive second message with pi==2
	c.Close()
	c, r = testMQTTConnect(t, cisub, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, true)
	// Check that message is received with proper pi
	pi := testMQTTCheckPubMsgNoAck(t, c, r, "foo", mqttPubQos1|mqttPubFlagDup, []byte("foo2"))
	if pi != 2 {
		t.Fatalf("Unexpected pi to be 2, got %v", pi)
	}
	// Now ack second message
	testMQTTSendPubAck(t, c, 2)
	// Flush to make sure it is processed before checking client's maps
	testMQTTFlush(t, c, nil, r)

	// Look for the sub client
	mc := testMQTTGetClient(t, s, "sub")
	mc.mu.Lock()
	sess := mc.mqtt.sess
	sess.mu.Lock()
	lpi := len(sess.pending)
	var lsseq int
	for _, sseqToPi := range sess.cpending {
		lsseq += len(sseqToPi)
	}
	sess.mu.Unlock()
	mc.mu.Unlock()
	if lpi != 0 || lsseq != 0 {
		t.Fatalf("Maps should be empty, got %v, %v", lpi, lsseq)
	}
}

func TestMQTTAckWaitConfigChange(t *testing.T) {
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = 250 * time.Millisecond
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownRestartedServer(&s)

	dir := strings.TrimSuffix(s.JetStreamConfig().StoreDir, JetStreamStoreDir)

	cisub := &mqttConnInfo{clientID: "sub", cleanSess: false}
	c, r := testMQTTConnect(t, cisub, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "foo", qos: 1}}, []byte{1})

	sendMsg := func(topic, payload string) {
		t.Helper()
		cipub := &mqttConnInfo{clientID: "pub", cleanSess: true}
		cp, rp := testMQTTConnect(t, cipub, o.MQTT.Host, o.MQTT.Port)
		defer cp.Close()
		testMQTTCheckConnAck(t, rp, mqttConnAckRCConnectionAccepted, false)

		testMQTTPublish(t, cp, rp, 1, false, false, topic, 1, []byte(payload))
		testMQTTDisconnect(t, cp, nil)
		cp.Close()
	}
	sendMsg("foo", "msg1")

	for i := 0; i < 2; i++ {
		flags := mqttPubQos1
		if i > 0 {
			flags |= mqttPubFlagDup
		}
		testMQTTCheckPubMsgNoAck(t, c, r, "foo", flags, []byte("msg1"))
	}

	// Restart the server with a different AckWait option value.
	// Verify that MQTT sub restart succeeds. It will keep the
	// original value.
	c.Close()
	s.Shutdown()

	o.Port = -1
	o.MQTT.Port = -1
	o.MQTT.AckWait = 10 * time.Millisecond
	o.StoreDir = dir
	s = testMQTTRunServer(t, o)
	// There is already the defer for shutdown at top of function

	c, r = testMQTTConnect(t, cisub, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, true)
	testMQTTCheckPubMsgNoAck(t, c, r, "foo", mqttPubQos1|mqttPubFlagDup, []byte("msg1"))
	start := time.Now()
	testMQTTCheckPubMsgNoAck(t, c, r, "foo", mqttPubQos1|mqttPubFlagDup, []byte("msg1"))
	if dur := time.Since(start); dur < 200*time.Millisecond {
		t.Fatalf("AckWait seem to have changed for existing subscription: %v", dur)
	}

	// Create new subscription
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "bar", qos: 1}}, []byte{1})
	sendMsg("bar", "msg2")
	testMQTTCheckPubMsgNoAck(t, c, r, "bar", mqttPubQos1, []byte("msg2"))
	start = time.Now()
	testMQTTCheckPubMsgNoAck(t, c, r, "bar", mqttPubQos1|mqttPubFlagDup, []byte("msg2"))
	if dur := time.Since(start); dur > 50*time.Millisecond {
		t.Fatalf("AckWait new value not used by new sub: %v", dur)
	}
	c.Close()
}

func TestMQTTUnsubscribeWithPendingAcks(t *testing.T) {
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = 250 * time.Millisecond
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	cisub := &mqttConnInfo{clientID: "sub", cleanSess: false}
	c, r := testMQTTConnect(t, cisub, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "foo", qos: 1}}, []byte{1})

	cipub := &mqttConnInfo{clientID: "pub", cleanSess: true}
	cp, rp := testMQTTConnect(t, cipub, o.MQTT.Host, o.MQTT.Port)
	defer cp.Close()
	testMQTTCheckConnAck(t, rp, mqttConnAckRCConnectionAccepted, false)

	testMQTTPublish(t, cp, rp, 1, false, false, "foo", 1, []byte("msg"))
	testMQTTDisconnect(t, cp, nil)
	cp.Close()

	for i := 0; i < 2; i++ {
		flags := mqttPubQos1
		if i > 0 {
			flags |= mqttPubFlagDup
		}
		testMQTTCheckPubMsgNoAck(t, c, r, "foo", flags, []byte("msg"))
	}

	testMQTTUnsub(t, 1, c, r, []*mqttFilter{{filter: "foo"}})
	testMQTTFlush(t, c, nil, r)

	mc := testMQTTGetClient(t, s, "sub")
	mc.mu.Lock()
	sess := mc.mqtt.sess
	sess.mu.Lock()
	pal := len(sess.pending)
	sess.mu.Unlock()
	mc.mu.Unlock()
	if pal != 0 {
		t.Fatalf("Expected pending ack map to be empty, got %v", pal)
	}
}

func TestMQTTMaxAckPending(t *testing.T) {
	o := testMQTTDefaultOptions()
	o.MQTT.MaxAckPending = 1
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownRestartedServer(&s)

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	dir := strings.TrimSuffix(s.JetStreamConfig().StoreDir, JetStreamStoreDir)

	cisub := &mqttConnInfo{clientID: "sub", cleanSess: false}
	c, r := testMQTTConnect(t, cisub, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "foo", qos: 1}}, []byte{1})

	cipub := &mqttConnInfo{clientID: "pub", cleanSess: true}
	cp, rp := testMQTTConnect(t, cipub, o.MQTT.Host, o.MQTT.Port)
	defer cp.Close()
	testMQTTCheckConnAck(t, rp, mqttConnAckRCConnectionAccepted, false)

	testMQTTPublish(t, cp, rp, 1, false, false, "foo", 1, []byte("msg1"))
	testMQTTPublish(t, cp, rp, 1, false, false, "foo", 1, []byte("msg2"))

	pi := testMQTTCheckPubMsgNoAck(t, c, r, "foo", mqttPubQos1, []byte("msg1"))
	// Check that we don't receive the second one due to max ack pending
	testMQTTExpectNothing(t, r)

	// Now ack first message
	testMQTTSendPubAck(t, c, pi)
	// Now we should receive message 2
	testMQTTCheckPubMsg(t, c, r, "foo", mqttPubQos1, []byte("msg2"))
	testMQTTDisconnect(t, c, nil)

	// Give a chance to the server to "close" the consumer
	checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
		for ci := range js.ConsumersInfo("$MQTT_msgs") {
			if ci.PushBound {
				return fmt.Errorf("Consumer still connected")
			}
		}
		return nil
	})

	// Send 2 messages while sub is offline
	testMQTTPublish(t, cp, rp, 1, false, false, "foo", 1, []byte("msg3"))
	testMQTTPublish(t, cp, rp, 1, false, false, "foo", 1, []byte("msg4"))

	// Restart consumer
	c, r = testMQTTConnect(t, cisub, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, true)

	// Should receive only message 3
	pi = testMQTTCheckPubMsgNoAck(t, c, r, "foo", mqttPubQos1, []byte("msg3"))
	testMQTTExpectNothing(t, r)

	// Ack and get the next
	testMQTTSendPubAck(t, c, pi)
	testMQTTCheckPubMsg(t, c, r, "foo", mqttPubQos1, []byte("msg4"))

	// Make sure this message gets ack'ed
	mcli := testMQTTGetClient(t, s, cisub.clientID)
	checkFor(t, time.Second, 15*time.Millisecond, func() error {
		mcli.mu.Lock()
		sess := mcli.mqtt.sess
		sess.mu.Lock()
		np := len(sess.pending)
		sess.mu.Unlock()
		mcli.mu.Unlock()
		if np != 0 {
			return fmt.Errorf("Still %v pending messages", np)
		}
		return nil
	})

	// Check that change to config does not prevent restart of sub.
	cp.Close()
	c.Close()
	s.Shutdown()

	o.Port = -1
	o.MQTT.Port = -1
	o.MQTT.MaxAckPending = 2
	o.StoreDir = dir
	s = testMQTTRunServer(t, o)
	// There is already the defer for shutdown at top of function

	cp, rp = testMQTTConnect(t, cipub, o.MQTT.Host, o.MQTT.Port)
	defer cp.Close()
	testMQTTCheckConnAck(t, rp, mqttConnAckRCConnectionAccepted, false)

	testMQTTPublish(t, cp, rp, 1, false, false, "foo", 1, []byte("msg5"))
	testMQTTPublish(t, cp, rp, 1, false, false, "foo", 1, []byte("msg6"))

	// Restart consumer
	c, r = testMQTTConnect(t, cisub, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, true)

	// Should receive only message 5
	pi = testMQTTCheckPubMsgNoAck(t, c, r, "foo", mqttPubQos1, []byte("msg5"))
	testMQTTExpectNothing(t, r)

	// Ack and get the next
	testMQTTSendPubAck(t, c, pi)
	testMQTTCheckPubMsg(t, c, r, "foo", mqttPubQos1, []byte("msg6"))
}

func TestMQTTMaxAckPendingForMultipleSubs(t *testing.T) {
	o := testMQTTDefaultOptions()
	o.MQTT.AckWait = time.Second
	o.MQTT.MaxAckPending = 1
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	cisub := &mqttConnInfo{clientID: "sub", cleanSess: true}
	c, r := testMQTTConnect(t, cisub, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "foo", qos: 1}}, []byte{1})
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "bar", qos: 1}}, []byte{1})

	cipub := &mqttConnInfo{clientID: "pub", cleanSess: true}
	cp, rp := testMQTTConnect(t, cipub, o.MQTT.Host, o.MQTT.Port)
	defer cp.Close()
	testMQTTCheckConnAck(t, rp, mqttConnAckRCConnectionAccepted, false)

	testMQTTPublish(t, cp, rp, 1, false, false, "foo", 1, []byte("msg1"))
	pi := testMQTTCheckPubMsgNoAck(t, c, r, "foo", mqttPubQos1, []byte("msg1"))

	// Now send a second message but on topic bar
	testMQTTPublish(t, cp, rp, 1, false, false, "bar", 1, []byte("msg2"))

	// JS allows us to limit per consumer, but we apply the limit to the
	// session, so although JS will attempt to delivery this message,
	// the MQTT code will suppress it.
	testMQTTExpectNothing(t, r)

	// Ack the first message.
	testMQTTSendPubAck(t, c, pi)

	// Now we should get the second message
	testMQTTCheckPubMsg(t, c, r, "bar", mqttPubQos1|mqttPubFlagDup, []byte("msg2"))
}

func TestMQTTMaxAckPendingOverLimit(t *testing.T) {
	o := testMQTTDefaultOptions()
	o.MQTT.MaxAckPending = 20000
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	checkTMax := func(sess *mqttSession, expected int) {
		t.Helper()
		sess.mu.Lock()
		tmax := sess.tmaxack
		sess.mu.Unlock()
		if tmax != expected {
			t.Fatalf("Expected current tmax to be %v, got %v", expected, tmax)
		}
	}

	cisub := &mqttConnInfo{clientID: "sub", cleanSess: false}
	c, r := testMQTTConnect(t, cisub, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

	mc := testMQTTGetClient(t, s, "sub")
	mc.mu.Lock()
	sess := mc.mqtt.sess
	mc.mu.Unlock()

	// After this one, total would be 20000
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "foo", qos: 1}}, []byte{1})
	checkTMax(sess, 20000)
	// This one will count for 2, so total will be 60000
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "bar/#", qos: 1}}, []byte{1})
	checkTMax(sess, 60000)

	// This should fail
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "bar", qos: 1}}, []byte{mqttSubAckFailure})
	checkTMax(sess, 60000)

	// Remove the one with wildcard
	testMQTTUnsub(t, 1, c, r, []*mqttFilter{{filter: "bar/#"}})
	checkTMax(sess, 20000)

	// Now we could add 2 more without wildcards
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "bar", qos: 1}}, []byte{1})
	checkTMax(sess, 40000)
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "baz", qos: 1}}, []byte{1})
	checkTMax(sess, 60000)

	// Again, this one should fail
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "bat", qos: 1}}, []byte{mqttSubAckFailure})
	checkTMax(sess, 60000)

	// Now remove all and check that we are at 0
	testMQTTUnsub(t, 1, c, r, []*mqttFilter{{filter: "foo"}})
	checkTMax(sess, 40000)
	testMQTTUnsub(t, 1, c, r, []*mqttFilter{{filter: "bar"}})
	checkTMax(sess, 20000)
	testMQTTUnsub(t, 1, c, r, []*mqttFilter{{filter: "baz"}})
	checkTMax(sess, 0)
}

func TestMQTTConfigReload(t *testing.T) {
	template := `
		jetstream: true
		server_name: mqtt
		mqtt {
			port: -1
			ack_wait: %s
			max_ack_pending: %s
		}
	`
	conf := createConfFile(t, []byte(fmt.Sprintf(template, `"5s"`, `10000`)))

	s, o := RunServerWithConfig(conf)
	defer testMQTTShutdownServer(s)

	if val := o.MQTT.AckWait; val != 5*time.Second {
		t.Fatalf("Invalid ackwait: %v", val)
	}
	if val := o.MQTT.MaxAckPending; val != 10000 {
		t.Fatalf("Invalid ackwait: %v", val)
	}

	changeCurrentConfigContentWithNewContent(t, conf, []byte(fmt.Sprintf(template, `"250ms"`, `1`)))
	if err := s.Reload(); err != nil {
		t.Fatalf("Error on reload: %v", err)
	}

	cisub := &mqttConnInfo{clientID: "sub", cleanSess: false}
	c, r := testMQTTConnect(t, cisub, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "foo", qos: 1}}, []byte{1})

	cipub := &mqttConnInfo{clientID: "pub", cleanSess: true}
	cp, rp := testMQTTConnect(t, cipub, o.MQTT.Host, o.MQTT.Port)
	defer cp.Close()
	testMQTTCheckConnAck(t, rp, mqttConnAckRCConnectionAccepted, false)

	testMQTTPublish(t, cp, rp, 1, false, false, "foo", 1, []byte("msg1"))
	testMQTTPublish(t, cp, rp, 1, false, false, "foo", 1, []byte("msg2"))

	testMQTTCheckPubMsgNoAck(t, c, r, "foo", mqttPubQos1, []byte("msg1"))
	start := time.Now()
	testMQTTCheckPubMsgNoAck(t, c, r, "foo", mqttPubQos1|mqttPubFlagDup, []byte("msg1"))
	if dur := time.Since(start); dur > 500*time.Millisecond {
		t.Fatalf("AckWait not applied? dur=%v", dur)
	}
	c.Close()
	cp.Close()
	testMQTTShutdownServer(s)

	changeCurrentConfigContentWithNewContent(t, conf, []byte(fmt.Sprintf(template, `"30s"`, `1`)))
	s, o = RunServerWithConfig(conf)
	defer testMQTTShutdownServer(s)

	cisub.cleanSess = true
	c, r = testMQTTConnect(t, cisub, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "foo", qos: 1}}, []byte{1})

	cipub = &mqttConnInfo{clientID: "pub", cleanSess: true}
	cp, rp = testMQTTConnect(t, cipub, o.MQTT.Host, o.MQTT.Port)
	defer cp.Close()
	testMQTTCheckConnAck(t, rp, mqttConnAckRCConnectionAccepted, false)

	testMQTTPublish(t, cp, rp, 1, false, false, "foo", 1, []byte("msg1"))
	testMQTTPublish(t, cp, rp, 1, false, false, "foo", 1, []byte("msg2"))

	testMQTTCheckPubMsgNoAck(t, c, r, "foo", mqttPubQos1, []byte("msg1"))
	testMQTTExpectNothing(t, r)

	// Increase the max ack pending
	changeCurrentConfigContentWithNewContent(t, conf, []byte(fmt.Sprintf(template, `"30s"`, `10`)))
	// Reload now
	if err := s.Reload(); err != nil {
		t.Fatalf("Error on reload: %v", err)
	}

	// Reload will have effect only on new subscriptions.
	// Create a new subscription, and we should not be able to get the 2 messages.
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "bar", qos: 1}}, []byte{1})

	testMQTTPublish(t, cp, rp, 1, false, false, "bar", 1, []byte("msg3"))
	testMQTTPublish(t, cp, rp, 1, false, false, "bar", 1, []byte("msg4"))

	testMQTTCheckPubMsg(t, c, r, "bar", mqttPubQos1, []byte("msg3"))
	testMQTTCheckPubMsg(t, c, r, "bar", mqttPubQos1, []byte("msg4"))
}

func TestMQTTStreamInfoReturnsNonEmptySubject(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	cisub := &mqttConnInfo{clientID: "sub", cleanSess: false}
	c, r := testMQTTConnect(t, cisub, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

	nc := natsConnect(t, s.ClientURL())
	defer nc.Close()

	// Check that we can query all MQTT streams. MQTT streams are
	// created without subject filter, however, if we return them like this,
	// the 'nats' utility will fail to display them due to some xml validation.
	for _, sname := range []string{
		mqttStreamName,
		mqttRetainedMsgsStreamName,
	} {
		t.Run(sname, func(t *testing.T) {
			resp, err := nc.Request(fmt.Sprintf(JSApiStreamInfoT, sname), nil, time.Second)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			var bResp JSApiStreamInfoResponse
			if err = json.Unmarshal(resp.Data, &bResp); err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if len(bResp.Config.Subjects) == 0 {
				t.Fatalf("No subject returned, which will cause nats tooling to fail: %+v", bResp.Config)
			}
		})
	}
}

func TestMQTTWebsocketToMQTTPort(t *testing.T) {
	conf := createConfFile(t, []byte(`
		listen: "127.0.0.1:-1"
		http: "127.0.0.1:-1"
		server_name: "mqtt"
		jetstream: enabled
		mqtt {
			listen: "127.0.0.1:-1"
		}
		websocket {
			listen: "127.0.0.1:-1"
			no_tls: true
		}
	`))
	s, o := RunServerWithConfig(conf)
	defer testMQTTShutdownServer(s)

	l := &captureErrorLogger{errCh: make(chan string, 10)}
	s.SetLogger(l, false, false)

	ci := &mqttConnInfo{cleanSess: true, ws: true}
	if _, _, err := testMQTTConnectRetryWithError(t, ci, o.MQTT.Host, o.MQTT.Port, 0); err == nil {
		t.Fatal("Expected error during connect")
	}
	select {
	case e := <-l.errCh:
		if !strings.Contains(e, errMQTTNotWebsocketPort.Error()) {
			t.Fatalf("Unexpected error: %v", e)
		}
	case <-time.After(time.Second):
		t.Fatal("No error regarding wrong port")
	}
}

func TestMQTTWebsocket(t *testing.T) {
	template := `
		listen: "127.0.0.1:-1"
		http: "127.0.0.1:-1"
		server_name: "mqtt"
		jetstream: enabled
		accounts {
			MQTT {
				jetstream: enabled
				users [
					{user: "mqtt", pass: "pwd", connection_types: ["%s"%s]}
				]
			}
		}
		mqtt {
			listen: "127.0.0.1:-1"
		}
		websocket {
			listen: "127.0.0.1:-1"
			no_tls: true
		}
	`
	s, o, conf := runReloadServerWithContent(t, []byte(fmt.Sprintf(template, jwt.ConnectionTypeMqtt, "")))
	defer testMQTTShutdownServer(s)

	cisub := &mqttConnInfo{clientID: "sub", user: "mqtt", pass: "pwd", ws: true}
	c, r := testMQTTConnect(t, cisub, o.Websocket.Host, o.Websocket.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCNotAuthorized, false)
	c.Close()

	ws := fmt.Sprintf(`, "%s"`, jwt.ConnectionTypeMqttWS)
	reloadUpdateConfig(t, s, conf, fmt.Sprintf(template, jwt.ConnectionTypeMqtt, ws))

	cisub = &mqttConnInfo{clientID: "sub", user: "mqtt", pass: "pwd", ws: true}
	c, r = testMQTTConnect(t, cisub, o.Websocket.Host, o.Websocket.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "foo", qos: 1}}, []byte{1})
	testMQTTFlush(t, c, nil, r)

	cipub := &mqttConnInfo{clientID: "pub", user: "mqtt", pass: "pwd", ws: true}
	cp, rp := testMQTTConnect(t, cipub, o.Websocket.Host, o.Websocket.Port)
	defer cp.Close()
	testMQTTCheckConnAck(t, rp, mqttConnAckRCConnectionAccepted, false)

	testMQTTPublish(t, cp, rp, 1, false, false, "foo", 1, []byte("msg1"))
	testMQTTCheckPubMsg(t, c, r, "foo", mqttPubQos1, []byte("msg1"))
}

type chunkWriteConn struct {
	net.Conn
}

func (cwc *chunkWriteConn) Write(p []byte) (int, error) {
	max := len(p)
	cs := rand.Intn(max) + 1
	if cs < max {
		if pn, perr := cwc.Conn.Write(p[:cs]); perr != nil {
			return pn, perr
		}
		time.Sleep(10 * time.Millisecond)
		if pn, perr := cwc.Conn.Write(p[cs:]); perr != nil {
			return pn, perr
		}
		return len(p), nil
	}
	return cwc.Conn.Write(p)
}

func TestMQTTPartial(t *testing.T) {
	conf := createConfFile(t, []byte(`
		listen: "127.0.0.1:-1"
		http: "127.0.0.1:-1"
		server_name: "mqtt"
		jetstream: enabled
		mqtt {
			listen: "127.0.0.1:-1"
		}
		websocket {
			listen: "127.0.0.1:-1"
			no_tls: true
		}
	`))
	s, o := RunServerWithConfig(conf)
	defer testMQTTShutdownServer(s)

	for _, test := range []struct {
		name string
		ws   bool
	}{
		{"standard", false},
		{"websocket", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			ci := &mqttConnInfo{cleanSess: true, ws: test.ws}
			host, port := o.MQTT.Host, o.MQTT.Port
			if test.ws {
				host, port = o.Websocket.Host, o.Websocket.Port
			}
			c, r := testMQTTConnect(t, ci, host, port)
			defer c.Close()
			testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
			c = &chunkWriteConn{Conn: c}

			cp, rp := testMQTTConnect(t, ci, host, port)
			defer cp.Close()
			testMQTTCheckConnAck(t, rp, mqttConnAckRCConnectionAccepted, false)
			cp = &chunkWriteConn{Conn: cp}

			subj := nuid.Next()
			testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: subj, qos: 1}}, []byte{1})
			testMQTTFlush(t, c, nil, r)
			for i := 0; i < 10; i++ {
				testMQTTPublish(t, cp, rp, 1, false, false, subj, 1, []byte("msg"))
				testMQTTCheckPubMsg(t, c, r, subj, mqttPubQos1, []byte("msg"))
			}
		})
	}
}

func TestMQTTWebsocketTLS(t *testing.T) {
	conf := createConfFile(t, []byte(`
		listen: "127.0.0.1:-1"
		http: "127.0.0.1:-1"
		server_name: "mqtt"
		jetstream: enabled
		mqtt {
			listen: "127.0.0.1:-1"
		}
		websocket {
			listen: "127.0.0.1:-1"
			tls {
				cert_file: '../test/configs/certs/server-cert.pem'
				key_file: '../test/configs/certs/server-key.pem'
				ca_file: '../test/configs/certs/ca.pem'
			}
		}
	`))
	s, o := RunServerWithConfig(conf)
	defer testMQTTShutdownServer(s)

	c, r := testMQTTConnect(t, &mqttConnInfo{clientID: "sub", ws: true, tls: true}, o.Websocket.Host, o.Websocket.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "foo", qos: 1}}, []byte{1})
	testMQTTFlush(t, c, nil, r)

	cp, rp := testMQTTConnect(t, &mqttConnInfo{clientID: "pub", ws: true, tls: true}, o.Websocket.Host, o.Websocket.Port)
	defer cp.Close()
	testMQTTCheckConnAck(t, rp, mqttConnAckRCConnectionAccepted, false)

	testMQTTPublish(t, cp, rp, 1, false, false, "foo", 1, []byte("msg1"))
	testMQTTCheckPubMsg(t, c, r, "foo", mqttPubQos1, []byte("msg1"))
}

func TestMQTTTransferSessionStreamsToMuxed(t *testing.T) {
	cl := createJetStreamClusterWithTemplate(t, testMQTTGetClusterTemplaceNoLeaf(), "MQTT", 3)
	defer cl.shutdown()

	nc, js := jsClientConnect(t, cl.randomServer())
	defer nc.Close()

	// Create 2 streams that start with "$MQTT_sess_" to check for transfer to new
	// mux'ed unique "$MQTT_sess" stream. One of this stream will not contain a
	// proper session record, and we will check that the stream does not get deleted.
	sessStreamName1 := mqttSessionsStreamNamePrefix + getHash("sub")
	if _, err := js.AddStream(&nats.StreamConfig{
		Name:     sessStreamName1,
		Subjects: []string{sessStreamName1},
		Replicas: 3,
		MaxMsgs:  1,
	}); err != nil {
		t.Fatalf("Unable to add stream: %v", err)
	}
	// Then add the session record
	ps := mqttPersistedSession{
		ID:   "sub",
		Subs: map[string]byte{"foo": 1},
		Cons: map[string]*ConsumerConfig{"foo": {
			Durable:        "d6INCtp3_cK39H5WHEtOSU7sLy2oQv3",
			DeliverSubject: "$MQTT.sub.cK39H5WHEtOSU7sLy2oQrR",
			DeliverPolicy:  DeliverNew,
			AckPolicy:      AckExplicit,
			FilterSubject:  "$MQTT.msgs.foo",
			MaxAckPending:  1024,
		}},
	}
	b, _ := json.Marshal(&ps)
	if _, err := js.Publish(sessStreamName1, b); err != nil {
		t.Fatalf("Error on publish: %v", err)
	}

	// Create the stream that has "$MQTT_sess_" prefix, but that is not really a MQTT session stream
	sessStreamName2 := mqttSessionsStreamNamePrefix + "ivan"
	if _, err := js.AddStream(&nats.StreamConfig{
		Name:     sessStreamName2,
		Subjects: []string{sessStreamName2},
		Replicas: 3,
		MaxMsgs:  1,
	}); err != nil {
		t.Fatalf("Unable to add stream: %v", err)
	}
	if _, err := js.Publish(sessStreamName2, []byte("some content")); err != nil {
		t.Fatalf("Error on publish: %v", err)
	}

	cl.waitOnStreamLeader(globalAccountName, sessStreamName1)
	cl.waitOnStreamLeader(globalAccountName, sessStreamName2)

	// Now create a real MQTT connection
	o := cl.opts[0]
	sc, sr := testMQTTConnectRetry(t, &mqttConnInfo{clientID: "sub"}, o.MQTT.Host, o.MQTT.Port, 10)
	defer sc.Close()
	testMQTTCheckConnAck(t, sr, mqttConnAckRCConnectionAccepted, true)

	// Check that old session stream is gone, but the non session stream is still present.
	var gotIt = false
	for info := range js.StreamsInfo() {
		if strings.HasPrefix(info.Config.Name, mqttSessionsStreamNamePrefix) {
			if strings.HasSuffix(info.Config.Name, "_ivan") {
				gotIt = true
			} else {
				t.Fatalf("The stream %q should have been deleted", info.Config.Name)
			}
		}
	}
	if !gotIt {
		t.Fatalf("The stream %q should not have been deleted", mqttSessionsStreamNamePrefix+"ivan")
	}

	// We want to check that the record was properly transferred.
	rmsg, err := js.GetMsg(mqttSessStreamName, 2)
	if err != nil {
		t.Fatalf("Unable to get session message: %v", err)
	}
	ps2 := &mqttPersistedSession{}
	if err := json.Unmarshal(rmsg.Data, ps2); err != nil {
		t.Fatalf("Error unpacking session record: %v", err)
	}
	if ps2.ID != "sub" {
		t.Fatalf("Unexpected session record, %+v vs %+v", ps2, ps)
	}
	if qos, ok := ps2.Subs["foo"]; !ok || qos != 1 {
		t.Fatalf("Unexpected session record, %+v vs %+v", ps2, ps)
	}
	if cons, ok := ps2.Cons["foo"]; !ok || !reflect.DeepEqual(cons, ps.Cons["foo"]) {
		t.Fatalf("Unexpected session record, %+v vs %+v", ps2, ps)
	}

	// Make sure we don't attempt to transfer again by creating a subscription
	// on the "stream names" API, which is used to get the list of streams to transfer
	sub := natsSubSync(t, nc, JSApiStreams)

	// Make sure to connect an MQTT client from a different node so that this node
	// gets a connection for the account for the first time and tries to create
	// all MQTT streams, etc..
	o = cl.opts[1]
	sc, sr = testMQTTConnectRetry(t, &mqttConnInfo{clientID: "sub2"}, o.MQTT.Host, o.MQTT.Port, 10)
	defer sc.Close()
	testMQTTCheckConnAck(t, sr, mqttConnAckRCConnectionAccepted, false)

	if _, err := sub.NextMsg(200 * time.Millisecond); err == nil {
		t.Fatal("Looks like attempt to transfer was done again")
	}
}

func TestMQTTConnectAndDisconnectEvent(t *testing.T) {
	conf := createConfFile(t, []byte(`
		listen: "127.0.0.1:-1"
		http: "127.0.0.1:-1"
		server_name: "mqtt"
		jetstream: enabled
		accounts {
			MQTT {
				jetstream: enabled
				users: [{user: "mqtt", password: "pwd"}]
			}
			SYS {
				users: [{user: "sys", password: "pwd"}]
			}
		}
		mqtt {
			listen: "127.0.0.1:-1"
		}
		system_account: "SYS"
	`))
	defer os.Remove(conf)
	s, o := RunServerWithConfig(conf)
	defer testMQTTShutdownServer(s)

	nc := natsConnect(t, s.ClientURL(), nats.UserInfo("sys", "pwd"))
	defer nc.Close()

	accConn := natsSubSync(t, nc, fmt.Sprintf(connectEventSubj, "MQTT"))
	accDisc := natsSubSync(t, nc, fmt.Sprintf(disconnectEventSubj, "MQTT"))
	accAuth := natsSubSync(t, nc, fmt.Sprintf(authErrorEventSubj, s.ID()))
	natsFlush(t, nc)

	checkConnEvent := func(data []byte, expected string) {
		t.Helper()
		var ce ConnectEventMsg
		json.Unmarshal(data, &ce)
		if ce.Client.MQTTClient != expected {
			t.Fatalf("Expected client ID %q, got this connect event: %+v", expected, ce)
		}
	}
	checkDiscEvent := func(data []byte, expected string) {
		t.Helper()
		var de DisconnectEventMsg
		json.Unmarshal(data, &de)
		if de.Client.MQTTClient != expected {
			t.Fatalf("Expected client ID %q, got this disconnect event: %+v", expected, de)
		}
	}

	c1, r1 := testMQTTConnect(t, &mqttConnInfo{user: "mqtt", pass: "pwd", clientID: "conn1", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer c1.Close()
	testMQTTCheckConnAck(t, r1, mqttConnAckRCConnectionAccepted, false)

	cm := natsNexMsg(t, accConn, time.Second)
	checkConnEvent(cm.Data, "conn1")

	c2, r2 := testMQTTConnect(t, &mqttConnInfo{user: "mqtt", pass: "pwd", clientID: "conn2", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, false)

	cm = natsNexMsg(t, accConn, time.Second)
	checkConnEvent(cm.Data, "conn2")

	c3, r3 := testMQTTConnect(t, &mqttConnInfo{user: "mqtt", pass: "pwd", clientID: "conn3", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer c3.Close()
	testMQTTCheckConnAck(t, r3, mqttConnAckRCConnectionAccepted, false)

	cm = natsNexMsg(t, accConn, time.Second)
	checkConnEvent(cm.Data, "conn3")

	testMQTTDisconnect(t, c3, nil)
	cm = natsNexMsg(t, accDisc, time.Second)
	checkDiscEvent(cm.Data, "conn3")

	// Now try a bad auth
	c4, r4 := testMQTTConnect(t, &mqttConnInfo{clientID: "conn4", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer c4.Close()
	testMQTTCheckConnAck(t, r4, mqttConnAckRCNotAuthorized, false)
	// This will generate an auth error, which is a disconnect event
	cm = natsNexMsg(t, accAuth, time.Second)
	checkDiscEvent(cm.Data, "conn4")

	url := fmt.Sprintf("http://127.0.0.1:%d/", s.MonitorAddr().Port)
	for mode := 0; mode < 2; mode++ {
		c := pollConz(t, s, mode, url+"connz", nil)
		if c.Conns == nil || len(c.Conns) != 3 {
			t.Fatalf("Expected 3 connections in array, got %v", len(c.Conns))
		}

		// Check that client ID is present
		for _, conn := range c.Conns {
			if conn.Type == clientTypeStringMap[MQTT] && conn.MQTTClient == _EMPTY_ {
				t.Fatalf("Expected a client ID to be set, got %+v", conn)
			}
		}

		// Check that we can select based on client ID:
		c = pollConz(t, s, mode, url+"connz?mqtt_client=conn2", &ConnzOptions{MQTTClient: "conn2"})
		if c.Conns == nil || len(c.Conns) != 1 {
			t.Fatalf("Expected 1 connection in array, got %v", len(c.Conns))
		}
		if c.Conns[0].MQTTClient != "conn2" {
			t.Fatalf("Unexpected client ID: %+v", c.Conns[0])
		}

		// Check that we have the closed ones
		c = pollConz(t, s, mode, url+"connz?state=closed", &ConnzOptions{State: ConnClosed})
		if c.Conns == nil || len(c.Conns) != 2 {
			t.Fatalf("Expected 2 connections in array, got %v", len(c.Conns))
		}
		for _, conn := range c.Conns {
			if conn.MQTTClient == _EMPTY_ {
				t.Fatalf("Expected a client ID, got %+v", conn)
			}
		}

		// Check that we can select with client ID for closed state
		c = pollConz(t, s, mode, url+"connz?state=closed&mqtt_client=conn3", &ConnzOptions{State: ConnClosed, MQTTClient: "conn3"})
		if c.Conns == nil || len(c.Conns) != 1 {
			t.Fatalf("Expected 1 connection in array, got %v", len(c.Conns))
		}
		if c.Conns[0].MQTTClient != "conn3" {
			t.Fatalf("Unexpected client ID: %+v", c.Conns[0])
		}
		// Check that we can select with client ID for closed state (but in this case not found)
		c = pollConz(t, s, mode, url+"connz?state=closed&mqtt_client=conn5", &ConnzOptions{State: ConnClosed, MQTTClient: "conn5"})
		if len(c.Conns) != 0 {
			t.Fatalf("Expected 0 connection in array, got %v", len(c.Conns))
		}
	}

	reply := nc.NewRespInbox()
	replySub := natsSubSync(t, nc, reply)

	// Test system events now
	for _, test := range []struct {
		opt interface{}
		cid string
	}{
		{&ConnzOptions{MQTTClient: "conn1"}, "conn1"},
		{&ConnzOptions{MQTTClient: "conn3", State: ConnClosed}, "conn3"},
		{&ConnzOptions{MQTTClient: "conn4", State: ConnClosed}, "conn4"},
		{&ConnzOptions{MQTTClient: "conn5"}, _EMPTY_},
		{json.RawMessage(`{"mqtt_client":"conn1"}`), "conn1"},
		{json.RawMessage(fmt.Sprintf(`{"mqtt_client":"conn3", "state":%v}`, ConnClosed)), "conn3"},
		{json.RawMessage(fmt.Sprintf(`{"mqtt_client":"conn4", "state":%v}`, ConnClosed)), "conn4"},
		{json.RawMessage(`{"mqtt_client":"conn5"}`), _EMPTY_},
	} {
		t.Run("sys connz", func(t *testing.T) {
			b, _ := json.Marshal(test.opt)

			// set a header to make sure request parsing knows to ignore them
			nc.PublishMsg(&nats.Msg{
				Subject: fmt.Sprintf("%s.CONNZ", serverStatsPingReqSubj),
				Reply:   reply,
				Data:    b,
			})

			msg := natsNexMsg(t, replySub, time.Second)
			var response ServerAPIResponse
			if err := json.Unmarshal(msg.Data, &response); err != nil {
				t.Fatalf("Error unmarshalling response json: %v", err)
			}
			tmp, _ := json.Marshal(response.Data)
			cz := &Connz{}
			if err := json.Unmarshal(tmp, cz); err != nil {
				t.Fatalf("Error unmarshalling connz: %v", err)
			}
			if test.cid == _EMPTY_ {
				if len(cz.Conns) != 0 {
					t.Fatalf("Expected no connections, got %v", len(cz.Conns))
				}
				return
			}
			if len(cz.Conns) != 1 {
				t.Fatalf("Expected single connection, got %v", len(cz.Conns))
			}
			conn := cz.Conns[0]
			if conn.MQTTClient != test.cid {
				t.Fatalf("Expected client ID %q, got %q", test.cid, conn.MQTTClient)
			}
		})
	}
}

func TestMQTTClientIDInLogStatements(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	l := &captureDebugLogger{dbgCh: make(chan string, 10)}
	s.SetLogger(l, true, false)

	cisub := &mqttConnInfo{clientID: "my_client_id", cleanSess: false}
	c, r := testMQTTConnect(t, cisub, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTDisconnect(t, c, nil)
	c.Close()

	tm := time.NewTimer(2 * time.Second)
	var connected bool
	var disconnected bool
	for {
		select {
		case dl := <-l.dbgCh:
			if strings.Contains(dl, "my_client_id") {
				if strings.Contains(dl, "Client connected") {
					connected = true
				} else if strings.Contains(dl, "Client connection closed") {
					disconnected = true
				}
				if connected && disconnected {
					// OK!
					return
				}
			}
		case <-tm.C:
			t.Fatal("Did not get the debug statements or client_id in them")
		}
	}
}

func TestMQTTStreamReplicasOverride(t *testing.T) {
	conf := `
		listen: 127.0.0.1:-1
		server_name: %s
		jetstream: {max_mem_store: 256MB, max_file_store: 2GB, store_dir: '%s'}

		cluster {
			name: %s
			listen: 127.0.0.1:%d
			routes = [%s]
		}

		mqtt {
			listen: 127.0.0.1:-1
			stream_replicas: 3
		}

		# For access to system account.
		accounts { $SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] } }
	`
	cl := createJetStreamClusterWithTemplate(t, conf, "MQTT", 3)
	defer cl.shutdown()

	connectAndCheck := func(restarted bool) {
		t.Helper()

		o := cl.opts[0]
		mc, r := testMQTTConnectRetry(t, &mqttConnInfo{clientID: "test", cleanSess: false}, o.MQTT.Host, o.MQTT.Port, 5)
		defer mc.Close()
		testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, restarted)

		nc, js := jsClientConnect(t, cl.servers[2])
		defer nc.Close()

		streams := []string{mqttStreamName, mqttRetainedMsgsStreamName, mqttSessStreamName}
		for _, sn := range streams {
			si, err := js.StreamInfo(sn)
			require_NoError(t, err)
			if n := len(si.Cluster.Replicas); n != 2 {
				t.Fatalf("Expected stream %q to have 2 replicas, got %v", sn, n)
			}
		}
	}
	connectAndCheck(false)

	cl.stopAll()
	for _, o := range cl.opts {
		o.MQTT.StreamReplicas = 2
	}
	cl.restartAllSamePorts()
	cl.waitOnStreamLeader(globalAccountName, mqttStreamName)
	cl.waitOnStreamLeader(globalAccountName, mqttRetainedMsgsStreamName)
	cl.waitOnStreamLeader(globalAccountName, mqttSessStreamName)

	l := &captureWarnLogger{warn: make(chan string, 10)}
	cl.servers[0].SetLogger(l, false, false)

	connectAndCheck(true)

	select {
	case w := <-l.warn:
		if !strings.Contains(w, "current is 3 but configuration is 2") {
			t.Fatalf("Unexpected warning: %q", w)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Should have warned against replicas mismatch")
	}
}

func TestMQTTStreamReplicasConfigReload(t *testing.T) {
	tmpl := `
		jetstream: enabled
		server_name: mqtt
		mqtt {
			port: -1
			stream_replicas: %v
		}
	`
	conf := createConfFile(t, []byte(fmt.Sprintf(tmpl, 3)))
	s, o := RunServerWithConfig(conf)
	defer testMQTTShutdownServer(s)

	l := &captureErrorLogger{errCh: make(chan string, 10)}
	s.SetLogger(l, false, false)

	_, _, err := testMQTTConnectRetryWithError(t, &mqttConnInfo{clientID: "mqtt", cleanSess: false}, o.MQTT.Host, o.MQTT.Port, 0)
	if err == nil {
		t.Fatal("Expected to fail, did not")
	}

	select {
	case e := <-l.errCh:
		if !strings.Contains(e, NewJSStreamReplicasNotSupportedError().Description) {
			t.Fatalf("Expected error regarding replicas, got %v", e)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("Did not get the error regarding replicas count")
	}

	reloadUpdateConfig(t, s, conf, fmt.Sprintf(tmpl, 1))

	mc, r := testMQTTConnect(t, &mqttConnInfo{clientID: "mqtt", cleanSess: false}, o.MQTT.Host, o.MQTT.Port)
	defer mc.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
}

func TestMQTTStreamReplicasInsufficientResources(t *testing.T) {
	conf := `
		listen: 127.0.0.1:-1
		server_name: %s
		jetstream: {max_mem_store: 256MB, max_file_store: 2GB, store_dir: '%s'}

		cluster {
			name: %s
			listen: 127.0.0.1:%d
			routes = [%s]
		}

		mqtt {
			listen: 127.0.0.1:-1
			stream_replicas: 5
		}

		# For access to system account.
		accounts { $SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] } }
	`
	cl := createJetStreamClusterWithTemplate(t, conf, "MQTT", 3)
	defer cl.shutdown()

	l := &captureErrorLogger{errCh: make(chan string, 10)}
	for _, s := range cl.servers {
		s.SetLogger(l, false, false)
	}

	o := cl.opts[1]
	_, _, err := testMQTTConnectRetryWithError(t, &mqttConnInfo{clientID: "mqtt", cleanSess: false}, o.MQTT.Host, o.MQTT.Port, 0)
	if err == nil {
		t.Fatal("Expected to fail, did not")
	}

	select {
	case e := <-l.errCh:
		if !strings.Contains(e, fmt.Sprintf("%d", NewJSClusterNoPeersError(errors.New("")).ErrCode)) {
			t.Fatalf("Expected error regarding no peers error, got %v", e)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("Did not get the error regarding replicas count")
	}
}

func TestMQTTConsumerReplicasValidate(t *testing.T) {
	o := testMQTTDefaultOptions()
	for _, test := range []struct {
		name string
		sr   int
		cr   int
		err  bool
	}{
		{"stream replicas neg", -1, 3, false},
		{"stream replicas 0", 0, 3, false},
		{"consumer replicas neg", 0, -1, false},
		{"consumer replicas 0", -1, 0, false},
		{"consumer replicas too high", 1, 2, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			o.MQTT.StreamReplicas = test.sr
			o.MQTT.ConsumerReplicas = test.cr
			err := validateMQTTOptions(o)
			if test.err {
				if err == nil {
					t.Fatal("Expected error, did not get one")
				}
				if !strings.Contains(err.Error(), "cannot be higher") {
					t.Fatalf("Unexpected error: %v", err)
				}
				// OK
				return
			} else if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
		})
	}
}

// This was ill-advised since the messages stream is currently interest policy.
// Interest policy streams require consumers match the replica count.
// Will leave her for now to make sure we do not override.
func TestMQTTConsumerReplicasOverride(t *testing.T) {
	conf := `
		listen: 127.0.0.1:-1
		server_name: %s
		jetstream: {max_mem_store: 256MB, max_file_store: 2GB, store_dir: '%s'}

		cluster {
			name: %s
			listen: 127.0.0.1:%d
			routes = [%s]
		}

		mqtt {
			listen: 127.0.0.1:-1
			stream_replicas: 5
			consumer_replicas: 1
			consumer_memory_storage: true
		}

		# For access to system account.
		accounts { $SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] } }
	`
	cl := createJetStreamClusterWithTemplate(t, conf, "MQTT", 5)
	defer cl.shutdown()

	connectAndCheck := func(subject string, restarted bool) {
		t.Helper()

		o := cl.opts[0]
		mc, r := testMQTTConnectRetry(t, &mqttConnInfo{clientID: "test", cleanSess: false}, o.MQTT.Host, o.MQTT.Port, 5)
		defer mc.Close()
		testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, restarted)
		testMQTTSub(t, 1, mc, r, []*mqttFilter{{filter: subject, qos: 1}}, []byte{1})

		nc, js := jsClientConnect(t, cl.servers[2])
		defer nc.Close()

		for ci := range js.ConsumersInfo(mqttStreamName) {
			if ci.Config.FilterSubject == mqttStreamSubjectPrefix+subject {
				if rf := len(ci.Cluster.Replicas) + 1; rf != 5 {
					t.Fatalf("Expected consumer to be R5, got: %d", rf)
				}
			}
		}
	}
	connectAndCheck("foo", false)

	cl.stopAll()
	for _, o := range cl.opts {
		o.MQTT.ConsumerReplicas = 2
		o.MQTT.ConsumerMemoryStorage = false
	}
	cl.restartAllSamePorts()
	cl.waitOnStreamLeader(globalAccountName, mqttStreamName)
	cl.waitOnStreamLeader(globalAccountName, mqttRetainedMsgsStreamName)
	cl.waitOnStreamLeader(globalAccountName, mqttSessStreamName)

	connectAndCheck("bar", true)
}

func TestMQTTConsumerMemStorageReload(t *testing.T) {
	tmpl := `
		jetstream: enabled
		server_name: mqtt
		mqtt {
			port: -1
			consumer_memory_storage: %s
		}
	`
	conf := createConfFile(t, []byte(fmt.Sprintf(tmpl, "false")))
	s, o := RunServerWithConfig(conf)
	defer testMQTTShutdownServer(s)

	l := &captureErrorLogger{errCh: make(chan string, 10)}
	s.SetLogger(l, false, false)

	c, r := testMQTTConnect(t, &mqttConnInfo{clientID: "sub", cleanSess: false}, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

	reloadUpdateConfig(t, s, conf, fmt.Sprintf(tmpl, "true"))

	testMQTTSub(t, 1, c, r, []*mqttFilter{{filter: "foo", qos: 1}}, []byte{1})

	mset, err := s.GlobalAccount().lookupStream(mqttStreamName)
	if err != nil {
		t.Fatalf("Error looking up stream: %v", err)
	}
	var cons *consumer
	mset.mu.RLock()
	for _, c := range mset.consumers {
		cons = c
		break
	}
	mset.mu.RUnlock()
	cons.mu.RLock()
	st := cons.store.Type()
	cons.mu.RUnlock()
	if st != MemoryStorage {
		t.Fatalf("Expected storage %v, got %v", MemoryStorage, st)
	}
}

type unableToDeleteConsLogger struct {
	DummyLogger
	errCh chan string
}

func (l *unableToDeleteConsLogger) Errorf(format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	if strings.Contains(msg, "unable to delete consumer") {
		l.errCh <- msg
	}
}

func TestMQTTSessionNotDeletedOnDeleteConsumerError(t *testing.T) {
	org := mqttJSAPITimeout
	mqttJSAPITimeout = 1000 * time.Millisecond
	defer func() { mqttJSAPITimeout = org }()

	cl := createJetStreamClusterWithTemplate(t, testMQTTGetClusterTemplaceNoLeaf(), "MQTT", 2)
	defer cl.shutdown()

	o := cl.opts[0]
	s1 := cl.servers[0]
	// Plug error logger to s1
	l := &unableToDeleteConsLogger{errCh: make(chan string, 10)}
	s1.SetLogger(l, false, false)

	nc, js := jsClientConnect(t, s1)
	defer nc.Close()

	mc, r := testMQTTConnectRetry(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port, 5)
	defer mc.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

	testMQTTSub(t, 1, mc, r, []*mqttFilter{{filter: "foo", qos: 1}}, []byte{1})
	testMQTTFlush(t, mc, nil, r)

	// Now shutdown server 2, we should lose quorum
	cl.servers[1].Shutdown()

	// Close the MQTT client:
	testMQTTDisconnect(t, mc, nil)

	// We should have reported that there was an error deleting the consumer
	select {
	case <-l.errCh:
		// OK
	case <-time.After(time.Second):
		t.Fatal("Server did not report any error")
	}

	// Now restart the server 2 so that we can check that the session is still persisted.
	cl.restartAllSamePorts()
	cl.waitOnStreamLeader(globalAccountName, mqttSessStreamName)

	si, err := js.StreamInfo(mqttSessStreamName)
	require_NoError(t, err)
	require_True(t, si.State.Msgs == 1)
}

// Test for auto-cleanup of consumers.
func TestMQTTConsumerInactiveThreshold(t *testing.T) {
	tmpl := `
		listen: 127.0.0.1:-1
		server_name: mqtt
		jetstream: enabled

		mqtt {
			listen: 127.0.0.1:-1
			consumer_inactive_threshold: %q
		}

		# For access to system account.
		accounts { $SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] } }
	`
	conf := createConfFile(t, []byte(fmt.Sprintf(tmpl, "0.2s")))
	s, o := RunServerWithConfig(conf)
	defer testMQTTShutdownServer(s)

	mc, r := testMQTTConnectRetry(t, &mqttConnInfo{clientID: "test", cleanSess: true}, o.MQTT.Host, o.MQTT.Port, 5)
	defer mc.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, mc, r, []*mqttFilter{{filter: "foo", qos: 1}}, []byte{1})

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	ci := <-js.ConsumersInfo("$MQTT_msgs")

	// Make sure we clean up this consumer.
	mc.Close()
	checkFor(t, 2*time.Second, 50*time.Millisecond, func() error {
		_, err := js.ConsumerInfo(ci.Stream, ci.Name)
		if err == nil {
			return fmt.Errorf("Consumer still present")
		}
		return nil
	})

	// Check reload.
	// We will not redo existing consumers however.
	reloadUpdateConfig(t, s, conf, fmt.Sprintf(tmpl, "22s"))
	if opts := s.getOpts(); opts.MQTT.ConsumerInactiveThreshold != 22*time.Second {
		t.Fatalf("Expected reloaded value of %v but got %v", 22*time.Second, opts.MQTT.ConsumerInactiveThreshold)
	}
}

func TestMQTTSubjectMapping(t *testing.T) {
	conf := createConfFile(t, []byte(`
		listen: 127.0.0.1:-1
		server_name: mqtt
		jetstream: enabled

		mappings = {
			foo0: bar0
			foo1: bar1
		}

		mqtt {
			listen: 127.0.0.1:-1
		}
	`))
	s, o := RunServerWithConfig(conf)
	defer testMQTTShutdownServer(s)

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	for _, qos := range []byte{0, 1} {
		t.Run(fmt.Sprintf("qos%d", qos), func(t *testing.T) {
			mc, r := testMQTTConnect(t, &mqttConnInfo{clientID: "sub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
			defer mc.Close()

			bar := fmt.Sprintf("bar%v", qos)
			foo := fmt.Sprintf("foo%v", qos)

			testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
			testMQTTSub(t, 1, mc, r, []*mqttFilter{{filter: bar, qos: qos}}, []byte{qos})
			testMQTTFlush(t, mc, nil, r)

			mcp, rp := testMQTTConnect(t, &mqttConnInfo{clientID: "pub", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
			defer mcp.Close()
			testMQTTCheckConnAck(t, rp, mqttConnAckRCConnectionAccepted, false)

			natsPub(t, nc, foo, []byte("msg0"))
			testMQTTCheckPubMsgNoAck(t, mc, r, bar, 0, []byte("msg0"))

			testMQTTPublish(t, mcp, rp, 0, false, false, foo, 0, []byte("msg1"))
			testMQTTCheckPubMsgNoAck(t, mc, r, bar, 0, []byte("msg1"))

			testMQTTPublish(t, mcp, rp, 0, false, true, foo, 0, []byte("msg1_retained"))
			testMQTTCheckPubMsgNoAck(t, mc, r, bar, 0, []byte("msg1_retained"))

			testMQTTPublish(t, mcp, rp, 1, false, false, foo, 1, []byte("msg2"))
			// For the receiving side, the expected QoS is based on the subscription's QoS,
			// not the publisher.
			expected := byte(0)
			if qos == 1 {
				expected = mqttPubQos1
			}
			testMQTTCheckPubMsgNoAck(t, mc, r, bar, expected, []byte("msg2"))

			testMQTTPublish(t, mcp, rp, 1, false, true, foo, 1, []byte("msg2_retained"))
			testMQTTCheckPubMsgNoAck(t, mc, r, bar, expected, []byte("msg2_retained"))

			testMQTTDisconnect(t, mcp, nil)

			// Try the with the "will" with QoS0 first
			mcp, rp = testMQTTConnect(t, &mqttConnInfo{
				clientID:  "pub",
				cleanSess: true,
				will:      &mqttWill{topic: []byte(foo), qos: 0, message: []byte("willmsg1")}},
				o.MQTT.Host, o.MQTT.Port)
			defer mcp.Close()
			testMQTTCheckConnAck(t, rp, mqttConnAckRCConnectionAccepted, false)
			testMQTTFlush(t, mcp, nil, rp)

			// Close the connection without proper disconnect for will to be sent
			mcp.Close()
			testMQTTCheckPubMsgNoAck(t, mc, r, bar, 0, []byte("willmsg1"))

			// Try the with the "will" with QoS1 now
			mcp, rp = testMQTTConnect(t, &mqttConnInfo{
				clientID:  "pub",
				cleanSess: true,
				will:      &mqttWill{topic: []byte(foo), qos: 1, message: []byte("willmsg2")}},
				o.MQTT.Host, o.MQTT.Port)
			defer mcp.Close()
			testMQTTCheckConnAck(t, rp, mqttConnAckRCConnectionAccepted, false)
			testMQTTFlush(t, mcp, nil, rp)

			// Close the connection without proper disconnect for will to be sent
			mcp.Close()
			testMQTTCheckPubMsgNoAck(t, mc, r, bar, expected, []byte("willmsg2"))

			si, err := js.StreamInfo("$MQTT_msgs")
			require_NoError(t, err)
			if qos == 0 {
				require_True(t, si.State.Msgs == 0)
				require_True(t, si.State.NumSubjects == 0)
			} else {
				// Number of QoS1 messages: 1 regular, 1 retained, 1 from will.
				require_True(t, si.State.Msgs == 3)
				require_True(t, si.State.NumSubjects == 1)
			}
			testMQTTDisconnect(t, mc, nil)
		})
	}
}

func TestMQTTSubjectMappingWithImportExport(t *testing.T) {
	conf := createConfFile(t, []byte(`
		listen: 127.0.0.1:-1
		server_name: mqtt
		jetstream: enabled

		accounts {
			A {
				users = [{user: "a", password: "pwd"}]
				mappings = {
					bar: foo
				}
				exports = [{service: "bar"}]
				jetstream: enabled
			}
			B {
				users = [{user: "b", password: "pwd"}]
				mappings = {
					foo: bar
				}
				imports = [{service: {account: "A", subject: "bar"}}]
				jetstream: enabled
			}
			$SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] }
		}

		mqtt {
			listen: 127.0.0.1:-1
		}
	`))
	s, o := RunServerWithConfig(conf)
	defer testMQTTShutdownServer(s)

	c1, r1 := testMQTTConnect(t, &mqttConnInfo{user: "a", pass: "pwd", clientID: "a1", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer c1.Close()
	testMQTTCheckConnAck(t, r1, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c1, r1, []*mqttFilter{{filter: "foo", qos: 0}}, []byte{0})
	testMQTTFlush(t, c1, nil, r1)

	c2, r2 := testMQTTConnect(t, &mqttConnInfo{user: "a", pass: "pwd", clientID: "a2", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c2, r2, []*mqttFilter{{filter: "foo", qos: 1}}, []byte{1})
	testMQTTFlush(t, c2, nil, r2)

	c3, r3 := testMQTTConnect(t, &mqttConnInfo{user: "a", pass: "pwd", clientID: "a3", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer c3.Close()
	testMQTTCheckConnAck(t, r3, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c3, r3, []*mqttFilter{{filter: "bar", qos: 0}}, []byte{0})
	testMQTTFlush(t, c3, nil, r3)

	c4, r4 := testMQTTConnect(t, &mqttConnInfo{user: "a", pass: "pwd", clientID: "a4", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer c4.Close()
	testMQTTCheckConnAck(t, r4, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c4, r4, []*mqttFilter{{filter: "bar", qos: 1}}, []byte{1})
	testMQTTFlush(t, c4, nil, r4)

	bc, br := testMQTTConnect(t, &mqttConnInfo{user: "b", pass: "pwd", clientID: "b0", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer bc.Close()
	testMQTTCheckConnAck(t, br, mqttConnAckRCConnectionAccepted, false)

	bc1, br1 := testMQTTConnect(t, &mqttConnInfo{user: "b", pass: "pwd", clientID: "b1", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer bc1.Close()
	testMQTTCheckConnAck(t, br1, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, bc1, br1, []*mqttFilter{{filter: "foo", qos: 0}}, []byte{0})
	testMQTTFlush(t, bc1, nil, br1)

	bc2, br2 := testMQTTConnect(t, &mqttConnInfo{user: "b", pass: "pwd", clientID: "b2", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer bc2.Close()
	testMQTTCheckConnAck(t, br2, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, bc2, br2, []*mqttFilter{{filter: "b", qos: 1}}, []byte{1})
	testMQTTFlush(t, bc2, nil, br2)

	bc3, br3 := testMQTTConnect(t, &mqttConnInfo{user: "b", pass: "pwd", clientID: "b3", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer bc3.Close()
	testMQTTCheckConnAck(t, br3, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, bc3, br3, []*mqttFilter{{filter: "bar", qos: 0}}, []byte{0})
	testMQTTFlush(t, bc3, nil, br3)

	bc4, br4 := testMQTTConnect(t, &mqttConnInfo{user: "b", pass: "pwd", clientID: "b4", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer bc4.Close()
	testMQTTCheckConnAck(t, br4, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, bc4, br4, []*mqttFilter{{filter: "bar", qos: 1}}, []byte{1})
	testMQTTFlush(t, bc4, nil, br4)

	nc := natsConnect(t, fmt.Sprintf("nats://b:pwd@%s:%d", o.Host, o.Port))
	defer nc.Close()

	natsPub(t, nc, "foo", []byte("msg0"))
	testMQTTCheckPubMsgNoAck(t, c1, r1, "foo", 0, []byte("msg0"))
	testMQTTCheckPubMsgNoAck(t, c2, r2, "foo", 0, []byte("msg0"))
	testMQTTExpectNothing(t, r3)
	testMQTTExpectNothing(t, r4)
	testMQTTExpectNothing(t, br1)
	testMQTTExpectNothing(t, br2)
	testMQTTCheckPubMsgNoAck(t, bc3, br3, "bar", 0, []byte("msg0"))
	testMQTTCheckPubMsgNoAck(t, bc4, br4, "bar", 0, []byte("msg0"))

	testMQTTPublish(t, bc, br, 0, false, false, "foo", 0, []byte("msg1"))
	testMQTTCheckPubMsgNoAck(t, c1, r1, "foo", 0, []byte("msg1"))
	testMQTTCheckPubMsgNoAck(t, c2, r2, "foo", 0, []byte("msg1"))
	testMQTTExpectNothing(t, r3)
	testMQTTExpectNothing(t, r4)
	testMQTTExpectNothing(t, br1)
	testMQTTExpectNothing(t, br2)
	testMQTTCheckPubMsgNoAck(t, bc3, br3, "bar", 0, []byte("msg1"))
	testMQTTCheckPubMsgNoAck(t, bc4, br4, "bar", 0, []byte("msg1"))

	testMQTTPublish(t, bc, br, 1, false, false, "foo", 1, []byte("msg2"))
	testMQTTCheckPubMsgNoAck(t, c1, r1, "foo", 0, []byte("msg2"))
	testMQTTCheckPubMsgNoAck(t, c2, r2, "foo", mqttPubQos1, []byte("msg2"))
	testMQTTExpectNothing(t, r3)
	testMQTTExpectNothing(t, r4)
	testMQTTExpectNothing(t, br1)
	testMQTTExpectNothing(t, br2)
	testMQTTCheckPubMsgNoAck(t, bc3, br3, "bar", 0, []byte("msg2"))
	testMQTTCheckPubMsgNoAck(t, bc4, br4, "bar", mqttPubQos1, []byte("msg2"))

	// Connection nc is for account B
	check := func(nc *nats.Conn, subj string) {
		t.Helper()
		req, err := json.Marshal(&JSApiStreamInfoRequest{SubjectsFilter: ">"})
		require_NoError(t, err)
		resp, err := nc.Request(fmt.Sprintf(JSApiStreamInfoT, "$MQTT_msgs"), req, time.Second)
		require_NoError(t, err)
		var si StreamInfo
		err = json.Unmarshal(resp.Data, &si)
		require_NoError(t, err)
		require_True(t, si.State.Msgs == 1)
		require_True(t, si.State.NumSubjects == 1)
		require_True(t, si.State.Subjects[subj] == 1)
	}
	// Currently, nc is for account B
	check(nc, "$MQTT.msgs.bar")

	nc.Close()
	nc = natsConnect(t, fmt.Sprintf("nats://a:pwd@%s:%d", o.Host, o.Port))
	defer nc.Close()
	check(nc, "$MQTT.msgs.foo")
}

// Issue https://github.com/nats-io/nats-server/issues/3924
// The MQTT Server MUST NOT match Topic Filters starting with a wildcard character (# or +),
// with Topic Names beginning with a $ character [MQTT-4.7.2-1]
func TestMQTTSubjectWildcardStart(t *testing.T) {
	conf := createConfFile(t, []byte(`
		listen: 127.0.0.1:-1
		server_name: mqtt
		jetstream: enabled
		mqtt {
			listen: 127.0.0.1:-1
		}
	`))
	s, o := RunServerWithConfig(conf)
	defer testMQTTShutdownServer(s)

	nc := natsConnect(t, s.ClientURL())
	defer nc.Close()

	mc, r := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mc.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

	mc1, r1 := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mc1.Close()
	testMQTTCheckConnAck(t, r1, mqttConnAckRCConnectionAccepted, false)

	mc2, r2 := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mc2.Close()
	testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, false)

	mc3, r3 := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mc3.Close()
	testMQTTCheckConnAck(t, r3, mqttConnAckRCConnectionAccepted, false)

	// These will fail already with the bug due to messages already being queue up before the subAck.
	testMQTTSub(t, 1, mc1, r1, []*mqttFilter{{filter: "*", qos: 0}}, []byte{0})
	testMQTTFlush(t, mc1, nil, r1)

	testMQTTSub(t, 1, mc2, r2, []*mqttFilter{{filter: "#", qos: 1}}, []byte{1})
	testMQTTFlush(t, mc2, nil, r2)

	testMQTTSub(t, 1, mc3, r3, []*mqttFilter{{filter: "*/foo", qos: 1}}, []byte{1})
	testMQTTFlush(t, mc2, nil, r2)

	// Just as a barrier
	natsFlush(t, nc)

	// Now publish

	// NATS Publish
	msg := []byte("HELLO WORLD")
	natsPubReq(t, nc, "foo", _EMPTY_, msg)

	// Check messages received
	testMQTTCheckPubMsg(t, mc1, r1, "foo", 0, msg)
	testMQTTExpectNothing(t, r1)

	testMQTTCheckPubMsg(t, mc2, r2, "foo", 0, msg)
	testMQTTExpectNothing(t, r2)

	testMQTTExpectNothing(t, r3)

	// Anything that starts with $ is reserved against wildcard subjects like above.
	natsPubReq(t, nc, "$JS.foo", _EMPTY_, msg)
	testMQTTExpectNothing(t, r1)
	testMQTTExpectNothing(t, r2)
	testMQTTExpectNothing(t, r3)

	// Now do MQTT QoS-0
	testMQTTPublish(t, mc, r, 0, false, false, "foo", 0, msg)

	testMQTTCheckPubMsg(t, mc1, r1, "foo", 0, msg)
	testMQTTExpectNothing(t, r1)

	testMQTTCheckPubMsg(t, mc2, r2, "foo", 0, msg)
	testMQTTExpectNothing(t, r2)

	testMQTTExpectNothing(t, r3)

	testMQTTPublish(t, mc, r, 0, false, false, "$JS/foo", 1, msg)

	testMQTTExpectNothing(t, r1)
	testMQTTExpectNothing(t, r2)
	testMQTTExpectNothing(t, r3)

	// Now do MQTT QoS-1
	msg = []byte("HELLO WORLD - RETAINED")
	testMQTTPublish(t, mc, r, 1, false, false, "$JS/foo", 4, msg)

	testMQTTExpectNothing(t, r1)
	testMQTTExpectNothing(t, r2)
	testMQTTExpectNothing(t, r3)

	testMQTTPublish(t, mc, r, 1, false, false, "foo", 2, msg)

	testMQTTCheckPubMsg(t, mc1, r1, "foo", 0, msg)
	testMQTTExpectNothing(t, r1)

	testMQTTCheckPubMsg(t, mc2, r2, "foo", 2, msg)
	testMQTTExpectNothing(t, r2)

	testMQTTExpectNothing(t, r3)

	testMQTTPublish(t, mc, r, 1, false, false, "foo/foo", 3, msg)

	testMQTTExpectNothing(t, r1)

	testMQTTCheckPubMsg(t, mc2, r2, "foo/foo", 2, msg)
	testMQTTExpectNothing(t, r2)

	testMQTTCheckPubMsg(t, mc3, r3, "foo/foo", 2, msg)
	testMQTTExpectNothing(t, r3)

	// Make sure we did not retain the messages prefixed with $.
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	si, err := js.StreamInfo(mqttStreamName)
	require_NoError(t, err)

	require_True(t, si.State.Msgs == 0)
}

func TestMQTTTopicWithDot(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	nc := natsConnect(t, s.ClientURL(), nats.UserInfo("mqtt", "pwd"))
	defer nc.Close()

	sub := natsSubSync(t, nc, "*.*")

	c1, r1 := testMQTTConnect(t, &mqttConnInfo{user: "mqtt", pass: "pwd", clientID: "conn1", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer c1.Close()
	testMQTTCheckConnAck(t, r1, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, c1, r1, []*mqttFilter{{filter: "spBv1.0/plant1", qos: 0}}, []byte{0})
	testMQTTSub(t, 1, c1, r1, []*mqttFilter{{filter: "spBv1.0/plant2", qos: 1}}, []byte{1})

	c2, r2 := testMQTTConnect(t, &mqttConnInfo{user: "mqtt", pass: "pwd", clientID: "conn2", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTCheckConnAck(t, r2, mqttConnAckRCConnectionAccepted, false)

	testMQTTPublish(t, c2, r2, 0, false, false, "spBv1.0/plant1", 0, []byte("msg1"))
	testMQTTCheckPubMsg(t, c1, r1, "spBv1.0/plant1", 0, []byte("msg1"))
	msg := natsNexMsg(t, sub, time.Second)
	require_Equal(t, msg.Subject, "spBv1//0.plant1")

	testMQTTPublish(t, c2, r2, 1, false, false, "spBv1.0/plant2", 1, []byte("msg2"))
	testMQTTCheckPubMsg(t, c1, r1, "spBv1.0/plant2", mqttPubQos1, []byte("msg2"))
	msg = natsNexMsg(t, sub, time.Second)
	require_Equal(t, msg.Subject, "spBv1//0.plant2")

	natsPub(t, nc, "spBv1//0.plant1", []byte("msg3"))
	testMQTTCheckPubMsg(t, c1, r1, "spBv1.0/plant1", 0, []byte("msg3"))
	msg = natsNexMsg(t, sub, time.Second)
	require_Equal(t, msg.Subject, "spBv1//0.plant1")

	natsPub(t, nc, "spBv1//0.plant2", []byte("msg4"))
	testMQTTCheckPubMsg(t, c1, r1, "spBv1.0/plant2", 0, []byte("msg4"))
	msg = natsNexMsg(t, sub, time.Second)
	require_Equal(t, msg.Subject, "spBv1//0.plant2")
}

// Issue https://github.com/nats-io/nats-server/issues/4291
func TestMQTTJetStreamRepublishAndQoS0Subscribers(t *testing.T) {
	conf := createConfFile(t, []byte(`
		listen: 127.0.0.1:-1
		server_name: mqtt
		jetstream: enabled
		mqtt {
			listen: 127.0.0.1:-1
		}
	`))
	s, o := RunServerWithConfig(conf)
	defer testMQTTShutdownServer(s)

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	// Setup stream with republish on it.
	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		RePublish: &nats.RePublish{
			Source:      "foo",
			Destination: "mqtt.foo",
		},
	})
	require_NoError(t, err)

	// Create QoS0 subscriber to catch re-publishes.
	mc, r := testMQTTConnect(t, &mqttConnInfo{cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer mc.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

	testMQTTSub(t, 1, mc, r, []*mqttFilter{{filter: "mqtt/foo", qos: 0}}, []byte{0})
	testMQTTFlush(t, mc, nil, r)

	msg := []byte("HELLO WORLD")
	_, err = js.Publish("foo", msg)
	require_NoError(t, err)

	testMQTTCheckPubMsg(t, mc, r, "mqtt/foo", 0, msg)
	testMQTTExpectNothing(t, r)
}

//////////////////////////////////////////////////////////////////////////
//
// Benchmarks
//
//////////////////////////////////////////////////////////////////////////

const (
	mqttPubSubj     = "a"
	mqttBenchBufLen = 32768
)

func mqttBenchPubQoS0(b *testing.B, subject, payload string, numSubs int) {
	b.StopTimer()
	o := testMQTTDefaultOptions()
	s := RunServer(o)
	defer testMQTTShutdownServer(s)

	ci := &mqttConnInfo{clientID: "pub", cleanSess: true}
	c, br := testMQTTConnect(b, ci, o.MQTT.Host, o.MQTT.Port)
	testMQTTCheckConnAck(b, br, mqttConnAckRCConnectionAccepted, false)
	w := &mqttWriter{}
	mqttWritePublish(w, 0, false, false, subject, 0, []byte(payload))
	sendOp := w.Bytes()

	dch := make(chan error, 1)
	totalSize := int64(len(sendOp))
	cdch := 0

	createSub := func(i int) {
		ci := &mqttConnInfo{clientID: fmt.Sprintf("sub%d", i), cleanSess: true}
		cs, brs := testMQTTConnect(b, ci, o.MQTT.Host, o.MQTT.Port)
		testMQTTCheckConnAck(b, brs, mqttConnAckRCConnectionAccepted, false)

		testMQTTSub(b, 1, cs, brs, []*mqttFilter{{filter: subject, qos: 0}}, []byte{0})
		testMQTTFlush(b, cs, nil, brs)

		w := &mqttWriter{}
		varHeaderAndPayload := 2 + len(subject) + len(payload)
		w.WriteVarInt(varHeaderAndPayload)
		size := 1 + w.Len() + varHeaderAndPayload
		totalSize += int64(size)

		go func() {
			mqttBenchConsumeMsgQoS0(cs, int64(b.N)*int64(size), dch)
			cs.Close()
		}()
	}
	for i := 0; i < numSubs; i++ {
		createSub(i + 1)
		cdch++
	}

	bw := bufio.NewWriterSize(c, mqttBenchBufLen)
	b.SetBytes(totalSize)
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		bw.Write(sendOp)
	}
	testMQTTFlush(b, c, bw, br)
	for i := 0; i < cdch; i++ {
		if e := <-dch; e != nil {
			b.Fatal(e.Error())
		}
	}
	b.StopTimer()
	c.Close()
	s.Shutdown()
}

func mqttBenchConsumeMsgQoS0(c net.Conn, total int64, dch chan<- error) {
	var buf [mqttBenchBufLen]byte
	var err error
	var n int
	for size := int64(0); size < total; {
		n, err = c.Read(buf[:])
		if err != nil {
			break
		}
		size += int64(n)
	}
	dch <- err
}

func mqttBenchPubQoS1(b *testing.B, subject, payload string, numSubs int) {
	b.StopTimer()
	o := testMQTTDefaultOptions()
	o.MQTT.MaxAckPending = 0xFFFF
	s := RunServer(o)
	defer testMQTTShutdownServer(s)

	ci := &mqttConnInfo{cleanSess: true}
	c, br := testMQTTConnect(b, ci, o.MQTT.Host, o.MQTT.Port)
	testMQTTCheckConnAck(b, br, mqttConnAckRCConnectionAccepted, false)

	w := &mqttWriter{}
	mqttWritePublish(w, 1, false, false, subject, 1, []byte(payload))
	// For reported bytes we will count the PUBLISH + PUBACK (4 bytes)
	totalSize := int64(len(w.Bytes()) + 4)
	w.Reset()

	pi := uint16(1)
	maxpi := uint16(60000)
	ppich := make(chan error, 10)
	dch := make(chan error, 1+numSubs)
	cdch := 1
	// Start go routine to consume PUBACK for published QoS 1 messages.
	go mqttBenchConsumePubAck(c, b.N, dch, ppich)

	createSub := func(i int) {
		ci := &mqttConnInfo{clientID: fmt.Sprintf("sub%d", i), cleanSess: true}
		cs, brs := testMQTTConnect(b, ci, o.MQTT.Host, o.MQTT.Port)
		testMQTTCheckConnAck(b, brs, mqttConnAckRCConnectionAccepted, false)

		testMQTTSub(b, 1, cs, brs, []*mqttFilter{{filter: subject, qos: 1}}, []byte{1})
		testMQTTFlush(b, cs, nil, brs)

		w := &mqttWriter{}
		varHeaderAndPayload := 2 + len(subject) + 2 + len(payload)
		w.WriteVarInt(varHeaderAndPayload)
		size := 1 + w.Len() + varHeaderAndPayload
		// Add to the bytes reported the size of message sent to subscriber + PUBACK (4 bytes)
		totalSize += int64(size + 4)

		go func() {
			mqttBenchConsumeMsgQos1(cs, b.N, size, dch)
			cs.Close()
		}()
	}
	for i := 0; i < numSubs; i++ {
		createSub(i + 1)
		cdch++
	}

	flush := func() {
		b.Helper()
		if _, err := c.Write(w.Bytes()); err != nil {
			b.Fatalf("Error on write: %v", err)
		}
		w.Reset()
	}

	b.SetBytes(totalSize)
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		if pi <= maxpi {
			mqttWritePublish(w, 1, false, false, subject, pi, []byte(payload))
			pi++
			if w.Len() >= mqttBenchBufLen {
				flush()
			}
		} else {
			if w.Len() > 0 {
				flush()
			}
			if pi > 60000 {
				pi = 1
				maxpi = 0
			}
			if e := <-ppich; e != nil {
				b.Fatal(e.Error())
			}
			maxpi += 10000
			i--
		}
	}
	if w.Len() > 0 {
		flush()
	}
	for i := 0; i < cdch; i++ {
		if e := <-dch; e != nil {
			b.Fatal(e.Error())
		}
	}
	b.StopTimer()
	c.Close()
	s.Shutdown()
}

func mqttBenchConsumeMsgQos1(c net.Conn, total, size int, dch chan<- error) {
	var buf [mqttBenchBufLen]byte
	pubAck := [4]byte{mqttPacketPubAck, 0x2, 0, 0}
	var err error
	var n int
	var pi uint16
	var prev int
	for i := 0; i < total; {
		n, err = c.Read(buf[:])
		if err != nil {
			break
		}
		n += prev
		for ; n >= size; n -= size {
			i++
			pi++
			pubAck[2] = byte(pi >> 8)
			pubAck[3] = byte(pi)
			if _, err = c.Write(pubAck[:4]); err != nil {
				dch <- err
				return
			}
			if pi == 60000 {
				pi = 0
			}
		}
		prev = n
	}
	dch <- err
}

func mqttBenchConsumePubAck(c net.Conn, total int, dch, ppich chan<- error) {
	var buf [mqttBenchBufLen]byte
	var err error
	var n int
	var pi uint16
	var prev int
	for i := 0; i < total; {
		n, err = c.Read(buf[:])
		if err != nil {
			break
		}
		n += prev
		for ; n >= 4; n -= 4 {
			i++
			pi++
			if pi%10000 == 0 {
				ppich <- nil
			}
			if pi == 60001 {
				pi = 0
			}
		}
		prev = n
	}
	ppich <- err
	dch <- err
}

func BenchmarkMQTT_QoS0_Pub_______0b_Payload(b *testing.B) {
	mqttBenchPubQoS0(b, mqttPubSubj, "", 0)
}

func BenchmarkMQTT_QoS0_Pub_______8b_Payload(b *testing.B) {
	mqttBenchPubQoS0(b, mqttPubSubj, sizedString(8), 0)
}

func BenchmarkMQTT_QoS0_Pub______32b_Payload(b *testing.B) {
	mqttBenchPubQoS0(b, mqttPubSubj, sizedString(32), 0)
}

func BenchmarkMQTT_QoS0_Pub_____128b_Payload(b *testing.B) {
	mqttBenchPubQoS0(b, mqttPubSubj, sizedString(128), 0)
}

func BenchmarkMQTT_QoS0_Pub_____256b_Payload(b *testing.B) {
	mqttBenchPubQoS0(b, mqttPubSubj, sizedString(256), 0)
}

func BenchmarkMQTT_QoS0_Pub_______1K_Payload(b *testing.B) {
	mqttBenchPubQoS0(b, mqttPubSubj, sizedString(1024), 0)
}

func BenchmarkMQTT_QoS0_PubSub1___0b_Payload(b *testing.B) {
	mqttBenchPubQoS0(b, mqttPubSubj, "", 1)
}

func BenchmarkMQTT_QoS0_PubSub1___8b_Payload(b *testing.B) {
	mqttBenchPubQoS0(b, mqttPubSubj, sizedString(8), 1)
}

func BenchmarkMQTT_QoS0_PubSub1__32b_Payload(b *testing.B) {
	mqttBenchPubQoS0(b, mqttPubSubj, sizedString(32), 1)
}

func BenchmarkMQTT_QoS0_PubSub1_128b_Payload(b *testing.B) {
	mqttBenchPubQoS0(b, mqttPubSubj, sizedString(128), 1)
}

func BenchmarkMQTT_QoS0_PubSub1_256b_Payload(b *testing.B) {
	mqttBenchPubQoS0(b, mqttPubSubj, sizedString(256), 1)
}

func BenchmarkMQTT_QoS0_PubSub1___1K_Payload(b *testing.B) {
	mqttBenchPubQoS0(b, mqttPubSubj, sizedString(1024), 1)
}

func BenchmarkMQTT_QoS0_PubSub2___0b_Payload(b *testing.B) {
	mqttBenchPubQoS0(b, mqttPubSubj, "", 2)
}

func BenchmarkMQTT_QoS0_PubSub2___8b_Payload(b *testing.B) {
	mqttBenchPubQoS0(b, mqttPubSubj, sizedString(8), 2)
}

func BenchmarkMQTT_QoS0_PubSub2__32b_Payload(b *testing.B) {
	mqttBenchPubQoS0(b, mqttPubSubj, sizedString(32), 2)
}

func BenchmarkMQTT_QoS0_PubSub2_128b_Payload(b *testing.B) {
	mqttBenchPubQoS0(b, mqttPubSubj, sizedString(128), 2)
}

func BenchmarkMQTT_QoS0_PubSub2_256b_Payload(b *testing.B) {
	mqttBenchPubQoS0(b, mqttPubSubj, sizedString(256), 2)
}

func BenchmarkMQTT_QoS0_PubSub2___1K_Payload(b *testing.B) {
	mqttBenchPubQoS0(b, mqttPubSubj, sizedString(1024), 2)
}

func BenchmarkMQTT_QoS1_Pub_______0b_Payload(b *testing.B) {
	mqttBenchPubQoS1(b, mqttPubSubj, "", 0)
}

func BenchmarkMQTT_QoS1_Pub_______8b_Payload(b *testing.B) {
	mqttBenchPubQoS1(b, mqttPubSubj, sizedString(8), 0)
}

func BenchmarkMQTT_QoS1_Pub______32b_Payload(b *testing.B) {
	mqttBenchPubQoS1(b, mqttPubSubj, sizedString(32), 0)
}

func BenchmarkMQTT_QoS1_Pub_____128b_Payload(b *testing.B) {
	mqttBenchPubQoS1(b, mqttPubSubj, sizedString(128), 0)
}

func BenchmarkMQTT_QoS1_Pub_____256b_Payload(b *testing.B) {
	mqttBenchPubQoS1(b, mqttPubSubj, sizedString(256), 0)
}

func BenchmarkMQTT_QoS1_Pub_______1K_Payload(b *testing.B) {
	mqttBenchPubQoS1(b, mqttPubSubj, sizedString(1024), 0)
}

func BenchmarkMQTT_QoS1_PubSub1___0b_Payload(b *testing.B) {
	mqttBenchPubQoS1(b, mqttPubSubj, "", 1)
}

func BenchmarkMQTT_QoS1_PubSub1___8b_Payload(b *testing.B) {
	mqttBenchPubQoS1(b, mqttPubSubj, sizedString(8), 1)
}

func BenchmarkMQTT_QoS1_PubSub1__32b_Payload(b *testing.B) {
	mqttBenchPubQoS1(b, mqttPubSubj, sizedString(32), 1)
}

func BenchmarkMQTT_QoS1_PubSub1_128b_Payload(b *testing.B) {
	mqttBenchPubQoS1(b, mqttPubSubj, sizedString(128), 1)
}

func BenchmarkMQTT_QoS1_PubSub1_256b_Payload(b *testing.B) {
	mqttBenchPubQoS1(b, mqttPubSubj, sizedString(256), 1)
}

func BenchmarkMQTT_QoS1_PubSub1___1K_Payload(b *testing.B) {
	mqttBenchPubQoS1(b, mqttPubSubj, sizedString(1024), 1)
}

func BenchmarkMQTT_QoS1_PubSub2___0b_Payload(b *testing.B) {
	mqttBenchPubQoS1(b, mqttPubSubj, "", 2)
}

func BenchmarkMQTT_QoS1_PubSub2___8b_Payload(b *testing.B) {
	mqttBenchPubQoS1(b, mqttPubSubj, sizedString(8), 2)
}

func BenchmarkMQTT_QoS1_PubSub2__32b_Payload(b *testing.B) {
	mqttBenchPubQoS1(b, mqttPubSubj, sizedString(32), 2)
}

func BenchmarkMQTT_QoS1_PubSub2_128b_Payload(b *testing.B) {
	mqttBenchPubQoS1(b, mqttPubSubj, sizedString(128), 2)
}

func BenchmarkMQTT_QoS1_PubSub2_256b_Payload(b *testing.B) {
	mqttBenchPubQoS1(b, mqttPubSubj, sizedString(256), 2)
}

func BenchmarkMQTT_QoS1_PubSub2___1K_Payload(b *testing.B) {
	mqttBenchPubQoS1(b, mqttPubSubj, sizedString(1024), 2)
}
