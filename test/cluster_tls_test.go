// Copyright 2013-2019 The NATS Authors
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

package test

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
)

func runTLSServers(t *testing.T) (srvA, srvB *server.Server, optsA, optsB *server.Options) {
	srvA, optsA = RunServerWithConfig("./configs/srv_a_tls.conf")
	srvB, optsB = RunServerWithConfig("./configs/srv_b_tls.conf")
	checkClusterFormed(t, srvA, srvB)
	return
}

func TestTLSClusterConfig(t *testing.T) {
	srvA, srvB, _, _ := runTLSServers(t)
	defer srvA.Shutdown()
	defer srvB.Shutdown()
}

func TestBasicTLSClusterPubSub(t *testing.T) {
	srvA, srvB, optsA, optsB := runTLSServers(t)
	defer srvA.Shutdown()
	defer srvB.Shutdown()

	clientA := createClientConn(t, optsA.Host, optsA.Port)
	defer clientA.Close()

	clientB := createClientConn(t, optsB.Host, optsB.Port)
	defer clientB.Close()

	sendA, expectA := setupConn(t, clientA)
	sendA("SUB foo 22\r\n")
	sendA("PING\r\n")
	expectA(pongRe)

	if err := checkExpectedSubs(1, srvA, srvB); err != nil {
		t.Fatalf("%v", err)
	}

	sendB, expectB := setupConn(t, clientB)
	sendB("PUB foo 2\r\nok\r\n")
	sendB("PING\r\n")
	expectB(pongRe)

	expectMsgs := expectMsgsCommand(t, expectA)

	matches := expectMsgs(1)
	checkMsg(t, matches[0], "foo", "22", "", "2", "ok")
}

type captureTLSError struct {
	dummyLogger
	ch chan struct{}
}

func (c *captureTLSError) Errorf(format string, v ...interface{}) {
	msg := fmt.Sprintf(format, v...)
	if strings.Contains(msg, "handshake error") {
		select {
		case c.ch <- struct{}{}:
		default:
		}
	}
}

type captureClusterTLSInsecureLogger struct {
	dummyLogger
	ch chan struct{}
}

func (c *captureClusterTLSInsecureLogger) Warnf(format string, v ...interface{}) {
	msg := fmt.Sprintf(format, v...)
	if strings.Contains(msg, "solicited routes will not be verified") {
		select {
		case c.ch <- struct{}{}:
		default:
		}
	}
}

func TestClusterTLSInsecure(t *testing.T) {
	confA := createConfFile(t, []byte(`
		port: -1
		cluster {
			name: "xyz"
			listen: "127.0.0.1:-1"
			tls {
			    cert_file: "./configs/certs/server-noip.pem"
				key_file:  "./configs/certs/server-key-noip.pem"
				ca_file:   "./configs/certs/ca.pem"
				timeout: 2
			}
		}
	`))
	srvA, optsA := RunServerWithConfig(confA)
	defer srvA.Shutdown()

	l := &captureTLSError{ch: make(chan struct{}, 1)}
	srvA.SetLogger(l, false, false)

	bConfigTemplate := `
		port: -1
		cluster {
			name: "xyz"
			listen: "127.0.0.1:-1"
			tls {
			    cert_file: "./configs/certs/server-noip.pem"
				key_file:  "./configs/certs/server-key-noip.pem"
				ca_file:   "./configs/certs/ca.pem"
				timeout: 2
				%s
			}
			routes [
				"nats://%s:%d"
			]
		}
	`
	confB := createConfFile(t, []byte(fmt.Sprintf(bConfigTemplate,
		"", optsA.Cluster.Host, optsA.Cluster.Port)))
	srvB, _ := RunServerWithConfig(confB)
	defer srvB.Shutdown()

	// We should get errors
	select {
	case <-l.ch:
	case <-time.After(2 * time.Second):
		t.Fatalf("Did not get handshake error")
	}

	// Set a logger that will capture the warning
	wl := &captureClusterTLSInsecureLogger{ch: make(chan struct{}, 1)}
	srvB.SetLogger(wl, false, false)

	// Need to add "insecure: true" and reload
	if err := os.WriteFile(confB,
		[]byte(fmt.Sprintf(bConfigTemplate, "insecure: true", optsA.Cluster.Host, optsA.Cluster.Port)),
		0666); err != nil {
		t.Fatalf("Error rewriting file: %v", err)
	}
	if err := srvB.Reload(); err != nil {
		t.Fatalf("Error on reload: %v", err)
	}

	checkClusterFormed(t, srvA, srvB)

	// Make sure we have the tracing
	select {
	case <-wl.ch:
	case <-time.After(2 * time.Second):
		t.Fatalf("Did not get warning about using cluster's insecure setting")
	}
}
