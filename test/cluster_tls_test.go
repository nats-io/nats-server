// Copyright 2013-2018 The NATS Authors
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
	"testing"

	"github.com/nats-io/gnatsd/server"
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

	sendB, expectB := setupConn(t, clientB)
	sendB("PUB foo 2\r\nok\r\n")
	sendB("PING\r\n")
	expectB(pongRe)

	if err := checkExpectedSubs(1, srvA, srvB); err != nil {
		t.Fatalf("%v", err)
	}

	expectMsgs := expectMsgsCommand(t, expectA)

	matches := expectMsgs(1)
	checkMsg(t, matches[0], "foo", "22", "", "2", "ok")
}
