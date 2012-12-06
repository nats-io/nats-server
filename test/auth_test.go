// Copyright 2012 Apcera Inc. All rights reserved.

package test

import (
	"encoding/json"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/apcera/gnatsd/server"
)

const AUTH_PORT=10422

func doAuthConnect(t tLogger, c net.Conn, token, user, pass string) {
	cs := fmt.Sprintf("CONNECT {\"verbose\":true,\"auth_token\":\"%s\",\"user\":\"%s\",\"pass\":\"%s\"}\r\n", token, user, pass)
	sendProto(t, c, cs)
}

func testInfoForAuth(t tLogger, infojs []byte) bool {
	var sinfo server.Info
	err := json.Unmarshal(infojs, &sinfo)
	if err != nil {
		t.Fatalf("Could not unmarshal INFO json: %v\n", err)
	}
	return sinfo.AuthRequired
}

func expectAuthRequired(t tLogger, c net.Conn) {
	buf := expectResult(t, c, infoRe)
	infojs := infoRe.FindAllSubmatch(buf, 1)[0][1]
	if testInfoForAuth(t, infojs) != true {
		t.Fatalf("Expected server to require authorization: '%s'", infojs)
	}
}

const AUTH_TOKEN = "_YZZ22_"

func TestStartupAuthToken(t *testing.T) {
	s = startServer(t, AUTH_PORT, fmt.Sprintf("--auth=%s", AUTH_TOKEN))
}

func TestNoAuthClient(t *testing.T) {
	c := createClientConn(t, "localhost", AUTH_PORT)
	defer c.Close()
	expectAuthRequired(t, c)
	doAuthConnect(t, c, "", "", "")
	expectResult(t, c, errRe)
}

func TestAuthClientBadToken(t *testing.T) {
	c := createClientConn(t, "localhost", AUTH_PORT)
	defer c.Close()
	expectAuthRequired(t, c)
	doAuthConnect(t, c, "ZZZ", "", "")
	expectResult(t, c, errRe)
}

func TestAuthClientNoConnect(t *testing.T) {
	c := createClientConn(t, "localhost", AUTH_PORT)
	defer c.Close()
	expectAuthRequired(t, c)
	// This is timing dependent..
	time.Sleep(server.AUTH_TIMEOUT)
	expectResult(t, c, errRe)
}

func TestAuthClientGoodConnect(t *testing.T) {
	c := createClientConn(t, "localhost", AUTH_PORT)
	defer c.Close()
	expectAuthRequired(t, c)
	doAuthConnect(t, c, AUTH_TOKEN, "", "")
	expectResult(t, c, okRe)
}

func TestAuthClientFailOnEverythingElse(t *testing.T) {
	c := createClientConn(t, "localhost", AUTH_PORT)
	defer c.Close()
	expectAuthRequired(t, c)
	sendProto(t, c, "PUB foo 2\r\nok\r\n")
	expectResult(t, c, errRe)
}

func TestStopServerAuthToken(t *testing.T) {
	s.stopServer()
}

const AUTH_USER = "derek"
const AUTH_PASS = "foobar"

// The username/password versions
func TestStartupAuthPassword(t *testing.T) {
	s = startServer(t, AUTH_PORT, fmt.Sprintf("--user=%s --pass=%s", AUTH_USER, AUTH_PASS))
}

func TestNoUserOrPasswordClient(t *testing.T) {
	c := createClientConn(t, "localhost", AUTH_PORT)
	defer c.Close()
	expectAuthRequired(t, c)
	doAuthConnect(t, c, "", "", "")
	expectResult(t, c, errRe)
}

func TestBadUserClient(t *testing.T) {
	c := createClientConn(t, "localhost", AUTH_PORT)
	defer c.Close()
	expectAuthRequired(t, c)
	doAuthConnect(t, c, "", "derekzz", AUTH_PASS)
	expectResult(t, c, errRe)
}

func TestBadPasswordClient(t *testing.T) {
	c := createClientConn(t, "localhost", AUTH_PORT)
	defer c.Close()
	expectAuthRequired(t, c)
	doAuthConnect(t, c, "", AUTH_USER, "ZZ")
	expectResult(t, c, errRe)
}

func TestPasswordClientGoodConnect(t *testing.T) {
	c := createClientConn(t, "localhost", AUTH_PORT)
	defer c.Close()
	expectAuthRequired(t, c)
	doAuthConnect(t, c, "", AUTH_USER, AUTH_PASS)
	expectResult(t, c, okRe)
}

func TestStopServerAuthPassword(t *testing.T) {
	s.stopServer()
}
