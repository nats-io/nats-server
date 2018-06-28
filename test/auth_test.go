// Copyright 2012-2018 The NATS Authors
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
	"encoding/json"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/nats-io/gnatsd/server"
)

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
	if !testInfoForAuth(t, infojs) {
		t.Fatalf("Expected server to require authorization: '%s'", infojs)
	}
}

////////////////////////////////////////////////////////////
// The authorization token version
////////////////////////////////////////////////////////////

const AUTH_PORT = 10422
const AUTH_TOKEN = "_YZZ22_"

func runAuthServerWithToken() *server.Server {
	opts := DefaultTestOptions
	opts.Port = AUTH_PORT
	opts.Authorization = AUTH_TOKEN
	return RunServer(&opts)
}

func TestNoAuthClient(t *testing.T) {
	s := runAuthServerWithToken()
	defer s.Shutdown()
	c := createClientConn(t, "127.0.0.1", AUTH_PORT)
	defer c.Close()
	expectAuthRequired(t, c)
	doAuthConnect(t, c, "", "", "")
	expectResult(t, c, errRe)
}

func TestAuthClientBadToken(t *testing.T) {
	s := runAuthServerWithToken()
	defer s.Shutdown()
	c := createClientConn(t, "127.0.0.1", AUTH_PORT)
	defer c.Close()
	expectAuthRequired(t, c)
	doAuthConnect(t, c, "ZZZ", "", "")
	expectResult(t, c, errRe)
}

func TestAuthClientNoConnect(t *testing.T) {
	s := runAuthServerWithToken()
	defer s.Shutdown()
	c := createClientConn(t, "127.0.0.1", AUTH_PORT)
	defer c.Close()
	expectAuthRequired(t, c)
	// This is timing dependent..
	time.Sleep(server.AUTH_TIMEOUT)
	expectResult(t, c, errRe)
}

func TestAuthClientGoodConnect(t *testing.T) {
	s := runAuthServerWithToken()
	defer s.Shutdown()
	c := createClientConn(t, "127.0.0.1", AUTH_PORT)
	defer c.Close()
	expectAuthRequired(t, c)
	doAuthConnect(t, c, AUTH_TOKEN, "", "")
	expectResult(t, c, okRe)
}

func TestAuthClientFailOnEverythingElse(t *testing.T) {
	s := runAuthServerWithToken()
	defer s.Shutdown()
	c := createClientConn(t, "127.0.0.1", AUTH_PORT)
	defer c.Close()
	expectAuthRequired(t, c)
	sendProto(t, c, "PUB foo 2\r\nok\r\n")
	expectResult(t, c, errRe)
}

////////////////////////////////////////////////////////////
// The username/password version
////////////////////////////////////////////////////////////

const AUTH_USER = "derek"
const AUTH_PASS = "foobar"

func runAuthServerWithUserPass() *server.Server {
	opts := DefaultTestOptions
	opts.Port = AUTH_PORT
	opts.Username = AUTH_USER
	opts.Password = AUTH_PASS
	return RunServer(&opts)
}

func TestNoUserOrPasswordClient(t *testing.T) {
	s := runAuthServerWithUserPass()
	defer s.Shutdown()
	c := createClientConn(t, "127.0.0.1", AUTH_PORT)
	defer c.Close()
	expectAuthRequired(t, c)
	doAuthConnect(t, c, "", "", "")
	expectResult(t, c, errRe)
}

func TestBadUserClient(t *testing.T) {
	s := runAuthServerWithUserPass()
	defer s.Shutdown()
	c := createClientConn(t, "127.0.0.1", AUTH_PORT)
	defer c.Close()
	expectAuthRequired(t, c)
	doAuthConnect(t, c, "", "derekzz", AUTH_PASS)
	expectResult(t, c, errRe)
}

func TestBadPasswordClient(t *testing.T) {
	s := runAuthServerWithUserPass()
	defer s.Shutdown()
	c := createClientConn(t, "127.0.0.1", AUTH_PORT)
	defer c.Close()
	expectAuthRequired(t, c)
	doAuthConnect(t, c, "", AUTH_USER, "ZZ")
	expectResult(t, c, errRe)
}

func TestPasswordClientGoodConnect(t *testing.T) {
	s := runAuthServerWithUserPass()
	defer s.Shutdown()
	c := createClientConn(t, "127.0.0.1", AUTH_PORT)
	defer c.Close()
	expectAuthRequired(t, c)
	doAuthConnect(t, c, "", AUTH_USER, AUTH_PASS)
	expectResult(t, c, okRe)
}

////////////////////////////////////////////////////////////
// The bcrypt username/password version
////////////////////////////////////////////////////////////

// Generated with util/mkpasswd (Cost 4 because of cost of --race, default is 11)
const BCRYPT_AUTH_PASS = "IW@$6v(y1(t@fhPDvf!5^%"
const BCRYPT_AUTH_HASH = "$2a$04$Q.CgCP2Sl9pkcTXEZHazaeMwPaAkSHk7AI51HkyMt5iJQQyUA4qxq"

func runAuthServerWithBcryptUserPass() *server.Server {
	opts := DefaultTestOptions
	opts.Port = AUTH_PORT
	opts.Username = AUTH_USER
	opts.Password = BCRYPT_AUTH_HASH
	return RunServer(&opts)
}

func TestBadBcryptPassword(t *testing.T) {
	s := runAuthServerWithBcryptUserPass()
	defer s.Shutdown()
	c := createClientConn(t, "127.0.0.1", AUTH_PORT)
	defer c.Close()
	expectAuthRequired(t, c)
	doAuthConnect(t, c, "", AUTH_USER, BCRYPT_AUTH_HASH)
	expectResult(t, c, errRe)
}

func TestGoodBcryptPassword(t *testing.T) {
	s := runAuthServerWithBcryptUserPass()
	defer s.Shutdown()
	c := createClientConn(t, "127.0.0.1", AUTH_PORT)
	defer c.Close()
	expectAuthRequired(t, c)
	doAuthConnect(t, c, "", AUTH_USER, BCRYPT_AUTH_PASS)
	expectResult(t, c, okRe)
}

////////////////////////////////////////////////////////////
// The bcrypt authorization token version
////////////////////////////////////////////////////////////

const BCRYPT_AUTH_TOKEN = "0uhJOSr3GW7xvHvtd^K6pa"
const BCRYPT_AUTH_TOKEN_HASH = "$2a$04$u5ZClXpcjHgpfc61Ee0VKuwI1K3vTC4zq7SjphjnlHMeb1Llkb5Y6"

func runAuthServerWithBcryptToken() *server.Server {
	opts := DefaultTestOptions
	opts.Port = AUTH_PORT
	opts.Authorization = BCRYPT_AUTH_TOKEN_HASH
	return RunServer(&opts)
}

func TestBadBcryptToken(t *testing.T) {
	s := runAuthServerWithBcryptToken()
	defer s.Shutdown()
	c := createClientConn(t, "127.0.0.1", AUTH_PORT)
	defer c.Close()
	expectAuthRequired(t, c)
	doAuthConnect(t, c, BCRYPT_AUTH_TOKEN_HASH, "", "")
	expectResult(t, c, errRe)
}

func TestGoodBcryptToken(t *testing.T) {
	s := runAuthServerWithBcryptToken()
	defer s.Shutdown()
	c := createClientConn(t, "127.0.0.1", AUTH_PORT)
	defer c.Close()
	expectAuthRequired(t, c)
	doAuthConnect(t, c, BCRYPT_AUTH_TOKEN, "", "")
	expectResult(t, c, okRe)
}
