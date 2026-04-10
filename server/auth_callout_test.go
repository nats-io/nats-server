// Copyright 2022-2025 The NATS Authors
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
	"bytes"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
)

// Helper function to decode an auth request.
func decodeAuthRequest(t *testing.T, ejwt []byte) (string, *jwt.ServerID, *jwt.ClientInformation, *jwt.ConnectOptions, *jwt.ClientTLS) {
	t.Helper()
	ac, err := jwt.DecodeAuthorizationRequestClaims(string(ejwt))
	require_NoError(t, err)
	return ac.UserNkey, &ac.Server, &ac.ClientInformation, &ac.ConnectOptions, ac.TLS
}

func authCalloutMQTTConnectPacket(clientID, user, pass string) []byte {
	flags := byte(mqttConnFlagCleanSession)
	if user != _EMPTY_ {
		flags |= mqttConnFlagUsernameFlag
	}
	if pass != _EMPTY_ {
		flags |= mqttConnFlagPasswordFlag
	}

	pkLen := 2 + len(mqttProtoName) +
		1 +
		1 +
		2 +
		2 + len(clientID)

	if user != _EMPTY_ {
		pkLen += 2 + len(user)
	}
	if pass != _EMPTY_ {
		pkLen += 2 + len(pass)
	}

	w := newMQTTWriter(0)
	w.WriteByte(mqttPacketConnect)
	w.WriteVarInt(pkLen)
	w.WriteString(string(mqttProtoName))
	w.WriteByte(0x4)
	w.WriteByte(flags)
	w.WriteUint16(0)
	w.WriteString(clientID)
	if user != _EMPTY_ {
		w.WriteString(user)
	}
	if pass != _EMPTY_ {
		w.WriteBytes([]byte(pass))
	}
	return w.Bytes()
}

func authCalloutMQTTConnect(t testing.TB, host string, port int, clientID, user, pass string) (net.Conn, byte) {
	t.Helper()

	c, err := net.Dial("tcp", net.JoinHostPort(host, fmt.Sprintf("%d", port)))
	require_NoError(t, err)

	c.SetDeadline(time.Now().Add(2 * time.Second))
	defer c.SetDeadline(time.Time{})

	_, err = c.Write(authCalloutMQTTConnectPacket(clientID, user, pass))
	require_NoError(t, err)

	var ack [4]byte
	_, err = io.ReadFull(c, ack[:])
	require_NoError(t, err)
	if ack[0] != mqttPacketConnectAck || ack[1] != 2 {
		t.Fatalf("Expected MQTT CONNACK, got %v", ack[:])
	}
	return c, ack[3]
}

func TestTitleCaseEmptyString(t *testing.T) {
	defer require_NoPanic(t)
	require_Equal(t, titleCase(_EMPTY_), _EMPTY_)
}

const (
	authCalloutPub        = "UBO2MQV67TQTVIRV3XFTEZOACM4WLOCMCDMAWN5QVN5PI2N6JHTVDRON"
	authCalloutSeed       = "SUAP277QP7U4JMFFPVZHLJYEQJ2UHOTYVEIZJYAWRJXQLP4FRSEHYZJJOU"
	authCalloutIssuer     = "ABJHLOVMPA4CI6R5KLNGOB4GSLNIY7IOUPAJC4YFNDLQVIOBYQGUWVLA"
	authCalloutIssuerSeed = "SAANDLKMXL6CUS3CP52WIXBEDN6YJ545GDKC65U5JZPPV6WH6ESWUA6YAI"
	authCalloutIssuerSK   = "SAAE46BB675HKZKSVJEUZAKKWIV6BJJO6XYE46Z3ZHO7TCI647M3V42IJE"
)

func serviceResponse(t *testing.T, userID string, serverID string, uJwt string, errMsg string, expires time.Duration) []byte {
	cr := jwt.NewAuthorizationResponseClaims(userID)
	cr.Audience = serverID
	cr.Error = errMsg
	cr.Jwt = uJwt
	if expires != 0 {
		cr.Expires = time.Now().Add(expires).Unix()
	}
	aa, err := nkeys.FromSeed([]byte(authCalloutIssuerSeed))
	require_NoError(t, err)
	token, err := cr.Encode(aa)
	require_NoError(t, err)
	return []byte(token)
}

func newScopedRole(t *testing.T, role string, pub []string, sub []string, allowResponses bool) (*jwt.UserScope, nkeys.KeyPair) {
	akp, pk := createKey(t)
	r := jwt.NewUserScope()
	r.Key = pk
	r.Template.Sub.Allow.Add(sub...)
	r.Template.Pub.Allow.Add(pub...)
	if allowResponses {
		r.Template.Resp = &jwt.ResponsePermission{
			MaxMsgs: 1,
			Expires: time.Second * 3,
		}
	}
	r.Role = role
	return r, akp
}

// Will create a signed user jwt as an authorized user.
func createAuthUser(t *testing.T, user, name, account, issuerAccount string, akp nkeys.KeyPair, expires time.Duration, limits *jwt.UserPermissionLimits) string {
	t.Helper()

	if akp == nil {
		var err error
		akp, err = nkeys.FromSeed([]byte(authCalloutIssuerSeed))
		require_NoError(t, err)
	}

	uc := jwt.NewUserClaims(user)
	if issuerAccount != "" {
		if _, err := nkeys.FromPublicKey(issuerAccount); err != nil {
			t.Fatalf("issuer account is not a public key: %v", err)
		}
		uc.IssuerAccount = issuerAccount
	}
	// The callout uses the audience as the target account
	// only if in non-operator mode, otherwise the user JWT has
	// correct attribution - issuer or issuer_account
	if _, err := nkeys.FromPublicKey(account); err != nil {
		// if it is not a public key, set the audience
		uc.Audience = account
	}

	if name != _EMPTY_ {
		uc.Name = name
	}
	if expires != 0 {
		uc.Expires = time.Now().Add(expires).Unix()
	}
	if limits != nil {
		uc.UserPermissionLimits = *limits
	}

	vr := jwt.CreateValidationResults()
	uc.Validate(vr)
	require_Len(t, len(vr.Errors()), 0)

	tok, err := uc.Encode(akp)
	require_NoError(t, err)

	return tok
}

type authTest struct {
	t          *testing.T
	srv        *Server
	conf       string
	authClient *nats.Conn
	clients    []*nats.Conn
}

func NewAuthTest(t *testing.T, config string, authHandler nats.MsgHandler, clientOptions ...nats.Option) *authTest {
	a := &authTest{t: t}
	a.conf = createConfFile(t, []byte(config))
	a.srv, _ = RunServerWithConfig(a.conf)

	var err error
	a.authClient = a.ConnectCallout(clientOptions...)
	_, err = a.authClient.Subscribe(AuthCalloutSubject, authHandler)
	require_NoError(t, err)
	return a
}

func (at *authTest) NewClient(clientOptions ...nats.Option) (*nats.Conn, error) {
	conn, err := nats.Connect(at.srv.ClientURL(), clientOptions...)
	if err != nil {
		return nil, err
	}
	at.clients = append(at.clients, conn)
	return conn, nil
}

func (at *authTest) ConnectCallout(clientOptions ...nats.Option) *nats.Conn {
	conn, err := at.NewClient(clientOptions...)
	if err != nil {
		err = fmt.Errorf("callout client failed: %w", err)
	}
	require_NoError(at.t, err)
	return conn
}

func (at *authTest) Connect(clientOptions ...nats.Option) *nats.Conn {
	conn, err := at.NewClient(clientOptions...)
	require_NoError(at.t, err)
	return conn
}

func (at *authTest) WSNewClient(clientOptions ...nats.Option) (*nats.Conn, error) {
	pi := at.srv.PortsInfo(10 * time.Millisecond)
	require_False(at.t, pi == nil)

	// test cert is SAN to DNS localhost, not local IPs returned by server in test environments
	wssUrl := strings.Replace(pi.WebSocket[0], "127.0.0.1", "localhost", 1)

	// Seeing 127.0.1.1 in some test environments...
	wssUrl = strings.Replace(wssUrl, "127.0.1.1", "localhost", 1)

	conn, err := nats.Connect(wssUrl, clientOptions...)
	if err != nil {
		return nil, err
	}
	at.clients = append(at.clients, conn)
	return conn, nil
}

func (at *authTest) WSConnect(clientOptions ...nats.Option) *nats.Conn {
	conn, err := at.WSNewClient(clientOptions...)
	require_NoError(at.t, err)
	return conn
}

func (at *authTest) RequireConnectError(clientOptions ...nats.Option) {
	_, err := at.NewClient(clientOptions...)
	require_Error(at.t, err)
}

func (at *authTest) Cleanup() {
	if at.authClient != nil {
		at.authClient.Close()
	}
	if at.srv != nil {
		at.srv.Shutdown()
		removeFile(at.t, at.conf)
	}
}

func TestAuthCalloutNoncePassthruBearerJWT(t *testing.T) {
	conf := `
        listen: "127.0.0.1:-1"
        server_name: A
        operator: eyJ0eXAiOiJKV1QiLCJhbGciOiJlZDI1NTE5LW5rZXkifQ.eyJqdGkiOiJJVVRKUzU0NktUTzRQNFVUV0Y2WE5LTkdINUpBM1pXUjdZVzJURkhBRVRWWTREV1E3UUZBIiwiaWF0IjoxNzc1ODM0Njg4LCJpc3MiOiJPQTRDQ1NBMlhBWDZOTEhKNzNCR0Q3SkFZNjdTREIySVNUNEpNM1ZNNVY1RkxEWllGRDdWTFJaNyIsIm5hbWUiOiJNeU9wZXJhdG9yIiwic3ViIjoiT0E0Q0NTQTJYQVg2TkxISjczQkdEN0pBWTY3U0RCMklTVDRKTTNWTTVWNUZMRFpZRkQ3VkxSWjciLCJuYXRzIjp7InNpZ25pbmdfa2V5cyI6WyJPQ05CSU1ONVJRUDRCTFVYNEZXRVJKRlhPUkZQMk82VERBRTJCVVdYNUVBTEdCTFhVSFNOT0lQUSJdLCJhY2NvdW50X3NlcnZlcl91cmwiOiJuYXRzOi8vbG9jYWxob3N0OjQyMjIiLCJzeXN0ZW1fYWNjb3VudCI6IkFEU0FJNERUUDJEVFZOQzJBSDVLUks0QzVTVEE0SVE1RlNNN0I2UVpYVzNCT1VaNVhRRkhGVjRSIiwic3RyaWN0X3NpZ25pbmdfa2V5X3VzYWdlIjp0cnVlLCJ0eXBlIjoib3BlcmF0b3IiLCJ2ZXJzaW9uIjoyfX0.FVrvVqzMbZCZsH2UEcgvNjvUz3Otj4MfZVFLUGjV2hKFDLV63Y3uQIqih8a4nBAUcNPC-sTK5GnhdkcH3DW_DQ
        system_account: ADSAI4DTP2DTVNC2AH5KRK4C5STA4IQ5FSM7B6QZXW3BOUZ5XQFHFV4R
        resolver: MEMORY
        resolver_preload: {
            # SYS
            ADSAI4DTP2DTVNC2AH5KRK4C5STA4IQ5FSM7B6QZXW3BOUZ5XQFHFV4R: eyJ0eXAiOiJKV1QiLCJhbGciOiJlZDI1NTE5LW5rZXkifQ.eyJqdGkiOiJZQUdHQlpKNERaTVdQT0dTTVhQUDRVV0dXUUc0UUNKUkszVUhRWE1IS1RSMlRUTUZTTUhRIiwiaWF0IjoxNzc1ODM0Njg4LCJpc3MiOiJPQ05CSU1ONVJRUDRCTFVYNEZXRVJKRlhPUkZQMk82VERBRTJCVVdYNUVBTEdCTFhVSFNOT0lQUSIsIm5hbWUiOiJTWVMiLCJzdWIiOiJBRFNBSTREVFAyRFRWTkMyQUg1S1JLNEM1U1RBNElRNUZTTTdCNlFaWFczQk9VWjVYUUZIRlY0UiIsIm5hdHMiOnsiZXhwb3J0cyI6W3sibmFtZSI6ImFjY291bnQtbW9uaXRvcmluZy1zdHJlYW1zIiwic3ViamVjdCI6IiRTWVMuQUNDT1VOVC4qLlx1MDAzZSIsInR5cGUiOiJzdHJlYW0iLCJhY2NvdW50X3Rva2VuX3Bvc2l0aW9uIjozLCJkZXNjcmlwdGlvbiI6IkFjY291bnQgc3BlY2lmaWMgbW9uaXRvcmluZyBzdHJlYW0iLCJpbmZvX3VybCI6Imh0dHBzOi8vZG9jcy5uYXRzLmlvL25hdHMtc2VydmVyL2NvbmZpZ3VyYXRpb24vc3lzX2FjY291bnRzIn0seyJuYW1lIjoiYWNjb3VudC1tb25pdG9yaW5nLXNlcnZpY2VzIiwic3ViamVjdCI6IiRTWVMuUkVRLkFDQ09VTlQuKi4qIiwidHlwZSI6InNlcnZpY2UiLCJyZXNwb25zZV90eXBlIjoiU3RyZWFtIiwiYWNjb3VudF90b2tlbl9wb3NpdGlvbiI6NCwiZGVzY3JpcHRpb24iOiJSZXF1ZXN0IGFjY291bnQgc3BlY2lmaWMgbW9uaXRvcmluZyBzZXJ2aWNlcyBmb3I6IFNVQlNaLCBDT05OWiwgTEVBRlosIEpTWiBhbmQgSU5GTyIsImluZm9fdXJsIjoiaHR0cHM6Ly9kb2NzLm5hdHMuaW8vbmF0cy1zZXJ2ZXIvY29uZmlndXJhdGlvbi9zeXNfYWNjb3VudHMifV0sImxpbWl0cyI6eyJzdWJzIjotMSwiZGF0YSI6LTEsInBheWxvYWQiOi0xLCJpbXBvcnRzIjotMSwiZXhwb3J0cyI6LTEsIndpbGRjYXJkcyI6dHJ1ZSwiY29ubiI6LTEsImxlYWYiOi0xfSwic2lnbmluZ19rZXlzIjpbIkFDWVRHUEtUNVVYR1dWRDQ0UVI0QllBSjNVM1VSWVNYRTNUNllNTE9YSVQ2VjNGQUxERVpST09BIl0sImRlZmF1bHRfcGVybWlzc2lvbnMiOnsicHViIjp7fSwic3ViIjp7fX0sImF1dGhvcml6YXRpb24iOnt9LCJ0eXBlIjoiYWNjb3VudCIsInZlcnNpb24iOjJ9fQ.cRx6dztXgu7GvTdPX4gZYVzp4_absTbPhLYGI2wWlJaMmNyk7NHeCjrG9jOblVVmm0j50z6i7c1q4b0xwf00CA
            # AUTH
            AAPYGIKL556K466JEXZYPWHGO7LZA45QLEBKKJ2QGPQGAZ5DV62TQTOD: eyJ0eXAiOiJKV1QiLCJhbGciOiJlZDI1NTE5LW5rZXkifQ.eyJqdGkiOiJDQ0xER1k2UDNDUFFMRlhSRU0yUUpBNTJKRTVDS1JHM0U1NTdKSjdTTEFNUkYzSDNKMk5BIiwiaWF0IjoxNzc1ODM0Njg5LCJpc3MiOiJPQ05CSU1ONVJRUDRCTFVYNEZXRVJKRlhPUkZQMk82VERBRTJCVVdYNUVBTEdCTFhVSFNOT0lQUSIsIm5hbWUiOiJBVVRIIiwic3ViIjoiQUFQWUdJS0w1NTZLNDY2SkVYWllQV0hHTzdMWkE0NVFMRUJLS0oyUUdQUUdBWjVEVjYyVFFUT0QiLCJuYXRzIjp7ImxpbWl0cyI6eyJzdWJzIjotMSwiZGF0YSI6LTEsInBheWxvYWQiOi0xLCJpbXBvcnRzIjotMSwiZXhwb3J0cyI6LTEsIndpbGRjYXJkcyI6dHJ1ZSwiY29ubiI6LTEsImxlYWYiOi0xLCJzdHJlYW1zIjotMSwiY29uc3VtZXIiOi0xLCJtYXhfYWNrX3BlbmRpbmciOi0xLCJtZW1fbWF4X3N0cmVhbV9ieXRlcyI6LTEsImRpc2tfbWF4X3N0cmVhbV9ieXRlcyI6LTF9LCJzaWduaW5nX2tleXMiOlsiQUFFUk5VN09ZM1haVTVKT0VGRUpTSk8yNFZITEdDT09BVzJERE1HVEdXRkpCU1ZJTVFSUFdaREUiXSwiZGVmYXVsdF9wZXJtaXNzaW9ucyI6eyJwdWIiOnt9LCJzdWIiOnt9fSwiYXV0aG9yaXphdGlvbiI6eyJhdXRoX3VzZXJzIjpbIlVDM1FBUVE3UVNTTkdWSTdTVU1GUFdGTFc0SzQ3T0oyTVlYSkpJWExIU0Q2N1dRTjIyUFJQQkczIl0sImFsbG93ZWRfYWNjb3VudHMiOlsiKiJdfSwidHlwZSI6ImFjY291bnQiLCJ2ZXJzaW9uIjoyfX0.LDvdKW7obcNabyVccdQB942v_VFsTwRTnsAadz0UaPbUSlOo-tWgoTmqznsrijyuuBzy6wl9LyuDWVeXs-L3BQ
            # APP
            ABD3HFCABJVXONMKEO5GP27J2YIT7EEYNUN5YPXQVBZCJXESTZASSWSA: eyJ0eXAiOiJKV1QiLCJhbGciOiJlZDI1NTE5LW5rZXkifQ.eyJqdGkiOiJBS0dHV1lZQ1NHMjNENEJCUlJKVlMzVlozSUkzWlZXTVpRUjNOTU1DQ01ER000TUZMTzVBIiwiaWF0IjoxNzc1ODM0Njg5LCJpc3MiOiJPQ05CSU1ONVJRUDRCTFVYNEZXRVJKRlhPUkZQMk82VERBRTJCVVdYNUVBTEdCTFhVSFNOT0lQUSIsIm5hbWUiOiJBUFAiLCJzdWIiOiJBQkQzSEZDQUJKVlhPTk1LRU81R1AyN0oyWUlUN0VFWU5VTjVZUFhRVkJaQ0pYRVNUWkFTU1dTQSIsIm5hdHMiOnsibGltaXRzIjp7InN1YnMiOi0xLCJkYXRhIjotMSwicGF5bG9hZCI6LTEsImltcG9ydHMiOi0xLCJleHBvcnRzIjotMSwid2lsZGNhcmRzIjp0cnVlLCJjb25uIjotMSwibGVhZiI6LTEsInN0cmVhbXMiOi0xLCJjb25zdW1lciI6LTEsIm1heF9hY2tfcGVuZGluZyI6LTEsIm1lbV9tYXhfc3RyZWFtX2J5dGVzIjotMSwiZGlza19tYXhfc3RyZWFtX2J5dGVzIjotMX0sInNpZ25pbmdfa2V5cyI6WyJBQUkyWE5YQ09LMklZS1VOSlozSVE1Rk1LNDdLVVE1VzZGM1o1RUlHWk5PTUFETjI2R0VBVUZCQiJdLCJkZWZhdWx0X3Blcm1pc3Npb25zIjp7InB1YiI6e30sInN1YiI6e319LCJhdXRob3JpemF0aW9uIjp7fSwidHlwZSI6ImFjY291bnQiLCJ2ZXJzaW9uIjoyfX0.ClYcKgq8lTK24JaYec_M1DlIEd95GOVlcoIE0tzSE497gaFKsfLZn8d0jM4HKZYu9hLZ5kewk9R-EnoCzjxIAA
        }
        default_sentinel: eyJ0eXAiOiJKV1QiLCJhbGciOiJlZDI1NTE5LW5rZXkifQ.eyJqdGkiOiJINEhaQVhZVUxBU1pPUVVKTEpUVFEzWDdSSkdQSkpGQTREUERYS1hZTElLVEU0NVVQTVdRIiwiaWF0IjoxNzc1ODM0Njg5LCJpc3MiOiJBQUVSTlU3T1kzWFpVNUpPRUZFSlNKTzI0VkhMR0NPT0FXMkRETUdUR1dGSkJTVklNUVJQV1pERSIsIm5hbWUiOiJkZWZhdWx0X3NlbnRpbmVsIiwic3ViIjoiVUNSS0lTRFZSRFlYNkVDTTNYRDJKSUVWWERPUzY0TzY0NUJZVU1SNEhLRlpES0FLWExBT1E0UVIiLCJuYXRzIjp7InB1YiI6eyJkZW55IjpbIlx1MDAzZSJdfSwic3ViIjp7ImRlbnkiOlsiXHUwMDNlIl19LCJzdWJzIjotMSwiZGF0YSI6LTEsInBheWxvYWQiOi0xLCJiZWFyZXJfdG9rZW4iOnRydWUsImlzc3Vlcl9hY2NvdW50IjoiQUFQWUdJS0w1NTZLNDY2SkVYWllQV0hHTzdMWkE0NVFMRUJLS0oyUUdQUUdBWjVEVjYyVFFUT0QiLCJ0eXBlIjoidXNlciIsInZlcnNpb24iOjJ9fQ.AVNsFUCG9X89C36omt65gw-oYUoRYlbD3HWfsKgC3YQGK2TMkSmIIJe0Cy0IpYWobS9MRbR3aAdQPGVzI7PuDw
        }`

	a := &authTest{t: t}
	a.conf = createConfFile(t, []byte(conf))
	a.srv, _ = RunServerWithConfig(a.conf)

	callouts := uint32(0)
	handler := func(m *nats.Msg) {
		atomic.AddUint32(&callouts, 1)
		user, si, ci, opts, _ := decodeAuthRequest(t, m.Data)
		require_True(t, si.Name == "A")
		require_True(t, ci.Host == "127.0.0.1")
		// Allow dlc user.
		if (opts.Username == "dlc" && opts.Password == "zzz") || opts.Token == "SECRET_TOKEN" {
			var j jwt.UserPermissionLimits
			j.Pub.Allow.Add("$SYS.>")
			j.Payload = 1024
			if opts.Token == "SECRET_TOKEN" {
				// Token MUST NOT be exposed in user info.
				require_Equal(t, ci.User, "[REDACTED]")
			}
			ujwt := createAuthUser(t, user, _EMPTY_, globalAccountName, "", nil, 10*time.Minute, &j)
			m.Respond(serviceResponse(t, user, si.ID, ujwt, "", 0))
		} else if opts.Username == "proxy" {
			var j jwt.UserPermissionLimits
			j.ProxyRequired = true
			ujwt := createAuthUser(t, user, _EMPTY_, globalAccountName, "", nil, 10*time.Minute, &j)
			m.Respond(serviceResponse(t, user, si.ID, ujwt, "", 0))
		} else {
			// Nil response signals no authentication.
			m.Respond(nil)
		}
	}

	var err error
	a.authClient = a.ConnectCallout(nats.UserJWTAndSeed("eyJ0eXAiOiJKV1QiLCJhbGciOiJlZDI1NTE5LW5rZXkifQ.eyJqdGkiOiIzMklNVFhPTDM0UUVXV0QyUjVBSVBDTDZESVNFUkZJWFdFVkFJSFZWNUk1SU9BUU82SVpBIiwiaWF0IjoxNzc1ODM0Njg4LCJpc3MiOiJBQUVSTlU3T1kzWFpVNUpPRUZFSlNKTzI0VkhMR0NPT0FXMkRETUdUR1dGSkJTVklNUVJQV1pERSIsIm5hbWUiOiJhdXRoIiwic3ViIjoiVUMzUUFRUTdRU1NOR1ZJN1NVTUZQV0ZMVzRLNDdPSjJNWVhKSklYTEhTRDY3V1FOMjJQUlBCRzMiLCJuYXRzIjp7InB1YiI6e30sInN1YiI6e30sInN1YnMiOi0xLCJkYXRhIjotMSwicGF5bG9hZCI6LTEsImlzc3Vlcl9hY2NvdW50IjoiQUFQWUdJS0w1NTZLNDY2SkVYWllQV0hHTzdMWkE0NVFMRUJLS0oyUUdQUUdBWjVEVjYyVFFUT0QiLCJ0eXBlIjoidXNlciIsInZlcnNpb24iOjJ9fQ.Lpwiyd4JKTf7jT_oHwlYezx0-K_qlTiKcNEkMVnvvRF4pKv5xF4XuZIoC1CGytq20lXL6gxSk11tp_BQHEbwBg", "SUADKMC6UBWLTM5VZZC67DUUM7KC2LFUYHE7HL22M63ROMMQA5LLRSJLH4"))
	_, err = a.authClient.Subscribe(AuthCalloutSubject, handler)
	require_NoError(t, err)
	//  return a

}

func TestAuthCalloutBasics(t *testing.T) {
	conf := `
		listen: "127.0.0.1:-1"
		server_name: A
		authorization {
			timeout: 1s
			users: [ { user: "auth", password: "pwd" } ]
			auth_callout {
				# Needs to be a public account nkey, will work for both server config and operator mode.
				issuer: "ABJHLOVMPA4CI6R5KLNGOB4GSLNIY7IOUPAJC4YFNDLQVIOBYQGUWVLA"
				# users that will power the auth callout service.
				auth_users: [ auth ]
			}
		}
	`
	callouts := uint32(0)
	handler := func(m *nats.Msg) {
		atomic.AddUint32(&callouts, 1)
		user, si, ci, opts, _ := decodeAuthRequest(t, m.Data)
		require_True(t, si.Name == "A")
		require_True(t, ci.Host == "127.0.0.1")
		// Allow dlc user.
		if (opts.Username == "dlc" && opts.Password == "zzz") || opts.Token == "SECRET_TOKEN" {
			var j jwt.UserPermissionLimits
			j.Pub.Allow.Add("$SYS.>")
			j.Payload = 1024
			if opts.Token == "SECRET_TOKEN" {
				// Token MUST NOT be exposed in user info.
				require_Equal(t, ci.User, "[REDACTED]")
			}
			ujwt := createAuthUser(t, user, _EMPTY_, globalAccountName, "", nil, 10*time.Minute, &j)
			m.Respond(serviceResponse(t, user, si.ID, ujwt, "", 0))
		} else if opts.Username == "proxy" {
			var j jwt.UserPermissionLimits
			j.ProxyRequired = true
			ujwt := createAuthUser(t, user, _EMPTY_, globalAccountName, "", nil, 10*time.Minute, &j)
			m.Respond(serviceResponse(t, user, si.ID, ujwt, "", 0))
		} else {
			// Nil response signals no authentication.
			m.Respond(nil)
		}
	}
	at := NewAuthTest(t, conf, handler, nats.UserInfo("auth", "pwd"))
	defer at.Cleanup()

	// This one should fail since bad password.
	at.RequireConnectError(nats.UserInfo("dlc", "xxx"))

	// This one should fail because it will require to be proxied and it is not.
	at.RequireConnectError(nats.UserInfo("proxy", "xxx"))

	// This one will use callout since not defined in server config.
	nc := at.Connect(nats.UserInfo("dlc", "zzz"))
	defer nc.Close()

	resp, err := nc.Request(userDirectInfoSubj, nil, time.Second)
	require_NoError(t, err)
	response := ServerAPIResponse{Data: &UserInfo{}}
	err = json.Unmarshal(resp.Data, &response)
	require_NoError(t, err)

	userInfo := response.Data.(*UserInfo)

	dlc := &UserInfo{
		UserID:      "dlc",
		Account:     globalAccountName,
		AccountName: globalAccountName,
		Permissions: &Permissions{
			Publish: &SubjectPermission{
				Allow: []string{"$SYS.>"},
				Deny:  []string{AuthCalloutSubject}, // Will be auto-added since in auth account.
			},
			Subscribe: &SubjectPermission{},
		},
	}
	expires := userInfo.Expires
	userInfo.Expires = 0
	if !reflect.DeepEqual(dlc, userInfo) {
		t.Fatalf("User info for %q did not match", "dlc")
	}
	if expires > 10*time.Minute || expires < (10*time.Minute-5*time.Second) {
		t.Fatalf("Expected expires of ~%v, got %v", 10*time.Minute, expires)
	}

	// Callout with a token should also work, regardless of it being redacted in the user info.
	nc.Close()
	nc = at.Connect(nats.Token("SECRET_TOKEN"))
	defer nc.Close()

	resp, err = nc.Request(userDirectInfoSubj, nil, time.Second)
	require_NoError(t, err)
	response = ServerAPIResponse{Data: &UserInfo{}}
	err = json.Unmarshal(resp.Data, &response)
	require_NoError(t, err)

	userInfo = response.Data.(*UserInfo)
	dlc = &UserInfo{
		// Token MUST NOT be exposed in user info.
		UserID:      "[REDACTED]",
		Account:     globalAccountName,
		AccountName: globalAccountName,
		Permissions: &Permissions{
			Publish: &SubjectPermission{
				Allow: []string{"$SYS.>"},
				Deny:  []string{AuthCalloutSubject}, // Will be auto-added since in auth account.
			},
			Subscribe: &SubjectPermission{},
		},
	}
	expires = userInfo.Expires
	userInfo.Expires = 0
	if !reflect.DeepEqual(dlc, userInfo) {
		t.Fatalf("User info for %q did not match", "dlc")
	}
	if expires > 10*time.Minute || expires < (10*time.Minute-5*time.Second) {
		t.Fatalf("Expected expires of ~%v, got %v", 10*time.Minute, expires)
	}
}

func TestAuthCalloutMQTTJwtPassedInConnectOptions(t *testing.T) {
	storeDir := t.TempDir()
	conf := fmt.Sprintf(`
		listen: "127.0.0.1:-1"
		server_name: A
		jetstream: { max_mem_store: 256MB, max_file_store: 1GB, store_dir: %q }
		mqtt { host: "127.0.0.1", port: -1 }
		authorization {
			timeout: 1s
			users: [ { user: "auth", password: "pwd" } ]
			auth_callout {
				issuer: "ABJHLOVMPA4CI6R5KLNGOB4GSLNIY7IOUPAJC4YFNDLQVIOBYQGUWVLA"
				auth_users: [ auth ]
			}
		}
	`, storeDir)

	ukp, err := nkeys.CreateUser()
	require_NoError(t, err)
	upub, err := ukp.PublicKey()
	require_NoError(t, err)
	expectedJWT := createAuthUser(t, upub, _EMPTY_, globalAccountName, "", nil, 10*time.Minute, nil)
	seenJWT := make(chan string, 1)
	handler := func(m *nats.Msg) {
		user, si, _, opts, _ := decodeAuthRequest(t, m.Data)
		select {
		case seenJWT <- opts.JWT:
		default:
		}
		if opts.JWT != expectedJWT {
			m.Respond(nil)
			return
		}
		ujwt := createAuthUser(t, user, _EMPTY_, globalAccountName, "", nil, 10*time.Minute, nil)
		m.Respond(serviceResponse(t, user, si.ID, ujwt, "", 0))
	}

	at := NewAuthTest(t, conf, handler, nats.UserInfo("auth", "pwd"))
	defer at.Cleanup()

	mc, rc := authCalloutMQTTConnect(t, at.srv.getOpts().MQTT.Host, at.srv.getOpts().MQTT.Port, "mqtt-auth-callout", "ignored", expectedJWT)
	defer mc.Close()

	select {
	case got := <-seenJWT:
		require_Equal(t, got, expectedJWT)
	case <-time.After(2 * time.Second):
		t.Fatal("Did not receive auth callout request")
	}
	require_Equal(t, rc, mqttConnAckRCConnectionAccepted)
}

func TestAuthCalloutNonOperatorMQTTOpaquePassword(t *testing.T) {
	storeDir := t.TempDir()
	conf := fmt.Sprintf(`
		listen: "127.0.0.1:-1"
		server_name: A
		jetstream: { max_mem_store: 256MB, max_file_store: 1GB, store_dir: %q }
		mqtt { host: "127.0.0.1", port: -1 }
		authorization {
			timeout: 1s
			users: [ { user: "auth", password: "pwd" } ]
			auth_callout {
				issuer: "ABJHLOVMPA4CI6R5KLNGOB4GSLNIY7IOUPAJC4YFNDLQVIOBYQGUWVLA"
				auth_users: [ auth ]
			}
		}
	`, storeDir)

	const opaquePassword = "external-jwt-or-token"
	type seenConnectOptions struct {
		jwt      string
		password string
	}
	seen := make(chan seenConnectOptions, 1)
	handler := func(m *nats.Msg) {
		user, si, _, opts, _ := decodeAuthRequest(t, m.Data)
		select {
		case seen <- seenConnectOptions{jwt: opts.JWT, password: opts.Password}:
		default:
		}
		if opts.JWT != _EMPTY_ || opts.Password != opaquePassword {
			m.Respond(nil)
			return
		}
		ujwt := createAuthUser(t, user, _EMPTY_, globalAccountName, "", nil, 10*time.Minute, nil)
		m.Respond(serviceResponse(t, user, si.ID, ujwt, "", 0))
	}

	at := NewAuthTest(t, conf, handler, nats.UserInfo("auth", "pwd"))
	defer at.Cleanup()

	mc, rc := authCalloutMQTTConnect(t, at.srv.getOpts().MQTT.Host, at.srv.getOpts().MQTT.Port, "mqtt-auth-callout", "ignored", opaquePassword)
	defer mc.Close()

	got := require_ChanRead(t, seen, 2*time.Second)
	require_Equal(t, got.jwt, _EMPTY_)
	require_Equal(t, got.password, opaquePassword)
	require_Equal(t, rc, mqttConnAckRCConnectionAccepted)
}

func TestAuthCalloutMultiAccounts(t *testing.T) {
	conf := `
		listen: "127.0.0.1:-1"
		server_name: ZZ
		accounts {
            AUTH { users [ {user: "auth", password: "pwd"} ] }
			FOO {}
			BAR {}
			BAZ {}
		}
		authorization {
			timeout: 1s
			auth_callout {
				# Needs to be a public account nkey, will work for both server config and operator mode.
				issuer: "ABJHLOVMPA4CI6R5KLNGOB4GSLNIY7IOUPAJC4YFNDLQVIOBYQGUWVLA"
				account: AUTH
				auth_users: [ auth ]
			}
		}
	`
	handler := func(m *nats.Msg) {
		user, si, ci, opts, _ := decodeAuthRequest(t, m.Data)
		require_True(t, si.Name == "ZZ")
		require_True(t, ci.Host == "127.0.0.1")
		// Allow dlc user and map to the BAZ account.
		if opts.Username == "dlc" && opts.Password == "zzz" {
			ujwt := createAuthUser(t, user, _EMPTY_, "BAZ", "", nil, 0, nil)
			m.Respond(serviceResponse(t, user, si.ID, ujwt, "", 0))
		} else {
			// Nil response signals no authentication.
			m.Respond(nil)
		}
	}

	at := NewAuthTest(t, conf, handler, nats.UserInfo("auth", "pwd"))
	defer at.Cleanup()

	// This one will use callout since not defined in server config.
	nc := at.Connect(nats.UserInfo("dlc", "zzz"))
	defer nc.Close()

	resp, err := nc.Request(userDirectInfoSubj, nil, time.Second)
	require_NoError(t, err)
	response := ServerAPIResponse{Data: &UserInfo{}}
	err = json.Unmarshal(resp.Data, &response)
	require_NoError(t, err)
	userInfo := response.Data.(*UserInfo)

	require_True(t, userInfo.UserID == "dlc")
	require_True(t, userInfo.Account == "BAZ")
}

func TestAuthCalloutAllowedAccounts(t *testing.T) {
	conf := `
		listen: "127.0.0.1:-1"
		server_name: ZZ
		accounts {
			AUTH { users [ {user: "auth", password: "pwd"} ] }
			FOO { users [ {user: "foo", password: "pwd"} ] }
			BAR {}
			SYS { users [ {user: "sys", password: "pwd"} ] }
		}
		system_account: SYS
		no_auth_user: foo
		authorization {
			timeout: 1s
			auth_callout {
				# Needs to be a public account nkey, will work for both server config and operator mode.
				issuer: "ABJHLOVMPA4CI6R5KLNGOB4GSLNIY7IOUPAJC4YFNDLQVIOBYQGUWVLA"
				account: AUTH
				auth_users: [ auth ]
				allowed_accounts: [ BAR ]
			}
		}
	`
	handler := func(m *nats.Msg) {
		t.Helper()
		user, si, ci, opts, _ := decodeAuthRequest(t, m.Data)
		require_True(t, si.Name == "ZZ")
		require_True(t, ci.Host == "127.0.0.1")
		// Allow dlc user and map to the BAZ account.
		if opts.Username == "dlc" && opts.Password == "zzz" {
			ujwt := createAuthUser(t, user, _EMPTY_, "BAR", "", nil, 0, nil)
			m.Respond(serviceResponse(t, user, si.ID, ujwt, "", 0))
		} else {
			// Nil response signals no authentication.
			m.Respond(nil)
		}
	}

	check := func(at *authTest, user, password, account string) {
		t.Helper()

		var nc *nats.Conn
		// Assume no auth user.
		if password == "" {
			nc = at.Connect()
		} else {
			nc = at.Connect(nats.UserInfo(user, password))
		}
		defer nc.Close()

		resp, err := nc.Request(userDirectInfoSubj, nil, time.Second)
		require_NoError(t, err)
		response := ServerAPIResponse{Data: &UserInfo{}}
		err = json.Unmarshal(resp.Data, &response)
		require_NoError(t, err)
		userInfo := response.Data.(*UserInfo)

		require_True(t, userInfo.UserID == user)
		require_True(t, userInfo.Account == account)
	}

	tests := []struct {
		user     string
		password string
		account  string
	}{
		{"dlc", "zzz", "BAR"},
		{"foo", "", "FOO"},
		{"sys", "pwd", "SYS"},
	}

	at := NewAuthTest(t, conf, handler, nats.UserInfo("auth", "pwd"))
	defer at.Cleanup()

	for _, test := range tests {
		t.Run(test.user, func(t *testing.T) {
			at.t = t
			check(at, test.user, test.password, test.account)
		})
	}
}

func TestAuthCalloutClientTLSCerts(t *testing.T) {
	conf := `
		listen: "localhost:-1"
		server_name: T

		tls {
			cert_file = "../test/configs/certs/tlsauth/server.pem"
			key_file = "../test/configs/certs/tlsauth/server-key.pem"
			ca_file = "../test/configs/certs/tlsauth/ca.pem"
			verify = true
		}

		accounts {
            AUTH { users [ {user: "auth", password: "pwd"} ] }
			FOO {}
		}
		authorization {
			timeout: 1s
			auth_callout {
				# Needs to be a public account nkey, will work for both server config and operator mode.
				issuer: "ABJHLOVMPA4CI6R5KLNGOB4GSLNIY7IOUPAJC4YFNDLQVIOBYQGUWVLA"
				account: AUTH
				auth_users: [ auth ]
			}
		}
	`
	handler := func(m *nats.Msg) {
		user, si, ci, _, ctls := decodeAuthRequest(t, m.Data)
		require_True(t, si.Name == "T")
		require_True(t, ci.Host == "127.0.0.1")
		require_True(t, ctls != nil)
		// Zero since we are verified and will be under verified chains.
		require_True(t, len(ctls.Certs) == 0)
		require_True(t, len(ctls.VerifiedChains) == 1)
		// Since we have a CA.
		require_True(t, len(ctls.VerifiedChains[0]) == 2)
		blk, _ := pem.Decode([]byte(ctls.VerifiedChains[0][0]))
		cert, err := x509.ParseCertificate(blk.Bytes)
		require_NoError(t, err)
		if strings.HasPrefix(cert.Subject.String(), "CN=example.com") {
			// Override blank name here, server will substitute.
			ujwt := createAuthUser(t, user, "dlc", "FOO", "", nil, 0, nil)
			m.Respond(serviceResponse(t, user, si.ID, ujwt, "", 0))
		}
	}

	ac := NewAuthTest(t, conf, handler,
		nats.UserInfo("auth", "pwd"),
		nats.ClientCert("../test/configs/certs/tlsauth/client2.pem", "../test/configs/certs/tlsauth/client2-key.pem"),
		nats.RootCAs("../test/configs/certs/tlsauth/ca.pem"))
	defer ac.Cleanup()

	// Will use client cert to determine user.
	nc := ac.Connect(
		nats.ClientCert("../test/configs/certs/tlsauth/client2.pem", "../test/configs/certs/tlsauth/client2-key.pem"),
		nats.RootCAs("../test/configs/certs/tlsauth/ca.pem"),
	)
	defer nc.Close()

	resp, err := nc.Request(userDirectInfoSubj, nil, time.Second)
	require_NoError(t, err)
	response := ServerAPIResponse{Data: &UserInfo{}}
	err = json.Unmarshal(resp.Data, &response)
	require_NoError(t, err)
	userInfo := response.Data.(*UserInfo)

	require_True(t, userInfo.UserID == "dlc")
	require_True(t, userInfo.Account == "FOO")
}

func TestAuthCalloutVerifiedUserCalloutsWithSig(t *testing.T) {
	conf := `
		listen: "127.0.0.1:-1"
		server_name: A
		authorization {
			timeout: 1s
			users: [
				{ user: "auth", password: "pwd" }
 				{ nkey: "UBO2MQV67TQTVIRV3XFTEZOACM4WLOCMCDMAWN5QVN5PI2N6JHTVDRON" }
 			]
			auth_callout {
				# Needs to be a public account nkey, will work for both server config and operator mode.
				issuer: "ABJHLOVMPA4CI6R5KLNGOB4GSLNIY7IOUPAJC4YFNDLQVIOBYQGUWVLA"
				# users that will power the auth callout service.
				auth_users: [ auth ]
			}
		}
	`
	callouts := uint32(0)
	handler := func(m *nats.Msg) {
		atomic.AddUint32(&callouts, 1)
		user, si, ci, opts, _ := decodeAuthRequest(t, m.Data)
		require_True(t, si.Name == "A")
		require_True(t, ci.Host == "127.0.0.1")
		require_True(t, opts.SignedNonce != _EMPTY_)
		require_True(t, ci.Nonce != _EMPTY_)
		ujwt := createAuthUser(t, user, _EMPTY_, globalAccountName, "", nil, 0, nil)
		m.Respond(serviceResponse(t, user, si.ID, ujwt, "", 0))
	}
	ac := NewAuthTest(t, conf, handler, nats.UserInfo("auth", "pwd"))
	defer ac.Cleanup()

	seedFile := createTempFile(t, _EMPTY_)
	defer removeFile(t, seedFile.Name())
	seedFile.WriteString(authCalloutSeed)
	nkeyOpt, err := nats.NkeyOptionFromSeed(seedFile.Name())
	require_NoError(t, err)

	nc := ac.Connect(nkeyOpt)
	defer nc.Close()

	// Make sure that the callout was called.
	if atomic.LoadUint32(&callouts) != 1 {
		t.Fatalf("Expected callout to be called")
	}

	resp, err := nc.Request(userDirectInfoSubj, nil, time.Second)
	require_NoError(t, err)
	response := ServerAPIResponse{Data: &UserInfo{}}
	err = json.Unmarshal(resp.Data, &response)
	require_NoError(t, err)

	userInfo := response.Data.(*UserInfo)

	dlc := &UserInfo{
		UserID:      "UBO2MQV67TQTVIRV3XFTEZOACM4WLOCMCDMAWN5QVN5PI2N6JHTVDRON",
		Account:     globalAccountName,
		AccountName: globalAccountName,
		Permissions: &Permissions{
			Publish: &SubjectPermission{
				Deny: []string{AuthCalloutSubject}, // Will be auto-added since in auth account.
			},
			Subscribe: &SubjectPermission{},
		},
	}
	if !reflect.DeepEqual(dlc, userInfo) {
		t.Fatalf("User info for %q did not match", "dlc")
	}
}

// For creating the authorized users in operator mode.
func createAuthServiceUser(t *testing.T, accKp nkeys.KeyPair) (pub, creds string) {
	t.Helper()
	ukp, _ := nkeys.CreateUser()
	seed, _ := ukp.Seed()
	upub, _ := ukp.PublicKey()
	uclaim := newJWTTestUserClaims()
	uclaim.Name = "auth-service"
	uclaim.Subject = upub
	vr := jwt.ValidationResults{}
	uclaim.Validate(&vr)
	require_Len(t, len(vr.Errors()), 0)
	ujwt, err := uclaim.Encode(accKp)
	require_NoError(t, err)
	return upub, genCredsFile(t, ujwt, seed)
}

func createBasicAccountUser(t *testing.T, accKp nkeys.KeyPair) (creds string) {
	return createBasicAccount(t, "auth-client", accKp, true)
}

func createBasicAccountLeaf(t *testing.T, accKp nkeys.KeyPair) (creds string) {
	return createBasicAccount(t, "auth-leaf", accKp, false)
}

func createBasicAccountBearer(t *testing.T, accKp nkeys.KeyPair) string {
	t.Helper()
	ukp, _ := nkeys.CreateUser()
	upub, _ := ukp.PublicKey()
	uclaim := newJWTTestUserClaims()
	uclaim.Name = "default_sentinel"
	uclaim.Subject = upub
	uclaim.BearerToken = true

	uclaim.Permissions.Pub.Deny.Add(">")
	uclaim.Permissions.Sub.Deny.Add(">")

	vr := jwt.ValidationResults{}
	uclaim.Validate(&vr)
	require_Len(t, len(vr.Errors()), 0)
	ujwt, err := uclaim.Encode(accKp)
	require_NoError(t, err)
	return ujwt
}

func createBasicAccount(t *testing.T, name string, accKp nkeys.KeyPair, addDeny bool) (creds string) {
	t.Helper()
	ukp, _ := nkeys.CreateUser()
	seed, _ := ukp.Seed()
	upub, _ := ukp.PublicKey()
	uclaim := newJWTTestUserClaims()
	uclaim.Subject = upub
	uclaim.Name = name
	if addDeny {
		// For these deny all permission
		uclaim.Permissions.Pub.Deny.Add(">")
		uclaim.Permissions.Sub.Deny.Add(">")
	}
	vr := jwt.ValidationResults{}
	uclaim.Validate(&vr)
	require_Len(t, len(vr.Errors()), 0)
	ujwt, err := uclaim.Encode(accKp)
	require_NoError(t, err)
	return genCredsFile(t, ujwt, seed)
}

func createScopedUser(t *testing.T, accKp nkeys.KeyPair, sk nkeys.KeyPair) (creds string) {
	t.Helper()
	ukp, _ := nkeys.CreateUser()
	seed, _ := ukp.Seed()
	upub, _ := ukp.PublicKey()
	uclaim := newJWTTestUserClaims()
	apk, _ := accKp.PublicKey()
	uclaim.IssuerAccount = apk
	uclaim.Subject = upub
	uclaim.Name = "scoped-user"
	uclaim.SetScoped(true)

	// Uncomment this to set the sub limits
	// uclaim.Limits.Subs = 0
	vr := jwt.ValidationResults{}
	uclaim.Validate(&vr)
	require_Len(t, len(vr.Errors()), 0)
	ujwt, err := uclaim.Encode(sk)
	require_NoError(t, err)
	return genCredsFile(t, ujwt, seed)
}

func TestAuthCalloutOperatorNoServerConfigCalloutAllowed(t *testing.T) {
	conf := createConfFile(t, []byte(fmt.Sprintf(`
		listen: 127.0.0.1:-1
		operator: %s
		resolver: MEM
		authorization {
			auth_callout {
				issuer: "ABJHLOVMPA4CI6R5KLNGOB4GSLNIY7IOUPAJC4YFNDLQVIOBYQGUWVLA"
				auth_users: [ auth ]
			}
		}
    `, ojwt)))
	defer removeFile(t, conf)

	opts := LoadConfig(conf)
	_, err := NewServer(opts)
	require_Error(t, err, errors.New("operators do not allow authorization callouts to be configured directly"))
}

func TestAuthCalloutOperatorModeBasics(t *testing.T) {
	_, spub := createKey(t)
	sysClaim := jwt.NewAccountClaims(spub)
	sysClaim.Name = "$SYS"
	sysJwt, err := sysClaim.Encode(oKp)
	require_NoError(t, err)

	// TEST account.
	tkp, tpub := createKey(t)
	tSigningKp, tSigningPub := createKey(t)
	accClaim := jwt.NewAccountClaims(tpub)
	accClaim.Name = "TEST"
	accClaim.SigningKeys.Add(tSigningPub)
	scope, scopedKp := newScopedRole(t, "foo", []string{"foo.>", "$SYS.REQ.USER.INFO"}, []string{"foo.>", "_INBOX.>"}, false)
	accClaim.SigningKeys.AddScopedSigner(scope)
	accJwt, err := accClaim.Encode(oKp)
	require_NoError(t, err)

	// AUTH service account.
	akp, err := nkeys.FromSeed([]byte(authCalloutIssuerSeed))
	require_NoError(t, err)

	apub, err := akp.PublicKey()
	require_NoError(t, err)

	// The authorized user for the service.
	upub, creds := createAuthServiceUser(t, akp)
	defer removeFile(t, creds)

	authClaim := jwt.NewAccountClaims(apub)
	authClaim.Name = "AUTH"
	authClaim.EnableExternalAuthorization(upub)
	authClaim.Authorization.AllowedAccounts.Add(tpub)
	authJwt, err := authClaim.Encode(oKp)
	require_NoError(t, err)

	defaultSentinel := createBasicAccountBearer(t, akp)

	conf := fmt.Sprintf(`
		listen: 127.0.0.1:-1
		operator: %s
		system_account: %s
		resolver: MEM
		resolver_preload: {
			%s: %s
			%s: %s
			%s: %s
		}
        default_sentinel: %s
    `, ojwt, spub, apub, authJwt, tpub, accJwt, spub, sysJwt, defaultSentinel)

	const secretToken = "--XX--"
	const dummyToken = "--ZZ--"
	const skKeyToken = "--SK--"
	const scopedToken = "--Scoped--"
	const badScopedToken = "--BADScoped--"
	const defaultToken = "--Default--"

	dkp, notAllowAccountPub := createKey(t)
	handler := func(m *nats.Msg) {
		user, si, _, opts, _ := decodeAuthRequest(t, m.Data)
		if opts.Token == secretToken {
			ujwt := createAuthUser(t, user, "dlc", tpub, "", tkp, 0, nil)
			m.Respond(serviceResponse(t, user, si.ID, ujwt, "", 0))
		} else if opts.Token == dummyToken {
			ujwt := createAuthUser(t, user, "dummy", notAllowAccountPub, "", dkp, 0, nil)
			m.Respond(serviceResponse(t, user, si.ID, ujwt, "", 0))
		} else if opts.Token == skKeyToken {
			ujwt := createAuthUser(t, user, "sk", tpub, tpub, tSigningKp, 0, nil)
			m.Respond(serviceResponse(t, user, si.ID, ujwt, "", 0))
		} else if opts.Token == scopedToken {
			// must have no limits set
			ujwt := createAuthUser(t, user, "scoped", tpub, tpub, scopedKp, 0, &jwt.UserPermissionLimits{})
			m.Respond(serviceResponse(t, user, si.ID, ujwt, "", 0))
		} else if opts.Token == badScopedToken {
			// limits are nil - here which result in a default user - this will fail scoped
			ujwt := createAuthUser(t, user, "bad-scoped", tpub, tpub, scopedKp, 0, nil)
			m.Respond(serviceResponse(t, user, si.ID, ujwt, "", 0))
		} else if opts.Token == defaultToken {
			ujwt := createAuthUser(t, user, "default", tpub, "", tkp, 0, nil)
			m.Respond(serviceResponse(t, user, si.ID, ujwt, "", 0))
		} else {
			m.Respond(nil)
		}
	}

	ac := NewAuthTest(t, conf, handler, nats.UserCredentials(creds))
	defer ac.Cleanup()
	resp, err := ac.authClient.Request(userDirectInfoSubj, nil, time.Second)
	require_NoError(t, err)
	response := ServerAPIResponse{Data: &UserInfo{}}
	err = json.Unmarshal(resp.Data, &response)
	require_NoError(t, err)

	userInfo := response.Data.(*UserInfo)
	expected := &UserInfo{
		UserID:      upub,
		Account:     apub,
		AccountName: "AUTH",
		UserName:    "auth-service",
		Permissions: &Permissions{
			Publish: &SubjectPermission{
				Deny: []string{AuthCalloutSubject}, // Will be auto-added since in auth account.
			},
			Subscribe: &SubjectPermission{},
		},
	}
	if !reflect.DeepEqual(expected, userInfo) {
		t.Fatalf("User info did not match expected, expected auto-deny permissions on callout subject")
	}

	// Bearer token etc..
	// This is used by all users, and the customization will be in other connect args.
	// This needs to also be bound to the authorization account.
	creds = createBasicAccountUser(t, akp)
	defer removeFile(t, creds)

	// We require a token.
	ac.RequireConnectError(nats.UserCredentials(creds))

	// Send correct token. This should switch us to the test account.
	nc := ac.Connect(nats.UserCredentials(creds), nats.Token(secretToken))
	require_NoError(t, err)
	defer nc.Close()

	resp, err = nc.Request(userDirectInfoSubj, nil, time.Second)
	require_NoError(t, err)
	response = ServerAPIResponse{Data: &UserInfo{}}
	err = json.Unmarshal(resp.Data, &response)
	require_NoError(t, err)

	userInfo = response.Data.(*UserInfo)

	// Make sure we switch accounts.
	if userInfo.Account != tpub {
		t.Fatalf("Expected to be switched to %q, but got %q", tpub, userInfo.Account)
	}

	// Now make sure that if the authorization service switches to an account that is not allowed, we reject.
	ac.RequireConnectError(nats.UserCredentials(creds), nats.Token(dummyToken))

	// Send the signing key token. This should switch us to the test account, but the user
	// is signed with the account signing key
	nc = ac.Connect(nats.UserCredentials(creds), nats.Token(skKeyToken))
	defer nc.Close()

	resp, err = nc.Request(userDirectInfoSubj, nil, time.Second)
	require_NoError(t, err)
	response = ServerAPIResponse{Data: &UserInfo{}}
	err = json.Unmarshal(resp.Data, &response)
	require_NoError(t, err)

	userInfo = response.Data.(*UserInfo)
	if userInfo.Account != tpub {
		t.Fatalf("Expected to be switched to %q, but got %q", tpub, userInfo.Account)
	}

	// bad scoped user
	ac.RequireConnectError(nats.UserCredentials(creds), nats.Token(badScopedToken))

	// Send the signing key token. This should switch us to the test account, but the user
	// is signed with the account signing key
	nc = ac.Connect(nats.UserCredentials(creds), nats.Token(scopedToken))
	require_NoError(t, err)
	defer nc.Close()

	resp, err = nc.Request(userDirectInfoSubj, nil, time.Second)
	require_NoError(t, err)
	response = ServerAPIResponse{Data: &UserInfo{}}
	err = json.Unmarshal(resp.Data, &response)
	require_NoError(t, err)

	userInfo = response.Data.(*UserInfo)
	if userInfo.Account != tpub {
		t.Fatalf("Expected to be switched to %q, but got %q", tpub, userInfo.Account)
	}
	require_True(t, len(userInfo.Permissions.Publish.Allow) == 2)
	slices.Sort(userInfo.Permissions.Publish.Allow)
	require_Equal(t, "foo.>", userInfo.Permissions.Publish.Allow[1])
	slices.Sort(userInfo.Permissions.Subscribe.Allow)
	require_True(t, len(userInfo.Permissions.Subscribe.Allow) == 2)
	require_Equal(t, "foo.>", userInfo.Permissions.Subscribe.Allow[1])

	// this connects without a credential, so will be assigned the default sentinel
	nc = ac.Connect(nats.Token(defaultToken))
	require_NoError(t, err)
	defer nc.Close()

	resp, err = nc.Request(userDirectInfoSubj, nil, time.Second)
	require_NoError(t, err)
	response = ServerAPIResponse{Data: &UserInfo{}}
	err = json.Unmarshal(resp.Data, &response)
	ui := response.Data.(*UserInfo)
	require_Equal(t, "default", ui.UserID)
	require_NoError(t, err)

}

func testAuthCalloutScopedUser(t *testing.T, allowAnyAccount bool) {
	_, spub := createKey(t)
	sysClaim := jwt.NewAccountClaims(spub)
	sysClaim.Name = "$SYS"
	sysJwt, err := sysClaim.Encode(oKp)
	require_NoError(t, err)

	// TEST account.
	_, tpub := createKey(t)
	_, tSigningPub := createKey(t)
	accClaim := jwt.NewAccountClaims(tpub)
	accClaim.Name = "TEST"
	accClaim.SigningKeys.Add(tSigningPub)
	scope, scopedKp := newScopedRole(t, "foo", []string{"foo.>", "$SYS.REQ.USER.INFO"}, []string{"foo.>", "_INBOX.>"}, true)
	scope.Template.Limits.Subs = 10
	scope.Template.Limits.Payload = 512
	accClaim.SigningKeys.AddScopedSigner(scope)
	accJwt, err := accClaim.Encode(oKp)
	require_NoError(t, err)

	// AUTH service account.
	akp, err := nkeys.FromSeed([]byte(authCalloutIssuerSeed))
	require_NoError(t, err)

	apub, err := akp.PublicKey()
	require_NoError(t, err)

	// The authorized user for the service.
	upub, creds := createAuthServiceUser(t, akp)
	defer removeFile(t, creds)

	authClaim := jwt.NewAccountClaims(apub)
	authClaim.Name = "AUTH"
	authClaim.EnableExternalAuthorization(upub)
	if allowAnyAccount {
		authClaim.Authorization.AllowedAccounts.Add("*")
	} else {
		authClaim.Authorization.AllowedAccounts.Add(tpub)
	}
	// the scope for the bearer token which has no permissions
	sentinelScope, authKP := newScopedRole(t, "sentinel", nil, nil, false)
	sentinelScope.Template.Sub.Deny.Add(">")
	sentinelScope.Template.Pub.Deny.Add(">")
	sentinelScope.Template.Limits.Subs = 0
	sentinelScope.Template.Payload = 0
	authClaim.SigningKeys.AddScopedSigner(sentinelScope)

	authJwt, err := authClaim.Encode(oKp)
	require_NoError(t, err)

	conf := fmt.Sprintf(`
        listen: 127.0.0.1:-1
        operator: %s
        system_account: %s
        resolver: MEM
        resolver_preload: {
            %s: %s
            %s: %s
            %s: %s
        }
    `, ojwt, spub, apub, authJwt, tpub, accJwt, spub, sysJwt)

	const scopedToken = "--Scoped--"
	handler := func(m *nats.Msg) {
		user, si, _, opts, _ := decodeAuthRequest(t, m.Data)
		if opts.Token == scopedToken {
			// must have no limits set
			ujwt := createAuthUser(t, user, "scoped", tpub, tpub, scopedKp, 0, &jwt.UserPermissionLimits{})
			m.Respond(serviceResponse(t, user, si.ID, ujwt, "", 0))
		} else {
			m.Respond(nil)
		}
	}

	ac := NewAuthTest(t, conf, handler, nats.UserCredentials(creds))
	defer ac.Cleanup()
	resp, err := ac.authClient.Request(userDirectInfoSubj, nil, time.Second)
	require_NoError(t, err)
	response := ServerAPIResponse{Data: &UserInfo{}}
	err = json.Unmarshal(resp.Data, &response)
	require_NoError(t, err)

	userInfo := response.Data.(*UserInfo)
	expected := &UserInfo{
		UserID:      upub,
		Account:     apub,
		AccountName: "AUTH",
		UserName:    "auth-service",
		Permissions: &Permissions{
			Publish: &SubjectPermission{
				Deny: []string{AuthCalloutSubject}, // Will be auto-added since in auth account.
			},
			Subscribe: &SubjectPermission{},
		},
	}
	if !reflect.DeepEqual(expected, userInfo) {
		t.Fatalf("User info did not match expected, expected auto-deny permissions on callout subject")
	}

	// Bearer token - this has no permissions see sentinelScope
	// This is used by all users, and the customization will be in other connect args.
	// This needs to also be bound to the authorization account.
	creds = createScopedUser(t, akp, authKP)
	defer removeFile(t, creds)

	// Send the signing key token. This should switch us to the test account, but the user
	// is signed with the account signing key
	nc := ac.Connect(nats.UserCredentials(creds), nats.Token(scopedToken))

	resp, err = nc.Request(userDirectInfoSubj, nil, time.Second)
	require_NoError(t, err)
	response = ServerAPIResponse{Data: &UserInfo{}}
	err = json.Unmarshal(resp.Data, &response)
	require_NoError(t, err)

	userInfo = response.Data.(*UserInfo)
	if userInfo.Account != tpub {
		t.Fatalf("Expected to be switched to %q, but got %q", tpub, userInfo.Account)
	}
	require_True(t, len(userInfo.Permissions.Publish.Allow) == 2)
	slices.Sort(userInfo.Permissions.Publish.Allow)
	require_Equal(t, "foo.>", userInfo.Permissions.Publish.Allow[1])
	slices.Sort(userInfo.Permissions.Subscribe.Allow)
	require_True(t, len(userInfo.Permissions.Subscribe.Allow) == 2)
	require_Equal(t, "foo.>", userInfo.Permissions.Subscribe.Allow[1])

	_, err = nc.Subscribe("foo.>", func(msg *nats.Msg) {
		t.Log("got request on foo.>")
		require_NoError(t, msg.Respond(nil))
	})
	require_NoError(t, err)

	m, err := nc.Request("foo.bar", nil, time.Second)
	require_NoError(t, err)
	require_NotNil(t, m)
	t.Log("go response from foo.bar")

	nc.Close()
}

func TestAuthCalloutScopedUserAssignedAccount(t *testing.T) {
	testAuthCalloutScopedUser(t, false)
}

func TestAuthCalloutScopedUserAllAccount(t *testing.T) {
	testAuthCalloutScopedUser(t, true)
}

const (
	curveSeed   = "SXAAXMRAEP6JWWHNB6IKFL554IE6LZVT6EY5MBRICPILTLOPHAG73I3YX4"
	curvePublic = "XAB3NANV3M6N7AHSQP2U5FRWKKUT7EG2ZXXABV4XVXYQRJGM4S2CZGHT"
)

func TestAuthCalloutServerConfigEncryption(t *testing.T) {
	tmpl := `
		listen: "127.0.0.1:-1"
		server_name: A
		authorization {
			timeout: 1s
			users: [ { user: "auth", password: "pwd" } ]
			auth_callout {
				# Needs to be a public account nkey, will work for both server config and operator mode.
				issuer: "ABJHLOVMPA4CI6R5KLNGOB4GSLNIY7IOUPAJC4YFNDLQVIOBYQGUWVLA"
				# users that will power the auth callout service.
				auth_users: [ auth ]
				# This is a public xkey (x25519). The auth service has the private key.
				xkey: "%s"
			}
		}
	`
	conf := fmt.Sprintf(tmpl, curvePublic)

	rkp, err := nkeys.FromCurveSeed([]byte(curveSeed))
	require_NoError(t, err)

	handler := func(m *nats.Msg) {
		// This will be encrypted.
		_, err := jwt.DecodeAuthorizationRequestClaims(string(m.Data))
		require_Error(t, err)

		xkey := m.Header.Get(AuthRequestXKeyHeader)
		require_True(t, xkey != _EMPTY_)
		decrypted, err := rkp.Open(m.Data, xkey)
		require_NoError(t, err)
		user, si, ci, opts, _ := decodeAuthRequest(t, decrypted)
		// The header xkey must match the signed xkey in server info.
		require_True(t, si.XKey == xkey)
		require_True(t, si.Name == "A")
		require_True(t, ci.Host == "127.0.0.1")
		// Allow dlc user.
		if opts.Username == "dlc" && opts.Password == "zzz" {
			ujwt := createAuthUser(t, user, _EMPTY_, globalAccountName, "", nil, 10*time.Minute, nil)
			m.Respond(serviceResponse(t, user, si.ID, ujwt, "", 0))
		} else if opts.Username == "dlc" && opts.Password == "xxx" {
			ujwt := createAuthUser(t, user, _EMPTY_, globalAccountName, "", nil, 10*time.Minute, nil)
			// Encrypt this response.
			data, err := rkp.Seal(serviceResponse(t, user, si.ID, ujwt, "", 0), si.XKey) // Server's public xkey.
			require_NoError(t, err)
			m.Respond(data)
		} else {
			// Nil response signals no authentication.
			m.Respond(nil)
		}
	}

	ac := NewAuthTest(t, conf, handler, nats.UserInfo("auth", "pwd"))
	defer ac.Cleanup()

	nc := ac.Connect(nats.UserInfo("dlc", "zzz"))
	defer nc.Close()

	// Authorization services can optionally encrypt the responses using the server's public xkey.
	nc = ac.Connect(nats.UserInfo("dlc", "xxx"))
	defer nc.Close()
}

func TestAuthCalloutOperatorModeEncryption(t *testing.T) {
	_, spub := createKey(t)
	sysClaim := jwt.NewAccountClaims(spub)
	sysClaim.Name = "$SYS"
	sysJwt, err := sysClaim.Encode(oKp)
	require_NoError(t, err)

	// TEST account.
	tkp, tpub := createKey(t)
	accClaim := jwt.NewAccountClaims(tpub)
	accClaim.Name = "TEST"
	accJwt, err := accClaim.Encode(oKp)
	require_NoError(t, err)

	// AUTH service account.
	akp, err := nkeys.FromSeed([]byte(authCalloutIssuerSeed))
	require_NoError(t, err)

	apub, err := akp.PublicKey()
	require_NoError(t, err)

	// The authorized user for the service.
	upub, creds := createAuthServiceUser(t, akp)
	defer removeFile(t, creds)

	authClaim := jwt.NewAccountClaims(apub)
	authClaim.Name = "AUTH"
	authClaim.EnableExternalAuthorization(upub)
	authClaim.Authorization.AllowedAccounts.Add(tpub)
	authClaim.Authorization.XKey = curvePublic

	authJwt, err := authClaim.Encode(oKp)
	require_NoError(t, err)

	conf := fmt.Sprintf(`
		listen: 127.0.0.1:-1
		operator: %s
		system_account: %s
		resolver: MEM
		resolver_preload: {
			%s: %s
			%s: %s
			%s: %s
		}
    `, ojwt, spub, apub, authJwt, tpub, accJwt, spub, sysJwt)

	rkp, err := nkeys.FromCurveSeed([]byte(curveSeed))
	require_NoError(t, err)

	const tokenA = "--XX--"
	const tokenB = "--ZZ--"

	handler := func(m *nats.Msg) {
		// Make sure this is an encrypted request.
		if bytes.HasPrefix(m.Data, []byte(jwtPrefix)) {
			t.Fatalf("Request not encrypted")
		}
		xkey := m.Header.Get(AuthRequestXKeyHeader)
		require_True(t, xkey != _EMPTY_)
		decrypted, err := rkp.Open(m.Data, xkey)
		require_NoError(t, err)
		user, si, ci, opts, _ := decodeAuthRequest(t, decrypted)
		// The header xkey must match the signed xkey in server info.
		require_True(t, si.XKey == xkey)
		require_True(t, ci.Host == "127.0.0.1")
		if opts.Token == tokenA {
			ujwt := createAuthUser(t, user, "dlc", tpub, "", tkp, 0, nil)
			m.Respond(serviceResponse(t, user, si.ID, ujwt, "", 0))
		} else if opts.Token == tokenB {
			ujwt := createAuthUser(t, user, "rip", tpub, "", tkp, 0, nil)
			// Encrypt this response.
			data, err := rkp.Seal(serviceResponse(t, user, si.ID, ujwt, "", 0), si.XKey) // Server's public xkey.
			require_NoError(t, err)
			m.Respond(data)
		} else {
			m.Respond(nil)
		}
	}

	ac := NewAuthTest(t, conf, handler, nats.UserCredentials(creds))
	defer ac.Cleanup()

	// Bearer token etc..
	// This is used by all users, and the customization will be in other connect args.
	// This needs to also be bound to the authorization account.
	creds = createBasicAccountUser(t, akp)
	defer removeFile(t, creds)

	// This will receive an encrypted request to the auth service but send plaintext response.
	nc := ac.Connect(nats.UserCredentials(creds), nats.Token(tokenA))
	defer nc.Close()

	// This will receive an encrypted request to the auth service and send an encrypted response.
	nc = ac.Connect(nats.UserCredentials(creds), nats.Token(tokenB))
	defer nc.Close()
}

func TestAuthCalloutServerTags(t *testing.T) {
	conf := `
		listen: "127.0.0.1:-1"
		server_name: A
		server_tags: ["foo", "bar"]
		authorization {
			users: [ { user: "auth", password: "pwd" } ]
			auth_callout {
				issuer: "ABJHLOVMPA4CI6R5KLNGOB4GSLNIY7IOUPAJC4YFNDLQVIOBYQGUWVLA"
				auth_users: [ auth ]
			}
		}
	`

	tch := make(chan jwt.TagList, 1)
	handler := func(m *nats.Msg) {
		user, si, _, _, _ := decodeAuthRequest(t, m.Data)
		tch <- si.Tags
		ujwt := createAuthUser(t, user, _EMPTY_, globalAccountName, "", nil, 10*time.Minute, nil)
		m.Respond(serviceResponse(t, user, si.ID, ujwt, "", 0))
	}

	ac := NewAuthTest(t, conf, handler, nats.UserInfo("auth", "pwd"))
	defer ac.Cleanup()

	nc := ac.Connect()
	defer nc.Close()

	tags := <-tch
	require_True(t, len(tags) == 2)
	require_True(t, tags.Contains("foo"))
	require_True(t, tags.Contains("bar"))
}

func TestAuthCalloutServerClusterAndVersion(t *testing.T) {
	conf := `
		listen: "127.0.0.1:-1"
		server_name: A
		authorization {
			users: [ { user: "auth", password: "pwd" } ]
			auth_callout {
				issuer: "ABJHLOVMPA4CI6R5KLNGOB4GSLNIY7IOUPAJC4YFNDLQVIOBYQGUWVLA"
				auth_users: [ auth ]
			}
		}
		cluster { name: HUB }
	`
	ch := make(chan string, 2)
	handler := func(m *nats.Msg) {
		user, si, _, _, _ := decodeAuthRequest(t, m.Data)
		ch <- si.Cluster
		ch <- si.Version
		ujwt := createAuthUser(t, user, _EMPTY_, globalAccountName, "", nil, 10*time.Minute, nil)
		m.Respond(serviceResponse(t, user, si.ID, ujwt, "", 0))
	}

	ac := NewAuthTest(t, conf, handler, nats.UserInfo("auth", "pwd"))
	defer ac.Cleanup()

	nc := ac.Connect()
	defer nc.Close()

	cluster := <-ch
	require_True(t, cluster == "HUB")

	version := <-ch
	require_True(t, len(version) > 0)
	ok, err := versionAtLeastCheckError(version, 2, 10, 0)
	require_NoError(t, err)
	require_True(t, ok)
}

func TestAuthCalloutErrorResponse(t *testing.T) {
	conf := `
		listen: "127.0.0.1:-1"
		server_name: A
		authorization {
			users: [ { user: "auth", password: "pwd" } ]
			auth_callout {
				issuer: "ABJHLOVMPA4CI6R5KLNGOB4GSLNIY7IOUPAJC4YFNDLQVIOBYQGUWVLA"
				auth_users: [ auth ]
			}
		}
	`
	handler := func(m *nats.Msg) {
		user, si, _, _, _ := decodeAuthRequest(t, m.Data)
		m.Respond(serviceResponse(t, user, si.ID, "", "BAD AUTH", 0))
	}

	ac := NewAuthTest(t, conf, handler, nats.UserInfo("auth", "pwd"))
	defer ac.Cleanup()

	ac.RequireConnectError(nats.UserInfo("dlc", "zzz"))
}

func TestAuthCalloutAuthUserFailDoesNotInvokeCallout(t *testing.T) {
	conf := `
		listen: "127.0.0.1:-1"
		server_name: A
		authorization {
			users: [ { user: "auth", password: "pwd" } ]
			auth_callout {
				issuer: "ABJHLOVMPA4CI6R5KLNGOB4GSLNIY7IOUPAJC4YFNDLQVIOBYQGUWVLA"
				auth_users: [ auth ]
			}
		}
	`
	callouts := uint32(0)
	handler := func(m *nats.Msg) {
		user, si, _, _, _ := decodeAuthRequest(t, m.Data)
		atomic.AddUint32(&callouts, 1)
		m.Respond(serviceResponse(t, user, si.ID, "", "WRONG PASSWORD", 0))
	}

	ac := NewAuthTest(t, conf, handler, nats.UserInfo("auth", "pwd"))
	defer ac.Cleanup()

	ac.RequireConnectError(nats.UserInfo("auth", "zzz"))

	if atomic.LoadUint32(&callouts) != 0 {
		t.Fatalf("Expected callout to not be called")
	}
}

func TestAuthCalloutAuthErrEvents(t *testing.T) {
	conf := `
		listen: "127.0.0.1:-1"
		server_name: A
		accounts {
            AUTH { users [ {user: "auth", password: "pwd"} ] }
			FOO {}
			BAR {}
		}
		authorization {
			auth_callout {
				issuer: "ABJHLOVMPA4CI6R5KLNGOB4GSLNIY7IOUPAJC4YFNDLQVIOBYQGUWVLA"
				account: AUTH
				auth_users: [ auth ]
			}
		}
	`

	handler := func(m *nats.Msg) {
		user, si, _, opts, _ := decodeAuthRequest(t, m.Data)
		// Allow dlc user and map to the BAZ account.
		if opts.Username == "dlc" && opts.Password == "zzz" {
			ujwt := createAuthUser(t, user, _EMPTY_, "FOO", "", nil, 0, nil)
			m.Respond(serviceResponse(t, user, si.ID, ujwt, "", 0))
		} else if opts.Username == "dlc" {
			m.Respond(serviceResponse(t, user, si.ID, "", "WRONG PASSWORD", 0))
		} else if opts.Username == "rip" {
			m.Respond(serviceResponse(t, user, si.ID, "", "BAD CREDS", 0))
		} else if opts.Username == "proxy" {
			var j jwt.UserPermissionLimits
			j.ProxyRequired = true
			ujwt := createAuthUser(t, user, _EMPTY_, "FOO", "", nil, 0, &j)
			m.Respond(serviceResponse(t, user, si.ID, ujwt, "", 0))
		}
	}

	ac := NewAuthTest(t, conf, handler, nats.UserInfo("auth", "pwd"))
	defer ac.Cleanup()

	// This is where the event fires, in this account.
	sub, err := ac.authClient.SubscribeSync(authErrorAccountEventSubj)
	require_NoError(t, err)

	// This one will use callout since not defined in server config.
	nc := ac.Connect(nats.UserInfo("dlc", "zzz"))
	defer nc.Close()
	checkSubsPending(t, sub, 0)

	checkAuthErrEvent := func(user, pass, reason string) {
		ac.RequireConnectError(nats.UserInfo(user, pass))

		m, err := sub.NextMsg(time.Second)
		require_NoError(t, err)

		var dm DisconnectEventMsg
		err = json.Unmarshal(m.Data, &dm)
		require_NoError(t, err)

		// Convert both reasons to lower case to do the comparison.
		dmr := strings.ToLower(dm.Reason)
		r := strings.ToLower(reason)

		if !strings.Contains(dmr, r) {
			t.Fatalf("Expected %q reason, but got %q", r, dmr)
		}
	}

	checkAuthErrEvent("dlc", "xxx", "WRONG PASSWORD")
	checkAuthErrEvent("rip", "abc", "BAD CREDS")
	// The auth callout uses as the reason the error string, not a closed state.
	checkAuthErrEvent("proxy", "proxy", ErrAuthProxyRequired.Error())
}

func TestAuthCalloutConnectEvents(t *testing.T) {
	conf := `
		listen: "127.0.0.1:-1"
		server_name: A
		accounts {
            AUTH { users [ {user: "auth", password: "pwd"} ] }
			FOO {}
			BAR {}
			$SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] }
		}
		authorization {
			auth_callout {
				issuer: "ABJHLOVMPA4CI6R5KLNGOB4GSLNIY7IOUPAJC4YFNDLQVIOBYQGUWVLA"
				account: AUTH
				auth_users: [ auth, admin ]
			}
		}
	`

	handler := func(m *nats.Msg) {
		user, si, _, opts, _ := decodeAuthRequest(t, m.Data)
		// Allow dlc user and map to the BAZ account.
		if opts.Username == "dlc" && opts.Password == "zzz" {
			ujwt := createAuthUser(t, user, _EMPTY_, "FOO", "", nil, 0, nil)
			m.Respond(serviceResponse(t, user, si.ID, ujwt, "", 0))
		} else if opts.Username == "rip" && opts.Password == "xxx" {
			ujwt := createAuthUser(t, user, _EMPTY_, "BAR", "", nil, 0, nil)
			m.Respond(serviceResponse(t, user, si.ID, ujwt, "", 0))
		} else {
			m.Respond(serviceResponse(t, user, si.ID, "", "BAD CREDS", 0))
		}
	}

	ac := NewAuthTest(t, conf, handler, nats.UserInfo("auth", "pwd"))
	defer ac.Cleanup()

	// Setup system user.
	snc := ac.Connect(nats.UserInfo("admin", "s3cr3t!"))
	defer snc.Close()

	// Allow this connect event to pass us by..
	time.Sleep(250 * time.Millisecond)

	// Watch for connect events.
	csub, err := snc.SubscribeSync(fmt.Sprintf(connectEventSubj, "*"))
	require_NoError(t, err)

	// Watch for disconnect events.
	dsub, err := snc.SubscribeSync(fmt.Sprintf(disconnectEventSubj, "*"))
	require_NoError(t, err)

	// Connections updates. Old
	acOldSub, err := snc.SubscribeSync(fmt.Sprintf(accConnsEventSubjOld, "*"))
	require_NoError(t, err)

	// Connections updates. New
	acNewSub, err := snc.SubscribeSync(fmt.Sprintf(accConnsEventSubjNew, "*"))
	require_NoError(t, err)

	snc.Flush()

	checkConnectEvents := func(user, pass, acc string) {
		nc := ac.Connect(nats.UserInfo(user, pass))
		require_NoError(t, err)

		m, err := csub.NextMsg(time.Second)
		require_NoError(t, err)

		var cm ConnectEventMsg
		err = json.Unmarshal(m.Data, &cm)
		require_NoError(t, err)
		require_True(t, cm.Client.User == user)
		require_True(t, cm.Client.Account == acc)

		// Check that we have updates, 1 each, for the connections updates.
		m, err = acOldSub.NextMsg(time.Second)
		require_NoError(t, err)

		var anc AccountNumConns
		err = json.Unmarshal(m.Data, &anc)
		require_NoError(t, err)
		require_True(t, anc.AccountStat.Account == acc)
		require_True(t, anc.AccountStat.Conns == 1)

		m, err = acNewSub.NextMsg(time.Second)
		require_NoError(t, err)

		err = json.Unmarshal(m.Data, &anc)
		require_NoError(t, err)
		require_True(t, anc.AccountStat.Account == acc)
		require_True(t, anc.AccountStat.Conns == 1)

		// Force the disconnect.
		nc.Close()

		m, err = dsub.NextMsg(time.Second)
		require_NoError(t, err)

		var dm DisconnectEventMsg
		err = json.Unmarshal(m.Data, &dm)
		require_NoError(t, err)

		m, err = acOldSub.NextMsg(time.Second)
		require_NoError(t, err)
		err = json.Unmarshal(m.Data, &anc)
		require_NoError(t, err)
		require_True(t, anc.AccountStat.Account == acc)
		require_True(t, anc.AccountStat.Conns == 0)

		m, err = acNewSub.NextMsg(time.Second)
		require_NoError(t, err)
		err = json.Unmarshal(m.Data, &anc)
		require_NoError(t, err)
		require_True(t, anc.AccountStat.Account == acc)
		require_True(t, anc.AccountStat.Conns == 0)

		// Make sure no double events sent.
		time.Sleep(200 * time.Millisecond)
		checkSubsPending(t, csub, 0)
		checkSubsPending(t, dsub, 0)
		checkSubsPending(t, acOldSub, 0)
		checkSubsPending(t, acNewSub, 0)
	}

	checkConnectEvents("dlc", "zzz", "FOO")
	checkConnectEvents("rip", "xxx", "BAR")
}

func TestAuthCalloutBadServer(t *testing.T) {
	conf := `
		listen: "127.0.0.1:-1"
		server_name: A
		accounts {
            AUTH { users [ {user: "auth", password: "pwd"} ] }
			FOO {}
		}
		authorization {
			auth_callout {
				issuer: "ABJHLOVMPA4CI6R5KLNGOB4GSLNIY7IOUPAJC4YFNDLQVIOBYQGUWVLA"
				account: AUTH
				auth_users: [ auth ]
			}
		}
	`

	handler := func(m *nats.Msg) {
		user, _, _, _, _ := decodeAuthRequest(t, m.Data)
		skp, err := nkeys.CreateServer()
		require_NoError(t, err)
		spk, err := skp.PublicKey()
		require_NoError(t, err)
		ujwt := createAuthUser(t, user, _EMPTY_, "FOO", "", nil, 0, nil)
		m.Respond(serviceResponse(t, user, spk, ujwt, "", 0))
	}

	ac := NewAuthTest(t, conf, handler, nats.UserInfo("auth", "pwd"))
	defer ac.Cleanup()

	// This is where the event fires, in this account.
	sub, err := ac.authClient.SubscribeSync(authErrorAccountEventSubj)
	require_NoError(t, err)

	checkAuthErrEvent := func(user, pass, reason string) {
		ac.RequireConnectError(nats.UserInfo(user, pass))

		m, err := sub.NextMsg(time.Second)
		require_NoError(t, err)

		var dm DisconnectEventMsg
		err = json.Unmarshal(m.Data, &dm)
		require_NoError(t, err)

		if !strings.Contains(dm.Reason, reason) {
			t.Fatalf("Expected %q reason, but got %q", reason, dm.Reason)
		}
	}
	checkAuthErrEvent("hello", "world", "response is not for server")
}

func TestAuthCalloutBadUser(t *testing.T) {
	conf := `
		listen: "127.0.0.1:-1"
		server_name: A
		accounts {
            AUTH { users [ {user: "auth", password: "pwd"} ] }
			FOO {}
		}
		authorization {
			auth_callout {
				issuer: "ABJHLOVMPA4CI6R5KLNGOB4GSLNIY7IOUPAJC4YFNDLQVIOBYQGUWVLA"
				account: AUTH
				auth_users: [ auth ]
			}
		}
	`

	handler := func(m *nats.Msg) {
		_, si, _, _, _ := decodeAuthRequest(t, m.Data)
		kp, err := nkeys.CreateUser()
		require_NoError(t, err)
		upk, err := kp.PublicKey()
		require_NoError(t, err)
		ujwt := createAuthUser(t, upk, _EMPTY_, "FOO", "", nil, 0, nil)
		m.Respond(serviceResponse(t, upk, si.ID, ujwt, "", 0))
	}

	ac := NewAuthTest(t, conf, handler, nats.UserInfo("auth", "pwd"))
	defer ac.Cleanup()

	// This is where the event fires, in this account.
	sub, err := ac.authClient.SubscribeSync(authErrorAccountEventSubj)
	require_NoError(t, err)

	checkAuthErrEvent := func(user, pass, reason string) {
		ac.RequireConnectError(nats.UserInfo(user, pass))

		m, err := sub.NextMsg(time.Second)
		require_NoError(t, err)

		var dm DisconnectEventMsg
		err = json.Unmarshal(m.Data, &dm)
		require_NoError(t, err)

		if !strings.Contains(dm.Reason, reason) {
			t.Fatalf("Expected %q reason, but got %q", reason, dm.Reason)
		}
	}
	checkAuthErrEvent("hello", "world", "auth callout response is not for expected user")
}

func TestAuthCalloutExpiredUser(t *testing.T) {
	conf := `
		listen: "127.0.0.1:-1"
		server_name: A
		accounts {
            AUTH { users [ {user: "auth", password: "pwd"} ] }
			FOO {}
		}
		authorization {
			auth_callout {
				issuer: "ABJHLOVMPA4CI6R5KLNGOB4GSLNIY7IOUPAJC4YFNDLQVIOBYQGUWVLA"
				account: AUTH
				auth_users: [ auth ]
			}
		}
	`

	handler := func(m *nats.Msg) {
		user, si, _, _, _ := decodeAuthRequest(t, m.Data)
		ujwt := createAuthUser(t, user, _EMPTY_, "FOO", "", nil, time.Second*-5, nil)
		m.Respond(serviceResponse(t, user, si.ID, ujwt, "", 0))
	}

	ac := NewAuthTest(t, conf, handler, nats.UserInfo("auth", "pwd"))
	defer ac.Cleanup()

	// This is where the event fires, in this account.
	sub, err := ac.authClient.SubscribeSync(authErrorAccountEventSubj)
	require_NoError(t, err)

	checkAuthErrEvent := func(user, pass, reason string) {
		ac.RequireConnectError(nats.UserInfo(user, pass))

		m, err := sub.NextMsg(time.Second)
		require_NoError(t, err)

		var dm DisconnectEventMsg
		err = json.Unmarshal(m.Data, &dm)
		require_NoError(t, err)

		if !strings.Contains(dm.Reason, reason) {
			t.Fatalf("Expected %q reason, but got %q", reason, dm.Reason)
		}
	}
	checkAuthErrEvent("hello", "world", "claim is expired")
}

func TestAuthCalloutExpiredResponse(t *testing.T) {
	conf := `
		listen: "127.0.0.1:-1"
		server_name: A
		accounts {
            AUTH { users [ {user: "auth", password: "pwd"} ] }
			FOO {}
		}
		authorization {
			auth_callout {
				issuer: "ABJHLOVMPA4CI6R5KLNGOB4GSLNIY7IOUPAJC4YFNDLQVIOBYQGUWVLA"
				account: AUTH
				auth_users: [ auth ]
			}
		}
	`

	handler := func(m *nats.Msg) {
		user, si, _, _, _ := decodeAuthRequest(t, m.Data)
		ujwt := createAuthUser(t, user, _EMPTY_, "FOO", "", nil, 0, nil)
		m.Respond(serviceResponse(t, user, si.ID, ujwt, "", time.Second*-5))
	}

	ac := NewAuthTest(t, conf, handler, nats.UserInfo("auth", "pwd"))
	defer ac.Cleanup()

	// This is where the event fires, in this account.
	sub, err := ac.authClient.SubscribeSync(authErrorAccountEventSubj)
	require_NoError(t, err)

	checkAuthErrEvent := func(user, pass, reason string) {
		ac.RequireConnectError(nats.UserInfo(user, pass))

		m, err := sub.NextMsg(time.Second)
		require_NoError(t, err)

		var dm DisconnectEventMsg
		err = json.Unmarshal(m.Data, &dm)
		require_NoError(t, err)

		if !strings.Contains(dm.Reason, reason) {
			t.Fatalf("Expected %q reason, but got %q", reason, dm.Reason)
		}
	}
	checkAuthErrEvent("hello", "world", "claim is expired")
}

func TestAuthCalloutOperator_AnyAccount(t *testing.T) {
	_, spub := createKey(t)
	sysClaim := jwt.NewAccountClaims(spub)
	sysClaim.Name = "$SYS"
	sysJwt, err := sysClaim.Encode(oKp)
	require_NoError(t, err)

	// A account.
	akp, apk := createKey(t)
	aClaim := jwt.NewAccountClaims(apk)
	aClaim.Name = "A"
	aJwt, err := aClaim.Encode(oKp)
	require_NoError(t, err)

	// B account.
	bkp, bpk := createKey(t)
	bClaim := jwt.NewAccountClaims(bpk)
	bClaim.Name = "B"
	bJwt, err := bClaim.Encode(oKp)
	require_NoError(t, err)

	// AUTH callout service account.
	ckp, err := nkeys.FromSeed([]byte(authCalloutIssuerSeed))
	require_NoError(t, err)

	cpk, err := ckp.PublicKey()
	require_NoError(t, err)

	// The authorized user for the service.
	upub, creds := createAuthServiceUser(t, ckp)
	defer removeFile(t, creds)

	authClaim := jwt.NewAccountClaims(cpk)
	authClaim.Name = "AUTH"
	authClaim.EnableExternalAuthorization(upub)
	authClaim.Authorization.AllowedAccounts.Add("*")
	authJwt, err := authClaim.Encode(oKp)
	require_NoError(t, err)

	conf := fmt.Sprintf(`
		listen: 127.0.0.1:-1
		operator: %s
		system_account: %s
		resolver: MEM
		resolver_preload: {
			%s: %s
			%s: %s
			%s: %s
			%s: %s
		}
    `, ojwt, spub, cpk, authJwt, apk, aJwt, bpk, bJwt, spub, sysJwt)

	handler := func(m *nats.Msg) {
		user, si, _, opts, _ := decodeAuthRequest(t, m.Data)
		if opts.Token == "PutMeInA" {
			ujwt := createAuthUser(t, user, "user_a", apk, "", akp, 0, nil)
			m.Respond(serviceResponse(t, user, si.ID, ujwt, "", 0))
		} else if opts.Token == "PutMeInB" {
			ujwt := createAuthUser(t, user, "user_b", bpk, "", bkp, 0, nil)
			m.Respond(serviceResponse(t, user, si.ID, ujwt, "", 0))
		} else {
			m.Respond(nil)
		}

	}

	ac := NewAuthTest(t, conf, handler, nats.UserCredentials(creds))
	defer ac.Cleanup()
	resp, err := ac.authClient.Request(userDirectInfoSubj, nil, time.Second)
	require_NoError(t, err)
	response := ServerAPIResponse{Data: &UserInfo{}}
	err = json.Unmarshal(resp.Data, &response)
	require_NoError(t, err)

	// Bearer token etc..
	// This is used by all users, and the customization will be in other connect args.
	// This needs to also be bound to the authorization account.
	creds = createBasicAccountUser(t, ckp)
	defer removeFile(t, creds)

	// We require a token.
	ac.RequireConnectError(nats.UserCredentials(creds))

	// Send correct token. This should switch us to the A account.
	nc := ac.Connect(nats.UserCredentials(creds), nats.Token("PutMeInA"))
	require_NoError(t, err)
	defer nc.Close()

	resp, err = nc.Request(userDirectInfoSubj, nil, time.Second)
	require_NoError(t, err)
	response = ServerAPIResponse{Data: &UserInfo{}}
	err = json.Unmarshal(resp.Data, &response)
	require_NoError(t, err)
	userInfo := response.Data.(*UserInfo)
	require_Equal(t, userInfo.Account, apk)

	nc = ac.Connect(nats.UserCredentials(creds), nats.Token("PutMeInB"))
	require_NoError(t, err)
	defer nc.Close()

	resp, err = nc.Request(userDirectInfoSubj, nil, time.Second)
	require_NoError(t, err)
	response = ServerAPIResponse{Data: &UserInfo{}}
	err = json.Unmarshal(resp.Data, &response)
	require_NoError(t, err)
	userInfo = response.Data.(*UserInfo)
	require_Equal(t, userInfo.Account, bpk)
}

func TestAuthCalloutWSClientTLSCerts(t *testing.T) {
	conf := `
		server_name: T
		listen: "localhost:-1"

		tls {
			cert_file = "../test/configs/certs/tlsauth/server.pem"
			key_file = "../test/configs/certs/tlsauth/server-key.pem"
			ca_file = "../test/configs/certs/tlsauth/ca.pem"
			verify = true
		}

		websocket: {
			listen: "localhost:-1"
			tls {
				cert_file = "../test/configs/certs/tlsauth/server.pem"
				key_file = "../test/configs/certs/tlsauth/server-key.pem"
				ca_file = "../test/configs/certs/tlsauth/ca.pem"
				verify = true
			}
		}

		accounts {
            AUTH { users [ {user: "auth", password: "pwd"} ] }
			FOO {}
		}
		authorization {
			timeout: 1s
			auth_callout {
				# Needs to be a public account nkey, will work for both server config and operator mode.
				issuer: "ABJHLOVMPA4CI6R5KLNGOB4GSLNIY7IOUPAJC4YFNDLQVIOBYQGUWVLA"
				account: AUTH
				auth_users: [ auth ]
			}
		}
	`
	handler := func(m *nats.Msg) {
		user, si, ci, _, ctls := decodeAuthRequest(t, m.Data)
		require_Equal(t, si.Name, "T")
		require_Equal(t, ci.Host, "127.0.0.1")
		require_NotEqual(t, ctls, nil)
		// Zero since we are verified and will be under verified chains.
		require_Equal(t, len(ctls.Certs), 0)
		require_Equal(t, len(ctls.VerifiedChains), 1)
		// Since we have a CA.
		require_Equal(t, len(ctls.VerifiedChains[0]), 2)
		blk, _ := pem.Decode([]byte(ctls.VerifiedChains[0][0]))
		cert, err := x509.ParseCertificate(blk.Bytes)
		require_NoError(t, err)
		if strings.HasPrefix(cert.Subject.String(), "CN=example.com") {
			// Override blank name here, server will substitute.
			ujwt := createAuthUser(t, user, "dlc", "FOO", "", nil, 0, nil)
			m.Respond(serviceResponse(t, user, si.ID, ujwt, "", 0))
		}
	}

	ac := NewAuthTest(t, conf, handler,
		nats.UserInfo("auth", "pwd"),
		nats.ClientCert("../test/configs/certs/tlsauth/client2.pem", "../test/configs/certs/tlsauth/client2-key.pem"),
		nats.RootCAs("../test/configs/certs/tlsauth/ca.pem"))
	defer ac.Cleanup()

	// Will use client cert to determine user.
	nc := ac.WSConnect(
		nats.ClientCert("../test/configs/certs/tlsauth/client2.pem", "../test/configs/certs/tlsauth/client2-key.pem"),
		nats.RootCAs("../test/configs/certs/tlsauth/ca.pem"),
	)
	defer nc.Close()

	resp, err := nc.Request(userDirectInfoSubj, nil, time.Second)
	require_NoError(t, err)
	response := ServerAPIResponse{Data: &UserInfo{}}
	err = json.Unmarshal(resp.Data, &response)
	require_NoError(t, err)
	userInfo := response.Data.(*UserInfo)

	require_Equal(t, userInfo.UserID, "dlc")
	require_Equal(t, userInfo.Account, "FOO")
}

func testConfClientClose(t *testing.T, respondNil bool) {
	conf := `
		listen: "127.0.0.1:-1"
		server_name: ZZ
		accounts {
            AUTH { users [ {user: "auth", password: "pwd"} ] }
		}
		authorization {
			timeout: 1s
			auth_callout {
				# Needs to be a public account nkey, will work for both server config and operator mode.
				issuer: "ABJHLOVMPA4CI6R5KLNGOB4GSLNIY7IOUPAJC4YFNDLQVIOBYQGUWVLA"
				account: AUTH
				auth_users: [ auth ]
			}
		}
	`
	handler := func(m *nats.Msg) {
		user, si, _, _, _ := decodeAuthRequest(t, m.Data)
		if respondNil {
			m.Respond(nil)
		} else {
			m.Respond(serviceResponse(t, user, si.ID, "", "not today", 0))
		}
	}

	at := NewAuthTest(t, conf, handler, nats.UserInfo("auth", "pwd"))
	defer at.Cleanup()

	// This one will use callout since not defined in server config.
	_, err := at.NewClient(nats.UserInfo("a", "x"))
	require_Error(t, err)
	require_True(t, strings.Contains(strings.ToLower(err.Error()), nats.AUTHORIZATION_ERR))
}

func TestAuthCallout_ClientAuthErrorConf(t *testing.T) {
	testConfClientClose(t, true)
	testConfClientClose(t, false)
}

func testAuthCall_ClientAuthErrorOperatorMode(t *testing.T, respondNil bool) {
	_, spub := createKey(t)
	sysClaim := jwt.NewAccountClaims(spub)
	sysClaim.Name = "$SYS"
	sysJwt, err := sysClaim.Encode(oKp)
	require_NoError(t, err)

	// AUTH service account.
	akp, err := nkeys.FromSeed([]byte(authCalloutIssuerSeed))
	require_NoError(t, err)

	apub, err := akp.PublicKey()
	require_NoError(t, err)

	// The authorized user for the service.
	upub, creds := createAuthServiceUser(t, akp)
	defer removeFile(t, creds)

	authClaim := jwt.NewAccountClaims(apub)
	authClaim.Name = "AUTH"
	authClaim.EnableExternalAuthorization(upub)
	authClaim.Authorization.AllowedAccounts.Add("*")

	// the scope for the bearer token which has no permissions
	sentinelScope, authKP := newScopedRole(t, "sentinel", nil, nil, false)
	sentinelScope.Template.Sub.Deny.Add(">")
	sentinelScope.Template.Pub.Deny.Add(">")
	sentinelScope.Template.Limits.Subs = 0
	sentinelScope.Template.Payload = 0
	authClaim.SigningKeys.AddScopedSigner(sentinelScope)

	authJwt, err := authClaim.Encode(oKp)
	require_NoError(t, err)

	conf := fmt.Sprintf(`
        listen: 127.0.0.1:-1
        operator: %s
        system_account: %s
        resolver: MEM
        resolver_preload: {
            %s: %s
            %s: %s
        }
    `, ojwt, spub, apub, authJwt, spub, sysJwt)

	handler := func(m *nats.Msg) {
		user, si, _, _, _ := decodeAuthRequest(t, m.Data)
		if respondNil {
			m.Respond(nil)
		} else {
			m.Respond(serviceResponse(t, user, si.ID, "", "not today", 0))
		}
	}

	ac := NewAuthTest(t, conf, handler, nats.UserCredentials(creds))
	defer ac.Cleanup()

	// Bearer token - this has no permissions see sentinelScope
	// This is used by all users, and the customization will be in other connect args.
	// This needs to also be bound to the authorization account.
	creds = createScopedUser(t, akp, authKP)
	defer removeFile(t, creds)

	// Send the signing key token. This should switch us to the test account, but the user
	// is signed with the account signing key
	_, err = ac.NewClient(nats.UserCredentials(creds))
	require_Error(t, err)
	require_True(t, strings.Contains(strings.ToLower(err.Error()), nats.AUTHORIZATION_ERR))
}

func TestAuthCallout_ClientAuthErrorOperatorMode(t *testing.T) {
	testAuthCall_ClientAuthErrorOperatorMode(t, true)
	testAuthCall_ClientAuthErrorOperatorMode(t, false)
}

func TestOperatorModeUserRevocation(t *testing.T) {
	skp, spub := createKey(t)
	sysClaim := jwt.NewAccountClaims(spub)
	sysClaim.Name = "$SYS"
	sysJwt, err := sysClaim.Encode(oKp)
	require_NoError(t, err)

	// TEST account.
	tkp, tpub := createKey(t)
	accClaim := jwt.NewAccountClaims(tpub)
	accClaim.Name = "TEST"
	accJwt, err := accClaim.Encode(oKp)
	require_NoError(t, err)

	// AUTH service account.
	akp, err := nkeys.FromSeed([]byte(authCalloutIssuerSeed))
	require_NoError(t, err)

	apub, err := akp.PublicKey()
	require_NoError(t, err)

	// The authorized user for the service.
	upub, creds := createAuthServiceUser(t, akp)
	defer removeFile(t, creds)

	authClaim := jwt.NewAccountClaims(apub)
	authClaim.Name = "AUTH"
	authClaim.EnableExternalAuthorization(upub)
	authClaim.Authorization.AllowedAccounts.Add(tpub)
	authJwt, err := authClaim.Encode(oKp)
	require_NoError(t, err)

	jwtDir, err := os.MkdirTemp("", "")
	require_NoError(t, err)
	defer func() {
		_ = os.RemoveAll(jwtDir)
	}()

	conf := fmt.Sprintf(`
		listen: 127.0.0.1:-1
		operator: %s
		system_account: %s
		resolver: {
			type: "full"
			dir: %s
			allow_delete: false
			interval: "2m"
			timeout: "1.9s"
		}
		resolver_preload: {
			%s: %s
			%s: %s
			%s: %s
		}
    `, ojwt, spub, jwtDir, apub, authJwt, tpub, accJwt, spub, sysJwt)

	const token = "--secret--"

	users := make(map[string]string)
	handler := func(m *nats.Msg) {
		user, si, _, opts, _ := decodeAuthRequest(t, m.Data)
		if opts.Token == token {
			// must have no limits set
			ujwt := createAuthUser(t, user, "user", tpub, tpub, tkp, 0, &jwt.UserPermissionLimits{})
			users[opts.Name] = ujwt
			m.Respond(serviceResponse(t, user, si.ID, ujwt, "", 0))
		} else {
			m.Respond(nil)
		}
	}

	ac := NewAuthTest(t, conf, handler, nats.UserCredentials(creds))
	defer ac.Cleanup()

	// create a system user
	_, sysCreds := createAuthServiceUser(t, skp)
	defer removeFile(t, sysCreds)
	// connect the system user
	sysNC, err := ac.NewClient(nats.UserCredentials(sysCreds))
	require_NoError(t, err)
	defer sysNC.Close()

	// Bearer token etc..
	// This is used by all users, and the customization will be in other connect args.
	// This needs to also be bound to the authorization account.
	creds = createBasicAccountUser(t, akp)
	defer removeFile(t, creds)

	var fwg sync.WaitGroup

	// connect three clients
	nc := ac.Connect(nats.UserCredentials(creds), nats.Name("first"), nats.Token(token), nats.NoReconnect(), nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
		if err != nil && strings.Contains(err.Error(), "authentication revoked") {
			fwg.Done()
		}
	}))
	fwg.Add(1)

	var swg sync.WaitGroup
	// connect another user
	ncA := ac.Connect(nats.UserCredentials(creds), nats.Token(token), nats.Name("second"), nats.NoReconnect(), nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
		if err != nil && strings.Contains(err.Error(), "authentication revoked") {
			swg.Done()
		}
	}))
	swg.Add(1)

	ncB := ac.Connect(nats.UserCredentials(creds), nats.Token(token), nats.NoReconnect(), nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
		if err != nil && strings.Contains(err.Error(), "authentication revoked") {
			swg.Done()
		}
	}))
	swg.Add(1)

	require_NoError(t, err)

	// revoke the user first - look at the JWT we issued
	uc, err := jwt.DecodeUserClaims(users["first"])
	require_NoError(t, err)
	// revoke the user in account
	accClaim.Revocations = make(map[string]int64)
	accClaim.Revocations.Revoke(uc.Subject, time.Now().Add(time.Minute))
	accJwt, err = accClaim.Encode(oKp)
	require_NoError(t, err)
	// send the update request
	updateAccount(t, sysNC, accJwt)

	// wait for the user to be disconnected with the error we expect
	fwg.Wait()
	require_Equal(t, nc.IsConnected(), false)

	// update the account to remove any revocations
	accClaim.Revocations = make(map[string]int64)
	accJwt, err = accClaim.Encode(oKp)
	require_NoError(t, err)
	updateAccount(t, sysNC, accJwt)
	// we should still be connected on the other 2 clients
	require_Equal(t, ncA.IsConnected(), true)
	require_Equal(t, ncB.IsConnected(), true)

	// update the jwt and revoke all users
	accClaim.Revocations.Revoke(jwt.All, time.Now().Add(time.Minute))
	accJwt, err = accClaim.Encode(oKp)
	require_NoError(t, err)
	updateAccount(t, sysNC, accJwt)

	swg.Wait()
	require_Equal(t, ncA.IsConnected(), false)
	require_Equal(t, ncB.IsConnected(), false)
}

func updateAccount(t *testing.T, sys *nats.Conn, jwtToken string) {
	ac, err := jwt.DecodeAccountClaims(jwtToken)
	require_NoError(t, err)
	r, err := sys.Request(fmt.Sprintf(`$SYS.REQ.ACCOUNT.%s.CLAIMS.UPDATE`, ac.Subject), []byte(jwtToken), time.Second*2)
	require_NoError(t, err)

	type data struct {
		Account string `json:"account"`
		Code    int    `json:"code"`
	}
	type serverResponse struct {
		Data data `json:"data"`
	}

	var response serverResponse
	err = json.Unmarshal(r.Data, &response)
	require_NoError(t, err)
	require_NotNil(t, response.Data)
	require_Equal(t, response.Data.Code, int(200))
}

func TestAuthCalloutLeafNodeAndOperatorMode(t *testing.T) {
	_, spub := createKey(t)
	sysClaim := jwt.NewAccountClaims(spub)
	sysClaim.Name = "$SYS"
	sysJwt, err := sysClaim.Encode(oKp)
	require_NoError(t, err)

	// A account.
	akp, apk := createKey(t)
	aClaim := jwt.NewAccountClaims(apk)
	aClaim.Name = "A"
	aJwt, err := aClaim.Encode(oKp)
	require_NoError(t, err)

	// AUTH callout service account.
	ckp, err := nkeys.FromSeed([]byte(authCalloutIssuerSeed))
	require_NoError(t, err)

	cpk, err := ckp.PublicKey()
	require_NoError(t, err)

	// The authorized user for the service.
	upub, creds := createAuthServiceUser(t, ckp)
	defer removeFile(t, creds)

	authClaim := jwt.NewAccountClaims(cpk)
	authClaim.Name = "AUTH"
	authClaim.EnableExternalAuthorization(upub)
	authClaim.Authorization.AllowedAccounts.Add("*")
	authJwt, err := authClaim.Encode(oKp)
	require_NoError(t, err)

	conf := fmt.Sprintf(`
		listen: 127.0.0.1:-1
		operator: %s
		system_account: %s
		resolver: MEM
		resolver_preload: {
			%s: %s
			%s: %s
			%s: %s
		}
		leafnodes {
			listen: "127.0.0.1:-1"
		}
    `, ojwt, spub, cpk, authJwt, apk, aJwt, spub, sysJwt)

	handler := func(m *nats.Msg) {
		user, si, _, opts, _ := decodeAuthRequest(t, m.Data)
		if (opts.Username == "leaf" && opts.Password == "pwd") || (opts.Token == "token") {
			ujwt := createAuthUser(t, user, "user_a", apk, "", akp, 0, nil)
			m.Respond(serviceResponse(t, user, si.ID, ujwt, "", 0))
		} else {
			m.Respond(nil)
		}
	}

	at := NewAuthTest(t, conf, handler, nats.UserCredentials(creds))
	defer at.Cleanup()

	ucreds := createBasicAccountUser(t, ckp)
	defer removeFile(t, ucreds)

	// This should switch us to the A account.
	nc := at.Connect(nats.UserCredentials(ucreds), nats.Token("token"))
	defer nc.Close()

	natsSub(t, nc, "foo", func(m *nats.Msg) {
		m.Respond([]byte("here"))
	})
	natsFlush(t, nc)

	// Create creds for the leaf account.
	lcreds := createBasicAccountLeaf(t, ckp)
	defer removeFile(t, lcreds)

	hopts := at.srv.getOpts()

	for _, test := range []struct {
		name string
		up   string
		ok   bool
	}{
		{"bad token", "tokenx", false},
		{"bad username and password", "leaf:pwdx", false},
		{"token", "token", true},
		{"username and password", "leaf:pwd", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			lconf := createConfFile(t, []byte(fmt.Sprintf(`
				listen: "127.0.0.1:-1"
				server_name: "LEAF"
				leafnodes {
					remotes [
						{
							url: "nats://%s@127.0.0.1:%d"
							credentials: "%s"
						}
					]
				}
			`, test.up, hopts.LeafNode.Port, lcreds)))
			leaf, _ := RunServerWithConfig(lconf)
			defer leaf.Shutdown()

			if !test.ok {
				// Expect failure to connect. Wait a bit before checking.
				time.Sleep(50 * time.Millisecond)
				checkLeafNodeConnectedCount(t, leaf, 0)
				return
			}

			checkLeafNodeConnected(t, leaf)

			checkSubInterest(t, leaf, globalAccountName, "foo", time.Second)

			ncl := natsConnect(t, leaf.ClientURL())
			defer ncl.Close()

			resp, err := ncl.Request("foo", []byte("hello"), time.Second)
			require_NoError(t, err)
			require_Equal(t, string(resp.Data), "here")
		})
	}
}

func TestAuthCalloutLeafNodeAndConfigMode(t *testing.T) {
	conf := `
		listen: "127.0.0.1:-1"
		accounts {
			AUTH { users [ {user: "auth", password: "pwd"} ] }
			A {}
		}
		authorization {
			timeout: 1s
			auth_callout {
				# Needs to be a public account nkey, will work for both server config and operator mode.
				issuer: "ABJHLOVMPA4CI6R5KLNGOB4GSLNIY7IOUPAJC4YFNDLQVIOBYQGUWVLA"
				account: AUTH
				auth_users: [ auth ]
			}
		}
		leafnodes {
			listen: "127.0.0.1:-1"
		}
	`
	handler := func(m *nats.Msg) {
		user, si, _, opts, _ := decodeAuthRequest(t, m.Data)
		if (opts.Username == "leaf" && opts.Password == "pwd") || (opts.Token == "token") {
			ujwt := createAuthUser(t, user, _EMPTY_, "A", "", nil, 0, nil)
			m.Respond(serviceResponse(t, user, si.ID, ujwt, "", 0))
		} else {
			m.Respond(nil)
		}
	}

	at := NewAuthTest(t, conf, handler, nats.UserInfo("auth", "pwd"))
	defer at.Cleanup()

	// This should switch us to the A account.
	nc := at.Connect(nats.Token("token"))
	defer nc.Close()

	natsSub(t, nc, "foo", func(m *nats.Msg) {
		m.Respond([]byte("here"))
	})
	natsFlush(t, nc)

	hopts := at.srv.getOpts()

	for _, test := range []struct {
		name string
		up   string
		ok   bool
	}{
		{"bad token", "tokenx", false},
		{"bad username and password", "leaf:pwdx", false},
		{"token", "token", true},
		{"username and password", "leaf:pwd", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			lconf := createConfFile(t, []byte(fmt.Sprintf(`
				listen: "127.0.0.1:-1"
				server_name: "LEAF"
				leafnodes {
					remotes [{url: "nats://%s@127.0.0.1:%d"}]
				}
			`, test.up, hopts.LeafNode.Port)))
			leaf, _ := RunServerWithConfig(lconf)
			defer leaf.Shutdown()

			if !test.ok {
				// Expect failure to connect. Wait a bit before checking.
				time.Sleep(50 * time.Millisecond)
				checkLeafNodeConnectedCount(t, leaf, 0)
				return
			}

			checkLeafNodeConnected(t, leaf)

			checkSubInterest(t, leaf, globalAccountName, "foo", time.Second)

			ncl := natsConnect(t, leaf.ClientURL())
			defer ncl.Close()

			resp, err := ncl.Request("foo", []byte("hello"), time.Second)
			require_NoError(t, err)
			require_Equal(t, string(resp.Data), "here")
		})
	}

}

func TestAuthCalloutProxyRequiredInUserNotInAuthJWT(t *testing.T) {
	conf := `
		listen: "127.0.0.1:-1"
		server_name: A
		accounts {
			AUTH: {
				users: [ { user: auth, password: auth } ]
			}
			APP: {
				users: [
					{ user: user, password: pwd }
					{ user: proxy, password: pwd, proxy_required: true }
				]
			}
			SYS: {}
		}
		system_account: SYS

		authorization {
			timeout: 1s
			auth_callout {
				issuer: "ABJHLOVMPA4CI6R5KLNGOB4GSLNIY7IOUPAJC4YFNDLQVIOBYQGUWVLA"
				auth_users: [ auth ]
				account: AUTH
			}
		}
	`
	var invoked atomic.Int32
	handler := func(m *nats.Msg) {
		invoked.Add(1)
		user, si, ci, opts, _ := decodeAuthRequest(t, m.Data)
		require_True(t, si.Name == "A")
		require_True(t, ci.Host == "127.0.0.1")
		if opts.Username == "proxy" {
			// If we don't set a ProxyRequired property explicitly here, but the
			// user has it, so it should still be rejected.
			ujwt := createAuthUser(t, user, _EMPTY_, "APP", _EMPTY_, nil, 10*time.Minute, nil)
			m.Respond(serviceResponse(t, user, si.ID, ujwt, _EMPTY_, 0))
		} else {
			m.Respond(nil)
		}
	}
	at := NewAuthTest(t, conf, handler, nats.UserInfo("auth", "auth"))
	defer at.Cleanup()

	sub, err := at.authClient.SubscribeSync(authErrorAccountEventSubj)
	require_NoError(t, err)

	// It should fail, even if the auth callout does not require proxy in its JWT.
	// In other words, we reject if not proxied and the user config or the auth JWT
	// requires proxy connection.
	at.RequireConnectError(nats.UserInfo("proxy", "pwd"))

	m, err := sub.NextMsg(time.Second)
	require_NoError(t, err)

	var dm DisconnectEventMsg
	err = json.Unmarshal(m.Data, &dm)
	require_NoError(t, err)

	// Convert both reasons to lower case to do the comparison.
	dmr := strings.ToLower(dm.Reason)
	r := strings.ToLower(ErrAuthProxyRequired.Error())
	if !strings.Contains(dmr, r) {
		t.Fatalf("Expected %q reason, but got %q", r, dmr)
	}

	// Auth callout should have been invoked once.
	require_Equal(t, 1, int(invoked.Load()))
}

func TestAuthCalloutOperatorModeMismatchedCalloutCreds(t *testing.T) {
	_, spub := createKey(t)
	sysClaim := jwt.NewAccountClaims(spub)
	sysClaim.Name = "$SYS"
	sysJwt, err := sysClaim.Encode(oKp)
	require_NoError(t, err)

	// AUTH callout service account.
	ckp, err := nkeys.FromSeed([]byte(authCalloutIssuerSeed))
	require_NoError(t, err)
	cpk, err := ckp.PublicKey()
	require_NoError(t, err)

	// The authorized user for the service.
	upub, _ := createAuthServiceUser(t, ckp)

	authClaim := jwt.NewAccountClaims(cpk)
	authClaim.Name = "AUTH"
	authClaim.EnableExternalAuthorization(upub)
	authClaim.Authorization.AllowedAccounts.Add("*")
	authJwt, err := authClaim.Encode(oKp)
	require_NoError(t, err)

	conf := createConfFile(t, []byte(fmt.Sprintf(`
		listen: 127.0.0.1:-1
		operator: %s
		system_account: %s
		resolver: MEM
		resolver_preload: {
			%s: %s
			%s: %s
		}
    `, ojwt, spub, cpk, authJwt, spub, sysJwt)))

	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	// Create mismatched creds for the auth service user:
	// JWT with the auth user's public key, but a different seed.
	ukpBad, _ := nkeys.CreateUser()
	seedBad, _ := ukpBad.Seed()

	uclaim := newJWTTestUserClaims()
	uclaim.Name = "auth-service"
	uclaim.Subject = upub // auth user's public key
	ujwt, err := uclaim.Encode(ckp)
	require_NoError(t, err)
	badCreds := genCredsFile(t, ujwt, seedBad)
	defer removeFile(t, badCreds)

	// Client auth will time out because the callout service didn't connect.
	_, err = nats.Connect(s.ClientURL(), nats.UserCredentials(badCreds), nats.MaxReconnects(0))
	require_Error(t, err)

	// Server should still be running.
	time.Sleep(500 * time.Millisecond)
	require_True(t, s.Running())
}

func TestAuthCalloutLeafNodeOperatorModeMismatchedCreds(t *testing.T) {
	_, spub := createKey(t)
	sysClaim := jwt.NewAccountClaims(spub)
	sysClaim.Name = "$SYS"
	sysJwt, err := sysClaim.Encode(oKp)
	require_NoError(t, err)

	// A account.
	akp, apk := createKey(t)
	aClaim := jwt.NewAccountClaims(apk)
	aClaim.Name = "A"
	aJwt, err := aClaim.Encode(oKp)
	require_NoError(t, err)

	// AUTH callout service account.
	ckp, err := nkeys.FromSeed([]byte(authCalloutIssuerSeed))
	require_NoError(t, err)
	cpk, err := ckp.PublicKey()
	require_NoError(t, err)

	// The authorized user for the service.
	upub, creds := createAuthServiceUser(t, ckp)
	defer removeFile(t, creds)

	authClaim := jwt.NewAccountClaims(cpk)
	authClaim.Name = "AUTH"
	authClaim.EnableExternalAuthorization(upub)
	authClaim.Authorization.AllowedAccounts.Add("*")
	authJwt, err := authClaim.Encode(oKp)
	require_NoError(t, err)

	conf := fmt.Sprintf(`
		listen: 127.0.0.1:-1
		operator: %s
		system_account: %s
		resolver: MEM
		resolver_preload: {
			%s: %s
			%s: %s
			%s: %s
		}
		leafnodes {
			listen: "127.0.0.1:-1"
		}
    `, ojwt, spub, cpk, authJwt, apk, aJwt, spub, sysJwt)

	handler := func(m *nats.Msg) {
		user, si, _, opts, _ := decodeAuthRequest(t, m.Data)
		if opts.Token == "token" {
			ujwt := createAuthUser(t, user, "user_a", apk, "", akp, 0, nil)
			m.Respond(serviceResponse(t, user, si.ID, ujwt, "", 0))
		} else {
			m.Respond(nil)
		}
	}

	at := NewAuthTest(t, conf, handler, nats.UserCredentials(creds))
	defer at.Cleanup()

	// Good leaf node connection.
	lcreds := createBasicAccountLeaf(t, ckp)
	defer removeFile(t, lcreds)

	hopts := at.srv.getOpts()
	lconf := createConfFile(t, []byte(fmt.Sprintf(`
		listen: "127.0.0.1:-1"
		server_name: "LEAF"
		leafnodes {
			remotes [
				{
					url: "nats://token@127.0.0.1:%d"
					credentials: "%s"
				}
			]
		}
	`, hopts.LeafNode.Port, lcreds)))
	leaf, _ := RunServerWithConfig(lconf)
	defer leaf.Shutdown()
	checkLeafNodeConnected(t, leaf)

	// Now create a leaf with mismatched JWT/seed - JWT from one user,
	// seed from another. This should not crash the server.
	ukp1, _ := nkeys.CreateUser()
	seed1, _ := ukp1.Seed()

	ukp2, _ := nkeys.CreateUser()
	upub2, _ := ukp2.PublicKey()

	// Create JWT for user2 but pair with seed from user1.
	uclaim := newJWTTestUserClaims()
	uclaim.Subject = upub2
	uclaim.Name = "mismatched-leaf"
	ujwt, err := uclaim.Encode(ckp)
	require_NoError(t, err)
	badCreds := genCredsFile(t, ujwt, seed1)
	defer removeFile(t, badCreds)

	lconf2 := createConfFile(t, []byte(fmt.Sprintf(`
		listen: "127.0.0.1:-1"
		server_name: "BAD_LEAF"
		leafnodes {
			remotes [
				{
					url: "nats://token@127.0.0.1:%d"
					credentials: "%s"
				}
			]
		}
	`, hopts.LeafNode.Port, badCreds)))
	leaf2, _ := RunServerWithConfig(lconf2)
	defer leaf2.Shutdown()

	// Bad leaf should fail to connect but NOT crash the server.
	time.Sleep(50 * time.Millisecond)
	checkLeafNodeConnectedCount(t, leaf2, 0)

	// Verify the hub server is still running and healthy.
	checkLeafNodeConnectedCount(t, at.srv, 1)
}

func TestAuthCalloutRegisterWithAccountAfterClose(t *testing.T) {
	conf := `
		listen: "127.0.0.1:-1"
		server_name: ZZ
		accounts {
			AUTH { users [ {user: "auth", password: "pwd"} ] }
			FOO {}
		}
		authorization {
			timeout: 1
			auth_callout {
				issuer: "ABJHLOVMPA4CI6R5KLNGOB4GSLNIY7IOUPAJC4YFNDLQVIOBYQGUWVLA"
				account: AUTH
				auth_users: [ auth ]
			}
		}
	`
	// Track whether the auth callout handler was called and collect the
	// client pointer for manual registration later.
	type calloutInfo struct {
		user      string
		serverID  string
		processed chan struct{}
	}
	var ci calloutInfo
	ci.processed = make(chan struct{})

	handler := func(m *nats.Msg) {
		user, si, _, opts, _ := decodeAuthRequest(t, m.Data)
		if opts.Username == "zombie" && opts.Password == "pwd" {
			ci.user = user
			ci.serverID = si.ID
			close(ci.processed)
			// Do NOT respond. Let the auth callout timeout.
			// This simulates the case where the response arrives late.
		} else {
			m.Respond(nil)
		}
	}

	at := NewAuthTest(t, conf, handler, nats.UserInfo("auth", "pwd"))
	defer at.Cleanup()
	s := at.srv

	fooAcc, err := s.lookupAccount("FOO")
	require_NoError(t, err)
	require_Equal(t, fooAcc.NumLocalConnections(), 0)
	baseServerClients := s.NumClients()

	// Connect a client that will go through auth callout. The handler will
	// NOT respond, so the callout will timeout and the client will be closed.
	var connectErr error
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, connectErr = nats.Connect(s.ClientURL(),
			nats.UserInfo("zombie", "pwd"),
			nats.Timeout(5*time.Second),
			nats.MaxReconnects(0),
		)
	}()

	// Wait for the auth handler to be invoked (request received).
	<-ci.processed

	// Wait for auth timeout (1s) + closeConnection to complete.
	<-done
	require_Error(t, connectErr)
	require_Equal(t, fooAcc.NumLocalConnections(), 0)
	require_Equal(t, s.NumClients(), baseServerClients)

	// Manually create a client, like auth callout would, so we have a reference to it and can close it.
	globalAcc, err := s.lookupAccount(globalAccountName)
	require_NoError(t, err)
	c := &client{srv: s, kind: CLIENT, acc: globalAcc}
	c.initClient()
	globalAcc.addClient(c)
	c.setNoReconnect()
	c.closeConnection(ClientClosed)

	// Simulate what processReply does after the auth callout response arrives:
	// It calls c.RegisterNkeyUser which calls registerWithAccount(targetAcc).
	// registerWithAccount should not add the client as we've closed it above.
	require_Error(t, c.registerWithAccount(fooAcc), ErrConnectionClosed)
	require_Equal(t, fooAcc.NumLocalConnections(), 0)
}

func TestAuthCalloutZombieInflatesAccountConnections(t *testing.T) {
	conf := `
		listen: "127.0.0.1:-1"
		server_name: ZZ
		accounts {
			AUTH { users [ {user: "auth", password: "pwd"} ] }
			FOO { limits { max_conn: 5 } }
		}
		authorization {
			timeout: 1
			auth_callout {
				issuer: "ABJHLOVMPA4CI6R5KLNGOB4GSLNIY7IOUPAJC4YFNDLQVIOBYQGUWVLA"
				account: AUTH
				auth_users: [ auth ]
			}
		}
	`

	handler := func(m *nats.Msg) {
		user, si, _, opts, _ := decodeAuthRequest(t, m.Data)
		if opts.Username == "legit" && opts.Password == "pwd" {
			ujwt := createAuthUser(t, user, _EMPTY_, "FOO", "", nil, 0, nil)
			m.Respond(serviceResponse(t, user, si.ID, ujwt, "", 0))
		} else {
			m.Respond(nil)
		}
	}

	at := NewAuthTest(t, conf, handler, nats.UserInfo("auth", "pwd"))
	defer at.Cleanup()
	s := at.srv

	globalAcc := s.globalAccount()
	fooAcc, err := s.lookupAccount("FOO")
	require_NoError(t, err)

	fooAcc.mu.RLock()
	mconns := fooAcc.mconns
	fooAcc.mu.RUnlock()
	require_Equal(t, mconns, 5)

	// Step 1: Inject zombie connections into FOO account.
	// This simulates what happens when processReply calls registerWithAccount
	// on a client that has already been through closeConnection.
	for range 3 {
		c := &client{srv: s, kind: CLIENT, acc: globalAcc}
		c.initClient()
		globalAcc.addClient(c)
		c.closeConnection(ClientClosed)
		require_Error(t, c.registerWithAccount(fooAcc), ErrConnectionClosed)
	}
	require_Equal(t, fooAcc.NumLocalConnections(), 0)

	// Step 2: Connect legitimate clients. FOO has max_connections: 5.
	// With 3 zombies, we should only be able to connect 2 real clients
	// before hitting the limit, even though there are 0 real connections.
	for range 5 {
		nc, err := nats.Connect(s.ClientURL(),
			nats.UserInfo("legit", "pwd"),
			nats.MaxReconnects(0),
		)
		require_NoError(t, err)
		//goland:noinspection GoDeferInLoop
		defer nc.Close()
	}
	require_Equal(t, fooAcc.NumLocalConnections(), 5)
}

func TestAuthCalloutOperatorModeMQTTOpaquePasswordUsesDefaultSentinel(t *testing.T) {
	_, spub := createKey(t)
	sysClaim := jwt.NewAccountClaims(spub)
	sysClaim.Name = "$SYS"
	sysJwt, err := sysClaim.Encode(oKp)
	require_NoError(t, err)

	tkp, tpub := createKey(t)
	accClaim := jwt.NewAccountClaims(tpub)
	accClaim.Name = "TEST"
	accClaim.Limits.JetStreamLimits.Consumer = -1
	accClaim.Limits.JetStreamLimits.Streams = -1
	accClaim.Limits.JetStreamLimits.MemoryStorage = 1024 * 1024
	accClaim.Limits.JetStreamLimits.DiskStorage = 1024 * 1024
	accJwt, err := accClaim.Encode(oKp)
	require_NoError(t, err)

	akp, err := nkeys.FromSeed([]byte(authCalloutIssuerSeed))
	require_NoError(t, err)
	apub, err := akp.PublicKey()
	require_NoError(t, err)

	upub, creds := createAuthServiceUser(t, akp)
	defer removeFile(t, creds)

	authClaim := jwt.NewAccountClaims(apub)
	authClaim.Name = "AUTH"
	authClaim.EnableExternalAuthorization(upub)
	authClaim.Authorization.AllowedAccounts.Add(tpub)
	authJwt, err := authClaim.Encode(oKp)
	require_NoError(t, err)

	defaultSentinel := createBasicAccountBearer(t, akp)
	storeDir := t.TempDir()
	conf := fmt.Sprintf(`
		listen: "127.0.0.1:-1"
		server_name: A
		jetstream: { max_mem_store: 256MB, max_file_store: 1GB, store_dir: %q }
		mqtt { host: "127.0.0.1", port: -1 }
		operator: %s
		system_account: %s
		resolver: MEM
		resolver_preload: {
			%s: %s
			%s: %s
			%s: %s
		}
		default_sentinel: %s
	`, storeDir, ojwt, spub, apub, authJwt, tpub, accJwt, spub, sysJwt, defaultSentinel)

	const opaquePassword = "external-jwt-or-token"
	type seenConnectOptions struct {
		jwt      string
		password string
	}
	seen := make(chan seenConnectOptions, 1)
	handler := func(m *nats.Msg) {
		user, si, _, opts, _ := decodeAuthRequest(t, m.Data)
		select {
		case seen <- seenConnectOptions{jwt: opts.JWT, password: opts.Password}:
		default:
		}
		if opts.JWT != defaultSentinel || opts.Password != opaquePassword {
			m.Respond(nil)
			return
		}
		ujwt := createAuthUser(t, user, "mqtt", tpub, "", tkp, 10*time.Minute, nil)
		m.Respond(serviceResponse(t, user, si.ID, ujwt, "", 0))
	}

	at := NewAuthTest(t, conf, handler, nats.UserCredentials(creds))
	defer at.Cleanup()

	mc, rc := authCalloutMQTTConnect(t, at.srv.getOpts().MQTT.Host, at.srv.getOpts().MQTT.Port, "mqtt-auth-callout", "ignored", opaquePassword)
	defer mc.Close()

	got := require_ChanRead(t, seen, 2*time.Second)
	require_Equal(t, got.jwt, defaultSentinel)
	require_Equal(t, got.password, opaquePassword)
	require_Equal(t, rc, mqttConnAckRCConnectionAccepted)
}
