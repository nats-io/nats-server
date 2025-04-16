// Copyright 2022-2024 The NATS Authors
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
		if opts.Username == "dlc" && opts.Password == "zzz" {
			var j jwt.UserPermissionLimits
			j.Pub.Allow.Add("$SYS.>")
			j.Payload = 1024
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
		UserID:  "dlc",
		Account: globalAccountName,
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
		UserID:  "UBO2MQV67TQTVIRV3XFTEZOACM4WLOCMCDMAWN5QVN5PI2N6JHTVDRON",
		Account: globalAccountName,
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
		UserID:  upub,
		Account: apub,
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
		UserID:  upub,
		Account: apub,
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
		} else {
			m.Respond(serviceResponse(t, user, si.ID, "", "BAD CREDS", 0))
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

		if !strings.Contains(dm.Reason, reason) {
			t.Fatalf("Expected %q reason, but got %q", reason, dm.Reason)
		}
	}

	checkAuthErrEvent("dlc", "xxx", "WRONG PASSWORD")
	checkAuthErrEvent("rip", "abc", "BAD CREDS")
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
