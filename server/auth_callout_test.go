// Copyright 2022-2023 The NATS Authors
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
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
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
)

func serviceResponse(t *testing.T, errMsg string, payload string) *nats.Msg {
	var err error
	r := nats.NewMsg("")
	cr := CalloutResponse{Error: errMsg, UserToken: payload}
	r.Data, err = json.Marshal(cr)
	require_NoError(t, err)

	// this envelope information is not necessary when the payload is encrypted
	// this is also ignored on servers running in non-operator mode
	aa, err := nkeys.FromSeed([]byte(authCalloutIssuerSeed))
	require_NoError(t, err)

	pk, err := aa.PublicKey()
	require_NoError(t, err)
	r.Header.Set(AuthAccountPKHeader, pk)

	sig, err := aa.Sign(r.Data)
	require_NoError(t, err)
	r.Header.Set(AuthAccountSigHeader, base64.RawURLEncoding.EncodeToString(sig))

	return r
}

func makeScopedRole(t *testing.T, role string, pub []string, sub []string) (jwt.Scope, nkeys.KeyPair) {
	akp, pk := createKey(t)
	r := jwt.NewUserScope()
	r.Key = pk
	r.Template.Sub.Allow.Add(sub...)
	r.Template.Pub.Allow.Add(pub...)
	r.Role = role
	return r, akp
}

// Will create a signed user jwt as an authorized user.
func createAuthUser(t *testing.T, user, name, account, server, issuerAccount string, akp nkeys.KeyPair, expires time.Duration, limits *jwt.UserPermissionLimits) string {
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
	uc.Tags.Add(fmt.Sprintf("%s:%s", AuthCalloutServerIdTag, server))

	if name != _EMPTY_ {
		uc.Name = name
	}
	if expires > 0 {
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

func TestAuthCalloutBasics(t *testing.T) {
	conf := createConfFile(t, []byte(`
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
	`))
	defer removeFile(t, conf)
	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	callouts := uint32(0)

	// This will not use callout since predefined as an auth_user.
	nc, err := nats.Connect(s.ClientURL(), nats.UserInfo("auth", "pwd"))
	require_NoError(t, err)
	defer nc.Close()

	// Make sure callout was not triggered.
	require_True(t, atomic.LoadUint32(&callouts) == 0)

	_, err = nc.Subscribe(AuthCalloutSubject, func(m *nats.Msg) {
		atomic.AddUint32(&callouts, 1)
		user, si, ci, opts, _ := decodeAuthRequest(t, m.Data)
		require_True(t, si.Name == "A")
		require_True(t, ci.Host == "127.0.0.1")
		// Allow dlc user.
		if opts.Username == "dlc" && opts.Password == "zzz" {
			var j jwt.UserPermissionLimits
			j.Pub.Allow.Add("$SYS.>")
			j.Payload = 1024
			ujwt := createAuthUser(t, user, _EMPTY_, globalAccountName, si.ID, "", nil, 10*time.Minute, &j)
			m.RespondMsg(serviceResponse(t, "", ujwt))
		} else {
			// Nil response signals no authentication.
			m.Respond(nil)
		}
	})
	require_NoError(t, err)

	// This one should fail since bad password.
	_, err = nats.Connect(s.ClientURL(), nats.UserInfo("dlc", "xxx"))
	require_Error(t, err)

	// This one will use callout since not defined in server config.
	nc, err = nats.Connect(s.ClientURL(), nats.UserInfo("dlc", "zzz"))
	require_NoError(t, err)
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
	conf := createConfFile(t, []byte(`
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
	`))
	defer removeFile(t, conf)
	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	// Auth callout user.
	nc, err := nats.Connect(s.ClientURL(), nats.UserInfo("auth", "pwd"))
	require_NoError(t, err)
	defer nc.Close()
	// Should always make auth callouts queue subscribers or proper services.
	_, err = nc.Subscribe(AuthCalloutSubject, func(m *nats.Msg) {
		user, si, ci, opts, _ := decodeAuthRequest(t, m.Data)
		require_True(t, si.Name == "ZZ")
		require_True(t, ci.Host == "127.0.0.1")
		// Allow dlc user and map to the BAZ account.
		if opts.Username == "dlc" && opts.Password == "zzz" {
			ujwt := createAuthUser(t, user, _EMPTY_, "BAZ", si.ID, "", nil, 0, nil)
			m.RespondMsg(serviceResponse(t, "", ujwt))
		} else {
			// Nil response signals no authentication.
			m.Respond(nil)
		}
	})
	require_NoError(t, err)

	// This one will use callout since not defined in server config.
	nc, err = nats.Connect(s.ClientURL(), nats.UserInfo("dlc", "zzz"))
	require_NoError(t, err)
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

func TestAuthCalloutClientTLSCerts(t *testing.T) {
	conf := createConfFile(t, []byte(`
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
	`))
	defer removeFile(t, conf)
	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	nc, err := nats.Connect(s.ClientURL(),
		nats.UserInfo("auth", "pwd"),
		nats.ClientCert("../test/configs/certs/tlsauth/client2.pem", "../test/configs/certs/tlsauth/client2-key.pem"),
		nats.RootCAs("../test/configs/certs/tlsauth/ca.pem"),
	)
	require_NoError(t, err)
	defer nc.Close()

	_, err = nc.Subscribe(AuthCalloutSubject, func(m *nats.Msg) {
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
			ujwt := createAuthUser(t, user, "dlc", "FOO", si.ID, "", nil, 0, nil)
			m.RespondMsg(serviceResponse(t, "", ujwt))
		}
	})
	require_NoError(t, err)

	// Will use client cert to determine user.
	nc, err = nats.Connect(s.ClientURL(),
		nats.ClientCert("../test/configs/certs/tlsauth/client2.pem", "../test/configs/certs/tlsauth/client2-key.pem"),
		nats.RootCAs("../test/configs/certs/tlsauth/ca.pem"),
	)
	require_NoError(t, err)
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
	conf := createConfFile(t, []byte(`
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
	`))
	defer removeFile(t, conf)
	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	// This will not use callout since predefined as an auth_user.
	nc, err := nats.Connect(s.ClientURL(), nats.UserInfo("auth", "pwd"))
	require_NoError(t, err)
	defer nc.Close()

	callouts := uint32(0)
	_, err = nc.Subscribe(AuthCalloutSubject, func(m *nats.Msg) {
		atomic.AddUint32(&callouts, 1)
		user, si, ci, opts, _ := decodeAuthRequest(t, m.Data)
		require_True(t, si.Name == "A")
		require_True(t, ci.Host == "127.0.0.1")
		require_True(t, opts.SignedNonce != _EMPTY_)
		require_True(t, ci.Nonce != _EMPTY_)
		ujwt := createAuthUser(t, user, _EMPTY_, globalAccountName, si.ID, "", nil, 0, nil)
		m.RespondMsg(serviceResponse(t, "", ujwt))
	})
	require_NoError(t, err)

	// This one will use callout since not part of auth_users.
	// Even though we will internally verify this user the callout will still be called.
	seedFile := createTempFile(t, _EMPTY_)
	defer removeFile(t, seedFile.Name())
	seedFile.WriteString(authCalloutSeed)
	nkeyOpt, err := nats.NkeyOptionFromSeed(seedFile.Name())
	require_NoError(t, err)

	nc, err = nats.Connect(s.ClientURL(), nkeyOpt)
	require_NoError(t, err)
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
	uclaim.Subject = upub
	vr := jwt.ValidationResults{}
	uclaim.Validate(&vr)
	require_Len(t, len(vr.Errors()), 0)
	ujwt, err := uclaim.Encode(accKp)
	require_NoError(t, err)
	return upub, genCredsFile(t, ujwt, seed)
}

func createBasicAccountUser(t *testing.T, accKp nkeys.KeyPair) (creds string) {
	t.Helper()
	ukp, _ := nkeys.CreateUser()
	seed, _ := ukp.Seed()
	upub, _ := ukp.PublicKey()
	uclaim := newJWTTestUserClaims()
	uclaim.Subject = upub
	// For these deny all permission
	uclaim.Permissions.Pub.Deny.Add(">")
	uclaim.Permissions.Sub.Deny.Add(">")
	vr := jwt.ValidationResults{}
	uclaim.Validate(&vr)
	require_Len(t, len(vr.Errors()), 0)
	ujwt, err := uclaim.Encode(accKp)
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
	scope, scopedKp := makeScopedRole(t, "foo", []string{"foo.>", "$SYS.REQ.USER.INFO"}, []string{"foo.>", "_INBOX.>"})
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

	conf := createConfFile(t, []byte(fmt.Sprintf(`
		listen: 127.0.0.1:-1
		operator: %s
		system_account: %s
		resolver: MEM
		resolver_preload: {
			%s: %s
			%s: %s
			%s: %s
		}
    `, ojwt, spub, apub, authJwt, tpub, accJwt, spub, sysJwt)))
	defer removeFile(t, conf)

	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	nc, err := nats.Connect(s.ClientURL(), nats.UserCredentials(creds))
	require_NoError(t, err)
	defer nc.Close()

	// Check that we have the deny permission autoset properly.
	resp, err := nc.Request(userDirectInfoSubj, nil, time.Second)
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

	const secretToken = "--XX--"
	const dummyToken = "--ZZ--"
	const skKeyToken = "--SK--"
	const scopedToken = "--Scoped--"
	const badScopedToken = "--BADScoped--"

	dkp, notAllowAccountPub := createKey(t)

	// Register authorization handlers.
	_, err = nc.Subscribe(AuthCalloutSubject, func(m *nats.Msg) {
		user, si, _, opts, _ := decodeAuthRequest(t, m.Data)
		if opts.Token == secretToken {
			ujwt := createAuthUser(t, user, "dlc", tpub, si.ID, "", tkp, 0, nil)
			m.RespondMsg(serviceResponse(t, "", ujwt))
		} else if opts.Token == dummyToken {
			ujwt := createAuthUser(t, user, "dummy", notAllowAccountPub, si.ID, "", dkp, 0, nil)
			m.RespondMsg(serviceResponse(t, "", ujwt))
		} else if opts.Token == skKeyToken {
			ujwt := createAuthUser(t, user, "sk", tpub, si.ID, tpub, tSigningKp, 0, nil)
			m.RespondMsg(serviceResponse(t, "", ujwt))
		} else if opts.Token == scopedToken {
			// must have no limits set
			ujwt := createAuthUser(t, user, "scoped", tpub, si.ID, tpub, scopedKp, 0, &jwt.UserPermissionLimits{})
			m.RespondMsg(serviceResponse(t, "", ujwt))
		} else if opts.Token == badScopedToken {
			// limits are nil - here which result in a default user - this will fail scoped
			ujwt := createAuthUser(t, user, "bad-scoped", tpub, si.ID, tpub, scopedKp, 0, nil)
			m.RespondMsg(serviceResponse(t, "", ujwt))
		} else {
			m.Respond(nil)
		}
	})
	require_NoError(t, err)

	// Bearer token etc..
	// This is used by all users, and the customization will be in other connect args.
	// This needs to also be bound to the authorization account.
	creds = createBasicAccountUser(t, akp)
	defer removeFile(t, creds)

	// We require a token.
	_, err = nats.Connect(s.ClientURL(), nats.UserCredentials(creds))
	require_Error(t, err)

	// Send correct token. This should switch us to the test account.
	nc, err = nats.Connect(s.ClientURL(), nats.UserCredentials(creds), nats.Token(secretToken))
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
	_, err = nats.Connect(s.ClientURL(), nats.UserCredentials(creds), nats.Token(dummyToken))
	require_Error(t, err)

	// Send the signing key token. This should switch us to the test account, but the user
	// is signed with the account signing key
	nc, err = nats.Connect(s.ClientURL(), nats.UserCredentials(creds), nats.Token(skKeyToken))
	require_NoError(t, err)

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
	_, err = nats.Connect(s.ClientURL(), nats.UserCredentials(creds), nats.Token(badScopedToken))
	require_Error(t, err)

	// Send the signing key token. This should switch us to the test account, but the user
	// is signed with the account signing key
	nc, err = nats.Connect(s.ClientURL(), nats.UserCredentials(creds), nats.Token(scopedToken))
	require_NoError(t, err)

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
	sort.Strings(userInfo.Permissions.Publish.Allow)
	require_Equal(t, "foo.>", userInfo.Permissions.Publish.Allow[1])
	sort.Strings(userInfo.Permissions.Subscribe.Allow)
	require_True(t, len(userInfo.Permissions.Subscribe.Allow) == 2)
	require_Equal(t, "foo.>", userInfo.Permissions.Subscribe.Allow[1])

	defer nc.Close()
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
	conf := createConfFile(t, []byte(fmt.Sprintf(tmpl, curvePublic)))
	defer removeFile(t, conf)
	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	// This will not use callout since predefined as an auth_user.
	nc, err := nats.Connect(s.ClientURL(), nats.UserInfo("auth", "pwd"))
	require_NoError(t, err)
	defer nc.Close()

	rkp, err := nkeys.FromCurveSeed([]byte(curveSeed))
	require_NoError(t, err)

	_, err = nc.Subscribe(AuthCalloutSubject, func(m *nats.Msg) {
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
			ujwt := createAuthUser(t, user, _EMPTY_, globalAccountName, si.ID, "", nil, 10*time.Minute, nil)
			m.RespondMsg(serviceResponse(t, "", ujwt))
		} else if opts.Username == "dlc" && opts.Password == "xxx" {
			ujwt := createAuthUser(t, user, _EMPTY_, globalAccountName, si.ID, "", nil, 10*time.Minute, nil)
			srm := serviceResponse(t, "", ujwt)
			// Encrypt this response.
			srm.Data, err = rkp.Seal([]byte(srm.Data), si.XKey) // Server's public xkey.
			require_NoError(t, err)
			m.RespondMsg(srm)
		} else {
			// Nil response signals no authentication.
			m.Respond(nil)
		}
	})
	require_NoError(t, err)

	nc, err = nats.Connect(s.ClientURL(), nats.UserInfo("dlc", "zzz"))
	require_NoError(t, err)
	defer nc.Close()

	// Authorization services can optionally encrypt the responses using the server's public xkey.
	nc, err = nats.Connect(s.ClientURL(), nats.UserInfo("dlc", "xxx"))
	require_NoError(t, err)
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

	conf := createConfFile(t, []byte(fmt.Sprintf(`
		listen: 127.0.0.1:-1
		operator: %s
		system_account: %s
		resolver: MEM
		resolver_preload: {
			%s: %s
			%s: %s
			%s: %s
		}
    `, ojwt, spub, apub, authJwt, tpub, accJwt, spub, sysJwt)))
	defer removeFile(t, conf)

	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	nc, err := nats.Connect(s.ClientURL(), nats.UserCredentials(creds))
	require_NoError(t, err)
	defer nc.Close()

	const tokenA = "--XX--"
	const tokenB = "--ZZ--"

	rkp, err := nkeys.FromCurveSeed([]byte(curveSeed))
	require_NoError(t, err)

	// Register authorization handlers.
	_, err = nc.Subscribe(AuthCalloutSubject, func(m *nats.Msg) {
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
			ujwt := createAuthUser(t, user, "dlc", tpub, si.ID, "", tkp, 0, nil)
			m.RespondMsg(serviceResponse(t, "", ujwt))
		} else if opts.Token == tokenB {
			ujwt := createAuthUser(t, user, "rip", tpub, si.ID, "", tkp, 0, nil)
			rm := serviceResponse(t, "", ujwt)
			// Encrypt this response.
			rm.Data, err = rkp.Seal(rm.Data, si.XKey) // Server's public xkey.
			require_NoError(t, err)
			m.RespondMsg(rm)
		} else {
			m.Respond(nil)
		}
	})
	require_NoError(t, err)

	// Bearer token etc..
	// This is used by all users, and the customization will be in other connect args.
	// This needs to also be bound to the authorization account.
	creds = createBasicAccountUser(t, akp)
	defer removeFile(t, creds)

	// This will receive an encrypted request to the auth service but send plaintext response.
	nc, err = nats.Connect(s.ClientURL(), nats.UserCredentials(creds), nats.Token(tokenA))
	require_NoError(t, err)
	defer nc.Close()

	// This will receive an encrypted request to the auth service and send an encrypted response.
	nc, err = nats.Connect(s.ClientURL(), nats.UserCredentials(creds), nats.Token(tokenB))
	require_NoError(t, err)
	defer nc.Close()
}

func TestAuthCalloutServerTags(t *testing.T) {
	conf := createConfFile(t, []byte(`
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
	`))
	defer removeFile(t, conf)
	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	// This will not use callout since predefined as an auth_user.
	nc, err := nats.Connect(s.ClientURL(), nats.UserInfo("auth", "pwd"))
	require_NoError(t, err)
	defer nc.Close()

	tch := make(chan jwt.TagList, 1)
	_, err = nc.Subscribe(AuthCalloutSubject, func(m *nats.Msg) {
		user, si, _, _, _ := decodeAuthRequest(t, m.Data)
		tch <- si.Tags
		ujwt := createAuthUser(t, user, _EMPTY_, globalAccountName, si.ID, "", nil, 10*time.Minute, nil)
		m.RespondMsg(serviceResponse(t, "", ujwt))
	})
	require_NoError(t, err)

	_, err = nats.Connect(s.ClientURL())
	require_NoError(t, err)

	tags := <-tch
	require_True(t, len(tags) == 2)
	require_True(t, tags.Contains("foo"))
	require_True(t, tags.Contains("bar"))
}

func TestAuthCalloutServerClusterAndVersion(t *testing.T) {
	conf := createConfFile(t, []byte(`
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
	`))
	defer removeFile(t, conf)
	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	// This will not use callout since predefined as an auth_user.
	nc, err := nats.Connect(s.ClientURL(), nats.UserInfo("auth", "pwd"))
	require_NoError(t, err)
	defer nc.Close()

	ch := make(chan string, 2)
	_, err = nc.Subscribe(AuthCalloutSubject, func(m *nats.Msg) {
		user, si, _, _, _ := decodeAuthRequest(t, m.Data)
		ch <- si.Cluster
		ch <- si.Version
		ujwt := createAuthUser(t, user, _EMPTY_, globalAccountName, si.ID, "", nil, 10*time.Minute, nil)
		m.RespondMsg(serviceResponse(t, "", ujwt))
	})
	require_NoError(t, err)

	_, err = nats.Connect(s.ClientURL())
	require_NoError(t, err)

	cluster := <-ch
	require_True(t, cluster == "HUB")

	version := <-ch
	require_True(t, len(version) > 0)
	ok, err := versionAtLeastCheckError(version, 2, 10, 0)
	require_NoError(t, err)
	require_True(t, ok)
}

func TestAuthCalloutErrorResponse(t *testing.T) {
	conf := createConfFile(t, []byte(`
		listen: "127.0.0.1:-1"
		server_name: A
		authorization {
			users: [ { user: "auth", password: "pwd" } ]
			auth_callout {
				issuer: "ABJHLOVMPA4CI6R5KLNGOB4GSLNIY7IOUPAJC4YFNDLQVIOBYQGUWVLA"
				auth_users: [ auth ]
			}
		}
	`))
	defer removeFile(t, conf)
	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	// This will not use callout since predefined as an auth_user.
	nc, err := nats.Connect(s.ClientURL(), nats.UserInfo("auth", "pwd"))
	require_NoError(t, err)
	defer nc.Close()

	_, err = nc.Subscribe(AuthCalloutSubject, func(m *nats.Msg) {
		m.RespondMsg(serviceResponse(t, "BAD AUTH", ""))
	})
	require_NoError(t, err)

	_, err = nats.Connect(s.ClientURL(), nats.UserInfo("dlc", "zzz"))
	require_Error(t, err)
}

func TestAuthCalloutAuthUserFailDoesNotInvokeCallout(t *testing.T) {
	conf := createConfFile(t, []byte(`
		listen: "127.0.0.1:-1"
		server_name: A
		authorization {
			users: [ { user: "auth", password: "pwd" } ]
			auth_callout {
				issuer: "ABJHLOVMPA4CI6R5KLNGOB4GSLNIY7IOUPAJC4YFNDLQVIOBYQGUWVLA"
				auth_users: [ auth ]
			}
		}
	`))
	defer removeFile(t, conf)
	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	nc, err := nats.Connect(s.ClientURL(), nats.UserInfo("auth", "pwd"))
	require_NoError(t, err)
	defer nc.Close()

	callouts := uint32(0)
	_, err = nc.Subscribe(AuthCalloutSubject, func(m *nats.Msg) {
		atomic.AddUint32(&callouts, 1)
		m.RespondMsg(serviceResponse(t, "WRONG PASSWORD", ""))
	})
	require_NoError(t, err)

	_, err = nats.Connect(s.ClientURL(), nats.UserInfo("auth", "zzz"))
	require_Error(t, err)

	if atomic.LoadUint32(&callouts) != 0 {
		t.Fatalf("Expected callout to not be called")
	}
}

func TestAuthCalloutAuthErrEvents(t *testing.T) {
	conf := createConfFile(t, []byte(`
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
	`))
	defer removeFile(t, conf)
	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	nc, err := nats.Connect(s.ClientURL(), nats.UserInfo("auth", "pwd"))
	require_NoError(t, err)
	defer nc.Close()

	// This is where the event fires, in this account.
	sub, err := nc.SubscribeSync(authErrorAccountEventSubj)
	require_NoError(t, err)

	_, err = nc.Subscribe(AuthCalloutSubject, func(m *nats.Msg) {
		user, si, _, opts, _ := decodeAuthRequest(t, m.Data)
		// Allow dlc user and map to the BAZ account.
		if opts.Username == "dlc" && opts.Password == "zzz" {
			ujwt := createAuthUser(t, user, _EMPTY_, "FOO", si.ID, "", nil, 0, nil)
			m.RespondMsg(serviceResponse(t, "", ujwt))
		} else if opts.Username == "dlc" {
			m.RespondMsg(serviceResponse(t, "WRONG PASSWORD", ""))
		} else {
			m.RespondMsg(serviceResponse(t, "BAD CREDS", ""))
		}
	})
	require_NoError(t, err)

	// This one will use callout since not defined in server config.
	nc, err = nats.Connect(s.ClientURL(), nats.UserInfo("dlc", "zzz"))
	require_NoError(t, err)
	nc.Close()
	checkSubsPending(t, sub, 0)

	checkAuthErrEvent := func(user, pass, reason string) {
		_, err = nats.Connect(s.ClientURL(), nats.UserInfo(user, pass))
		require_Error(t, err)

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
	conf := createConfFile(t, []byte(`
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
	`))
	defer removeFile(t, conf)
	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	nc, err := nats.Connect(s.ClientURL(), nats.UserInfo("auth", "pwd"))
	require_NoError(t, err)
	defer nc.Close()

	_, err = nc.Subscribe(AuthCalloutSubject, func(m *nats.Msg) {
		user, si, _, opts, _ := decodeAuthRequest(t, m.Data)
		// Allow dlc user and map to the BAZ account.
		if opts.Username == "dlc" && opts.Password == "zzz" {
			ujwt := createAuthUser(t, user, _EMPTY_, "FOO", si.ID, "", nil, 0, nil)
			m.RespondMsg(serviceResponse(t, "", ujwt))
		} else if opts.Username == "rip" && opts.Password == "xxx" {
			ujwt := createAuthUser(t, user, _EMPTY_, "BAR", si.ID, "", nil, 0, nil)
			m.RespondMsg(serviceResponse(t, "", ujwt))
		} else {
			m.RespondMsg(serviceResponse(t, "BAD CREDS", ""))
		}
	})
	require_NoError(t, err)

	// Setup system user.
	snc, err := nats.Connect(s.ClientURL(), nats.UserInfo("admin", "s3cr3t!"))
	require_NoError(t, err)
	defer nc.Close()

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
		nc, err := nats.Connect(s.ClientURL(), nats.UserInfo(user, pass))
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
