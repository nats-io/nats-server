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
	"crypto/tls"
	"encoding/pem"
	"fmt"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nkeys"
)

const (
	AuthCalloutSubject    = "$SYS.REQ.USER.AUTH"
	AuthRequestSubject    = "nats-authorization-request"
	AuthRequestXKeyHeader = "Nats-Server-Xkey"
)

// Process a callout on this client's behalf.
func (s *Server) processClientOrLeafCallout(c *client, opts *Options) (authorized bool, errStr string) {
	isOperatorMode := len(opts.TrustedKeys) > 0

	var acc *Account
	if !isOperatorMode && opts.AuthCallout != nil && opts.AuthCallout.Account != _EMPTY_ {
		aname := opts.AuthCallout.Account
		var err error
		acc, err = s.LookupAccount(aname)
		if err != nil {
			errStr = fmt.Sprintf("No valid account %q for auth callout request: %v", aname, err)
			s.Warnf(errStr)
			return false, errStr
		}
	} else {
		acc = c.acc
	}

	// Check if we have been requested to encrypt.
	var xkp nkeys.KeyPair
	var xkey string
	var pubAccXKey string
	if !isOperatorMode && opts.AuthCallout != nil && opts.AuthCallout.XKey != _EMPTY_ {
		pubAccXKey = opts.AuthCallout.XKey
	} else if isOperatorMode {
		pubAccXKey = acc.externalAuthXKey()
	}
	// If set grab server's xkey keypair and public key.
	if pubAccXKey != _EMPTY_ {
		// These are only set on creation, so lock not needed.
		xkp, xkey = s.xkp, s.info.XKey
	}

	// Create a keypair for the user. We will expect this public user to be in the signed response.
	// This prevents replay attacks.
	ukp, _ := nkeys.CreateUser()
	pub, _ := ukp.PublicKey()

	reply := s.newRespInbox()
	respCh := make(chan string, 1)

	processReply := func(_ *subscription, rc *client, racc *Account, subject, reply string, rmsg []byte) {
		_, msg := rc.msgParts(rmsg)
		// This signals not authorized.
		// Since this is an account subscription will always have "\r\n".
		if len(msg) <= LEN_CR_LF {
			respCh <- fmt.Sprintf("Auth callout violation: %q on account %q", "no reason supplied", racc.Name)
			return
		}
		// Strip trailing CRLF.
		msg = msg[:len(msg)-LEN_CR_LF]

		// If we sent an encrypted request the response could be encrypted as well.
		if xkp != nil && len(msg) > len(jwtPrefix) && !bytes.HasPrefix(msg, []byte(jwtPrefix)) {
			var err error
			msg, err = xkp.Open(msg, pubAccXKey)
			if err != nil {
				respCh <- fmt.Sprintf("Error decrypting auth callout response on account %q: %v", racc.Name, err)
				return
			}
		}

		arc, err := jwt.DecodeAuthorizationResponseClaims(string(msg))
		if err != nil {
			respCh <- fmt.Sprintf("Error decoding auth callout response on account %q: %v", racc.Name, err)
			return
		}
		// This changes it the configuration so that we can trace the happy path as
		// it is done for a regular connection
		if arc.Jwt != _EMPTY_ {
			// decode the user claims
			u, err := jwt.DecodeUserClaims(arc.Jwt)
			if err != nil {
				respCh <- fmt.Sprintf("Error decoding auth callout embedded JWT %q: %v", racc.Name, err)
				return
			}
			// the current connection is in the auth account, so we need to lookup the
			// right place based on the issuer/issuer_account
			arc.User = u
			aid := u.Issuer
			if u.IssuerAccount != _EMPTY_ {
				aid = u.IssuerAccount
			}
			// now we get the account the user will land on
			userAccount, err := s.LookupAccount(aid)
			if err != nil {
				respCh <- fmt.Sprintf("Error looking up the user target account %v", err)
				return
			}
			// this will validate the scope - so if it doesn't have the right issuer it gets rejected
			if scope, ok := userAccount.hasIssuer(u.Issuer); !ok {
				respCh <- fmt.Sprint("User JWT issuer is not known")
				return
			} else if scope != nil {
				// this possibly has to be different because it could just be a plain issued by a non-scoped signing key
				if err := scope.ValidateScopedSigner(u); err != nil {
					respCh <- fmt.Sprintf("User JWT is not valid: %v", err)
					return
				} else if uSc, ok := scope.(*jwt.UserScope); !ok {
					respCh <- fmt.Sprint("User JWT is not valid")
					return
				} else if arc.User.UserPermissionLimits, err = processUserPermissionsTemplate(uSc.Template, u, userAccount); err != nil {
					respCh <- fmt.Sprint("User JWT generated invalid permissions")
					return
				}
			}
		}

		// FIXME(dlc) - push error through here.
		if arc.Error != nil || arc.User == nil {
			if arc.Error != nil {
				respCh <- fmt.Sprintf("Auth callout violation: %q on account %q", arc.Error.Description, racc.Name)
			} else {
				respCh <- fmt.Sprintf("Auth callout violation: no user returned  on account %q", racc.Name)
			}
			return
		}

		// Make sure correct issuer.
		var issuer string
		if opts.AuthCallout != nil {
			issuer = opts.AuthCallout.Issuer
		} else {
			// Operator mode is who we send the request on unless switching accounts.
			issuer = acc.Name
		}
		// By default issuer needs to match server config or the requesting account in operator mode.
		if arc.Issuer != issuer {
			// this would enable signing keys for the account to work the trick here is we need to
			// verify this key exists in the account JWT
			if arc.IssuerAccount != _EMPTY_ {
				// this is possibly wrong - I think what we want here is for the auth account to use
				// whatever keys it wants, but set the target audience to be the account we want
				// then really we don't have to worry about attributions at this point, the response
				// is issed by the auth account, we only care for the stuff in it...
				arc.Issuer = arc.IssuerAccount
			}
			if !isOperatorMode {
				respCh <- fmt.Sprintf("Wrong issuer for auth callout response on account %q, expected %q got %q", racc.Name, issuer, arc.Issuer)
				return
			} else if !acc.isAllowedAcount(arc.Issuer) {
				respCh <- fmt.Sprintf("Account %q not permitted as valid account option for auth callout for account %q",
					arc.Issuer, racc.Name)
				return
			}
		}

		// Require the response to have pinned the audience to this server.
		if arc.Audience != s.info.ID {
			respCh <- fmt.Sprintf("Wrong server audience received for auth callout response on account %q, expected %q got %q",
				racc.Name, s.info.ID, arc.Audience)
			return
		}

		juc := arc.User
		// Make sure that the user is what we requested.
		if juc.Subject != pub {
			respCh <- fmt.Sprintf("Expected authorized user of %q but got %q on account %q", pub, juc.Subject, racc.Name)
			return
		}

		allowNow, validFor := validateTimes(juc)
		if !allowNow {
			c.Errorf("Outside connect times")
			respCh <- fmt.Sprintf("Authorized user on account %q outside of valid connect times", racc.Name)
			return
		}
		allowedConnTypes, err := convertAllowedConnectionTypes(juc.AllowedConnectionTypes)
		if err != nil {
			c.Debugf("%v", err)
			if len(allowedConnTypes) == 0 {
				respCh <- fmt.Sprintf("Authorized user on account %q using invalid connection type", racc.Name)
				return
			}
		}
		// Apply to this client.
		targetAcc := acc
		// Check if we are being asked to switch accounts.
		if aname := juc.Audience; aname != _EMPTY_ {
			targetAcc, err = s.LookupAccount(aname)
			if err != nil {
				respCh <- fmt.Sprintf("No valid account %q for auth callout response on account %q: %v", aname, racc.Name, err)
				return
			}
			// In operator mode make sure this account matches the issuer.
			if isOperatorMode && aname != arc.Issuer {
				// FIXME: this is not quite right, because on JWT the signer entry
				// can be a string or a scope - if the signer has a scope this
				// is an invalid operation - as we would want for this purpose
				// to just have a plain public key...
				signer := targetAcc.signingKeys[arc.Issuer]
				if signer == nil {
					respCh <- fmt.Sprintf("Account %q does not match issuer %q", aname, juc.Issuer)
					return
				}
			}
		}

		// Build internal user and bind to the targeted account.
		nkuser := buildInternalNkeyUser(juc, allowedConnTypes, targetAcc)
		if err := c.RegisterNkeyUser(nkuser); err != nil {
			respCh <- fmt.Sprintf("Could not register auth callout user: %v", err)
			return
		}

		// See if the response wants to override the username.
		if juc.Name != _EMPTY_ {
			c.mu.Lock()
			c.opts.Username = juc.Name
			if arc.Jwt != _EMPTY_ {
				// this is required if we want to assign signing key permissions
				// there are some other account limits that are assigned by the server
				// when the user connects, but they are all based on JWT decoding
				// so without a source JWT, this type of reuse is not possible
				c.opts.JWT = arc.Jwt
				if arc.User.IssuerAccount != _EMPTY_ {
					c.user.SigningKey = arc.User.Issuer
				}
				c.applyAccountLimits()
			}
			// Clear any others.
			c.opts.Nkey = _EMPTY_
			c.pubKey = _EMPTY_
			c.opts.Token = _EMPTY_
			c.mu.Unlock()
		}

		// Check if we need to set an auth timer if the user jwt expires.
		c.setExpiration(juc.Claims(), validFor)

		respCh <- _EMPTY_
	}

	sub, err := acc.subscribeInternal(reply, processReply)
	if err != nil {
		errStr = fmt.Sprintf("Error setting up reply subscription for auth request: %v", err)
		s.Warnf(errStr)
		return false, errStr
	}
	defer acc.unsubscribeInternal(sub)

	// Build our request claims - jwt subject should be nkey
	jwtSub := acc.Name
	if opts.AuthCallout != nil {
		jwtSub = opts.AuthCallout.Issuer
	}
	claim := jwt.NewAuthorizationRequestClaims(jwtSub)
	claim.Audience = AuthRequestSubject
	// Set expected public user nkey.
	claim.UserNkey = pub

	s.mu.RLock()
	claim.Server = jwt.ServerID{
		Name:    s.info.Name,
		Host:    s.info.Host,
		ID:      s.info.ID,
		Version: s.info.Version,
		Cluster: s.info.Cluster,
	}
	s.mu.RUnlock()

	// Tags
	claim.Server.Tags = s.getOpts().Tags

	// Check if we have been requested to encrypt.
	if xkp != nil {
		claim.Server.XKey = xkey
	}

	authTimeout := secondsToDuration(s.getOpts().AuthTimeout)
	claim.Expires = time.Now().Add(time.Duration(authTimeout)).UTC().Unix()

	// Grab client info for the request.
	c.mu.Lock()
	c.fillClientInfo(&claim.ClientInformation)
	c.fillConnectOpts(&claim.ConnectOptions)
	// If we have a sig in the client opts, fill in nonce.
	if claim.ConnectOptions.SignedNonce != _EMPTY_ {
		claim.ClientInformation.Nonce = string(c.nonce)
	}

	// TLS
	if c.flags.isSet(handshakeComplete) && c.nc != nil {
		var ct jwt.ClientTLS
		conn := c.nc.(*tls.Conn)
		cs := conn.ConnectionState()
		ct.Version = tlsVersion(cs.Version)
		ct.Cipher = tlsCipher(cs.CipherSuite)
		// Check verified chains.
		for _, vs := range cs.VerifiedChains {
			var certs []string
			for _, c := range vs {
				blk := &pem.Block{
					Type:  "CERTIFICATE",
					Bytes: c.Raw,
				}
				certs = append(certs, string(pem.EncodeToMemory(blk)))
			}
			ct.VerifiedChains = append(ct.VerifiedChains, certs)
		}
		// If we do not have verified chains put in peer certs.
		if len(ct.VerifiedChains) == 0 {
			for _, c := range cs.PeerCertificates {
				blk := &pem.Block{
					Type:  "CERTIFICATE",
					Bytes: c.Raw,
				}
				ct.Certs = append(ct.Certs, string(pem.EncodeToMemory(blk)))
			}
		}
		claim.TLS = &ct
	}
	c.mu.Unlock()

	b, err := claim.Encode(s.kp)
	if err != nil {
		errStr = fmt.Sprintf("Error encoding auth request claim on account %q: %v", acc.Name, err)
		s.Warnf(errStr)
		return false, errStr
	}
	req := []byte(b)
	var hdr map[string]string

	// Check if we have been asked to encrypt.
	if xkp != nil {
		req, err = xkp.Seal([]byte(req), pubAccXKey)
		if err != nil {
			errStr = fmt.Sprintf("Error encrypting auth request claim on account %q: %v", acc.Name, err)
			s.Warnf(errStr)
			return false, errStr
		}
		hdr = map[string]string{AuthRequestXKeyHeader: xkey}
	}

	// Send out our request.
	s.sendInternalAccountMsgWithReply(acc, AuthCalloutSubject, reply, hdr, req, false)

	select {
	case errStr = <-respCh:
		if authorized = errStr == _EMPTY_; !authorized {
			s.Warnf(errStr)
		}
	case <-time.After(authTimeout):
		errStr = fmt.Sprintf("Authorization callout response not received in time on account %q", acc.Name)
		s.Debugf(errStr)
	}

	return authorized, errStr
}

// Fill in client information for the request.
// Lock should be held.
func (c *client) fillClientInfo(ci *jwt.ClientInformation) {
	if c == nil || (c.kind != CLIENT && c.kind != LEAF && c.kind != JETSTREAM && c.kind != ACCOUNT) {
		return
	}

	// Do it this way to fail to compile if fields are added to jwt.ClientInformation.
	*ci = jwt.ClientInformation{
		Host:    c.host,
		ID:      c.cid,
		User:    c.getRawAuthUser(),
		Name:    c.opts.Name,
		Tags:    c.tags,
		NameTag: c.nameTag,
		Kind:    c.kindString(),
		Type:    c.clientTypeString(),
		MQTT:    c.getMQTTClientID(),
	}
}

// Fill in client options.
// Lock should be held.
func (c *client) fillConnectOpts(opts *jwt.ConnectOptions) {
	if c == nil || (c.kind != CLIENT && c.kind != LEAF && c.kind != JETSTREAM && c.kind != ACCOUNT) {
		return
	}

	o := c.opts

	// Do it this way to fail to compile if fields are added to jwt.ClientInformation.
	*opts = jwt.ConnectOptions{
		JWT:         o.JWT,
		Nkey:        o.Nkey,
		SignedNonce: o.Sig,
		Token:       o.Token,
		Username:    o.Username,
		Password:    o.Password,
		Name:        o.Name,
		Lang:        o.Lang,
		Version:     o.Version,
		Protocol:    o.Protocol,
	}
}
