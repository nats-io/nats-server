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
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nkeys"
)

const (
	AuthCalloutSubject     = "$SYS.REQ.USER.AUTH"
	AuthRequestSubject     = "nats-authorization-request"
	AuthRequestXKeyHeader  = "Nats-Server-Xkey"
	AuthCalloutServerIdTag = "Auth-Callout-Server-ID"
	AuthAccountPKHeader    = "Auth-Account-Pk"
	AuthAccountSigHeader   = "Auth-Account-Sig"
)

type CalloutResponse struct {
	Error     string `json:"error,omitempty"`
	UserToken string `json:"user_token,omitempty"`
}

func getTagValue(uc *jwt.UserClaims, key string) string {
	// we are expecting `tag: serverID` or `tag:serverID`
	// note that tags lower case
	k := fmt.Sprintf("%s:", strings.ToLower(key))
	for _, t := range uc.Tags {
		if strings.HasPrefix(t, k) {
			return t[len(k):]
		}
	}
	// not found
	return ""
}

// Process a callout on this client's behalf.
func (s *Server) processClientOrLeafCallout(c *client, opts *Options) (authorized bool, errStr string) {
	isOperatorMode := len(opts.TrustedKeys) > 0

	// this is the account the user connected in, or the one running the callout
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

	// FIXME: so things like the server ID that get assigned, are used as a sort of nonce - but
	//  reality is that the keypair here, is generated, so the response generated a JWT has to be
	//  this user - no replay possible
	// Create a keypair for the user. We will expect this public user to be in the signed response.
	// This prevents replay attacks.
	ukp, _ := nkeys.CreateUser()
	pub, _ := ukp.PublicKey()

	reply := s.newRespInbox()
	respCh := make(chan string, 1)

	decodeResponse := func(rc *client, rmsg []byte, acc *Account) (*jwt.UserClaims, error) {
		account := acc.Name
		hdr, msg := rc.msgParts(rmsg)
		if len(hdr) == 0 {
			return nil, errors.New("Auth callout violation - expected headers")
		}

		// This signals not authorized.
		// Since this is an account subscription will always have "\r\n".
		if len(msg) <= LEN_CR_LF {
			return nil, fmt.Errorf("Auth callout violation: %q on account %q", "no reason supplied", account)
		}
		// Strip trailing CRLF.
		msg = msg[:len(msg)-LEN_CR_LF]
		encrypted := false
		// If we sent an encrypted request the response could be encrypted as well.
		// we are expecting JSON, so first char is `{`
		// possibly faster to do bytes.HasPrefix(msg, []byte("{"))
		if xkp != nil && len(msg) > 0 && !json.Valid(msg) {
			var err error
			msg, err = xkp.Open(msg, pubAccXKey)
			if err != nil {
				return nil, fmt.Errorf("Error decrypting auth callout response on account %q: %v", account, err)
			}
			encrypted = true
		}

		ar := CalloutResponse{}
		if err := json.Unmarshal(msg, &ar); err != nil {
			return nil, fmt.Errorf("Auth callout violation: error deserializing JSON: %v", err)
		}

		// if response is encrypted none of this is needed
		if isOperatorMode && !encrypted {
			pkStr := string(getHeader(AuthAccountPKHeader, hdr))
			if pkStr == "" {
				return nil, errors.New("Auth callout violation - expected auth account pk")
			}
			if pkStr != account {
				if _, ok := acc.signingKeys[pkStr]; !ok {
					return nil, errors.New("Auth callout signing key is unknown")
				}
			}
			pk, err := nkeys.FromPublicKey(pkStr)
			if err != nil {
				return nil, fmt.Errorf("Error parsing auth key: %v", err)
			}
			sig := string(getHeader(AuthAccountSigHeader, hdr))
			if sig == "" {
				return nil, errors.New("Auth callout violation - expected auth sig")
			}
			dSig, err := base64.RawURLEncoding.DecodeString(sig)
			if err != nil {
				return nil, fmt.Errorf("Error decoding sig: %v", err)
			}
			// verify that the payload we got, is signed by the auth account
			if err := pk.Verify(msg, dSig); err != nil {
				return nil, fmt.Errorf("Unable to validate sig: %v", err)
			}
		}

		if len(ar.Error) > 0 {
			return nil, fmt.Errorf("Auth callout violation: %q", ar.Error)
		}

		// if we got a message - we got a JWT, the authentication shouldn't be yielding JWTs on failure
		return jwt.DecodeUserClaims(ar.UserToken)
	}

	// FIXME: we were making a point of checking the server id, but, this is not really necessary
	//  because the user public key, has to be the one that we generated so all this code here
	//  is not necessary and complicates the response - refactored here so that we can zap
	// checkServerID validates the server ID matches a value sent to the callout
	// and expected to be stated in one of the tags
	checkServerID := func(arc *jwt.UserClaims, expectedServerID string) error {
		serverID := getTagValue(arc, AuthCalloutServerIdTag)
		if serverID == "" {
			return fmt.Errorf("Auth callout violation: missing server id on account %q", expectedServerID)
		}
		// Require the response to have pinned the audience to this server.
		// but the server ID will be lowercase
		if serverID != strings.ToLower(s.info.ID) {
			return fmt.Errorf("Wrong server id received for auth callout response on account %q, expected %q got %q",
				expectedServerID, s.info.ID, serverID)
		}
		return nil
	}

	// getIssuerAccount returns the issuer (as per JWT) - it also asserts that
	// only in operator mode we expect to receive `issuer_account`.
	getIssuerAccount := func(arc *jwt.UserClaims, account string) (string, error) {
		// Make sure correct issuer.
		var issuer string
		if opts.AuthCallout != nil {
			issuer = opts.AuthCallout.Issuer
		} else {
			// Operator mode is who we send the request on unless switching accounts.
			issuer = acc.Name
		}

		// the jwt issuer can be a signing key
		jwtIssuer := arc.Issuer
		if arc.IssuerAccount != "" {
			if !isOperatorMode {
				// this should be invalid - effectively it would allow the auth callout
				// to issue on another account which may be allowed given the configuration
				// where the auth callout account can handle multiple different ones..
				return "", fmt.Errorf("Error non operator mode account %q: attempted to use issuer_account", account)
			}
			jwtIssuer = arc.IssuerAccount
		}

		if jwtIssuer != issuer {
			if !isOperatorMode {
				return "", fmt.Errorf("Wrong issuer for auth callout response on account %q, expected %q got %q", account, issuer, jwtIssuer)
			} else if !acc.isAllowedAcount(jwtIssuer) {
				return "", fmt.Errorf("Account %q not permitted as valid account option for auth callout for account %q",
					arc.Issuer, account)
			}
		}
		return jwtIssuer, nil
	}

	getExpirationAndAllowedConnections := func(arc *jwt.UserClaims, account string) (time.Duration, map[string]struct{}, error) {
		allowNow, expiration := validateTimes(arc)
		if !allowNow {
			c.Errorf("Outside connect times")
			return 0, nil, fmt.Errorf("Authorized user on account %q outside of valid connect times", account)
		}

		allowedConnTypes, err := convertAllowedConnectionTypes(arc.User.AllowedConnectionTypes)
		if err != nil {
			c.Debugf("%v", err)
			if len(allowedConnTypes) == 0 {
				return 0, nil, fmt.Errorf("Authorized user on account %q using invalid connection type", account)
			}
		}

		return expiration, allowedConnTypes, nil
	}

	assignAccountAndPermissions := func(arc *jwt.UserClaims, account string) (*Account, error) {
		// Apply to this client.
		var err error
		issuerAccount, err := getIssuerAccount(arc, account)
		if err != nil {
			return nil, err
		}

		// if we are not in operator mode, they can specify placement as a tag
		placement := ""
		if !isOperatorMode {
			// only allow placement if we are not in operator mode
			placement = arc.Audience
		} else {
			placement = issuerAccount
		}

		targetAcc, err := s.LookupAccount(placement)
		if err != nil {
			return nil, fmt.Errorf("No valid account %q for auth callout response on account %q: %v", placement, account, err)
		}
		if isOperatorMode {
			// this will validate the signing key that emitted the user, and if it is a signing
			// key it assigns the permissions from the target account
			if scope, ok := targetAcc.hasIssuer(arc.Issuer); !ok {
				return nil, fmt.Errorf("User JWT issuer %q is not known", arc.Issuer)
			} else if scope != nil {
				// this possibly has to be different because it could just be a plain issued by a non-scoped signing key
				if err := scope.ValidateScopedSigner(arc); err != nil {
					return nil, fmt.Errorf("User JWT is not valid: %v", err)
				} else if uSc, ok := scope.(*jwt.UserScope); !ok {
					return nil, fmt.Errorf("User JWT is not a valid scoped user")
				} else if arc.User.UserPermissionLimits, err = processUserPermissionsTemplate(uSc.Template, arc, targetAcc); err != nil {
					return nil, fmt.Errorf("User JWT generated invalid permissions: %v", err)
				}
			}
		}

		return targetAcc, nil
	}

	processReply := func(_ *subscription, rc *client, racc *Account, subject, reply string, rmsg []byte) {
		arc, err := decodeResponse(rc, rmsg, racc)
		if err != nil {
			respCh <- err.Error()
			return
		}

		// Make sure that the user is what we requested.
		if arc.Subject != pub {
			respCh <- fmt.Sprintf("Expected authorized user of %q but got %q on account %q", pub, arc.Subject, racc.Name)
			return
		}

		// FIXME: this is likely not necessary - user ID is already the nonce
		if err := checkServerID(arc, racc.Name); err != nil {
			respCh <- err.Error()
			return
		}

		expiration, allowedConnTypes, err := getExpirationAndAllowedConnections(arc, racc.Name)
		if err != nil {
			respCh <- err.Error()
			return
		}

		targetAcc, err := assignAccountAndPermissions(arc, racc.Name)
		if err != nil {
			respCh <- err.Error()
			return
		}

		// Build internal user and bind to the targeted account.
		nkuser := buildInternalNkeyUser(arc, allowedConnTypes, targetAcc)
		if err := c.RegisterNkeyUser(nkuser); err != nil {
			respCh <- fmt.Sprintf("Could not register auth callout user: %v", err)
			return
		}

		// See if the response wants to override the username.
		if arc.Name != _EMPTY_ {
			c.mu.Lock()
			c.opts.Username = arc.Name
			// Clear any others.
			c.opts.Nkey = _EMPTY_
			c.pubKey = _EMPTY_
			c.opts.Token = _EMPTY_
			c.mu.Unlock()
		}

		// Check if we need to set an auth timer if the user jwt expires.
		c.setExpiration(arc.Claims(), expiration)

		respCh <- _EMPTY_
	}

	// create a subscription to receive a response from the authcallout
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

	// The public key of the server, if set is available on Varz.Key
	// This means that when a service connects, it can now peer
	// authenticate if it wants to - but that also means that it needs to be
	// listening to cluster changes
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
	// FIXME: possibly this public key also needs to be on the
	//  Varz, because then it can be peer verified?
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
	if err := s.sendInternalAccountMsgWithReply(acc, AuthCalloutSubject, reply, hdr, req, false); err != nil {
		errStr = fmt.Sprintf("Error sending authorization request: %v", err)
		s.Debugf(errStr)
		return false, errStr
	}
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
