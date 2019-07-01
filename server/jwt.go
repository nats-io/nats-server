// Copyright 2018-2019 The NATS Authors
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
	"fmt"
	"io/ioutil"
	"regexp"
	"strings"

	"github.com/nats-io/jwt"
	"github.com/nats-io/nkeys"
)

var nscDecoratedRe = regexp.MustCompile(`\s*(?:(?:[-]{3,}[^\n]*[-]{3,}\n)(.+)(?:\n\s*[-]{3,}[^\n]*[-]{3,}[\n]*))`)

// All JWTs once encoded start with this
const jwtPrefix = "eyJ"

// ReadOperatorJWT will read a jwt file for an operator claim. This can be a decorated file.
func ReadOperatorJWT(jwtfile string) (*jwt.OperatorClaims, error) {
	contents, err := ioutil.ReadFile(jwtfile)
	if err != nil {
		// Check to see if the JWT has been inlined.
		if !strings.HasPrefix(jwtfile, jwtPrefix) {
			return nil, err
		}
		// We may have an inline jwt here.
		contents = []byte(jwtfile)
	}
	defer wipeSlice(contents)

	var claim string
	items := nscDecoratedRe.FindAllSubmatch(contents, -1)
	if len(items) == 0 {
		claim = string(contents)
	} else {
		// First result should be the JWT.
		// We copy here so that if the file contained a seed file too we wipe appropriately.
		raw := items[0][1]
		tmp := make([]byte, len(raw))
		copy(tmp, raw)
		claim = string(tmp)
	}
	opc, err := jwt.DecodeOperatorClaims(claim)
	if err != nil {
		return nil, err
	}
	return opc, nil
}

// Just wipe slice with 'x', for clearing contents of nkey seed file.
func wipeSlice(buf []byte) {
	for i := range buf {
		buf[i] = 'x'
	}
}

// validateTrustedOperators will check that we do not have conflicts with
// assigned trusted keys and trusted operators. If operators are defined we
// will expand the trusted keys in options.
func validateTrustedOperators(o *Options) error {
	if len(o.TrustedOperators) == 0 {
		return nil
	}
	if o.AllowNewAccounts {
		return fmt.Errorf("operators do not allow dynamic creation of new accounts")
	}
	if o.AccountResolver == nil {
		return fmt.Errorf("operators require an account resolver to be configured")
	}
	if len(o.Accounts) > 0 {
		return fmt.Errorf("operators do not allow Accounts to be configured directly")
	}
	if len(o.Users) > 0 || len(o.Nkeys) > 0 {
		return fmt.Errorf("operators do not allow users to be configured directly")
	}
	if len(o.TrustedOperators) > 0 && len(o.TrustedKeys) > 0 {
		return fmt.Errorf("conflicting options for 'TrustedKeys' and 'TrustedOperators'")
	}
	// If we have operators, fill in the trusted keys.
	// FIXME(dlc) - We had TrustedKeys before TrustedOperators. The jwt.OperatorClaims
	// has a DidSign(). Use that longer term. For now we can expand in place.
	for _, opc := range o.TrustedOperators {
		if o.TrustedKeys == nil {
			o.TrustedKeys = make([]string, 0, 4)
		}
		o.TrustedKeys = append(o.TrustedKeys, opc.Issuer)
		o.TrustedKeys = append(o.TrustedKeys, opc.SigningKeys...)
	}
	for _, key := range o.TrustedKeys {
		if !nkeys.IsValidPublicOperatorKey(key) {
			return fmt.Errorf("trusted Keys %q are required to be a valid public operator nkey", key)
		}
	}
	return nil
}
