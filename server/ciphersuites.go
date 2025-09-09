// Copyright 2016-2025 The NATS Authors
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
)

func init() {
	for _, cs := range tls.CipherSuites() {
		cipherMap[cs.Name] = cs
		cipherMapByID[cs.ID] = cs
	}
	for _, cs := range tls.InsecureCipherSuites() {
		cipherMap[cs.Name] = cs
		cipherMapByID[cs.ID] = cs
	}
}

var cipherMap = map[string]*tls.CipherSuite{}
var cipherMapByID = map[uint16]*tls.CipherSuite{}

func defaultCipherSuites() []uint16 {
	ciphers := tls.CipherSuites()
	defaults := make([]uint16, 0, len(ciphers))
	for _, cs := range ciphers {
		defaults = append(defaults, cs.ID)
	}
	return defaults
}

// Where we maintain available curve preferences
var curvePreferenceMap = map[string]tls.CurveID{
	"X25519MLKEM768": tls.X25519MLKEM768,
	"X25519":         tls.X25519,
	"CurveP256":      tls.CurveP256,
	"CurveP384":      tls.CurveP384,
	"CurveP521":      tls.CurveP521,
}

// reorder to default to the highest level of security.  See:
// https://blog.bracebin.com/achieving-perfect-ssl-labs-score-with-go
func defaultCurvePreferences() []tls.CurveID {
	return []tls.CurveID{
		tls.X25519MLKEM768, // post-quantum
		tls.X25519,         // faster than P256, arguably more secure
		tls.CurveP256,
		tls.CurveP384,
		tls.CurveP521,
	}
}
