// Copyright 2016-2026 The NATS Authors
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

//go:build go1.26

package server

import (
	"crypto/fips140"
	"crypto/tls"
)

// Where we maintain available curve preferences
var curvePreferenceMap = map[string]tls.CurveID{
	"SecP384r1MLKEM1024": tls.SecP384r1MLKEM1024,
	"SecP256r1MLKEM768":  tls.SecP256r1MLKEM768,
	"X25519MLKEM768":     tls.X25519MLKEM768,
	"X25519":             tls.X25519,
	"CurveP256":          tls.CurveP256,
	"CurveP384":          tls.CurveP384,
	"CurveP521":          tls.CurveP521,
}

// reorder to default to the highest level of security.  See:
// https://blog.bracebin.com/achieving-perfect-ssl-labs-score-with-go
func defaultCurvePreferences() []tls.CurveID {
	if fips140.Enabled() {
		// X25519 is not FIPS-approved by itself, but it is when combined with MLKEM768.
		// SecP256r1MLKEM768 and SecP384r1MLKEM1024 are both FIPS-approved hybrids.
		return []tls.CurveID{
			tls.X25519MLKEM768,     // post-quantum
			tls.SecP384r1MLKEM1024, // post-quantum
			tls.SecP256r1MLKEM768,  // post-quantum
			tls.CurveP256,
			tls.CurveP384,
			tls.CurveP521,
		}
	}
	return []tls.CurveID{
		tls.X25519MLKEM768,     // post-quantum
		tls.SecP384r1MLKEM1024, // post-quantum
		tls.SecP256r1MLKEM768,  // post-quantum
		tls.X25519,             // faster than P256, arguably more secure
		tls.CurveP256,
		tls.CurveP384,
		tls.CurveP521,
	}
}
