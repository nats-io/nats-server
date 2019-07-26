/*
 * Copyright 2018 The NATS Authors
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package jwt

import (
	"fmt"
	"time"
)

// Export represents a single export
type Export struct {
	Name        string         `json:"name,omitempty"`
	Subject     Subject        `json:"subject,omitempty"`
	Type        ExportType     `json:"type,omitempty"`
	TokenReq    bool           `json:"token_req,omitempty"`
	Revocations RevocationList `json:"revocations,omitempty"`
}

// IsService returns true if an export is for a service
func (e *Export) IsService() bool {
	return e.Type == Service
}

// IsStream returns true if an export is for a stream
func (e *Export) IsStream() bool {
	return e.Type == Stream
}

// Validate appends validation issues to the passed in results list
func (e *Export) Validate(vr *ValidationResults) {
	if !e.IsService() && !e.IsStream() {
		vr.AddError("invalid export type: %q", e.Type)
	}
	e.Subject.Validate(vr)
}

// Revoke enters a revocation by publickey using time.Now().
func (e *Export) Revoke(pubKey string) {
	e.RevokeAt(pubKey, time.Now())
}

// RevokeAt enters a revocation by publickey and timestamp into this export
// If there is already a revocation for this public key that is newer, it is kept.
func (e *Export) RevokeAt(pubKey string, timestamp time.Time) {
	if e.Revocations == nil {
		e.Revocations = RevocationList{}
	}

	e.Revocations.Revoke(pubKey, timestamp)
}

// ClearRevocation removes any revocation for the public key
func (e *Export) ClearRevocation(pubKey string) {
	e.Revocations.ClearRevocation(pubKey)
}

// IsRevokedAt checks if the public key is in the revoked list with a timestamp later than
// the one passed in. Generally this method is called with time.Now() but other time's can
// be used for testing.
func (e *Export) IsRevokedAt(pubKey string, timestamp time.Time) bool {
	return e.Revocations.IsRevoked(pubKey, timestamp)
}

// IsRevoked checks if the public key is in the revoked list with time.Now()
func (e *Export) IsRevoked(pubKey string) bool {
	return e.Revocations.IsRevoked(pubKey, time.Now())
}

// Exports is an array of exports
type Exports []*Export

// Add appends exports to the list
func (e *Exports) Add(i ...*Export) {
	*e = append(*e, i...)
}

func isContainedIn(kind ExportType, subjects []Subject, vr *ValidationResults) {
	m := make(map[string]string)
	for i, ns := range subjects {
		for j, s := range subjects {
			if i == j {
				continue
			}
			if ns.IsContainedIn(s) {
				str := string(s)
				_, ok := m[str]
				if !ok {
					m[str] = string(ns)
				}
			}
		}
	}

	if len(m) != 0 {
		for k, v := range m {
			var vi ValidationIssue
			vi.Blocking = true
			vi.Description = fmt.Sprintf("%s export subject %q already exports %q", kind, k, v)
			vr.Add(&vi)
		}
	}
}

// Validate calls validate on all of the exports
func (e *Exports) Validate(vr *ValidationResults) error {
	var serviceSubjects []Subject
	var streamSubjects []Subject

	for _, v := range *e {
		if v.IsService() {
			serviceSubjects = append(serviceSubjects, v.Subject)
		} else {
			streamSubjects = append(streamSubjects, v.Subject)
		}
		v.Validate(vr)
	}

	isContainedIn(Service, serviceSubjects, vr)
	isContainedIn(Stream, streamSubjects, vr)

	return nil
}

// HasExportContainingSubject checks if the export list has an export with the provided subject
func (e *Exports) HasExportContainingSubject(subject Subject) bool {
	for _, s := range *e {
		if subject.IsContainedIn(s.Subject) {
			return true
		}
	}
	return false
}
