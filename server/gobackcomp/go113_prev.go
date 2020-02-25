// Copyright 2020 The NATS Authors
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

// +build !go1.13

package gobackcomp

// implements: go 1.13 errors.Unwrap(err error) error
func errorsUnwrap(err error) error {
	u, ok := err.(interface {
		Unwrap() error
	})
	if !ok {
		return nil
	}
	return u.Unwrap()
}

// implements: go 1.13 errors.Is(err, target error) bool
func ErrorsIs(err, target error) bool {
	// this is an outright copy of go 1.13 errors.Is(err, target error) bool
	// removed isComparable
	if target == nil {
		return err == target
	}

	for {
		if err == target {
			return true
		}
		if x, ok := err.(interface{ Is(error) bool }); ok && x.Is(target) {
			return true
		}
		// TODO: consider supporing target.Is(err). This would allow
		// user-definable predicates, but also may allow for coping with sloppy
		// APIs, thereby making it easier to get away with them.
		if err = errorsUnwrap(err); err == nil {
			return false
		}
	}

	return false
}
