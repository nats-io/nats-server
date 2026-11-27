// Copyright 2026 JEMES contributors
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

// Package filerename retries short-lived Windows handle conflicts on rename.
package filerename

import "time"

// Result records a retry without changing the OS error returned to the caller.
type Result struct {
	Attempts  int
	FirstCode uint32
	Elapsed   time.Duration
	from, to  string
}

// Report emits one outcome through the owner's logger after a retry sequence.
func (r Result) Report(logf func(string, ...any), err error) {
	if r.Attempts > 1 && logf != nil {
		logf("File rename retry: from=%q to=%q first_error_code=%d attempts=%d elapsed=%s recovered=%t final_error=%v",
			r.from, r.to, r.FirstCode, r.Attempts, r.Elapsed, err == nil, err)
	}
}
