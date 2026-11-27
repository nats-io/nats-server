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

package filerename

import (
	"errors"
	"os"
	"time"

	"golang.org/x/sys/windows"
)

func retryCode(err error) uint32 {
	switch {
	case errors.Is(err, windows.ERROR_ACCESS_DENIED):
		return uint32(windows.ERROR_ACCESS_DENIED)
	case errors.Is(err, windows.ERROR_SHARING_VIOLATION):
		return uint32(windows.ERROR_SHARING_VIOLATION)
	default:
		return 0
	}
}

// Rename retries only the failing rename. A custom wait can yield a held disk I/O slot.
func Rename(from, to string, wait func(time.Duration)) (Result, error) {
	err := os.Rename(from, to)
	if err == nil {
		return Result{Attempts: 1}, nil
	}
	code := retryCode(err)
	if code == 0 {
		return Result{Attempts: 1}, err
	}
	if wait == nil {
		wait = time.Sleep
	}
	started := time.Now()
	result := Result{from: from, to: to, Attempts: 1, FirstCode: code}
	for _, delay := range [...]time.Duration{50, 100, 200, 400} {
		wait(delay * time.Millisecond)
		result.Attempts++
		err = os.Rename(from, to)
		if err == nil || retryCode(err) == 0 {
			break
		}
	}
	result.Elapsed = time.Since(started)
	return result, err
}
