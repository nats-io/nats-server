// Copyright 2025 The NATS Authors
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

//go:build illumos || solaris

package sysmem

import (
	"golang.org/x/sys/unix"
)

const (
	_SC_PHYS_PAGES = 500
	_SC_PAGESIZE   = 11
)

func Memory() int64 {
	pages, err := unix.Sysconf(_SC_PHYS_PAGES)
	if err != nil {
		return 0
	}
	pageSize, err := unix.Sysconf(_SC_PAGESIZE)
	if err != nil {
		return 0
	}
	return int64(pages) * int64(pageSize)
}
