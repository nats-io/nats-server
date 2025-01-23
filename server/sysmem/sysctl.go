// Copyright 2019-2024 The NATS Authors
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

//go:build darwin || freebsd || openbsd || dragonfly || netbsd

package sysmem

import (
	"syscall"
	"unsafe"
)

func sysctlInt64(name string) int64 {
	s, err := syscall.Sysctl(name)
	if err != nil {
		return 0
	}
	// Make sure it's 8 bytes when we do the cast below.
	// We were getting fatal error: checkptr: converted pointer straddles multiple allocations in go 1.22.1 on darwin.
	var b [8]byte
	copy(b[:], s)
	return *(*int64)(unsafe.Pointer(&b[0]))
}
