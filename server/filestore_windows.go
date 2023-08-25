// Copyright 2023 The NATS Authors
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

//go:build windows
// +build windows

package server

import (
	"fmt"
	"os"
	"syscall"
	"unsafe"
)

// Acquire filesystem-level lock for the filestore to ensure exclusive access
// for this server instance.
func lockFile(f *os.File) error {
	var (
		modkernel32  = syscall.NewLazyDLL("kernel32.dll")
		prLockFileEx = modkernel32.NewProc("LockFileEx")
		ol           = unsafe.Pointer(new(syscall.Overlapped))
	)

	a := prLockFileEx.Addr()
	h := syscall.Handle(f.Fd())
	r, _, e := syscall.Syscall6(a, 6, uintptr(h), 0x3, 0, ^uintptr(0), ^uintptr(0), uintptr(ol))
	if r == 0 {
		return fmt.Errorf("lock `%s': %v", f.Name(), error(e))
	}
	return nil
}
