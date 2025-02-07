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

//go:build windows

package sysmem

import (
	"unsafe"

	"golang.org/x/sys/windows"
)

var winKernel32 = windows.NewLazySystemDLL("kernel32.dll")
var winGlobalMemoryStatusEx = winKernel32.NewProc("GlobalMemoryStatusEx")

func init() {
	if err := winKernel32.Load(); err != nil {
		panic(err)
	}
	if err := winGlobalMemoryStatusEx.Find(); err != nil {
		panic(err)
	}
}

// https://docs.microsoft.com/en-us/windows/win32/api/sysinfoapi/ns-sysinfoapi-memorystatusex
type _memoryStatusEx struct {
	dwLength     uint32
	dwMemoryLoad uint32
	ullTotalPhys uint64
	unused       [6]uint64 // ignore rest of struct
}

func Memory() int64 {
	msx := &_memoryStatusEx{dwLength: 64}
	res, _, _ := winGlobalMemoryStatusEx.Call(uintptr(unsafe.Pointer(msx)))
	if res == 0 {
		return 0
	}
	return int64(msx.ullTotalPhys)
}
