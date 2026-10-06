// Copyright 2015-2025 The NATS Authors
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

// There are two FreeBSD implementations; one which uses cgo and should build
// locally on any FreeBSD, and this one which uses sysctl but needs us to know
// the offset constants for the fields we care about.
//
// The advantage of this one is that without cgo, it is much easier to
// cross-compile to a target.  The official releases are all built with
// cross-compilation.
//
// We've switched the other implementation to include '_cgo' in the filename,
// to show that it's not the default.  This isn't an os or arch build tag,
// so we have to use explicit build-tags within.
// If lacking CGO support and targeting an unsupported arch, then before the
// change you would have a compile failure for not being able to cross-compile.
// After the change, you have a compile failure for not having the symbols
// because no source file satisfies them.
// Thus we are no worse off, and it's now much easier to extend support for
// non-CGO to new architectures, just by editing this file.
//
// To generate for other architectures:
//   1. Copy `freebsd.txt` to have a .c filename on a box running the target
//      architecture, compile and run it.
//   2. Update the init() function below to include a case for this architecture
//   3. Update the build-tags in this file.

//go:build !cgo && freebsd && (amd64 || arm64)

package pse

import (
	"encoding/binary"
	"runtime"
	"syscall"

	"golang.org/x/sys/unix"
)

// On FreeBSD, to get proc information we read it out of the kernel using a
// binary sysctl.  The endianness of integers is thus explicitly "host", rather
// than little or big endian.
var nativeEndian = binary.LittleEndian

var pageshift int // derived from getpagesize(3) in init() below

var (
	// These are populated in the init function, based on the current architecture.
	// (It's less file-count explosion than having one small file for each
	// FreeBSD architecture).
	KIP_OFF_size   int
	KIP_OFF_rssize int
	KIP_OFF_pctcpu int
)

func init() {
	switch runtime.GOARCH {
	// These are the values which come from compiling and running
	// freebsd.txt as a C program.
	// Most recently validated: 2025-04 with FreeBSD 14.2R in AWS.
	case "amd64":
		KIP_OFF_size = 256
		KIP_OFF_rssize = 264
		KIP_OFF_pctcpu = 308
	case "arm64":
		KIP_OFF_size = 256
		KIP_OFF_rssize = 264
		KIP_OFF_pctcpu = 308
	default:
		panic("code bug: server/pse FreeBSD support missing case for '" + runtime.GOARCH + "' but build-tags allowed us to build anyway?")
	}

	// To get the physical page size, the C library checks two places:
	//   process ELF auxiliary info, AT_PAGESZ
	//   as a fallback, the hw.pagesize sysctl
	// In looking closely, I found that the Go runtime support is handling
	// this for us, and exposing that as syscall.Getpagesize, having checked
	// both in the same ways, at process start, so a call to that should return
	// a memory value without even a syscall bounce.
	pagesize := syscall.Getpagesize()
	pageshift = 0
	for pagesize > 1 {
		pageshift += 1
		pagesize >>= 1
	}
}

func ProcUsage(pcpu *float64, rss, vss *int64) error {
	rawdata, err := unix.SysctlRaw("kern.proc.pid", unix.Getpid())
	if err != nil {
		return err
	}

	r_vss_bytes := nativeEndian.Uint32(rawdata[KIP_OFF_size:])
	r_rss_pages := nativeEndian.Uint32(rawdata[KIP_OFF_rssize:])
	rss_bytes := r_rss_pages << pageshift

	// In C: fixpt_t ki_pctcpu
	// Doc: %cpu for process during ki_swtime
	// fixpt_t is __uint32_t
	// usr.bin/top uses pctdouble to convert to a double (float64)
	// define pctdouble(p) ((double)(p) / FIXED_PCTCPU)
	// FIXED_PCTCPU is _usually_ FSCALE (some architectures are special)
	// <sys/param.h> has:
	//   #define FSHIFT  11              /* bits to right of fixed binary point */
	//   #define FSCALE  (1<<FSHIFT)
	r_pcpu := nativeEndian.Uint32(rawdata[KIP_OFF_pctcpu:])
	f_pcpu := float64(r_pcpu) / float64(2048)

	*rss = int64(rss_bytes)
	*vss = int64(r_vss_bytes)
	*pcpu = f_pcpu

	return nil
}
