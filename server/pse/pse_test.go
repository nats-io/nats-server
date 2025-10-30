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

package pse

import (
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"runtime/debug"
	"testing"
)

func TestInServerPSE(t *testing.T) {

}

func TestPSEmulationCPU(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skipf("Skipping this test on Windows")
	}
	var rss, vss int64
	var pcpu, psPcpu float64

	debug.FreeOSMemory()

	// PS version first
	pidStr := fmt.Sprintf("%d", os.Getpid())
	out, err := exec.Command("ps", "o", "pcpu=", "-p", pidStr).Output()
	if err != nil {
		t.Fatalf("Failed to execute ps command: %v\n", err)
	}

	// Our internal version
	ProcUsage(&pcpu, &rss, &vss)

	fmt.Sscanf(string(out), "%f", &psPcpu)

	if pcpu != psPcpu {
		delta := int64(pcpu - psPcpu)
		if delta < 0 {
			delta = -delta
		}
		if delta > 30 { // 30%?
			t.Fatalf("CPUs did not match close enough: %f vs %f", pcpu, psPcpu)
		}
	}
}

func TestPSEmulationMem(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skipf("Skipping this test on Windows")
	}
	var rss, vss, psRss, psVss int64
	var pcpu float64

	debug.FreeOSMemory()

	// PS version first
	pidStr := fmt.Sprintf("%d", os.Getpid())
	out, err := exec.Command("ps", "o", "rss=,vsz=", "-p", pidStr).Output()
	if err != nil {
		t.Fatalf("Failed to execute ps command: %v\n", err)
	}

	fmt.Sscanf(string(out), "%d %d", &psRss, &psVss)
	psRss *= 1024 // 1k blocks, want bytes.
	psVss *= 1024 // 1k blocks, want bytes.

	debug.FreeOSMemory()

	// Our internal version
	ProcUsage(&pcpu, &rss, &vss)

	if rss != psRss {
		delta := rss - psRss
		if delta < 0 {
			delta = -delta
		}
		if delta > 1024*1024 { // 1MB
			t.Fatalf("RSSs did not match close enough: %d vs %d", rss, psRss)
		}
	}
}
