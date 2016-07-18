// Copyright 2015-2016 Apcera Inc. All rights reserved.

package pse

import (
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"testing"
)

func TestPSEmulation(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skipf("Skipping this test on Windows")
	}
	var rss, vss, psRss, psVss int64
	var pcpu, psPcpu float64

	runtime.GC()

	// PS version first
	pidStr := fmt.Sprintf("%d", os.Getpid())
	out, err := exec.Command("ps", "o", "pcpu=,rss=,vsz=", "-p", pidStr).Output()
	if err != nil {
		t.Fatalf("Failed to execute ps command: %v\n", err)
	}

	fmt.Sscanf(string(out), "%f %d %d", &psPcpu, &psRss, &psVss)
	psRss *= 1024 // 1k blocks, want bytes.
	psVss *= 1024 // 1k blocks, want bytes.

	runtime.GC()

	// Our internal version
	ProcUsage(&pcpu, &rss, &vss)

	if pcpu != psPcpu {
		delta := int64(pcpu - psPcpu)
		if delta < 0 {
			delta = -delta
		}
		if delta > 30 { // 30%?
			t.Fatalf("CPUs did not match close enough: %f vs %f", pcpu, psPcpu)
		}
	}
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
