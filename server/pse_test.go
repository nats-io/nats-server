// Copyright 2015 Apcera Inc. All rights reserved.

package server

import (
	"fmt"
	"os"
	"os/exec"
	"testing"
)

func TestPSEmulation(t *testing.T) {
	var rss, vss, psRss, psVss int64
	var pcpu, psPcpu float64

	// PS version first
	pidStr := fmt.Sprintf("%d", os.Getpid())
	out, err := exec.Command("ps", "o", "pcpu=,rss=,vsz=", "-p", pidStr).Output()
	if err != nil {
		t.Fatalf("Failed to execute ps command: %v\n", err)
	}

	fmt.Sscanf(string(out), "%f %d %d", &psPcpu, &psRss, &psVss)
	psRss *= 1024 // 1k blocks, want bytes.
	psVss *= 1024 // 1k blocks, want bytes.

	// Our internal version
	procUsage(&pcpu, &rss, &vss)

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
		if delta > 200*1024 { // 200k
			t.Fatalf("RSSs did not match close enough: %d vs %d", rss, psRss)
		}
	}
}
