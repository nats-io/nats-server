// Copyright 2016 Apcera Inc. All rights reserved.
// +build win
package server

import (
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"testing"
)

func checkValues(t *testing.T, pcpu, tPcpu float64, rss, tRss int64) {
	if pcpu != tPcpu {
		delta := int64(pcpu - tPcpu)
		if delta < 0 {
			delta = -delta
		}
		if delta > 30 { // 30%?
			t.Fatalf("CPUs did not match close enough: %f vs %f", pcpu, tPcpu)
		}
	}
	if rss != tRss {
		delta := rss - tRss
		if delta < 0 {
			delta = -delta
		}
		if delta > 200*1024 { // 200k
			t.Fatalf("RSSs did not match close enough: %d vs %d", rss, tRss)
		}
	}
}

func TestWinPSEmulation(t *testing.T) {
	var pcpu, tPcpu float64
	var rss, vss, tRss int64

	// set the image prefix for our test.
	SetWinPSEImagePrefix("server.test")

	// query the counters using typeperf
	out, err := exec.Command("typeperf",
		fmt.Sprintf("\\Process(%s)\\%% Processor Time", imagePrefix),
		fmt.Sprintf("\\Process(%s)\\Working Set - Private", imagePrefix),
		fmt.Sprintf("\\Process(%s)\\Virtual Bytes", imagePrefix),
		"-sc", "1").Output()
	if err != nil {
		t.Fatal("unable to run command", err)
	}

	// parse out results - refer to comments in procUsage for detail
	results := strings.Split(string(out), "\r\n")
	values := strings.Split(results[2], ",")

	// parse pcpu
	tPcpu, err = strconv.ParseFloat(strings.Trim(values[1], "\""), 64)
	if err != nil {
		t.Fatal("Unable to parse percent cpu: %s", values[1])
	}

	// parse private bytes (rss)
	fval, err := strconv.ParseFloat(strings.Trim(values[2], "\""), 64)
	if err != nil {
		t.Fatal("Unable to parse private bytes: %s", values[2])
	}
	tRss = int64(fval)

	if err = procUsage(&pcpu, &rss, &vss); err == nil {
		t.Fatal("Error:  %v", err)
	}
	checkValues(t, pcpu, tPcpu, rss, tRss)

	// Again to test image name cacheing
	if err = procUsage(&pcpu, &rss, &vss); err == nil {
		t.Fatal("Error:  %v", err)
	}
	checkValues(t, pcpu, tPcpu, rss, tRss)

	// Test not finding an image
	SetWinPSEImagePrefix("invalid")
	if err = procUsage(&pcpu, &rss, &vss); err == nil {
		t.Fatal("Expected an error for an invalid image name.")
	}
}
