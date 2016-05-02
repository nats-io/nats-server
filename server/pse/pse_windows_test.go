// Copyright 2016 Apcera Inc. All rights reserved.
// +build windows

package pse

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

func testParseValues(t *testing.T) {
	var pid int
	var pcpu float64
	var rss, vss int64

	err := parseValues("invalid", &pid, &pcpu, &rss, &vss)
	if err == nil {
		t.Fatal("Did not receive expected error.")
	}
	err = parseValues(
		"\"date time\",\"invalid float\",\"invalid float\",\"invalid float\"",
		&pid, &pcpu, &rss, &vss)
	if err == nil {
		t.Fatal("Did not receive expected error.")
	}
	err = parseValues(
		"\"date time\",\"1234.00000\",\"invalid float\",\"invalid float\"",
		&pid, &pcpu, &rss, &vss)
	if err == nil {
		t.Fatal("Did not receive expected error.")
	}
	err = parseValues(
		"\"date time\",\"1234.00000\",\"1234.00000\",\"invalid float\"",
		&pid, &pcpu, &rss, &vss)
	if err == nil {
		t.Fatal("Did not receive expected error.")
	}
}

func TestPSEmulationWin(t *testing.T) {
	var pcpu, tPcpu float64
	var rss, vss, tRss int64

	imageName := getProcessImageName()
	// query the counters using typeperf
	out, err := exec.Command("typeperf.exe",
		fmt.Sprintf("\\Process(%s)\\%% Processor Time", imageName),
		fmt.Sprintf("\\Process(%s)\\Working Set - Private", imageName),
		fmt.Sprintf("\\Process(%s)\\Virtual Bytes", imageName),
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

	if err = ProcUsage(&pcpu, &rss, &vss); err != nil {
		t.Fatal("Error:  %v", err)
	}
	checkValues(t, pcpu, tPcpu, rss, tRss)

	// Again to test image name caching
	if err = ProcUsage(&pcpu, &rss, &vss); err != nil {
		t.Fatal("Error:  %v", err)
	}
	checkValues(t, pcpu, tPcpu, rss, tRss)

	testParseValues(t)

	var ppid int
	if err = getStatsForProcess("invalid", &pcpu, &rss, &vss, &ppid); err != nil {
		t.Fatal("Did not receive expected error.")
	}
}
