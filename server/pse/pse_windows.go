// Copyright 2015-2016 Apcera Inc. All rights reserved.

package pse

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

// cache the image name to optimize repeated calls
var imageName string
var imageLock sync.Mutex

// parseValues parses the results of data returned by typeperf.exe.  This
// is a series of comma delimited quoted strings, containing date time,
// pid, pcpu, rss, and vss.  All numeric values are floating point.
// eg: "04/17/2016 15.38.00.016", "5123.00000", "1.23400", "123.00000", "123.00000"
func parseValues(line string, pid *int, pcpu *float64, rss, vss *int64) (err error) {
	values := strings.Split(line, ",")
	if len(values) < 4 {
		return errors.New("Invalid result.")
	}
	// values[0] will be date, time, ignore them
	// parse the pid
	fVal, err := strconv.ParseFloat(strings.Trim(values[1], "\""), 64)
	if err != nil {
		return errors.New(fmt.Sprintf("Unable to parse pid: %s", values[1]))
	}
	*pid = int(fVal)

	// parse pcpu
	*pcpu, err = strconv.ParseFloat(strings.Trim(values[2], "\""), 64)
	if err != nil {
		return errors.New(fmt.Sprintf("Unable to parse percent cpu: %s", values[2]))
	}

	// parse private working set (rss)
	fVal, err = strconv.ParseFloat(strings.Trim(values[3], "\""), 64)
	if err != nil {
		return errors.New(fmt.Sprintf("Unable to parse working set: %s", values[3]))
	}
	*rss = int64(fVal)

	// parse virtual bytes (vsz)
	fVal, err = strconv.ParseFloat(strings.Trim(values[4], "\""), 64)
	if err != nil {
		return errors.New(fmt.Sprintf("Unable to parse virtual bytes: %s", values[4]))
	}
	*vss = int64(fVal)

	return nil
}

// getStatsForProcess retrieves process information for a given instance name.
// typeperf.exe is the windows native command line utility to get pcpu, rss,
// and vsz equivalents through queries of performance counters.
// An alternative is to map the Pdh* native windows API from pdh.dll,
// and call those APIs directly - this is a simpler and cleaner approach.
func getStatsForProcess(name string, pcpu *float64, rss, vss *int64, pid *int) (err error) {
	// query the counters using typeperf. "-sc","1" requests one
	// set of data (versus continuous monitoring)
	out, err := exec.Command("typeperf.exe",
		fmt.Sprintf("\\Process(%s)\\ID Process", name),
		fmt.Sprintf("\\Process(%s)\\%% Processor Time", name),
		fmt.Sprintf("\\Process(%s)\\Working Set - Private", name),
		fmt.Sprintf("\\Process(%s)\\Virtual Bytes", name),
		"-sc", "1").Output()
	if err != nil {
		// Signal that the command ran, but the image instance was not found
		// through a PID of -1.
		if strings.Contains(string(out), "The data is not valid") {
			*pid = -1
			return nil
		} else {
			// something went wrong executing the command
			// Debugf("exec failure: %s\n", string(out))
			return errors.New(fmt.Sprintf("typeperf failed: %v", err))
		}
	}

	results := strings.Split(string(out), "\r\n")
	// results[0] = newline
	// results[1] = headers
	// results[2] = values
	// ignore the rest...
	if len(results) < 3 {
		return errors.New(fmt.Sprintf("unexpected results from typeperf"))
	}
	if err = parseValues(results[2], pid, pcpu, rss, vss); err != nil {
		return err
	}
	return nil
}

// getProcessImageName returns the name of the process image, as expected by
// typeperf.
func getProcessImageName() (name string) {
	name = filepath.Base(os.Args[0])
	name = strings.TrimRight(name, ".exe")
	return
}

// procUsage retrieves process cpu and memory information.
// Under the hood, typeperf is called.  Notably, typeperf cannot search
// using a pid, but instead uses a somewhat volatile process image name.
// If there is more than one instance, "#<instancecount>" is appended to
// the image name. Wildcard filters are supported, but result in a very
// complex data set to parse.
func ProcUsage(pcpu *float64, rss, vss *int64) error {
	var ppid int = -1

	imageLock.Lock()
	name := imageName
	imageLock.Unlock()

	// Get the pid to retrieve the right set of information for this process.
	procPid := os.Getpid()

	// if we have cached the image name, try that first
	if name != "" {
		err := getStatsForProcess(name, pcpu, rss, vss, &ppid)
		if err != nil {
			return err
		}
		// If the instance name's pid matches ours, we're done.
		// Otherwise, this instance has been renamed, which is possible
		// as other process instances start and stop on the system.
		if ppid == procPid {
			return nil
		}
	}
	// If we get here, the instance name is invalid (nil, or out of sync)
	// Query pid and counters until the correct image name is found and
	// cache it.  This is optimized for one or two instances on a windows
	// node. An alternative is using a wildcard to first lookup up pids,
	// and parse those to find instance name, then lookup the
	// performance counters.
	prefix := getProcessImageName()
	for i := 0; ppid != procPid; i++ {
		name = fmt.Sprintf("%s#%d", prefix, i)
		err := getStatsForProcess(name, pcpu, rss, vss, &ppid)
		if err != nil {
			return err
		}

		// Bail out if an image name is not found.
		if ppid < 0 {
			break
		}

		// if the pids equal, this is the right process and cache our
		// image name
		if ppid == procPid {
			imageLock.Lock()
			imageName = name
			imageLock.Unlock()
			break
		}
	}
	if ppid < 0 {
		return errors.New("unable to retrieve process counters")
	}
	return nil
}
