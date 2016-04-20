// Copyright 2015 Apcera Inc. All rights reserved.

package pse

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"syscall"
)

var procStatFile string
var ticks int64

const (
	utimePos = 13
	stimePos = 14
	startPos = 21
	vssPos   = 22
	rssPos   = 23
)

func init() {
	// Avoiding to generate docker image without CGO
	ticks = 100 // int64(C.sysconf(C._SC_CLK_TCK))
	procStatFile = fmt.Sprintf("/proc/%d/stat", os.Getpid())
}

func ProcUsage(pcpu *float64, rss, vss *int64) error {
	contents, err := ioutil.ReadFile(procStatFile)
	if err != nil {
		return err
	}
	fields := bytes.Fields(contents)

	*rss = (parseInt64(fields[rssPos])) << 12
	*vss = parseInt64(fields[vssPos])

	startTime := parseInt64(fields[startPos])
	utime := parseInt64(fields[utimePos])
	stime := parseInt64(fields[stimePos])
	totalTime := utime + stime

	var sysinfo syscall.Sysinfo_t
	if err := syscall.Sysinfo(&sysinfo); err != nil {
		return err
	}

	seconds := int64(sysinfo.Uptime) - (startTime / ticks)

	if seconds > 0 {
		ipcpu := (totalTime * 1000 / ticks) / seconds
		*pcpu = float64(ipcpu) / 10.0
	}

	return nil
}

// Ascii numbers 0-9
const (
	asciiZero = 48
	asciiNine = 57
)

// parseInt64 expects decimal positive numbers. We
// return -1 to signal error
func parseInt64(d []byte) (n int64) {
	if len(d) == 0 {
		return -1
	}
	for _, dec := range d {
		if dec < asciiZero || dec > asciiNine {
			return -1
		}
		n = n*10 + (int64(dec) - asciiZero)
	}
	return n
}
