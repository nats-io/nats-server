// Copyright 2013-2025 The NATS Authors
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

package server

import (
	"time"
)

// ConnInfos represents a connection info list. We use pointers since it will be sorted.
type ConnInfos []*ConnInfo

// For sorting
// Len returns length for sorting.
func (cl ConnInfos) Len() int { return len(cl) }

// Swap will sawap the elements.
func (cl ConnInfos) Swap(i, j int) { cl[i], cl[j] = cl[j], cl[i] }

// SortOpt is a helper type to sort clients
type SortOpt string

// Possible sort options
const (
	ByCid      SortOpt = "cid"        // By connection ID
	ByStart    SortOpt = "start"      // By connection start time, same as CID
	BySubs     SortOpt = "subs"       // By number of subscriptions
	ByPending  SortOpt = "pending"    // By amount of data in bytes waiting to be sent to client
	ByOutMsgs  SortOpt = "msgs_to"    // By number of messages sent
	ByInMsgs   SortOpt = "msgs_from"  // By number of messages received
	ByOutBytes SortOpt = "bytes_to"   // By amount of bytes sent
	ByInBytes  SortOpt = "bytes_from" // By amount of bytes received
	ByLast     SortOpt = "last"       // By the last activity
	ByIdle     SortOpt = "idle"       // By the amount of inactivity
	ByUptime   SortOpt = "uptime"     // By the amount of time connections exist
	ByStop     SortOpt = "stop"       // By the stop time for a closed connection
	ByReason   SortOpt = "reason"     // By the reason for a closed connection
	ByRTT      SortOpt = "rtt"        // By the round trip time
)

// Individual sort options provide the Less for sort.Interface. Len and Swap are on cList.
// CID
type SortByCid struct{ ConnInfos }

func (l SortByCid) Less(i, j int) bool { return l.ConnInfos[i].Cid < l.ConnInfos[j].Cid }

// Number of Subscriptions
type SortBySubs struct{ ConnInfos }

func (l SortBySubs) Less(i, j int) bool { return l.ConnInfos[i].NumSubs < l.ConnInfos[j].NumSubs }

// Pending Bytes
type SortByPending struct{ ConnInfos }

func (l SortByPending) Less(i, j int) bool { return l.ConnInfos[i].Pending < l.ConnInfos[j].Pending }

// Outbound Msgs
type SortByOutMsgs struct{ ConnInfos }

func (l SortByOutMsgs) Less(i, j int) bool { return l.ConnInfos[i].OutMsgs < l.ConnInfos[j].OutMsgs }

// Inbound Msgs
type SortByInMsgs struct{ ConnInfos }

func (l SortByInMsgs) Less(i, j int) bool { return l.ConnInfos[i].InMsgs < l.ConnInfos[j].InMsgs }

// Outbound Bytes
type SortByOutBytes struct{ ConnInfos }

func (l SortByOutBytes) Less(i, j int) bool { return l.ConnInfos[i].OutBytes < l.ConnInfos[j].OutBytes }

// Inbound Bytes
type SortByInBytes struct{ ConnInfos }

func (l SortByInBytes) Less(i, j int) bool { return l.ConnInfos[i].InBytes < l.ConnInfos[j].InBytes }

// Last Activity
type SortByLast struct{ ConnInfos }

func (l SortByLast) Less(i, j int) bool {
	return l.ConnInfos[i].LastActivity.UnixNano() < l.ConnInfos[j].LastActivity.UnixNano()
}

// Idle time
type SortByIdle struct {
	ConnInfos
	now time.Time
}

func (l SortByIdle) Less(i, j int) bool {
	return l.now.Sub(l.ConnInfos[i].LastActivity) < l.now.Sub(l.ConnInfos[j].LastActivity)
}

// Uptime
type SortByUptime struct {
	ConnInfos
	now time.Time
}

func (l SortByUptime) Less(i, j int) bool {
	ci := l.ConnInfos[i]
	cj := l.ConnInfos[j]
	var upi, upj time.Duration
	if ci.Stop == nil || ci.Stop.IsZero() {
		upi = l.now.Sub(ci.Start)
	} else {
		upi = ci.Stop.Sub(ci.Start)
	}
	if cj.Stop == nil || cj.Stop.IsZero() {
		upj = l.now.Sub(cj.Start)
	} else {
		upj = cj.Stop.Sub(cj.Start)
	}
	return upi < upj
}

// Stop
type SortByStop struct{ ConnInfos }

func (l SortByStop) Less(i, j int) bool {
	ciStop := l.ConnInfos[i].Stop
	cjStop := l.ConnInfos[j].Stop
	return ciStop.Before(*cjStop)
}

// Reason
type SortByReason struct{ ConnInfos }

func (l SortByReason) Less(i, j int) bool {
	return l.ConnInfos[i].Reason < l.ConnInfos[j].Reason
}

// RTT - Default is descending
type SortByRTT struct{ ConnInfos }

func (l SortByRTT) Less(i, j int) bool { return l.ConnInfos[i].rtt < l.ConnInfos[j].rtt }

// IsValid determines if a sort option is valid
func (s SortOpt) IsValid() bool {
	switch s {
	case _EMPTY_, ByCid, ByStart, BySubs, ByPending, ByOutMsgs, ByInMsgs, ByOutBytes, ByInBytes, ByLast, ByIdle, ByUptime, ByStop, ByReason, ByRTT:
		return true
	default:
		return false
	}
}
