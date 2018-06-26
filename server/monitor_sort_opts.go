// Copyright 2013-2018 The NATS Authors
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

// Represents a connection info list. We use pointers since it will be sorted.
type ConnInfos []*ConnInfo

// For sorting
func (cl ConnInfos) Len() int      { return len(cl) }
func (cl ConnInfos) Swap(i, j int) { cl[i], cl[j] = cl[j], cl[i] }

// SortOpt is a helper type to sort clients
type SortOpt string

// Possible sort options
const (
	ByCid      SortOpt = "cid"        // By connection ID
	BySubs     SortOpt = "subs"       // By number of subscriptions
	ByPending  SortOpt = "pending"    // By amount of data in bytes waiting to be sent to client
	ByOutMsgs  SortOpt = "msgs_to"    // By number of messages sent
	ByInMsgs   SortOpt = "msgs_from"  // By number of messages received
	ByOutBytes SortOpt = "bytes_to"   // By amount of bytes sent
	ByInBytes  SortOpt = "bytes_from" // By amount of bytes received
	ByLast     SortOpt = "last"       // By the last activity
	ByIdle     SortOpt = "idle"       // By the amount of inactivity
	ByUptime   SortOpt = "uptime"     // By the amount of time connections exist
)

// Individual sort options provide the Less for sort.Interface. Len and Swap are on cList.
// CID
type byCid struct{ ConnInfos }

func (l byCid) Less(i, j int) bool { return l.ConnInfos[i].Cid < l.ConnInfos[j].Cid }

// Number of Subscriptions
type bySubs struct{ ConnInfos }

func (l bySubs) Less(i, j int) bool { return l.ConnInfos[i].NumSubs < l.ConnInfos[j].NumSubs }

// Pending Bytes
type byPending struct{ ConnInfos }

func (l byPending) Less(i, j int) bool { return l.ConnInfos[i].Pending < l.ConnInfos[j].Pending }

// Outbound Msgs
type byOutMsgs struct{ ConnInfos }

func (l byOutMsgs) Less(i, j int) bool { return l.ConnInfos[i].OutMsgs < l.ConnInfos[j].OutMsgs }

// Inbound Msgs
type byInMsgs struct{ ConnInfos }

func (l byInMsgs) Less(i, j int) bool { return l.ConnInfos[i].InMsgs < l.ConnInfos[j].InMsgs }

// Outbound Bytes
type byOutBytes struct{ ConnInfos }

func (l byOutBytes) Less(i, j int) bool { return l.ConnInfos[i].OutBytes < l.ConnInfos[j].OutBytes }

// Inbound Bytes
type byInBytes struct{ ConnInfos }

func (l byInBytes) Less(i, j int) bool { return l.ConnInfos[i].InBytes < l.ConnInfos[j].InBytes }

// Last Activity
type byLast struct{ ConnInfos }

func (l byLast) Less(i, j int) bool {
	return l.ConnInfos[i].LastActivity.UnixNano() < l.ConnInfos[j].LastActivity.UnixNano()
}

// Idle time
type byIdle struct{ ConnInfos }

func (l byIdle) Less(i, j int) bool { return l.ConnInfos[i].Idle < l.ConnInfos[j].Idle }

// IsValid determines if a sort option is valid
func (s SortOpt) IsValid() bool {
	switch s {
	case "", ByCid, BySubs, ByPending, ByOutMsgs, ByInMsgs, ByOutBytes, ByInBytes, ByLast, ByIdle, ByUptime:
		return true
	default:
		return false
	}
}

// Pair type is internally used.
type Pair struct {
	Key *client
	Val int64
}

// Pairs type is internally used.
type Pairs []Pair

func (d Pairs) Len() int {
	return len(d)
}

func (d Pairs) Swap(i, j int) {
	d[i], d[j] = d[j], d[i]
}

func (d Pairs) Less(i, j int) bool {
	return d[i].Val < d[j].Val
}
