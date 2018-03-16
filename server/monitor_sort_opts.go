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

// SortOpt is a helper type to sort by ConnInfo values
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
