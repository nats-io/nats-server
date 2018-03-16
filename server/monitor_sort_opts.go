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

const (
	byCid      SortOpt = "cid"
	bySubs     SortOpt = "subs"
	byPending  SortOpt = "pending"
	byOutMsgs  SortOpt = "msgs_to"
	byInMsgs   SortOpt = "msgs_from"
	byOutBytes SortOpt = "bytes_to"
	byInBytes  SortOpt = "bytes_from"
	byLast     SortOpt = "last"
	byIdle     SortOpt = "idle"
	byUptime   SortOpt = "uptime"
)

// IsValid determines if a sort option is valid
func (s SortOpt) IsValid() bool {
	switch s {
	case "", byCid, bySubs, byPending, byOutMsgs, byInMsgs, byOutBytes, byInBytes, byLast, byIdle, byUptime:
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
