// Copyright 2013-2015 Apcera Inc. All rights reserved.

package server

// Helper types to sort by ConnInfo values
type SortOpt string

const (
	byCid      SortOpt = "cid"
	bySubs             = "subs"
	byOutMsgs          = "msgs_to"
	byInMsgs           = "msgs_from"
	byOutBytes         = "bytes_to"
	byInBytes          = "bytes_from"
)

type Pair struct {
	Key uint64
	Val *client
}

type ByCid []Pair

func (d ByCid) Len() int {
	return len(d)
}
func (d ByCid) Swap(i, j int) {
	d[uint64(i)], d[uint64(j)] = d[uint64(j)], d[uint64(i)]
}
func (d ByCid) Less(i, j int) bool {
	return d[uint64(i)].Val.cid < d[uint64(j)].Val.cid
}

type BySubs []Pair

func (d BySubs) Len() int {
	return len(d)
}
func (d BySubs) Swap(i, j int) {
	d[uint64(i)], d[uint64(j)] = d[uint64(j)], d[uint64(i)]
}
func (d BySubs) Less(i, j int) bool {
	return d[uint64(i)].Val.subs.Count() < d[uint64(j)].Val.subs.Count()
}

type ByOutMsgs []Pair

func (d ByOutMsgs) Len() int {
	return len(d)
}
func (d ByOutMsgs) Swap(i, j int) {
	d[uint64(i)], d[uint64(j)] = d[uint64(j)], d[uint64(i)]
}
func (d ByOutMsgs) Less(i, j int) bool {
	return d[uint64(i)].Val.outMsgs < d[uint64(j)].Val.outMsgs
}

type ByInMsgs []Pair

func (d ByInMsgs) Len() int {
	return len(d)
}
func (d ByInMsgs) Swap(i, j int) {
	d[uint64(i)], d[uint64(j)] = d[uint64(j)], d[uint64(i)]
}
func (d ByInMsgs) Less(i, j int) bool {
	return d[uint64(i)].Val.inMsgs < d[uint64(j)].Val.inMsgs
}

type ByOutBytes []Pair

func (d ByOutBytes) Len() int {
	return len(d)
}
func (d ByOutBytes) Swap(i, j int) {
	d[uint64(i)], d[uint64(j)] = d[uint64(j)], d[uint64(i)]
}
func (d ByOutBytes) Less(i, j int) bool {
	return d[uint64(i)].Val.outBytes < d[uint64(j)].Val.outBytes
}

type ByInBytes []Pair

func (d ByInBytes) Len() int {
	return len(d)
}
func (d ByInBytes) Swap(i, j int) {
	d[uint64(i)], d[uint64(j)] = d[uint64(j)], d[uint64(i)]
}
func (d ByInBytes) Less(i, j int) bool {
	return d[uint64(i)].Val.inBytes < d[uint64(j)].Val.inBytes
}
