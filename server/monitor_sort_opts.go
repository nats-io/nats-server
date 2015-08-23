// Copyright 2013-2015 Apcera Inc. All rights reserved.

package server

// Helper types to sort by ConnInfo values
type SortOpt string

const (
	byCid      SortOpt = "cid"
	bySubs             = "subs"
	byPending          = "pending"
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
	d[i], d[j] = d[j], d[i]
}
func (d ByCid) Less(i, j int) bool {
	return d[i].Val.cid < d[j].Val.cid
}

type BySubs []Pair

func (d BySubs) Len() int {
	return len(d)
}
func (d BySubs) Swap(i, j int) {
	d[i], d[j] = d[j], d[i]
}
func (d BySubs) Less(i, j int) bool {
	return d[i].Val.subs.Count() < d[j].Val.subs.Count()
}

type ByPending []Pair

func (d ByPending) Len() int {
	return len(d)
}
func (d ByPending) Swap(i, j int) {
	d[i], d[j] = d[j], d[i]
}
func (d ByPending) Less(i, j int) bool {
	client := d[i].Val
	client.mu.Lock()
	bwi := client.bw.Buffered()
	client.mu.Unlock()
	client = d[j].Val
	client.mu.Lock()
	bwj := client.bw.Buffered()
	client.mu.Unlock()
	return bwi < bwj
}

type ByOutMsgs []Pair

func (d ByOutMsgs) Len() int {
	return len(d)
}
func (d ByOutMsgs) Swap(i, j int) {
	d[i], d[j] = d[j], d[i]
}
func (d ByOutMsgs) Less(i, j int) bool {
	return d[i].Val.outMsgs < d[j].Val.outMsgs
}

type ByInMsgs []Pair

func (d ByInMsgs) Len() int {
	return len(d)
}
func (d ByInMsgs) Swap(i, j int) {
	d[i], d[j] = d[j], d[i]
}
func (d ByInMsgs) Less(i, j int) bool {
	return d[i].Val.inMsgs < d[j].Val.inMsgs
}

type ByOutBytes []Pair

func (d ByOutBytes) Len() int {
	return len(d)
}
func (d ByOutBytes) Swap(i, j int) {
	d[i], d[j] = d[j], d[i]
}
func (d ByOutBytes) Less(i, j int) bool {
	return d[i].Val.outBytes < d[j].Val.outBytes
}

type ByInBytes []Pair

func (d ByInBytes) Len() int {
	return len(d)
}
func (d ByInBytes) Swap(i, j int) {
	d[i], d[j] = d[j], d[i]
}
func (d ByInBytes) Less(i, j int) bool {
	return d[i].Val.inBytes < d[j].Val.inBytes
}
