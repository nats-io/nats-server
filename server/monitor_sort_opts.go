// Copyright 2013-2016 Apcera Inc. All rights reserved.

package server

import (
	"sync/atomic"
	"time"
)

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

type ClientInfo struct {
	client   *client
	cid      uint64
	subCount uint32
	buffered int
	outMsgs  int64
	inMsgs   int64
	outBytes int64
	inBytes  int64
	last     time.Time
}

func NewClientInfo(c *client) *ClientInfo {
	o := &ClientInfo{}
	c.mu.Lock()
	o.client = c
	o.cid = c.cid
	o.subCount = c.subs.Count()
	o.buffered = c.bw.Buffered()
	o.outMsgs = c.outMsgs
	o.outBytes = c.outBytes
	o.last = c.last
	// inMsgs and inBytes are updated outside of client's lock.
	// So use atomic here to read (and updated in processMsg)
	o.inMsgs = atomic.LoadInt64(&c.inMsgs)
	o.inBytes = atomic.LoadInt64(&c.inBytes)
	c.mu.Unlock()
	return o
}

type ByCid []*ClientInfo

func (d ByCid) Len() int {
	return len(d)
}
func (d ByCid) Swap(i, j int) {
	d[i], d[j] = d[j], d[i]
}
func (d ByCid) Less(i, j int) bool {
	return d[i].cid < d[j].cid
}

type BySubs []*ClientInfo

func (d BySubs) Len() int {
	return len(d)
}
func (d BySubs) Swap(i, j int) {
	d[i], d[j] = d[j], d[i]
}
func (d BySubs) Less(i, j int) bool {
	return d[i].subCount < d[j].subCount
}

type ByPending []*ClientInfo

func (d ByPending) Len() int {
	return len(d)
}
func (d ByPending) Swap(i, j int) {
	d[i], d[j] = d[j], d[i]
}
func (d ByPending) Less(i, j int) bool {
	return d[i].buffered < d[j].buffered
}

type ByOutMsgs []*ClientInfo

func (d ByOutMsgs) Len() int {
	return len(d)
}
func (d ByOutMsgs) Swap(i, j int) {
	d[i], d[j] = d[j], d[i]
}
func (d ByOutMsgs) Less(i, j int) bool {
	return d[i].outMsgs < d[j].outMsgs
}

type ByInMsgs []*ClientInfo

func (d ByInMsgs) Len() int {
	return len(d)
}
func (d ByInMsgs) Swap(i, j int) {
	d[i], d[j] = d[j], d[i]
}
func (d ByInMsgs) Less(i, j int) bool {
	return d[i].inMsgs < d[j].inMsgs
}

type ByOutBytes []*ClientInfo

func (d ByOutBytes) Len() int {
	return len(d)
}
func (d ByOutBytes) Swap(i, j int) {
	d[i], d[j] = d[j], d[i]
}
func (d ByOutBytes) Less(i, j int) bool {
	return d[i].outBytes < d[j].outBytes
}

type ByInBytes []*ClientInfo

func (d ByInBytes) Len() int {
	return len(d)
}
func (d ByInBytes) Swap(i, j int) {
	d[i], d[j] = d[j], d[i]
}
func (d ByInBytes) Less(i, j int) bool {
	return d[i].inBytes < d[j].inBytes
}
