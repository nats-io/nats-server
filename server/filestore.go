// Copyright 2019 The NATS Authors
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
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"hash"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/minio/highwayhash"
)

type FileStoreConfig struct {
	// Where the parent directory for all storage will be located.
	StoreDir string
	// BlockSize is the file block size. This also represents the maximum overhead size.
	BlockSize uint64
	// ReadCacheExpire is how long with no activity til we expire the read cache.
	ReadCacheExpire time.Duration
	// SyncInterval is how often we sync to disk in the background.
	SyncInterval time.Duration
}

type fileStore struct {
	mu      sync.RWMutex
	stats   MsgSetStats
	scb     func(int64)
	ageChk  *time.Timer
	syncTmr *time.Timer
	cfg     MsgSetConfig
	fcfg    FileStoreConfig
	blks    []*msgBlock
	lmb     *msgBlock
	hh      hash.Hash64
	wmb     *bytes.Buffer
	fch     chan struct{}
	qch     chan struct{}
	bad     []uint64
	closed  bool
}

// Represents a message store block and its data.
type msgBlock struct {
	mfn    string
	mfd    *os.File
	ifn    string
	ifd    *os.File
	index  uint64
	bytes  uint64
	msgs   uint64
	first  msgId
	last   msgId
	dmap   map[uint64]struct{}
	dch    chan struct{}
	qch    chan struct{}
	cache  map[uint64]*fileStoredMsg
	ctmr   *time.Timer
	cbytes uint64
	cgenid uint64
	cloads uint64
	lchk   [8]byte
}

type msgId struct {
	seq uint64
	ts  int64
}

type fileStoredMsg struct {
	subj string
	msg  []byte
	seq  uint64
	ts   int64 // nanoseconds
	off  int64 // offset into block file
}

const (
	// This is where we keep the message store blocks.
	msgDir = "msgs"
	// used to scan blk file names.
	blkScan = "%d.blk"
	// used to scan index file names.
	indexScan = "%d.idx"
	// This is where we keep state on observers.
	obsDir = "obs"
	// Maximum size of a write buffer we may consider for re-use.
	maxBufReuse = 4 * 1024 * 1024
	// Default stream block size.
	defaultStreamBlockSize = 128 * 1024 * 1024 // 128MB
	// Default for workqueue or interest based.
	defaultOtherBlockSize = 32 * 1024 * 1024 // 32MB
	// max block size for now.
	maxBlockSize = defaultStreamBlockSize
	// default cache expiration
	defaultCacheExpiration = 2 * time.Second
	// default sync interval
	defaultSyncInterval = 10 * time.Second
)

func newFileStore(fcfg FileStoreConfig, cfg MsgSetConfig) (*fileStore, error) {
	if cfg.Name == "" {
		return nil, fmt.Errorf("name required")
	}
	if cfg.Storage != FileStorage {
		return nil, fmt.Errorf("fileStore requires file storage type in config")
	}
	// Default values.
	if fcfg.BlockSize == 0 {
		fcfg.BlockSize = dynBlkSize(cfg.Retention, cfg.MaxBytes)
	}
	if fcfg.BlockSize > maxBlockSize {
		return nil, fmt.Errorf("filestore max block size is %s", FriendlyBytes(maxBlockSize))
	}
	if fcfg.ReadCacheExpire == 0 {
		fcfg.ReadCacheExpire = defaultCacheExpiration
	}
	if fcfg.SyncInterval == 0 {
		fcfg.SyncInterval = defaultSyncInterval
	}

	// Check the directory
	if stat, err := os.Stat(fcfg.StoreDir); os.IsNotExist(err) {
		return nil, fmt.Errorf("store directory does not exist")
	} else if stat == nil || !stat.IsDir() {
		return nil, fmt.Errorf("store directory is not a directory")
	}
	tmpfile, err := ioutil.TempFile(fcfg.StoreDir, "_test_")
	if err != nil {
		return nil, fmt.Errorf("storage directory is not writable")
	}
	os.Remove(tmpfile.Name())

	fs := &fileStore{
		fcfg: fcfg,
		cfg:  cfg,
		wmb:  &bytes.Buffer{},
		fch:  make(chan struct{}),
		qch:  make(chan struct{}),
	}

	// Check if this is a new setup.
	mdir := path.Join(fcfg.StoreDir, msgDir)
	odir := path.Join(fcfg.StoreDir, obsDir)
	if err := os.MkdirAll(mdir, 0755); err != nil {
		return nil, fmt.Errorf("could not create message storage directory - %v", err)
	}
	if err := os.MkdirAll(odir, 0755); err != nil {
		return nil, fmt.Errorf("could not create message storage directory - %v", err)
	}

	// Create highway hash for message blocks. Use 256 hash of directory as key.
	key := sha256.Sum256([]byte(mdir))
	fs.hh, err = highwayhash.New64(key[:])
	if err != nil {
		return nil, fmt.Errorf("could not create hash: %v", err)
	}

	if err := fs.recoverState(); err != nil {
		return nil, err
	}

	go fs.flushLoop(fs.fch, fs.qch)

	fs.syncTmr = time.AfterFunc(fs.fcfg.SyncInterval, fs.syncBlocks)

	return fs, nil
}

func dynBlkSize(retention RetentionPolicy, maxBytes int64) uint64 {
	if retention == StreamPolicy {
		// TODO(dlc) - Make the blocksize relative to this if set.
		return defaultStreamBlockSize
	} else {
		// TODO(dlc) - Make the blocksize relative to this if set.
		return defaultOtherBlockSize
	}
}

func (ms *fileStore) recoverState() error {
	return ms.recoverMsgs()
	// FIXME(dlc) - Observables
}

const msgHdrSize = 22
const indexHdrSize = 56

func (ms *fileStore) recoverMsgBlock(fi os.FileInfo, index uint64) *msgBlock {
	var le = binary.LittleEndian

	mb := &msgBlock{index: index}

	mb.mfn = path.Join(ms.fcfg.StoreDir, msgDir, fi.Name())
	mb.ifn = path.Join(ms.fcfg.StoreDir, msgDir, fmt.Sprintf(indexScan, index))

	// Open up the message file, but we will try to recover from the index file.
	// We will check that the last checksums match.
	file, err := os.Open(mb.mfn)
	if err != nil {
		return nil
	}
	defer file.Close()

	// Read our index file. Use this as source of truth if possible.
	if err := mb.readIndexInfo(); err == nil {
		// Quick sanity check here.
		// Note this only checks that the message blk file is not newer then this file.
		var lchk [8]byte
		file.ReadAt(lchk[:], fi.Size()-8)
		if bytes.Equal(lchk[:], mb.lchk[:]) {
			ms.blks = append(ms.blks, mb)
			ms.lmb = mb
			return mb
		}
		// Fall back on the data file itself. We will keep the delete map if present.
		mb.msgs = 0
		mb.bytes = 0
		mb.first.seq = 0
	}
	// Use data file itself to rebuild.
	var hdr [msgHdrSize]byte
	var offset int64

	for {
		if _, err := file.ReadAt(hdr[:], offset); err != nil {
			// FIXME(dlc) - If this is not EOF we probably should try to fix.
			break
		}
		rl := le.Uint32(hdr[0:])
		seq := le.Uint64(hdr[4:])
		// This is an erased message.
		if seq == 0 {
			offset += int64(rl)
			continue
		}
		ts := int64(le.Uint64(hdr[12:]))
		if mb.first.seq == 0 {
			mb.first.seq = seq
			mb.first.ts = ts
		}
		mb.last.seq = seq
		mb.last.ts = ts

		mb.msgs++
		mb.bytes += uint64(rl)
		offset += int64(rl)
	}
	// Rewrite this to make sure we are sync'd.
	mb.writeIndexInfo()
	ms.blks = append(ms.blks, mb)
	ms.lmb = mb
	return mb
}

func (ms *fileStore) recoverMsgs() error {
	mdir := path.Join(ms.fcfg.StoreDir, msgDir)
	fis, err := ioutil.ReadDir(mdir)
	if err != nil {
		return fmt.Errorf("storage directory not readable")
	}
	// Recover all of the msg blocks.
	for _, fi := range fis {
		var index uint64
		if n, err := fmt.Sscanf(fi.Name(), blkScan, &index); err == nil && n == 1 {
			if mb := ms.recoverMsgBlock(fi, index); mb != nil {
				if ms.stats.FirstSeq == 0 {
					ms.stats.FirstSeq = mb.first.seq
				}
				if mb.last.seq > ms.stats.LastSeq {
					ms.stats.LastSeq = mb.last.seq
				}
				ms.stats.Msgs += mb.msgs
				ms.stats.Bytes += mb.bytes
			}
		}
	}

	// Limits checks and enforcement.
	ms.enforceMsgLimit()
	ms.enforceBytesLimit()
	// Do age checks to, make sure to call in place.
	if ms.cfg.MaxAge != 0 {
		ms.startAgeChk()
		ms.expireMsgs()
	}

	if len(ms.blks) == 0 {
		_, err = ms.newMsgBlockForWrite()
	}

	return err
}

// This rolls to a new append msg block.
func (ms *fileStore) newMsgBlockForWrite() (*msgBlock, error) {
	var index uint64

	if ms.lmb != nil {
		index = ms.lmb.index + 1
		ms.flushPendingWritesLocked()
		ms.closeLastMsgBlock(false)
	} else {
		index = 1
	}

	mb := &msgBlock{index: index}
	ms.blks = append(ms.blks, mb)
	ms.lmb = mb

	mb.mfn = path.Join(ms.fcfg.StoreDir, msgDir, fmt.Sprintf(blkScan, mb.index))
	mfd, err := os.OpenFile(mb.mfn, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("Error creating msg block file [%q]: %v", mb.mfn, err)
	}
	mb.mfd = mfd

	mb.ifn = path.Join(ms.fcfg.StoreDir, msgDir, fmt.Sprintf(indexScan, mb.index))
	ifd, err := os.OpenFile(mb.ifn, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("Error creating msg index file [%q]: %v", mb.mfn, err)
	}
	mb.ifd = ifd

	return mb, nil
}

// Store stores a message.
func (ms *fileStore) StoreMsg(subj string, msg []byte) (uint64, error) {
	ms.mu.Lock()
	seq := ms.stats.LastSeq + 1

	if ms.stats.FirstSeq == 0 {
		ms.stats.FirstSeq = seq
	}

	startBytes := int64(ms.stats.Bytes)

	n, err := ms.writeMsgRecord(seq, subj, msg)
	if err != nil {
		ms.mu.Unlock()
		return 0, err
	}
	ms.kickFlusher()

	ms.stats.Msgs++
	ms.stats.Bytes += n
	ms.stats.LastSeq = seq

	// Limits checks and enforcement.
	ms.enforceMsgLimit()
	ms.enforceBytesLimit()

	// Check it we have and need age expiration timer running.
	if ms.ageChk == nil && ms.cfg.MaxAge != 0 {
		ms.startAgeChk()
	}
	cb := ms.scb
	stopBytes := int64(ms.stats.Bytes)
	ms.mu.Unlock()

	if cb != nil {
		cb(stopBytes - startBytes)
	}

	return seq, nil
}

// Will check the msg limit and drop firstSeq msg if needed.
// Lock should be held.
func (ms *fileStore) enforceMsgLimit() {
	if ms.cfg.MaxMsgs <= 0 || ms.stats.Msgs <= uint64(ms.cfg.MaxMsgs) {
		return
	}
	ms.deleteFirstMsg()
}

// Will check the bytes limit and drop msgs if needed.
// Lock should be held.
func (ms *fileStore) enforceBytesLimit() {
	if ms.cfg.MaxBytes <= 0 || ms.stats.Bytes <= uint64(ms.cfg.MaxBytes) {
		return
	}
	for bs := ms.stats.Bytes; bs > uint64(ms.cfg.MaxBytes); bs = ms.stats.Bytes {
		ms.deleteFirstMsg()
	}
}

func (ms *fileStore) deleteFirstMsg() bool {
	return ms.removeMsg(ms.stats.FirstSeq, false)
}

// RemoveMsg will remove the message from this store.
// Will return the number of bytes removed.
func (ms *fileStore) RemoveMsg(seq uint64) bool {
	ms.mu.Lock()
	removed := ms.removeMsg(seq, false)
	ms.mu.Unlock()
	return removed
}

func (ms *fileStore) EraseMsg(seq uint64) bool {
	ms.mu.Lock()
	removed := ms.removeMsg(seq, true)
	ms.mu.Unlock()
	return removed
}

func (ms *fileStore) removeMsg(seq uint64, secure bool) bool {
	mb := ms.selectMsgBlock(seq)
	if mb == nil {
		return false
	}
	var sm *fileStoredMsg
	if mb.cache != nil {
		sm = mb.cache[seq]
	}
	if sm == nil {
		sm = ms.readAndCacheMsgs(mb, seq)
	}
	// We have the message here, so we can delete it.
	if sm != nil {
		ms.deleteMsgFromBlock(mb, seq, sm, secure)
	}
	return sm != nil
}

// Loop on requests to write out our index file. This is used when calling
// remove for a message. Updates to the last.seq etc are handled by main
// flush loop when storing messages.
func (ms *fileStore) flushWriteIndexLoop(mb *msgBlock, dch, qch chan struct{}) {
	for {
		select {
		case <-dch:
			ms.mu.Lock()
			mb.writeIndexInfo()
			ms.mu.Unlock()
		case <-qch:
			return
		}
	}
}

func (mb *msgBlock) kickWriteFlusher() {
	select {
	case mb.dch <- struct{}{}:
	default:
	}
}

func (mb *msgBlock) selectNextFirst() {
	var seq uint64
	for seq = mb.first.seq + 1; seq <= mb.last.seq; seq++ {
		if _, ok := mb.dmap[seq]; ok {
			// We will move past this so we can delete the entry.
			delete(mb.dmap, seq)
		} else {
			break
		}
	}
	mb.first.seq = seq
}

// Lock should be held.
func (ms *fileStore) deleteMsgFromBlock(mb *msgBlock, seq uint64, sm *fileStoredMsg, secure bool) {
	// Update global accounting.
	msz := fileStoreMsgSize(sm.subj, sm.msg)
	ms.stats.Msgs--
	ms.stats.Bytes -= msz

	// Now local updates.
	mb.msgs--
	mb.bytes -= msz
	mb.cgenid++

	// Delete cache entry
	if mb.cache != nil {
		delete(mb.cache, seq)
	}

	var shouldWriteIndex bool

	// Optimize for FIFO case.
	if seq == mb.first.seq {
		mb.selectNextFirst()
		if seq == ms.stats.FirstSeq {
			ms.stats.FirstSeq = mb.first.seq
		}
		if mb.first.seq > mb.last.seq {
			ms.removeMsgBlock(mb)
		} else {
			shouldWriteIndex = true
		}
	} else {
		// Out of order delete.
		if mb.dmap == nil {
			mb.dmap = make(map[uint64]struct{})
		}
		mb.dmap[seq] = struct{}{}
		shouldWriteIndex = true
	}
	if secure {
		ms.eraseMsg(mb, sm)
	}
	if shouldWriteIndex {
		if mb.dch == nil {
			// Spin up the write flusher.
			mb.qch = make(chan struct{})
			mb.dch = make(chan struct{})
			go ms.flushWriteIndexLoop(mb, mb.dch, mb.qch)
			// Write first one in place.
			mb.writeIndexInfo()
		} else {
			mb.kickWriteFlusher()
		}
	}
}

// Lock should be held.
func (ms *fileStore) doExpireTimer(mb *msgBlock) {
	genid := mb.cgenid
	if mb.ctmr == nil {
		mb.ctmr = time.AfterFunc(ms.fcfg.ReadCacheExpire, func() { ms.expireCache(mb, genid) })
	} else {
		mb.ctmr.Reset(ms.fcfg.ReadCacheExpire)
	}
}

// Called to possibly expire a message block read cache.
func (ms *fileStore) expireCache(mb *msgBlock, genid uint64) {
	ms.mu.Lock()
	if genid == mb.cgenid {
		mb.cbytes = 0
		mb.cache = nil
	} else {
		genid := mb.cgenid
		mb.ctmr = time.AfterFunc(ms.fcfg.ReadCacheExpire, func() { ms.expireCache(mb, genid) })
	}
	ms.mu.Unlock()
}

func (ms *fileStore) startAgeChk() {
	if ms.ageChk == nil && ms.cfg.MaxAge != 0 {
		ms.ageChk = time.AfterFunc(ms.cfg.MaxAge, ms.expireMsgs)
	}
}

// Will expire msgs that are too old.
func (ms *fileStore) expireMsgs() {
	now := time.Now().UnixNano()
	minAge := now - int64(ms.cfg.MaxAge)

	for {
		if sm := ms.msgForSeq(0); sm != nil && sm.ts <= minAge {
			ms.mu.Lock()
			ms.deleteFirstMsg()
			ms.mu.Unlock()
		} else {
			ms.mu.Lock()
			if sm == nil {
				if ms.ageChk != nil {
					ms.ageChk.Stop()
					ms.ageChk = nil
				}
			} else {
				fireIn := time.Duration(sm.ts-now) + ms.cfg.MaxAge
				ms.ageChk.Reset(fireIn)
			}
			ms.mu.Unlock()
			return
		}
	}
}

// Check all the checksums for a message block.
func checkMsgBlockFile(fp *os.File, hh hash.Hash) []uint64 {
	var le = binary.LittleEndian
	var hdr [msgHdrSize]byte
	var bad []uint64

	r := bufio.NewReaderSize(fp, 32*1024*1024)

	for {
		if _, err := io.ReadFull(r, hdr[0:]); err != nil {
			break
		}
		rl := le.Uint32(hdr[0:])
		seq := le.Uint64(hdr[4:])
		slen := le.Uint16(hdr[20:])
		dlen := int(rl) - msgHdrSize
		if dlen < 0 || int(slen) > dlen || dlen > int(rl) {
			bad = append(bad, seq)
			break
		}
		data := make([]byte, dlen)
		if _, err := io.ReadFull(r, data); err != nil {
			bad = append(bad, seq)
			break
		}
		hh.Reset()
		hh.Write(hdr[4:20])
		hh.Write(data[:slen])
		hh.Write(data[slen : dlen-8])
		checksum := hh.Sum(nil)
		if !bytes.Equal(checksum, data[len(data)-8:]) {
			bad = append(bad, seq)
		}
	}
	return bad
}

// This will check all the checksums on messages and report back any sequence numbers with errors.
func (ms *fileStore) checkMsgs() []uint64 {
	ms.mu.Lock()
	if ms.wmb.Len() > 0 {
		ms.flushPendingWritesLocked()
	}
	ms.mu.Unlock()

	mdir := path.Join(ms.fcfg.StoreDir, msgDir)
	fis, err := ioutil.ReadDir(mdir)
	if err != nil {
		return nil
	}

	key := sha256.Sum256([]byte(mdir))
	hh, _ := highwayhash.New64(key[:])

	// Check all of the msg blocks.
	for _, fi := range fis {
		var index uint64
		if n, err := fmt.Sscanf(fi.Name(), blkScan, &index); err == nil && n == 1 {
			if fp, err := os.Open(path.Join(mdir, fi.Name())); err != nil {
				continue
			} else {
				ms.bad = append(ms.bad, checkMsgBlockFile(fp, hh)...)
				fp.Close()
			}
		}
	}
	return ms.bad
}

// This will kick out our flush routine if its waiting.
func (ms *fileStore) kickFlusher() {
	select {
	case ms.fch <- struct{}{}:
	default:
	}
}

func (ms *fileStore) flushLoop(fch, qch chan struct{}) {
	for {
		select {
		case <-fch:
			ms.flushPendingWrites()
		case <-qch:
			return
		}
	}
}

// Lock should be held.
func (ms *fileStore) writeMsgRecord(seq uint64, subj string, msg []byte) (uint64, error) {
	var err error

	// Get size for this message.
	rl := fileStoreMsgSize(subj, msg)

	// Grab our current last message block.
	mb := ms.lmb
	if mb == nil || mb.bytes+rl > ms.fcfg.BlockSize {
		if mb, err = ms.newMsgBlockForWrite(); err != nil {
			return 0, err
		}
	}

	// Make sure we have room.
	ms.wmb.Grow(int(rl))

	// Grab time
	ts := time.Now().UnixNano()

	// Update accounting.
	// Update our index info.
	if mb.first.seq == 0 {
		mb.first.seq = seq
		mb.first.ts = ts
	}
	mb.last.seq = seq
	mb.last.ts = ts
	mb.bytes += rl
	mb.msgs++

	// First write header, etc.
	var le = binary.LittleEndian
	var hdr [msgHdrSize]byte

	le.PutUint32(hdr[0:], uint32(rl))
	le.PutUint64(hdr[4:], seq)
	le.PutUint64(hdr[12:], uint64(ts))
	le.PutUint16(hdr[20:], uint16(len(subj)))

	// Now write to underlying buffer.
	ms.wmb.Write(hdr[:])
	ms.wmb.WriteString(subj)
	ms.wmb.Write(msg)

	// Calculate hash.
	ms.hh.Reset()
	ms.hh.Write(hdr[4:20])
	ms.hh.Write([]byte(subj))
	ms.hh.Write(msg)
	checksum := ms.hh.Sum(nil)
	// Write to msg record.
	ms.wmb.Write(checksum)
	// Grab last checksum
	copy(mb.lchk[0:], checksum)

	return uint64(rl), nil
}

// Will rewrite the message in the underlying store.
func (ms *fileStore) eraseMsg(mb *msgBlock, sm *fileStoredMsg) error {
	if sm == nil || sm.off < 0 {
		return fmt.Errorf("bad stored message")
	}
	// erase contents and rewrite with new hash.
	rand.Read(sm.msg)
	sm.seq, sm.ts = 0, 0
	chars := []rune("ABCDEFGHIJKLMNOPQRSTUVWXYZ")
	var b strings.Builder
	for i := 0; i < len(sm.subj); i++ {
		b.WriteRune(chars[rand.Intn(len(chars))])
	}
	sm.subj = b.String()

	var le = binary.LittleEndian
	var hdr [msgHdrSize]byte

	rl := fileStoreMsgSize(sm.subj, sm.msg)

	le.PutUint32(hdr[0:], uint32(rl))
	le.PutUint64(hdr[4:], 0)
	le.PutUint64(hdr[12:], 0)
	le.PutUint16(hdr[20:], uint16(len(sm.subj)))

	// Now write to underlying buffer.
	var wmb bytes.Buffer

	wmb.Write(hdr[:])
	wmb.WriteString(sm.subj)
	wmb.Write(sm.msg)

	// Calculate hash.
	ms.hh.Reset()
	ms.hh.Write(hdr[4:20])
	ms.hh.Write([]byte(sm.subj))
	ms.hh.Write(sm.msg)
	checksum := ms.hh.Sum(nil)
	// Write to msg record.
	wmb.Write(checksum)

	mfd, err := os.OpenFile(mb.mfn, os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	_, err = mfd.WriteAt(wmb.Bytes(), sm.off)

	mfd.Sync()
	mfd.Close()

	return err
}

// Sync msg and index files as needed. This is called from a timer.
func (ms *fileStore) syncBlocks() {
	ms.mu.Lock()
	if ms.closed {
		ms.mu.Unlock()
		return
	}
	for _, mb := range ms.blks {
		if mb.mfd != nil {
			mb.mfd.Sync()
		}
		if mb.ifd != nil {
			mb.ifd.Sync()
		}
	}
	ms.syncTmr = time.AfterFunc(ms.fcfg.SyncInterval, ms.syncBlocks)
	ms.mu.Unlock()
}

// Select the message block where this message should be found.
// Return nil if not in the set.
// Read lock should be held.
func (ms *fileStore) selectMsgBlock(seq uint64) *msgBlock {
	if seq < ms.stats.FirstSeq || seq > ms.stats.LastSeq {
		return nil
	}
	for _, mb := range ms.blks {
		if seq >= mb.first.seq && seq <= mb.last.seq {
			return mb
		}
	}
	return nil
}

// Read and cache message from the underlying block.
func (ms *fileStore) readAndCacheMsgs(mb *msgBlock, seq uint64) *fileStoredMsg {
	// This detects if what we may be looking for is staged in the write buffer.
	if mb == ms.lmb && ms.wmb.Len() > 0 {
		ms.flushPendingWritesLocked()
	}
	if mb.cache == nil {
		mb.cache = make(map[uint64]*fileStoredMsg)
	}

	// TODO(dlc) - Could reuse if already open fd. Also release locks for
	// load in parallel. For now opt for simple approach.
	buf, err := ioutil.ReadFile(mb.mfn)
	if err != nil {
		// FIXME(dlc) - complain somehow.
		return nil
	}

	var le = binary.LittleEndian
	var sm *fileStoredMsg

	// Read until we get our message, cache the rest.
	for index, skip := 0, 0; index < len(buf); {
		hdr := buf[index : index+msgHdrSize]
		rl := le.Uint32(hdr[0:])
		dlen := int(rl) - msgHdrSize
		mseq := le.Uint64(hdr[4:])

		// Skip if we already have it in our cache.
		if mb.cache[mseq] != nil {
			// Skip over
			index += int(rl)
			skip += int(rl)
			continue
		}
		// If we have a delete map check it.
		if mb.dmap != nil {
			if _, ok := mb.dmap[mseq]; ok {
				// Skip over
				index += int(rl)
				skip += int(rl)
				continue
			}
		}
		// Read in the message.
		ts := int64(le.Uint64(hdr[12:]))
		slen := le.Uint16(hdr[20:])

		// Do some quick sanity checks here.
		if dlen < 0 || int(slen) > dlen || dlen > int(rl) {
			// This means something is off.
			ms.bad = append(ms.bad, seq)
			index += int(rl)
			skip += int(rl)
			continue
		}
		index += msgHdrSize
		data := buf[index : index+dlen]
		// Check the checksum here.
		ms.hh.Reset()
		ms.hh.Write(hdr[4:20])
		ms.hh.Write(data[:slen])
		ms.hh.Write(data[slen : dlen-8])
		checksum := ms.hh.Sum(nil)
		if !bytes.Equal(checksum, data[len(data)-8:]) {
			index += dlen
			ms.bad = append(ms.bad, seq)
			continue
		}
		msg := &fileStoredMsg{
			subj: string(data[:slen]),
			msg:  data[slen : dlen-8],
			seq:  mseq,
			ts:   ts,
			off:  int64(index - msgHdrSize),
		}
		mb.cache[mseq] = msg
		if mseq == seq {
			sm = msg
		}
		index += dlen
		mb.cbytes += uint64(rl)
	}

	// Setup the cache expiration timer.
	if mb.cbytes > 0 {
		mb.cloads++
		ms.doExpireTimer(mb)
	}

	return sm
}

func (ms *fileStore) checkPrefetch(seq uint64, mb *msgBlock) {
	gap := mb.msgs/20 + 1
	if seq < mb.last.seq-gap {
		return
	}
	nseq := mb.last.seq + 1
	if nmb := ms.selectMsgBlock(nseq); nmb != nil && nmb != mb && nmb.cache == nil {
		nmb.cache = make(map[uint64]*fileStoredMsg)
		go func() {
			ms.mu.Lock()
			ms.readAndCacheMsgs(nmb, nseq)
			ms.mu.Unlock()
		}()
	}
}

// Will return message for the given sequence number.
func (ms *fileStore) msgForSeq(seq uint64) *fileStoredMsg {
	ms.mu.Lock()
	// seq == 0 indidcates we want first msg.
	if seq == 0 {
		seq = ms.stats.FirstSeq
	}
	mb := ms.selectMsgBlock(seq)
	if mb == nil {
		ms.mu.Unlock()
		return nil
	}

	// Check for prefetch
	ms.checkPrefetch(seq, mb)

	// Check cache.
	if mb.cache != nil {
		if sm, ok := mb.cache[seq]; ok {
			mb.cgenid++
			ms.mu.Unlock()
			return sm
		}
	}
	// If we are here we do not have the message in our cache currently.
	sm := ms.readAndCacheMsgs(mb, seq)
	if sm != nil {
		mb.cgenid++
	}
	ms.mu.Unlock()
	return sm
}

// Internal function to return msg parts from a raw buffer.
func msgFromBuf(buf []byte) (string, []byte, uint64, int64, error) {
	if len(buf) < msgHdrSize {
		return "", nil, 0, 0, fmt.Errorf("buf too small for msg")
	}
	var le = binary.LittleEndian
	hdr := buf[:msgHdrSize]
	rl := le.Uint32(hdr[0:])
	dlen := int(rl) - msgHdrSize
	seq := le.Uint64(hdr[4:])
	ts := int64(le.Uint64(hdr[12:]))
	slen := le.Uint16(hdr[20:])
	if dlen < 0 || int(slen) > dlen || dlen > int(rl) {
		return "", nil, 0, 0, fmt.Errorf("malformed or corrupt msg")
	}
	data := buf[msgHdrSize:]
	return string(data[:slen]), data[slen : dlen-8], seq, ts, nil
}

// LoadMsg will lookup the message by sequence number and return it if found.
func (ms *fileStore) LoadMsg(seq uint64) (string, []byte, int64, error) {
	if sm := ms.msgForSeq(seq); sm != nil {
		return sm.subj, sm.msg, sm.ts, nil
	}
	return "", nil, 0, ErrStoreMsgNotFound
}

func (ms *fileStore) Stats() MsgSetStats {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	return ms.stats
}

func fileStoreMsgSize(subj string, msg []byte) uint64 {
	// length of the message record (4bytes) + seq(8) + ts(8) + subj_len(2) + subj + msg + hash(8)
	return uint64(4 + 16 + 2 + len(subj) + len(msg) + 8)
}

// Flush the write buffer to disk.
func (ms *fileStore) flushPendingWrites() {
	ms.mu.Lock()
	ms.flushPendingWritesLocked()
	ms.mu.Unlock()
}

// Lock should be held.
func (ms *fileStore) flushPendingWritesLocked() {
	mb := ms.lmb
	if mb == nil {
		return
	}

	// Append new data to the message block file.
	if lbb := ms.wmb.Len(); lbb > 0 && mb.mfd != nil {
		n, _ := ms.wmb.WriteTo(mb.mfd)
		if int(n) != lbb {
			ms.wmb.Truncate(int(n))
		} else if lbb <= maxBufReuse {
			ms.wmb.Reset()
		} else {
			ms.wmb = &bytes.Buffer{}
		}
	}

	// Now index info
	mb.writeIndexInfo()
}

// Write index info to the appropriate file.
func (mb *msgBlock) writeIndexInfo() error {
	// msgs bytes fseq fts lseq lts
	var le = binary.LittleEndian
	var hdr [indexHdrSize]byte

	le.PutUint64(hdr[0:], mb.msgs)
	le.PutUint64(hdr[8:], mb.bytes)
	le.PutUint64(hdr[16:], mb.first.seq)
	le.PutUint64(hdr[24:], uint64(mb.first.ts))
	le.PutUint64(hdr[32:], mb.last.seq)
	le.PutUint64(hdr[40:], uint64(mb.last.ts))
	// copy last checksum
	copy(hdr[48:], mb.lchk[:])
	buf := hdr[:]
	// Append a delete map if needed
	if len(mb.dmap) > 0 {
		buf = append(buf, mb.genDeleteMap()...)
	}
	var err error
	if mb.ifd == nil {
		ifd, err := os.OpenFile(mb.ifn, os.O_CREATE|os.O_RDWR, 0644)
		if err != nil {
			return err
		}
		mb.ifd = ifd
	}
	if fi, serr := mb.ifd.Stat(); serr == nil {
		fsz := fi.Size()
		if n, _ := mb.ifd.WriteAt(buf, 0); n > 0 && fsz > int64(n) {
			err = mb.ifd.Truncate(int64(n))
		}
	} else {
		err = serr
	}
	return err
}

func (mb *msgBlock) readIndexInfo() error {
	fp, err := os.Open(mb.ifn)
	if err != nil {
		return err
	}
	defer fp.Close()

	var le = binary.LittleEndian
	var hdr [indexHdrSize]byte

	if n, _ := fp.Read(hdr[:]); n != indexHdrSize {
		defer os.Remove(mb.ifn)
		return fmt.Errorf("bad index file")
	}
	// Header first
	mb.msgs = le.Uint64(hdr[0:])
	mb.bytes = le.Uint64(hdr[8:])
	mb.first.seq = le.Uint64(hdr[16:])
	mb.first.ts = int64(le.Uint64(hdr[24:]))
	mb.last.seq = le.Uint64(hdr[32:])
	mb.last.ts = int64(le.Uint64(hdr[40:]))
	copy(mb.lchk[0:], hdr[48:])
	// Now check for presence of a delete map
	if buf, err := ioutil.ReadAll(fp); err == nil && len(buf) > 0 {
		mb.dmap = make(map[uint64]struct{})
		for i := 0; ; {
			if seq, n := binary.Uvarint(buf[i:]); n <= 0 {
				break
			} else {
				i += n
				mb.dmap[seq+mb.first.seq] = struct{}{}
			}
		}
	}
	return nil
}

func (mb *msgBlock) genDeleteMap() []byte {
	if len(mb.dmap) == 0 {
		return nil
	}
	buf := make([]byte, len(mb.dmap)*binary.MaxVarintLen64)
	// We use first seq as an offset to cut down on size.
	fseq, n := uint64(mb.first.seq), 0
	for seq := range mb.dmap {
		// This is for lazy cleanup as the first sequence moves up.
		if seq <= fseq {
			delete(mb.dmap, seq)
		} else {
			n += binary.PutUvarint(buf[n:], seq-fseq)
		}
	}
	return buf[:n]
}

func syncAndClose(mfd, ifd *os.File) {
	if mfd != nil {
		mfd.Sync()
		mfd.Close()
	}
	if ifd != nil {
		ifd.Sync()
		ifd.Close()
	}
}

// Will return total number of cache loads.
func (ms *fileStore) cacheLoads() uint64 {
	var tl uint64
	ms.mu.Lock()
	for _, mb := range ms.blks {
		tl += mb.cloads
	}
	ms.mu.Unlock()
	return tl
}

// Will return total number of cached bytes.
func (ms *fileStore) cacheSize() uint64 {
	var sz uint64
	ms.mu.Lock()
	for _, mb := range ms.blks {
		sz += mb.cbytes
	}
	ms.mu.Unlock()
	return sz
}

// Will return total number of dmapEntries for all msg blocks.
func (ms *fileStore) dmapEntries() int {
	var total int
	ms.mu.Lock()
	for _, mb := range ms.blks {
		total += len(mb.dmap)
	}
	ms.mu.Unlock()
	return total
}

// Purge will remove all messages from this store.
// Will return the number of purged messages.
func (ms *fileStore) Purge() uint64 {
	ms.mu.Lock()
	ms.flushPendingWritesLocked()
	purged := ms.stats.Msgs
	cb := ms.scb
	bytes := int64(ms.stats.Bytes)
	ms.stats.FirstSeq = ms.stats.LastSeq + 1
	ms.stats.Bytes = 0
	ms.stats.Msgs = 0

	blks := ms.blks
	lmb := ms.lmb
	ms.blks = nil
	ms.lmb = nil

	for _, mb := range blks {
		ms.removeMsgBlock(mb)
	}
	// Now place new write msg block with correct info.
	ms.newMsgBlockForWrite()
	if lmb != nil {
		ms.lmb.first = lmb.last
		ms.lmb.first.seq += 1
		ms.lmb.last = lmb.last
		ms.lmb.writeIndexInfo()
	}
	ms.mu.Unlock()

	if cb != nil {
		cb(-bytes)
	}

	return purged
}

// Returns number of msg blks.
func (ms *fileStore) numMsgBlocks() int {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	return len(ms.blks)
}

// Removes the msgBlock
// Lock should be held.
func (ms *fileStore) removeMsgBlock(mb *msgBlock) {
	if mb.ifd != nil {
		mb.ifd.Close()
		mb.ifd = nil
	}
	os.Remove(mb.ifn)
	if mb.mfd != nil {
		mb.mfd.Close()
		mb.mfd = nil
	}
	os.Remove(mb.mfn)

	for i, omb := range ms.blks {
		if mb == omb {
			ms.blks = append(ms.blks[:i], ms.blks[i+1:]...)
			break
		}
	}
	// Check for us being last message block
	if mb == ms.lmb {
		ms.lmb = nil
		ms.newMsgBlockForWrite()
		ms.lmb.first = mb.first
		ms.lmb.last = mb.last
		ms.lmb.writeIndexInfo()
	}
	mb.close(false)
}

func (mb *msgBlock) close(sync bool) {
	if mb == nil {
		return
	}
	// Close cache
	mb.cbytes = 0
	mb.cache = nil
	// Quit our loops.
	if mb.qch != nil {
		close(mb.qch)
		mb.qch = nil
	}
	if sync {
		syncAndClose(mb.mfd, mb.ifd)
	} else {
		go syncAndClose(mb.mfd, mb.ifd)
	}
	mb.mfd = nil
	mb.ifd = nil
}

func (ms *fileStore) closeAllMsgBlocks(sync bool) {
	for _, mb := range ms.blks {
		mb.close(sync)
	}
}

func (ms *fileStore) closeLastMsgBlock(sync bool) {
	ms.lmb.close(sync)
}

func (ms *fileStore) Stop() {
	ms.mu.Lock()
	if ms.closed {
		ms.mu.Unlock()
		return
	}
	ms.closed = true
	close(ms.qch)

	ms.flushPendingWritesLocked()
	ms.wmb = &bytes.Buffer{}
	ms.lmb = nil

	ms.closeAllMsgBlocks(true)

	if ms.syncTmr != nil {
		ms.syncTmr.Stop()
		ms.syncTmr = nil
	}
	if ms.ageChk != nil {
		ms.ageChk.Stop()
		ms.ageChk = nil
	}
	ms.mu.Unlock()
}
