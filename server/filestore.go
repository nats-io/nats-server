// Copyright 2019-2020 The NATS Authors
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
	"archive/tar"
	"bufio"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"io/ioutil"
	"math/rand"
	"net"
	"os"
	"path"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/minio/highwayhash"
)

type FileStoreConfig struct {
	// Where the parent directory for all storage will be located.
	StoreDir string
	// BlockSize is the file block size. This also represents the maximum overhead size.
	BlockSize uint64
	// ReadCacheExpire is how long with no activity until we expire the read cache.
	ReadCacheExpire time.Duration
	// SyncInterval is how often we sync to disk in the background.
	SyncInterval time.Duration
}

// FileStreamInfo allows us to remember created time.
type FileStreamInfo struct {
	Created time.Time
	StreamConfig
}

// File ConsumerInfo is used for creating consumer stores.
type FileConsumerInfo struct {
	Created time.Time
	Name    string
	ConsumerConfig
}

type fileStore struct {
	mu      sync.RWMutex
	state   StreamState
	scb     func(int64)
	ageChk  *time.Timer
	syncTmr *time.Timer
	cfg     FileStreamInfo
	fcfg    FileStoreConfig
	lmb     *msgBlock
	blks    []*msgBlock
	hh      hash.Hash64
	wmb     *bytes.Buffer
	fch     chan struct{}
	qch     chan struct{}
	cfs     []*consumerFileStore
	closed  bool
	sips    int
}

// Represents a message store block and its data.
type msgBlock struct {
	mu     sync.RWMutex
	mfn    string
	mfd    *os.File
	ifn    string
	ifd    *os.File
	liwsz  int64
	index  uint64
	bytes  uint64
	msgs   uint64
	first  msgId
	last   msgId
	hh     hash.Hash64
	cache  *cache
	expire time.Duration
	ctmr   *time.Timer
	cgenid uint64
	cloads uint64
	dmap   map[uint64]struct{}
	dch    chan struct{}
	qch    chan struct{}
	lchk   [8]byte
}

type cache struct {
	buf  []byte
	idx  []uint32
	fseq uint64
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
	// Magic is used to identify the file store files.
	magic = uint8(22)
	// Version
	version = uint8(1)
	// hdrLen
	hdrLen = 2
	// This is where we keep the streams.
	streamsDir = "streams"
	// This is where we keep the message store blocks.
	msgDir = "msgs"
	// This is where we temporarily move the messages dir.
	purgeDir = "__msgs__"
	// used to scan blk file names.
	blkScan = "%d.blk"
	// used to scan index file names.
	indexScan = "%d.idx"
	// This is where we keep state on consumers.
	consumerDir = "obs"
	// Index file for a consumer.
	consumerState = "o.dat"
	// This is where we keep state on templates.
	tmplsDir = "templates"
	// Maximum size of a write buffer we may consider for re-use.
	maxBufReuse = 2 * 1024 * 1024
	// Default stream block size.
	defaultStreamBlockSize = 64 * 1024 * 1024 // 64MB
	// Default for workqueue or interest based.
	defaultOtherBlockSize = 32 * 1024 * 1024 // 32MB
	// max block size for now.
	maxBlockSize = 2 * defaultStreamBlockSize
	// default cache expiration
	defaultCacheExpiration = 5 * time.Second
	// default sync interval
	defaultSyncInterval = 10 * time.Second
	// coalesceMinimum
	coalesceMinimum = 64 * 1024

	// Metafiles for streams and consumers.
	JetStreamMetaFile    = "meta.inf"
	JetStreamMetaFileSum = "meta.sum"
)

func newFileStore(fcfg FileStoreConfig, cfg StreamConfig) (*fileStore, error) {
	return newFileStoreWithCreated(fcfg, cfg, time.Now())
}

func newFileStoreWithCreated(fcfg FileStoreConfig, cfg StreamConfig, created time.Time) (*fileStore, error) {
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
		if err := os.MkdirAll(fcfg.StoreDir, 0755); err != nil {
			return nil, fmt.Errorf("could not create storage directory - %v", err)
		}
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
		cfg:  FileStreamInfo{Created: created, StreamConfig: cfg},
		wmb:  &bytes.Buffer{},
		fch:  make(chan struct{}),
		qch:  make(chan struct{}),
	}

	// Check if this is a new setup.
	mdir := path.Join(fcfg.StoreDir, msgDir)
	odir := path.Join(fcfg.StoreDir, consumerDir)
	if err := os.MkdirAll(mdir, 0755); err != nil {
		return nil, fmt.Errorf("could not create message storage directory - %v", err)
	}
	if err := os.MkdirAll(odir, 0755); err != nil {
		return nil, fmt.Errorf("could not create message storage directory - %v", err)
	}

	// Create highway hash for message blocks. Use sha256 of directory as key.
	key := sha256.Sum256([]byte(cfg.Name))
	fs.hh, err = highwayhash.New64(key[:])
	if err != nil {
		return nil, fmt.Errorf("could not create hash: %v", err)
	}

	// Recover our state.
	if err := fs.recoverMsgs(); err != nil {
		return nil, err
	}

	// Write our meta data iff does not exist.
	meta := path.Join(fcfg.StoreDir, JetStreamMetaFile)
	if _, err := os.Stat(meta); err != nil && os.IsNotExist(err) {
		if err := fs.writeStreamMeta(); err != nil {
			return nil, err
		}
	}

	go fs.flushLoop(fs.fch, fs.qch)

	fs.syncTmr = time.AfterFunc(fs.fcfg.SyncInterval, fs.syncBlocks)

	return fs, nil
}

func (fs *fileStore) UpdateConfig(cfg *StreamConfig) error {
	if fs.isClosed() {
		return ErrStoreClosed
	}

	if cfg.Name == "" {
		return fmt.Errorf("name required")
	}
	if cfg.Storage != FileStorage {
		return fmt.Errorf("fileStore requires file storage type in config")
	}

	fs.mu.Lock()
	new_cfg := FileStreamInfo{Created: fs.cfg.Created, StreamConfig: *cfg}
	old_cfg := fs.cfg
	fs.cfg = new_cfg
	if err := fs.writeStreamMeta(); err != nil {
		fs.cfg = old_cfg
		fs.mu.Unlock()
		return err
	}
	// Limits checks and enforcement.
	fs.enforceMsgLimit()
	fs.enforceBytesLimit()
	// Do age timers.
	if fs.ageChk == nil && fs.cfg.MaxAge != 0 {
		fs.startAgeChk()
	}
	if fs.ageChk != nil && fs.cfg.MaxAge == 0 {
		fs.ageChk.Stop()
		fs.ageChk = nil
	}
	fs.mu.Unlock()

	if cfg.MaxAge != 0 {
		fs.expireMsgs()
	}
	return nil
}

func dynBlkSize(retention RetentionPolicy, maxBytes int64) uint64 {
	if retention == LimitsPolicy {
		// TODO(dlc) - Make the blocksize relative to this if set.
		return defaultStreamBlockSize
	} else {
		// TODO(dlc) - Make the blocksize relative to this if set.
		return defaultOtherBlockSize
	}
}

// Write out meta and the checksum.
// Lock should be held.
func (fs *fileStore) writeStreamMeta() error {
	meta := path.Join(fs.fcfg.StoreDir, JetStreamMetaFile)
	if _, err := os.Stat(meta); err != nil && !os.IsNotExist(err) {
		return err
	}
	b, err := json.MarshalIndent(fs.cfg, _EMPTY_, "  ")
	if err != nil {
		return err
	}
	if err := ioutil.WriteFile(meta, b, 0644); err != nil {
		return err
	}
	fs.hh.Reset()
	fs.hh.Write(b)
	checksum := hex.EncodeToString(fs.hh.Sum(nil))
	sum := path.Join(fs.fcfg.StoreDir, JetStreamMetaFileSum)
	if err := ioutil.WriteFile(sum, []byte(checksum), 0644); err != nil {
		return err
	}
	return nil
}

const msgHdrSize = 22
const checksumSize = 8

// This is the max room needed for index header.
const indexHdrSize = 7*binary.MaxVarintLen64 + hdrLen + checksumSize

func (fs *fileStore) recoverMsgBlock(fi os.FileInfo, index uint64) *msgBlock {
	var le = binary.LittleEndian

	mb := &msgBlock{index: index, expire: fs.fcfg.ReadCacheExpire}

	mdir := path.Join(fs.fcfg.StoreDir, msgDir)
	mb.mfn = path.Join(mdir, fi.Name())
	mb.ifn = path.Join(mdir, fmt.Sprintf(indexScan, index))

	if mb.hh == nil {
		key := sha256.Sum256(fs.hashKeyForBlock(index))
		mb.hh, _ = highwayhash.New64(key[:])
	}

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
			fs.blks = append(fs.blks, mb)
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
	fs.blks = append(fs.blks, mb)
	fs.lmb = mb
	return mb
}

func (fs *fileStore) recoverMsgs() error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Check for any left over purged messages.
	pdir := path.Join(fs.fcfg.StoreDir, purgeDir)
	if _, err := os.Stat(pdir); err == nil {
		os.RemoveAll(pdir)
	}

	mdir := path.Join(fs.fcfg.StoreDir, msgDir)
	fis, err := ioutil.ReadDir(mdir)
	if err != nil {
		return fmt.Errorf("storage directory not readable")
	}

	// Recover all of the msg blocks.
	// These can come in a random order, so account for that.
	for _, fi := range fis {
		var index uint64
		if n, err := fmt.Sscanf(fi.Name(), blkScan, &index); err == nil && n == 1 {
			if mb := fs.recoverMsgBlock(fi, index); mb != nil {
				if fs.state.FirstSeq == 0 || mb.first.seq < fs.state.FirstSeq {
					fs.state.FirstSeq = mb.first.seq
					fs.state.FirstTime = time.Unix(0, mb.first.ts).UTC()
				}
				if mb.last.seq > fs.state.LastSeq {
					fs.state.LastSeq = mb.last.seq
					fs.state.LastTime = time.Unix(0, mb.last.ts).UTC()
				}
				fs.state.Msgs += mb.msgs
				fs.state.Bytes += mb.bytes
			}
		}
	}

	// Now make sure to sort blks for efficient lookup later with selectMsgBlock().
	if len(fs.blks) > 0 {
		sort.Slice(fs.blks, func(i, j int) bool { return fs.blks[i].index < fs.blks[j].index })
		fs.lmb = fs.blks[len(fs.blks)-1]
		err = fs.enableLastMsgBlockForWriting()
	} else {
		_, err = fs.newMsgBlockForWrite()
	}

	if err != nil {
		return err
	}

	// Limits checks and enforcement.
	fs.enforceMsgLimit()
	fs.enforceBytesLimit()

	// Do age checks too, make sure to call in place.
	if fs.cfg.MaxAge != 0 && fs.state.Msgs > 0 {
		fs.startAgeChk()
		fs.expireMsgsLocked()
	}
	return nil
}

// GetSeqFromTime looks for the first sequence number that has
// the message with >= timestamp.
// FIXME(dlc) - inefficient, and dumb really. Make this better.
func (fs *fileStore) GetSeqFromTime(t time.Time) uint64 {
	fs.mu.RLock()
	lastSeq := fs.state.LastSeq
	closed := fs.closed
	fs.mu.RUnlock()

	if closed {
		return 0
	}

	mb := fs.selectMsgBlockForStart(t)
	if mb == nil {
		return lastSeq + 1
	}

	mb.mu.RLock()
	fseq := mb.first.seq
	lseq := mb.last.seq
	mb.mu.RUnlock()

	// Linear search, hence the dumb part..
	ts := t.UnixNano()
	for seq := fseq; seq <= lseq; seq++ {
		sm, _ := mb.fetchMsg(seq)
		if sm != nil && sm.ts >= ts {
			return sm.seq
		}
	}
	return 0
}

// StorageBytesUpdate registers an async callback for updates to storage changes.
func (fs *fileStore) StorageBytesUpdate(cb func(int64)) {
	fs.mu.Lock()
	fs.scb = cb
	bsz := fs.state.Bytes
	fs.mu.Unlock()
	if cb != nil && bsz > 0 {
		cb(int64(bsz))
	}
}

// Helper to get hash key for specific message block.
// Lock should be held
func (fs *fileStore) hashKeyForBlock(index uint64) []byte {
	return []byte(fmt.Sprintf("%s-%d", fs.cfg.Name, index))
}

// This rolls to a new append msg block.
// Lock should be held.
func (fs *fileStore) newMsgBlockForWrite() (*msgBlock, error) {
	index := uint64(1)

	if fs.lmb != nil {
		index = fs.lmb.index + 1
		fs.flushPendingWrites()
		fs.closeLastMsgBlock(false)
	}

	mb := &msgBlock{index: index, expire: fs.fcfg.ReadCacheExpire}
	fs.blks = append(fs.blks, mb)
	fs.lmb = mb

	mdir := path.Join(fs.fcfg.StoreDir, msgDir)
	mb.mfn = path.Join(mdir, fmt.Sprintf(blkScan, mb.index))
	mfd, err := os.OpenFile(mb.mfn, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("Error creating msg block file [%q]: %v", mb.mfn, err)
	}
	mb.mfd = mfd

	mb.ifn = path.Join(mdir, fmt.Sprintf(indexScan, mb.index))
	ifd, err := os.OpenFile(mb.ifn, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("Error creating msg index file [%q]: %v", mb.mfn, err)
	}
	mb.ifd = ifd

	// Now do local hash.
	key := sha256.Sum256(fs.hashKeyForBlock(index))
	mb.hh, err = highwayhash.New64(key[:])
	if err != nil {
		return nil, fmt.Errorf("could not create hash: %v", err)
	}

	return mb, nil
}

// Make sure we can write to the last message block.
// Lock should be held.
func (fs *fileStore) enableLastMsgBlockForWriting() error {
	mb := fs.lmb
	if mb == nil {
		return fmt.Errorf("no last message block assigned, can not enable for writing")
	}
	if mb.mfd != nil {
		return nil
	}
	mfd, err := os.OpenFile(mb.mfn, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("error opening msg block file [%q]: %v", mb.mfn, err)
	}
	mb.mfd = mfd
	return nil
}

// Store stores a message.
func (fs *fileStore) StoreMsg(subj string, msg []byte) (uint64, int64, error) {
	fs.mu.Lock()
	if fs.closed {
		fs.mu.Unlock()
		return 0, 0, ErrStoreClosed
	}

	// Check if we are discarding new messages when we reach the limit.
	if fs.cfg.Discard == DiscardNew {
		if fs.cfg.MaxMsgs > 0 && fs.state.Msgs >= uint64(fs.cfg.MaxMsgs) {
			fs.mu.Unlock()
			return 0, 0, ErrMaxMsgs
		}
		if fs.cfg.MaxBytes > 0 && fs.state.Bytes+uint64(len(msg)) >= uint64(fs.cfg.MaxBytes) {
			fs.mu.Unlock()
			return 0, 0, ErrMaxBytes
		}
	}

	seq := fs.state.LastSeq + 1

	n, ts, err := fs.writeMsgRecord(seq, subj, msg)
	if err != nil {
		fs.mu.Unlock()
		return 0, 0, err
	}
	fs.kickFlusher()

	if fs.state.FirstSeq == 0 {
		fs.state.FirstSeq = seq
		fs.state.FirstTime = time.Unix(0, ts).UTC()
	}

	fs.state.Msgs++
	fs.state.Bytes += n
	fs.state.LastSeq = seq
	fs.state.LastTime = time.Unix(0, ts).UTC()

	// Limits checks and enforcement.
	// If they do any deletions they will update the
	// byte count on their own, so no need to compensate.
	fs.enforceMsgLimit()
	fs.enforceBytesLimit()

	// Check if we have and need the age expiration timer running.
	if fs.ageChk == nil && fs.cfg.MaxAge != 0 {
		fs.startAgeChk()
	}

	cb := fs.scb
	fs.mu.Unlock()

	if cb != nil {
		cb(int64(n))
	}

	return seq, ts, nil
}

// Will check the msg limit and drop firstSeq msg if needed.
// Lock should be held.
func (fs *fileStore) enforceMsgLimit() {
	if fs.cfg.MaxMsgs <= 0 || fs.state.Msgs <= uint64(fs.cfg.MaxMsgs) {
		return
	}
	for nmsgs := fs.state.Msgs; nmsgs > uint64(fs.cfg.MaxMsgs); nmsgs = fs.state.Msgs {
		fs.deleteFirstMsgLocked()
	}
}

// Will check the bytes limit and drop msgs if needed.
// Lock should be held.
func (fs *fileStore) enforceBytesLimit() {
	if fs.cfg.MaxBytes <= 0 || fs.state.Bytes <= uint64(fs.cfg.MaxBytes) {
		return
	}
	for bs := fs.state.Bytes; bs > uint64(fs.cfg.MaxBytes); bs = fs.state.Bytes {
		fs.deleteFirstMsgLocked()
	}
}

// Lock should be held but will be released during actual remove.
func (fs *fileStore) deleteFirstMsgLocked() (bool, error) {
	fs.mu.Unlock()
	defer fs.mu.Lock()
	return fs.removeMsg(fs.state.FirstSeq, false)
}

// Lock should NOT be held.
func (fs *fileStore) deleteFirstMsg() (bool, error) {
	fs.mu.RLock()
	seq := fs.state.FirstSeq
	fs.mu.RUnlock()
	return fs.removeMsg(seq, false)
}

// RemoveMsg will remove the message from this store.
// Will return the number of bytes removed.
func (fs *fileStore) RemoveMsg(seq uint64) (bool, error) {
	return fs.removeMsg(seq, false)
}

func (fs *fileStore) EraseMsg(seq uint64) (bool, error) {
	return fs.removeMsg(seq, true)
}

func (fs *fileStore) isClosed() bool {
	fs.mu.RLock()
	closed := fs.closed
	fs.mu.RUnlock()
	return closed
}

func (fs *fileStore) isSnapshotting() bool {
	fs.mu.RLock()
	iss := fs.sips > 0
	fs.mu.RUnlock()
	return iss
}

// Remove a message, optionally rewriting the mb file.
func (fs *fileStore) removeMsg(seq uint64, secure bool) (bool, error) {
	if fs.isClosed() {
		return false, ErrStoreClosed
	}
	if fs.isSnapshotting() {
		return false, ErrStoreSnapshotInProgress
	}
	mb := fs.selectMsgBlock(seq)
	if mb == nil {
		return false, nil
	}
	sm, _ := mb.fetchMsg(seq)
	// We have the message here, so we can delete it.
	if sm != nil {
		fs.deleteMsgFromBlock(mb, seq, sm, secure)
	}
	return sm != nil, nil
}

// Loop on requests to write out our index file. This is used when calling
// remove for a message. Updates to the last.seq etc are handled by main
// flush loop when storing messages.
func (fs *fileStore) flushWriteIndexLoop(mb *msgBlock, dch, qch chan struct{}) {
	for {
		select {
		case <-dch:
			mb.writeIndexInfo()
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

// Lock should be held.
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
	// Set new first sequence.
	mb.first.seq = seq

	// Need to get the timestamp.
	// We will try the cache direct and fallback if needed.
	sm, _ := mb.cacheLookupLocked(seq)
	if sm == nil {
		// Slow path, need to unlock.
		mb.mu.Unlock()
		sm, _ = mb.fetchMsg(seq)
		mb.mu.Lock()
	}
	if sm != nil {
		mb.first.ts = sm.ts
	} else {
		mb.first.ts = 0
	}
}

func (fs *fileStore) deleteMsgFromBlock(mb *msgBlock, seq uint64, sm *fileStoredMsg, secure bool) {
	// Update global accounting.
	msz := fileStoreMsgSize(sm.subj, sm.msg)

	fs.mu.Lock()
	mb.mu.Lock()

	// Make sure the seq still exists.
	if seq < mb.cache.fseq || (seq-mb.cache.fseq) >= uint64(len(mb.cache.idx)) {
		mb.mu.Unlock()
		fs.mu.Unlock()
		return
	}
	// Now check dmap if it is there.
	if mb.dmap != nil {
		if _, ok := mb.dmap[seq]; ok {
			mb.mu.Unlock()
			fs.mu.Unlock()
			return
		}
	}

	// Global stats
	fs.state.Msgs--
	fs.state.Bytes -= msz

	// Now local mb updates.
	mb.msgs--
	mb.bytes -= msz
	atomic.AddUint64(&mb.cgenid, 1)

	var shouldWriteIndex bool

	// Optimize for FIFO case.
	if seq == mb.first.seq {
		mb.selectNextFirst()
		if seq == fs.state.FirstSeq {
			fs.state.FirstSeq = mb.first.seq // new one.
			fs.state.FirstTime = time.Unix(0, mb.first.ts).UTC()
		}
		if mb.first.seq > mb.last.seq {
			fs.removeMsgBlock(mb)
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
		fs.eraseMsg(mb, sm)
	}

	if shouldWriteIndex {
		if mb.dch == nil {
			// Spin up the write flusher.
			mb.qch = make(chan struct{})
			mb.dch = make(chan struct{})
			go fs.flushWriteIndexLoop(mb, mb.dch, mb.qch)
			// Do a blocking kick here.
			mb.dch <- struct{}{}
		} else {
			mb.kickWriteFlusher()
		}
	}
	mb.mu.Unlock()
	fs.mu.Unlock()

	if fs.scb != nil {
		delta := int64(msz)
		fs.scb(-delta)
	}
}

// Lock should be held.
func (mb *msgBlock) startCacheExpireTimer() {
	genid := mb.cgenid
	if mb.ctmr == nil {
		mb.ctmr = time.AfterFunc(mb.expire, func() { mb.expireCache(genid) })
	} else {
		mb.ctmr.Reset(mb.expire)
	}
}

// Called to possibly expire a message block read cache.
func (mb *msgBlock) expireCache(genid uint64) {
	mb.mu.Lock()
	defer mb.mu.Unlock()

	cgenid := atomic.LoadUint64(&mb.cgenid)

	if genid == cgenid {
		mb.cache = nil
		mb.ctmr = nil
	} else {
		mb.ctmr = time.AfterFunc(mb.expire, func() { mb.expireCache(cgenid) })
	}
}

func (fs *fileStore) startAgeChk() {
	if fs.ageChk == nil && fs.cfg.MaxAge != 0 {
		fs.ageChk = time.AfterFunc(fs.cfg.MaxAge, fs.expireMsgs)
	}
}

// Lock should be held.
func (fs *fileStore) expireMsgsLocked() {
	fs.mu.Unlock()
	fs.expireMsgs()
	fs.mu.Lock()
}

// Will expire msgs that are too old.
func (fs *fileStore) expireMsgs() {
	now := time.Now().UnixNano()
	minAge := now - int64(fs.cfg.MaxAge)

	for {
		sm, _ := fs.msgForSeq(0)
		if sm != nil && sm.ts <= minAge {
			fs.deleteFirstMsg()
		} else {
			fs.mu.Lock()
			if sm == nil {
				if fs.ageChk != nil {
					fs.ageChk.Stop()
					fs.ageChk = nil
				}
			} else {
				fireIn := time.Duration(sm.ts-now) + fs.cfg.MaxAge
				if fs.ageChk != nil {
					fs.ageChk.Reset(fireIn)
				} else {
					fs.ageChk = time.AfterFunc(fireIn, fs.expireMsgs)
				}
			}
			fs.mu.Unlock()
			return
		}
	}
}

// Check all the checksums for a message block.
func checkMsgBlockFile(fp *os.File, hh hash.Hash) []uint64 {
	var le = binary.LittleEndian
	var hdr [msgHdrSize]byte
	var bad []uint64

	r := bufio.NewReaderSize(fp, 64*1024*1024)

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
func (fs *fileStore) checkMsgs() []uint64 {
	fs.flushPendingWritesUnlocked()

	mdir := path.Join(fs.fcfg.StoreDir, msgDir)
	fis, err := ioutil.ReadDir(mdir)
	if err != nil {
		return nil
	}

	var bad []uint64

	// Check all of the msg blocks.
	for _, fi := range fis {
		var index uint64
		if n, err := fmt.Sscanf(fi.Name(), blkScan, &index); err == nil && n == 1 {
			if fp, err := os.Open(path.Join(mdir, fi.Name())); err != nil {
				continue
			} else {
				key := sha256.Sum256(fs.hashKeyForBlock(index))
				hh, _ := highwayhash.New64(key[:])
				bad = append(bad, checkMsgBlockFile(fp, hh)...)
				fp.Close()
			}
		}
	}
	return bad
}

// This will kick out our flush routine if its waiting.
func (fs *fileStore) kickFlusher() {
	select {
	case fs.fch <- struct{}{}:
	default:
	}
}

func (fs *fileStore) pendingWriteSize() int {
	var sz int
	fs.mu.RLock()
	if fs.wmb != nil {
		sz = fs.wmb.Len()
	}
	fs.mu.RUnlock()
	return sz
}

func (fs *fileStore) flushLoop(fch, qch chan struct{}) {
	for {
		select {
		case <-fch:
			waiting := fs.pendingWriteSize()
			if waiting == 0 {
				continue
			}
			ts := 1 * time.Millisecond
			for waiting < coalesceMinimum {
				time.Sleep(ts)
				newWaiting := fs.pendingWriteSize()
				if newWaiting <= waiting {
					break
				}
				waiting = newWaiting
				ts *= 2
			}
			fs.flushPendingWritesUnlocked()
		case <-qch:
			return
		}
	}
}

// Return the number of bytes in this message block.
func (mb *msgBlock) numBytes() uint64 {
	mb.mu.RLock()
	nb := mb.bytes
	mb.mu.RUnlock()
	return nb
}

// Update accounting on a write msg.
func (mb *msgBlock) updateAccounting(seq uint64, ts int64, rl uint64) {
	mb.mu.Lock()
	if mb.first.seq == 0 {
		mb.first.seq = seq
		mb.first.ts = ts
	}
	// Need atomics here for selectMsgBlock speed.
	atomic.StoreUint64(&mb.last.seq, seq)
	mb.last.ts = ts
	mb.bytes += rl
	mb.msgs++
	mb.mu.Unlock()
}

// Lock should be held.
func (fs *fileStore) writeMsgRecord(seq uint64, subj string, msg []byte) (uint64, int64, error) {
	var err error

	// Get size for this message.
	rl := fileStoreMsgSize(subj, msg)

	// Grab our current last message block.
	mb := fs.lmb
	if mb == nil || mb.numBytes()+rl > fs.fcfg.BlockSize {
		if mb, err = fs.newMsgBlockForWrite(); err != nil {
			return 0, 0, err
		}
	}

	// Make sure we have room.
	fs.wmb.Grow(int(rl))

	// Grab time
	ts := time.Now().UnixNano()

	// Update accounting.
	mb.updateAccounting(seq, ts, rl)

	// First write header, etc.
	var le = binary.LittleEndian
	var hdr [msgHdrSize]byte

	le.PutUint32(hdr[0:], uint32(rl))
	le.PutUint64(hdr[4:], seq)
	le.PutUint64(hdr[12:], uint64(ts))
	le.PutUint16(hdr[20:], uint16(len(subj)))

	// Now write to underlying buffer.
	fs.wmb.Write(hdr[:])
	fs.wmb.WriteString(subj)
	fs.wmb.Write(msg)

	// Calculate hash.
	mb.mu.Lock()
	mb.hh.Reset()
	mb.hh.Write(hdr[4:20])
	mb.hh.Write([]byte(subj))
	mb.hh.Write(msg)
	checksum := mb.hh.Sum(nil)
	// Grab last checksum
	copy(mb.lchk[0:], checksum)
	mb.mu.Unlock()

	// Write to msg record.
	fs.wmb.Write(checksum)

	return rl, ts, nil
}

// Will rewrite the message in the underlying store.
// Both fs and mb locks should be held.
func (fs *fileStore) eraseMsg(mb *msgBlock, sm *fileStoredMsg) error {
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
	mb.hh.Reset()
	mb.hh.Write(hdr[4:20])
	mb.hh.Write([]byte(sm.subj))
	mb.hh.Write(sm.msg)
	checksum := mb.hh.Sum(nil)
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
func (fs *fileStore) syncBlocks() {
	fs.mu.RLock()
	closed := fs.closed
	blks := fs.blks
	fs.mu.RUnlock()

	if closed {
		return
	}
	for _, mb := range blks {
		mb.mu.RLock()
		if mb.mfd != nil {
			mb.mfd.Sync()
		}
		if mb.ifd != nil {
			mb.ifd.Sync()
			mb.ifd.Truncate(mb.liwsz)
		}
		mb.mu.RUnlock()
	}

	var _cfs [256]*consumerFileStore

	fs.mu.Lock()
	cfs := append(_cfs[:0], fs.cfs...)
	fs.syncTmr = time.AfterFunc(fs.fcfg.SyncInterval, fs.syncBlocks)
	fs.mu.Unlock()

	// Do consumers.
	for _, o := range cfs {
		o.syncStateFile()
	}
}

// Select the message block where this message should be found.
// Return nil if not in the set.
func (fs *fileStore) selectMsgBlock(seq uint64) *msgBlock {
	fs.mu.RLock()
	// Check for out of range.
	if seq < fs.state.FirstSeq || seq > fs.state.LastSeq {
		fs.mu.RUnlock()
		return nil
	}

	var smb *msgBlock
	var needsFlush bool

	// blks are sorted in ascending order.
	// TODO(dlc) - Can be smarter here, when lots of blks maybe use binary search.
	// For now this is cache friendly for small to medium num blks.
	for _, mb := range fs.blks {
		if seq <= atomic.LoadUint64(&mb.last.seq) {
			// This detects if what we may be looking for is staged in the write buffer.
			if mb == fs.lmb {
				needsFlush = true
			}
			smb = mb
			break
		}
	}
	fs.mu.RUnlock()

	if needsFlush {
		fs.flushPendingWritesUnlocked()
	}

	return smb
}

// Select the message block where this message should be found.
// Return nil if not in the set.
func (fs *fileStore) selectMsgBlockForStart(minTime time.Time) *msgBlock {
	fs.mu.RLock()
	blks := fs.blks
	lmb := fs.lmb
	fs.mu.RUnlock()

	t := minTime.UnixNano()
	for _, mb := range blks {
		mb.mu.RLock()
		found := t <= mb.last.ts
		mb.mu.RUnlock()

		if found {
			// This detects if what we may be looking for is staged in the write buffer.
			if mb == lmb {
				fs.flushPendingWritesUnlocked()
			}
			return mb
		}
	}
	return nil
}

// This will update the cache for a block that is actively being written too.
func (mb *msgBlock) checkUpdateCache(nbuf []byte) {
	mb.mu.Lock()
	defer mb.mu.Unlock()

	// Just return if nothing to do.
	if mb.cache == nil || len(nbuf) == 0 {
		return
	}

	if err := mb.indexCacheBuf(nbuf); err != nil {
		panic(fmt.Sprintf("Error indexing: %v", err))
	}
}

// Index a raw msg buffer.
// Lock should be held.
func (mb *msgBlock) indexCacheBuf(buf []byte) error {
	var le = binary.LittleEndian

	var fseq uint64
	var idx []uint32
	var index uint32

	if mb.cache == nil {
		// Approximation, may adjust below.
		fseq = mb.first.seq
		idx = make([]uint32, 0, mb.msgs)
	} else {
		fseq = mb.cache.fseq
		idx = mb.cache.idx
		index = uint32(len(mb.cache.buf))
		buf = append(mb.cache.buf, buf...)
	}

	lbuf := uint32(len(buf))

	for index < lbuf {
		hdr := buf[index : index+msgHdrSize]
		rl := le.Uint32(hdr[0:])
		seq := le.Uint64(hdr[4:])
		slen := le.Uint16(hdr[20:])
		dlen := int(rl) - msgHdrSize

		// Do some quick sanity checks here.
		if dlen < 0 || int(slen) > dlen || dlen > int(rl) {
			// This means something is off.
			// Add into bad list?
			return fmt.Errorf("malformed or corrupt msg")
		}
		// Adjust if we guessed wrong.
		if seq != 0 && seq < fseq {
			fseq = seq
		}
		// We defer checksum checks to individual msg cache lookups to amortorize costs and
		// not introduce latency for first message from a newly loaded block.
		idx = append(idx, index)
		index += uint32(rl)
	}
	if mb.cache == nil {
		mb.cache = &cache{}
	}
	mb.cache.buf = buf
	mb.cache.idx = idx
	mb.cache.fseq = fseq
	return nil
}

// Will load msgs from disk.
func (mb *msgBlock) loadMsgs() error {
	mb.mu.RLock()
	mfn := mb.mfn
	mb.mu.RUnlock()

	// Load in the whole block.
	buf, err := ioutil.ReadFile(mfn)
	if err != nil {
		return err
	}

	mb.mu.Lock()
	// Someone else may have filled this in by the time we get here.
	if mb.cache != nil {
		mb.mu.Unlock()
		return nil
	}

	if err := mb.indexCacheBuf(buf); err != nil {
		mb.mu.Unlock()
		return err
	}

	if len(buf) > 0 {
		mb.cloads++
		mb.startCacheExpireTimer()
	}
	mb.mu.Unlock()

	return nil
}

// Fetch a message from this block, possibly reading in and caching the messages.
// We assume the block was selected and is correct, so we do not do range checks.
func (mb *msgBlock) fetchMsg(seq uint64) (*fileStoredMsg, error) {
	var sm *fileStoredMsg

	sm, err := mb.cacheLookup(seq)
	if err == nil || err != errNoCache {
		return sm, err
	}

	// We have a cache miss here.
	if err := mb.loadMsgs(); err != nil {
		return nil, err
	}

	return mb.cacheLookup(seq)
}

var (
	errNoCache    = errors.New("no message cache")
	errBadMsg     = errors.New("malformed or corrupt msg")
	errDeletedMsg = errors.New("deleted msg")
)

// Used for marking messages that have had their checksums checked.
const hbit = 1 << 31

// Will do a lookup from the cache.
func (mb *msgBlock) cacheLookup(seq uint64) (*fileStoredMsg, error) {
	// Currently grab the write lock for optional use of mb.hh. Prefer this for now
	// vs read lock and promote. Also defer based on 1.14 performance.
	mb.mu.Lock()
	defer mb.mu.Unlock()
	return mb.cacheLookupLocked(seq)
}

// Will do a lookup from cache.
// lock should be held.
func (mb *msgBlock) cacheLookupLocked(seq uint64) (*fileStoredMsg, error) {
	if mb.cache == nil {
		return nil, errNoCache
	}

	if seq < mb.cache.fseq || (seq-mb.cache.fseq) >= uint64(len(mb.cache.idx)) {
		return nil, ErrStoreMsgNotFound
	}

	// If we have a delete map check it.
	if mb.dmap != nil {
		if _, ok := mb.dmap[seq]; ok {
			return nil, errDeletedMsg
		}
	}

	bi := mb.cache.idx[seq-mb.cache.fseq]

	// We use the high bit to denote we have already checked the checksum.
	var hh hash.Hash64
	if bi&hbit == 0 {
		hh = mb.hh
		mb.cache.idx[seq-mb.cache.fseq] = (bi | hbit)
	} else {
		bi &^= hbit
	}

	buf := mb.cache.buf[bi:]
	// Parse from the raw buffer.
	subj, msg, mseq, ts, err := msgFromBuf(buf, hh)
	if err != nil {
		return nil, err
	}
	if seq != mseq {
		return nil, fmt.Errorf("sequence numbers for cache load did not match, %d vs %d", seq, mseq)
	}
	sm := &fileStoredMsg{
		subj: subj,
		msg:  msg,
		seq:  seq,
		ts:   ts,
		off:  int64(bi),
	}
	atomic.AddUint64(&mb.cgenid, 1)
	return sm, nil
}

// Will return message for the given sequence number.
func (fs *fileStore) msgForSeq(seq uint64) (*fileStoredMsg, error) {
	fs.mu.RLock()
	if fs.closed {
		fs.mu.RUnlock()
		return nil, ErrStoreClosed
	}
	fseq := fs.state.FirstSeq
	fs.mu.RUnlock()

	// Indicates we want first msg.
	if seq == 0 {
		seq = fseq
	}

	mb := fs.selectMsgBlock(seq)
	if mb == nil {
		var err = ErrStoreEOF
		fs.mu.RLock()
		if seq <= fs.state.LastSeq {
			err = ErrStoreMsgNotFound
		}
		fs.mu.RUnlock()
		return nil, err
	}
	// TODO(dlc) - older design had a check to prefetch when we knew we were
	// loading in order and getting close to end of current mb. Should add
	// something like it back in.
	return mb.fetchMsg(seq)
}

// Internal function to return msg parts from a raw buffer.
func msgFromBuf(buf []byte, hh hash.Hash64) (string, []byte, uint64, int64, error) {
	if len(buf) < msgHdrSize {
		return "", nil, 0, 0, errBadMsg
	}
	var le = binary.LittleEndian

	hdr := buf[:msgHdrSize]
	dlen := int(le.Uint32(hdr[0:])) - msgHdrSize
	slen := int(le.Uint16(hdr[20:]))
	// Simple sanity check.
	if dlen < 0 || slen > dlen {
		return "", nil, 0, 0, errBadMsg
	}
	data := buf[msgHdrSize : msgHdrSize+dlen]
	// Do checksum tests here if requested.
	if hh != nil {
		hh.Reset()
		hh.Write(hdr[4:20])
		hh.Write(data[:slen])
		hh.Write(data[slen : dlen-8])
		if !bytes.Equal(hh.Sum(nil), data[len(data)-8:]) {
			return "", nil, 0, 0, errBadMsg
		}
	}
	seq := le.Uint64(hdr[4:])
	ts := int64(le.Uint64(hdr[12:]))
	// FIXME(dlc) - We need to not allow appends to the underlying buffer, so we will
	// fix the capacity. This will cause a copy though in stream:internalSendLoop when
	// we append CRLF but this was causing a race. Need to rethink more to avoid this copy.
	end := dlen - 8
	return string(data[:slen]), data[slen:end:end], seq, ts, nil
}

// LoadMsg will lookup the message by sequence number and return it if found.
func (fs *fileStore) LoadMsg(seq uint64) (string, []byte, int64, error) {
	sm, err := fs.msgForSeq(seq)
	if sm != nil {
		return sm.subj, sm.msg, sm.ts, nil
	}
	return "", nil, 0, err
}

func (fs *fileStore) State() StreamState {
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	state := fs.state
	state.Consumers = len(fs.cfs)
	return state
}

func fileStoreMsgSize(subj string, msg []byte) uint64 {
	// length of the message record (4bytes) + seq(8) + ts(8) + subj_len(2) + subj + msg + hash(8)
	return uint64(4 + 16 + 2 + len(subj) + len(msg) + 8)
}

// Lock should not be held.
func (fs *fileStore) flushPendingWritesUnlocked() error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return fs.flushPendingWrites()
}

// Lock should be held.
func (fs *fileStore) flushPendingWrites() error {
	mb := fs.lmb
	if mb == nil || mb.mfd == nil {
		return fmt.Errorf("filestore does not have last message block")
	}
	// Append new data to the message block file.
	for lbb := fs.wmb.Len(); lbb > 0; lbb = fs.wmb.Len() {
		n, err := fs.wmb.WriteTo(mb.mfd)
		if err != nil {
			mb.mu.Lock()
			mb.removeIndex()
			mb.mu.Unlock()
			return err
		}

		// Update the cache if needed.
		mb.checkUpdateCache(fs.wmb.Bytes()[:n])

		if int(n) != lbb {
			fs.wmb.Truncate(int(n))
		} else if lbb <= maxBufReuse {
			fs.wmb.Reset()
		} else {
			fs.wmb = &bytes.Buffer{}
		}
	}
	// Now index info
	return mb.writeIndexInfo()
}

// Write index info to the appropriate file.
func (mb *msgBlock) writeIndexInfo() error {
	// HEADER: magic version msgs bytes fseq fts lseq lts checksum
	var hdr [indexHdrSize]byte

	// Write header
	hdr[0] = magic
	hdr[1] = version

	mb.mu.Lock()
	n := hdrLen
	n += binary.PutUvarint(hdr[n:], mb.msgs)
	n += binary.PutUvarint(hdr[n:], mb.bytes)
	n += binary.PutUvarint(hdr[n:], mb.first.seq)
	n += binary.PutVarint(hdr[n:], mb.first.ts)
	n += binary.PutUvarint(hdr[n:], mb.last.seq)
	n += binary.PutVarint(hdr[n:], mb.last.ts)
	n += binary.PutUvarint(hdr[n:], uint64(len(mb.dmap)))
	buf := append(hdr[:n], mb.lchk[:]...)

	// Append a delete map if needed
	if len(mb.dmap) > 0 {
		buf = append(buf, mb.genDeleteMap()...)
	}
	var err error
	if mb.ifd == nil {
		ifd, err := os.OpenFile(mb.ifn, os.O_CREATE|os.O_RDWR, 0644)
		if err != nil {
			mb.mu.Unlock()
			return err
		}
		mb.ifd = ifd
	}
	// TODO(dlc) - don't hold lock here.
	n, err = mb.ifd.WriteAt(buf, 0)
	if err == nil {
		mb.liwsz = int64(n)
	}
	mb.mu.Unlock()

	return err
}

func (mb *msgBlock) readIndexInfo() error {
	buf, err := ioutil.ReadFile(mb.ifn)
	if err != nil {
		return err
	}

	if err := checkHeader(buf); err != nil {
		defer os.Remove(mb.ifn)
		return fmt.Errorf("bad index file")
	}

	bi := hdrLen

	// Helpers, will set i to -1 on error.
	readSeq := func() uint64 {
		if bi < 0 {
			return 0
		}
		seq, n := binary.Uvarint(buf[bi:])
		if n <= 0 {
			bi = -1
			return 0
		}
		bi += n
		return seq
	}
	readCount := readSeq
	readTimeStamp := func() int64 {
		if bi < 0 {
			return 0
		}
		ts, n := binary.Varint(buf[bi:])
		if n <= 0 {
			bi = -1
			return -1
		}
		bi += n
		return ts
	}
	mb.msgs = readCount()
	mb.bytes = readCount()
	mb.first.seq = readSeq()
	mb.first.ts = readTimeStamp()
	mb.last.seq = readSeq()
	mb.last.ts = readTimeStamp()
	dmapLen := readCount()

	// Checksum
	copy(mb.lchk[0:], buf[bi:bi+checksumSize])
	bi += checksumSize

	// Now check for presence of a delete map
	if dmapLen > 0 {
		mb.dmap = make(map[uint64]struct{}, dmapLen)
		for i := 0; i < int(dmapLen); i++ {
			seq := readSeq()
			if seq == 0 {
				break
			}
			mb.dmap[seq+mb.first.seq] = struct{}{}
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
func (fs *fileStore) cacheLoads() uint64 {
	var tl uint64
	fs.mu.RLock()
	for _, mb := range fs.blks {
		tl += mb.cloads
	}
	fs.mu.RUnlock()
	return tl
}

// Will return total number of cached bytes.
func (fs *fileStore) cacheSize() uint64 {
	var sz uint64
	fs.mu.RLock()
	for _, mb := range fs.blks {
		mb.mu.RLock()
		if mb.cache != nil {
			sz += uint64(len(mb.cache.buf))
		}
		mb.mu.RUnlock()
	}
	fs.mu.RUnlock()
	return sz
}

// Will return total number of dmapEntries for all msg blocks.
func (fs *fileStore) dmapEntries() int {
	var total int
	fs.mu.RLock()
	for _, mb := range fs.blks {
		total += len(mb.dmap)
	}
	fs.mu.RUnlock()
	return total
}

// Purge will remove all messages from this store.
// Will return the number of purged messages.
func (fs *fileStore) Purge() uint64 {
	fs.mu.Lock()
	if fs.closed {
		fs.mu.Unlock()
		return 0
	}

	purged := fs.state.Msgs
	rbytes := int64(fs.state.Bytes)

	fs.state.FirstSeq = fs.state.LastSeq + 1
	fs.state.FirstTime = time.Time{}

	fs.state.Bytes = 0
	fs.state.Msgs = 0

	for _, mb := range fs.blks {
		mb.dirtyClose()
	}

	fs.blks = nil
	fs.wmb = &bytes.Buffer{}
	fs.lmb = nil

	// Move the msgs directory out of the way, will delete out of band.
	// FIXME(dlc) - These can error and we need to change api above to propagate?
	mdir := path.Join(fs.fcfg.StoreDir, msgDir)
	pdir := path.Join(fs.fcfg.StoreDir, purgeDir)
	// If purge directory still exists then we need to wait
	// in place and remove since rename would fail.
	if _, err := os.Stat(pdir); err == nil {
		os.RemoveAll(pdir)
	}
	os.Rename(mdir, pdir)
	go os.RemoveAll(pdir)
	// Create new one.
	os.MkdirAll(mdir, 0755)

	// Make sure we have a lmb to write to.
	fs.newMsgBlockForWrite()

	fs.lmb.first.seq = fs.state.FirstSeq
	fs.lmb.last.seq = fs.state.LastSeq
	fs.lmb.writeIndexInfo()

	cb := fs.scb
	fs.mu.Unlock()

	if cb != nil {
		cb(-rbytes)
	}

	return purged
}

// Returns number of msg blks.
func (fs *fileStore) numMsgBlocks() int {
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	return len(fs.blks)
}

// Lock should be held.
func (mb *msgBlock) removeIndex() {
	if mb.ifd != nil {
		mb.ifd.Close()
		mb.ifd = nil
	}
	os.Remove(mb.ifn)
}

// Removes the msgBlock
// Both locks should be held.
func (fs *fileStore) removeMsgBlock(mb *msgBlock) {
	mb.removeIndex()
	if mb.mfd != nil {
		mb.mfd.Close()
		mb.mfd = nil
	}
	os.Remove(mb.mfn)

	for i, omb := range fs.blks {
		if mb == omb {
			fs.blks = append(fs.blks[:i], fs.blks[i+1:]...)
			break
		}
	}
	// Check for us being last message block
	if mb == fs.lmb {
		fs.lmb = nil
		fs.newMsgBlockForWrite()
		fs.lmb.first = mb.first
		fs.lmb.last = mb.last
		fs.lmb.writeIndexInfo()
	}
	go mb.close(true)
}

// Called by purge to simply get rid of the cache and close and fds.
// FIXME(dlc) - Merge with below func.
func (mb *msgBlock) dirtyClose() {
	if mb == nil {
		return
	}
	mb.mu.Lock()
	// Close cache
	mb.cache = nil
	// Quit our loops.
	if mb.qch != nil {
		close(mb.qch)
		mb.qch = nil
	}
	if mb.mfd != nil {
		mb.mfd.Close()
		mb.mfd = nil
	}
	if mb.ifd != nil {
		mb.ifd.Close()
		mb.ifd = nil
	}
	mb.mu.Unlock()
}

func (mb *msgBlock) close(sync bool) {
	if mb == nil {
		return
	}
	mb.mu.Lock()
	// Close cache
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
	mb.mu.Unlock()
}

func (fs *fileStore) closeAllMsgBlocks(sync bool) {
	for _, mb := range fs.blks {
		mb.close(sync)
	}
}

func (fs *fileStore) closeLastMsgBlock(sync bool) {
	fs.lmb.close(sync)
}

func (fs *fileStore) Delete() error {
	if fs.isClosed() {
		return ErrStoreClosed
	}
	// TODO(dlc) - check error here?
	fs.Purge()
	if err := fs.Stop(); err != nil {
		return err
	}
	return os.RemoveAll(fs.fcfg.StoreDir)
}

func (fs *fileStore) Stop() error {
	fs.mu.Lock()
	if fs.closed {
		fs.mu.Unlock()
		return ErrStoreClosed
	}
	fs.closed = true
	close(fs.qch)

	err := fs.flushPendingWrites()
	fs.wmb = &bytes.Buffer{}
	fs.lmb = nil

	fs.closeAllMsgBlocks(true)

	if fs.syncTmr != nil {
		fs.syncTmr.Stop()
		fs.syncTmr = nil
	}
	if fs.ageChk != nil {
		fs.ageChk.Stop()
		fs.ageChk = nil
	}

	var _cfs [256]*consumerFileStore
	cfs := append(_cfs[:0], fs.cfs...)
	fs.cfs = nil
	fs.mu.Unlock()

	for _, o := range cfs {
		o.Stop()
	}

	return err
}

const errFile = "errors.txt"

// Stream our snapshot through gzip and tar.
func (fs *fileStore) streamSnapshot(w io.WriteCloser, blks []*msgBlock, includeConsumers bool) {
	defer w.Close()

	bw := bufio.NewWriter(w)
	defer bw.Flush()

	gzw, _ := gzip.NewWriterLevel(bw, gzip.BestSpeed)
	defer gzw.Close()

	tw := tar.NewWriter(gzw)
	defer tw.Close()

	defer func() {
		fs.mu.Lock()
		fs.sips--
		fs.mu.Unlock()
	}()

	modTime := time.Now().UTC()

	writeFile := func(name string, buf []byte) error {
		hdr := &tar.Header{
			Name:    name,
			Mode:    0600,
			ModTime: modTime,
			Uname:   "nats",
			Gname:   "nats",
			Size:    int64(len(buf)),
			Format:  tar.FormatPAX,
		}
		if err := tw.WriteHeader(hdr); err != nil {
			return err
		}
		if _, err := tw.Write(buf); err != nil {
			return err
		}
		return nil
	}

	writeErr := func(err string) {
		writeFile(errFile, []byte(err))
	}

	fs.mu.Lock()
	// Write our general meta data.
	if err := fs.writeStreamMeta(); err != nil {
		fs.mu.Unlock()
		writeErr(fmt.Sprintf("Could not write stream meta file: %v", err))
		return
	}
	meta, err := ioutil.ReadFile(path.Join(fs.fcfg.StoreDir, JetStreamMetaFile))
	if err != nil {
		fs.mu.Unlock()
		writeErr(fmt.Sprintf("Could not read stream meta file: %v", err))
		return
	}
	sum, err := ioutil.ReadFile(path.Join(fs.fcfg.StoreDir, JetStreamMetaFileSum))
	if err != nil {
		fs.mu.Unlock()
		writeErr(fmt.Sprintf("Could not read stream checksum file: %v", err))
		return
	}
	fs.mu.Unlock()

	// Meta first.
	if writeFile(JetStreamMetaFile, meta) != nil {
		return
	}
	if writeFile(JetStreamMetaFileSum, sum) != nil {
		return
	}

	// Now do messages themselves.
	fs.mu.Lock()
	lmb := fs.lmb
	fs.mu.Unlock()

	// Can't use join path here, zip only recognizes relative paths with forward slashes.
	msgPre := msgDir + "/"

	for _, mb := range blks {
		if mb == lmb {
			fs.flushPendingWritesUnlocked()
		}
		mb.mu.Lock()
		buf, err := ioutil.ReadFile(mb.ifn)
		if err != nil {
			mb.mu.Unlock()
			writeErr(fmt.Sprintf("Could not read message block [%d] meta file: %v", mb.index, err))
			return
		}
		if writeFile(msgPre+fmt.Sprintf(indexScan, mb.index), buf) != nil {
			mb.mu.Unlock()
			return
		}
		// We could stream but don't want to hold the lock and prevent changes, so just read in and
		// release the lock for now.
		// TODO(dlc) - Maybe reuse buffer?
		buf, err = ioutil.ReadFile(mb.mfn)
		if err != nil {
			mb.mu.Unlock()
			writeErr(fmt.Sprintf("Could not read message block [%d]: %v", mb.index, err))
			return
		}
		mb.mu.Unlock()
		// Do this one unlocked.
		if writeFile(msgPre+fmt.Sprintf(blkScan, mb.index), buf) != nil {
			return
		}
	}

	// Bail if no consumers requested.
	if !includeConsumers {
		return
	}

	// Do consumers' state last.
	fs.mu.Lock()
	cfs := fs.cfs
	fs.mu.Unlock()

	for _, o := range cfs {
		o.syncStateFile()
		o.mu.Lock()
		meta, err := ioutil.ReadFile(path.Join(o.odir, JetStreamMetaFile))
		if err != nil {
			o.mu.Unlock()
			writeErr(fmt.Sprintf("Could not read consumer meta file for %q: %v", o.name, err))
			return
		}
		sum, err := ioutil.ReadFile(path.Join(o.odir, JetStreamMetaFileSum))
		if err != nil {
			o.mu.Unlock()
			writeErr(fmt.Sprintf("Could not read consumer checksum file for %q: %v", o.name, err))
			return
		}
		state, err := ioutil.ReadFile(path.Join(o.odir, consumerState))
		if err != nil {
			o.mu.Unlock()
			writeErr(fmt.Sprintf("Could not read consumer state for %q: %v", o.name, err))
			return
		}
		odirPre := consumerDir + "/" + o.name
		o.mu.Unlock()

		// Write all the consumer files.
		if writeFile(path.Join(odirPre, JetStreamMetaFile), meta) != nil {
			return
		}
		if writeFile(path.Join(odirPre, JetStreamMetaFileSum), sum) != nil {
			return
		}
		writeFile(path.Join(odirPre, consumerState), state)
	}
}

// Create a snapshot of this stream and its consumer's state along with messages.
func (fs *fileStore) Snapshot(deadline time.Duration, includeConsumers bool) (*SnapshotResult, error) {
	fs.mu.Lock()
	if fs.closed {
		fs.mu.Unlock()
		return nil, ErrStoreClosed
	}
	// Only allow one at a time.
	if fs.sips > 0 {
		fs.mu.Unlock()
		return nil, ErrStoreSnapshotInProgress
	}
	// Mark us as snapshotting
	fs.sips += 1
	blks := fs.blks
	blkSize := int(fs.fcfg.BlockSize)
	fs.mu.Unlock()

	pr, pw := net.Pipe()

	// Set a write deadline here to protect ourselves.
	if deadline > 0 {
		pw.SetWriteDeadline(time.Now().Add(deadline))
	}
	// Stream in separate Go routine.
	go fs.streamSnapshot(pw, blks, includeConsumers)

	return &SnapshotResult{pr, blkSize, len(blks)}, nil
}

////////////////////////////////////////////////////////////////////////////////
// Consumers
////////////////////////////////////////////////////////////////////////////////

type consumerFileStore struct {
	mu     sync.Mutex
	fs     *fileStore
	cfg    *FileConsumerInfo
	name   string
	odir   string
	ifn    string
	ifd    *os.File
	lwsz   int64
	hh     hash.Hash64
	fch    chan struct{}
	qch    chan struct{}
	closed bool
}

func (fs *fileStore) ConsumerStore(name string, cfg *ConsumerConfig) (ConsumerStore, error) {
	if fs == nil {
		return nil, fmt.Errorf("filestore is nil")
	}
	if fs.isClosed() {
		return nil, ErrStoreClosed
	}
	if cfg == nil || name == "" {
		return nil, fmt.Errorf("bad consumer config")
	}
	odir := path.Join(fs.fcfg.StoreDir, consumerDir, name)
	if err := os.MkdirAll(odir, 0755); err != nil {
		return nil, fmt.Errorf("could not create consumer directory - %v", err)
	}
	csi := &FileConsumerInfo{ConsumerConfig: *cfg}
	o := &consumerFileStore{
		fs:   fs,
		cfg:  csi,
		name: name,
		odir: odir,
		ifn:  path.Join(odir, consumerState),
		fch:  make(chan struct{}),
		qch:  make(chan struct{}),
	}
	key := sha256.Sum256([]byte(fs.cfg.Name + "/" + name))
	hh, err := highwayhash.New64(key[:])
	if err != nil {
		return nil, fmt.Errorf("could not create hash: %v", err)
	}
	o.hh = hh

	// Write our meta data iff does not exist.
	meta := path.Join(odir, JetStreamMetaFile)
	if _, err := os.Stat(meta); err != nil && os.IsNotExist(err) {
		csi.Created = time.Now().UTC()
		if err := o.writeConsumerMeta(); err != nil {
			return nil, err
		}
	}

	fs.mu.Lock()
	fs.cfs = append(fs.cfs, o)
	fs.mu.Unlock()

	return o, nil
}

const seqsHdrSize = 6*binary.MaxVarintLen64 + hdrLen

func (o *consumerFileStore) Update(state *ConsumerState) error {
	// Sanity checks.
	if state.Delivered.ConsumerSeq < 1 || state.Delivered.StreamSeq < 1 {
		return fmt.Errorf("bad delivered sequences")
	}
	if state.AckFloor.ConsumerSeq > state.Delivered.ConsumerSeq {
		return fmt.Errorf("bad ack floor for consumer")
	}
	if state.AckFloor.StreamSeq > state.Delivered.StreamSeq {
		return fmt.Errorf("bad ack floor for stream")
	}

	var hdr [seqsHdrSize]byte

	// Write header
	hdr[0] = magic
	hdr[1] = version

	n := hdrLen
	n += binary.PutUvarint(hdr[n:], state.AckFloor.ConsumerSeq)
	n += binary.PutUvarint(hdr[n:], state.AckFloor.StreamSeq)
	n += binary.PutUvarint(hdr[n:], state.Delivered.ConsumerSeq-state.AckFloor.ConsumerSeq)
	n += binary.PutUvarint(hdr[n:], state.Delivered.StreamSeq-state.AckFloor.StreamSeq)
	n += binary.PutUvarint(hdr[n:], uint64(len(state.Pending)))
	buf := hdr[:n]

	// These are optional, but always write len. This is to avoid truncate inline.
	// If these get big might make more sense to do writes directly to the file.

	if len(state.Pending) > 0 {
		mbuf := make([]byte, len(state.Pending)*(2*binary.MaxVarintLen64)+binary.MaxVarintLen64)
		aflr := state.AckFloor.StreamSeq
		maxd := state.Delivered.StreamSeq

		// To save space we select the smallest timestamp.
		var mints int64
		for k, v := range state.Pending {
			if mints == 0 || v < mints {
				mints = v
			}
			if k <= aflr || k > maxd {
				return fmt.Errorf("bad pending entry, sequence [%d] out of range", k)
			}
		}

		// Downsample the minimum timestamp.
		mints /= int64(time.Second)
		var n int
		// Write minimum timestamp we found from above.
		n += binary.PutVarint(mbuf[n:], mints)

		for k, v := range state.Pending {
			n += binary.PutUvarint(mbuf[n:], k-aflr)
			// Downsample to seconds to save on space. Subsecond resolution not
			// needed for recovery etc.
			n += binary.PutVarint(mbuf[n:], (v/int64(time.Second))-mints)
		}
		buf = append(buf, mbuf[:n]...)
	}

	var lenbuf [binary.MaxVarintLen64]byte
	n = binary.PutUvarint(lenbuf[0:], uint64(len(state.Redelivered)))
	buf = append(buf, lenbuf[:n]...)

	// We expect these to be small so will not do anything too crazy here to
	// keep the size small. Trick could be to offset sequence like above, but
	// we would need to know low sequence number for redelivery, can't depend on ackfloor etc.
	if len(state.Redelivered) > 0 {
		mbuf := make([]byte, len(state.Redelivered)*(2*binary.MaxVarintLen64))
		var n int
		for k, v := range state.Redelivered {
			n += binary.PutUvarint(mbuf[n:], k)
			n += binary.PutUvarint(mbuf[n:], v)
		}
		buf = append(buf, mbuf[:n]...)
	}

	// Check if we have the index file open.
	o.mu.Lock()

	err := o.ensureStateFileOpen()
	if err == nil {
		n, err = o.ifd.WriteAt(buf, 0)
		o.lwsz = int64(n)
	}
	o.mu.Unlock()

	return err
}

// Will upodate the config. Only used when recovering ephemerals.
func (o *consumerFileStore) updateConfig(cfg ConsumerConfig) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.cfg = &FileConsumerInfo{ConsumerConfig: cfg}
	return o.writeConsumerMeta()
}

// Write out the consumer meta data, i.e. state.
// Lock should be held.
func (cfs *consumerFileStore) writeConsumerMeta() error {
	meta := path.Join(cfs.odir, JetStreamMetaFile)
	if _, err := os.Stat(meta); (err != nil && !os.IsNotExist(err)) || err == nil {
		return err
	}
	b, err := json.MarshalIndent(cfs.cfg, _EMPTY_, "  ")
	if err != nil {
		return err
	}
	if err := ioutil.WriteFile(meta, b, 0644); err != nil {
		return err
	}
	cfs.hh.Reset()
	cfs.hh.Write(b)
	checksum := hex.EncodeToString(cfs.hh.Sum(nil))
	sum := path.Join(cfs.odir, JetStreamMetaFileSum)
	if err := ioutil.WriteFile(sum, []byte(checksum), 0644); err != nil {
		return err
	}
	return nil
}

func (o *consumerFileStore) syncStateFile() {
	// FIXME(dlc) - Hold last error?
	o.mu.Lock()
	if o.ifd != nil {
		o.ifd.Sync()
		o.ifd.Truncate(o.lwsz)
	}
	o.mu.Unlock()
}

// Lock should be held.
func (o *consumerFileStore) ensureStateFileOpen() error {
	if o.ifd == nil {
		ifd, err := os.OpenFile(o.ifn, os.O_CREATE|os.O_RDWR, 0644)
		if err != nil {
			return err
		}
		o.ifd = ifd
	}
	return nil
}

func checkHeader(hdr []byte) error {
	if hdr == nil || len(hdr) < 2 || hdr[0] != magic || hdr[1] != version {
		return fmt.Errorf("corrupt state file")
	}
	return nil
}

// State retrieves the state from the state file.
// This is not expected to be called in high performance code, only on startup.
func (o *consumerFileStore) State() (*ConsumerState, error) {
	o.mu.Lock()
	defer o.mu.Unlock()

	buf, err := ioutil.ReadFile(o.ifn)
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}

	var state *ConsumerState

	if len(buf) == 0 {
		return state, nil
	}

	if err := checkHeader(buf); err != nil {
		return nil, err
	}

	bi := hdrLen

	// Helpers, will set i to -1 on error.
	readSeq := func() uint64 {
		if bi < 0 {
			return 0
		}
		seq, n := binary.Uvarint(buf[bi:])
		if n <= 0 {
			bi = -1
			return 0
		}
		bi += n
		return seq
	}
	readTimeStamp := func() int64 {
		if bi < 0 {
			return 0
		}
		ts, n := binary.Varint(buf[bi:])
		if n <= 0 {
			bi = -1
			return -1
		}
		bi += n
		return ts
	}
	// Just for clarity below.
	readLen := readSeq
	readCount := readSeq

	state = &ConsumerState{}
	state.AckFloor.ConsumerSeq = readSeq()
	state.AckFloor.StreamSeq = readSeq()
	state.Delivered.ConsumerSeq = readSeq()
	state.Delivered.StreamSeq = readSeq()

	if bi == -1 {
		return nil, fmt.Errorf("corrupt state file")
	}
	// Adjust back.
	state.Delivered.ConsumerSeq += state.AckFloor.ConsumerSeq
	state.Delivered.StreamSeq += state.AckFloor.StreamSeq

	numPending := readLen()

	// We have additional stuff.
	if numPending > 0 {
		mints := readTimeStamp()
		state.Pending = make(map[uint64]int64, numPending)
		for i := 0; i < int(numPending); i++ {
			seq := readSeq()
			ts := readTimeStamp()
			if seq == 0 || ts == -1 {
				return nil, fmt.Errorf("corrupt state file")
			}
			// Adjust seq back.
			seq += state.AckFloor.StreamSeq
			// Adjust the timestamp back.
			ts = (ts + mints) * int64(time.Second)
			// Store in pending.
			state.Pending[seq] = ts
		}
	}

	numRedelivered := readLen()

	// We have redelivered entries here.
	if numRedelivered > 0 {
		state.Redelivered = make(map[uint64]uint64, numRedelivered)
		for i := 0; i < int(numRedelivered); i++ {
			seq := readSeq()
			n := readCount()
			if seq == 0 || n == 0 {
				return nil, fmt.Errorf("corrupt state file")
			}
			state.Redelivered[seq] = n
		}
	}
	return state, nil
}

// Stop the processing of the consumers's state.
func (o *consumerFileStore) Stop() error {
	o.mu.Lock()
	if o.closed {
		o.mu.Unlock()
		return nil
	}
	o.closed = true
	if o.ifd != nil {
		o.ifd.Sync()
		o.ifd.Close()
		o.ifd = nil
	}
	fs := o.fs
	o.mu.Unlock()
	fs.removeConsumer(o)
	return nil
}

// Delete the consumer.
func (o *consumerFileStore) Delete() error {
	// Call stop first. OK if already stopped.
	o.Stop()
	o.mu.Lock()
	var err error
	if o.odir != "" {
		err = os.RemoveAll(o.odir)
	}
	o.mu.Unlock()
	return err
}

func (fs *fileStore) removeConsumer(cfs *consumerFileStore) {
	fs.mu.Lock()
	for i, o := range fs.cfs {
		if o == cfs {
			fs.cfs = append(fs.cfs[:i], fs.cfs[i+1:]...)
			break
		}
	}
	fs.mu.Unlock()
}

// Templates
type templateFileStore struct {
	dir string
	hh  hash.Hash64
}

func newTemplateFileStore(storeDir string) *templateFileStore {
	tdir := path.Join(storeDir, tmplsDir)
	key := sha256.Sum256([]byte("templates"))
	hh, err := highwayhash.New64(key[:])
	if err != nil {
		return nil
	}
	return &templateFileStore{dir: tdir, hh: hh}
}

func (ts *templateFileStore) Store(t *StreamTemplate) error {
	dir := path.Join(ts.dir, t.Name)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("could not create templates storage directory for %q- %v", t.Name, err)
	}
	meta := path.Join(dir, JetStreamMetaFile)
	if _, err := os.Stat(meta); (err != nil && !os.IsNotExist(err)) || err == nil {
		return err
	}
	t.mu.Lock()
	b, err := json.MarshalIndent(t, _EMPTY_, "  ")
	t.mu.Unlock()
	if err != nil {
		return err
	}
	if err := ioutil.WriteFile(meta, b, 0644); err != nil {
		return err
	}
	// FIXME(dlc) - Do checksum
	ts.hh.Reset()
	ts.hh.Write(b)
	checksum := hex.EncodeToString(ts.hh.Sum(nil))
	sum := path.Join(dir, JetStreamMetaFileSum)
	if err := ioutil.WriteFile(sum, []byte(checksum), 0644); err != nil {
		return err
	}
	return nil
}

func (ts *templateFileStore) Delete(t *StreamTemplate) error {
	return os.RemoveAll(path.Join(ts.dir, t.Name))
}
