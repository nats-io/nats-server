// Copyright 2019-2021 The NATS Authors
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
	"bytes"
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
	"sync"
	"sync/atomic"
	"time"

	"github.com/klauspost/compress/s2"
	"github.com/minio/highwayhash"
)

type FileStoreConfig struct {
	// Where the parent directory for all storage will be located.
	StoreDir string
	// BlockSize is the file block size. This also represents the maximum overhead size.
	BlockSize uint64
	// CacheExpire is how long with no activity until we expire the cache.
	CacheExpire time.Duration
	// SyncInterval is how often we sync to disk in the background.
	SyncInterval time.Duration
	// AsyncFlush allows async flush to batch write operations.
	AsyncFlush bool
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
	mu       sync.RWMutex
	state    StreamState
	ld       *LostStreamData
	scb      StorageUpdateHandler
	ageChk   *time.Timer
	syncTmr  *time.Timer
	cfg      FileStreamInfo
	fcfg     FileStoreConfig
	lmb      *msgBlock
	blks     []*msgBlock
	hh       hash.Hash64
	qch      chan struct{}
	cfs      []*consumerFileStore
	closed   bool
	expiring bool
	fip      bool
	sips     int
}

// Represents a message store block and its data.
type msgBlock struct {
	// Here for 32bit systems and atomic.
	first   msgId
	last    msgId
	mu      sync.RWMutex
	fs      *fileStore
	mfn     string
	mfd     *os.File
	ifn     string
	ifd     *os.File
	liwsz   int64
	index   uint64
	bytes   uint64
	msgs    uint64
	lwits   int64
	lwts    int64
	llts    int64
	lrts    int64
	llseq   uint64
	hh      hash.Hash64
	cache   *cache
	cloads  uint64
	cexp    time.Duration
	ctmr    *time.Timer
	werr    error
	loading bool
	flusher bool
	dmap    map[uint64]struct{}
	fch     chan struct{}
	qch     chan struct{}
	lchk    [8]byte
}

// Write through caching layer that is also used on loading messages.
type cache struct {
	buf   []byte
	off   int
	wp    int
	idx   []uint32
	lrl   uint32
	fseq  uint64
	flush bool
}

type msgId struct {
	seq uint64
	ts  int64
}

type fileStoredMsg struct {
	subj string
	hdr  []byte
	msg  []byte
	seq  uint64
	ts   int64 // nanoseconds
	mb   *msgBlock
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
	// default cache buffer expiration
	defaultCacheBufferExpiration = 5 * time.Second
	// cache idx expiration
	defaultCacheIdxExpiration = 5 * time.Minute
	// default sync interval
	defaultSyncInterval = 60 * time.Second
	// coalesceMinimum
	coalesceMinimum = 16 * 1024
	// maxFlushWait is maximum we will wait to gather messages to flush.
	maxFlushWait = 8 * time.Millisecond
	// Metafiles for streams and consumers.
	JetStreamMetaFile    = "meta.inf"
	JetStreamMetaFileSum = "meta.sum"

	// Default stream block size.
	defaultStreamBlockSize = 16 * 1024 * 1024 // 16MB
	// Default for workqueue or interest based.
	defaultOtherBlockSize = 8 * 1024 * 1024 // 8MB
	// max block size for now.
	maxBlockSize = defaultStreamBlockSize
	// FileStoreMinBlkSize is minimum size we will do for a blk size.
	FileStoreMinBlkSize = 32 * 1000 // 32kib
	// FileStoreMaxBlkSize is maximum size we will do for a blk size.
	FileStoreMaxBlkSize = maxBlockSize
)

func newFileStore(fcfg FileStoreConfig, cfg StreamConfig) (*fileStore, error) {
	return newFileStoreWithCreated(fcfg, cfg, time.Now().UTC())
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
		return nil, fmt.Errorf("filestore max block size is %s", friendlyBytes(maxBlockSize))
	}
	if fcfg.CacheExpire == 0 {
		fcfg.CacheExpire = defaultCacheBufferExpiration
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
		return nil, fmt.Errorf("storage directory is not a directory")
	}
	tmpfile, err := ioutil.TempFile(fcfg.StoreDir, "_test_")
	if err != nil {
		return nil, fmt.Errorf("storage directory is not writable")
	}
	os.Remove(tmpfile.Name())

	fs := &fileStore{
		fcfg: fcfg,
		cfg:  FileStreamInfo{Created: created, StreamConfig: cfg},
		qch:  make(chan struct{}),
	}

	// Set flush in place to AsyncFlush which by default is false.
	fs.fip = !fcfg.AsyncFlush

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
	if maxBytes > 0 {
		blkSize := (maxBytes / 4) + 1 // (25% overhead)
		// Round up to nearest 100
		if m := blkSize % 100; m != 0 {
			blkSize += 100 - m
		}
		if blkSize < FileStoreMinBlkSize {
			blkSize = FileStoreMinBlkSize
		}
		if blkSize > FileStoreMaxBlkSize {
			blkSize = FileStoreMaxBlkSize
		}
		return uint64(blkSize)
	}

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
	b, err := json.Marshal(fs.cfg)
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
	mb := &msgBlock{fs: fs, index: index, cexp: fs.fcfg.CacheExpire}

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
	}

	// Close here since we need to rebuild state.
	file.Close()

	// If we get data loss rebuilding the message block state record that with the fs itself.
	if ld, _ := mb.rebuildState(); ld != nil {
		fs.rebuildState(ld)
	}
	// Rewrite this to make sure we are sync'd.
	mb.writeIndexInfo()
	fs.blks = append(fs.blks, mb)
	fs.lmb = mb
	return mb
}

func (fs *fileStore) lostData() *LostStreamData {
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	if fs.ld == nil {
		return nil
	}
	nld := *fs.ld
	return &nld
}

func (fs *fileStore) rebuildState(ld *LostStreamData) {
	if fs.ld != nil {
		fs.ld.Msgs = append(fs.ld.Msgs, ld.Msgs...)
		msgs := fs.ld.Msgs
		sort.Slice(msgs, func(i, j int) bool { return msgs[i] < msgs[j] })
		fs.ld.Bytes += ld.Bytes
	} else {
		fs.ld = ld
	}
	fs.state.Msgs, fs.state.Bytes = 0, 0
	fs.state.FirstSeq, fs.state.LastSeq = 0, 0

	for _, mb := range fs.blks {
		mb.mu.RLock()
		fs.state.Msgs += mb.msgs
		fs.state.Bytes += mb.bytes
		if fs.state.FirstSeq == 0 || mb.first.seq < fs.state.FirstSeq {
			fs.state.FirstSeq = mb.first.seq
			fs.state.FirstTime = time.Unix(0, mb.first.ts).UTC()
		}
		fs.state.LastSeq = mb.last.seq
		fs.state.LastTime = time.Unix(0, mb.last.ts).UTC()
		mb.mu.RUnlock()
	}
}

func (mb *msgBlock) rebuildState() (*LostStreamData, error) {
	mb.mu.Lock()
	defer mb.mu.Unlock()

	startLastSeq := mb.last.seq

	// Clear state we need to rebuild.
	mb.msgs, mb.bytes = 0, 0
	mb.last.seq, mb.last.ts = 0, 0
	firstNeedsSet := true

	buf, err := ioutil.ReadFile(mb.mfn)
	if err != nil {
		return nil, err
	}

	addToDmap := func(seq uint64) {
		if seq == 0 {
			return
		}
		if mb.dmap == nil {
			mb.dmap = make(map[uint64]struct{})
		}
		mb.dmap[seq] = struct{}{}
	}

	var le = binary.LittleEndian

	truncate := func(index uint32) {
		var fd *os.File
		if mb.mfd != nil {
			fd = mb.mfd
		} else {
			fd, err = os.OpenFile(mb.mfn, os.O_RDWR, 0644)
			if err != nil {
				defer fd.Close()
			}
		}
		if fd == nil {
			return
		}
		if err := fd.Truncate(int64(index)); err == nil {
			// Update our checksum.
			if index >= 8 {
				var lchk [8]byte
				fd.ReadAt(lchk[:], int64(index-8))
				copy(mb.lchk[0:], lchk[:])
			}
			fd.Sync()
		}
	}

	gatherLost := func(lb uint32) *LostStreamData {
		var ld LostStreamData
		for seq := mb.last.seq + 1; seq <= startLastSeq; seq++ {
			ld.Msgs = append(ld.Msgs, seq)
		}
		ld.Bytes = uint64(lb)
		return &ld
	}

	for index, lbuf := uint32(0), uint32(len(buf)); index < lbuf; {
		if index+msgHdrSize >= lbuf {
			truncate(index)
			return gatherLost(lbuf - index), nil
		}

		hdr := buf[index : index+msgHdrSize]
		rl := le.Uint32(hdr[0:])
		slen := le.Uint16(hdr[20:])

		hasHeaders := rl&hbit != 0
		// Clear any headers bit that could be set.
		rl &^= hbit
		dlen := int(rl) - msgHdrSize
		// Do some quick sanity checks here.
		if dlen < 0 || int(slen) > dlen || dlen > int(rl) {
			truncate(index)
			return gatherLost(lbuf - index), errBadMsg
		}

		if index+rl > lbuf {
			truncate(index)
			return gatherLost(lbuf - index), errBadMsg
		}

		seq := le.Uint64(hdr[4:])
		ts := int64(le.Uint64(hdr[12:]))

		// This is an old erased message, or a new one that we can track.
		if seq == 0 || seq&ebit != 0 || seq < mb.first.seq {
			seq = seq &^ ebit
			addToDmap(seq)
			index += rl
			continue
		}

		// This is for when we have index info that adjusts for deleted messages
		// at the head. So the first.seq will be already set here. If this is larger
		// replace what we have with this seq.
		if firstNeedsSet && seq > mb.first.seq {
			firstNeedsSet = false
			mb.first.seq = seq
			mb.first.ts = ts
		}

		var deleted bool
		if mb.dmap != nil {
			if _, ok := mb.dmap[seq]; ok {
				deleted = true
			}
		}

		if !deleted {
			if hh := mb.hh; hh != nil {
				data := buf[index+msgHdrSize : index+rl]
				hh.Reset()
				hh.Write(hdr[4:20])
				hh.Write(data[:slen])
				if hasHeaders {
					hh.Write(data[slen+4 : dlen-8])
				} else {
					hh.Write(data[slen : dlen-8])
				}
				checksum := hh.Sum(nil)
				if !bytes.Equal(checksum, data[len(data)-8:]) {
					truncate(index)
					return gatherLost(lbuf - index), errBadMsg
				}
				copy(mb.lchk[0:], checksum)
			}

			if firstNeedsSet {
				firstNeedsSet = false
				mb.first.seq = seq
				mb.first.ts = ts
			}
			mb.last.seq = seq
			mb.last.ts = ts

			mb.msgs++
			mb.bytes += uint64(rl)
		}

		index += rl
	}

	return nil, nil
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
		return errNotReadable
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

// RegisterStorageUpdates registers a callback for updates to storage changes.
// It will present number of messages and bytes as a signed integer and an
// optional sequence number of the message if a single.
func (fs *fileStore) RegisterStorageUpdates(cb StorageUpdateHandler) {
	fs.mu.Lock()
	fs.scb = cb
	bsz := fs.state.Bytes
	fs.mu.Unlock()
	if cb != nil && bsz > 0 {
		cb(0, int64(bsz), 0, _EMPTY_)
	}
}

// Helper to get hash key for specific message block.
// Lock should be held
func (fs *fileStore) hashKeyForBlock(index uint64) []byte {
	return []byte(fmt.Sprintf("%s-%d", fs.cfg.Name, index))
}

func (mb *msgBlock) setupWriteCache(buf []byte) {
	// Make sure we have a cache setup.
	if mb.cache != nil {
		return
	}
	mb.cache = &cache{buf: buf}
	// Make sure we set the proper cache offset if we have existing data.
	var fi os.FileInfo
	if mb.mfd != nil {
		fi, _ = mb.mfd.Stat()
	} else {
		fi, _ = os.Stat(mb.mfn)
	}
	if fi != nil {
		mb.cache.off = int(fi.Size())
	}
	mb.startCacheExpireTimer()
}

// This rolls to a new append msg block.
// Lock should be held.
func (fs *fileStore) newMsgBlockForWrite() (*msgBlock, error) {
	var mbuf []byte
	index := uint64(1)
	if lmb := fs.lmb; lmb != nil {
		index = lmb.index + 1

		// Determine if we can reclaim resources here.
		if fs.fip {
			// Reset write timestamp and see if we can expire this cache.
			lmb.mu.Lock()
			lmb.closeFDsLocked()
			lmb.lwts = 0
			mbuf = lmb.expireCacheLocked()
			lmb.mu.Unlock()
		}
	}

	mb := &msgBlock{fs: fs, index: index, cexp: fs.fcfg.CacheExpire}
	mb.setupWriteCache(mbuf)

	// Now do local hash.
	key := sha256.Sum256(fs.hashKeyForBlock(index))
	hh, err := highwayhash.New64(key[:])
	if err != nil {
		return nil, fmt.Errorf("could not create hash: %v", err)
	}
	mb.hh = hh

	mdir := path.Join(fs.fcfg.StoreDir, msgDir)
	mb.mfn = path.Join(mdir, fmt.Sprintf(blkScan, mb.index))
	mfd, err := os.OpenFile(mb.mfn, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		mb.dirtyCloseWithRemove(true)
		return nil, fmt.Errorf("Error creating msg block file [%q]: %v", mb.mfn, err)
	}
	mb.mfd = mfd

	mb.ifn = path.Join(mdir, fmt.Sprintf(indexScan, mb.index))
	ifd, err := os.OpenFile(mb.ifn, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		mb.dirtyCloseWithRemove(true)
		return nil, fmt.Errorf("Error creating msg index file [%q]: %v", mb.mfn, err)
	}
	mb.ifd = ifd

	// Set cache time to creation time to start.
	ts := time.Now().UnixNano()
	// Race detector wants these protected.
	mb.mu.Lock()
	mb.llts, mb.lwts = ts, ts
	mb.mu.Unlock()

	// Remember our last sequence number.
	mb.first.seq = fs.state.LastSeq + 1
	mb.last.seq = fs.state.LastSeq

	// If we know we will need this so go ahead and spin up.
	if !fs.fip {
		mb.spinUpFlushLoop()
	}

	// Add to our list of blocks and mark as last.
	fs.blks = append(fs.blks, mb)
	fs.lmb = mb

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
	mfd, err := os.OpenFile(mb.mfn, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("error opening msg block file [%q]: %v", mb.mfn, err)
	}
	mb.mfd = mfd

	// Spin up our flusher loop if needed.
	if !fs.fip {
		mb.spinUpFlushLoop()
	}

	return nil
}

// Stores a raw message with expected sequence number and timestamp.
// Lock should be held.
func (fs *fileStore) storeRawMsg(subj string, hdr, msg []byte, seq uint64, ts int64) error {
	if fs.closed {
		return ErrStoreClosed
	}

	// Check if we are discarding new messages when we reach the limit.
	if fs.cfg.Discard == DiscardNew {
		if fs.cfg.MaxMsgs > 0 && fs.state.Msgs >= uint64(fs.cfg.MaxMsgs) {
			return ErrMaxMsgs
		}
		if fs.cfg.MaxBytes > 0 && fs.state.Bytes+uint64(len(msg)+len(hdr)) >= uint64(fs.cfg.MaxBytes) {
			return ErrMaxBytes
		}
	}

	// Check sequence.
	if seq != fs.state.LastSeq+1 {
		if seq > 0 {
			return ErrSequenceMismatch
		}
		seq = fs.state.LastSeq + 1
	}

	// Write msg record.
	n, err := fs.writeMsgRecord(seq, ts, subj, hdr, msg)
	if err != nil {
		return err
	}

	// Adjust first if needed.
	now := time.Unix(0, ts).UTC()
	if fs.state.Msgs == 0 {
		fs.state.FirstSeq = seq
		fs.state.FirstTime = now
	}

	fs.state.Msgs++
	fs.state.Bytes += n
	fs.state.LastSeq = seq
	fs.state.LastTime = now

	// Limits checks and enforcement.
	// If they do any deletions they will update the
	// byte count on their own, so no need to compensate.
	fs.enforceMsgLimit()
	fs.enforceBytesLimit()

	// Check if we have and need the age expiration timer running.
	if fs.ageChk == nil && fs.cfg.MaxAge != 0 {
		fs.startAgeChk()
	}

	return nil
}

// StoreRawMsg stores a raw message with expected sequence number and timestamp.
func (fs *fileStore) StoreRawMsg(subj string, hdr, msg []byte, seq uint64, ts int64) error {
	fs.mu.Lock()
	err := fs.storeRawMsg(subj, hdr, msg, seq, ts)
	cb := fs.scb
	fs.mu.Unlock()

	if err == nil && cb != nil {
		cb(1, int64(fileStoreMsgSize(subj, hdr, msg)), seq, subj)
	}

	return err
}

// Store stores a message. We hold the main filestore lock for any write operation.
func (fs *fileStore) StoreMsg(subj string, hdr, msg []byte) (uint64, int64, error) {
	fs.mu.Lock()
	seq, ts := fs.state.LastSeq+1, time.Now().UnixNano()
	err := fs.storeRawMsg(subj, hdr, msg, seq, ts)
	cb := fs.scb
	fs.mu.Unlock()

	if err != nil {
		seq, ts = 0, 0
	} else if cb != nil {
		cb(1, int64(fileStoreMsgSize(subj, hdr, msg)), seq, subj)
	}

	return seq, ts, err
}

// skipMsg will update this message block for a skipped message.
// If we do not have any messages, just update the metadata, otherwise
// we will place and empty record marking the sequence as used. The
// sequence will be marked erased.
// fs lock should be held.
func (mb *msgBlock) skipMsg(seq uint64, now time.Time) {
	if mb == nil {
		return
	}
	var needsRecord bool

	mb.mu.Lock()
	// If we are empty can just do meta.
	if mb.msgs == 0 {
		mb.last.seq = seq
		mb.last.ts = now.UnixNano()
		mb.first.seq = seq + 1
		mb.first.ts = now.UnixNano()
	} else {
		needsRecord = true
		if mb.dmap == nil {
			mb.dmap = make(map[uint64]struct{})
		}
		mb.dmap[seq] = struct{}{}
		mb.msgs--
		mb.bytes -= emptyRecordLen
	}
	mb.mu.Unlock()

	if needsRecord {
		mb.writeMsgRecord(emptyRecordLen, seq|ebit, _EMPTY_, nil, nil, now.UnixNano(), true)
	} else {
		mb.kickFlusher()
	}
}

// SkipMsg will use the next sequence number but not store anything.
func (fs *fileStore) SkipMsg() uint64 {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Grab time.
	now := time.Now().UTC()
	seq := fs.state.LastSeq + 1
	fs.state.LastSeq = seq
	fs.state.LastTime = now
	if fs.state.Msgs == 0 {
		fs.state.FirstSeq = seq
		fs.state.FirstTime = now
	}
	if seq == fs.state.FirstSeq {
		fs.state.FirstSeq = seq + 1
		fs.state.FirstTime = now
	}
	fs.lmb.skipMsg(seq, now)

	return seq
}

// Lock should be held.
func (fs *fileStore) rebuildFirst() {
	if len(fs.blks) == 0 {
		return
	}
	if fmb := fs.blks[0]; fmb != nil {
		fmb.removeIndexFile()
		fmb.rebuildState()
		fmb.writeIndexInfo()
		fs.selectNextFirst()
	}
}

// Will check the msg limit and drop firstSeq msg if needed.
// Lock should be held.
func (fs *fileStore) enforceMsgLimit() {
	if fs.cfg.MaxMsgs <= 0 || fs.state.Msgs <= uint64(fs.cfg.MaxMsgs) {
		return
	}
	for nmsgs := fs.state.Msgs; nmsgs > uint64(fs.cfg.MaxMsgs); nmsgs = fs.state.Msgs {
		if removed, err := fs.deleteFirstMsgLocked(); err != nil || !removed {
			fs.rebuildFirst()
			return
		}
	}
}

// Will check the bytes limit and drop msgs if needed.
// Lock should be held.
func (fs *fileStore) enforceBytesLimit() {
	if fs.cfg.MaxBytes <= 0 || fs.state.Bytes <= uint64(fs.cfg.MaxBytes) {
		return
	}
	for bs := fs.state.Bytes; bs > uint64(fs.cfg.MaxBytes); bs = fs.state.Bytes {
		if removed, err := fs.deleteFirstMsgLocked(); err != nil || !removed {
			fs.rebuildFirst()
			return
		}
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

// Remove a message, optionally rewriting the mb file.
func (fs *fileStore) removeMsg(seq uint64, secure bool) (bool, error) {
	fs.mu.Lock()

	if fs.closed {
		fs.mu.Unlock()
		return false, ErrStoreClosed
	}
	if fs.sips > 0 {
		fs.mu.Unlock()
		return false, ErrStoreSnapshotInProgress
	}

	mb := fs.selectMsgBlock(seq)
	if mb == nil {
		var err = ErrStoreEOF
		if seq <= fs.state.LastSeq {
			err = ErrStoreMsgNotFound
		}
		fs.mu.Unlock()
		return false, err
	}

	// If we have a callback grab the message since we need the subject.
	// TODO(dlc) - This will cause whole buffer to be loaded which I was trying
	// to avoid. Maybe use side cache for subjects or understand when we really need them.
	// Meaning if the stream above is only a single subject no need to store, this is just
	// for updating stream pending for consumers.
	var sm *fileStoredMsg
	if fs.scb != nil {
		sm, _ = mb.fetchMsg(seq)
	}

	mb.mu.Lock()

	// Check cache. This should be very rare.
	if mb.cache == nil || mb.cache.idx == nil || seq < mb.cache.fseq && mb.cache.off > 0 {
		mb.mu.Unlock()
		fs.mu.Unlock()
		if err := mb.loadMsgs(); err != nil {
			return false, err
		}
		fs.mu.Lock()
		mb.mu.Lock()
	}

	// See if the sequence numbers is still relevant. Check first and cache first.
	if seq < mb.first.seq || seq < mb.cache.fseq || (seq-mb.cache.fseq) >= uint64(len(mb.cache.idx)) {
		mb.mu.Unlock()
		fs.mu.Unlock()
		return false, nil
	}

	// Now check dmap if it is there.
	if mb.dmap != nil {
		if _, ok := mb.dmap[seq]; ok {
			mb.mu.Unlock()
			fs.mu.Unlock()
			return false, nil
		}
	}

	// Set cache timestamp for last remove.
	mb.lrts = time.Now().UnixNano()

	// Grab record length from idx.
	slot := seq - mb.cache.fseq
	ri, rl, _, _ := mb.slotInfo(int(slot))
	msz := uint64(rl)

	// Global stats
	fs.state.Msgs--
	fs.state.Bytes -= msz

	// Now local mb updates.
	mb.msgs--
	mb.bytes -= msz

	var shouldWriteIndex, firstSeqNeedsUpdate bool

	if secure {
		mb.eraseMsg(seq, int(ri), int(rl))
	}

	// Optimize for FIFO case.
	if seq == mb.first.seq {
		mb.selectNextFirst()
		if mb.isEmpty() {
			fs.removeMsgBlock(mb)
			firstSeqNeedsUpdate = seq == fs.state.FirstSeq
		} else {
			shouldWriteIndex = true
			if seq == fs.state.FirstSeq {
				fs.state.FirstSeq = mb.first.seq // new one.
				fs.state.FirstTime = time.Unix(0, mb.first.ts).UTC()
			}
		}
	} else {
		// Out of order delete.
		if mb.dmap == nil {
			mb.dmap = make(map[uint64]struct{})
		}
		mb.dmap[seq] = struct{}{}
		shouldWriteIndex = true
	}

	var qch, fch chan struct{}
	if shouldWriteIndex {
		qch = mb.qch
		fch = mb.fch
	}
	cb := fs.scb
	mb.mu.Unlock()

	if secure {
		mb.flushPendingMsgs()
	}

	// Kick outside of lock.
	if shouldWriteIndex {
		if !fs.fip {
			if qch == nil {
				mb.spinUpFlushLoop()
			}
			select {
			case fch <- struct{}{}:
			default:
			}
		} else {
			mb.writeIndexInfo()
		}
	}

	// If we emptied the current message block and the seq was state.First.Seq
	// then we need to jump message blocks.
	if firstSeqNeedsUpdate {
		fs.selectNextFirst()
	}
	fs.mu.Unlock()

	// Storage updates.
	if cb != nil {
		subj := _EMPTY_
		if sm != nil {
			subj = sm.subj
		}
		delta := int64(msz)
		cb(-1, -delta, seq, subj)
	}

	return true, nil
}

// Grab info from a slot.
// Lock should be held.
func (mb *msgBlock) slotInfo(slot int) (uint32, uint32, bool, error) {
	if mb.cache == nil || slot >= len(mb.cache.idx) {
		return 0, 0, false, errPartialCache
	}
	bi := mb.cache.idx[slot]
	ri := (bi &^ hbit)
	hashChecked := (bi & hbit) != 0
	// Determine record length
	var rl uint32
	if len(mb.cache.idx) > slot+1 {
		ni := mb.cache.idx[slot+1] &^ hbit
		rl = ni - ri
	} else {
		rl = mb.cache.lrl
	}
	if rl < msgHdrSize {
		return 0, 0, false, errBadMsg
	}
	return uint32(ri), rl, hashChecked, nil
}

func (fs *fileStore) isClosed() bool {
	fs.mu.RLock()
	closed := fs.closed
	fs.mu.RUnlock()
	return closed
}

// Will spin up our flush loop.
func (mb *msgBlock) spinUpFlushLoop() {
	mb.mu.Lock()
	defer mb.mu.Unlock()

	// Are we already running?
	if mb.flusher {
		return
	}
	mb.flusher = true
	mb.fch = make(chan struct{}, 1)
	mb.qch = make(chan struct{})
	fch, qch := mb.fch, mb.qch

	go mb.flushLoop(fch, qch)
}

// Raw low level kicker for flush loops.
func kickFlusher(fch chan struct{}) {
	if fch != nil {
		select {
		case fch <- struct{}{}:
		default:
		}
	}
}

// Kick flusher for this message block.
func (mb *msgBlock) kickFlusher() {
	mb.mu.RLock()
	defer mb.mu.RUnlock()
	kickFlusher(mb.fch)
}

func (mb *msgBlock) setInFlusher() {
	mb.mu.Lock()
	mb.flusher = true
	mb.mu.Unlock()
}

func (mb *msgBlock) clearInFlusher() {
	mb.mu.Lock()
	mb.flusher = false
	mb.mu.Unlock()
}

// flushLoop watches for messages, index info, or recently closed msg block updates.
func (mb *msgBlock) flushLoop(fch, qch chan struct{}) {
	mb.setInFlusher()
	defer mb.clearInFlusher()

	// Will use to test if we have meta data updates.
	var firstSeq, lastSeq uint64
	var dmapLen int

	infoChanged := func() bool {
		mb.mu.RLock()
		defer mb.mu.RUnlock()
		var changed bool
		if firstSeq != mb.first.seq || lastSeq != mb.last.seq || dmapLen != len(mb.dmap) {
			changed = true
			firstSeq, lastSeq = mb.first.seq, mb.last.seq
			dmapLen = len(mb.dmap)
		}
		return changed
	}

	for {
		select {
		case <-fch:
			// If we have pending messages process them first.
			if waiting := mb.pendingWriteSize(); waiting != 0 {
				ts := 1 * time.Millisecond
				var waited time.Duration

				for waiting < coalesceMinimum {
					time.Sleep(ts)
					select {
					case <-qch:
						return
					default:
					}
					newWaiting := mb.pendingWriteSize()
					if waited = waited + ts; waited > maxFlushWait || newWaiting <= waiting {
						break
					}
					waiting = newWaiting
					ts *= 2
				}
				mb.flushPendingMsgs()
				// Check if we are no longer the last message block. If we are
				// not we can close FDs and exit.
				mb.fs.mu.RLock()
				notLast := mb != mb.fs.lmb
				mb.fs.mu.RUnlock()
				if notLast {
					if err := mb.closeFDs(); err == nil {
						return
					}
				}
			}
			if infoChanged() {
				mb.writeIndexInfo()
			}
		case <-qch:
			return
		}
	}
}

// Lock should be held.
func (mb *msgBlock) eraseMsg(seq uint64, ri, rl int) error {
	var le = binary.LittleEndian
	var hdr [msgHdrSize]byte

	le.PutUint32(hdr[0:], uint32(rl))
	le.PutUint64(hdr[4:], seq|ebit)
	le.PutUint64(hdr[12:], 0)
	le.PutUint16(hdr[20:], 0)

	// Randomize record
	data := make([]byte, rl-emptyRecordLen)
	rand.Read(data)

	// Now write to underlying buffer.
	var b bytes.Buffer
	b.Write(hdr[:])
	b.Write(data)

	// Calculate hash.
	mb.hh.Reset()
	mb.hh.Write(hdr[4:20])
	mb.hh.Write(data)
	checksum := mb.hh.Sum(nil)
	// Write to msg record.
	b.Write(checksum)

	// Update both cache and disk.
	nbytes := b.Bytes()

	// Cache
	if ri >= mb.cache.off {
		li := ri - mb.cache.off
		buf := mb.cache.buf[li : li+rl]
		copy(buf, nbytes)
	}

	// Disk
	if mb.cache.off+mb.cache.wp > ri {
		mfd, err := os.OpenFile(mb.mfn, os.O_RDWR, 0644)
		if err != nil {
			return err
		}
		defer mfd.Close()
		if _, err = mfd.WriteAt(nbytes, int64(ri)); err == nil {
			mfd.Sync()
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// Truncate this message block to the storedMsg.
func (mb *msgBlock) truncate(sm *fileStoredMsg) (nmsgs, nbytes uint64, err error) {
	// Make sure we are loaded to process messages etc.
	if err := mb.loadMsgs(); err != nil {
		return 0, 0, err
	}

	// Calculate new eof using slot info from our new last sm.
	ri, rl, _, err := mb.slotInfo(int(sm.seq - mb.cache.fseq))
	if err != nil {
		return 0, 0, err
	}
	// Calculate new eof.
	eof := int64(ri + rl)

	var purged, bytes uint64

	mb.mu.Lock()
	checkDmap := len(mb.dmap) > 0
	for seq := mb.last.seq; seq > sm.seq; seq-- {
		if checkDmap {
			if _, ok := mb.dmap[seq]; ok {
				// Delete and skip to next.
				delete(mb.dmap, seq)
				continue
			}
		}
		// We should have a valid msg to calculate removal stats.
		_, rl, _, err := mb.slotInfo(int(seq - mb.cache.fseq))
		if err != nil {
			mb.mu.Unlock()
			return 0, 0, err
		}
		purged++
		bytes += uint64(rl)
	}

	// Truncate our msgs and close file.
	if mb.mfd != nil {
		mb.mfd.Truncate(eof)
		mb.mfd.Sync()
		// Update our checksum.
		var lchk [8]byte
		mb.mfd.ReadAt(lchk[:], eof-8)
		copy(mb.lchk[0:], lchk[:])
	} else {
		mb.mu.Unlock()
		return 0, 0, fmt.Errorf("failed to truncate msg block %d, file not open", mb.index)
	}

	// Do local mb stat updates.
	mb.msgs -= purged
	mb.bytes -= bytes
	// Update our last msg.
	mb.last.seq = sm.seq
	mb.last.ts = sm.ts

	// Clear our cache.
	mb.clearCacheAndOffset()
	mb.mu.Unlock()

	// Write our index file.
	mb.writeIndexInfo()
	// Load msgs again.
	mb.loadMsgs()

	return purged, bytes, nil
}

// Lock should be held.
func (mb *msgBlock) isEmpty() bool {
	return mb.first.seq > mb.last.seq
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

	// Check if we are empty..
	if mb.isEmpty() {
		mb.first.ts = 0
		return
	}

	// Need to get the timestamp.
	// We will try the cache direct and fallback if needed.
	sm, _ := mb.cacheLookupWithLock(seq)
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

// Select the next FirstSeq
func (fs *fileStore) selectNextFirst() {
	if len(fs.blks) > 0 {
		mb := fs.blks[0]
		mb.mu.RLock()
		fs.state.FirstSeq = mb.first.seq
		fs.state.FirstTime = time.Unix(0, mb.first.ts).UTC()
		mb.mu.RUnlock()
	} else {
		// Could not find anything, so treat like purge
		fs.state.FirstSeq = fs.state.LastSeq + 1
		fs.state.FirstTime = time.Time{}
	}
}

// Lock should be held.
func (mb *msgBlock) resetCacheExpireTimer(td time.Duration) {
	if td == 0 {
		td = mb.cexp
	}
	if mb.ctmr == nil {
		mb.ctmr = time.AfterFunc(td, mb.expireCache)
	} else {
		mb.ctmr.Reset(td)
	}
}

// Lock should be held.
func (mb *msgBlock) startCacheExpireTimer() {
	mb.resetCacheExpireTimer(0)
}

// Used when we load in a message block.
// Lock should be held.
func (mb *msgBlock) clearCacheAndOffset() {
	if mb.cache != nil {
		mb.cache.off = 0
		mb.cache.wp = 0
	}
	mb.clearCache()
}

// Lock should be held.
func (mb *msgBlock) clearCache() {
	if mb.ctmr != nil {
		mb.ctmr.Stop()
		mb.ctmr = nil
	}
	if mb.cache == nil {
		return
	}

	if mb.cache.off == 0 {
		mb.cache = nil
	} else {
		// Clear msgs and index.
		mb.cache.buf = nil
		mb.cache.idx = nil
		mb.cache.wp = 0
	}
}

// Called to possibly expire a message block cache.
func (mb *msgBlock) expireCache() {
	mb.mu.Lock()
	defer mb.mu.Unlock()
	mb.expireCacheLocked()
}

func (mb *msgBlock) expireCacheLocked() []byte {
	if mb.cache == nil {
		if mb.ctmr != nil {
			mb.ctmr.Stop()
			mb.ctmr = nil
		}
		return nil
	}

	// Can't expire if we are flushing or still have pending.
	if mb.cache.flush || (len(mb.cache.buf)-int(mb.cache.wp) > 0) {
		mb.resetCacheExpireTimer(mb.cexp)
		return nil
	}

	// Grab timestamp to compare.
	tns := time.Now().UnixNano()

	// For the core buffer of messages, we care about reads and writes, but not removes.
	bufts := mb.llts
	if mb.lwts > bufts {
		bufts = mb.lwts
	}

	// Check for activity on the cache that would prevent us from expiring.
	if tns-bufts <= int64(mb.cexp) {
		mb.resetCacheExpireTimer(mb.cexp - time.Duration(tns-bufts))
		return nil
	}

	// If we are here we will at least expire the core msg buffer.
	// We need to capture offset in case we do a write next before a full load.
	buf := mb.cache.buf
	mb.cache.off += len(mb.cache.buf)
	mb.cache.buf = nil
	mb.cache.wp = 0

	// The idx is used in removes, and will have a longer timeframe.
	// See if we should also remove the idx.
	if tns-mb.lrts > int64(defaultCacheIdxExpiration) {
		mb.clearCache()
	} else {
		mb.resetCacheExpireTimer(mb.cexp)
	}

	return buf[:0]
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
	// Make sure this is only running one at a time.
	fs.mu.Lock()
	if fs.expiring {
		fs.mu.Unlock()
		return
	}
	fs.expiring = true
	fs.mu.Unlock()

	defer func() {
		fs.mu.Lock()
		fs.expiring = false
		fs.mu.Unlock()
	}()

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

// Lock should be held.
func (fs *fileStore) checkAndFlushAllBlocks() {
	for _, mb := range fs.blks {
		if mb.pendingWriteSize() > 0 {
			mb.flushPendingMsgsAndWait()
		}
		mb.writeIndexInfo()
	}
}

// This will check all the checksums on messages and report back any sequence numbers with errors.
func (fs *fileStore) checkMsgs() *LostStreamData {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	fs.checkAndFlushAllBlocks()

	for _, mb := range fs.blks {
		if ld, err := mb.rebuildState(); err != nil && ld != nil {
			// Rebuild fs state too.
			mb.fs.rebuildState(ld)
		}
	}
	return fs.ld
}

// Will write the message record to the underlying message block.
// filestore lock will be held.
func (mb *msgBlock) writeMsgRecord(rl, seq uint64, subj string, mhdr, msg []byte, ts int64, flush bool) error {
	mb.mu.Lock()
	// Make sure we have a cache setup.
	if mb.cache == nil {
		mb.setupWriteCache(nil)
	}

	// Indexing
	index := len(mb.cache.buf) + int(mb.cache.off)

	// Formats
	// Format with no header
	// total_len(4) sequence(8) timestamp(8) subj_len(2) subj msg hash(8)
	// With headers, high bit on total length will be set.
	// total_len(4) sequence(8) timestamp(8) subj_len(2) subj hdr_len(4) hdr msg hash(8)

	// First write header, etc.
	var le = binary.LittleEndian
	var hdr [msgHdrSize]byte

	l := uint32(rl)
	hasHeaders := len(mhdr) > 0
	if hasHeaders {
		l |= hbit
	}

	le.PutUint32(hdr[0:], l)
	le.PutUint64(hdr[4:], seq)
	le.PutUint64(hdr[12:], uint64(ts))
	le.PutUint16(hdr[20:], uint16(len(subj)))

	// Now write to underlying buffer.
	mb.cache.buf = append(mb.cache.buf, hdr[:]...)
	mb.cache.buf = append(mb.cache.buf, subj...)

	if hasHeaders {
		var hlen [4]byte
		le.PutUint32(hlen[0:], uint32(len(mhdr)))
		mb.cache.buf = append(mb.cache.buf, hlen[:]...)
		mb.cache.buf = append(mb.cache.buf, mhdr...)
	}
	mb.cache.buf = append(mb.cache.buf, msg...)

	// Calculate hash.
	mb.hh.Reset()
	mb.hh.Write(hdr[4:20])
	mb.hh.Write([]byte(subj))
	if hasHeaders {
		mb.hh.Write(mhdr)
	}
	mb.hh.Write(msg)
	checksum := mb.hh.Sum(nil)
	// Grab last checksum
	copy(mb.lchk[0:], checksum)

	// Update write through cache.
	// Write to msg record.
	mb.cache.buf = append(mb.cache.buf, checksum...)
	// Write index
	mb.cache.idx = append(mb.cache.idx, uint32(index)|hbit)
	mb.cache.lrl = uint32(rl)
	if mb.cache.fseq == 0 {
		mb.cache.fseq = seq
	}

	// Set cache timestamp for last store.
	mb.lwts = ts
	// Decide if we write index info if flushing in place.
	writeIndex := ts-mb.lwits > int64(time.Second)

	// Accounting
	mb.updateAccounting(seq, ts, rl)

	fch, werr := mb.fch, mb.werr
	mb.mu.Unlock()

	// If we should be flushing in place do so here. We will also flip to flushing in place if we
	// had a write error.
	if flush || werr != nil {
		if err := mb.flushPendingMsgs(); err != nil && err != errFlushRunning && err != errNoPending {
			return err
		}
		if writeIndex {
			if err := mb.writeIndexInfo(); err != nil {
				return err
			}
		}
	} else {
		// Kick the flusher here.
		kickFlusher(fch)
	}

	return nil
}

// How many bytes pending to be written for this message block.
func (mb *msgBlock) pendingWriteSize() int {
	if mb == nil {
		return 0
	}
	var pending int
	mb.mu.RLock()
	if mb.mfd != nil && mb.cache != nil {
		pending = len(mb.cache.buf) - int(mb.cache.wp)
	}
	mb.mu.RUnlock()
	return pending
}

// Lock should be held.
func (mb *msgBlock) clearFlushing() {
	if mb.cache != nil {
		mb.cache.flush = false
	}
}

// Lock should be held.
func (mb *msgBlock) setFlushing() {
	if mb.cache != nil {
		mb.cache.flush = true
	}
}

// Try to close our FDs if we can.
func (mb *msgBlock) closeFDs() error {
	mb.mu.Lock()
	defer mb.mu.Unlock()
	return mb.closeFDsLocked()
}

func (mb *msgBlock) closeFDsLocked() error {
	if buf, err := mb.bytesPending(); err == errFlushRunning || len(buf) > 0 {
		return errPendingData
	}
	if mb.mfd != nil {
		mb.mfd.Close()
		mb.mfd = nil
	}
	if mb.ifd != nil {
		mb.ifd.Close()
		mb.ifd = nil
	}
	return nil
}

// bytesPending returns the buffer to be used for writing to the underlying file.
// This marks we are in flush and will return nil if asked again until cleared.
// Lock should be held.
func (mb *msgBlock) bytesPending() ([]byte, error) {
	if mb == nil || mb.mfd == nil {
		return nil, errNoPending
	}
	if mb.cache == nil {
		return nil, errNoCache
	}
	if mb.cache.flush {
		return nil, errFlushRunning
	}
	buf := mb.cache.buf[mb.cache.wp:]
	if len(buf) == 0 {
		return nil, errNoPending
	}
	return buf, nil
}

// Return the number of bytes in this message block.
func (mb *msgBlock) numBytes() uint64 {
	mb.mu.RLock()
	nb := mb.bytes
	mb.mu.RUnlock()
	return nb
}

// Update accounting on a write msg.
// Lock should be held.
func (mb *msgBlock) updateAccounting(seq uint64, ts int64, rl uint64) {
	if mb.first.seq == 0 || mb.first.ts == 0 {
		mb.first.seq = seq
		mb.first.ts = ts
	}
	// Need atomics here for selectMsgBlock speed.
	atomic.StoreUint64(&mb.last.seq, seq)
	mb.last.ts = ts
	mb.bytes += rl
	mb.msgs++
}

// Lock should be held.
func (fs *fileStore) writeMsgRecord(seq uint64, ts int64, subj string, hdr, msg []byte) (uint64, error) {
	var err error

	// Get size for this message.
	rl := fileStoreMsgSize(subj, hdr, msg)
	if rl&hbit != 0 {
		return 0, ErrMsgTooLarge
	}
	// Grab our current last message block.
	mb := fs.lmb
	if mb == nil || mb.numBytes()+rl > fs.fcfg.BlockSize {
		if mb, err = fs.newMsgBlockForWrite(); err != nil {
			return 0, err
		}
	}

	// Ask msg block to store in write through cache.
	err = mb.writeMsgRecord(rl, seq, subj, hdr, msg, ts, fs.fip)

	return rl, err
}

// Sync msg and index files as needed. This is called from a timer.
func (fs *fileStore) syncBlocks() {
	fs.mu.RLock()
	if fs.closed {
		fs.mu.RUnlock()
		return
	}
	blks := append([]*msgBlock(nil), fs.blks...)
	fs.mu.RUnlock()

	for _, mb := range blks {
		mb.mu.RLock()
		mfd := mb.mfd
		ifd := mb.ifd
		liwsz := mb.liwsz
		mb.mu.RUnlock()

		if mfd != nil {
			mfd.Sync()
		}
		if ifd != nil {
			ifd.Truncate(liwsz)
			ifd.Sync()
		}
	}

	fs.mu.RLock()
	cfs := append([]*consumerFileStore(nil), fs.cfs...)
	fs.mu.RUnlock()

	// Do consumers.
	for _, o := range cfs {
		o.syncStateFile()
	}

	fs.mu.Lock()
	fs.syncTmr = time.AfterFunc(fs.fcfg.SyncInterval, fs.syncBlocks)
	fs.mu.Unlock()
}

// Select the message block where this message should be found.
// Return nil if not in the set.
// Read lock should be held.
func (fs *fileStore) selectMsgBlock(seq uint64) *msgBlock {
	// Check for out of range.
	if seq < fs.state.FirstSeq || seq > fs.state.LastSeq {
		return nil
	}
	// blks are sorted in ascending order.
	// TODO(dlc) - Can be smarter here, when lots of blks maybe use binary search.
	// For now this is cache friendly for small to medium numbers of blks.
	for _, mb := range fs.blks {
		if seq <= atomic.LoadUint64(&mb.last.seq) {
			return mb
		}
	}
	return nil
}

// Select the message block where this message should be found.
// Return nil if not in the set.
func (fs *fileStore) selectMsgBlockForStart(minTime time.Time) *msgBlock {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	t := minTime.UnixNano()
	for _, mb := range fs.blks {
		mb.mu.RLock()
		found := t <= mb.last.ts
		mb.mu.RUnlock()
		if found {
			return mb
		}
	}
	return nil
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
		mb.cache = &cache{}
	} else {
		fseq = mb.cache.fseq
		idx = mb.cache.idx
		if len(idx) == 0 {
			idx = make([]uint32, 0, mb.msgs)
		}
		index = uint32(len(mb.cache.buf))
		buf = append(mb.cache.buf, buf...)
	}

	lbuf := uint32(len(buf))

	for index < lbuf {
		if index+msgHdrSize >= lbuf {
			return errCorruptState
		}
		hdr := buf[index : index+msgHdrSize]
		rl := le.Uint32(hdr[0:])
		seq := le.Uint64(hdr[4:])
		slen := le.Uint16(hdr[20:])

		// Clear any headers bit that could be set.
		rl &^= hbit

		dlen := int(rl) - msgHdrSize

		// Do some quick sanity checks here.
		if dlen < 0 || int(slen) > dlen || dlen > int(rl) {
			// This means something is off.
			// TODO(dlc) - Add into bad list?
			return errCorruptState
		}
		// Clear erase bit.
		seq = seq &^ ebit
		// Adjust if we guessed wrong.
		if seq != 0 && seq < fseq {
			fseq = seq
		}
		// We defer checksum checks to individual msg cache lookups to amortorize costs and
		// not introduce latency for first message from a newly loaded block.
		idx = append(idx, index)
		mb.cache.lrl = uint32(rl)
		index += mb.cache.lrl
	}
	mb.cache.buf = buf
	mb.cache.idx = idx
	mb.cache.fseq = fseq
	mb.cache.wp += int(lbuf)

	return nil
}

func (mb *msgBlock) quitChan() chan struct{} {
	mb.mu.RLock()
	defer mb.mu.RUnlock()
	return mb.qch
}

// When called directly, flushPending could be busy already and return errFlushRunning.
// This function is called for in place flushing so we need to wait.
func (mb *msgBlock) flushPendingMsgsAndWait() error {
	var err error

	// If we are in flush wait for that to clear.
	for err = mb.flushPendingMsgs(); err == errFlushRunning; err = mb.flushPendingMsgs() {
		qch := mb.quitChan()
		select {
		case <-qch:
			return nil
		case <-time.After(time.Millisecond):
		}
	}
	return err
}

// flushPendingMsgs writes out any messages for this message block.
func (mb *msgBlock) flushPendingMsgs() error {
	// We will not hold the lock across I/O so we can add more messages
	// in parallel but we allow only one flush to be running.
	mb.mu.Lock()
	if mb.cache == nil || mb.mfd == nil {
		mb.mu.Unlock()
		return nil
	}

	// bytesPending will return with errFlushRunning
	// if we are already flushing this message block.
	buf, err := mb.bytesPending()
	// If we got an error back return here.
	if err != nil {
		mb.mu.Unlock()
		// No pending data to be written is not an error.
		if err == errNoPending {
			err = nil
		}
		return err
	}

	woff := int64(mb.cache.off + mb.cache.wp)
	lob := len(buf)

	// Only one can be flushing at a time.
	mb.setFlushing()
	mfd := mb.mfd
	mb.mu.Unlock()

	var n, tn int

	// Append new data to the message block file.
	for lbb := lob; lbb > 0; lbb = len(buf) {
		n, err = mfd.WriteAt(buf, woff)
		if err != nil {
			mb.removeIndexFile()
			mb.dirtyClose()
			if !isOutOfSpaceErr(err) {
				if ld, err := mb.rebuildState(); err != nil && ld != nil {
					// Rebuild fs state too.
					mb.fs.mu.Lock()
					mb.fs.rebuildState(ld)
					mb.fs.mu.Unlock()
				}
			}
			return err
		}

		woff += int64(n)
		tn += n

		// Success
		if n == lbb {
			break
		}
		// Partial write..
		buf = buf[n:]
	}

	// We did a successful write.
	// Re-acquire lock to update.
	mb.mu.Lock()
	defer mb.mu.Unlock()
	// Clear on exit.
	defer mb.clearFlushing()
	// set write err to any error.
	mb.werr = err

	// Cache may be gone.
	if mb.cache == nil || mb.mfd == nil {
		return mb.werr
	}

	// Check for additional writes while we were writing to the disk.
	moreBytes := len(mb.cache.buf) - mb.cache.wp - lob

	// Decide what we want to do with the buffer in hand. If we have load interest
	// we will hold onto the whole thing, otherwise empty the buffer, possibly reusing it.
	if ts := time.Now().UnixNano(); ts < mb.llts || (ts-mb.llts) <= int64(mb.cexp) {
		mb.cache.wp += tn
	} else {
		if cap(buf) <= maxBufReuse {
			buf = buf[:0]
		} else {
			buf = nil
		}
		if moreBytes > 0 {
			nbuf := mb.cache.buf[len(mb.cache.buf)-moreBytes:]
			if moreBytes > (len(mb.cache.buf)/4*3) && cap(nbuf) <= maxBufReuse {
				buf = nbuf
			} else {
				buf = append(buf, nbuf...)
			}
		}
		// Update our cache offset.
		mb.cache.off = int(woff)
		// Reset write pointer.
		mb.cache.wp = 0
		// Place buffer back in the cache structure.
		mb.cache.buf = buf
	}

	return mb.werr
}

//  Lock should be held.
func (mb *msgBlock) clearLoading() {
	mb.loading = false
}

// Will load msgs from disk.
func (mb *msgBlock) loadMsgs() error {
	// We hold the lock here the whole time by design.
	mb.mu.Lock()
	defer mb.mu.Unlock()

	// Check to see if we are loading already.
	if mb.loading {
		return nil
	}

	// Set loading status.
	mb.loading = true
	defer mb.clearLoading()

	var nchecks int

checkCache:
	nchecks++
	if nchecks > 8 {
		return errCorruptState
	}

	// Check to see if we have a full cache.
	if mb.cache != nil && len(mb.cache.idx) == int(mb.msgs) && mb.cache.off == 0 && len(mb.cache.buf) > 0 {
		return nil
	}

	mfn := mb.mfn
	mb.llts = time.Now().UnixNano()

	// FIXME(dlc) - We could be smarter here.
	if mb.cache != nil && len(mb.cache.buf)-mb.cache.wp > 0 {
		mb.mu.Unlock()
		err := mb.flushPendingMsgsAndWait()
		mb.mu.Lock()
		if err != nil && err != errFlushRunning {
			return err
		}
		goto checkCache
	}

	// Load in the whole block. We want to hold the mb lock here to avoid any changes to
	// state.
	buf, err := ioutil.ReadFile(mfn)
	if err != nil {
		return err
	}

	// Reset the cache since we just read everything in.
	// Make sure this is cleared in case we had a partial when we started.
	mb.clearCacheAndOffset()

	if err := mb.indexCacheBuf(buf); err != nil {
		if err == errCorruptState {
			fs := mb.fs
			mb.mu.Unlock()
			var ld *LostStreamData
			if ld, err = mb.rebuildState(); ld != nil {
				fs.mu.Lock()
				fs.rebuildState(ld)
				fs.mu.Unlock()
			}
			mb.mu.Lock()
		}
		if err != nil {
			return err
		}
		goto checkCache
	}

	if len(buf) > 0 {
		mb.cloads++
		mb.startCacheExpireTimer()
	}

	return nil
}

// Fetch a message from this block, possibly reading in and caching the messages.
// We assume the block was selected and is correct, so we do not do range checks.
func (mb *msgBlock) fetchMsg(seq uint64) (*fileStoredMsg, error) {
	var sm *fileStoredMsg

	sm, err := mb.cacheLookup(seq)
	if err == nil || (err != errNoCache && err != errPartialCache) {
		return sm, err
	}

	// We have a cache miss here.
	if err := mb.loadMsgs(); err != nil {
		return nil, err
	}

	return mb.cacheLookup(seq)
}

var (
	errNoCache      = errors.New("no message cache")
	errBadMsg       = errors.New("malformed or corrupt message")
	errDeletedMsg   = errors.New("deleted message")
	errPartialCache = errors.New("partial cache")
	errNoPending    = errors.New("message block does not have pending data")
	errNotReadable  = errors.New("storage directory not readable")
	errFlushRunning = errors.New("flush is already running")
	errCorruptState = errors.New("corrupt state file")
	errPendingData  = errors.New("pending data still present")
)

// Used for marking messages that have had their checksums checked.
// Used to signal a message record with headers.
const hbit = 1 << 31

// Used for marking erased messages sequences.
const ebit = 1 << 63

// Will do a lookup from the cache.
func (mb *msgBlock) cacheLookup(seq uint64) (*fileStoredMsg, error) {
	// Currently grab the write lock for optional use of mb.hh. Prefer this for now
	// vs read lock and promote. Also defer based on 1.14 performance.
	mb.mu.Lock()
	defer mb.mu.Unlock()
	return mb.cacheLookupWithLock(seq)
}

// Will do a lookup from cache assuming lock is held.
func (mb *msgBlock) cacheLookupWithLock(seq uint64) (*fileStoredMsg, error) {
	if mb.cache == nil || len(mb.cache.idx) == 0 {
		return nil, errNoCache
	}

	if seq < mb.first.seq || seq < mb.cache.fseq || seq > mb.last.seq {
		return nil, ErrStoreMsgNotFound
	}

	// If we have a delete map check it.
	if mb.dmap != nil {
		if _, ok := mb.dmap[seq]; ok {
			return nil, errDeletedMsg
		}
	}

	if mb.cache.off > 0 {
		return nil, errPartialCache
	}

	bi, _, hashChecked, err := mb.slotInfo(int(seq - mb.cache.fseq))
	if err != nil {
		return nil, errPartialCache
	}

	// Update cache activity.
	mb.llts = time.Now().UnixNano()
	mb.llseq = seq

	// We use the high bit to denote we have already checked the checksum.
	var hh hash.Hash64
	if !hashChecked {
		hh = mb.hh // This will force the hash check in msgFromBuf.
		mb.cache.idx[seq-mb.cache.fseq] = (bi | hbit)
	}

	li := int(bi) - mb.cache.off
	buf := mb.cache.buf[li:]

	// Parse from the raw buffer.
	subj, hdr, msg, mseq, ts, err := msgFromBuf(buf, hh)
	if err != nil {
		return nil, err
	}
	if seq != mseq {
		return nil, fmt.Errorf("sequence numbers for cache load did not match, %d vs %d", seq, mseq)
	}
	sm := &fileStoredMsg{
		subj: subj,
		hdr:  hdr,
		msg:  msg,
		seq:  seq,
		ts:   ts,
		mb:   mb,
		off:  int64(bi),
	}

	return sm, nil
}

// Will return message for the given sequence number.
func (fs *fileStore) msgForSeq(seq uint64) (*fileStoredMsg, error) {
	// TODO(dlc) - Since Store, Remove, Skip all hold the write lock on fs this will
	// be stalled. Need another lock if want to happen in parallel.
	fs.mu.RLock()
	if fs.closed {
		fs.mu.RUnlock()
		return nil, ErrStoreClosed
	}
	// Indicates we want first msg.
	if seq == 0 {
		seq = fs.state.FirstSeq
	}
	// Make sure to snapshot here.
	lseq := fs.state.LastSeq
	mb, lmb := fs.selectMsgBlock(seq), fs.lmb
	fs.mu.RUnlock()

	if mb == nil {
		var err = ErrStoreEOF
		if seq <= lseq {
			err = ErrStoreMsgNotFound
		}
		return nil, err
	}

	// Check to see if we are the last seq for this message block and are doing
	// a linear scan. If that is true and we are not the last message block we can
	// expire try to expire the cache.
	mb.mu.RLock()
	shouldTryExpire := mb != lmb && seq == mb.last.seq && mb.llseq == seq-1
	mb.mu.RUnlock()

	// TODO(dlc) - older design had a check to prefetch when we knew we were
	// loading in order and getting close to end of current mb. Should add
	// something like it back in.
	fsm, err := mb.fetchMsg(seq)
	if err != nil {
		return nil, err
	}

	// We detected a linear scan and access to the last message.
	if shouldTryExpire {
		mb.mu.Lock()
		mb.llts = 0
		mb.expireCacheLocked()
		mb.mu.Unlock()
	}

	return fsm, nil
}

// Internal function to return msg parts from a raw buffer.
func msgFromBuf(buf []byte, hh hash.Hash64) (string, []byte, []byte, uint64, int64, error) {
	if len(buf) < msgHdrSize {
		return _EMPTY_, nil, nil, 0, 0, errBadMsg
	}
	var le = binary.LittleEndian

	hdr := buf[:msgHdrSize]
	rl := le.Uint32(hdr[0:])
	hasHeaders := rl&hbit != 0
	rl &^= hbit // clear header bit
	dlen := int(rl) - msgHdrSize
	slen := int(le.Uint16(hdr[20:]))
	// Simple sanity check.
	if dlen < 0 || slen > dlen || int(rl) > len(buf) {
		return _EMPTY_, nil, nil, 0, 0, errBadMsg
	}
	data := buf[msgHdrSize : msgHdrSize+dlen]
	// Do checksum tests here if requested.
	if hh != nil {
		hh.Reset()
		hh.Write(hdr[4:20])
		hh.Write(data[:slen])
		if hasHeaders {
			hh.Write(data[slen+4 : dlen-8])
		} else {
			hh.Write(data[slen : dlen-8])
		}
		if !bytes.Equal(hh.Sum(nil), data[len(data)-8:]) {
			return _EMPTY_, nil, nil, 0, 0, errBadMsg
		}
	}
	seq := le.Uint64(hdr[4:])
	if seq&ebit != 0 {
		seq = 0
	}
	ts := int64(le.Uint64(hdr[12:]))
	// FIXME(dlc) - We need to not allow appends to the underlying buffer, so we will
	// fix the capacity. This will cause a copy though in stream:internalSendLoop when
	// we append CRLF but this was causing a race. Need to rethink more to avoid this copy.
	end := dlen - 8
	var mhdr, msg []byte
	if hasHeaders {
		hl := le.Uint32(data[slen:])
		bi := slen + 4
		li := bi + int(hl)
		mhdr = data[bi:li:li]
		msg = data[li:end:end]
	} else {
		msg = data[slen:end:end]
	}
	return string(data[:slen]), mhdr, msg, seq, ts, nil
}

// LoadMsg will lookup the message by sequence number and return it if found.
func (fs *fileStore) LoadMsg(seq uint64) (string, []byte, []byte, int64, error) {
	sm, err := fs.msgForSeq(seq)
	if sm != nil {
		return sm.subj, sm.hdr, sm.msg, sm.ts, nil
	}
	return "", nil, nil, 0, err
}

// Type returns the type of the underlying store.
func (fs *fileStore) Type() StorageType {
	return FileStorage
}

// FastState will fill in state with only the following.
// Msgs, Bytes, FirstSeq, LastSeq
func (fs *fileStore) FastState(state *StreamState) {
	fs.mu.RLock()
	state.Msgs = fs.state.Msgs
	state.Bytes = fs.state.Bytes
	state.FirstSeq = fs.state.FirstSeq
	state.LastSeq = fs.state.LastSeq
	fs.mu.RUnlock()
}

// State returns the current state of the stream.
func (fs *fileStore) State() StreamState {
	fs.mu.RLock()
	state := fs.state
	state.Consumers = len(fs.cfs)
	state.Deleted = nil // make sure.
	for _, mb := range fs.blks {
		mb.mu.Lock()
		fseq := mb.first.seq
		for seq := range mb.dmap {
			if seq <= fseq {
				delete(mb.dmap, seq)
			} else {
				state.Deleted = append(state.Deleted, seq)
			}
		}
		mb.mu.Unlock()
	}
	fs.mu.RUnlock()

	state.Lost = fs.lostData()

	// Can not be guaranteed to be sorted.
	if len(state.Deleted) > 0 {
		sort.Slice(state.Deleted, func(i, j int) bool {
			return state.Deleted[i] < state.Deleted[j]
		})
	}
	return state
}

const emptyRecordLen = 22 + 8

func fileStoreMsgSize(subj string, hdr, msg []byte) uint64 {
	if len(hdr) == 0 {
		// length of the message record (4bytes) + seq(8) + ts(8) + subj_len(2) + subj + msg + hash(8)
		return uint64(22 + len(subj) + len(msg) + 8)
	}
	// length of the message record (4bytes) + seq(8) + ts(8) + subj_len(2) + subj + hdr_len(4) + hdr + msg + hash(8)
	return uint64(22 + len(subj) + 4 + len(hdr) + len(msg) + 8)
}

func fileStoreMsgSizeEstimate(slen, maxPayload int) uint64 {
	return uint64(emptyRecordLen + slen + 4 + maxPayload)
}

// Write index info to the appropriate file.
func (mb *msgBlock) writeIndexInfo() error {
	// HEADER: magic version msgs bytes fseq fts lseq lts checksum
	var hdr [indexHdrSize]byte

	// Write header
	hdr[0] = magic
	hdr[1] = version

	mb.mu.Lock()
	defer mb.mu.Unlock()

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
			return err
		}
		mb.ifd = ifd
	}

	mb.lwits = time.Now().UnixNano()

	if n, err = mb.ifd.WriteAt(buf, 0); err == nil {
		mb.liwsz = int64(n)
		mb.werr = nil
	} else {
		mb.werr = err
	}

	return err
}

// readIndexInfo will read in the index information for the message block.
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
		return seq &^ ebit
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

	// Check if this is a short write index file.
	if bi < 0 || bi+checksumSize > len(buf) {
		defer os.Remove(mb.ifn)
		return fmt.Errorf("short index file")
	}

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
func (fs *fileStore) Purge() (uint64, error) {
	return fs.purge(0)
}

func (fs *fileStore) purge(fseq uint64) (uint64, error) {
	fs.mu.Lock()
	if fs.closed {
		fs.mu.Unlock()
		return 0, ErrStoreClosed
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
	if _, err := fs.newMsgBlockForWrite(); err != nil {
		fs.mu.Unlock()
		return purged, err
	}

	// Check if we need to set the first seq to a new number.
	if fseq > fs.state.FirstSeq {
		fs.state.FirstSeq = fseq
		fs.state.LastSeq = fseq - 1
	}
	fs.lmb.first.seq = fs.state.FirstSeq
	fs.lmb.last.seq = fs.state.LastSeq
	fs.lmb.writeIndexInfo()

	cb := fs.scb
	fs.mu.Unlock()

	if cb != nil {
		cb(-int64(purged), -rbytes, 0, _EMPTY_)
	}

	return purged, nil
}

// Compact will remove all messages from this store up to
// but not including the seq parameter.
// Will return the number of purged messages.
func (fs *fileStore) Compact(seq uint64) (uint64, error) {
	if seq == 0 || seq > fs.lastSeq() {
		return fs.purge(seq)
	}

	var purged uint64
	var bytes uint64

	// We have to delete interior messages.
	fs.mu.Lock()
	smb := fs.selectMsgBlock(seq)
	if smb == nil {
		fs.mu.Unlock()
		return 0, nil
	}
	// All msgblocks up to this one can be thrown away.
	for i, mb := range fs.blks {
		if mb == smb {
			fs.blks = append(fs.blks[:0:0], fs.blks[i:]...)
			break
		}
		mb.mu.Lock()
		purged += mb.msgs
		bytes += mb.bytes
		mb.dirtyCloseWithRemove(true)
		mb.mu.Unlock()
	}
	fs.mu.Unlock()

	if err := smb.loadMsgs(); err != nil {
		return purged, err
	}

	smb.mu.Lock()
	for mseq := smb.first.seq; mseq < seq; mseq++ {
		if _, rl, _, err := smb.slotInfo(int(mseq - smb.cache.fseq)); err != nil {
			smb.bytes -= uint64(rl)
		}
		smb.msgs--
		purged++
	}
	// Update first entry.
	sm, _ := smb.cacheLookupWithLock(seq)
	if sm != nil {
		smb.first.seq = sm.seq
		smb.first.ts = sm.ts
	}
	smb.mu.Unlock()

	if sm != nil {
		// Reset our version of first.
		fs.mu.Lock()
		fs.state.FirstSeq = sm.seq
		fs.state.FirstTime = time.Unix(0, sm.ts).UTC()
		fs.state.Msgs -= purged
		fs.state.Bytes -= bytes
		fs.mu.Unlock()
	}

	return purged, nil
}

// Truncate will truncate a stream store up to and including seq. Sequence needs to be valid.
func (fs *fileStore) Truncate(seq uint64) error {
	fs.mu.Lock()

	if fs.closed {
		fs.mu.Unlock()
		return ErrStoreClosed
	}
	if fs.sips > 0 {
		fs.mu.Unlock()
		return ErrStoreSnapshotInProgress
	}

	nlmb := fs.selectMsgBlock(seq)
	if nlmb == nil {
		fs.mu.Unlock()
		return ErrInvalidSequence
	}
	lsm, _ := nlmb.fetchMsg(seq)
	if lsm == nil {
		fs.mu.Unlock()
		return ErrInvalidSequence
	}

	// Set lmb to nlmb and make sure writeable.
	fs.lmb = nlmb
	fs.enableLastMsgBlockForWriting()

	var purged, bytes uint64

	// Truncate our new last message block.
	nmsgs, nbytes, err := nlmb.truncate(lsm)
	if err != nil {
		fs.mu.Unlock()
		return err
	}
	// Account for the truncated msgs and bytes.
	purged += nmsgs
	bytes += nbytes

	// Remove any left over msg blocks.
	getLastMsgBlock := func() *msgBlock { return fs.blks[len(fs.blks)-1] }
	for mb := getLastMsgBlock(); mb != nlmb; mb = getLastMsgBlock() {
		mb.mu.Lock()
		purged += mb.msgs
		bytes += mb.bytes
		fs.removeMsgBlock(mb)
		mb.mu.Unlock()
	}

	// Reset last.
	fs.state.LastSeq = lsm.seq
	fs.state.LastTime = time.Unix(0, lsm.ts).UTC()
	// Update msgs and bytes.
	fs.state.Msgs -= purged
	fs.state.Bytes -= bytes

	cb := fs.scb
	fs.mu.Unlock()

	if cb != nil {
		cb(-int64(purged), -int64(bytes), 0, _EMPTY_)
	}

	return nil
}

func (fs *fileStore) lastSeq() uint64 {
	fs.mu.RLock()
	seq := fs.state.LastSeq
	fs.mu.RUnlock()
	return seq
}

// Returns number of msg blks.
func (fs *fileStore) numMsgBlocks() int {
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	return len(fs.blks)
}

// Will remove our index file.
func (mb *msgBlock) removeIndexFile() {
	mb.mu.RLock()
	defer mb.mu.RUnlock()
	if mb.ifd != nil {
		mb.ifd.Close()
		mb.ifd = nil
	}
	if mb.ifn != _EMPTY_ {
		os.Remove(mb.ifn)
	}
}

// Removes the msgBlock
// Both locks should be held.
func (fs *fileStore) removeMsgBlock(mb *msgBlock) {
	mb.dirtyCloseWithRemove(true)

	// Remove from list.
	for i, omb := range fs.blks {
		if mb == omb {
			blks := append(fs.blks[:i], fs.blks[i+1:]...)
			fs.blks = append(blks[:0:0], blks...)
			break
		}
	}
	// Check for us being last message block
	if mb == fs.lmb {
		// Creating a new message write block requires that the lmb lock is not held.
		mb.mu.Unlock()
		fs.newMsgBlockForWrite()
		mb.mu.Lock()
	}
}

// Called by purge to simply get rid of the cache and close our fds.
// Lock should not be held.
func (mb *msgBlock) dirtyClose() {
	mb.mu.Lock()
	defer mb.mu.Unlock()
	mb.dirtyCloseWithRemove(false)
}

// Should be called with lock held.
func (mb *msgBlock) dirtyCloseWithRemove(remove bool) {
	if mb == nil {
		return
	}
	// Close cache
	mb.clearCacheAndOffset()
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
	if remove {
		if mb.ifn != _EMPTY_ {
			os.Remove(mb.ifn)
			mb.ifn = _EMPTY_
		}
		if mb.mfn != _EMPTY_ {
			os.Remove(mb.mfn)
			mb.mfn = _EMPTY_
		}
	}
}

func (mb *msgBlock) close(sync bool) {
	if mb == nil {
		return
	}
	mb.mu.Lock()
	defer mb.mu.Unlock()

	if mb.qch == nil {
		return
	}

	// Close cache
	mb.clearCacheAndOffset()
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

func (fs *fileStore) closeAllMsgBlocks(sync bool) {
	for _, mb := range fs.blks {
		mb.close(sync)
	}
}

func (fs *fileStore) Delete() error {
	if fs.isClosed() {
		return ErrStoreClosed
	}
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
	fs.lmb = nil

	fs.checkAndFlushAllBlocks()
	fs.closeAllMsgBlocks(false)

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

	return nil
}

const errFile = "errors.txt"

// Stream our snapshot through S2 compression and tar.
func (fs *fileStore) streamSnapshot(w io.WriteCloser, state *StreamState, includeConsumers bool) {
	defer w.Close()

	enc := s2.NewWriter(w)
	defer enc.Close()

	tw := tar.NewWriter(enc)
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
	blks := fs.blks
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

	// Can't use join path here, tar only recognizes relative paths with forward slashes.
	msgPre := msgDir + "/"

	// Now do messages themselves.
	for _, mb := range blks {
		if mb.pendingWriteSize() > 0 {
			mb.flushPendingMsgsAndWait()
			mb.writeIndexInfo()
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

		// We can have the running state directly encoded now.
		state, err := o.encodeState()
		if err != nil {
			o.mu.Unlock()
			writeErr(fmt.Sprintf("Could not encode consumer state for %q: %v", o.name, err))
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
func (fs *fileStore) Snapshot(deadline time.Duration, checkMsgs, includeConsumers bool) (*SnapshotResult, error) {
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
	fs.mu.Unlock()

	// We can add to our stream while snapshotting but not delete anything.
	state := fs.State()

	if checkMsgs {
		ld := fs.checkMsgs()
		if ld != nil && len(ld.Msgs) > 0 {
			return nil, fmt.Errorf("snapshot check detected %d bad messages", len(ld.Msgs))
		}
	}

	pr, pw := net.Pipe()

	// Set a write deadline here to protect ourselves.
	if deadline > 0 {
		pw.SetWriteDeadline(time.Now().Add(deadline))
	}
	// Stream in separate Go routine.
	go fs.streamSnapshot(pw, &state, includeConsumers)

	return &SnapshotResult{pr, state}, nil
}

// Helper to return the config.
func (fs *fileStore) fileStoreConfig() FileStoreConfig {
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	return fs.fcfg
}

////////////////////////////////////////////////////////////////////////////////
// Consumers
////////////////////////////////////////////////////////////////////////////////

type consumerFileStore struct {
	mu      sync.Mutex
	fs      *fileStore
	cfg     *FileConsumerInfo
	name    string
	odir    string
	ifn     string
	ifd     *os.File
	lwsz    int64
	hh      hash.Hash64
	state   ConsumerState
	fch     chan struct{}
	qch     chan struct{}
	flusher bool
	writing bool
	closed  bool
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

	// Create channels to control our flush go routine.
	o.fch = make(chan struct{}, 1)
	o.qch = make(chan struct{})
	go o.flushLoop()

	fs.mu.Lock()
	fs.cfs = append(fs.cfs, o)
	fs.mu.Unlock()

	return o, nil
}

// Kick flusher for this consumer.
// Lock should be held.
func (o *consumerFileStore) kickFlusher() {
	if o.fch != nil {
		select {
		case o.fch <- struct{}{}:
		default:
		}
	}
}

// Set in flusher status
func (o *consumerFileStore) setInFlusher() {
	o.mu.Lock()
	o.flusher = true
	o.mu.Unlock()
}

// Clear in flusher status
func (o *consumerFileStore) clearInFlusher() {
	o.mu.Lock()
	o.flusher = false
	o.mu.Unlock()
}

// Report in flusher status
func (o *consumerFileStore) inFlusher() bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.flusher
}

// flushLoop watches for consumer updates and the quit channel.
func (o *consumerFileStore) flushLoop() {
	o.mu.Lock()
	fch, qch := o.fch, o.qch
	o.mu.Unlock()

	o.setInFlusher()
	defer o.clearInFlusher()

	for {
		select {
		case <-fch:
			time.Sleep(10 * time.Millisecond)
			select {
			case <-qch:
				return
			default:
			}
			o.mu.Lock()
			if o.closed {
				o.mu.Unlock()
				return
			}
			buf, err := o.encodeState()
			o.mu.Unlock()
			if err != nil {
				return
			}
			// TODO(dlc) - if we error should start failing upwards.
			o.writeState(buf)
		case <-qch:
			return
		}
	}
}

// UpdateDelivered is called whenever a new message has been delivered.
func (o *consumerFileStore) UpdateDelivered(dseq, sseq, dc uint64, ts int64) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	if dc != 1 && o.cfg.AckPolicy == AckNone {
		return ErrNoAckPolicy
	}

	// See if we expect an ack for this.
	if o.cfg.AckPolicy != AckNone {
		// Need to create pending records here.
		if o.state.Pending == nil {
			o.state.Pending = make(map[uint64]*Pending)
		}
		var p *Pending
		// Check for an update to a message already delivered.
		if sseq <= o.state.Delivered.Stream {
			if p = o.state.Pending[sseq]; p != nil {
				p.Timestamp = ts
			}
		}
		if p == nil {
			// Move delivered if this is new.
			o.state.Delivered.Consumer = dseq
			o.state.Delivered.Stream = sseq
			p = &Pending{dseq, ts}
		}
		if dc > 1 {
			if o.state.Redelivered == nil {
				o.state.Redelivered = make(map[uint64]uint64)
			}
			o.state.Redelivered[sseq] = dc - 1
		}
		o.state.Pending[sseq] = &Pending{dseq, ts}
	} else {
		// For AckNone just update delivered and ackfloor at the same time.
		o.state.Delivered.Consumer = dseq
		o.state.Delivered.Stream = sseq
		o.state.AckFloor.Consumer = dseq
		o.state.AckFloor.Stream = sseq
	}
	// Make sure we flush to disk.
	o.kickFlusher()

	return nil
}

// UpdateAcks is called whenever a consumer with explicit ack or ack all acks a message.
func (o *consumerFileStore) UpdateAcks(dseq, sseq uint64) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.cfg.AckPolicy == AckNone {
		return ErrNoAckPolicy
	}
	if len(o.state.Pending) == 0 {
		return ErrStoreMsgNotFound
	}
	p := o.state.Pending[sseq]
	if p == nil {
		return ErrStoreMsgNotFound
	}
	// Delete from our state.
	delete(o.state.Pending, sseq)
	if len(o.state.Redelivered) > 0 {
		delete(o.state.Redelivered, sseq)
		if len(o.state.Redelivered) == 0 {
			o.state.Redelivered = nil
		}
	}

	if len(o.state.Pending) == 0 {
		o.state.Pending = nil
		o.state.AckFloor.Consumer = o.state.Delivered.Consumer
		o.state.AckFloor.Stream = o.state.Delivered.Stream
	} else if o.state.AckFloor.Consumer == dseq-1 {
		notFirst := o.state.AckFloor.Consumer != 0
		o.state.AckFloor.Consumer = dseq
		o.state.AckFloor.Stream = sseq
		// Close the gap if needed.
		if notFirst && o.state.Delivered.Consumer > dseq {
			for ss := sseq + 1; ss < o.state.Delivered.Stream; ss++ {
				if p, ok := o.state.Pending[ss]; ok {
					if p.Sequence > 0 {
						o.state.AckFloor.Consumer = p.Sequence - 1
						o.state.AckFloor.Stream = ss - 1
					}
					break
				}
			}
		}
	}
	o.kickFlusher()
	return nil
}

const seqsHdrSize = 6*binary.MaxVarintLen64 + hdrLen

// Encode our consumer state, version 2.
// Lock should be held.
func (o *consumerFileStore) encodeState() ([]byte, error) {
	if o.closed {
		return nil, ErrStoreClosed
	}
	return encodeConsumerState(&o.state), nil
}

func encodeConsumerState(state *ConsumerState) []byte {
	var hdr [seqsHdrSize]byte
	var buf []byte

	maxSize := seqsHdrSize
	if lp := len(state.Pending); lp > 0 {
		maxSize += lp*(3*binary.MaxVarintLen64) + binary.MaxVarintLen64
	}
	if lr := len(state.Redelivered); lr > 0 {
		maxSize += lr*(2*binary.MaxVarintLen64) + binary.MaxVarintLen64
	}
	if maxSize == seqsHdrSize {
		buf = hdr[:seqsHdrSize]
	} else {
		buf = make([]byte, maxSize)
	}

	// Write header
	buf[0] = magic
	buf[1] = 2

	n := hdrLen
	n += binary.PutUvarint(buf[n:], state.AckFloor.Consumer)
	n += binary.PutUvarint(buf[n:], state.AckFloor.Stream)
	n += binary.PutUvarint(buf[n:], state.Delivered.Consumer)
	n += binary.PutUvarint(buf[n:], state.Delivered.Stream)
	n += binary.PutUvarint(buf[n:], uint64(len(state.Pending)))

	asflr := state.AckFloor.Stream
	adflr := state.AckFloor.Consumer

	// These are optional, but always write len. This is to avoid a truncate inline.
	if len(state.Pending) > 0 {
		// To save space we will use now rounded to seconds to be base timestamp.
		mints := time.Now().Round(time.Second).Unix()
		// Write minimum timestamp we found from above.
		n += binary.PutVarint(buf[n:], mints)

		for k, v := range state.Pending {
			n += binary.PutUvarint(buf[n:], k-asflr)
			n += binary.PutUvarint(buf[n:], v.Sequence-adflr)
			// Downsample to seconds to save on space.
			// Subsecond resolution not needed for recovery etc.
			ts := v.Timestamp / 1_000_000_000
			n += binary.PutVarint(buf[n:], mints-ts)
		}
	}

	// We always write the redelivered len.
	n += binary.PutUvarint(buf[n:], uint64(len(state.Redelivered)))

	// We expect these to be small.
	if len(state.Redelivered) > 0 {
		for k, v := range state.Redelivered {
			n += binary.PutUvarint(buf[n:], k-asflr)
			n += binary.PutUvarint(buf[n:], v)
		}
	}

	return buf[:n]
}

func (o *consumerFileStore) Update(state *ConsumerState) error {
	// Sanity checks.
	if state.AckFloor.Consumer > state.Delivered.Consumer {
		return fmt.Errorf("bad ack floor for consumer")
	}
	if state.AckFloor.Stream > state.Delivered.Stream {
		return fmt.Errorf("bad ack floor for stream")
	}

	// Copy to our state.
	var pending map[uint64]*Pending
	var redelivered map[uint64]uint64
	if len(state.Pending) > 0 {
		pending = make(map[uint64]*Pending, len(state.Pending))
		for seq, p := range state.Pending {
			pending[seq] = &Pending{p.Sequence, p.Timestamp}
		}
		for seq := range pending {
			if seq <= state.AckFloor.Stream || seq > state.Delivered.Stream {
				return fmt.Errorf("bad pending entry, sequence [%d] out of range", seq)
			}
		}
	}
	if len(state.Redelivered) > 0 {
		redelivered = make(map[uint64]uint64, len(state.Redelivered))
		for seq, dc := range state.Redelivered {
			redelivered[seq] = dc
		}
	}

	// Replace our state.
	o.mu.Lock()
	o.state.Delivered = state.Delivered
	o.state.AckFloor = state.AckFloor
	o.state.Pending = pending
	o.state.Redelivered = redelivered
	o.mu.Unlock()

	o.kickFlusher()
	return nil
}

func (o *consumerFileStore) writeState(buf []byte) error {
	// Check if we have the index file open.
	o.mu.Lock()
	if o.writing || len(buf) == 0 {
		o.mu.Unlock()
		return nil
	}
	if err := o.ensureStateFileOpen(); err != nil {
		o.mu.Unlock()
		return err
	}
	o.writing = true
	ifd := o.ifd
	o.mu.Unlock()

	n, err := ifd.WriteAt(buf, 0)

	o.mu.Lock()
	if err == nil {
		o.lwsz = int64(n)
	}
	o.writing = false
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
	b, err := json.Marshal(cfs.cfg)
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

// Make sure the header is correct.
func checkHeader(hdr []byte) error {
	if hdr == nil || len(hdr) < 2 || hdr[0] != magic || hdr[1] != version {
		return errCorruptState
	}
	return nil
}

func checkConsumerHeader(hdr []byte) (uint8, error) {
	if hdr == nil || len(hdr) < 2 || hdr[0] != magic {
		return 0, errCorruptState
	}
	version := hdr[1]
	switch version {
	case 1, 2:
		return version, nil
	}
	return 0, fmt.Errorf("unsupported version: %d", version)
}

func (o *consumerFileStore) copyPending() map[uint64]*Pending {
	pending := make(map[uint64]*Pending, len(o.state.Pending))
	for seq, p := range o.state.Pending {
		pending[seq] = &Pending{p.Sequence, p.Timestamp}
	}
	return pending
}

func (o *consumerFileStore) copyRedelivered() map[uint64]uint64 {
	redelivered := make(map[uint64]uint64, len(o.state.Redelivered))
	for seq, dc := range o.state.Redelivered {
		redelivered[seq] = dc
	}
	return redelivered
}

// State retrieves the state from the state file.
// This is not expected to be called in high performance code, only on startup.
func (o *consumerFileStore) State() (*ConsumerState, error) {
	o.mu.Lock()
	defer o.mu.Unlock()

	var state *ConsumerState

	// See if we have a running state or if we need to read in from disk.
	if o.state.Delivered.Consumer != 0 {
		state = &ConsumerState{}
		state.Delivered = o.state.Delivered
		state.AckFloor = o.state.AckFloor
		if len(o.state.Pending) > 0 {
			state.Pending = o.copyPending()
		}
		if len(o.state.Redelivered) > 0 {
			state.Redelivered = o.copyRedelivered()
		}
		return state, nil
	}

	// Read the state in here from disk..
	buf, err := ioutil.ReadFile(o.ifn)
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}

	if len(buf) == 0 {
		return state, nil
	}

	state, err = decodeConsumerState(buf)
	if err != nil {
		return nil, err
	}

	// Copy this state into our own.
	o.state.Delivered = state.Delivered
	o.state.AckFloor = state.AckFloor
	if len(state.Pending) > 0 {
		o.state.Pending = make(map[uint64]*Pending, len(state.Pending))
		for seq, p := range state.Pending {
			o.state.Pending[seq] = &Pending{p.Sequence, p.Timestamp}
		}
	}
	if len(state.Redelivered) > 0 {
		o.state.Redelivered = make(map[uint64]uint64, len(state.Redelivered))
		for seq, dc := range state.Redelivered {
			o.state.Redelivered[seq] = dc
		}
	}

	return state, nil
}

func decodeConsumerState(buf []byte) (*ConsumerState, error) {
	version, err := checkConsumerHeader(buf)
	if err != nil {
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

	state := &ConsumerState{}
	state.AckFloor.Consumer = readSeq()
	state.AckFloor.Stream = readSeq()
	state.Delivered.Consumer = readSeq()
	state.Delivered.Stream = readSeq()

	if bi == -1 {
		return nil, errCorruptState
	}
	if version == 1 {
		// Adjust back. Version 1 also stored delivered as next to be delivered,
		// so adjust that back down here.
		state.Delivered.Consumer += state.AckFloor.Consumer - 1
		state.Delivered.Stream += state.AckFloor.Stream - 1
	}

	// We have additional stuff.
	if numPending := readLen(); numPending > 0 {
		mints := readTimeStamp()
		state.Pending = make(map[uint64]*Pending, numPending)
		for i := 0; i < int(numPending); i++ {
			sseq := readSeq()
			var dseq uint64
			if version == 2 {
				dseq = readSeq()
			}
			ts := readTimeStamp()
			if sseq == 0 || ts == -1 {
				return nil, errCorruptState
			}
			// Adjust seq back.
			sseq += state.AckFloor.Stream
			if version == 2 {
				dseq += state.AckFloor.Consumer
			}
			// Adjust the timestamp back.
			if version == 1 {
				ts = (ts + mints) * int64(time.Second)
			} else {
				ts = (mints - ts) * int64(time.Second)
			}
			// Store in pending.
			state.Pending[sseq] = &Pending{dseq, ts}
		}
	}

	// We have redelivered entries here.
	if numRedelivered := readLen(); numRedelivered > 0 {
		state.Redelivered = make(map[uint64]uint64, numRedelivered)
		for i := 0; i < int(numRedelivered); i++ {
			seq := readSeq()
			n := readCount()
			if seq == 0 || n == 0 {
				return nil, errCorruptState
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
	if o.qch != nil {
		close(o.qch)
		o.qch = nil
	}

	var err error
	if !o.writing {
		if err = o.ensureStateFileOpen(); err == nil {
			var buf []byte
			// Make sure to write this out..
			if buf, err = o.encodeState(); err == nil && len(buf) > 0 {
				_, err = o.ifd.WriteAt(buf, 0)
			}
		}
	}

	o.ifd, o.odir = nil, _EMPTY_
	fs, ifd := o.fs, o.ifd
	o.closed = true
	o.mu.Unlock()

	if ifd != nil {
		ifd.Close()
	}

	fs.removeConsumer(o)
	return err
}

// Delete the consumer.
func (o *consumerFileStore) Delete() error {
	o.mu.Lock()

	if o.closed {
		o.mu.Unlock()
		return nil
	}
	if o.qch != nil {
		close(o.qch)
		o.qch = nil
	}

	if o.ifd != nil {
		o.ifd.Close()
		o.ifd = nil
	}
	var err error
	if o.odir != _EMPTY_ {
		err = os.RemoveAll(o.odir)
	}
	o.ifd, o.odir = nil, _EMPTY_
	o.closed = true
	fs := o.fs
	o.mu.Unlock()

	fs.removeConsumer(o)
	return err
}

func (fs *fileStore) removeConsumer(cfs *consumerFileStore) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	for i, o := range fs.cfs {
		if o == cfs {
			fs.cfs = append(fs.cfs[:i], fs.cfs[i+1:]...)
			break
		}
	}
}

////////////////////////////////////////////////////////////////////////////////
// Templates
////////////////////////////////////////////////////////////////////////////////

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

func (ts *templateFileStore) Store(t *streamTemplate) error {
	dir := path.Join(ts.dir, t.Name)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("could not create templates storage directory for %q- %v", t.Name, err)
	}
	meta := path.Join(dir, JetStreamMetaFile)
	if _, err := os.Stat(meta); (err != nil && !os.IsNotExist(err)) || err == nil {
		return err
	}
	t.mu.Lock()
	b, err := json.Marshal(t)
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

func (ts *templateFileStore) Delete(t *streamTemplate) error {
	return os.RemoveAll(path.Join(ts.dir, t.Name))
}
