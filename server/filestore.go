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
	"encoding/hex"
	"encoding/json"
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
	obs     []*observableFileStore
	closed  bool
}

// Represents a message store block and its data.
type msgBlock struct {
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
	// Magic is used to identify the file store files.
	magic = uint8(22)
	// Version
	version = uint8(1)
	// hdrLen
	hdrLen = 2
	// This is where we keep the message store blocks.
	msgDir = "msgs"
	// used to scan blk file names.
	blkScan = "%d.blk"
	// used to scan index file names.
	indexScan = "%d.idx"
	// This is where we keep state on observers.
	obsDir = "obs"
	// Index file for observable
	obsState = "o.dat"
	// Maximum size of a write buffer we may consider for re-use.
	maxBufReuse = 2 * 1024 * 1024
	// Default stream block size.
	defaultStreamBlockSize = 128 * 1024 * 1024 // 128MB
	// Default for workqueue or interest based.
	defaultOtherBlockSize = 32 * 1024 * 1024 // 32MB
	// max block size for now.
	maxBlockSize = 2 * defaultStreamBlockSize
	// default cache expiration
	defaultCacheExpiration = 2 * time.Second
	// default sync interval
	defaultSyncInterval = 10 * time.Second
	// coalesceDelay
	coalesceDelay = 10 * time.Millisecond
	// coalesceMaximum
	coalesceMaximum = 64 * 1024
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

	// Create highway hash for message blocks. Use sha256 of directory as key.
	key := sha256.Sum256([]byte(mdir))
	fs.hh, err = highwayhash.New64(key[:])
	if err != nil {
		return nil, fmt.Errorf("could not create hash: %v", err)
	}

	// Recover our state.
	if err := fs.recoverMsgs(); err != nil {
		return nil, err
	}
	// Write our meta data iff new.
	if err := fs.writeMsgSetMeta(); err != nil {
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

// Write out meta and the checksum.
func (fs *fileStore) writeMsgSetMeta() error {
	meta := path.Join(fs.fcfg.StoreDir, JetStreamMetaFile)
	if _, err := os.Stat(meta); (err != nil && !os.IsNotExist(err)) || err == nil {
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

func (obs *observableFileStore) writeObservableMeta() error {
	meta := path.Join(obs.odir, JetStreamMetaFile)
	if _, err := os.Stat(meta); (err != nil && !os.IsNotExist(err)) || err == nil {
		return err
	}
	b, err := json.MarshalIndent(obs.cfg, _EMPTY_, "  ")
	if err != nil {
		return err
	}
	if err := ioutil.WriteFile(meta, b, 0644); err != nil {
		return err
	}
	obs.hh.Reset()
	obs.hh.Write(b)
	checksum := hex.EncodeToString(obs.hh.Sum(nil))
	sum := path.Join(obs.odir, JetStreamMetaFileSum)
	if err := ioutil.WriteFile(sum, []byte(checksum), 0644); err != nil {
		return err
	}
	return nil
}

const msgHdrSize = 22
const checksumSize = 8

// This is max room needed for index header.
const indexHdrSize = 7*binary.MaxVarintLen64 + hdrLen + checksumSize

func (fs *fileStore) recoverMsgBlock(fi os.FileInfo, index uint64) *msgBlock {
	var le = binary.LittleEndian

	mb := &msgBlock{index: index}

	mb.mfn = path.Join(fs.fcfg.StoreDir, msgDir, fi.Name())
	mb.ifn = path.Join(fs.fcfg.StoreDir, msgDir, fmt.Sprintf(indexScan, index))

	// Open up the message file, but we will try to recover from the index file.
	// We will check that the last checksufs match.
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
			fs.lmb = mb
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
	mdir := path.Join(fs.fcfg.StoreDir, msgDir)
	fis, err := ioutil.ReadDir(mdir)
	if err != nil {
		return fmt.Errorf("storage directory not readable")
	}
	// Recover all of the msg blocks.
	for _, fi := range fis {
		var index uint64
		if n, err := fmt.Sscanf(fi.Name(), blkScan, &index); err == nil && n == 1 {
			if mb := fs.recoverMsgBlock(fi, index); mb != nil {
				if fs.stats.FirstSeq == 0 {
					fs.stats.FirstSeq = mb.first.seq
				}
				if mb.last.seq > fs.stats.LastSeq {
					fs.stats.LastSeq = mb.last.seq
				}
				fs.stats.Msgs += mb.msgs
				fs.stats.Bytes += mb.bytes
			}
		}
	}

	// Limits checks and enforcement.
	fs.enforceMsgLimit()
	fs.enforceBytesLimit()
	// Do age checks to, make sure to call in place.
	if fs.cfg.MaxAge != 0 {
		fs.startAgeChk()
		fs.expireMsgs()
	}

	if len(fs.blks) == 0 {
		_, err = fs.newMsgBlockForWrite()
	}

	return err
}

// GetSeqFromTime looks for the first sequence number that has the message
// with >= timestamp.
func (ms *fileStore) GetSeqFromTime(t time.Time) uint64 {
	// TODO(dlc) - IMPL
	return 0
}

// StorageBytesUpdate registers an async callback for updates to storage changes.
func (fs *fileStore) StorageBytesUpdate(cb func(int64)) {
	fs.mu.Lock()
	fs.scb = cb
	bsz := fs.stats.Bytes
	fs.mu.Unlock()
	if cb != nil && bsz > 0 {
		cb(int64(bsz))
	}
}

// This rolls to a new append msg block.
func (fs *fileStore) newMsgBlockForWrite() (*msgBlock, error) {
	var index uint64

	if fs.lmb != nil {
		index = fs.lmb.index + 1
		fs.flushPendingWrites()
		fs.closeLastMsgBlock(false)
	} else {
		index = 1
	}

	mb := &msgBlock{index: index}
	fs.blks = append(fs.blks, mb)
	fs.lmb = mb

	mb.mfn = path.Join(fs.fcfg.StoreDir, msgDir, fmt.Sprintf(blkScan, mb.index))
	mfd, err := os.OpenFile(mb.mfn, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("Error creating msg block file [%q]: %v", mb.mfn, err)
	}
	mb.mfd = mfd

	mb.ifn = path.Join(fs.fcfg.StoreDir, msgDir, fmt.Sprintf(indexScan, mb.index))
	ifd, err := os.OpenFile(mb.ifn, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("Error creating msg index file [%q]: %v", mb.mfn, err)
	}
	mb.ifd = ifd

	return mb, nil
}

// Store stores a message.
func (fs *fileStore) StoreMsg(subj string, msg []byte) (uint64, error) {
	fs.mu.Lock()
	seq := fs.stats.LastSeq + 1

	if fs.stats.FirstSeq == 0 {
		fs.stats.FirstSeq = seq
	}

	n, err := fs.writeMsgRecord(seq, subj, msg)
	if err != nil {
		fs.mu.Unlock()
		return 0, err
	}
	fs.kickFlusher()

	fs.stats.Msgs++
	fs.stats.Bytes += n
	fs.stats.LastSeq = seq

	// Limits checks and enforcement.
	// If they do any deletions they will update the
	// byte count on their own, so no need to compensate.
	fs.enforceMsgLimit()
	fs.enforceBytesLimit()

	// Check it we have and need age expiration timer running.
	if fs.ageChk == nil && fs.cfg.MaxAge != 0 {
		fs.startAgeChk()
	}

	cb := fs.scb
	fs.mu.Unlock()

	if cb != nil {
		cb(int64(n))
	}

	return seq, nil
}

// Will check the msg limit and drop firstSeq msg if needed.
// Lock should be held.
func (fs *fileStore) enforceMsgLimit() {
	if fs.cfg.MaxMsgs <= 0 || fs.stats.Msgs <= uint64(fs.cfg.MaxMsgs) {
		return
	}
	fs.deleteFirstMsg()
}

// Will check the bytes limit and drop msgs if needed.
// Lock should be held.
func (fs *fileStore) enforceBytesLimit() {
	if fs.cfg.MaxBytes <= 0 || fs.stats.Bytes <= uint64(fs.cfg.MaxBytes) {
		return
	}
	for bs := fs.stats.Bytes; bs > uint64(fs.cfg.MaxBytes); bs = fs.stats.Bytes {
		fs.deleteFirstMsg()
	}
}

func (fs *fileStore) deleteFirstMsg() bool {
	return fs.removeMsg(fs.stats.FirstSeq, false)
}

// RemoveMsg will remove the message from this store.
// Will return the number of bytes removed.
func (fs *fileStore) RemoveMsg(seq uint64) bool {
	fs.mu.Lock()
	removed := fs.removeMsg(seq, false)
	fs.mu.Unlock()
	return removed
}

func (fs *fileStore) EraseMsg(seq uint64) bool {
	fs.mu.Lock()
	removed := fs.removeMsg(seq, true)
	fs.mu.Unlock()
	return removed
}

func (fs *fileStore) removeMsg(seq uint64, secure bool) bool {
	mb := fs.selectMsgBlock(seq)
	if mb == nil {
		return false
	}
	var sm *fileStoredMsg
	if mb.cache != nil {
		sm = mb.cache[seq]
	}
	if sm == nil {
		sm = fs.readAndCacheMsgs(mb, seq)
	}
	// We have the message here, so we can delete it.
	if sm != nil {
		fs.deleteMsgFromBlock(mb, seq, sm, secure)
	}
	return sm != nil
}

// Loop on requests to write out our index file. This is used when calling
// remove for a message. Updates to the last.seq etc are handled by main
// flush loop when storing messages.
func (fs *fileStore) flushWriteIndexLoop(mb *msgBlock, dch, qch chan struct{}) {
	for {
		select {
		case <-dch:
			fs.mu.Lock()
			mb.writeIndexInfo()
			fs.mu.Unlock()
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
func (fs *fileStore) deleteMsgFromBlock(mb *msgBlock, seq uint64, sm *fileStoredMsg, secure bool) {
	// Update global accounting.
	msz := fileStoreMsgSize(sm.subj, sm.msg)
	fs.stats.Msgs--
	fs.stats.Bytes -= msz

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
		if seq == fs.stats.FirstSeq {
			fs.stats.FirstSeq = mb.first.seq
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
			// Write first one in place.
			mb.writeIndexInfo()
		} else {
			mb.kickWriteFlusher()
		}
	}
	if fs.scb != nil {
		delta := int64(msz)
		fs.scb(-delta)
	}
}

// Lock should be held.
func (fs *fileStore) doExpireTimer(mb *msgBlock) {
	genid := mb.cgenid
	if mb.ctmr == nil {
		mb.ctmr = time.AfterFunc(fs.fcfg.ReadCacheExpire, func() { fs.expireCache(mb, genid) })
	} else {
		mb.ctmr.Reset(fs.fcfg.ReadCacheExpire)
	}
}

// Called to possibly expire a message block read cache.
func (fs *fileStore) expireCache(mb *msgBlock, genid uint64) {
	fs.mu.Lock()
	if genid == mb.cgenid {
		mb.cbytes = 0
		mb.cache = nil
	} else {
		genid := mb.cgenid
		mb.ctmr = time.AfterFunc(fs.fcfg.ReadCacheExpire, func() { fs.expireCache(mb, genid) })
	}
	fs.mu.Unlock()
}

func (fs *fileStore) startAgeChk() {
	if fs.ageChk == nil && fs.cfg.MaxAge != 0 {
		fs.ageChk = time.AfterFunc(fs.cfg.MaxAge, fs.expireMsgs)
	}
}

// Will expire msgs that are too old.
func (fs *fileStore) expireMsgs() {
	now := time.Now().UnixNano()
	minAge := now - int64(fs.cfg.MaxAge)

	for {
		if sm, _ := fs.msgForSeq(0); sm != nil && sm.ts <= minAge {
			fs.mu.Lock()
			fs.deleteFirstMsg()
			fs.mu.Unlock()
		} else {
			fs.mu.Lock()
			if sm == nil {
				if fs.ageChk != nil {
					fs.ageChk.Stop()
					fs.ageChk = nil
				}
			} else {
				fireIn := time.Duration(sm.ts-now) + fs.cfg.MaxAge
				fs.ageChk.Reset(fireIn)
			}
			fs.mu.Unlock()
			return
		}
	}
}

// Check all the checksufs for a message block.
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

// This will check all the checksufs on messages and report back any sequence numbers with errors.
func (fs *fileStore) checkMsgs() []uint64 {
	fs.mu.Lock()
	if fs.wmb.Len() > 0 {
		fs.flushPendingWrites()
	}
	fs.mu.Unlock()

	mdir := path.Join(fs.fcfg.StoreDir, msgDir)
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
				fs.bad = append(fs.bad, checkMsgBlockFile(fp, hh)...)
				fp.Close()
			}
		}
	}
	return fs.bad
}

// This will kick out our flush routine if its waiting.
func (fs *fileStore) kickFlusher() {
	select {
	case fs.fch <- struct{}{}:
	default:
	}
}

func (fs *fileStore) flushLoop(fch, qch chan struct{}) {
	for {
		select {
		case <-fch:
			fs.mu.Lock()
			waiting := fs.wmb.Len()
			if waiting < coalesceMaximum {
				fs.mu.Unlock()
				time.Sleep(coalesceDelay)
				fs.mu.Lock()
			}
			fs.flushPendingWrites()
			fs.mu.Unlock()
		case <-qch:
			return
		}
	}
}

// Lock should be held.
func (fs *fileStore) writeMsgRecord(seq uint64, subj string, msg []byte) (uint64, error) {
	var err error

	// Get size for this message.
	rl := fileStoreMsgSize(subj, msg)

	// Grab our current last message block.
	mb := fs.lmb
	if mb == nil || mb.bytes+rl > fs.fcfg.BlockSize {
		if mb, err = fs.newMsgBlockForWrite(); err != nil {
			return 0, err
		}
	}

	// Make sure we have room.
	fs.wmb.Grow(int(rl))

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
	fs.wmb.Write(hdr[:])
	fs.wmb.WriteString(subj)
	fs.wmb.Write(msg)

	// Calculate hash.
	fs.hh.Reset()
	fs.hh.Write(hdr[4:20])
	fs.hh.Write([]byte(subj))
	fs.hh.Write(msg)
	checksum := fs.hh.Sum(nil)
	// Write to msg record.
	fs.wmb.Write(checksum)
	// Grab last checksum
	copy(mb.lchk[0:], checksum)

	return uint64(rl), nil
}

// Will rewrite the message in the underlying store.
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
	fs.hh.Reset()
	fs.hh.Write(hdr[4:20])
	fs.hh.Write([]byte(sm.subj))
	fs.hh.Write(sm.msg)
	checksum := fs.hh.Sum(nil)
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
	fs.mu.Lock()
	if fs.closed {
		fs.mu.Unlock()
		return
	}
	for _, mb := range fs.blks {
		if mb.mfd != nil {
			mb.mfd.Sync()
		}
		if mb.ifd != nil {
			mb.ifd.Sync()
			mb.ifd.Truncate(mb.liwsz)
		}
	}
	var _obs [256]*observableFileStore
	obs := append(_obs[:0], fs.obs...)
	fs.syncTmr = time.AfterFunc(fs.fcfg.SyncInterval, fs.syncBlocks)
	fs.mu.Unlock()

	// Do observables.
	for _, o := range obs {
		o.syncStateFile()
	}
}

// Select the message block where this message should be found.
// Return nil if not in the set.
// Read lock should be held.
func (fs *fileStore) selectMsgBlock(seq uint64) *msgBlock {
	if seq < fs.stats.FirstSeq || seq > fs.stats.LastSeq {
		return nil
	}
	for _, mb := range fs.blks {
		if seq >= mb.first.seq && seq <= mb.last.seq {
			return mb
		}
	}
	return nil
}

// Read and cache message from the underlying block.
func (fs *fileStore) readAndCacheMsgs(mb *msgBlock, seq uint64) *fileStoredMsg {
	// This detects if what we may be looking for is staged in the write buffer.
	if mb == fs.lmb && fs.wmb.Len() > 0 {
		fs.flushPendingWrites()
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
			fs.bad = append(fs.bad, seq)
			index += int(rl)
			skip += int(rl)
			continue
		}
		index += msgHdrSize
		data := buf[index : index+dlen]
		// Check the checksum here.
		fs.hh.Reset()
		fs.hh.Write(hdr[4:20])
		fs.hh.Write(data[:slen])
		fs.hh.Write(data[slen : dlen-8])
		checksum := fs.hh.Sum(nil)
		if !bytes.Equal(checksum, data[len(data)-8:]) {
			index += dlen
			fs.bad = append(fs.bad, seq)
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
		fs.doExpireTimer(mb)
	}

	return sm
}

func (fs *fileStore) checkPrefetch(seq uint64, mb *msgBlock) {
	gap := mb.msgs/20 + 1
	if seq < mb.last.seq-gap {
		return
	}
	nseq := mb.last.seq + 1
	if nmb := fs.selectMsgBlock(nseq); nmb != nil && nmb != mb && nmb.cache == nil {
		nmb.cache = make(map[uint64]*fileStoredMsg)
		go func() {
			fs.mu.Lock()
			fs.readAndCacheMsgs(nmb, nseq)
			fs.mu.Unlock()
		}()
	}
}

// Will return message for the given sequence number.
func (fs *fileStore) msgForSeq(seq uint64) (*fileStoredMsg, error) {
	var err = ErrStoreEOF
	fs.mu.Lock()
	// seq == 0 indicates we want first msg.
	if seq == 0 {
		seq = fs.stats.FirstSeq
	}
	mb := fs.selectMsgBlock(seq)
	if mb == nil {
		if seq <= fs.stats.LastSeq {
			err = ErrStoreMsgNotFound
		}
		fs.mu.Unlock()
		return nil, err
	}

	// Check cache.
	if mb.cache != nil {
		if sm, ok := mb.cache[seq]; ok {
			mb.cgenid++
			fs.mu.Unlock()
			return sm, nil
		}
	}

	// Check for prefetch
	fs.checkPrefetch(seq, mb)

	// If we are here we do not have the message in our cache currently.
	sm := fs.readAndCacheMsgs(mb, seq)
	if sm != nil {
		mb.cgenid++
	} else if seq <= fs.stats.LastSeq {
		err = ErrStoreMsgNotFound
	}
	fs.mu.Unlock()
	return sm, err
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
func (fs *fileStore) LoadMsg(seq uint64) (string, []byte, int64, error) {
	sm, err := fs.msgForSeq(seq)
	if sm != nil {
		return sm.subj, sm.msg, sm.ts, nil
	}
	return "", nil, 0, err
}

func (fs *fileStore) Stats() MsgSetStats {
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	stats := fs.stats
	stats.Observables = len(fs.obs)
	return stats
}

func fileStoreMsgSize(subj string, msg []byte) uint64 {
	// length of the message record (4bytes) + seq(8) + ts(8) + subj_len(2) + subj + msg + hash(8)
	return uint64(4 + 16 + 2 + len(subj) + len(msg) + 8)
}

// Lock should be held.
func (fs *fileStore) flushPendingWrites() {
	mb := fs.lmb
	if mb == nil || mb.mfd == nil {
		return
	}

	// Append new data to the message block file.
	if lbb := fs.wmb.Len(); lbb > 0 {
		n, _ := fs.wmb.WriteTo(mb.mfd)
		if int(n) != lbb {
			fs.wmb.Truncate(int(n))
		} else if lbb <= maxBufReuse {
			fs.wmb.Reset()
		} else {
			fs.wmb = &bytes.Buffer{}
		}
	}

	// Now index info
	mb.writeIndexInfo()
}

// Write index info to the appropriate file.
func (mb *msgBlock) writeIndexInfo() error {
	// HEADER: magic version msgs bytes fseq fts lseq lts checksum
	var hdr [indexHdrSize]byte

	// Write header
	hdr[0] = magic
	hdr[1] = version

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

	n, err = mb.ifd.WriteAt(buf, 0)
	if err == nil {
		mb.liwsz = int64(n)
	}
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
	fs.mu.Lock()
	for _, mb := range fs.blks {
		tl += mb.cloads
	}
	fs.mu.Unlock()
	return tl
}

// Will return total number of cached bytes.
func (fs *fileStore) cacheSize() uint64 {
	var sz uint64
	fs.mu.Lock()
	for _, mb := range fs.blks {
		sz += mb.cbytes
	}
	fs.mu.Unlock()
	return sz
}

// Will return total number of dmapEntries for all msg blocks.
func (fs *fileStore) dmapEntries() int {
	var total int
	fs.mu.Lock()
	for _, mb := range fs.blks {
		total += len(mb.dmap)
	}
	fs.mu.Unlock()
	return total
}

// Purge will remove all messages from this store.
// Will return the number of purged messages.
func (fs *fileStore) Purge() uint64 {
	fs.mu.Lock()
	fs.flushPendingWrites()
	purged := fs.stats.Msgs
	cb := fs.scb
	bytes := int64(fs.stats.Bytes)
	fs.stats.FirstSeq = fs.stats.LastSeq + 1
	fs.stats.Bytes = 0
	fs.stats.Msgs = 0

	blks := fs.blks
	lmb := fs.lmb
	fs.blks = nil
	fs.lmb = nil

	for _, mb := range blks {
		fs.removeMsgBlock(mb)
	}
	// Now place new write msg block with correct info.
	fs.newMsgBlockForWrite()
	if lmb != nil {
		fs.lmb.first = lmb.last
		fs.lmb.first.seq += 1
		fs.lmb.last = lmb.last
		fs.lmb.writeIndexInfo()
	}
	fs.mu.Unlock()

	if cb != nil {
		cb(-bytes)
	}

	return purged
}

// Returns number of msg blks.
func (fs *fileStore) numMsgBlocks() int {
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	return len(fs.blks)
}

// Removes the msgBlock
// Lock should be held.
func (fs *fileStore) removeMsgBlock(mb *msgBlock) {
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

func (fs *fileStore) closeAllMsgBlocks(sync bool) {
	for _, mb := range fs.blks {
		mb.close(sync)
	}
}

func (fs *fileStore) closeLastMsgBlock(sync bool) {
	fs.lmb.close(sync)
}

func (fs *fileStore) Delete() {
	fs.Purge()
	fs.Stop()
	os.RemoveAll(fs.fcfg.StoreDir)
}

func (fs *fileStore) Stop() {
	fs.mu.Lock()
	if fs.closed {
		fs.mu.Unlock()
		return
	}
	fs.closed = true
	close(fs.qch)

	fs.flushPendingWrites()
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

	var _obs [256]*observableFileStore
	obs := append(_obs[:0], fs.obs...)
	fs.obs = nil
	fs.mu.Unlock()

	for _, o := range obs {
		o.Stop()
	}
}

////////////////////////////////////////////////////////////////////////////////
// Observable state
////////////////////////////////////////////////////////////////////////////////

type observableFileStore struct {
	mu     sync.Mutex
	fs     *fileStore
	cfg    *ObservableConfig
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

func (fs *fileStore) ObservableStore(name string, cfg *ObservableConfig) (ObservableStore, error) {
	if fs == nil {
		return nil, fmt.Errorf("filestore is nil")
	}
	if cfg == nil || name == "" {
		return nil, fmt.Errorf("bad observable config")
	}
	odir := path.Join(fs.fcfg.StoreDir, obsDir, name)
	if err := os.MkdirAll(odir, 0755); err != nil {
		return nil, fmt.Errorf("could not create observable  directory - %v", err)
	}
	o := &observableFileStore{
		fs:   fs,
		cfg:  cfg,
		name: name,
		odir: odir,
		ifn:  path.Join(odir, obsState),
		fch:  make(chan struct{}),
		qch:  make(chan struct{}),
	}
	key := sha256.Sum256([]byte(odir))
	hh, err := highwayhash.New64(key[:])
	if err != nil {
		return nil, fmt.Errorf("could not create hash: %v", err)
	}
	o.hh = hh

	if err := o.writeObservableMeta(); err != nil {
		return nil, err
	}

	fs.mu.Lock()
	fs.obs = append(fs.obs, o)
	fs.mu.Unlock()

	return o, nil
}

const seqsHdrSize = 6*binary.MaxVarintLen64 + hdrLen

func (o *observableFileStore) Update(state *ObservableState) error {
	// Sanity checks.
	if state.Delivered.ObsSeq < 1 || state.Delivered.SetSeq < 1 {
		return fmt.Errorf("bad delivered sequences")
	}
	if state.AckFloor.ObsSeq > state.Delivered.ObsSeq {
		return fmt.Errorf("bad ack floor for observable")
	}
	if state.AckFloor.SetSeq > state.Delivered.SetSeq {
		return fmt.Errorf("bad ack floor for set")
	}

	var hdr [seqsHdrSize]byte

	// Write header
	hdr[0] = magic
	hdr[1] = version

	n := hdrLen
	n += binary.PutUvarint(hdr[n:], state.AckFloor.ObsSeq)
	n += binary.PutUvarint(hdr[n:], state.AckFloor.SetSeq)
	n += binary.PutUvarint(hdr[n:], state.Delivered.ObsSeq-state.AckFloor.ObsSeq)
	n += binary.PutUvarint(hdr[n:], state.Delivered.SetSeq-state.AckFloor.SetSeq)
	n += binary.PutUvarint(hdr[n:], uint64(len(state.Pending)))
	buf := hdr[:n]

	// These are optional, but always write len. This is to avoid truncate inline.
	// If these get big might make more sense to do writes directly to the file.

	if len(state.Pending) > 0 {
		mbuf := make([]byte, len(state.Pending)*(2*binary.MaxVarintLen64)+binary.MaxVarintLen64)
		aflr := state.AckFloor.SetSeq
		maxd := state.Delivered.SetSeq

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
	n = binary.PutUvarint(lenbuf[0:], uint64(len(state.Redelivery)))
	buf = append(buf, lenbuf[:n]...)

	// We expect these to be small so will not do anything too crazy here to
	// keep the size small. Trick could be to offset sequence like above, but
	// we would need to know low sequence number for redelivery, can't depend on ackfloor etc.
	if len(state.Redelivery) > 0 {
		mbuf := make([]byte, len(state.Redelivery)*(2*binary.MaxVarintLen64))
		var n int
		for k, v := range state.Redelivery {
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

func (o *observableFileStore) syncStateFile() {
	// FIXME(dlc) - Hold last error?
	o.mu.Lock()
	if o.ifd != nil {
		o.ifd.Sync()
		o.ifd.Truncate(o.lwsz)
	}
	o.mu.Unlock()
}

// Lock should be held.
func (o *observableFileStore) ensureStateFileOpen() error {
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
func (o *observableFileStore) State() (*ObservableState, error) {
	o.mu.Lock()
	defer o.mu.Unlock()

	buf, err := ioutil.ReadFile(o.ifn)
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}

	var state *ObservableState

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

	state = &ObservableState{}
	state.AckFloor.ObsSeq = readSeq()
	state.AckFloor.SetSeq = readSeq()
	state.Delivered.ObsSeq = readSeq()
	state.Delivered.SetSeq = readSeq()

	if bi == -1 {
		return nil, fmt.Errorf("corrupt state file")
	}
	// Adjust back.
	state.Delivered.ObsSeq += state.AckFloor.ObsSeq
	state.Delivered.SetSeq += state.AckFloor.SetSeq

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
			seq += state.AckFloor.SetSeq
			// Adjust the timestamp back.
			ts = (ts + mints) * int64(time.Second)
			// Store in pending.
			state.Pending[seq] = ts
		}
	}

	numRedelivered := readLen()

	// We have redelivery entries here.
	if numRedelivered > 0 {
		state.Redelivery = make(map[uint64]uint64, numRedelivered)
		for i := 0; i < int(numRedelivered); i++ {
			seq := readSeq()
			n := readCount()
			if seq == 0 || n == 0 {
				return nil, fmt.Errorf("corrupt state file")
			}
			state.Redelivery[seq] = n
		}
	}
	return state, nil
}

// Stop the processing of the observable's state.
func (o *observableFileStore) Stop() {
	o.mu.Lock()
	if o.closed {
		o.mu.Unlock()
		return
	}
	o.closed = true
	if o.ifd != nil {
		o.ifd.Sync()
		o.ifd.Close()
		o.ifd = nil
	}
	fs := o.fs
	o.mu.Unlock()
	fs.removeObs(o)
}

// Delete the observable.
func (o *observableFileStore) Delete() {
	// Call stop first. OK if already stopped.
	o.Stop()
	o.mu.Lock()
	if o.odir != "" {
		os.RemoveAll(o.odir)
	}
	o.mu.Unlock()
}

func (fs *fileStore) removeObs(obs *observableFileStore) {
	fs.mu.Lock()
	for i, o := range fs.obs {
		if o == obs {
			fs.obs = append(fs.obs[:i], fs.obs[i+1:]...)
			break
		}
	}
	fs.mu.Unlock()
}
