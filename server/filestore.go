// Copyright 2019-2022 The NATS Authors
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
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"io/ioutil"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	mrand "math/rand"

	"github.com/klauspost/compress/s2"
	"github.com/minio/highwayhash"
	"golang.org/x/crypto/chacha20"
	"golang.org/x/crypto/chacha20poly1305"
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

// Default file and directory permissions.
const (
	defaultDirPerms  = os.FileMode(0750)
	defaultFilePerms = os.FileMode(0640)
)

type fileStore struct {
	mu      sync.RWMutex
	state   StreamState
	ld      *LostStreamData
	scb     StorageUpdateHandler
	ageChk  *time.Timer
	syncTmr *time.Timer
	cfg     FileStreamInfo
	fcfg    FileStoreConfig
	prf     keyGen
	aek     cipher.AEAD
	lmb     *msgBlock
	blks    []*msgBlock
	psmc    map[string]uint64
	hh      hash.Hash64
	qch     chan struct{}
	cfs     []*consumerFileStore
	sips    int
	closed  bool
	fip     bool
	tms     bool
}

// Represents a message store block and its data.
type msgBlock struct {
	// Here for 32bit systems and atomic.
	first   msgId
	last    msgId
	mu      sync.RWMutex
	fs      *fileStore
	aek     cipher.AEAD
	bek     *chacha20.Cipher
	seed    []byte
	nonce   []byte
	mfn     string
	mfd     *os.File
	ifn     string
	ifd     *os.File
	liwsz   int64
	index   uint64
	bytes   uint64 // User visible bytes count.
	rbytes  uint64 // Total bytes (raw) including deleted. Used for rolling to new blk.
	msgs    uint64 // User visible message count.
	fss     map[string]*SimpleState
	sfn     string
	kfn     string
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
	closed  bool
}

// Write through caching layer that is also used on loading messages.
type cache struct {
	buf  []byte
	off  int
	wp   int
	idx  []uint32
	lrl  uint32
	fseq uint64
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
	// used for compacted blocks that are staged.
	newScan = "%d.new"
	// used to scan index file names.
	indexScan = "%d.idx"
	// used to load per subject meta information.
	fssScan = "%d.fss"
	// to look for orphans
	fssScanAll = "*.fss"
	// used to store our block encryption key.
	keyScan = "%d.key"
	// to look for orphans
	keyScanAll = "*.key"
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
	// default idle timeout to close FDs.
	closeFDsIdle = 30 * time.Second
	// coalesceMinimum
	coalesceMinimum = 16 * 1024
	// maxFlushWait is maximum we will wait to gather messages to flush.
	maxFlushWait = 8 * time.Millisecond

	// Metafiles for streams and consumers.
	JetStreamMetaFile    = "meta.inf"
	JetStreamMetaFileSum = "meta.sum"
	JetStreamMetaFileKey = "meta.key"

	// AEK key sizes
	metaKeySize = 72
	blkKeySize  = 72

	// Default stream block size.
	defaultStreamBlockSize = 16 * 1024 * 1024 // 16MB
	// Default for workqueue or interest based.
	defaultOtherBlockSize = 8 * 1024 * 1024 // 8MB
	// Default for KV based
	defaultKVBlockSize = 8 * 1024 * 1024 // 8MB
	// max block size for now.
	maxBlockSize = defaultStreamBlockSize
	// Compact minimum threshold.
	compactMinimum = 2 * 1024 * 1024 // 2MB
	// FileStoreMinBlkSize is minimum size we will do for a blk size.
	FileStoreMinBlkSize = 32 * 1000 // 32kib
	// FileStoreMaxBlkSize is maximum size we will do for a blk size.
	FileStoreMaxBlkSize = maxBlockSize

	// Check for bad record length value due to corrupt data.
	rlBadThresh = 32 * 1024 * 1024
)

func newFileStore(fcfg FileStoreConfig, cfg StreamConfig) (*fileStore, error) {
	return newFileStoreWithCreated(fcfg, cfg, time.Now().UTC(), nil)
}

func newFileStoreWithCreated(fcfg FileStoreConfig, cfg StreamConfig, created time.Time, prf keyGen) (*fileStore, error) {
	if cfg.Name == _EMPTY_ {
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
		if err := os.MkdirAll(fcfg.StoreDir, defaultDirPerms); err != nil {
			return nil, fmt.Errorf("could not create storage directory - %v", err)
		}
	} else if stat == nil || !stat.IsDir() {
		return nil, fmt.Errorf("storage directory is not a directory")
	}
	tmpfile, err := ioutil.TempFile(fcfg.StoreDir, "_test_")
	if err != nil {
		return nil, fmt.Errorf("storage directory is not writable")
	}
	tmpfile.Close()
	os.Remove(tmpfile.Name())

	fs := &fileStore{
		fcfg: fcfg,
		cfg:  FileStreamInfo{Created: created, StreamConfig: cfg},
		psmc: make(map[string]uint64),
		prf:  prf,
		qch:  make(chan struct{}),
	}

	// Set flush in place to AsyncFlush which by default is false.
	fs.fip = !fcfg.AsyncFlush

	// Check if this is a new setup.
	mdir := filepath.Join(fcfg.StoreDir, msgDir)
	odir := filepath.Join(fcfg.StoreDir, consumerDir)
	if err := os.MkdirAll(mdir, defaultDirPerms); err != nil {
		return nil, fmt.Errorf("could not create message storage directory - %v", err)
	}
	if err := os.MkdirAll(odir, defaultDirPerms); err != nil {
		return nil, fmt.Errorf("could not create consumer storage directory - %v", err)
	}

	// Create highway hash for message blocks. Use sha256 of directory as key.
	key := sha256.Sum256([]byte(cfg.Name))
	fs.hh, err = highwayhash.New64(key[:])
	if err != nil {
		return nil, fmt.Errorf("could not create hash: %v", err)
	}

	// Always track per subject information.
	fs.tms = true

	// Recover our message state.
	if err := fs.recoverMsgs(); err != nil {
		return nil, err
	}

	// Write our meta data iff does not exist.
	meta := filepath.Join(fcfg.StoreDir, JetStreamMetaFile)
	if _, err := os.Stat(meta); err != nil && os.IsNotExist(err) {
		if err := fs.writeStreamMeta(); err != nil {
			return nil, err
		}
	}

	// If we expect to be encrypted check that what we are restoring is not plaintext.
	// This can happen on snapshot restores or conversions.
	if fs.prf != nil {
		keyFile := filepath.Join(fs.fcfg.StoreDir, JetStreamMetaFileKey)
		if _, err := os.Stat(keyFile); err != nil && os.IsNotExist(err) {
			if err := fs.writeStreamMeta(); err != nil {
				return nil, err
			}
		}
	}

	fs.syncTmr = time.AfterFunc(fs.fcfg.SyncInterval, fs.syncBlocks)

	return fs, nil
}

func (fs *fileStore) UpdateConfig(cfg *StreamConfig) error {
	if fs.isClosed() {
		return ErrStoreClosed
	}
	if cfg.Name == _EMPTY_ {
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

// Generate an asset encryption key from the context and server PRF.
func (fs *fileStore) genEncryptionKeys(context string) (aek cipher.AEAD, bek *chacha20.Cipher, seed, encrypted []byte, err error) {
	if fs.prf == nil {
		return nil, nil, nil, nil, errNoEncryption
	}
	// Generate key encryption key.
	rb, err := fs.prf([]byte(context))
	if err != nil {
		return nil, nil, nil, nil, err
	}
	kek, err := chacha20poly1305.NewX(rb)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	// Generate random asset encryption key seed.
	seed = make([]byte, 32)
	if n, err := rand.Read(seed); err != nil || n != 32 {
		return nil, nil, nil, nil, err
	}
	aek, err = chacha20poly1305.NewX(seed)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	// Generate our nonce. Use same buffer to hold encrypted seed.
	nonce := make([]byte, kek.NonceSize(), kek.NonceSize()+len(seed)+kek.Overhead())
	mrand.Read(nonce)
	bek, err = chacha20.NewUnauthenticatedCipher(seed[:], nonce)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	return aek, bek, seed, kek.Seal(nonce, nonce, seed, nil), nil
}

// Write out meta and the checksum.
// Lock should be held.
func (fs *fileStore) writeStreamMeta() error {
	if fs.prf != nil && fs.aek == nil {
		key, _, _, encrypted, err := fs.genEncryptionKeys(fs.cfg.Name)
		if err != nil {
			return err
		}
		fs.aek = key
		keyFile := filepath.Join(fs.fcfg.StoreDir, JetStreamMetaFileKey)
		if _, err := os.Stat(keyFile); err != nil && !os.IsNotExist(err) {
			return err
		}
		if err := ioutil.WriteFile(keyFile, encrypted, defaultFilePerms); err != nil {
			return err
		}
	}

	meta := filepath.Join(fs.fcfg.StoreDir, JetStreamMetaFile)
	if _, err := os.Stat(meta); err != nil && !os.IsNotExist(err) {
		return err
	}
	b, err := json.Marshal(fs.cfg)
	if err != nil {
		return err
	}
	// Encrypt if needed.
	if fs.aek != nil {
		nonce := make([]byte, fs.aek.NonceSize(), fs.aek.NonceSize()+len(b)+fs.aek.Overhead())
		mrand.Read(nonce)
		b = fs.aek.Seal(nonce, nonce, b, nil)
	}

	if err := ioutil.WriteFile(meta, b, defaultFilePerms); err != nil {
		return err
	}
	fs.hh.Reset()
	fs.hh.Write(b)
	checksum := hex.EncodeToString(fs.hh.Sum(nil))
	sum := filepath.Join(fs.fcfg.StoreDir, JetStreamMetaFileSum)
	if err := ioutil.WriteFile(sum, []byte(checksum), defaultFilePerms); err != nil {
		return err
	}
	return nil
}

const (
	msgHdrSize     = 22
	checksumSize   = 8
	emptyRecordLen = msgHdrSize + checksumSize
)

// This is the max room needed for index header.
const indexHdrSize = 7*binary.MaxVarintLen64 + hdrLen + checksumSize

func (fs *fileStore) recoverMsgBlock(fi os.FileInfo, index uint64) (*msgBlock, error) {
	mb := &msgBlock{fs: fs, index: index, cexp: fs.fcfg.CacheExpire}

	mdir := filepath.Join(fs.fcfg.StoreDir, msgDir)
	mb.mfn = filepath.Join(mdir, fi.Name())
	mb.ifn = filepath.Join(mdir, fmt.Sprintf(indexScan, index))
	mb.sfn = filepath.Join(mdir, fmt.Sprintf(fssScan, index))

	if mb.hh == nil {
		key := sha256.Sum256(fs.hashKeyForBlock(index))
		mb.hh, _ = highwayhash.New64(key[:])
	}

	var createdKeys bool

	// Check if encryption is enabled.
	if fs.prf != nil {
		ekey, err := ioutil.ReadFile(filepath.Join(mdir, fmt.Sprintf(keyScan, mb.index)))
		if err != nil {
			// We do not seem to have keys even though we should. Could be a plaintext conversion.
			// Create the keys and we will double check below.
			if err := fs.genEncryptionKeysForBlock(mb); err != nil {
				return nil, err
			}
			createdKeys = true
		} else {
			if len(ekey) != blkKeySize {
				return nil, errBadKeySize
			}
			// Recover key encryption key.
			rb, err := fs.prf([]byte(fmt.Sprintf("%s:%d", fs.cfg.Name, mb.index)))
			if err != nil {
				return nil, err
			}
			kek, err := chacha20poly1305.NewX(rb)
			if err != nil {
				return nil, err
			}
			ns := kek.NonceSize()
			seed, err := kek.Open(nil, ekey[:ns], ekey[ns:], nil)
			if err != nil {
				return nil, err
			}
			mb.seed, mb.nonce = seed, ekey[:ns]
			if mb.aek, err = chacha20poly1305.NewX(seed); err != nil {
				return nil, err
			}
			if mb.bek, err = chacha20.NewUnauthenticatedCipher(seed, ekey[:ns]); err != nil {
				return nil, err
			}
		}
	}

	// If we created keys here, let's check the data and if it is plaintext convert here.
	if createdKeys {
		buf, err := mb.loadBlock(nil)
		if err != nil {
			return nil, err
		}
		if err := mb.indexCacheBuf(buf); err != nil {
			// This likely indicates this was already encrypted or corrupt.
			mb.cache = nil
			return nil, err
		}
		// Undo cache from above for later.
		mb.cache = nil
		wbek, err := chacha20.NewUnauthenticatedCipher(mb.seed, mb.nonce)
		if err != nil {
			return nil, err
		}
		wbek.XORKeyStream(buf, buf)
		if err := ioutil.WriteFile(mb.mfn, buf, defaultFilePerms); err != nil {
			return nil, err
		}
		if buf, err = ioutil.ReadFile(mb.ifn); err == nil && len(buf) > 0 {
			if err := checkHeader(buf); err != nil {
				return nil, err
			}
			buf = mb.aek.Seal(buf[:0], mb.nonce, buf, nil)
			if err := ioutil.WriteFile(mb.ifn, buf, defaultFilePerms); err != nil {
				return nil, err
			}
		}
	}

	// Open up the message file, but we will try to recover from the index file.
	// We will check that the last checksums match.
	file, err := os.Open(mb.mfn)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	if fi, err := file.Stat(); fi != nil {
		mb.rbytes = uint64(fi.Size())
	} else {
		return nil, err
	}
	// Grab last checksum from main block file.
	var lchk [8]byte
	file.ReadAt(lchk[:], fi.Size()-8)
	file.Close()

	// Read our index file. Use this as source of truth if possible.
	if err := mb.readIndexInfo(); err == nil {
		// Quick sanity check here.
		// Note this only checks that the message blk file is not newer then this file.
		if bytes.Equal(lchk[:], mb.lchk[:]) {
			if fs.tms {
				if err = mb.readPerSubjectInfo(); err != nil {
					return nil, err
				}
			}
			fs.blks = append(fs.blks, mb)
			return mb, nil
		}
	}

	// If we get data loss rebuilding the message block state record that with the fs itself.
	if ld, _ := mb.rebuildState(); ld != nil {
		fs.rebuildStateLocked(ld)
	}

	// Rewrite this to make sure we are sync'd.
	mb.writeIndexInfo()
	mb.closeFDs()
	fs.blks = append(fs.blks, mb)
	fs.lmb = mb
	return mb, nil
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
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs.rebuildStateLocked(ld)
}

// Lock should be held.
func (fs *fileStore) rebuildStateLocked(ld *LostStreamData) {
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
	return mb.rebuildStateLocked()
}

func (mb *msgBlock) rebuildStateLocked() (*LostStreamData, error) {
	startLastSeq := mb.last.seq

	// Clear state we need to rebuild.
	mb.msgs, mb.bytes, mb.rbytes, mb.fss = 0, 0, 0, nil
	mb.last.seq, mb.last.ts = 0, 0
	firstNeedsSet := true

	buf, err := mb.loadBlock(nil)
	if err != nil {
		return nil, err
	}

	// Check if we need to decrypt.
	if mb.bek != nil && len(buf) > 0 {
		// Recreate to reset counter.
		mb.bek, err = chacha20.NewUnauthenticatedCipher(mb.seed, mb.nonce)
		if err != nil {
			return nil, err
		}
		mb.bek.XORKeyStream(buf, buf)
	}

	mb.rbytes = uint64(len(buf))

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
			fd, err = os.OpenFile(mb.mfn, os.O_RDWR, defaultFilePerms)
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

	// Rebuild per subject info.
	if mb.fs.tms {
		mb.fss = make(map[string]*SimpleState)
	}

	for index, lbuf := uint32(0), uint32(len(buf)); index < lbuf; {
		if index+msgHdrSize > lbuf {
			truncate(index)
			return gatherLost(lbuf - index), nil
		}

		hdr := buf[index : index+msgHdrSize]
		rl, slen := le.Uint32(hdr[0:]), le.Uint16(hdr[20:])

		hasHeaders := rl&hbit != 0
		// Clear any headers bit that could be set.
		rl &^= hbit
		dlen := int(rl) - msgHdrSize
		// Do some quick sanity checks here.
		if dlen < 0 || int(slen) > dlen || dlen > int(rl) || rl > rlBadThresh {
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
			mb.last.seq = seq
			mb.last.ts = ts
			continue
		}

		// This is for when we have index info that adjusts for deleted messages
		// at the head. So the first.seq will be already set here. If this is larger
		// replace what we have with this seq.
		if firstNeedsSet && seq > mb.first.seq {
			firstNeedsSet, mb.first.seq, mb.first.ts = false, seq, ts
		}

		var deleted bool
		if mb.dmap != nil {
			_, deleted = mb.dmap[seq]
		}

		// Always set last.
		mb.last.seq = seq
		mb.last.ts = ts

		if !deleted {
			data := buf[index+msgHdrSize : index+rl]
			if hh := mb.hh; hh != nil {
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
				firstNeedsSet, mb.first.seq, mb.first.ts = false, seq, ts
			}

			mb.msgs++
			mb.bytes += uint64(rl)

			// Do per subject info.
			if mb.fss != nil {
				if subj := string(data[:slen]); len(subj) > 0 {
					if ss := mb.fss[subj]; ss != nil {
						ss.Msgs++
						ss.Last = seq
					} else {
						mb.fss[subj] = &SimpleState{Msgs: 1, First: seq, Last: seq}
					}
				}
			}
		}
		// Advance to next record.
		index += rl
	}

	// For empty msg blocks make sure we recover last seq correctly based off of first.
	if mb.msgs == 0 && mb.first.seq > 0 {
		mb.last.seq = mb.first.seq - 1
	}

	return nil, nil
}

func (fs *fileStore) recoverMsgs() error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Check for any left over purged messages.
	pdir := filepath.Join(fs.fcfg.StoreDir, purgeDir)
	if _, err := os.Stat(pdir); err == nil {
		os.RemoveAll(pdir)
	}

	mdir := filepath.Join(fs.fcfg.StoreDir, msgDir)
	fis, err := ioutil.ReadDir(mdir)
	if err != nil {
		return errNotReadable
	}

	// Recover all of the msg blocks.
	// These can come in a random order, so account for that.
	for _, fi := range fis {
		var index uint64
		if n, err := fmt.Sscanf(fi.Name(), blkScan, &index); err == nil && n == 1 {
			if mb, err := fs.recoverMsgBlock(fi, index); err == nil && mb != nil {
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
				// Walk the fss for this mb and fill in fs.psmc
				for subj, ss := range mb.fss {
					if len(subj) > 0 {
						fs.psmc[subj] += ss.Msgs
					}
				}
			} else {
				return err
			}
		}
	}

	// Now make sure to sort blks for efficient lookup later with selectMsgBlock().
	if len(fs.blks) > 0 {
		sort.Slice(fs.blks, func(i, j int) bool { return fs.blks[i].index < fs.blks[j].index })
		fs.lmb = fs.blks[len(fs.blks)-1]
	} else {
		_, err = fs.newMsgBlockForWrite()
	}

	if err != nil {
		return err
	}

	// We had a bug that would leave fss files around during a snapshot.
	// Clean them up here if we see them.
	if fms, err := filepath.Glob(filepath.Join(mdir, fssScanAll)); err == nil && len(fms) > 0 {
		for _, fn := range fms {
			os.Remove(fn)
		}
	}
	// Same bug for keyfiles but for these we just need to identify orphans.
	if kms, err := filepath.Glob(filepath.Join(mdir, keyScanAll)); err == nil && len(kms) > 0 {
		valid := make(map[uint64]bool)
		for _, mb := range fs.blks {
			valid[mb.index] = true
		}
		for _, fn := range kms {
			var index uint64
			shouldRemove := true
			if n, err := fmt.Sscanf(filepath.Base(fn), keyScan, &index); err == nil && n == 1 && valid[index] {
				shouldRemove = false
			}
			if shouldRemove {
				os.Remove(fn)
			}
		}
	}

	// Limits checks and enforcement.
	fs.enforceMsgLimit()
	fs.enforceBytesLimit()

	// Do age checks too, make sure to call in place.
	if fs.cfg.MaxAge != 0 {
		fs.expireMsgsOnRecover()
		fs.startAgeChk()
	}

	return nil
}

// Will expire msgs that have aged out on restart.
// We will treat this differently in case we have a recovery
// that will expire alot of messages on startup.
// Should only be called on startup.
// Lock should be held.
func (fs *fileStore) expireMsgsOnRecover() {
	if fs.state.Msgs == 0 {
		return
	}

	var minAge = time.Now().UnixNano() - int64(fs.cfg.MaxAge)
	var purged, bytes uint64
	var deleted int
	var nts int64

	for _, mb := range fs.blks {
		mb.mu.Lock()
		if minAge < mb.first.ts {
			nts = mb.first.ts
			mb.mu.Unlock()
			break
		}
		// Can we remove whole block here?
		if mb.last.ts <= minAge {
			purged += mb.msgs
			bytes += mb.bytes
			mb.dirtyCloseWithRemove(true)
			newFirst := mb.last.seq + 1
			mb.mu.Unlock()
			// Update fs first here as well.
			fs.state.FirstSeq = newFirst
			fs.state.FirstTime = time.Time{}
			deleted++
			continue
		}

		// If we are here we have to process the interior messages of this blk.
		if err := mb.loadMsgsWithLock(); err != nil {
			mb.mu.Unlock()
			break
		}

		// Walk messages and remove if expired.
		for seq := mb.first.seq; seq <= mb.last.seq; seq++ {
			sm, err := mb.cacheLookup(seq)
			// Process interior deleted msgs.
			if err == errDeletedMsg {
				// Update dmap.
				if len(mb.dmap) > 0 {
					delete(mb.dmap, seq)
					if len(mb.dmap) == 0 {
						mb.dmap = nil
					}
				}
				continue
			}
			// Break on other errors.
			if err != nil || sm == nil {
				break
			}

			// No error and sm != nil from here onward.

			// Check for done.
			if minAge < sm.ts {
				mb.first.seq = sm.seq
				mb.first.ts = sm.ts
				nts = sm.ts
				break
			}

			// Delete the message here.
			sz := fileStoreMsgSize(sm.subj, sm.hdr, sm.msg)
			mb.bytes -= sz
			bytes += sz
			mb.msgs--
			purged++
			// Update fss
			fs.removePerSubject(sm.subj)
			mb.removeSeqPerSubject(sm.subj, seq)
		}

		// Check if empty after processing, could happen if tail of messages are all deleted.
		isEmpty := mb.msgs == 0
		if isEmpty {
			mb.dirtyCloseWithRemove(true)
			// Update fs first here as well.
			fs.state.FirstSeq = mb.last.seq + 1
			fs.state.FirstTime = time.Time{}
			deleted++
		} else {
			// Update fs first seq and time.
			fs.state.FirstSeq = mb.first.seq
			fs.state.FirstTime = time.Unix(0, mb.first.ts).UTC()
		}
		mb.mu.Unlock()

		if !isEmpty {
			// Make sure to write out our index info.
			mb.writeIndexInfo()
		}
		break
	}

	if nts > 0 {
		// Make sure to set age check based on this value.
		fs.resetAgeChk(nts - minAge)
	}

	if deleted > 0 {
		// Update blks slice.
		fs.blks = copyMsgBlocks(fs.blks[deleted:])
		if lb := len(fs.blks); lb == 0 {
			fs.lmb = nil
		} else {
			fs.lmb = fs.blks[lb-1]
		}
	}
	// Update top level accounting.
	fs.state.Msgs -= purged
	fs.state.Bytes -= bytes
}

func copyMsgBlocks(src []*msgBlock) []*msgBlock {
	if src == nil {
		return nil
	}
	dst := make([]*msgBlock, len(src))
	copy(dst, src)
	return dst
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
		sm, _, _ := mb.fetchMsg(seq)
		if sm != nil && sm.ts >= ts {
			return sm.seq
		}
	}
	return 0
}

// Find the first matching message.
func (mb *msgBlock) firstMatching(filter string, wc bool, start uint64) (*fileStoredMsg, bool, error) {
	mb.mu.Lock()
	defer mb.mu.Unlock()

	isAll, subs := filter == _EMPTY_ || filter == fwcs, []string{filter}
	// If we have a wildcard match against all tracked subjects we know about.
	if wc || isAll {
		subs = subs[:0]
		for subj := range mb.fss {
			if isAll || subjectIsSubsetMatch(subj, filter) {
				subs = append(subs, subj)
			}
		}
	}
	fseq := mb.last.seq + 1
	for _, subj := range subs {
		ss := mb.fss[subj]
		if ss == nil || start > ss.Last || ss.First >= fseq {
			continue
		}
		if ss.First < start {
			fseq = start
		} else {
			fseq = ss.First
		}
	}
	if fseq > mb.last.seq {
		return nil, false, ErrStoreMsgNotFound
	}

	if mb.cacheNotLoaded() {
		if err := mb.loadMsgsWithLock(); err != nil {
			return nil, false, err
		}
	}

	for seq := fseq; seq <= mb.last.seq; seq++ {
		llseq := mb.llseq
		sm, err := mb.cacheLookup(seq)
		if err != nil {
			continue
		}
		expireOk := seq == mb.last.seq && mb.llseq == seq-1
		if len(subs) == 1 && sm.subj == subs[0] {
			return sm, expireOk, nil
		}
		for _, subj := range subs {
			if sm.subj == subj {
				return sm, expireOk, nil
			}
		}
		// If we are here we did not match, so put the llseq back.
		mb.llseq = llseq
	}

	return nil, false, ErrStoreMsgNotFound
}

// This will traverse a message block and generate the filtered pending.
func (mb *msgBlock) filteredPending(subj string, wc bool, seq uint64) (total, first, last uint64) {
	mb.mu.Lock()
	defer mb.mu.Unlock()
	return mb.filteredPendingLocked(subj, wc, seq)
}

// This will traverse a message block and generate the filtered pending.
// Lock should be held.
func (mb *msgBlock) filteredPendingLocked(filter string, wc bool, seq uint64) (total, first, last uint64) {
	if mb.fss == nil {
		return 0, 0, 0
	}

	isAll := filter == _EMPTY_ || filter == fwcs
	subs := []string{filter}
	// If we have a wildcard match against all tracked subjects we know about.
	if wc || isAll {
		subs = subs[:0]
		for subj := range mb.fss {
			if isAll || subjectIsSubsetMatch(subj, filter) {
				subs = append(subs, subj)
			}
		}
	}
	// If we load the cache for a linear scan we want to expire that cache upon exit.
	var shouldExpire bool

	update := func(ss *SimpleState) {
		total += ss.Msgs
		if first == 0 || ss.First < first {
			first = ss.First
		}
		if ss.Last > last {
			last = ss.Last
		}
	}

	for i, subj := range subs {
		// If the starting seq is less then or equal that means we want all and we do not need to load any messages.
		ss := mb.fss[subj]
		if ss == nil || seq > ss.Last {
			continue
		}

		// If the seq we are starting at is less then the simple state's first sequence we can just return the total msgs.
		if seq <= ss.First {
			update(ss)
			continue
		}

		// We may need to scan this one block since we have a partial set to consider.
		// If we are all inclusive then we can do simple math and avoid the scan.
		if allInclusive := ss.Msgs == ss.Last-ss.First+1; allInclusive {
			update(ss)
			// Make sure to compensate for the diff from the head.
			if seq > ss.First {
				first, total = seq, total-(seq-ss.First)
			}
			continue
		}

		// We need to scan this block to compute the correct number of pending for this block.
		// We want to only do this once so we will adjust subs and test against them all here.

		if mb.cacheNotLoaded() {
			mb.loadMsgsWithLock()
			shouldExpire = true
		}

		var all, lseq uint64
		// Grab last applicable sequence as a union of all applicable subjects.
		for _, subj := range subs[i:] {
			if ss := mb.fss[subj]; ss != nil {
				all += ss.Msgs
				if ss.Last > lseq {
					lseq = ss.Last
				}
			}
		}
		numScanIn, numScanOut := lseq-seq, seq-mb.first.seq

		isMatch := func(seq uint64) bool {
			if sm, _ := mb.cacheLookup(seq); sm != nil {
				if len(subs) == 1 && sm.subj == subs[0] {
					return true
				}
				for _, subj := range subs {
					if sm.subj == subj {
						return true
					}
				}
			}
			return false
		}

		// Decide on whether to scan those included or those excluded based on which scan amount is less.
		if numScanIn < numScanOut {
			for tseq := seq; tseq <= lseq; tseq++ {
				if isMatch(tseq) {
					total++
					if first == 0 || tseq < first {
						first = tseq
					}
					last = tseq
				}
			}
		} else {
			// Here its more efficient to scan the out nodes.
			var discard uint64
			for tseq := mb.first.seq; tseq < seq; tseq++ {
				if isMatch(tseq) {
					discard++
				}
			}
			total += (all - discard)
			// Now make sure we match our first
			for tseq := seq; tseq <= lseq; tseq++ {
				if isMatch(tseq) {
					first = tseq
					break
				}
			}
		}
		// We can bail since we scanned all remaining in this pass.
		break
	}

	// If we loaded this block for this operation go ahead and expire it here.
	if shouldExpire {
		mb.tryForceExpireCacheLocked()
	}

	return total, first, last
}

// FilteredState will return the SimpleState associated with the filtered subject and a proposed starting sequence.
func (fs *fileStore) FilteredState(sseq uint64, subj string) SimpleState {
	fs.mu.RLock()
	lseq := fs.state.LastSeq
	if sseq < fs.state.FirstSeq {
		sseq = fs.state.FirstSeq
	}
	fs.mu.RUnlock()

	var ss SimpleState

	// If past the end no results.
	if sseq > lseq {
		return ss
	}

	// If subj is empty or we are not tracking multiple subjects.
	if subj == _EMPTY_ || subj == fwcs || !fs.tms {
		total := lseq - sseq + 1
		if state := fs.State(); len(state.Deleted) > 0 {
			for _, dseq := range state.Deleted {
				if dseq >= sseq && dseq <= lseq {
					total--
				}
			}
		}
		ss.Msgs, ss.First, ss.Last = total, sseq, lseq
		return ss
	}

	wc := subjectHasWildcard(subj)
	// Are we tracking multiple subject states?
	if fs.tms {
		for _, mb := range fs.blks {
			// Skip blocks that are less than our starting sequence.
			if sseq > atomic.LoadUint64(&mb.last.seq) {
				continue
			}
			t, f, l := mb.filteredPending(subj, wc, sseq)
			ss.Msgs += t
			if ss.First == 0 || (f > 0 && f < ss.First) {
				ss.First = f
			}
			if l > ss.Last {
				ss.Last = l
			}
		}
	} else {
		// Fallback to linear scan.
		eq := compareFn(subj)
		for seq := sseq; seq <= lseq; seq++ {
			if sm, _ := fs.msgForSeq(seq); sm != nil && eq(sm.subj, subj) {
				ss.Msgs++
				if ss.First == 0 {
					ss.First = seq
				}
				ss.Last = seq
			}
		}
	}

	return ss
}

// Will gather complete filtered state for the subject.
// Lock should be held.
func (fs *fileStore) perSubjectState(subj string) (total, first, last uint64) {
	if !fs.tms {
		return
	}
	wc := subjectHasWildcard(subj)
	for _, mb := range fs.blks {
		t, f, l := mb.filteredPending(subj, wc, 1)
		total += t
		if first == 0 || (f > 0 && f < first) {
			first = f
		}
		if l > last {
			last = l
		}
	}
	return total, first, last
}

// SubjectsState returns a map of SimpleState for all matching subjects.
func (fs *fileStore) SubjectsState(subject string) map[string]SimpleState {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	if !fs.tms || fs.state.Msgs == 0 {
		return nil
	}

	fss := make(map[string]SimpleState)
	for _, mb := range fs.blks {
		mb.mu.RLock()
		for subj, ss := range mb.fss {
			if subject == _EMPTY_ || subject == fwcs || subjectIsSubsetMatch(subj, subject) {
				oss := fss[subj]
				if oss.First == 0 { // New
					fss[subj] = *ss
				} else {
					// Merge here.
					oss.Last, oss.Msgs = ss.Last, oss.Msgs+ss.Msgs
					fss[subj] = oss
				}
			}
		}
		mb.mu.RUnlock()
	}
	return fss
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
	// Setup simple cache.
	mb.cache = &cache{buf: buf}
	// Make sure we set the proper cache offset if we have existing data.
	var fi os.FileInfo
	if mb.mfd != nil {
		fi, _ = mb.mfd.Stat()
	} else if mb.mfn != _EMPTY_ {
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
	index := uint64(1)
	var rbuf []byte

	if lmb := fs.lmb; lmb != nil {
		index = lmb.index + 1

		// Make sure to write out our index file if needed.
		if lmb.indexNeedsUpdate() {
			lmb.writeIndexInfo()
		}

		// Determine if we can reclaim any resources here.
		if fs.fip {
			lmb.mu.Lock()
			lmb.closeFDsLocked()
			if lmb.cache != nil {
				// Reset write timestamp and see if we can expire this cache.
				lwts, buf, llts := lmb.lwts, lmb.cache.buf, lmb.llts
				lmb.lwts = 0
				lmb.expireCacheLocked()
				lmb.lwts = lwts
				// We could check for a certain time since last load, but to be safe just reuse if no loads at all.
				if llts == 0 && (lmb.cache == nil || lmb.cache.buf == nil) {
					rbuf = buf[:0]
				}
			}
			lmb.mu.Unlock()
		}
	}

	mb := &msgBlock{fs: fs, index: index, cexp: fs.fcfg.CacheExpire}

	// Lock should be held to quiet race detector.
	mb.mu.Lock()
	mb.setupWriteCache(rbuf)
	if fs.tms {
		mb.fss = make(map[string]*SimpleState)
	}
	mb.mu.Unlock()

	// Now do local hash.
	key := sha256.Sum256(fs.hashKeyForBlock(index))
	hh, err := highwayhash.New64(key[:])
	if err != nil {
		return nil, fmt.Errorf("could not create hash: %v", err)
	}
	mb.hh = hh

	mdir := filepath.Join(fs.fcfg.StoreDir, msgDir)
	mb.mfn = filepath.Join(mdir, fmt.Sprintf(blkScan, mb.index))
	mfd, err := os.OpenFile(mb.mfn, os.O_CREATE|os.O_RDWR, defaultFilePerms)
	if err != nil {
		mb.dirtyCloseWithRemove(true)
		return nil, fmt.Errorf("Error creating msg block file [%q]: %v", mb.mfn, err)
	}
	mb.mfd = mfd

	mb.ifn = filepath.Join(mdir, fmt.Sprintf(indexScan, mb.index))
	ifd, err := os.OpenFile(mb.ifn, os.O_CREATE|os.O_RDWR, defaultFilePerms)
	if err != nil {
		mb.dirtyCloseWithRemove(true)
		return nil, fmt.Errorf("Error creating msg index file [%q]: %v", mb.mfn, err)
	}
	mb.ifd = ifd

	// For subject based info.
	mb.sfn = filepath.Join(mdir, fmt.Sprintf(fssScan, mb.index))

	// Check if encryption is enabled.
	if fs.prf != nil {
		if err := fs.genEncryptionKeysForBlock(mb); err != nil {
			return nil, err
		}
	}

	// Set cache time to creation time to start.
	ts := time.Now().UnixNano()
	// Race detector wants these protected.
	mb.mu.Lock()
	mb.llts, mb.lwts = 0, ts
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

// Generate the keys for this message block and write them out.
func (fs *fileStore) genEncryptionKeysForBlock(mb *msgBlock) error {
	if mb == nil {
		return nil
	}
	key, bek, seed, encrypted, err := fs.genEncryptionKeys(fmt.Sprintf("%s:%d", fs.cfg.Name, mb.index))
	if err != nil {
		return err
	}
	mb.aek, mb.bek, mb.seed, mb.nonce = key, bek, seed, encrypted[:key.NonceSize()]
	mdir := filepath.Join(fs.fcfg.StoreDir, msgDir)
	keyFile := filepath.Join(mdir, fmt.Sprintf(keyScan, mb.index))
	if _, err := os.Stat(keyFile); err != nil && !os.IsNotExist(err) {
		return err
	}
	if err := ioutil.WriteFile(keyFile, encrypted, defaultFilePerms); err != nil {
		return err
	}
	mb.kfn = keyFile
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
		var asl bool
		var fseq uint64
		if fs.cfg.MaxMsgsPer > 0 && len(subj) > 0 {
			var msgs uint64
			if msgs, fseq, _ = fs.perSubjectState(subj); msgs >= uint64(fs.cfg.MaxMsgsPer) {
				asl = true
			}
		}
		if fs.cfg.MaxMsgs > 0 && fs.state.Msgs >= uint64(fs.cfg.MaxMsgs) {
			if !asl {
				return ErrMaxMsgs
			}
		}
		if fs.cfg.MaxBytes > 0 && fs.state.Bytes+uint64(len(msg)+len(hdr)) >= uint64(fs.cfg.MaxBytes) {
			if !asl || fs.sizeForSeq(fseq) <= len(msg)+len(hdr) {
				return ErrMaxBytes
			}
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

	// Adjust top level tracking of per subject msg counts.
	if len(subj) > 0 {
		fs.psmc[subj]++
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

	// Enforce per message limits.
	if fs.cfg.MaxMsgsPer > 0 && len(subj) > 0 {
		fs.enforcePerSubjectLimit(subj)
	}

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

// Will check the msg limit for this tracked subject.
// Lock should be held.
func (fs *fileStore) enforcePerSubjectLimit(subj string) {
	if fs.closed || fs.sips > 0 || fs.cfg.MaxMsgsPer <= 0 || !fs.tms {
		return
	}
	for {
		msgs, first, _ := fs.perSubjectState(subj)
		if msgs <= uint64(fs.cfg.MaxMsgsPer) {
			return
		}
		if ok, _ := fs.removeMsg(first, false, false); !ok {
			break
		}
	}
}

// Will check the msg limit and drop firstSeq msg if needed.
// Lock should be held.
func (fs *fileStore) enforceMsgLimit() {
	if fs.cfg.MaxMsgs <= 0 || fs.state.Msgs <= uint64(fs.cfg.MaxMsgs) {
		return
	}
	for nmsgs := fs.state.Msgs; nmsgs > uint64(fs.cfg.MaxMsgs); nmsgs = fs.state.Msgs {
		if removed, err := fs.deleteFirstMsg(); err != nil || !removed {
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
		if removed, err := fs.deleteFirstMsg(); err != nil || !removed {
			fs.rebuildFirst()
			return
		}
	}
}

// Lock should be held.
func (fs *fileStore) deleteFirstMsg() (bool, error) {
	return fs.removeMsg(fs.state.FirstSeq, false, false)
}

// RemoveMsg will remove the message from this store.
// Will return the number of bytes removed.
func (fs *fileStore) RemoveMsg(seq uint64) (bool, error) {
	return fs.removeMsg(seq, false, true)
}

func (fs *fileStore) EraseMsg(seq uint64) (bool, error) {
	return fs.removeMsg(seq, true, true)
}

// Convenience function to remove per subject tracking at the filestore level.
// Lock should be held.
func (fs *fileStore) removePerSubject(subj string) {
	if len(subj) == 0 {
		return
	}
	if n, ok := fs.psmc[subj]; ok && n == 1 {
		delete(fs.psmc, subj)
	} else if ok {
		fs.psmc[subj]--
	}
}

// Remove a message, optionally rewriting the mb file.
func (fs *fileStore) removeMsg(seq uint64, secure, needFSLock bool) (bool, error) {
	fsLock := func() {
		if needFSLock {
			fs.mu.Lock()
		}
	}
	fsUnlock := func() {
		if needFSLock {
			fs.mu.Unlock()
		}
	}

	fsLock()

	if fs.closed {
		fsUnlock()
		return false, ErrStoreClosed
	}
	if fs.sips > 0 {
		fsUnlock()
		return false, ErrStoreSnapshotInProgress
	}
	// If in encrypted mode negate secure rewrite here.
	if secure && fs.prf != nil {
		secure = false
	}
	mb := fs.selectMsgBlock(seq)
	if mb == nil {
		var err = ErrStoreEOF
		if seq <= fs.state.LastSeq {
			err = ErrStoreMsgNotFound
		}
		fsUnlock()
		return false, err
	}

	mb.mu.Lock()

	// See if the sequence numbers is still relevant.
	if seq < mb.first.seq {
		mb.mu.Unlock()
		fsUnlock()
		return false, nil
	}

	// Now check dmap if it is there.
	if mb.dmap != nil {
		if _, ok := mb.dmap[seq]; ok {
			mb.mu.Unlock()
			fsUnlock()
			return false, nil
		}
	}

	// We used to not have to load in the messages except with callbacks or the filtered subject state (which is now always on).
	// Now just load regardless.
	// TODO(dlc) - Figure out a way not to have to load it in, we need subject tracking outside main data block.
	if mb.cacheNotLoaded() {
		if err := mb.loadMsgsWithLock(); err != nil {
			mb.mu.Unlock()
			fsUnlock()
			return false, err
		}
	}

	sm, err := mb.cacheLookup(seq)
	if err != nil {
		mb.mu.Unlock()
		fsUnlock()
		return false, err
	}
	// Grab size
	msz := fileStoreMsgSize(sm.subj, sm.hdr, sm.msg)

	// Set cache timestamp for last remove.
	mb.lrts = time.Now().UnixNano()

	// Global stats
	fs.state.Msgs--
	fs.state.Bytes -= msz

	// Now local mb updates.
	mb.msgs--
	mb.bytes -= msz

	// If we are tracking multiple subjects here make sure we update that accounting.
	fs.removePerSubject(sm.subj)
	mb.removeSeqPerSubject(sm.subj, seq)

	var shouldWriteIndex, firstSeqNeedsUpdate bool

	if secure {
		// Grab record info.
		ri, rl, _, _ := mb.slotInfo(int(seq - mb.cache.fseq))
		mb.eraseMsg(seq, int(ri), int(rl))
	}

	// Optimize for FIFO case.
	fifo := seq == mb.first.seq
	if fifo {
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
		// Check if we are empty first, as long as not the last message block.
		if notLast := mb != fs.lmb; notLast && mb.msgs == 0 {
			fs.removeMsgBlock(mb)
			firstSeqNeedsUpdate = seq == fs.state.FirstSeq
		} else {
			// Out of order delete.
			shouldWriteIndex = true
			if mb.dmap == nil {
				mb.dmap = make(map[uint64]struct{})
			}
			mb.dmap[seq] = struct{}{}
			// Check if <25% utilization and minimum size met.
			if notLast && mb.rbytes > compactMinimum && mb.rbytes>>2 > mb.bytes {
				mb.compact()
			}
		}
	}

	var qch, fch chan struct{}
	if shouldWriteIndex {
		qch, fch = mb.qch, mb.fch
	}
	cb := fs.scb

	if secure {
		if ld, _ := mb.flushPendingMsgsLocked(); ld != nil {
			fs.rebuildStateLocked(ld)
		}
	}
	// Check if we need to write the index file and we are flush in place (fip).
	if shouldWriteIndex && fs.fip {
		// Check if this is the first message, common during expirations etc.
		if !fifo || time.Now().UnixNano()-mb.lwits > int64(2*time.Second) {
			mb.writeIndexInfoLocked()
		}
	}
	mb.mu.Unlock()

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
		}
	}

	// If we emptied the current message block and the seq was state.First.Seq
	// then we need to jump message blocks. We will also write the index so
	// we don't lose track of the first sequence.
	if firstSeqNeedsUpdate {
		fs.selectNextFirst()
		fs.lmb.writeIndexInfo()
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

	if !needFSLock {
		fs.mu.Lock()
	}

	return true, nil
}

// This will compact and rewrite this block. This should only be called when we know we want to rewrite this block.
// This should not be called on the lmb since we will prune tail deleted messages which could cause issues with
// writing new messages. We will silently bail on any issues with the underlying block and let someone else detect.
// Write lock needs to be held.
func (mb *msgBlock) compact() {
	if mb.cacheNotLoaded() {
		if err := mb.loadMsgsWithLock(); err != nil {
			return
		}
	}

	buf := mb.cache.buf
	nbuf := make([]byte, 0, len(buf))

	var le = binary.LittleEndian
	var firstSet bool

	isDeleted := func(seq uint64) bool {
		if seq == 0 || seq&ebit != 0 || seq < mb.first.seq {
			return true
		}
		var deleted bool
		if mb.dmap != nil {
			_, deleted = mb.dmap[seq]
		}
		return deleted
	}

	// For skip msgs.
	var smh [msgHdrSize]byte

	for index, lbuf := uint32(0), uint32(len(buf)); index < lbuf; {
		if index+msgHdrSize > lbuf {
			return
		}
		hdr := buf[index : index+msgHdrSize]
		rl, slen := le.Uint32(hdr[0:]), le.Uint16(hdr[20:])
		// Clear any headers bit that could be set.
		rl &^= hbit
		dlen := int(rl) - msgHdrSize
		// Do some quick sanity checks here.
		if dlen < 0 || int(slen) > dlen || dlen > int(rl) || rl > rlBadThresh || index+rl > lbuf {
			return
		}
		// Only need to process non-deleted messages.
		seq := le.Uint64(hdr[4:])
		if !isDeleted(seq) {
			// Normal message here.
			nbuf = append(nbuf, buf[index:index+rl]...)
			if !firstSet {
				firstSet = true
				mb.first.seq = seq
			}
		} else if firstSet {
			// This is an interior delete that we need to make sure we have a placeholder for.
			le.PutUint32(smh[0:], emptyRecordLen)
			le.PutUint64(smh[4:], seq|ebit)
			le.PutUint64(smh[12:], 0)
			le.PutUint16(smh[20:], 0)
			nbuf = append(nbuf, smh[:]...)
			mb.hh.Reset()
			mb.hh.Write(smh[4:20])
			checksum := mb.hh.Sum(nil)
			nbuf = append(nbuf, checksum...)
		}
		// Always set last.
		mb.last.seq = seq &^ ebit

		// Advance to next record.
		index += rl
	}

	// Check for encryption.
	if mb.bek != nil && len(nbuf) > 0 {
		// Recreate to reset counter.
		rbek, err := chacha20.NewUnauthenticatedCipher(mb.seed, mb.nonce)
		if err != nil {
			return
		}
		rbek.XORKeyStream(nbuf, nbuf)
	}

	// Close FDs first.
	mb.closeFDsLocked()

	// We will write to a new file and mv/rename it in case of failure.
	mfn := filepath.Join(filepath.Join(mb.fs.fcfg.StoreDir, msgDir), fmt.Sprintf(newScan, mb.index))
	defer os.Remove(mfn)
	if err := ioutil.WriteFile(mfn, nbuf, defaultFilePerms); err != nil {
		return
	}
	if err := os.Rename(mfn, mb.mfn); err != nil {
		return
	}

	// Close cache and index file and wipe delete map, then rebuild.
	mb.clearCacheAndOffset()
	mb.removeIndexFileLocked()
	mb.deleteDmap()
	mb.rebuildStateLocked()
	mb.loadMsgsWithLock()
}

// Nil out our dmap.
func (mb *msgBlock) deleteDmap() {
	mb.dmap = nil
}

// Grab info from a slot.
// Lock should be held.
func (mb *msgBlock) slotInfo(slot int) (uint32, uint32, bool, error) {
	if mb.cache == nil || slot >= len(mb.cache.idx) {
		return 0, 0, false, errPartialCache
	}
	bi := mb.cache.idx[slot]
	ri, hashChecked := (bi &^ hbit), (bi&hbit) != 0

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
	mrand.Read(data)

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
		mfd, err := os.OpenFile(mb.mfn, os.O_RDWR, defaultFilePerms)
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
	mb.rbytes -= bytes

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
	sm, _ := mb.cacheLookup(seq)
	if sm == nil {
		// Slow path, need to unlock.
		mb.mu.Unlock()
		sm, _, _ = mb.fetchMsg(seq)
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
	// Reset linear scan tracker.
	mb.llseq = 0
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

func (mb *msgBlock) tryForceExpireCache() {
	mb.mu.Lock()
	defer mb.mu.Unlock()
	mb.tryForceExpireCacheLocked()
}

// We will attempt to force expire this by temporarily clearing the last load time.
func (mb *msgBlock) tryForceExpireCacheLocked() {
	llts := mb.llts
	mb.llts = 0
	mb.expireCacheLocked()
	mb.llts = llts
}

// Lock should be held.
func (mb *msgBlock) expireCacheLocked() {
	if mb.cache == nil {
		if mb.ctmr != nil {
			mb.ctmr.Stop()
			mb.ctmr = nil
		}
		return
	}

	// Can't expire if we still have pending.
	if len(mb.cache.buf)-int(mb.cache.wp) > 0 {
		mb.resetCacheExpireTimer(mb.cexp)
		return
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
		return
	}

	// If we are here we will at least expire the core msg buffer.
	// We need to capture offset in case we do a write next before a full load.
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
}

func (fs *fileStore) startAgeChk() {
	if fs.ageChk == nil && fs.cfg.MaxAge != 0 {
		fs.ageChk = time.AfterFunc(fs.cfg.MaxAge, fs.expireMsgs)
	}
}

// Lock should be held.
func (fs *fileStore) resetAgeChk(delta int64) {
	fireIn := fs.cfg.MaxAge
	if delta > 0 {
		fireIn = time.Duration(delta)
	}
	if fs.ageChk != nil {
		fs.ageChk.Reset(fireIn)
	} else {
		fs.ageChk = time.AfterFunc(fireIn, fs.expireMsgs)
	}
}

// Lock should be held.
func (fs *fileStore) cancelAgeChk() {
	if fs.ageChk != nil {
		fs.ageChk.Stop()
		fs.ageChk = nil
	}
}

// Will expire msgs that are too old.
func (fs *fileStore) expireMsgs() {
	// We need to delete one by one here and can not optimize for the time being.
	// Reason is that we need more information to adjust ack pending in consumers.
	var sm *fileStoredMsg
	minAge := time.Now().UnixNano() - int64(fs.cfg.MaxAge)
	for sm, _ = fs.msgForSeq(0); sm != nil && sm.ts <= minAge; sm, _ = fs.msgForSeq(0) {
		fs.removeMsg(sm.seq, false, true)
	}

	fs.mu.Lock()
	defer fs.mu.Unlock()

	if sm == nil {
		fs.cancelAgeChk()
	} else {
		fs.resetAgeChk(sm.ts - minAge)
	}
}

// Lock should be held.
func (fs *fileStore) checkAndFlushAllBlocks() {
	for _, mb := range fs.blks {
		if mb.pendingWriteSize() > 0 {
			// Since fs lock is held need to pull this apart in case we need to rebuild state.
			mb.mu.Lock()
			ld, _ := mb.flushPendingMsgsLocked()
			mb.mu.Unlock()
			if ld != nil {
				fs.rebuildStateLocked(ld)
			}
		}
		if mb.indexNeedsUpdate() {
			mb.writeIndexInfo()
		}
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
			mb.fs.rebuildStateLocked(ld)
		}
	}
	return fs.ld
}

// Lock should be held.
func (mb *msgBlock) enableForWriting(fip bool) error {
	if mb == nil {
		return errNoMsgBlk
	}
	if mb.mfd != nil {
		return nil
	}
	mfd, err := os.OpenFile(mb.mfn, os.O_CREATE|os.O_RDWR, defaultFilePerms)
	if err != nil {
		return fmt.Errorf("error opening msg block file [%q]: %v", mb.mfn, err)
	}
	mb.mfd = mfd

	// Spin up our flusher loop if needed.
	if !fip {
		mb.spinUpFlushLoop()
	}

	return nil
}

// Will write the message record to the underlying message block.
// filestore lock will be held.
func (mb *msgBlock) writeMsgRecord(rl, seq uint64, subj string, mhdr, msg []byte, ts int64, flush bool) error {
	mb.mu.Lock()
	defer mb.mu.Unlock()

	// Make sure we have a cache setup.
	if mb.cache == nil {
		mb.setupWriteCache(nil)
	}
	// Enable for writing if our mfd is not open.
	if mb.mfd == nil {
		if err := mb.enableForWriting(flush); err != nil {
			return err
		}
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
	writeIndex := ts-mb.lwits > int64(2*time.Second)

	// Accounting
	mb.updateAccounting(seq&^ebit, ts, rl)

	// Check if we are tracking per subject for our simple state.
	if len(subj) > 0 && mb.fss != nil {
		if ss := mb.fss[subj]; ss != nil {
			ss.Msgs++
			ss.Last = seq
		} else {
			mb.fss[subj] = &SimpleState{Msgs: 1, First: seq, Last: seq}
		}
	}

	fch, werr := mb.fch, mb.werr

	// If we should be flushing, or had a write error, do so here.
	if flush || werr != nil {
		ld, err := mb.flushPendingMsgsLocked()
		if ld != nil && mb.fs != nil {
			mb.fs.rebuildStateLocked(ld)
		}
		if err != nil {
			return err
		}
		if writeIndex {
			if err := mb.writeIndexInfoLocked(); err != nil {
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

// Try to close our FDs if we can.
func (mb *msgBlock) closeFDs() error {
	mb.mu.Lock()
	defer mb.mu.Unlock()
	return mb.closeFDsLocked()
}

func (mb *msgBlock) closeFDsLocked() error {
	if buf, _ := mb.bytesPending(); len(buf) > 0 {
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
	if len(mb.cache.buf) <= mb.cache.wp {
		return nil, errNoPending
	}
	buf := mb.cache.buf[mb.cache.wp:]
	if len(buf) == 0 {
		return nil, errNoPending
	}
	return buf, nil
}

// Returns the current blkSize including deleted msgs etc.
func (mb *msgBlock) blkSize() uint64 {
	mb.mu.RLock()
	nb := mb.rbytes
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
	mb.rbytes += rl
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
	if mb == nil || mb.blkSize()+rl > fs.fcfg.BlockSize {
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
		// Flush anything that may be pending.
		if mb.pendingWriteSize() > 0 {
			mb.flushPendingMsgs()
		}
		if mb.indexNeedsUpdate() {
			mb.writeIndexInfo()
		}
		// Do actual sync. Hold lock for consistency.
		mb.mu.Lock()
		if !mb.closed {
			if mb.mfd != nil {
				mb.mfd.Sync()
			}
			if mb.ifd != nil {
				mb.ifd.Truncate(mb.liwsz)
				mb.ifd.Sync()
			}
			// See if we can close FDs do to being idle.
			if mb.ifd != nil || mb.mfd != nil && mb.sinceLastWriteActivity() > closeFDsIdle {
				mb.dirtyCloseWithRemove(false)
			}
		}
		mb.mu.Unlock()
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

	// Starting index, defaults to beginning.
	si := 0

	// Max threshold before we probe for a starting block to start our linear search.
	const maxl = 256
	if nb := len(fs.blks); nb > maxl {
		d := nb / 8
		for _, i := range []int{d, 2 * d, 3 * d, 4 * d, 5 * d, 6 * d, 7 * d} {
			mb := fs.blks[i]
			if seq <= atomic.LoadUint64(&mb.last.seq) {
				break
			}
			si = i
		}
	}

	// blks are sorted in ascending order.
	for i := si; i < len(fs.blks); i++ {
		mb := fs.blks[i]
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
		if index+msgHdrSize > lbuf {
			return errCorruptState
		}
		hdr := buf[index : index+msgHdrSize]
		rl, seq, slen := le.Uint32(hdr[0:]), le.Uint64(hdr[4:]), le.Uint16(hdr[20:])

		// Clear any headers bit that could be set.
		rl &^= hbit
		dlen := int(rl) - msgHdrSize

		// Do some quick sanity checks here.
		if dlen < 0 || int(slen) > dlen || dlen > int(rl) || rl > 32*1024*1024 {
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

// flushPendingMsgs writes out any messages for this message block.
func (mb *msgBlock) flushPendingMsgs() error {
	mb.mu.Lock()
	fsLostData, err := mb.flushPendingMsgsLocked()
	fs := mb.fs
	mb.mu.Unlock()

	// Signals us that we need to rebuild filestore state.
	if fsLostData != nil && fs != nil {
		// Rebuild fs state too.
		fs.rebuildState(fsLostData)
	}
	return err
}

// flushPendingMsgsLocked writes out any messages for this message block.
// Lock should be held.
func (mb *msgBlock) flushPendingMsgsLocked() (*LostStreamData, error) {
	// Signals us that we need to rebuild filestore state.
	var fsLostData *LostStreamData

	if mb.cache == nil || mb.mfd == nil {
		return nil, nil
	}

	buf, err := mb.bytesPending()
	// If we got an error back return here.
	if err != nil {
		// No pending data to be written is not an error.
		if err == errNoPending || err == errNoCache {
			err = nil
		}
		return nil, err
	}

	woff := int64(mb.cache.off + mb.cache.wp)
	lob := len(buf)

	// TODO(dlc) - Normally we would not hold the lock across I/O so we can improve performance.
	// We will hold to stabilize the code base, as we have had a few anomalies with partial cache errors
	// under heavy load.

	// Check if we need to encrypt.
	if mb.bek != nil && lob > 0 {
		const rsz = 32 * 1024 // 32k
		var rdst [rsz]byte
		var dst []byte
		if lob > rsz {
			dst = make([]byte, lob)
		} else {
			dst = rdst[:lob]
		}
		// Need to leave original alone.
		mb.bek.XORKeyStream(dst, buf)
		buf = dst
	}

	// Append new data to the message block file.
	for lbb := lob; lbb > 0; lbb = len(buf) {
		n, err := mb.mfd.WriteAt(buf, woff)
		if err != nil {
			mb.removeIndexFileLocked()
			mb.dirtyCloseWithRemove(false)
			if !isOutOfSpaceErr(err) {
				if ld, err := mb.rebuildStateLocked(); err != nil && ld != nil {
					fsLostData = ld
				}
			}
			return fsLostData, err
		}
		// Partial write.
		if n != lbb {
			buf = buf[n:]
		} else {
			// Done.
			break
		}
	}

	// Update our write offset.
	woff += int64(lob)

	// set write err to any error.
	mb.werr = err

	// Cache may be gone.
	if mb.cache == nil || mb.mfd == nil {
		return fsLostData, mb.werr
	}

	// Check for additional writes while we were writing to the disk.
	moreBytes := len(mb.cache.buf) - mb.cache.wp - lob

	// Decide what we want to do with the buffer in hand. If we have load interest
	// we will hold onto the whole thing, otherwise empty the buffer, possibly reusing it.
	if ts := time.Now().UnixNano(); ts < mb.llts || (ts-mb.llts) <= int64(mb.cexp) {
		mb.cache.wp += lob
	} else {
		if cap(mb.cache.buf) <= maxBufReuse {
			buf = mb.cache.buf[:0]
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
		// Mark fseq to 0
		mb.cache.fseq = 0
	}

	return fsLostData, mb.werr
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
	return mb.loadMsgsWithLock()
}

// Lock should be held.
func (mb *msgBlock) cacheAlreadyLoaded() bool {
	if mb.cache == nil || mb.cache.off != 0 || mb.cache.fseq == 0 || len(mb.cache.buf) == 0 {
		return false
	}
	numEntries := mb.msgs + uint64(len(mb.dmap)) + (mb.first.seq - mb.cache.fseq)
	return numEntries == uint64(len(mb.cache.idx))
}

// Lock should be held.
func (mb *msgBlock) cacheNotLoaded() bool {
	return !mb.cacheAlreadyLoaded()
}

// Used to load in the block contents.
// Lock should be held and all conditionals satisfied prior.
func (mb *msgBlock) loadBlock(buf []byte) ([]byte, error) {
	f, err := os.Open(mb.mfn)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var sz int
	if info, err := f.Stat(); err == nil {
		sz64 := info.Size()
		if int64(int(sz64)) == sz64 {
			sz = int(sz64)
		} else {
			return nil, errMsgBlkTooBig
		}
	}

	if sz > cap(buf) {
		buf = make([]byte, sz)
	} else {
		buf = buf[:sz]
	}

	n, err := io.ReadFull(f, buf)
	return buf[:n], err
}

// Lock should be held.
func (mb *msgBlock) loadMsgsWithLock() error {
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
	if mb.cacheAlreadyLoaded() {
		return nil
	}

	mb.llts = time.Now().UnixNano()

	// FIXME(dlc) - We could be smarter here.
	if buf, _ := mb.bytesPending(); len(buf) > 0 {
		ld, err := mb.flushPendingMsgsLocked()
		if ld != nil && mb.fs != nil {
			// We do not know if fs is locked or not at this point.
			// This should be an exceptional condition so do so in Go routine.
			go mb.fs.rebuildState(ld)
		}
		if err != nil {
			return err
		}
		goto checkCache
	}

	// Load in the whole block.
	// We want to hold the mb lock here to avoid any changes to state.
	buf, err := mb.loadBlock(nil)
	if err != nil {
		return err
	}

	// Reset the cache since we just read everything in.
	// Make sure this is cleared in case we had a partial when we started.
	mb.clearCacheAndOffset()

	// Check if we need to decrypt.
	if mb.bek != nil && len(buf) > 0 {
		rbek, err := chacha20.NewUnauthenticatedCipher(mb.seed, mb.nonce)
		if err != nil {
			return err
		}
		rbek.XORKeyStream(buf, buf)
	}

	if err := mb.indexCacheBuf(buf); err != nil {
		if err == errCorruptState {
			fs := mb.fs
			mb.mu.Unlock()
			var ld *LostStreamData
			if ld, err = mb.rebuildState(); ld != nil {
				// We do not know if fs is locked or not at this point.
				// This should be an exceptional condition so do so in Go routine.
				go fs.rebuildState(ld)
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
func (mb *msgBlock) fetchMsg(seq uint64) (*fileStoredMsg, bool, error) {
	mb.mu.Lock()
	defer mb.mu.Unlock()

	if mb.cacheNotLoaded() {
		if err := mb.loadMsgsWithLock(); err != nil {
			return nil, false, err
		}
	}
	sm, err := mb.cacheLookup(seq)
	if err != nil {
		return nil, false, err
	}
	expireOk := seq == mb.last.seq && mb.llseq == seq-1
	return sm, expireOk, err
}

var (
	errNoCache      = errors.New("no message cache")
	errBadMsg       = errors.New("malformed or corrupt message")
	errDeletedMsg   = errors.New("deleted message")
	errPartialCache = errors.New("partial cache")
	errNoPending    = errors.New("message block does not have pending data")
	errNotReadable  = errors.New("storage directory not readable")
	errCorruptState = errors.New("corrupt state file")
	errPendingData  = errors.New("pending data still present")
	errNoEncryption = errors.New("encryption not enabled")
	errBadKeySize   = errors.New("encryption bad key size")
	errNoMsgBlk     = errors.New("no message block")
	errMsgBlkTooBig = errors.New("message block size exceeded int capacity")
)

// Used for marking messages that have had their checksums checked.
// Used to signal a message record with headers.
const hbit = 1 << 31

// Used for marking erased messages sequences.
const ebit = 1 << 63

// Will do a lookup from cache.
// Lock should be held.
func (mb *msgBlock) cacheLookup(seq uint64) (*fileStoredMsg, error) {
	if seq < mb.first.seq || seq > mb.last.seq {
		return nil, ErrStoreMsgNotFound
	}

	// If we have a delete map check it.
	if mb.dmap != nil {
		if _, ok := mb.dmap[seq]; ok {
			return nil, errDeletedMsg
		}
	}
	// Detect no cache loaded.
	if mb.cache == nil || mb.cache.fseq == 0 || len(mb.cache.idx) == 0 || len(mb.cache.buf) == 0 {
		return nil, errNoCache
	}
	// Check partial cache status.
	if seq < mb.cache.fseq {
		return nil, errPartialCache
	}

	bi, _, hashChecked, err := mb.slotInfo(int(seq - mb.cache.fseq))
	if err != nil {
		return nil, err
	}

	// Update cache activity.
	mb.llts = time.Now().UnixNano()
	// The llseq signals us when we can expire a cache at the end of a linear scan.
	// We want to only update when we know the last reads (multiple consumers) are sequential.
	if mb.llseq == 0 || seq < mb.llseq || seq == mb.llseq+1 {
		mb.llseq = seq
	}

	li := int(bi) - mb.cache.off
	if li >= len(mb.cache.buf) {
		return nil, errPartialCache
	}
	buf := mb.cache.buf[li:]

	// We use the high bit to denote we have already checked the checksum.
	var hh hash.Hash64
	if !hashChecked {
		hh = mb.hh // This will force the hash check in msgFromBuf.
	}

	// Parse from the raw buffer.
	subj, hdr, msg, mseq, ts, err := msgFromBuf(buf, hh)
	if err != nil {
		return nil, err
	}
	if seq != mseq {
		mb.cache.buf = nil
		return nil, fmt.Errorf("sequence numbers for cache load did not match, %d vs %d", seq, mseq)
	}

	// Clear the check bit here after we know all is good.
	if !hashChecked {
		mb.cache.idx[seq-mb.cache.fseq] = (bi | hbit)
	}

	return &fileStoredMsg{subj, hdr, msg, seq, ts, mb, int64(bi)}, nil
}

// Used when we are checking if discarding a message due to max msgs per subject will give us
// enough room for a max bytes condition.
// Lock should be already held.
func (fs *fileStore) sizeForSeq(seq uint64) int {
	if mb := fs.selectMsgBlock(seq); mb != nil {
		if sm, _, _ := mb.fetchMsg(seq); sm != nil {
			return int(fileStoreMsgSize(sm.subj, sm.hdr, sm.msg))
		}
	}
	return 0
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
	mb, lmb, lseq := fs.selectMsgBlock(seq), fs.lmb, fs.state.LastSeq
	fs.mu.RUnlock()

	if mb == nil {
		var err = ErrStoreEOF
		if seq <= lseq {
			err = ErrStoreMsgNotFound
		}
		return nil, err
	}

	fsm, expireOk, err := mb.fetchMsg(seq)
	if err != nil {
		return nil, err
	}

	// We detected a linear scan and access to the last message.
	// If we are not the last message block we can try to expire the cache.
	if mb != lmb && expireOk {
		mb.tryForceExpireCache()
	}

	return fsm, nil
}

// Internal function to return msg parts from a raw buffer.
func msgFromBuf(buf []byte, hh hash.Hash64) (string, []byte, []byte, uint64, int64, error) {
	if len(buf) < emptyRecordLen {
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
	return _EMPTY_, nil, nil, 0, err
}

// LoadLastMsg will return the last message we have that matches a given subject.
// The subject can be a wildcard.
func (fs *fileStore) LoadLastMsg(subject string) (subj string, seq uint64, hdr, msg []byte, ts int64, err error) {
	var sm *fileStoredMsg
	if subject == _EMPTY_ || subject == fwcs {
		sm, _ = fs.msgForSeq(fs.lastSeq())
	} else if ss := fs.FilteredState(1, subject); ss.Msgs > 0 {
		sm, _ = fs.msgForSeq(ss.Last)
	}
	if sm == nil {
		return _EMPTY_, 0, nil, nil, 0, ErrStoreMsgNotFound
	}
	return sm.subj, sm.seq, sm.hdr, sm.msg, sm.ts, nil
}

// LoadNextMsg will find the next message matching the filter subject starting at the start sequence.
// The filter subject can be a wildcard.
func (fs *fileStore) LoadNextMsg(filter string, wc bool, start uint64) (subj string, seq uint64, hdr, msg []byte, ts int64, err error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	if fs.closed {
		return _EMPTY_, 0, nil, nil, 0, ErrStoreClosed
	}
	if start < fs.state.FirstSeq {
		start = fs.state.FirstSeq
	}

	for _, mb := range fs.blks {
		// Skip blocks that are less than our starting sequence.
		if start > atomic.LoadUint64(&mb.last.seq) {
			continue
		}
		if sm, expireOk, err := mb.firstMatching(filter, wc, start); err == nil {
			if expireOk && mb != fs.lmb {
				mb.tryForceExpireCache()
			}
			return sm.subj, sm.seq, sm.hdr, sm.msg, sm.ts, nil
		} else if err != ErrStoreMsgNotFound {
			return _EMPTY_, 0, nil, nil, 0, err
		}
	}

	return _EMPTY_, fs.state.LastSeq, nil, nil, 0, ErrStoreEOF
}

// Type returns the type of the underlying store.
func (fs *fileStore) Type() StorageType {
	return FileStorage
}

// FastState will fill in state with only the following.
// Msgs, Bytes, First and Last Sequence and Time and NumDeleted.
func (fs *fileStore) FastState(state *StreamState) {
	fs.mu.RLock()
	state.Msgs = fs.state.Msgs
	state.Bytes = fs.state.Bytes
	state.FirstSeq = fs.state.FirstSeq
	state.FirstTime = fs.state.FirstTime
	state.LastSeq = fs.state.LastSeq
	state.LastTime = fs.state.LastTime
	if state.LastSeq > state.FirstSeq {
		state.NumDeleted = int((state.LastSeq - state.FirstSeq + 1) - state.Msgs)
		if state.NumDeleted < 0 {
			state.NumDeleted = 0
		}
	}
	state.Consumers = len(fs.cfs)
	state.NumSubjects = len(fs.psmc)
	fs.mu.RUnlock()
}

// State returns the current state of the stream.
func (fs *fileStore) State() StreamState {
	fs.mu.RLock()
	state := fs.state
	state.Consumers = len(fs.cfs)
	state.NumSubjects = len(fs.psmc)
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
		state.NumDeleted = len(state.Deleted)
	}
	return state
}

func (fs *fileStore) Utilization() (total, reported uint64, err error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	for _, mb := range fs.blks {
		mb.mu.RLock()
		reported += mb.bytes
		total += mb.rbytes
		mb.mu.RUnlock()
	}
	return total, reported, nil
}

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

// Determine time since last write or remove of a message.
// Read lock should be held.
func (mb *msgBlock) sinceLastWriteActivity() time.Duration {
	if mb.closed {
		return 0
	}
	last := mb.lwts
	if mb.lrts > last {
		last = mb.lrts
	}
	return time.Since(time.Unix(0, last).UTC())
}

// Determine if we need to write out this index info.
func (mb *msgBlock) indexNeedsUpdate() bool {
	mb.mu.RLock()
	defer mb.mu.RUnlock()
	return mb.lwits < mb.lwts || mb.lwits < mb.lrts
}

// Write index info to the appropriate file.
// Filestore lock should be held.
func (mb *msgBlock) writeIndexInfo() error {
	mb.mu.Lock()
	defer mb.mu.Unlock()
	return mb.writeIndexInfoLocked()
}

// Write index info to the appropriate file.
// Filestore lock and mb lock should be held.
func (mb *msgBlock) writeIndexInfoLocked() error {
	// HEADER: magic version msgs bytes fseq fts lseq lts ndel checksum
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

	// Open our FD if needed.
	if mb.ifd == nil {
		ifd, err := os.OpenFile(mb.ifn, os.O_CREATE|os.O_RDWR, defaultFilePerms)
		if err != nil {
			return err
		}
		mb.ifd = ifd
	}

	// Encrypt if needed.
	if mb.aek != nil {
		buf = mb.aek.Seal(buf[:0], mb.nonce, buf, nil)
	}

	mb.lwits = time.Now().UnixNano()

	var err error
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

	// Decrypt if needed.
	if mb.aek != nil {
		buf, err = mb.aek.Open(buf[:0], mb.nonce, buf, nil)
		if err != nil {
			return err
		}
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

// Fixed helper for iterating.
func subjectsEqual(a, b string) bool {
	return a == b
}

func subjectsAll(a, b string) bool {
	return true
}

func compareFn(subject string) func(string, string) bool {
	if subject == _EMPTY_ || subject == fwcs {
		return subjectsAll
	}
	if subjectHasWildcard(subject) {
		return subjectIsSubsetMatch
	}
	return subjectsEqual
}

// PurgeEx will remove messages based on subject filters, sequence and number of messages to keep.
// Will return the number of purged messages.
func (fs *fileStore) PurgeEx(subject string, sequence, keep uint64) (purged uint64, err error) {
	if subject == _EMPTY_ || subject == fwcs {
		if keep == 0 && (sequence == 0 || sequence == 1) {
			return fs.Purge()
		}
		if sequence > 1 {
			return fs.Compact(sequence)
		} else if keep > 0 {
			fs.mu.RLock()
			msgs, lseq := fs.state.Msgs, fs.state.LastSeq
			fs.mu.RUnlock()
			if keep >= msgs {
				return 0, nil
			}
			return fs.Compact(lseq - keep + 1)
		}
		return 0, nil
	}

	eq, wc := compareFn(subject), subjectHasWildcard(subject)
	var firstSeqNeedsUpdate bool

	// If we have a "keep" designation need to get full filtered state so we know how many to purge.
	var maxp uint64
	if keep > 0 {
		ss := fs.FilteredState(1, subject)
		if keep >= ss.Msgs {
			return 0, nil
		}
		maxp = ss.Msgs - keep
	}

	fs.mu.Lock()
	for _, mb := range fs.blks {
		mb.mu.Lock()
		t, f, l := mb.filteredPendingLocked(subject, wc, mb.first.seq)
		if t == 0 {
			mb.mu.Unlock()
			continue
		}

		var shouldExpire bool
		if mb.cacheNotLoaded() {
			mb.loadMsgsWithLock()
			shouldExpire = true
		}
		if sequence > 0 && sequence <= l {
			l = sequence - 1
		}

		for seq := f; seq <= l; seq++ {
			if sm, _ := mb.cacheLookup(seq); sm != nil && eq(sm.subj, subject) {
				rl := fileStoreMsgSize(sm.subj, sm.hdr, sm.msg)
				// Do fast in place remove.
				// Stats
				fs.state.Msgs--
				fs.state.Bytes -= rl
				mb.msgs--
				mb.bytes -= rl
				// FSS updates.
				fs.removePerSubject(sm.subj)
				mb.removeSeqPerSubject(sm.subj, seq)
				// Check for first message.
				if seq == mb.first.seq {
					mb.selectNextFirst()
					if mb.isEmpty() {
						fs.removeMsgBlock(mb)
						firstSeqNeedsUpdate = seq == fs.state.FirstSeq
					} else if seq == fs.state.FirstSeq {
						fs.state.FirstSeq = mb.first.seq // new one.
						fs.state.FirstTime = time.Unix(0, mb.first.ts).UTC()
					}
				} else {
					// Out of order delete.
					if mb.dmap == nil {
						mb.dmap = make(map[uint64]struct{})
					}
					mb.dmap[seq] = struct{}{}
				}
				purged++
				if maxp > 0 && purged >= maxp {
					break
				}
			}
		}
		// Expire if we were responsible for loading.
		if shouldExpire {
			// Expire this cache before moving on.
			mb.tryForceExpireCacheLocked()
		}
		mb.mu.Unlock()
		// Update our index info on disk.
		mb.writeIndexInfo()

		// Check if we should break out of top level too.
		if maxp > 0 && purged >= maxp {
			break
		}
	}
	if firstSeqNeedsUpdate {
		fs.selectNextFirst()
	}

	fs.mu.Unlock()
	return purged, nil
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
	mdir := filepath.Join(fs.fcfg.StoreDir, msgDir)
	pdir := filepath.Join(fs.fcfg.StoreDir, purgeDir)
	// If purge directory still exists then we need to wait
	// in place and remove since rename would fail.
	if _, err := os.Stat(pdir); err == nil {
		os.RemoveAll(pdir)
	}
	os.Rename(mdir, pdir)
	go os.RemoveAll(pdir)
	// Create new one.
	os.MkdirAll(mdir, defaultDirPerms)

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

	// Clear any per subject tracking.
	fs.psmc = make(map[string]uint64)

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

	var purged, bytes uint64

	// We have to delete interior messages.
	fs.mu.Lock()
	smb := fs.selectMsgBlock(seq)
	if smb == nil {
		fs.mu.Unlock()
		return 0, nil
	}
	if err := smb.loadMsgs(); err != nil {
		fs.mu.Unlock()
		return 0, err
	}

	// All msgblocks up to this one can be thrown away.
	var deleted int
	for _, mb := range fs.blks {
		if mb == smb {
			break
		}
		mb.mu.Lock()
		purged += mb.msgs
		bytes += mb.bytes
		mb.dirtyCloseWithRemove(true)
		mb.mu.Unlock()
		deleted++
	}

	smb.mu.Lock()
	for mseq := smb.first.seq; mseq < seq; mseq++ {
		sm, err := smb.cacheLookup(mseq)
		if err == errDeletedMsg {
			// Update dmap.
			if len(smb.dmap) > 0 {
				delete(smb.dmap, seq)
				if len(smb.dmap) == 0 {
					smb.dmap = nil
				}
			}
		} else if sm != nil {
			sz := fileStoreMsgSize(sm.subj, sm.hdr, sm.msg)
			smb.bytes -= sz
			bytes += sz
			smb.msgs--
			purged++
			// Update fss
			fs.removePerSubject(sm.subj)
			smb.removeSeqPerSubject(sm.subj, mseq)
		}
	}

	// Check if empty after processing, could happen if tail of messages are all deleted.
	isEmpty := smb.msgs == 0
	if isEmpty {
		smb.dirtyCloseWithRemove(true)
		// Update fs first here as well.
		fs.state.FirstSeq = smb.last.seq + 1
		fs.state.FirstTime = time.Time{}
		deleted++
	} else {
		// Update fs first seq and time.
		smb.first.seq = seq - 1 // Just for start condition for selectNextFirst.
		smb.selectNextFirst()
		fs.state.FirstSeq = smb.first.seq
		fs.state.FirstTime = time.Unix(0, smb.first.ts).UTC()
	}
	smb.mu.Unlock()

	if !isEmpty {
		// Make sure to write out our index info.
		smb.writeIndexInfo()
	}

	if deleted > 0 {
		// Update blks slice.
		fs.blks = copyMsgBlocks(fs.blks[deleted:])
	}

	// Update top level accounting.
	fs.state.Msgs -= purged
	fs.state.Bytes -= bytes

	cb := fs.scb
	fs.mu.Unlock()

	if cb != nil {
		cb(-int64(purged), -int64(bytes), 0, _EMPTY_)
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
	lsm, _, _ := nlmb.fetchMsg(seq)
	if lsm == nil {
		fs.mu.Unlock()
		return ErrInvalidSequence
	}

	// Set lmb to nlmb and make sure writeable.
	fs.lmb = nlmb
	if err := nlmb.enableForWriting(fs.fip); err != nil {
		return err
	}

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
	mb.removeIndexFileLocked()
}

func (mb *msgBlock) removeIndexFileLocked() {
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
			fs.blks = copyMsgBlocks(blks)
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
		if mb.sfn != _EMPTY_ {
			os.Remove(mb.sfn)
			mb.sfn = _EMPTY_
		}
		if mb.kfn != _EMPTY_ {
			os.Remove(mb.kfn)
		}
	}
}

// Remove a seq from the fss and select new first.
// Lock should be held.
func (mb *msgBlock) removeSeqPerSubject(subj string, seq uint64) {
	ss := mb.fss[subj]
	if ss == nil {
		return
	}
	if ss.Msgs == 1 {
		delete(mb.fss, subj)
		return
	}

	ss.Msgs--
	if seq != ss.First {
		return
	}

	// Here what we are removing is the first message.
	// If we only have one message left we can simply assign it to last.
	if ss.Msgs == 1 {
		ss.First = ss.Last
		return
	}

	// TODO(dlc) - Might want to optimize this.
	for tseq := seq + 1; tseq <= ss.Last; tseq++ {
		if sm, _ := mb.cacheLookup(tseq); sm != nil {
			if sm.subj == subj {
				ss.First = tseq
				return
			}
		}
	}
}

// generatePerSubjectInfo will generate the per subject info via the raw msg block.
func (mb *msgBlock) generatePerSubjectInfo() error {
	mb.mu.Lock()
	defer mb.mu.Unlock()

	var shouldExpire bool
	if mb.cacheNotLoaded() {
		if err := mb.loadMsgsWithLock(); err != nil {
			return err
		}
		shouldExpire = true
	}
	if mb.fss == nil {
		mb.fss = make(map[string]*SimpleState)
	}

	fseq, lseq := mb.first.seq, mb.last.seq
	for seq := fseq; seq <= lseq; seq++ {
		sm, err := mb.cacheLookup(seq)
		if err != nil {
			// Since we are walking by sequence we can ignore some errors that are benign to rebuilding our state.
			if err == ErrStoreMsgNotFound || err == errDeletedMsg {
				continue
			}
			if err == errNoCache {
				return nil
			}
			return err
		}
		if sm != nil && len(sm.subj) > 0 {
			if ss := mb.fss[sm.subj]; ss != nil {
				ss.Msgs++
				ss.Last = seq
			} else {
				mb.fss[sm.subj] = &SimpleState{Msgs: 1, First: seq, Last: seq}
			}
		}
	}
	if shouldExpire {
		// Expire this cache before moving on.
		mb.tryForceExpireCacheLocked()
	}
	return nil
}

// readPerSubjectInfo will attempt to restore the per subject information.
func (mb *msgBlock) readPerSubjectInfo() error {
	// Remove after processing regardless.
	defer os.Remove(mb.sfn)

	const (
		fileHashIndex = 16
		mbHashIndex   = 8
		minFileSize   = 24
	)

	buf, err := ioutil.ReadFile(mb.sfn)

	if err != nil || len(buf) < minFileSize || checkHeader(buf) != nil {
		return mb.generatePerSubjectInfo()
	}

	// Check that we did not have any bit flips.
	mb.hh.Reset()
	mb.hh.Write(buf[0 : len(buf)-fileHashIndex])
	fhash := buf[len(buf)-fileHashIndex : len(buf)-mbHashIndex]
	if checksum := mb.hh.Sum(nil); !bytes.Equal(checksum, fhash) {
		return mb.generatePerSubjectInfo()
	}

	if !bytes.Equal(buf[len(buf)-mbHashIndex:], mb.lchk[:]) {
		return mb.generatePerSubjectInfo()
	}

	fss := make(map[string]*SimpleState)

	bi := hdrLen
	readU64 := func() uint64 {
		if bi < 0 {
			return 0
		}
		num, n := binary.Uvarint(buf[bi:])
		if n <= 0 {
			bi = -1
			return 0
		}
		bi += n
		return num
	}

	for i, numEntries := uint64(0), readU64(); i < numEntries; i++ {
		lsubj := readU64()
		subj := buf[bi : bi+int(lsubj)]
		bi += int(lsubj)
		msgs, first, last := readU64(), readU64(), readU64()
		fss[string(subj)] = &SimpleState{Msgs: msgs, First: first, Last: last}
	}
	mb.mu.Lock()
	mb.fss = fss
	mb.mu.Unlock()
	return nil
}

// writePerSubjectInfo will write out per subject information if we are tracking per subject.
// Lock should be held.
func (mb *msgBlock) writePerSubjectInfo() error {
	// Raft groups do not have any subjects.
	if len(mb.fss) == 0 {
		return nil
	}
	var scratch [4 * binary.MaxVarintLen64]byte
	var b bytes.Buffer
	b.WriteByte(magic)
	b.WriteByte(version)
	n := binary.PutUvarint(scratch[0:], uint64(len(mb.fss)))
	b.Write(scratch[0:n])
	for subj, ss := range mb.fss {
		n := binary.PutUvarint(scratch[0:], uint64(len(subj)))
		b.Write(scratch[0:n])
		b.WriteString(subj)
		// Encode all three parts of our simple state into same scratch buffer.
		n = binary.PutUvarint(scratch[0:], ss.Msgs)
		n += binary.PutUvarint(scratch[n:], ss.First)
		n += binary.PutUvarint(scratch[n:], ss.Last)
		b.Write(scratch[0:n])
	}
	// Calculate hash for this information.
	mb.hh.Reset()
	mb.hh.Write(b.Bytes())
	b.Write(mb.hh.Sum(nil))
	// Now copy over checksum from the block itself, this allows us to know if we are in sync.
	b.Write(mb.lchk[:])

	return ioutil.WriteFile(mb.sfn, b.Bytes(), defaultFilePerms)
}

// Close the message block.
func (mb *msgBlock) close(sync bool) {
	if mb == nil {
		return
	}
	mb.mu.Lock()
	defer mb.mu.Unlock()

	if mb.closed {
		return
	}
	mb.closed = true

	// Check if we are tracking by subject.
	if mb.fss != nil {
		mb.writePerSubjectInfo()
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
		if mb.mfd != nil {
			mb.mfd.Close()
		}
		if mb.ifd != nil {
			mb.ifd.Close()
		}
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

	pdir := filepath.Join(fs.fcfg.StoreDir, purgeDir)
	// If purge directory still exists then we need to wait
	// in place and remove since rename would fail.
	if _, err := os.Stat(pdir); err == nil {
		os.RemoveAll(pdir)
	}

	if err := fs.Stop(); err != nil {
		return err
	}

	err := os.RemoveAll(fs.fcfg.StoreDir)
	if err == nil {
		return nil
	}
	ttl := time.Now().Add(time.Second)
	for time.Now().Before(ttl) {
		time.Sleep(10 * time.Millisecond)
		if err = os.RemoveAll(fs.fcfg.StoreDir); err == nil {
			return nil
		}
	}
	return err
}

// Lock should be held.
func (fs *fileStore) cancelSyncTimer() {
	if fs.syncTmr != nil {
		fs.syncTmr.Stop()
		fs.syncTmr = nil
	}
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

	fs.cancelSyncTimer()
	fs.cancelAgeChk()

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
	// Grab our general meta data.
	// We do this now instead of pulling from files since they could be encrypted.
	meta, err := json.Marshal(fs.cfg)
	if err != nil {
		fs.mu.Unlock()
		writeErr(fmt.Sprintf("Could not gather stream meta file: %v", err))
		return
	}
	fs.hh.Reset()
	fs.hh.Write(meta)
	sum := []byte(hex.EncodeToString(fs.hh.Sum(nil)))
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

	var bbuf []byte

	// Now do messages themselves.
	for _, mb := range blks {
		if mb.pendingWriteSize() > 0 {
			mb.flushPendingMsgs()
		}
		if mb.indexNeedsUpdate() {
			mb.writeIndexInfo()
		}
		mb.mu.Lock()
		buf, err := ioutil.ReadFile(mb.ifn)
		if err != nil {
			mb.mu.Unlock()
			writeErr(fmt.Sprintf("Could not read message block [%d] index file: %v", mb.index, err))
			return
		}
		// Check for encryption.
		if mb.aek != nil && len(buf) > 0 {
			buf, err = mb.aek.Open(buf[:0], mb.nonce, buf, nil)
			if err != nil {
				mb.mu.Unlock()
				writeErr(fmt.Sprintf("Could not decrypt message block [%d] index file: %v", mb.index, err))
				return
			}
		}
		if writeFile(msgPre+fmt.Sprintf(indexScan, mb.index), buf) != nil {
			mb.mu.Unlock()
			return
		}
		// We could stream but don't want to hold the lock and prevent changes, so just read in and
		// release the lock for now.
		bbuf, err = mb.loadBlock(bbuf)
		if err != nil {
			mb.mu.Unlock()
			writeErr(fmt.Sprintf("Could not read message block [%d]: %v", mb.index, err))
			return
		}
		// Check for encryption.
		if mb.bek != nil && len(bbuf) > 0 {
			rbek, err := chacha20.NewUnauthenticatedCipher(mb.seed, mb.nonce)
			if err != nil {
				mb.mu.Unlock()
				writeErr(fmt.Sprintf("Could not create encryption key for message block [%d]: %v", mb.index, err))
				return
			}
			rbek.XORKeyStream(bbuf, bbuf)
		}
		// Make sure we snapshot the per subject info.
		mb.writePerSubjectInfo()
		buf, err = ioutil.ReadFile(mb.sfn)
		// If not there that is ok and not fatal.
		if err == nil && writeFile(msgPre+fmt.Sprintf(fssScan, mb.index), buf) != nil {
			mb.mu.Unlock()
			return
		}
		mb.mu.Unlock()
		// Do this one unlocked.
		if writeFile(msgPre+fmt.Sprintf(blkScan, mb.index), bbuf) != nil {
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
		// Grab our general meta data.
		// We do this now instead of pulling from files since they could be encrypted.
		meta, err := json.Marshal(o.cfg)
		if err != nil {
			o.mu.Unlock()
			writeErr(fmt.Sprintf("Could not gather consumer meta file for %q: %v", o.name, err))
			return
		}
		o.hh.Reset()
		o.hh.Write(meta)
		sum := []byte(hex.EncodeToString(o.hh.Sum(nil)))

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
		if writeFile(filepath.Join(odirPre, JetStreamMetaFile), meta) != nil {
			return
		}
		if writeFile(filepath.Join(odirPre, JetStreamMetaFileSum), sum) != nil {
			return
		}
		writeFile(filepath.Join(odirPre, consumerState), state)
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

	// We can add to our stream while snapshotting but not delete anything.
	state := fs.State()

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
	prf     keyGen
	aek     cipher.AEAD
	name    string
	odir    string
	ifn     string
	hh      hash.Hash64
	state   ConsumerState
	fch     chan struct{}
	qch     chan struct{}
	flusher bool
	writing bool
	dirty   bool
	closed  bool
}

func (fs *fileStore) ConsumerStore(name string, cfg *ConsumerConfig) (ConsumerStore, error) {
	if fs == nil {
		return nil, fmt.Errorf("filestore is nil")
	}
	if fs.isClosed() {
		return nil, ErrStoreClosed
	}
	if cfg == nil || name == _EMPTY_ {
		return nil, fmt.Errorf("bad consumer config")
	}
	odir := filepath.Join(fs.fcfg.StoreDir, consumerDir, name)
	if err := os.MkdirAll(odir, defaultDirPerms); err != nil {
		return nil, fmt.Errorf("could not create consumer directory - %v", err)
	}
	csi := &FileConsumerInfo{ConsumerConfig: *cfg}
	o := &consumerFileStore{
		fs:   fs,
		cfg:  csi,
		prf:  fs.prf,
		name: name,
		odir: odir,
		ifn:  filepath.Join(odir, consumerState),
	}
	key := sha256.Sum256([]byte(fs.cfg.Name + "/" + name))
	hh, err := highwayhash.New64(key[:])
	if err != nil {
		return nil, fmt.Errorf("could not create hash: %v", err)
	}
	o.hh = hh

	// Check for encryption.
	if o.prf != nil {
		if ekey, err := ioutil.ReadFile(filepath.Join(odir, JetStreamMetaFileKey)); err == nil {
			// Recover key encryption key.
			rb, err := fs.prf([]byte(fs.cfg.Name + tsep + o.name))
			if err != nil {
				return nil, err
			}
			kek, err := chacha20poly1305.NewX(rb)
			if err != nil {
				return nil, err
			}
			ns := kek.NonceSize()
			seed, err := kek.Open(nil, ekey[:ns], ekey[ns:], nil)
			if err != nil {
				return nil, err
			}
			if o.aek, err = chacha20poly1305.NewX(seed); err != nil {
				return nil, err
			}
		}
	}

	// Write our meta data iff does not exist.
	meta := filepath.Join(odir, JetStreamMetaFile)
	if _, err := os.Stat(meta); err != nil && os.IsNotExist(err) {
		csi.Created = time.Now().UTC()
		if err := o.writeConsumerMeta(); err != nil {
			return nil, err
		}
	}

	// If we expect to be encrypted check that what we are restoring is not plaintext.
	// This can happen on snapshot restores or conversions.
	if o.prf != nil {
		keyFile := filepath.Join(odir, JetStreamMetaFileKey)
		if _, err := os.Stat(keyFile); err != nil && os.IsNotExist(err) {
			if err := o.writeConsumerMeta(); err != nil {
				return nil, err
			}
			// Redo the state file as well here if we have one and we can tell it was plaintext.
			if buf, err := ioutil.ReadFile(o.ifn); err == nil {
				if _, err := decodeConsumerState(buf); err == nil {
					if err := ioutil.WriteFile(o.ifn, o.encryptState(buf), defaultFilePerms); err != nil {
						return nil, err
					}
				}
			}
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
	o.dirty = true
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

	// Maintain approximately 10 updates per second per consumer under load.
	const minTime = 100 * time.Millisecond
	var lastWrite time.Time
	var dt *time.Timer

	setDelayTimer := func(addWait time.Duration) {
		if dt == nil {
			dt = time.NewTimer(addWait)
			return
		}
		if !dt.Stop() {
			select {
			case <-dt.C:
			default:
			}
		}
		dt.Reset(addWait)
	}

	for {
		select {
		case <-fch:
			if ts := time.Since(lastWrite); ts < minTime {
				setDelayTimer(minTime - ts)
				select {
				case <-dt.C:
				case <-qch:
					return
				}
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
			lastWrite = time.Now()
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

	// On restarts the old leader may get a replay from the raft logs that are old.
	if dseq <= o.state.AckFloor.Consumer {
		return nil
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
				p.Sequence, p.Timestamp = dseq, ts
			}
		} else {
			// Add to pending.
			o.state.Pending[sseq] = &Pending{dseq, ts}
		}
		// Update delivered as needed.
		if dseq > o.state.Delivered.Consumer {
			o.state.Delivered.Consumer = dseq
		}
		if sseq > o.state.Delivered.Stream {
			o.state.Delivered.Stream = sseq
		}

		if dc > 1 {
			if o.state.Redelivered == nil {
				o.state.Redelivered = make(map[uint64]uint64)
			}
			o.state.Redelivered[sseq] = dc - 1
		}
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
	if len(o.state.Pending) == 0 || o.state.Pending[sseq] == nil {
		return ErrStoreMsgNotFound
	}

	// On restarts the old leader may get a replay from the raft logs that are old.
	if dseq <= o.state.AckFloor.Consumer {
		return nil
	}

	// Check for AckAll here.
	if o.cfg.AckPolicy == AckAll {
		sgap := sseq - o.state.AckFloor.Stream
		o.state.AckFloor.Consumer = dseq
		o.state.AckFloor.Stream = sseq
		for seq := sseq; seq > sseq-sgap; seq-- {
			delete(o.state.Pending, seq)
			if len(o.state.Redelivered) > 0 {
				delete(o.state.Redelivered, seq)
			}
		}
		o.kickFlusher()
		return nil
	}

	// AckExplicit

	// First delete from our pending state.
	if p, ok := o.state.Pending[sseq]; ok {
		delete(o.state.Pending, sseq)
		dseq = p.Sequence // Use the original.
	}
	// Now remove from redelivered.
	if len(o.state.Redelivered) > 0 {
		delete(o.state.Redelivered, sseq)
	}

	if len(o.state.Pending) == 0 {
		o.state.AckFloor.Consumer = o.state.Delivered.Consumer
		o.state.AckFloor.Stream = o.state.Delivered.Stream
	} else if dseq == o.state.AckFloor.Consumer+1 {
		first := o.state.AckFloor.Consumer == 0
		o.state.AckFloor.Consumer = dseq
		o.state.AckFloor.Stream = sseq

		if !first && o.state.Delivered.Consumer > dseq {
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

func (o *consumerFileStore) UpdateConfig(cfg *ConsumerConfig) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	// This is mostly unchecked here. We are assuming the upper layers have done sanity checking.
	csi := o.cfg
	csi.ConsumerConfig = *cfg

	return o.writeConsumerMeta()
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

	// Check to see if this is an outdated update.
	if state.Delivered.Consumer < o.state.Delivered.Consumer {
		o.mu.Unlock()
		return fmt.Errorf("old update ignored")
	}

	o.state.Delivered = state.Delivered
	o.state.AckFloor = state.AckFloor
	o.state.Pending = pending
	o.state.Redelivered = redelivered
	o.kickFlusher()
	o.mu.Unlock()

	return nil
}

// Will encrypt the state with our asset key. Will be a no-op if encryption not enabled.
// Lock should be held.
func (o *consumerFileStore) encryptState(buf []byte) []byte {
	if o.aek == nil {
		return buf
	}
	// TODO(dlc) - Optimize on space usage a bit?
	nonce := make([]byte, o.aek.NonceSize(), o.aek.NonceSize()+len(buf)+o.aek.Overhead())
	mrand.Read(nonce)
	return o.aek.Seal(nonce, nonce, buf, nil)
}

// Used to limit number of disk IO calls in flight since they could all be blocking an OS thread.
// https://github.com/nats-io/nats-server/issues/2742
var dios chan struct{}

// Used to setup our simplistic counting semaphore using buffered channels.
// golang.org's semaphore seemed a bit heavy.
func init() {
	// Minimum for blocking disk IO calls.
	const minNIO = 4
	nIO := runtime.GOMAXPROCS(0)
	if nIO < minNIO {
		nIO = minNIO
	}
	dios = make(chan struct{}, nIO)
	// Fill it up to start.
	for i := 0; i < nIO; i++ {
		dios <- struct{}{}
	}
}

func (o *consumerFileStore) writeState(buf []byte) error {
	// Check if we have the index file open.
	o.mu.Lock()
	if o.writing || len(buf) == 0 {
		o.mu.Unlock()
		return nil
	}

	// Check on encryption.
	if o.aek != nil {
		buf = o.encryptState(buf)
	}

	o.writing = true
	o.dirty = false
	ifn := o.ifn
	o.mu.Unlock()

	// Lock not held here but we do limit number of outstanding calls that could block OS threads.
	<-dios
	err := ioutil.WriteFile(ifn, buf, defaultFilePerms)
	dios <- struct{}{}

	o.mu.Lock()
	if err != nil {
		o.dirty = true
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
	meta := filepath.Join(cfs.odir, JetStreamMetaFile)
	if _, err := os.Stat(meta); err != nil && !os.IsNotExist(err) {
		return err
	}

	if cfs.prf != nil && cfs.aek == nil {
		fs := cfs.fs
		key, _, _, encrypted, err := fs.genEncryptionKeys(fs.cfg.Name + tsep + cfs.name)
		if err != nil {
			return err
		}
		cfs.aek = key
		keyFile := filepath.Join(cfs.odir, JetStreamMetaFileKey)
		if _, err := os.Stat(keyFile); err != nil && !os.IsNotExist(err) {
			return err
		}
		if err := ioutil.WriteFile(keyFile, encrypted, defaultFilePerms); err != nil {
			return err
		}
	}

	b, err := json.Marshal(cfs.cfg)
	if err != nil {
		return err
	}
	// Encrypt if needed.
	if cfs.aek != nil {
		nonce := make([]byte, cfs.aek.NonceSize(), cfs.aek.NonceSize()+len(b)+cfs.aek.Overhead())
		mrand.Read(nonce)
		b = cfs.aek.Seal(nonce, nonce, b, nil)
	}

	if err := ioutil.WriteFile(meta, b, defaultFilePerms); err != nil {
		return err
	}
	cfs.hh.Reset()
	cfs.hh.Write(b)
	checksum := hex.EncodeToString(cfs.hh.Sum(nil))
	sum := filepath.Join(cfs.odir, JetStreamMetaFileSum)
	if err := ioutil.WriteFile(sum, []byte(checksum), defaultFilePerms); err != nil {
		return err
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

// Consumer version.
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

// Type returns the type of the underlying store.
func (o *consumerFileStore) Type() StorageType { return FileStorage }

// State retrieves the state from the state file.
// This is not expected to be called in high performance code, only on startup.
func (o *consumerFileStore) State() (*ConsumerState, error) {
	o.mu.Lock()
	defer o.mu.Unlock()

	state := &ConsumerState{}

	// See if we have a running state or if we need to read in from disk.
	if o.state.Delivered.Consumer != 0 {
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

	// Check on encryption.
	if o.aek != nil {
		ns := o.aek.NonceSize()
		buf, err = o.aek.Open(nil, buf[:ns], buf[ns:], nil)
		if err != nil {
			return nil, err
		}
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
		if state.AckFloor.Consumer > 1 {
			state.Delivered.Consumer += state.AckFloor.Consumer - 1
		}
		if state.AckFloor.Stream > 1 {
			state.Delivered.Stream += state.AckFloor.Stream - 1
		}
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
			if ts == -1 {
				return nil, errCorruptState
			}
			// Adjust seq back.
			sseq += state.AckFloor.Stream
			if sseq == 0 {
				return nil, errCorruptState
			}
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
			if seq, n := readSeq(), readCount(); seq > 0 && n > 0 {
				// Adjust seq back.
				seq += state.AckFloor.Stream
				state.Redelivered[seq] = n
			}
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
	var buf []byte

	if o.dirty {
		// Make sure to write this out..
		if buf, err = o.encodeState(); err == nil && len(buf) > 0 {
			if o.aek != nil {
				buf = o.encryptState(buf)
			}
		}
	}

	o.odir = _EMPTY_
	o.closed = true
	ifn, fs := o.ifn, o.fs
	o.mu.Unlock()

	fs.removeConsumer(o)

	if len(buf) > 0 {
		o.waitOnFlusher()
		<-dios
		err = ioutil.WriteFile(ifn, buf, defaultFilePerms)
		dios <- struct{}{}
	}
	return err
}

func (o *consumerFileStore) waitOnFlusher() {
	if !o.inFlusher() {
		return
	}

	timeout := time.Now().Add(100 * time.Millisecond)
	for time.Now().Before(timeout) {
		if !o.inFlusher() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// Delete the consumer.
func (o *consumerFileStore) Delete() error {
	return o.delete(false)
}

func (o *consumerFileStore) StreamDelete() error {
	return o.delete(true)
}

func (o *consumerFileStore) delete(streamDeleted bool) error {
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
	odir := o.odir
	o.odir = _EMPTY_
	o.closed = true
	fs := o.fs
	o.mu.Unlock()

	// If our stream was not deleted this will remove the directories.
	if odir != _EMPTY_ && !streamDeleted {
		<-dios
		err = os.RemoveAll(odir)
		dios <- struct{}{}
	}

	if !streamDeleted {
		fs.removeConsumer(o)
	}

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
	tdir := filepath.Join(storeDir, tmplsDir)
	key := sha256.Sum256([]byte("templates"))
	hh, err := highwayhash.New64(key[:])
	if err != nil {
		return nil
	}
	return &templateFileStore{dir: tdir, hh: hh}
}

func (ts *templateFileStore) Store(t *streamTemplate) error {
	dir := filepath.Join(ts.dir, t.Name)
	if err := os.MkdirAll(dir, defaultDirPerms); err != nil {
		return fmt.Errorf("could not create templates storage directory for %q- %v", t.Name, err)
	}
	meta := filepath.Join(dir, JetStreamMetaFile)
	if _, err := os.Stat(meta); (err != nil && !os.IsNotExist(err)) || err == nil {
		return err
	}
	t.mu.Lock()
	b, err := json.Marshal(t)
	t.mu.Unlock()
	if err != nil {
		return err
	}
	if err := ioutil.WriteFile(meta, b, defaultFilePerms); err != nil {
		return err
	}
	// FIXME(dlc) - Do checksum
	ts.hh.Reset()
	ts.hh.Write(b)
	checksum := hex.EncodeToString(ts.hh.Sum(nil))
	sum := filepath.Join(dir, JetStreamMetaFileSum)
	if err := ioutil.WriteFile(sum, []byte(checksum), defaultFilePerms); err != nil {
		return err
	}
	return nil
}

func (ts *templateFileStore) Delete(t *streamTemplate) error {
	return os.RemoveAll(filepath.Join(ts.dir, t.Name))
}
