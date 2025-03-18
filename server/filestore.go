// Copyright 2019-2025 The NATS Authors
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
	"crypto/aes"
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
	"io/fs"
	"math"
	mrand "math/rand"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/klauspost/compress/s2"
	"github.com/minio/highwayhash"
	"github.com/nats-io/nats-server/v2/server/avl"
	"github.com/nats-io/nats-server/v2/server/stree"
	"github.com/nats-io/nats-server/v2/server/thw"
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
	// SubjectStateExpire is how long with no activity until we expire a msg block's subject state.
	SubjectStateExpire time.Duration
	// SyncInterval is how often we sync to disk in the background.
	SyncInterval time.Duration
	// SyncAlways is when the stream should sync all data writes.
	SyncAlways bool
	// AsyncFlush allows async flush to batch write operations.
	AsyncFlush bool
	// Cipher is the cipher to use when encrypting.
	Cipher StoreCipher
	// Compression is the algorithm to use when compressing.
	Compression StoreCompression

	// Internal reference to our server.
	srv *Server
}

// FileStreamInfo allows us to remember created time.
type FileStreamInfo struct {
	Created time.Time
	StreamConfig
}

type StoreCipher int

const (
	ChaCha StoreCipher = iota
	AES
	NoCipher
)

func (cipher StoreCipher) String() string {
	switch cipher {
	case ChaCha:
		return "ChaCha20-Poly1305"
	case AES:
		return "AES-GCM"
	case NoCipher:
		return "None"
	default:
		return "Unknown StoreCipher"
	}
}

type StoreCompression uint8

const (
	NoCompression StoreCompression = iota
	S2Compression
)

func (alg StoreCompression) String() string {
	switch alg {
	case NoCompression:
		return "None"
	case S2Compression:
		return "S2"
	default:
		return "Unknown StoreCompression"
	}
}

func (alg StoreCompression) MarshalJSON() ([]byte, error) {
	var str string
	switch alg {
	case S2Compression:
		str = "s2"
	case NoCompression:
		str = "none"
	default:
		return nil, fmt.Errorf("unknown compression algorithm")
	}
	return json.Marshal(str)
}

func (alg *StoreCompression) UnmarshalJSON(b []byte) error {
	var str string
	if err := json.Unmarshal(b, &str); err != nil {
		return err
	}
	switch str {
	case "s2":
		*alg = S2Compression
	case "none":
		*alg = NoCompression
	default:
		return fmt.Errorf("unknown compression algorithm")
	}
	return nil
}

// File ConsumerInfo is used for creating consumer stores.
type FileConsumerInfo struct {
	Created time.Time
	Name    string
	ConsumerConfig
}

// Default file and directory permissions.
const (
	defaultDirPerms  = os.FileMode(0700)
	defaultFilePerms = os.FileMode(0600)
)

type psi struct {
	total uint64
	fblk  uint32
	lblk  uint32
}

type fileStore struct {
	srv         *Server
	mu          sync.RWMutex
	state       StreamState
	tombs       []uint64
	ld          *LostStreamData
	scb         StorageUpdateHandler
	sdmcb       SubjectDeleteMarkerUpdateHandler
	ageChk      *time.Timer
	syncTmr     *time.Timer
	cfg         FileStreamInfo
	fcfg        FileStoreConfig
	prf         keyGen
	oldprf      keyGen
	aek         cipher.AEAD
	lmb         *msgBlock
	blks        []*msgBlock
	bim         map[uint32]*msgBlock
	psim        *stree.SubjectTree[psi]
	tsl         int
	adml        int
	hh          hash.Hash64
	qch         chan struct{}
	fsld        chan struct{}
	cmu         sync.RWMutex
	cfs         []ConsumerStore
	sips        int
	dirty       int
	closing     bool
	closed      bool
	fip         bool
	receivedAny bool
	firstMoved  bool
	ttls        *thw.HashWheel
	ttlseq      uint64 // How up-to-date is the `ttls` THW?
	markers     []string
}

// Represents a message store block and its data.
type msgBlock struct {
	// Here for 32bit systems and atomic.
	first      msgId
	last       msgId
	mu         sync.RWMutex
	fs         *fileStore
	aek        cipher.AEAD
	bek        cipher.Stream
	seed       []byte
	nonce      []byte
	mfn        string
	mfd        *os.File
	cmp        StoreCompression // Effective compression at the time of loading the block
	liwsz      int64
	index      uint32
	bytes      uint64 // User visible bytes count.
	rbytes     uint64 // Total bytes (raw) including deleted. Used for rolling to new blk.
	cbytes     uint64 // Bytes count after last compaction. 0 if no compaction happened yet.
	msgs       uint64 // User visible message count.
	fss        *stree.SubjectTree[SimpleState]
	kfn        string
	lwts       int64
	llts       int64
	lrts       int64
	lsts       int64
	llseq      uint64
	hh         hash.Hash64
	cache      *cache
	cloads     uint64
	cexp       time.Duration
	fexp       time.Duration
	ctmr       *time.Timer
	werr       error
	dmap       avl.SequenceSet
	fch        chan struct{}
	qch        chan struct{}
	lchk       [8]byte
	loading    bool
	flusher    bool
	noTrack    bool
	needSync   bool
	syncAlways bool
	noCompact  bool
	closed     bool
	ttls       uint64 // How many msgs have TTLs?

	// Used to mock write failures.
	mockWriteErr bool
}

// Write through caching layer that is also used on loading messages.
type cache struct {
	buf  []byte
	off  int
	wp   int
	idx  []uint32
	lrl  uint32
	fseq uint64
	nra  bool
}

type msgId struct {
	seq uint64
	ts  int64
}

const (
	// Magic is used to identify the file store files.
	magic = uint8(22)
	// Version
	version = uint8(1)
	// New IndexInfo Version
	newVersion = uint8(2)
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
	// suffix of a block file
	blkSuffix = ".blk"
	// used for compacted blocks that are staged.
	newScan = "%d.new"
	// used to scan index file names.
	indexScan = "%d.idx"
	// used to store our block encryption key.
	keyScan = "%d.key"
	// to look for orphans
	keyScanAll = "*.key"
	// This is where we keep state on consumers.
	consumerDir = "obs"
	// Index file for a consumer.
	consumerState = "o.dat"
	// The suffix that will be given to a new temporary block during compression.
	compressTmpSuffix = ".tmp"
	// This is where we keep state on templates.
	tmplsDir = "templates"
	// Maximum size of a write buffer we may consider for re-use.
	maxBufReuse = 2 * 1024 * 1024
	// default cache buffer expiration
	defaultCacheBufferExpiration = 10 * time.Second
	// default sync interval
	defaultSyncInterval = 2 * time.Minute
	// default idle timeout to close FDs.
	closeFDsIdle = 30 * time.Second
	// default expiration time for mb.fss when idle.
	defaultFssExpiration = 2 * time.Minute
	// coalesceMinimum
	coalesceMinimum = 16 * 1024
	// maxFlushWait is maximum we will wait to gather messages to flush.
	maxFlushWait = 8 * time.Millisecond

	// Metafiles for streams and consumers.
	JetStreamMetaFile    = "meta.inf"
	JetStreamMetaFileSum = "meta.sum"
	JetStreamMetaFileKey = "meta.key"

	// This is the full snapshotted state for the stream.
	streamStreamStateFile = "index.db"

	// This is the encoded time hash wheel for TTLs.
	ttlStreamStateFile = "thw.db"

	// AEK key sizes
	minMetaKeySize = 64
	minBlkKeySize  = 64

	// Default stream block size.
	defaultLargeBlockSize = 8 * 1024 * 1024 // 8MB
	// Default for workqueue or interest based.
	defaultMediumBlockSize = 4 * 1024 * 1024 // 4MB
	// For smaller reuse buffers. Usually being generated during contention on the lead write buffer.
	// E.g. mirrors/sources etc.
	defaultSmallBlockSize = 1 * 1024 * 1024 // 1MB
	// Maximum size for the encrypted head block.
	maximumEncryptedBlockSize = 2 * 1024 * 1024 // 2MB
	// Default for KV based
	defaultKVBlockSize = defaultMediumBlockSize
	// max block size for now.
	maxBlockSize = defaultLargeBlockSize
	// Compact minimum threshold.
	compactMinimum = 2 * 1024 * 1024 // 2MB
	// FileStoreMinBlkSize is minimum size we will do for a blk size.
	FileStoreMinBlkSize = 32 * 1000 // 32kib
	// FileStoreMaxBlkSize is maximum size we will do for a blk size.
	FileStoreMaxBlkSize = maxBlockSize
	// Check for bad record length value due to corrupt data.
	rlBadThresh = 32 * 1024 * 1024
	// Checksum size for hash for msg records.
	recordHashSize = 8
)

func newFileStore(fcfg FileStoreConfig, cfg StreamConfig) (*fileStore, error) {
	return newFileStoreWithCreated(fcfg, cfg, time.Now().UTC(), nil, nil)
}

func newFileStoreWithCreated(fcfg FileStoreConfig, cfg StreamConfig, created time.Time, prf, oldprf keyGen) (*fileStore, error) {
	if cfg.Name == _EMPTY_ {
		return nil, fmt.Errorf("name required")
	}
	if cfg.Storage != FileStorage {
		return nil, fmt.Errorf("fileStore requires file storage type in config")
	}
	// Default values.
	if fcfg.BlockSize == 0 {
		fcfg.BlockSize = dynBlkSize(cfg.Retention, cfg.MaxBytes, prf != nil)
	}
	if fcfg.BlockSize > maxBlockSize {
		return nil, fmt.Errorf("filestore max block size is %s", friendlyBytes(maxBlockSize))
	}
	if fcfg.CacheExpire == 0 {
		fcfg.CacheExpire = defaultCacheBufferExpiration
	}
	if fcfg.SubjectStateExpire == 0 {
		fcfg.SubjectStateExpire = defaultFssExpiration
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
	tmpfile, err := os.CreateTemp(fcfg.StoreDir, "_test_")
	if err != nil {
		return nil, fmt.Errorf("storage directory is not writable")
	}

	tmpfile.Close()
	<-dios
	os.Remove(tmpfile.Name())
	dios <- struct{}{}

	fs := &fileStore{
		fcfg:   fcfg,
		psim:   stree.NewSubjectTree[psi](),
		bim:    make(map[uint32]*msgBlock),
		cfg:    FileStreamInfo{Created: created, StreamConfig: cfg},
		prf:    prf,
		oldprf: oldprf,
		qch:    make(chan struct{}),
		fsld:   make(chan struct{}),
		srv:    fcfg.srv,
	}

	// Only create a THW if we're going to allow TTLs.
	if cfg.AllowMsgTTL {
		fs.ttls = thw.NewHashWheel()
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

	keyFile := filepath.Join(fs.fcfg.StoreDir, JetStreamMetaFileKey)
	// Make sure we do not have an encrypted store underneath of us but no main key.
	if fs.prf == nil {
		if _, err := os.Stat(keyFile); err == nil {
			return nil, errNoMainKey
		}
	}

	// Attempt to recover our state.
	err = fs.recoverFullState()
	if err != nil {
		if !os.IsNotExist(err) {
			fs.warn("Recovering stream state from index errored: %v", err)
		}
		// Hold onto state
		prior := fs.state
		// Reset anything that could have been set from above.
		fs.state = StreamState{}
		fs.psim, fs.tsl = fs.psim.Empty(), 0
		fs.bim = make(map[uint32]*msgBlock)
		fs.blks = nil
		fs.tombs = nil

		// Recover our message state the old way
		if err := fs.recoverMsgs(); err != nil {
			return nil, err
		}

		// Check if our prior state remembers a last sequence past where we can see.
		if fs.ld != nil && prior.LastSeq > fs.state.LastSeq {
			fs.state.LastSeq, fs.state.LastTime = prior.LastSeq, prior.LastTime
			if fs.state.Msgs == 0 {
				fs.state.FirstSeq = fs.state.LastSeq + 1
				fs.state.FirstTime = time.Time{}
			}
			if _, err := fs.newMsgBlockForWrite(); err == nil {
				if err = fs.writeTombstone(prior.LastSeq, prior.LastTime.UnixNano()); err != nil {
					return nil, err
				}
			} else {
				return nil, err
			}
		}
		// Since we recovered here, make sure to kick ourselves to write out our stream state.
		fs.dirty++
	}

	// See if we can bring back our TTL timed hash wheel state from disk.
	if cfg.AllowMsgTTL {
		if err = fs.recoverTTLState(); err != nil && !os.IsNotExist(err) {
			fs.warn("Recovering TTL state from index errored: %v", err)
		}
	}

	// Also make sure we get rid of old idx and fss files on return.
	// Do this in separate go routine vs inline and at end of processing.
	defer func() {
		go fs.cleanupOldMeta()
	}()

	// Lock while we do enforcements and removals.
	fs.mu.Lock()

	// Check if we have any left over tombstones to process.
	if len(fs.tombs) > 0 {
		for _, seq := range fs.tombs {
			fs.removeMsg(seq, false, true, false)
			fs.removeFromLostData(seq)
		}
		// Not needed after this phase.
		fs.tombs = nil
	}

	// Limits checks and enforcement.
	fs.enforceMsgLimit()
	fs.enforceBytesLimit()

	// Do age checks too, make sure to call in place.
	if fs.cfg.MaxAge != 0 {
		err := fs.expireMsgsOnRecover()
		if isPermissionError(err) {
			return nil, err
		}
		fs.startAgeChk()
	}

	// If we have max msgs per subject make sure the is also enforced.
	if fs.cfg.MaxMsgsPer > 0 {
		fs.enforceMsgPerSubjectLimit(false)
	}

	// Grab first sequence for check below while we have lock.
	firstSeq := fs.state.FirstSeq
	fs.mu.Unlock()

	// If the stream has an initial sequence number then make sure we
	// have purged up until that point. We will do this only if the
	// recovered first sequence number is before our configured first
	// sequence. Need to do this locked as by now the age check timer
	// has started.
	if cfg.FirstSeq > 0 && firstSeq <= cfg.FirstSeq {
		if _, err := fs.purge(cfg.FirstSeq, true); err != nil {
			return nil, err
		}
	}

	// Write our meta data if it does not exist or is zero'd out.
	meta := filepath.Join(fcfg.StoreDir, JetStreamMetaFile)
	fi, err := os.Stat(meta)
	if err != nil && os.IsNotExist(err) || fi != nil && fi.Size() == 0 {
		if err := fs.writeStreamMeta(); err != nil {
			return nil, err
		}
	}

	// If we expect to be encrypted check that what we are restoring is not plaintext.
	// This can happen on snapshot restores or conversions.
	if fs.prf != nil {
		if _, err := os.Stat(keyFile); err != nil && os.IsNotExist(err) {
			if err := fs.writeStreamMeta(); err != nil {
				return nil, err
			}
		}
	}

	// Setup our sync timer.
	fs.setSyncTimer()

	// Spin up the go routine that will write out our full state stream index.
	go fs.flushStreamStateLoop(fs.qch, fs.fsld)

	return fs, nil
}

// Lock all existing message blocks.
// Lock held on entry.
func (fs *fileStore) lockAllMsgBlocks() {
	for _, mb := range fs.blks {
		mb.mu.Lock()
	}
}

// Unlock all existing message blocks.
// Lock held on entry.
func (fs *fileStore) unlockAllMsgBlocks() {
	for _, mb := range fs.blks {
		mb.mu.Unlock()
	}
}

func (fs *fileStore) UpdateConfig(cfg *StreamConfig) error {
	start := time.Now()
	defer func() {
		if took := time.Since(start); took > time.Minute {
			fs.warn("UpdateConfig took %v", took.Round(time.Millisecond))
		}
	}()

	if fs.isClosed() {
		return ErrStoreClosed
	}
	if cfg.Name == _EMPTY_ {
		return fmt.Errorf("name required")
	}
	if cfg.Storage != FileStorage {
		return fmt.Errorf("fileStore requires file storage type in config")
	}
	if cfg.MaxMsgsPer < -1 {
		cfg.MaxMsgsPer = -1
	}

	fs.mu.Lock()
	new_cfg := FileStreamInfo{Created: fs.cfg.Created, StreamConfig: *cfg}
	old_cfg := fs.cfg
	// The reference story has changed here, so this full msg block lock
	// may not be needed.
	fs.lockAllMsgBlocks()
	fs.cfg = new_cfg
	fs.unlockAllMsgBlocks()
	if err := fs.writeStreamMeta(); err != nil {
		fs.lockAllMsgBlocks()
		fs.cfg = old_cfg
		fs.unlockAllMsgBlocks()
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

	if fs.cfg.MaxMsgsPer > 0 && (old_cfg.MaxMsgsPer == 0 || fs.cfg.MaxMsgsPer < old_cfg.MaxMsgsPer) {
		fs.enforceMsgPerSubjectLimit(true)
	}
	fs.mu.Unlock()

	if cfg.MaxAge != 0 {
		fs.expireMsgs()
	}
	return nil
}

func dynBlkSize(retention RetentionPolicy, maxBytes int64, encrypted bool) uint64 {
	if maxBytes > 0 {
		blkSize := (maxBytes / 4) + 1 // (25% overhead)
		// Round up to nearest 100
		if m := blkSize % 100; m != 0 {
			blkSize += 100 - m
		}
		if blkSize <= FileStoreMinBlkSize {
			blkSize = FileStoreMinBlkSize
		} else if blkSize >= FileStoreMaxBlkSize {
			blkSize = FileStoreMaxBlkSize
		} else {
			blkSize = defaultMediumBlockSize
		}
		if encrypted && blkSize > maximumEncryptedBlockSize {
			// Notes on this below.
			blkSize = maximumEncryptedBlockSize
		}
		return uint64(blkSize)
	}

	switch {
	case encrypted:
		// In the case of encrypted stores, large blocks can result in worsened perf
		// since many writes on disk involve re-encrypting the entire block. For now,
		// we will enforce a cap on the block size when encryption is enabled to avoid
		// this.
		return maximumEncryptedBlockSize
	case retention == LimitsPolicy:
		// TODO(dlc) - Make the blocksize relative to this if set.
		return defaultLargeBlockSize
	default:
		// TODO(dlc) - Make the blocksize relative to this if set.
		return defaultMediumBlockSize
	}
}

func genEncryptionKey(sc StoreCipher, seed []byte) (ek cipher.AEAD, err error) {
	if sc == ChaCha {
		ek, err = chacha20poly1305.NewX(seed)
	} else if sc == AES {
		block, e := aes.NewCipher(seed)
		if e != nil {
			return nil, e
		}
		ek, err = cipher.NewGCMWithNonceSize(block, block.BlockSize())
	} else {
		err = errUnknownCipher
	}
	return ek, err
}

// Generate an asset encryption key from the context and server PRF.
func (fs *fileStore) genEncryptionKeys(context string) (aek cipher.AEAD, bek cipher.Stream, seed, encrypted []byte, err error) {
	if fs.prf == nil {
		return nil, nil, nil, nil, errNoEncryption
	}
	// Generate key encryption key.
	rb, err := fs.prf([]byte(context))
	if err != nil {
		return nil, nil, nil, nil, err
	}

	sc := fs.fcfg.Cipher

	kek, err := genEncryptionKey(sc, rb)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	// Generate random asset encryption key seed.

	const seedSize = 32
	seed = make([]byte, seedSize)
	if n, err := rand.Read(seed); err != nil {
		return nil, nil, nil, nil, err
	} else if n != seedSize {
		return nil, nil, nil, nil, fmt.Errorf("not enough seed bytes read (%d != %d", n, seedSize)
	}

	aek, err = genEncryptionKey(sc, seed)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	// Generate our nonce. Use same buffer to hold encrypted seed.
	nonce := make([]byte, kek.NonceSize(), kek.NonceSize()+len(seed)+kek.Overhead())
	if n, err := rand.Read(nonce); err != nil {
		return nil, nil, nil, nil, err
	} else if n != len(nonce) {
		return nil, nil, nil, nil, fmt.Errorf("not enough nonce bytes read (%d != %d)", n, len(nonce))
	}

	bek, err = genBlockEncryptionKey(sc, seed[:], nonce)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	return aek, bek, seed, kek.Seal(nonce, nonce, seed, nil), nil
}

// Will generate the block encryption key.
func genBlockEncryptionKey(sc StoreCipher, seed, nonce []byte) (cipher.Stream, error) {
	if sc == ChaCha {
		return chacha20.NewUnauthenticatedCipher(seed, nonce)
	} else if sc == AES {
		block, err := aes.NewCipher(seed)
		if err != nil {
			return nil, err
		}
		return cipher.NewCTR(block, nonce), nil
	}
	return nil, errUnknownCipher
}

// Lock should be held.
func (fs *fileStore) recoverAEK() error {
	if fs.prf != nil && fs.aek == nil {
		ekey, err := os.ReadFile(filepath.Join(fs.fcfg.StoreDir, JetStreamMetaFileKey))
		if err != nil {
			return err
		}
		rb, err := fs.prf([]byte(fs.cfg.Name))
		if err != nil {
			return err
		}
		kek, err := genEncryptionKey(fs.fcfg.Cipher, rb)
		if err != nil {
			return err
		}
		ns := kek.NonceSize()
		seed, err := kek.Open(nil, ekey[:ns], ekey[ns:], nil)
		if err != nil {
			return err
		}
		aek, err := genEncryptionKey(fs.fcfg.Cipher, seed)
		if err != nil {
			return err
		}
		fs.aek = aek
	}
	return nil
}

// Lock should be held.
func (fs *fileStore) setupAEK() error {
	if fs.prf != nil && fs.aek == nil {
		key, _, _, encrypted, err := fs.genEncryptionKeys(fs.cfg.Name)
		if err != nil {
			return err
		}
		keyFile := filepath.Join(fs.fcfg.StoreDir, JetStreamMetaFileKey)
		if _, err := os.Stat(keyFile); err != nil && !os.IsNotExist(err) {
			return err
		}
		err = fs.writeFileWithOptionalSync(keyFile, encrypted, defaultFilePerms)
		if err != nil {
			return err
		}
		// Set our aek.
		fs.aek = key
	}
	return nil
}

// Write out meta and the checksum.
// Lock should be held.
func (fs *fileStore) writeStreamMeta() error {
	if err := fs.setupAEK(); err != nil {
		return err
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
		if n, err := rand.Read(nonce); err != nil {
			return err
		} else if n != len(nonce) {
			return fmt.Errorf("not enough nonce bytes read (%d != %d)", n, len(nonce))
		}
		b = fs.aek.Seal(nonce, nonce, b, nil)
	}

	err = fs.writeFileWithOptionalSync(meta, b, defaultFilePerms)
	if err != nil {
		return err
	}
	fs.hh.Reset()
	fs.hh.Write(b)
	checksum := hex.EncodeToString(fs.hh.Sum(nil))
	sum := filepath.Join(fs.fcfg.StoreDir, JetStreamMetaFileSum)
	err = fs.writeFileWithOptionalSync(sum, []byte(checksum), defaultFilePerms)
	if err != nil {
		return err
	}
	return nil
}

// Pools to recycle the blocks to help with memory pressure.
var blkPoolBig sync.Pool    // 16MB
var blkPoolMedium sync.Pool // 8MB
var blkPoolSmall sync.Pool  // 2MB

// Get a new msg block based on sz estimate.
func getMsgBlockBuf(sz int) (buf []byte) {
	var pb any
	if sz <= defaultSmallBlockSize {
		pb = blkPoolSmall.Get()
	} else if sz <= defaultMediumBlockSize {
		pb = blkPoolMedium.Get()
	} else {
		pb = blkPoolBig.Get()
	}
	if pb != nil {
		buf = *(pb.(*[]byte))
	} else {
		// Here we need to make a new blk.
		// If small leave as is..
		if sz > defaultSmallBlockSize && sz <= defaultMediumBlockSize {
			sz = defaultMediumBlockSize
		} else if sz > defaultMediumBlockSize {
			sz = defaultLargeBlockSize
		}
		buf = make([]byte, sz)
	}
	return buf[:0]
}

// Recycle the msg block.
func recycleMsgBlockBuf(buf []byte) {
	if buf == nil || cap(buf) < defaultSmallBlockSize {
		return
	}
	// Make sure to reset before placing back into pool.
	buf = buf[:0]

	// We need to make sure the load code gets a block that can fit the maximum for a size block.
	// E.g. 8, 16 etc. otherwise we thrash and actually make things worse by pulling it out, and putting
	// it right back in and making a new []byte.
	// From above we know its already >= defaultSmallBlockSize
	if sz := cap(buf); sz < defaultMediumBlockSize {
		blkPoolSmall.Put(&buf)
	} else if sz < defaultLargeBlockSize {
		blkPoolMedium.Put(&buf)
	} else {
		blkPoolBig.Put(&buf)
	}
}

const (
	msgHdrSize     = 22
	checksumSize   = 8
	emptyRecordLen = msgHdrSize + checksumSize
)

// Lock should be held.
func (fs *fileStore) noTrackSubjects() bool {
	return !(fs.psim.Size() > 0 || len(fs.cfg.Subjects) > 0 || fs.cfg.Mirror != nil || len(fs.cfg.Sources) > 0)
}

// Will init the basics for a message block.
func (fs *fileStore) initMsgBlock(index uint32) *msgBlock {
	mb := &msgBlock{
		fs:         fs,
		index:      index,
		cexp:       fs.fcfg.CacheExpire,
		fexp:       fs.fcfg.SubjectStateExpire,
		noTrack:    fs.noTrackSubjects(),
		syncAlways: fs.fcfg.SyncAlways,
	}

	mdir := filepath.Join(fs.fcfg.StoreDir, msgDir)
	mb.mfn = filepath.Join(mdir, fmt.Sprintf(blkScan, index))

	if mb.hh == nil {
		key := sha256.Sum256(fs.hashKeyForBlock(index))
		mb.hh, _ = highwayhash.New64(key[:])
	}
	return mb
}

// Lock for fs should be held.
func (fs *fileStore) loadEncryptionForMsgBlock(mb *msgBlock) error {
	if fs.prf == nil {
		return nil
	}

	var createdKeys bool
	mdir := filepath.Join(fs.fcfg.StoreDir, msgDir)
	ekey, err := os.ReadFile(filepath.Join(mdir, fmt.Sprintf(keyScan, mb.index)))
	if err != nil {
		// We do not seem to have keys even though we should. Could be a plaintext conversion.
		// Create the keys and we will double check below.
		if err := fs.genEncryptionKeysForBlock(mb); err != nil {
			return err
		}
		createdKeys = true
	} else {
		if len(ekey) < minBlkKeySize {
			return errBadKeySize
		}
		// Recover key encryption key.
		rb, err := fs.prf([]byte(fmt.Sprintf("%s:%d", fs.cfg.Name, mb.index)))
		if err != nil {
			return err
		}

		sc := fs.fcfg.Cipher
		kek, err := genEncryptionKey(sc, rb)
		if err != nil {
			return err
		}
		ns := kek.NonceSize()
		seed, err := kek.Open(nil, ekey[:ns], ekey[ns:], nil)
		if err != nil {
			// We may be here on a cipher conversion, so attempt to convert.
			if err = mb.convertCipher(); err != nil {
				return err
			}
		} else {
			mb.seed, mb.nonce = seed, ekey[:ns]
		}
		mb.aek, err = genEncryptionKey(sc, mb.seed)
		if err != nil {
			return err
		}
		if mb.bek, err = genBlockEncryptionKey(sc, mb.seed, mb.nonce); err != nil {
			return err
		}
	}

	// If we created keys here, let's check the data and if it is plaintext convert here.
	if createdKeys {
		if err := mb.convertToEncrypted(); err != nil {
			return err
		}
	}

	return nil
}

// Load a last checksum if needed from the block file.
// Lock should be held.
func (mb *msgBlock) ensureLastChecksumLoaded() {
	var empty [8]byte
	if mb.lchk != empty {
		return
	}
	copy(mb.lchk[0:], mb.lastChecksum())
}

// Lock held on entry
func (fs *fileStore) recoverMsgBlock(index uint32) (*msgBlock, error) {
	mb := fs.initMsgBlock(index)
	// Open up the message file, but we will try to recover from the index file.
	// We will check that the last checksums match.
	file, err := mb.openBlock()
	if err != nil {
		return nil, err
	}
	defer file.Close()

	if fi, err := file.Stat(); fi != nil {
		mb.rbytes = uint64(fi.Size())
	} else {
		return nil, err
	}

	// Make sure encryption loaded if needed.
	fs.loadEncryptionForMsgBlock(mb)

	// Grab last checksum from main block file.
	var lchk [8]byte
	if mb.rbytes >= checksumSize {
		if mb.bek != nil {
			if buf, _ := mb.loadBlock(nil); len(buf) >= checksumSize {
				mb.bek.XORKeyStream(buf, buf)
				copy(lchk[0:], buf[len(buf)-checksumSize:])
			}
		} else {
			file.ReadAt(lchk[:], int64(mb.rbytes)-checksumSize)
		}
	}

	file.Close()

	// Read our index file. Use this as source of truth if possible.
	// This not applicable in >= 2.10 servers. Here for upgrade paths from < 2.10.
	if err := mb.readIndexInfo(); err == nil {
		// Quick sanity check here.
		// Note this only checks that the message blk file is not newer then this file, or is empty and we expect empty.
		if (mb.rbytes == 0 && mb.msgs == 0) || bytes.Equal(lchk[:], mb.lchk[:]) {
			if mb.msgs > 0 && !mb.noTrack && fs.psim != nil {
				fs.populateGlobalPerSubjectInfo(mb)
				// Try to dump any state we needed on recovery.
				mb.tryForceExpireCacheLocked()
			}
			fs.addMsgBlock(mb)
			return mb, nil
		}
	}

	// If we get data loss rebuilding the message block state record that with the fs itself.
	ld, tombs, _ := mb.rebuildState()
	if ld != nil {
		fs.addLostData(ld)
	}
	// Collect all tombstones.
	if len(tombs) > 0 {
		fs.tombs = append(fs.tombs, tombs...)
	}

	if mb.msgs > 0 && !mb.noTrack && fs.psim != nil {
		fs.populateGlobalPerSubjectInfo(mb)
		// Try to dump any state we needed on recovery.
		mb.tryForceExpireCacheLocked()
	}

	mb.closeFDs()
	fs.addMsgBlock(mb)

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

// Lock should be held.
func (fs *fileStore) addLostData(ld *LostStreamData) {
	if ld == nil {
		return
	}
	if fs.ld != nil {
		var added bool
		for _, seq := range ld.Msgs {
			if _, found := fs.ld.exists(seq); !found {
				fs.ld.Msgs = append(fs.ld.Msgs, seq)
				added = true
			}
		}
		if added {
			msgs := fs.ld.Msgs
			slices.Sort(msgs)
			fs.ld.Bytes += ld.Bytes
		}
	} else {
		fs.ld = ld
	}
}

// Helper to see if we already have this sequence reported in our lost data.
func (ld *LostStreamData) exists(seq uint64) (int, bool) {
	i := slices.IndexFunc(ld.Msgs, func(i uint64) bool {
		return i == seq
	})
	return i, i > -1
}

func (fs *fileStore) removeFromLostData(seq uint64) {
	if fs.ld == nil {
		return
	}
	if i, found := fs.ld.exists(seq); found {
		fs.ld.Msgs = append(fs.ld.Msgs[:i], fs.ld.Msgs[i+1:]...)
		if len(fs.ld.Msgs) == 0 {
			fs.ld = nil
		}
	}
}

func (fs *fileStore) rebuildState(ld *LostStreamData) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs.rebuildStateLocked(ld)
}

// Lock should be held.
func (fs *fileStore) rebuildStateLocked(ld *LostStreamData) {
	fs.addLostData(ld)

	fs.state.Msgs, fs.state.Bytes = 0, 0
	fs.state.FirstSeq, fs.state.LastSeq = 0, 0

	for _, mb := range fs.blks {
		mb.mu.RLock()
		fs.state.Msgs += mb.msgs
		fs.state.Bytes += mb.bytes
		fseq := atomic.LoadUint64(&mb.first.seq)
		if fs.state.FirstSeq == 0 || fseq < fs.state.FirstSeq {
			fs.state.FirstSeq = fseq
			fs.state.FirstTime = time.Unix(0, mb.first.ts).UTC()
		}
		fs.state.LastSeq = atomic.LoadUint64(&mb.last.seq)
		fs.state.LastTime = time.Unix(0, mb.last.ts).UTC()
		mb.mu.RUnlock()
	}
}

// Attempt to convert the cipher used for this message block.
func (mb *msgBlock) convertCipher() error {
	fs := mb.fs
	sc := fs.fcfg.Cipher

	var osc StoreCipher
	switch sc {
	case ChaCha:
		osc = AES
	case AES:
		osc = ChaCha
	}

	mdir := filepath.Join(fs.fcfg.StoreDir, msgDir)
	ekey, err := os.ReadFile(filepath.Join(mdir, fmt.Sprintf(keyScan, mb.index)))
	if err != nil {
		return err
	}
	if len(ekey) < minBlkKeySize {
		return errBadKeySize
	}
	type prfWithCipher struct {
		keyGen
		StoreCipher
	}
	var prfs []prfWithCipher
	if fs.prf != nil {
		prfs = append(prfs, prfWithCipher{fs.prf, sc})
		prfs = append(prfs, prfWithCipher{fs.prf, osc})
	}
	if fs.oldprf != nil {
		prfs = append(prfs, prfWithCipher{fs.oldprf, sc})
		prfs = append(prfs, prfWithCipher{fs.oldprf, osc})
	}

	for _, prf := range prfs {
		// Recover key encryption key.
		rb, err := prf.keyGen([]byte(fmt.Sprintf("%s:%d", fs.cfg.Name, mb.index)))
		if err != nil {
			continue
		}
		kek, err := genEncryptionKey(prf.StoreCipher, rb)
		if err != nil {
			continue
		}
		ns := kek.NonceSize()
		seed, err := kek.Open(nil, ekey[:ns], ekey[ns:], nil)
		if err != nil {
			continue
		}
		nonce := ekey[:ns]
		bek, err := genBlockEncryptionKey(prf.StoreCipher, seed, nonce)
		if err != nil {
			return err
		}

		buf, _ := mb.loadBlock(nil)
		bek.XORKeyStream(buf, buf)
		// Make sure we can parse with old cipher and key file.
		if err = mb.indexCacheBuf(buf); err != nil {
			return err
		}
		// Reset the cache since we just read everything in.
		mb.cache = nil

		// Generate new keys. If we error for some reason then we will put
		// the old keyfile back.
		if err := fs.genEncryptionKeysForBlock(mb); err != nil {
			keyFile := filepath.Join(mdir, fmt.Sprintf(keyScan, mb.index))
			fs.writeFileWithOptionalSync(keyFile, ekey, defaultFilePerms)
			return err
		}
		mb.bek.XORKeyStream(buf, buf)
		<-dios
		err = os.WriteFile(mb.mfn, buf, defaultFilePerms)
		dios <- struct{}{}
		if err != nil {
			return err
		}
		return nil
	}
	return fmt.Errorf("unable to recover keys")
}

// Convert a plaintext block to encrypted.
func (mb *msgBlock) convertToEncrypted() error {
	if mb.bek == nil {
		return nil
	}
	buf, err := mb.loadBlock(nil)
	if err != nil {
		return err
	}
	if err := mb.indexCacheBuf(buf); err != nil {
		// This likely indicates this was already encrypted or corrupt.
		mb.cache = nil
		return err
	}
	// Undo cache from above for later.
	mb.cache = nil
	mb.bek.XORKeyStream(buf, buf)
	<-dios
	err = os.WriteFile(mb.mfn, buf, defaultFilePerms)
	dios <- struct{}{}
	if err != nil {
		return err
	}
	return nil
}

// Return the mb's index.
func (mb *msgBlock) getIndex() uint32 {
	mb.mu.RLock()
	defer mb.mu.RUnlock()
	return mb.index
}

// Rebuild the state of the blk based on what we have on disk in the N.blk file.
// We will return any lost data, and we will return any delete tombstones we encountered.
func (mb *msgBlock) rebuildState() (*LostStreamData, []uint64, error) {
	mb.mu.Lock()
	defer mb.mu.Unlock()
	return mb.rebuildStateLocked()
}

// Rebuild the state of the blk based on what we have on disk in the N.blk file.
// Lock should be held.
func (mb *msgBlock) rebuildStateLocked() (*LostStreamData, []uint64, error) {
	startLastSeq := atomic.LoadUint64(&mb.last.seq)

	// Remove the .fss file and clear any cache we have set.
	mb.clearCacheAndOffset()

	buf, err := mb.loadBlock(nil)
	defer recycleMsgBlockBuf(buf)

	if err != nil || len(buf) == 0 {
		var ld *LostStreamData
		// No data to rebuild from here.
		if mb.msgs > 0 {
			// We need to declare lost data here.
			ld = &LostStreamData{Msgs: make([]uint64, 0, mb.msgs), Bytes: mb.bytes}
			firstSeq, lastSeq := atomic.LoadUint64(&mb.first.seq), atomic.LoadUint64(&mb.last.seq)
			for seq := firstSeq; seq <= lastSeq; seq++ {
				if !mb.dmap.Exists(seq) {
					ld.Msgs = append(ld.Msgs, seq)
				}
			}
			// Clear invalid state. We will let this blk be added in here.
			mb.msgs, mb.bytes, mb.rbytes, mb.fss = 0, 0, 0, nil
			mb.dmap.Empty()
			atomic.StoreUint64(&mb.first.seq, atomic.LoadUint64(&mb.last.seq)+1)
		}
		return ld, nil, err
	}

	// Clear state we need to rebuild.
	mb.msgs, mb.bytes, mb.rbytes, mb.fss = 0, 0, 0, nil
	atomic.StoreUint64(&mb.last.seq, 0)
	mb.last.ts = 0
	firstNeedsSet := true

	// Check if we need to decrypt.
	if mb.bek != nil && len(buf) > 0 {
		// Recreate to reset counter.
		mb.bek, err = genBlockEncryptionKey(mb.fs.fcfg.Cipher, mb.seed, mb.nonce)
		if err != nil {
			return nil, nil, err
		}
		mb.bek.XORKeyStream(buf, buf)
	}

	// Check for compression.
	if buf, err = mb.decompressIfNeeded(buf); err != nil {
		return nil, nil, err
	}

	mb.rbytes = uint64(len(buf))

	addToDmap := func(seq uint64) {
		if seq == 0 {
			return
		}
		mb.dmap.Insert(seq)
	}

	var le = binary.LittleEndian

	truncate := func(index uint32) {
		var fd *os.File
		if mb.mfd != nil {
			fd = mb.mfd
		} else {
			<-dios
			fd, err = os.OpenFile(mb.mfn, os.O_RDWR, defaultFilePerms)
			dios <- struct{}{}
			if err == nil {
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
		for seq := atomic.LoadUint64(&mb.last.seq) + 1; seq <= startLastSeq; seq++ {
			ld.Msgs = append(ld.Msgs, seq)
		}
		ld.Bytes = uint64(lb)
		return &ld
	}

	// For tombstones that we find and collect.
	var (
		tombstones      []uint64
		minTombstoneSeq uint64
		minTombstoneTs  int64
	)

	// To detect gaps from compaction.
	var last uint64

	for index, lbuf := uint32(0), uint32(len(buf)); index < lbuf; {
		if index+msgHdrSize > lbuf {
			truncate(index)
			return gatherLost(lbuf - index), tombstones, nil
		}

		hdr := buf[index : index+msgHdrSize]
		rl, slen := le.Uint32(hdr[0:]), int(le.Uint16(hdr[20:]))

		hasHeaders := rl&hbit != 0
		var ttl int64
		if mb.fs.ttls != nil && len(hdr) > 0 {
			ttl, _ = getMessageTTL(hdr)
		}
		// Clear any headers bit that could be set.
		rl &^= hbit
		dlen := int(rl) - msgHdrSize
		// Do some quick sanity checks here.
		if dlen < 0 || slen > (dlen-recordHashSize) || dlen > int(rl) || index+rl > lbuf || rl > rlBadThresh {
			truncate(index)
			return gatherLost(lbuf - index), tombstones, errBadMsg
		}

		// Check for checksum failures before additional processing.
		data := buf[index+msgHdrSize : index+rl]
		if hh := mb.hh; hh != nil {
			hh.Reset()
			hh.Write(hdr[4:20])
			hh.Write(data[:slen])
			if hasHeaders {
				hh.Write(data[slen+4 : dlen-recordHashSize])
			} else {
				hh.Write(data[slen : dlen-recordHashSize])
			}
			checksum := hh.Sum(nil)
			if !bytes.Equal(checksum, data[len(data)-recordHashSize:]) {
				truncate(index)
				return gatherLost(lbuf - index), tombstones, errBadMsg
			}
			copy(mb.lchk[0:], checksum)
		}

		// Grab our sequence and timestamp.
		seq := le.Uint64(hdr[4:])
		ts := int64(le.Uint64(hdr[12:]))

		// Check if this is a delete tombstone.
		if seq&tbit != 0 {
			seq = seq &^ tbit
			// Need to process this here and make sure we have accounted for this properly.
			tombstones = append(tombstones, seq)
			if minTombstoneSeq == 0 || seq < minTombstoneSeq {
				minTombstoneSeq, minTombstoneTs = seq, ts
			}
			index += rl
			continue
		}

		fseq := atomic.LoadUint64(&mb.first.seq)
		// This is an old erased message, or a new one that we can track.
		if seq == 0 || seq&ebit != 0 || seq < fseq {
			seq = seq &^ ebit
			if seq >= fseq {
				atomic.StoreUint64(&mb.last.seq, seq)
				mb.last.ts = ts
				if mb.msgs == 0 {
					atomic.StoreUint64(&mb.first.seq, seq+1)
					mb.first.ts = 0
				} else if seq != 0 {
					// Only add to dmap if past recorded first seq and non-zero.
					addToDmap(seq)
				}
			}
			index += rl
			continue
		}

		// This is for when we have index info that adjusts for deleted messages
		// at the head. So the first.seq will be already set here. If this is larger
		// replace what we have with this seq.
		if firstNeedsSet && seq >= fseq {
			atomic.StoreUint64(&mb.first.seq, seq)
			firstNeedsSet, mb.first.ts = false, ts
		}

		if !mb.dmap.Exists(seq) {
			mb.msgs++
			mb.bytes += uint64(rl)
			if mb.fs.ttls != nil && ttl > 0 {
				expires := time.Duration(ts) + (time.Second * time.Duration(ttl))
				mb.fs.ttls.Add(seq, int64(expires))
				mb.ttls++
			}
		}

		// Check for any gaps from compaction, meaning no ebit entry.
		if last > 0 && seq != last+1 {
			for dseq := last + 1; dseq < seq; dseq++ {
				addToDmap(dseq)
			}
		}

		// Always set last
		last = seq
		atomic.StoreUint64(&mb.last.seq, last)
		mb.last.ts = ts

		// Advance to next record.
		index += rl
	}

	// For empty msg blocks make sure we recover last seq correctly based off of first.
	// Or if we seem to have no messages but had a tombstone, which we use to remember
	// sequences and timestamps now, use that to properly setup the first and last.
	if mb.msgs == 0 {
		fseq := atomic.LoadUint64(&mb.first.seq)
		if fseq > 0 {
			atomic.StoreUint64(&mb.last.seq, fseq-1)
		} else if fseq == 0 && minTombstoneSeq > 0 {
			atomic.StoreUint64(&mb.first.seq, minTombstoneSeq+1)
			mb.first.ts = 0
			if mb.last.seq == 0 {
				atomic.StoreUint64(&mb.last.seq, minTombstoneSeq)
				mb.last.ts = minTombstoneTs
			}
		}
	}

	return nil, tombstones, nil
}

// For doing warn logging.
// Lock should be held.
func (fs *fileStore) warn(format string, args ...any) {
	// No-op if no server configured.
	if fs.srv == nil {
		return
	}
	fs.srv.Warnf(fmt.Sprintf("Filestore [%s] %s", fs.cfg.Name, format), args...)
}

// For doing debug logging.
// Lock should be held.
func (fs *fileStore) debug(format string, args ...any) {
	// No-op if no server configured.
	if fs.srv == nil {
		return
	}
	fs.srv.Debugf(fmt.Sprintf("Filestore [%s] %s", fs.cfg.Name, format), args...)
}

// Track local state but ignore timestamps here.
func updateTrackingState(state *StreamState, mb *msgBlock) {
	if state.FirstSeq == 0 {
		state.FirstSeq = mb.first.seq
	} else if mb.first.seq < state.FirstSeq {
		state.FirstSeq = mb.first.seq
	}
	if mb.last.seq > state.LastSeq {
		state.LastSeq = mb.last.seq
	}
	state.Msgs += mb.msgs
	state.Bytes += mb.bytes
}

// Determine if our tracking states are the same.
func trackingStatesEqual(fs, mb *StreamState) bool {
	// When a fs is brand new the fs state will have first seq of 0, but tracking mb may have 1.
	// If either has a first sequence that is not 0 or 1 we will check if they are the same, otherwise skip.
	if (fs.FirstSeq > 1 && mb.FirstSeq > 1) || mb.FirstSeq > 1 {
		return fs.Msgs == mb.Msgs && fs.FirstSeq == mb.FirstSeq && fs.LastSeq == mb.LastSeq && fs.Bytes == mb.Bytes
	}
	return fs.Msgs == mb.Msgs && fs.LastSeq == mb.LastSeq && fs.Bytes == mb.Bytes
}

// recoverFullState will attempt to receover our last full state and re-process any state changes
// that happened afterwards.
func (fs *fileStore) recoverFullState() (rerr error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Check for any left over purged messages.
	<-dios
	pdir := filepath.Join(fs.fcfg.StoreDir, purgeDir)
	if _, err := os.Stat(pdir); err == nil {
		os.RemoveAll(pdir)
	}
	// Grab our stream state file and load it in.
	fn := filepath.Join(fs.fcfg.StoreDir, msgDir, streamStreamStateFile)
	buf, err := os.ReadFile(fn)
	dios <- struct{}{}

	if err != nil {
		if !os.IsNotExist(err) {
			fs.warn("Could not read stream state file: %v", err)
		}
		return err
	}

	const minLen = 32
	if len(buf) < minLen {
		os.Remove(fn)
		fs.warn("Stream state too short (%d bytes)", len(buf))
		return errCorruptState
	}

	// The highwayhash will be on the end. Check that it still matches.
	h := buf[len(buf)-highwayhash.Size64:]
	buf = buf[:len(buf)-highwayhash.Size64]
	fs.hh.Reset()
	fs.hh.Write(buf)
	if !bytes.Equal(h, fs.hh.Sum(nil)) {
		os.Remove(fn)
		fs.warn("Stream state checksum did not match")
		return errCorruptState
	}

	// Decrypt if needed.
	if fs.prf != nil {
		// We can be setup for encryption but if this is a snapshot restore we will be missing the keyfile
		// since snapshots strip encryption.
		if err := fs.recoverAEK(); err == nil {
			ns := fs.aek.NonceSize()
			buf, err = fs.aek.Open(nil, buf[:ns], buf[ns:], nil)
			if err != nil {
				fs.warn("Stream state error reading encryption key: %v", err)
				return err
			}
		}
	}

	version := buf[1]
	if buf[0] != fullStateMagic || version < fullStateMinVersion || version > fullStateVersion {
		os.Remove(fn)
		fs.warn("Stream state magic and version mismatch")
		return errCorruptState
	}

	bi := hdrLen

	readU64 := func() uint64 {
		if bi < 0 {
			return 0
		}
		v, n := binary.Uvarint(buf[bi:])
		if n <= 0 {
			bi = -1
			return 0
		}
		bi += n
		return v
	}
	readI64 := func() int64 {
		if bi < 0 {
			return 0
		}
		v, n := binary.Varint(buf[bi:])
		if n <= 0 {
			bi = -1
			return -1
		}
		bi += n
		return v
	}

	setTime := func(t *time.Time, ts int64) {
		if ts == 0 {
			*t = time.Time{}
		} else {
			*t = time.Unix(0, ts).UTC()
		}
	}

	var state StreamState
	state.Msgs = readU64()
	state.Bytes = readU64()
	state.FirstSeq = readU64()
	baseTime := readI64()
	setTime(&state.FirstTime, baseTime)
	state.LastSeq = readU64()
	setTime(&state.LastTime, readI64())

	// Check for per subject info.
	if numSubjects := int(readU64()); numSubjects > 0 {
		fs.psim, fs.tsl = fs.psim.Empty(), 0
		for i := 0; i < numSubjects; i++ {
			if lsubj := int(readU64()); lsubj > 0 {
				if bi+lsubj > len(buf) {
					os.Remove(fn)
					fs.warn("Stream state bad subject len (%d)", lsubj)
					return errCorruptState
				}
				// If we have lots of subjects this will alloc for each one.
				// We could reference the underlying buffer, but we could guess wrong if
				// number of blocks is large and subjects is low, since we would reference buf.
				subj := buf[bi : bi+lsubj]
				// We had a bug that could cause memory corruption in the PSIM that could have gotten stored to disk.
				// Only would affect subjects, so do quick check.
				if !isValidSubject(bytesToString(subj), true) {
					os.Remove(fn)
					fs.warn("Stream state corrupt subject detected")
					return errCorruptState
				}
				bi += lsubj
				psi := psi{total: readU64(), fblk: uint32(readU64())}
				if psi.total > 1 {
					psi.lblk = uint32(readU64())
				} else {
					psi.lblk = psi.fblk
				}
				fs.psim.Insert(subj, psi)
				fs.tsl += lsubj
			}
		}
	}

	// Track the state as represented by the blocks themselves.
	var mstate StreamState

	if numBlocks := readU64(); numBlocks > 0 {
		lastIndex := int(numBlocks - 1)
		fs.blks = make([]*msgBlock, 0, numBlocks)
		for i := 0; i < int(numBlocks); i++ {
			index, nbytes, fseq, fts, lseq, lts, numDeleted := uint32(readU64()), readU64(), readU64(), readI64(), readU64(), readI64(), readU64()
			var ttls uint64
			if version >= 2 {
				ttls = readU64()
			}
			if bi < 0 {
				os.Remove(fn)
				return errCorruptState
			}
			mb := fs.initMsgBlock(index)
			atomic.StoreUint64(&mb.first.seq, fseq)
			atomic.StoreUint64(&mb.last.seq, lseq)
			mb.msgs, mb.bytes = lseq-fseq+1, nbytes
			mb.first.ts, mb.last.ts = fts+baseTime, lts+baseTime
			mb.ttls = ttls
			if numDeleted > 0 {
				dmap, n, err := avl.Decode(buf[bi:])
				if err != nil {
					os.Remove(fn)
					fs.warn("Stream state error decoding avl dmap: %v", err)
					return errCorruptState
				}
				mb.dmap = *dmap
				if mb.msgs > numDeleted {
					mb.msgs -= numDeleted
				} else {
					mb.msgs = 0
				}
				bi += n
			}
			// Only add in if not empty or the lmb.
			if mb.msgs > 0 || i == lastIndex {
				fs.addMsgBlock(mb)
				updateTrackingState(&mstate, mb)
			} else {
				// Mark dirty to cleanup.
				fs.dirty++
			}
		}
	}

	// Pull in last block index for the block that had last checksum when we wrote the full state.
	blkIndex := uint32(readU64())
	var lchk [8]byte
	if bi+len(lchk) > len(buf) {
		bi = -1
	} else {
		copy(lchk[0:], buf[bi:bi+len(lchk)])
	}

	// Check if we had any errors.
	if bi < 0 {
		os.Remove(fn)
		fs.warn("Stream state has no checksum present")
		return errCorruptState
	}

	// Move into place our state, msgBlks and subject info.
	fs.state = state

	// First let's check the happy path, open the blk file that was the lmb when we created the full state.
	// See if we have the last block available.
	var matched bool
	mb := fs.lmb
	if mb == nil || mb.index != blkIndex {
		os.Remove(fn)
		fs.warn("Stream state block does not exist or index mismatch")
		return errCorruptState
	}
	if _, err := os.Stat(mb.mfn); err != nil && os.IsNotExist(err) {
		// If our saved state is past what we see on disk, fallback and rebuild.
		if ld, _, _ := mb.rebuildState(); ld != nil {
			fs.addLostData(ld)
		}
		fs.warn("Stream state detected prior state, could not locate msg block %d", blkIndex)
		return errPriorState
	}
	if matched = bytes.Equal(mb.lastChecksum(), lchk[:]); !matched {
		// Detected a stale index.db, we didn't write it upon shutdown so can't rely on it being correct.
		fs.warn("Stream state outdated, last block has additional entries, will rebuild")
		return errPriorState
	}

	// We need to see if any blocks exist after our last one even though we matched the last record exactly.
	mdir := filepath.Join(fs.fcfg.StoreDir, msgDir)
	var dirs []os.DirEntry

	<-dios
	if f, err := os.Open(mdir); err == nil {
		dirs, _ = f.ReadDir(-1)
		f.Close()
	}
	dios <- struct{}{}

	var index uint32
	for _, fi := range dirs {
		// Ensure it's actually a block file, otherwise fmt.Sscanf also matches %d.blk.tmp
		if !strings.HasSuffix(fi.Name(), blkSuffix) {
			continue
		}
		if n, err := fmt.Sscanf(fi.Name(), blkScan, &index); err == nil && n == 1 {
			if index > blkIndex {
				fs.warn("Stream state outdated, found extra blocks, will rebuild")
				return errPriorState
			}
		}
	}

	// We check first and last seq and number of msgs and bytes. If there is a difference,
	// return and error so we rebuild from the message block state on disk.
	if !trackingStatesEqual(&fs.state, &mstate) {
		os.Remove(fn)
		fs.warn("Stream state encountered internal inconsistency on recover")
		return errCorruptState
	}

	return nil
}

func (fs *fileStore) recoverTTLState() error {
	// See if we have a timed hash wheel for TTLs.
	<-dios
	fn := filepath.Join(fs.fcfg.StoreDir, msgDir, ttlStreamStateFile)
	buf, err := os.ReadFile(fn)
	dios <- struct{}{}

	if err != nil && !os.IsNotExist(err) {
		return err
	}

	fs.ttls = thw.NewHashWheel()

	if err == nil {
		fs.ttlseq, err = fs.ttls.Decode(buf)
		if err != nil {
			fs.warn("Error decoding TTL state: %s", err)
			os.Remove(fn)
		}
	}

	if fs.ttlseq < fs.state.FirstSeq {
		fs.ttlseq = fs.state.FirstSeq
	}

	defer fs.resetAgeChk(0)
	if fs.state.Msgs > 0 && fs.ttlseq <= fs.state.LastSeq {
		fs.warn("TTL state is outdated; attempting to recover using linear scan (seq %d to %d)", fs.ttlseq, fs.state.LastSeq)
		var sm StoreMsg
		mb := fs.selectMsgBlock(fs.ttlseq)
		if mb == nil {
			return nil
		}
		mblseq := atomic.LoadUint64(&mb.last.seq)
		for seq := fs.ttlseq; seq <= fs.state.LastSeq; seq++ {
		retry:
			if mb.ttls == 0 {
				// None of the messages in the block have message TTLs so don't
				// bother doing anything further with this block, skip to the end.
				seq = atomic.LoadUint64(&mb.last.seq) + 1
			}
			if seq > mblseq {
				// We've reached the end of the loaded block, see if we can continue
				// by loading the next one.
				mb.tryForceExpireCache()
				if mb = fs.selectMsgBlock(seq); mb == nil {
					// TODO(nat): Deal with gaps properly. Right now this will be
					// probably expensive on CPU.
					continue
				}
				mblseq = atomic.LoadUint64(&mb.last.seq)
				// At this point we've loaded another block, so let's go back to the
				// beginning and see if we need to skip this one too.
				goto retry
			}
			msg, _, err := mb.fetchMsg(seq, &sm)
			if err != nil {
				fs.warn("Error loading msg seq %d for recovering TTL: %s", seq, err)
				continue
			}
			if len(msg.hdr) == 0 {
				continue
			}
			if ttl, _ := getMessageTTL(msg.hdr); ttl > 0 {
				expires := time.Duration(msg.ts) + (time.Second * time.Duration(ttl))
				fs.ttls.Add(seq, int64(expires))
				if seq > fs.ttlseq {
					fs.ttlseq = seq
				}
			}
		}
	}
	return nil
}

// Grabs last checksum for the named block file.
// Takes into account encryption etc.
func (mb *msgBlock) lastChecksum() []byte {
	f, err := mb.openBlock()
	if err != nil {
		return nil
	}
	defer f.Close()

	var lchk [8]byte
	if fi, _ := f.Stat(); fi != nil {
		mb.rbytes = uint64(fi.Size())
	}
	if mb.rbytes < checksumSize {
		return lchk[:]
	}
	// Encrypted?
	// Check for encryption, we do not load keys on startup anymore so might need to load them here.
	if mb.fs != nil && mb.fs.prf != nil && (mb.aek == nil || mb.bek == nil) {
		if err := mb.fs.loadEncryptionForMsgBlock(mb); err != nil {
			return nil
		}
	}
	if mb.bek != nil {
		if buf, _ := mb.loadBlock(nil); len(buf) >= checksumSize {
			bek, err := genBlockEncryptionKey(mb.fs.fcfg.Cipher, mb.seed, mb.nonce)
			if err != nil {
				return nil
			}
			mb.bek = bek
			mb.bek.XORKeyStream(buf, buf)
			copy(lchk[0:], buf[len(buf)-checksumSize:])
		}
	} else {
		f.ReadAt(lchk[:], int64(mb.rbytes)-checksumSize)
	}
	return lchk[:]
}

// This will make sure we clean up old idx and fss files.
func (fs *fileStore) cleanupOldMeta() {
	fs.mu.RLock()
	mdir := filepath.Join(fs.fcfg.StoreDir, msgDir)
	fs.mu.RUnlock()

	<-dios
	f, err := os.Open(mdir)
	dios <- struct{}{}
	if err != nil {
		return
	}

	dirs, _ := f.ReadDir(-1)
	f.Close()

	const (
		minLen    = 4
		idxSuffix = ".idx"
		fssSuffix = ".fss"
	)
	for _, fi := range dirs {
		if name := fi.Name(); strings.HasSuffix(name, idxSuffix) || strings.HasSuffix(name, fssSuffix) {
			os.Remove(filepath.Join(mdir, name))
		}
	}
}

func (fs *fileStore) recoverMsgs() error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Check for any left over purged messages.
	<-dios
	pdir := filepath.Join(fs.fcfg.StoreDir, purgeDir)
	if _, err := os.Stat(pdir); err == nil {
		os.RemoveAll(pdir)
	}
	mdir := filepath.Join(fs.fcfg.StoreDir, msgDir)
	f, err := os.Open(mdir)
	if err != nil {
		dios <- struct{}{}
		return errNotReadable
	}
	dirs, err := f.ReadDir(-1)
	f.Close()
	dios <- struct{}{}

	if err != nil {
		return errNotReadable
	}

	indices := make(sort.IntSlice, 0, len(dirs))
	var index int
	for _, fi := range dirs {
		// Ensure it's actually a block file, otherwise fmt.Sscanf also matches %d.blk.tmp
		if !strings.HasSuffix(fi.Name(), blkSuffix) {
			continue
		}
		if n, err := fmt.Sscanf(fi.Name(), blkScan, &index); err == nil && n == 1 {
			indices = append(indices, index)
		}
	}
	indices.Sort()

	// Recover all of the msg blocks.
	// We now guarantee they are coming in order.
	for _, index := range indices {
		if mb, err := fs.recoverMsgBlock(uint32(index)); err == nil && mb != nil {
			// This is a truncate block with possibly no index. If the OS got shutdown
			// out from underneath of us this is possible.
			if mb.first.seq == 0 {
				mb.dirtyCloseWithRemove(true)
				fs.removeMsgBlockFromList(mb)
				continue
			}
			if fseq := atomic.LoadUint64(&mb.first.seq); fs.state.FirstSeq == 0 || fseq < fs.state.FirstSeq {
				fs.state.FirstSeq = fseq
				if mb.first.ts == 0 {
					fs.state.FirstTime = time.Time{}
				} else {
					fs.state.FirstTime = time.Unix(0, mb.first.ts).UTC()
				}
			}
			if lseq := atomic.LoadUint64(&mb.last.seq); lseq > fs.state.LastSeq {
				fs.state.LastSeq = lseq
				if mb.last.ts == 0 {
					fs.state.LastTime = time.Time{}
				} else {
					fs.state.LastTime = time.Unix(0, mb.last.ts).UTC()
				}
			}
			fs.state.Msgs += mb.msgs
			fs.state.Bytes += mb.bytes
		} else {
			return err
		}
	}

	if len(fs.blks) > 0 {
		fs.lmb = fs.blks[len(fs.blks)-1]
	} else {
		_, err = fs.newMsgBlockForWrite()
	}

	// Check if we encountered any lost data.
	if fs.ld != nil {
		var emptyBlks []*msgBlock
		for _, mb := range fs.blks {
			if mb.msgs == 0 && mb.rbytes == 0 {
				emptyBlks = append(emptyBlks, mb)
			}
		}
		for _, mb := range emptyBlks {
			// Need the mb lock here.
			mb.mu.Lock()
			fs.removeMsgBlock(mb)
			mb.mu.Unlock()
		}
	}

	if err != nil {
		return err
	}

	// Check for keyfiles orphans.
	if kms, err := filepath.Glob(filepath.Join(mdir, keyScanAll)); err == nil && len(kms) > 0 {
		valid := make(map[uint32]bool)
		for _, mb := range fs.blks {
			valid[mb.index] = true
		}
		for _, fn := range kms {
			var index uint32
			shouldRemove := true
			if n, err := fmt.Sscanf(filepath.Base(fn), keyScan, &index); err == nil && n == 1 && valid[index] {
				shouldRemove = false
			}
			if shouldRemove {
				os.Remove(fn)
			}
		}
	}

	return nil
}

// Will expire msgs that have aged out on restart.
// We will treat this differently in case we have a recovery
// that will expire alot of messages on startup.
// Should only be called on startup.
func (fs *fileStore) expireMsgsOnRecover() error {
	if fs.state.Msgs == 0 {
		return nil
	}

	var minAge = time.Now().UnixNano() - int64(fs.cfg.MaxAge)
	var purged, bytes uint64
	var deleted int
	var nts int64

	// If we expire all make sure to write out a tombstone. Need to be done by hand here,
	// usually taken care of by fs.removeMsgBlock() but we do not call that here.
	var last msgId

	deleteEmptyBlock := func(mb *msgBlock) error {
		// If we are the last keep state to remember first/last sequence.
		// Do this part by hand since not deleting one by one.
		if mb == fs.lmb {
			last.seq = atomic.LoadUint64(&mb.last.seq)
			last.ts = mb.last.ts
		}
		// Make sure we do subject cleanup as well.
		mb.ensurePerSubjectInfoLoaded()
		mb.fss.IterOrdered(func(bsubj []byte, ss *SimpleState) bool {
			subj := bytesToString(bsubj)
			for i := uint64(0); i < ss.Msgs; i++ {
				fs.removePerSubject(subj, false)
			}
			return true
		})
		err := mb.dirtyCloseWithRemove(true)
		if isPermissionError(err) {
			return err
		}
		deleted++
		return nil
	}

	for _, mb := range fs.blks {
		mb.mu.Lock()
		if minAge < mb.first.ts {
			nts = mb.first.ts
			mb.mu.Unlock()
			break
		}
		// Can we remove whole block here?
		// TODO(nat): We can't do this with LimitsTTL as we have no way to know
		// if we're throwing away real messages or other tombstones without
		// loading them, so in this case we'll fall through to the "slow path".
		// There might be a better way of handling this though.
		if mb.fs.cfg.SubjectDeleteMarkerTTL <= 0 && mb.last.ts <= minAge {
			purged += mb.msgs
			bytes += mb.bytes
			err := deleteEmptyBlock(mb)
			mb.mu.Unlock()
			if isPermissionError(err) {
				return err
			}
			continue
		}

		// If we are here we have to process the interior messages of this blk.
		// This will load fss as well.
		if err := mb.loadMsgsWithLock(); err != nil {
			mb.mu.Unlock()
			break
		}

		var smv StoreMsg
		var needNextFirst bool

		// Walk messages and remove if expired.
		fseq, lseq := atomic.LoadUint64(&mb.first.seq), atomic.LoadUint64(&mb.last.seq)
		for seq := fseq; seq <= lseq; seq++ {
			sm, err := mb.cacheLookup(seq, &smv)
			// Process interior deleted msgs.
			if err == errDeletedMsg {
				// Update dmap.
				if mb.dmap.Exists(seq) {
					mb.dmap.Delete(seq)
				}
				// Keep this updated just in case since we are removing dmap entries.
				atomic.StoreUint64(&mb.first.seq, seq)
				needNextFirst = true
				continue
			}
			// Break on other errors.
			if err != nil || sm == nil {
				atomic.StoreUint64(&mb.first.seq, seq)
				needNextFirst = true
				break
			}

			// No error and sm != nil from here onward.

			// Check for done.
			if minAge < sm.ts {
				atomic.StoreUint64(&mb.first.seq, sm.seq)
				mb.first.ts = sm.ts
				needNextFirst = false
				nts = sm.ts
				break
			}

			// Delete the message here.
			if mb.msgs > 0 {
				atomic.StoreUint64(&mb.first.seq, seq)
				needNextFirst = true
				sz := fileStoreMsgSize(sm.subj, sm.hdr, sm.msg)
				if sz > mb.bytes {
					sz = mb.bytes
				}
				mb.bytes -= sz
				bytes += sz
				mb.msgs--
				purged++
			}
			// Update fss
			// Make sure we have fss loaded.
			mb.removeSeqPerSubject(sm.subj, seq)
			fs.removePerSubject(sm.subj, fs.cfg.SubjectDeleteMarkerTTL > 0 && len(getHeader(JSMarkerReason, sm.hdr)) == 0)
		}
		// Make sure we have a proper next first sequence.
		if needNextFirst {
			mb.selectNextFirst()
		}
		// Check if empty after processing, could happen if tail of messages are all deleted.
		if mb.msgs == 0 {
			deleteEmptyBlock(mb)
		}
		mb.mu.Unlock()
		break
	}

	if nts > 0 {
		// Make sure to set age check based on this value.
		fs.resetAgeChk(nts - minAge)
	}

	if deleted > 0 {
		// Update block map.
		if fs.bim != nil {
			for _, mb := range fs.blks[:deleted] {
				delete(fs.bim, mb.index)
			}
		}
		// Update blks slice.
		fs.blks = copyMsgBlocks(fs.blks[deleted:])
		if lb := len(fs.blks); lb == 0 {
			fs.lmb = nil
		} else {
			fs.lmb = fs.blks[lb-1]
		}
	}
	// Update top level accounting.
	if purged < fs.state.Msgs {
		fs.state.Msgs -= purged
	} else {
		fs.state.Msgs = 0
	}
	if bytes < fs.state.Bytes {
		fs.state.Bytes -= bytes
	} else {
		fs.state.Bytes = 0
	}
	// Make sure to we properly set the fs first sequence and timestamp.
	fs.selectNextFirst()

	// Check if we have no messages and blocks left.
	if fs.lmb == nil && last.seq != 0 {
		if lmb, _ := fs.newMsgBlockForWrite(); lmb != nil {
			fs.writeTombstone(last.seq, last.ts)
		}
		// Clear any global subject state.
		fs.psim, fs.tsl = fs.psim.Empty(), 0
	}

	// If we have pending markers, then create them.
	fs.subjectDeleteMarkersAfterOperation(JSMarkerReasonMaxAge)

	// If we purged anything, make sure we kick flush state loop.
	if purged > 0 {
		fs.dirty++
	}
	return nil
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

	fseq := atomic.LoadUint64(&mb.first.seq)
	lseq := atomic.LoadUint64(&mb.last.seq)

	var smv StoreMsg

	// Linear search, hence the dumb part..
	ts := t.UnixNano()
	for seq := fseq; seq <= lseq; seq++ {
		sm, _, _ := mb.fetchMsg(seq, &smv)
		if sm != nil && sm.ts >= ts {
			return sm.seq
		}
	}
	return 0
}

// Find the first matching message against a sublist.
func (mb *msgBlock) firstMatchingMulti(sl *Sublist, start uint64, sm *StoreMsg) (*StoreMsg, bool, error) {
	mb.mu.Lock()
	var didLoad bool
	var updateLLTS bool
	defer func() {
		if updateLLTS {
			mb.llts = time.Now().UnixNano()
		}
		mb.mu.Unlock()
	}()

	// Need messages loaded from here on out.
	if mb.cacheNotLoaded() {
		if err := mb.loadMsgsWithLock(); err != nil {
			return nil, false, err
		}
		didLoad = true
	}

	// Make sure to start at mb.first.seq if fseq < mb.first.seq
	if seq := atomic.LoadUint64(&mb.first.seq); seq > start {
		start = seq
	}
	lseq := atomic.LoadUint64(&mb.last.seq)

	if sm == nil {
		sm = new(StoreMsg)
	}

	// If the FSS state has fewer entries than sequences in the linear scan,
	// then use intersection instead as likely going to be cheaper. This will
	// often be the case with high numbers of deletes, as well as a smaller
	// number of subjects in the block.
	if uint64(mb.fss.Size()) < lseq-start {
		// If there are no subject matches then this is effectively no-op.
		hseq := uint64(math.MaxUint64)
		IntersectStree(mb.fss, sl, func(subj []byte, ss *SimpleState) {
			if ss.firstNeedsUpdate || ss.lastNeedsUpdate {
				// mb is already loaded into the cache so should be fast-ish.
				mb.recalculateForSubj(bytesToString(subj), ss)
			}
			first := ss.First
			if start > first {
				first = start
			}
			if first > ss.Last || first >= hseq {
				// The start cutoff is after the last sequence for this subject,
				// or we think we already know of a subject with an earlier msg
				// than our first seq for this subject.
				return
			}
			if first == ss.First {
				// If the start floor is below where this subject starts then we can
				// short-circuit, avoiding needing to scan for the next message.
				if fsm, err := mb.cacheLookup(ss.First, sm); err == nil {
					sm = fsm
					hseq = ss.First
				}
				return
			}
			for seq := first; seq <= ss.Last; seq++ {
				// Otherwise we have a start floor that intersects where this subject
				// has messages in the block, so we need to walk up until we find a
				// message matching the subject.
				if mb.dmap.Exists(seq) {
					// Optimisation to avoid calling cacheLookup which hits time.Now().
					// Instead we will update it only once in a defer.
					updateLLTS = true
					continue
				}
				llseq := mb.llseq
				fsm, err := mb.cacheLookup(seq, sm)
				if err != nil {
					continue
				}
				updateLLTS = false // cacheLookup already updated it.
				if sl.HasInterest(fsm.subj) {
					hseq = seq
					sm = fsm
					break
				}
				// If we are here we did not match, so put the llseq back.
				mb.llseq = llseq
			}
		})
		if hseq < uint64(math.MaxUint64) && sm != nil {
			return sm, didLoad, nil
		}
	} else {
		for seq := start; seq <= lseq; seq++ {
			if mb.dmap.Exists(seq) {
				// Optimisation to avoid calling cacheLookup which hits time.Now().
				// Instead we will update it only once in a defer.
				updateLLTS = true
				continue
			}
			llseq := mb.llseq
			fsm, err := mb.cacheLookup(seq, sm)
			if err != nil {
				continue
			}
			expireOk := seq == lseq && mb.llseq == seq
			updateLLTS = false // cacheLookup already updated it.
			if sl.HasInterest(fsm.subj) {
				return fsm, expireOk, nil
			}
			// If we are here we did not match, so put the llseq back.
			mb.llseq = llseq
		}
	}

	return nil, didLoad, ErrStoreMsgNotFound
}

// Find the first matching message.
// fs lock should be held.
func (mb *msgBlock) firstMatching(filter string, wc bool, start uint64, sm *StoreMsg) (*StoreMsg, bool, error) {
	mb.mu.Lock()
	var updateLLTS bool
	defer func() {
		if updateLLTS {
			mb.llts = time.Now().UnixNano()
		}
		mb.mu.Unlock()
	}()

	fseq, isAll, subs := start, filter == _EMPTY_ || filter == fwcs, []string{filter}

	var didLoad bool
	if mb.fssNotLoaded() {
		// Make sure we have fss loaded.
		mb.loadMsgsWithLock()
		didLoad = true
	}
	// Mark fss activity.
	mb.lsts = time.Now().UnixNano()

	if filter == _EMPTY_ {
		filter = fwcs
		wc = true
	}

	// If we only have 1 subject currently and it matches our filter we can also set isAll.
	if !isAll && mb.fss.Size() == 1 {
		if !wc {
			_, isAll = mb.fss.Find(stringToBytes(filter))
		} else {
			// Since mb.fss.Find won't work if filter is a wildcard, need to use Match instead.
			mb.fss.Match(stringToBytes(filter), func(subject []byte, _ *SimpleState) {
				isAll = true
			})
		}
	}
	// Make sure to start at mb.first.seq if fseq < mb.first.seq
	if seq := atomic.LoadUint64(&mb.first.seq); seq > fseq {
		fseq = seq
	}
	lseq := atomic.LoadUint64(&mb.last.seq)

	// Optionally build the isMatch for wildcard filters.
	_tsa, _fsa := [32]string{}, [32]string{}
	tsa, fsa := _tsa[:0], _fsa[:0]
	var isMatch func(subj string) bool
	// Decide to build.
	if wc {
		fsa = tokenizeSubjectIntoSlice(fsa[:0], filter)
		isMatch = func(subj string) bool {
			tsa = tokenizeSubjectIntoSlice(tsa[:0], subj)
			return isSubsetMatchTokenized(tsa, fsa)
		}
	}

	subjs := mb.fs.cfg.Subjects
	// If isAll or our single filter matches the filter arg do linear scan.
	doLinearScan := isAll || (wc && len(subjs) == 1 && subjs[0] == filter)
	// If we do not think we should do a linear scan check how many fss we
	// would need to scan vs the full range of the linear walk. Optimize for
	// 25th quantile of a match in a linear walk. Filter should be a wildcard.
	// We should consult fss if our cache is not loaded and we only have fss loaded.
	if !doLinearScan && wc && mb.cacheAlreadyLoaded() {
		doLinearScan = mb.fss.Size()*4 > int(lseq-fseq)
	}

	if !doLinearScan {
		// If we have a wildcard match against all tracked subjects we know about.
		if wc {
			subs = subs[:0]
			mb.fss.Match(stringToBytes(filter), func(bsubj []byte, _ *SimpleState) {
				subs = append(subs, string(bsubj))
			})
			// Check if we matched anything
			if len(subs) == 0 {
				return nil, didLoad, ErrStoreMsgNotFound
			}
		}
		fseq = lseq + 1
		for _, subj := range subs {
			ss, _ := mb.fss.Find(stringToBytes(subj))
			if ss != nil && (ss.firstNeedsUpdate || ss.lastNeedsUpdate) {
				mb.recalculateForSubj(subj, ss)
			}
			if ss == nil || start > ss.Last || ss.First >= fseq {
				continue
			}
			if ss.First < start {
				fseq = start
			} else {
				fseq = ss.First
			}
		}
	}

	if fseq > lseq {
		return nil, didLoad, ErrStoreMsgNotFound
	}

	// If we guess to not do a linear scan, but the above resulted in alot of subs that will
	// need to be checked for every scanned message, revert.
	// TODO(dlc) - we could memoize the subs across calls.
	if !doLinearScan && len(subs) > int(lseq-fseq) {
		doLinearScan = true
	}

	// Need messages loaded from here on out.
	if mb.cacheNotLoaded() {
		if err := mb.loadMsgsWithLock(); err != nil {
			return nil, false, err
		}
		didLoad = true
	}

	if sm == nil {
		sm = new(StoreMsg)
	}

	for seq := fseq; seq <= lseq; seq++ {
		if mb.dmap.Exists(seq) {
			// Optimisation to avoid calling cacheLookup which hits time.Now().
			// Instead we will update it only once in a defer.
			updateLLTS = true
			continue
		}
		llseq := mb.llseq
		fsm, err := mb.cacheLookup(seq, sm)
		if err != nil {
			if err == errPartialCache || err == errNoCache {
				return nil, false, err
			}
			continue
		}
		updateLLTS = false // cacheLookup already updated it.
		expireOk := seq == lseq && mb.llseq == seq
		if isAll {
			return fsm, expireOk, nil
		}
		if doLinearScan {
			if wc && isMatch(sm.subj) {
				return fsm, expireOk, nil
			} else if !wc && fsm.subj == filter {
				return fsm, expireOk, nil
			}
		} else {
			for _, subj := range subs {
				if fsm.subj == subj {
					return fsm, expireOk, nil
				}
			}
		}
		// If we are here we did not match, so put the llseq back.
		mb.llseq = llseq
	}

	return nil, didLoad, ErrStoreMsgNotFound
}

// This will traverse a message block and generate the filtered pending.
func (mb *msgBlock) filteredPending(subj string, wc bool, seq uint64) (total, first, last uint64) {
	mb.mu.Lock()
	defer mb.mu.Unlock()
	return mb.filteredPendingLocked(subj, wc, seq)
}

// This will traverse a message block and generate the filtered pending.
// Lock should be held.
func (mb *msgBlock) filteredPendingLocked(filter string, wc bool, sseq uint64) (total, first, last uint64) {
	isAll := filter == _EMPTY_ || filter == fwcs

	// First check if we can optimize this part.
	// This means we want all and the starting sequence was before this block.
	if isAll {
		if fseq := atomic.LoadUint64(&mb.first.seq); sseq <= fseq {
			return mb.msgs, fseq, atomic.LoadUint64(&mb.last.seq)
		}
	}

	if filter == _EMPTY_ {
		filter = fwcs
		wc = true
	}

	update := func(ss *SimpleState) {
		total += ss.Msgs
		if first == 0 || ss.First < first {
			first = ss.First
		}
		if ss.Last > last {
			last = ss.Last
		}
	}

	// Make sure we have fss loaded.
	mb.ensurePerSubjectInfoLoaded()

	_tsa, _fsa := [32]string{}, [32]string{}
	tsa, fsa := _tsa[:0], _fsa[:0]
	fsa = tokenizeSubjectIntoSlice(fsa[:0], filter)

	// 1. See if we match any subs from fss.
	// 2. If we match and the sseq is past ss.Last then we can use meta only.
	// 3. If we match and we need to do a partial, break and clear any totals and do a full scan like num pending.

	isMatch := func(subj string) bool {
		if !wc {
			return subj == filter
		}
		tsa = tokenizeSubjectIntoSlice(tsa[:0], subj)
		return isSubsetMatchTokenized(tsa, fsa)
	}

	var havePartial bool
	mb.fss.Match(stringToBytes(filter), func(bsubj []byte, ss *SimpleState) {
		if havePartial {
			// If we already found a partial then don't do anything else.
			return
		}
		if ss.firstNeedsUpdate || ss.lastNeedsUpdate {
			mb.recalculateForSubj(bytesToString(bsubj), ss)
		}
		if sseq <= ss.First {
			update(ss)
		} else if sseq <= ss.Last {
			// We matched but its a partial.
			havePartial = true
		}
	})

	// If we did not encounter any partials we can return here.
	if !havePartial {
		return total, first, last
	}

	// If we are here we need to scan the msgs.
	// Clear what we had.
	total, first, last = 0, 0, 0

	// If we load the cache for a linear scan we want to expire that cache upon exit.
	var shouldExpire bool
	if mb.cacheNotLoaded() {
		mb.loadMsgsWithLock()
		shouldExpire = true
	}

	var smv StoreMsg
	for seq, lseq := sseq, atomic.LoadUint64(&mb.last.seq); seq <= lseq; seq++ {
		sm, _ := mb.cacheLookup(seq, &smv)
		if sm == nil {
			continue
		}
		if isAll || isMatch(sm.subj) {
			total++
			if first == 0 || seq < first {
				first = seq
			}
			if seq > last {
				last = seq
			}
		}
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
	defer fs.mu.RUnlock()

	lseq := fs.state.LastSeq
	if sseq < fs.state.FirstSeq {
		sseq = fs.state.FirstSeq
	}

	// Returned state.
	var ss SimpleState

	// If past the end no results.
	if sseq > lseq {
		// Make sure we track sequences
		ss.First = fs.state.FirstSeq
		ss.Last = fs.state.LastSeq
		return ss
	}

	// If we want all msgs that match we can shortcircuit.
	// TODO(dlc) - This can be extended for all cases but would
	// need to be careful on total msgs calculations etc.
	if sseq == fs.state.FirstSeq {
		fs.numFilteredPending(subj, &ss)
	} else {
		wc := subjectHasWildcard(subj)
		// Tracking subject state.
		// TODO(dlc) - Optimize for 2.10 with avl tree and no atomics per block.
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
	}

	return ss
}

// This is used to see if we can selectively jump start blocks based on filter subject and a starting block index.
// Will return -1 and ErrStoreEOF if no matches at all or no more from where we are.
func (fs *fileStore) checkSkipFirstBlock(filter string, wc bool, bi int) (int, error) {
	// If we match everything, just move to next blk.
	if filter == _EMPTY_ || filter == fwcs {
		return bi + 1, nil
	}
	// Move through psim to gather start and stop bounds.
	start, stop := uint32(math.MaxUint32), uint32(0)
	if wc {
		fs.psim.Match(stringToBytes(filter), func(_ []byte, psi *psi) {
			if psi.fblk < start {
				start = psi.fblk
			}
			if psi.lblk > stop {
				stop = psi.lblk
			}
		})
	} else if psi, ok := fs.psim.Find(stringToBytes(filter)); ok {
		start, stop = psi.fblk, psi.lblk
	}
	// Nothing was found.
	if start == uint32(math.MaxUint32) {
		return -1, ErrStoreEOF
	}
	// Can not be nil so ok to inline dereference.
	mbi := fs.blks[bi].getIndex()
	// All matching msgs are behind us.
	// Less than AND equal is important because we were called because we missed searching bi.
	if stop <= mbi {
		return -1, ErrStoreEOF
	}
	// If start is > index return dereference of fs.blks index.
	if start > mbi {
		if mb := fs.bim[start]; mb != nil {
			ni, _ := fs.selectMsgBlockWithIndex(atomic.LoadUint64(&mb.last.seq))
			return ni, nil
		}
	}
	// Otherwise just bump to the next one.
	return bi + 1, nil
}

// Optimized way for getting all num pending matching a filter subject.
// Lock should be held.
func (fs *fileStore) numFilteredPending(filter string, ss *SimpleState) {
	fs.numFilteredPendingWithLast(filter, true, ss)
}

// Optimized way for getting all num pending matching a filter subject and first sequence only.
// Lock should be held.
func (fs *fileStore) numFilteredPendingNoLast(filter string, ss *SimpleState) {
	fs.numFilteredPendingWithLast(filter, false, ss)
}

// Optimized way for getting all num pending matching a filter subject.
// Optionally look up last sequence. Sometimes do not need last and this avoids cost.
// Read lock should be held.
func (fs *fileStore) numFilteredPendingWithLast(filter string, last bool, ss *SimpleState) {
	isAll := filter == _EMPTY_ || filter == fwcs

	// If isAll we do not need to do anything special to calculate the first and last and total.
	if isAll {
		ss.First = fs.state.FirstSeq
		ss.Last = fs.state.LastSeq
		ss.Msgs = fs.state.Msgs
		return
	}
	// Always reset.
	ss.First, ss.Last, ss.Msgs = 0, 0, 0

	// We do need to figure out the first and last sequences.
	wc := subjectHasWildcard(filter)
	start, stop := uint32(math.MaxUint32), uint32(0)

	if wc {
		fs.psim.Match(stringToBytes(filter), func(_ []byte, psi *psi) {
			ss.Msgs += psi.total
			// Keep track of start and stop indexes for this subject.
			if psi.fblk < start {
				start = psi.fblk
			}
			if psi.lblk > stop {
				stop = psi.lblk
			}
		})
	} else if psi, ok := fs.psim.Find(stringToBytes(filter)); ok {
		ss.Msgs += psi.total
		start, stop = psi.fblk, psi.lblk
	}

	// Did not find anything.
	if stop == 0 {
		return
	}

	// Do start
	mb := fs.bim[start]
	if mb != nil {
		_, f, _ := mb.filteredPending(filter, wc, 0)
		ss.First = f
	}

	if ss.First == 0 {
		// This is a miss. This can happen since psi.fblk is lazy.
		// We will make sure to update fblk.

		// Hold this outside loop for psim fblk updates when done.
		i := start + 1
		for ; i <= stop; i++ {
			mb := fs.bim[i]
			if mb == nil {
				continue
			}
			if _, f, _ := mb.filteredPending(filter, wc, 0); f > 0 {
				ss.First = f
				break
			}
		}
		// Update fblk since fblk was outdated.
		// We only require read lock here as that is desirable,
		// so we need to do this in a go routine to acquire write lock.
		go func() {
			fs.mu.Lock()
			defer fs.mu.Unlock()
			if !wc {
				if info, ok := fs.psim.Find(stringToBytes(filter)); ok {
					if i > info.fblk {
						info.fblk = i
					}
				}
			} else {
				fs.psim.Match(stringToBytes(filter), func(subj []byte, psi *psi) {
					if i > psi.fblk {
						psi.fblk = i
					}
				})
			}
		}()
	}
	// Now gather last sequence if asked to do so.
	if last {
		if mb = fs.bim[stop]; mb != nil {
			_, _, l := mb.filteredPending(filter, wc, 0)
			ss.Last = l
		}
	}
}

// SubjectsState returns a map of SimpleState for all matching subjects.
func (fs *fileStore) SubjectsState(subject string) map[string]SimpleState {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	if fs.state.Msgs == 0 || fs.noTrackSubjects() {
		return nil
	}

	if subject == _EMPTY_ {
		subject = fwcs
	}

	start, stop := fs.blks[0], fs.lmb
	// We can short circuit if not a wildcard using psim for start and stop.
	if !subjectHasWildcard(subject) {
		info, ok := fs.psim.Find(stringToBytes(subject))
		if !ok {
			return nil
		}
		if f := fs.bim[info.fblk]; f != nil {
			start = f
		}
		if l := fs.bim[info.lblk]; l != nil {
			stop = l
		}
	}

	// Aggregate fss.
	fss := make(map[string]SimpleState)
	var startFound bool

	for _, mb := range fs.blks {
		if !startFound {
			if mb != start {
				continue
			}
			startFound = true
		}

		mb.mu.Lock()
		var shouldExpire bool
		if mb.fssNotLoaded() {
			// Make sure we have fss loaded.
			mb.loadMsgsWithLock()
			shouldExpire = true
		}
		// Mark fss activity.
		mb.lsts = time.Now().UnixNano()
		mb.fss.Match(stringToBytes(subject), func(bsubj []byte, ss *SimpleState) {
			subj := string(bsubj)
			if ss.firstNeedsUpdate || ss.lastNeedsUpdate {
				mb.recalculateForSubj(subj, ss)
			}
			oss := fss[subj]
			if oss.First == 0 { // New
				fss[subj] = *ss
			} else {
				// Merge here.
				oss.Last, oss.Msgs = ss.Last, oss.Msgs+ss.Msgs
				fss[subj] = oss
			}
		})
		if shouldExpire {
			// Expire this cache before moving on.
			mb.tryForceExpireCacheLocked()
		}
		mb.mu.Unlock()

		if mb == stop {
			break
		}
	}

	return fss
}

// MultiLastSeqs will return a sorted list of sequences that match all subjects presented in filters.
// We will not exceed the maxSeq, which if 0 becomes the store's last sequence.
func (fs *fileStore) MultiLastSeqs(filters []string, maxSeq uint64, maxAllowed int) ([]uint64, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	if fs.state.Msgs == 0 || fs.noTrackSubjects() {
		return nil, nil
	}

	lastBlkIndex := len(fs.blks) - 1
	lastMB := fs.blks[lastBlkIndex]

	// Implied last sequence.
	if maxSeq == 0 {
		maxSeq = fs.state.LastSeq
	} else {
		// Udate last mb index if not last seq.
		lastBlkIndex, lastMB = fs.selectMsgBlockWithIndex(maxSeq)
	}
	//Make sure non-nil
	if lastMB == nil {
		return nil, nil
	}

	// Grab our last mb index (not same as blk index).
	lastMB.mu.RLock()
	lastMBIndex := lastMB.index
	lastMB.mu.RUnlock()

	subs := make(map[string]*psi)
	ltSeen := make(map[string]uint32)
	for _, filter := range filters {
		fs.psim.Match(stringToBytes(filter), func(subj []byte, psi *psi) {
			s := string(subj)
			subs[s] = psi
			if psi.lblk < lastMBIndex {
				ltSeen[s] = psi.lblk
			}
		})
	}

	// If all subjects have a lower last index, select the largest for our walk backwards.
	if len(ltSeen) == len(subs) {
		max := uint32(0)
		for _, mbi := range ltSeen {
			if mbi > max {
				max = mbi
			}
		}
		lastMB = fs.bim[max]
	}

	// Collect all sequences needed.
	seqs := make([]uint64, 0, len(subs))
	for i, lnf := lastBlkIndex, false; i >= 0; i-- {
		if len(subs) == 0 {
			break
		}
		mb := fs.blks[i]
		if !lnf {
			if mb != lastMB {
				continue
			}
			lnf = true
		}
		// We can start properly looking here.
		mb.mu.Lock()
		mb.ensurePerSubjectInfoLoaded()
		for subj, psi := range subs {
			if ss, ok := mb.fss.Find(stringToBytes(subj)); ok && ss != nil {
				if ss.Last <= maxSeq {
					seqs = append(seqs, ss.Last)
					delete(subs, subj)
				} else {
					// Need to search for it since last is > maxSeq.
					if mb.cacheNotLoaded() {
						mb.loadMsgsWithLock()
					}
					var smv StoreMsg
					fseq := atomic.LoadUint64(&mb.first.seq)
					for seq := maxSeq; seq >= fseq; seq-- {
						sm, _ := mb.cacheLookup(seq, &smv)
						if sm == nil || sm.subj != subj {
							continue
						}
						seqs = append(seqs, sm.seq)
						delete(subs, subj)
						break
					}
				}
			} else if mb.index <= psi.fblk {
				// Track which subs are no longer applicable, meaning we will not find a valid msg at this point.
				delete(subs, subj)
			}
			// TODO(dlc) we could track lblk like above in case some subs are very far apart.
			// Not too bad if fss loaded since we will skip over quickly with it loaded, but might be worth it.
		}
		mb.mu.Unlock()

		// If maxAllowed was sepcified check that we will not exceed that.
		if maxAllowed > 0 && len(seqs) > maxAllowed {
			return nil, ErrTooManyResults
		}

	}
	if len(seqs) == 0 {
		return nil, nil
	}
	slices.Sort(seqs)
	return seqs, nil
}

// NumPending will return the number of pending messages matching the filter subject starting at sequence.
// Optimized for stream num pending calculations for consumers.
func (fs *fileStore) NumPending(sseq uint64, filter string, lastPerSubject bool) (total, validThrough uint64) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	// This can always be last for these purposes.
	validThrough = fs.state.LastSeq

	if fs.state.Msgs == 0 || sseq > fs.state.LastSeq {
		return 0, validThrough
	}

	// If sseq is less then our first set to first.
	if sseq < fs.state.FirstSeq {
		sseq = fs.state.FirstSeq
	}
	// Track starting for both block for the sseq and staring block that matches any subject.
	var seqStart int
	// See if we need to figure out starting block per sseq.
	if sseq > fs.state.FirstSeq {
		// This should not, but can return -1, so make sure we check to avoid panic below.
		if seqStart, _ = fs.selectMsgBlockWithIndex(sseq); seqStart < 0 {
			seqStart = 0
		}
	}

	isAll := filter == _EMPTY_ || filter == fwcs
	if isAll && filter == _EMPTY_ {
		filter = fwcs
	}
	wc := subjectHasWildcard(filter)

	// See if filter was provided but its the only subject.
	if !isAll && !wc && fs.psim.Size() == 1 {
		_, isAll = fs.psim.Find(stringToBytes(filter))
	}
	// If we are isAll and have no deleted we can do a simpler calculation.
	if !lastPerSubject && isAll && (fs.state.LastSeq-fs.state.FirstSeq+1) == fs.state.Msgs {
		if sseq == 0 {
			return fs.state.Msgs, validThrough
		}
		return fs.state.LastSeq - sseq + 1, validThrough
	}

	_tsa, _fsa := [32]string{}, [32]string{}
	tsa, fsa := _tsa[:0], _fsa[:0]
	if wc {
		fsa = tokenizeSubjectIntoSlice(fsa[:0], filter)
	}

	isMatch := func(subj string) bool {
		if isAll {
			return true
		}
		if !wc {
			return subj == filter
		}
		tsa = tokenizeSubjectIntoSlice(tsa[:0], subj)
		return isSubsetMatchTokenized(tsa, fsa)
	}

	// Handle last by subject a bit differently.
	// We will scan PSIM since we accurately track the last block we have seen the subject in. This
	// allows us to only need to load at most one block now.
	// For the last block, we need to track the subjects that we know are in that block, and track seen
	// while in the block itself, but complexity there worth it.
	if lastPerSubject {
		// If we want all and our start sequence is equal or less than first return number of subjects.
		if isAll && sseq <= fs.state.FirstSeq {
			return uint64(fs.psim.Size()), validThrough
		}
		// If we are here we need to scan. We are going to scan the PSIM looking for lblks that are >= seqStart.
		// This will build up a list of all subjects from the selected block onward.
		lbm := make(map[string]bool)
		mb := fs.blks[seqStart]
		bi := mb.index

		fs.psim.Match(stringToBytes(filter), func(subj []byte, psi *psi) {
			// If the select blk start is greater than entry's last blk skip.
			if bi > psi.lblk {
				return
			}
			total++
			// We will track the subjects that are an exact match to the last block.
			// This is needed for last block processing.
			if psi.lblk == bi {
				lbm[string(subj)] = true
			}
		})

		// Now check if we need to inspect the seqStart block.
		// Grab write lock in case we need to load in msgs.
		mb.mu.Lock()
		var updateLLTS bool
		var shouldExpire bool
		// We need to walk this block to correct accounting from above.
		if sseq > mb.first.seq {
			// Track the ones we add back in case more than one.
			seen := make(map[string]bool)
			// We need to discount the total by subjects seen before sseq, but also add them right back in if they are >= sseq for this blk.
			// This only should be subjects we know have the last blk in this block.
			if mb.cacheNotLoaded() {
				mb.loadMsgsWithLock()
				shouldExpire = true
			}
			var smv StoreMsg
			for seq, lseq := atomic.LoadUint64(&mb.first.seq), atomic.LoadUint64(&mb.last.seq); seq <= lseq; seq++ {
				if mb.dmap.Exists(seq) {
					// Optimisation to avoid calling cacheLookup which hits time.Now().
					updateLLTS = true
					continue
				}
				sm, _ := mb.cacheLookup(seq, &smv)
				if sm == nil || sm.subj == _EMPTY_ || !lbm[sm.subj] {
					continue
				}
				updateLLTS = false // cacheLookup already updated it.
				if isMatch(sm.subj) {
					// If less than sseq adjust off of total as long as this subject matched the last block.
					if seq < sseq {
						if !seen[sm.subj] {
							total--
							seen[sm.subj] = true
						}
					} else if seen[sm.subj] {
						// This is equal or more than sseq, so add back in.
						total++
						// Make sure to not process anymore.
						delete(seen, sm.subj)
					}
				}
			}
		}
		// If we loaded the block try to force expire.
		if shouldExpire {
			mb.tryForceExpireCacheLocked()
		}
		if updateLLTS {
			mb.llts = time.Now().UnixNano()
		}
		mb.mu.Unlock()
		return total, validThrough
	}

	// If we would need to scan more from the beginning, revert back to calculating directly here.
	// TODO(dlc) - Redo properly with sublists etc for subject-based filtering.
	if seqStart >= (len(fs.blks) / 2) {
		for i := seqStart; i < len(fs.blks); i++ {
			var shouldExpire bool
			mb := fs.blks[i]
			// Hold write lock in case we need to load cache.
			mb.mu.Lock()
			if isAll && sseq <= atomic.LoadUint64(&mb.first.seq) {
				total += mb.msgs
				mb.mu.Unlock()
				continue
			}
			// If we are here we need to at least scan the subject fss.
			// Make sure we have fss loaded.
			if mb.fssNotLoaded() {
				mb.loadMsgsWithLock()
				shouldExpire = true
			}
			// Mark fss activity.
			mb.lsts = time.Now().UnixNano()

			var t uint64
			var havePartial bool
			mb.fss.Match(stringToBytes(filter), func(bsubj []byte, ss *SimpleState) {
				if havePartial {
					// If we already found a partial then don't do anything else.
					return
				}
				subj := bytesToString(bsubj)
				if ss.firstNeedsUpdate || ss.lastNeedsUpdate {
					mb.recalculateForSubj(subj, ss)
				}
				if sseq <= ss.First {
					t += ss.Msgs
				} else if sseq <= ss.Last {
					// We matched but its a partial.
					havePartial = true
				}
			})

			// See if we need to scan msgs here.
			if havePartial {
				// Make sure we have the cache loaded.
				if mb.cacheNotLoaded() {
					mb.loadMsgsWithLock()
					shouldExpire = true
				}
				// Clear on partial.
				t = 0
				start := sseq
				if fseq := atomic.LoadUint64(&mb.first.seq); fseq > start {
					start = fseq
				}
				var smv StoreMsg
				for seq, lseq := start, atomic.LoadUint64(&mb.last.seq); seq <= lseq; seq++ {
					if sm, _ := mb.cacheLookup(seq, &smv); sm != nil && isMatch(sm.subj) {
						t++
					}
				}
			}
			// If we loaded this block for this operation go ahead and expire it here.
			if shouldExpire {
				mb.tryForceExpireCacheLocked()
			}
			mb.mu.Unlock()
			total += t
		}
		return total, validThrough
	}

	// If we are here it's better to calculate totals from psim and adjust downward by scanning less blocks.
	// TODO(dlc) - Eventually when sublist uses generics, make this sublist driven instead.
	start := uint32(math.MaxUint32)
	fs.psim.Match(stringToBytes(filter), func(_ []byte, psi *psi) {
		total += psi.total
		// Keep track of start index for this subject.
		if psi.fblk < start {
			start = psi.fblk
		}
	})
	// See if we were asked for all, if so we are done.
	if sseq <= fs.state.FirstSeq {
		return total, validThrough
	}

	// If we are here we need to calculate partials for the first blocks.
	firstSubjBlk := fs.bim[start]
	var firstSubjBlkFound bool
	// Adjust in case not found.
	if firstSubjBlk == nil {
		firstSubjBlkFound = true
	}

	// Track how many we need to adjust against the total.
	var adjust uint64
	for i := 0; i <= seqStart; i++ {
		mb := fs.blks[i]
		// We can skip blks if we know they are below the first one that has any subject matches.
		if !firstSubjBlkFound {
			if firstSubjBlkFound = (mb == firstSubjBlk); !firstSubjBlkFound {
				continue
			}
		}
		// We need to scan this block.
		var shouldExpire bool
		var updateLLTS bool
		mb.mu.Lock()
		// Check if we should include all of this block in adjusting. If so work with metadata.
		if sseq > atomic.LoadUint64(&mb.last.seq) {
			if isAll {
				adjust += mb.msgs
			} else {
				// We need to adjust for all matches in this block.
				// Make sure we have fss loaded. This loads whole block now.
				if mb.fssNotLoaded() {
					mb.loadMsgsWithLock()
					shouldExpire = true
				}
				// Mark fss activity.
				mb.lsts = time.Now().UnixNano()

				mb.fss.Match(stringToBytes(filter), func(bsubj []byte, ss *SimpleState) {
					adjust += ss.Msgs
				})
			}
		} else {
			// This is the last block. We need to scan per message here.
			if mb.cacheNotLoaded() {
				mb.loadMsgsWithLock()
				shouldExpire = true
			}
			var last = atomic.LoadUint64(&mb.last.seq)
			if sseq < last {
				last = sseq
			}
			// We need to walk all messages in this block
			var smv StoreMsg
			for seq := atomic.LoadUint64(&mb.first.seq); seq < last; seq++ {
				if mb.dmap.Exists(seq) {
					// Optimisation to avoid calling cacheLookup which hits time.Now().
					updateLLTS = true
					continue
				}
				sm, _ := mb.cacheLookup(seq, &smv)
				if sm == nil || sm.subj == _EMPTY_ {
					continue
				}
				updateLLTS = false // cacheLookup already updated it.
				// Check if it matches our filter.
				if sm.seq < sseq && isMatch(sm.subj) {
					adjust++
				}
			}
		}
		// If we loaded the block try to force expire.
		if shouldExpire {
			mb.tryForceExpireCacheLocked()
		}
		if updateLLTS {
			mb.llts = time.Now().UnixNano()
		}
		mb.mu.Unlock()
	}
	// Make final adjustment.
	total -= adjust

	return total, validThrough
}

// NumPending will return the number of pending messages matching any subject in the sublist starting at sequence.
// Optimized for stream num pending calculations for consumers with lots of filtered subjects.
// Subjects should not overlap, this property is held when doing multi-filtered consumers.
func (fs *fileStore) NumPendingMulti(sseq uint64, sl *Sublist, lastPerSubject bool) (total, validThrough uint64) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	// This can always be last for these purposes.
	validThrough = fs.state.LastSeq

	if fs.state.Msgs == 0 || sseq > fs.state.LastSeq {
		return 0, validThrough
	}

	// If sseq is less then our first set to first.
	if sseq < fs.state.FirstSeq {
		sseq = fs.state.FirstSeq
	}
	// Track starting for both block for the sseq and staring block that matches any subject.
	var seqStart int
	// See if we need to figure out starting block per sseq.
	if sseq > fs.state.FirstSeq {
		// This should not, but can return -1, so make sure we check to avoid panic below.
		if seqStart, _ = fs.selectMsgBlockWithIndex(sseq); seqStart < 0 {
			seqStart = 0
		}
	}

	isAll := sl == nil

	// See if filter was provided but its the only subject.
	if !isAll && fs.psim.Size() == 1 {
		fs.psim.IterFast(func(subject []byte, _ *psi) bool {
			isAll = sl.HasInterest(bytesToString(subject))
			return true
		})
	}
	// If we are isAll and have no deleted we can do a simpler calculation.
	if !lastPerSubject && isAll && (fs.state.LastSeq-fs.state.FirstSeq+1) == fs.state.Msgs {
		if sseq == 0 {
			return fs.state.Msgs, validThrough
		}
		return fs.state.LastSeq - sseq + 1, validThrough
	}
	// Setup the isMatch function.
	isMatch := func(subj string) bool {
		if isAll {
			return true
		}
		return sl.HasInterest(subj)
	}

	// Handle last by subject a bit differently.
	// We will scan PSIM since we accurately track the last block we have seen the subject in. This
	// allows us to only need to load at most one block now.
	// For the last block, we need to track the subjects that we know are in that block, and track seen
	// while in the block itself, but complexity there worth it.
	if lastPerSubject {
		// If we want all and our start sequence is equal or less than first return number of subjects.
		if isAll && sseq <= fs.state.FirstSeq {
			return uint64(fs.psim.Size()), validThrough
		}
		// If we are here we need to scan. We are going to scan the PSIM looking for lblks that are >= seqStart.
		// This will build up a list of all subjects from the selected block onward.
		lbm := make(map[string]bool)
		mb := fs.blks[seqStart]
		bi := mb.index

		subs := make([]*subscription, 0, sl.Count())
		sl.All(&subs)
		for _, sub := range subs {
			fs.psim.Match(sub.subject, func(subj []byte, psi *psi) {
				// If the select blk start is greater than entry's last blk skip.
				if bi > psi.lblk {
					return
				}
				total++
				// We will track the subjects that are an exact match to the last block.
				// This is needed for last block processing.
				if psi.lblk == bi {
					lbm[string(subj)] = true
				}
			})
		}

		// Now check if we need to inspect the seqStart block.
		// Grab write lock in case we need to load in msgs.
		mb.mu.Lock()
		var shouldExpire bool
		var updateLLTS bool
		// We need to walk this block to correct accounting from above.
		if sseq > mb.first.seq {
			// Track the ones we add back in case more than one.
			seen := make(map[string]bool)
			// We need to discount the total by subjects seen before sseq, but also add them right back in if they are >= sseq for this blk.
			// This only should be subjects we know have the last blk in this block.
			if mb.cacheNotLoaded() {
				mb.loadMsgsWithLock()
				shouldExpire = true
			}
			var smv StoreMsg
			for seq, lseq := atomic.LoadUint64(&mb.first.seq), atomic.LoadUint64(&mb.last.seq); seq <= lseq; seq++ {
				if mb.dmap.Exists(seq) {
					// Optimisation to avoid calling cacheLookup which hits time.Now().
					updateLLTS = true
					continue
				}
				sm, _ := mb.cacheLookup(seq, &smv)
				if sm == nil || sm.subj == _EMPTY_ || !lbm[sm.subj] {
					continue
				}
				updateLLTS = false // cacheLookup already updated it.
				if isMatch(sm.subj) {
					// If less than sseq adjust off of total as long as this subject matched the last block.
					if seq < sseq {
						if !seen[sm.subj] {
							total--
							seen[sm.subj] = true
						}
					} else if seen[sm.subj] {
						// This is equal or more than sseq, so add back in.
						total++
						// Make sure to not process anymore.
						delete(seen, sm.subj)
					}
				}
			}
		}
		// If we loaded the block try to force expire.
		if shouldExpire {
			mb.tryForceExpireCacheLocked()
		}
		if updateLLTS {
			mb.llts = time.Now().UnixNano()
		}
		mb.mu.Unlock()
		return total, validThrough
	}

	// If we would need to scan more from the beginning, revert back to calculating directly here.
	if seqStart >= (len(fs.blks) / 2) {
		for i := seqStart; i < len(fs.blks); i++ {
			var shouldExpire bool
			mb := fs.blks[i]
			// Hold write lock in case we need to load cache.
			mb.mu.Lock()
			if isAll && sseq <= atomic.LoadUint64(&mb.first.seq) {
				total += mb.msgs
				mb.mu.Unlock()
				continue
			}
			// If we are here we need to at least scan the subject fss.
			// Make sure we have fss loaded.
			if mb.fssNotLoaded() {
				mb.loadMsgsWithLock()
				shouldExpire = true
			}
			// Mark fss activity.
			mb.lsts = time.Now().UnixNano()

			var t uint64
			var havePartial bool
			var updateLLTS bool
			IntersectStree[SimpleState](mb.fss, sl, func(bsubj []byte, ss *SimpleState) {
				subj := bytesToString(bsubj)
				if havePartial {
					// If we already found a partial then don't do anything else.
					return
				}
				if ss.firstNeedsUpdate || ss.lastNeedsUpdate {
					mb.recalculateForSubj(subj, ss)
				}
				if sseq <= ss.First {
					t += ss.Msgs
				} else if sseq <= ss.Last {
					// We matched but its a partial.
					havePartial = true
				}
			})

			// See if we need to scan msgs here.
			if havePartial {
				// Make sure we have the cache loaded.
				if mb.cacheNotLoaded() {
					mb.loadMsgsWithLock()
					shouldExpire = true
				}
				// Clear on partial.
				t = 0
				start := sseq
				if fseq := atomic.LoadUint64(&mb.first.seq); fseq > start {
					start = fseq
				}
				var smv StoreMsg
				for seq, lseq := start, atomic.LoadUint64(&mb.last.seq); seq <= lseq; seq++ {
					if mb.dmap.Exists(seq) {
						// Optimisation to avoid calling cacheLookup which hits time.Now().
						updateLLTS = true
						continue
					}
					if sm, _ := mb.cacheLookup(seq, &smv); sm != nil && isMatch(sm.subj) {
						t++
						updateLLTS = false // cacheLookup already updated it.
					}
				}
			}
			// If we loaded this block for this operation go ahead and expire it here.
			if shouldExpire {
				mb.tryForceExpireCacheLocked()
			}
			if updateLLTS {
				mb.llts = time.Now().UnixNano()
			}
			mb.mu.Unlock()
			total += t
		}
		return total, validThrough
	}

	// If we are here it's better to calculate totals from psim and adjust downward by scanning less blocks.
	start := uint32(math.MaxUint32)
	subs := make([]*subscription, 0, sl.Count())
	sl.All(&subs)
	for _, sub := range subs {
		fs.psim.Match(sub.subject, func(_ []byte, psi *psi) {
			total += psi.total
			// Keep track of start index for this subject.
			if psi.fblk < start {
				start = psi.fblk
			}
		})
	}
	// See if we were asked for all, if so we are done.
	if sseq <= fs.state.FirstSeq {
		return total, validThrough
	}

	// If we are here we need to calculate partials for the first blocks.
	firstSubjBlk := fs.bim[start]
	var firstSubjBlkFound bool
	// Adjust in case not found.
	if firstSubjBlk == nil {
		firstSubjBlkFound = true
	}

	// Track how many we need to adjust against the total.
	var adjust uint64
	for i := 0; i <= seqStart; i++ {
		mb := fs.blks[i]
		// We can skip blks if we know they are below the first one that has any subject matches.
		if !firstSubjBlkFound {
			if firstSubjBlkFound = (mb == firstSubjBlk); !firstSubjBlkFound {
				continue
			}
		}
		// We need to scan this block.
		var shouldExpire bool
		var updateLLTS bool
		mb.mu.Lock()
		// Check if we should include all of this block in adjusting. If so work with metadata.
		if sseq > atomic.LoadUint64(&mb.last.seq) {
			if isAll {
				adjust += mb.msgs
			} else {
				// We need to adjust for all matches in this block.
				// Make sure we have fss loaded. This loads whole block now.
				if mb.fssNotLoaded() {
					mb.loadMsgsWithLock()
					shouldExpire = true
				}
				// Mark fss activity.
				mb.lsts = time.Now().UnixNano()
				IntersectStree(mb.fss, sl, func(bsubj []byte, ss *SimpleState) {
					adjust += ss.Msgs
				})
			}
		} else {
			// This is the last block. We need to scan per message here.
			if mb.cacheNotLoaded() {
				mb.loadMsgsWithLock()
				shouldExpire = true
			}
			var last = atomic.LoadUint64(&mb.last.seq)
			if sseq < last {
				last = sseq
			}
			// We need to walk all messages in this block
			var smv StoreMsg
			for seq := atomic.LoadUint64(&mb.first.seq); seq < last; seq++ {
				if mb.dmap.Exists(seq) {
					// Optimisation to avoid calling cacheLookup which hits time.Now().
					updateLLTS = true
					continue
				}
				sm, _ := mb.cacheLookup(seq, &smv)
				if sm == nil || sm.subj == _EMPTY_ {
					continue
				}
				updateLLTS = false // cacheLookup already updated it.
				// Check if it matches our filter.
				if sm.seq < sseq && isMatch(sm.subj) {
					adjust++
				}
			}
		}
		// If we loaded the block try to force expire.
		if shouldExpire {
			mb.tryForceExpireCacheLocked()
		}
		if updateLLTS {
			mb.llts = time.Now().UnixNano()
		}
		mb.mu.Unlock()
	}
	// Make final adjustment.
	total -= adjust

	return total, validThrough
}

// SubjectsTotals return message totals per subject.
func (fs *fileStore) SubjectsTotals(filter string) map[string]uint64 {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	if fs.psim.Size() == 0 {
		return nil
	}
	// Match all if no filter given.
	if filter == _EMPTY_ {
		filter = fwcs
	}
	fst := make(map[string]uint64)
	fs.psim.Match(stringToBytes(filter), func(subj []byte, psi *psi) {
		fst[string(subj)] = psi.total
	})
	return fst
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

// RegisterSubjectDeleteMarkerUpdates registers a callback for updates to new tombstones.
func (fs *fileStore) RegisterSubjectDeleteMarkerUpdates(cb SubjectDeleteMarkerUpdateHandler) {
	fs.mu.Lock()
	fs.sdmcb = cb
	fs.mu.Unlock()
}

// Helper to get hash key for specific message block.
// Lock should be held
func (fs *fileStore) hashKeyForBlock(index uint32) []byte {
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
	mb.llts = time.Now().UnixNano()
	mb.startCacheExpireTimer()
}

// This rolls to a new append msg block.
// Lock should be held.
func (fs *fileStore) newMsgBlockForWrite() (*msgBlock, error) {
	index := uint32(1)
	var rbuf []byte

	if lmb := fs.lmb; lmb != nil {
		index = lmb.index + 1
		// Determine if we can reclaim any resources here.
		if fs.fip {
			lmb.mu.Lock()
			lmb.closeFDsLocked()
			if lmb.cache != nil {
				// Reset write timestamp and see if we can expire this cache.
				rbuf = lmb.tryExpireWriteCache()
			}
			lmb.mu.Unlock()
		}
	}

	mb := fs.initMsgBlock(index)
	// Lock should be held to quiet race detector.
	mb.mu.Lock()
	mb.setupWriteCache(rbuf)
	mb.fss = stree.NewSubjectTree[SimpleState]()

	// Set cache time to creation time to start.
	ts := time.Now().UnixNano()
	mb.llts, mb.lwts = 0, ts
	// Remember our last sequence number.
	atomic.StoreUint64(&mb.first.seq, fs.state.LastSeq+1)
	atomic.StoreUint64(&mb.last.seq, fs.state.LastSeq)
	mb.mu.Unlock()

	// Now do local hash.
	key := sha256.Sum256(fs.hashKeyForBlock(index))
	hh, err := highwayhash.New64(key[:])
	if err != nil {
		return nil, fmt.Errorf("could not create hash: %v", err)
	}
	mb.hh = hh

	<-dios
	mfd, err := os.OpenFile(mb.mfn, os.O_CREATE|os.O_RDWR, defaultFilePerms)
	dios <- struct{}{}

	if err != nil {
		if isPermissionError(err) {
			return nil, err
		}
		mb.dirtyCloseWithRemove(true)
		return nil, fmt.Errorf("Error creating msg block file: %v", err)
	}
	mb.mfd = mfd

	// Check if encryption is enabled.
	if fs.prf != nil {
		if err := fs.genEncryptionKeysForBlock(mb); err != nil {
			return nil, err
		}
	}

	// If we know we will need this so go ahead and spin up.
	if !fs.fip {
		mb.spinUpFlushLoop()
	}

	// Add to our list of blocks and mark as last.
	fs.addMsgBlock(mb)

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
	err = fs.writeFileWithOptionalSync(keyFile, encrypted, defaultFilePerms)
	if err != nil {
		return err
	}
	mb.kfn = keyFile
	return nil
}

// Stores a raw message with expected sequence number and timestamp.
// Lock should be held.
func (fs *fileStore) storeRawMsg(subj string, hdr, msg []byte, seq uint64, ts, ttl int64) (err error) {
	if fs.closed {
		return ErrStoreClosed
	}

	// Per subject max check needed.
	mmp := uint64(fs.cfg.MaxMsgsPer)
	var psmc uint64
	psmax := mmp > 0 && len(subj) > 0
	if psmax {
		if info, ok := fs.psim.Find(stringToBytes(subj)); ok {
			psmc = info.total
		}
	}

	var fseq uint64
	// Check if we are discarding new messages when we reach the limit.
	if fs.cfg.Discard == DiscardNew {
		var asl bool
		if psmax && psmc >= mmp {
			// If we are instructed to discard new per subject, this is an error.
			if fs.cfg.DiscardNewPer {
				return ErrMaxMsgsPerSubject
			}
			if fseq, err = fs.firstSeqForSubj(subj); err != nil {
				return err
			}
			asl = true
		}
		// If we are discard new and limits policy and clustered, we do the enforcement
		// above and should not disqualify the message here since it could cause replicas to drift.
		if fs.cfg.Retention == LimitsPolicy || fs.cfg.Replicas == 1 {
			if fs.cfg.MaxMsgs > 0 && fs.state.Msgs >= uint64(fs.cfg.MaxMsgs) && !asl {
				return ErrMaxMsgs
			}
			if fs.cfg.MaxBytes > 0 && fs.state.Bytes+fileStoreMsgSize(subj, hdr, msg) >= uint64(fs.cfg.MaxBytes) {
				if !asl || fs.sizeForSeq(fseq) <= int(fileStoreMsgSize(subj, hdr, msg)) {
					return ErrMaxBytes
				}
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
	// Add expiry bit to sequence if needed. This is so that if we need to
	// rebuild, we know which messages to look at more quickly.
	n, err := fs.writeMsgRecord(seq, ts, subj, hdr, msg)
	if err != nil {
		return err
	}

	// Adjust top level tracking of per subject msg counts.
	if len(subj) > 0 && fs.psim != nil {
		index := fs.lmb.index
		if info, ok := fs.psim.Find(stringToBytes(subj)); ok {
			info.total++
			if index > info.lblk {
				info.lblk = index
			}
		} else {
			fs.psim.Insert(stringToBytes(subj), psi{total: 1, fblk: index, lblk: index})
			fs.tsl += len(subj)
		}
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
	// We snapshotted psmc before our actual write, so >= comparison needed.
	if psmax && psmc >= mmp {
		// We may have done this above.
		if fseq == 0 {
			fseq, _ = fs.firstSeqForSubj(subj)
		}
		if ok, _ := fs.removeMsgViaLimits(fseq); ok {
			// Make sure we are below the limit.
			if psmc--; psmc >= mmp {
				bsubj := stringToBytes(subj)
				for info, ok := fs.psim.Find(bsubj); ok && info.total > mmp; info, ok = fs.psim.Find(bsubj) {
					if seq, _ := fs.firstSeqForSubj(subj); seq > 0 {
						if ok, _ := fs.removeMsgViaLimits(seq); !ok {
							break
						}
					} else {
						break
					}
				}
			}
		} else if mb := fs.selectMsgBlock(fseq); mb != nil {
			// If we are here we could not remove fseq from above, so rebuild.
			var ld *LostStreamData
			if ld, _, _ = mb.rebuildState(); ld != nil {
				fs.rebuildStateLocked(ld)
			}
		}
	}

	// Limits checks and enforcement.
	// If they do any deletions they will update the
	// byte count on their own, so no need to compensate.
	fs.enforceMsgLimit()
	fs.enforceBytesLimit()

	// Per-message TTL.
	if fs.ttls != nil && ttl > 0 {
		expires := time.Duration(ts) + (time.Second * time.Duration(ttl))
		fs.ttls.Add(seq, int64(expires))
		fs.lmb.ttls++
		if seq > fs.ttlseq {
			fs.ttlseq = seq
		}
	}

	// Check if we have and need the age expiration timer running.
	switch {
	case fs.ttls != nil && ttl > 0:
		fs.resetAgeChk(0)
	case fs.ageChk == nil && (fs.cfg.MaxAge > 0 || fs.ttls != nil):
		fs.startAgeChk()
	}

	return nil
}

// StoreRawMsg stores a raw message with expected sequence number and timestamp.
func (fs *fileStore) StoreRawMsg(subj string, hdr, msg []byte, seq uint64, ts, ttl int64) error {
	fs.mu.Lock()
	err := fs.storeRawMsg(subj, hdr, msg, seq, ts, ttl)
	cb := fs.scb
	// Check if first message timestamp requires expiry
	// sooner than initial replica expiry timer set to MaxAge when initializing.
	if !fs.receivedAny && fs.cfg.MaxAge != 0 && ts > 0 {
		fs.receivedAny = true
		// don't block here by calling expireMsgs directly.
		// Instead, set short timeout.
		fs.resetAgeChk(int64(time.Millisecond * 50))
	}
	fs.mu.Unlock()

	if err == nil && cb != nil {
		cb(1, int64(fileStoreMsgSize(subj, hdr, msg)), seq, subj)
	}

	return err
}

// Store stores a message. We hold the main filestore lock for any write operation.
func (fs *fileStore) StoreMsg(subj string, hdr, msg []byte, ttl int64) (uint64, int64, error) {
	fs.mu.Lock()
	seq, ts := fs.state.LastSeq+1, time.Now().UnixNano()
	err := fs.storeRawMsg(subj, hdr, msg, seq, ts, ttl)
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
// we will place an empty record marking the sequence as used. The
// sequence will be marked erased.
// fs lock should be held.
func (mb *msgBlock) skipMsg(seq uint64, now time.Time) {
	if mb == nil {
		return
	}
	var needsRecord bool

	nowts := now.UnixNano()

	mb.mu.Lock()
	// If we are empty can just do meta.
	if mb.msgs == 0 {
		atomic.StoreUint64(&mb.last.seq, seq)
		mb.last.ts = nowts
		atomic.StoreUint64(&mb.first.seq, seq+1)
		mb.first.ts = nowts
		needsRecord = mb == mb.fs.lmb
		if needsRecord && mb.rbytes > 0 {
			// We want to make sure since we have no messages
			// that we write to the beginning since we only need last one.
			mb.rbytes, mb.cache = 0, &cache{}
			// If encrypted we need to reset counter since we just keep one.
			if mb.bek != nil {
				// Recreate to reset counter.
				mb.bek, _ = genBlockEncryptionKey(mb.fs.fcfg.Cipher, mb.seed, mb.nonce)
			}
		}
	} else {
		needsRecord = true
		mb.dmap.Insert(seq)
	}
	mb.mu.Unlock()

	if needsRecord {
		mb.writeMsgRecord(emptyRecordLen, seq|ebit, _EMPTY_, nil, nil, nowts, true)
	} else {
		mb.kickFlusher()
	}
}

// SkipMsg will use the next sequence number but not store anything.
func (fs *fileStore) SkipMsg() uint64 {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Grab our current last message block.
	mb := fs.lmb
	if mb == nil || mb.msgs > 0 && mb.blkSize()+emptyRecordLen > fs.fcfg.BlockSize {
		if mb != nil && fs.fcfg.Compression != NoCompression {
			// We've now reached the end of this message block, if we want
			// to compress blocks then now's the time to do it.
			go mb.recompressOnDiskIfNeeded()
		}
		var err error
		if mb, err = fs.newMsgBlockForWrite(); err != nil {
			return 0
		}
	}

	// Grab time and last seq.
	now, seq := time.Now().UTC(), fs.state.LastSeq+1

	// Write skip msg.
	mb.skipMsg(seq, now)

	// Update fs state.
	fs.state.LastSeq, fs.state.LastTime = seq, now
	if fs.state.Msgs == 0 {
		fs.state.FirstSeq, fs.state.FirstTime = seq, now
	}
	if seq == fs.state.FirstSeq {
		fs.state.FirstSeq, fs.state.FirstTime = seq+1, now
	}
	// Mark as dirty for stream state.
	fs.dirty++

	return seq
}

// Skip multiple msgs. We will determine if we can fit into current lmb or we need to create a new block.
func (fs *fileStore) SkipMsgs(seq uint64, num uint64) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Check sequence matches our last sequence.
	if seq != fs.state.LastSeq+1 {
		if seq > 0 {
			return ErrSequenceMismatch
		}
		seq = fs.state.LastSeq + 1
	}

	// Limit number of dmap entries
	const maxDeletes = 64 * 1024
	mb := fs.lmb

	numDeletes := int(num)
	if mb != nil {
		numDeletes += mb.dmap.Size()
	}
	if mb == nil || numDeletes > maxDeletes && mb.msgs > 0 || mb.msgs > 0 && mb.blkSize()+emptyRecordLen > fs.fcfg.BlockSize {
		if mb != nil && fs.fcfg.Compression != NoCompression {
			// We've now reached the end of this message block, if we want
			// to compress blocks then now's the time to do it.
			go mb.recompressOnDiskIfNeeded()
		}
		var err error
		if mb, err = fs.newMsgBlockForWrite(); err != nil {
			return err
		}
	}

	// Insert into dmap all entries and place last as marker.
	now := time.Now().UTC()
	nowts := now.UnixNano()
	lseq := seq + num - 1

	mb.mu.Lock()
	// If we are empty update meta directly.
	if mb.msgs == 0 {
		atomic.StoreUint64(&mb.last.seq, lseq)
		mb.last.ts = nowts
		atomic.StoreUint64(&mb.first.seq, lseq+1)
		mb.first.ts = nowts
	} else {
		for ; seq <= lseq; seq++ {
			mb.dmap.Insert(seq)
		}
	}
	mb.mu.Unlock()

	// Write out our placeholder.
	mb.writeMsgRecord(emptyRecordLen, lseq|ebit, _EMPTY_, nil, nil, nowts, true)

	// Now update FS accounting.
	// Update fs state.
	fs.state.LastSeq, fs.state.LastTime = lseq, now
	if fs.state.Msgs == 0 {
		fs.state.FirstSeq, fs.state.FirstTime = lseq+1, now
	}

	// Mark as dirty for stream state.
	fs.dirty++

	return nil
}

// Lock should be held.
func (fs *fileStore) rebuildFirst() {
	if len(fs.blks) == 0 {
		return
	}
	fmb := fs.blks[0]
	if fmb == nil {
		return
	}

	ld, _, _ := fmb.rebuildState()
	fmb.mu.RLock()
	isEmpty := fmb.msgs == 0
	fmb.mu.RUnlock()
	if isEmpty {
		fmb.mu.Lock()
		fs.removeMsgBlock(fmb)
		fmb.mu.Unlock()
	}
	fs.selectNextFirst()
	fs.rebuildStateLocked(ld)
}

// Optimized helper function to return first sequence.
// subj will always be publish subject here, meaning non-wildcard.
// We assume a fast check that this subj even exists already happened.
// Write lock should be held.
func (fs *fileStore) firstSeqForSubj(subj string) (uint64, error) {
	if len(fs.blks) == 0 {
		return 0, nil
	}

	// See if we can optimize where we start.
	start, stop := fs.blks[0].index, fs.lmb.index
	if info, ok := fs.psim.Find(stringToBytes(subj)); ok {
		start, stop = info.fblk, info.lblk
	}

	for i := start; i <= stop; i++ {
		mb := fs.bim[i]
		if mb == nil {
			continue
		}
		// If we need to load msgs here and we need to walk multiple blocks this
		// could tie up the upper fs lock, so release while dealing with the block.
		fs.mu.Unlock()

		mb.mu.Lock()
		var shouldExpire bool
		if mb.fssNotLoaded() {
			// Make sure we have fss loaded.
			if err := mb.loadMsgsWithLock(); err != nil {
				mb.mu.Unlock()
				// Re-acquire fs lock
				fs.mu.Lock()
				return 0, err
			}
			shouldExpire = true
		}
		// Mark fss activity.
		mb.lsts = time.Now().UnixNano()

		bsubj := stringToBytes(subj)
		if ss, ok := mb.fss.Find(bsubj); ok && ss != nil {
			// Adjust first if it was not where we thought it should be.
			if i != start {
				if info, ok := fs.psim.Find(bsubj); ok {
					info.fblk = i
				}
			}
			if ss.firstNeedsUpdate || ss.lastNeedsUpdate {
				mb.recalculateForSubj(subj, ss)
			}
			mb.mu.Unlock()
			// Re-acquire fs lock
			fs.mu.Lock()
			return ss.First, nil
		}
		// If we did not find it and we loaded this msgBlock try to expire as long as not the last.
		if shouldExpire {
			// Expire this cache before moving on.
			mb.tryForceExpireCacheLocked()
		}
		mb.mu.Unlock()
		// Re-acquire fs lock
		fs.mu.Lock()
	}
	return 0, nil
}

// Will check the msg limit and drop firstSeq msg if needed.
// Lock should be held.
func (fs *fileStore) enforceMsgLimit() {
	if fs.cfg.Discard != DiscardOld {
		return
	}
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
	if fs.cfg.Discard != DiscardOld {
		return
	}
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

// Will make sure we have limits honored for max msgs per subject on recovery or config update.
// We will make sure to go through all msg blocks etc. but in practice this
// will most likely only be the last one, so can take a more conservative approach.
// Lock should be held.
func (fs *fileStore) enforceMsgPerSubjectLimit(fireCallback bool) {
	start := time.Now()
	defer func() {
		if took := time.Since(start); took > time.Minute {
			fs.warn("enforceMsgPerSubjectLimit took %v", took.Round(time.Millisecond))
		}
	}()

	maxMsgsPer := uint64(fs.cfg.MaxMsgsPer)

	// We may want to suppress callbacks from remove during this process
	// since these should have already been deleted and accounted for.
	if !fireCallback {
		cb := fs.scb
		fs.scb = nil
		defer func() { fs.scb = cb }()
	}

	var numMsgs uint64

	// collect all that are not correct.
	needAttention := make(map[string]*psi)
	fs.psim.IterFast(func(subj []byte, psi *psi) bool {
		numMsgs += psi.total
		if psi.total > maxMsgsPer {
			needAttention[string(subj)] = psi
		}
		return true
	})

	// We had an issue with a use case where psim (and hence fss) were correct but idx was not and was not properly being caught.
	// So do a quick sanity check here. If we detect a skew do a rebuild then re-check.
	if numMsgs != fs.state.Msgs {
		fs.warn("Detected skew in subject-based total (%d) vs raw total (%d), rebuilding", numMsgs, fs.state.Msgs)
		// Clear any global subject state.
		fs.psim, fs.tsl = fs.psim.Empty(), 0
		for _, mb := range fs.blks {
			ld, _, err := mb.rebuildState()
			if err != nil && ld != nil {
				fs.addLostData(ld)
			}
			fs.populateGlobalPerSubjectInfo(mb)
		}
		// Rebuild fs state too.
		fs.rebuildStateLocked(nil)
		// Need to redo blocks that need attention.
		needAttention = make(map[string]*psi)
		fs.psim.IterFast(func(subj []byte, psi *psi) bool {
			if psi.total > maxMsgsPer {
				needAttention[string(subj)] = psi
			}
			return true
		})
	}

	// Collect all the msgBlks we alter.
	blks := make(map[*msgBlock]struct{})

	// For re-use below.
	var sm StoreMsg

	// Walk all subjects that need attention here.
	for subj, info := range needAttention {
		total, start, stop := info.total, info.fblk, info.lblk

		for i := start; i <= stop; i++ {
			mb := fs.bim[i]
			if mb == nil {
				continue
			}
			// Grab the ss entry for this subject in case sparse.
			mb.mu.Lock()
			mb.ensurePerSubjectInfoLoaded()
			ss, ok := mb.fss.Find(stringToBytes(subj))
			if ok && ss != nil && (ss.firstNeedsUpdate || ss.lastNeedsUpdate) {
				mb.recalculateForSubj(subj, ss)
			}
			mb.mu.Unlock()
			if ss == nil {
				continue
			}
			for seq := ss.First; seq <= ss.Last && total > maxMsgsPer; {
				m, _, err := mb.firstMatching(subj, false, seq, &sm)
				if err == nil {
					seq = m.seq + 1
					if removed, _ := fs.removeMsgViaLimits(m.seq); removed {
						total--
						blks[mb] = struct{}{}
					}
				} else {
					// On error just do single increment.
					seq++
				}
			}
		}
	}

	// Expire the cache if we can.
	for mb := range blks {
		mb.mu.Lock()
		if mb.msgs > 0 {
			mb.tryForceExpireCacheLocked()
		}
		mb.mu.Unlock()
	}
}

// Lock should be held.
func (fs *fileStore) deleteFirstMsg() (bool, error) {
	return fs.removeMsgViaLimits(fs.state.FirstSeq)
}

// If we remove via limits that can always be recovered on a restart we
// do not force the system to update the index file.
// Lock should be held.
func (fs *fileStore) removeMsgViaLimits(seq uint64) (bool, error) {
	return fs.removeMsg(seq, false, true, false)
}

// RemoveMsg will remove the message from this store.
// Will return the number of bytes removed.
func (fs *fileStore) RemoveMsg(seq uint64) (bool, error) {
	return fs.removeMsg(seq, false, false, true)
}

func (fs *fileStore) EraseMsg(seq uint64) (bool, error) {
	return fs.removeMsg(seq, true, false, true)
}

// Convenience function to remove per subject tracking at the filestore level.
// Lock should be held. Returns if we deleted the last message on the subject.
func (fs *fileStore) removePerSubject(subj string, marker bool) bool {
	if len(subj) == 0 || fs.psim == nil {
		return false
	}
	// We do not update sense of fblk here but will do so when we resolve during lookup.
	bsubj := stringToBytes(subj)
	if info, ok := fs.psim.Find(bsubj); ok {
		info.total--
		if info.total == 1 {
			info.fblk = info.lblk
		} else if info.total == 0 {
			if _, ok = fs.psim.Delete(bsubj); ok {
				fs.tsl -= len(subj)
				if marker {
					fs.markers = append(fs.markers, subj)
				}
				return true
			}
		}
	}
	return false
}

// Remove a message, optionally rewriting the mb file.
func (fs *fileStore) removeMsg(seq uint64, secure, viaLimits, needFSLock bool) (bool, error) {
	if seq == 0 {
		return false, ErrStoreMsgNotFound
	}
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
	if !viaLimits && fs.sips > 0 {
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

	// See if we are closed or the sequence number is still relevant or if we know its deleted.
	if mb.closed || seq < atomic.LoadUint64(&mb.first.seq) || mb.dmap.Exists(seq) {
		mb.mu.Unlock()
		fsUnlock()
		return false, nil
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

	var smv StoreMsg
	sm, err := mb.cacheLookup(seq, &smv)
	if err != nil {
		mb.mu.Unlock()
		fsUnlock()
		// Mimic err behavior from above check to dmap. No error returned if already removed.
		if err == errDeletedMsg {
			err = nil
		}
		return false, err
	}
	// Grab size
	msz := fileStoreMsgSize(sm.subj, sm.hdr, sm.msg)

	// Set cache timestamp for last remove.
	mb.lrts = time.Now().UnixNano()

	// Global stats
	if fs.state.Msgs > 0 {
		fs.state.Msgs--
	}
	if msz < fs.state.Bytes {
		fs.state.Bytes -= msz
	} else {
		fs.state.Bytes = 0
	}

	// Now local mb updates.
	if mb.msgs > 0 {
		mb.msgs--
	}
	if msz < mb.bytes {
		mb.bytes -= msz
	} else {
		mb.bytes = 0
	}

	// Allow us to check compaction again.
	mb.noCompact = false

	// Mark as dirty for stream state.
	fs.dirty++

	// If we are tracking subjects here make sure we update that accounting.
	mb.ensurePerSubjectInfoLoaded()

	// If we are tracking multiple subjects here make sure we update that accounting.
	mb.removeSeqPerSubject(sm.subj, seq)
	wasLast := fs.removePerSubject(sm.subj, false)

	if secure {
		// Grab record info.
		ri, rl, _, _ := mb.slotInfo(int(seq - mb.cache.fseq))
		if err := mb.eraseMsg(seq, int(ri), int(rl)); err != nil {
			return false, err
		}
	}

	fifo := seq == atomic.LoadUint64(&mb.first.seq)
	isLastBlock := mb == fs.lmb
	isEmpty := mb.msgs == 0

	if fifo {
		mb.selectNextFirst()
		if !isEmpty {
			// Can update this one in place.
			if seq == fs.state.FirstSeq {
				fs.state.FirstSeq = atomic.LoadUint64(&mb.first.seq) // new one.
				if mb.first.ts == 0 {
					fs.state.FirstTime = time.Time{}
				} else {
					fs.state.FirstTime = time.Unix(0, mb.first.ts).UTC()
				}
			}
		}
	} else if !isEmpty {
		// Out of order delete.
		mb.dmap.Insert(seq)
		// Make simple check here similar to Compact(). If we can save 50% and over a certain threshold do inline.
		// All other more thorough cleanup will happen in syncBlocks logic.
		// Note that we do not have to store empty records for the deleted, so don't use to calculate.
		// TODO(dlc) - This should not be inline, should kick the sync routine.
		if !isLastBlock && mb.shouldCompactInline() {
			mb.compact()
		}
	}

	if secure {
		if ld, _ := mb.flushPendingMsgsLocked(); ld != nil {
			// We have the mb lock here, this needs the mb locks so do in its own go routine.
			go fs.rebuildState(ld)
		}
	}

	// If empty remove this block and check if we need to update first sequence.
	// We will write a tombstone at the end.
	var firstSeqNeedsUpdate bool
	if isEmpty {
		// This writes tombstone iff mb == lmb, so no need to do below.
		fs.removeMsgBlock(mb)
		firstSeqNeedsUpdate = seq == fs.state.FirstSeq
	}
	mb.mu.Unlock()

	// If the deleted message was itself a delete marker then
	// don't write out more of them or we'll churn endlessly.
	var sdmcb func()
	if wasLast && len(getHeader(JSMarkerReason, sm.hdr)) == 0 { // Not a marker.
		if viaLimits {
			sdmcb = fs.subjectDeleteMarkerIfNeeded(sm.subj, JSMarkerReasonMaxAge)
		}
	}

	// If we emptied the current message block and the seq was state.FirstSeq
	// then we need to jump message blocks. We will also write the index so
	// we don't lose track of the first sequence.
	if firstSeqNeedsUpdate {
		fs.selectNextFirst()
	}

	// Check if we need to write a deleted record tombstone.
	// This is for user initiated removes or to hold the first seq
	// when the last block is empty.

	// If not via limits and not empty (empty writes tombstone above if last) write tombstone.
	if !viaLimits && !isEmpty && sm != nil {
		fs.writeTombstone(sm.seq, sm.ts)
	}

	if cb := fs.scb; cb != nil || sdmcb != nil {
		// If we have a callback registered we need to release lock regardless since cb might need it to lookup msg, etc.
		fs.mu.Unlock()
		// Storage updates.
		if cb != nil {
			var subj string
			if sm != nil {
				subj = sm.subj
			}
			delta := int64(msz)
			cb(-1, -delta, seq, subj)
		}
		if sdmcb != nil {
			sdmcb()
		}

		if !needFSLock {
			fs.mu.Lock()
		}
	} else if needFSLock {
		// We acquired it so release it.
		fs.mu.Unlock()
	}

	return true, nil
}

// Tests whether we should try to compact this block while inline removing msgs.
// We will want rbytes to be over the minimum and have a 2x potential savings.
// If we compacted before but rbytes didn't improve much, guard against constantly compacting.
// Lock should be held.
func (mb *msgBlock) shouldCompactInline() bool {
	return mb.rbytes > compactMinimum && mb.bytes*2 < mb.rbytes && (mb.cbytes == 0 || mb.bytes*2 < mb.cbytes)
}

// Tests whether we should try to compact this block while running periodic sync.
// We will want rbytes to be over the minimum and have a 2x potential savings.
// Ignores 2MB minimum.
// Lock should be held.
func (mb *msgBlock) shouldCompactSync() bool {
	return mb.bytes*2 < mb.rbytes && !mb.noCompact
}

// This will compact and rewrite this block. This version will not process any tombstone cleanup.
// Write lock needs to be held.
func (mb *msgBlock) compact() {
	mb.compactWithFloor(0)
}

// This will compact and rewrite this block. This should only be called when we know we want to rewrite this block.
// This should not be called on the lmb since we will prune tail deleted messages which could cause issues with
// writing new messages. We will silently bail on any issues with the underlying block and let someone else detect.
// if fseq > 0 we will attempt to cleanup stale tombstones.
// Write lock needs to be held.
func (mb *msgBlock) compactWithFloor(floor uint64) {
	wasLoaded := mb.cacheAlreadyLoaded()
	if !wasLoaded {
		if err := mb.loadMsgsWithLock(); err != nil {
			return
		}
	}

	buf := mb.cache.buf
	nbuf := getMsgBlockBuf(len(buf))
	// Recycle our nbuf when we are done.
	defer recycleMsgBlockBuf(nbuf)

	var le = binary.LittleEndian
	var firstSet bool

	fseq := atomic.LoadUint64(&mb.first.seq)
	isDeleted := func(seq uint64) bool {
		return seq == 0 || seq&ebit != 0 || mb.dmap.Exists(seq) || seq < fseq
	}

	for index, lbuf := uint32(0), uint32(len(buf)); index < lbuf; {
		if index+msgHdrSize > lbuf {
			return
		}
		hdr := buf[index : index+msgHdrSize]
		rl, slen := le.Uint32(hdr[0:]), int(le.Uint16(hdr[20:]))
		// Clear any headers bit that could be set.
		rl &^= hbit
		dlen := int(rl) - msgHdrSize
		// Do some quick sanity checks here.
		if dlen < 0 || slen > (dlen-recordHashSize) || dlen > int(rl) || index+rl > lbuf || rl > rlBadThresh {
			return
		}
		// Only need to process non-deleted messages.
		seq := le.Uint64(hdr[4:])

		if !isDeleted(seq) {
			// Check for tombstones.
			if seq&tbit != 0 {
				seq = seq &^ tbit
				// If this entry is for a lower seq than ours then keep around.
				// We also check that it is greater than our floor. Floor is zero on normal
				// calls to compact.
				if seq < fseq && seq >= floor {
					nbuf = append(nbuf, buf[index:index+rl]...)
				}
			} else {
				// Normal message here.
				nbuf = append(nbuf, buf[index:index+rl]...)
				if !firstSet {
					firstSet = true
					atomic.StoreUint64(&mb.first.seq, seq)
				}
			}
		}
		// Advance to next record.
		index += rl
	}

	// Handle compression
	if mb.cmp != NoCompression && len(nbuf) > 0 {
		cbuf, err := mb.cmp.Compress(nbuf)
		if err != nil {
			return
		}
		meta := &CompressionInfo{
			Algorithm:    mb.cmp,
			OriginalSize: uint64(len(nbuf)),
		}
		nbuf = append(meta.MarshalMetadata(), cbuf...)
	}

	// Check for encryption.
	if mb.bek != nil && len(nbuf) > 0 {
		// Recreate to reset counter.
		rbek, err := genBlockEncryptionKey(mb.fs.fcfg.Cipher, mb.seed, mb.nonce)
		if err != nil {
			return
		}
		rbek.XORKeyStream(nbuf, nbuf)
	}

	// Close FDs first.
	mb.closeFDsLocked()

	// We will write to a new file and mv/rename it in case of failure.
	mfn := filepath.Join(mb.fs.fcfg.StoreDir, msgDir, fmt.Sprintf(newScan, mb.index))
	<-dios
	err := os.WriteFile(mfn, nbuf, defaultFilePerms)
	dios <- struct{}{}
	if err != nil {
		os.Remove(mfn)
		return
	}
	if err := os.Rename(mfn, mb.mfn); err != nil {
		os.Remove(mfn)
		return
	}

	// Make sure to sync
	mb.needSync = true

	// Capture the updated rbytes.
	if rbytes := uint64(len(nbuf)); rbytes == mb.rbytes {
		// No change, so set our noCompact bool here to avoid attempting to continually compress in syncBlocks.
		mb.noCompact = true
	} else {
		mb.rbytes = rbytes
	}
	mb.cbytes = mb.bytes

	// Remove any seqs from the beginning of the blk.
	for seq, nfseq := fseq, atomic.LoadUint64(&mb.first.seq); seq < nfseq; seq++ {
		mb.dmap.Delete(seq)
	}
	// Make sure we clear the cache since no longer valid.
	mb.clearCacheAndOffset()
	// If we entered with the msgs loaded make sure to reload them.
	if wasLoaded {
		mb.loadMsgsWithLock()
	}
}

// Grab info from a slot.
// Lock should be held.
func (mb *msgBlock) slotInfo(slot int) (uint32, uint32, bool, error) {
	if mb.cache == nil || slot >= len(mb.cache.idx) {
		return 0, 0, false, errPartialCache
	}

	bi := mb.cache.idx[slot]
	ri, hashChecked := (bi &^ cbit), (bi&cbit) != 0

	// If this is a deleted slot return here.
	if bi == dbit {
		return 0, 0, false, errDeletedMsg
	}

	// Determine record length
	var rl uint32
	if slot >= len(mb.cache.idx) {
		rl = mb.cache.lrl
	} else {
		// Need to account for dbit markers in idx.
		// So we will walk until we find valid idx slot to calculate rl.
		for i := 1; slot+i < len(mb.cache.idx); i++ {
			ni := mb.cache.idx[slot+i] &^ cbit
			if ni == dbit {
				continue
			}
			rl = ni - ri
			break
		}
		// check if we had all trailing dbits.
		// If so use len of cache buf minus ri.
		if rl == 0 {
			rl = uint32(len(mb.cache.buf)) - ri
		}
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

	// Are we already running or closed?
	if mb.flusher || mb.closed {
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
	if n, err := rand.Read(data); err != nil {
		return err
	} else if n != len(data) {
		return fmt.Errorf("not enough overwrite bytes read (%d != %d)", n, len(data))
	}

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
		<-dios
		mfd, err := os.OpenFile(mb.mfn, os.O_RDWR, defaultFilePerms)
		dios <- struct{}{}
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
func (mb *msgBlock) truncate(sm *StoreMsg) (nmsgs, nbytes uint64, err error) {
	mb.mu.Lock()
	defer mb.mu.Unlock()

	// Make sure we are loaded to process messages etc.
	if err := mb.loadMsgsWithLock(); err != nil {
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

	checkDmap := mb.dmap.Size() > 0
	var smv StoreMsg

	for seq := atomic.LoadUint64(&mb.last.seq); seq > sm.seq; seq-- {
		if checkDmap {
			if mb.dmap.Exists(seq) {
				// Delete and skip to next.
				mb.dmap.Delete(seq)
				checkDmap = !mb.dmap.IsEmpty()
				continue
			}
		}
		// We should have a valid msg to calculate removal stats.
		if m, err := mb.cacheLookup(seq, &smv); err == nil {
			if mb.msgs > 0 {
				rl := fileStoreMsgSize(m.subj, m.hdr, m.msg)
				mb.msgs--
				if rl > mb.bytes {
					rl = mb.bytes
				}
				mb.bytes -= rl
				mb.rbytes -= rl
				// For return accounting.
				purged++
				bytes += uint64(rl)
			}
		}
	}

	// If the block is compressed then we have to load it into memory
	// and decompress it, truncate it and then write it back out.
	// Otherwise, truncate the file itself and close the descriptor.
	if mb.cmp != NoCompression {
		buf, err := mb.loadBlock(nil)
		if err != nil {
			return 0, 0, fmt.Errorf("failed to load block from disk: %w", err)
		}
		if mb.bek != nil && len(buf) > 0 {
			bek, err := genBlockEncryptionKey(mb.fs.fcfg.Cipher, mb.seed, mb.nonce)
			if err != nil {
				return 0, 0, err
			}
			mb.bek = bek
			mb.bek.XORKeyStream(buf, buf)
		}
		buf, err = mb.decompressIfNeeded(buf)
		if err != nil {
			return 0, 0, fmt.Errorf("failed to decompress block: %w", err)
		}
		buf = buf[:eof]
		copy(mb.lchk[0:], buf[:len(buf)-checksumSize])
		buf, err = mb.cmp.Compress(buf)
		if err != nil {
			return 0, 0, fmt.Errorf("failed to recompress block: %w", err)
		}
		meta := &CompressionInfo{
			Algorithm:    mb.cmp,
			OriginalSize: uint64(eof),
		}
		buf = append(meta.MarshalMetadata(), buf...)
		if mb.bek != nil && len(buf) > 0 {
			bek, err := genBlockEncryptionKey(mb.fs.fcfg.Cipher, mb.seed, mb.nonce)
			if err != nil {
				return 0, 0, err
			}
			mb.bek = bek
			mb.bek.XORKeyStream(buf, buf)
		}
		n, err := mb.writeAt(buf, 0)
		if err != nil {
			return 0, 0, fmt.Errorf("failed to rewrite compressed block: %w", err)
		}
		if n != len(buf) {
			return 0, 0, fmt.Errorf("short write (%d != %d)", n, len(buf))
		}
		mb.mfd.Truncate(int64(len(buf)))
		mb.mfd.Sync()
	} else if mb.mfd != nil {
		mb.mfd.Truncate(eof)
		mb.mfd.Sync()
		// Update our checksum.
		var lchk [8]byte
		mb.mfd.ReadAt(lchk[:], eof-8)
		copy(mb.lchk[0:], lchk[:])
	} else {
		return 0, 0, fmt.Errorf("failed to truncate msg block %d, file not open", mb.index)
	}

	// Update our last msg.
	atomic.StoreUint64(&mb.last.seq, sm.seq)
	mb.last.ts = sm.ts

	// Clear our cache.
	mb.clearCacheAndOffset()

	// Redo per subject info for this block.
	mb.resetPerSubjectInfo()

	// Load msgs again.
	mb.loadMsgsWithLock()

	return purged, bytes, nil
}

// Helper to determine if the mb is empty.
func (mb *msgBlock) isEmpty() bool {
	return atomic.LoadUint64(&mb.first.seq) > atomic.LoadUint64(&mb.last.seq)
}

// Lock should be held.
func (mb *msgBlock) selectNextFirst() {
	var seq uint64
	fseq, lseq := atomic.LoadUint64(&mb.first.seq), atomic.LoadUint64(&mb.last.seq)
	for seq = fseq + 1; seq <= lseq; seq++ {
		if mb.dmap.Exists(seq) {
			// We will move past this so we can delete the entry.
			mb.dmap.Delete(seq)
		} else {
			break
		}
	}
	// Set new first sequence.
	atomic.StoreUint64(&mb.first.seq, seq)

	// Check if we are empty..
	if seq > lseq {
		mb.first.ts = 0
		return
	}

	// Need to get the timestamp.
	// We will try the cache direct and fallback if needed.
	var smv StoreMsg
	sm, _ := mb.cacheLookup(seq, &smv)
	if sm == nil {
		// Slow path, need to unlock.
		mb.mu.Unlock()
		sm, _, _ = mb.fetchMsg(seq, &smv)
		mb.mu.Lock()
	}
	if sm != nil {
		mb.first.ts = sm.ts
	} else {
		mb.first.ts = 0
	}
}

// Select the next FirstSeq
// Lock should be held.
func (fs *fileStore) selectNextFirst() {
	if len(fs.blks) > 0 {
		mb := fs.blks[0]
		mb.mu.RLock()
		fs.state.FirstSeq = atomic.LoadUint64(&mb.first.seq)
		fs.state.FirstTime = time.Unix(0, mb.first.ts).UTC()
		mb.mu.RUnlock()
	} else {
		// Could not find anything, so treat like purge
		fs.state.FirstSeq = fs.state.LastSeq + 1
		fs.state.FirstTime = time.Time{}
	}
	// Mark first as moved. Plays into tombstone cleanup for syncBlocks.
	fs.firstMoved = true
}

// Lock should be held.
func (mb *msgBlock) resetCacheExpireTimer(td time.Duration) {
	if td == 0 {
		td = mb.cexp + 100*time.Millisecond
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
		tsla := mb.sinceLastActivity()
		if mb.fss == nil || tsla > mb.fexp {
			// Force
			mb.fss = nil
			mb.ctmr.Stop()
			mb.ctmr = nil
		} else {
			mb.resetCacheExpireTimer(mb.fexp - tsla)
		}
	}

	if mb.cache == nil {
		return
	}

	buf := mb.cache.buf
	if mb.cache.off == 0 {
		mb.cache = nil
	} else {
		// Clear msgs and index.
		mb.cache.buf = nil
		mb.cache.idx = nil
		mb.cache.wp = 0
	}
	recycleMsgBlockBuf(buf)
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

// This is for expiration of the write cache, which will be partial with fip.
// So we want to bypass the Pools here.
// Lock should be held.
func (mb *msgBlock) tryExpireWriteCache() []byte {
	if mb.cache == nil {
		return nil
	}
	lwts, buf, llts, nra := mb.lwts, mb.cache.buf, mb.llts, mb.cache.nra
	mb.lwts, mb.cache.nra = 0, true
	mb.expireCacheLocked()
	mb.lwts = lwts
	if mb.cache != nil {
		mb.cache.nra = nra
	}
	// We could check for a certain time since last load, but to be safe just reuse if no loads at all.
	if llts == 0 && (mb.cache == nil || mb.cache.buf == nil) {
		// Clear last write time since we now are about to move on to a new lmb.
		mb.lwts = 0
		return buf[:0]
	}
	return nil
}

// Lock should be held.
func (mb *msgBlock) expireCacheLocked() {
	if mb.cache == nil && mb.fss == nil {
		if mb.ctmr != nil {
			mb.ctmr.Stop()
			mb.ctmr = nil
		}
		return
	}

	// Can't expire if we still have pending.
	if mb.cache != nil && len(mb.cache.buf)-int(mb.cache.wp) > 0 {
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
	if mb.cache != nil {
		mb.cache.off += len(mb.cache.buf)
		if !mb.cache.nra {
			recycleMsgBlockBuf(mb.cache.buf)
		}
		mb.cache.buf = nil
		mb.cache.wp = 0
	}

	// Check if we can clear out our idx unless under force expire.
	// fss we keep longer and expire under sync timer checks.
	mb.clearCache()
}

func (fs *fileStore) startAgeChk() {
	if fs.ageChk != nil {
		return
	}
	if fs.cfg.MaxAge != 0 || fs.ttls != nil {
		fs.ageChk = time.AfterFunc(fs.cfg.MaxAge, fs.expireMsgs)
	}
}

// Lock should be held.
func (fs *fileStore) resetAgeChk(delta int64) {
	var next int64 = math.MaxInt64
	if fs.ttls != nil {
		next = fs.ttls.GetNextExpiration(next)
	}

	// If there's no MaxAge and there's nothing waiting to be expired then
	// don't bother continuing. The next storeRawMsg() will wake us up if
	// needs be.
	if fs.cfg.MaxAge <= 0 && next == math.MaxInt64 {
		clearTimer(&fs.ageChk)
		return
	}

	// Check to see if we should be firing sooner than MaxAge for an expiring TTL.
	fireIn := fs.cfg.MaxAge
	if next < math.MaxInt64 {
		// Looks like there's a next expiration, use it either if there's no
		// MaxAge set or if it looks to be sooner than MaxAge is.
		if until := time.Until(time.Unix(0, next)); fireIn == 0 || until < fireIn {
			fireIn = until
		}
	}

	// If not then look at the delta provided (usually gap to next age expiry).
	if delta > 0 {
		if fireIn == 0 || time.Duration(delta) < fireIn {
			fireIn = time.Duration(delta)
		}
	}

	// Make sure we aren't firing too often either way, otherwise we can
	// negatively impact stream ingest performance.
	if fireIn < 250*time.Millisecond {
		fireIn = 250 * time.Millisecond
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

// Lock must be held so that nothing else can interleave and write a
// new message on this subject before we get the chance to write the
// delete marker. If the delete marker is written successfully then
// this function returns a callback func to call scb and sdmcb after
// the lock has been released.
func (fs *fileStore) subjectDeleteMarkerIfNeeded(subj string, reason string) func() {
	if fs.cfg.SubjectDeleteMarkerTTL <= 0 {
		return nil
	}
	if _, ok := fs.psim.Find(stringToBytes(subj)); ok {
		// There are still messages left with this subject,
		// therefore it wasn't the last message deleted.
		return nil
	}
	// Build the subject delete marker. If no TTL is specified then
	// we'll default to 15 minutes — by that time every possible condition
	// should have cleared (i.e. ordered consumer timeout, client timeouts,
	// route/gateway interruptions, even device/client restarts etc).
	ttl := int64(fs.cfg.SubjectDeleteMarkerTTL.Seconds())
	if ttl <= 0 {
		return nil
	}
	var _hdr [128]byte
	hdr := fmt.Appendf(
		_hdr[:0],
		"NATS/1.0\r\n%s: %s\r\n%s: %s\r\n%s: %d\r\n%s: %s\r\n\r\n\r\n",
		JSMarkerReason, reason,
		JSMessageTTL, time.Duration(ttl)*time.Second,
		JSExpectedLastSubjSeq, 0,
		JSExpectedLastSubjSeqSubj, subj,
	)
	msg := &inMsg{
		subj: subj,
		hdr:  hdr,
	}
	sdmcb := fs.sdmcb
	return func() {
		if sdmcb != nil {
			sdmcb(msg)
		}
	}
}

// Filestore lock must be held but message block locks must not be.
// The caller should call the callback, if non-nil, after releasing
// the filestore lock.
func (fs *fileStore) subjectDeleteMarkersAfterOperation(reason string) func() {
	if fs.cfg.SubjectDeleteMarkerTTL <= 0 || len(fs.markers) == 0 {
		return nil
	}
	cbs := make([]func(), 0, len(fs.markers))
	for _, subject := range fs.markers {
		if cb := fs.subjectDeleteMarkerIfNeeded(subject, reason); cb != nil {
			cbs = append(cbs, cb)
		}
	}
	fs.markers = nil
	return func() {
		for _, cb := range cbs {
			cb()
		}
	}
}

// Will expire msgs that are too old.
func (fs *fileStore) expireMsgs() {
	// We need to delete one by one here and can not optimize for the time being.
	// Reason is that we need more information to adjust ack pending in consumers.
	var smv StoreMsg
	var sm *StoreMsg
	fs.mu.RLock()
	maxAge := int64(fs.cfg.MaxAge)
	minAge := time.Now().UnixNano() - maxAge
	fs.mu.RUnlock()

	if maxAge > 0 {
		var seq uint64
		for sm, seq, _ = fs.LoadNextMsg(fwcs, true, 0, &smv); sm != nil && sm.ts <= minAge; sm, seq, _ = fs.LoadNextMsg(fwcs, true, seq+1, &smv) {
			if len(sm.hdr) > 0 {
				if ttl, err := getMessageTTL(sm.hdr); err == nil && ttl < 0 {
					// The message has a negative TTL, therefore it must "never expire".
					minAge = time.Now().UnixNano() - maxAge
					continue
				}
			}
			// Remove the message and then, if LimitsTTL is enabled, try and work out
			// if it was the last message of that particular subject that we just deleted.
			fs.mu.Lock()
			fs.removeMsgViaLimits(sm.seq)
			fs.mu.Unlock()
			// Recalculate in case we are expiring a bunch.
			minAge = time.Now().UnixNano() - maxAge
		}
	}

	fs.mu.Lock()
	defer fs.mu.Unlock()

	// TODO: Not great that we're holding the lock here, but the timed hash wheel isn't thread-safe.
	nextTTL := int64(math.MaxInt64)
	if fs.ttls != nil {
		fs.ttls.ExpireTasks(func(seq uint64, ts int64) {
			fs.removeMsg(seq, false, false, false)
		})
		if maxAge > 0 {
			// Only check if we're expiring something in the next MaxAge interval, saves us a bit
			// of work if MaxAge will beat us to the next expiry anyway.
			nextTTL = fs.ttls.GetNextExpiration(time.Now().Add(time.Duration(maxAge)).UnixNano())
		} else {
			nextTTL = fs.ttls.GetNextExpiration(math.MaxInt64)
		}
	}

	// Only cancel if no message left, not on potential lookup error that would result in sm == nil.
	if fs.state.Msgs == 0 && nextTTL == math.MaxInt64 {
		fs.cancelAgeChk()
	} else {
		if sm == nil {
			fs.resetAgeChk(0)
		} else {
			fs.resetAgeChk(sm.ts - minAge)
		}
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
	}
}

// This will check all the checksums on messages and report back any sequence numbers with errors.
func (fs *fileStore) checkMsgs() *LostStreamData {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	fs.checkAndFlushAllBlocks()

	// Clear any global subject state.
	fs.psim, fs.tsl = fs.psim.Empty(), 0

	for _, mb := range fs.blks {
		// Make sure encryption loaded if needed for the block.
		fs.loadEncryptionForMsgBlock(mb)
		// FIXME(dlc) - check tombstones here too?
		if ld, _, err := mb.rebuildState(); err != nil && ld != nil {
			// Rebuild fs state too.
			fs.rebuildStateLocked(ld)
		}
		fs.populateGlobalPerSubjectInfo(mb)
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
	<-dios
	mfd, err := os.OpenFile(mb.mfn, os.O_CREATE|os.O_RDWR, defaultFilePerms)
	dios <- struct{}{}
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

// Helper function to place a delete tombstone.
func (mb *msgBlock) writeTombstone(seq uint64, ts int64) error {
	return mb.writeMsgRecord(emptyRecordLen, seq|tbit, _EMPTY_, nil, nil, ts, true)
}

// Will write the message record to the underlying message block.
// filestore lock will be held.
func (mb *msgBlock) writeMsgRecord(rl, seq uint64, subj string, mhdr, msg []byte, ts int64, flush bool) error {
	mb.mu.Lock()
	defer mb.mu.Unlock()

	// Enable for writing if our mfd is not open.
	if mb.mfd == nil {
		if err := mb.enableForWriting(flush); err != nil {
			return err
		}
	}

	// Make sure we have a cache setup.
	if mb.cache == nil {
		mb.setupWriteCache(nil)
	}

	// Check if we are tracking per subject for our simple state.
	// Do this before changing the cache that would trigger a flush pending msgs call
	// if we needed to regenerate the per subject info.
	// Note that tombstones have no subject so will not trigger here.
	if len(subj) > 0 && !mb.noTrack {
		if err := mb.ensurePerSubjectInfoLoaded(); err != nil {
			return err
		}
		// Mark fss activity.
		mb.lsts = time.Now().UnixNano()
		if ss, ok := mb.fss.Find(stringToBytes(subj)); ok && ss != nil {
			ss.Msgs++
			ss.Last = seq
			ss.lastNeedsUpdate = false
		} else {
			mb.fss.Insert(stringToBytes(subj), SimpleState{Msgs: 1, First: seq, Last: seq})
		}
	}

	// Indexing
	index := len(mb.cache.buf) + int(mb.cache.off)

	// Formats
	// Format with no header
	// total_len(4) sequence(8) timestamp(8) subj_len(2) subj msg hash(8)
	// With headers, high bit on total length will be set.
	// total_len(4) sequence(8) timestamp(8) subj_len(2) subj hdr_len(4) hdr msg hash(8)

	var le = binary.LittleEndian

	l := uint32(rl)
	hasHeaders := len(mhdr) > 0
	if hasHeaders {
		l |= hbit
	}

	// Reserve space for the header on the underlying buffer.
	mb.cache.buf = append(mb.cache.buf, make([]byte, msgHdrSize)...)
	hdr := mb.cache.buf[len(mb.cache.buf)-msgHdrSize : len(mb.cache.buf)]
	le.PutUint32(hdr[0:], l)
	le.PutUint64(hdr[4:], seq)
	le.PutUint64(hdr[12:], uint64(ts))
	le.PutUint16(hdr[20:], uint16(len(subj)))

	// Now write to underlying buffer.
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
	mb.hh.Write(stringToBytes(subj))
	if hasHeaders {
		mb.hh.Write(mhdr)
	}
	mb.hh.Write(msg)
	checksum := mb.hh.Sum(mb.lchk[:0:highwayhash.Size64])
	copy(mb.lchk[0:], checksum)

	// Update write through cache.
	// Write to msg record.
	mb.cache.buf = append(mb.cache.buf, checksum...)
	mb.cache.lrl = uint32(rl)

	// Set cache timestamp for last store.
	mb.lwts = ts

	// Only update index and do accounting if not a delete tombstone.
	if seq&tbit == 0 {
		// Accounting, do this before stripping ebit, it is ebit aware.
		mb.updateAccounting(seq, ts, rl)
		// Strip ebit if set.
		seq = seq &^ ebit
		if mb.cache.fseq == 0 {
			mb.cache.fseq = seq
		}
		// Write index
		mb.cache.idx = append(mb.cache.idx, uint32(index)|cbit)
	} else {
		// Make sure to account for tombstones in rbytes.
		mb.rbytes += rl
	}

	fch, werr := mb.fch, mb.werr

	// If we should be flushing, or had a write error, do so here.
	if flush || werr != nil {
		ld, err := mb.flushPendingMsgsLocked()
		if ld != nil && mb.fs != nil {
			// We have the mb lock here, this needs the mb locks so do in its own go routine.
			go mb.fs.rebuildState(ld)
		}
		if err != nil {
			return err
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
	mb.mu.RLock()
	defer mb.mu.RUnlock()
	return mb.pendingWriteSizeLocked()
}

// How many bytes pending to be written for this message block.
func (mb *msgBlock) pendingWriteSizeLocked() int {
	if mb == nil {
		return 0
	}
	var pending int
	if !mb.closed && mb.mfd != nil && mb.cache != nil {
		pending = len(mb.cache.buf) - int(mb.cache.wp)
	}
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
	mb.closeFDsLockedNoCheck()
	return nil
}

func (mb *msgBlock) closeFDsLockedNoCheck() {
	if mb.mfd != nil {
		mb.mfd.Close()
		mb.mfd = nil
	}
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
	isDeleted := seq&ebit != 0
	if isDeleted {
		seq = seq &^ ebit
	}

	fseq := atomic.LoadUint64(&mb.first.seq)
	if (fseq == 0 || mb.first.ts == 0) && seq >= fseq {
		atomic.StoreUint64(&mb.first.seq, seq)
		mb.first.ts = ts
	}
	// Need atomics here for selectMsgBlock speed.
	atomic.StoreUint64(&mb.last.seq, seq)
	mb.last.ts = ts
	mb.rbytes += rl
	if !isDeleted {
		mb.bytes += rl
		mb.msgs++
	}
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

	// Mark as dirty for stream state.
	fs.dirty++

	if mb == nil || mb.msgs > 0 && mb.blkSize()+rl > fs.fcfg.BlockSize {
		if mb != nil && fs.fcfg.Compression != NoCompression {
			// We've now reached the end of this message block, if we want
			// to compress blocks then now's the time to do it.
			go mb.recompressOnDiskIfNeeded()
		}
		if mb, err = fs.newMsgBlockForWrite(); err != nil {
			return 0, err
		}
	}

	// Ask msg block to store in write through cache.
	err = mb.writeMsgRecord(rl, seq, subj, hdr, msg, ts, fs.fip)

	return rl, err
}

// For writing tombstones to our lmb. This version will enforce maximum block sizes.
// Lock should be held.
func (fs *fileStore) writeTombstone(seq uint64, ts int64) error {
	// Grab our current last message block.
	lmb := fs.lmb
	var err error

	if lmb == nil || lmb.blkSize()+emptyRecordLen > fs.fcfg.BlockSize {
		if lmb != nil && fs.fcfg.Compression != NoCompression {
			// We've now reached the end of this message block, if we want
			// to compress blocks then now's the time to do it.
			go lmb.recompressOnDiskIfNeeded()
		}
		if lmb, err = fs.newMsgBlockForWrite(); err != nil {
			return err
		}
	}
	return lmb.writeTombstone(seq, ts)
}

func (mb *msgBlock) recompressOnDiskIfNeeded() error {
	alg := mb.fs.fcfg.Compression
	mb.mu.Lock()
	defer mb.mu.Unlock()

	origFN := mb.mfn                    // The original message block on disk.
	tmpFN := mb.mfn + compressTmpSuffix // The compressed block will be written here.

	// Open up the file block and read in the entire contents into memory.
	// One of two things will happen:
	// 1. The block will be compressed already and have a valid metadata
	//    header, in which case we do nothing.
	// 2. The block will be uncompressed, in which case we will compress it
	//    and then write it back out to disk, re-encrypting if necessary.
	<-dios
	origBuf, err := os.ReadFile(origFN)
	dios <- struct{}{}

	if err != nil {
		return fmt.Errorf("failed to read original block from disk: %w", err)
	}

	// If the block is encrypted then we will need to decrypt it before
	// doing anything. We always encrypt after compressing because then the
	// compression can be as efficient as possible on the raw data, whereas
	// the encrypted ciphertext will not compress anywhere near as well.
	// The block encryption also covers the optional compression metadata.
	if mb.bek != nil && len(origBuf) > 0 {
		bek, err := genBlockEncryptionKey(mb.fs.fcfg.Cipher, mb.seed, mb.nonce)
		if err != nil {
			return err
		}
		mb.bek = bek
		mb.bek.XORKeyStream(origBuf, origBuf)
	}

	meta := &CompressionInfo{}
	if _, err := meta.UnmarshalMetadata(origBuf); err != nil {
		// An error is only returned here if there's a problem with parsing
		// the metadata. If the file has no metadata at all, no error is
		// returned and the algorithm defaults to no compression.
		return fmt.Errorf("failed to read existing metadata header: %w", err)
	}
	if meta.Algorithm == alg {
		// The block is already compressed with the chosen algorithm so there
		// is nothing else to do. This is not a common case, it is here only
		// to ensure we don't do unnecessary work in case something asked us
		// to recompress an already compressed block with the same algorithm.
		return nil
	} else if alg != NoCompression {
		// The block is already compressed using some algorithm, so we need
		// to decompress the block using the existing algorithm before we can
		// recompress it with the new one.
		if origBuf, err = meta.Algorithm.Decompress(origBuf); err != nil {
			return fmt.Errorf("failed to decompress original block: %w", err)
		}
	}

	// Rather than modifying the existing block on disk (which is a dangerous
	// operation if something goes wrong), create a new temporary file. We will
	// write out the new block here and then swap the files around afterwards
	// once everything else has succeeded correctly.
	<-dios
	tmpFD, err := os.OpenFile(tmpFN, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, defaultFilePerms)
	dios <- struct{}{}

	if err != nil {
		return fmt.Errorf("failed to create temporary file: %w", err)
	}

	errorCleanup := func(err error) error {
		tmpFD.Close()
		os.Remove(tmpFN)
		return err
	}

	// The original buffer at this point is uncompressed, so we will now compress
	// it if needed. Note that if the selected algorithm is NoCompression, the
	// Compress function will just return the input buffer unmodified.
	cmpBuf, err := alg.Compress(origBuf)
	if err != nil {
		return errorCleanup(fmt.Errorf("failed to compress block: %w", err))
	}

	// We only need to write out the metadata header if compression is enabled.
	// If we're trying to uncompress the file on disk at this point, don't bother
	// writing metadata.
	if alg != NoCompression {
		meta := &CompressionInfo{
			Algorithm:    alg,
			OriginalSize: uint64(len(origBuf)),
		}
		cmpBuf = append(meta.MarshalMetadata(), cmpBuf...)
	}

	// Re-encrypt the block if necessary.
	if mb.bek != nil && len(cmpBuf) > 0 {
		bek, err := genBlockEncryptionKey(mb.fs.fcfg.Cipher, mb.seed, mb.nonce)
		if err != nil {
			return errorCleanup(err)
		}
		mb.bek = bek
		mb.bek.XORKeyStream(cmpBuf, cmpBuf)
	}

	// Write the new block data (which might be compressed or encrypted) to the
	// temporary file.
	if n, err := tmpFD.Write(cmpBuf); err != nil {
		return errorCleanup(fmt.Errorf("failed to write to temporary file: %w", err))
	} else if n != len(cmpBuf) {
		return errorCleanup(fmt.Errorf("short write to temporary file (%d != %d)", n, len(cmpBuf)))
	}
	if err := tmpFD.Sync(); err != nil {
		return errorCleanup(fmt.Errorf("failed to sync temporary file: %w", err))
	}
	if err := tmpFD.Close(); err != nil {
		return errorCleanup(fmt.Errorf("failed to close temporary file: %w", err))
	}

	// Now replace the original file with the newly updated temp file.
	if err := os.Rename(tmpFN, origFN); err != nil {
		return fmt.Errorf("failed to move temporary file into place: %w", err)
	}

	// Since the message block might be retained in memory, make sure the
	// compression algorithm is up-to-date, since this will be needed when
	// compacting or truncating.
	mb.cmp = alg

	// Also update rbytes
	mb.rbytes = uint64(len(cmpBuf))

	return nil
}

func (mb *msgBlock) decompressIfNeeded(buf []byte) ([]byte, error) {
	var meta CompressionInfo
	if n, err := meta.UnmarshalMetadata(buf); err != nil {
		// There was a problem parsing the metadata header of the block.
		// If there's no metadata header, an error isn't returned here,
		// we will instead just use default values of no compression.
		return nil, err
	} else if n == 0 {
		// There were no metadata bytes, so we assume the block is not
		// compressed and return it as-is.
		return buf, nil
	} else {
		// Metadata was present so it's quite likely the block contents
		// are compressed. If by any chance the metadata claims that the
		// block is uncompressed, then the input slice is just returned
		// unmodified.
		return meta.Algorithm.Decompress(buf[n:])
	}
}

// Lock should be held.
func (mb *msgBlock) ensureRawBytesLoaded() error {
	if mb.rbytes > 0 {
		return nil
	}
	f, err := mb.openBlock()
	if err != nil {
		return err
	}
	defer f.Close()
	if fi, err := f.Stat(); fi != nil && err == nil {
		mb.rbytes = uint64(fi.Size())
	} else {
		return err
	}
	return nil
}

// Sync msg and index files as needed. This is called from a timer.
func (fs *fileStore) syncBlocks() {
	fs.mu.Lock()
	// If closed or a snapshot is in progress bail.
	if fs.closed || fs.sips > 0 {
		fs.mu.Unlock()
		return
	}
	blks := append([]*msgBlock(nil), fs.blks...)
	lmb, firstMoved, firstSeq := fs.lmb, fs.firstMoved, fs.state.FirstSeq
	// Clear first moved.
	fs.firstMoved = false
	fs.mu.Unlock()

	var markDirty bool
	for _, mb := range blks {
		// Do actual sync. Hold lock for consistency.
		mb.mu.Lock()
		if mb.closed {
			mb.mu.Unlock()
			continue
		}
		// See if we can close FDs due to being idle.
		if mb.mfd != nil && mb.sinceLastWriteActivity() > closeFDsIdle {
			mb.dirtyCloseWithRemove(false)
		}

		// If our first has moved and we are set to noCompact (which is from tombstones),
		// clear so that we might cleanup tombstones.
		if firstMoved && mb.noCompact {
			mb.noCompact = false
		}
		// Check if we should compact here as well.
		// Do not compact last mb.
		var needsCompact bool
		if mb != lmb && mb.ensureRawBytesLoaded() == nil && mb.shouldCompactSync() {
			needsCompact = true
			markDirty = true
		}

		// Check if we need to sync. We will not hold lock during actual sync.
		needSync := mb.needSync
		if needSync {
			// Flush anything that may be pending.
			mb.flushPendingMsgsLocked()
		}
		mb.mu.Unlock()

		// Check if we should compact here.
		// Need to hold fs lock in case we reference psim when loading in the mb and we may remove this block if truly empty.
		if needsCompact {
			fs.mu.RLock()
			mb.mu.Lock()
			mb.compactWithFloor(firstSeq)
			// If this compact removed all raw bytes due to tombstone cleanup, schedule to remove.
			shouldRemove := mb.rbytes == 0
			mb.mu.Unlock()
			fs.mu.RUnlock()

			// Check if we should remove. This will not be common, so we will re-take fs write lock here vs changing
			//  it above which we would prefer to be a readlock such that other lookups can occur while compacting this block.
			if shouldRemove {
				fs.mu.Lock()
				mb.mu.Lock()
				fs.removeMsgBlock(mb)
				mb.mu.Unlock()
				fs.mu.Unlock()
				needSync = false
			}
		}

		// Check if we need to sync this block.
		if needSync {
			mb.mu.Lock()
			var fd *os.File
			var didOpen bool
			if mb.mfd != nil {
				fd = mb.mfd
			} else {
				<-dios
				fd, _ = os.OpenFile(mb.mfn, os.O_RDWR, defaultFilePerms)
				dios <- struct{}{}
				didOpen = true
			}
			// If we have an fd.
			if fd != nil {
				canClear := fd.Sync() == nil
				// If we opened the file close the fd.
				if didOpen {
					fd.Close()
				}
				// Only clear sync flag on success.
				if canClear {
					mb.needSync = false
				}
			}
			mb.mu.Unlock()
		}
	}

	fs.mu.Lock()
	if fs.closed {
		fs.mu.Unlock()
		return
	}
	fs.setSyncTimer()
	if markDirty {
		fs.dirty++
	}

	// Sync state file if we are not running with sync always.
	if !fs.fcfg.SyncAlways {
		fn := filepath.Join(fs.fcfg.StoreDir, msgDir, streamStreamStateFile)
		<-dios
		fd, _ := os.OpenFile(fn, os.O_RDWR, defaultFilePerms)
		dios <- struct{}{}
		if fd != nil {
			fd.Sync()
			fd.Close()
		}
	}
	fs.mu.Unlock()
}

// Select the message block where this message should be found.
// Return nil if not in the set.
// Read lock should be held.
func (fs *fileStore) selectMsgBlock(seq uint64) *msgBlock {
	_, mb := fs.selectMsgBlockWithIndex(seq)
	return mb
}

// Lock should be held.
func (fs *fileStore) selectMsgBlockWithIndex(seq uint64) (int, *msgBlock) {
	// Check for out of range.
	if seq < fs.state.FirstSeq || seq > fs.state.LastSeq || fs.state.Msgs == 0 {
		return -1, nil
	}

	const linearThresh = 32
	nb := len(fs.blks) - 1

	if nb < linearThresh {
		for i, mb := range fs.blks {
			if seq <= atomic.LoadUint64(&mb.last.seq) {
				return i, mb
			}
		}
		return -1, nil
	}

	// Do traditional binary search here since we know the blocks are sorted by sequence first and last.
	for low, high, mid := 0, nb, nb/2; low <= high; mid = (low + high) / 2 {
		mb := fs.blks[mid]
		// Right now these atomic loads do not factor in, so fine to leave. Was considering
		// uplifting these to fs scope to avoid atomic load but not needed.
		first, last := atomic.LoadUint64(&mb.first.seq), atomic.LoadUint64(&mb.last.seq)
		if seq > last {
			low = mid + 1
		} else if seq < first {
			// A message block's first sequence can change here meaning we could find a gap.
			// We want to behave like above, which if inclusive (we check at start) should
			// always return an index and a valid mb.
			// If we have a gap then our seq would be > fs.blks[mid-1].last.seq
			if mid == 0 || seq > atomic.LoadUint64(&fs.blks[mid-1].last.seq) {
				return mid, mb
			}
			high = mid - 1
		} else {
			return mid, mb
		}
	}

	return -1, nil
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

	mbFirstSeq := atomic.LoadUint64(&mb.first.seq)
	mbLastSeq := atomic.LoadUint64(&mb.last.seq)

	// Sanity check here since we calculate size to allocate based on this.
	if mbFirstSeq > (mbLastSeq + 1) { // Purged state first == last + 1
		mb.fs.warn("indexCacheBuf corrupt state: mb.first %d mb.last %d", mbFirstSeq, mbLastSeq)
		// This would cause idxSz to wrap.
		return errCorruptState
	}

	// Capture beginning size of dmap.
	dms := uint64(mb.dmap.Size())
	idxSz := mbLastSeq - mbFirstSeq + 1

	if mb.cache == nil {
		// Approximation, may adjust below.
		fseq = mbFirstSeq
		idx = make([]uint32, 0, idxSz)
		mb.cache = &cache{}
	} else {
		fseq = mb.cache.fseq
		idx = mb.cache.idx
		if len(idx) == 0 {
			idx = make([]uint32, 0, idxSz)
		}
		index = uint32(len(mb.cache.buf))
		buf = append(mb.cache.buf, buf...)
	}

	// Create FSS if we should track.
	var popFss bool
	if mb.fssNotLoaded() {
		mb.fss = stree.NewSubjectTree[SimpleState]()
		popFss = true
	}
	// Mark fss activity.
	mb.lsts = time.Now().UnixNano()
	mb.ttls = 0

	lbuf := uint32(len(buf))
	var seq, ttls uint64
	var sm StoreMsg // Used for finding TTL headers
	for index < lbuf {
		if index+msgHdrSize > lbuf {
			return errCorruptState
		}
		hdr := buf[index : index+msgHdrSize]
		rl, slen := le.Uint32(hdr[0:]), int(le.Uint16(hdr[20:]))
		seq = le.Uint64(hdr[4:])

		// Clear any headers bit that could be set.
		hasHeaders := rl&hbit != 0
		rl &^= hbit
		dlen := int(rl) - msgHdrSize

		// Do some quick sanity checks here.
		if dlen < 0 || slen > (dlen-recordHashSize) || dlen > int(rl) || index+rl > lbuf || rl > rlBadThresh {
			mb.fs.warn("indexCacheBuf corrupt record state: dlen %d slen %d index %d rl %d lbuf %d", dlen, slen, index, rl, lbuf)
			// This means something is off.
			// TODO(dlc) - Add into bad list?
			return errCorruptState
		}

		// Check for tombstones which we can skip in terms of indexing.
		if seq&tbit != 0 {
			index += rl
			continue
		}

		// Clear any erase bits.
		erased := seq&ebit != 0
		seq = seq &^ ebit

		// We defer checksum checks to individual msg cache lookups to amortorize costs and
		// not introduce latency for first message from a newly loaded block.
		if seq >= mbFirstSeq {
			// Track that we do not have holes.
			if slot := int(seq - mbFirstSeq); slot != len(idx) {
				// If we have a hole fill it.
				for dseq := mbFirstSeq + uint64(len(idx)); dseq < seq; dseq++ {
					idx = append(idx, dbit)
					if dms == 0 {
						mb.dmap.Insert(dseq)
					}
				}
			}
			// Add to our index.
			idx = append(idx, index)
			mb.cache.lrl = uint32(rl)
			// Adjust if we guessed wrong.
			if seq != 0 && seq < fseq {
				fseq = seq
			}

			// Make sure our dmap has this entry if it was erased.
			if erased && dms == 0 {
				mb.dmap.Insert(seq)
			}

			// Handle FSS inline here.
			if popFss && slen > 0 && !mb.noTrack && !erased && !mb.dmap.Exists(seq) {
				bsubj := buf[index+msgHdrSize : index+msgHdrSize+uint32(slen)]
				if ss, ok := mb.fss.Find(bsubj); ok && ss != nil {
					ss.Msgs++
					ss.Last = seq
					ss.lastNeedsUpdate = false
				} else {
					mb.fss.Insert(bsubj, SimpleState{
						Msgs:  1,
						First: seq,
						Last:  seq,
					})
				}
			}

			// Count how many TTLs we think are in this message block.
			// TODO(nat): Not terribly optimal...
			if hasHeaders {
				if fsm, err := mb.msgFromBuf(buf, &sm, nil); err == nil && fsm != nil {
					if _, err = getMessageTTL(fsm.hdr); err == nil && len(fsm.hdr) > 0 {
						ttls++
					}
				}
			}
		}
		index += rl
	}

	// Track holes at the end of the block, these would be missed in the
	// earlier loop if we've ran out of block file to look at, but should
	// be easily noticed because the seq will be below the last seq from
	// the index.
	if seq > 0 && seq < mbLastSeq {
		for dseq := seq; dseq < mbLastSeq; dseq++ {
			idx = append(idx, dbit)
			if dms == 0 {
				mb.dmap.Insert(dseq)
			}
		}
	}

	mb.cache.buf = buf
	mb.cache.idx = idx
	mb.cache.fseq = fseq
	mb.cache.wp += int(lbuf)
	mb.ttls = ttls

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

// Write function for actual data.
// mb.mfd should not be nil.
// Lock should held.
func (mb *msgBlock) writeAt(buf []byte, woff int64) (int, error) {
	// Used to mock write failures.
	if mb.mockWriteErr {
		// Reset on trip.
		mb.mockWriteErr = false
		return 0, errors.New("mock write error")
	}
	<-dios
	n, err := mb.mfd.WriteAt(buf, woff)
	dios <- struct{}{}
	return n, err
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
		// Need to leave original alone.
		var dst []byte
		if lob <= defaultLargeBlockSize {
			dst = getMsgBlockBuf(lob)[:lob]
		} else {
			dst = make([]byte, lob)
		}
		mb.bek.XORKeyStream(dst, buf)
		buf = dst
	}

	// Append new data to the message block file.
	for lbb := lob; lbb > 0; lbb = len(buf) {
		n, err := mb.writeAt(buf, woff)
		if err != nil {
			mb.dirtyCloseWithRemove(false)
			ld, _, _ := mb.rebuildStateLocked()
			mb.werr = err
			return ld, err
		}
		// Update our write offset.
		woff += int64(n)
		// Partial write.
		if n != lbb {
			buf = buf[n:]
		} else {
			// Done.
			break
		}
	}

	// Clear any error.
	mb.werr = nil

	// Cache may be gone.
	if mb.cache == nil || mb.mfd == nil {
		return fsLostData, mb.werr
	}

	// Check if we are in sync always mode.
	if mb.syncAlways {
		mb.mfd.Sync()
	} else {
		mb.needSync = true
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
			recycleMsgBlockBuf(mb.cache.buf)
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

// Lock should be held.
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
	numEntries := mb.msgs + uint64(mb.dmap.Size()) + (atomic.LoadUint64(&mb.first.seq) - mb.cache.fseq)
	return numEntries == uint64(len(mb.cache.idx))
}

// Lock should be held.
func (mb *msgBlock) cacheNotLoaded() bool {
	return !mb.cacheAlreadyLoaded()
}

// Report if our fss is not loaded.
// Lock should be held.
func (mb *msgBlock) fssNotLoaded() bool {
	return mb.fss == nil && !mb.noTrack
}

// Wrap openBlock for the gated semaphore processing.
// Lock should be held
func (mb *msgBlock) openBlock() (*os.File, error) {
	// Gate with concurrent IO semaphore.
	<-dios
	f, err := os.Open(mb.mfn)
	dios <- struct{}{}
	return f, err
}

// Used to load in the block contents.
// Lock should be held and all conditionals satisfied prior.
func (mb *msgBlock) loadBlock(buf []byte) ([]byte, error) {
	var f *os.File
	// Re-use if we have mfd open.
	if mb.mfd != nil {
		f = mb.mfd
		if n, err := f.Seek(0, 0); n != 0 || err != nil {
			f = nil
			mb.closeFDsLockedNoCheck()
		}
	}
	if f == nil {
		var err error
		f, err = mb.openBlock()
		if err != nil {
			if os.IsNotExist(err) {
				err = errNoBlkData
			}
			return nil, err
		}
		defer f.Close()
	}

	var sz int
	if info, err := f.Stat(); err == nil {
		sz64 := info.Size()
		if int64(int(sz64)) == sz64 {
			sz = int(sz64)
		} else {
			return nil, errMsgBlkTooBig
		}
	}

	if buf == nil {
		buf = getMsgBlockBuf(sz)
		if sz > cap(buf) {
			// We know we will make a new one so just recycle for now.
			recycleMsgBlockBuf(buf)
			buf = nil
		}
	}

	if sz > cap(buf) {
		buf = make([]byte, sz)
	} else {
		buf = buf[:sz]
	}

	<-dios
	n, err := io.ReadFull(f, buf)
	dios <- struct{}{}
	// On success capture raw bytes size.
	if err == nil {
		mb.rbytes = uint64(n)
	}
	return buf[:n], err
}

// Lock should be held.
func (mb *msgBlock) loadMsgsWithLock() error {
	// Check for encryption, we do not load keys on startup anymore so might need to load them here.
	if mb.fs != nil && mb.fs.prf != nil && (mb.aek == nil || mb.bek == nil) {
		if err := mb.fs.loadEncryptionForMsgBlock(mb); err != nil {
			return err
		}
	}

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
		mb.fs.warn("loadBlock error: %v", err)
		if err == errNoBlkData {
			if ld, _, err := mb.rebuildStateLocked(); err != nil && ld != nil {
				// Rebuild fs state too.
				go mb.fs.rebuildState(ld)
			}
		}
		return err
	}

	// Reset the cache since we just read everything in.
	// Make sure this is cleared in case we had a partial when we started.
	mb.clearCacheAndOffset()

	// Check if we need to decrypt.
	if mb.bek != nil && len(buf) > 0 {
		bek, err := genBlockEncryptionKey(mb.fs.fcfg.Cipher, mb.seed, mb.nonce)
		if err != nil {
			return err
		}
		mb.bek = bek
		mb.bek.XORKeyStream(buf, buf)
	}

	// Check for compression.
	if buf, err = mb.decompressIfNeeded(buf); err != nil {
		return err
	}

	if err := mb.indexCacheBuf(buf); err != nil {
		if err == errCorruptState {
			var ld *LostStreamData
			if ld, _, err = mb.rebuildStateLocked(); ld != nil {
				// We do not know if fs is locked or not at this point.
				// This should be an exceptional condition so do so in Go routine.
				go mb.fs.rebuildState(ld)
			}
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
func (mb *msgBlock) fetchMsg(seq uint64, sm *StoreMsg) (*StoreMsg, bool, error) {
	mb.mu.Lock()
	defer mb.mu.Unlock()

	fseq, lseq := atomic.LoadUint64(&mb.first.seq), atomic.LoadUint64(&mb.last.seq)
	if seq < fseq || seq > lseq {
		return nil, false, ErrStoreMsgNotFound
	}

	// See if we can short circuit if we already know msg deleted.
	if mb.dmap.Exists(seq) {
		// Update for scanning like cacheLookup would have.
		llseq := mb.llseq
		if mb.llseq == 0 || seq < mb.llseq || seq == mb.llseq+1 || seq == mb.llseq-1 {
			mb.llseq = seq
		}
		expireOk := (seq == lseq && llseq == seq-1) || (seq == fseq && llseq == seq+1)
		return nil, expireOk, errDeletedMsg
	}

	if mb.cacheNotLoaded() {
		if err := mb.loadMsgsWithLock(); err != nil {
			return nil, false, err
		}
	}
	llseq := mb.llseq

	fsm, err := mb.cacheLookup(seq, sm)
	if err != nil {
		return nil, false, err
	}
	expireOk := (seq == lseq && llseq == seq-1) || (seq == fseq && llseq == seq+1)
	return fsm, expireOk, err
}

var (
	errNoCache       = errors.New("no message cache")
	errBadMsg        = errors.New("malformed or corrupt message")
	errDeletedMsg    = errors.New("deleted message")
	errPartialCache  = errors.New("partial cache")
	errNoPending     = errors.New("message block does not have pending data")
	errNotReadable   = errors.New("storage directory not readable")
	errCorruptState  = errors.New("corrupt state file")
	errPriorState    = errors.New("prior state file")
	errPendingData   = errors.New("pending data still present")
	errNoEncryption  = errors.New("encryption not enabled")
	errBadKeySize    = errors.New("encryption bad key size")
	errNoMsgBlk      = errors.New("no message block")
	errMsgBlkTooBig  = errors.New("message block size exceeded int capacity")
	errUnknownCipher = errors.New("unknown cipher")
	errNoMainKey     = errors.New("encrypted store encountered with no main key")
	errNoBlkData     = errors.New("message block data missing")
	errStateTooBig   = errors.New("store state too big for optional write")
)

const (
	// "Checksum bit" is used in "mb.cache.idx" for marking messages that have had their checksums checked.
	cbit = 1 << 31
	// "Delete bit" is used in "mb.cache.idx" to mark an index as deleted and non-existent.
	dbit = 1 << 30
	// "Header bit" is used in "rl" to signal a message record with headers.
	hbit = 1 << 31
	// "Erase bit" is used in "seq" for marking erased messages sequences.
	ebit = 1 << 63
	// "Tombstone bit" is used in "seq" for marking tombstone sequences.
	tbit = 1 << 62
)

// Will do a lookup from cache.
// Lock should be held.
func (mb *msgBlock) cacheLookup(seq uint64, sm *StoreMsg) (*StoreMsg, error) {
	if seq < atomic.LoadUint64(&mb.first.seq) || seq > atomic.LoadUint64(&mb.last.seq) {
		return nil, ErrStoreMsgNotFound
	}

	// The llseq signals us when we can expire a cache at the end of a linear scan.
	// We want to only update when we know the last reads (multiple consumers) are sequential.
	// We want to account for forwards and backwards linear scans.
	if mb.llseq == 0 || seq < mb.llseq || seq == mb.llseq+1 || seq == mb.llseq-1 {
		mb.llseq = seq
	}

	// If we have a delete map check it.
	if mb.dmap.Exists(seq) {
		mb.llts = time.Now().UnixNano()
		return nil, errDeletedMsg
	}

	// Detect no cache loaded.
	if mb.cache == nil || mb.cache.fseq == 0 || len(mb.cache.idx) == 0 || len(mb.cache.buf) == 0 {
		var reason string
		if mb.cache == nil {
			reason = "no cache"
		} else if mb.cache.fseq == 0 {
			reason = "fseq is 0"
		} else if len(mb.cache.idx) == 0 {
			reason = "no idx present"
		} else {
			reason = "cache buf empty"
		}
		mb.fs.warn("Cache lookup detected no cache: %s", reason)
		return nil, errNoCache
	}
	// Check partial cache status.
	if seq < mb.cache.fseq {
		mb.fs.warn("Cache lookup detected partial cache: seq %d vs cache fseq %d", seq, mb.cache.fseq)
		return nil, errPartialCache
	}

	bi, _, hashChecked, err := mb.slotInfo(int(seq - mb.cache.fseq))
	if err != nil {
		return nil, err
	}

	// Update cache activity.
	mb.llts = time.Now().UnixNano()

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
	fsm, err := mb.msgFromBuf(buf, sm, hh)
	if err != nil || fsm == nil {
		return nil, err
	}

	// Deleted messages that are decoded return a 0 for sequence.
	if fsm.seq == 0 {
		return nil, errDeletedMsg
	}

	if seq != fsm.seq {
		recycleMsgBlockBuf(mb.cache.buf)
		mb.cache.buf = nil
		return nil, fmt.Errorf("sequence numbers for cache load did not match, %d vs %d", seq, fsm.seq)
	}

	// Clear the check bit here after we know all is good.
	if !hashChecked {
		mb.cache.idx[seq-mb.cache.fseq] = (bi | cbit)
	}

	return fsm, nil
}

// Used when we are checking if discarding a message due to max msgs per subject will give us
// enough room for a max bytes condition.
// Lock should be already held.
func (fs *fileStore) sizeForSeq(seq uint64) int {
	if seq == 0 {
		return 0
	}
	var smv StoreMsg
	if mb := fs.selectMsgBlock(seq); mb != nil {
		if sm, _, _ := mb.fetchMsg(seq, &smv); sm != nil {
			return int(fileStoreMsgSize(sm.subj, sm.hdr, sm.msg))
		}
	}
	return 0
}

// Will return message for the given sequence number.
func (fs *fileStore) msgForSeq(seq uint64, sm *StoreMsg) (*StoreMsg, error) {
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
	mb, lseq := fs.selectMsgBlock(seq), fs.state.LastSeq
	fs.mu.RUnlock()

	if mb == nil {
		var err = ErrStoreEOF
		if seq <= lseq {
			err = ErrStoreMsgNotFound
		}
		return nil, err
	}

	fsm, expireOk, err := mb.fetchMsg(seq, sm)
	if err != nil {
		return nil, err
	}

	// We detected a linear scan and access to the last message.
	// If we are not the last message block we can try to expire the cache.
	if expireOk {
		mb.tryForceExpireCache()
	}

	return fsm, nil
}

// Internal function to return msg parts from a raw buffer.
// Lock should be held.
func (mb *msgBlock) msgFromBuf(buf []byte, sm *StoreMsg, hh hash.Hash64) (*StoreMsg, error) {
	if len(buf) < emptyRecordLen {
		return nil, errBadMsg
	}
	var le = binary.LittleEndian

	hdr := buf[:msgHdrSize]
	rl := le.Uint32(hdr[0:])
	hasHeaders := rl&hbit != 0
	rl &^= hbit // clear header bit
	dlen := int(rl) - msgHdrSize
	slen := int(le.Uint16(hdr[20:]))
	// Simple sanity check.
	if dlen < 0 || slen > (dlen-recordHashSize) || dlen > int(rl) || int(rl) > len(buf) || rl > rlBadThresh {
		return nil, errBadMsg
	}
	data := buf[msgHdrSize : msgHdrSize+dlen]
	// Do checksum tests here if requested.
	if hh != nil {
		hh.Reset()
		hh.Write(hdr[4:20])
		hh.Write(data[:slen])
		if hasHeaders {
			hh.Write(data[slen+4 : dlen-recordHashSize])
		} else {
			hh.Write(data[slen : dlen-recordHashSize])
		}
		if !bytes.Equal(hh.Sum(nil), data[len(data)-8:]) {
			return nil, errBadMsg
		}
	}
	seq := le.Uint64(hdr[4:])
	if seq&ebit != 0 {
		seq = 0
	}
	ts := int64(le.Uint64(hdr[12:]))

	// Create a StoreMsg if needed.
	if sm == nil {
		sm = new(StoreMsg)
	} else {
		sm.clear()
	}
	// To recycle the large blocks we can never pass back a reference, so need to copy for the upper
	// layers and for us to be safe to expire, and recycle, the large msgBlocks.
	end := dlen - 8

	if hasHeaders {
		hl := le.Uint32(data[slen:])
		bi := slen + 4
		li := bi + int(hl)
		sm.buf = append(sm.buf, data[bi:end]...)
		li, end = li-bi, end-bi
		sm.hdr = sm.buf[0:li:li]
		sm.msg = sm.buf[li:end]
	} else {
		sm.buf = append(sm.buf, data[slen:end]...)
		sm.msg = sm.buf[0 : end-slen]
	}
	sm.seq, sm.ts = seq, ts
	if slen > 0 {
		// Make a copy since sm.subj lifetime may last longer.
		sm.subj = string(data[:slen])
	}

	return sm, nil
}

// LoadMsg will lookup the message by sequence number and return it if found.
func (fs *fileStore) LoadMsg(seq uint64, sm *StoreMsg) (*StoreMsg, error) {
	return fs.msgForSeq(seq, sm)
}

// loadLast will load the last message for a subject. Subject should be non empty and not ">".
func (fs *fileStore) loadLast(subj string, sm *StoreMsg) (lsm *StoreMsg, err error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	if fs.closed || fs.lmb == nil {
		return nil, ErrStoreClosed
	}

	if len(fs.blks) == 0 {
		return nil, ErrStoreMsgNotFound
	}

	wc := subjectHasWildcard(subj)
	var start, stop uint32

	// If literal subject check for presence.
	if wc {
		start = fs.lmb.index
		fs.psim.Match(stringToBytes(subj), func(_ []byte, psi *psi) {
			// Keep track of start and stop indexes for this subject.
			if psi.fblk < start {
				start = psi.fblk
			}
			if psi.lblk > stop {
				stop = psi.lblk
			}
		})
		// None matched.
		if stop == 0 {
			return nil, ErrStoreMsgNotFound
		}
		// These need to be swapped.
		start, stop = stop, start
	} else if info, ok := fs.psim.Find(stringToBytes(subj)); ok {
		start, stop = info.lblk, info.fblk
	} else {
		return nil, ErrStoreMsgNotFound
	}

	// Walk blocks backwards.
	for i := start; i >= stop; i-- {
		mb := fs.bim[i]
		if mb == nil {
			continue
		}
		mb.mu.Lock()
		if err := mb.ensurePerSubjectInfoLoaded(); err != nil {
			mb.mu.Unlock()
			return nil, err
		}
		// Mark fss activity.
		mb.lsts = time.Now().UnixNano()

		var l uint64
		// Optimize if subject is not a wildcard.
		if !wc {
			if ss, ok := mb.fss.Find(stringToBytes(subj)); ok && ss != nil {
				l = ss.Last
			}
		}
		if l == 0 {
			_, _, l = mb.filteredPendingLocked(subj, wc, atomic.LoadUint64(&mb.first.seq))
		}
		if l > 0 {
			if mb.cacheNotLoaded() {
				if err := mb.loadMsgsWithLock(); err != nil {
					mb.mu.Unlock()
					return nil, err
				}
			}
			lsm, err = mb.cacheLookup(l, sm)
		}
		mb.mu.Unlock()
		if l > 0 {
			break
		}
	}
	return lsm, err
}

// LoadLastMsg will return the last message we have that matches a given subject.
// The subject can be a wildcard.
func (fs *fileStore) LoadLastMsg(subject string, smv *StoreMsg) (sm *StoreMsg, err error) {
	if subject == _EMPTY_ || subject == fwcs {
		sm, err = fs.msgForSeq(fs.lastSeq(), smv)
	} else {
		sm, err = fs.loadLast(subject, smv)
	}
	if sm == nil || (err != nil && err != ErrStoreClosed) {
		err = ErrStoreMsgNotFound
	}
	return sm, err
}

// LoadNextMsgMulti will find the next message matching any entry in the sublist.
func (fs *fileStore) LoadNextMsgMulti(sl *Sublist, start uint64, smp *StoreMsg) (sm *StoreMsg, skip uint64, err error) {
	if sl == nil {
		return fs.LoadNextMsg(_EMPTY_, false, start, smp)
	}
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	if fs.closed {
		return nil, 0, ErrStoreClosed
	}
	if fs.state.Msgs == 0 || start > fs.state.LastSeq {
		return nil, fs.state.LastSeq, ErrStoreEOF
	}
	if start < fs.state.FirstSeq {
		start = fs.state.FirstSeq
	}

	if bi, _ := fs.selectMsgBlockWithIndex(start); bi >= 0 {
		for i := bi; i < len(fs.blks); i++ {
			mb := fs.blks[i]
			if sm, expireOk, err := mb.firstMatchingMulti(sl, start, smp); err == nil {
				if expireOk {
					mb.tryForceExpireCache()
				}
				return sm, sm.seq, nil
			} else if err != ErrStoreMsgNotFound {
				return nil, 0, err
			} else if expireOk {
				mb.tryForceExpireCache()
			}
		}
	}

	return nil, fs.state.LastSeq, ErrStoreEOF

}

func (fs *fileStore) LoadNextMsg(filter string, wc bool, start uint64, sm *StoreMsg) (*StoreMsg, uint64, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	if fs.closed {
		return nil, 0, ErrStoreClosed
	}
	if fs.state.Msgs == 0 || start > fs.state.LastSeq {
		return nil, fs.state.LastSeq, ErrStoreEOF
	}
	if start < fs.state.FirstSeq {
		start = fs.state.FirstSeq
	}

	// If start is less than or equal to beginning of our stream, meaning our first call,
	// let's check the psim to see if we can skip ahead.
	if start <= fs.state.FirstSeq {
		var ss SimpleState
		fs.numFilteredPendingNoLast(filter, &ss)
		// Nothing available.
		if ss.Msgs == 0 {
			return nil, fs.state.LastSeq, ErrStoreEOF
		}
		// We can skip ahead.
		if ss.First > start {
			start = ss.First
		}
	}

	if bi, _ := fs.selectMsgBlockWithIndex(start); bi >= 0 {
		for i := bi; i < len(fs.blks); i++ {
			mb := fs.blks[i]
			if sm, expireOk, err := mb.firstMatching(filter, wc, start, sm); err == nil {
				if expireOk {
					mb.tryForceExpireCache()
				}
				return sm, sm.seq, nil
			} else if err != ErrStoreMsgNotFound {
				return nil, 0, err
			} else {
				// Nothing found in this block. We missed, if first block (bi) check psim.
				// Similar to above if start <= first seq.
				// TODO(dlc) - For v2 track these by filter subject since they will represent filtered consumers.
				// We should not do this at all if we are already on the last block.
				// Also if we are a wildcard do not check if large subject space.
				const wcMaxSizeToCheck = 64 * 1024
				if i == bi && i < len(fs.blks)-1 && (!wc || fs.psim.Size() < wcMaxSizeToCheck) {
					nbi, err := fs.checkSkipFirstBlock(filter, wc, bi)
					// Nothing available.
					if err == ErrStoreEOF {
						return nil, fs.state.LastSeq, ErrStoreEOF
					}
					// See if we can jump ahead here.
					// Right now we can only spin on first, so if we have interior sparseness need to favor checking per block fss if loaded.
					// For v2 will track all blocks that have matches for psim.
					if nbi > i {
						i = nbi - 1 // For the iterator condition i++
					}
				}
				// Check is we can expire.
				if expireOk {
					mb.tryForceExpireCache()
				}
			}
		}
	}

	return nil, fs.state.LastSeq, ErrStoreEOF
}

// Will load the next non-deleted msg starting at the start sequence and walking backwards.
func (fs *fileStore) LoadPrevMsg(start uint64, smp *StoreMsg) (sm *StoreMsg, err error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	if fs.closed {
		return nil, ErrStoreClosed
	}
	if fs.state.Msgs == 0 || start < fs.state.FirstSeq {
		return nil, ErrStoreEOF
	}

	if start > fs.state.LastSeq {
		start = fs.state.LastSeq
	}
	if smp == nil {
		smp = new(StoreMsg)
	}

	if bi, _ := fs.selectMsgBlockWithIndex(start); bi >= 0 {
		for i := bi; i >= 0; i-- {
			mb := fs.blks[i]
			mb.mu.Lock()
			// Need messages loaded from here on out.
			if mb.cacheNotLoaded() {
				if err := mb.loadMsgsWithLock(); err != nil {
					mb.mu.Unlock()
					return nil, err
				}
			}

			lseq, fseq := atomic.LoadUint64(&mb.last.seq), atomic.LoadUint64(&mb.first.seq)
			if start > lseq {
				start = lseq
			}
			for seq := start; seq >= fseq; seq-- {
				if mb.dmap.Exists(seq) {
					continue
				}
				if sm, err := mb.cacheLookup(seq, smp); err == nil {
					mb.mu.Unlock()
					return sm, nil
				}
			}
			mb.mu.Unlock()
		}
	}

	return nil, ErrStoreEOF
}

// Type returns the type of the underlying store.
func (fs *fileStore) Type() StorageType {
	return FileStorage
}

// Returns number of subjects in this store.
// Lock should be held.
func (fs *fileStore) numSubjects() int {
	return fs.psim.Size()
}

// numConsumers uses new lock.
func (fs *fileStore) numConsumers() int {
	fs.cmu.RLock()
	defer fs.cmu.RUnlock()
	return len(fs.cfs)
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
	// Make sure to reset if being re-used.
	state.Deleted, state.NumDeleted = nil, 0
	if state.LastSeq > state.FirstSeq {
		state.NumDeleted = int((state.LastSeq - state.FirstSeq + 1) - state.Msgs)
		if state.NumDeleted < 0 {
			state.NumDeleted = 0
		}
	}
	state.Consumers = fs.numConsumers()
	state.NumSubjects = fs.numSubjects()
	fs.mu.RUnlock()
}

// State returns the current state of the stream.
func (fs *fileStore) State() StreamState {
	fs.mu.RLock()
	state := fs.state
	state.Consumers = fs.numConsumers()
	state.NumSubjects = fs.numSubjects()
	state.Deleted = nil // make sure.

	if numDeleted := int((state.LastSeq - state.FirstSeq + 1) - state.Msgs); numDeleted > 0 {
		state.Deleted = make([]uint64, 0, numDeleted)
		cur := fs.state.FirstSeq

		for _, mb := range fs.blks {
			mb.mu.Lock()
			fseq := atomic.LoadUint64(&mb.first.seq)
			// Account for messages missing from the head.
			if fseq > cur {
				for seq := cur; seq < fseq; seq++ {
					state.Deleted = append(state.Deleted, seq)
				}
			}
			// Only advance cur if we are increasing. We could have marker blocks with just tombstones.
			if last := atomic.LoadUint64(&mb.last.seq); last >= cur {
				cur = last + 1 // Expected next first.
			}
			// Add in deleted.
			mb.dmap.Range(func(seq uint64) bool {
				state.Deleted = append(state.Deleted, seq)
				return true
			})
			mb.mu.Unlock()
		}
	}
	fs.mu.RUnlock()

	state.Lost = fs.lostData()

	// Can not be guaranteed to be sorted.
	if len(state.Deleted) > 0 {
		slices.Sort(state.Deleted)
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

// Determine time since any last activity, read/load, write or remove.
func (mb *msgBlock) sinceLastActivity() time.Duration {
	if mb.closed {
		return 0
	}
	last := mb.lwts
	if mb.lrts > last {
		last = mb.lrts
	}
	if mb.llts > last {
		last = mb.llts
	}
	if mb.lsts > last {
		last = mb.lsts
	}
	return time.Since(time.Unix(0, last).UTC())
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

func checkNewHeader(hdr []byte) error {
	if len(hdr) < 2 || hdr[0] != magic ||
		(hdr[1] != version && hdr[1] != newVersion) {
		return errCorruptState
	}
	return nil
}

// readIndexInfo will read in the index information for the message block.
func (mb *msgBlock) readIndexInfo() error {
	ifn := filepath.Join(mb.fs.fcfg.StoreDir, msgDir, fmt.Sprintf(indexScan, mb.index))
	buf, err := os.ReadFile(ifn)
	if err != nil {
		return err
	}

	// Set if first time.
	if mb.liwsz == 0 {
		mb.liwsz = int64(len(buf))
	}

	// Decrypt if needed.
	if mb.aek != nil {
		buf, err = mb.aek.Open(buf[:0], mb.nonce, buf, nil)
		if err != nil {
			return err
		}
	}

	if err := checkNewHeader(buf); err != nil {
		defer os.Remove(ifn)
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
	atomic.StoreUint64(&mb.first.seq, readSeq())
	mb.first.ts = readTimeStamp()
	atomic.StoreUint64(&mb.last.seq, readSeq())
	mb.last.ts = readTimeStamp()
	dmapLen := readCount()

	// Check if this is a short write index file.
	if bi < 0 || bi+checksumSize > len(buf) {
		os.Remove(ifn)
		return fmt.Errorf("short index file")
	}

	// Check for consistency if accounting. If something is off bail and we will rebuild.
	if mb.msgs != (atomic.LoadUint64(&mb.last.seq)-atomic.LoadUint64(&mb.first.seq)+1)-dmapLen {
		os.Remove(ifn)
		return fmt.Errorf("accounting inconsistent")
	}

	// Checksum
	copy(mb.lchk[0:], buf[bi:bi+checksumSize])
	bi += checksumSize

	// Now check for presence of a delete map
	if dmapLen > 0 {
		// New version is encoded avl seqset.
		if buf[1] == newVersion {
			dmap, _, err := avl.Decode(buf[bi:])
			if err != nil {
				return fmt.Errorf("could not decode avl dmap: %v", err)
			}
			mb.dmap = *dmap
		} else {
			// This is the old version.
			for i, fseq := 0, atomic.LoadUint64(&mb.first.seq); i < int(dmapLen); i++ {
				seq := readSeq()
				if seq == 0 {
					break
				}
				mb.dmap.Insert(seq + fseq)
			}
		}
	}

	return nil
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
		total += mb.dmap.Size()
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
func (fs *fileStore) PurgeEx(subject string, sequence, keep uint64, _ /* noMarkers */ bool) (purged uint64, err error) {
	// TODO: Don't write markers on purge until we have solved performance
	// issues with them.
	noMarkers := true

	if subject == _EMPTY_ || subject == fwcs {
		if keep == 0 && sequence == 0 {
			return fs.purge(0, noMarkers)
		}
		if sequence > 1 {
			return fs.compact(sequence, noMarkers)
		}
	}

	eq, wc := compareFn(subject), subjectHasWildcard(subject)
	var firstSeqNeedsUpdate bool
	var bytes uint64

	// If we have a "keep" designation need to get full filtered state so we know how many to purge.
	var maxp uint64
	if keep > 0 {
		ss := fs.FilteredState(1, subject)
		if keep >= ss.Msgs {
			return 0, nil
		}
		maxp = ss.Msgs - keep
	}

	var smv StoreMsg
	var tombs []msgId

	fs.mu.Lock()
	// We may remove blocks as we purge, so don't range directly on fs.blks
	// otherwise we may jump over some (see https://github.com/nats-io/nats-server/issues/3528)
	for i := 0; i < len(fs.blks); i++ {
		mb := fs.blks[i]
		mb.mu.Lock()

		// If we do not have our fss, try to expire the cache if we have no items in this block.
		shouldExpire := mb.fssNotLoaded()

		t, f, l := mb.filteredPendingLocked(subject, wc, atomic.LoadUint64(&mb.first.seq))
		if t == 0 {
			// Expire if we were responsible for loading.
			if shouldExpire {
				// Expire this cache before moving on.
				mb.tryForceExpireCacheLocked()
			}
			mb.mu.Unlock()
			continue
		}

		if sequence > 1 && sequence <= l {
			l = sequence - 1
		}

		if mb.cacheNotLoaded() {
			mb.loadMsgsWithLock()
			shouldExpire = true
		}

		for seq := f; seq <= l; seq++ {
			if sm, _ := mb.cacheLookup(seq, &smv); sm != nil && eq(sm.subj, subject) {
				rl := fileStoreMsgSize(sm.subj, sm.hdr, sm.msg)
				// Do fast in place remove.
				// Stats
				if mb.msgs > 0 {
					// Msgs
					fs.state.Msgs--
					mb.msgs--
					// Bytes, make sure to not go negative.
					if rl > fs.state.Bytes {
						rl = fs.state.Bytes
					}
					if rl > mb.bytes {
						rl = mb.bytes
					}
					fs.state.Bytes -= rl
					mb.bytes -= rl
					// Totals
					purged++
					bytes += rl
				}
				// PSIM and FSS updates.
				mb.removeSeqPerSubject(sm.subj, seq)
				fs.removePerSubject(sm.subj, !noMarkers && fs.cfg.SubjectDeleteMarkerTTL > 0)
				// Track tombstones we need to write.
				tombs = append(tombs, msgId{sm.seq, sm.ts})

				// Check for first message.
				if seq == atomic.LoadUint64(&mb.first.seq) {
					mb.selectNextFirst()
					if mb.isEmpty() {
						fs.removeMsgBlock(mb)
						i--
						// keep flag set, if set previously
						firstSeqNeedsUpdate = firstSeqNeedsUpdate || seq == fs.state.FirstSeq
					} else if seq == fs.state.FirstSeq {
						fs.state.FirstSeq = atomic.LoadUint64(&mb.first.seq) // new one.
						fs.state.FirstTime = time.Unix(0, mb.first.ts).UTC()
					}
				} else {
					// Out of order delete.
					mb.dmap.Insert(seq)
				}

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

		// Check if we should break out of top level too.
		if maxp > 0 && purged >= maxp {
			break
		}
	}
	if firstSeqNeedsUpdate {
		fs.selectNextFirst()
	}
	fseq := fs.state.FirstSeq

	// Write any tombstones as needed.
	for _, tomb := range tombs {
		if tomb.seq > fseq {
			fs.writeTombstone(tomb.seq, tomb.ts)
		}
	}

	os.Remove(filepath.Join(fs.fcfg.StoreDir, msgDir, streamStreamStateFile))
	fs.dirty++
	cb := fs.scb
	sdmcb := fs.subjectDeleteMarkersAfterOperation(JSMarkerReasonPurge)
	fs.mu.Unlock()

	if cb != nil {
		cb(-int64(purged), -int64(bytes), 0, _EMPTY_)
	}
	if sdmcb != nil {
		sdmcb()
	}

	return purged, nil
}

// Purge will remove all messages from this store.
// Will return the number of purged messages.
func (fs *fileStore) Purge() (uint64, error) {
	return fs.purge(0, false)
}

func (fs *fileStore) purge(fseq uint64, _ /* noMarkers */ bool) (uint64, error) {
	// TODO: Don't write markers on purge until we have solved performance
	// issues with them.
	noMarkers := true

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
	fs.bim = make(map[uint32]*msgBlock)
	// Subject delete markers if needed.
	if !noMarkers && fs.cfg.SubjectDeleteMarkerTTL > 0 {
		fs.psim.IterOrdered(func(subject []byte, _ *psi) bool {
			fs.markers = append(fs.markers, string(subject))
			return true
		})
	}
	// Clear any per subject tracking.
	fs.psim, fs.tsl = fs.psim.Empty(), 0
	// Mark dirty.
	fs.dirty++

	// Move the msgs directory out of the way, will delete out of band.
	// FIXME(dlc) - These can error and we need to change api above to propagate?
	mdir := filepath.Join(fs.fcfg.StoreDir, msgDir)
	pdir := filepath.Join(fs.fcfg.StoreDir, purgeDir)
	// If purge directory still exists then we need to wait
	// in place and remove since rename would fail.
	if _, err := os.Stat(pdir); err == nil {
		<-dios
		os.RemoveAll(pdir)
		dios <- struct{}{}
	}

	<-dios
	os.Rename(mdir, pdir)
	dios <- struct{}{}

	go func() {
		<-dios
		os.RemoveAll(pdir)
		dios <- struct{}{}
	}()

	// Create new one.
	<-dios
	os.MkdirAll(mdir, defaultDirPerms)
	dios <- struct{}{}

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

	lmb := fs.lmb
	atomic.StoreUint64(&lmb.first.seq, fs.state.FirstSeq)
	atomic.StoreUint64(&lmb.last.seq, fs.state.LastSeq)
	lmb.last.ts = fs.state.LastTime.UnixNano()

	if lseq := atomic.LoadUint64(&lmb.last.seq); lseq > 1 {
		// Leave a tombstone so we can remember our starting sequence in case
		// full state becomes corrupted.
		fs.writeTombstone(lseq, lmb.last.ts)
	}

	cb := fs.scb
	sdmcb := fs.subjectDeleteMarkersAfterOperation(JSMarkerReasonPurge)
	fs.mu.Unlock()

	// Force a new index.db to be written.
	if purged > 0 {
		fs.forceWriteFullState()
	}

	if cb != nil {
		cb(-int64(purged), -rbytes, 0, _EMPTY_)
	}
	if sdmcb != nil {
		sdmcb()
	}

	return purged, nil
}

// Compact will remove all messages from this store up to
// but not including the seq parameter.
// No subject delete markers will be left if they are enabled. If they are disabled,
// then this is functionally equivalent to a normal Compact() call.
// Will return the number of purged messages.
func (fs *fileStore) Compact(seq uint64) (uint64, error) {
	return fs.compact(seq, false)
}

func (fs *fileStore) compact(seq uint64, _ /* noMarkers */ bool) (uint64, error) {
	// TODO: Don't write markers on compact until we have solved performance
	// issues with them.
	noMarkers := true

	if seq == 0 {
		return fs.purge(seq, noMarkers)
	}

	var purged, bytes uint64

	fs.mu.Lock()
	// Same as purge all.
	if lseq := fs.state.LastSeq; seq > lseq {
		fs.mu.Unlock()
		return fs.purge(seq, noMarkers)
	}
	// We have to delete interior messages.
	smb := fs.selectMsgBlock(seq)
	if smb == nil {
		fs.mu.Unlock()
		return 0, nil
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
		// Make sure we do subject cleanup as well.
		mb.ensurePerSubjectInfoLoaded()
		mb.fss.IterOrdered(func(bsubj []byte, ss *SimpleState) bool {
			subj := bytesToString(bsubj)
			for i := uint64(0); i < ss.Msgs; i++ {
				fs.removePerSubject(subj, !noMarkers && fs.cfg.SubjectDeleteMarkerTTL > 0)
			}
			return true
		})
		// Now close.
		mb.dirtyCloseWithRemove(true)
		mb.mu.Unlock()
		deleted++
	}

	var smv StoreMsg
	var err error

	smb.mu.Lock()
	if atomic.LoadUint64(&smb.first.seq) == seq {
		fs.state.FirstSeq = atomic.LoadUint64(&smb.first.seq)
		fs.state.FirstTime = time.Unix(0, smb.first.ts).UTC()
		goto SKIP
	}

	// Make sure we have the messages loaded.
	if smb.cacheNotLoaded() {
		if err = smb.loadMsgsWithLock(); err != nil {
			goto SKIP
		}
	}
	for mseq := atomic.LoadUint64(&smb.first.seq); mseq < seq; mseq++ {
		sm, err := smb.cacheLookup(mseq, &smv)
		if err == errDeletedMsg {
			// Update dmap.
			if !smb.dmap.IsEmpty() {
				smb.dmap.Delete(mseq)
			}
		} else if sm != nil {
			sz := fileStoreMsgSize(sm.subj, sm.hdr, sm.msg)
			if smb.msgs > 0 {
				smb.msgs--
				if sz > smb.bytes {
					sz = smb.bytes
				}
				smb.bytes -= sz
				bytes += sz
				purged++
			}
			// Update fss
			smb.removeSeqPerSubject(sm.subj, mseq)
			fs.removePerSubject(sm.subj, !noMarkers && fs.cfg.SubjectDeleteMarkerTTL > 0)
		}
	}

	// Check if empty after processing, could happen if tail of messages are all deleted.
	if isEmpty := smb.msgs == 0; isEmpty {
		// Only remove if not the last block.
		if smb != fs.lmb {
			smb.dirtyCloseWithRemove(true)
			deleted++
		} else {
			// Make sure to sync changes.
			smb.needSync = true
		}
		// Update fs first here as well.
		fs.state.FirstSeq = atomic.LoadUint64(&smb.last.seq) + 1
		fs.state.FirstTime = time.Time{}

	} else {
		// Make sure to sync changes.
		smb.needSync = true
		// Update fs first seq and time.
		atomic.StoreUint64(&smb.first.seq, seq-1) // Just for start condition for selectNextFirst.
		smb.selectNextFirst()

		fs.state.FirstSeq = atomic.LoadUint64(&smb.first.seq)
		fs.state.FirstTime = time.Unix(0, smb.first.ts).UTC()

		// Check if we should reclaim the head space from this block.
		// This will be optimistic only, so don't continue if we encounter any errors here.
		if smb.rbytes > compactMinimum && smb.bytes*2 < smb.rbytes {
			var moff uint32
			moff, _, _, err = smb.slotInfo(int(atomic.LoadUint64(&smb.first.seq) - smb.cache.fseq))
			if err != nil || moff >= uint32(len(smb.cache.buf)) {
				goto SKIP
			}
			buf := smb.cache.buf[moff:]
			// Don't reuse, copy to new recycled buf.
			nbuf := getMsgBlockBuf(len(buf))
			nbuf = append(nbuf, buf...)
			smb.closeFDsLockedNoCheck()
			// Check for encryption.
			if smb.bek != nil && len(nbuf) > 0 {
				// Recreate to reset counter.
				bek, err := genBlockEncryptionKey(smb.fs.fcfg.Cipher, smb.seed, smb.nonce)
				if err != nil {
					goto SKIP
				}
				// For future writes make sure to set smb.bek to keep counter correct.
				smb.bek = bek
				smb.bek.XORKeyStream(nbuf, nbuf)
			}
			// Recompress if necessary (smb.cmp contains the algorithm used when
			// the block was loaded from disk, or defaults to NoCompression if not)
			if nbuf, err = smb.cmp.Compress(nbuf); err != nil {
				goto SKIP
			}
			<-dios
			err = os.WriteFile(smb.mfn, nbuf, defaultFilePerms)
			dios <- struct{}{}
			if err != nil {
				goto SKIP
			}
			// Make sure to remove fss state.
			smb.fss = nil
			smb.clearCacheAndOffset()
			smb.rbytes = uint64(len(nbuf))
		}
	}

SKIP:
	smb.mu.Unlock()

	if deleted > 0 {
		// Update block map.
		if fs.bim != nil {
			for _, mb := range fs.blks[:deleted] {
				delete(fs.bim, mb.index)
			}
		}
		// Update blks slice.
		fs.blks = copyMsgBlocks(fs.blks[deleted:])
		if lb := len(fs.blks); lb == 0 {
			fs.lmb = nil
		} else {
			fs.lmb = fs.blks[lb-1]
		}
	}

	// Update top level accounting.
	if purged > fs.state.Msgs {
		purged = fs.state.Msgs
	}
	fs.state.Msgs -= purged
	if fs.state.Msgs == 0 {
		fs.state.FirstSeq = fs.state.LastSeq + 1
		fs.state.FirstTime = time.Time{}
	}

	if bytes > fs.state.Bytes {
		bytes = fs.state.Bytes
	}
	fs.state.Bytes -= bytes

	// Any existing state file no longer applicable. We will force write a new one
	// after we release the lock.
	os.Remove(filepath.Join(fs.fcfg.StoreDir, msgDir, streamStreamStateFile))
	fs.dirty++
	// Subject delete markers if needed.
	sdmcb := fs.subjectDeleteMarkersAfterOperation(JSMarkerReasonPurge)
	cb := fs.scb
	fs.mu.Unlock()

	// Force a new index.db to be written.
	if purged > 0 {
		fs.forceWriteFullState()
	}

	if cb != nil && purged > 0 {
		cb(-int64(purged), -int64(bytes), 0, _EMPTY_)
	}
	if sdmcb != nil {
		sdmcb()
	}

	return purged, err
}

// Will completely reset our store.
func (fs *fileStore) reset() error {
	fs.mu.Lock()
	if fs.closed {
		fs.mu.Unlock()
		return ErrStoreClosed
	}
	if fs.sips > 0 {
		fs.mu.Unlock()
		return ErrStoreSnapshotInProgress
	}

	var purged, bytes uint64
	cb := fs.scb

	for _, mb := range fs.blks {
		mb.mu.Lock()
		purged += mb.msgs
		bytes += mb.bytes
		mb.dirtyCloseWithRemove(true)
		mb.mu.Unlock()
	}

	// Reset
	fs.state.FirstSeq = 0
	fs.state.FirstTime = time.Time{}
	fs.state.LastSeq = 0
	fs.state.LastTime = time.Now().UTC()
	// Update msgs and bytes.
	fs.state.Msgs = 0
	fs.state.Bytes = 0

	// Reset blocks.
	fs.blks, fs.lmb = nil, nil

	// Reset subject mappings.
	fs.psim, fs.tsl = fs.psim.Empty(), 0
	fs.bim = make(map[uint32]*msgBlock)

	// If we purged anything, make sure we kick flush state loop.
	if purged > 0 {
		fs.dirty++
	}

	fs.mu.Unlock()

	if cb != nil {
		cb(-int64(purged), -int64(bytes), 0, _EMPTY_)
	}

	return nil
}

// Return all active tombstones in this msgBlock.
func (mb *msgBlock) tombs() []msgId {
	mb.mu.Lock()
	defer mb.mu.Unlock()
	return mb.tombsLocked()
}

// Return all active tombstones in this msgBlock.
// Write lock should be held.
func (mb *msgBlock) tombsLocked() []msgId {
	if mb.cacheNotLoaded() {
		if err := mb.loadMsgsWithLock(); err != nil {
			return nil
		}
	}

	var tombs []msgId
	var le = binary.LittleEndian
	buf := mb.cache.buf

	for index, lbuf := uint32(0), uint32(len(buf)); index < lbuf; {
		if index+msgHdrSize > lbuf {
			return tombs
		}
		hdr := buf[index : index+msgHdrSize]
		rl, seq := le.Uint32(hdr[0:]), le.Uint64(hdr[4:])
		// Clear any headers bit that could be set.
		rl &^= hbit
		// Check for tombstones.
		if seq&tbit != 0 {
			ts := int64(le.Uint64(hdr[12:]))
			tombs = append(tombs, msgId{seq &^ tbit, ts})
		}
		// Advance to next record.
		index += rl
	}

	return tombs
}

// Truncate will truncate a stream store up to seq. Sequence needs to be valid.
func (fs *fileStore) Truncate(seq uint64) error {
	// Check for request to reset.
	if seq == 0 {
		return fs.reset()
	}

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
	lsm, _, _ := nlmb.fetchMsg(seq, nil)
	if lsm == nil {
		fs.mu.Unlock()
		return ErrInvalidSequence
	}

	// Set lmb to nlmb and make sure writeable.
	fs.lmb = nlmb
	if err := nlmb.enableForWriting(fs.fip); err != nil {
		fs.mu.Unlock()
		return err
	}
	// Collect all tombstones, we want to put these back so we can survive
	// a restore without index.db properly.
	var tombs []msgId
	tombs = append(tombs, nlmb.tombs()...)

	var purged, bytes uint64

	// Truncate our new last message block.
	nmsgs, nbytes, err := nlmb.truncate(lsm)
	if err != nil {
		fs.mu.Unlock()
		return fmt.Errorf("nlmb.truncate: %w", err)
	}
	// Account for the truncated msgs and bytes.
	purged += nmsgs
	bytes += nbytes

	// Remove any left over msg blocks.
	getLastMsgBlock := func() *msgBlock { return fs.blks[len(fs.blks)-1] }
	for mb := getLastMsgBlock(); mb != nlmb; mb = getLastMsgBlock() {
		mb.mu.Lock()
		// We do this to load tombs.
		tombs = append(tombs, mb.tombsLocked()...)
		purged += mb.msgs
		bytes += mb.bytes
		fs.removeMsgBlock(mb)
		mb.mu.Unlock()
	}

	// Reset last.
	fs.state.LastSeq = lsm.seq
	fs.state.LastTime = time.Unix(0, lsm.ts).UTC()
	// Update msgs and bytes.
	if purged > fs.state.Msgs {
		purged = fs.state.Msgs
	}
	fs.state.Msgs -= purged
	if bytes > fs.state.Bytes {
		bytes = fs.state.Bytes
	}
	fs.state.Bytes -= bytes

	// Reset our subject lookup info.
	fs.resetGlobalPerSubjectInfo()

	// Always create new write block.
	fs.newMsgBlockForWrite()

	// Write any tombstones as needed.
	for _, tomb := range tombs {
		if tomb.seq <= lsm.seq {
			fs.writeTombstone(tomb.seq, tomb.ts)
		}
	}

	// Any existing state file no longer applicable. We will force write a new one
	// after we release the lock.
	os.Remove(filepath.Join(fs.fcfg.StoreDir, msgDir, streamStreamStateFile))
	fs.dirty++

	cb := fs.scb
	fs.mu.Unlock()

	// Force a new index.db to be written.
	if purged > 0 {
		fs.forceWriteFullState()
	}

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

// Will add a new msgBlock.
// Lock should be held.
func (fs *fileStore) addMsgBlock(mb *msgBlock) {
	fs.blks = append(fs.blks, mb)
	fs.lmb = mb
	fs.bim[mb.index] = mb
}

// Remove from our list of blks.
// Both locks should be held.
func (fs *fileStore) removeMsgBlockFromList(mb *msgBlock) {
	// Remove from list.
	for i, omb := range fs.blks {
		if mb == omb {
			fs.dirty++
			blks := append(fs.blks[:i], fs.blks[i+1:]...)
			fs.blks = copyMsgBlocks(blks)
			if fs.bim != nil {
				delete(fs.bim, mb.index)
			}
			break
		}
	}
}

// Removes the msgBlock
// Both locks should be held.
func (fs *fileStore) removeMsgBlock(mb *msgBlock) {
	mb.dirtyCloseWithRemove(true)
	fs.removeMsgBlockFromList(mb)
	// Check for us being last message block
	if mb == fs.lmb {
		lseq, lts := atomic.LoadUint64(&mb.last.seq), mb.last.ts
		// Creating a new message write block requires that the lmb lock is not held.
		mb.mu.Unlock()
		// Write the tombstone to remember since this was last block.
		if lmb, _ := fs.newMsgBlockForWrite(); lmb != nil {
			fs.writeTombstone(lseq, lts)
		}
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
func (mb *msgBlock) dirtyCloseWithRemove(remove bool) error {
	if mb == nil {
		return nil
	}
	// Stop cache expiration timer.
	if mb.ctmr != nil {
		mb.ctmr.Stop()
		mb.ctmr = nil
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
	if remove {
		// Clear any tracking by subject if we are removing.
		mb.fss = nil
		if mb.mfn != _EMPTY_ {
			err := os.Remove(mb.mfn)
			if isPermissionError(err) {
				return err
			}
			mb.mfn = _EMPTY_
		}
		if mb.kfn != _EMPTY_ {
			err := os.Remove(mb.kfn)
			if isPermissionError(err) {
				return err
			}
		}
	}
	return nil
}

// Remove a seq from the fss and select new first.
// Lock should be held.
func (mb *msgBlock) removeSeqPerSubject(subj string, seq uint64) {
	mb.ensurePerSubjectInfoLoaded()
	if mb.fss == nil {
		return
	}
	bsubj := stringToBytes(subj)
	ss, ok := mb.fss.Find(bsubj)
	if !ok || ss == nil {
		return
	}

	if ss.Msgs == 1 {
		mb.fss.Delete(bsubj)
		return
	}

	ss.Msgs--

	// Only one left.
	if ss.Msgs == 1 {
		if !ss.lastNeedsUpdate && seq != ss.Last {
			ss.First = ss.Last
			ss.firstNeedsUpdate = false
			return
		}
		if !ss.firstNeedsUpdate && seq != ss.First {
			ss.Last = ss.First
			ss.lastNeedsUpdate = false
			return
		}
	}

	// We can lazily calculate the first/last sequence when needed.
	ss.firstNeedsUpdate = seq == ss.First || ss.firstNeedsUpdate
	ss.lastNeedsUpdate = seq == ss.Last || ss.lastNeedsUpdate
}

// Will recalculate the first and/or last sequence for this subject in this block.
// Will avoid slower path message lookups and scan the cache directly instead.
func (mb *msgBlock) recalculateForSubj(subj string, ss *SimpleState) {
	// Need to make sure messages are loaded.
	if mb.cacheNotLoaded() {
		if err := mb.loadMsgsWithLock(); err != nil {
			return
		}
	}

	startSlot := int(ss.First - mb.cache.fseq)
	if startSlot < 0 {
		startSlot = 0
	}
	if startSlot >= len(mb.cache.idx) {
		ss.First = ss.Last
		ss.firstNeedsUpdate = false
		ss.lastNeedsUpdate = false
		return
	}

	endSlot := int(ss.Last - mb.cache.fseq)
	if endSlot < 0 {
		endSlot = 0
	}
	if endSlot >= len(mb.cache.idx) || startSlot > endSlot {
		return
	}

	var le = binary.LittleEndian
	if ss.firstNeedsUpdate {
		// Mark first as updated.
		ss.firstNeedsUpdate = false

		fseq := ss.First + 1
		if mbFseq := atomic.LoadUint64(&mb.first.seq); fseq < mbFseq {
			fseq = mbFseq
		}
		for slot := startSlot; slot < len(mb.cache.idx); slot++ {
			bi := mb.cache.idx[slot] &^ cbit
			if bi == dbit {
				// delete marker so skip.
				continue
			}
			li := int(bi) - mb.cache.off
			if li >= len(mb.cache.buf) {
				ss.First = ss.Last
				// Only need to reset ss.lastNeedsUpdate, ss.firstNeedsUpdate is already reset above.
				ss.lastNeedsUpdate = false
				return
			}
			buf := mb.cache.buf[li:]
			hdr := buf[:msgHdrSize]
			slen := int(le.Uint16(hdr[20:]))
			if subj == bytesToString(buf[msgHdrSize:msgHdrSize+slen]) {
				seq := le.Uint64(hdr[4:])
				if seq < fseq || seq&ebit != 0 || mb.dmap.Exists(seq) {
					continue
				}
				ss.First = seq
				if ss.Msgs == 1 {
					ss.Last = seq
					ss.lastNeedsUpdate = false
					return
				}
				// Skip the start slot ahead, if we need to recalculate last we can stop early.
				startSlot = slot
				break
			}
		}
	}
	if ss.lastNeedsUpdate {
		// Mark last as updated.
		ss.lastNeedsUpdate = false

		lseq := ss.Last - 1
		if mbLseq := atomic.LoadUint64(&mb.last.seq); lseq > mbLseq {
			lseq = mbLseq
		}
		for slot := endSlot; slot >= startSlot; slot-- {
			bi := mb.cache.idx[slot] &^ cbit
			if bi == dbit {
				// delete marker so skip.
				continue
			}
			li := int(bi) - mb.cache.off
			if li >= len(mb.cache.buf) {
				// Can't overwrite ss.Last, just skip.
				return
			}
			buf := mb.cache.buf[li:]
			hdr := buf[:msgHdrSize]
			slen := int(le.Uint16(hdr[20:]))
			if subj == bytesToString(buf[msgHdrSize:msgHdrSize+slen]) {
				seq := le.Uint64(hdr[4:])
				if seq > lseq || seq&ebit != 0 || mb.dmap.Exists(seq) {
					continue
				}
				// Sequence should never be lower, but guard against it nonetheless.
				if seq < ss.First {
					seq = ss.First
				}
				ss.Last = seq
				if ss.Msgs == 1 {
					ss.First = seq
					ss.firstNeedsUpdate = false
				}
				return
			}
		}
	}
}

// Lock should be held.
func (fs *fileStore) resetGlobalPerSubjectInfo() {
	// Clear any global subject state.
	fs.psim, fs.tsl = fs.psim.Empty(), 0
	for _, mb := range fs.blks {
		fs.populateGlobalPerSubjectInfo(mb)
	}
}

// Lock should be held.
func (mb *msgBlock) resetPerSubjectInfo() error {
	mb.fss = nil
	return mb.generatePerSubjectInfo()
}

// generatePerSubjectInfo will generate the per subject info via the raw msg block.
// Lock should be held.
func (mb *msgBlock) generatePerSubjectInfo() error {
	// Check if this mb is empty. This can happen when its the last one and we are holding onto it for seq and timestamp info.
	if mb.msgs == 0 {
		return nil
	}

	if mb.cacheNotLoaded() {
		if err := mb.loadMsgsWithLock(); err != nil {
			return err
		}
		// indexCacheBuf can produce fss now, so if non-nil we are good.
		if mb.fss != nil {
			return nil
		}
	}

	// Create new one regardless.
	mb.fss = stree.NewSubjectTree[SimpleState]()

	var smv StoreMsg
	fseq, lseq := atomic.LoadUint64(&mb.first.seq), atomic.LoadUint64(&mb.last.seq)
	for seq := fseq; seq <= lseq; seq++ {
		if mb.dmap.Exists(seq) {
			// Optimisation to avoid calling cacheLookup which hits time.Now().
			// It gets set later on if the fss is non-empty anyway.
			continue
		}
		sm, err := mb.cacheLookup(seq, &smv)
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
			if ss, ok := mb.fss.Find(stringToBytes(sm.subj)); ok && ss != nil {
				ss.Msgs++
				ss.Last = seq
				ss.lastNeedsUpdate = false
			} else {
				mb.fss.Insert(stringToBytes(sm.subj), SimpleState{Msgs: 1, First: seq, Last: seq})
			}
		}
	}

	if mb.fss.Size() > 0 {
		// Make sure we run the cache expire timer.
		mb.llts = time.Now().UnixNano()
		// Mark fss activity.
		mb.lsts = time.Now().UnixNano()
		mb.startCacheExpireTimer()
	}
	return nil
}

// Helper to make sure fss loaded if we are tracking.
// Lock should be held
func (mb *msgBlock) ensurePerSubjectInfoLoaded() error {
	if mb.fss != nil || mb.noTrack {
		if mb.fss != nil {
			// Mark fss activity.
			mb.lsts = time.Now().UnixNano()
		}
		return nil
	}
	if mb.msgs == 0 {
		mb.fss = stree.NewSubjectTree[SimpleState]()
		return nil
	}
	return mb.generatePerSubjectInfo()
}

// Called on recovery to populate the global psim state.
// Lock should be held.
func (fs *fileStore) populateGlobalPerSubjectInfo(mb *msgBlock) {
	mb.mu.Lock()
	defer mb.mu.Unlock()

	if err := mb.ensurePerSubjectInfoLoaded(); err != nil {
		return
	}

	// Now populate psim.
	mb.fss.IterFast(func(bsubj []byte, ss *SimpleState) bool {
		if len(bsubj) > 0 {
			if info, ok := fs.psim.Find(bsubj); ok {
				info.total += ss.Msgs
				if mb.index > info.lblk {
					info.lblk = mb.index
				}
			} else {
				fs.psim.Insert(bsubj, psi{total: ss.Msgs, fblk: mb.index, lblk: mb.index})
				fs.tsl += len(bsubj)
			}
		}
		return true
	})
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

	// Stop cache expiration timer.
	if mb.ctmr != nil {
		mb.ctmr.Stop()
		mb.ctmr = nil
	}

	// Clear fss.
	mb.fss = nil

	// Close cache
	mb.clearCacheAndOffset()
	// Quit our loops.
	if mb.qch != nil {
		close(mb.qch)
		mb.qch = nil
	}
	if mb.mfd != nil {
		if sync {
			mb.mfd.Sync()
		}
		mb.mfd.Close()
	}
	mb.mfd = nil
	// Mark as closed.
	mb.closed = true
}

func (fs *fileStore) closeAllMsgBlocks(sync bool) {
	for _, mb := range fs.blks {
		mb.close(sync)
	}
}

func (fs *fileStore) Delete() error {
	if fs.isClosed() {
		// Always attempt to remove since we could have been closed beforehand.
		os.RemoveAll(fs.fcfg.StoreDir)
		// Since we did remove, if we did have anything remaining make sure to
		// call into any storage updates that had been registered.
		fs.mu.Lock()
		cb, msgs, bytes := fs.scb, int64(fs.state.Msgs), int64(fs.state.Bytes)
		// Guard against double accounting if called twice.
		fs.state.Msgs, fs.state.Bytes = 0, 0
		fs.mu.Unlock()
		if msgs > 0 && cb != nil {
			cb(-msgs, -bytes, 0, _EMPTY_)
		}
		return ErrStoreClosed
	}

	pdir := filepath.Join(fs.fcfg.StoreDir, purgeDir)
	// If purge directory still exists then we need to wait
	// in place and remove since rename would fail.
	if _, err := os.Stat(pdir); err == nil {
		os.RemoveAll(pdir)
	}

	// Quickly close all blocks and simulate a purge w/o overhead an new write block.
	fs.mu.Lock()
	for _, mb := range fs.blks {
		mb.dirtyClose()
	}
	dmsgs := fs.state.Msgs
	dbytes := int64(fs.state.Bytes)
	fs.state.Msgs, fs.state.Bytes = 0, 0
	fs.blks = nil
	cb := fs.scb
	fs.mu.Unlock()

	if cb != nil {
		cb(-int64(dmsgs), -dbytes, 0, _EMPTY_)
	}

	if err := fs.stop(true, false); err != nil {
		return err
	}

	// Make sure we will not try to recover if killed before removal below completes.
	if err := os.Remove(filepath.Join(fs.fcfg.StoreDir, JetStreamMetaFile)); err != nil {
		return err
	}
	// Now move into different directory with "." prefix.
	ndir := filepath.Join(filepath.Dir(fs.fcfg.StoreDir), tsep+filepath.Base(fs.fcfg.StoreDir))
	if err := os.Rename(fs.fcfg.StoreDir, ndir); err != nil {
		return err
	}
	// Do this in separate Go routine in case lots of blocks.
	// Purge above protects us as does the removal of meta artifacts above.
	go func() {
		<-dios
		err := os.RemoveAll(ndir)
		dios <- struct{}{}
		if err == nil {
			return
		}
		ttl := time.Now().Add(time.Second)
		for time.Now().Before(ttl) {
			time.Sleep(10 * time.Millisecond)
			<-dios
			err = os.RemoveAll(ndir)
			dios <- struct{}{}
			if err == nil {
				return
			}
		}
	}()

	return nil
}

// Lock should be held.
func (fs *fileStore) setSyncTimer() {
	if fs.syncTmr != nil {
		fs.syncTmr.Reset(fs.fcfg.SyncInterval)
	} else {
		// First time this fires will be between SyncInterval/2 and SyncInterval,
		// so that different stores are spread out, rather than having many of
		// them trying to all sync at once, causing blips and contending dios.
		start := (fs.fcfg.SyncInterval / 2) + (time.Duration(mrand.Int63n(int64(fs.fcfg.SyncInterval / 2))))
		fs.syncTmr = time.AfterFunc(start, fs.syncBlocks)
	}
}

// Lock should be held.
func (fs *fileStore) cancelSyncTimer() {
	if fs.syncTmr != nil {
		fs.syncTmr.Stop()
		fs.syncTmr = nil
	}
}

// The full state file is versioned.
// - 0x1: original binary index.db format
// - 0x2: adds support for TTL count field after num deleted
const (
	fullStateMagic      = uint8(11)
	fullStateMinVersion = uint8(1) // What is the minimum version we know how to parse?
	fullStateVersion    = uint8(2) // What is the current version written out to index.db?
)

// This go routine periodically writes out our full stream state index.
func (fs *fileStore) flushStreamStateLoop(qch, done chan struct{}) {
	// Signal we are done on exit.
	defer close(done)

	// Make sure we do not try to write these out too fast.
	// Spread these out for large numbers on a server restart.
	const writeThreshold = 2 * time.Minute
	writeJitter := time.Duration(mrand.Int63n(int64(30 * time.Second)))
	t := time.NewTicker(writeThreshold + writeJitter)
	defer t.Stop()

	for {
		select {
		case <-t.C:
			err := fs.writeFullState()
			if isPermissionError(err) && fs.srv != nil {
				fs.warn("File system permission denied when flushing stream state, disabling JetStream: %v", err)
				// messages in block cache could be lost in the worst case.
				// In the clustered mode it is very highly unlikely as a result of replication.
				fs.srv.DisableJetStream()
				return
			}

		case <-qch:
			return
		}
	}
}

// Helper since unixnano of zero time undefined.
func timestampNormalized(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixNano()
}

// writeFullState will proceed to write the full meta state iff not complex and time consuming.
// Since this is for quick recovery it is optional and should not block/stall normal operations.
func (fs *fileStore) writeFullState() error {
	return fs._writeFullState(false)
}

// forceWriteFullState will proceed to write the full meta state. This should only be called by stop()
func (fs *fileStore) forceWriteFullState() error {
	return fs._writeFullState(true)
}

// This will write the full binary state for the stream.
// This plus everything new since last hash will be the total recovered state.
// This state dump will have the following.
// 1. Stream summary - Msgs, Bytes, First and Last (Sequence and Timestamp)
// 2. PSIM - Per Subject Index Map - Tracks first and last blocks with subjects present.
// 3. MBs - Index, Bytes, First and Last Sequence and Timestamps, and the deleted map (avl.seqset).
// 4. Last block index and hash of record inclusive to this stream state.
func (fs *fileStore) _writeFullState(force bool) error {
	start := time.Now()
	fs.mu.Lock()
	if fs.closed || fs.dirty == 0 {
		fs.mu.Unlock()
		return nil
	}

	// For calculating size and checking time costs for non forced calls.
	numSubjects := fs.numSubjects()

	// If we are not being forced to write out our state, check the complexity for time costs as to not
	// block or stall normal operations.
	// We will base off of number of subjects and interior deletes. A very large number of msg blocks could also
	// be used, but for next server version will redo all meta handling to be disk based. So this is temporary.
	if !force {
		const numThreshold = 1_000_000
		// Calculate interior deletes.
		var numDeleted int
		if fs.state.LastSeq > fs.state.FirstSeq {
			numDeleted = int((fs.state.LastSeq - fs.state.FirstSeq + 1) - fs.state.Msgs)
		}
		if numSubjects > numThreshold || numDeleted > numThreshold {
			fs.mu.Unlock()
			return errStateTooBig
		}
	}

	// We track this through subsequent runs to get an avg per blk used for subsequent runs.
	avgDmapLen := fs.adml
	// If first time through could be 0
	if avgDmapLen == 0 && ((fs.state.LastSeq-fs.state.FirstSeq+1)-fs.state.Msgs) > 0 {
		avgDmapLen = 1024
	}

	// Calculate and estimate of the uper bound on the  size to avoid multiple allocations.
	sz := hdrLen + // Magic and Version
		(binary.MaxVarintLen64 * 6) + // FS data
		binary.MaxVarintLen64 + fs.tsl + // NumSubjects + total subject length
		numSubjects*(binary.MaxVarintLen64*4) + // psi record
		binary.MaxVarintLen64 + // Num blocks.
		len(fs.blks)*((binary.MaxVarintLen64*8)+avgDmapLen) + // msg blocks, avgDmapLen is est for dmaps
		binary.MaxVarintLen64 + 8 + 8 // last index + record checksum + full state checksum

	// Do 4k on stack if possible.
	const ssz = 4 * 1024
	var buf []byte

	if sz <= ssz {
		var _buf [ssz]byte
		buf, sz = _buf[0:hdrLen:ssz], ssz
	} else {
		buf = make([]byte, hdrLen, sz)
	}

	buf[0], buf[1] = fullStateMagic, fullStateVersion
	buf = binary.AppendUvarint(buf, fs.state.Msgs)
	buf = binary.AppendUvarint(buf, fs.state.Bytes)
	buf = binary.AppendUvarint(buf, fs.state.FirstSeq)
	buf = binary.AppendVarint(buf, timestampNormalized(fs.state.FirstTime))
	buf = binary.AppendUvarint(buf, fs.state.LastSeq)
	buf = binary.AppendVarint(buf, timestampNormalized(fs.state.LastTime))

	// Do per subject information map if applicable.
	buf = binary.AppendUvarint(buf, uint64(numSubjects))
	if numSubjects > 0 {
		fs.psim.Match([]byte(fwcs), func(subj []byte, psi *psi) {
			buf = binary.AppendUvarint(buf, uint64(len(subj)))
			buf = append(buf, subj...)
			buf = binary.AppendUvarint(buf, psi.total)
			buf = binary.AppendUvarint(buf, uint64(psi.fblk))
			if psi.total > 1 {
				buf = binary.AppendUvarint(buf, uint64(psi.lblk))
			}
		})
	}

	// Now walk all blocks and write out first and last and optional dmap encoding.
	var lbi uint32
	var lchk [8]byte

	nb := len(fs.blks)
	buf = binary.AppendUvarint(buf, uint64(nb))

	// Use basetime to save some space.
	baseTime := timestampNormalized(fs.state.FirstTime)
	var scratch [8 * 1024]byte

	// Track the state as represented by the mbs.
	var mstate StreamState

	var dmapTotalLen int
	for _, mb := range fs.blks {
		mb.mu.RLock()
		buf = binary.AppendUvarint(buf, uint64(mb.index))
		buf = binary.AppendUvarint(buf, mb.bytes)
		buf = binary.AppendUvarint(buf, atomic.LoadUint64(&mb.first.seq))
		buf = binary.AppendVarint(buf, mb.first.ts-baseTime)
		buf = binary.AppendUvarint(buf, atomic.LoadUint64(&mb.last.seq))
		buf = binary.AppendVarint(buf, mb.last.ts-baseTime)

		numDeleted := mb.dmap.Size()
		buf = binary.AppendUvarint(buf, uint64(numDeleted))
		buf = binary.AppendUvarint(buf, mb.ttls) // Field is new in version 2
		if numDeleted > 0 {
			dmap, _ := mb.dmap.Encode(scratch[:0])
			dmapTotalLen += len(dmap)
			buf = append(buf, dmap...)
		}
		// If this is the last one grab the last checksum and the block index, e.g. 22.blk, 22 is the block index.
		// We use this to quickly open this file on recovery.
		if mb == fs.lmb {
			lbi = mb.index
			mb.ensureLastChecksumLoaded()
			copy(lchk[0:], mb.lchk[:])
		}
		updateTrackingState(&mstate, mb)
		mb.mu.RUnlock()
	}
	if dmapTotalLen > 0 {
		fs.adml = dmapTotalLen / len(fs.blks)
	}

	// Place block index and hash onto the end.
	buf = binary.AppendUvarint(buf, uint64(lbi))
	buf = append(buf, lchk[:]...)

	// Encrypt if needed.
	if fs.prf != nil {
		if err := fs.setupAEK(); err != nil {
			fs.mu.Unlock()
			return err
		}
		nonce := make([]byte, fs.aek.NonceSize(), fs.aek.NonceSize()+len(buf)+fs.aek.Overhead())
		if n, err := rand.Read(nonce); err != nil {
			return err
		} else if n != len(nonce) {
			return fmt.Errorf("not enough nonce bytes read (%d != %d)", n, len(nonce))
		}
		buf = fs.aek.Seal(nonce, nonce, buf, nil)
	}

	fn := filepath.Join(fs.fcfg.StoreDir, msgDir, streamStreamStateFile)

	fs.hh.Reset()
	fs.hh.Write(buf)
	buf = fs.hh.Sum(buf)

	// Snapshot prior dirty count.
	priorDirty := fs.dirty

	statesEqual := trackingStatesEqual(&fs.state, &mstate)
	// Release lock.
	fs.mu.Unlock()

	// Check consistency here.
	if !statesEqual {
		fs.warn("Stream state encountered internal inconsistency on write")
		// Rebuild our fs state from the mb state.
		fs.rebuildState(nil)
		return errCorruptState
	}

	if cap(buf) > sz {
		fs.debug("WriteFullState reallocated from %d to %d", sz, cap(buf))
	}

	// Only warn about construction time since file write not holding any locks.
	if took := time.Since(start); took > time.Minute {
		fs.warn("WriteFullState took %v (%d bytes)", took.Round(time.Millisecond), len(buf))
	}

	// Write our update index.db
	// Protect with dios.
	<-dios
	err := os.WriteFile(fn, buf, defaultFilePerms)
	// if file system is not writable isPermissionError is set to true
	dios <- struct{}{}
	if isPermissionError(err) {
		return err
	}

	// Update dirty if successful.
	if err == nil {
		fs.mu.Lock()
		fs.dirty -= priorDirty
		fs.mu.Unlock()
	}

	return fs.writeTTLState()
}

func (fs *fileStore) writeTTLState() error {
	if fs.ttls == nil {
		return nil
	}

	fs.mu.RLock()
	fn := filepath.Join(fs.fcfg.StoreDir, msgDir, ttlStreamStateFile)
	buf := fs.ttls.Encode(fs.state.LastSeq)
	fs.mu.RUnlock()

	<-dios
	err := os.WriteFile(fn, buf, defaultFilePerms)
	dios <- struct{}{}

	return err
}

// Stop the current filestore.
func (fs *fileStore) Stop() error {
	return fs.stop(false, true)
}

// Stop the current filestore.
func (fs *fileStore) stop(delete, writeState bool) error {
	fs.mu.Lock()
	if fs.closed || fs.closing {
		fs.mu.Unlock()
		return ErrStoreClosed
	}

	// Mark as closing. Do before releasing the lock to writeFullState
	// so we don't end up with this function running more than once.
	fs.closing = true

	if writeState {
		fs.checkAndFlushAllBlocks()
	}
	fs.closeAllMsgBlocks(false)

	fs.cancelSyncTimer()
	fs.cancelAgeChk()

	// Release the state flusher loop.
	if fs.qch != nil {
		close(fs.qch)
		fs.qch = nil
	}

	if writeState {
		// Wait for the state flush loop to exit.
		fsld := fs.fsld
		fs.mu.Unlock()
		<-fsld
		// Write full state if needed. If not dirty this is a no-op.
		fs.forceWriteFullState()
		fs.mu.Lock()
	}

	// Mark as closed. Last message block needs to be cleared after
	// writeFullState has completed.
	fs.closed = true
	fs.lmb = nil

	// We should update the upper usage layer on a stop.
	cb, bytes := fs.scb, int64(fs.state.Bytes)
	fs.mu.Unlock()

	fs.cmu.Lock()
	var _cfs [256]ConsumerStore
	cfs := append(_cfs[:0], fs.cfs...)
	fs.cfs = nil
	fs.cmu.Unlock()

	for _, o := range cfs {
		if delete {
			o.StreamDelete()
		} else {
			o.Stop()
		}
	}

	if bytes > 0 && cb != nil {
		cb(0, -bytes, 0, _EMPTY_)
	}

	return nil
}

const errFile = "errors.txt"

// Stream our snapshot through S2 compression and tar.
func (fs *fileStore) streamSnapshot(w io.WriteCloser, includeConsumers bool, errCh chan string) {
	defer close(errCh)
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
		errCh <- err
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
	hh := fs.hh
	hh.Reset()
	hh.Write(meta)
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
		mb.mu.Lock()
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
			rbek, err := genBlockEncryptionKey(fs.fcfg.Cipher, mb.seed, mb.nonce)
			if err != nil {
				mb.mu.Unlock()
				writeErr(fmt.Sprintf("Could not create encryption key for message block [%d]: %v", mb.index, err))
				return
			}
			rbek.XORKeyStream(bbuf, bbuf)
		}
		// Check for compression.
		if bbuf, err = mb.decompressIfNeeded(bbuf); err != nil {
			mb.mu.Unlock()
			writeErr(fmt.Sprintf("Could not decompress message block [%d]: %v", mb.index, err))
			return
		}
		mb.mu.Unlock()

		// Do this one unlocked.
		if writeFile(msgPre+fmt.Sprintf(blkScan, mb.index), bbuf) != nil {
			return
		}
	}

	// Do index.db last. We will force a write as well.
	// Write out full state as well before proceeding.
	if err := fs.forceWriteFullState(); err == nil {
		const minLen = 32
		sfn := filepath.Join(fs.fcfg.StoreDir, msgDir, streamStreamStateFile)
		if buf, err := os.ReadFile(sfn); err == nil && len(buf) >= minLen {
			if fs.aek != nil {
				ns := fs.aek.NonceSize()
				buf, err = fs.aek.Open(nil, buf[:ns], buf[ns:len(buf)-highwayhash.Size64], nil)
				if err == nil {
					// Redo hash checksum at end on plaintext.
					fs.mu.Lock()
					hh.Reset()
					hh.Write(buf)
					buf = fs.hh.Sum(buf)
					fs.mu.Unlock()
				}
			}
			if err == nil && writeFile(msgPre+streamStreamStateFile, buf) != nil {
				return
			}
		}
	}

	// Bail if no consumers requested.
	if !includeConsumers {
		return
	}

	// Do consumers' state last.
	fs.cmu.RLock()
	cfs := fs.cfs
	fs.cmu.RUnlock()

	for _, cs := range cfs {
		o, ok := cs.(*consumerFileStore)
		if !ok {
			continue
		}
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
		odirPre := filepath.Join(consumerDir, o.name)
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

	// We can add to our stream while snapshotting but not "user" delete anything.
	var state StreamState
	fs.FastState(&state)

	// Stream in separate Go routine.
	errCh := make(chan string, 1)
	go fs.streamSnapshot(pw, includeConsumers, errCh)

	return &SnapshotResult{pr, state, errCh}, nil
}

// Helper to return the config.
func (fs *fileStore) fileStoreConfig() FileStoreConfig {
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	return fs.fcfg
}

// Read lock all existing message blocks.
// Lock held on entry.
func (fs *fileStore) readLockAllMsgBlocks() {
	for _, mb := range fs.blks {
		mb.mu.RLock()
	}
}

// Read unlock all existing message blocks.
// Lock held on entry.
func (fs *fileStore) readUnlockAllMsgBlocks() {
	for _, mb := range fs.blks {
		mb.mu.RUnlock()
	}
}

// Binary encoded state snapshot, >= v2.10 server.
func (fs *fileStore) EncodedStreamState(failed uint64) ([]byte, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	// Calculate deleted.
	var numDeleted int64
	if fs.state.LastSeq > fs.state.FirstSeq {
		numDeleted = int64(fs.state.LastSeq-fs.state.FirstSeq+1) - int64(fs.state.Msgs)
		if numDeleted < 0 {
			numDeleted = 0
		}
	}

	// Encoded is Msgs, Bytes, FirstSeq, LastSeq, Failed, NumDeleted and optional DeletedBlocks
	var buf [1024]byte
	buf[0], buf[1] = streamStateMagic, streamStateVersion
	n := hdrLen
	n += binary.PutUvarint(buf[n:], fs.state.Msgs)
	n += binary.PutUvarint(buf[n:], fs.state.Bytes)
	n += binary.PutUvarint(buf[n:], fs.state.FirstSeq)
	n += binary.PutUvarint(buf[n:], fs.state.LastSeq)
	n += binary.PutUvarint(buf[n:], failed)
	n += binary.PutUvarint(buf[n:], uint64(numDeleted))

	b := buf[0:n]

	if numDeleted > 0 {
		var scratch [4 * 1024]byte

		fs.readLockAllMsgBlocks()
		defer fs.readUnlockAllMsgBlocks()

		for _, db := range fs.deleteBlocks() {
			switch db := db.(type) {
			case *DeleteRange:
				first, _, num := db.State()
				scratch[0] = runLengthMagic
				i := 1
				i += binary.PutUvarint(scratch[i:], first)
				i += binary.PutUvarint(scratch[i:], num)
				b = append(b, scratch[0:i]...)
			case *avl.SequenceSet:
				buf, err := db.Encode(scratch[:0])
				if err != nil {
					return nil, err
				}
				b = append(b, buf...)
			default:
				return nil, errors.New("no impl")
			}
		}
	}

	return b, nil
}

// We used to be more sophisticated to save memory, but speed is more important.
// All blocks should be at least read locked.
func (fs *fileStore) deleteBlocks() DeleteBlocks {
	var dbs DeleteBlocks
	var prevLast uint64

	for _, mb := range fs.blks {
		// Detect if we have a gap between these blocks.
		fseq := atomic.LoadUint64(&mb.first.seq)
		if prevLast > 0 && prevLast+1 != fseq {
			dbs = append(dbs, &DeleteRange{First: prevLast + 1, Num: fseq - prevLast - 1})
		}
		if mb.dmap.Size() > 0 {
			dbs = append(dbs, &mb.dmap)
		}
		prevLast = atomic.LoadUint64(&mb.last.seq)
	}
	return dbs
}

// SyncDeleted will make sure this stream has same deleted state as dbs.
// This will only process deleted state within our current state.
func (fs *fileStore) SyncDeleted(dbs DeleteBlocks) {
	if len(dbs) == 0 {
		return
	}

	fs.mu.Lock()
	defer fs.mu.Unlock()

	lseq := fs.state.LastSeq
	var needsCheck DeleteBlocks

	fs.readLockAllMsgBlocks()
	mdbs := fs.deleteBlocks()
	for i, db := range dbs {
		first, last, num := db.State()
		// If the block is same as what we have we can skip.
		if i < len(mdbs) {
			eFirst, eLast, eNum := mdbs[i].State()
			if first == eFirst && last == eLast && num == eNum {
				continue
			}
		} else if first > lseq {
			// Skip blocks not applicable to our current state.
			continue
		}
		// Need to insert these.
		needsCheck = append(needsCheck, db)
	}
	fs.readUnlockAllMsgBlocks()

	for _, db := range needsCheck {
		db.Range(func(dseq uint64) bool {
			fs.removeMsg(dseq, false, true, false)
			return true
		})
	}
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

	// We now allow overrides from a stream being a filestore type and forcing a consumer to be memory store.
	if cfg.MemoryStorage {
		// Create directly here.
		o := &consumerMemStore{ms: fs, cfg: *cfg}
		fs.AddConsumer(o)
		return o, nil
	}

	odir := filepath.Join(fs.fcfg.StoreDir, consumerDir, name)
	if err := os.MkdirAll(odir, defaultDirPerms); err != nil {
		return nil, fmt.Errorf("could not create consumer directory - %v", err)
	}
	csi := &FileConsumerInfo{Name: name, Created: time.Now().UTC(), ConsumerConfig: *cfg}
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
		if ekey, err := os.ReadFile(filepath.Join(odir, JetStreamMetaFileKey)); err == nil {
			if len(ekey) < minBlkKeySize {
				return nil, errBadKeySize
			}
			// Recover key encryption key.
			rb, err := fs.prf([]byte(fs.cfg.Name + tsep + o.name))
			if err != nil {
				return nil, err
			}

			sc := fs.fcfg.Cipher
			kek, err := genEncryptionKey(sc, rb)
			if err != nil {
				return nil, err
			}
			ns := kek.NonceSize()
			nonce := ekey[:ns]
			seed, err := kek.Open(nil, nonce, ekey[ns:], nil)
			if err != nil {
				// We may be here on a cipher conversion, so attempt to convert.
				if err = o.convertCipher(); err != nil {
					return nil, err
				}
			} else {
				o.aek, err = genEncryptionKey(sc, seed)
			}
			if err != nil {
				return nil, err
			}
		}
	}

	// Track if we are creating the directory so that we can clean up if we encounter an error.
	var didCreate bool

	// Write our meta data iff does not exist.
	meta := filepath.Join(odir, JetStreamMetaFile)
	if _, err := os.Stat(meta); err != nil && os.IsNotExist(err) {
		didCreate = true
		csi.Created = time.Now().UTC()
		if err := o.writeConsumerMeta(); err != nil {
			os.RemoveAll(odir)
			return nil, err
		}
	}

	// If we expect to be encrypted check that what we are restoring is not plaintext.
	// This can happen on snapshot restores or conversions.
	if o.prf != nil {
		keyFile := filepath.Join(odir, JetStreamMetaFileKey)
		if _, err := os.Stat(keyFile); err != nil && os.IsNotExist(err) {
			if err := o.writeConsumerMeta(); err != nil {
				if didCreate {
					os.RemoveAll(odir)
				}
				return nil, err
			}
			// Redo the state file as well here if we have one and we can tell it was plaintext.
			if buf, err := os.ReadFile(o.ifn); err == nil {
				if _, err := decodeConsumerState(buf); err == nil {
					state, err := o.encryptState(buf)
					if err != nil {
						return nil, err
					}
					err = fs.writeFileWithOptionalSync(o.ifn, state, defaultFilePerms)
					if err != nil {
						if didCreate {
							os.RemoveAll(odir)
						}
						return nil, err
					}
				}
			}
		}
	}

	// Create channels to control our flush go routine.
	o.fch = make(chan struct{}, 1)
	o.qch = make(chan struct{})
	go o.flushLoop(o.fch, o.qch)

	// Make sure to load in our state from disk if needed.
	o.loadState()

	// Assign to filestore.
	fs.AddConsumer(o)

	return o, nil
}

func (o *consumerFileStore) convertCipher() error {
	fs := o.fs
	odir := filepath.Join(fs.fcfg.StoreDir, consumerDir, o.name)

	ekey, err := os.ReadFile(filepath.Join(odir, JetStreamMetaFileKey))
	if err != nil {
		return err
	}
	if len(ekey) < minBlkKeySize {
		return errBadKeySize
	}
	// Recover key encryption key.
	rb, err := fs.prf([]byte(fs.cfg.Name + tsep + o.name))
	if err != nil {
		return err
	}

	// Do these in reverse since converting.
	sc := fs.fcfg.Cipher
	osc := AES
	if sc == AES {
		osc = ChaCha
	}
	kek, err := genEncryptionKey(osc, rb)
	if err != nil {
		return err
	}
	ns := kek.NonceSize()
	nonce := ekey[:ns]
	seed, err := kek.Open(nil, nonce, ekey[ns:], nil)
	if err != nil {
		return err
	}
	aek, err := genEncryptionKey(osc, seed)
	if err != nil {
		return err
	}
	// Now read in and decode our state using the old cipher.
	buf, err := os.ReadFile(o.ifn)
	if err != nil {
		return err
	}
	buf, err = aek.Open(nil, buf[:ns], buf[ns:], nil)
	if err != nil {
		return err
	}

	// Since we are here we recovered our old state.
	// Now write our meta, which will generate the new keys with the new cipher.
	if err := o.writeConsumerMeta(); err != nil {
		return err
	}

	// Now write out or state with the new cipher.
	return o.writeState(buf)
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
func (o *consumerFileStore) flushLoop(fch, qch chan struct{}) {

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
			if err := o.writeState(buf); err == nil {
				lastWrite = time.Now()
			}
		case <-qch:
			return
		}
	}
}

// SetStarting sets our starting stream sequence.
func (o *consumerFileStore) SetStarting(sseq uint64) error {
	o.mu.Lock()
	o.state.Delivered.Stream = sseq
	buf, err := o.encodeState()
	o.mu.Unlock()
	if err != nil {
		return err
	}
	return o.writeState(buf)
}

// UpdateStarting updates our starting stream sequence.
func (o *consumerFileStore) UpdateStarting(sseq uint64) {
	o.mu.Lock()
	defer o.mu.Unlock()

	if sseq > o.state.Delivered.Stream {
		o.state.Delivered.Stream = sseq
		// For AckNone just update delivered and ackfloor at the same time.
		if o.cfg.AckPolicy == AckNone {
			o.state.AckFloor.Stream = sseq
		}
	}
	// Make sure we flush to disk.
	o.kickFlusher()
}

// HasState returns if this store has a recorded state.
func (o *consumerFileStore) HasState() bool {
	o.mu.Lock()
	// We have a running state, or stored on disk but not yet initialized.
	if o.state.Delivered.Consumer != 0 || o.state.Delivered.Stream != 0 {
		o.mu.Unlock()
		return true
	}
	_, err := os.Stat(o.ifn)
	o.mu.Unlock()
	return err == nil
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
				// Do not update p.Sequence, that should be the original delivery sequence.
				p.Timestamp = ts
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
			if maxdc := uint64(o.cfg.MaxDeliver); maxdc > 0 && dc > maxdc {
				// Make sure to remove from pending.
				delete(o.state.Pending, sseq)
			}
			if o.state.Redelivered == nil {
				o.state.Redelivered = make(map[uint64]uint64)
			}
			// Only update if greater than what we already have.
			if o.state.Redelivered[sseq] < dc-1 {
				o.state.Redelivered[sseq] = dc - 1
			}
		}
	} else {
		// For AckNone just update delivered and ackfloor at the same time.
		if dseq > o.state.Delivered.Consumer {
			o.state.Delivered.Consumer = dseq
			o.state.AckFloor.Consumer = dseq
		}
		if sseq > o.state.Delivered.Stream {
			o.state.Delivered.Stream = sseq
			o.state.AckFloor.Stream = sseq
		}
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

	// On restarts the old leader may get a replay from the raft logs that are old.
	if dseq <= o.state.AckFloor.Consumer {
		return nil
	}

	if len(o.state.Pending) == 0 || o.state.Pending[sseq] == nil {
		delete(o.state.Redelivered, sseq)
		return ErrStoreMsgNotFound
	}

	// Check for AckAll here.
	if o.cfg.AckPolicy == AckAll {
		sgap := sseq - o.state.AckFloor.Stream
		o.state.AckFloor.Consumer = dseq
		o.state.AckFloor.Stream = sseq
		if sgap > uint64(len(o.state.Pending)) {
			for seq := range o.state.Pending {
				if seq <= sseq {
					delete(o.state.Pending, seq)
					delete(o.state.Redelivered, seq)
				}
			}
		} else {
			for seq := sseq; seq > sseq-sgap && len(o.state.Pending) > 0; seq-- {
				delete(o.state.Pending, seq)
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
		if dseq > p.Sequence && p.Sequence > 0 {
			dseq = p.Sequence // Use the original.
		}
	}
	if len(o.state.Pending) == 0 {
		o.state.AckFloor.Consumer = o.state.Delivered.Consumer
		o.state.AckFloor.Stream = o.state.Delivered.Stream
	} else if dseq == o.state.AckFloor.Consumer+1 {
		o.state.AckFloor.Consumer = dseq
		o.state.AckFloor.Stream = sseq

		if o.state.Delivered.Consumer > dseq {
			for ss := sseq + 1; ss <= o.state.Delivered.Stream; ss++ {
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
	// We do these regardless.
	delete(o.state.Redelivered, sseq)

	o.kickFlusher()
	return nil
}

const seqsHdrSize = 6*binary.MaxVarintLen64 + hdrLen

// Encode our consumer state, version 2.
// Lock should be held.

func (o *consumerFileStore) EncodedState() ([]byte, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.encodeState()
}

func (o *consumerFileStore) encodeState() ([]byte, error) {
	// Grab reference to state, but make sure we load in if needed, so do not reference o.state directly.
	state, err := o.stateWithCopyLocked(false)
	if err != nil {
		return nil, err
	}
	return encodeConsumerState(state), nil
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
	defer o.mu.Unlock()

	// Check to see if this is an outdated update.
	if state.Delivered.Consumer < o.state.Delivered.Consumer || state.AckFloor.Stream < o.state.AckFloor.Stream {
		return fmt.Errorf("old update ignored")
	}

	o.state.Delivered = state.Delivered
	o.state.AckFloor = state.AckFloor
	o.state.Pending = pending
	o.state.Redelivered = redelivered

	o.kickFlusher()

	return nil
}

// Will encrypt the state with our asset key. Will be a no-op if encryption not enabled.
// Lock should be held.
func (o *consumerFileStore) encryptState(buf []byte) ([]byte, error) {
	if o.aek == nil {
		return buf, nil
	}
	// TODO(dlc) - Optimize on space usage a bit?
	nonce := make([]byte, o.aek.NonceSize(), o.aek.NonceSize()+len(buf)+o.aek.Overhead())
	if n, err := rand.Read(nonce); err != nil {
		return nil, err
	} else if n != len(nonce) {
		return nil, fmt.Errorf("not enough nonce bytes read (%d != %d)", n, len(nonce))
	}
	return o.aek.Seal(nonce, nonce, buf, nil), nil
}

// Used to limit number of disk IO calls in flight since they could all be blocking an OS thread.
// https://github.com/nats-io/nats-server/issues/2742
var dios chan struct{}

// Used to setup our simplistic counting semaphore using buffered channels.
// golang.org's semaphore seemed a bit heavy.
func init() {
	// Limit ourselves to a sensible number of blocking I/O calls. Range between
	// 4-16 concurrent disk I/Os based on CPU cores, or 50% of cores if greater
	// than 32 cores.
	mp := runtime.GOMAXPROCS(-1)
	nIO := min(16, max(4, mp))
	if mp > 32 {
		// If the system has more than 32 cores then limit dios to 50% of cores.
		nIO = max(16, min(mp, mp/2))
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
		var err error
		if buf, err = o.encryptState(buf); err != nil {
			return err
		}
	}

	o.writing = true
	o.dirty = false
	ifn := o.ifn
	o.mu.Unlock()

	// Lock not held here but we do limit number of outstanding calls that could block OS threads.
	err := o.fs.writeFileWithOptionalSync(ifn, buf, defaultFilePerms)

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
		err = cfs.fs.writeFileWithOptionalSync(keyFile, encrypted, defaultFilePerms)
		if err != nil {
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
		if n, err := rand.Read(nonce); err != nil {
			return err
		} else if n != len(nonce) {
			return fmt.Errorf("not enough nonce bytes read (%d != %d)", n, len(nonce))
		}
		b = cfs.aek.Seal(nonce, nonce, b, nil)
	}

	err = cfs.fs.writeFileWithOptionalSync(meta, b, defaultFilePerms)
	if err != nil {
		return err
	}
	cfs.hh.Reset()
	cfs.hh.Write(b)
	checksum := hex.EncodeToString(cfs.hh.Sum(nil))
	sum := filepath.Join(cfs.odir, JetStreamMetaFileSum)

	err = cfs.fs.writeFileWithOptionalSync(sum, []byte(checksum), defaultFilePerms)
	if err != nil {
		return err
	}
	return nil
}

// Consumer version.
func checkConsumerHeader(hdr []byte) (uint8, error) {
	if len(hdr) < 2 || hdr[0] != magic {
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
	return o.stateWithCopy(true)
}

// This will not copy pending or redelivered, so should only be done under the
// consumer owner's lock.
func (o *consumerFileStore) BorrowState() (*ConsumerState, error) {
	return o.stateWithCopy(false)
}

func (o *consumerFileStore) stateWithCopy(doCopy bool) (*ConsumerState, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.stateWithCopyLocked(doCopy)
}

// Lock should be held.
func (o *consumerFileStore) stateWithCopyLocked(doCopy bool) (*ConsumerState, error) {
	if o.closed {
		return nil, ErrStoreClosed
	}

	state := &ConsumerState{}

	// See if we have a running state or if we need to read in from disk.
	if o.state.Delivered.Consumer != 0 || o.state.Delivered.Stream != 0 {
		state.Delivered = o.state.Delivered
		state.AckFloor = o.state.AckFloor
		if len(o.state.Pending) > 0 {
			if doCopy {
				state.Pending = o.copyPending()
			} else {
				state.Pending = o.state.Pending
			}
		}
		if len(o.state.Redelivered) > 0 {
			if doCopy {
				state.Redelivered = o.copyRedelivered()
			} else {
				state.Redelivered = o.state.Redelivered
			}
		}
		return state, nil
	}

	// Read the state in here from disk..
	<-dios
	buf, err := os.ReadFile(o.ifn)
	dios <- struct{}{}

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
		if doCopy {
			o.state.Pending = make(map[uint64]*Pending, len(state.Pending))
			for seq, p := range state.Pending {
				o.state.Pending[seq] = &Pending{p.Sequence, p.Timestamp}
			}
		} else {
			o.state.Pending = state.Pending
		}
	}
	if len(state.Redelivered) > 0 {
		if doCopy {
			o.state.Redelivered = make(map[uint64]uint64, len(state.Redelivered))
			for seq, dc := range state.Redelivered {
				o.state.Redelivered[seq] = dc
			}
		} else {
			o.state.Redelivered = state.Redelivered
		}
	}

	return state, nil
}

// Lock should be held. Called at startup.
func (o *consumerFileStore) loadState() {
	if _, err := os.Stat(o.ifn); err == nil {
		// This will load our state in from disk.
		o.stateWithCopyLocked(false)
	}
}

// Decode consumer state.
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

	// Protect ourselves against rolling backwards.
	const hbit = 1 << 63
	if state.AckFloor.Stream&hbit != 0 || state.Delivered.Stream&hbit != 0 {
		return nil, errCorruptState
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
			// Check the state machine for corruption, not the value which could be -1.
			if bi == -1 {
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
				if buf, err = o.encryptState(buf); err != nil {
					return err
				}
			}
		}
	}

	o.odir = _EMPTY_
	o.closed = true
	ifn, fs := o.ifn, o.fs
	o.mu.Unlock()

	fs.RemoveConsumer(o)

	if len(buf) > 0 {
		o.waitOnFlusher()
		err = o.fs.writeFileWithOptionalSync(ifn, buf, defaultFilePerms)
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
		fs.RemoveConsumer(o)
	}

	return err
}

func (fs *fileStore) AddConsumer(o ConsumerStore) error {
	fs.cmu.Lock()
	defer fs.cmu.Unlock()
	fs.cfs = append(fs.cfs, o)
	return nil
}

func (fs *fileStore) RemoveConsumer(o ConsumerStore) error {
	fs.cmu.Lock()
	defer fs.cmu.Unlock()
	for i, cfs := range fs.cfs {
		if o == cfs {
			fs.cfs = append(fs.cfs[:i], fs.cfs[i+1:]...)
			break
		}
	}
	return nil
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
	if err := os.WriteFile(meta, b, defaultFilePerms); err != nil {
		return err
	}
	// FIXME(dlc) - Do checksum
	ts.hh.Reset()
	ts.hh.Write(b)
	checksum := hex.EncodeToString(ts.hh.Sum(nil))
	sum := filepath.Join(dir, JetStreamMetaFileSum)
	if err := os.WriteFile(sum, []byte(checksum), defaultFilePerms); err != nil {
		return err
	}
	return nil
}

func (ts *templateFileStore) Delete(t *streamTemplate) error {
	return os.RemoveAll(filepath.Join(ts.dir, t.Name))
}

////////////////////////////////////////////////////////////////////////////////
// Compression
////////////////////////////////////////////////////////////////////////////////

type CompressionInfo struct {
	Algorithm    StoreCompression
	OriginalSize uint64
}

func (c *CompressionInfo) MarshalMetadata() []byte {
	b := make([]byte, 14) // 4 + potentially up to 10 for uint64
	b[0], b[1], b[2] = 'c', 'm', 'p'
	b[3] = byte(c.Algorithm)
	n := binary.PutUvarint(b[4:], c.OriginalSize)
	return b[:4+n]
}

func (c *CompressionInfo) UnmarshalMetadata(b []byte) (int, error) {
	c.Algorithm = NoCompression
	c.OriginalSize = 0
	if len(b) < 5 { // 4 + min 1 for uvarint uint64
		return 0, nil
	}
	if b[0] != 'c' || b[1] != 'm' || b[2] != 'p' {
		return 0, nil
	}
	var n int
	c.Algorithm = StoreCompression(b[3])
	c.OriginalSize, n = binary.Uvarint(b[4:])
	if n <= 0 {
		return 0, fmt.Errorf("metadata incomplete")
	}
	return 4 + n, nil
}

func (alg StoreCompression) Compress(buf []byte) ([]byte, error) {
	if len(buf) < checksumSize {
		return nil, fmt.Errorf("uncompressed buffer is too short")
	}
	bodyLen := int64(len(buf) - checksumSize)
	var output bytes.Buffer
	var writer io.WriteCloser
	switch alg {
	case NoCompression:
		return buf, nil
	case S2Compression:
		writer = s2.NewWriter(&output)
	default:
		return nil, fmt.Errorf("compression algorithm not known")
	}

	input := bytes.NewReader(buf[:bodyLen])
	checksum := buf[bodyLen:]

	// Compress the block content, but don't compress the checksum.
	// We will preserve it at the end of the block as-is.
	if n, err := io.CopyN(writer, input, bodyLen); err != nil {
		return nil, fmt.Errorf("error writing to compression writer: %w", err)
	} else if n != bodyLen {
		return nil, fmt.Errorf("short write on body (%d != %d)", n, bodyLen)
	}
	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("error closing compression writer: %w", err)
	}

	// Now add the checksum back onto the end of the block.
	if n, err := output.Write(checksum); err != nil {
		return nil, fmt.Errorf("error writing checksum: %w", err)
	} else if n != checksumSize {
		return nil, fmt.Errorf("short write on checksum (%d != %d)", n, checksumSize)
	}

	return output.Bytes(), nil
}

func (alg StoreCompression) Decompress(buf []byte) ([]byte, error) {
	if len(buf) < checksumSize {
		return nil, fmt.Errorf("compressed buffer is too short")
	}
	bodyLen := int64(len(buf) - checksumSize)
	input := bytes.NewReader(buf[:bodyLen])

	var reader io.ReadCloser
	switch alg {
	case NoCompression:
		return buf, nil
	case S2Compression:
		reader = io.NopCloser(s2.NewReader(input))
	default:
		return nil, fmt.Errorf("compression algorithm not known")
	}

	// Decompress the block content. The checksum isn't compressed so
	// we can preserve it from the end of the block as-is.
	checksum := buf[bodyLen:]
	output, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("error reading compression reader: %w", err)
	}
	output = append(output, checksum...)

	return output, reader.Close()
}

// writeFileWithOptionalSync is equivalent to os.WriteFile() but optionally
// sets O_SYNC on the open file if SyncAlways is set. The dios semaphore is
// handled automatically by this function, so don't wrap calls to it in dios.
func (fs *fileStore) writeFileWithOptionalSync(name string, data []byte, perm fs.FileMode) error {
	if fs.fcfg.SyncAlways {
		return writeFileWithSync(name, data, perm)
	}
	<-dios
	defer func() {
		dios <- struct{}{}
	}()
	return os.WriteFile(name, data, perm)
}

func writeFileWithSync(name string, data []byte, perm fs.FileMode) error {
	<-dios
	defer func() {
		dios <- struct{}{}
	}()
	flags := os.O_WRONLY | os.O_CREATE | os.O_TRUNC | os.O_SYNC
	f, err := os.OpenFile(name, flags, perm)
	if err != nil {
		return err
	}
	if _, err = f.Write(data); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}
