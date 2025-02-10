// Copyright 2023-2024 The NATS Authors
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
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/klauspost/compress/s2"
	"golang.org/x/crypto/ocsp"

	"github.com/nats-io/nats-server/v2/server/certidp"
)

const (
	OCSPResponseCacheDefaultDir            = "_rc_"
	OCSPResponseCacheDefaultFilename       = "cache.json"
	OCSPResponseCacheDefaultTempFilePrefix = "ocsprc-*"
	OCSPResponseCacheMinimumSaveInterval   = 1 * time.Second
	OCSPResponseCacheDefaultSaveInterval   = 5 * time.Minute
)

type OCSPResponseCacheType int

const (
	NONE OCSPResponseCacheType = iota + 1
	LOCAL
)

var OCSPResponseCacheTypeMap = map[string]OCSPResponseCacheType{
	"none":  NONE,
	"local": LOCAL,
}

type OCSPResponseCacheConfig struct {
	Type            OCSPResponseCacheType
	LocalStore      string
	PreserveRevoked bool
	SaveInterval    float64
}

func NewOCSPResponseCacheConfig() *OCSPResponseCacheConfig {
	return &OCSPResponseCacheConfig{
		Type:            LOCAL,
		LocalStore:      OCSPResponseCacheDefaultDir,
		PreserveRevoked: false,
		SaveInterval:    OCSPResponseCacheDefaultSaveInterval.Seconds(),
	}
}

type OCSPResponseCacheStats struct {
	Responses int64 `json:"size"`
	Hits      int64 `json:"hits"`
	Misses    int64 `json:"misses"`
	Revokes   int64 `json:"revokes"`
	Goods     int64 `json:"goods"`
	Unknowns  int64 `json:"unknowns"`
}

type OCSPResponseCacheItem struct {
	Subject     string                  `json:"subject,omitempty"`
	CachedAt    time.Time               `json:"cached_at"`
	RespStatus  certidp.StatusAssertion `json:"resp_status"`
	RespExpires time.Time               `json:"resp_expires,omitempty"`
	Resp        []byte                  `json:"resp"`
}

type OCSPResponseCache interface {
	Put(key string, resp *ocsp.Response, subj string, log *certidp.Log)
	Get(key string, log *certidp.Log) []byte
	Delete(key string, miss bool, log *certidp.Log)
	Type() string
	Start(s *Server)
	Stop(s *Server)
	Online() bool
	Config() *OCSPResponseCacheConfig
	Stats() *OCSPResponseCacheStats
}

// NoOpCache is a no-op implementation of OCSPResponseCache
type NoOpCache struct {
	config *OCSPResponseCacheConfig
	stats  *OCSPResponseCacheStats
	online bool
	mu     *sync.RWMutex
}

func (c *NoOpCache) Put(_ string, _ *ocsp.Response, _ string, _ *certidp.Log) {}

func (c *NoOpCache) Get(_ string, _ *certidp.Log) []byte {
	return nil
}

func (c *NoOpCache) Delete(_ string, _ bool, _ *certidp.Log) {}

func (c *NoOpCache) Start(_ *Server) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.stats = &OCSPResponseCacheStats{}
	c.online = true
}

func (c *NoOpCache) Stop(_ *Server) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.online = false
}

func (c *NoOpCache) Online() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.online
}

func (c *NoOpCache) Type() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return "none"
}

func (c *NoOpCache) Config() *OCSPResponseCacheConfig {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.config
}

func (c *NoOpCache) Stats() *OCSPResponseCacheStats {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.stats
}

// LocalCache is a local file implementation of OCSPResponseCache
type LocalCache struct {
	config       *OCSPResponseCacheConfig
	stats        *OCSPResponseCacheStats
	online       bool
	cache        map[string]OCSPResponseCacheItem
	mu           *sync.RWMutex
	saveInterval time.Duration
	dirty        bool
	timer        *time.Timer
}

// Put captures a CA OCSP response to the OCSP peer cache indexed by response fingerprint (a hash)
func (c *LocalCache) Put(key string, caResp *ocsp.Response, subj string, log *certidp.Log) {
	c.mu.RLock()
	if !c.online || caResp == nil || key == "" {
		c.mu.RUnlock()
		return
	}
	c.mu.RUnlock()
	log.Debugf(certidp.DbgCachingResponse, subj, key)
	rawC, err := c.Compress(caResp.Raw)
	if err != nil {
		log.Errorf(certidp.ErrResponseCompressFail, key, err)
		return
	}
	log.Debugf(certidp.DbgAchievedCompression, float64(len(rawC))/float64(len(caResp.Raw)))
	c.mu.Lock()
	defer c.mu.Unlock()
	// check if we are replacing and do stats
	item, ok := c.cache[key]
	if ok {
		c.adjustStats(-1, item.RespStatus)
	}
	item = OCSPResponseCacheItem{
		Subject:     subj,
		CachedAt:    time.Now().UTC().Round(time.Second),
		RespStatus:  certidp.StatusAssertionIntToVal[caResp.Status],
		RespExpires: caResp.NextUpdate,
		Resp:        rawC,
	}
	c.cache[key] = item
	c.adjustStats(1, item.RespStatus)
	c.dirty = true
}

// Get returns a CA OCSP response from the OCSP peer cache matching the response fingerprint (a hash)
func (c *LocalCache) Get(key string, log *certidp.Log) []byte {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if !c.online || key == "" {
		return nil
	}
	val, ok := c.cache[key]
	if ok {
		atomic.AddInt64(&c.stats.Hits, 1)
		log.Debugf(certidp.DbgCacheHit, key)
	} else {
		atomic.AddInt64(&c.stats.Misses, 1)
		log.Debugf(certidp.DbgCacheMiss, key)
		return nil
	}
	resp, err := c.Decompress(val.Resp)
	if err != nil {
		log.Errorf(certidp.ErrResponseDecompressFail, key, err)
		return nil
	}
	return resp
}

func (c *LocalCache) adjustStatsHitToMiss() {
	atomic.AddInt64(&c.stats.Misses, 1)
	atomic.AddInt64(&c.stats.Hits, -1)
}

func (c *LocalCache) adjustStats(delta int64, rs certidp.StatusAssertion) {
	if delta == 0 {
		return
	}
	atomic.AddInt64(&c.stats.Responses, delta)
	switch rs {
	case ocsp.Good:
		atomic.AddInt64(&c.stats.Goods, delta)
	case ocsp.Revoked:
		atomic.AddInt64(&c.stats.Revokes, delta)
	case ocsp.Unknown:
		atomic.AddInt64(&c.stats.Unknowns, delta)
	}
}

// Delete removes a CA OCSP response from the OCSP peer cache matching the response fingerprint (a hash)
func (c *LocalCache) Delete(key string, wasMiss bool, log *certidp.Log) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.online || key == "" || c.config == nil {
		return
	}
	item, ok := c.cache[key]
	if !ok {
		return
	}
	if item.RespStatus == ocsp.Revoked && c.config.PreserveRevoked {
		log.Debugf(certidp.DbgPreservedRevocation, key)
		if wasMiss {
			c.adjustStatsHitToMiss()
		}
		return
	}
	log.Debugf(certidp.DbgDeletingCacheResponse, key)
	delete(c.cache, key)
	c.adjustStats(-1, item.RespStatus)
	if wasMiss {
		c.adjustStatsHitToMiss()
	}
	c.dirty = true
}

// Start initializes the configured OCSP peer cache, loads a saved cache from disk (if present), and initializes runtime statistics
func (c *LocalCache) Start(s *Server) {
	s.Debugf(certidp.DbgStartingCache)
	c.loadCache(s)
	c.initStats()
	c.mu.Lock()
	c.online = true
	c.mu.Unlock()
}

func (c *LocalCache) Stop(s *Server) {
	c.mu.Lock()
	s.Debugf(certidp.DbgStoppingCache)
	c.online = false
	c.timer.Stop()
	c.mu.Unlock()
	c.saveCache(s)
}

func (c *LocalCache) Online() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.online
}

func (c *LocalCache) Type() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return "local"
}

func (c *LocalCache) Config() *OCSPResponseCacheConfig {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.config
}

func (c *LocalCache) Stats() *OCSPResponseCacheStats {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.stats == nil {
		return nil
	}
	stats := OCSPResponseCacheStats{
		Responses: c.stats.Responses,
		Hits:      c.stats.Hits,
		Misses:    c.stats.Misses,
		Revokes:   c.stats.Revokes,
		Goods:     c.stats.Goods,
		Unknowns:  c.stats.Unknowns,
	}
	return &stats
}

func (c *LocalCache) initStats() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.stats = &OCSPResponseCacheStats{}
	c.stats.Hits = 0
	c.stats.Misses = 0
	c.stats.Responses = int64(len(c.cache))
	for _, resp := range c.cache {
		switch resp.RespStatus {
		case ocsp.Good:
			c.stats.Goods++
		case ocsp.Revoked:
			c.stats.Revokes++
		case ocsp.Unknown:
			c.stats.Unknowns++
		}
	}
}

func (c *LocalCache) Compress(buf []byte) ([]byte, error) {
	bodyLen := int64(len(buf))
	var output bytes.Buffer
	writer := s2.NewWriter(&output)
	input := bytes.NewReader(buf[:bodyLen])
	if n, err := io.CopyN(writer, input, bodyLen); err != nil {
		return nil, fmt.Errorf(certidp.ErrCannotWriteCompressed, err)
	} else if n != bodyLen {
		return nil, fmt.Errorf(certidp.ErrTruncatedWrite, n, bodyLen)
	}
	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf(certidp.ErrCannotCloseWriter, err)
	}
	return output.Bytes(), nil
}

func (c *LocalCache) Decompress(buf []byte) ([]byte, error) {
	bodyLen := int64(len(buf))
	input := bytes.NewReader(buf[:bodyLen])
	reader := io.NopCloser(s2.NewReader(input))
	output, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf(certidp.ErrCannotReadCompressed, err)
	}
	return output, reader.Close()
}

func (c *LocalCache) loadCache(s *Server) {
	d := s.opts.OCSPCacheConfig.LocalStore
	if d == _EMPTY_ {
		d = OCSPResponseCacheDefaultDir
	}
	f := OCSPResponseCacheDefaultFilename
	store, err := filepath.Abs(path.Join(d, f))
	if err != nil {
		s.Errorf(certidp.ErrLoadCacheFail, err)
		return
	}
	s.Debugf(certidp.DbgLoadingCache, store)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cache = make(map[string]OCSPResponseCacheItem)
	dat, err := os.ReadFile(store)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			s.Debugf(certidp.DbgNoCacheFound)
		} else {
			s.Warnf(certidp.ErrLoadCacheFail, err)
		}
		return
	}
	err = json.Unmarshal(dat, &c.cache)
	if err != nil {
		// make sure clean cache
		c.cache = make(map[string]OCSPResponseCacheItem)
		s.Warnf(certidp.ErrLoadCacheFail, err)
		c.dirty = true
		return
	}
	c.dirty = false
}

func (c *LocalCache) saveCache(s *Server) {
	c.mu.RLock()
	dirty := c.dirty
	c.mu.RUnlock()
	if !dirty {
		return
	}
	s.Debugf(certidp.DbgCacheDirtySave)
	var d string
	if c.config.LocalStore != _EMPTY_ {
		d = c.config.LocalStore
	} else {
		d = OCSPResponseCacheDefaultDir
	}
	f := OCSPResponseCacheDefaultFilename
	store, err := filepath.Abs(path.Join(d, f))
	if err != nil {
		s.Errorf(certidp.ErrSaveCacheFail, err)
		return
	}
	s.Debugf(certidp.DbgSavingCache, store)
	if _, err := os.Stat(d); os.IsNotExist(err) {
		err = os.Mkdir(d, defaultDirPerms)
		if err != nil {
			s.Errorf(certidp.ErrSaveCacheFail, err)
			return
		}
	}
	tmp, err := os.CreateTemp(d, OCSPResponseCacheDefaultTempFilePrefix)
	if err != nil {
		s.Errorf(certidp.ErrSaveCacheFail, err)
		return
	}
	defer func() {
		tmp.Close()
		os.Remove(tmp.Name())
	}() // clean up any temp files

	// RW lock here because we're going to snapshot the cache to disk and mark as clean if successful
	c.mu.Lock()
	defer c.mu.Unlock()
	dat, err := json.MarshalIndent(c.cache, "", " ")
	if err != nil {
		s.Errorf(certidp.ErrSaveCacheFail, err)
		return
	}
	cacheSize, err := tmp.Write(dat)
	if err != nil {
		s.Errorf(certidp.ErrSaveCacheFail, err)
		return
	}
	err = tmp.Sync()
	if err != nil {
		s.Errorf(certidp.ErrSaveCacheFail, err)
		return
	}
	err = tmp.Close()
	if err != nil {
		s.Errorf(certidp.ErrSaveCacheFail, err)
		return
	}
	// do the final swap and overwrite any old saved peer cache
	err = os.Rename(tmp.Name(), store)
	if err != nil {
		s.Errorf(certidp.ErrSaveCacheFail, err)
		return
	}
	c.dirty = false
	s.Debugf(certidp.DbgCacheSaved, cacheSize)
}

var OCSPResponseCacheUsage = `
You may enable OCSP peer response cacheing at server configuration root level:

(If no TLS blocks are configured with OCSP peer verification, ocsp_cache is ignored.)

    ...
    # short form enables with defaults
    ocsp_cache: true

    # if false or undefined and one or more TLS blocks are configured with OCSP peer verification, "none" is implied

    # long form includes settable options
    ocsp_cache {

        # Cache type <none, local> (default local)
        type: local

        # Cache file directory for local-type cache (default _rc_ in current working directory)
        local_store: "_rc_"

        # Ignore cache deletes if cached OCSP response is Revoked status (default false)
        preserve_revoked: false

        # For local store, interval to save in-memory cache to disk in seconds (default 300 seconds, minimum 1 second)
        save_interval: 300
    }
    ...

Note: Cache of server's own OCSP response (staple) is enabled using the 'ocsp' configuration option.
`

func (s *Server) initOCSPResponseCache() {
	// No mTLS OCSP or Leaf OCSP enablements, so no need to init cache
	s.mu.RLock()
	if !s.ocspPeerVerify {
		s.mu.RUnlock()
		return
	}
	s.mu.RUnlock()
	so := s.getOpts()
	if so.OCSPCacheConfig == nil {
		so.OCSPCacheConfig = NewOCSPResponseCacheConfig()
	}
	var cc = so.OCSPCacheConfig
	s.mu.Lock()
	defer s.mu.Unlock()
	switch cc.Type {
	case NONE:
		s.ocsprc = &NoOpCache{config: cc, online: true, mu: &sync.RWMutex{}}
	case LOCAL:
		c := &LocalCache{
			config: cc,
			online: false,
			cache:  make(map[string]OCSPResponseCacheItem),
			mu:     &sync.RWMutex{},
			dirty:  false,
		}
		c.saveInterval = time.Duration(cc.SaveInterval) * time.Second
		c.timer = time.AfterFunc(c.saveInterval, func() {
			s.Debugf(certidp.DbgCacheSaveTimerExpired)
			c.saveCache(s)
			c.timer.Reset(c.saveInterval)
		})
		s.ocsprc = c
	default:
		s.Fatalf(certidp.ErrBadCacheTypeConfig, cc.Type)
	}
}

func (s *Server) startOCSPResponseCache() {
	// No mTLS OCSP or Leaf OCSP enablements, so no need to start cache
	s.mu.RLock()
	if !s.ocspPeerVerify || s.ocsprc == nil {
		s.mu.RUnlock()
		return
	}
	s.mu.RUnlock()

	// Could be heavier operation depending on cache implementation
	s.ocsprc.Start(s)
	if s.ocsprc.Online() {
		s.Noticef(certidp.MsgCacheOnline, s.ocsprc.Type())
	} else {
		s.Noticef(certidp.MsgCacheOffline, s.ocsprc.Type())
	}
}

func (s *Server) stopOCSPResponseCache() {
	s.mu.RLock()
	if s.ocsprc == nil {
		s.mu.RUnlock()
		return
	}
	s.mu.RUnlock()
	s.ocsprc.Stop(s)
}

func parseOCSPResponseCache(v any) (pcfg *OCSPResponseCacheConfig, retError error) {
	var lt token
	defer convertPanicToError(&lt, &retError)
	tk, v := unwrapValue(v, &lt)
	cm, ok := v.(map[string]any)
	if !ok {
		return nil, &configErr{tk, fmt.Sprintf(certidp.ErrIllegalCacheOptsConfig, v)}
	}
	pcfg = NewOCSPResponseCacheConfig()
	retError = nil
	for mk, mv := range cm {
		// Again, unwrap token value if line check is required.
		tk, mv = unwrapValue(mv, &lt)
		switch strings.ToLower(mk) {
		case "type":
			cache, ok := mv.(string)
			if !ok {
				return nil, &configErr{tk, fmt.Sprintf(certidp.ErrParsingCacheOptFieldGeneric, mk)}
			}
			cacheType, exists := OCSPResponseCacheTypeMap[strings.ToLower(cache)]
			if !exists {
				return nil, &configErr{tk, fmt.Sprintf(certidp.ErrUnknownCacheType, cache)}
			}
			pcfg.Type = cacheType
		case "local_store":
			store, ok := mv.(string)
			if !ok {
				return nil, &configErr{tk, fmt.Sprintf(certidp.ErrParsingCacheOptFieldGeneric, mk)}
			}
			pcfg.LocalStore = store
		case "preserve_revoked":
			preserve, ok := mv.(bool)
			if !ok {
				return nil, &configErr{tk, fmt.Sprintf(certidp.ErrParsingCacheOptFieldGeneric, mk)}
			}
			pcfg.PreserveRevoked = preserve
		case "save_interval":
			at := float64(0)
			switch mv := mv.(type) {
			case int64:
				at = float64(mv)
			case float64:
				at = mv
			case string:
				d, err := time.ParseDuration(mv)
				if err != nil {
					return nil, &configErr{tk, fmt.Sprintf(certidp.ErrParsingPeerOptFieldTypeConversion, err)}
				}
				at = d.Seconds()
			default:
				return nil, &configErr{tk, fmt.Sprintf(certidp.ErrParsingCacheOptFieldTypeConversion, "unexpected type")}
			}
			si := time.Duration(at) * time.Second
			if si < OCSPResponseCacheMinimumSaveInterval {
				si = OCSPResponseCacheMinimumSaveInterval
			}
			pcfg.SaveInterval = si.Seconds()
		default:
			return nil, &configErr{tk, fmt.Sprintf(certidp.ErrParsingCacheOptFieldGeneric, mk)}
		}
	}
	return pcfg, nil
}
