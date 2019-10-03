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
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/nats-io/nats-server/v2/server/sysmem"
)

// JetStreamConfig determines this server's configuration.
// MaxMemory and MaxStore are in bytes.
type JetStreamConfig struct {
	MaxMemory int64
	MaxStore  int64
	StoreDir  string
}

// This is for internal accounting for JetStream for this server.
type jetStream struct {
	mu       sync.RWMutex
	srv      *Server
	config   JetStreamConfig
	accounts map[*Account]*jsAccount
	// TODO(dlc) - need to track
	// memUsage  int64
	// diskUsage int64
}

// TODO(dlc) - decide on what these look like for single vs cluster vs supercluster.
// TODO(dlc) - need to track and rollup against server limits, etc.
type JetStreamAccountLimits struct {
	MaxMemory  int64
	MaxStore   int64
	MaxMsgSets int
}

const (
	// OK response
	JsOK = "+OK"

	// JsEnabled allows a user to dynamically check if JetStream is enabled
	// with a simple request.
	// Will return +OK on success and will timeout if not imported.
	JsEnabledExport = "$JS.*.ENABLED"
	JsEnabled       = "$JS.ENABLED"

	// jsInfo is for obtaining general information about JetStream for this account.
	// Will return JSON response.
	JsInfoExport = "$JS.*.INFO"
	JsInfo       = "$JS.INFO"

	// JsCreatObservable is the endpoint to create observers for a message set.
	// Will return +OK on success and -ERR on failure.
	JsCreateObservableExport = "$JS.*.OBSERVABLE.CREATE"
	JsCreateObservable       = "$JS.OBSERVABLE.CREATE"

	// JsCreatMsgSet is the endpoint to create message sets.
	// Will return +OK on success and -ERR on failure.
	JsCreateMsgSetExport = "$JS.*.MSGSET.CREATE"
	JsCreateMsgSet       = "$JS.MSGSET.CREATE"

	// JsMsgSetInfo is for obtaining general information about a named message set.
	// Will return JSON response.
	JsMsgSetInfoExport = "$JS.*.MSGSET.INFO"
	JsMsgSetInfo       = "$JS.MSGSET.INFO"

	// JsAckPre is the prefix for the ack stream coming back to observable.
	JsAckPre = "$JS.A"
)

// For easier handling of exports and imports.
var allJsExports = []string{JsEnabledExport, JsInfoExport, JsCreateObservableExport, JsCreateMsgSetExport, JsMsgSetInfoExport}

// This represents a jetstream  enabled account.
// Worth noting that we include the js ptr, this is because
// in general we want to be very efficient when receiving messages on
// and internal sub for a msgSet, so we will direct link to the msgSet
// and walk backwards as needed vs multiple hash lookups and locks, etc.
type jsAccount struct {
	mu      sync.Mutex
	js      *jetStream
	account *Account
	limits  JetStreamAccountLimits
	msgSets map[string]*MsgSet
}

// EnableJetStream will enable JetStream support on this server with
// the given configuration. A nil configuration will dynamically choose
// the limits and temporary file storage directory.
// If this server is part of a cluster, a system account will need to be defined.
// This allows the server to send and receive messages.
func (s *Server) EnableJetStream(config *JetStreamConfig) error {
	s.mu.Lock()
	if !s.standAloneMode() {
		s.mu.Unlock()
		return fmt.Errorf("jetstream restricted to single server mode")
	}
	if s.js != nil {
		s.mu.Unlock()
		return fmt.Errorf("jetstream already enabled")
	}
	s.Noticef("Starting jetstream")
	if config == nil {
		s.Debugf("Jetstream creating dynamic configuration - 75%% system memory, %s disk", friendlyBytes(JetStreamMaxStoreDefault))
		config = s.dynJetStreamConfig()
	}
	s.js = &jetStream{srv: s, config: *config, accounts: make(map[*Account]*jsAccount)}
	s.mu.Unlock()

	if stat, err := os.Stat(config.StoreDir); os.IsNotExist(err) {
		if err := os.MkdirAll(config.StoreDir, 0755); err != nil {
			return fmt.Errorf("could not create storage directory - %v", err)
		}
	} else {
		// Make sure its a directory and that we can write to it.
		if stat == nil || !stat.IsDir() {
			return fmt.Errorf("storage directory is not a directory")
		}
		tmpfile, err := ioutil.TempFile(config.StoreDir, "_test_")
		if err != nil {
			return fmt.Errorf("storage directory is not writable")
		}
		os.Remove(tmpfile.Name())
		// TODO(dlc) - Recover state
	}

	// JetStream is an internal service so we need to make sure we have a system account.
	// This system account will export the JetStream service endpoints.
	if sacc := s.SystemAccount(); sacc == nil {
		s.SetDefaultSystemAccount()
	}

	// Setup our internal subscriptions.
	if _, err := s.sysSubscribe(JsEnabledExport, s.isJsEnabledRequest); err != nil {
		return fmt.Errorf("Error setting up internal jetstream subscriptions: %v", err)
	}

	s.Noticef("----------- jetstream config -----------")
	s.Noticef("  MaxMemory:  %s", friendlyBytes(config.MaxMemory))
	s.Noticef("  MaxStore:   %s", friendlyBytes(config.MaxStore))
	s.Noticef("  StoreDir:   %q", config.StoreDir)

	// If not in operator mode, setup our internal system exports if needed.
	// If we are in operator mode, the system account will need to have them
	// added externally.
	if !s.inOperatorMode() {
		sacc := s.SystemAccount()
		// FIXME(dlc) - Should we lock these down?
		s.Debugf("  Exports:")
		for _, export := range allJsExports {
			s.Debugf("     %s", export)
			if err := sacc.AddServiceExport(export, nil); err != nil {
				return fmt.Errorf("Error setting up jetstream service exports: %v", err)
			}
		}
	}
	s.Noticef("----------------------------------------")

	// If we have no configured accounts setup then setup imports on global account.
	if s.globalAccountOnly() && !s.inOperatorMode() {
		if err := s.JetStreamEnableAccount(s.GlobalAccount(), JetStreamAccountLimitsNoLimits); err != nil {
			return fmt.Errorf("Error enabling jetstream on the global account")
		}
	}

	return nil
}

// JetStreamEnabled reports if jetstream is enabled.
func (s *Server) JetStreamEnabled() bool {
	s.mu.Lock()
	enabled := s.js != nil
	s.mu.Unlock()
	return enabled
}

// JetStreamConfig will return the current config. Useful if the system
// created a dynamic configuration. A copy is returned.
func (s *Server) JetStreamConfig() *JetStreamConfig {
	var c *JetStreamConfig
	s.mu.Lock()
	if s.js != nil {
		copy := s.js.config
		c = &(copy)
	}
	s.mu.Unlock()
	return c
}

// JetStreamAccountLimitsNoLimits defines no limits for the given account.
var JetStreamAccountLimitsNoLimits = &JetStreamAccountLimits{-1, -1, -1}

// JetStreamEnableAccount enables jetstream capabilties for the given account with the defined limits.
func (s *Server) JetStreamEnableAccount(a *Account, limits *JetStreamAccountLimits) error {
	js := s.getJetStream()
	if js == nil {
		return fmt.Errorf("jetstream not enabled")
	}
	if a, err := s.LookupAccount(a.Name); err != nil || a == nil {
		return fmt.Errorf("jetstream unknown account")
	}

	// TODO(dlc) - Should we have accounts marked to allow?
	// TODO(dlc) - Should we check and warn if these limits are above the server? Maybe only if single server?

	js.mu.Lock()
	if _, ok := js.accounts[a]; ok {
		js.mu.Unlock()
		return fmt.Errorf("jetstream already enabled for account")
	}
	jsa := &jsAccount{js: js, account: a, limits: *limits, msgSets: make(map[string]*MsgSet)}
	js.accounts[a] = jsa
	js.mu.Unlock()

	// If we are not in operator mode we should create the proper imports here.
	if !s.inOperatorMode() {
		sys := s.SystemAccount()
		for _, export := range allJsExports {
			importTo := strings.Replace(export, "*", a.Name, -1)
			importFrom := strings.Replace(export, ".*.", tsep, -1)
			//	fmt.Printf("export is %q %q \n", importTo, importFrom)
			if err := a.AddServiceImport(sys, importFrom, importTo); err != nil {
				return fmt.Errorf("Error setting up jetstream service imports for global account: %v", err)
			}
		}
	}

	s.Debugf("Enabled jetstream for account: %q", a.Name)
	return nil
}

func (s *Server) JetStreamDisableAccount(a *Account) error {
	js := s.getJetStream()
	if js == nil {
		return fmt.Errorf("jetstream not enabled")
	}

	js.mu.Lock()
	defer js.mu.Unlock()

	if _, ok := js.accounts[a]; !ok {
		return fmt.Errorf("jetstream not enabled for account")
	}
	delete(js.accounts, a)

	return nil
}

// JetStreamEnabled is a helper to determine if jetstream is enabled for an account.
func (a *Account) JetStreamEnabled() bool {
	if a == nil {
		return false
	}
	a.mu.RLock()
	s := a.srv
	a.mu.RUnlock()
	if s == nil {
		return false
	}
	js := s.getJetStream()
	if js == nil {
		return false
	}
	js.mu.Lock()
	_, ok := js.accounts[a]
	js.mu.Unlock()

	return ok
}

// JetStreamNumAccounts returns the number of enabled accounts this server is tracking.
func (s *Server) JetStreamNumAccounts() int {
	js := s.getJetStream()
	if js == nil {
		return 0
	}
	js.mu.Lock()
	defer js.mu.Unlock()
	return len(js.accounts)
}

func (s *Server) getJetStream() *jetStream {
	s.mu.Lock()
	js := s.js
	s.mu.Unlock()
	return js
}

func (js *jetStream) lookupAccount(a *Account) *jsAccount {
	js.mu.RLock()
	jsa := js.accounts[a]
	js.mu.RUnlock()
	return jsa
}

// Request to check if jetstream is enabled.
func (s *Server) isJsEnabledRequest(sub *subscription, c *client, subject, reply string, msg []byte) {
	if c != nil && c.acc != nil && c.acc.JetStreamEnabled() {
		s.sendInternalAccountMsg(c.acc, reply, JsOK)
	}
}

const (
	// JetStreamStoreDir is the prefix we use.
	JetStreamStoreDir = "nats-jetstream"
	// JetStreamMaxStoreDefault is the default disk storage limit. 1TB
	JetStreamMaxStoreDefault = 1024 * 1024 * 1024 * 1024
	// JetStreamMaxMemDefault is only used when we can't determine system memory. 256MB
	JetStreamMaxMemDefault = 1024 * 1024 * 256
)

// Dynamically create a config with a tmp based directory (repeatable) and 75% of system memory.
func (s *Server) dynJetStreamConfig() *JetStreamConfig {
	jsc := &JetStreamConfig{}
	jsc.StoreDir = filepath.Join(os.TempDir(), JetStreamStoreDir)
	jsc.MaxStore = JetStreamMaxStoreDefault
	// Estimate to 75% of total memory if we can determine system memory.
	if sysMem := sysmem.Memory(); sysMem > 0 {
		jsc.MaxMemory = sysMem / 4 * 3
	} else {
		jsc.MaxMemory = JetStreamMaxMemDefault
	}
	return jsc
}

// friendlyBytes returns a string with the given bytes int64
// represented as a size, such as 1KB, 10MB, etc...
func friendlyBytes(bytes int64) string {
	fbytes := float64(bytes)
	base := 1024
	pre := []string{"K", "M", "G", "T", "P", "E"}
	if fbytes < float64(base) {
		return fmt.Sprintf("%v B", fbytes)
	}
	exp := int(math.Log(fbytes) / math.Log(float64(base)))
	index := exp - 1
	return fmt.Sprintf("%.2f %sB", fbytes/math.Pow(float64(base), float64(exp)), pre[index])
}
