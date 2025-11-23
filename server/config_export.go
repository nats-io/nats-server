package server

import (
	"slices"
	"strings"
)

type AccountJetStreamLimitsConfig struct {
	Enabled               bool   `json:"enabled"`
	MaxMemory             int64  `json:"max_memory"`
	MaxStorage            int64  `json:"max_storage"`
	MaxStreams            int    `json:"max_streams"`
	MaxConsumers          int    `json:"max_consumers"`
	MaxBytesRequired      bool   `json:"max_bytes_required"`
	MemoryMaxStreamBytes  int64  `json:"memory_max_stream_bytes"`
	StorageMaxStreamBytes int64  `json:"storage_max_stream_bytes"`
	MaxAckPending         int    `json:"max_ack_pending"`
	ClusterTraffic        string `json:"cluster_traffic"`
}

type AccountImportConfig struct {
	Account string `json:"account,omitempty"`

	Stream string `json:"stream,omitempty"`
	Prefix string `json:"prefix,omitempty"`

	Service string `json:"service,omitempty"`
	To      string `json:"to,omitempty"`

	AllowTrace bool `json:"allow_trace,omitempty"`
}

type AccountExportConfig struct {
	Stream string `json:"stream,omitempty"`

	Service      string `json:"service,omitempty"`
	ResponseType string `json:"response_type,omitempty"`

	Accounts []string `json:"accounts,omitempty"`

	AllowTrace bool `json:"allow_trace,omitempty"`
}

type AccountMappingConfig struct {
	Destination string `json:"destination,omitempty"`
	Weight      uint8  `json:"weight,omitempty"`
	Cluster     string `json:"cluster,omitempty"`
}

type AccountLimitsConfig struct {
	MaxPayload int32 `json:"max_payload,omitempty"`
	MaxSubs    int32 `json:"max_subs,omitempty"`
	MaxConns   int32 `json:"max_conns,omitempty"`
	MaxLeafs   int32 `json:"max_leafs,omitempty"`
}

type AccountUserConfig struct {
	Permissions            *Permissions `json:"permissions,omitempty"`
	AllowedConnectionTypes []string     `json:"allowed_connection_types,omitempty"`
}

type AccountMsgTraceConfig struct {
	Destination string `json:"destination"`
	Sampling    int    `json:"sampling,omitempty"`
}

type AccountConfig struct {
	IsSystemAccount bool                               `json:"is_system_account,omitempty"`
	Permissions     *Permissions                       `json:"permissions,omitempty"`
	JetStream       *AccountJetStreamLimitsConfig      `json:"jetstream,omitempty"`
	Users           map[string]*AccountUserConfig      `json:"users,omitempty"`
	Imports         []*AccountImportConfig             `json:"imports,omitempty"`
	Exports         []*AccountExportConfig             `json:"exports,omitempty"`
	Mappings        map[string][]*AccountMappingConfig `json:"mappings,omitempty"`
	Limits          *AccountLimitsConfig               `json:"limits,omitempty"`
	MsgTrace        *AccountMsgTraceConfig             `json:"msg_trace,omitempty"`
}

type Config struct {
	Accounts map[string]*AccountConfig `json:"accounts,omitempty"`
}

var responseTypeMap = map[ServiceRespType]string{
	Singleton: "singleton",
	Streamed:  "stream",
	Chunked:   "chunked",
}

func exportLimits(l *limits) *AccountLimitsConfig {
	if l == nil {
		return nil
	}
	el := AccountLimitsConfig{}

	var keep bool
	if mpay := l.mpay; mpay >= 0 {
		keep = true
		el.MaxPayload = mpay
	}
	if msubs := l.msubs; msubs >= 0 {
		keep = true
		el.MaxSubs = msubs
	}
	if mconns := l.mconns; mconns >= 0 {
		keep = true
		el.MaxConns = mconns
	}
	if mleafs := l.mleafs; mleafs >= 0 {
		keep = true
		el.MaxLeafs = mleafs
	}

	if keep {
		return &el
	}
	return nil
}

func exportMappings(amaps []*mapping) map[string][]*AccountMappingConfig {
	maps := make(map[string][]*AccountMappingConfig)

	for _, m := range amaps {
		var dests []*AccountMappingConfig
		// This will be in order of the lowest weight first. These are also
		// relative to the next dest.
		for i, d := range m.dests {
			// Skip if the source and destination are the same.
			if d.tr.src == d.tr.dest && d.weight == 100 {
				continue
			}

			var weight uint8
			if i > 0 {
				weight = d.weight - m.dests[i-1].weight
			} else {
				weight = d.weight
			}

			em := &AccountMappingConfig{
				Destination: d.tr.dest,
				Weight:      weight,
			}
			dests = append(dests, em)
		}

		for c, cdests := range m.cdests {
			// This will be in order of the lowest weight first. These are also
			// relative to the next dest.
			for i, d := range cdests {
				var weight uint8
				if i > 0 {
					weight = d.weight - cdests[i-1].weight
				} else {
					weight = d.weight
				}

				em := &AccountMappingConfig{
					Destination: d.tr.dest,
					Weight:      weight,
					Cluster:     c,
				}
				dests = append(dests, em)
			}
		}

		// Sort for predicable ordering for testing.
		slices.SortFunc(dests, func(a, b *AccountMappingConfig) int {
			if a.Cluster == b.Cluster {
				if a.Weight > b.Weight {
					return -1
				} else if a.Weight < b.Weight {
					return 1
				}
				return strings.Compare(a.Destination, b.Destination)
			}
			return strings.Compare(a.Cluster, b.Cluster)
		})

		maps[m.src] = dests
	}

	return maps
}

// ExportAccountConfig exports parsed and validated account and user configuration
// specifically to be used for deriving operator mode account and user JWTs.
func ExportAccountConfig(opts *Options) *Config {
	accounts := make(map[string]*AccountConfig)

	// Multi-user setup
	for _, u := range opts.Users {
		var accName string

		// Implies non-global account user.
		if u.Account == nil {
			accName = globalAccountName
		} else {
			accName = u.Account.Name
		}

		var connTypes []string
		for k := range u.AllowedConnectionTypes {
			connTypes = append(connTypes, k)
		}
		eu := AccountUserConfig{
			Permissions:            u.Permissions,
			AllowedConnectionTypes: connTypes,
		}

		if _, ok := accounts[accName]; !ok {
			accounts[accName] = &AccountConfig{
				Users: make(map[string]*AccountUserConfig),
			}
		}
		accounts[accName].Users[u.Username] = &eu
	}

	for _, u := range opts.Nkeys {
		var accName string

		// Implies non-global account user.
		if u.Account == nil {
			accName = globalAccountName
		} else {
			accName = u.Account.Name
		}

		var connTypes []string
		for k := range u.AllowedConnectionTypes {
			connTypes = append(connTypes, k)
		}
		eu := AccountUserConfig{
			Permissions:            u.Permissions,
			AllowedConnectionTypes: connTypes,
		}
		accounts[accName].Users[u.Nkey] = &eu

		if _, ok := accounts[accName]; !ok {
			accounts[accName] = &AccountConfig{
				Users: make(map[string]*AccountUserConfig),
			}
		}
		accounts[accName].Users[u.Nkey] = &eu
	}

	// Implies single user mode.
	if accounts[globalAccountName] == nil {
		ea := &AccountConfig{
			Users: make(map[string]*AccountUserConfig),
		}
		accounts[globalAccountName] = ea

		// Implies single user with global account. Note if token based,
		// the user name will be empty.
		if opts.Username != "" {
			eu := AccountUserConfig{}
			ea.Users[opts.Username] = &eu
		}

		// Token based single user.
		if opts.Authorization != "" {
			eu := AccountUserConfig{}
			ea.Users[""] = &eu
		}
	}

	for _, a := range opts.Accounts {
		ea, ok := accounts[a.Name]
		// This should only occur if this is the global account
		// or no users were defined.
		if !ok {
			ea = &AccountConfig{}
			accounts[a.Name] = ea
		}

		// System account
		if a.Name == opts.SystemAccount {
			ea.IsSystemAccount = true
		}

		// Mappings
		ea.Mappings = exportMappings(a.mappings)

		// Msg trace
		if a.traceDest != "" {
			ea.MsgTrace = &AccountMsgTraceConfig{
				Destination: a.traceDest,
				Sampling:    a.traceDestSampling,
			}
		}

		// Limits
		ea.Limits = exportLimits(&a.limits)

		// JetStream
		jsLimits, ok := a.jsLimits[""]
		if ok {
			var clusterTraffic string
			if a.nrgAccount != "" {
				clusterTraffic = "owner"
			}
			ea.JetStream = &AccountJetStreamLimitsConfig{
				Enabled:               true,
				MaxMemory:             jsLimits.MaxMemory,
				MaxStorage:            jsLimits.MaxStore,
				MaxStreams:            jsLimits.MaxStreams,
				MaxConsumers:          jsLimits.MaxConsumers,
				MaxBytesRequired:      jsLimits.MaxBytesRequired,
				MemoryMaxStreamBytes:  jsLimits.MemoryMaxStreamBytes,
				StorageMaxStreamBytes: jsLimits.StoreMaxStreamBytes,
				MaxAckPending:         jsLimits.MaxAckPending,
				ClusterTraffic:        clusterTraffic,
			}
		}

		// Exports
		for k, s := range a.exports.streams {
			var accounts []string
			if len(s.approved) > 0 {
				for acc := range s.approved {
					accounts = append(accounts, acc)
				}
			}
			ea.Exports = append(ea.Exports, &AccountExportConfig{
				Stream:   k,
				Accounts: accounts,
			})
		}

		for k, s := range a.exports.services {
			ea.Exports = append(ea.Exports, &AccountExportConfig{
				Service:      k,
				ResponseType: responseTypeMap[s.respType],
				AllowTrace:   s.atrc,
			})
		}

		// Imports
		for _, s := range a.imports.streams {
			prefix := s.to
			if prefix != "" {
				prefix = strings.TrimRight(prefix, s.from)
			}
			ea.Imports = append(ea.Imports, &AccountImportConfig{
				Account:    s.acc.Name,
				Stream:     s.from,
				Prefix:     prefix,
				AllowTrace: s.atrc,
			})
		}

		for _, services := range a.imports.services {
			for _, s := range services {
				ea.Imports = append(ea.Imports, &AccountImportConfig{
					Account: s.acc.Name,
					Service: s.to,
					To:      s.from,
				})
			}
		}
	}

	// Remove global account if there are multiple accounts.
	if len(accounts) > 1 {
		delete(accounts, globalAccountName)
	}

	return &Config{
		Accounts: accounts,
	}
}
