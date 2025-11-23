package server

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestExportAccountConfig(t *testing.T) {
	tests := map[string]struct {
		config   string
		expected *Config
	}{
		"user-pass": {
			config: `
				authorization {
					user: foo
					password: bar
				}
			`,
			expected: &Config{
				Accounts: map[string]*AccountConfig{
					globalAccountName: {
						Users: map[string]*AccountUserConfig{
							"foo": {},
						},
					},
				},
			},
		},
		"users": {
			config: `
				authorization {
					default_permissions {
						subscribe: "bar.>"
						publish: "bar.>"
					}

					users = [
						{user: "foo", password: "foo", permissions: {
							subscribe: "foo.>",
							publish: "foo.>",
							allow_responses: {max: 3, expires: "1s"}
						}},
						{user: "bar", password: "bar"}
					]
				}

				no_auth_user: bar
			`,
			expected: &Config{
				Accounts: map[string]*AccountConfig{
					globalAccountName: {
						Users: map[string]*AccountUserConfig{
							"foo": {
								Permissions: &Permissions{
									Publish: &SubjectPermission{
										Allow: []string{"foo.>"},
									},
									Subscribe: &SubjectPermission{
										Allow: []string{"foo.>"},
									},
									Response: &ResponsePermission{
										MaxMsgs: 3,
										Expires: time.Second,
									},
								},
							},
							"bar": {
								Permissions: &Permissions{
									Publish: &SubjectPermission{
										Allow: []string{"bar.>"},
									},
									Subscribe: &SubjectPermission{
										Allow: []string{"bar.>"},
									},
								},
							},
						},
					},
				},
			},
		},
		"accounts": {
			config: `
				jetstream: true

				system_account: SYS

				accounts {
					SYS: {}

					A: {
						jetstream {
							max_streams: 2
							max_consumers: 5
							max_mem: 10MB
							max_file: 100MB
							max_bytes_required: true
							cluster_traffic: owner
						}

						users = [
							{user: "a", password: "a"}
						]

						exports = [
							{service: "a.*", response_type: "stream", allow_trace: true}
							{stream: "b.>", accounts: ["B"]}
						]

						mappings {
							"c.*": "d.e.{{wildcard(1)}}"
							"f": [
								{ destination: "f.v1", weight: 60 }
								{ destination: "f.v2", weight: 30 }
								{ destination: "f.v3", weight: 10 }
							]
							"g": [
								{ destination: "g.west.v1", weight: 30, cluster: "west" }
								{ destination: "g.west.v2", weight: 70, cluster: "west" }
								{ destination: "g.central", weight: 100, cluster: "central" }
								{ destination: "g.east", weight: 100, cluster: "east" }
							]
						}

						msg_trace: "trace.events"
					}

					B: {
						limits: {
							max_conn: 10
							max_subs: 100
						}

						users = [
							{user: "b", password: "b"}
						]

						imports = [
							{service: {account: "A", subject: "a.*"}, to: "Aservice.*"}
							{stream: {account: "A", subject: "b.>"}, prefix: "fromA", allow_trace: true}
						]

						msg_trace: {
							dest: "trace.events"
							sampling: 30
						}
					}
				}
			`,
			expected: &Config{
				Accounts: map[string]*AccountConfig{
					"SYS": {
						IsSystemAccount: true,
					},
					"A": {
						JetStream: &AccountJetStreamLimitsConfig{
							Enabled:               true,
							MaxMemory:             10 * 1024 * 1024,
							MaxStorage:            100 * 1024 * 1024,
							MaxStreams:            2,
							MaxConsumers:          5,
							MaxBytesRequired:      true,
							MemoryMaxStreamBytes:  -1,
							StorageMaxStreamBytes: -1,
							MaxAckPending:         -1,
							ClusterTraffic:        "owner",
						},
						Users: map[string]*AccountUserConfig{
							"a": {},
						},
						Exports: []*AccountExportConfig{
							{
								Stream:   "b.>",
								Accounts: []string{"B"},
							},
							{
								Service:      "a.*",
								ResponseType: "stream",
								AllowTrace:   true,
							},
						},
						Mappings: map[string][]*AccountMappingConfig{
							"c.*": {
								{Destination: "d.e.{{wildcard(1)}}", Weight: 100},
							},
							"f": {
								{Destination: "f.v1", Weight: 60},
								{Destination: "f.v2", Weight: 30},
								{Destination: "f.v3", Weight: 10},
							},
							"g": {
								{Destination: "g.central", Weight: 100, Cluster: "central"},
								{Destination: "g.east", Weight: 100, Cluster: "east"},
								{Destination: "g.west.v2", Weight: 70, Cluster: "west"},
								{Destination: "g.west.v1", Weight: 30, Cluster: "west"},
							},
						},
						MsgTrace: &AccountMsgTraceConfig{
							Destination: "trace.events",
							Sampling:    100,
						},
					},
					"B": {
						Users: map[string]*AccountUserConfig{
							"b": {},
						},
						Limits: &AccountLimitsConfig{
							MaxConns: 10,
							MaxSubs:  100,
						},
						Imports: []*AccountImportConfig{
							{
								Account:    "A",
								Stream:     "b.>",
								Prefix:     "fromA",
								AllowTrace: true,
							},
							{
								Account: "A",
								Service: "a.*",
								To:      "Aservice.*",
							},
						},
						MsgTrace: &AccountMsgTraceConfig{
							Destination: "trace.events",
							Sampling:    30,
						},
					},
				},
			},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			path := createConfFile(t, []byte(test.config))
			opts, err := ProcessConfigFile(path)
			require_NoError(t, err)
			cfg := ExportAccountConfig(opts)

			// Use JSON comparison to avoid reflect.DeepEqual nuances and get better diffs.
			got, _ := json.MarshalIndent(cfg, "", "  ")
			exp, _ := json.MarshalIndent(test.expected, "", "  ")
			gotDiff, expDiff := diffLines(string(got), string(exp))
			if gotDiff != expDiff {
				t.Fatalf("Expected and got differ:\n--- expected\n%s\n--- got\n%s\n", expDiff, gotDiff)
			}
		})
	}
}

func diffLines(a, b string) (string, string) {
	aLines := strings.Split(a, "\n")
	bLines := strings.Split(b, "\n")

	var aDiff, bDiff string
	maxLines := len(aLines)
	if len(bLines) > maxLines {
		maxLines = len(bLines)
	}

	for i := 0; i < maxLines; i++ {
		var aLine, bLine string
		if i < len(aLines) {
			aLine = aLines[i]
		}
		if i < len(bLines) {
			bLine = bLines[i]
		}
		if aLine != bLine {
			aDiff += aLine + "\n"
			bDiff += bLine + "\n"
		}
	}
	return aDiff, bDiff
}
