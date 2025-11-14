package test

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
)

func RunServerWithLogging(opts *server.Options) *server.Server {
	if opts == nil {
		opts = &DefaultTestOptions
	}
	opts.NoLog = false
	opts.Cluster.PoolSize = -1
	opts.Cluster.Compression.Mode = server.CompressionOff
	opts.LeafNode.Compression.Mode = server.CompressionOff
	s, err := server.NewServer(opts)
	if err != nil || s == nil {
		panic(fmt.Sprintf("No NATS Server object returned: %v", err))
	}
	s.ConfigureLogger()
	go s.Start()
	if !s.ReadyForConnections(10 * time.Second) {
		panic("Unable to start NATS Server in Go Routine")
	}
	return s
}

func TestLogMaxArchives(t *testing.T) {
	// With logfile_size_limit set to small 100 characters, plain startup rotates 8 times
	for _, test := range []struct {
		name               string
		config             string
		totEntriesExpected int
	}{
		{
			name: "Default implicit, no max logs, expect 0 purged logs",
			config: `
				port: -1
				log_file: %s
				logfile_size_limit: 100
			`,
			totEntriesExpected: 9,
		},
		{
			name: "Default explicit, no max logs, expect 0 purged logs",
			config: `
				port: -1
				log_file: %s
				logfile_size_limit: 100
				logfile_max_num: 0
			`,
			totEntriesExpected: 9,
		},
		{
			name: "Default explicit - negative val, no max logs, expect 0 purged logs",
			config: `
				port: -1
				log_file: %s
				logfile_size_limit: 100
				logfile_max_num: -42
			`,
			totEntriesExpected: 9,
		},
		{
			name: "1-max num, expect 8 purged logs",
			config: `
				port: -1
				log_file: %s
				logfile_size_limit: 100
				logfile_max_num: 1
			`,
			totEntriesExpected: 1,
		},
		{
			name: "5-max num, expect 4 purged logs; use opt alias",
			config: `
				port: -1
				log_file: %s
				log_size_limit: 100
				log_max_num: 5
			`,
			totEntriesExpected: 5,
		},
		{
			name: "100-max num, expect 0 purged logs",
			config: `
				port: -1
				log_file: %s
				logfile_size_limit: 100
				logfile_max_num: 100
			`,
			totEntriesExpected: 9,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			d, err := os.MkdirTemp("", "logtest")
			if err != nil {
				t.Fatalf("Error creating temp dir: %v", err)
			}
			content := fmt.Sprintf(test.config, filepath.Join(d, "nats-server.log"))
			// server config does not like plain windows backslash
			if runtime.GOOS == "windows" {
				content = filepath.ToSlash(content)
			}
			opts, err := server.ProcessConfigFile(createConfFile(t, []byte(content)))
			if err != nil {
				t.Fatalf("Error processing config file: %v", err)
			}
			s := RunServerWithLogging(opts)
			if s == nil {
				t.Fatalf("No NATS Server object returned")
			}
			s.Shutdown()
			// Windows filesystem can be a little pokey on the flush, so wait a bit after shutdown...
			time.Sleep(500 * time.Millisecond)
			entries, err := os.ReadDir(d)
			if err != nil {
				t.Fatalf("Error reading dir: %v", err)
			}
			if len(entries) != test.totEntriesExpected {
				t.Fatalf("Expected %d log files, got %d", test.totEntriesExpected, len(entries))
			}
		})
	}
}
