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
			"Default implicit, no max archives, expect 0 purged archives",
			`
				port: -1
				log_file: %s
				logfile_size_limit: 100
			`,
			9,
		},
		{
			"Default explicit, no max archives, expect 0 purged archives",
			`
				port: -1
				log_file: %s
				logfile_size_limit: 100
				logfile_max_archives: 0
			`,
			9,
		},
		{
			"Default explicit - negative val, no max archives, expect 0 purged archives",
			`
				port: -1
				log_file: %s
				logfile_size_limit: 100
				logfile_max_archives: -42
			`,
			9,
		},
		{
			"1-archive limit, expect 7 purged archives",
			`
				port: -1
				log_file: %s
				logfile_size_limit: 100
				logfile_max_archives: 1
			`,
			2,
		},
		{
			"5-archive limit, expect 4 purged archives",
			`
				port: -1
				log_file: %s
				logfile_size_limit: 100
				logfile_max_archives: 5
			`,
			6,
		},
		{
			"100-archive limit, expect 0 purged archives",
			`
				port: -1
				log_file: %s
				logfile_size_limit: 100
				logfile_max_archives: 100
			`,
			9,
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
