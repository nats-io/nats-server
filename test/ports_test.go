// Copyright 2012-2018 The NATS Authors
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

package test

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/gnatsd/server"
)

// waits until a calculated list of listeners is resolved or a timeout
func waitForFile(path string, dur time.Duration) ([]byte, error) {
	end := time.Now().Add(dur)
	for time.Now().Before(end) {
		if _, err := os.Stat(path); os.IsNotExist(err) {
			time.Sleep(25 * time.Millisecond)
			continue
		} else {
			return ioutil.ReadFile(path)
		}
	}
	return nil, errors.New("Timeout")
}

func TestPortsFile(t *testing.T) {
	opts := DefaultTestOptions
	opts.PortsFileDir = os.TempDir()
	opts.HTTPPort = -1
	opts.ProfPort = -1
	opts.Cluster.Port = -1

	s := RunServer(&opts)
	if !s.ReadyForListeners(nil, 5*time.Second) {
		t.Fatal("services failed to start in 5 seconds")
	}

	// the pid file should be
	portsFile := s.PortsFile(opts.PortsFileDir)

	if portsFile == "" {
		t.Fatal("Expected a ports file")
	}

	// try to read a file here

	// the file should be a json
	buf, err := waitForFile(portsFile, 5*time.Second)
	if err != nil {
		t.Fatalf("Could not read ports file: %v", err)
	}
	if len(buf) <= 0 {
		t.Fatal("Expected a non-zero length ports file")
	}

	ports := server.Ports{}
	json.Unmarshal(buf, &ports)

	if len(ports.Nats) == 0 || !strings.HasPrefix(ports.Nats[0], "nats://") {
		t.Fatal("Expected at least one nats url")
	}

	if len(ports.Monitoring) == 0 || !strings.HasPrefix(ports.Monitoring[0], "http://") {
		t.Fatal("Expected at least one monitoring url")
	}

	if len(ports.Cluster) == 0 || !strings.HasPrefix(ports.Cluster[0], "nats://") {
		t.Fatal("Expected at least one cluster listen url")
	}

	if len(ports.Profile) == 0 || !strings.HasPrefix(ports.Profile[0], "http://") {
		t.Fatal("Expected at least one profile listen url")
	}

	s.Shutdown()

	// if we called shutdown, the cleanup code should have kicked
	if _, err := os.Stat(portsFile); os.IsNotExist(err) {
		// good
	} else {
		t.Fatalf("the port file %s was not deleted", portsFile)
	}

}
