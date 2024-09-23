// Copyright 2024 The NATS Authors
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
	"testing"
)

var serverConfig1 = `
	server_name: server1
	listen: 127.0.0.1:4222
	http: 8222

	prof_port = 18222

	jetstream: {max_mem_store: 256MB, max_file_store: 2GB, store_dir: '%s'}

	cluster {
  		name: my_cluster
  		listen: 127.0.0.1:4248
  		routes: [nats://127.0.0.1:4249,nats://127.0.0.1:4250]
	}

	accounts { $SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] }}
`

var serverConfig2 = `
	server_name: server2
	listen: 127.0.0.1:5222
	http: 8223

	prof_port = 18223

	jetstream: {max_mem_store: 256MB, max_file_store: 2GB, store_dir: '%s'}

	cluster {
  		name: my_cluster
  		listen: 127.0.0.1:4249
  		routes: [nats://127.0.0.1:4248,nats://127.0.0.1:4250]
	}

	accounts { $SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] }}
`

var serverConfig3 = `
	server_name: server3
	listen: 127.0.0.1:6222
	http: 8224

	prof_port = 18224

	jetstream: {max_mem_store: 256MB, max_file_store: 2GB, store_dir: '%s'}

	cluster {
  		name: my_cluster
  		listen: 127.0.0.1:4250
  		routes: [nats://127.0.0.1:4248,nats://127.0.0.1:4249]
	}

	accounts { $SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] }}
`

var connectURL = "nats://127.0.0.1:4222,nats://127.0.0.1:4223,nats://127.0.0.1:4224"

func createMyLocalCluster(t *testing.T) *cluster {
	c := &cluster{servers: make([]*Server, 0, 3), opts: make([]*Options, 0, 3), name: "C3"}

	storeDir1 := t.TempDir()
	s1, o := RunServerWithConfig(createConfFile(t, []byte(fmt.Sprintf(serverConfig1, storeDir1))))
	c.servers = append(c.servers, s1)
	c.opts = append(c.opts, o)

	storeDir2 := t.TempDir()
	s2, o := RunServerWithConfig(createConfFile(t, []byte(fmt.Sprintf(serverConfig2, storeDir2))))
	c.servers = append(c.servers, s2)
	c.opts = append(c.opts, o)

	storeDir3 := t.TempDir()
	s3, o := RunServerWithConfig(createConfFile(t, []byte(fmt.Sprintf(serverConfig3, storeDir3))))
	c.servers = append(c.servers, s3)
	c.opts = append(c.opts, o)

	c.t = t

	// Wait til we are formed and have a leader.
	c.checkClusterFormed()
	c.waitOnClusterReady()

	return c
}
