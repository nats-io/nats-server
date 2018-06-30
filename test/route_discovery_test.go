// Copyright 2015-2018 The NATS Authors
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
	"bufio"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/gnatsd/server"
)

func runSeedServer(t *testing.T) (*server.Server, *server.Options) {
	return RunServerWithConfig("./configs/seed.conf")
}

func runAuthSeedServer(t *testing.T) (*server.Server, *server.Options) {
	return RunServerWithConfig("./configs/auth_seed.conf")
}

func TestSeedFirstRouteInfo(t *testing.T) {
	s, opts := runSeedServer(t)
	defer s.Shutdown()

	rc := createRouteConn(t, opts.Cluster.Host, opts.Cluster.Port)
	defer rc.Close()

	_, routeExpect := setupRoute(t, rc, opts)
	buf := routeExpect(infoRe)

	info := server.Info{}
	if err := json.Unmarshal(buf[4:], &info); err != nil {
		t.Fatalf("Could not unmarshal route info: %v", err)
	}

	if info.ID != s.ID() {
		t.Fatalf("Expected seed's ID %q, got %q", s.ID(), info.ID)
	}
}

func TestSeedMultipleRouteInfo(t *testing.T) {
	s, opts := runSeedServer(t)
	defer s.Shutdown()

	rc1 := createRouteConn(t, opts.Cluster.Host, opts.Cluster.Port)
	defer rc1.Close()

	rc1ID := "2222"
	rc1Port := 22
	rc1Host := "127.0.0.1"

	routeSend1, route1Expect := setupRouteEx(t, rc1, opts, rc1ID)
	route1Expect(infoRe)

	// register ourselves via INFO
	r1Info := server.Info{ID: rc1ID, Host: rc1Host, Port: rc1Port}
	b, _ := json.Marshal(r1Info)
	infoJSON := fmt.Sprintf(server.InfoProto, b)
	routeSend1(infoJSON)
	routeSend1("PING\r\n")
	route1Expect(pongRe)

	rc2 := createRouteConn(t, opts.Cluster.Host, opts.Cluster.Port)
	defer rc2.Close()

	rc2ID := "2224"
	rc2Port := 24
	rc2Host := "127.0.0.1"

	routeSend2, route2Expect := setupRouteEx(t, rc2, opts, rc2ID)

	hp2 := fmt.Sprintf("nats-route://%s/", net.JoinHostPort(rc2Host, strconv.Itoa(rc2Port)))

	// register ourselves via INFO
	r2Info := server.Info{ID: rc2ID, Host: rc2Host, Port: rc2Port}
	b, _ = json.Marshal(r2Info)
	infoJSON = fmt.Sprintf(server.InfoProto, b)
	routeSend2(infoJSON)

	// Now read back the second INFO route1 should receive letting
	// it know about route2
	buf := route1Expect(infoRe)

	info := server.Info{}
	if err := json.Unmarshal(buf[4:], &info); err != nil {
		t.Fatalf("Could not unmarshal route info: %v", err)
	}

	if info.ID != rc2ID {
		t.Fatalf("Expected info.ID to be %q, got %q", rc2ID, info.ID)
	}
	if info.IP == "" {
		t.Fatalf("Expected a IP for the implicit route")
	}
	if info.IP != hp2 {
		t.Fatalf("Expected IP Host of %s, got %s\n", hp2, info.IP)
	}

	route2Expect(infoRe)
	routeSend2("PING\r\n")
	route2Expect(pongRe)

	// Now let's do a third.
	rc3 := createRouteConn(t, opts.Cluster.Host, opts.Cluster.Port)
	defer rc3.Close()

	rc3ID := "2226"
	rc3Port := 26
	rc3Host := "127.0.0.1"

	routeSend3, _ := setupRouteEx(t, rc3, opts, rc3ID)

	// register ourselves via INFO
	r3Info := server.Info{ID: rc3ID, Host: rc3Host, Port: rc3Port}
	b, _ = json.Marshal(r3Info)
	infoJSON = fmt.Sprintf(server.InfoProto, b)
	routeSend3(infoJSON)

	// Now read back out the info from the seed route
	buf = route1Expect(infoRe)

	info = server.Info{}
	if err := json.Unmarshal(buf[4:], &info); err != nil {
		t.Fatalf("Could not unmarshal route info: %v", err)
	}

	if info.ID != rc3ID {
		t.Fatalf("Expected info.ID to be %q, got %q", rc3ID, info.ID)
	}

	// Now read back out the info from the seed route
	buf = route2Expect(infoRe)

	info = server.Info{}
	if err := json.Unmarshal(buf[4:], &info); err != nil {
		t.Fatalf("Could not unmarshal route info: %v", err)
	}

	if info.ID != rc3ID {
		t.Fatalf("Expected info.ID to be %q, got %q", rc3ID, info.ID)
	}
}

func TestSeedSolicitWorks(t *testing.T) {
	s1, opts := runSeedServer(t)
	defer s1.Shutdown()

	// Create the routes string for others to connect to the seed.
	routesStr := fmt.Sprintf("nats-route://%s:%d/", opts.Cluster.Host, opts.Cluster.Port)

	// Run Server #2
	s2Opts := nextServerOpts(opts)
	s2Opts.Routes = server.RoutesFromStr(routesStr)

	s2 := RunServer(s2Opts)
	defer s2.Shutdown()

	// Run Server #3
	s3Opts := nextServerOpts(s2Opts)

	s3 := RunServer(s3Opts)
	defer s3.Shutdown()

	// Wait for a bit for graph to connect
	time.Sleep(500 * time.Millisecond)

	// Grab Routez from monitor ports, make sure we are fully connected
	url := fmt.Sprintf("http://%s:%d/", opts.Host, opts.HTTPPort)
	rz := readHTTPRoutez(t, url)
	ris := expectRids(t, rz, []string{s2.ID(), s3.ID()})
	if ris[s2.ID()].IsConfigured {
		t.Fatalf("Expected server not to be configured\n")
	}
	if ris[s3.ID()].IsConfigured {
		t.Fatalf("Expected server not to be configured\n")
	}

	url = fmt.Sprintf("http://%s:%d/", s2Opts.Host, s2Opts.HTTPPort)
	rz = readHTTPRoutez(t, url)
	ris = expectRids(t, rz, []string{s1.ID(), s3.ID()})
	if !ris[s1.ID()].IsConfigured {
		t.Fatalf("Expected seed server to be configured\n")
	}
	if ris[s3.ID()].IsConfigured {
		t.Fatalf("Expected server not to be configured\n")
	}

	url = fmt.Sprintf("http://%s:%d/", s3Opts.Host, s3Opts.HTTPPort)
	rz = readHTTPRoutez(t, url)
	ris = expectRids(t, rz, []string{s1.ID(), s2.ID()})
	if !ris[s1.ID()].IsConfigured {
		t.Fatalf("Expected seed server to be configured\n")
	}
	if ris[s2.ID()].IsConfigured {
		t.Fatalf("Expected server not to be configured\n")
	}
}

type serverInfo struct {
	server *server.Server
	opts   *server.Options
}

func checkConnected(t *testing.T, servers []serverInfo, current int, oneSeed bool) error {
	s := servers[current]

	// Grab Routez from monitor ports, make sure we are fully connected
	url := fmt.Sprintf("http://%s:%d/", s.opts.Host, s.opts.HTTPPort)
	rz := readHTTPRoutez(t, url)
	total := len(servers)
	var ids []string
	for i := 0; i < total; i++ {
		if i == current {
			continue
		}
		ids = append(ids, servers[i].server.ID())
	}
	ris, err := expectRidsNoFatal(t, true, rz, ids)
	if err != nil {
		return err
	}
	for i := 0; i < total; i++ {
		if i == current {
			continue
		}
		s := servers[i]
		if current == 0 || ((oneSeed && i > 0) || (!oneSeed && (i != current-1))) {
			if ris[s.server.ID()].IsConfigured {
				return fmt.Errorf("Expected server %s:%d not to be configured", s.opts.Host, s.opts.Port)
			}
		} else if oneSeed || (i == current-1) {
			if !ris[s.server.ID()].IsConfigured {
				return fmt.Errorf("Expected server %s:%d to be configured", s.opts.Host, s.opts.Port)
			}
		}
	}
	return nil
}

func TestStressSeedSolicitWorks(t *testing.T) {
	s1, opts := runSeedServer(t)
	defer s1.Shutdown()

	// Create the routes string for others to connect to the seed.
	routesStr := fmt.Sprintf("nats-route://%s:%d/", opts.Cluster.Host, opts.Cluster.Port)

	s2Opts := nextServerOpts(opts)
	s2Opts.Routes = server.RoutesFromStr(routesStr)

	s3Opts := nextServerOpts(s2Opts)
	s4Opts := nextServerOpts(s3Opts)

	for i := 0; i < 10; i++ {
		func() {
			// Run these servers manually, because we want them to start and
			// connect to s1 as fast as possible.

			s2 := server.New(s2Opts)
			if s2 == nil {
				panic("No NATS Server object returned.")
			}
			defer s2.Shutdown()
			go s2.Start()

			s3 := server.New(s3Opts)
			if s3 == nil {
				panic("No NATS Server object returned.")
			}
			defer s3.Shutdown()
			go s3.Start()

			s4 := server.New(s4Opts)
			if s4 == nil {
				panic("No NATS Server object returned.")
			}
			defer s4.Shutdown()
			go s4.Start()

			serversInfo := []serverInfo{{s1, opts}, {s2, s2Opts}, {s3, s3Opts}, {s4, s4Opts}}

			checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
				for j := 0; j < len(serversInfo); j++ {
					if err := checkConnected(t, serversInfo, j, true); err != nil {
						return err
					}
				}
				return nil
			})
		}()
		checkNumRoutes(t, s1, 0)
	}
}

func TestChainedSolicitWorks(t *testing.T) {
	s1, opts := runSeedServer(t)
	defer s1.Shutdown()

	// Create the routes string for others to connect to the seed.
	routesStr := fmt.Sprintf("nats-route://%s:%d/", opts.Cluster.Host, opts.Cluster.Port)

	// Run Server #2
	s2Opts := nextServerOpts(opts)
	s2Opts.Routes = server.RoutesFromStr(routesStr)

	s2 := RunServer(s2Opts)
	defer s2.Shutdown()

	// Run Server #3
	s3Opts := nextServerOpts(s2Opts)
	// We will have s3 connect to s2, not the seed.
	routesStr = fmt.Sprintf("nats-route://%s:%d/", s2Opts.Cluster.Host, s2Opts.Cluster.Port)
	s3Opts.Routes = server.RoutesFromStr(routesStr)

	s3 := RunServer(s3Opts)
	defer s3.Shutdown()

	// Wait for a bit for graph to connect
	time.Sleep(500 * time.Millisecond)

	// Grab Routez from monitor ports, make sure we are fully connected
	url := fmt.Sprintf("http://%s:%d/", opts.Host, opts.HTTPPort)
	rz := readHTTPRoutez(t, url)
	ris := expectRids(t, rz, []string{s2.ID(), s3.ID()})
	if ris[s2.ID()].IsConfigured {
		t.Fatalf("Expected server not to be configured\n")
	}
	if ris[s3.ID()].IsConfigured {
		t.Fatalf("Expected server not to be configured\n")
	}

	url = fmt.Sprintf("http://%s:%d/", s2Opts.Host, s2Opts.HTTPPort)
	rz = readHTTPRoutez(t, url)
	ris = expectRids(t, rz, []string{s1.ID(), s3.ID()})
	if !ris[s1.ID()].IsConfigured {
		t.Fatalf("Expected seed server to be configured\n")
	}
	if ris[s3.ID()].IsConfigured {
		t.Fatalf("Expected server not to be configured\n")
	}

	url = fmt.Sprintf("http://%s:%d/", s3Opts.Host, s3Opts.HTTPPort)
	rz = readHTTPRoutez(t, url)
	ris = expectRids(t, rz, []string{s1.ID(), s2.ID()})
	if !ris[s2.ID()].IsConfigured {
		t.Fatalf("Expected s2 server to be configured\n")
	}
	if ris[s1.ID()].IsConfigured {
		t.Fatalf("Expected seed server not to be configured\n")
	}
}

func TestStressChainedSolicitWorks(t *testing.T) {
	s1, opts := runSeedServer(t)
	defer s1.Shutdown()

	// Create the routes string for s2 to connect to the seed
	routesStr := fmt.Sprintf("nats-route://%s:%d/", opts.Cluster.Host, opts.Cluster.Port)
	s2Opts := nextServerOpts(opts)
	s2Opts.Routes = server.RoutesFromStr(routesStr)

	s3Opts := nextServerOpts(s2Opts)
	// Create the routes string for s3 to connect to s2
	routesStr = fmt.Sprintf("nats-route://%s:%d/", s2Opts.Cluster.Host, s2Opts.Cluster.Port)
	s3Opts.Routes = server.RoutesFromStr(routesStr)

	s4Opts := nextServerOpts(s3Opts)
	// Create the routes string for s4 to connect to s3
	routesStr = fmt.Sprintf("nats-route://%s:%d/", s3Opts.Cluster.Host, s3Opts.Cluster.Port)
	s4Opts.Routes = server.RoutesFromStr(routesStr)

	for i := 0; i < 10; i++ {
		func() {
			// Run these servers manually, because we want them to start and
			// connect to s1 as fast as possible.

			s2 := server.New(s2Opts)
			if s2 == nil {
				panic("No NATS Server object returned.")
			}
			defer s2.Shutdown()
			go s2.Start()

			s3 := server.New(s3Opts)
			if s3 == nil {
				panic("No NATS Server object returned.")
			}
			defer s3.Shutdown()
			go s3.Start()

			s4 := server.New(s4Opts)
			if s4 == nil {
				panic("No NATS Server object returned.")
			}
			defer s4.Shutdown()
			go s4.Start()

			serversInfo := []serverInfo{{s1, opts}, {s2, s2Opts}, {s3, s3Opts}, {s4, s4Opts}}

			checkFor(t, 5*time.Second, 100*time.Millisecond, func() error {
				for j := 0; j < len(serversInfo); j++ {
					if err := checkConnected(t, serversInfo, j, false); err != nil {
						return err
					}
				}
				return nil
			})
		}()
		checkNumRoutes(t, s1, 0)
	}
}

func TestAuthSeedSolicitWorks(t *testing.T) {
	s1, opts := runAuthSeedServer(t)
	defer s1.Shutdown()

	// Create the routes string for others to connect to the seed.
	routesStr := fmt.Sprintf("nats-route://%s:%s@%s:%d/", opts.Cluster.Username, opts.Cluster.Password, opts.Cluster.Host, opts.Cluster.Port)

	// Run Server #2
	s2Opts := nextServerOpts(opts)
	s2Opts.Routes = server.RoutesFromStr(routesStr)

	s2 := RunServer(s2Opts)
	defer s2.Shutdown()

	// Run Server #3
	s3Opts := nextServerOpts(s2Opts)

	s3 := RunServer(s3Opts)
	defer s3.Shutdown()

	// Wait for a bit for graph to connect
	time.Sleep(500 * time.Millisecond)

	// Grab Routez from monitor ports, make sure we are fully connected
	url := fmt.Sprintf("http://%s:%d/", opts.Host, opts.HTTPPort)
	rz := readHTTPRoutez(t, url)
	ris := expectRids(t, rz, []string{s2.ID(), s3.ID()})
	if ris[s2.ID()].IsConfigured {
		t.Fatalf("Expected server not to be configured\n")
	}
	if ris[s3.ID()].IsConfigured {
		t.Fatalf("Expected server not to be configured\n")
	}

	url = fmt.Sprintf("http://%s:%d/", s2Opts.Host, s2Opts.HTTPPort)
	rz = readHTTPRoutez(t, url)
	ris = expectRids(t, rz, []string{s1.ID(), s3.ID()})
	if !ris[s1.ID()].IsConfigured {
		t.Fatalf("Expected seed server to be configured\n")
	}
	if ris[s3.ID()].IsConfigured {
		t.Fatalf("Expected server not to be configured\n")
	}

	url = fmt.Sprintf("http://%s:%d/", s3Opts.Host, s3Opts.HTTPPort)
	rz = readHTTPRoutez(t, url)
	ris = expectRids(t, rz, []string{s1.ID(), s2.ID()})
	if !ris[s1.ID()].IsConfigured {
		t.Fatalf("Expected seed server to be configured\n")
	}
	if ris[s2.ID()].IsConfigured {
		t.Fatalf("Expected server not to be configured\n")
	}
}

// Helper to check for correct route memberships
func expectRids(t *testing.T, rz *server.Routez, rids []string) map[string]*server.RouteInfo {
	ri, err := expectRidsNoFatal(t, false, rz, rids)
	if err != nil {
		t.Fatalf("%v", err)
	}
	return ri
}

func expectRidsNoFatal(t *testing.T, direct bool, rz *server.Routez, rids []string) (map[string]*server.RouteInfo, error) {
	caller := 1
	if !direct {
		caller++
	}
	if len(rids) != rz.NumRoutes {
		_, fn, line, _ := runtime.Caller(caller)
		return nil, fmt.Errorf("[%s:%d] Expecting %d routes, got %d\n", fn, line, len(rids), rz.NumRoutes)
	}
	set := make(map[string]bool)
	for _, v := range rids {
		set[v] = true
	}
	// Make result map for additional checking
	ri := make(map[string]*server.RouteInfo)
	for _, r := range rz.Routes {
		if !set[r.RemoteID] {
			_, fn, line, _ := runtime.Caller(caller)
			return nil, fmt.Errorf("[%s:%d] Route with rid %s unexpected, expected %+v\n", fn, line, r.RemoteID, rids)
		}
		ri[r.RemoteID] = r
	}
	return ri, nil
}

// Helper to easily grab routez info.
func readHTTPRoutez(t *testing.T, url string) *server.Routez {
	resetPreviousHTTPConnections()
	resp, err := http.Get(url + "routez")
	if err != nil {
		stackFatalf(t, "Expected no error: Got %v\n", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		stackFatalf(t, "Expected a 200 response, got %d\n", resp.StatusCode)
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		stackFatalf(t, "Got an error reading the body: %v\n", err)
	}
	r := server.Routez{}
	if err := json.Unmarshal(body, &r); err != nil {
		stackFatalf(t, "Got an error unmarshalling the body: %v\n", err)
	}
	return &r
}

func TestSeedReturnIPInInfo(t *testing.T) {
	s, opts := runSeedServer(t)
	defer s.Shutdown()

	rc1 := createRouteConn(t, opts.Cluster.Host, opts.Cluster.Port)
	defer rc1.Close()

	rc1ID := "2222"
	rc1Port := 22
	rc1Host := "127.0.0.1"

	routeSend1, route1Expect := setupRouteEx(t, rc1, opts, rc1ID)
	route1Expect(infoRe)

	// register ourselves via INFO
	r1Info := server.Info{ID: rc1ID, Host: rc1Host, Port: rc1Port}
	b, _ := json.Marshal(r1Info)
	infoJSON := fmt.Sprintf(server.InfoProto, b)
	routeSend1(infoJSON)
	routeSend1("PING\r\n")
	route1Expect(pongRe)

	rc2 := createRouteConn(t, opts.Cluster.Host, opts.Cluster.Port)
	defer rc2.Close()

	rc2ID := "2224"
	rc2Port := 24
	rc2Host := "127.0.0.1"

	routeSend2, _ := setupRouteEx(t, rc2, opts, rc2ID)

	// register ourselves via INFO
	r2Info := server.Info{ID: rc2ID, Host: rc2Host, Port: rc2Port}
	b, _ = json.Marshal(r2Info)
	infoJSON = fmt.Sprintf(server.InfoProto, b)
	routeSend2(infoJSON)

	// Now read info that route1 should have received from the seed
	buf := route1Expect(infoRe)

	info := server.Info{}
	if err := json.Unmarshal(buf[4:], &info); err != nil {
		t.Fatalf("Could not unmarshal route info: %v", err)
	}

	if info.IP == "" {
		t.Fatal("Expected to have IP in INFO")
	}
	rip, _, err := net.SplitHostPort(strings.TrimPrefix(info.IP, "nats-route://"))
	if err != nil {
		t.Fatalf("Error parsing url: %v", err)
	}
	addr, ok := rc1.RemoteAddr().(*net.TCPAddr)
	if !ok {
		t.Fatal("Unable to get IP address from route")
	}
	s1 := strings.ToLower(addr.IP.String())
	s2 := strings.ToLower(rip)
	if s1 != s2 {
		t.Fatalf("Expected IP %s, got %s", s1, s2)
	}
}

func TestImplicitRouteRetry(t *testing.T) {
	srvSeed, optsSeed := runSeedServer(t)
	defer srvSeed.Shutdown()

	optsA := nextServerOpts(optsSeed)
	optsA.Routes = server.RoutesFromStr(fmt.Sprintf("nats://%s:%d", optsSeed.Cluster.Host, optsSeed.Cluster.Port))
	optsA.Cluster.ConnectRetries = 5
	srvA := RunServer(optsA)
	defer srvA.Shutdown()

	optsB := nextServerOpts(optsA)
	rcb := createRouteConn(t, optsSeed.Cluster.Host, optsSeed.Cluster.Port)
	defer rcb.Close()
	rcbID := "ServerB"
	routeBSend, routeBExpect := setupRouteEx(t, rcb, optsB, rcbID)
	routeBExpect(infoRe)
	// register ourselves via INFO
	rbInfo := server.Info{ID: rcbID, Host: optsB.Cluster.Host, Port: optsB.Cluster.Port}
	b, _ := json.Marshal(rbInfo)
	infoJSON := fmt.Sprintf(server.InfoProto, b)
	routeBSend(infoJSON)
	routeBSend("PING\r\n")
	routeBExpect(pongRe)

	// srvA should try to connect. Wait to make sure that it fails.
	time.Sleep(1200 * time.Millisecond)

	// Setup a fake route listen for routeB
	rbListen, err := net.Listen("tcp", fmt.Sprintf("%s:%d", optsB.Cluster.Host, optsB.Cluster.Port))
	if err != nil {
		t.Fatalf("Error during listen: %v", err)
	}
	c, err := rbListen.Accept()
	if err != nil {
		t.Fatalf("Error during accept: %v", err)
	}
	defer c.Close()

	br := bufio.NewReaderSize(c, 32768)
	// Consume CONNECT and INFO
	for i := 0; i < 2; i++ {
		c.SetReadDeadline(time.Now().Add(2 * time.Second))
		buf, _, err := br.ReadLine()
		c.SetReadDeadline(time.Time{})
		if err != nil {
			t.Fatalf("Error reading: %v", err)
		}
		if i == 0 {
			continue
		}
		buf = buf[len("INFO "):]
		info := &server.Info{}
		if err := json.Unmarshal(buf, info); err != nil {
			t.Fatalf("Error during unmarshal: %v", err)
		}
		// Check INFO is from server A.
		if info.ID != srvA.ID() {
			t.Fatalf("Expected CONNECT from %v, got CONNECT from %v", srvA.ID(), info.ID)
		}
	}
}
