// Copyright 2019-2020 The NATS Authors
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
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/jwt"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
)

// Used to setup superclusters for tests.
type supercluster struct {
	clusters []*cluster
}

func (sc *supercluster) shutdown() {
	for _, c := range sc.clusters {
		shutdownCluster(c)
	}
}

const digits = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"
const base = 36
const cnlen = 8

func randClusterName() string {
	var name []byte
	rn := rand.Int63()
	for i := 0; i < cnlen; i++ {
		name = append(name, digits[rn%base])
		rn /= base
	}
	return string(name[:cnlen])
}

func createSuperCluster(t *testing.T, numServersPer, numClusters int) *supercluster {
	clusters := []*cluster{}

	for i := 0; i < numClusters; i++ {
		// Pick cluster name and setup default accounts.
		c := createClusterEx(t, true, 5*time.Millisecond, true, randClusterName(), numServersPer, clusters...)
		clusters = append(clusters, c)
	}
	return &supercluster{clusters}
}

func (sc *supercluster) setResponseThreshold(t *testing.T, maxTime time.Duration) {
	t.Helper()
	for _, c := range sc.clusters {
		for _, s := range c.servers {
			foo, err := s.LookupAccount("FOO")
			if err != nil {
				t.Fatalf("Error looking up account 'FOO': %v", err)
			}
			if err := foo.SetServiceExportResponseThreshold("ngs.usage.*", maxTime); err != nil {
				t.Fatalf("Error setting response threshold")
			}
		}
	}
}

func (sc *supercluster) setImportShare(t *testing.T) {
	t.Helper()
	for _, c := range sc.clusters {
		for _, s := range c.servers {
			foo, err := s.LookupAccount("FOO")
			if err != nil {
				t.Fatalf("Error looking up account 'FOO': %v", err)
			}
			bar, err := s.LookupAccount("BAR")
			if err != nil {
				t.Fatalf("Error looking up account 'BAR': %v", err)
			}
			if err := bar.SetServiceImportSharing(foo, "ngs.usage.bar", true); err != nil {
				t.Fatalf("Error setting import sharing: %v", err)
			}
		}
	}
}

func (sc *supercluster) setupLatencyTracking(t *testing.T, p int) {
	t.Helper()
	for _, c := range sc.clusters {
		for _, s := range c.servers {
			foo, err := s.LookupAccount("FOO")
			if err != nil {
				t.Fatalf("Error looking up account 'FOO': %v", err)
			}
			bar, err := s.LookupAccount("BAR")
			if err != nil {
				t.Fatalf("Error looking up account 'BAR': %v", err)
			}
			if err := foo.AddServiceExport("ngs.usage.*", nil); err != nil {
				t.Fatalf("Error adding service export to 'FOO': %v", err)
			}
			if err := foo.TrackServiceExportWithSampling("ngs.usage.*", "results", p); err != nil {
				t.Fatalf("Error adding latency tracking to 'FOO': %v", err)
			}
			if err := bar.AddServiceImport(foo, "ngs.usage", "ngs.usage.bar"); err != nil {
				t.Fatalf("Error adding latency tracking to 'FOO': %v", err)
			}
		}
	}
}

func (sc *supercluster) removeLatencyTracking(t *testing.T) {
	t.Helper()
	for _, c := range sc.clusters {
		for _, s := range c.servers {
			foo, err := s.LookupAccount("FOO")
			if err != nil {
				t.Fatalf("Error looking up account 'FOO': %v", err)
			}
			foo.UnTrackServiceExport("ngs.usage.*")
		}
	}
}

func (sc *supercluster) totalSubs() int {
	totalSubs := 0
	for _, c := range sc.clusters {
		totalSubs += c.totalSubs()
	}
	return totalSubs
}

func clientConnectWithName(t *testing.T, opts *server.Options, user, appname string) *nats.Conn {
	t.Helper()
	url := fmt.Sprintf("nats://%s:pass@%s:%d", user, opts.Host, opts.Port)
	nc, err := nats.Connect(url, nats.Name(appname))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	return nc
}

func clientConnect(t *testing.T, opts *server.Options, user string) *nats.Conn {
	t.Helper()
	return clientConnectWithName(t, opts, user, "")
}

func clientConnectOldRequest(t *testing.T, opts *server.Options, user string) *nats.Conn {
	t.Helper()
	url := fmt.Sprintf("nats://%s:pass@%s:%d", user, opts.Host, opts.Port)
	nc, err := nats.Connect(url, nats.UseOldRequestStyle())
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	return nc
}

func checkServiceLatency(t *testing.T, sl server.ServiceLatency, start time.Time, serviceTime time.Duration) {
	t.Helper()

	if sl.Status != 200 {
		t.Fatalf("Bad status received, wanted 200 got %d", sl.Status)
	}

	serviceTime = serviceTime.Round(time.Millisecond)

	startDelta := sl.RequestStart.Sub(start)
	// Original test was 5ms, but got GitHub Action failure with "Bad start delta 5.033929ms",
	// so be more generous.
	if startDelta > 10*time.Millisecond {
		t.Fatalf("Bad start delta %v", startDelta)
	}
	// Since RTT during tests is estimate we remove from calculation.
	if (sl.ServiceLatency + sl.Responder.RTT) < time.Duration(float64(serviceTime)*0.8) {
		t.Fatalf("Bad service latency: %v (%v)", sl.ServiceLatency, serviceTime)
	}
	if sl.TotalLatency < sl.ServiceLatency {
		t.Fatalf("Bad total latency: %v (%v)", sl.TotalLatency, sl.ServiceLatency)
	}
	// We should have NATS latency here that is non-zero with real clients.
	if sl.Requestor.RTT == 0 {
		t.Fatalf("Expected non-zero NATS Requestor latency")
	}
	if sl.Responder.RTT == 0 {
		t.Fatalf("Expected non-zero NATS Requestor latency")
	}

	// Make sure they add up
	got := sl.TotalLatency
	expected := sl.ServiceLatency + sl.NATSTotalTime()
	if got != expected {
		t.Fatalf("Numbers do not add up: %+v,\ngot: %v\nexpected: %v", sl, got, expected)
	}
}

func extendedCheck(t *testing.T, lc *server.LatencyClient, eUser, appName, eServer string) {
	t.Helper()
	if lc.User != eUser {
		t.Fatalf("Expected user of %q, got %q", eUser, lc.User)
	}
	if appName != "" && appName != lc.Name {
		t.Fatalf("Expected appname of %q, got %q\n", appName, lc.Name)
	}
	if lc.IP == "" {
		t.Fatalf("Expected non-empty IP")
	}
	if lc.CID < 1 || lc.CID > 20 {
		t.Fatalf("Expected a CID in range, got %d", lc.CID)
	}
	if eServer != "" && eServer != lc.Server {
		t.Fatalf("Expected server of %q, got %q", eServer, lc.Server)
	}
}

func noShareCheck(t *testing.T, lc *server.LatencyClient) {
	t.Helper()
	if lc.Name != "" {
		t.Fatalf("appname should not have been shared, got %q", lc.Name)
	}
	if lc.User != "" {
		t.Fatalf("user should not have been shared, got %q", lc.User)
	}
	if lc.IP != "" {
		t.Fatalf("client ip should not have been shared, got %q", lc.IP)
	}
	if lc.CID != 0 {
		t.Fatalf("client id should not have been shared, got %d", lc.CID)
	}
	if lc.Server != "" {
		t.Fatalf("client' server should not have been shared, got %q", lc.Server)
	}
}

func TestServiceLatencySingleServerConnect(t *testing.T) {
	sc := createSuperCluster(t, 3, 2)
	defer sc.shutdown()

	// Now add in new service export to FOO and have bar import that with tracking enabled.
	sc.setupLatencyTracking(t, 100)

	// Now we can setup and test, do single node only first.
	// This is the service provider.
	nc := clientConnectWithName(t, sc.clusters[0].opts[0], "foo", "service22")
	defer nc.Close()

	// The service listener.
	serviceTime := 25 * time.Millisecond
	nc.Subscribe("ngs.usage.*", func(msg *nats.Msg) {
		time.Sleep(serviceTime)
		msg.Respond([]byte("22 msgs"))
	})

	// Listen for metrics
	rsub, _ := nc.SubscribeSync("results")

	// Requestor
	nc2 := clientConnect(t, sc.clusters[0].opts[0], "bar")
	defer nc2.Close()

	// Send the request.
	start := time.Now()
	if _, err := nc2.Request("ngs.usage", []byte("1h"), time.Second); err != nil {
		t.Fatalf("Expected a response")
	}

	var sl server.ServiceLatency
	rmsg, _ := rsub.NextMsg(time.Second)
	json.Unmarshal(rmsg.Data, &sl)

	checkServiceLatency(t, sl, start, serviceTime)

	rs := sc.clusters[0].servers[0]
	extendedCheck(t, &sl.Responder, "foo", "service22", rs.Name())
	// Normally requestor's don't share
	noShareCheck(t, &sl.Requestor)

	// Now make sure normal use case works with old request style.
	nc3 := clientConnectOldRequest(t, sc.clusters[0].opts[0], "bar")
	defer nc3.Close()

	start = time.Now()
	if _, err := nc3.Request("ngs.usage", []byte("1h"), time.Second); err != nil {
		t.Fatalf("Expected a response")
	}
	nc3.Close()

	checkServiceLatency(t, sl, start, serviceTime)
	extendedCheck(t, &sl.Responder, "foo", "service22", rs.Name())
	// Normally requestor's don't share
	noShareCheck(t, &sl.Requestor)
}

// If a client has a longer RTT that the effective RTT for NATS + responder
// the requestor RTT will be marked as 0. This can happen quite often with
// utility programs that are far away from a cluster like NGS but the service
// response time has a shorter RTT.
func TestServiceLatencyClientRTTSlowerVsServiceRTT(t *testing.T) {
	sc := createSuperCluster(t, 2, 2)
	defer sc.shutdown()

	// Now add in new service export to FOO and have BAR import that with tracking enabled.
	sc.setupLatencyTracking(t, 100)

	nc := clientConnect(t, sc.clusters[0].opts[0], "foo")
	defer nc.Close()

	// The service listener. Mostly instant response.
	nc.Subscribe("ngs.usage.*", func(msg *nats.Msg) {
		time.Sleep(time.Millisecond)
		msg.Respond([]byte("22 msgs"))
	})

	// Listen for metrics
	rsub, _ := nc.SubscribeSync("results")
	nc.Flush()

	// Requestor and processing
	requestAndCheck := func(cindex, sindex int) {
		t.Helper()
		sopts := sc.clusters[cindex].opts[sindex]

		if nmsgs, _, err := rsub.Pending(); err != nil || nmsgs != 0 {
			t.Fatalf("Did not expect any latency results, got %d", nmsgs)
		}

		rtt := 10 * time.Millisecond
		sp, opts := newSlowProxy(rtt+5*time.Millisecond, sopts)
		defer sp.Stop()

		nc2 := clientConnect(t, opts, "bar")
		defer nc2.Close()

		start := time.Now()
		nc2.Flush()
		// Check rtt for slow proxy
		if d := time.Since(start); d < rtt {
			t.Fatalf("Expected an rtt of at least %v, got %v", rtt, d)
		}

		// Send the request.
		start = time.Now()
		_, err := nc2.Request("ngs.usage", []byte("1h"), time.Second)
		if err != nil {
			t.Fatalf("Expected a response")
		}

		var sl server.ServiceLatency
		rmsg, err := rsub.NextMsg(time.Second)
		if err != nil || rmsg == nil {
			t.Fatalf("Did not receive latency results")
		}
		json.Unmarshal(rmsg.Data, &sl)

		if sl.Status != 200 {
			t.Fatalf("Expected a status 200 Ok, got [%d] %q", sl.Status, sl.Error)
		}
		// We want to test here that when the client requestor RTT is larger then the response time
		// we still deliver a requestor value > 0.
		// Now check that it is close to rtt.
		if sl.Requestor.RTT < rtt {
			t.Fatalf("Expected requestor latency to be > %v, got %v", rtt, sl.Requestor.RTT)
		}
		if sl.TotalLatency < rtt {
			t.Fatalf("Expected total latency to be > %v, got %v", rtt, sl.TotalLatency)
		}

		rs := sc.clusters[0].servers[0]
		extendedCheck(t, &sl.Responder, "foo", "", rs.Name())
		// Normally requestor's don't share
		noShareCheck(t, &sl.Requestor)

		// Check for trailing duplicates..
		rmsg, err = rsub.NextMsg(100 * time.Millisecond)
		if err == nil {
			t.Fatalf("Duplicate metric result, %q", rmsg.Data)
		}
	}

	// Check same server.
	requestAndCheck(0, 0)
	// Check from remote server across GW.
	requestAndCheck(1, 1)
	// Same cluster but different server
	requestAndCheck(0, 1)
}

func connRTT(nc *nats.Conn) time.Duration {
	// Do 5x to flatten
	total := time.Duration(0)
	for i := 0; i < 5; i++ {
		start := time.Now()
		nc.Flush()
		total += time.Since(start)
	}
	return total / 5
}

func TestServiceLatencyRemoteConnect(t *testing.T) {
	sc := createSuperCluster(t, 3, 2)
	defer sc.shutdown()

	// Now add in new service export to FOO and have bar import that with tracking enabled.
	sc.setupLatencyTracking(t, 100)

	// Now we can setup and test, do single node only first.
	// This is the service provider.
	nc := clientConnect(t, sc.clusters[0].opts[0], "foo")
	defer nc.Close()

	subsBefore := int(sc.clusters[0].servers[0].NumSubscriptions())

	// The service listener.
	serviceTime := 25 * time.Millisecond
	nc.Subscribe("ngs.usage.*", func(msg *nats.Msg) {
		time.Sleep(serviceTime)
		msg.Respond([]byte("22 msgs"))
	})

	// Listen for metrics
	rsub, _ := nc.SubscribeSync("results")
	nc.Flush()

	if err := checkExpectedSubs(subsBefore+2, sc.clusters[0].servers...); err != nil {
		t.Fatal(err.Error())
	}

	// Same Cluster Requestor
	nc2 := clientConnect(t, sc.clusters[0].opts[2], "bar")
	defer nc2.Close()

	// Send the request.
	start := time.Now()
	_, err := nc2.Request("ngs.usage", []byte("1h"), time.Second)
	if err != nil {
		t.Fatalf("Expected a response")
	}

	var sl server.ServiceLatency
	rmsg, err := rsub.NextMsg(time.Second)
	if err != nil {
		t.Fatalf("Error getting latency measurement: %v", err)
	}
	json.Unmarshal(rmsg.Data, &sl)
	checkServiceLatency(t, sl, start, serviceTime)
	rs := sc.clusters[0].servers[0]
	extendedCheck(t, &sl.Responder, "foo", "", rs.Name())
	// Normally requestor's don't share
	noShareCheck(t, &sl.Requestor)

	// Lastly here, we need to make sure we are properly tracking the extra hops.
	// We will make sure that NATS latency is close to what we see from the outside in terms of RTT.
	if crtt := connRTT(nc) + connRTT(nc2); sl.NATSTotalTime() < crtt {
		t.Fatalf("Not tracking second measurement for NATS latency across servers: %v vs %v", sl.NATSTotalTime(), crtt)
	}

	// Gateway Requestor
	nc2 = clientConnect(t, sc.clusters[1].opts[1], "bar")
	defer nc2.Close()

	// Send the request.
	start = time.Now()
	_, err = nc2.Request("ngs.usage", []byte("1h"), time.Second)
	if err != nil {
		t.Fatalf("Expected a response")
	}

	rmsg, err = rsub.NextMsg(time.Second)
	if err != nil {
		t.Fatalf("Error getting latency measurement: %v", err)
	}
	json.Unmarshal(rmsg.Data, &sl)
	checkServiceLatency(t, sl, start, serviceTime)
	extendedCheck(t, &sl.Responder, "foo", "", rs.Name())
	// Normally requestor's don't share
	noShareCheck(t, &sl.Requestor)

	// Lastly here, we need to make sure we are properly tracking the extra hops.
	// We will make sure that NATS latency is close to what we see from the outside in terms of RTT.
	if crtt := connRTT(nc) + connRTT(nc2); sl.NATSTotalTime() < crtt {
		t.Fatalf("Not tracking second measurement for NATS latency across servers: %v vs %v", sl.NATSTotalTime(), crtt)
	}

	// Now turn off and make sure we no longer receive updates.
	sc.removeLatencyTracking(t)
	_, err = nc2.Request("ngs.usage", []byte("1h"), time.Second)
	if err != nil {
		t.Fatalf("Expected a response")
	}

	_, err = rsub.NextMsg(100 * time.Millisecond)
	if err == nil {
		t.Fatalf("Did not expect to receive a latency metric")
	}
}

func TestServiceLatencySampling(t *testing.T) {
	sc := createSuperCluster(t, 3, 2)
	defer sc.shutdown()

	// Now add in new service export to FOO and have bar import that with tracking enabled.
	sc.setupLatencyTracking(t, 50)

	// Now we can setup and test, do single node only first.
	// This is the service provider.
	nc := clientConnect(t, sc.clusters[0].opts[0], "foo")
	defer nc.Close()

	// The service listener.
	nc.Subscribe("ngs.usage.*", func(msg *nats.Msg) {
		msg.Respond([]byte("22 msgs"))
	})

	// Listen for metrics
	received := int32(0)

	nc.Subscribe("results", func(msg *nats.Msg) {
		atomic.AddInt32(&received, 1)
	})

	// Same Cluster Requestor
	nc2 := clientConnect(t, sc.clusters[0].opts[2], "bar")
	defer nc2.Close()

	toSend := 1000
	for i := 0; i < toSend; i++ {
		nc2.Request("ngs.usage", []byte("1h"), time.Second)
	}
	// Wait for results to flow in.
	time.Sleep(100 * time.Millisecond)

	mid := toSend / 2
	delta := toSend / 10 // 10%
	got := int(atomic.LoadInt32(&received))

	if got > mid+delta || got < mid-delta {
		t.Fatalf("Sampling number incorrect: %d vs %d", mid, got)
	}
}

func TestServiceLatencyNoSubsLeak(t *testing.T) {
	sc := createSuperCluster(t, 3, 3)
	defer sc.shutdown()

	// Now add in new service export to FOO and have bar import that with tracking enabled.
	sc.setupLatencyTracking(t, 100)

	nc := clientConnectWithName(t, sc.clusters[0].opts[1], "foo", "dlc22")
	defer nc.Close()

	// The service listener.
	nc.Subscribe("ngs.usage.*", func(msg *nats.Msg) {
		msg.Respond([]byte("22 msgs"))
	})
	nc.Flush()
	// Propagation of sub through super cluster.
	time.Sleep(100 * time.Millisecond)

	startSubs := sc.totalSubs()

	fooAcc, _ := sc.clusters[1].servers[1].LookupAccount("FOO")
	startNumSis := fooAcc.NumServiceImports()

	for i := 0; i < 100; i++ {
		nc := clientConnect(t, sc.clusters[1].opts[1], "bar")
		if _, err := nc.Request("ngs.usage", []byte("1h"), time.Second); err != nil {
			t.Fatalf("Error on request: %v", err)
		}
		nc.Close()
	}

	// We are adding 3 here for the wildcard response subject for service replies.
	// we only have one but it will show in three places.
	startSubs += 3

	checkFor(t, time.Second, 50*time.Millisecond, func() error {
		if numSubs := sc.totalSubs(); numSubs != startSubs {
			return fmt.Errorf("Leaked %d subs", numSubs-startSubs)
		}
		return nil
	})

	// Now also check to make sure the service imports created for the request go away as well.
	checkFor(t, time.Second, 50*time.Millisecond, func() error {
		if numSis := fooAcc.NumServiceImports(); numSis != startNumSis {
			return fmt.Errorf("Leaked %d service imports", numSis-startNumSis)
		}
		return nil
	})
}

func TestServiceLatencyWithName(t *testing.T) {
	sc := createSuperCluster(t, 1, 1)
	defer sc.shutdown()

	// Now add in new service export to FOO and have bar import that with tracking enabled.
	sc.setupLatencyTracking(t, 100)

	opts := sc.clusters[0].opts[0]

	nc := clientConnectWithName(t, opts, "foo", "dlc22")
	defer nc.Close()

	// The service listener.
	nc.Subscribe("ngs.usage.*", func(msg *nats.Msg) {
		msg.Respond([]byte("22 msgs"))
	})

	// Listen for metrics
	rsub, _ := nc.SubscribeSync("results")

	nc2 := clientConnect(t, opts, "bar")
	defer nc2.Close()
	nc2.Request("ngs.usage", []byte("1h"), time.Second)

	var sl server.ServiceLatency
	rmsg, _ := rsub.NextMsg(time.Second)
	json.Unmarshal(rmsg.Data, &sl)

	// Make sure we have AppName set.
	rs := sc.clusters[0].servers[0]
	extendedCheck(t, &sl.Responder, "foo", "dlc22", rs.Name())
	// Normally requestor's don't share
	noShareCheck(t, &sl.Requestor)
}

func TestServiceLatencyWithNameMultiServer(t *testing.T) {
	sc := createSuperCluster(t, 3, 2)
	defer sc.shutdown()

	// Now add in new service export to FOO and have bar import that with tracking enabled.
	sc.setupLatencyTracking(t, 100)

	nc := clientConnectWithName(t, sc.clusters[0].opts[1], "foo", "dlc22")
	defer nc.Close()

	// The service listener.
	nc.Subscribe("ngs.usage.*", func(msg *nats.Msg) {
		msg.Respond([]byte("22 msgs"))
	})

	// Listen for metrics
	rsub, _ := nc.SubscribeSync("results")
	nc.Flush()

	nc2 := clientConnect(t, sc.clusters[1].opts[1], "bar")
	defer nc2.Close()
	nc2.Request("ngs.usage", []byte("1h"), time.Second)

	var sl server.ServiceLatency
	rmsg, _ := rsub.NextMsg(time.Second)
	json.Unmarshal(rmsg.Data, &sl)

	// Make sure we have AppName set.
	rs := sc.clusters[0].servers[1]
	extendedCheck(t, &sl.Responder, "foo", "dlc22", rs.Name())
	// Normally requestor's don't share
	noShareCheck(t, &sl.Requestor)
}

func createAccountWithJWT(t *testing.T) (string, nkeys.KeyPair, *jwt.AccountClaims) {
	t.Helper()
	okp, _ := nkeys.FromSeed(oSeed)
	akp, _ := nkeys.CreateAccount()
	pub, _ := akp.PublicKey()
	nac := jwt.NewAccountClaims(pub)
	jwt, _ := nac.Encode(okp)
	return jwt, akp, nac
}

func TestServiceLatencyWithJWT(t *testing.T) {
	okp, _ := nkeys.FromSeed(oSeed)

	// Create three accounts, system, service and normal account.
	sysJWT, sysKP, _ := createAccountWithJWT(t)
	sysPub, _ := sysKP.PublicKey()

	_, svcKP, svcAcc := createAccountWithJWT(t)
	svcPub, _ := svcKP.PublicKey()

	// Add in the service export with latency tracking here.
	serviceExport := &jwt.Export{Subject: "req.*", Type: jwt.Service}
	svcAcc.Exports.Add(serviceExport)
	svcJWT, err := svcAcc.Encode(okp)
	if err != nil {
		t.Fatalf("Error generating account JWT: %v", err)
	}

	_, accKP, accAcc := createAccountWithJWT(t)
	accPub, _ := accKP.PublicKey()

	// Add in the import.
	serviceImport := &jwt.Import{Account: svcPub, Subject: "request", To: "req.echo", Type: jwt.Service}
	accAcc.Imports.Add(serviceImport)
	accJWT, err := accAcc.Encode(okp)
	if err != nil {
		t.Fatalf("Error generating account JWT: %v", err)
	}

	cf := `
	listen: 127.0.0.1:-1
	cluster {
		listen: 127.0.0.1:-1
		authorization {
			timeout: 2.2
		} %s
	}

	operator = "./configs/nkeys/op.jwt"
	system_account = "%s"

	resolver = MEMORY
	resolver_preload = {
		%s : "%s"
		%s : "%s"
		%s : "%s"
	}
	`
	contents := strings.Replace(fmt.Sprintf(cf, "", sysPub, sysPub, sysJWT, svcPub, svcJWT, accPub, accJWT), "\n\t", "\n", -1)
	conf := createConfFile(t, []byte(contents))
	defer os.Remove(conf)

	s, opts := RunServerWithConfig(conf)
	defer s.Shutdown()

	// Create a new server and route to main one.
	routeStr := fmt.Sprintf("\n\t\troutes = [nats-route://%s:%d]", opts.Cluster.Host, opts.Cluster.Port)
	contents2 := strings.Replace(fmt.Sprintf(cf, routeStr, sysPub, sysPub, sysJWT, svcPub, svcJWT, accPub, accJWT), "\n\t", "\n", -1)

	conf2 := createConfFile(t, []byte(contents2))
	defer os.Remove(conf2)

	s2, opts2 := RunServerWithConfig(conf2)
	defer s2.Shutdown()

	checkClusterFormed(t, s, s2)

	// Create service provider.
	url := fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port)
	copt, pubUser := createUserCredsOption(t, s, svcKP)
	nc, err := nats.Connect(url, copt, nats.Name("fooService"))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc.Close()

	// The service listener.
	serviceTime := 25 * time.Millisecond
	nc.Subscribe("req.echo", func(msg *nats.Msg) {
		time.Sleep(serviceTime)
		msg.Respond(msg.Data)
	})

	// Listen for metrics
	rsub, _ := nc.SubscribeSync("results")
	nc.Flush()

	// Create second client and send request from this one.
	url2 := fmt.Sprintf("nats://%s:%d/", opts2.Host, opts2.Port)
	nc2, err := nats.Connect(url2, createUserCreds(t, s2, accKP))
	if err != nil {
		t.Fatalf("Error creating client: %v\n", err)
	}
	defer nc2.Close()

	// Send the request.
	_, err = nc2.Request("request", []byte("hello"), time.Second)
	if err != nil {
		t.Fatalf("Expected a response")
	}

	// We should not receive latency at this time.
	_, err = rsub.NextMsg(100 * time.Millisecond)
	if err == nil {
		t.Fatalf("Did not expect to receive a latency metric")
	}

	// Now turn it on..
	updateAccount := func() {
		t.Helper()
		for _, s := range []*server.Server{s, s2} {
			svcAccount, err := s.LookupAccount(svcPub)
			if err != nil {
				t.Fatalf("Could not lookup service account from server %+v", s)
			}
			s.UpdateAccountClaims(svcAccount, svcAcc)
		}
	}
	serviceExport.Latency = &jwt.ServiceLatency{Sampling: 100, Results: "results"}
	updateAccount()

	// Grab the service responder's user.

	// Send the request.
	start := time.Now()
	_, err = nc2.Request("request", []byte("hello"), time.Second)
	if err != nil {
		t.Fatalf("Expected a response")
	}

	var sl server.ServiceLatency
	rmsg, err := rsub.NextMsg(time.Second)
	if err != nil || rmsg == nil {
		t.Fatalf("Did not receive a latency metric")
	}
	json.Unmarshal(rmsg.Data, &sl)
	checkServiceLatency(t, sl, start, serviceTime)
	extendedCheck(t, &sl.Responder, pubUser, "fooService", s.Name())
	// Normally requestor's don't share
	noShareCheck(t, &sl.Requestor)

	// Now we will remove tracking. Do this by simulating a JWT update.
	serviceExport.Latency = nil
	updateAccount()

	// Now we should not get any tracking data.
	_, err = nc2.Request("request", []byte("hello"), time.Second)
	if err != nil {
		t.Fatalf("Expected a response")
	}
	_, err = rsub.NextMsg(100 * time.Millisecond)
	if err == nil {
		t.Fatalf("Did not expect to receive a latency metric")
	}
}

func TestServiceLatencyAdjustNegativeLatencyValues(t *testing.T) {
	sc := createSuperCluster(t, 3, 2)
	defer sc.shutdown()

	// Now add in new service export to FOO and have bar import
	// that with tracking enabled.
	sc.setupLatencyTracking(t, 100)

	// Now we can setup and test, do single node only first.
	// This is the service provider.
	nc := clientConnect(t, sc.clusters[0].opts[0], "foo")
	defer nc.Close()

	// The service listener.
	nc.Subscribe("ngs.usage.*", func(msg *nats.Msg) {
		msg.Respond([]byte("22 msgs"))
	})

	// Listen for metrics
	rsub, err := nc.SubscribeSync("results")
	if err != nil {
		t.Fatal(err)
	}
	nc.Flush()

	// Requestor
	nc2 := clientConnect(t, sc.clusters[0].opts[0], "bar")
	defer nc2.Close()

	// Send the request.
	totalSamples := 50
	for i := 0; i < totalSamples; i++ {
		if _, err := nc2.Request("ngs.usage", []byte("1h"), time.Second); err != nil {
			t.Fatalf("Expected a response")
		}
	}

	var sl server.ServiceLatency
	for i := 0; i < totalSamples; i++ {
		rmsg, err := rsub.NextMsg(time.Second)
		if err != nil {
			t.Fatalf("Expected to receive latency metric: %d, %s", i, err)
		}
		if err := json.Unmarshal(rmsg.Data, &sl); err != nil {
			t.Errorf("Unexpected error processing latency metric: %s", err)
		}
		if sl.ServiceLatency < 0 {
			t.Fatalf("Unexpected negative latency value: %v", sl)
		}
	}
}

func TestServiceLatencyRemoteConnectAdjustNegativeValues(t *testing.T) {
	sc := createSuperCluster(t, 3, 2)
	defer sc.shutdown()

	// Now add in new service export to FOO and have bar import that with tracking enabled.
	sc.setupLatencyTracking(t, 100)

	// Now we can setup and test, do single node only first.
	// This is the service provider.
	nc := clientConnect(t, sc.clusters[0].opts[0], "foo")
	defer nc.Close()

	// The service listener.
	nc.Subscribe("ngs.usage.*", func(msg *nats.Msg) {
		msg.Respond([]byte("22 msgs"))
	})

	// Listen for metrics
	rsub, err := nc.SubscribeSync("results")
	if err != nil {
		t.Fatal(err)
	}
	nc.Flush()

	// Same Cluster Requestor
	nc2 := clientConnect(t, sc.clusters[0].opts[2], "bar")
	defer nc2.Close()

	// Gateway Requestor
	nc3 := clientConnect(t, sc.clusters[1].opts[1], "bar")
	defer nc3.Close()

	// Send a few initial requests to ensure interest is propagated
	// both for cluster and gateway requestors.
	checkFor(t, 3*time.Second, time.Second, func() error {
		_, err1 := nc2.Request("ngs.usage", []byte("1h"), time.Second)
		_, err2 := nc3.Request("ngs.usage", []byte("1h"), time.Second)

		if err1 != nil || err2 != nil {
			return fmt.Errorf("Timed out waiting for super cluster to be ready")
		}
		return nil
	})

	// Send the request.
	totalSamples := 20
	for i := 0; i < totalSamples; i++ {
		if _, err := nc2.Request("ngs.usage", []byte("1h"), time.Second); err != nil {
			t.Fatalf("Expected a response")
		}
	}

	for i := 0; i < totalSamples; i++ {
		if _, err := nc3.Request("ngs.usage", []byte("1h"), time.Second); err != nil {
			t.Fatalf("Expected a response")
		}
	}

	var sl server.ServiceLatency
	for i := 0; i < totalSamples*2; i++ {
		rmsg, err := rsub.NextMsg(time.Second)
		if err != nil {
			t.Fatalf("Expected to receive latency metric: %d, %s", i, err)
		}
		if err = json.Unmarshal(rmsg.Data, &sl); err != nil {
			t.Errorf("Unexpected error processing latency metric: %s", err)
		}
		if sl.ServiceLatency < 0 {
			t.Fatalf("Unexpected negative service latency value: %v", sl)
		}
		if sl.SystemLatency < 0 {
			t.Fatalf("Unexpected negative system latency value: %v", sl)
		}
	}
}

func TestServiceLatencyFailureReportingSingleServer(t *testing.T) {
	sc := createSuperCluster(t, 1, 1)
	defer sc.shutdown()

	// Now add in new service export to FOO and have bar import that with tracking enabled.
	sc.setupLatencyTracking(t, 100)
	sc.setResponseThreshold(t, 20*time.Millisecond)

	nc := clientConnect(t, sc.clusters[0].opts[0], "foo")
	defer nc.Close()

	// Listen for metrics
	rsub, err := nc.SubscribeSync("results")
	if err != nil {
		t.Fatal(err)
	}
	nc.Flush()

	getMetricResult := func() *server.ServiceLatency {
		t.Helper()
		rmsg, err := rsub.NextMsg(time.Second)
		if err != nil {
			t.Fatalf("Expected to receive latency metric: %v", err)
		}
		var sl server.ServiceLatency
		if err = json.Unmarshal(rmsg.Data, &sl); err != nil {
			t.Errorf("Unexpected error processing latency metric: %s", err)
		}
		return &sl
	}

	// Same server
	nc2 := clientConnect(t, sc.clusters[0].opts[0], "bar")
	defer nc2.Close()

	// Test a request with no reply subject
	nc2.Publish("ngs.usage", []byte("1h"))
	sl := getMetricResult()

	if sl.Status != 400 {
		t.Fatalf("Expected to get a bad request status [400], got %d", sl.Status)
	}

	// Proper request, but no responders.
	nc2.Request("ngs.usage", []byte("1h"), 20*time.Millisecond)
	sl = getMetricResult()
	if sl.Status != 503 {
		t.Fatalf("Expected to get a service unavailable status [503], got %d", sl.Status)
	}

	// The service listener. Make it slow. 10ms is respThreshold, so take 2X
	sub, _ := nc.Subscribe("ngs.usage.bar", func(msg *nats.Msg) {
		time.Sleep(20 * time.Millisecond)
		msg.Respond([]byte("22 msgs"))
	})
	nc.Flush()

	nc2.Request("ngs.usage", []byte("1h"), 10*time.Millisecond)
	sl = getMetricResult()
	if sl.Status != 504 {
		t.Fatalf("Expected to get a service timeout status [504], got %d", sl.Status)
	}

	// Make sure we do not get duplicates.
	if rmsg, err := rsub.NextMsg(50 * time.Millisecond); err == nil {
		t.Fatalf("Unexpected second response metric: %q\n", rmsg.Data)
	}

	// Now setup a responder that will respond under the threshold.
	sub.Unsubscribe()
	nc.Subscribe("ngs.usage.bar", func(msg *nats.Msg) {
		time.Sleep(5 * time.Millisecond)
		msg.Respond([]byte("22 msgs"))
	})
	nc.Flush()
	time.Sleep(100 * time.Millisecond)

	// Now create a responder using old request and we will do a short timeout.
	nc3 := clientConnectOldRequest(t, sc.clusters[0].opts[0], "bar")
	defer nc3.Close()

	nc3.Request("ngs.usage", []byte("1h"), time.Millisecond)
	sl = getMetricResult()
	if sl.Status != 408 {
		t.Fatalf("Expected to get a request timeout status [408], got %d", sl.Status)
	}
}

func TestServiceLatencyFailureReportingMultipleServers(t *testing.T) {
	sc := createSuperCluster(t, 3, 3)
	defer sc.shutdown()

	// Now add in new service export to FOO and have bar import that with tracking enabled.
	sc.setupLatencyTracking(t, 100)
	sc.setResponseThreshold(t, 10*time.Millisecond)

	nc := clientConnect(t, sc.clusters[0].opts[0], "foo")
	defer nc.Close()

	// Listen for metrics
	rsub, err := nc.SubscribeSync("results")
	if err != nil {
		t.Fatal(err)
	}
	nc.Flush()

	getMetricResult := func() *server.ServiceLatency {
		t.Helper()
		rmsg, err := rsub.NextMsg(time.Second)
		if err != nil {
			t.Fatalf("Expected to receive latency metric: %v", err)
		}
		var sl server.ServiceLatency
		if err = json.Unmarshal(rmsg.Data, &sl); err != nil {
			t.Errorf("Unexpected error processing latency metric: %s", err)
		}
		return &sl
	}

	cases := []struct {
		ci, si int
		desc   string
	}{
		{0, 0, "same server"},
		{0, 1, "same cluster, different server"},
		{1, 1, "different cluster"},
	}

	for _, cs := range cases {
		// Select the server to send request from.
		nc2 := clientConnect(t, sc.clusters[cs.ci].opts[cs.si], "bar")
		defer nc2.Close()

		// Test a request with no reply subject
		nc2.Publish("ngs.usage", []byte("1h"))
		sl := getMetricResult()
		if sl.Status != 400 {
			t.Fatalf("Test %q, Expected to get a bad request status [400], got %d", cs.desc, sl.Status)
		}

		// We wait here for the gateways to report no interest b/c optimistic mode.
		time.Sleep(50 * time.Millisecond)

		// Proper request, but no responders.
		nc2.Request("ngs.usage", []byte("1h"), 10*time.Millisecond)
		sl = getMetricResult()
		if sl.Status != 503 {
			t.Fatalf("Test %q, Expected to get a service unavailable status [503], got %d", cs.desc, sl.Status)
		}

		// The service listener. Make it slow. 10ms is respThreshold, so make 3X
		sub, _ := nc.Subscribe("ngs.usage.bar", func(msg *nats.Msg) {
			time.Sleep(30 * time.Millisecond)
			msg.Respond([]byte("22 msgs"))
		})
		defer sub.Unsubscribe()
		nc.Flush()
		// Wait to propagate.
		time.Sleep(200 * time.Millisecond)

		nc2.Request("ngs.usage", []byte("1h"), 10*time.Millisecond)
		sl = getMetricResult()
		if sl.Status != 504 {
			t.Fatalf("Test %q, Expected to get a service timeout status [504], got %d", cs.desc, sl.Status)
		}

		// Clean up subscriber and requestor
		nc2.Close()
		sub.Unsubscribe()
		nc.Flush()
		// Wait to propagate.
		time.Sleep(200 * time.Millisecond)
	}
}

// To test a bug rip@nats.io is seeing.
func TestServiceLatencyOldRequestStyleSingleServer(t *testing.T) {
	conf := createConfFile(t, []byte(`
		listen: 127.0.0.1:-1
		accounts: {
		    SVC: {
		        users: [ {user: svc, password: pass} ]
		        exports: [  {
					service: "svc.echo"
					accounts: [CLIENT]
					latency: {
						sampling: 100%
						subject: latency.svc
					}
				} ]
		    },
		    CLIENT: {
		        users: [{user: client, password: pass} ]
			    imports: [ {service: {account: SVC, subject: svc.echo}, to: SVC} ]
		    },
		    SYS: { users: [{user: admin, password: pass}] }
		}

		system_account: SYS
	`))
	defer os.Remove(conf)

	srv, opts := RunServerWithConfig(conf)
	defer srv.Shutdown()

	// Responder
	nc, err := nats.Connect(fmt.Sprintf("nats://svc:pass@%s:%d", opts.Host, opts.Port))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc.Close()

	// Listen for metrics
	rsub, _ := nc.SubscribeSync("latency.svc")

	// Requestor
	nc2, err := nats.Connect(fmt.Sprintf("nats://client:pass@%s:%d", opts.Host, opts.Port), nats.UseOldRequestStyle())
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc2.Close()

	// Setup responder
	serviceTime := 25 * time.Millisecond
	sub, _ := nc.Subscribe("svc.echo", func(msg *nats.Msg) {
		time.Sleep(serviceTime)
		msg.Respond([]byte("world"))
	})
	nc.Flush()
	defer sub.Unsubscribe()

	// Send a request
	start := time.Now()
	if _, err := nc2.Request("SVC", []byte("1h"), time.Second); err != nil {
		t.Fatalf("Expected a response")
	}

	var sl server.ServiceLatency
	rmsg, _ := rsub.NextMsg(time.Second)
	json.Unmarshal(rmsg.Data, &sl)

	checkServiceLatency(t, sl, start, serviceTime)
	extendedCheck(t, &sl.Responder, "svc", "", srv.Name())
	noShareCheck(t, &sl.Requestor)
}

// Check we get the proper detailed information for the requestor when allowed.
func TestServiceLatencyRequestorSharesDetailedInfo(t *testing.T) {
	sc := createSuperCluster(t, 3, 3)
	defer sc.shutdown()

	// Now add in new service export to FOO and have bar import that with tracking enabled.
	sc.setupLatencyTracking(t, 100)
	sc.setResponseThreshold(t, 10*time.Millisecond)
	sc.setImportShare(t)

	nc := clientConnect(t, sc.clusters[0].opts[0], "foo")
	defer nc.Close()

	// Listen for metrics
	rsub, err := nc.SubscribeSync("results")
	if err != nil {
		t.Fatal(err)
	}
	nc.Flush()

	getMetricResult := func() *server.ServiceLatency {
		t.Helper()
		rmsg, err := rsub.NextMsg(time.Second)
		if err != nil {
			t.Fatalf("Expected to receive latency metric: %v", err)
		}
		var sl server.ServiceLatency
		if err = json.Unmarshal(rmsg.Data, &sl); err != nil {
			t.Errorf("Unexpected error processing latency metric: %s", err)
		}
		return &sl
	}

	cases := []struct {
		ci, si int
		desc   string
	}{
		{0, 0, "same server"},
		{0, 1, "same cluster, different server"},
		{1, 1, "different cluster"},
	}

	for _, cs := range cases {
		// Select the server to send request from.
		nc2 := clientConnect(t, sc.clusters[cs.ci].opts[cs.si], "bar")
		defer nc2.Close()

		rs := sc.clusters[cs.ci].servers[cs.si]

		// Test a request with no reply subject
		nc2.Publish("ngs.usage", []byte("1h"))
		sl := getMetricResult()
		if sl.Status != 400 {
			t.Fatalf("Test %q, Expected to get a bad request status [400], got %d", cs.desc, sl.Status)
		}
		extendedCheck(t, &sl.Requestor, "bar", "", rs.Name())

		// We wait here for the gateways to report no interest b/c optimistic mode.
		time.Sleep(50 * time.Millisecond)

		// Proper request, but no responders.
		nc2.Request("ngs.usage", []byte("1h"), 10*time.Millisecond)
		sl = getMetricResult()
		if sl.Status != 503 {
			t.Fatalf("Test %q, Expected to get a service unavailable status [503], got %d", cs.desc, sl.Status)
		}
		extendedCheck(t, &sl.Requestor, "bar", "", rs.Name())

		// The service listener. Make it slow. 10ms is respThreshold, so take 2.5X
		sub, _ := nc.Subscribe("ngs.usage.bar", func(msg *nats.Msg) {
			time.Sleep(25 * time.Millisecond)
			msg.Respond([]byte("22 msgs"))
		})
		defer sub.Unsubscribe()
		nc.Flush()
		// Wait to propagate.
		time.Sleep(200 * time.Millisecond)

		nc2.Request("ngs.usage", []byte("1h"), 10*time.Millisecond)
		sl = getMetricResult()
		if sl.Status != 504 {
			t.Fatalf("Test %q, Expected to get a service timeout status [504], got %d", cs.desc, sl.Status)
		}
		extendedCheck(t, &sl.Requestor, "bar", "", rs.Name())

		// Clean up subscriber and requestor
		nc2.Close()
		sub.Unsubscribe()
		nc.Flush()
		// Wait to propagate.
		time.Sleep(200 * time.Millisecond)
	}
}

func TestServiceLatencyRequestorSharesConfig(t *testing.T) {
	conf := createConfFile(t, []byte(`
		listen: 127.0.0.1:-1
		accounts: {
		    SVC: {
		        users: [ {user: svc, password: pass} ]
		        exports: [  {
					service: "svc.echo"
					accounts: [CLIENT]
					latency: {
						sampling: 100%
						subject: latency.svc
					}
				} ]
		    },
		    CLIENT: {
		        users: [{user: client, password: pass} ]
			    imports: [ {service: {account: SVC, subject: svc.echo}, to: SVC, share:true} ]
		    },
		    SYS: { users: [{user: admin, password: pass}] }
		}

		system_account: SYS
	`))
	defer os.Remove(conf)

	srv, opts := RunServerWithConfig(conf)
	defer srv.Shutdown()

	// Responder
	nc, err := nats.Connect(fmt.Sprintf("nats://svc:pass@%s:%d", opts.Host, opts.Port))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc.Close()

	// Listen for metrics
	rsub, _ := nc.SubscribeSync("latency.svc")

	// Requestor
	nc2, err := nats.Connect(fmt.Sprintf("nats://client:pass@%s:%d", opts.Host, opts.Port), nats.UseOldRequestStyle())
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc2.Close()

	// Setup responder
	serviceTime := 25 * time.Millisecond
	sub, _ := nc.Subscribe("svc.echo", func(msg *nats.Msg) {
		time.Sleep(serviceTime)
		msg.Respond([]byte("world"))
	})
	nc.Flush()
	defer sub.Unsubscribe()

	// Send a request
	start := time.Now()
	if _, err := nc2.Request("SVC", []byte("1h"), time.Second); err != nil {
		t.Fatalf("Expected a response")
	}

	var sl server.ServiceLatency
	rmsg, _ := rsub.NextMsg(time.Second)
	json.Unmarshal(rmsg.Data, &sl)

	checkServiceLatency(t, sl, start, serviceTime)
	extendedCheck(t, &sl.Responder, "svc", "", srv.Name())
	extendedCheck(t, &sl.Requestor, "client", "", srv.Name())

	// Check reload.
	newConf := []byte(`
		listen: 127.0.0.1:-1
		accounts: {
		    SVC: {
		        users: [ {user: svc, password: pass} ]
		        exports: [  {
					service: "svc.echo"
					accounts: [CLIENT]
					latency: {
						sampling: 100%
						subject: latency.svc
					}
				} ]
		    },
		    CLIENT: {
		        users: [{user: client, password: pass} ]
			    imports: [ {service: {account: SVC, subject: svc.echo}, to: SVC} ]
		    },
		    SYS: { users: [{user: admin, password: pass}] }
		}

		system_account: SYS
	`)
	if err := ioutil.WriteFile(conf, newConf, 0600); err != nil {
		t.Fatalf("Error rewriting server's config file: %v", err)
	}
	if err := srv.Reload(); err != nil {
		t.Fatalf("Error on server reload: %v", err)
	}

	start = time.Now()
	if _, err = nc2.Request("SVC", []byte("1h"), time.Second); err != nil {
		t.Fatalf("Expected a response")
	}

	var sl2 server.ServiceLatency
	rmsg, _ = rsub.NextMsg(time.Second)
	json.Unmarshal(rmsg.Data, &sl2)
	noShareCheck(t, &sl2.Requestor)
}
