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

package test

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"sync"
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
		c := createClusterEx(t, true, randClusterName(), numServersPer, clusters...)
		clusters = append(clusters, c)
	}
	return &supercluster{clusters}
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

func checkServiceLatency(t *testing.T, sl server.ServiceLatency, start time.Time, serviceTime time.Duration) {
	t.Helper()

	serviceTime = serviceTime.Round(time.Millisecond)

	startDelta := sl.RequestStart.Sub(start)
	if startDelta > 5*time.Millisecond {
		t.Fatalf("Bad start delta %v", startDelta)
	}
	if sl.ServiceLatency < time.Duration(float64(serviceTime)*0.8) {
		t.Fatalf("Bad service latency: %v (%v)", sl.ServiceLatency, serviceTime)
	}
	if sl.TotalLatency < sl.ServiceLatency {
		t.Fatalf("Bad total latency: %v (%v)", sl.TotalLatency, sl.ServiceLatency)
	}

	// We should have NATS latency here that is non-zero with real clients.
	if sl.NATSLatency.Requestor == 0 {
		t.Fatalf("Expected non-zero NATS Requestor latency")
	}
	if sl.NATSLatency.Responder == 0 {
		t.Fatalf("Expected non-zero NATS Requestor latency")
	}

	// Make sure they add up
	got := sl.TotalLatency
	expected := sl.ServiceLatency + sl.NATSLatency.TotalTime()
	if got != expected {
		t.Fatalf("Numbers do not add up: %+v,\ngot: %v\nexpected: %v", sl, got, expected)
	}
}

func TestServiceLatencySingleServerConnect(t *testing.T) {
	sc := createSuperCluster(t, 3, 2)
	defer sc.shutdown()

	// Now add in new service export to FOO and have bar import that with tracking enabled.
	sc.setupLatencyTracking(t, 100)

	// Now we can setup and test, do single node only first.
	// This is the service provider.
	nc := clientConnect(t, sc.clusters[0].opts[0], "foo")
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
	_, err := nc2.Request("ngs.usage", []byte("1h"), time.Second)
	if err != nil {
		t.Fatalf("Expected a response")
	}

	var sl server.ServiceLatency
	rmsg, _ := rsub.NextMsg(time.Second)
	json.Unmarshal(rmsg.Data, &sl)

	checkServiceLatency(t, sl, start, serviceTime)
}

// If a client has a longer RTT that the effective RTT for NATS + responder
// the requestor RTT will be marked as 0. This can happen quite often with
// utility programs that are far away from a cluster like NGS but the service
// response time has a shorter RTT.
func TestServiceLatencyClientRTTSlowerVsServiceRTT(t *testing.T) {
	sc := createSuperCluster(t, 2, 2)
	defer sc.shutdown()

	// Now add in new service export to FOO and have bar import that with tracking enabled.
	sc.setupLatencyTracking(t, 100)

	nc := clientConnect(t, sc.clusters[0].opts[0], "foo")
	defer nc.Close()

	// The service listener. Instant response.
	nc.Subscribe("ngs.usage.*", func(msg *nats.Msg) {
		time.Sleep(time.Millisecond)
		msg.Respond([]byte("22 msgs"))
	})

	// Listen for metrics
	rsub, _ := nc.SubscribeSync("results")

	nc.Flush()

	// Requestor and processing
	requestAndCheck := func(sopts *server.Options) {
		rtt := 10 * time.Millisecond
		sp, opts := newSlowProxy(rtt, sopts)
		defer sp.Stop()

		nc2 := clientConnect(t, opts, "bar")
		defer nc2.Close()

		start := time.Now()
		nc2.Flush()
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
		rmsg, _ := rsub.NextMsg(time.Second)
		json.Unmarshal(rmsg.Data, &sl)

		// We want to test here that when the client requestor RTT is larger then the response time
		// we still deliver a requestor value > 0.
		// Now check that it is close to rtt.
		if sl.NATSLatency.Requestor < rtt {
			t.Fatalf("Expected requestor latency to be > %v, got %v", rtt, sl.NATSLatency.Requestor)
		}
		if sl.TotalLatency < rtt {
			t.Fatalf("Expected total latency to be > %v, got %v", rtt, sl.TotalLatency)
		}
	}

	// Check same server.
	requestAndCheck(sc.clusters[0].opts[0])
	// Check from remote server across GW.
	requestAndCheck(sc.clusters[1].opts[1])
	// Same cluster but different server
	requestAndCheck(sc.clusters[0].opts[1])
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

	// The service listener.
	serviceTime := 25 * time.Millisecond
	nc.Subscribe("ngs.usage.*", func(msg *nats.Msg) {
		time.Sleep(serviceTime)
		msg.Respond([]byte("22 msgs"))
	})

	// Listen for metrics
	rsub, _ := nc.SubscribeSync("results")

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

	// Lastly here, we need to make sure we are properly tracking the extra hops.
	// We will make sure that NATS latency is close to what we see from the outside in terms of RTT.
	if crtt := connRTT(nc) + connRTT(nc2); sl.NATSLatency.TotalTime() < crtt {
		t.Fatalf("Not tracking second measurement for NATS latency across servers: %v vs %v", sl.NATSLatency.TotalTime(), crtt)
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

	// Lastly here, we need to make sure we are properly tracking the extra hops.
	// We will make sure that NATS latency is close to what we see from the outside in terms of RTT.
	if crtt := connRTT(nc) + connRTT(nc2); sl.NATSLatency.TotalTime() < crtt {
		t.Fatalf("Not tracking second measurement for NATS latency across servers: %v vs %v", sl.NATSLatency.TotalTime(), crtt)
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

	// We are adding 2 here for the wildcard response subject for service replies.
	// we only have one but it will show in two places.
	startSubs += 2

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
	if sl.AppName != "dlc22" {
		t.Fatalf("Expected to have AppName set correctly, %q vs %q", "dlc22", sl.AppName)
	}
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
	if sl.AppName != "dlc22" {
		t.Fatalf("Expected to have AppName set correctly, %q vs %q", "dlc22", sl.AppName)
	}
}

func TestServiceLatencyWithQueueSubscribersAndNames(t *testing.T) {
	numServers := 3
	numClusters := 3
	sc := createSuperCluster(t, numServers, numClusters)
	defer sc.shutdown()

	// Now add in new service export to FOO and have bar import that with tracking enabled.
	sc.setupLatencyTracking(t, 100)

	selectServer := func() *server.Options {
		si, ci := rand.Int63n(int64(numServers)), rand.Int63n(int64(numServers))
		return sc.clusters[ci].opts[si]
	}

	sname := func(i int) string {
		return fmt.Sprintf("SERVICE-%d", i+1)
	}

	numResponders := 5

	// Create 10 queue subscribers for the service. Randomly select the server.
	for i := 0; i < numResponders; i++ {
		nc := clientConnectWithName(t, selectServer(), "foo", sname(i))
		nc.QueueSubscribe("ngs.usage.*", "SERVICE", func(msg *nats.Msg) {
			time.Sleep(time.Duration(rand.Int63n(10)) * time.Millisecond)
			msg.Respond([]byte("22 msgs"))
		})
		nc.Flush()
	}

	doRequest := func() {
		nc := clientConnect(t, selectServer(), "bar")
		if _, err := nc.Request("ngs.usage", []byte("1h"), time.Second); err != nil {
			t.Fatalf("Failed to get request response: %v", err)
		}
		nc.Close()
	}

	// To collect the metrics
	nc := clientConnect(t, sc.clusters[0].opts[0], "foo")
	defer nc.Close()

	results := make(map[string]time.Duration)
	var rlock sync.Mutex
	ch := make(chan (bool))
	received := int32(0)
	toSend := int32(100)

	// Capture the results.
	nc.Subscribe("results", func(msg *nats.Msg) {
		var sl server.ServiceLatency
		json.Unmarshal(msg.Data, &sl)
		rlock.Lock()
		results[sl.AppName] += sl.ServiceLatency
		rlock.Unlock()
		if r := atomic.AddInt32(&received, 1); r >= toSend {
			ch <- true
		}
	})
	nc.Flush()

	// Send 100 requests from random locations.
	for i := 0; i < 100; i++ {
		doRequest()
	}

	// Wait on all results.
	<-ch

	rlock.Lock()
	defer rlock.Unlock()

	// Make sure each total is generally over 10ms
	thresh := 10 * time.Millisecond
	for i := 0; i < numResponders; i++ {
		if rl := results[sname(i)]; rl < thresh {
			t.Fatalf("Total for %q is less then threshold: %v vs %v", sname(i), thresh, rl)
		}
	}
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
	nc, err := nats.Connect(url, createUserCreds(t, s, svcKP))
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
		// time.Sleep(serviceTime)
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
		if sl.NATSLatency.System < 0 {
			t.Fatalf("Unexpected negative system latency value: %v", sl)
		}
	}
}
