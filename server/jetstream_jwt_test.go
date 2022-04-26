// Copyright 2020-2022 The NATS Authors
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

//go:build !skip_js_tests
// +build !skip_js_tests

package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	jwt "github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
)

func TestJetStreamJWTLimits(t *testing.T) {
	updateJwt := func(url string, creds string, pubKey string, jwt string) {
		t.Helper()
		c := natsConnect(t, url, nats.UserCredentials(creds))
		defer c.Close()
		if msg, err := c.Request(fmt.Sprintf(accUpdateEventSubjNew, pubKey), []byte(jwt), time.Second); err != nil {
			t.Fatal("error not expected in this test", err)
		} else {
			content := make(map[string]interface{})
			if err := json.Unmarshal(msg.Data, &content); err != nil {
				t.Fatalf("%v", err)
			} else if _, ok := content["data"]; !ok {
				t.Fatalf("did not get an ok response got: %v", content)
			}
		}
	}
	require_IdenticalLimits := func(infoLim JetStreamAccountLimits, lim jwt.JetStreamLimits) {
		t.Helper()
		if int64(infoLim.MaxConsumers) != lim.Consumer || int64(infoLim.MaxStreams) != lim.Streams ||
			infoLim.MaxMemory != lim.MemoryStorage || infoLim.MaxStore != lim.DiskStorage {
			t.Fatalf("limits do not match %v != %v", infoLim, lim)
		}
	}
	expect_JSDisabledForAccount := func(c *nats.Conn) {
		t.Helper()
		if _, err := c.Request("$JS.API.INFO", nil, time.Second); err != nats.ErrTimeout && err != nats.ErrNoResponders {
			t.Fatalf("Unexpected error: %v", err)
		}
	}
	expect_InfoError := func(c *nats.Conn) {
		t.Helper()
		var info JSApiAccountInfoResponse
		if resp, err := c.Request("$JS.API.INFO", nil, time.Second); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		} else if err = json.Unmarshal(resp.Data, &info); err != nil {
			t.Fatalf("response1 %v got error %v", string(resp.Data), err)
		} else if info.Error == nil {
			t.Fatalf("expected error")
		}
	}
	validate_limits := func(c *nats.Conn, expectedLimits jwt.JetStreamLimits) {
		t.Helper()
		var info JSApiAccountInfoResponse
		if resp, err := c.Request("$JS.API.INFO", nil, time.Second); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		} else if err = json.Unmarshal(resp.Data, &info); err != nil {
			t.Fatalf("response1 %v got error %v", string(resp.Data), err)
		} else {
			require_IdenticalLimits(info.Limits, expectedLimits)
		}
	}
	// create system account
	sysKp, _ := nkeys.CreateAccount()
	sysPub, _ := sysKp.PublicKey()
	sysUKp, _ := nkeys.CreateUser()
	sysUSeed, _ := sysUKp.Seed()
	uclaim := newJWTTestUserClaims()
	uclaim.Subject, _ = sysUKp.PublicKey()
	sysUserJwt, err := uclaim.Encode(sysKp)
	require_NoError(t, err)
	sysKp.Seed()
	sysCreds := genCredsFile(t, sysUserJwt, sysUSeed)
	// limits to apply and check
	limits1 := jwt.JetStreamLimits{MemoryStorage: 1024 * 1024, DiskStorage: 2048 * 1024, Streams: 1, Consumer: 2, MaxBytesRequired: true}
	// has valid limits that would fail when incorrectly applied twice
	limits2 := jwt.JetStreamLimits{MemoryStorage: 4096 * 1024, DiskStorage: 8192 * 1024, Streams: 3, Consumer: 4}
	// limits exceeding actual configured value of DiskStorage
	limitsExceeded := jwt.JetStreamLimits{MemoryStorage: 8192 * 1024, DiskStorage: 16384 * 1024, Streams: 5, Consumer: 6}
	// create account using jetstream with both limits
	akp, _ := nkeys.CreateAccount()
	aPub, _ := akp.PublicKey()
	claim := jwt.NewAccountClaims(aPub)
	claim.Limits.JetStreamLimits = limits1
	aJwt1, err := claim.Encode(oKp)
	require_NoError(t, err)
	claim.Limits.JetStreamLimits = limits2
	aJwt2, err := claim.Encode(oKp)
	require_NoError(t, err)
	claim.Limits.JetStreamLimits = limitsExceeded
	aJwtLimitsExceeded, err := claim.Encode(oKp)
	require_NoError(t, err)
	claim.Limits.JetStreamLimits = jwt.JetStreamLimits{} // disabled
	aJwt4, err := claim.Encode(oKp)
	require_NoError(t, err)
	// account user
	uKp, _ := nkeys.CreateUser()
	uSeed, _ := uKp.Seed()
	uclaim = newJWTTestUserClaims()
	uclaim.Subject, _ = uKp.PublicKey()
	userJwt, err := uclaim.Encode(akp)
	require_NoError(t, err)
	userCreds := genCredsFile(t, userJwt, uSeed)
	dir := createDir(t, "srv")
	defer removeDir(t, dir)
	conf := createConfFile(t, []byte(fmt.Sprintf(`
		listen: 127.0.0.1:-1
		jetstream: {max_mem_store: 10Mb, max_file_store: 10Mb}
		operator: %s
		resolver: {
			type: full
			dir: '%s'
		}
		system_account: %s
    `, ojwt, dir, sysPub)))
	defer removeFile(t, conf)
	s, opts := RunServerWithConfig(conf)
	defer s.Shutdown()
	port := opts.Port
	updateJwt(s.ClientURL(), sysCreds, aPub, aJwt1)
	c := natsConnect(t, s.ClientURL(), nats.UserCredentials(userCreds), nats.ReconnectWait(200*time.Millisecond))
	defer c.Close()
	validate_limits(c, limits1)
	// keep using the same connection
	updateJwt(s.ClientURL(), sysCreds, aPub, aJwt2)
	validate_limits(c, limits2)
	// keep using the same connection but do NOT CHANGE anything.
	// This tests if the jwt is applied a second time (would fail)
	updateJwt(s.ClientURL(), sysCreds, aPub, aJwt2)
	validate_limits(c, limits2)
	// keep using the same connection. This update EXCEEDS LIMITS
	updateJwt(s.ClientURL(), sysCreds, aPub, aJwtLimitsExceeded)
	validate_limits(c, limits2)
	// disable test after failure
	updateJwt(s.ClientURL(), sysCreds, aPub, aJwt4)
	expect_InfoError(c)
	// re enable, again testing with a value that can't be applied twice
	updateJwt(s.ClientURL(), sysCreds, aPub, aJwt2)
	validate_limits(c, limits2)
	// disable test no prior failure
	updateJwt(s.ClientURL(), sysCreds, aPub, aJwt4)
	expect_InfoError(c)
	// Wrong limits form start
	updateJwt(s.ClientURL(), sysCreds, aPub, aJwtLimitsExceeded)
	expect_JSDisabledForAccount(c)
	// enable js but exceed limits. Followed by fix via restart
	updateJwt(s.ClientURL(), sysCreds, aPub, aJwt2)
	validate_limits(c, limits2)
	updateJwt(s.ClientURL(), sysCreds, aPub, aJwtLimitsExceeded)
	validate_limits(c, limits2)
	s.Shutdown()
	conf = createConfFile(t, []byte(fmt.Sprintf(`
		listen: 127.0.0.1:%d
		jetstream: {max_mem_store: 20Mb, max_file_store: 20Mb}
		operator: %s
		resolver: {
			type: full
			dir: '%s'
		}
		system_account: %s
    `, port, ojwt, dir, sysPub)))
	defer removeFile(t, conf)
	s, _ = RunServerWithConfig(conf)
	defer s.Shutdown()
	c.Flush() // force client to discover the disconnect
	checkClientsCount(t, s, 1)
	validate_limits(c, limitsExceeded)
	s.Shutdown()
	// disable jetstream test
	conf = createConfFile(t, []byte(fmt.Sprintf(`
		listen: 127.0.0.1:%d
		operator: %s
		resolver: {
			type: full
			dir: '%s'
		}
		system_account: %s
    `, port, ojwt, dir, sysPub)))
	defer removeFile(t, conf)
	s, _ = RunServerWithConfig(conf)
	defer s.Shutdown()
	c.Flush() // force client to discover the disconnect
	checkClientsCount(t, s, 1)
	expect_JSDisabledForAccount(c)
	// test that it stays disabled
	updateJwt(s.ClientURL(), sysCreds, aPub, aJwt2)
	expect_JSDisabledForAccount(c)
	c.Close()
}

func TestJetStreamJWTMoveWithTiers(t *testing.T) {
	_, syspub := createKey(t)
	sysJwt := encodeClaim(t, jwt.NewAccountClaims(syspub), syspub)

	accKp, aExpPub := createKey(t)
	accClaim := jwt.NewAccountClaims(aExpPub)
	accClaim.Name = "acc"
	accClaim.Limits.JetStreamTieredLimits["R1"] = jwt.JetStreamLimits{
		DiskStorage: 1100, Consumer: 1, Streams: 1}
	accClaim.Limits.JetStreamTieredLimits["R3"] = jwt.JetStreamLimits{
		DiskStorage: 3300, Consumer: 1, Streams: 1}
	accJwt := encodeClaim(t, accClaim, aExpPub)
	accCreds := newUser(t, accKp)

	test := func(t *testing.T, replicas int) {
		tmlp := `
			listen: 127.0.0.1:-1
			server_name: %s
			jetstream: {max_mem_store: 256MB, max_file_store: 2GB, store_dir: '%s'}
			leaf {
				listen: 127.0.0.1:-1
			}
			cluster {
				name: %s
				listen: 127.0.0.1:%d
				routes = [%s]
			}
		`
		s := createJetStreamSuperClusterWithTemplateAndModHook(t, tmlp, 3, 3,
			func(serverName, clustername, storeDir, conf string) string {
				return conf + fmt.Sprintf(`
					server_tags: [cloud:%s-tag]
					operator: %s
					system_account: %s
					resolver = MEMORY
					resolver_preload = {
						%s : %s
						%s : %s
					}
				`, clustername, ojwt, syspub, syspub, sysJwt, aExpPub, accJwt)
			})
		defer s.shutdown()

		nc := natsConnect(t, s.randomServer().ClientURL(), nats.UserCredentials(accCreds))
		defer nc.Close()

		js, err := nc.JetStream()
		require_NoError(t, err)

		ci, err := js.AddStream(&nats.StreamConfig{Name: "MOVE-ME", Replicas: replicas,
			Placement: &nats.Placement{Tags: []string{"cloud:C1-tag"}}})
		require_NoError(t, err)
		require_Equal(t, ci.Cluster.Name, "C1")
		ci, err = js.UpdateStream(&nats.StreamConfig{Name: "MOVE-ME", Replicas: replicas,
			Placement: &nats.Placement{Tags: []string{"cloud:C2-tag"}}})
		require_NoError(t, err)
		require_Equal(t, ci.Cluster.Name, "C1")

		checkFor(t, 10*time.Second, 10*time.Millisecond, func() error {
			if si, err := js.StreamInfo("MOVE-ME"); err != nil {
				return err
			} else if si.Cluster.Name != "C2" {
				return fmt.Errorf("Wrong cluster: %q", si.Cluster.Name)
			} else if si.Cluster.Leader == _EMPTY_ {
				return fmt.Errorf("No leader yet")
			} else if !strings.HasPrefix(si.Cluster.Leader, "C2-") {
				return fmt.Errorf("Wrong leader: %q", si.Cluster.Leader)
			} else if len(si.Cluster.Replicas) != replicas-1 {
				return fmt.Errorf("Expected %d replicas, got %d", replicas-1, len(si.Cluster.Replicas))
			}
			return nil
		})
	}

	t.Run("R1", func(t *testing.T) {
		test(t, 1)
	})
	t.Run("R3", func(t *testing.T) {
		test(t, 3)
	})
}

func TestJetStreamJWTClusteredTiers(t *testing.T) {
	sysKp, syspub := createKey(t)
	sysJwt := encodeClaim(t, jwt.NewAccountClaims(syspub), syspub)
	sysCreds := newUser(t, sysKp)
	defer removeFile(t, sysCreds)

	accKp, aExpPub := createKey(t)
	accClaim := jwt.NewAccountClaims(aExpPub)
	accClaim.Name = "acc"
	accClaim.Limits.JetStreamTieredLimits["R1"] = jwt.JetStreamLimits{
		DiskStorage: 1100, Consumer: 2, Streams: 2}
	accClaim.Limits.JetStreamTieredLimits["R3"] = jwt.JetStreamLimits{
		DiskStorage: 3300, Consumer: 1, Streams: 1}
	accJwt := encodeClaim(t, accClaim, aExpPub)
	accCreds := newUser(t, accKp)
	tmlp := `
		listen: 127.0.0.1:-1
		server_name: %s
		jetstream: {max_mem_store: 256MB, max_file_store: 2GB, store_dir: '%s'}
		leaf {
			listen: 127.0.0.1:-1
		}
		cluster {
			name: %s
			listen: 127.0.0.1:%d
			routes = [%s]
		}
	` + fmt.Sprintf(`
		operator: %s
		system_account: %s
		resolver = MEMORY
		resolver_preload = {
			%s : %s
			%s : %s
		}
	`, ojwt, syspub, syspub, sysJwt, aExpPub, accJwt)

	c := createJetStreamClusterWithTemplate(t, tmlp, "cluster", 3)
	defer c.shutdown()

	nc := natsConnect(t, c.randomServer().ClientURL(), nats.UserCredentials(accCreds))
	defer nc.Close()

	js, err := nc.JetStream()
	require_NoError(t, err)

	// Test absent tiers
	_, err = js.AddStream(&nats.StreamConfig{Name: "testR2", Replicas: 2, Subjects: []string{"testR2"}})
	require_Error(t, err)
	require_Equal(t, err.Error(), "no JetStream default or applicable tiered limit present")
	_, err = js.AddStream(&nats.StreamConfig{Name: "testR5", Replicas: 5, Subjects: []string{"testR5"}})
	require_Error(t, err)
	require_Equal(t, err.Error(), "no JetStream default or applicable tiered limit present")

	// Test tiers up to stream limits
	_, err = js.AddStream(&nats.StreamConfig{Name: "testR1-1", Replicas: 1, Subjects: []string{"testR1-1"}})
	require_NoError(t, err)
	_, err = js.AddStream(&nats.StreamConfig{Name: "testR3-1", Replicas: 3, Subjects: []string{"testR3-1"}})
	require_NoError(t, err)
	_, err = js.AddStream(&nats.StreamConfig{Name: "testR1-2", Replicas: 1, Subjects: []string{"testR1-2"}})
	require_NoError(t, err)

	// Test exceeding tiered stream limit
	_, err = js.AddStream(&nats.StreamConfig{Name: "testR1-3", Replicas: 1, Subjects: []string{"testR1-3"}})
	require_Error(t, err)
	require_Equal(t, err.Error(), "maximum number of streams reached")
	_, err = js.AddStream(&nats.StreamConfig{Name: "testR3-3", Replicas: 3, Subjects: []string{"testR3-3"}})
	require_Error(t, err)
	require_Equal(t, err.Error(), "maximum number of streams reached")

	// Test tiers up to consumer limits
	_, err = js.AddConsumer("testR1-1", &nats.ConsumerConfig{Durable: "dur1", AckPolicy: nats.AckExplicitPolicy})
	require_NoError(t, err)
	_, err = js.AddConsumer("testR3-1", &nats.ConsumerConfig{Durable: "dur2", AckPolicy: nats.AckExplicitPolicy})
	require_NoError(t, err)
	_, err = js.AddConsumer("testR1-1", &nats.ConsumerConfig{Durable: "dur3", AckPolicy: nats.AckExplicitPolicy})
	require_NoError(t, err)

	// test exceeding tiered consumer limits
	_, err = js.AddConsumer("testR1-1", &nats.ConsumerConfig{Durable: "dur4", AckPolicy: nats.AckExplicitPolicy})
	require_Error(t, err)
	require_Equal(t, err.Error(), "maximum consumers limit reached")
	_, err = js.AddConsumer("testR1-1", &nats.ConsumerConfig{Durable: "dur5", AckPolicy: nats.AckExplicitPolicy})
	require_Error(t, err)
	require_Equal(t, err.Error(), "maximum consumers limit reached")

	// test tiered storage limit
	msg := [512]byte{}
	_, err = js.Publish("testR1-1", msg[:])
	require_NoError(t, err)
	_, err = js.Publish("testR3-1", msg[:])
	require_NoError(t, err)
	_, err = js.Publish("testR3-1", msg[:])
	require_NoError(t, err)
	_, err = js.Publish("testR1-2", msg[:])
	require_NoError(t, err)

	time.Sleep(2000 * time.Millisecond) // wait for update timer to synchronize totals

	// test exceeding tiered storage limit
	_, err = js.Publish("testR1-1", []byte("1"))
	require_Error(t, err)
	require_Equal(t, err.Error(), "nats: resource limits exceeded for account")
	_, err = js.Publish("testR3-1", []byte("fail this message!"))
	require_Error(t, err)
	require_Equal(t, err.Error(), "nats: resource limits exceeded for account")

	// retrieve limits
	var info JSApiAccountInfoResponse
	m, err := nc.Request("$JS.API.INFO", nil, time.Second)
	require_NoError(t, err)
	err = json.Unmarshal(m.Data, &info)
	require_NoError(t, err)

	require_True(t, info.Memory == 0)
	// R1 streams fail message with an add followed by remove, if the update was sent in between, the count is > limit
	// Alternative to checking both values is, prior to the info request, wait for another update
	require_True(t, info.Store == 4400 || info.Store == 4439)
	require_True(t, info.Streams == 3)
	require_True(t, info.Consumers == 3)
	require_True(t, info.Limits == JetStreamAccountLimits{})
	r1 := info.Tiers["R1"]
	require_True(t, r1.Streams == 2)
	require_True(t, r1.Consumers == 2)
	// R1 streams fail message with an add followed by remove, if the update was sent in between, the count is > limit
	// Alternative to checking both values is, prior to the info request, wait for another update
	require_True(t, r1.Store == 1100 || r1.Store == 1139)
	require_True(t, r1.Memory == 0)
	require_True(t, r1.Limits == JetStreamAccountLimits{
		MaxMemory:            0,
		MaxStore:             1100,
		MaxStreams:           2,
		MaxConsumers:         2,
		MaxAckPending:        -1,
		MemoryMaxStreamBytes: -1,
		StoreMaxStreamBytes:  -1,
		MaxBytesRequired:     false,
	})
	r3 := info.Tiers["R3"]
	require_True(t, r3.Streams == 1)
	require_True(t, r3.Consumers == 1)
	require_True(t, r3.Store == 3300)
	require_True(t, r3.Memory == 0)
	require_True(t, r3.Limits == JetStreamAccountLimits{
		MaxMemory:            0,
		MaxStore:             3300,
		MaxStreams:           1,
		MaxConsumers:         1,
		MaxAckPending:        -1,
		MemoryMaxStreamBytes: -1,
		StoreMaxStreamBytes:  -1,
		MaxBytesRequired:     false,
	})
}

func TestJetStreamJWTClusteredTiersChange(t *testing.T) {
	sysKp, syspub := createKey(t)
	sysJwt := encodeClaim(t, jwt.NewAccountClaims(syspub), syspub)
	sysCreds := newUser(t, sysKp)
	defer removeFile(t, sysCreds)

	accKp, aExpPub := createKey(t)
	accClaim := jwt.NewAccountClaims(aExpPub)
	accClaim.Name = "acc"
	accClaim.Limits.JetStreamTieredLimits["R1"] = jwt.JetStreamLimits{
		DiskStorage: 1000, MemoryStorage: 0, Consumer: 1, Streams: 1}
	accClaim.Limits.JetStreamTieredLimits["R3"] = jwt.JetStreamLimits{
		DiskStorage: 1500, MemoryStorage: 0, Consumer: 1, Streams: 1}
	accJwt1 := encodeClaim(t, accClaim, aExpPub)
	accCreds := newUser(t, accKp)
	start := time.Now()

	tmlp := `
		listen: 127.0.0.1:-1
		server_name: %s
		jetstream: {max_mem_store: 256MB, max_file_store: 2GB, store_dir: '%s'}
		leaf {
			listen: 127.0.0.1:-1
		}
		cluster {
			name: %s
			listen: 127.0.0.1:%d
			routes = [%s]
		}
	`
	c := createJetStreamClusterWithTemplateAndModHook(t, tmlp, "cluster", 3,
		func(serverName, clustername, storeDir, conf string) string {
			return conf + fmt.Sprintf(`
				operator: %s
				system_account: %s
				resolver: {
					type: full
					dir: '%s/jwt'
				}`, ojwt, syspub, storeDir)
		})
	defer c.shutdown()

	updateJwt(t, c.randomServer().ClientURL(), sysCreds, sysJwt, 3)
	updateJwt(t, c.randomServer().ClientURL(), sysCreds, accJwt1, 3)

	nc := natsConnect(t, c.randomServer().ClientURL(), nats.UserCredentials(accCreds))
	defer nc.Close()

	js, err := nc.JetStream()
	require_NoError(t, err)

	// Test tiers up to stream limits
	cfg := &nats.StreamConfig{Name: "testR1-1", Replicas: 1, Subjects: []string{"testR1-1"}, MaxBytes: 1000}
	_, err = js.AddStream(cfg)
	require_NoError(t, err)

	cfg.Replicas = 3
	_, err = js.UpdateStream(cfg)
	require_Error(t, err)
	require_Equal(t, err.Error(), "insufficient storage resources available")

	time.Sleep(time.Second - time.Since(start)) // make sure the time stamp changes
	accClaim.Limits.JetStreamTieredLimits["R3"] = jwt.JetStreamLimits{
		DiskStorage: 3000, MemoryStorage: 0, Consumer: 1, Streams: 1}
	accJwt2 := encodeClaim(t, accClaim, aExpPub)

	updateJwt(t, c.randomServer().ClientURL(), sysCreds, accJwt2, 3)

	var rBefore, rAfter JSApiAccountInfoResponse
	m, err := nc.Request("$JS.API.INFO", nil, time.Second)
	require_NoError(t, err)
	err = json.Unmarshal(m.Data, &rBefore)
	require_NoError(t, err)
	_, err = js.UpdateStream(cfg)
	require_NoError(t, err)

	m, err = nc.Request("$JS.API.INFO", nil, time.Second)
	require_NoError(t, err)
	err = json.Unmarshal(m.Data, &rAfter)
	require_NoError(t, err)
	require_True(t, rBefore.Tiers["R1"].Streams == 1)
	require_True(t, rBefore.Tiers["R1"].Streams == rAfter.Tiers["R3"].Streams)
	require_True(t, rBefore.Tiers["R3"].Streams == 0)
	require_True(t, rAfter.Tiers["R1"].Streams == 0)
}

func TestJetStreamJWTClusteredDeleteTierWithStreamAndMove(t *testing.T) {
	sysKp, syspub := createKey(t)
	sysJwt := encodeClaim(t, jwt.NewAccountClaims(syspub), syspub)
	sysCreds := newUser(t, sysKp)
	defer removeFile(t, sysCreds)

	accKp, aExpPub := createKey(t)
	accClaim := jwt.NewAccountClaims(aExpPub)
	accClaim.Name = "acc"
	accClaim.Limits.JetStreamTieredLimits["R1"] = jwt.JetStreamLimits{
		DiskStorage: 1000, MemoryStorage: 0, Consumer: 1, Streams: 1}
	accClaim.Limits.JetStreamTieredLimits["R3"] = jwt.JetStreamLimits{
		DiskStorage: 3000, MemoryStorage: 0, Consumer: 1, Streams: 1}
	accJwt1 := encodeClaim(t, accClaim, aExpPub)
	accCreds := newUser(t, accKp)
	start := time.Now()

	tmlp := `
		listen: 127.0.0.1:-1
		server_name: %s
		jetstream: {max_mem_store: 256MB, max_file_store: 2GB, store_dir: '%s'}
		leaf {
			listen: 127.0.0.1:-1
		}
		cluster {
			name: %s
			listen: 127.0.0.1:%d
			routes = [%s]
		}
	`
	c := createJetStreamClusterWithTemplateAndModHook(t, tmlp, "cluster", 3,
		func(serverName, clustername, storeDir, conf string) string {
			return conf + fmt.Sprintf(`
				operator: %s
				system_account: %s
				resolver: {
					type: full
					dir: '%s/jwt'
				}`, ojwt, syspub, storeDir)
		})
	defer c.shutdown()

	updateJwt(t, c.randomServer().ClientURL(), sysCreds, sysJwt, 3)
	updateJwt(t, c.randomServer().ClientURL(), sysCreds, accJwt1, 3)

	nc := natsConnect(t, c.randomServer().ClientURL(), nats.UserCredentials(accCreds))
	defer nc.Close()

	js, err := nc.JetStream()
	require_NoError(t, err)

	// Test tiers up to stream limits
	cfg := &nats.StreamConfig{Name: "testR1-1", Replicas: 1, Subjects: []string{"testR1-1"}, MaxBytes: 1000}
	_, err = js.AddStream(cfg)
	require_NoError(t, err)

	_, err = js.Publish("testR1-1", nil)
	require_NoError(t, err)

	time.Sleep(time.Second - time.Since(start)) // make sure the time stamp changes
	delete(accClaim.Limits.JetStreamTieredLimits, "R1")
	accJwt2 := encodeClaim(t, accClaim, aExpPub)
	updateJwt(t, c.randomServer().ClientURL(), sysCreds, accJwt2, 3)

	var respBefore JSApiAccountInfoResponse
	m, err := nc.Request("$JS.API.INFO", nil, time.Second)
	require_NoError(t, err)
	err = json.Unmarshal(m.Data, &respBefore)
	require_NoError(t, err)

	require_True(t, respBefore.JetStreamAccountStats.Tiers["R3"].Streams == 0)
	require_True(t, respBefore.JetStreamAccountStats.Tiers["R1"].Streams == 1)

	_, err = js.Publish("testR1-1", nil)
	require_Error(t, err)
	require_Equal(t, err.Error(), "nats: no JetStream default or applicable tiered limit present")

	cfg.Replicas = 3
	_, err = js.UpdateStream(cfg)
	require_NoError(t, err)

	// I noticed this taking > 5 seconds
	checkFor(t, 10*time.Second, 250*time.Millisecond, func() error {
		_, err = js.Publish("testR1-1", nil)
		return err
	})

	var respAfter JSApiAccountInfoResponse
	m, err = nc.Request("$JS.API.INFO", nil, time.Second)
	require_NoError(t, err)
	err = json.Unmarshal(m.Data, &respAfter)
	require_NoError(t, err)

	require_True(t, respAfter.JetStreamAccountStats.Tiers["R3"].Streams == 1)
	require_True(t, respAfter.JetStreamAccountStats.Tiers["R3"].Store > 0)

	_, ok := respAfter.JetStreamAccountStats.Tiers["R1"]
	require_True(t, !ok)
}

func TestJetStreamJWTSysAccUpdateMixedMode(t *testing.T) {
	skp, spub := createKey(t)
	sUsr := createUserCreds(t, nil, skp)
	sysClaim := jwt.NewAccountClaims(spub)
	sysClaim.Name = "SYS"
	sysJwt := encodeClaim(t, sysClaim, spub)
	encodeJwt1Time := time.Now()

	akp, apub := createKey(t)
	aUsr := createUserCreds(t, nil, akp)
	claim := jwt.NewAccountClaims(apub)
	claim.Limits.JetStreamLimits.DiskStorage = 1024 * 1024
	claim.Limits.JetStreamLimits.Streams = 1
	jwt1 := encodeClaim(t, claim, apub)

	basePath := "/ngs/v1/accounts/jwt/"
	reqCount := int32(0)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == basePath {
			w.Write([]byte("ok"))
		} else if strings.HasSuffix(r.URL.Path, spub) {
			w.Write([]byte(sysJwt))
		} else if strings.HasSuffix(r.URL.Path, apub) {
			w.Write([]byte(jwt1))
		} else {
			// only count requests that could be filled
			return
		}
		atomic.AddInt32(&reqCount, 1)
	}))
	defer ts.Close()

	tmpl := `
		listen: 127.0.0.1:-1
		server_name: %s
		jetstream: {max_mem_store: 256MB, max_file_store: 2GB, store_dir: '%s'}
		cluster {
			name: %s
			listen: 127.0.0.1:%d
			routes = [%s]
		}
    `

	sc := createJetStreamSuperClusterWithTemplateAndModHook(t, tmpl, 3, 2,
		func(serverName, clusterName, storeDir, conf string) string {
			// create an ngs like setup, with connection and non connection server
			if clusterName == "C1" {
				conf = strings.ReplaceAll(conf, "jetstream", "#jetstream")
			}
			return fmt.Sprintf(`%s
				operator: %s
				system_account: %s
				resolver: URL("%s%s")`, conf, ojwt, spub, ts.URL, basePath)
		})
	defer sc.shutdown()
	disconnectChan := make(chan struct{}, 100)
	defer close(disconnectChan)
	disconnectCb := nats.DisconnectErrHandler(func(conn *nats.Conn, err error) {
		disconnectChan <- struct{}{}
	})

	s := sc.clusterForName("C1").randomServer()

	sysNc := natsConnect(t, s.ClientURL(), sUsr, disconnectCb, nats.NoCallbacksAfterClientClose())
	defer sysNc.Close()
	aNc := natsConnect(t, s.ClientURL(), aUsr, disconnectCb, nats.NoCallbacksAfterClientClose())
	defer aNc.Close()

	js, err := aNc.JetStream()
	require_NoError(t, err)

	si, err := js.AddStream(&nats.StreamConfig{Name: "bar", Subjects: []string{"bar"}, Replicas: 3})
	require_NoError(t, err)
	require_Equal(t, si.Cluster.Name, "C2")
	_, err = js.AccountInfo()
	require_NoError(t, err)

	r, err := sysNc.Request(fmt.Sprintf(serverPingReqSubj, "ACCOUNTZ"),
		[]byte(fmt.Sprintf(`{"account":"%s"}`, spub)), time.Second)
	require_NoError(t, err)
	respb := ServerAPIResponse{Data: &Accountz{}}
	require_NoError(t, json.Unmarshal(r.Data, &respb))

	hasJSExp := func(resp *ServerAPIResponse) bool {
		found := false
		for _, e := range resp.Data.(*Accountz).Account.Exports {
			if e.Subject == jsAllAPI {
				found = true
				break
			}
		}
		return found
	}
	require_True(t, hasJSExp(&respb))

	// make sure jti increased
	time.Sleep(time.Second - time.Since(encodeJwt1Time))
	sysJwt2 := encodeClaim(t, sysClaim, spub)

	oldRcount := atomic.LoadInt32(&reqCount)
	_, err = sysNc.Request(fmt.Sprintf(accUpdateEventSubjNew, spub), []byte(sysJwt2), time.Second)
	require_NoError(t, err)
	// test to make sure connected client (aNc) was not kicked
	time.Sleep(200 * time.Millisecond)
	require_True(t, len(disconnectChan) == 0)

	// ensure nothing new has happened, lookup for account not found is skipped during inc
	require_True(t, atomic.LoadInt32(&reqCount) == oldRcount)
	// no responders
	_, err = aNc.Request("foo", nil, time.Second)
	require_Error(t, err)
	require_Equal(t, err.Error(), "nats: no responders available for request")

	nc2, js2 := jsClientConnect(t, sc.clusterForName("C2").randomServer(), aUsr)
	defer nc2.Close()
	_, err = js2.AccountInfo()
	require_NoError(t, err)

	r, err = sysNc.Request(fmt.Sprintf(serverPingReqSubj, "ACCOUNTZ"),
		[]byte(fmt.Sprintf(`{"account":"%s"}`, spub)), time.Second)
	require_NoError(t, err)
	respa := ServerAPIResponse{Data: &Accountz{}}
	require_NoError(t, json.Unmarshal(r.Data, &respa))
	require_True(t, hasJSExp(&respa))

	_, err = js.AccountInfo()
	require_NoError(t, err)
}
