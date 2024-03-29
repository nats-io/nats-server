// Copyright 2020-2023 The NATS Authors
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
	"errors"
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
			content := make(map[string]any)
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
	dir := t.TempDir()
	conf := createConfFile(t, []byte(fmt.Sprintf(`
		listen: 127.0.0.1:-1
		jetstream: {max_mem_store: 10Mb, max_file_store: 10Mb, store_dir: "%s"}
		operator: %s
		resolver: {
			type: full
			dir: '%s'
		}
		system_account: %s
    `, dir, ojwt, dir, sysPub)))
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
		jetstream: {max_mem_store: 20Mb, max_file_store: 20Mb, store_dir: "%s"}
		operator: %s
		resolver: {
			type: full
			dir: '%s'
		}
		system_account: %s
    `, port, dir, ojwt, dir, sysPub)))
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

func TestJetStreamJWTDisallowBearer(t *testing.T) {
	sysKp, syspub := createKey(t)
	sysJwt := encodeClaim(t, jwt.NewAccountClaims(syspub), syspub)
	sysCreds := newUser(t, sysKp)

	accKp, err := nkeys.CreateAccount()
	require_NoError(t, err)
	accIdPub, err := accKp.PublicKey()
	require_NoError(t, err)
	aClaim := jwt.NewAccountClaims(accIdPub)
	accJwt1, err := aClaim.Encode(oKp)
	require_NoError(t, err)
	aClaim.Limits.DisallowBearer = true
	accJwt2, err := aClaim.Encode(oKp)
	require_NoError(t, err)

	uc := jwt.NewUserClaims("dummy")
	uc.BearerToken = true
	uOpt1 := createUserCredsEx(t, uc, accKp)
	uc.BearerToken = false
	uOpt2 := createUserCredsEx(t, uc, accKp)

	dir := t.TempDir()
	cf := createConfFile(t, []byte(fmt.Sprintf(`
		port: -1
		operator = %s
		system_account: %s
		resolver: {
			type: full
			dir: '%s/jwt'
		}
		resolver_preload = {
			%s : "%s"
		}
		`, ojwt, syspub, dir, syspub, sysJwt)))
	s, _ := RunServerWithConfig(cf)
	defer s.Shutdown()

	updateJwt(t, s.ClientURL(), sysCreds, accJwt1, 1)
	disconnectErrCh := make(chan error, 10)
	defer close(disconnectErrCh)
	nc1, err := nats.Connect(s.ClientURL(), uOpt1,
		nats.NoReconnect(),
		nats.ErrorHandler(func(conn *nats.Conn, s *nats.Subscription, err error) {
			disconnectErrCh <- err
		}))
	require_NoError(t, err)
	defer nc1.Close()

	// update jwt and observe bearer token get disconnected
	updateJwt(t, s.ClientURL(), sysCreds, accJwt2, 1)
	select {
	case err := <-disconnectErrCh:
		require_Contains(t, err.Error(), "authorization violation")
	case <-time.After(time.Second):
		t.Fatalf("expected error on disconnect")
	}

	// assure bearer token is not allowed to connect
	_, err = nats.Connect(s.ClientURL(), uOpt1)
	require_Error(t, err)

	// assure non bearer token can connect
	nc2, err := nats.Connect(s.ClientURL(), uOpt2)
	require_NoError(t, err)
	defer nc2.Close()
}

func TestJetStreamJWTMove(t *testing.T) {
	sysKp, syspub := createKey(t)
	sysJwt := encodeClaim(t, jwt.NewAccountClaims(syspub), syspub)
	sysCreds := newUser(t, sysKp)

	accKp, aExpPub := createKey(t)

	test := func(t *testing.T, replicas int, accClaim *jwt.AccountClaims) {
		accClaim.Name = "acc"
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
		`
		sc := createJetStreamSuperClusterWithTemplateAndModHook(t, tmlp, 5, 2,
			func(serverName, clustername, storeDir, conf string) string {
				switch sname := serverName[strings.Index(serverName, "-")+1:]; sname {
				case "S1", "S2":
					conf = strings.ReplaceAll(conf, "jetstream", "#jetstream")
				}
				return conf + fmt.Sprintf(`
					server_tags: [cloud:%s-tag]
					operator: %s
					system_account: %s
					resolver: {
						type: full
						dir: '%s/jwt'
					}
					resolver_preload = {
						%s : %s
					}
				`, clustername, ojwt, syspub, storeDir, syspub, sysJwt)
			}, nil)
		defer sc.shutdown()

		s := sc.serverByName("C1-S1")
		require_False(t, s.JetStreamEnabled())
		updateJwt(t, s.ClientURL(), sysCreds, accJwt, 10)

		s = sc.serverByName("C2-S1")
		require_False(t, s.JetStreamEnabled())

		nc := natsConnect(t, s.ClientURL(), nats.UserCredentials(accCreds))
		defer nc.Close()

		js, err := nc.JetStream()
		require_NoError(t, err)

		ci, err := js.AddStream(&nats.StreamConfig{Name: "MOVE-ME", Replicas: replicas,
			Placement: &nats.Placement{Tags: []string{"cloud:C1-tag"}}})
		require_NoError(t, err)
		require_Equal(t, ci.Cluster.Name, "C1")

		_, err = js.AddConsumer("MOVE-ME", &nats.ConsumerConfig{Durable: "dur", AckPolicy: nats.AckExplicitPolicy})
		require_NoError(t, err)
		_, err = js.Publish("MOVE-ME", []byte("hello world"))
		require_NoError(t, err)

		// Perform actual move
		ci, err = js.UpdateStream(&nats.StreamConfig{Name: "MOVE-ME", Replicas: replicas,
			Placement: &nats.Placement{Tags: []string{"cloud:C2-tag"}}})
		require_NoError(t, err)
		require_Equal(t, ci.Cluster.Name, "C1")

		sc.clusterForName("C2").waitOnStreamLeader(aExpPub, "MOVE-ME")

		checkFor(t, 30*time.Second, 250*time.Millisecond, func() error {
			if si, err := js.StreamInfo("MOVE-ME"); err != nil {
				return fmt.Errorf("stream: %v", err)
			} else if si.Cluster.Name != "C2" {
				return fmt.Errorf("Wrong cluster: %q", si.Cluster.Name)
			} else if !strings.HasPrefix(si.Cluster.Leader, "C2-") {
				return fmt.Errorf("Wrong leader: %q", si.Cluster.Leader)
			} else if len(si.Cluster.Replicas) != replicas-1 {
				return fmt.Errorf("Expected %d replicas, got %d", replicas-1, len(si.Cluster.Replicas))
			} else if si.State.Msgs != 1 {
				return fmt.Errorf("expected one message")
			}
			// Now make sure consumer has leader etc..
			if ci, err := js.ConsumerInfo("MOVE-ME", "dur"); err != nil {
				return fmt.Errorf("stream: %v", err)
			} else if ci.Cluster.Name != "C2" {
				return fmt.Errorf("Wrong cluster: %q", ci.Cluster.Name)
			} else if ci.Cluster.Leader == _EMPTY_ {
				return fmt.Errorf("No leader yet")
			}
			return nil
		})

		sub, err := js.PullSubscribe("", "dur", nats.BindStream("MOVE-ME"))
		require_NoError(t, err)
		m, err := sub.Fetch(1)
		require_NoError(t, err)
		require_NoError(t, m[0].AckSync())
	}

	t.Run("tiered", func(t *testing.T) {
		accClaim := jwt.NewAccountClaims(aExpPub)
		accClaim.Limits.JetStreamTieredLimits["R1"] = jwt.JetStreamLimits{
			DiskStorage: 1100, Consumer: 1, Streams: 1}
		accClaim.Limits.JetStreamTieredLimits["R3"] = jwt.JetStreamLimits{
			DiskStorage: 3300, Consumer: 1, Streams: 1}

		t.Run("R3", func(t *testing.T) {
			test(t, 3, accClaim)
		})
		t.Run("R1", func(t *testing.T) {
			test(t, 1, accClaim)
		})
	})

	t.Run("non-tiered", func(t *testing.T) {
		accClaim := jwt.NewAccountClaims(aExpPub)
		accClaim.Limits.JetStreamLimits = jwt.JetStreamLimits{
			DiskStorage: 4400, Consumer: 2, Streams: 2}

		t.Run("R3", func(t *testing.T) {
			test(t, 3, accClaim)
		})
		t.Run("R1", func(t *testing.T) {
			test(t, 1, accClaim)
		})
	})
}

func TestJetStreamJWTClusteredTiers(t *testing.T) {
	sysKp, syspub := createKey(t)
	sysJwt := encodeClaim(t, jwt.NewAccountClaims(syspub), syspub)
	newUser(t, sysKp)

	accKp, aExpPub := createKey(t)
	accClaim := jwt.NewAccountClaims(aExpPub)
	accClaim.Name = "acc"
	accClaim.Limits.JetStreamTieredLimits["R1"] = jwt.JetStreamLimits{
		DiskStorage: 1100, Consumer: 2, Streams: 2}
	accClaim.Limits.JetStreamTieredLimits["R3"] = jwt.JetStreamLimits{
		DiskStorage: 1100, Consumer: 1, Streams: 1}
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
	require_Equal(t, err.Error(), "nats: no JetStream default or applicable tiered limit present")
	_, err = js.AddStream(&nats.StreamConfig{Name: "testR5", Replicas: 5, Subjects: []string{"testR5"}})
	require_Error(t, err)
	require_Equal(t, err.Error(), "nats: no JetStream default or applicable tiered limit present")

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
	require_Equal(t, err.Error(), "nats: maximum number of streams reached")
	_, err = js.AddStream(&nats.StreamConfig{Name: "testR3-3", Replicas: 3, Subjects: []string{"testR3-3"}})
	require_Error(t, err)
	require_Equal(t, err.Error(), "nats: maximum number of streams reached")

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
	require_Equal(t, err.Error(), "nats: maximum consumers limit reached")
	_, err = js.AddConsumer("testR1-1", &nats.ConsumerConfig{Durable: "dur5", AckPolicy: nats.AckExplicitPolicy})
	require_Error(t, err)
	require_Equal(t, err.Error(), "nats: maximum consumers limit reached")

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
		MaxStore:             1100,
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

	accKp, aExpPub := createKey(t)
	accClaim := jwt.NewAccountClaims(aExpPub)
	accClaim.Name = "acc"
	accClaim.Limits.JetStreamTieredLimits["R1"] = jwt.JetStreamLimits{
		DiskStorage: 1000, MemoryStorage: 0, Consumer: 1, Streams: 1}
	accClaim.Limits.JetStreamTieredLimits["R3"] = jwt.JetStreamLimits{
		DiskStorage: 500, MemoryStorage: 0, Consumer: 1, Streams: 1}
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
	require_Error(t, err, errors.New("nats: insufficient storage resources available"))

	time.Sleep(time.Second - time.Since(start)) // make sure the time stamp changes
	accClaim.Limits.JetStreamTieredLimits["R3"] = jwt.JetStreamLimits{
		DiskStorage: 1000, MemoryStorage: 0, Consumer: 1, Streams: 1}
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
		}, nil)
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

func TestJetStreamJWTExpiredAccountNotCountedTowardLimits(t *testing.T) {
	op, _ := nkeys.CreateOperator()
	opPk, _ := op.PublicKey()
	sk, _ := nkeys.CreateOperator()
	skPk, _ := sk.PublicKey()
	opClaim := jwt.NewOperatorClaims(opPk)
	opClaim.SigningKeys.Add(skPk)
	opJwt, err := opClaim.Encode(op)
	require_NoError(t, err)
	createAccountAndUser := func(pubKey, jwt1, creds1 *string) {
		t.Helper()
		kp, _ := nkeys.CreateAccount()
		*pubKey, _ = kp.PublicKey()
		claim := jwt.NewAccountClaims(*pubKey)
		claim.Limits.JetStreamLimits = jwt.JetStreamLimits{MemoryStorage: 7 * 1024 * 1024, DiskStorage: 7 * 1024 * 1024, Streams: 10}
		var err error
		*jwt1, err = claim.Encode(sk)
		require_NoError(t, err)

		ukp, _ := nkeys.CreateUser()
		seed, _ := ukp.Seed()
		upub, _ := ukp.PublicKey()
		uclaim := newJWTTestUserClaims()
		uclaim.Subject = upub

		ujwt1, err := uclaim.Encode(kp)
		require_NoError(t, err)
		*creds1 = genCredsFile(t, ujwt1, seed)
	}
	generateRequest := func(accs []string, kp nkeys.KeyPair) []byte {
		t.Helper()
		opk, _ := kp.PublicKey()
		c := jwt.NewGenericClaims(opk)
		c.Data["accounts"] = accs
		cJwt, err := c.Encode(kp)
		if err != nil {
			t.Fatalf("Expected no error %v", err)
		}
		return []byte(cJwt)
	}

	var syspub, sysjwt, sysCreds string
	createAccountAndUser(&syspub, &sysjwt, &sysCreds)

	dirSrv := t.TempDir()
	conf := createConfFile(t, []byte(fmt.Sprintf(`
		listen: 127.0.0.1:-1
		operator: %s
		jetstream: {max_mem_store: 10Mb, max_file_store: 10Mb, store_dir: "%s"}
		system_account: %s
		resolver: {
			type: full
			allow_delete: true
			dir: '%s'
			timeout: "500ms"
		}
    `, opJwt, dirSrv, syspub, dirSrv)))

	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	// update system account jwt
	updateJwt(t, s.ClientURL(), sysCreds, sysjwt, 1)

	var apub, ajwt1, aCreds1 string
	createAccountAndUser(&apub, &ajwt1, &aCreds1)
	// push jwt (for full resolver)
	updateJwt(t, s.ClientURL(), sysCreds, ajwt1, 1)

	ncA, jsA := jsClientConnect(t, s, nats.UserCredentials(aCreds1))
	defer ncA.Close()

	ai, err := jsA.AccountInfo()
	require_NoError(t, err)
	require_True(t, ai.Limits.MaxMemory == 7*1024*1024)
	ncA.Close()

	nc := natsConnect(t, s.ClientURL(), nats.UserCredentials(sysCreds))
	defer nc.Close()
	resp, err := nc.Request(accDeleteReqSubj, generateRequest([]string{apub}, sk), time.Second)
	require_NoError(t, err)
	require_True(t, strings.Contains(string(resp.Data), `"message":"deleted 1 accounts"`))

	var apub2, ajwt2, aCreds2 string
	createAccountAndUser(&apub2, &ajwt2, &aCreds2)
	// push jwt (for full resolver)
	updateJwt(t, s.ClientURL(), sysCreds, ajwt2, 1)

	ncB, jsB := jsClientConnect(t, s, nats.UserCredentials(aCreds2))
	defer ncB.Close()

	ai, err = jsB.AccountInfo()
	require_NoError(t, err)
	require_True(t, ai.Limits.MaxMemory == 7*1024*1024)
}

func TestJetStreamJWTDeletedAccountDoesNotLeakSubscriptions(t *testing.T) {
	op, _ := nkeys.CreateOperator()
	opPk, _ := op.PublicKey()
	sk, _ := nkeys.CreateOperator()
	skPk, _ := sk.PublicKey()
	opClaim := jwt.NewOperatorClaims(opPk)
	opClaim.SigningKeys.Add(skPk)
	opJwt, err := opClaim.Encode(op)
	require_NoError(t, err)
	createAccountAndUser := func(pubKey, jwt1, creds1 *string) {
		t.Helper()
		kp, _ := nkeys.CreateAccount()
		*pubKey, _ = kp.PublicKey()
		claim := jwt.NewAccountClaims(*pubKey)
		claim.Limits.JetStreamLimits = jwt.JetStreamLimits{MemoryStorage: 7 * 1024 * 1024, DiskStorage: 7 * 1024 * 1024, Streams: 10}
		var err error
		*jwt1, err = claim.Encode(sk)
		require_NoError(t, err)

		ukp, _ := nkeys.CreateUser()
		seed, _ := ukp.Seed()
		upub, _ := ukp.PublicKey()
		uclaim := newJWTTestUserClaims()
		uclaim.Subject = upub

		ujwt1, err := uclaim.Encode(kp)
		require_NoError(t, err)
		*creds1 = genCredsFile(t, ujwt1, seed)
	}
	generateRequest := func(accs []string, kp nkeys.KeyPair) []byte {
		t.Helper()
		opk, _ := kp.PublicKey()
		c := jwt.NewGenericClaims(opk)
		c.Data["accounts"] = accs
		cJwt, err := c.Encode(kp)
		if err != nil {
			t.Fatalf("Expected no error %v", err)
		}
		return []byte(cJwt)
	}

	var syspub, sysjwt, sysCreds string
	createAccountAndUser(&syspub, &sysjwt, &sysCreds)

	dirSrv := t.TempDir()
	conf := createConfFile(t, []byte(fmt.Sprintf(`
		listen: 127.0.0.1:-1
		operator: %s
		jetstream: {max_mem_store: 10Mb, max_file_store: 10Mb, store_dir: %v}
		system_account: %s
		resolver: {
			type: full
			allow_delete: true
			dir: '%s'
			timeout: "500ms"
		}
	`, opJwt, dirSrv, syspub, dirSrv)))

	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	checkNumSubs := func(expected uint32) uint32 {
		t.Helper()
		// Wait a bit before capturing number of subs...
		time.Sleep(250 * time.Millisecond)

		var ns uint32
		checkFor(t, time.Second, 50*time.Millisecond, func() error {
			subsz, err := s.Subsz(nil)
			if err != nil {
				return err
			}
			ns = subsz.NumSubs
			if expected > 0 && ns > expected {
				return fmt.Errorf("Expected num subs to be back at %v, got %v",
					expected, ns)
			}
			return nil
		})
		return ns
	}
	beforeCreate := checkNumSubs(0)

	// update system account jwt
	updateJwt(t, s.ClientURL(), sysCreds, sysjwt, 1)

	createAndDelete := func() {
		t.Helper()

		var apub, ajwt1, aCreds1 string
		createAccountAndUser(&apub, &ajwt1, &aCreds1)
		// push jwt (for full resolver)
		updateJwt(t, s.ClientURL(), sysCreds, ajwt1, 1)

		ncA, jsA := jsClientConnect(t, s, nats.UserCredentials(aCreds1))
		defer ncA.Close()

		ai, err := jsA.AccountInfo()
		require_NoError(t, err)
		require_True(t, ai.Limits.MaxMemory == 7*1024*1024)
		ncA.Close()

		nc := natsConnect(t, s.ClientURL(), nats.UserCredentials(sysCreds))
		defer nc.Close()

		resp, err := nc.Request(accDeleteReqSubj, generateRequest([]string{apub}, sk), time.Second)
		require_NoError(t, err)
		require_True(t, strings.Contains(string(resp.Data), `"message":"deleted 1 accounts"`))
	}

	// Create and delete multiple accounts
	for i := 0; i < 10; i++ {
		createAndDelete()
	}

	// There is a subscription on `_R_.>` that is created on the system account
	// and that will not go away, so discount it.
	checkNumSubs(beforeCreate + 1)
}

func TestJetStreamJWTDeletedAccountIsReEnabled(t *testing.T) {
	op, _ := nkeys.CreateOperator()
	opPk, _ := op.PublicKey()
	sk, _ := nkeys.CreateOperator()
	skPk, _ := sk.PublicKey()
	opClaim := jwt.NewOperatorClaims(opPk)
	opClaim.SigningKeys.Add(skPk)
	opJwt, err := opClaim.Encode(op)
	require_NoError(t, err)
	createAccountAndUser := func(pubKey, jwt1, creds1 *string) {
		t.Helper()
		kp, _ := nkeys.CreateAccount()
		*pubKey, _ = kp.PublicKey()
		claim := jwt.NewAccountClaims(*pubKey)
		claim.Limits.JetStreamLimits = jwt.JetStreamLimits{MemoryStorage: 7 * 1024 * 1024, DiskStorage: 7 * 1024 * 1024, Streams: 10}
		var err error
		*jwt1, err = claim.Encode(sk)
		require_NoError(t, err)

		ukp, _ := nkeys.CreateUser()
		seed, _ := ukp.Seed()
		upub, _ := ukp.PublicKey()
		uclaim := newJWTTestUserClaims()
		uclaim.Subject = upub

		ujwt1, err := uclaim.Encode(kp)
		require_NoError(t, err)
		*creds1 = genCredsFile(t, ujwt1, seed)
	}
	generateRequest := func(accs []string, kp nkeys.KeyPair) []byte {
		t.Helper()
		opk, _ := kp.PublicKey()
		c := jwt.NewGenericClaims(opk)
		c.Data["accounts"] = accs
		cJwt, err := c.Encode(kp)
		if err != nil {
			t.Fatalf("Expected no error %v", err)
		}
		return []byte(cJwt)
	}

	// admin user
	var syspub, sysjwt, sysCreds string
	createAccountAndUser(&syspub, &sysjwt, &sysCreds)

	dirSrv := t.TempDir()
	conf := createConfFile(t, []byte(fmt.Sprintf(`
		listen: 127.0.0.1:-1
		operator: %s
		jetstream: {max_mem_store: 10Mb, max_file_store: 10Mb, store_dir: "%s"}
		system_account: %s
		resolver: {
			type: full
			allow_delete: true
			dir: '%s'
			timeout: "500ms"
		}
	`, opJwt, dirSrv, syspub, dirSrv)))

	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	// update system account jwt
	updateJwt(t, s.ClientURL(), sysCreds, sysjwt, 1)

	// create account
	var apub, ajwt1, aCreds1 string
	kp, _ := nkeys.CreateAccount()
	apub, _ = kp.PublicKey()
	claim := jwt.NewAccountClaims(apub)
	claim.Limits.JetStreamLimits = jwt.JetStreamLimits{
		MemoryStorage: 7 * 1024 * 1024,
		DiskStorage:   7 * 1024 * 1024,
		Streams:       10,
	}
	ajwt1, err = claim.Encode(sk)
	require_NoError(t, err)

	// user
	ukp, _ := nkeys.CreateUser()
	seed, _ := ukp.Seed()
	upub, _ := ukp.PublicKey()
	uclaim := newJWTTestUserClaims()
	uclaim.Subject = upub

	ujwt1, err := uclaim.Encode(kp)
	require_NoError(t, err)
	aCreds1 = genCredsFile(t, ujwt1, seed)

	// push user account
	updateJwt(t, s.ClientURL(), sysCreds, ajwt1, 1)

	ncA, jsA := jsClientConnect(t, s, nats.UserCredentials(aCreds1))
	defer ncA.Close()

	jsA.AddStream(&nats.StreamConfig{Name: "foo"})
	jsA.Publish("foo", []byte("Hello World"))
	jsA.Publish("foo", []byte("Hello Again"))

	// JS should be working
	ai, err := jsA.AccountInfo()
	require_NoError(t, err)
	require_True(t, ai.Limits.MaxMemory == 7*1024*1024)
	require_True(t, ai.Limits.MaxStore == 7*1024*1024)
	require_True(t, ai.Tier.Streams == 1)

	// connect with a different connection and delete the account.
	nc := natsConnect(t, s.ClientURL(), nats.UserCredentials(sysCreds))
	defer nc.Close()

	// delete account
	resp, err := nc.Request(accDeleteReqSubj, generateRequest([]string{apub}, sk), time.Second)
	require_NoError(t, err)
	require_True(t, strings.Contains(string(resp.Data), `"message":"deleted 1 accounts"`))

	// account was disabled and now disconnected, this should get a connection is closed error.
	_, err = jsA.AccountInfo()
	if err == nil || !errors.Is(err, nats.ErrConnectionClosed) {
		t.Errorf("Expected connection closed error, got: %v", err)
	}
	ncA.Close()

	// re-enable, same claims would be detected
	updateJwt(t, s.ClientURL(), sysCreds, ajwt1, 1)

	// expected to get authorization timeout at this time
	_, err = nats.Connect(s.ClientURL(), nats.UserCredentials(aCreds1))
	if !errors.Is(err, nats.ErrAuthorization) {
		t.Errorf("Expected authorization issue on connect, got: %v", err)
	}

	// edit the account and push again with updated claims to same account
	claim = jwt.NewAccountClaims(apub)
	claim.Limits.JetStreamLimits = jwt.JetStreamLimits{
		MemoryStorage: -1,
		DiskStorage:   10 * 1024 * 1024,
		Streams:       10,
	}
	ajwt1, err = claim.Encode(sk)
	require_NoError(t, err)
	updateJwt(t, s.ClientURL(), sysCreds, ajwt1, 1)

	// reconnect with the updated account
	ncA, jsA = jsClientConnect(t, s, nats.UserCredentials(aCreds1))
	defer ncA.Close()
	ai, err = jsA.AccountInfo()
	if err != nil {
		t.Fatal(err)
	}
	require_True(t, ai.Limits.MaxMemory == -1)
	require_True(t, ai.Limits.MaxStore == 10*1024*1024)
	require_True(t, ai.Tier.Streams == 1)

	// should be possible to get stream info again
	si, err := jsA.StreamInfo("foo")
	if err != nil {
		t.Fatal(err)
	}
	if si.State.Msgs != 2 {
		t.Fatal("Unexpected number of messages from recovered stream")
	}
	msg, err := jsA.GetMsg("foo", 1)
	if err != nil {
		t.Fatal(err)
	}
	if string(msg.Data) != "Hello World" {
		t.Error("Unexpected message")
	}
	ncA.Close()
}

// Make sure 100MB HA means 100MB of R3, not 33.3MB.
func TestJetStreamJWTHAStorageLimitsAndAccounting(t *testing.T) {
	sysKp, syspub := createKey(t)
	sysJwt := encodeClaim(t, jwt.NewAccountClaims(syspub), syspub)
	newUser(t, sysKp)

	maxFileStorage := int64(100 * 1024 * 1024)
	maxMemStorage := int64(2 * 1024 * 1024)

	accKp, aExpPub := createKey(t)
	accClaim := jwt.NewAccountClaims(aExpPub)
	accClaim.Name = "acc"
	accClaim.Limits.JetStreamTieredLimits["R3"] = jwt.JetStreamLimits{DiskStorage: maxFileStorage, MemoryStorage: maxMemStorage}
	accJwt := encodeClaim(t, accClaim, aExpPub)
	accCreds := newUser(t, accKp)
	tmlp := `
		listen: 127.0.0.1:-1
		server_name: %s
		jetstream: {max_mem_store: 256MB, max_file_store: 2GB, store_dir: '%s'}
		leaf { listen: 127.0.0.1:-1 }
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

	// Test max bytes first.
	_, err = js.AddStream(&nats.StreamConfig{Name: "TEST", Replicas: 3, MaxBytes: maxFileStorage, Subjects: []string{"foo"}})
	require_NoError(t, err)

	require_NoError(t, js.DeleteStream("TEST"))

	_, err = js.AddStream(&nats.StreamConfig{Name: "TEST", Replicas: 3, Subjects: []string{"foo"}})
	require_NoError(t, err)

	// Now test actual usage.
	// We should be able to send just over 200 of these.
	msg := [500 * 1024]byte{}
	for i := 0; i < 250; i++ {
		if _, err := js.Publish("foo", msg[:]); err != nil {
			require_Error(t, err, NewJSAccountResourcesExceededError())
			require_True(t, i > 200)
			break
		}
	}

	si, err := js.StreamInfo("TEST")
	require_NoError(t, err)
	// Make sure we are no more then 1 msg below our max in terms of size.
	delta := maxFileStorage - int64(si.State.Bytes)
	require_True(t, int(delta) < len(msg))

	// Now memory as well.
	require_NoError(t, js.DeleteStream("TEST"))

	// Test max bytes first.
	_, err = js.AddStream(&nats.StreamConfig{Name: "TEST", Replicas: 3, MaxBytes: maxMemStorage, Storage: nats.MemoryStorage, Subjects: []string{"foo"}})
	require_NoError(t, err)

	require_NoError(t, js.DeleteStream("TEST"))

	_, err = js.AddStream(&nats.StreamConfig{Name: "TEST", Replicas: 3, Storage: nats.MemoryStorage, Subjects: []string{"foo"}})
	require_NoError(t, err)

	// This is much smaller, so should only be able to send 4.
	for i := 0; i < 5; i++ {
		if _, err := js.Publish("foo", msg[:]); err != nil {
			require_Error(t, err, NewJSAccountResourcesExceededError())
			require_Equal(t, i, 4)
			break
		}
	}

	si, err = js.StreamInfo("TEST")
	require_NoError(t, err)
	// Make sure we are no more then 1 msg below our max in terms of size.
	delta = maxMemStorage - int64(si.State.Bytes)
	require_True(t, int(delta) < len(msg))
}

func TestJetStreamJWTHAStorageLimitsOnScaleAndUpdate(t *testing.T) {
	sysKp, syspub := createKey(t)
	sysJwt := encodeClaim(t, jwt.NewAccountClaims(syspub), syspub)
	newUser(t, sysKp)

	maxFileStorage := int64(5 * 1024 * 1024)
	maxMemStorage := int64(1 * 1024 * 1024)

	accKp, aExpPub := createKey(t)
	accClaim := jwt.NewAccountClaims(aExpPub)
	accClaim.Name = "acc"
	accClaim.Limits.JetStreamTieredLimits["R3"] = jwt.JetStreamLimits{DiskStorage: maxFileStorage, MemoryStorage: maxMemStorage}
	accClaim.Limits.JetStreamTieredLimits["R1"] = jwt.JetStreamLimits{DiskStorage: maxFileStorage, MemoryStorage: maxMemStorage}

	accJwt := encodeClaim(t, accClaim, aExpPub)
	accCreds := newUser(t, accKp)
	tmlp := `
		listen: 127.0.0.1:-1
		server_name: %s
		jetstream: {max_mem_store: 256MB, max_file_store: 2GB, store_dir: '%s'}
		leaf { listen: 127.0.0.1:-1 }
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

	// Test max bytes first.
	_, err = js.AddStream(&nats.StreamConfig{Name: "TEST", Replicas: 3, MaxBytes: maxFileStorage, Subjects: []string{"foo"}})
	require_NoError(t, err)
	// Now delete
	require_NoError(t, js.DeleteStream("TEST"))
	// Now do 5 1MB streams.
	for i := 1; i <= 5; i++ {
		sname := fmt.Sprintf("TEST%d", i)
		_, err = js.AddStream(&nats.StreamConfig{Name: sname, Replicas: 3, MaxBytes: 1 * 1024 * 1024})
		require_NoError(t, err)
	}
	// Should fail.
	_, err = js.AddStream(&nats.StreamConfig{Name: "TEST6", Replicas: 3, MaxBytes: 1 * 1024 * 1024})
	require_Error(t, err, errors.New("insufficient storage resources"))

	// Update Test1 and Test2 to smaller reservations.
	_, err = js.UpdateStream(&nats.StreamConfig{Name: "TEST1", Replicas: 3, MaxBytes: 512 * 1024})
	require_NoError(t, err)
	_, err = js.UpdateStream(&nats.StreamConfig{Name: "TEST2", Replicas: 3, MaxBytes: 512 * 1024})
	require_NoError(t, err)
	// Now make sure TEST6 succeeds.
	_, err = js.AddStream(&nats.StreamConfig{Name: "TEST6", Replicas: 3, MaxBytes: 1 * 1024 * 1024})
	require_NoError(t, err)
	// Now delete the R3 version.
	require_NoError(t, js.DeleteStream("TEST6"))
	// Now do R1 version and then we will scale up.
	_, err = js.AddStream(&nats.StreamConfig{Name: "TEST6", Replicas: 1, MaxBytes: 1 * 1024 * 1024})
	require_NoError(t, err)
	// Now make sure scale up works.
	_, err = js.UpdateStream(&nats.StreamConfig{Name: "TEST6", Replicas: 3, MaxBytes: 1 * 1024 * 1024})
	require_NoError(t, err)
	// Add in a few more streams to check reserved reporting in account info.
	_, err = js.AddStream(&nats.StreamConfig{Name: "TEST7", Replicas: 1, MaxBytes: 2 * 1024 * 1024})
	require_NoError(t, err)
	_, err = js.AddStream(&nats.StreamConfig{Name: "TEST8", Replicas: 1, MaxBytes: 256 * 1024, Storage: nats.MemoryStorage})
	require_NoError(t, err)
	_, err = js.AddStream(&nats.StreamConfig{Name: "TEST9", Replicas: 3, MaxBytes: 22 * 1024, Storage: nats.MemoryStorage})
	require_NoError(t, err)

	// Now make sure we report reserved correctly.
	// Do this direct to server since client does not support it yet.
	var info JSApiAccountInfoResponse
	resp, err := nc.Request("$JS.API.INFO", nil, time.Second)
	require_NoError(t, err)
	require_NoError(t, json.Unmarshal(resp.Data, &info))
	stats := info.JetStreamAccountStats
	r1, r3 := stats.Tiers["R1"], stats.Tiers["R3"]

	require_Equal(t, r1.ReservedMemory, 256*1024)   // TEST8
	require_Equal(t, r1.ReservedStore, 2*1024*1024) // TEST7
	require_Equal(t, r3.ReservedMemory, 22*1024)    // TEST9
	require_Equal(t, r3.ReservedStore, 5*1024*1024) // TEST1-TEST6
}
