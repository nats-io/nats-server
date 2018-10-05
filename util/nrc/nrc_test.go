package main

import (
	"fmt"
	"github.com/nats-io/jwt"
	"github.com/nats-io/nkeys"
	"io/ioutil"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestParseDate(t *testing.T) {
	type testd struct {
		input   string
		output  int64
		isError bool
	}
	tests := []testd{
		{"", 0, false},
		{"19-1-6", 0, true},
		{"2019-1-6", 0, true},
		{"2019-01-6", 0, true},
		{"2019-01-06", time.Date(2019, 1, 6, 0, 0, 0, 0, time.UTC).Unix(), false},
		{"1m", time.Now().Unix() + 60, false},
		{"32m", time.Now().Unix() + 60*32, false},
		{"1h", time.Now().Unix() + 60*60, false},
		{"3h", time.Now().Unix() + 60*60*3, false},
		{"1d", time.Now().Unix() + 60*60*24, false},
		{"3d", time.Now().Unix() + 60*60*24*3, false},
		{"1w", time.Now().Unix() + 60*60*24*7, false},
		{"3w", time.Now().AddDate(0, 0, 7*3).Unix(), false},
		{"2M", time.Now().AddDate(0, 2, 0).Unix(), false},
		{"2y", time.Now().AddDate(2, 0, 0).Unix(), false},
	}
	for _, d := range tests {
		v, err := parseExpiry(d.input)
		if err != nil && !d.isError {
			t.Errorf("%s didn't expect error: %v", d.input, err)
			continue
		}
		if v != d.output {
			t.Errorf("%s expected %d but got %d", d.input, d.output, v)
		}
	}
}

func TestGenerateAll(t *testing.T) {
	p, err := ioutil.TempDir("", "ncr_tests")
	if err != nil {
		t.Fatal("error creating test dir", err)
	}

	cmd := ncrCmd{}
	cmd.create = true
	cmd.outFile = filepath.Join(p, "test_generate.jwt")

	if err := cmd.run(); err != nil {
		t.Fatal(err)
	}

	if cmd.token == "" {
		t.Fatal("failed to generate token")
	}

	if cmd.server.seed == "" {
		t.Fatal("failed to generate seed")
	}

	if cmd.cluster.seed == "" {
		t.Fatal("failed to generate seed")
	}

	c, err := jwt.DecodeServerClaims(cmd.token)
	if err != nil {
		t.Fatal("failed to decode claim", err)
	}

	if c.Subject != cmd.server.pub {
		t.Fatalf("subjects didn't match expected %q got %q", cmd.server.pub, c.Subject)
	}

	data, err := ioutil.ReadFile(cmd.outFile)
	s := string(data)

	if !strings.Contains(s, cmd.token) {
		t.Fatal("token was not found")
	}

	if !strings.Contains(s, cmd.cluster.seed) {
		t.Fatal("cluster seed was not found")
	}

	if !strings.Contains(s, cmd.cluster.pub) {
		t.Fatal("cluster pub was not found")
	}

	if !strings.Contains(s, cmd.server.seed) {
		t.Fatal("server seed was not found")
	}

	if !strings.Contains(s, cmd.server.pub) {
		t.Fatal("server pub was not found")
	}
}

func TestGenerateServer(t *testing.T) {
	p, err := ioutil.TempDir("", "ncr_tests")
	if err != nil {
		t.Fatal("error creating test dir", err)
	}

	keys := generateKeys(nkeys.CreateCluster, t)
	kfile := store(keys.seed, t)

	cmd := ncrCmd{}
	cmd.create = true
	cmd.cluster.path = kfile
	cmd.outFile = filepath.Join(p, "test_generate.jwt")

	if err := cmd.run(); err != nil {
		t.Fatal(err)
	}

	if cmd.token == "" {
		t.Fatal("failed to generate token")
	}

	if cmd.server.seed == "" {
		t.Fatal("failed to generate seed")
	}

	if cmd.cluster.seed != "" {
		t.Fatal("generate seed - shouldn't have")
	}

	c, err := jwt.DecodeServerClaims(cmd.token)
	if err != nil {
		t.Fatal("failed to decode claim", err)
	}

	if c.Subject != cmd.server.pub {
		t.Fatalf("subjects didn't match expected %q got %q", cmd.server.pub, c.Subject)
	}

	data, err := ioutil.ReadFile(cmd.outFile)
	s := string(data)

	if !strings.Contains(s, cmd.token) {
		t.Fatal("token was not found")
	}

	if cmd.cluster.seed != "" && strings.Contains(s, cmd.cluster.seed) {
		t.Fatal("cluster seed shouldn't be in the output", s)
	}

	if cmd.cluster.pub != "" && strings.Contains(s, cmd.cluster.pub) {
		t.Fatal("cluster pub shoudn't be in the output")
	}

	if !strings.Contains(s, cmd.server.seed) {
		t.Fatal("server seed was not found")
	}

	if !strings.Contains(s, cmd.server.pub) {
		t.Fatal("server pub was not found")
	}
}

func TestGenerateNone(t *testing.T) {
	p, err := ioutil.TempDir("", "ncr_tests")
	if err != nil {
		t.Fatal("error creating test dir", err)
	}

	ck := generateKeys(nkeys.CreateCluster, t)
	ckf := store(ck.seed, t)

	sk := generateKeys(nkeys.CreateServer, t)
	skf := store(sk.pub, t)

	cmd := ncrCmd{}
	cmd.create = true
	cmd.cluster.path = ckf
	cmd.server.path = skf
	cmd.outFile = filepath.Join(p, "test_generate.jwt")
	cmd.expiry, err = parseExpiry("1d")
	if err != nil {
		t.Fatal("failed to parse expiry")
	}

	if err := cmd.run(); err != nil {
		t.Fatal(err)
	}

	if cmd.token == "" {
		t.Fatal("failed to generate token")
	}

	if cmd.server.seed != "" {
		t.Fatal("generated server seed - shouldn't have")
	}

	if cmd.cluster.seed != "" {
		t.Fatal("generate seed - shouldn't have")
	}

	data, err := ioutil.ReadFile(cmd.outFile)
	s := string(data)

	if !strings.Contains(s, cmd.token) {
		t.Fatal("token was not found")
	}

	if cmd.cluster.seed != "" && strings.Contains(s, cmd.cluster.seed) {
		t.Fatal("cluster seed shouldn't be in the output", s)
	}

	if cmd.cluster.pub != "" && strings.Contains(s, cmd.cluster.pub) {
		t.Fatal("cluster pub shoudn't be in the output")
	}

	if cmd.server.seed != "" && strings.Contains(s, cmd.server.seed) {
		t.Fatal("server seed shouldn't be in the output")
	}

	if cmd.server.pub != "" && strings.Contains(s, cmd.server.pub) {
		t.Fatal("server pub shouldn't be in the output")
	}

	c, err := jwt.DecodeServerClaims(cmd.token)
	if err != nil {
		t.Fatal("failed to decode claim", err)
	}

	if c.Subject != cmd.server.pub {
		t.Fatalf("subjects didn't match expected %q got %q", cmd.server.pub, c.Subject)
	}

	if c.Expires == 0 {
		t.Fatal("expiration was not set and should be")
	}
}

func TestInvalidClusterKey(t *testing.T) {
	p, err := ioutil.TempDir("", "ncr_tests")
	if err != nil {
		t.Fatal("error creating test dir", err)
	}

	keys := generateKeys(nkeys.CreateServer, t)
	kfile := store(keys.seed, t)

	cmd := ncrCmd{}
	cmd.create = true
	cmd.cluster.path = kfile
	cmd.outFile = filepath.Join(p, "test_generate.jwt")

	if err := cmd.run(); err != nil {
		if !strings.Contains(err.Error(), "invalid cluster public key") {
			t.Fatal("unexpected error", err)
		}
		return
	}
	t.Fatal("should have failed - with bad cluster key")
}

func TestInvalidClusterNKey(t *testing.T) {
	p, err := ioutil.TempDir("", "ncr_tests")
	if err != nil {
		t.Fatal("error creating test dir", err)
	}

	keys := generateKeys(nkeys.CreateCluster, t)
	kfile := store(keys.pub, t)

	cmd := ncrCmd{}
	cmd.create = true
	cmd.cluster.path = kfile
	cmd.outFile = filepath.Join(p, "test_generate.jwt")

	if err := cmd.run(); err != nil {
		if !strings.Contains(err.Error(), "no private key available") {
			t.Fatal("unexpected error", err)
		}
		return
	}
	t.Fatal("should have failed - with bad cluster key")
}

func TestInvalidServerKey(t *testing.T) {
	p, err := ioutil.TempDir("", "ncr_tests")
	if err != nil {
		t.Fatal("error creating test dir", err)
	}

	keys := generateKeys(nkeys.CreateCluster, t)
	kfile := store(keys.seed, t)

	cmd := ncrCmd{}
	cmd.create = true
	cmd.cluster.path = kfile
	cmd.server.path = kfile
	cmd.outFile = filepath.Join(p, "test_generate.jwt")

	if err := cmd.run(); err != nil {
		if !strings.Contains(err.Error(), "invalid server public key") {
			t.Fatal("unexpected error", err)
		}
		return
	}
	t.Fatal("should have failed - with bad server key")
}

type kpfun func() (nkeys.KeyPair, error)

func generateKeys(f kpfun, t *testing.T) *serverKey {
	var err error
	sk := serverKey{}
	sk.pair, err = f()
	if err != nil {
		t.Fatal("error generating key", err)
	}
	sk.seed, err = sk.pair.Seed()
	if err != nil {
		t.Fatal("error reading seed", err)
	}
	sk.pub, err = sk.pair.PublicKey()
	if err != nil {
		t.Fatal("error reading seed", err)
	}

	return &sk
}

func store(s string, t *testing.T) string {
	f, err := ioutil.TempFile("", "nkey")
	if err != nil {
		t.Fatal("error creating temp file", err)
	}
	defer f.Close()

	_, err = fmt.Fprintln(f, s)
	if err != nil {
		t.Fatal("error writing temp file", err)
	}
	f.Sync()
	return f.Name()
}
