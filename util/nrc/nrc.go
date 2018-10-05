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

package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/jwt"
	"github.com/nats-io/nkeys"
)

func usage() {
	printUsage()
	os.Exit(1)
}

func printUsage() {
	fmt.Fprintf(os.Stderr, "Usage: nrc [-h] [-g] -c <cluster key path> -s <server key/path> [-o <output>]\n")
	flag.PrintDefaults()
}

func main() {
	cmd, err := NewNcrCmd()
	if err != nil {
		log.Fatal(err)
	}
	if err := cmd.run(); err != nil {
		log.Fatal(err)
	}
}

type ncrCmd struct {
	create  bool
	outFile string
	expiry  int64
	cluster clusterKey
	server  serverKey
	token   string
}

func NewNcrCmd() (*ncrCmd, error) {
	var err error

	var cmd = ncrCmd{}
	flag.BoolVar(&cmd.create, "g", false, "processArgs cluster and/or server keys")
	flag.StringVar(&cmd.cluster.path, "c", "", "cluster key path")
	flag.StringVar(&cmd.server.path, "s", "", "server key or path")
	flag.StringVar(&cmd.outFile, "o", "--", "output file (defaults to stdout)")
	expiry := flag.String("e", "", "token expiry (YYYY-MM-DD, 1m, 1h, 1d, 1M, 1y, 1w)")

	log.SetFlags(0)
	flag.Usage = usage
	flag.Parse()

	cmd.expiry, err = parseExpiry(*expiry)
	if err != nil {
		return nil, err
	}

	return &cmd, nil
}

// parse expiration argument - supported are YYYY-MM-DD for absolute, and relative
// (m)inute, (h)our, (d)ay, (w)week, (M)onth, (y)ear expressions
func parseExpiry(s string) (int64, error) {
	if s == "" {
		return 0, nil
	}
	re := regexp.MustCompile(`(\d){4}\-(\d){2}\-(\d){2}`)
	if re.MatchString(s) {
		t, err := time.Parse("2006-01-02", s)
		if err != nil {
			return 0, err
		}
		return t.Unix(), nil
	}
	re = regexp.MustCompile(`(?P<count>\d+)(?P<qualifier>[mhdMyw])`)
	m := re.FindStringSubmatch(s)
	if m != nil {
		v, err := strconv.ParseInt(m[1], 10, 64)
		if err != nil {
			return 0, err
		}
		count := int(v)
		if count == 0 {
			return 0, nil
		}
		dur := time.Duration(count)
		now := time.Now()
		switch m[2] {
		case "m":
			return now.Add(dur * time.Minute).Unix(), nil
		case "h":
			return now.Add(dur * time.Hour).Unix(), nil
		case "d":
			return now.AddDate(0, 0, count).Unix(), nil
		case "w":
			return now.AddDate(0, 0, 7*count).Unix(), nil
		case "M":
			return now.AddDate(0, count, 0).Unix(), nil
		case "y":
			return now.AddDate(count, 0, 0).Unix(), nil
		}
	}
	return 0, fmt.Errorf("couldn't parse expiry: %v", s)
}

// process arguments the cluster and server keys as per arguments
func (n *ncrCmd) processArgs() error {
	if err := n.cluster.init(n.create); err != nil {
		return fmt.Errorf("error processing cluster keypair: %v", err)
	}
	if n.cluster.pair == nil {
		return fmt.Errorf("mssing cluster key")
	}
	if err := n.server.init(n.create); err != nil {
		return fmt.Errorf("error processing cluster keypair: %v", err)
	}
	if n.server.pair == nil {
		return fmt.Errorf("mssing server key")
	}
	return nil
}

// runs the command
func (n *ncrCmd) run() error {
	if err := n.processArgs(); err != nil {
		if strings.HasPrefix(err.Error(), "missing") {
			printUsage()
		}
		return err
	}
	if err := n.createToken(); err != nil {
		return err
	}
	return n.doOutput()
}

type serverKey struct {
	prefix byte
	path   string
	pair   nkeys.KeyPair
	pub    string
	seed   string
}

type clusterKey struct {
	serverKey
}

// server keys can be created/loaded/read from files or arguments
func (sk *serverKey) init(create bool) error {
	var err error
	if create && sk.path == "" {
		sk.pair, err = nkeys.CreateServer()
		if err != nil {
			return err
		}
		sk.seed, err = sk.pair.Seed()
		if err != nil {
			return err
		}
	} else {
		if looksLikeNKey(sk.path, sk.prefix) {
			sk.pair, err = parseNKey(sk.path)
			if err != nil {
				return err
			}
		} else {
			v, err := loadFromPath(sk.path)
			if err != nil {
				return err
			}
			sk.pair, err = parseNKey(v)
			if err != nil {
				return err
			}
		}
	}
	if sk.pair != nil {
		sk.pub, err = sk.pair.PublicKey()
		if err != nil {
			return err
		}
		if !nkeys.IsValidPublicServerKey(sk.pub) {
			return fmt.Errorf("invalid server public key: %q", sk.pub)
		}
	}

	return nil
}
// cluster keys can only be created or loaded from files
func (ck *clusterKey) init(create bool) error {
	var err error
	if create && ck.path == "" {
		ck.pair, err = nkeys.CreateCluster()
		if err != nil {
			return err
		}
		ck.seed, err = ck.pair.Seed()
		if err != nil {
			return err
		}
		ck.pub, err = ck.pair.PublicKey()
		if err != nil {
			return err
		}
	} else {
		v, err := loadFromPath(ck.path)
		if err != nil {
			return err
		}
		ck.pair, err = parseNKey(v)
		if err != nil {
			return err
		}
	}
	if ck.pair != nil {
		ck.pub, err = ck.pair.PublicKey()
		if err != nil {
			return err
		}
		if !nkeys.IsValidPublicClusterKey(ck.pub) {
			return fmt.Errorf("invalid cluster public key: %q", ck.pub)
		}
	}

	return nil
}

// generates the JWT token
func (n *ncrCmd) createToken() error {
	var err error
	// processArgs the JWT
	sc := jwt.NewServerClaims(n.server.pub)
	if n.expiry > 0 {
		sc.Expires = n.expiry
	}
	n.token, err = sc.Encode(n.cluster.pair)
	if err != nil {
		return fmt.Errorf("error encoding server token: %v", err)
	}
	return nil
}

// template for a generated key
func (sk *serverKey) printGeneratedKeys(f *os.File, label string) {
	if sk.isGenerated() {
		label = strings.ToUpper(label)
		fmt.Fprintf(f, "-----BEGIN %s NKEY-----\n", label)
		fmt.Fprintln(f, sk.seed)
		fmt.Fprintf(f, "------END %s NKEY------\n", label)
		fmt.Fprintln(f)

		fmt.Fprintf(f, "-----BEGIN %s PUB KEY-----\n", label)
		fmt.Fprintln(f, sk.pub)
		fmt.Fprintf(f, "------END %s PUB KEY------\n", label)
		fmt.Fprintln(f)
	}
}

func (sk *serverKey) isGenerated() bool {
	return sk.seed != ""
}

func (n *ncrCmd) printToken(f *os.File) {
	fmt.Fprintln(f, "-----BEGIN SERVER JWT-----")
	fmt.Fprintln(f, n.token)
	fmt.Fprintln(f, "------END SERVER JWT------")
	fmt.Fprintln(f)
}

func (n *ncrCmd) doOutput() error {
	var err error
	var f *os.File

	if n.outFile == "--" {
		f = os.Stdout
	} else {
		f, err = os.Create(n.outFile)
		if err != nil {
			return fmt.Errorf("error creating output file %q: %v", n.outFile, err)
		}
		defer f.Close()
	}

	if n.server.isGenerated() || n.cluster.isGenerated() {
		fmt.Fprintln(f, "************************* IMPORTANT *************************")
		fmt.Fprintln(f, "Your options generated cluster and/or server NKEYs which can")
		fmt.Fprintln(f, "be used to create access tokens or prove identity.")
		fmt.Fprintln(f, "Generated NKEYs printed below are sensitive and should be")
		fmt.Fprintln(f, "treated as secrets to prevent unauthorized access to your")
		fmt.Fprintln(f, "cluster.")
		fmt.Fprintln(f)
		fmt.Fprintln(f)

		n.cluster.printGeneratedKeys(f, "cluster")
		n.server.printGeneratedKeys(f, "server")

		fmt.Fprintln(f, "*************************************************************")
		fmt.Fprintln(f)
	}
	n.printToken(f)
	f.Sync()

	if n.outFile != "--" {
		fmt.Printf("Success! - wrote server JWT to %q\n", n.outFile)
	}

	return nil
}

func looksLikeNKey(s string, prefix byte) bool {
	pre := string(prefix)
	if len(s) == 58 {
		pre = "S" + string(prefix)
		return strings.HasPrefix(s, pre) && !strings.Contains(s, string(filepath.Separator))
	}
	if len(s) == 56 {
		return strings.HasPrefix(s, pre) && !strings.Contains(s, string(filepath.Separator))
	}
	return false
}

func loadFromPath(s string) (string, error) {
	// otherwise see if they gave us a file
	fi, err := os.Stat(s)
	if err != nil && os.IsNotExist(err) {
		return "", fmt.Errorf("file %q was not found", s)
	}
	if fi.IsDir() {
		return "", fmt.Errorf("file %q is a directory", s)
	}
	// return the contents
	d, err := ioutil.ReadFile(s)
	if err != nil {
		return "", fmt.Errorf("error reading %q: %v", s, err)
	}
	return string(d), nil
}

func parseNKey(s string) (nkeys.KeyPair, error) {
	if s[0:1] == "S" {
		return nkeys.FromSeed(s)
	} else {
		return nkeys.FromPublicKey(s)
	}
}
