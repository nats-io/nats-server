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
	"errors"
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

type ncrCmd struct {
	makeNKeys *bool
	outFile   *string
	expiry    int64
	cluster   keyInfo
	server    keyInfo
	token     string
}

type keyInfo struct {
	path *string
	pair nkeys.KeyPair
	pub  string
	seed string
}

func NewNcrCmd() (*ncrCmd, error) {
	var args = ncrCmd{}

	args.makeNKeys = flag.Bool("g", false, "generate cluster and/or server keys")
	args.cluster.path = flag.String("c", "", "cluster key path")
	args.server.path = flag.String("s", "", "server key or path")
	args.outFile = flag.String("o", "--", "output file (defaults to stdout)")
	expiry := flag.String("e", "", "token expiry (YYYY-MM-DD, 1m, 1h, 1d, 1M, 1y, 1w)")

	log.SetFlags(0)
	flag.Usage = usage
	flag.Parse()

	var err error
	args.expiry, err = parseExpiry(*expiry)
	if err != nil {
		return nil, err
	}

	return &args, nil
}

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
	if len(m) > 0 {
		count, err := strconv.ParseInt(m[1], 10, 64)
		if err != nil {
			return 0, err
		}
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
			return now.Add(dur * time.Hour * 24).Unix(), nil
		case "w":
			return now.Add(dur * time.Hour * 24 * 7).Unix(), nil
		case "M":
			return now.AddDate(0, 1, 0).Unix(), nil
		case "y":
			return now.AddDate(1, 0, 0).Unix(), nil
		}
	}
	return 0, fmt.Errorf("couldn't parse: %v", s)
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

func (n *ncrCmd) run() error {
	if err := n.generate(); err != nil {
		return err
	}

	if err, usageErr := n.loadClusterKey(); err != nil {
		if usageErr {
			printUsage()
		}
		return err
	}

	if err, usageErr := n.loadServerKey(); err != nil {
		if usageErr {
			printUsage()
		}
		return err
	}

	if err := n.generateToken(); err != nil {
		return err
	}

	return n.doOutput()
}

func (n *ncrCmd) generate() error {
	var err error
	if *n.cluster.path == "" {
		n.cluster.pair, err = nkeys.CreateCluster()
		if err != nil {
			return fmt.Errorf("error creating cluster keypair: %v", err)
		}
	}
	if *n.server.path == "" {
		n.server.pair, err = nkeys.CreateServer()
		if err != nil {
			return fmt.Errorf("error creating server keypair: %v", err)
		}
	}
	return nil
}

func (n *ncrCmd) loadClusterKey() (error, bool) {
	var err error
	// cluster is read from a file

	if n.cluster.pair == nil {
		d, err := maybeParsePath(*n.cluster.path)
		if err != nil {
			return fmt.Errorf("unable to read cluster key: %v", err), false
		}

		n.cluster.pair, err = parseNKey(string(d), 'C')
		if err != nil {
			return fmt.Errorf("unable to parse cluster key: %v", err), false
		}
	}

	if n.cluster.pair == nil {
		return errors.New("cluster key was not provided"), true
	}

	n.cluster.pub, err = n.cluster.pair.PublicKey()
	if err != nil {
		return fmt.Errorf("error reading cluster public key: %v", err), false
	}

	if !nkeys.IsValidPublicClusterKey(n.cluster.pub) {
		return fmt.Errorf("cluster key is not a valid key"), false
	}
	return nil, false
}

func (n *ncrCmd) loadServerKey() (error, bool) {
	var err error
	// server can be read from file or as an argument

	if n.server.pair == nil {
		n.server.pair, err = maybeParseNKey(*n.server.path, 'N')
		if err != nil {
			return fmt.Errorf("unable to parse server key: %v\n", err), false
		}
	}

	if n.server.pair == nil {
		return errors.New("server key was not provided"), true
	}

	n.server.pub, err = n.server.pair.PublicKey()
	if err != nil {
		return fmt.Errorf("error reading server public key: %v\n", err), false
	}

	if !nkeys.IsValidPublicServerKey(n.server.pub) {
		return fmt.Errorf("server key is not a valid key"), false
	}

	return nil, false
}

func (n *ncrCmd) generateToken() error {
	var err error
	// generate the JWT
	sc := jwt.NewServerClaims(n.server.pub)
	if n.expiry > 0 {
		sc.Expires = n.expiry
	}
	n.token, err = sc.Encode(n.cluster.pair)
	if err != nil {
		return fmt.Errorf("error encoding server token: %v", err)
	}

	// if we generated the cluster - show them a public key
	if *n.cluster.path == "" {
		n.cluster.seed, err = n.cluster.pair.Seed()
		if err != nil {
			return fmt.Errorf("unable to generate cluster seed: %v", err)
		}
	}

	// if we generated a server - show them a public key
	if *n.server.path == "" {
		n.server.seed, err = n.server.pair.Seed()
		if err != nil {
			return fmt.Errorf("unable to generate server seed: %v", err)
		}
	}
	return nil
}

func (ki *keyInfo) printSeed(f *os.File) {
	fmt.Fprintln(f, "-----BEGIN CLUSTER NKEY-----")
	fmt.Fprintln(f, ki.seed)
	fmt.Fprintln(f, "------END CLUSTER NKEY------")
	fmt.Fprintln(f)

	fmt.Fprintln(f, "-----BEGIN CLUSTER PUB KEY-----")
	fmt.Fprintln(f, ki.pub)
	fmt.Fprintln(f, "------END CLUSTER PUB KEY------")
	fmt.Fprintln(f)
}

func (ki *keyInfo) isGenerated() bool {
	return ki.seed != ""
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

	if *n.outFile == "--" {
		f = os.Stdout
	} else {
		f, err = os.Create(*n.outFile)
		if err != nil {
			return fmt.Errorf("error creating output file %q: %v", *n.outFile, err)
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
		if n.cluster.isGenerated() {
			n.cluster.printSeed(f)
		}
		if n.server.isGenerated() {
			n.server.printSeed(f)
		}
		fmt.Fprintln(f, "*************************************************************")
		fmt.Fprintln(f)
	}
	n.printToken(f)
	f.Sync()

	if *n.outFile != "--" {
		fmt.Printf("Success! - wrote server JWT to %q\n", *n.outFile)
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

func maybeParsePath(s string) (string, error) {
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

func maybeParseNKey(s string, prefix byte) (nkeys.KeyPair, error) {
	var err error
	var v string
	if s == "" {
		return nil, nil
	}

	if !looksLikeNKey(s, prefix) {
		v, err = maybeParsePath(s)
		if err != nil {
			return nil, err
		}
		v = strings.TrimSpace(v)
	} else {
		v = s
	}

	if v[0:1] == "S" {
		return nkeys.FromSeed(v)
	} else {
		return nkeys.FromPublicKey(v)
	}
}

func parseNKey(s string, prefix byte) (nkeys.KeyPair, error) {
	if !looksLikeNKey(s, prefix) {
		if s[0:1] == "S" {
			return nkeys.FromSeed(s)
		} else {
			return nkeys.FromPublicKey(s)
		}
	}
	return nil, fmt.Errorf("%q is not an nkey", s)
}
