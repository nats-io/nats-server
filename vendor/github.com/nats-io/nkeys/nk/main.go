// Copyright 2018 The NATS Authors
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
	"encoding/base64"
	"flag"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strings"

	"github.com/nats-io/nkeys"
)

func usage() {
	log.Fatalf("Usage: nk [-gen type] [-sign file] [-verify file] [-inkey keyfile] [-pubin keyfile] [-pubout] [-e entropy]\n")
}

func main() {
	var entropy = flag.String("e", "", "Entropy file, e.g. /dev/urandom")
	var keyFile = flag.String("inkey", "", "Input key file (seed/private key)")
	var pubFile = flag.String("pubin", "", "Public key file")

	var signFile = flag.String("sign", "", "Sign <file> with -inkey <key>")
	var sigFile = flag.String("sigfile", "", "Signature file")

	var verifyFile = flag.String("verify", "", "Verfify <file> with -inkey <keyfile> or -pubin <public> and -sigfile <file>")

	var keyType = flag.String("gen", "", "Generate key for <type>, e.g. nk -gen user")
	var pubout = flag.Bool("pubout", false, "Output public key")

	log.SetFlags(0)
	log.SetOutput(os.Stdout)

	flag.Usage = usage
	flag.Parse()

	// Create Key
	if *keyType != "" {
		createKey(*keyType, *entropy)
		return
	}

	if *entropy != "" {
		log.Fatalf("Entropy file only used when creating keys with -gen")
	}

	// Sign
	if *signFile != "" {
		sign(*signFile, *keyFile)
		return
	}

	// Verfify
	if *verifyFile != "" {
		verify(*verifyFile, *keyFile, *pubFile, *sigFile)
		return
	}

	// Show public key from seed/private
	if *keyFile != "" && *pubout {
		printPublicFromSeed(*keyFile)
		return
	}

	usage()
}

func printPublicFromSeed(keyFile string) {
	seed, err := ioutil.ReadFile(keyFile)
	if err != nil {
		log.Fatal(err)
	}

	kp, err := nkeys.FromSeed(string(seed))
	if err != nil {
		log.Fatal(err)
	}
	pub, _ := kp.PublicKey()
	log.Printf("%s", pub)
}

func sign(fname, keyFile string) {
	if keyFile == "" {
		log.Fatalf("Sign requires a seed/private key via -inkey <file>")
	}
	seed, err := ioutil.ReadFile(keyFile)
	if err != nil {
		log.Fatal(err)
	}

	kp, err := nkeys.FromSeed(string(seed))
	if err != nil {
		log.Fatal(err)
	}

	content, err := ioutil.ReadFile(fname)
	if err != nil {
		log.Fatal(err)
	}

	sigraw, err := kp.Sign(content)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("%s", base64.RawURLEncoding.EncodeToString(sigraw))
}

func verify(fname, keyFile, pubFile, sigFile string) {
	if keyFile == "" && pubFile == "" {
		log.Fatalf("Verify requires a seed key via -inkey or a public key via -pubin")
	}
	if sigFile == "" {
		log.Fatalf("Verify requires a signature via -sigfile")
	}
	var err error
	var kp nkeys.KeyPair
	if keyFile != "" {
		var seed []byte
		seed, err = ioutil.ReadFile(keyFile)
		if err != nil {
			log.Fatal(err)
		}
		kp, err = nkeys.FromSeed(string(seed))
	} else {
		// Public Key
		var public []byte
		public, err = ioutil.ReadFile(pubFile)
		if err != nil {
			log.Fatal(err)
		}
		kp, err = nkeys.FromPublicKey(string(public))
	}
	if err != nil {
		log.Fatal(err)
	}

	content, err := ioutil.ReadFile(fname)
	if err != nil {
		log.Fatal(err)
	}

	sigEnc, err := ioutil.ReadFile(sigFile)
	if err != nil {
		log.Fatal(err)
	}
	sig, err := base64.RawURLEncoding.DecodeString(string(sigEnc))
	if err != nil {
		log.Fatal(err)
	}
	if err := kp.Verify(content, sig); err != nil {
		log.Fatal(err)
	}
	log.Printf("verification succeeded")
}

func createKey(keyType, entropy string) {
	keyType = strings.ToLower(keyType)
	var kp nkeys.KeyPair
	var err error

	var ef io.Reader

	if entropy != "" {
		r, err := os.Open(entropy)
		if err != nil {
			log.Fatal(err)
		}
		ef = r
	}

	switch keyType {
	case "user":
		kp, err = nkeys.CreateUser(ef)
	case "account":
		kp, err = nkeys.CreateAccount(ef)
	case "server":
		kp, err = nkeys.CreateServer(ef)
	case "cluster":
		kp, err = nkeys.CreateCluster(ef)
	case "operator":
		kp, err = nkeys.CreateOperator(ef)
	default:
		log.Fatalf("Usage: nk -gen [user|account|server|cluster|operator]\n")
	}
	if err != nil {
		log.Fatalf("Error creating %s: %v", keyType, err)
	}
	seed, err := kp.Seed()
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("%s", seed)
}
