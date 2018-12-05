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
	"bytes"
	"crypto/rand"
	"encoding/base32"
	"encoding/base64"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strings"

	"github.com/nats-io/nkeys"
)

// this will be set during compilation when a release is made on tools
var Version string

func usage() {
	log.Fatalf("Usage: nk [-v] [-gen type] [-sign file] [-verify file] [-inkey keyfile] [-pubin keyfile] [-sigfile file] [-pubout] [-e entropy]\n")
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

	var version = flag.Bool("v", false, "Show version")
	var vanPre = flag.String("pre", "", "Attempt to generate public key given prefix, e.g. nk -gen user -pre derek")
	var vanMax = flag.Int("maxpre", 1000000, "Maximum attempts at generating the correct key prefix")

	log.SetFlags(0)
	log.SetOutput(os.Stdout)

	flag.Usage = usage
	flag.Parse()

	if *version {
		fmt.Printf("nk version %s\n", Version)
	}

	// Create Key
	if *keyType != "" {
		var kp nkeys.KeyPair
		// Check to see if we are trying to do a vanity public key.
		if *vanPre != "" {
			kp = createVanityKey(*keyType, *vanPre, *entropy, *vanMax)
		} else {
			kp = genKeyPair(preForType(*keyType), *entropy)
		}
		seed, err := kp.Seed()
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("%s", seed)
		if *pubout || *vanPre != "" {
			pub, _ := kp.PublicKey()
			log.Printf("%s", pub)
		}
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
	seed := readKeyFile(keyFile)
	kp, err := nkeys.FromSeed(seed)
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
	seed := readKeyFile(keyFile)
	kp, err := nkeys.FromSeed(seed)
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
	log.Printf("%s", base64.StdEncoding.EncodeToString(sigraw))
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
		kp, err = nkeys.FromSeed(seed)
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
	sig, err := base64.StdEncoding.DecodeString(string(sigEnc))
	if err != nil {
		log.Fatal(err)
	}
	if err := kp.Verify(content, sig); err != nil {
		log.Fatal(err)
	}
	log.Printf("Verified OK")
}

func preForType(keyType string) nkeys.PrefixByte {
	keyType = strings.ToLower(keyType)
	switch keyType {
	case "user":
		return nkeys.PrefixByteUser
	case "account":
		return nkeys.PrefixByteAccount
	case "server":
		return nkeys.PrefixByteServer
	case "cluster":
		return nkeys.PrefixByteCluster
	case "operator":
		return nkeys.PrefixByteOperator
	default:
		log.Fatalf("Usage: nk -gen [user|account|server|cluster|operator]\n")
	}
	return nkeys.PrefixByte(0)
}

func genKeyPair(pre nkeys.PrefixByte, entropy string) nkeys.KeyPair {
	// See if we override entropy.
	ef := rand.Reader
	if entropy != "" {
		r, err := os.Open(entropy)
		if err != nil {
			log.Fatal(err)
		}
		ef = r
	}

	// Create raw seed from source or random.
	var rawSeed [32]byte
	_, err := io.ReadFull(ef, rawSeed[:]) // Or some other random source.
	if err != nil {
		log.Fatalf("Error reading from %s: %v", ef, err)
	}
	kp, err := nkeys.FromRawSeed(pre, rawSeed[:])
	if err != nil {
		log.Fatalf("Error creating %c: %v", pre, err)
	}
	return kp
}

var b32Enc = base32.StdEncoding.WithPadding(base32.NoPadding)

func createVanityKey(keyType, vanity, entropy string, max int) nkeys.KeyPair {
	spinners := []rune(`⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏`)
	pre := preForType(keyType)
	vanity = strings.ToUpper(vanity)
	// Check to make sure we can base32 into it by trying to decode it.
	_, err := b32Enc.DecodeString(vanity)
	if err != nil {
		log.Fatalf("Can not generate base32 encoded strings to match '%s'", vanity)
	}

	for i := 0; i < max; i++ {
		spin := spinners[i%len(spinners)]
		fmt.Fprintf(os.Stderr, "\r\033[mcomputing\033[m %s ", string(spin))
		kp := genKeyPair(pre, entropy)
		pub, _ := kp.PublicKey()
		if strings.HasPrefix(pub[1:], vanity) {
			fmt.Fprintf(os.Stderr, "\r")
			return kp
		}
	}
	fmt.Fprintf(os.Stderr, "\r")
	log.Fatalf("Failed to generate prefix after %d attempts", max)
	return nil
}

func readKeyFile(filename string) []byte {
	var key []byte
	contents, err := ioutil.ReadFile(filename)
	if err != nil {
		log.Fatal(err)
	}
	defer wipeSlice(contents)

	lines := bytes.Split(contents, []byte("\n"))
	for _, line := range lines {
		if nkeys.IsValidEncoding(line) {
			key = make([]byte, len(line))
			copy(key, line)
			return key
		}
	}
	if key == nil {
		log.Fatalf("Could not find a valid key")
	}
	return key
}

func wipeSlice(buf []byte) {
	for i := range buf {
		buf[i] = 'x'
	}
}
