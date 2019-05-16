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

// +build ignore

package main

import (
	"bytes"
	"crypto/rand"
	"flag"
	"fmt"
	"log"
	"math/big"
	"syscall"

	"golang.org/x/crypto/bcrypt"
	"golang.org/x/crypto/ssh/terminal"
)

func usage() {
	fmt.Printf("Usage: mkpasswd [-p <stdin password>] [-c COST] \n")
	flag.PrintDefaults()
}

const (
	// Make sure the password is reasonably long to generate enough entropy.
	PasswordLength = 22
	// Common advice from the past couple of years suggests that 10 should be sufficient.
	// Up that a little, to 11. Feel free to raise this higher if this value from 2015 is
	// no longer appropriate. Min is bcrypt.MinCost, Max is bcrypt.MaxCost.
	DefaultCost = 11
)

func main() {
	var pw = flag.Bool("p", false, "Input password via stdin")
	var cost = flag.Int("c", DefaultCost, fmt.Sprintf("The cost weight, range of %d-%d", bcrypt.MinCost, bcrypt.MaxCost))

	log.SetFlags(0)
	flag.Usage = usage
	flag.Parse()

	var password string

	if *pw {
		fmt.Printf("Enter Password: ")
		bytePassword, err := terminal.ReadPassword(int(syscall.Stdin))
		if err != nil {
			log.Fatalf("Error reading password: %v\n", err)
		}
		fmt.Printf("\nReenter Password: ")
		bytePassword2, err := terminal.ReadPassword(int(syscall.Stdin))
		if err != nil {
			log.Fatalf("Error reading password: %v\n", err)
		}
		if !bytes.Equal(bytePassword, bytePassword2) {
			log.Fatalf("Error, passwords do not match\n")
		}
		password = string(bytePassword)
		fmt.Printf("\n")
	} else {
		password = genPassword()
		fmt.Printf("pass: %s\n", password)
	}

	cb, err := bcrypt.GenerateFromPassword([]byte(password), *cost)
	if err != nil {
		log.Fatalf("Error producing bcrypt hash: %v\n", err)
	}
	fmt.Printf("bcrypt hash: %s\n", cb)
}

func genPassword() string {
	var ch = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!@$#%^&*()")
	b := make([]byte, PasswordLength)
	max := big.NewInt(int64(len(ch)))
	for i := range b {
		ri, err := rand.Int(rand.Reader, max)
		if err != nil {
			log.Fatalf("Error producing random integer: %v\n", err)
		}
		b[i] = ch[int(ri.Int64())]
	}
	return string(b)
}
