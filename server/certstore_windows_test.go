// Copyright 2022-2024 The NATS Authors
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

//go:build windows

package server

import (
	"crypto/tls"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func runPowershellScript(scriptFile string, args []string) error {
	psExec, _ := exec.LookPath("powershell.exe")

	execArgs := []string{psExec, "-command", fmt.Sprintf("& '%s'", scriptFile)}
	if len(args) > 0 {
		execArgs = append(execArgs, args...)
	}

	cmdImport := &exec.Cmd{
		Path:   psExec,
		Args:   execArgs,
		Stdout: os.Stdout,
		Stderr: os.Stderr,
	}
	return cmdImport.Run()
}

func runConfiguredLeaf(t *testing.T, hubPort int, certStore string, matchBy string, match string, caMatch string, expectedLeafCount int) {

	// Fire up the leaf
	u, err := url.Parse(fmt.Sprintf("nats://localhost:%d", hubPort))
	if err != nil {
		t.Fatalf("Error parsing url: %v", err)
	}

	configStr := fmt.Sprintf(`
		port: -1
		leaf {
			remotes [
				{
					url: "%s"
					tls {
						cert_store: "%s"
						cert_match_by: "%s"
						cert_match: "%s"
						ca_certs_match: %s

						# Test settings that succeed should be equivalent to:
						# cert_file: "../test/configs/certs/tlsauth/client.pem"
						# key_file: "../test/configs/certs/tlsauth/client-key.pem"
						# ca_file: "../test/configs/certs/tlsauth/ca.pem"
						timeout: 5
					}
				}
			]
		}
	`, u.String(), certStore, matchBy, match, caMatch)

	leafConfig := createConfFile(t, []byte(configStr))
	defer removeFile(t, leafConfig)
	leafServer, _ := RunServerWithConfig(leafConfig)
	defer leafServer.Shutdown()

	// After client verify, hub will match by SAN email, SAN dns, and Subject (in that order)
	// Our test client specifies Subject only so we should match on that...

	// A little settle time
	time.Sleep(1 * time.Second)
	checkLeafNodeConnectedCount(t, leafServer, expectedLeafCount)
}

// TestLeafTLSWindowsCertStore tests the topology of two NATS Servers connected as leaf and hub with authentication of
// leaf to hub via mTLS with leaf's certificate and signing key provisioned in the Windows certificate store.
func TestLeafTLSWindowsCertStore(t *testing.T) {

	// Client Identity (client.pem)
	// Issuer: O = NATS CA, OU = NATS.io, CN = localhost
	// Subject: OU = NATS.io, CN = example.com

	// Make sure windows cert store is reset to avoid conflict with other tests
	err := runPowershellScript("../test/configs/certs/tlsauth/certstore/delete-cert-from-store.ps1", nil)
	if err != nil {
		t.Fatalf("expected powershell cert delete to succeed: %s", err.Error())
	}

	// Provision Windows cert store with client cert and secret
	err = runPowershellScript("../test/configs/certs/tlsauth/certstore/import-p12-client.ps1", nil)
	if err != nil {
		t.Fatalf("expected powershell provision to succeed: %s", err.Error())
	}

	err = runPowershellScript("../test/configs/certs/tlsauth/certstore/import-p12-ca.ps1", nil)
	if err != nil {
		t.Fatalf("expected powershell provision CA to succeed: %s", err.Error())
	}

	// Fire up the hub
	hubConfig := createConfFile(t, []byte(`
		port: -1
		leaf {
			listen: "127.0.0.1:-1"
			tls {
				ca_file: "../test/configs/certs/tlsauth/ca.pem"
				cert_file: "../test/configs/certs/tlsauth/server.pem"
				key_file:  "../test/configs/certs/tlsauth/server-key.pem"
				timeout: 5
				verify_and_map: true
			}
		}

		accounts: {
			AcctA: {
			  users: [ {user: "OU = NATS.io, CN = example.com"} ]
			},
			AcctB: {
			  users: [ {user: UserB1} ]
			},
			SYS: {
				users: [ {user: System} ]
			}
		}
		system_account: "SYS"
	`))
	defer removeFile(t, hubConfig)
	hubServer, hubOptions := RunServerWithConfig(hubConfig)
	defer hubServer.Shutdown()

	testCases := []struct {
		certStore         string
		certMatchBy       string
		certMatch         string
		caCertsMatch      string
		expectedLeafCount int
	}{
		// Test subject and issuer
		{"WindowsCurrentUser", "Subject", "example.com", "\"NATS CA\"", 1},
		{"WindowsCurrentUser", "Issuer", "NATS CA", "\"NATS CA\"", 1},
		{"WindowsCurrentUser", "Issuer", "Frodo Baggins, Inc.", "\"NATS CA\"", 0},
		{"WindowsCurrentUser", "Thumbprint", "7e44f478114a2e29b98b00beb1b3687d8dc0e481", "\"NATS CA\"", 0},
		// Test CAs, NATS CA is valid, others are missing
		{"WindowsCurrentUser", "Subject", "example.com", "[\"NATS CA\"]", 1},
		{"WindowsCurrentUser", "Subject", "example.com", "[\"GlobalSign\"]", 0},
		{"WindowsCurrentUser", "Subject", "example.com", "[\"Missing NATS Cert\"]", 0},
		{"WindowsCurrentUser", "Subject", "example.com", "[\"NATS CA\", \"Missing NATS Cert1\"]", 1},
		{"WindowsCurrentUser", "Subject", "example.com", "[\"Missing Cert2\",\"NATS CA\"]", 1},
		{"WindowsCurrentUser", "Subject", "example.com", "[\"Missing, Cert3\",\"Missing NATS Cert4\"]", 0},
	}
	for _, tc := range testCases {
		testName := fmt.Sprintf("%s by %s match %s", tc.certStore, tc.certMatchBy, tc.certMatch)
		t.Run(fmt.Sprintf(testName, tc.certStore, tc.certMatchBy, tc.certMatch, tc.caCertsMatch), func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					if tc.expectedLeafCount != 0 {
						t.Fatalf("did not expect panic: %s", testName)
					} else {
						if !strings.Contains(fmt.Sprintf("%v", r), "Error processing configuration file") {
							t.Fatalf("did not expect unknown panic: %s", testName)
						}
					}
				}
			}()
			runConfiguredLeaf(t, hubOptions.LeafNode.Port,
				tc.certStore, tc.certMatchBy, tc.certMatch,
				tc.caCertsMatch, tc.expectedLeafCount)
		})
	}
}

// TestServerTLSWindowsCertStore tests the topology of a NATS server requiring TLS and gettings it own server
// cert identity (as used when accepting NATS client connections and negotiating TLS) from Windows certificate store.
func TestServerTLSWindowsCertStore(t *testing.T) {

	// Server Identity (server.pem)
	// Issuer: O = NATS CA, OU = NATS.io, CN = localhost
	// Subject: OU = NATS.io Operators, CN = localhost

	// Make sure windows cert store is reset to avoid conflict with other tests
	err := runPowershellScript("../test/configs/certs/tlsauth/certstore/delete-cert-from-store.ps1", nil)
	if err != nil {
		t.Fatalf("expected powershell cert delete to succeed: %s", err.Error())
	}

	// Provision Windows cert store with server cert and secret
	err = runPowershellScript("../test/configs/certs/tlsauth/certstore/import-p12-server.ps1", nil)
	if err != nil {
		t.Fatalf("expected powershell provision to succeed: %s", err.Error())
	}

	err = runPowershellScript("../test/configs/certs/tlsauth/certstore/import-p12-ca.ps1", nil)
	if err != nil {
		t.Fatalf("expected powershell provision CA to succeed: %s", err.Error())
	}

	// Fire up the server
	srvConfig := createConfFile(t, []byte(`
	listen: "localhost:-1"
	tls {
		cert_store: "WindowsCurrentUser"
		cert_match_by: "Subject"
		cert_match: "NATS.io Operators"
		ca_certs_match: ["NATS CA"]
		timeout: 5
	}
	`))
	defer removeFile(t, srvConfig)
	srvServer, _ := RunServerWithConfig(srvConfig)
	if srvServer == nil {
		t.Fatalf("expected to be able start server with cert store configuration")
	}
	defer srvServer.Shutdown()

	testCases := []struct {
		clientCA string
		expect   bool
	}{
		{"../test/configs/certs/tlsauth/ca.pem", true},
		{"../test/configs/certs/tlsauth/client.pem", false},
	}
	for _, tc := range testCases {
		t.Run(fmt.Sprintf("Client CA: %s", tc.clientCA), func(t *testing.T) {
			nc, _ := nats.Connect(srvServer.clientConnectURLs[0], nats.RootCAs(tc.clientCA))
			err := nc.Publish("foo", []byte("hello TLS server-authenticated server"))
			if (err != nil) == tc.expect {
				t.Fatalf("expected publish result %v to TLS authenticated server", tc.expect)
			}
			nc.Close()

			for i := 0; i < 5; i++ {
				nc, _ = nats.Connect(srvServer.clientConnectURLs[0], nats.RootCAs(tc.clientCA))
				err = nc.Publish("foo", []byte("hello TLS server-authenticated server"))
				if (err != nil) == tc.expect {
					t.Fatalf("expected repeated connection result %v to TLS authenticated server", tc.expect)
				}
				nc.Close()
			}
		})
	}
}

// TestServerIgnoreExpiredCerts tests if the server skips expired certificates in configuration, and finds non-expired ones
func TestServerIgnoreExpiredCerts(t *testing.T) {

	// Server Identities: expired.pem; not-expired.pem
	// Issuer: OU = NATS.io, CN = localhost
	// Subject: OU = NATS.io Operators, CN = localhost

	testCases := []struct {
		certFile string
		expect   bool
	}{
		{"expired.p12", false},
		{"not-expired.p12", true},
	}
	for _, tc := range testCases {
		t.Run(fmt.Sprintf("Server certificate: %s", tc.certFile), func(t *testing.T) {
			// Make sure windows cert store is reset to avoid conflict with other tests
			err := runPowershellScript("../test/configs/certs/tlsauth/certstore/delete-cert-from-store.ps1", nil)
			if err != nil {
				t.Fatalf("expected powershell cert delete to succeed: %s", err.Error())
			}

			// Provision Windows cert store with server cert and secret
			err = runPowershellScript("../test/configs/certs/tlsauth/certstore/import-p12-server.ps1", []string{tc.certFile})
			if err != nil {
				t.Fatalf("expected powershell provision to succeed: %s", err.Error())
			}
			// Fire up the server
			srvConfig := createConfFile(t, []byte(`
			listen: "localhost:-1"
			tls {
				cert_store: "WindowsCurrentUser"
				cert_match_by: "Subject"
				cert_match: "NATS.io Operators"
				cert_match_skip_invalid: true
				timeout: 5
			}
			`))
			defer removeFile(t, srvConfig)
			cfg, _ := ProcessConfigFile(srvConfig)
			if (cfg != nil) == tc.expect {
				return
			}
			if tc.expect == false {
				t.Fatalf("expected server start to fail with expired certificate")
			} else {
				t.Fatalf("expected server to start with non expired certificate")
			}
		})
	}
}

func TestWindowsTLS12ECDSA(t *testing.T) {
	err := runPowershellScript("../test/configs/certs/tlsauth/certstore/import-p12-server.ps1", []string{"ecdsa_server.pfx"})
	if err != nil {
		t.Fatalf("expected powershell provision to succeed: %v", err)
	}

	config := createConfFile(t, []byte(`
	listen: "localhost:-1"
	tls {
		cert_store: "WindowsCurrentUser"
		cert_match_by: "Thumbprint"
		cert_match: "4F8AF21756E5DBBD54619BBB6F3CC5D455ED4468"
		cert_match_skip_invalid: true
		timeout: 5
	}
	`))
	defer removeFile(t, config)

	srv, _ := RunServerWithConfig(config)
	if srv == nil {
		t.Fatalf("expected to be able start server with cert store configuration")
	}
	defer srv.Shutdown()

	for name, version := range map[string]uint16{
		"TLS 1.3": tls.VersionTLS13,
		"TLS 1.2": tls.VersionTLS12,
	} {
		t.Run(name, func(t *testing.T) {
			tc := &tls.Config{MaxVersion: version, MinVersion: version, InsecureSkipVerify: true}

			if _, err = nats.Connect(srv.clientConnectURLs[0], nats.Secure(tc)); err != nil {
				t.Fatalf("connection with %s: %v", name, err)
			}

			t.Logf("successful connection with %s", name)
		})
	}
}
