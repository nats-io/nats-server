// Copyright 2025 The NATS Authors
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

package server

import (
	"crypto/x509"
	"crypto/x509/pkix"
	"fmt"
	"os"
	"strings"
	"testing"
)

func testCertForIDMap() *x509.Certificate {
	return &x509.Certificate{
		Subject: pkix.Name{
			CommonName:         "example.com",
			OrganizationalUnit: []string{"infrastructure", "eu-west"},
			Organization:       []string{"Synadia"},
			Country:            []string{"GB"},
		},
		DNSNames:       []string{"example.com"},
		EmailAddresses: []string{"user@example.com"},
	}
}

func TestCertIDMapTemplateFields(t *testing.T) {
	for _, test := range []struct {
		name     string
		tmpl     string
		expected string
	}{
		{"common name", "{{.CommonName}}", "example.com"},
		{"first OU", "{{first .OrganizationalUnit}}", "infrastructure"},
		{"indexed OU", "{{index .OrganizationalUnit 1}}", "eu-west"},
		{"has OU true", `{{if has "infrastructure" .OrganizationalUnit}}yes{{else}}no{{end}}`, "yes"},
		{"has OU false", `{{if has "marketing" .OrganizationalUnit}}yes{{else}}no{{end}}`, "no"},
		{"organization", "{{first .Organization}}", "Synadia"},
		{"san email", "{{first .EmailAddresses}}", "user@example.com"},
		{"san dns", "{{first .DNSNames}}", "example.com"},
		{"combine fields", "{{.CommonName}}-{{first .OrganizationalUnit}}", "example.com-infrastructure"},
		{"and combo match", `{{if and (has "infrastructure" .OrganizationalUnit) (has "GB" .Country)}}infra-gb{{end}}`, "infra-gb"},
		{"or combo match", `{{if or (has "marketing" .OrganizationalUnit) (has "GB" .Country)}}yes{{end}}`, "yes"},
	} {
		t.Run(test.name, func(t *testing.T) {
			tmpl, err := parseCertIDMapTemplate("test", test.tmpl)
			if err != nil {
				t.Fatalf("Error parsing template: %v", err)
			}
			var sb strings.Builder
			if err := tmpl.Execute(&sb, newCertIDMapData(testCertForIDMap())); err != nil {
				t.Fatalf("Error executing template: %v", err)
			}
			if got := sb.String(); got != test.expected {
				t.Fatalf("Expected %q, got %q", test.expected, got)
			}
		})
	}
}

// An index beyond sampleCertIDMapData's historical 2-element sample must
// still validate and work against a real (longer) certificate.
func TestCertIDMapTemplateDeepIndex(t *testing.T) {
	tmpl, err := parseCertIDMapTemplate("test", `{{index .OrganizationalUnit 2}}`)
	if err != nil {
		t.Fatalf("Error parsing template: %v", err)
	}
	cert := testCertForIDMap()
	cert.Subject.OrganizationalUnit = []string{"a", "b", "c"}
	var sb strings.Builder
	if err := tmpl.Execute(&sb, newCertIDMapData(cert)); err != nil {
		t.Fatalf("Error executing template: %v", err)
	}
	if got := sb.String(); got != "c" {
		t.Fatalf("Expected %q, got %q", "c", got)
	}
}

// Dynamic sample sizing extends the sample to fit a literal index - it
// doesn't disable bounds checking.
func TestCertIDMapTemplateAbsurdIndexRejected(t *testing.T) {
	_, err := parseCertIDMapTemplate("test", `{{index .OrganizationalUnit 999999999}}`)
	if err == nil {
		t.Fatalf("Expected an error for an absurd literal index")
	}
}

// index also works on strings (byte offset), not just []string - the
// scalar fields (CommonName, SerialNumber) need the same deep-index fix.
func TestCertIDMapTemplateDeepIndexOnScalarField(t *testing.T) {
	if _, err := parseCertIDMapTemplate("test", `{{index .CommonName 20}}`); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
}

// index .OrganizationalUnit 0 20 indexes the *string* at element 0, not the
// slice again - each sample slice element needs to be long enough too.
func TestCertIDMapTemplateDeepIndexOnSliceElement(t *testing.T) {
	if _, err := parseCertIDMapTemplate("test", `{{index .OrganizationalUnit 0 20}}`); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
}

// A parenthesized literal bound, e.g. `(20)`, must size the sample the same
// as a bare `20` - it's still just a NumberNode once unwrapped.
func TestCertIDMapTemplateParenthesizedLiteralIndex(t *testing.T) {
	if _, err := parseCertIDMapTemplate("test", `{{index .OrganizationalUnit ((20))}}`); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
}

// A template can restrict a match to a combination of fields (e.g. this OU,
// but only in this country) using text/template's `and` plus `has`.
func TestCertIDMapTemplateComboFields(t *testing.T) {
	const tmplText = `{{if and (has "infrastructure" .OrganizationalUnit) (has "GB" .Country)}}infra-gb{{end}}`
	tmpl, err := parseCertIDMapTemplate("test", tmplText)
	if err != nil {
		t.Fatalf("Error parsing template: %v", err)
	}

	for _, test := range []struct {
		name     string
		country  string
		expected string
	}{
		{"matching country", "GB", "infra-gb"},
		{"non-matching country", "US", ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			cert := testCertForIDMap()
			cert.Subject.Country = []string{test.country}

			var sb strings.Builder
			if err := tmpl.Execute(&sb, newCertIDMapData(cert)); err != nil {
				t.Fatalf("Error executing template: %v", err)
			}
			if got := sb.String(); got != test.expected {
				t.Fatalf("Expected %q, got %q", test.expected, got)
			}
		})
	}
}

func TestCertIDMapTemplateInvalidField(t *testing.T) {
	if _, err := parseCertIDMapTemplate("test", "{{.NoSuchField}}"); err == nil {
		t.Fatalf("Expected an error parsing/validating a template with an invalid field reference")
	}
}

// A bad field reference must be caught even in a branch the sample data
// never takes (e.g. one gated on OU "prod").
func TestCertIDMapTemplateInvalidFieldInUntakenBranch(t *testing.T) {
	_, err := parseCertIDMapTemplate("test", `{{if has "prod" .OrganizationalUnit}}{{.NoSuchField}}{{end}}`)
	if err == nil {
		t.Fatalf("Expected an error for an invalid field reference inside an untaken branch")
	}
}

// Same, but the bad field is behind a parenthesized chain: (.CommonName).NoSuchField.
func TestCertIDMapTemplateInvalidChainedFieldInUntakenBranch(t *testing.T) {
	_, err := parseCertIDMapTemplate("test", `{{if has "prod" .OrganizationalUnit}}{{(.CommonName).NoSuchField}}{{end}}`)
	if err == nil {
		t.Fatalf("Expected an error for an invalid chained field reference inside an untaken branch")
	}
}

// Same, but the bad field is referenced via the root variable: $.NoSuchField.
func TestCertIDMapTemplateInvalidVariableFieldInUntakenBranch(t *testing.T) {
	_, err := parseCertIDMapTemplate("test", `{{if has "prod" .OrganizationalUnit}}{{$.NoSuchField}}{{end}}`)
	if err == nil {
		t.Fatalf("Expected an error for an invalid $.Field reference inside an untaken branch")
	}
}

func TestCertIDMapTemplateValidRootVariable(t *testing.T) {
	if _, err := parseCertIDMapTemplate("test", `{{$.CommonName}}`); err != nil {
		t.Fatalf("Unexpected error for valid $.CommonName: %v", err)
	}
	if _, err := parseCertIDMapTemplate("test", `{{($.CommonName).Foo}}`); err == nil {
		t.Fatalf("Expected an error chaining a field onto $.CommonName (a string, not a struct)")
	}
}

func TestCertIDMapTemplateChainOnFunctionResultRejected(t *testing.T) {
	_, err := parseCertIDMapTemplate("test", `{{(first .OrganizationalUnit).Foo}}`)
	if err == nil {
		t.Fatalf("Expected an error for a chain built on a function call's result")
	}
}

func TestCertIDMapTemplateValidParenthesizedField(t *testing.T) {
	tmpl, err := parseCertIDMapTemplate("test", `{{(.CommonName)}}`)
	if err != nil {
		t.Fatalf("Unexpected error for a valid parenthesized field: %v", err)
	}
	var sb strings.Builder
	if err := tmpl.Execute(&sb, newCertIDMapData(testCertForIDMap())); err != nil {
		t.Fatalf("Error executing template: %v", err)
	}
	if got := sb.String(); got != "example.com" {
		t.Fatalf("Expected %q, got %q", "example.com", got)
	}
}

func TestCertIDMapTemplateRejectsRangeWithVariables(t *testing.T) {
	for _, tmpl := range []string{
		`{{range .OrganizationalUnit}}{{.}}{{end}}`,
		`{{with .CommonName}}{{.}}{{end}}`,
		`{{$x := .CommonName}}{{$x}}`,
	} {
		if _, err := parseCertIDMapTemplate("test", tmpl); err == nil {
			t.Fatalf("Expected an error rejecting unsupported construct: %s", tmpl)
		}
	}
}

func TestCertIDMapTemplateParseError(t *testing.T) {
	if _, err := parseCertIDMapTemplate("test", "{{.CommonName"); err == nil {
		t.Fatalf("Expected an error for malformed template syntax")
	}
}

func TestVerifyAndMapConfigTemplate(t *testing.T) {
	conf := createConfFile(t, []byte(`
		listen: 127.0.0.1:-1
		tls {
			cert_file: "../test/configs/certs/tlsauth/server.pem"
			key_file:  "../test/configs/certs/tlsauth/server-key.pem"
			ca_file:   "../test/configs/certs/tlsauth/ca.pem"
			verify_and_map: "{{ first .OrganizationalUnit }}"
		}
	`))
	opts, err := ProcessConfigFile(conf)
	if err != nil {
		t.Fatalf("Unexpected error processing config: %v", err)
	}
	if !opts.TLSMap {
		t.Fatalf("Expected TLSMap to be true")
	}
	if opts.tlsConfigOpts == nil || opts.tlsConfigOpts.CertMap == nil {
		t.Fatalf("Expected a compiled CertMap template")
	}
}

func TestVerifyAndMapConfigTemplateBadField(t *testing.T) {
	conf := createConfFile(t, []byte(`
		listen: 127.0.0.1:-1
		tls {
			cert_file: "../test/configs/certs/tlsauth/server.pem"
			key_file:  "../test/configs/certs/tlsauth/server-key.pem"
			ca_file:   "../test/configs/certs/tlsauth/ca.pem"
			verify_and_map: "{{ .NoSuchField }}"
		}
	`))
	if _, err := ProcessConfigFile(conf); err == nil {
		t.Fatalf("Expected an error for a template referencing a nonexistent field")
	}
}

func TestVerifyAndMapConfigTemplateNotSupportedForCluster(t *testing.T) {
	conf := createConfFile(t, []byte(`
		listen: 127.0.0.1:-1
		cluster {
			listen: 127.0.0.1:-1
			tls {
				cert_file: "../test/configs/certs/tlsauth/server.pem"
				key_file:  "../test/configs/certs/tlsauth/server-key.pem"
				ca_file:   "../test/configs/certs/tlsauth/ca.pem"
				verify_and_map: "{{ .CommonName }}"
			}
		}
	`))
	_, err := ProcessConfigFile(conf)
	if err == nil || !strings.Contains(err.Error(), "not supported in this context") {
		t.Fatalf("Expected a 'not supported in this context' error, got: %v", err)
	}
}

// gateway{} goes through getTLSConfig, a different call site than
// cluster/leafnode/client - needs its own coverage.
func TestVerifyAndMapConfigTemplateNotSupportedForGateway(t *testing.T) {
	conf := createConfFile(t, []byte(`
		listen: 127.0.0.1:-1
		gateway {
			name: A
			listen: 127.0.0.1:-1
			tls {
				cert_file: "../test/configs/certs/tlsauth/server.pem"
				key_file:  "../test/configs/certs/tlsauth/server-key.pem"
				ca_file:   "../test/configs/certs/tlsauth/ca.pem"
				verify_and_map: "{{ .CommonName }}"
			}
		}
	`))
	_, err := ProcessConfigFile(conf)
	if err == nil || !strings.Contains(err.Error(), "not supported in this context") {
		t.Fatalf("Expected a 'not supported in this context' error, got: %v", err)
	}
}

// A template-text-only change must be rejected on reload, not silently
// applied - Options.TLSCertMap mirrors the source text so reload's
// field-by-field diff can see the change (the compiled template itself
// lives on the unexported tlsConfigOpts snapshot).
func TestVerifyAndMapConfigTemplateChangeDetectedOnReload(t *testing.T) {
	confText := `
		listen: 127.0.0.1:-1
		tls {
			cert_file: "../test/configs/certs/tlsauth/server.pem"
			key_file:  "../test/configs/certs/tlsauth/server-key.pem"
			ca_file:   "../test/configs/certs/tlsauth/ca.pem"
			verify_and_map: "%s"
		}
	`
	conf := createConfFile(t, []byte(strings.ReplaceAll(confText, "%s", "{{ first .OrganizationalUnit }}")))
	opts, err := ProcessConfigFile(conf)
	if err != nil {
		t.Fatalf("Error processing config: %v", err)
	}
	opts.NoSigs = true
	s := RunServer(opts)
	defer s.Shutdown()

	newConfText := strings.ReplaceAll(confText, "%s", "{{ .CommonName }}")
	if err := os.WriteFile(conf, []byte(newConfText), 0666); err != nil {
		t.Fatalf("Error writing config: %v", err)
	}
	if err := s.Reload(); err == nil {
		t.Fatalf("Expected reload to reject a verify_and_map template change")
	}
}

// Same, for websocket{} and mqtt{} - each has its own TLSConfigOpts/TLSCertMap.
func TestVerifyAndMapConfigTemplateChangeDetectedOnReloadWebsocketAndMQTT(t *testing.T) {
	confText := `
		listen: 127.0.0.1:-1
		jetstream: { store_dir: ` + fmt.Sprintf("%q", t.TempDir()) + ` }
		websocket {
			listen: 127.0.0.1:-1
			tls {
				cert_file: "../test/configs/certs/tlsauth/server.pem"
				key_file:  "../test/configs/certs/tlsauth/server-key.pem"
				ca_file:   "../test/configs/certs/tlsauth/ca.pem"
				verify_and_map: "WSPLACEHOLDER"
			}
		}
		mqtt {
			listen: 127.0.0.1:-1
			tls {
				cert_file: "../test/configs/certs/tlsauth/server.pem"
				key_file:  "../test/configs/certs/tlsauth/server-key.pem"
				ca_file:   "../test/configs/certs/tlsauth/ca.pem"
				verify_and_map: "MQTTPLACEHOLDER"
			}
		}
	`
	render := func(ws, mqtt string) string {
		c := strings.ReplaceAll(confText, "WSPLACEHOLDER", ws)
		return strings.ReplaceAll(c, "MQTTPLACEHOLDER", mqtt)
	}

	conf := createConfFile(t, []byte(render("{{ first .OrganizationalUnit }}", "{{ first .OrganizationalUnit }}")))
	opts, err := ProcessConfigFile(conf)
	if err != nil {
		t.Fatalf("Error processing config: %v", err)
	}
	opts.NoSigs = true
	s := RunServer(opts)
	defer s.Shutdown()

	if err := os.WriteFile(conf, []byte(render("{{ .CommonName }}", "{{ first .OrganizationalUnit }}")), 0666); err != nil {
		t.Fatalf("Error writing config: %v", err)
	}
	if err := s.Reload(); err == nil {
		t.Fatalf("Expected reload to reject a verify_and_map template change on websocket")
	}

	if err := os.WriteFile(conf, []byte(render("{{ first .OrganizationalUnit }}", "{{ .CommonName }}")), 0666); err != nil {
		t.Fatalf("Error writing config: %v", err)
	}
	if err := s.Reload(); err == nil {
		t.Fatalf("Expected reload to reject a verify_and_map template change on mqtt")
	}
}

// first expects []string, not string - same untaken-branch problem as the
// field tests above, for a function argument instead of a field.
func TestCertIDMapTemplateInvalidArgTypeInUntakenBranch(t *testing.T) {
	_, err := parseCertIDMapTemplate("test", `{{if has "prod" .OrganizationalUnit}}{{first .CommonName}}{{end}}`)
	if err == nil {
		t.Fatalf("Expected an error for a wrong-typed function argument inside an untaken branch")
	}
}

func TestCertIDMapTemplateSwappedArgsRejected(t *testing.T) {
	_, err := parseCertIDMapTemplate("test", `{{has .OrganizationalUnit "prod"}}`)
	if err == nil {
		t.Fatalf("Expected an error for has() called with swapped argument types")
	}
}

func TestCertIDMapTemplateUnknownFunctionChainRejected(t *testing.T) {
	// len returns an int, which has no field named Foo.
	_, err := parseCertIDMapTemplate("test", `{{(len .OrganizationalUnit).Foo}}`)
	if err == nil {
		t.Fatalf("Expected an error chaining a field onto an unrecognized function's result")
	}
}

// Same problem, piped form: `.CommonName | first`.
func TestCertIDMapTemplateInvalidPipedArgTypeInUntakenBranch(t *testing.T) {
	_, err := parseCertIDMapTemplate("test", `{{if has "prod" .OrganizationalUnit}}{{.CommonName | first}}{{end}}`)
	if err == nil {
		t.Fatalf("Expected an error for a wrong-typed piped argument inside an untaken branch")
	}
}

func TestCertIDMapTemplateValidPipedArg(t *testing.T) {
	if _, err := parseCertIDMapTemplate("test", `{{.OrganizationalUnit | first}}`); err != nil {
		t.Fatalf("Unexpected error for valid piped usage: %v", err)
	}
}

func TestCertIDMapTemplateMultiStagePipeTypeMismatch(t *testing.T) {
	// .CommonName | first is a string, piped into first again - []string wanted.
	_, err := parseCertIDMapTemplate("test", `{{if has "prod" .OrganizationalUnit}}{{.CommonName | first | first}}{{end}}`)
	if err == nil {
		t.Fatalf("Expected an error for a type mismatch two pipe stages deep")
	}
}

// resolver_tls is outbound-only, so no cert is ever mapped to a user there.
func TestVerifyAndMapConfigTemplateNotSupportedForResolverTLS(t *testing.T) {
	conf := createConfFile(t, []byte(`
		listen: 127.0.0.1:-1
		operator: "../test/configs/nkeys/op.jwt"
		resolver: URL("http://localhost:9090/ngs/v1/accounts/")
		resolver_tls {
			cert_file: "../test/configs/certs/tlsauth/server.pem"
			key_file:  "../test/configs/certs/tlsauth/server-key.pem"
			ca_file:   "../test/configs/certs/tlsauth/ca.pem"
			verify_and_map: "{{ .CommonName }}"
		}
	`))
	_, err := ProcessConfigFile(conf)
	if err == nil || !strings.Contains(err.Error(), "not supported in this context") {
		t.Fatalf("Expected a 'not supported in this context' error, got: %v", err)
	}
}

// A leafnode remote's tls block is outbound too - only the inbound leaf{}
// listener maps a peer cert to a user.
func TestVerifyAndMapConfigTemplateNotSupportedForLeafRemote(t *testing.T) {
	conf := createConfFile(t, []byte(`
		listen: 127.0.0.1:-1
		leaf {
			remotes [
				{
					url: "tls://localhost:1234"
					tls {
						cert_file: "../test/configs/certs/tlsauth/client.pem"
						key_file:  "../test/configs/certs/tlsauth/client-key.pem"
						ca_file:   "../test/configs/certs/tlsauth/ca.pem"
						verify_and_map: "{{ .CommonName }}"
					}
				}
			]
		}
	`))
	_, err := ProcessConfigFile(conf)
	if err == nil || !strings.Contains(err.Error(), "not supported in this context") {
		t.Fatalf("Expected a 'not supported in this context' error, got: %v", err)
	}
}

// Wrong arity, same untaken-branch problem: first wants exactly one argument.
func TestCertIDMapTemplateTooFewArgsInUntakenBranch(t *testing.T) {
	_, err := parseCertIDMapTemplate("test", `{{if has "prod" .OrganizationalUnit}}{{first}}{{end}}`)
	if err == nil {
		t.Fatalf("Expected an error for a helper call with too few arguments inside an untaken branch")
	}
}

func TestCertIDMapTemplateTooManyArgsInUntakenBranch(t *testing.T) {
	_, err := parseCertIDMapTemplate("test", `{{if has "prod" .OrganizationalUnit}}{{has "prod" .OrganizationalUnit "extra"}}{{end}}`)
	if err == nil {
		t.Fatalf("Expected an error for a helper call with too many arguments inside an untaken branch")
	}
}

func TestCertIDMapTemplateTooFewPipedArgs(t *testing.T) {
	// has wants two arguments; a single piped value is one short.
	_, err := parseCertIDMapTemplate("test", `{{.OrganizationalUnit | has}}`)
	if err == nil {
		t.Fatalf("Expected an error for a piped helper call with too few arguments")
	}
}

// Numeric literals get the same treatment: has wants a string, not an int.
func TestCertIDMapTemplateNumericLiteralArgInUntakenBranch(t *testing.T) {
	_, err := parseCertIDMapTemplate("test", `{{if has "prod" .OrganizationalUnit}}{{has 1 .OrganizationalUnit}}{{end}}`)
	if err == nil {
		t.Fatalf("Expected an error for a numeric literal argument inside an untaken branch")
	}
}

// Builtins get the same untaken-branch treatment as has/first: eq comparing
// a string to an int is a runtime error.
func TestCertIDMapTemplateBuiltinTypeErrorInUntakenBranch(t *testing.T) {
	_, err := parseCertIDMapTemplate("test", `{{if has "prod" .OrganizationalUnit}}{{eq .CommonName 1}}{{end}}`)
	if err == nil {
		t.Fatalf("Expected an error for a builtin type mismatch inside an untaken branch")
	}
}

// The sample data takes the if-branch here, so the bad field in the
// else-branch is only caught if every branch gets executed.
func TestCertIDMapTemplateInvalidFieldInUntakenElseBranch(t *testing.T) {
	_, err := parseCertIDMapTemplate("test", `{{if has "sample" .OrganizationalUnit}}ok{{else}}{{.NoSuchField}}{{end}}`)
	if err == nil {
		t.Fatalf("Expected an error for an invalid field reference inside an untaken else branch")
	}
}

// and short-circuits past .NoSuchField with the sample data (OU isn't
// "prod"), but a real "prod" cert would reach it.
func TestCertIDMapTemplateShortCircuitedAndOperandRejected(t *testing.T) {
	_, err := parseCertIDMapTemplate("test", `{{if and (has "prod" .OrganizationalUnit) .NoSuchField}}x{{end}}`)
	if err == nil {
		t.Fatalf("Expected an error for a bad field short-circuited by and")
	}
}

// Same, but short-circuited the other way: the first operand is true, so
// or never evaluates .NoSuchField.
func TestCertIDMapTemplateShortCircuitedOrOperandRejected(t *testing.T) {
	_, err := parseCertIDMapTemplate("test", `{{if or (has "sample" .OrganizationalUnit) .NoSuchField}}x{{end}}`)
	if err == nil {
		t.Fatalf("Expected an error for a bad field short-circuited by or")
	}
}

// A valid parenthesized operand (rather than a bare field) must still work.
func TestCertIDMapTemplateShortCircuitedOperandValid(t *testing.T) {
	_, err := parseCertIDMapTemplate("test", `{{if and (has "sample" .OrganizationalUnit) (has "sample" .Country)}}x{{end}}`)
	if err != nil {
		t.Fatalf("Unexpected error for valid and operands: %v", err)
	}
}

// Bare nil is a valid and/or operand, unlike every other literal it isn't
// valid as a standalone action.
func TestCertIDMapTemplateShortCircuitedNilOperandValid(t *testing.T) {
	_, err := parseCertIDMapTemplate("test", `{{if and (has "sample" .OrganizationalUnit) nil}}x{{end}}`)
	if err != nil {
		t.Fatalf("Unexpected error for a nil and operand: %v", err)
	}
}

// Unlike bare nil, parenthesized (nil) is genuinely invalid and must still
// be rejected, even short-circuited past by the sample data.
func TestCertIDMapTemplateShortCircuitedParenNilOperandRejected(t *testing.T) {
	_, err := parseCertIDMapTemplate("test", `{{if and (has "prod" .OrganizationalUnit) (nil)}}x{{end}}`)
	if err == nil {
		t.Fatalf("Expected an error for a parenthesized nil and operand")
	}
}
