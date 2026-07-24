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
	"fmt"
	"slices"
	"strings"
	"text/template"
	"text/template/parse"
)

// certIDMapData is the value made available to a `verify_and_map` template.
// Field names mirror crypto/x509.Certificate and crypto/x509/pkix.Name.
// Subject attributes are slices since a DN may legally repeat an attribute
// (e.g. more than one OU).
type certIDMapData struct {
	CommonName         string
	SerialNumber       string
	Organization       []string
	OrganizationalUnit []string
	Locality           []string
	Province           []string
	Country            []string
	StreetAddress      []string
	PostalCode         []string

	DNSNames       []string
	EmailAddresses []string
	IPAddresses    []string
	URIs           []string
}

func newCertIDMapData(cert *x509.Certificate) *certIDMapData {
	d := &certIDMapData{
		CommonName:         cert.Subject.CommonName,
		SerialNumber:       cert.Subject.SerialNumber,
		Organization:       cert.Subject.Organization,
		OrganizationalUnit: cert.Subject.OrganizationalUnit,
		Locality:           cert.Subject.Locality,
		Province:           cert.Subject.Province,
		Country:            cert.Subject.Country,
		StreetAddress:      cert.Subject.StreetAddress,
		PostalCode:         cert.Subject.PostalCode,
		DNSNames:           cert.DNSNames,
		EmailAddresses:     cert.EmailAddresses,
	}
	for _, ip := range cert.IPAddresses {
		d.IPAddresses = append(d.IPAddresses, ip.String())
	}
	for _, u := range cert.URIs {
		d.URIs = append(d.URIs, u.String())
	}
	return d
}

// certIDMapFuncs adds membership/indexing helpers for multi-valued subject
// attributes (e.g. `{{if has "eng" .OrganizationalUnit}}`) on top of the
// standard text/template builtins.
var certIDMapFuncs = template.FuncMap{
	"has": func(needle string, haystack []string) bool {
		return slices.Contains(haystack, needle)
	},
	"first": func(s []string) string {
		if len(s) == 0 {
			return _EMPTY_
		}
		return s[0]
	},
}

// sampleCertIDMapData is used to validate a `verify_and_map` template at
// config-load time. Fields carry 2 entries, not 0, so a fixed-index lookup
// like `{{index .OrganizationalUnit 1}}` doesn't fail validation just
// because this sample is thinner than a real cert.
func sampleCertIDMapData() *certIDMapData {
	sample := []string{"sample", "sample"}
	return &certIDMapData{
		CommonName:         "sample",
		SerialNumber:       "sample",
		Organization:       sample,
		OrganizationalUnit: sample,
		Locality:           sample,
		Province:           sample,
		Country:            sample,
		StreetAddress:      sample,
		PostalCode:         sample,
		DNSNames:           sample,
		EmailAddresses:     sample,
		IPAddresses:        sample,
		URIs:               sample,
	}
}

// validateCertIDMapTemplate executes every {{if}}/{{else}} branch against
// sample data, not just whichever branch the sample happens to take, so a
// mistake gated on a real certificate's value (e.g. an OU of "prod") is
// still caught at config load.
//
// This is only sound because dot always refers to the root certIDMapData:
// collectCertIDMapBranchLists rejects anything that could rebind it or
// depend on a value from an enclosing scope.
func validateCertIDMapTemplate(tmpl *template.Template) error {
	var lists []*parse.ListNode
	if err := collectCertIDMapBranchLists(tmpl.Tree.Root, &lists); err != nil {
		return err
	}
	// Execute each collected branch list as if it were the whole template,
	// restoring the real root afterwards.
	root := tmpl.Tree.Root
	defer func() { tmpl.Tree.Root = root }()
	data := sampleCertIDMapData()
	for _, list := range lists {
		tmpl.Tree.Root = list
		if err := tmpl.Execute(new(strings.Builder), data); err != nil {
			return err
		}
	}
	return nil
}

// collectCertIDMapBranchLists gathers the root list and every {{if}}/{{else}}
// branch list beneath it, rejecting range/with/template (rebind dot or
// splice in another template) and variable declarations (undefined once
// their branch is executed in isolation).
func collectCertIDMapBranchLists(n parse.Node, lists *[]*parse.ListNode) error {
	if n == nil {
		return nil
	}
	switch x := n.(type) {
	case *parse.ListNode:
		if x == nil {
			return nil
		}
		*lists = append(*lists, x)
		for _, c := range x.Nodes {
			if err := collectCertIDMapBranchLists(c, lists); err != nil {
				return err
			}
		}
	case *parse.IfNode:
		if err := collectCertIDMapBranchLists(x.Pipe, lists); err != nil {
			return err
		}
		if err := collectCertIDMapBranchLists(x.List, lists); err != nil {
			return err
		}
		return collectCertIDMapBranchLists(x.ElseList, lists)
	case *parse.RangeNode:
		return fmt.Errorf("range is not supported in verify_and_map templates")
	case *parse.WithNode:
		return fmt.Errorf("with is not supported in verify_and_map templates")
	case *parse.TemplateNode:
		return fmt.Errorf("template is not supported in verify_and_map templates")
	case *parse.ActionNode:
		return collectCertIDMapBranchLists(x.Pipe, lists)
	case *parse.PipeNode:
		if x == nil {
			return nil
		}
		if len(x.Decl) > 0 {
			return fmt.Errorf("variable declarations are not supported in verify_and_map templates")
		}
		for _, c := range x.Cmds {
			if err := collectCertIDMapBranchLists(c, lists); err != nil {
				return err
			}
		}
	case *parse.CommandNode:
		// and/or short-circuit, so an operand after the first may never
		// actually be evaluated (e.g. `and (has "prod" .OU) .NoSuchField`
		// never touches .NoSuchField unless the sample OU is "prod") -
		// give each such operand its own list so it gets executed anyway.
		if isCertIDMapShortCircuitCall(x) {
			for _, a := range x.Args[1:] {
				// nil has no fields or calls to validate, and - unlike
				// every other literal - isn't valid as a standalone
				// action (`{{nil}}` fails to execute on its own even
				// though `and x nil` is a normal operand), so wrapping
				// it would reject an otherwise valid template.
				if isCertIDMapNilOperand(a) {
					continue
				}
				*lists = append(*lists, certIDMapOperandList(a))
			}
		}
		for _, a := range x.Args {
			if err := collectCertIDMapBranchLists(a, lists); err != nil {
				return err
			}
		}
	case *parse.ChainNode:
		return collectCertIDMapBranchLists(x.Node, lists)
	}
	return nil
}

// isCertIDMapShortCircuitCall reports whether cmd calls the short-circuiting
// and/or builtins, whose operands after the first aren't always evaluated.
func isCertIDMapShortCircuitCall(cmd *parse.CommandNode) bool {
	if len(cmd.Args) == 0 {
		return false
	}
	ident, ok := cmd.Args[0].(*parse.IdentifierNode)
	return ok && (ident.Ident == "and" || ident.Ident == "or")
}

// isCertIDMapNilOperand reports whether n is the bare, unparenthesized nil
// keyword. Parenthesized (nil) is deliberately not matched here: unlike bare
// nil it's genuinely invalid (text/template errors evaluating it, whether
// standalone or as a real and/or operand once short-circuiting reaches it),
// so it must still go through normal validation rather than being skipped.
func isCertIDMapNilOperand(n parse.Node) bool {
	_, ok := n.(*parse.NilNode)
	return ok
}

// certIDMapOperandList wraps a single operand node in a standalone action
// list, so it can be executed on its own even though text/template might
// never evaluate it in place (see isCertIDMapShortCircuitCall).
func certIDMapOperandList(n parse.Node) *parse.ListNode {
	pipe, ok := n.(*parse.PipeNode)
	if !ok {
		pipe = &parse.PipeNode{
			NodeType: parse.NodePipe,
			Cmds:     []*parse.CommandNode{{NodeType: parse.NodeCommand, Args: []parse.Node{n}}},
		}
	}
	return &parse.ListNode{
		NodeType: parse.NodeList,
		Nodes:    []parse.Node{&parse.ActionNode{NodeType: parse.NodeAction, Pipe: pipe}},
	}
}

// parseCertIDMapTemplate compiles and validates a `verify_and_map` template,
// so a template mistake fails at config load rather than on first connect.
func parseCertIDMapTemplate(name, text string) (*template.Template, error) {
	tmpl, err := template.New(name).Funcs(certIDMapFuncs).Parse(text)
	if err != nil {
		return nil, fmt.Errorf("invalid verify_and_map template: %w", err)
	}
	if err := validateCertIDMapTemplate(tmpl); err != nil {
		return nil, fmt.Errorf("invalid verify_and_map template: %w", err)
	}
	return tmpl, nil
}

func tlsCertMapTemplate(tc *TLSConfigOpts) *template.Template {
	if tc == nil {
		return nil
	}
	return tc.CertMap
}

func execCertIDMapTemplate(c *client, tmpl *template.Template) (string, bool) {
	tlsState := c.GetTLSConnectionState()
	if tlsState == nil || len(tlsState.PeerCertificates) == 0 || tlsState.PeerCertificates[0] == nil {
		c.Debugf("User required in cert, no peer certificates found")
		return _EMPTY_, false
	}
	cert := tlsState.PeerCertificates[0]
	if len(tlsState.PeerCertificates) > 1 {
		c.Debugf("Multiple peer certificates found, selecting first")
	}

	var sb strings.Builder
	if err := tmpl.Execute(&sb, newCertIDMapData(cert)); err != nil {
		c.Debugf("Error executing verify_and_map template: %v", err)
		return _EMPTY_, false
	}
	return sb.String(), true
}

func mapCertTemplateToUser(c *client, tmpl *template.Template, fn func(string) (string, bool)) bool {
	u, ok := execCertIDMapTemplate(c, tmpl)
	if !ok {
		return false
	}
	match, ok := fn(u)
	if ok {
		c.Debugf("Using verify_and_map template result for auth [%q]", match)
	} else {
		c.Debugf("User in cert [%q] from verify_and_map template, not found", u)
	}
	return ok
}
