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
	CommonName string
	// crypto/x509 spells both of these "SerialNumber": the CA-assigned one
	// on the certificate, the subject DN attribute on its pkix.Name.
	SerialNumber        string
	SubjectSerialNumber string
	Organization        []string
	OrganizationalUnit  []string
	Locality            []string
	Province            []string
	Country             []string
	StreetAddress       []string
	PostalCode          []string

	DNSNames       []string
	EmailAddresses []string
	IPAddresses    []string
	URIs           []string
}

func newCertIDMapData(cert *x509.Certificate) *certIDMapData {
	d := &certIDMapData{
		CommonName:          cert.Subject.CommonName,
		SubjectSerialNumber: cert.Subject.SerialNumber,
		Organization:        cert.Subject.Organization,
		OrganizationalUnit:  cert.Subject.OrganizationalUnit,
		Locality:            cert.Subject.Locality,
		Province:            cert.Subject.Province,
		Country:             cert.Subject.Country,
		StreetAddress:       cert.Subject.StreetAddress,
		PostalCode:          cert.Subject.PostalCode,
		DNSNames:            cert.DNSNames,
		EmailAddresses:      cert.EmailAddresses,
	}
	if cert.SerialNumber != nil {
		d.SerialNumber = cert.SerialNumber.String()
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

// sampleCertIDMapData is the data a template is validated against. n sets
// both slice length and string length, since index/slice work on strings
// too; see certIDMapSampleLen.
func sampleCertIDMapData(n int) *certIDMapData {
	scalar := "sample"
	if n > len(scalar) {
		scalar = strings.Repeat("s", n)
	}
	sample := make([]string, n)
	for i := range sample {
		sample[i] = scalar
	}
	return &certIDMapData{
		CommonName:          scalar,
		SerialNumber:        scalar,
		SubjectSerialNumber: scalar,
		Organization:        sample,
		OrganizationalUnit:  sample,
		Locality:            sample,
		Province:            sample,
		Country:             sample,
		StreetAddress:       sample,
		PostalCode:          sample,
		DNSNames:            sample,
		EmailAddresses:      sample,
		IPAddresses:         sample,
		URIs:                sample,
	}
}

// validateCertIDMapTemplate executes every {{if}}/{{else}} branch against
// sample data, so a mistake gated on a real certificate's value (an OU of
// "prod", say) still fails at config load. Executing a branch on its own is
// only sound because dot can never be rebound - see
// collectCertIDMapBranchLists.
func validateCertIDMapTemplate(tmpl *template.Template) error {
	var (
		lists   []*parse.ListNode
		sampleN = minCertIDMapSampleLen
	)
	// A {{define}} block gets its own tree, so tmpl.Tree.Root alone would
	// leave its contents unvalidated.
	for _, t := range tmpl.Templates() {
		if t.Tree == nil {
			continue
		}
		if err := collectCertIDMapBranchLists(t.Tree.Root, &lists); err != nil {
			return err
		}
		if n := certIDMapSampleLen(t.Tree.Root); n > sampleN {
			sampleN = n
		}
	}
	root := tmpl.Tree.Root
	defer func() { tmpl.Tree.Root = root }()
	data := sampleCertIDMapData(sampleN)
	for _, list := range lists {
		tmpl.Tree.Root = list
		if err := tmpl.Execute(new(strings.Builder), data); err != nil {
			return err
		}
	}
	return nil
}

// collectCertIDMapBranchLists gathers a tree's root list and every
// {{if}}/{{else}} branch list beneath it, rejecting what would make a branch
// unsafe to execute alone: range, with, template and variable declarations.
func collectCertIDMapBranchLists(root parse.Node, lists *[]*parse.ListNode) error {
	return walkCertIDMapNodes(root, func(n parse.Node) error {
		switch x := n.(type) {
		case *parse.ListNode:
			*lists = append(*lists, x)
		case *parse.RangeNode:
			return fmt.Errorf("range is not supported in verify_and_map templates")
		case *parse.WithNode:
			return fmt.Errorf("with is not supported in verify_and_map templates")
		case *parse.TemplateNode:
			return fmt.Errorf("template is not supported in verify_and_map templates")
		case *parse.PipeNode:
			if len(x.Decl) > 0 {
				return fmt.Errorf("variable declarations are not supported in verify_and_map templates")
			}
		case *parse.CommandNode:
			// and/or short-circuit, so give each operand that might be
			// skipped its own list. Args[1] always runs and the walk already
			// covers it - starting at Args[2] keeps this linear.
			if !isCertIDMapShortCircuitCall(x) || len(x.Args) < 3 {
				return nil
			}
			for _, a := range x.Args[2:] {
				// nil, unlike every other literal, isn't valid as a
				// standalone action even though it's a normal operand.
				if isCertIDMapNilOperand(a) {
					continue
				}
				*lists = append(*lists, certIDMapOperandList(a))
			}
		}
		return nil
	})
}

func isCertIDMapShortCircuitCall(cmd *parse.CommandNode) bool {
	if len(cmd.Args) == 0 {
		return false
	}
	ident, ok := cmd.Args[0].(*parse.IdentifierNode)
	return ok && (ident.Ident == "and" || ident.Ident == "or")
}

// Bare nil only: parenthesized (nil) is genuinely invalid and must still be
// validated.
func isCertIDMapNilOperand(n parse.Node) bool {
	_, ok := n.(*parse.NilNode)
	return ok
}

const minCertIDMapSampleLen = 2

// Caps growth so a mistyped huge literal fails validation rather than
// allocating.
const maxCertIDMapSampleLen = 4096

// certIDMapSampleLen sizes the sample to cover any literal index/slice bound
// in the template, so `{{index .OrganizationalUnit 2}}` isn't rejected just
// for reaching past it.
func certIDMapSampleLen(root parse.Node) int {
	n := minCertIDMapSampleLen
	_ = walkCertIDMapNodes(root, func(node parse.Node) error {
		cmd, ok := node.(*parse.CommandNode)
		if !ok || len(cmd.Args) < 2 {
			return nil
		}
		ident, ok := cmd.Args[0].(*parse.IdentifierNode)
		if !ok || (ident.Ident != "index" && ident.Ident != "slice") {
			return nil
		}
		for _, a := range cmd.Args[1:] {
			num, ok := certIDMapLiteralNumber(a)
			if !ok || !num.IsInt || num.Int64 < 0 {
				continue
			}
			if need := int(num.Int64) + 1; need > n && need <= maxCertIDMapSampleLen {
				n = need
			}
		}
		return nil
	})
	return n
}

// certIDMapLiteralNumber unwraps any parenthesized layers, so `((20))` reads
// the same as a bare `20`.
func certIDMapLiteralNumber(n parse.Node) (*parse.NumberNode, bool) {
	for {
		p, ok := n.(*parse.PipeNode)
		if !ok || len(p.Cmds) != 1 || len(p.Cmds[0].Args) != 1 {
			break
		}
		n = p.Cmds[0].Args[0]
	}
	num, ok := n.(*parse.NumberNode)
	return num, ok
}

// walkCertIDMapNodes calls visit on n and every node beneath it, stopping at
// the first error. Types with no case here are leaves for the constructs
// this file accepts.
func walkCertIDMapNodes(n parse.Node, visit func(parse.Node) error) error {
	// A missing {{else}} is a nil *ListNode held in a non-nil parse.Node.
	switch x := n.(type) {
	case nil:
		return nil
	case *parse.ListNode:
		if x == nil {
			return nil
		}
	case *parse.PipeNode:
		if x == nil {
			return nil
		}
	}
	if err := visit(n); err != nil {
		return err
	}
	switch x := n.(type) {
	case *parse.ListNode:
		for _, c := range x.Nodes {
			if err := walkCertIDMapNodes(c, visit); err != nil {
				return err
			}
		}
	case *parse.IfNode:
		if err := walkCertIDMapNodes(x.Pipe, visit); err != nil {
			return err
		}
		if err := walkCertIDMapNodes(x.List, visit); err != nil {
			return err
		}
		return walkCertIDMapNodes(x.ElseList, visit)
	case *parse.ActionNode:
		return walkCertIDMapNodes(x.Pipe, visit)
	case *parse.PipeNode:
		for _, c := range x.Cmds {
			if err := walkCertIDMapNodes(c, visit); err != nil {
				return err
			}
		}
	case *parse.CommandNode:
		for _, a := range x.Args {
			if err := walkCertIDMapNodes(a, visit); err != nil {
				return err
			}
		}
	case *parse.ChainNode:
		return walkCertIDMapNodes(x.Node, visit)
	}
	return nil
}

// certIDMapOperandList wraps an operand in a standalone action list so it can
// be executed even where and/or would skip it.
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

// mapCertToUser derives a user from the peer certificate, via the
// `verify_and_map` template if one is configured and the default
// email -> SAN -> DN precedence otherwise.
func mapCertToUser(c *client, tmpl *template.Template, lookup func(string) (string, bool), legacy tlsMapAuthFn) bool {
	if tmpl != nil {
		return mapCertTemplateToUser(c, tmpl, lookup)
	}
	return checkClientTLSCertSubject(c, legacy)
}

func mapCertTemplateToUser(c *client, tmpl *template.Template, fn func(string) (string, bool)) bool {
	u, ok := execCertIDMapTemplate(c, tmpl)
	if !ok {
		return false
	}
	if u == _EMPTY_ {
		// Centralized so a blank configured username can never match.
		c.Debugf("User in cert from verify_and_map template is empty")
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
