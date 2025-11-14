// Copyright (c) 2011-2015 Michael Mitton (mmitton@gmail.com)
// Portions copyright (c) 2015-2016 go-ldap Authors
// Static-Check Fixes Copyright 2024 The NATS Authors

package ldap

import (
	"reflect"
	"testing"
)

func TestSuccessfulDNParsing(t *testing.T) {
	testcases := map[string]DN{
		"": {RDNs: []*RelativeDN{}},
		"cn=Jim\\2C \\22Hasse Hö\\22 Hansson!,dc=dummy,dc=com": {RDNs: []*RelativeDN{
			{Attributes: []*AttributeTypeAndValue{{Type: "cn", Value: "Jim, \"Hasse Hö\" Hansson!"}}},
			{Attributes: []*AttributeTypeAndValue{{Type: "dc", Value: "dummy"}}},
			{Attributes: []*AttributeTypeAndValue{{Type: "dc", Value: "com"}}}}},
		"UID=jsmith,DC=example,DC=net": {RDNs: []*RelativeDN{
			{Attributes: []*AttributeTypeAndValue{{Type: "UID", Value: "jsmith"}}},
			{Attributes: []*AttributeTypeAndValue{{Type: "DC", Value: "example"}}},
			{Attributes: []*AttributeTypeAndValue{{Type: "DC", Value: "net"}}}}},
		"OU=Sales+CN=J. Smith,DC=example,DC=net": {RDNs: []*RelativeDN{
			{Attributes: []*AttributeTypeAndValue{
				{Type: "OU", Value: "Sales"},
				{Type: "CN", Value: "J. Smith"}}},
			{Attributes: []*AttributeTypeAndValue{{Type: "DC", Value: "example"}}},
			{Attributes: []*AttributeTypeAndValue{{Type: "DC", Value: "net"}}}}},
		//
		// "1.3.6.1.4.1.1466.0=#04024869": {[]*RelativeDN{
		// 	{[]*AttributeTypeAndValue{{"1.3.6.1.4.1.1466.0", "Hi"}}}}},
		// "1.3.6.1.4.1.1466.0=#04024869,DC=net": {[]*RelativeDN{
		// 	{[]*AttributeTypeAndValue{{"1.3.6.1.4.1.1466.0", "Hi"}}},
		// 	{[]*AttributeTypeAndValue{{"DC", "net"}}}}},
		"CN=Lu\\C4\\8Di\\C4\\87": {RDNs: []*RelativeDN{
			{Attributes: []*AttributeTypeAndValue{{Type: "CN", Value: "Lučić"}}}}},
		"  CN  =  Lu\\C4\\8Di\\C4\\87  ": {RDNs: []*RelativeDN{
			{Attributes: []*AttributeTypeAndValue{{Type: "CN", Value: "Lučić"}}}}},
		`   A   =   1   ,   B   =   2   `: {RDNs: []*RelativeDN{
			{Attributes: []*AttributeTypeAndValue{{Type: "A", Value: "1"}}},
			{Attributes: []*AttributeTypeAndValue{{Type: "B", Value: "2"}}}}},
		`   A   =   1   +   B   =   2   `: {RDNs: []*RelativeDN{
			{Attributes: []*AttributeTypeAndValue{
				{Type: "A", Value: "1"},
				{Type: "B", Value: "2"}}}}},
		`   \ \ A\ \    =   \ \ 1\ \    ,   \ \ B\ \    =   \ \ 2\ \    `: {RDNs: []*RelativeDN{
			{Attributes: []*AttributeTypeAndValue{{Type: "  A  ", Value: "  1  "}}},
			{Attributes: []*AttributeTypeAndValue{{Type: "  B  ", Value: "  2  "}}}}},
		`   \ \ A\ \    =   \ \ 1\ \    +   \ \ B\ \    =   \ \ 2\ \    `: {RDNs: []*RelativeDN{
			{Attributes: []*AttributeTypeAndValue{
				{Type: "  A  ", Value: "  1  "},
				{Type: "  B  ", Value: "  2  "}}}}},
	}

	for test, answer := range testcases {
		dn, err := ParseDN(test)
		if err != nil {
			t.Error(err.Error())
			continue
		}
		if !reflect.DeepEqual(dn, &answer) {
			t.Errorf("Parsed DN %s is not equal to the expected structure", test)
			t.Logf("Expected:")
			for _, rdn := range answer.RDNs {
				for _, attribs := range rdn.Attributes {
					t.Logf("#%v\n", attribs)
				}
			}
			t.Logf("Actual:")
			for _, rdn := range dn.RDNs {
				for _, attribs := range rdn.Attributes {
					t.Logf("#%v\n", attribs)
				}
			}
		}
	}
}

func TestErrorDNParsing(t *testing.T) {
	testcases := map[string]string{
		"*":               "DN ended with incomplete type, value pair",
		"cn=Jim\\0Test":   "failed to decode escaped character: encoding/hex: invalid byte: U+0054 'T'",
		"cn=Jim\\0":       "got corrupted escaped character",
		"DC=example,=net": "DN ended with incomplete type, value pair",
		// "1=#0402486":              "failed to decode BER encoding: encoding/hex: odd length hex string",
		"test,DC=example,DC=com":  "incomplete type, value pair",
		"=test,DC=example,DC=com": "incomplete type, value pair",
	}

	for test, answer := range testcases {
		_, err := ParseDN(test)
		if err == nil {
			t.Errorf("Expected %s to fail parsing but succeeded\n", test)
		} else if err.Error() != answer {
			t.Errorf("Unexpected error on %s:\n%s\nvs.\n%s\n", test, answer, err.Error())
		}
	}
}

func TestDNEqual(t *testing.T) {
	testcases := []struct {
		A     string
		B     string
		Equal bool
	}{
		// Exact match
		{A: "", B: "", Equal: true},
		{A: "o=A", B: "o=A", Equal: true},
		{A: "o=A", B: "o=B", Equal: false},

		{A: "o=A,o=B", B: "o=A,o=B", Equal: true},
		{A: "o=A,o=B", B: "o=A,o=C", Equal: false},

		{A: "o=A+o=B", B: "o=A+o=B", Equal: true},
		{A: "o=A+o=B", B: "o=A+o=C", Equal: false},

		// Case mismatch in type is ignored
		{A: "o=A", B: "O=A", Equal: true},
		{A: "o=A,o=B", B: "o=A,O=B", Equal: true},
		{A: "o=A+o=B", B: "o=A+O=B", Equal: true},

		// Case mismatch in value is significant
		{A: "o=a", B: "O=A", Equal: false},
		{A: "o=a,o=B", B: "o=A,O=B", Equal: false},
		{A: "o=a+o=B", B: "o=A+O=B", Equal: false},

		// Multi-valued RDN order mismatch is ignored
		{A: "o=A+o=B", B: "O=B+o=A", Equal: true},
		// Number of RDN attributes is significant
		{A: "o=A+o=B", B: "O=B+o=A+O=B", Equal: false},

		// Missing values are significant
		{A: "o=A+o=B", B: "O=B+o=A+O=C", Equal: false}, // missing values matter
		{A: "o=A+o=B+o=C", B: "O=B+o=A", Equal: false}, // missing values matter

		// Whitespace tests
		// Matching
		{
			A:     "cn=John Doe, ou=People, dc=sun.com",
			B:     "cn=John Doe, ou=People, dc=sun.com",
			Equal: true,
		},
		// Difference in leading/trailing chars is ignored
		{
			A:     "cn=John Doe, ou=People, dc=sun.com",
			B:     "cn=John Doe,ou=People,dc=sun.com",
			Equal: true,
		},
		// Difference in values is significant
		{
			A:     "cn=John Doe, ou=People, dc=sun.com",
			B:     "cn=John  Doe, ou=People, dc=sun.com",
			Equal: false,
		},
	}

	for i, tc := range testcases {
		a, err := ParseDN(tc.A)
		if err != nil {
			t.Errorf("%d: %v", i, err)
			continue
		}
		b, err := ParseDN(tc.B)
		if err != nil {
			t.Errorf("%d: %v", i, err)
			continue
		}
		if expected, actual := tc.Equal, a.Equal(b); expected != actual {
			t.Errorf("%d: when comparing '%s' and '%s' expected %v, got %v", i, tc.A, tc.B, expected, actual)
			continue
		}
		if expected, actual := tc.Equal, b.Equal(a); expected != actual {
			t.Errorf("%d: when comparing '%s' and '%s' expected %v, got %v", i, tc.A, tc.B, expected, actual)
			continue
		}
	}
}

func TestDNAncestor(t *testing.T) {
	testcases := []struct {
		A        string
		B        string
		Ancestor bool
	}{
		// Exact match returns false
		{A: "", B: "", Ancestor: false},
		{A: "o=A", B: "o=A", Ancestor: false},
		{A: "o=A,o=B", B: "o=A,o=B", Ancestor: false},
		{A: "o=A+o=B", B: "o=A+o=B", Ancestor: false},

		// Mismatch
		{A: "ou=C,ou=B,o=A", B: "ou=E,ou=D,ou=B,o=A", Ancestor: false},

		// Descendant
		{A: "ou=C,ou=B,o=A", B: "ou=E,ou=C,ou=B,o=A", Ancestor: true},
	}

	for i, tc := range testcases {
		a, err := ParseDN(tc.A)
		if err != nil {
			t.Errorf("%d: %v", i, err)
			continue
		}
		b, err := ParseDN(tc.B)
		if err != nil {
			t.Errorf("%d: %v", i, err)
			continue
		}
		if expected, actual := tc.Ancestor, a.AncestorOf(b); expected != actual {
			t.Errorf("%d: when comparing '%s' and '%s' expected %v, got %v", i, tc.A, tc.B, expected, actual)
			continue
		}
	}
}
