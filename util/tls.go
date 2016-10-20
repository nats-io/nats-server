// Copyright 2016 Apcera Inc. All rights reserved.

package util

import (
	"crypto/tls"
	"reflect"
)

// CloneTLSConfig returns a copy of c. Only the exported fields are copied.
// This is temporary, until this is provided by the language.
// https://go-review.googlesource.com/#/c/28075/
func CloneTLSConfig(c *tls.Config) *tls.Config {
	newConfig := &tls.Config{
		Rand:                     c.Rand,
		Time:                     c.Time,
		Certificates:             c.Certificates,
		NameToCertificate:        c.NameToCertificate,
		GetCertificate:           c.GetCertificate,
		RootCAs:                  c.RootCAs,
		NextProtos:               c.NextProtos,
		ServerName:               c.ServerName,
		ClientAuth:               c.ClientAuth,
		ClientCAs:                c.ClientCAs,
		InsecureSkipVerify:       c.InsecureSkipVerify,
		CipherSuites:             c.CipherSuites,
		PreferServerCipherSuites: c.PreferServerCipherSuites,
		SessionTicketsDisabled:   c.SessionTicketsDisabled,
		SessionTicketKey:         c.SessionTicketKey,
		ClientSessionCache:       c.ClientSessionCache,
		MinVersion:               c.MinVersion,
		MaxVersion:               c.MaxVersion,
		CurvePreferences:         c.CurvePreferences,
	}
	fieldName := "DynamicRecordSizingDisabled"
	if cField := reflect.ValueOf(c).Elem().FieldByName(fieldName); cField.IsValid() {
		newField := reflect.ValueOf(newConfig).Elem().FieldByName(fieldName)
		newField.SetBool(cField.Bool())

		fieldName = "Renegotiation"
		cField = reflect.ValueOf(c).Elem().FieldByName(fieldName)
		newField = reflect.ValueOf(newConfig).Elem().FieldByName(fieldName)
		newField.SetInt(cField.Int())
	}
	return newConfig
}
