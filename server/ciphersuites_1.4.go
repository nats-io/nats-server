// Copyright 2015 Apcera Inc. All rights reserved.

// +build go1.4,!go1.5

package server

import (
	"crypto/tls"
)

func defaultCipherSuites() []uint16 {
	return []uint16{
		tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
		tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
	}
}
