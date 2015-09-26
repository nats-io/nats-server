// Copyright 2012-2014 Apcera Inc. All rights reserved.

package gnatsd

type Auth interface {
	Check(c ClientAuth) bool
}

type ClientAuth interface {
	GetOpts() *clientOpts
}
