// Copyright 2012 Apcera Inc. All rights reserved.

package server

import "errors"

var (
	ErrConnectionClosed = errors.New("Connection closed")
	ErrAuthorization    = errors.New("Authorization Error")
)
