// Copyright 2012 Apcera Inc. All rights reserved.

package server

import "errors"

var (
	// ErrConnectionClosed represents error condition on a closed connection.
	ErrConnectionClosed = errors.New("Connection closed")

	// ErrAuthorization represents error condition on failed authorization.
	ErrAuthorization = errors.New("Authorization Error")
)
