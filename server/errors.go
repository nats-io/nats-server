// Copyright 2012 Apcera Inc. All rights reserved.

package server

import "errors"

var (
	// ErrConnectionClosed represents error condition on a closed connection.
	ErrConnectionClosed = errors.New("Connection Closed")

	// ErrAuthorization represents error condition on failed authorization.
	ErrAuthorization = errors.New("Authorization Error")

	// ErrAuthTimeout represents error condition on failed authorization due to timeout.
	ErrAuthTimeout = errors.New("Authorization Timeout")

	// ErrMaxPayload represents error condition when the payload is too big.
	ErrMaxPayload = errors.New("Maximum Payload Exceeded")
)
