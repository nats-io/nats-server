// Copyright 2012-2016 Apcera Inc. All rights reserved.

package server

import "errors"

var (
	// ErrConnectionClosed represents an error condition on a closed connection.
	ErrConnectionClosed = errors.New("Connection Closed")

	// ErrAuthorization represents an error condition on failed authorization.
	ErrAuthorization = errors.New("Authorization Error")

	// ErrAuthTimeout represents an error condition on failed authorization due to timeout.
	ErrAuthTimeout = errors.New("Authorization Timeout")

	// ErrMaxPayload represents an error condition when the payload is too big.
	ErrMaxPayload = errors.New("Maximum Payload Exceeded")

	// ErrReservedPublish represents an error condition when the payload is too big.
	ErrReservedPublishSubject = errors.New("Reserved Internal Subject")
)
