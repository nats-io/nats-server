// Copyright 2012-2018 The NATS Authors
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

import "errors"

var (
	// ErrConnectionClosed represents an error condition on a closed connection.
	ErrConnectionClosed = errors.New("connection closed")

	// ErrAuthorization represents an error condition on failed authorization.
	ErrAuthorization = errors.New("authorization error")

	// ErrAuthTimeout represents an error condition on failed authorization due to timeout.
	ErrAuthTimeout = errors.New("authorization timeout")

	// ErrMaxPayload represents an error condition when the payload is too big.
	ErrMaxPayload = errors.New("maximum payload exceeded")

	// ErrMaxControlLine represents an error condition when the control line is too big.
	ErrMaxControlLine = errors.New("maximum control line exceeded")

	// ErrReservedPublishSubject represents an error condition when sending to a reserved subject, e.g. _SYS.>
	ErrReservedPublishSubject = errors.New("reserved internal subject")

	// ErrBadClientProtocol signals a client requested an invalud client protocol.
	ErrBadClientProtocol = errors.New("invalid client protocol")

	// ErrTooManyConnections signals a client that the maximum number of connections supported by the
	// server has been reached.
	ErrTooManyConnections = errors.New("maximum connections exceeded")

	// ErrTooManySubs signals a client that the maximum number of subscriptions per connection
	// has been reached.
	ErrTooManySubs = errors.New("maximum subscriptions exceeded")

	// ErrClientConnectedToRoutePort represents an error condition when a client
	// attempted to connect to the route listen port.
	ErrClientConnectedToRoutePort = errors.New("attempted to connect to route port")
)
