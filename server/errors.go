// Copyright 2012-2019 The NATS Authors
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
	"errors"
	"fmt"
)

var (
	// ErrConnectionClosed represents an error condition on a closed connection.
	ErrConnectionClosed = errors.New("connection closed")

	// ErrAuthentication represents an error condition on failed authentication.
	ErrAuthentication = errors.New("authentication error")

	// ErrAuthTimeout represents an error condition on failed authorization due to timeout.
	ErrAuthTimeout = errors.New("authentication timeout")

	// ErrAuthExpired represents an expired authorization due to timeout.
	ErrAuthExpired = errors.New("authentication expired")

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

	// ErrTooManyAccountConnections signals that an acount has reached its maximum number of active
	// connections.
	ErrTooManyAccountConnections = errors.New("maximum account active connections exceeded")

	// ErrTooManySubs signals a client that the maximum number of subscriptions per connection
	// has been reached.
	ErrTooManySubs = errors.New("maximum subscriptions exceeded")

	// ErrClientConnectedToRoutePort represents an error condition when a client
	// attempted to connect to the route listen port.
	ErrClientConnectedToRoutePort = errors.New("attempted to connect to route port")

	// ErrClientConnectedToLeafNodePort represents an error condition when a client
	// attempted to connect to the leaf node listen port.
	ErrClientConnectedToLeafNodePort = errors.New("attempted to connect to leaf node port")

	// ErrAccountExists is returned when an account is attempted to be registered
	// but already exists.
	ErrAccountExists = errors.New("account exists")

	// ErrBadAccount represents a malformed or incorrect account.
	ErrBadAccount = errors.New("bad account")

	// ErrReservedAccount represents a reserved account that can not be created.
	ErrReservedAccount = errors.New("reserved account")

	// ErrMissingAccount is returned when an account does not exist.
	ErrMissingAccount = errors.New("account missing")

	// ErrAccountValidation is returned when an account has failed validation.
	ErrAccountValidation = errors.New("account validation failed")

	// ErrAccountExpired is returned when an account has expired.
	ErrAccountExpired = errors.New("account expired")

	// ErrNoAccountResolver is returned when we attempt an update but do not have an account resolver.
	ErrNoAccountResolver = errors.New("account resolver missing")

	// ErrAccountResolverUpdateTooSoon is returned when we attempt an update too soon to last request.
	ErrAccountResolverUpdateTooSoon = errors.New("account resolver update too soon")

	// ErrAccountResolverSameClaims is returned when same claims have been fetched.
	ErrAccountResolverSameClaims = errors.New("account resolver no new claims")

	// ErrStreamImportAuthorization is returned when a stream import is not authorized.
	ErrStreamImportAuthorization = errors.New("stream import not authorized")

	// ErrServiceImportAuthorization is returned when a service import is not authorized.
	ErrServiceImportAuthorization = errors.New("service import not authorized")

	// ErrClientOrRouteConnectedToGatewayPort represents an error condition when
	// a client or route attempted to connect to the Gateway port.
	ErrClientOrRouteConnectedToGatewayPort = errors.New("attempted to connect to gateway port")

	// ErrWrongGateway represents an error condition when a server receives a connect
	// request from a remote Gateway with a destination name that does not match the server's
	// Gateway's name.
	ErrWrongGateway = errors.New("wrong gateway")

	// ErrNoSysAccount is returned when an attempt to publish or subscribe is made
	// when there is no internal system account defined.
	ErrNoSysAccount = errors.New("system account not setup")
)

// configErr is a configuration error.
type configErr struct {
	token  token
	reason string
}

// Source reports the location of a configuration error.
func (e *configErr) Source() string {
	return fmt.Sprintf("%s:%d:%d", e.token.SourceFile(), e.token.Line(), e.token.Position())
}

// Error reports the location and reason from a configuration error.
func (e *configErr) Error() string {
	if e.token != nil {
		return fmt.Sprintf("%s: %s", e.Source(), e.reason)
	}
	return e.reason
}

// unknownConfigFieldErr is an error reported in pedantic mode.
type unknownConfigFieldErr struct {
	configErr
	field string
}

// Error reports that an unknown field was in the configuration.
func (e *unknownConfigFieldErr) Error() string {
	return fmt.Sprintf("%s: unknown field %q", e.Source(), e.field)
}

// configWarningErr is an error reported in pedantic mode.
type configWarningErr struct {
	configErr
	field string
}

// Error reports a configuration warning.
func (e *configWarningErr) Error() string {
	return fmt.Sprintf("%s: invalid use of field %q: %s", e.Source(), e.field, e.reason)
}

// processConfigErr is the result of processing the configuration from the server.
type processConfigErr struct {
	errors   []error
	warnings []error
}

// Error returns the collection of errors separated by new lines,
// warnings appear first then hard errors.
func (e *processConfigErr) Error() string {
	var msg string
	for _, err := range e.Warnings() {
		msg += err.Error() + "\n"
	}
	for _, err := range e.Errors() {
		msg += err.Error() + "\n"
	}
	return msg
}

// Warnings returns the list of warnings.
func (e *processConfigErr) Warnings() []error {
	return e.warnings
}

// Errors returns the list of errors.
func (e *processConfigErr) Errors() []error {
	return e.errors
}
