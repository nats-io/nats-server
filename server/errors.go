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

import (
	"errors"
	"fmt"
)

var (
	// ErrConnectionClosed represents an error condition on a closed connection.
	ErrConnectionClosed = errors.New("Connection Closed")

	// ErrAuthorization represents an error condition on failed authentication.
	ErrAuthentication = errors.New("Authentication Error")

	// ErrAuthTimeout represents an error condition on failed authorization due to timeout.
	ErrAuthTimeout = errors.New("Authentication Timeout")

	// ErrAuthExpired represents an expired authorization due to timeout.
	ErrAuthExpired = errors.New("Authentication Expired")

	// ErrMaxPayload represents an error condition when the payload is too big.
	ErrMaxPayload = errors.New("Maximum Payload Exceeded")

	// ErrMaxControlLine represents an error condition when the control line is too big.
	ErrMaxControlLine = errors.New("Maximum Control Line Exceeded")

	// ErrReservedPublishSubject represents an error condition when sending to a reserved subject, e.g. _SYS.>
	ErrReservedPublishSubject = errors.New("Reserved Internal Subject")

	// ErrBadClientProtocol signals a client requested an invalud client protocol.
	ErrBadClientProtocol = errors.New("Invalid Client Protocol")

	// ErrTooManyConnections signals a client that the maximum number of connections supported by the
	// server has been reached.
	ErrTooManyConnections = errors.New("Maximum Connections Exceeded")

	// ErrTooManyAccountConnections signals that an acount has reached its maximum number of active
	// connections.
	ErrTooManyAccountConnections = errors.New("Maximum Account Active Connections Exceeded")

	// ErrTooManySubs signals a client that the maximum number of subscriptions per connection
	// has been reached.
	ErrTooManySubs = errors.New("Maximum Subscriptions Exceeded")

	// ErrClientConnectedToRoutePort represents an error condition when a client
	// attempted to connect to the route listen port.
	ErrClientConnectedToRoutePort = errors.New("Attempted To Connect To Route Port")

	// ErrAccountExists is returned when an account is attempted to be registered
	// but already exists.
	ErrAccountExists = errors.New("Account Exists")

	// ErrBadAccount represents a malformed or incorrect account.
	ErrBadAccount = errors.New("Bad Account")

	// ErrReservedAccount represents a reserved account that can not be created.
	ErrReservedAccount = errors.New("Reserved Account")

	// ErrMissingAccount is returned when an account does not exist.
	ErrMissingAccount = errors.New("Account Missing")

	// ErrAccountValidation is returned when an account has failed validation.
	ErrAccountValidation = errors.New("Account Validation Failed")

	// ErrNoAccountResolver is returned when we attempt an update but do not have an account resolver.
	ErrNoAccountResolver = errors.New("Account Resolver Missing")

	// ErrStreamImportAuthorization is returned when a stream import is not authorized.
	ErrStreamImportAuthorization = errors.New("Stream Import Not Authorized")

	// ErrServiceImportAuthorization is returned when a service import is not authorized.
	ErrServiceImportAuthorization = errors.New("Service Import Not Authorized")

	// ErrClientOrRouteConnectedToGatewayPort represents an error condition when
	// a client or route attempted to connect to the Gateway port.
	ErrClientOrRouteConnectedToGatewayPort = errors.New("Attempted To Connect To Gateway Port")

	// ErrWrongGateway represents an error condition when a server receives a connect
	// request from a remote Gateway with a destination name that does not match the server's
	// Gateway's name.
	ErrWrongGateway = errors.New("Wrong Gateway")
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
