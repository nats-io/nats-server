// Copyright 2017 Apcera Inc. All rights reserved.

package server

import (
	"crypto/tls"
	"errors"
	"fmt"
	"reflect"
	"strings"
)

// FlagSnapshot captures the server options as specified by CLI flags at
// startup. This should not be modified once the server has started.
var FlagSnapshot *Options

// option is a hot-swappable configuration setting.
type option interface {
	// Apply the server option.
	Apply(server *Server)

	// IsLoggingChange indicates if this option requires reloading the logger.
	IsLoggingChange() bool

	// IsAuthChange indicates if this option requires reloading authorization.
	IsAuthChange() bool
}

// loggingOption is a base struct that provides default option behaviors for
// logging-related options.
type loggingOption struct{}

func (l loggingOption) IsLoggingChange() bool {
	return true
}

func (l loggingOption) IsAuthChange() bool {
	return false
}

// traceOption implements the option interface for the `trace` setting.
type traceOption struct {
	loggingOption
	newValue bool
}

// Apply is a no-op because authorization will be reloaded after options are
// applied
func (t *traceOption) Apply(server *Server) {
	server.Noticef("Reloaded: trace = %v", t.newValue)
}

// debugOption implements the option interface for the `debug` setting.
type debugOption struct {
	loggingOption
	newValue bool
}

// Apply is a no-op because authorization will be reloaded after options are
// applied
func (d *debugOption) Apply(server *Server) {
	server.Noticef("Reloaded: debug = %v", d.newValue)
}

// noopOption is a base struct that provides default no-op behaviors.
type noopOption struct{}

func (n noopOption) IsLoggingChange() bool {
	return false
}

func (n noopOption) IsAuthChange() bool {
	return false
}

// tlsOption implements the option interface for the `tls` setting.
type tlsOption struct {
	noopOption
	newValue *tls.Config
}

// Apply the tls change.
func (t *tlsOption) Apply(server *Server) {
	server.mu.Lock()
	tlsRequired := t.newValue != nil
	server.info.TLSRequired = tlsRequired
	message := "disabled"
	if tlsRequired {
		server.info.TLSVerify = (t.newValue.ClientAuth == tls.RequireAndVerifyClientCert)
		message = "enabled"
	}
	server.generateServerInfoJSON()
	server.mu.Unlock()
	server.Noticef("Reloaded: tls = %s", message)
}

// tlsTimeoutOption implements the option interface for the tls `timeout`
// setting.
type tlsTimeoutOption struct {
	noopOption
	newValue float64
}

// Apply is a no-op because the timeout will be reloaded after options are
// applied.
func (t *tlsTimeoutOption) Apply(server *Server) {
	server.Noticef("Reloaded: tls timeout = %v", t.newValue)
}

// authOption is a base struct that provides default option behaviors.
type authOption struct{}

func (o authOption) IsLoggingChange() bool {
	return false
}

func (o authOption) IsAuthChange() bool {
	return true
}

// usernameOption implements the option interface for the `username` setting.
type usernameOption struct {
	authOption
}

// Apply is a no-op because authorization will be reloaded after options are
// applied.
func (u *usernameOption) Apply(server *Server) {
	server.Noticef("Reloaded: authorization username")
}

// passwordOption implements the option interface for the `password` setting.
type passwordOption struct {
	authOption
}

// Apply is a no-op because authorization will be reloaded after options are
// applied.
func (p *passwordOption) Apply(server *Server) {
	server.Noticef("Reloaded: authorization password")
}

// authorizationOption implements the option interface for the `token`
// authorization setting.
type authorizationOption struct {
	authOption
}

// Apply is a no-op because authorization will be reloaded after options are
// applied.
func (a *authorizationOption) Apply(server *Server) {
	server.Noticef("Reloaded: authorization token")
}

// authTimeoutOption implements the option interface for the authorization
// `timeout` setting.
type authTimeoutOption struct {
	noopOption
	newValue float64
}

// Apply is a no-op because the timeout will be reloaded after options are
// applied.
func (a *authTimeoutOption) Apply(server *Server) {
	server.Noticef("Reloaded: authorization timeout = %v", a.newValue)
}

// usersOption implements the option interface for the authorization `users`
// setting.
type usersOption struct {
	authOption
	newValue []*User
}

func (u *usersOption) Apply(server *Server) {
	server.Noticef("Reloaded: authorization users")
}

// Reload reads the current configuration file and applies any supported
// changes. This returns an error if the server was not started with a config
// file or an option which doesn't support hot-swapping was changed.
func (s *Server) Reload() error {
	s.mu.Lock()
	if s.configFile == "" {
		s.mu.Unlock()
		return errors.New("Can only reload config when a file is provided using -c or --config")
	}
	newOpts, err := ProcessConfigFile(s.configFile)
	if err != nil {
		s.mu.Unlock()
		// TODO: Dump previous good config to a .bak file?
		return fmt.Errorf("Config reload failed: %s", err)
	}
	s.mu.Unlock()

	// Apply flags over config file settings.
	newOpts = MergeOptions(newOpts, FlagSnapshot)
	processOptions(newOpts)
	return s.reloadOptions(newOpts)
}

// reloadOptions reloads the server config with the provided options. If an
// option that doesn't support hot-swapping is changed, this returns an error.
func (s *Server) reloadOptions(newOpts *Options) error {
	changed, err := s.diffOptions(newOpts)
	if err != nil {
		return err
	}
	s.setOpts(newOpts)
	s.applyOptions(changed)
	return nil
}

// diffOptions returns a slice containing options which have been changed. If
// an option that doesn't support hot-swapping is changed, this returns an
// error.
func (s *Server) diffOptions(newOpts *Options) ([]option, error) {
	var (
		oldConfig = reflect.ValueOf(s.getOpts()).Elem()
		newConfig = reflect.ValueOf(newOpts).Elem()
		diffOpts  = []option{}
	)

	for i := 0; i < oldConfig.NumField(); i++ {
		var (
			field    = oldConfig.Type().Field(i)
			oldValue = oldConfig.Field(i).Interface()
			newValue = newConfig.Field(i).Interface()
			changed  = !reflect.DeepEqual(oldValue, newValue)
		)
		if !changed {
			continue
		}
		switch strings.ToLower(field.Name) {
		case "trace":
			diffOpts = append(diffOpts, &traceOption{newValue: newValue.(bool)})
		case "debug":
			diffOpts = append(diffOpts, &debugOption{newValue: newValue.(bool)})
		case "tlsconfig":
			diffOpts = append(diffOpts, &tlsOption{newValue: newValue.(*tls.Config)})
		case "tlstimeout":
			diffOpts = append(diffOpts, &tlsTimeoutOption{newValue: newValue.(float64)})
		case "username":
			diffOpts = append(diffOpts, &usernameOption{})
		case "password":
			diffOpts = append(diffOpts, &passwordOption{})
		case "authorization":
			diffOpts = append(diffOpts, &authorizationOption{})
		case "authtimeout":
			diffOpts = append(diffOpts, &authTimeoutOption{newValue: newValue.(float64)})
		case "users":
			diffOpts = append(diffOpts, &usersOption{newValue: newValue.([]*User)})
		default:
			// Bail out if attempting to reload any unsupported options.
			return nil, fmt.Errorf("Config reload not supported for %s", field.Name)
		}
	}

	return diffOpts, nil
}

func (s *Server) applyOptions(opts []option) {
	var (
		reloadLogging = false
		reloadAuth    = false
	)
	for _, opt := range opts {
		opt.Apply(s)
		if opt.IsLoggingChange() {
			reloadLogging = true
		}
		if opt.IsAuthChange() {
			reloadAuth = true
		}
	}

	if reloadLogging {
		s.ConfigureLogger()
	}
	if reloadAuth {
		s.reloadAuthorization()
	}

	s.Noticef("Reloaded server configuration")
}

// reloadAuthorization reconfigures the server authorization settings,
// disconnects any clients who are no longer authorized, and removes any
// unauthorized subscriptions.
func (s *Server) reloadAuthorization() {
	s.mu.Lock()
	s.configureAuthorization()
	s.generateServerInfoJSON()
	clients := make(map[uint64]*client, len(s.clients))
	for i, client := range s.clients {
		clients[i] = client
	}
	s.mu.Unlock()

	for _, client := range clients {
		// Disconnect any unauthorized clients.
		if !s.isClientAuthorized(client) {
			client.authViolation()
			continue
		}

		// Remove any unauthorized subscriptions.
		s.removeUnauthorizedSubs(client)
	}
}
