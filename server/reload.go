// Copyright 2017-2018 The NATS Authors
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
	"crypto/tls"
	"errors"
	"fmt"
	"net/url"
	"reflect"
	"strings"
	"sync/atomic"
	"time"
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

// Apply is a no-op because logging will be reloaded after options are applied.
func (t *traceOption) Apply(server *Server) {
	server.Noticef("Reloaded: trace = %v", t.newValue)
}

// debugOption implements the option interface for the `debug` setting.
type debugOption struct {
	loggingOption
	newValue bool
}

// Apply is a no-op because logging will be reloaded after options are applied.
func (d *debugOption) Apply(server *Server) {
	server.Noticef("Reloaded: debug = %v", d.newValue)
}

// logtimeOption implements the option interface for the `logtime` setting.
type logtimeOption struct {
	loggingOption
	newValue bool
}

// Apply is a no-op because logging will be reloaded after options are applied.
func (l *logtimeOption) Apply(server *Server) {
	server.Noticef("Reloaded: logtime = %v", l.newValue)
}

// logfileOption implements the option interface for the `log_file` setting.
type logfileOption struct {
	loggingOption
	newValue string
}

// Apply is a no-op because logging will be reloaded after options are applied.
func (l *logfileOption) Apply(server *Server) {
	server.Noticef("Reloaded: log_file = %v", l.newValue)
}

// syslogOption implements the option interface for the `syslog` setting.
type syslogOption struct {
	loggingOption
	newValue bool
}

// Apply is a no-op because logging will be reloaded after options are applied.
func (s *syslogOption) Apply(server *Server) {
	server.Noticef("Reloaded: syslog = %v", s.newValue)
}

// remoteSyslogOption implements the option interface for the `remote_syslog`
// setting.
type remoteSyslogOption struct {
	loggingOption
	newValue string
}

// Apply is a no-op because logging will be reloaded after options are applied.
func (r *remoteSyslogOption) Apply(server *Server) {
	server.Noticef("Reloaded: remote_syslog = %v", r.newValue)
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
	noopOption // Not authOption because this is a no-op; will be reloaded with options.
	newValue   float64
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

// clusterOption implements the option interface for the `cluster` setting.
type clusterOption struct {
	authOption
	newValue ClusterOpts
}

// Apply the cluster change.
func (c *clusterOption) Apply(server *Server) {
	// TODO: support enabling/disabling clustering.
	server.mu.Lock()
	tlsRequired := c.newValue.TLSConfig != nil
	server.routeInfo.TLSRequired = tlsRequired
	server.routeInfo.TLSVerify = tlsRequired
	server.routeInfo.AuthRequired = c.newValue.Username != ""
	if c.newValue.NoAdvertise {
		server.routeInfo.ClientConnectURLs = nil
	} else {
		server.routeInfo.ClientConnectURLs = server.clientConnectURLs
	}
	server.setRouteInfoHostPortAndIP()
	server.mu.Unlock()
	server.Noticef("Reloaded: cluster")
}

// routesOption implements the option interface for the cluster `routes`
// setting.
type routesOption struct {
	noopOption
	add    []*url.URL
	remove []*url.URL
}

// Apply the route changes by adding and removing the necessary routes.
func (r *routesOption) Apply(server *Server) {
	server.mu.Lock()
	routes := make([]*client, len(server.routes))
	i := 0
	for _, client := range server.routes {
		routes[i] = client
		i++
	}
	server.mu.Unlock()

	// Remove routes.
	for _, remove := range r.remove {
		for _, client := range routes {
			if client.route.url == remove {
				// Do not attempt to reconnect when route is removed.
				client.setRouteNoReconnectOnClose()
				client.closeConnection()
				server.Noticef("Removed route %v", remove)
			}
		}
	}

	// Add routes.
	server.solicitRoutes(r.add)

	server.Noticef("Reloaded: cluster routes")
}

// maxConnOption implements the option interface for the `max_connections`
// setting.
type maxConnOption struct {
	noopOption
	newValue int
}

// Apply the max connections change by closing random connections til we are
// below the limit if necessary.
func (m *maxConnOption) Apply(server *Server) {
	server.mu.Lock()
	var (
		clients = make([]*client, len(server.clients))
		i       = 0
	)
	// Map iteration is random, which allows us to close random connections.
	for _, client := range server.clients {
		clients[i] = client
		i++
	}
	server.mu.Unlock()

	if m.newValue > 0 && len(clients) > m.newValue {
		// Close connections til we are within the limit.
		var (
			numClose = len(clients) - m.newValue
			closed   = 0
		)
		for _, client := range clients {
			client.maxConnExceeded()
			closed++
			if closed >= numClose {
				break
			}
		}
		server.Noticef("Closed %d connections to fall within max_connections", closed)
	}
	server.Noticef("Reloaded: max_connections = %v", m.newValue)
}

// pidFileOption implements the option interface for the `pid_file` setting.
type pidFileOption struct {
	noopOption
	newValue string
}

// Apply the setting by logging the pid to the new file.
func (p *pidFileOption) Apply(server *Server) {
	if p.newValue == "" {
		return
	}
	if err := server.logPid(); err != nil {
		server.Errorf("Failed to write pidfile: %v", err)
	}
	server.Noticef("Reloaded: pid_file = %v", p.newValue)
}

// maxControlLineOption implements the option interface for the
// `max_control_line` setting.
type maxControlLineOption struct {
	noopOption
	newValue int
}

// Apply is a no-op because the max control line will be reloaded after options
// are applied
func (m *maxControlLineOption) Apply(server *Server) {
	server.Noticef("Reloaded: max_control_line = %d", m.newValue)
}

// maxPayloadOption implements the option interface for the `max_payload`
// setting.
type maxPayloadOption struct {
	noopOption
	newValue int
}

// Apply the setting by updating the server info and each client.
func (m *maxPayloadOption) Apply(server *Server) {
	server.mu.Lock()
	server.info.MaxPayload = m.newValue
	server.generateServerInfoJSON()
	for _, client := range server.clients {
		atomic.StoreInt64(&client.mpay, int64(m.newValue))
	}
	server.mu.Unlock()
	server.Noticef("Reloaded: max_payload = %d", m.newValue)
}

// pingIntervalOption implements the option interface for the `ping_interval`
// setting.
type pingIntervalOption struct {
	noopOption
	newValue time.Duration
}

// Apply is a no-op because the ping interval will be reloaded after options
// are applied.
func (p *pingIntervalOption) Apply(server *Server) {
	server.Noticef("Reloaded: ping_interval = %s", p.newValue)
}

// maxPingsOutOption implements the option interface for the `ping_max`
// setting.
type maxPingsOutOption struct {
	noopOption
	newValue int
}

// Apply is a no-op because the ping interval will be reloaded after options
// are applied.
func (m *maxPingsOutOption) Apply(server *Server) {
	server.Noticef("Reloaded: ping_max = %d", m.newValue)
}

// writeDeadlineOption implements the option interface for the `write_deadline`
// setting.
type writeDeadlineOption struct {
	noopOption
	newValue time.Duration
}

// Apply is a no-op because the write deadline will be reloaded after options
// are applied.
func (w *writeDeadlineOption) Apply(server *Server) {
	server.Noticef("Reloaded: write_deadline = %s", w.newValue)
}

// clientAdvertiseOption implements the option interface for the `client_advertise` setting.
type clientAdvertiseOption struct {
	noopOption
	newValue string
}

// Apply the setting by updating the server info and regenerate the infoJSON byte array.
func (c *clientAdvertiseOption) Apply(server *Server) {
	server.mu.Lock()
	server.setInfoHostPortAndGenerateJSON()
	server.mu.Unlock()
	server.Noticef("Reload: client_advertise = %s", c.newValue)
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
		return err
	}
	clientOrgPort := s.clientActualPort
	clusterOrgPort := s.clusterActualPort
	s.mu.Unlock()

	// Apply flags over config file settings.
	newOpts = MergeOptions(newOpts, FlagSnapshot)
	processOptions(newOpts)

	// processOptions sets Port to 0 if set to -1 (RANDOM port)
	// If that's the case, set it to the saved value when the accept loop was
	// created.
	if newOpts.Port == 0 {
		newOpts.Port = clientOrgPort
	}
	// We don't do that for cluster, so check against -1.
	if newOpts.Cluster.Port == -1 {
		newOpts.Cluster.Port = clusterOrgPort
	}

	if err := s.reloadOptions(newOpts); err != nil {
		return err
	}
	s.mu.Lock()
	s.configTime = time.Now()
	s.mu.Unlock()
	return nil
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
		case "logtime":
			diffOpts = append(diffOpts, &logtimeOption{newValue: newValue.(bool)})
		case "logfile":
			diffOpts = append(diffOpts, &logfileOption{newValue: newValue.(string)})
		case "syslog":
			diffOpts = append(diffOpts, &syslogOption{newValue: newValue.(bool)})
		case "remotesyslog":
			diffOpts = append(diffOpts, &remoteSyslogOption{newValue: newValue.(string)})
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
		case "cluster":
			newClusterOpts := newValue.(ClusterOpts)
			if err := validateClusterOpts(oldValue.(ClusterOpts), newClusterOpts); err != nil {
				return nil, err
			}
			diffOpts = append(diffOpts, &clusterOption{newValue: newClusterOpts})
		case "routes":
			add, remove := diffRoutes(oldValue.([]*url.URL), newValue.([]*url.URL))
			diffOpts = append(diffOpts, &routesOption{add: add, remove: remove})
		case "maxconn":
			diffOpts = append(diffOpts, &maxConnOption{newValue: newValue.(int)})
		case "pidfile":
			diffOpts = append(diffOpts, &pidFileOption{newValue: newValue.(string)})
		case "maxcontrolline":
			diffOpts = append(diffOpts, &maxControlLineOption{newValue: newValue.(int)})
		case "maxpayload":
			diffOpts = append(diffOpts, &maxPayloadOption{newValue: newValue.(int)})
		case "pinginterval":
			diffOpts = append(diffOpts, &pingIntervalOption{newValue: newValue.(time.Duration)})
		case "maxpingsout":
			diffOpts = append(diffOpts, &maxPingsOutOption{newValue: newValue.(int)})
		case "writedeadline":
			diffOpts = append(diffOpts, &writeDeadlineOption{newValue: newValue.(time.Duration)})
		case "clientadvertise":
			cliAdv := newValue.(string)
			if cliAdv != "" {
				// Validate ClientAdvertise syntax
				if _, _, err := parseHostPort(cliAdv, 0); err != nil {
					return nil, fmt.Errorf("invalid ClientAdvertise value of %s, err=%v", cliAdv, err)
				}
			}
			diffOpts = append(diffOpts, &clientAdvertiseOption{newValue: cliAdv})
		case "nolog":
			// Ignore NoLog option since it's not parsed and only used in
			// testing.
			continue
		case "port":
			// check to see if newValue == 0 and continue if so.
			if newValue == 0 {
				// ignore RANDOM_PORT
				continue
			}
			fallthrough
		default:
			// Bail out if attempting to reload any unsupported options.
			return nil, fmt.Errorf("Config reload not supported for %s: old=%v, new=%v",
				field.Name, oldValue, newValue)
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
	routes := make(map[uint64]*client, len(s.routes))
	for i, route := range s.routes {
		routes[i] = route
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

	for _, client := range routes {
		// Disconnect any unauthorized routes.
		if !s.isRouterAuthorized(client) {
			client.setRouteNoReconnectOnClose()
			client.authViolation()
		}
	}
}

// validateClusterOpts ensures the new ClusterOpts does not change host or
// port, which do not support reload.
func validateClusterOpts(old, new ClusterOpts) error {
	if old.Host != new.Host {
		return fmt.Errorf("Config reload not supported for cluster host: old=%s, new=%s",
			old.Host, new.Host)
	}
	if old.Port != new.Port {
		return fmt.Errorf("Config reload not supported for cluster port: old=%d, new=%d",
			old.Port, new.Port)
	}
	// Validate Cluster.Advertise syntax
	if new.Advertise != "" {
		if _, _, err := parseHostPort(new.Advertise, 0); err != nil {
			return fmt.Errorf("invalid Cluster.Advertise value of %s, err=%v", new.Advertise, err)
		}
	}
	return nil
}

// diffRoutes diffs the old routes and the new routes and returns the ones that
// should be added and removed from the server.
func diffRoutes(old, new []*url.URL) (add, remove []*url.URL) {
	// Find routes to remove.
removeLoop:
	for _, oldRoute := range old {
		for _, newRoute := range new {
			if oldRoute == newRoute {
				continue removeLoop
			}
		}
		remove = append(remove, oldRoute)
	}

	// Find routes to add.
addLoop:
	for _, newRoute := range new {
		for _, oldRoute := range old {
			if oldRoute == newRoute {
				continue addLoop
			}
		}
		add = append(add, newRoute)
	}

	return add, remove
}
