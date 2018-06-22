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
	"crypto/tls"
	"fmt"
	"strings"

	"golang.org/x/crypto/bcrypt"
)

// Authentication is an interface for implementing authentication
type Authentication interface {
	// Check if a client is authorized to connect
	Check(c ClientAuthentication) bool
}

// ClientAuthentication is an interface for client authentication
type ClientAuthentication interface {
	// Get options associated with a client
	GetOpts() *clientOpts
	// If TLS is enabled, TLS ConnectionState, nil otherwise
	GetTLSConnectionState() *tls.ConnectionState
	// Optionally map a user after auth.
	RegisterUser(*User)
}

// User is for multiple accounts/users.
type User struct {
	Username    string       `json:"user"`
	Password    string       `json:"password"`
	Permissions *Permissions `json:"permissions"`
}

// clone performs a deep copy of the User struct, returning a new clone with
// all values copied.
func (u *User) clone() *User {
	if u == nil {
		return nil
	}
	clone := &User{}
	*clone = *u
	clone.Permissions = u.Permissions.clone()
	return clone
}

// Permissions are the allowed subjects on a per
// publish or subscribe basis.
type Permissions struct {
	Publish   []string `json:"publish"`
	Subscribe []string `json:"subscribe"`
}

// RoutePermissions are similar to user permissions
// but describe what a server can import/export from and to
// another server.
type RoutePermissions struct {
	Import []string `json:"import"`
	Export []string `json:"export"`
}

// clone performs a deep copy of the Permissions struct, returning a new clone
// with all values copied.
func (p *Permissions) clone() *Permissions {
	if p == nil {
		return nil
	}
	clone := &Permissions{}
	if p.Publish != nil {
		clone.Publish = make([]string, len(p.Publish))
		copy(clone.Publish, p.Publish)
	}
	if p.Subscribe != nil {
		clone.Subscribe = make([]string, len(p.Subscribe))
		copy(clone.Subscribe, p.Subscribe)
	}
	return clone
}

// configureAuthorization will do any setup needed for authorization.
// Lock is assumed held.
func (s *Server) configureAuthorization() {
	if s.opts == nil {
		return
	}

	// Snapshot server options.
	opts := s.getOpts()

	// Check for multiple users first
	// This just checks and sets up the user map if we have multiple users.
	if opts.CustomClientAuthentication != nil {
		s.info.AuthRequired = true
	} else if opts.Users != nil {
		s.users = make(map[string]*User)
		for _, u := range opts.Users {
			s.users[u.Username] = u
		}
		s.info.AuthRequired = true
	} else if opts.Username != "" || opts.Authorization != "" {
		s.info.AuthRequired = true
	} else {
		s.users = nil
		s.info.AuthRequired = false
	}
}

// checkAuthorization will check authorization based on client type and
// return boolean indicating if client is authorized.
func (s *Server) checkAuthorization(c *client) bool {
	switch c.typ {
	case CLIENT:
		return s.isClientAuthorized(c)
	case ROUTER:
		return s.isRouterAuthorized(c)
	default:
		return false
	}
}

// hasUsers leyt's us know if we have a users array.
func (s *Server) hasUsers() bool {
	s.mu.Lock()
	hu := s.users != nil
	s.mu.Unlock()
	return hu
}

// isClientAuthorized will check the client against the proper authorization method and data.
// This could be token or username/password based.
func (s *Server) isClientAuthorized(c *client) bool {
	// Snapshot server options.
	opts := s.getOpts()

	// Check custom auth first, then multiple users, then token, then single user/pass.
	if opts.CustomClientAuthentication != nil {
		return opts.CustomClientAuthentication.Check(c)
	} else if s.hasUsers() {
		s.mu.Lock()
		user, ok := s.users[c.opts.Username]
		s.mu.Unlock()

		if !ok {
			return false
		}
		ok = comparePasswords(user.Password, c.opts.Password)
		// If we are authorized, register the user which will properly setup any permissions
		// for pub/sub authorizations.
		if ok {
			c.RegisterUser(user)
		}
		return ok

	} else if opts.Authorization != "" {
		return comparePasswords(opts.Authorization, c.opts.Authorization)

	} else if opts.Username != "" {
		if opts.Username != c.opts.Username {
			return false
		}
		return comparePasswords(opts.Password, c.opts.Password)
	}

	return true
}

// checkRouterAuth checks optional router authorization which can be nil or username/password.
func (s *Server) isRouterAuthorized(c *client) bool {
	// Snapshot server options.
	opts := s.getOpts()

	if s.opts.CustomRouterAuthentication != nil {
		return s.opts.CustomRouterAuthentication.Check(c)
	}

	if opts.Cluster.Username == "" {
		return true
	}

	if opts.Cluster.Username != c.opts.Username {
		return false
	}
	if !comparePasswords(opts.Cluster.Password, c.opts.Password) {
		return false
	}
	c.setRoutePermissions(opts.Cluster.Permissions)
	return true
}

// removeUnauthorizedSubs removes any subscriptions the client has that are no
// longer authorized, e.g. due to a config reload.
func (s *Server) removeUnauthorizedSubs(c *client) {
	c.mu.Lock()
	if c.perms == nil {
		c.mu.Unlock()
		return
	}

	subs := make(map[string]*subscription, len(c.subs))
	for sid, sub := range c.subs {
		subs[sid] = sub
	}
	c.mu.Unlock()

	for sid, sub := range subs {
		if !c.canSubscribe(sub.subject) {
			_ = s.sl.Remove(sub)
			c.mu.Lock()
			delete(c.subs, sid)
			c.mu.Unlock()
			c.sendErr(fmt.Sprintf("Permissions Violation for Subscription to %q (sid %s)",
				sub.subject, sub.sid))
			s.Noticef("Removed sub %q for user %q - not authorized",
				string(sub.subject), c.opts.Username)
		}
	}
}

// Support for bcrypt stored passwords and tokens.
const bcryptPrefix = "$2a$"

// isBcrypt checks whether the given password or token is bcrypted.
func isBcrypt(password string) bool {
	return strings.HasPrefix(password, bcryptPrefix)
}

func comparePasswords(serverPassword, clientPassword string) bool {
	// Check to see if the server password is a bcrypt hash
	if isBcrypt(serverPassword) {
		if err := bcrypt.CompareHashAndPassword([]byte(serverPassword), []byte(clientPassword)); err != nil {
			return false
		}
	} else if serverPassword != clientPassword {
		return false
	}
	return true
}
