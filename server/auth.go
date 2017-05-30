// Copyright 2012-2017 Apcera Inc. All rights reserved.

package server

import (
	"strings"

	"golang.org/x/crypto/bcrypt"
)

// User is for multiple accounts/users.
type User struct {
	Username    string       `json:"user"`
	Password    string       `json:"password"`
	Permissions *Permissions `json:"permissions"`
}

// Permissions are the allowed subjects on a per
// publish or subscribe basis.
type Permissions struct {
	Publish   []string `json:"publish"`
	Subscribe []string `json:"subscribe"`
}

// configureAuthorization will do any setup needed for authorization.
// Lock is assumed held.
func (s *Server) configureAuthorization() {
	if s.opts == nil {
		return
	}
	// Check for multiple users first
	// This just checks and sets up the user map if we have multiple users.
	if s.getOpts().Users != nil {
		s.users = make(map[string]*User)
		for _, u := range s.getOpts().Users {
			s.users[u.Username] = u
		}
		s.info.AuthRequired = true
	} else if s.getOpts().Username != "" || s.getOpts().Authorization != "" {
		s.info.AuthRequired = true
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

// isClientAuthorized will check the client against the proper authorization method and data.
// This could be token or username/password based.
func (s *Server) isClientAuthorized(c *client) bool {
	// Check multiple users first, then token, then single user/pass.
	if s.users != nil {
		user, ok := s.users[c.opts.Username]
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

	} else if s.getOpts().Authorization != "" {
		return comparePasswords(s.getOpts().Authorization, c.opts.Authorization)

	} else if s.getOpts().Username != "" {
		if s.getOpts().Username != c.opts.Username {
			return false
		}
		return comparePasswords(s.getOpts().Password, c.opts.Password)
	}

	return true
}

// checkRouterAuth checks optional router authorization which can be nil or username/password.
func (s *Server) isRouterAuthorized(c *client) bool {
	if s.getOpts().Cluster.Username != c.opts.Username {
		return false
	}
	return comparePasswords(s.getOpts().Cluster.Password, c.opts.Password)
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
