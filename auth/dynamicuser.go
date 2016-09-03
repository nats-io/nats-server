// Copyright 2016 Apcera Inc. All rights reserved.

package auth

import (
	"fmt"
	"strings"
	"time"

	"github.com/nats-io/gnatsd/server"
	"github.com/nats-io/nats"
	"golang.org/x/crypto/bcrypt"
)

const (
	pwc   = '*'
	fwc   = '>'
	tsep  = "."
	btsep = '.'
)

type Permissions struct {
	Publish   []string `json:"publish"`
	Subscribe []string `json:"subscribe"`
}

// Plain authentication is a basic username and password
type DynamicUser struct {
	users            map[string]*server.User
	authenticatorhub *nats.Conn
	Token            string
}

// Create a new multi-user
func NewDynamicUser(users []*server.User, authenticatorhub []string) *DynamicUser {
	servers := strings.Join(authenticatorhub, ",")
	nc, _ := nats.Connect(servers,
		nats.DisconnectHandler(func(nc *nats.Conn) {
			fmt.Printf("Got disconnected!\n")
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			fmt.Printf("Got reconnected to %v!\n", nc.ConnectedUrl())
		}),
		nats.ClosedHandler(func(nc *nats.Conn) {
			fmt.Printf("Connection closed. Reason: %q\n", nc.LastError())
		}),
	)

	m := &DynamicUser{users: make(map[string]*server.User), authenticatorhub: nc, Token: ""}
	for _, u := range users {
		if u.Username != "" {
			m.users[u.Username] = u
		} else if u.Token != "" {
			m.users[u.Token] = u
		}
	}

	m.authenticatorhub = nc

	return m
}

// Check authenticates the client using a username and password against a list of multiple users.
func (m *DynamicUser) Check(c server.ClientAuth) bool {
	opts := c.GetOpts()

	ok := false
	user, ok := m.users[opts.Username]
	pass, optsPass := "", ""
	if opts.Username != "" {
		for k, _ := range m.users {
			if matchLiteral(k, opts.Username) {
				if user, ok = m.users[k]; ok {
					pass = user.Password
					optsPass = opts.Password
					break
				}
			}
		}
	} else if opts.Token != "" {
		for k, _ := range m.users {
			if matchLiteral(k, opts.Token) {
				if user, ok = m.users[k]; ok {
					pass = user.Password
					optsPass = opts.Token
					break
				}
			}
		}
	} else {
		return false
	}
	if !ok {
		return false
	}

	if opts.Authenticator != "" {
		msg, err := m.authenticatorhub.Request(opts.Authenticator, []byte(opts.Authenticator), 10*time.Millisecond)
		fmt.Println(err)
		fmt.Println(msg)
		// user.Permissions = &Permissions{} //parseUserPermissions(msg) //
	} else
	// Check to see if the password is a bcrypt hash
	if isBcrypt(pass) {
		if err := bcrypt.CompareHashAndPassword([]byte(pass), []byte(optsPass)); err != nil {
			return false
		}
	} else if pass != optsPass {
		return false
	}

	c.RegisterUser(user)

	return true
}

// Todo
func parseUserPermissions(msg *nats.Msg) *Permissions {
	p := &Permissions{}
	return p
}

// from github.com/nats-io/gnatsd/server/sublist.go
// matchLiteral is used to test literal subjects, those that do not have any
// wildcards, with a target subject. This is used in the cache layer.
func matchLiteral(literal, subject string) bool {
	li := 0
	ll := len(literal)
	for i := 0; i < len(subject); i++ {
		if li >= ll {
			return false
		}
		b := subject[i]
		switch b {
		case pwc:
			// Skip token in literal
			ll := len(literal)
			for {
				if li >= ll || literal[li] == btsep {
					li--
					break
				}
				li++
			}
		case fwc:
			return true
		default:
			if b != literal[li] {
				return false
			}
		}
		li++
	}
	// Make sure we have processed all of the literal's chars..
	if li < ll {
		return false
	}
	return true
}
