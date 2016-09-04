// Copyright 2016 Apcera Inc. All rights reserved.

package auth

import (
	"encoding/json"
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
	users                map[string]*server.User
	authenticatorhub     []string
	authenticatorHubConn *nats.Conn
}

// Create a new multi-user
func NewDynamicUser(users []*server.User, authenticatorhub []string) *DynamicUser {
	nc, err := authenticatorHubConnect(authenticatorhub)
	if err != nil {
		fmt.Errorf("AuthenticatorHub Connection Failed: ", err)
	}

	m := &DynamicUser{users: make(map[string]*server.User), authenticatorHubConn: nc}
	for _, u := range users {
		if u.Username != "" {
			m.users[u.Username] = u
		} else if u.Token != "" {
			m.users[u.Token] = u
		}
	}

	m.authenticatorhub = authenticatorhub
	m.authenticatorHubConn = nc
	return m
}

// Check authenticates the server using dynamic auth.
func (m *DynamicUser) Check(c server.ClientAuth) bool {
	opts := c.GetOpts()

	ok := false
	user, ok := m.users[opts.Username]
	pass, optsPass := "", ""
	if opts.Username != "" {
		for k, _ := range m.users {
			if matchLiteral(opts.Username, k) {
				if user, ok = m.users[k]; ok {
					pass = user.Password
					optsPass = opts.Password
					break
				}
			}
		}
	} else if opts.Authorization != "" {
		for k, _ := range m.users {
			if matchLiteral(opts.Authorization, k) {
				if user, ok = m.users[k]; ok {
					pass = user.Password
					optsPass = opts.Authorization
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

	// Check to see if the password is a bcrypt hash
	if isBcrypt(pass) {
		if err := bcrypt.CompareHashAndPassword([]byte(pass), []byte(optsPass)); err != nil {
			return false
		}
	} else if pass != optsPass {
		return false
	}

	if user.Authenticator != "" {
		if m.authenticatorHubConn == nil {
			fmt.Println("IamIN    d")
			nc, err := authenticatorHubConnect(m.authenticatorhub)
			if err != nil {
				server.Errorf("AuthenticatorHub Connection Failed: ", err)
				return false
			}
			m.authenticatorHubConn = nc
		}
		server.Noticef("Dynamic Auth, Authenticator: %v", user.Authenticator)
		msg, err := m.authenticatorHubConn.Request(user.Authenticator, []byte(user.Authenticator), 10*time.Millisecond)
		if err != nil {
			server.Errorf("Authenticator Request Error: %v", err)
			return false
		}
		err = json.Unmarshal(msg.Data, &user.Permissions)
		if err != nil {
			server.Errorf("Authenticator Reply Msg Json Error: %v", err)
			return false
		}
	}

	c.RegisterUser(user)

	return true
}

func authenticatorHubConnect(authenticatorhub []string) (*nats.Conn, error) {
	servers := strings.Join(authenticatorhub, ",")
	nc, err := nats.Connect(servers,
		nats.DisconnectHandler(func(nc *nats.Conn) {
			fmt.Errorf("Got disconnected!\n")
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			fmt.Errorf("Got reconnected to %v!\n", nc.ConnectedUrl())
		}),
		nats.ClosedHandler(func(nc *nats.Conn) {
			fmt.Errorf("Connection closed. Reason: %q\n", nc.LastError())
		}),
	)
	return nc, err
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
