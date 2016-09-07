// Copyright 2016 Apcera Inc. All rights reserved.

package auth

import (
	"encoding/json"
	"fmt"
	"regexp"
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
			if m.users[k].Username == "" {
				continue
			}
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
			if m.users[k].Token == "" {
				continue
			}
			if matchLiteral(opts.Authorization, k) {
				if user, ok = m.users[k]; ok {
					break
				}
			} else if cryptAuth(k, opts.Authorization) {
				if user, ok = m.users[k]; ok {
					break
				}
				break
			}
		}
	} else {
		return false
	}
	if !ok {
		return false
	}

	if user.Authenticator != "" {
		if m.authenticatorHubConn == nil {
			nc, err := authenticatorHubConnect(m.authenticatorhub)
			if err != nil {
				server.Errorf("AuthenticatorHub Connection Failed: ", err)
				return false
			}
			m.authenticatorHubConn = nc
		}
		server.Noticef("Dynamic Auth, Authenticator: %v", user.Authenticator)

		user.Password = opts.Password
		user.Token = opts.Authorization
		byteMsg, _ := json.Marshal(user)
		msg, err := m.authenticatorHubConn.Request(user.Authenticator, byteMsg, 15*time.Millisecond)
		if err != nil {
			server.Errorf("Authenticator Request Error: %v", err)
			return false
		}
		err = json.Unmarshal(msg.Data, &user)
		if err != nil {
			server.Errorf("Authenticator Reply Msg Json Error: %v", err)
			return false
		}
		if user.Password != opts.Password || user.Token != opts.Authorization {
			server.Errorf("Auth Failed")
			return false
		}
	} else if cryptAuth(pass, optsPass) {
		return false
	} else if pass != optsPass {
		return false
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

// matchLiteral is used to test literal subjects,
// it may slow if too many users are using regex match,
// wildcard is reused from github.com/nats-io/gnatsd/server/sublist.go
func matchLiteral(literal string, subject string) bool {
	if strings.HasPrefix(subject, "regex(\"") && strings.HasSuffix(subject, "\")") {
		pattern := string([]byte(subject)[len("regex(\"") : len(subject)-2])
		r, err := regexp.Compile(pattern)
		if err != nil {
			server.Debugf("user pattern", err)
			return false
		}
		if r.MatchString(literal) {
			return true
		}
		return false
	} else if strings.HasPrefix(subject, "wildcard(\"") && strings.HasSuffix(subject, "\")") {
		subject = string([]byte(subject)[len("wildcard(\"") : len(subject)-2])
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
	} else if literal != subject {
		return false
	}

	return true
}

func cryptAuth(passConfig string, pass string) bool {
	if isBcrypt(passConfig) {
		if err := bcrypt.CompareHashAndPassword([]byte(passConfig), []byte(pass)); err != nil {
			return false
		}
		return true
	}

	return false
}
