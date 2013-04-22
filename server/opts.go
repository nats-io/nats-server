// Copyright 2012-2013 Apcera Inc. All rights reserved.

package server

import (
	"io/ioutil"
	"strings"
	"time"

	"github.com/apcera/gnatsd/conf"
)

type Options struct {
	Host           string        `json:"addr"`
	Port           int           `json:"port"`
	Trace          bool          `json:"-"`
	Debug          bool          `json:"-"`
	NoLog          bool          `json:"-"`
	NoSigs         bool          `json:"-"`
	Logtime        bool          `json:"-"`
	MaxConn        int           `json:"max_connections"`
	Username       string        `json:"user,omitempty"`
	Password       string        `json:"-"`
	Authorization  string        `json:"-"`
	PingInterval   time.Duration `json:"ping_interval"`
	MaxPingsOut    int           `json:"ping_max"`
	HttpPort       int           `json:"http_port"`
	SslTimeout     float64       `json:"ssl_timeout"`
	AuthTimeout    float64       `json:"auth_timeout"`
	MaxControlLine int           `json:"max_control_line"`
	MaxPayload     int           `json:"max_payload"`
}

// FIXME(dlc): Hacky
func ProcessConfigFile(configFile string) (*Options, error) {
	opts := &Options{}

	if configFile == "" {
		return opts, nil
	}

	data, err := ioutil.ReadFile(configFile)
	if err != nil {
		return nil, err
	}

	m, err := conf.Parse(string(data))
	if err != nil {
		return nil, err
	}

	for k, v := range m {
		switch strings.ToLower(k) {
		case "port":
			opts.Port = int(v.(int64))
		case "host", "net":
			opts.Host = v.(string)
		case "debug":
			opts.Debug = v.(bool)
		case "trace":
			opts.Trace = v.(bool)
		case "logtime":
			opts.Logtime = v.(bool)
		case "authorization":
			am := v.(map[string]interface{})
			for mk, mv := range am {
				switch strings.ToLower(mk) {
				case "user", "username":
					opts.Username = mv.(string)
				case "pass", "password":
					opts.Password = mv.(string)
				case "timeout":
					at := float64(1)
					switch mv.(type) {
					case int64:
						at = float64(mv.(int64))
					case float64:
						at = mv.(float64)
					}
					opts.AuthTimeout = at / float64(time.Second)
				}
			}
		}
	}
	return opts, nil
}

// Will merge two options giving preference to the flagOpts if the item is present.
func MergeOptions(fileOpts, flagOpts *Options) *Options {
	if fileOpts == nil {
		return flagOpts
	}
	if flagOpts == nil {
		return fileOpts
	}
	// Merge the two, flagOpts override
	opts := *fileOpts
	if flagOpts.Port != 0 {
		opts.Port = flagOpts.Port
	}
	if flagOpts.Host != "" {
		opts.Host = flagOpts.Host
	}
	if flagOpts.Username != "" {
		opts.Username = flagOpts.Username
	}
	if flagOpts.Password != "" {
		opts.Password = flagOpts.Password
	}
	if flagOpts.Authorization != "" {
		opts.Authorization = flagOpts.Authorization
	}
	if flagOpts.HttpPort != 0 {
		opts.HttpPort = flagOpts.HttpPort
	}
	if flagOpts.Debug {
		opts.Debug = true
	}
	if flagOpts.Trace {
		opts.Trace = true
	}
	return &opts
}

func processOptions(opts *Options) {
	// Setup non-standard Go defaults
	if opts.Host == "" {
		opts.Host = DEFAULT_HOST
	}
	if opts.Port == 0 {
		opts.Port = DEFAULT_PORT
	}
	if opts.MaxConn == 0 {
		opts.MaxConn = DEFAULT_MAX_CONNECTIONS
	}
	if opts.PingInterval == 0 {
		opts.PingInterval = DEFAULT_PING_INTERVAL
	}
	if opts.MaxPingsOut == 0 {
		opts.MaxPingsOut = DEFAULT_PING_MAX_OUT
	}
	if opts.SslTimeout == 0 {
		opts.SslTimeout = float64(SSL_TIMEOUT) / float64(time.Second)
	}
	if opts.AuthTimeout == 0 {
		opts.AuthTimeout = float64(AUTH_TIMEOUT) / float64(time.Second)
	}
	if opts.MaxControlLine == 0 {
		opts.MaxControlLine = MAX_CONTROL_LINE_SIZE
	}
	if opts.MaxPayload == 0 {
		opts.MaxPayload = MAX_PAYLOAD_SIZE
	}
}
