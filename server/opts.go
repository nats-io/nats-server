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
	"crypto/x509"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/gnatsd/conf"
	"github.com/nats-io/gnatsd/util"
)

// ClusterOpts are options for clusters.
type ClusterOpts struct {
	Host           string            `json:"addr,omitempty"`
	Port           int               `json:"cluster_port,omitempty"`
	Username       string            `json:"-"`
	Password       string            `json:"-"`
	AuthTimeout    float64           `json:"auth_timeout,omitempty"`
	Permissions    *RoutePermissions `json:"-"`
	TLSTimeout     float64           `json:"-"`
	TLSConfig      *tls.Config       `json:"-"`
	ListenStr      string            `json:"-"`
	Advertise      string            `json:"-"`
	NoAdvertise    bool              `json:"-"`
	ConnectRetries int               `json:"-"`
}

// Options block for gnatsd server.
type Options struct {
	ConfigFile       string        `json:"-"`
	Host             string        `json:"addr"`
	Port             int           `json:"port"`
	ClientAdvertise  string        `json:"-"`
	Trace            bool          `json:"-"`
	Debug            bool          `json:"-"`
	NoLog            bool          `json:"-"`
	NoSigs           bool          `json:"-"`
	Logtime          bool          `json:"-"`
	MaxConn          int           `json:"max_connections"`
	MaxSubs          int           `json:"max_subscriptions,omitempty"`
	Users            []*User       `json:"-"`
	Username         string        `json:"-"`
	Password         string        `json:"-"`
	Authorization    string        `json:"-"`
	PingInterval     time.Duration `json:"ping_interval"`
	MaxPingsOut      int           `json:"ping_max"`
	HTTPHost         string        `json:"http_host"`
	HTTPPort         int           `json:"http_port"`
	HTTPSPort        int           `json:"https_port"`
	AuthTimeout      float64       `json:"auth_timeout"`
	MaxControlLine   int           `json:"max_control_line"`
	MaxPayload       int           `json:"max_payload"`
	MaxPending       int64         `json:"max_pending"`
	Cluster          ClusterOpts   `json:"cluster,omitempty"`
	ProfPort         int           `json:"-"`
	PidFile          string        `json:"-"`
	PortsFileDir     string        `json:"-"`
	LogFile          string        `json:"-"`
	Syslog           bool          `json:"-"`
	RemoteSyslog     string        `json:"-"`
	Routes           []*url.URL    `json:"-"`
	RoutesStr        string        `json:"-"`
	TLSTimeout       float64       `json:"tls_timeout"`
	TLS              bool          `json:"-"`
	TLSVerify        bool          `json:"-"`
	TLSCert          string        `json:"-"`
	TLSKey           string        `json:"-"`
	TLSCaCert        string        `json:"-"`
	TLSConfig        *tls.Config   `json:"-"`
	WriteDeadline    time.Duration `json:"-"`
	RQSubsSweep      time.Duration `json:"-"`
	MaxClosedClients int           `json:"-"`

	CustomClientAuthentication Authentication `json:"-"`
	CustomRouterAuthentication Authentication `json:"-"`
}

// Clone performs a deep copy of the Options struct, returning a new clone
// with all values copied.
func (o *Options) Clone() *Options {
	if o == nil {
		return nil
	}
	clone := &Options{}
	*clone = *o
	if o.Users != nil {
		clone.Users = make([]*User, len(o.Users))
		for i, user := range o.Users {
			clone.Users[i] = user.clone()
		}
	}
	if o.Routes != nil {
		clone.Routes = make([]*url.URL, len(o.Routes))
		for i, route := range o.Routes {
			routeCopy := &url.URL{}
			*routeCopy = *route
			clone.Routes[i] = routeCopy
		}
	}
	if o.TLSConfig != nil {
		clone.TLSConfig = util.CloneTLSConfig(o.TLSConfig)
	}
	if o.Cluster.TLSConfig != nil {
		clone.Cluster.TLSConfig = util.CloneTLSConfig(o.Cluster.TLSConfig)
	}
	return clone
}

// Configuration file authorization section.
type authorization struct {
	// Singles
	user  string
	pass  string
	token string
	// Multiple Users
	users              []*User
	timeout            float64
	defaultPermissions *Permissions
}

// TLSConfigOpts holds the parsed tls config information,
// used with flag parsing
type TLSConfigOpts struct {
	CertFile         string
	KeyFile          string
	CaFile           string
	Verify           bool
	Timeout          float64
	Ciphers          []uint16
	CurvePreferences []tls.CurveID
}

var tlsUsage = `
TLS configuration is specified in the tls section of a configuration file:

e.g.

    tls {
        cert_file: "./certs/server-cert.pem"
        key_file:  "./certs/server-key.pem"
        ca_file:   "./certs/ca.pem"
        verify:    true

        cipher_suites: [
            "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256",
            "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256"
        ]
        curve_preferences: [
            "CurveP256",
            "CurveP384",
            "CurveP521"
        ]
    }

Available cipher suites include:
`

// ProcessConfigFile processes a configuration file.
// FIXME(dlc): Hacky
func ProcessConfigFile(configFile string) (*Options, error) {
	opts := &Options{}
	if err := opts.ProcessConfigFile(configFile); err != nil {
		return nil, err
	}
	return opts, nil
}

// ProcessConfigFile updates the Options structure with options
// present in the given configuration file.
// This version is convenient if one wants to set some default
// options and then override them with what is in the config file.
// For instance, this version allows you to do something such as:
//
// opts := &Options{Debug: true}
// opts.ProcessConfigFile(myConfigFile)
//
// If the config file contains "debug: false", after this call,
// opts.Debug would really be false. It would be impossible to
// achieve that with the non receiver ProcessConfigFile() version,
// since one would not know after the call if "debug" was not present
// or was present but set to false.
func (o *Options) ProcessConfigFile(configFile string) error {
	o.ConfigFile = configFile
	if configFile == "" {
		return nil
	}

	m, err := conf.ParseFile(configFile)
	if err != nil {
		return err
	}

	for k, v := range m {
		switch strings.ToLower(k) {
		case "listen":
			hp, err := parseListen(v)
			if err != nil {
				return err
			}
			o.Host = hp.host
			o.Port = hp.port
		case "client_advertise":
			o.ClientAdvertise = v.(string)
		case "port":
			o.Port = int(v.(int64))
		case "host", "net":
			o.Host = v.(string)
		case "debug":
			o.Debug = v.(bool)
		case "trace":
			o.Trace = v.(bool)
		case "logtime":
			o.Logtime = v.(bool)
		case "authorization":
			am := v.(map[string]interface{})
			auth, err := parseAuthorization(am)
			if err != nil {
				return err
			}
			o.Username = auth.user
			o.Password = auth.pass
			o.Authorization = auth.token
			if (auth.user != "" || auth.pass != "") && auth.token != "" {
				return fmt.Errorf("Cannot have a user/pass and token")
			}
			o.AuthTimeout = auth.timeout
			// Check for multiple users defined
			if auth.users != nil {
				if auth.user != "" {
					return fmt.Errorf("Can not have a single user/pass and a users array")
				}
				if auth.token != "" {
					return fmt.Errorf("Can not have a token and a users array")
				}
				o.Users = auth.users
			}
		case "http":
			hp, err := parseListen(v)
			if err != nil {
				return err
			}
			o.HTTPHost = hp.host
			o.HTTPPort = hp.port
		case "https":
			hp, err := parseListen(v)
			if err != nil {
				return err
			}
			o.HTTPHost = hp.host
			o.HTTPSPort = hp.port
		case "http_port", "monitor_port":
			o.HTTPPort = int(v.(int64))
		case "https_port":
			o.HTTPSPort = int(v.(int64))
		case "cluster":
			cm := v.(map[string]interface{})
			if err := parseCluster(cm, o); err != nil {
				return err
			}
		case "logfile", "log_file":
			o.LogFile = v.(string)
		case "syslog":
			o.Syslog = v.(bool)
		case "remote_syslog":
			o.RemoteSyslog = v.(string)
		case "pidfile", "pid_file":
			o.PidFile = v.(string)
		case "ports_file_dir":
			o.PortsFileDir = v.(string)
		case "prof_port":
			o.ProfPort = int(v.(int64))
		case "max_control_line":
			o.MaxControlLine = int(v.(int64))
		case "max_payload":
			o.MaxPayload = int(v.(int64))
		case "max_pending":
			o.MaxPending = v.(int64)
		case "max_connections", "max_conn":
			o.MaxConn = int(v.(int64))
		case "max_subscriptions", "max_subs":
			o.MaxSubs = int(v.(int64))
		case "ping_interval":
			o.PingInterval = time.Duration(int(v.(int64))) * time.Second
		case "ping_max":
			o.MaxPingsOut = int(v.(int64))
		case "tls":
			tlsm := v.(map[string]interface{})
			tc, err := parseTLS(tlsm)
			if err != nil {
				return err
			}
			if o.TLSConfig, err = GenTLSConfig(tc); err != nil {
				return err
			}
			o.TLSTimeout = tc.Timeout
		case "write_deadline":
			wd, ok := v.(string)
			if ok {
				dur, err := time.ParseDuration(wd)
				if err != nil {
					return fmt.Errorf("error parsing write_deadline: %v", err)
				}
				o.WriteDeadline = dur
			} else {
				// Backward compatible with old type, assume this is the
				// number of seconds.
				o.WriteDeadline = time.Duration(v.(int64)) * time.Second
				fmt.Printf("WARNING: write_deadline should be converted to a duration\n")
			}
		}
	}
	return nil
}

// hostPort is simple struct to hold parsed listen/addr strings.
type hostPort struct {
	host string
	port int
}

// parseListen will parse listen option which is replacing host/net and port
func parseListen(v interface{}) (*hostPort, error) {
	hp := &hostPort{}
	switch v.(type) {
	// Only a port
	case int64:
		hp.port = int(v.(int64))
	case string:
		host, port, err := net.SplitHostPort(v.(string))
		if err != nil {
			return nil, fmt.Errorf("Could not parse address string %q", v)
		}
		hp.port, err = strconv.Atoi(port)
		if err != nil {
			return nil, fmt.Errorf("Could not parse port %q", port)
		}
		hp.host = host
	}
	return hp, nil
}

// parseCluster will parse the cluster config.
func parseCluster(cm map[string]interface{}, opts *Options) error {
	for mk, mv := range cm {
		switch strings.ToLower(mk) {
		case "listen":
			hp, err := parseListen(mv)
			if err != nil {
				return err
			}
			opts.Cluster.Host = hp.host
			opts.Cluster.Port = hp.port
		case "port":
			opts.Cluster.Port = int(mv.(int64))
		case "host", "net":
			opts.Cluster.Host = mv.(string)
		case "authorization":
			am := mv.(map[string]interface{})
			auth, err := parseAuthorization(am)
			if err != nil {
				return err
			}
			if auth.users != nil {
				return fmt.Errorf("Cluster authorization does not allow multiple users")
			}
			opts.Cluster.Username = auth.user
			opts.Cluster.Password = auth.pass
			opts.Cluster.AuthTimeout = auth.timeout
			if auth.defaultPermissions != nil {
				// Import is whether or not we will send a SUB for interest to the other side.
				// Export is whether or not we will accept a SUB from the remote for a given subject.
				// Both only effect interest registration.
				// The parsing sets Import into Publish and Export into Subscribe, convert
				// accordingly.
				opts.Cluster.Permissions = &RoutePermissions{
					Import: auth.defaultPermissions.Publish,
					Export: auth.defaultPermissions.Subscribe,
				}
			}
		case "routes":
			ra := mv.([]interface{})
			opts.Routes = make([]*url.URL, 0, len(ra))
			for _, r := range ra {
				routeURL := r.(string)
				url, err := url.Parse(routeURL)
				if err != nil {
					return fmt.Errorf("error parsing route url [%q]", routeURL)
				}
				opts.Routes = append(opts.Routes, url)
			}
		case "tls":
			tlsm := mv.(map[string]interface{})
			tc, err := parseTLS(tlsm)
			if err != nil {
				return err
			}
			if opts.Cluster.TLSConfig, err = GenTLSConfig(tc); err != nil {
				return err
			}
			// For clusters, we will force strict verification. We also act
			// as both client and server, so will mirror the rootCA to the
			// clientCA pool.
			opts.Cluster.TLSConfig.ClientAuth = tls.RequireAndVerifyClientCert
			opts.Cluster.TLSConfig.RootCAs = opts.Cluster.TLSConfig.ClientCAs
			opts.Cluster.TLSTimeout = tc.Timeout
		case "cluster_advertise", "advertise":
			opts.Cluster.Advertise = mv.(string)
		case "no_advertise":
			opts.Cluster.NoAdvertise = mv.(bool)
		case "connect_retries":
			opts.Cluster.ConnectRetries = int(mv.(int64))
		}
	}
	return nil
}

// Helper function to parse Authorization configs.
func parseAuthorization(am map[string]interface{}) (*authorization, error) {
	auth := &authorization{}
	for mk, mv := range am {
		switch strings.ToLower(mk) {
		case "user", "username":
			auth.user = mv.(string)
		case "pass", "password":
			auth.pass = mv.(string)
		case "token":
			auth.token = mv.(string)
		case "timeout":
			at := float64(1)
			switch mv.(type) {
			case int64:
				at = float64(mv.(int64))
			case float64:
				at = mv.(float64)
			}
			auth.timeout = at
		case "users":
			users, err := parseUsers(mv)
			if err != nil {
				return nil, err
			}
			auth.users = users
		case "default_permission", "default_permissions", "permissions":
			pm, ok := mv.(map[string]interface{})
			if !ok {
				return nil, fmt.Errorf("Expected default permissions to be a map/struct, got %+v", mv)
			}
			permissions, err := parseUserPermissions(pm)
			if err != nil {
				return nil, err
			}
			auth.defaultPermissions = permissions
		}

		// Now check for permission defaults with multiple users, etc.
		if auth.users != nil && auth.defaultPermissions != nil {
			for _, user := range auth.users {
				if user.Permissions == nil {
					user.Permissions = auth.defaultPermissions
				}
			}
		}

	}
	return auth, nil
}

// Helper function to parse multiple users array with optional permissions.
func parseUsers(mv interface{}) ([]*User, error) {
	// Make sure we have an array
	uv, ok := mv.([]interface{})
	if !ok {
		return nil, fmt.Errorf("Expected users field to be an array, got %v", mv)
	}
	users := []*User{}
	for _, u := range uv {
		// Check its a map/struct
		um, ok := u.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("Expected user entry to be a map/struct, got %v", u)
		}
		user := &User{}
		for k, v := range um {
			switch strings.ToLower(k) {
			case "user", "username":
				user.Username = v.(string)
			case "pass", "password":
				user.Password = v.(string)
			case "permission", "permissions", "authorization":
				pm, ok := v.(map[string]interface{})
				if !ok {
					return nil, fmt.Errorf("Expected user permissions to be a map/struct, got %+v", v)
				}
				permissions, err := parseUserPermissions(pm)
				if err != nil {
					return nil, err
				}
				user.Permissions = permissions
			}
		}
		// Check to make sure we have at least username and password
		if user.Username == "" || user.Password == "" {
			return nil, fmt.Errorf("User entry requires a user and a password")
		}
		users = append(users, user)
	}
	return users, nil
}

// Helper function to parse user/account permissions
func parseUserPermissions(pm map[string]interface{}) (*Permissions, error) {
	p := &Permissions{}
	for k, v := range pm {
		switch strings.ToLower(k) {
		// For routes:
		// Import is Publish
		// Export is Subscribe
		case "pub", "publish", "import":
			subjects, err := parseSubjects(v)
			if err != nil {
				return nil, err
			}
			p.Publish = subjects
		case "sub", "subscribe", "export":
			subjects, err := parseSubjects(v)
			if err != nil {
				return nil, err
			}
			p.Subscribe = subjects
		default:
			return nil, fmt.Errorf("Unknown field %s parsing permissions", k)
		}
	}
	return p, nil
}

// Helper function to parse subject singeltons and/or arrays
func parseSubjects(v interface{}) ([]string, error) {
	var subjects []string
	switch v.(type) {
	case string:
		subjects = append(subjects, v.(string))
	case []string:
		subjects = v.([]string)
	case []interface{}:
		for _, i := range v.([]interface{}) {
			subject, ok := i.(string)
			if !ok {
				return nil, fmt.Errorf("Subject in permissions array cannot be cast to string")
			}
			subjects = append(subjects, subject)
		}
	default:
		return nil, fmt.Errorf("Expected subject permissions to be a subject, or array of subjects, got %T", v)
	}
	return checkSubjectArray(subjects)
}

// Helper function to validate subjects, etc for account permissioning.
func checkSubjectArray(sa []string) ([]string, error) {
	for _, s := range sa {
		if !IsValidSubject(s) {
			return nil, fmt.Errorf("Subject %q is not a valid subject", s)
		}
	}
	return sa, nil
}

// PrintTLSHelpAndDie prints TLS usage and exits.
func PrintTLSHelpAndDie() {
	fmt.Printf("%s", tlsUsage)
	for k := range cipherMap {
		fmt.Printf("    %s\n", k)
	}
	fmt.Printf("\nAvailable curve preferences include:\n")
	for k := range curvePreferenceMap {
		fmt.Printf("    %s\n", k)
	}
	os.Exit(0)
}

func parseCipher(cipherName string) (uint16, error) {

	cipher, exists := cipherMap[cipherName]
	if !exists {
		return 0, fmt.Errorf("Unrecognized cipher %s", cipherName)
	}

	return cipher, nil
}

func parseCurvePreferences(curveName string) (tls.CurveID, error) {
	curve, exists := curvePreferenceMap[curveName]
	if !exists {
		return 0, fmt.Errorf("Unrecognized curve preference %s", curveName)
	}
	return curve, nil
}

// Helper function to parse TLS configs.
func parseTLS(tlsm map[string]interface{}) (*TLSConfigOpts, error) {
	tc := TLSConfigOpts{}
	for mk, mv := range tlsm {
		switch strings.ToLower(mk) {
		case "cert_file":
			certFile, ok := mv.(string)
			if !ok {
				return nil, fmt.Errorf("error parsing tls config, expected 'cert_file' to be filename")
			}
			tc.CertFile = certFile
		case "key_file":
			keyFile, ok := mv.(string)
			if !ok {
				return nil, fmt.Errorf("error parsing tls config, expected 'key_file' to be filename")
			}
			tc.KeyFile = keyFile
		case "ca_file":
			caFile, ok := mv.(string)
			if !ok {
				return nil, fmt.Errorf("error parsing tls config, expected 'ca_file' to be filename")
			}
			tc.CaFile = caFile
		case "verify":
			verify, ok := mv.(bool)
			if !ok {
				return nil, fmt.Errorf("error parsing tls config, expected 'verify' to be a boolean")
			}
			tc.Verify = verify
		case "cipher_suites":
			ra := mv.([]interface{})
			if len(ra) == 0 {
				return nil, fmt.Errorf("error parsing tls config, 'cipher_suites' cannot be empty")
			}
			tc.Ciphers = make([]uint16, 0, len(ra))
			for _, r := range ra {
				cipher, err := parseCipher(r.(string))
				if err != nil {
					return nil, err
				}
				tc.Ciphers = append(tc.Ciphers, cipher)
			}
		case "curve_preferences":
			ra := mv.([]interface{})
			if len(ra) == 0 {
				return nil, fmt.Errorf("error parsing tls config, 'curve_preferences' cannot be empty")
			}
			tc.CurvePreferences = make([]tls.CurveID, 0, len(ra))
			for _, r := range ra {
				cps, err := parseCurvePreferences(r.(string))
				if err != nil {
					return nil, err
				}
				tc.CurvePreferences = append(tc.CurvePreferences, cps)
			}
		case "timeout":
			at := float64(0)
			switch mv.(type) {
			case int64:
				at = float64(mv.(int64))
			case float64:
				at = mv.(float64)
			}
			tc.Timeout = at
		default:
			return nil, fmt.Errorf("error parsing tls config, unknown field [%q]", mk)
		}
	}

	// If cipher suites were not specified then use the defaults
	if tc.Ciphers == nil {
		tc.Ciphers = defaultCipherSuites()
	}

	// If curve preferences were not specified, then use the defaults
	if tc.CurvePreferences == nil {
		tc.CurvePreferences = defaultCurvePreferences()
	}

	return &tc, nil
}

// GenTLSConfig loads TLS related configuration parameters.
func GenTLSConfig(tc *TLSConfigOpts) (*tls.Config, error) {

	// Now load in cert and private key
	cert, err := tls.LoadX509KeyPair(tc.CertFile, tc.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("error parsing X509 certificate/key pair: %v", err)
	}
	cert.Leaf, err = x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		return nil, fmt.Errorf("error parsing certificate: %v", err)
	}

	// Create TLSConfig
	// We will determine the cipher suites that we prefer.
	config := tls.Config{
		CurvePreferences:         tc.CurvePreferences,
		Certificates:             []tls.Certificate{cert},
		PreferServerCipherSuites: true,
		MinVersion:               tls.VersionTLS12,
		CipherSuites:             tc.Ciphers,
	}

	// Require client certificates as needed
	if tc.Verify {
		config.ClientAuth = tls.RequireAndVerifyClientCert
	}
	// Add in CAs if applicable.
	if tc.CaFile != "" {
		rootPEM, err := ioutil.ReadFile(tc.CaFile)
		if err != nil || rootPEM == nil {
			return nil, err
		}
		pool := x509.NewCertPool()
		ok := pool.AppendCertsFromPEM(rootPEM)
		if !ok {
			return nil, fmt.Errorf("failed to parse root ca certificate")
		}
		config.ClientCAs = pool
	}

	return &config, nil
}

// MergeOptions will merge two options giving preference to the flagOpts
// if the item is present.
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
	if flagOpts.ClientAdvertise != "" {
		opts.ClientAdvertise = flagOpts.ClientAdvertise
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
	if flagOpts.HTTPPort != 0 {
		opts.HTTPPort = flagOpts.HTTPPort
	}
	if flagOpts.Debug {
		opts.Debug = true
	}
	if flagOpts.Trace {
		opts.Trace = true
	}
	if flagOpts.Logtime {
		opts.Logtime = true
	}
	if flagOpts.LogFile != "" {
		opts.LogFile = flagOpts.LogFile
	}
	if flagOpts.PidFile != "" {
		opts.PidFile = flagOpts.PidFile
	}
	if flagOpts.PortsFileDir != "" {
		opts.PortsFileDir = flagOpts.PortsFileDir
	}
	if flagOpts.ProfPort != 0 {
		opts.ProfPort = flagOpts.ProfPort
	}
	if flagOpts.Cluster.ListenStr != "" {
		opts.Cluster.ListenStr = flagOpts.Cluster.ListenStr
	}
	if flagOpts.Cluster.NoAdvertise {
		opts.Cluster.NoAdvertise = true
	}
	if flagOpts.Cluster.ConnectRetries != 0 {
		opts.Cluster.ConnectRetries = flagOpts.Cluster.ConnectRetries
	}
	if flagOpts.Cluster.Advertise != "" {
		opts.Cluster.Advertise = flagOpts.Cluster.Advertise
	}
	if flagOpts.RoutesStr != "" {
		mergeRoutes(&opts, flagOpts)
	}
	return &opts
}

// RoutesFromStr parses route URLs from a string
func RoutesFromStr(routesStr string) []*url.URL {
	routes := strings.Split(routesStr, ",")
	if len(routes) == 0 {
		return nil
	}
	routeUrls := []*url.URL{}
	for _, r := range routes {
		r = strings.TrimSpace(r)
		u, _ := url.Parse(r)
		routeUrls = append(routeUrls, u)
	}
	return routeUrls
}

// This will merge the flag routes and override anything that was present.
func mergeRoutes(opts, flagOpts *Options) {
	routeUrls := RoutesFromStr(flagOpts.RoutesStr)
	if routeUrls == nil {
		return
	}
	opts.Routes = routeUrls
	opts.RoutesStr = flagOpts.RoutesStr
}

// RemoveSelfReference removes this server from an array of routes
func RemoveSelfReference(clusterPort int, routes []*url.URL) ([]*url.URL, error) {
	var cleanRoutes []*url.URL
	cport := strconv.Itoa(clusterPort)

	selfIPs, err := getInterfaceIPs()
	if err != nil {
		return nil, err
	}
	for _, r := range routes {
		host, port, err := net.SplitHostPort(r.Host)
		if err != nil {
			return nil, err
		}

		ipList, err := getURLIP(host)
		if err != nil {
			return nil, err
		}
		if cport == port && isIPInList(selfIPs, ipList) {
			continue
		}
		cleanRoutes = append(cleanRoutes, r)
	}

	return cleanRoutes, nil
}

func isIPInList(list1 []net.IP, list2 []net.IP) bool {
	for _, ip1 := range list1 {
		for _, ip2 := range list2 {
			if ip1.Equal(ip2) {
				return true
			}
		}
	}
	return false
}

func getURLIP(ipStr string) ([]net.IP, error) {
	ipList := []net.IP{}

	ip := net.ParseIP(ipStr)
	if ip != nil {
		ipList = append(ipList, ip)
		return ipList, nil
	}

	hostAddr, err := net.LookupHost(ipStr)
	if err != nil {
		return nil, fmt.Errorf("Error looking up host with route hostname: %v", err)
	}
	for _, addr := range hostAddr {
		ip = net.ParseIP(addr)
		if ip != nil {
			ipList = append(ipList, ip)
		}
	}
	return ipList, nil
}

func getInterfaceIPs() ([]net.IP, error) {
	var localIPs []net.IP

	interfaceAddr, err := net.InterfaceAddrs()
	if err != nil {
		return nil, fmt.Errorf("Error getting self referencing address: %v", err)
	}

	for i := 0; i < len(interfaceAddr); i++ {
		interfaceIP, _, _ := net.ParseCIDR(interfaceAddr[i].String())
		if net.ParseIP(interfaceIP.String()) != nil {
			localIPs = append(localIPs, interfaceIP)
		} else {
			return nil, fmt.Errorf("Error parsing self referencing address: %v", err)
		}
	}
	return localIPs, nil
}

func processOptions(opts *Options) {
	// Setup non-standard Go defaults
	if opts.Host == "" {
		opts.Host = DEFAULT_HOST
	}
	if opts.HTTPHost == "" {
		// Default to same bind from server if left undefined
		opts.HTTPHost = opts.Host
	}
	if opts.Port == 0 {
		opts.Port = DEFAULT_PORT
	} else if opts.Port == RANDOM_PORT {
		// Choose randomly inside of net.Listen
		opts.Port = 0
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
	if opts.TLSTimeout == 0 {
		opts.TLSTimeout = float64(TLS_TIMEOUT) / float64(time.Second)
	}
	if opts.AuthTimeout == 0 {
		opts.AuthTimeout = float64(AUTH_TIMEOUT) / float64(time.Second)
	}
	if opts.Cluster.Port != 0 {
		if opts.Cluster.Host == "" {
			opts.Cluster.Host = DEFAULT_HOST
		}
		if opts.Cluster.TLSTimeout == 0 {
			opts.Cluster.TLSTimeout = float64(TLS_TIMEOUT) / float64(time.Second)
		}
		if opts.Cluster.AuthTimeout == 0 {
			opts.Cluster.AuthTimeout = float64(AUTH_TIMEOUT) / float64(time.Second)
		}
	}
	if opts.MaxControlLine == 0 {
		opts.MaxControlLine = MAX_CONTROL_LINE_SIZE
	}
	if opts.MaxPayload == 0 {
		opts.MaxPayload = MAX_PAYLOAD_SIZE
	}
	if opts.MaxPending == 0 {
		opts.MaxPending = MAX_PENDING_SIZE
	}
	if opts.WriteDeadline == time.Duration(0) {
		opts.WriteDeadline = DEFAULT_FLUSH_DEADLINE
	}
	if opts.RQSubsSweep == time.Duration(0) {
		opts.RQSubsSweep = DEFAULT_REMOTE_QSUBS_SWEEPER
	}
	if opts.MaxClosedClients == 0 {
		opts.MaxClosedClients = DEFAULT_MAX_CLOSED_CLIENTS
	}
}

// ConfigureOptions accepts a flag set and augment it with NATS Server
// specific flags. On success, an options structure is returned configured
// based on the selected flags and/or configuration file.
// The command line options take precedence to the ones in the configuration file.
func ConfigureOptions(fs *flag.FlagSet, args []string, printVersion, printHelp, printTLSHelp func()) (*Options, error) {
	opts := &Options{}
	var (
		showVersion bool
		showHelp    bool
		showTLSHelp bool
		signal      string
		configFile  string
		err         error
	)

	fs.BoolVar(&showHelp, "h", false, "Show this message.")
	fs.BoolVar(&showHelp, "help", false, "Show this message.")
	fs.IntVar(&opts.Port, "port", 0, "Port to listen on.")
	fs.IntVar(&opts.Port, "p", 0, "Port to listen on.")
	fs.StringVar(&opts.Host, "addr", "", "Network host to listen on.")
	fs.StringVar(&opts.Host, "a", "", "Network host to listen on.")
	fs.StringVar(&opts.Host, "net", "", "Network host to listen on.")
	fs.StringVar(&opts.ClientAdvertise, "client_advertise", "", "Client URL to advertise to other servers.")
	fs.BoolVar(&opts.Debug, "D", false, "Enable Debug logging.")
	fs.BoolVar(&opts.Debug, "debug", false, "Enable Debug logging.")
	fs.BoolVar(&opts.Trace, "V", false, "Enable Trace logging.")
	fs.BoolVar(&opts.Trace, "trace", false, "Enable Trace logging.")
	fs.Bool("DV", false, "Enable Debug and Trace logging.")
	fs.BoolVar(&opts.Logtime, "T", true, "Timestamp log entries.")
	fs.BoolVar(&opts.Logtime, "logtime", true, "Timestamp log entries.")
	fs.StringVar(&opts.Username, "user", "", "Username required for connection.")
	fs.StringVar(&opts.Password, "pass", "", "Password required for connection.")
	fs.StringVar(&opts.Authorization, "auth", "", "Authorization token required for connection.")
	fs.IntVar(&opts.HTTPPort, "m", 0, "HTTP Port for /varz, /connz endpoints.")
	fs.IntVar(&opts.HTTPPort, "http_port", 0, "HTTP Port for /varz, /connz endpoints.")
	fs.IntVar(&opts.HTTPSPort, "ms", 0, "HTTPS Port for /varz, /connz endpoints.")
	fs.IntVar(&opts.HTTPSPort, "https_port", 0, "HTTPS Port for /varz, /connz endpoints.")
	fs.StringVar(&configFile, "c", "", "Configuration file.")
	fs.StringVar(&configFile, "config", "", "Configuration file.")
	fs.StringVar(&signal, "sl", "", "Send signal to gnatsd process (stop, quit, reopen, reload)")
	fs.StringVar(&signal, "signal", "", "Send signal to gnatsd process (stop, quit, reopen, reload)")
	fs.StringVar(&opts.PidFile, "P", "", "File to store process pid.")
	fs.StringVar(&opts.PidFile, "pid", "", "File to store process pid.")
	fs.StringVar(&opts.PortsFileDir, "ports_file_dir", "", "Creates a ports file in the specified directory (<executable_name>_<pid>.ports)")
	fs.StringVar(&opts.LogFile, "l", "", "File to store logging output.")
	fs.StringVar(&opts.LogFile, "log", "", "File to store logging output.")
	fs.BoolVar(&opts.Syslog, "s", false, "Enable syslog as log method.")
	fs.BoolVar(&opts.Syslog, "syslog", false, "Enable syslog as log method..")
	fs.StringVar(&opts.RemoteSyslog, "r", "", "Syslog server addr (udp://127.0.0.1:514).")
	fs.StringVar(&opts.RemoteSyslog, "remote_syslog", "", "Syslog server addr (udp://127.0.0.1:514).")
	fs.BoolVar(&showVersion, "version", false, "Print version information.")
	fs.BoolVar(&showVersion, "v", false, "Print version information.")
	fs.IntVar(&opts.ProfPort, "profile", 0, "Profiling HTTP port")
	fs.StringVar(&opts.RoutesStr, "routes", "", "Routes to actively solicit a connection.")
	fs.StringVar(&opts.Cluster.ListenStr, "cluster", "", "Cluster url from which members can solicit routes.")
	fs.StringVar(&opts.Cluster.ListenStr, "cluster_listen", "", "Cluster url from which members can solicit routes.")
	fs.StringVar(&opts.Cluster.Advertise, "cluster_advertise", "", "Cluster URL to advertise to other servers.")
	fs.BoolVar(&opts.Cluster.NoAdvertise, "no_advertise", false, "Advertise known cluster IPs to clients.")
	fs.IntVar(&opts.Cluster.ConnectRetries, "connect_retries", 0, "For implicit routes, number of connect retries")
	fs.BoolVar(&showTLSHelp, "help_tls", false, "TLS help.")
	fs.BoolVar(&opts.TLS, "tls", false, "Enable TLS.")
	fs.BoolVar(&opts.TLSVerify, "tlsverify", false, "Enable TLS with client verification.")
	fs.StringVar(&opts.TLSCert, "tlscert", "", "Server certificate file.")
	fs.StringVar(&opts.TLSKey, "tlskey", "", "Private key for server certificate.")
	fs.StringVar(&opts.TLSCaCert, "tlscacert", "", "Client certificate CA for verification.")

	// The flags definition above set "default" values to some of the options.
	// Calling Parse() here will override the default options with any value
	// specified from the command line. This is ok. We will then update the
	// options with the content of the configuration file (if present), and then,
	// call Parse() again to override the default+config with command line values.
	// Calling Parse() before processing config file is necessary since configFile
	// itself is a command line argument, and also Parse() is required in order
	// to know if user wants simply to show "help" or "version", etc...
	if err := fs.Parse(args); err != nil {
		return nil, err
	}

	if showVersion {
		printVersion()
		return nil, nil
	}

	if showHelp {
		printHelp()
		return nil, nil
	}

	if showTLSHelp {
		printTLSHelp()
		return nil, nil
	}

	// Process args looking for non-flag options,
	// 'version' and 'help' only for now
	showVersion, showHelp, err = ProcessCommandLineArgs(fs)
	if err != nil {
		return nil, err
	} else if showVersion {
		printVersion()
		return nil, nil
	} else if showHelp {
		printHelp()
		return nil, nil
	}

	// Snapshot flag options.
	FlagSnapshot = opts.Clone()

	// Process signal control.
	if signal != "" {
		if err := processSignal(signal); err != nil {
			return nil, err
		}
	}

	// Parse config if given
	if configFile != "" {
		// This will update the options with values from the config file.
		if err := opts.ProcessConfigFile(configFile); err != nil {
			return nil, err
		}
		// Call this again to override config file options with options from command line.
		// Note: We don't need to check error here since if there was an error, it would
		// have been caught the first time this function was called (after setting up the
		// flags).
		fs.Parse(args)
	}

	// Special handling of some flags
	var (
		flagErr     error
		tlsDisabled bool
		tlsOverride bool
	)
	fs.Visit(func(f *flag.Flag) {
		// short-circuit if an error was encountered
		if flagErr != nil {
			return
		}
		if strings.HasPrefix(f.Name, "tls") {
			if f.Name == "tls" {
				if !opts.TLS {
					// User has specified "-tls=false", we need to disable TLS
					opts.TLSConfig = nil
					tlsDisabled = true
					tlsOverride = false
					return
				}
				tlsOverride = true
			} else if !tlsDisabled {
				tlsOverride = true
			}
		} else {
			switch f.Name {
			case "DV":
				// Check value to support -DV=false
				boolValue, _ := strconv.ParseBool(f.Value.String())
				opts.Trace, opts.Debug = boolValue, boolValue
			case "cluster", "cluster_listen":
				// Override cluster config if explicitly set via flags.
				flagErr = overrideCluster(opts)
			case "routes":
				// Keep in mind that the flag has updated opts.RoutesStr at this point.
				if opts.RoutesStr == "" {
					// Set routes array to nil since routes string is empty
					opts.Routes = nil
					return
				}
				routeUrls := RoutesFromStr(opts.RoutesStr)
				opts.Routes = routeUrls
			}
		}
	})
	if flagErr != nil {
		return nil, flagErr
	}

	// This will be true if some of the `-tls` params have been set and
	// `-tls=false` has not been set.
	if tlsOverride {
		if err := overrideTLS(opts); err != nil {
			return nil, err
		}
	}

	// If we don't have cluster defined in the configuration
	// file and no cluster listen string override, but we do
	// have a routes override, we need to report misconfiguration.
	if opts.RoutesStr != "" && opts.Cluster.ListenStr == "" && opts.Cluster.Host == "" && opts.Cluster.Port == 0 {
		return nil, errors.New("solicited routes require cluster capabilities, e.g. --cluster")
	}

	return opts, nil
}

// overrideTLS is called when at least "-tls=true" has been set.
func overrideTLS(opts *Options) error {
	if opts.TLSCert == "" {
		return errors.New("TLS Server certificate must be present and valid")
	}
	if opts.TLSKey == "" {
		return errors.New("TLS Server private key must be present and valid")
	}

	tc := TLSConfigOpts{}
	tc.CertFile = opts.TLSCert
	tc.KeyFile = opts.TLSKey
	tc.CaFile = opts.TLSCaCert
	tc.Verify = opts.TLSVerify

	var err error
	opts.TLSConfig, err = GenTLSConfig(&tc)
	return err
}

// overrideCluster updates Options.Cluster if that flag "cluster" (or "cluster_listen")
// has explicitly be set in the command line. If it is set to empty string, it will
// clear the Cluster options.
func overrideCluster(opts *Options) error {
	if opts.Cluster.ListenStr == "" {
		// This one is enough to disable clustering.
		opts.Cluster.Port = 0
		return nil
	}
	clusterURL, err := url.Parse(opts.Cluster.ListenStr)
	if err != nil {
		return err
	}
	h, p, err := net.SplitHostPort(clusterURL.Host)
	if err != nil {
		return err
	}
	opts.Cluster.Host = h
	_, err = fmt.Sscan(p, &opts.Cluster.Port)
	if err != nil {
		return err
	}

	if clusterURL.User != nil {
		pass, hasPassword := clusterURL.User.Password()
		if !hasPassword {
			return errors.New("expected cluster password to be set")
		}
		opts.Cluster.Password = pass

		user := clusterURL.User.Username()
		opts.Cluster.Username = user
	} else {
		// Since we override from flag and there is no user/pwd, make
		// sure we clear what we may have gotten from config file.
		opts.Cluster.Username = ""
		opts.Cluster.Password = ""
	}

	return nil
}

func processSignal(signal string) error {
	var (
		pid           string
		commandAndPid = strings.Split(signal, "=")
	)
	if l := len(commandAndPid); l == 2 {
		pid = commandAndPid[1]
	} else if l > 2 {
		return fmt.Errorf("invalid signal parameters: %v", commandAndPid[2:])
	}
	if err := ProcessSignal(Command(commandAndPid[0]), pid); err != nil {
		return err
	}
	os.Exit(0)
	return nil
}
