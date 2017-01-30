// Copyright 2012-2016 Apcera Inc. All rights reserved.

package main

import (
	"flag"
	"fmt"
	"net"
	"net/url"
	"os"
	"strings"

	"github.com/nats-io/gnatsd/auth"
	"github.com/nats-io/gnatsd/logger"
	"github.com/nats-io/gnatsd/server"
)

var usageStr = `
Usage: gnatsd [options]

Server Options:
    -a, --addr <host>                Bind to host address (default: 0.0.0.0)
    -p, --port <port>                Use port for clients (default: 4222)
    -P, --pid <file>                 File to store PID
    -m, --http_port <port>           Use port for http monitoring
    -ms,--https_port <port>          Use port for https monitoring
    -c, --config <file>              Configuration file

Logging Options:
    -l, --log <file>                 File to redirect log output
    -T, --logtime                    Timestamp log entries (default: true)
    -s, --syslog                     Enable syslog as log method
    -r, --remote_syslog <addr>       Syslog server addr (udp://localhost:514)
    -D, --debug                      Enable debugging output
    -V, --trace                      Trace the raw protocol
    -DV                              Debug and trace

Authorization Options:
        --user <user>                User required for connections
        --pass <password>            Password required for connections
        --auth <token>               Authorization token required for connections

TLS Options:
        --tls                        Enable TLS, do not verify clients (default: false)
        --tlscert <file>             Server certificate file
        --tlskey <file>              Private key for server certificate
        --tlsverify                  Enable TLS, verify client certificates
        --tlscacert <file>           Client certificate CA for verification

Cluster Options:
        --routes <rurl-1, rurl-2>    Routes to solicit and connect
        --cluster <cluster-url>      Cluster URL for solicited routes
        --no_advertise <bool>        Advertise known cluster IPs to clients
		

Common Options:
    -h, --help                       Show this message
    -v, --version                    Show version
        --help_tls                   TLS help
`

// usage will print out the flag options for the server.
func usage() {
	fmt.Printf("%s\n", usageStr)
	os.Exit(0)
}

func main() {
	// Server Options
	opts := server.Options{}

	var showVersion bool
	var debugAndTrace bool
	var configFile string
	var showTLSHelp bool

	// Parse flags
	flag.IntVar(&opts.Port, "port", 0, "Port to listen on.")
	flag.IntVar(&opts.Port, "p", 0, "Port to listen on.")
	flag.StringVar(&opts.Host, "addr", "", "Network host to listen on.")
	flag.StringVar(&opts.Host, "a", "", "Network host to listen on.")
	flag.StringVar(&opts.Host, "net", "", "Network host to listen on.")
	flag.BoolVar(&opts.Debug, "D", false, "Enable Debug logging.")
	flag.BoolVar(&opts.Debug, "debug", false, "Enable Debug logging.")
	flag.BoolVar(&opts.Trace, "V", false, "Enable Trace logging.")
	flag.BoolVar(&opts.Trace, "trace", false, "Enable Trace logging.")
	flag.BoolVar(&debugAndTrace, "DV", false, "Enable Debug and Trace logging.")
	flag.BoolVar(&opts.Logtime, "T", true, "Timestamp log entries.")
	flag.BoolVar(&opts.Logtime, "logtime", true, "Timestamp log entries.")
	flag.StringVar(&opts.Username, "user", "", "Username required for connection.")
	flag.StringVar(&opts.Password, "pass", "", "Password required for connection.")
	flag.StringVar(&opts.Authorization, "auth", "", "Authorization token required for connection.")
	flag.IntVar(&opts.HTTPPort, "m", 0, "HTTP Port for /varz, /connz endpoints.")
	flag.IntVar(&opts.HTTPPort, "http_port", 0, "HTTP Port for /varz, /connz endpoints.")
	flag.IntVar(&opts.HTTPSPort, "ms", 0, "HTTPS Port for /varz, /connz endpoints.")
	flag.IntVar(&opts.HTTPSPort, "https_port", 0, "HTTPS Port for /varz, /connz endpoints.")
	flag.StringVar(&configFile, "c", "", "Configuration file.")
	flag.StringVar(&configFile, "config", "", "Configuration file.")
	flag.StringVar(&opts.PidFile, "P", "", "File to store process pid.")
	flag.StringVar(&opts.PidFile, "pid", "", "File to store process pid.")
	flag.StringVar(&opts.LogFile, "l", "", "File to store logging output.")
	flag.StringVar(&opts.LogFile, "log", "", "File to store logging output.")
	flag.BoolVar(&opts.Syslog, "s", false, "Enable syslog as log method.")
	flag.BoolVar(&opts.Syslog, "syslog", false, "Enable syslog as log method..")
	flag.StringVar(&opts.RemoteSyslog, "r", "", "Syslog server addr (udp://localhost:514).")
	flag.StringVar(&opts.RemoteSyslog, "remote_syslog", "", "Syslog server addr (udp://localhost:514).")
	flag.BoolVar(&showVersion, "version", false, "Print version information.")
	flag.BoolVar(&showVersion, "v", false, "Print version information.")
	flag.IntVar(&opts.ProfPort, "profile", 0, "Profiling HTTP port")
	flag.StringVar(&opts.RoutesStr, "routes", "", "Routes to actively solicit a connection.")
	flag.StringVar(&opts.ClusterListenStr, "cluster", "", "Cluster url from which members can solicit routes.")
	flag.StringVar(&opts.ClusterListenStr, "cluster_listen", "", "Cluster url from which members can solicit routes.")
	flag.BoolVar(&opts.ClusterNoAdvertise, "no_advertise", false, "Advertise known cluster IPs to clients.")
	flag.BoolVar(&showTLSHelp, "help_tls", false, "TLS help.")
	flag.BoolVar(&opts.TLS, "tls", false, "Enable TLS.")
	flag.BoolVar(&opts.TLSVerify, "tlsverify", false, "Enable TLS with client verification.")
	flag.StringVar(&opts.TLSCert, "tlscert", "", "Server certificate file.")
	flag.StringVar(&opts.TLSKey, "tlskey", "", "Private key for server certificate.")
	flag.StringVar(&opts.TLSCaCert, "tlscacert", "", "Client certificate CA for verification.")

	flag.Usage = usage

	flag.Parse()

	// Show version and exit
	if showVersion {
		server.PrintServerAndExit()
	}

	if showTLSHelp {
		server.PrintTLSHelpAndDie()
	}

	// One flag can set multiple options.
	if debugAndTrace {
		opts.Trace, opts.Debug = true, true
	}

	// Process args looking for non-flag options,
	// 'version' and 'help' only for now
	for _, arg := range flag.Args() {
		switch strings.ToLower(arg) {
		case "version":
			server.PrintServerAndExit()
		case "help":
			usage()
		}
	}

	// Parse config if given
	if configFile != "" {
		fileOpts, err := server.ProcessConfigFile(configFile)
		if err != nil {
			server.PrintAndDie(err.Error())
		}
		opts = *server.MergeOptions(fileOpts, &opts)
	}

	// Remove any host/ip that points to itself in Route
	newroutes, err := server.RemoveSelfReference(opts.ClusterPort, opts.Routes)
	if err != nil {
		server.PrintAndDie(err.Error())
	}
	opts.Routes = newroutes

	// Configure TLS based on any present flags
	configureTLS(&opts)

	// Configure cluster opts if explicitly set via flags.
	err = configureClusterOpts(&opts)
	if err != nil {
		server.PrintAndDie(err.Error())
	}

	// Create the server with appropriate options.
	s := server.New(&opts)

	// Configure the authentication mechanism
	configureAuth(s, &opts)

	// Configure the logger based on the flags
	configureLogger(s, &opts)

	// Start things up. Block here until done.
	s.Start()
}

func configureAuth(s *server.Server, opts *server.Options) {
	// Client
	// Check for multiple users first
	if opts.Users != nil {
		auth := auth.NewMultiUser(opts.Users)
		s.SetClientAuthMethod(auth)
	} else if opts.Username != "" {
		auth := &auth.Plain{
			Username: opts.Username,
			Password: opts.Password,
		}
		s.SetClientAuthMethod(auth)
	} else if opts.Authorization != "" {
		auth := &auth.Token{
			Token: opts.Authorization,
		}
		s.SetClientAuthMethod(auth)
	}
	// Routes
	if opts.ClusterUsername != "" {
		auth := &auth.Plain{
			Username: opts.ClusterUsername,
			Password: opts.ClusterPassword,
		}
		s.SetRouteAuthMethod(auth)
	}
}

func configureLogger(s *server.Server, opts *server.Options) {
	var log server.Logger

	if opts.LogFile != "" {
		log = logger.NewFileLogger(opts.LogFile, opts.Logtime, opts.Debug, opts.Trace, true)
	} else if opts.RemoteSyslog != "" {
		log = logger.NewRemoteSysLogger(opts.RemoteSyslog, opts.Debug, opts.Trace)
	} else if opts.Syslog {
		log = logger.NewSysLogger(opts.Debug, opts.Trace)
	} else {
		colors := true
		// Check to see if stderr is being redirected and if so turn off color
		// Also turn off colors if we're running on Windows where os.Stderr.Stat() returns an invalid handle-error
		stat, err := os.Stderr.Stat()
		if err != nil || (stat.Mode()&os.ModeCharDevice) == 0 {
			colors = false
		}
		log = logger.NewStdLogger(opts.Logtime, opts.Debug, opts.Trace, colors, true)
	}

	s.SetLogger(log, opts.Debug, opts.Trace)
}

func configureTLS(opts *server.Options) {
	// If no trigger flags, ignore the others
	if !opts.TLS && !opts.TLSVerify {
		return
	}
	if opts.TLSCert == "" {
		server.PrintAndDie("TLS Server certificate must be present and valid.")
	}
	if opts.TLSKey == "" {
		server.PrintAndDie("TLS Server private key must be present and valid.")
	}

	tc := server.TLSConfigOpts{}
	tc.CertFile = opts.TLSCert
	tc.KeyFile = opts.TLSKey
	tc.CaFile = opts.TLSCaCert

	if opts.TLSVerify {
		tc.Verify = true
	}
	var err error
	if opts.TLSConfig, err = server.GenTLSConfig(&tc); err != nil {
		server.PrintAndDie(err.Error())
	}
}

func configureClusterOpts(opts *server.Options) error {
	// If we don't have cluster defined in the configuration
	// file and no cluster listen string override, but we do
	// have a routes override, we need to report misconfiguration.
	if opts.ClusterListenStr == "" && opts.ClusterHost == "" &&
		opts.ClusterPort == 0 {
		if opts.RoutesStr != "" {
			server.PrintAndDie("Solicited routes require cluster capabilities, e.g. --cluster.")
		}
		return nil
	}

	// If cluster flag override, process it
	if opts.ClusterListenStr != "" {
		clusterURL, err := url.Parse(opts.ClusterListenStr)
		h, p, err := net.SplitHostPort(clusterURL.Host)
		if err != nil {
			return err
		}
		opts.ClusterHost = h
		_, err = fmt.Sscan(p, &opts.ClusterPort)
		if err != nil {
			return err
		}

		if clusterURL.User != nil {
			pass, hasPassword := clusterURL.User.Password()
			if !hasPassword {
				return fmt.Errorf("Expected cluster password to be set.")
			}
			opts.ClusterPassword = pass

			user := clusterURL.User.Username()
			opts.ClusterUsername = user
		} else {
			// Since we override from flag and there is no user/pwd, make
			// sure we clear what we may have gotten from config file.
			opts.ClusterUsername = ""
			opts.ClusterPassword = ""
		}
	}

	// If we have routes but no config file, fill in here.
	if opts.RoutesStr != "" && opts.Routes == nil {
		opts.Routes = server.RoutesFromStr(opts.RoutesStr)
	}

	return nil
}
