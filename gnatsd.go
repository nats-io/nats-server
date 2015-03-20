// Copyright 2012-2013 Apcera Inc. All rights reserved.

package main

import (
	"flag"
	"os"
	"strings"

	"github.com/apcera/gnatsd/auth"
	"github.com/apcera/gnatsd/logger"
	"github.com/apcera/gnatsd/server"
)

func main() {
	// Server Options
	opts := server.Options{}

	var showVersion bool
	var debugAndTrace bool
	var configFile string

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

	flag.Usage = server.Usage

	flag.Parse()

	// Show version and exit
	if showVersion {
		server.PrintServerAndExit()
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
			server.Usage()
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
	if opts.Username != "" {
		auth := &auth.Plain{
			Username: opts.Username,
			Password: opts.Password,
		}

		s.SetAuthMethod(auth)
	} else if opts.Authorization != "" {
		auth := &auth.Token{
			Token: opts.Authorization,
		}

		s.SetAuthMethod(auth)
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
