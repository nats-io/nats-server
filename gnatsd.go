// Copyright 2012 Apcera Inc. All rights reserved.

package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"

	"github.com/apcera/gnatsd/server"
)

func main() {
	// logging setup
	server.LogSetup()

	opts := server.Options{}

	var debugAndTrace bool

	// Parse flags
	flag.IntVar(&opts.Port, "port", server.DEFAULT_PORT, "Port to listen on.")
	flag.IntVar(&opts.Port, "p", server.DEFAULT_PORT, "Port to listen on.")
	flag.StringVar(&opts.Host, "host", server.DEFAULT_HOST, "Network host to listen on.")
	flag.StringVar(&opts.Host, "h", server.DEFAULT_HOST, "Network host to listen on.")
	flag.BoolVar(&opts.Debug, "D", false, "Enable Debug logging.")
	flag.BoolVar(&opts.Debug, "debug", false, "Enable Debug logging.")
	flag.BoolVar(&opts.Trace, "V", false, "Enable Trace logging.")
	flag.BoolVar(&opts.Trace, "trace", false, "Enable Trace logging.")
	flag.BoolVar(&debugAndTrace, "DV", false, "Enable Debug and Trace logging.")
	flag.StringVar(&opts.Username, "user", "", "Username required for connection.")
	flag.StringVar(&opts.Password, "pass", "", "Password required for connection.")
	flag.StringVar(&opts.Authorization, "auth", "", "Authorization token required for connection.")

	flag.IntVar(&opts.HttpPort, "m", 0, "HTTP Port for /varz, /connz endpoints.")
	flag.IntVar(&opts.HttpPort, "http_port", 0, "HTTP Port for /varz, /connz endpoints.")

	flag.Parse()

	if debugAndTrace {
		opts.Trace, opts.Debug = true, true
	}

	// TBD: Parse config if given

	// Profiler
	go func() {
		log.Println(http.ListenAndServe("localhost:6062", nil))
	}()

	// Create the server with appropriate options.
	s := server.New(&opts)

	// Start up the http server if needed.
	if opts.HttpPort != 0 {
		go func() {
			// FIXME(dlc): port config
			lm := fmt.Sprintf("Starting http monitor on port %d", opts.HttpPort)
			server.Log(lm)
			http.HandleFunc("/varz", func(w http.ResponseWriter, r *http.Request) {
				s.HandleVarz(w, r)
			})
			hp := fmt.Sprintf("%s:%d", opts.Host, opts.HttpPort)
			log.Fatal(http.ListenAndServe(hp, nil))
		}()
	}

	// Wait for clients.
	s.AcceptLoop()
}

