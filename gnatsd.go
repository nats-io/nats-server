// Copyright 2012 Apcera Inc. All rights reserved.

package main

import (
	"flag"
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
	flag.Parse()

	if debugAndTrace {
		opts.Trace, opts.Debug = true, true
	}

	// Profiler
	go func() {
		log.Println(http.ListenAndServe("localhost:6062", nil))
	}()

	// Parse config if given

	s := server.New(opts)
	s.AcceptLoop()
}

