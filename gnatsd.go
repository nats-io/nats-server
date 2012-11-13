// Copyright 2012 Apcera Inc. All rights reserved.

package main

import (
	"flag"
	"log"
	"net/http"
	_ "net/http/pprof"
	"github.com/apcera/gnatsd/server"
)

var port = server.DEFAULT_PORT
var host = server.DEFAULT_HOST

func main() {
	// Parse flags

	flag.IntVar(&port, "port", server.DEFAULT_PORT, "Port to listen on.")
	flag.IntVar(&port, "p", server.DEFAULT_PORT, "Port to listen on.")
	flag.StringVar(&host, "host", server.DEFAULT_HOST, "Network host to listen on.")
	flag.StringVar(&host, "h", server.DEFAULT_HOST, "Network host to listen on.")
	flag.Parse()

	// Profiler
	go func() {
		log.Println(http.ListenAndServe("localhost:6062", nil))
	}()
	// Parse config if given
	log.Println("starting up!")

	s := server.New()
	s.AcceptLoop(host, port)
}