// Copyright 2012 Apcera Inc. All rights reserved.

package main

import (
	"log"
	"net/http"
	_ "net/http/pprof"
	"github.com/apcera/gnatsd/server"
)

func main() {
	// Parse flags

	// Profiler
	go func() {
		log.Println(http.ListenAndServe("localhost:6062", nil))
	}()
	// Parse config if given
	log.Println("starting up!")

	s := server.New()
	s.AcceptLoop()
}