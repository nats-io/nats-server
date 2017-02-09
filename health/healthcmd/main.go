package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"

	"github.com/nats-io/gnatsd/health"
	"github.com/nats-io/gnatsd/logger"
)

// healthcmd runs an allcall election from a standalone
// command line nats client. It exercises the same
// gnatsd/health library code that runs as an internal client
// in process with gnatsd.

func usage() {
	log.Fatalf("use: healthcmd {host}:port {rank}\n")
}

func main() {

	log.SetFlags(0)
	flag.Usage = usage
	flag.Parse()

	args := flag.Args()
	if len(args) < 1 {
		usage()
	}

	rank := 0
	var err error
	if len(args) >= 2 {
		rank, err = strconv.Atoi(args[1])
		if err != nil {
			fmt.Fprintf(os.Stderr, "2nd arg should be our numeric rank")
		}
	}

	const colors = false
	const micros, pid = true, true
	const trace = false
	const debug = true
	aLogger := logger.NewStdLogger(micros, debug, trace, colors, pid, log.LUTC)

	cfg := &health.MembershipCfg{
		NatsURL: "nats://" + args[0], // "nats://127.0.0.1:4222"
		MyRank:  rank,
		Log:     aLogger,
	}
	m := health.NewMembership(cfg)
	err = m.Start()
	if err != nil {
		panic(err)
	}

	select {}
}
