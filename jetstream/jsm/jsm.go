// Copyright 2019 The NATS Authors
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

package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	api "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

var usageStr = `
Usage:
  jsm [-s server] [-creds file] [command]

Available Commands:
  info                   # General account info

  ls                     # List all message sets
  add                    # Add a new message set
  rm     [mset]          # Delete a message set
  purge  [mset]          # Purge a message set
  rm-msg [mset] [seq]    # Securely delete a message from a message set
  info   [mset]          # Get information about a message set

  add-obs                # Add an observable to a message set
  ls-obs   [mset]        # List all observables for the message set
  rm-obs   [mset] [obs]  # Delete an observable to a message set
  info-obs [mset] [obs]  # Get information about an observable
  next     [mset] [obs]  # Get the next message for a pull based observable
`

func usage() {
	fmt.Printf("%s\n", usageStr)
}

func showUsageAndExit(exitcode int) {
	usage()
	os.Exit(exitcode)
}

func main() {
	var url = flag.String("s", nats.DefaultURL, "The NATS System URL")
	var userCreds = flag.String("creds", "", "User Credentials File")
	var showHelp = flag.Bool("h", false, "Show help message")

	log.SetFlags(0)
	flag.Usage = usage
	flag.Parse()

	if *showHelp {
		showUsageAndExit(0)
	}

	args := flag.Args()
	if len(args) < 1 {
		showUsageAndExit(1)
	}

	// Connect Options.
	opts := []nats.Option{nats.Name("JetStream Management CLI"), nats.NoReconnect()}

	// Use UserCredentials
	if *userCreds != "" {
		opts = append(opts, nats.UserCredentials(*userCreds))
	}

	// Connect to NATS
	nc, err := nats.Connect(*url, opts...)
	if err != nil {
		log.Fatal(err)
	}

	cmd := args[0]

	switch strings.ToLower(cmd) {
	case "info":
		if len(args) == 3 {
			req := []byte(args[1] + " " + args[2])
			resp, _ := nc.Request(api.JetStreamObservableInfo, req, time.Second)
			log.Printf("Received response of %s", resp.Data)
		} else if len(args) > 1 {
			getMsgSetInfo(nc, args[1])
		} else {
			getAccountInfo(nc)
		}
	case "ls", "list":
		var names []string
		resp, _ := nc.Request(api.JetStreamMsgSets, nil, time.Second)
		if err := json.Unmarshal(resp.Data, &names); err != nil {
			log.Fatalf("Unexpected error: %v", err)
		}
		log.Println()
		if len(names) == 0 {
			log.Printf("None")
		}
		for i, name := range names {
			log.Printf("%d) %s", i+1, name)
		}
		log.Println()
	case "add", "create":
		scanner := bufio.NewScanner(os.Stdin)
		fmt.Println("Enter the following information")
		fmt.Print("Name: ")
		scanner.Scan()
		name := scanner.Text()

		fmt.Print("Subjects: ")
		scanner.Scan()
		subjects := processSubjects(scanner.Text())

		fmt.Print("Limits (msgs, bytes, age): ")
		scanner.Scan()
		maxMsgs, maxBytes, maxAge := processLimits(scanner.Text())

		fmt.Print("Storage: ")
		scanner.Scan()
		storage := processStorage(scanner.Text())

		// FIXME(dlc) - Add in other options.
		cfg := api.MsgSetConfig{
			Name:     name,
			Storage:  storage,
			Subjects: subjects,
			MaxMsgs:  maxMsgs,
			MaxBytes: maxBytes,
			MaxAge:   maxAge,
		}
		req, err := json.Marshal(cfg)
		if err != nil {
			log.Fatalf("Unexpected error: %v", err)
		}
		resp, _ := nc.Request(api.JetStreamCreateMsgSet, req, time.Second)
		log.Printf("Received response of %q", resp.Data)

	case "del", "delete", "rm":
		if len(args) == 1 {
			showUsageAndExit(1)
		}
		name := []byte(args[1])
		resp, _ := nc.Request(api.JetStreamDeleteMsgSet, name, time.Second)
		log.Printf("Received response of %q", resp.Data)

	case "purge":
		if len(args) == 1 {
			showUsageAndExit(1)
		}
		name := []byte(args[1])
		resp, _ := nc.Request(api.JetStreamPurgeMsgSet, name, time.Second)
		log.Printf("Received response of %q", resp.Data)

	case "del-msg", "delmsg", "rmmsg":
		if len(args) != 3 {
			showUsageAndExit(1)
		}
		name := args[1]
		seq := args[2]
		req := []byte(name + " " + seq)
		resp, _ := nc.Request(api.JetStreamDeleteMsg, req, time.Second)
		log.Printf("Received response of %q", resp.Data)

	case "add-obs", "create-obs":
		var names []string
		resp, _ := nc.Request(api.JetStreamMsgSets, nil, time.Second)
		if err := json.Unmarshal(resp.Data, &names); err != nil {
			log.Fatalf("Unexpected error: %v", err)
		}
		if len(names) == 0 {
			log.Fatalf("No message sets available")
		}

		scanner := bufio.NewScanner(os.Stdin)
		fmt.Println("Enter the following information")

		// The create request
		var req api.CreateObservableRequest
		cfg := &req.Config

		fmt.Print("Message Set Name: ")
		scanner.Scan()
		req.MsgSet = scanner.Text()

		// Quickly make sure its a valid message set.
		var ok bool
		for _, ms := range names {
			if ms == req.MsgSet {
				ok = true
				break
			}
		}
		if !ok {
			log.Fatalf("Not a valid message set")
		}

		fmt.Print("Durable Name: ")
		scanner.Scan()
		cfg.Durable = scanner.Text()

		fmt.Print("Push or Pull: ")
		scanner.Scan()
		porp := scanner.Text()

		switch strings.ToLower(porp) {
		case "push":
			fmt.Print("Delivery Subject: ")
			scanner.Scan()
			cfg.Delivery = scanner.Text()
		}

		// Default to yes
		cfg.DeliverAll = true
		fmt.Print("Deliver All? (Y|n): ")
		scanner.Scan()
		ans := scanner.Text()
		if len(ans) > 0 {
			cfg.DeliverAll, _ = strconv.ParseBool(ans)
		}
		if !cfg.DeliverAll {
			fmt.Print("Deliver Last? (y|n): ")
			scanner.Scan()
			cfg.DeliverLast, _ = strconv.ParseBool(scanner.Text())
		}
		if !cfg.DeliverAll && !cfg.DeliverLast {
			fmt.Print("Start at Time Delta: ")
			scanner.Scan()
			ans := scanner.Text()
			if len(ans) > 0 {
				timeDelta, err := time.ParseDuration(ans)
				if err != nil {
					log.Fatalf("Error parsing time duration: %v", err)
				}
				cfg.StartTime = time.Now().Add(-timeDelta)
			} else {
				fmt.Print("Start at Message Set Sequence: ")
				scanner.Scan()
				if seq, err := strconv.Atoi(scanner.Text()); err != nil {
					log.Fatalf("Error parsing sequence: %v", err)
				} else {
					cfg.MsgSetSeq = uint64(seq)
				}
			}
		}

		// Pull based are always explicit ack.
		if cfg.Delivery != "" {
			fmt.Print("AckPolicy (None|all|explicit): ")
			scanner.Scan()
			ackPolicy := scanner.Text()
			switch strings.ToLower(ackPolicy) {
			case "none", "no", "":
				cfg.AckPolicy = api.AckNone
			case "all":
				cfg.AckPolicy = api.AckAll
			case "explicit", "exp":
				cfg.AckPolicy = api.AckExplicit
			default:
				log.Fatalf("Unknown ack policy %q", ackPolicy)
			}
		} else {
			cfg.AckPolicy = api.AckExplicit
		}

		fmt.Print("Replay Policy (Instant|original): ")
		scanner.Scan()
		replayPolicy := scanner.Text()
		switch strings.ToLower(replayPolicy) {
		case "", "instant":
			cfg.ReplayPolicy = api.ReplayInstant
		case "original":
			cfg.ReplayPolicy = api.ReplayOriginal
		default:
			log.Fatalf("Unknown replay policy %q", replayPolicy)
		}
		jreq, err := json.Marshal(req)
		if err != nil {
			log.Fatalf("Unexpected error: %v", err)
		}
		resp, _ = nc.Request(api.JetStreamCreateObservable, jreq, time.Second)
		log.Printf("Received response of %q", resp.Data)

	case "ls-obs", "list-obs":
		if len(args) == 1 {
			showUsageAndExit(1)
		}
		mset := []byte(args[1])

		var names []string
		resp, _ := nc.Request(api.JetStreamObservables, mset, time.Second)
		if err := json.Unmarshal(resp.Data, &names); err != nil {
			log.Fatalf("Unexpected error: %v", err)
		}
		log.Println()
		if len(names) == 0 {
			log.Printf("None")
		}
		for i, name := range names {
			log.Printf("%d) %s", i+1, name)
		}
		log.Println()

	case "delete-obs", "del-obs", "rm-obs":
		if len(args) < 3 {
			showUsageAndExit(1)
		}
		req := []byte(args[1] + " " + args[2])
		resp, _ := nc.Request(api.JetStreamDeleteObservable, req, time.Second)
		log.Printf("Received response of %q", resp.Data)

	case "info-obs", "inf-obs":
		if len(args) < 3 {
			showUsageAndExit(1)
		}
		req := []byte(args[1] + " " + args[2])
		resp, _ := nc.Request(api.JetStreamObservableInfo, req, time.Second)
		log.Printf("Received response of %s", resp.Data)

	case "next", "next-msg":
		if len(args) < 3 {
			showUsageAndExit(1)
		}
		// Create the pull subject. Default to 1. We can add in batching later.
		subject := api.JetStreamRequestNextPre + "." + args[1] + "." + args[2]
		resp, err := nc.Request(subject, nil, time.Second)
		if err != nil {
			log.Fatalf("Error requesting next from %q: %v", subject, err)
		}
		log.Printf("Received response of %s", resp.Data)
		log.Printf("Reply: %s", resp.Reply)
		scanner := bufio.NewScanner(os.Stdin)
		shouldAck := true
		fmt.Printf("Ack? (Y|n)")
		scanner.Scan()
		ans := scanner.Text()
		if len(ans) > 0 {
			shouldAck, _ = strconv.ParseBool(ans)
		}
		if shouldAck {
			resp.Respond(api.AckAck)
		}
	default:
		showUsageAndExit(1)
	}
}

func getMsgSetInfo(nc *nats.Conn, name string) {
	resp, err := nc.Request(api.JetStreamMsgSetInfo, []byte(name), time.Second)
	if err != nil {
		log.Fatalf("Unexpected error: %v", err)
	}
	if strings.HasPrefix(string(resp.Data), api.ErrPrefix) {
		log.Fatalf("%q", resp.Data)
	}
	var mstats api.MsgSetStats
	if err = json.Unmarshal(resp.Data, &mstats); err != nil {
		log.Fatalf("Unexpected error: %v", err)
	}
	log.Println()
	log.Printf("Messages: %s", humanize.Comma(int64(mstats.Msgs)))
	log.Printf("Bytes:    %s", humanize.Bytes(mstats.Bytes))
	log.Printf("FirstSeq: %s", humanize.Comma(int64(mstats.FirstSeq)))
	log.Printf("LastSeq:  %s", humanize.Comma(int64(mstats.LastSeq)))
	log.Println()
}

func getAccountInfo(nc *nats.Conn) {
	resp, err := nc.Request(api.JetStreamInfo, nil, time.Second)
	if err != nil {
		log.Fatalf("Unexpected error: %v", err)
	}
	var info api.JetStreamAccountStats
	if err := json.Unmarshal(resp.Data, &info); err != nil {
		log.Fatalf("Unexpected error: %v", err)
	}
	log.Println()
	log.Printf("Memory:  %s of %s", humanize.Bytes(info.Memory), humanize.Bytes(uint64(info.Limits.MaxMemory)))
	log.Printf("Storage: %s of %s", humanize.Bytes(info.Store), humanize.Bytes(uint64(info.Limits.MaxStore)))
	log.Printf("MsgSets: %d of %s", info.MsgSets, unlimitedOrFriendly(info.Limits.MaxMsgSets))
	log.Println()
}

func processStorage(storage string) api.StorageType {
	storage = strings.TrimSpace(storage)
	if strings.HasPrefix(strings.ToLower(storage), "m") {
		return api.MemoryStorage
	}
	return api.FileStorage
}

func parseDurStr(dstr string) time.Duration {
	dstr = strings.TrimSpace(dstr)
	var dur time.Duration

	if len(dstr) <= 0 {
		return dur
	}

	ls := len(dstr)
	di := ls - 1
	unit := dstr[di:]
	switch unit {
	case "d", "D":
		// Day
		val, _ := strconv.Atoi(dstr[:di])
		dur = time.Duration(val*24) * time.Hour
	case "M":
		// Month, estimate
		val, _ := strconv.Atoi(dstr[:di])
		dur = time.Duration(val*24*30) * time.Hour
	case "Y", "y":
		// Year
		val, _ := strconv.Atoi(dstr[:di])
		dur = time.Duration(val*24*365) * time.Hour
	case "s", "S", "m", "h", "H":
		dur, _ = time.ParseDuration(dstr)
	}
	return dur
}

func processLimits(limits string) (int64, int64, time.Duration) {
	limits = strings.TrimSpace(limits)
	if limits == "" {
		return -1, -1, 0
	}
	maxMsgs := -1
	maxBytes := -1
	maxAge := time.Duration(0)

	var vals []string
	if strings.ContainsAny(limits, ",") {
		vals = strings.Split(limits, ",")
	} else if strings.ContainsAny(limits, "\t") {
		vals = strings.Split(limits, "\t")
	} else {
		vals = strings.Split(limits, " ")
	}
	for i := 0; i < len(vals); i++ {
		vals[i] = strings.TrimSpace(vals[i])
	}
	switch len(vals) {
	case 1:
		maxMsgs, _ = strconv.Atoi(vals[0])
	case 2:
		maxMsgs, _ = strconv.Atoi(vals[0])
		maxBytes, _ = strconv.Atoi(vals[1])
	case 3:
		maxMsgs, _ = strconv.Atoi(vals[0])
		maxBytes, _ = strconv.Atoi(vals[1])
		maxAge = parseDurStr(vals[2])
	}
	return int64(maxMsgs), int64(maxBytes), maxAge
}

func processSubjects(subjects string) []string {
	if !strings.ContainsAny(subjects, " \t") {
		return []string{strings.TrimSpace(subjects)}
	}
	var sa []string
	// Check for commas.
	if strings.ContainsAny(subjects, ",") {
		sa = strings.Split(subjects, ",")
	} else if strings.ContainsAny(subjects, "\t") {
		sa = strings.Split(subjects, "\t")
	} else {
		sa = strings.Split(subjects, " ")
	}
	for i := 0; i < len(sa); i++ {
		sa[i] = strings.TrimSpace(sa[i])
	}
	return sa
}

func unlimitedOrFriendly(n int) string {
	if n == -1 {
		return "Unlimited"
	}
	return fmt.Sprintf("%d", n)
}
