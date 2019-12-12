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
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/AlecAivazis/survey/v2"
	"github.com/dustin/go-humanize"
	api "github.com/nats-io/nats-server/v2/server"
	"gopkg.in/alecthomas/kingpin.v2"
)

type msCmd struct {
	set   string
	force bool
	json  bool
	msgID int64

	subjects      []string
	ack           bool
	storage       string
	maxMsgLimit   int64
	maxBytesLimit int64
	maxAgeLimit   string
}

func configureMSCommand(app *kingpin.Application) {
	c := &msCmd{msgID: -1}

	ms := app.Command("messageset", "Message set management").Alias("ms")

	msInfo := ms.Command("info", "Message set information").Alias("nfo").Action(c.infoAction)
	msInfo.Arg("set", "Message set to retrieve information for").StringVar(&c.set)
	msInfo.Flag("json", "Produce JSON output").Short('j').BoolVar(&c.json)

	msAdd := ms.Command("create", "Create a new message set").Alias("add").Action(c.addAction)
	msAdd.Arg("name", "Message set name").Required().StringVar(&c.set)
	msAdd.Flag("subjects", "Subjets thats belong to the Message set").Default().StringsVar(&c.subjects)
	msAdd.Flag("ack", "Acknowledge publishes").Default("true").BoolVar(&c.ack)
	msAdd.Flag("max-msgs", "Maximum amount of messages to keep").Default("0").Int64Var(&c.maxMsgLimit)
	msAdd.Flag("max-bytes", "Maximum bytes to keep").Default("0").Int64Var(&c.maxBytesLimit)
	msAdd.Flag("max-age", "Maximum age of messages to keep").Default("").StringVar(&c.maxAgeLimit)
	msAdd.Flag("storage", "Storage backend to use").EnumVar(&c.storage, "file", "f", "memory", "m")
	msAdd.Flag("json", "Produce JSON output").Short('j').BoolVar(&c.json)

	msRm := ms.Command("rm", "Removes a message set").Alias("delete").Alias("del").Action(c.rmAction)
	msRm.Arg("name", "Message set name").StringVar(&c.set)
	msRm.Flag("force", "Force removal without prompting").Short('f').BoolVar(&c.force)

	msLs := ms.Command("ls", "List all known message sets").Alias("list").Alias("l").Action(c.lsAction)
	msLs.Flag("json", "Produce JSON output").Short('j').BoolVar(&c.json)

	msPurge := ms.Command("purge", "Purge a message set witout deleting it").Action(c.purgeAction)
	msPurge.Arg("name", "Message set name").StringVar(&c.set)
	msPurge.Flag("json", "Produce JSON output").Short('j').BoolVar(&c.json)
	msPurge.Flag("force", "Force removal without prompting").Short('f').BoolVar(&c.force)

	msGet := ms.Command("get", "Retrieves a specific message from a message set").Action(c.getAction)
	msGet.Arg("name", "Message set name").StringVar(&c.set)
	msGet.Arg("id", "Message ID to retrieve").Int64Var(&c.msgID)
	msGet.Flag("json", "Produce JSON output").Short('j').BoolVar(&c.json)
}

func (c *msCmd) infoAction(_ *kingpin.ParseContext) error {
	jsm := c.connectAndAskSet()

	mstats, err := jsm.MessageSetInfo(c.set)
	kingpin.FatalIfError(err, "could not request message set info")

	if c.json {
		err = printJSON(mstats)
		kingpin.FatalIfError(err, "could not display info")
		return nil
	}

	fmt.Printf("Information for message set %s\n", c.set)
	fmt.Println()
	fmt.Println("Configuration:")
	fmt.Println()
	fmt.Printf("             Subjects: %s\n", strings.Join(mstats.Config.Subjects, ", "))
	fmt.Printf("  No Acknowledgements: %v\n", mstats.Config.NoAck)
	fmt.Printf("            Retention: %s - %s\n", mstats.Config.Storage.String(), mstats.Config.Retention.String())
	fmt.Printf("             Replicas: %d\n", mstats.Config.Replicas)
	fmt.Printf("     Maximum Messages: %d\n", mstats.Config.MaxMsgs)
	fmt.Printf("        Maximum Bytes: %d\n", mstats.Config.MaxBytes)
	fmt.Printf("          Maximum Age: %s\n", mstats.Config.MaxAge.String())
	fmt.Printf("  Maximum Observables: %d\n", mstats.Config.MaxObservables)
	fmt.Println()
	fmt.Println("Statistics:")
	fmt.Println()
	fmt.Printf("            Messages: %s\n", humanize.Comma(int64(mstats.Stats.Msgs)))
	fmt.Printf("               Bytes: %s\n", humanize.Bytes(mstats.Stats.Bytes))
	fmt.Printf("            FirstSeq: %s\n", humanize.Comma(int64(mstats.Stats.FirstSeq)))
	fmt.Printf("             LastSeq: %s\n", humanize.Comma(int64(mstats.Stats.LastSeq)))
	fmt.Printf("  Active Observables: %d\n", mstats.Stats.Observables)

	fmt.Println()

	return nil
}

func (c *msCmd) addAction(pc *kingpin.ParseContext) (err error) {
	nc, err := connect()
	kingpin.FatalIfError(err, "could not connect")

	if len(c.subjects) == 0 {
		subjects := ""
		err = survey.AskOne(&survey.Input{
			Message: "Subjects to consume",
			Help:    "Message sets consume messages from subjects, this is a space or comma separated list that can include wildcards. Settable using --subjects",
		}, &subjects)
		kingpin.FatalIfError(err, "invalid input")

		c.subjects = splitString(subjects)
	}

	// if cli gave a single string as a list we split it up, else they can pass --subject x --subject y for multiples
	matched, _ := regexp.MatchString(`,|\t|\s`, c.subjects[0])
	if len(c.subjects) == 1 && matched {
		c.subjects = splitString(c.subjects[0])
	}

	if c.storage == "" {
		err = survey.AskOne(&survey.Select{
			Message: "Storage backend",
			Options: []string{"file", "memory"},
			Help:    "Messages sets are stored on the server, this can be one of many backends and all are usable in clustering mode. Settable using --storage",
		}, &c.storage)
		kingpin.FatalIfError(err, "invalid input")
	}

	var storage api.StorageType
	switch c.storage {
	case "file", "f":
		storage = api.FileStorage
	case "memory", "m":
		storage = api.MemoryStorage
	default:
		kingpin.Fatalf("invalid storage type %s", c.storage)
	}

	if c.maxMsgLimit == 0 {
		c.maxMsgLimit, err = askOneInt("Message count limit", "-1", "Defines the amount of messages to keep in the store for this message set, when exceeded oldest messages are removed, -1 for unlimited. Settable using --max-msgs")
		kingpin.FatalIfError(err, "invalid input")
	}

	if c.maxBytesLimit == 0 {
		c.maxBytesLimit, err = askOneInt("Message size limit", "-1", "Defines the combined size of all messages in a message set, when exceeded oldest messages are removed, -1 for unlimited. Settable using --max-bytes")
		kingpin.FatalIfError(err, "invalid input")
	}

	if c.maxAgeLimit == "" {
		err = survey.AskOne(&survey.Input{
			Message: "Maximum message age limit",
			Default: "-1",
			Help:    "Defines the oldest messages that can be stored in the message set, any messages older than this period will be removed, -1 for unlimited. Supports units (s)econds, (m)inutes, (h)ours, (y)ears, (M)onths, (d)ays. Setable using --max-age",
		}, &c.maxAgeLimit)
		kingpin.FatalIfError(err, "invalid input")
	}

	var maxAge time.Duration

	if c.maxAgeLimit != "-1" {
		maxAge, err = parseDurationString(c.maxAgeLimit)
		kingpin.FatalIfError(err, "invalid maximum age limit format")
	}

	err = NewJSM(nc, timeout).MessageSetCreate(&api.MsgSetConfig{
		Name:     c.set,
		Subjects: c.subjects,
		MaxMsgs:  c.maxMsgLimit,
		MaxBytes: c.maxBytesLimit,
		MaxAge:   maxAge,
		Storage:  storage,
		NoAck:    !c.ack,
	})
	kingpin.FatalIfError(err, "could not create message set")

	fmt.Printf("Message set %s was created\n\n", c.set)

	return c.infoAction(pc)
}

func (c *msCmd) rmAction(_ *kingpin.ParseContext) (err error) {
	jsm := c.connectAndAskSet()

	if !c.force {
		ok, err := askConfirmation(fmt.Sprintf("Really delete message set %s", c.set), false)
		kingpin.FatalIfError(err, "could not obtain confirmation")

		if !ok {
			return nil
		}
	}

	err = jsm.MessageSetDelete(c.set)
	kingpin.FatalIfError(err, "could not remove message set")

	return nil
}

func (c *msCmd) purgeAction(pc *kingpin.ParseContext) (err error) {
	jsm := c.connectAndAskSet()

	if !c.force {
		ok, err := askConfirmation(fmt.Sprintf("Really purge message set %s", c.set), false)
		kingpin.FatalIfError(err, "could not obtain confirmation")

		if !ok {
			return nil
		}
	}

	err = jsm.MessageSetPurge(c.set)
	kingpin.FatalIfError(err, "could not purge message set")

	return c.infoAction(pc)
}

func (c *msCmd) lsAction(_ *kingpin.ParseContext) (err error) {
	nc, err := connect()
	kingpin.FatalIfError(err, "could not connect")

	sets, err := NewJSM(nc, timeout).MessageSets()
	kingpin.FatalIfError(err, "could not list message set")

	if c.json {
		err = printJSON(sets)
		kingpin.FatalIfError(err, "could not display sets")
		return nil
	}

	if len(sets) == 0 {
		fmt.Println("No message sets defined")
		return nil
	}

	fmt.Println("Message Sets:")
	fmt.Println()
	for _, s := range sets {
		fmt.Printf("\t%s\n", s)
	}
	fmt.Println()

	return nil
}

func (c *msCmd) getAction(_ *kingpin.ParseContext) (err error) {
	jsm := c.connectAndAskSet()

	if c.msgID == -1 {
		id := ""
		err = survey.AskOne(&survey.Input{
			Message: "Message ID to retrieve"}, &id)
		kingpin.FatalIfError(err, "invalid input")

		idint, err := strconv.Atoi(id)
		kingpin.FatalIfError(err, "invalid number")

		c.msgID = int64(idint)
	}

	item, err := jsm.MessageSetGetItem(c.set, c.msgID)
	kingpin.FatalIfError(err, "could not retrieve %s#%d", c.set, c.msgID)

	if c.json {
		printJSON(item)
		return nil
	}

	fmt.Printf("Item: %s#%d received %v on subject %s\n\n", c.set, c.msgID, item.Time, item.Subject)
	fmt.Println(string(item.Data))
	fmt.Println()
	return nil
}

func (c *msCmd) connectAndAskSet() (jsm *JetStreamMgmt) {
	nc, err := connect()
	kingpin.FatalIfError(err, "could not connect")

	jsm = NewJSM(nc, timeout)

	c.set, err = selectMessageSet(jsm, c.set)
	kingpin.FatalIfError(err, "could not pick a message set")

	return jsm
}
