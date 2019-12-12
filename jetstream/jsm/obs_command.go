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
	"time"

	"github.com/AlecAivazis/survey/v2"
	api "github.com/nats-io/nats-server/v2/server"
	"gopkg.in/alecthomas/kingpin.v2"
)

type obsCmd struct {
	obs        string
	messageSet string
	json       bool
	force      bool
	ack        bool
	raw        bool

	pull        bool
	replyPolicy string
	startPolicy string
	ackPolicy   string

	cfg *api.ObservableConfig
}

func configureObsCommand(app *kingpin.Application) {
	c := &obsCmd{
		cfg: &api.ObservableConfig{
			AckPolicy: api.AckExplicit,
		},
	}

	obs := app.Command("observable", "Observable management").Alias("obs")

	obsInfo := obs.Command("info", "Observable information").Alias("nfo").Action(c.infoAction)
	obsInfo.Arg("messageset", "Message set name").StringVar(&c.messageSet)
	obsInfo.Arg("obs", "Observable name").StringVar(&c.obs)
	obsInfo.Flag("json", "Produce JSON output").Short('j').BoolVar(&c.json)

	obsLs := obs.Command("ls", "List known observables").Alias("list").Action(c.lsAction)
	obsLs.Arg("messageset", "Message set name").StringVar(&c.messageSet)
	obsLs.Flag("json", "Produce JSON output").Short('j').BoolVar(&c.json)

	obsRm := obs.Command("rm", "Removes an observables").Alias("delete").Alias("del").Action(c.rmAction)
	obsRm.Arg("messageset", "Message set name").StringVar(&c.messageSet)
	obsRm.Arg("name", "Observable name").StringVar(&c.obs)
	obsRm.Flag("force", "Force removal without prompting").Short('f').BoolVar(&c.force)

	obsAdd := obs.Command("add", "Creates a new observable").Alias("create").Action(c.createAction)
	obsAdd.Arg("messageset", "Message set name").StringVar(&c.messageSet)
	obsAdd.Arg("name", "Observable name").StringVar(&c.cfg.Durable)
	obsAdd.Flag("target", "Push based delivery target subject").StringVar(&c.cfg.Delivery)
	obsAdd.Flag("subject", "Message set topic").StringVar(&c.cfg.Subject)
	obsAdd.Flag("replay", "Replay Policy (instant, original)").EnumVar(&c.replyPolicy, "instant", "original")
	obsAdd.Flag("deliver", "Start policy (all, last, 1h, msg sequence)").StringVar(&c.startPolicy)
	obsAdd.Flag("ack", "Acknowledgement policy (none, all, explicit)").StringVar(&c.ackPolicy)
	obsAdd.Flag("pull", "Deliver messages in 'pull' mode").BoolVar(&c.pull)

	obsNext := obs.Command("next", "Retrieves the next message from a push message set").Action(c.nextAction)
	obsNext.Arg("messageset", "Message set name").StringVar(&c.messageSet)
	obsNext.Arg("obs", "Observable name").StringVar(&c.obs)
	obsNext.Flag("ack", "Acknowledge received message").Default("true").BoolVar(&c.ack)
	obsNext.Flag("raw", "Show only the message").Short('r').BoolVar(&c.raw)

}

func (c *obsCmd) rmAction(_ *kingpin.ParseContext) error {
	jsm := c.connectAndSetup(true, true)

	if !c.force {
		ok, err := askConfirmation(fmt.Sprintf("Really delete observable %s#%s", c.messageSet, c.obs), false)
		kingpin.FatalIfError(err, "could not obtain confirmation")

		if !ok {
			return nil
		}
	}

	return jsm.ObservableDelete(c.messageSet, c.obs)
}

func (c *obsCmd) lsAction(pc *kingpin.ParseContext) error {
	jsm := c.connectAndSetup(true, false)

	obs, err := jsm.Observables(c.messageSet)
	kingpin.FatalIfError(err, "could not load observables")

	if c.json {
		err = printJSON(obs)
		kingpin.FatalIfError(err, "could not display observables")
		return nil
	}

	if len(obs) == 0 {
		fmt.Println("No observables defined")
		return nil
	}

	fmt.Printf("Observables for message set %s:\n", c.messageSet)
	fmt.Println()
	for _, s := range obs {
		fmt.Printf("\t%s\n", s)
	}
	fmt.Println()

	return nil
}

func (c *obsCmd) infoAction(pc *kingpin.ParseContext) error {
	jsm := c.connectAndSetup(true, true)

	info, err := jsm.ObservableInfo(c.messageSet, c.obs)
	kingpin.FatalIfError(err, "could not load observable %s#%s", c.messageSet, c.obs)

	if c.json {
		printJSON(info)
		return nil
	}

	fmt.Printf("Information for observable %s#%s\n", c.messageSet, c.obs)
	fmt.Println()
	fmt.Println("Configuration:")
	fmt.Println()
	if info.Config.Durable != "" {
		fmt.Printf("      Durable Name: %s\n", info.Config.Durable)
	}
	if info.Config.Delivery != "" {
		fmt.Printf("  Delivery Subject: %s\n", info.Config.Delivery)
	} else {
		fmt.Printf("         Pull Mode: true\n")
	}
	if info.Config.Subject != "" {
		fmt.Printf("           Subject: %s\n", info.Config.Subject)
	}
	if info.Config.MsgSetSeq != 0 {
		fmt.Printf("    Start Sequence: %d\n", info.Config.MsgSetSeq)
	}
	if !info.Config.StartTime.IsZero() {
		fmt.Printf("        Start Time: %v\n", info.Config.StartTime)
	}
	fmt.Printf("       Deliver All: %v\n", info.Config.DeliverAll)
	fmt.Printf("      Deliver Last: %v\n", info.Config.DeliverLast)
	fmt.Printf("        Ack Policy: %s\n", info.Config.AckPolicy.String())
	if info.Config.AckPolicy != api.AckNone {
		fmt.Printf("          Ack Wait: %v\n", info.Config.AckWait)
	}
	fmt.Printf("     Replay Policy: %s\n", info.Config.ReplayPolicy.String())
	fmt.Println()

	fmt.Println("State:")
	fmt.Println()
	fmt.Printf("  Last Delivered Message: Observable sequence: %d Message set sequence: %d\n", info.State.Delivered.ObsSeq, info.State.Delivered.SetSeq)
	fmt.Printf("    Acknowledgment floor: Observable sequence: %d Message set sequence: %d\n", info.State.AckFloor.ObsSeq, info.State.AckFloor.SetSeq)
	fmt.Printf("        Pending Messages: %d\n", len(info.State.Pending))
	fmt.Printf("    Redelivered Messages: %d\n", len(info.State.Redelivery))
	fmt.Println()

	return nil
}

func (c *obsCmd) createAction(pc *kingpin.ParseContext) (err error) {
	jsm := c.connectAndSetup(true, false)

	c.cfg.AckPolicy = api.AckExplicit

	if c.cfg.Durable == "" {
		err = survey.AskOne(&survey.Input{
			Message: "Observable name",
			Help:    "This will be used for the name of the durable subscription to be used when referencing this observable later. Settable using 'name' CLI argument",
		}, &c.cfg.Durable)
		kingpin.FatalIfError(err, "could not request observable name")
		c.obs = c.cfg.Durable
	}

	if ok, _ := regexp.MatchString(`\.|\*|>`, c.cfg.Durable); ok {
		kingpin.Fatalf("durable name can not contain '.', '*', '>'")
	}

	if !c.pull && c.cfg.Delivery == "" {
		err = survey.AskOne(&survey.Input{
			Message: "Delivery target",
			Help:    "Observables can be in 'push' or 'pull' mode, in 'push' mode messages are dispatched in real time to a target NATS subject, this is that subject. Leaving this blank creates a 'pull' mode observable. Settable using --target and --pull",
		}, &c.cfg.Delivery)
		kingpin.FatalIfError(err, "could not request delivery target")
	}

	if c.startPolicy == "" {
		err = survey.AskOne(&survey.Input{
			Message: "Start policy (all, last, 1h, msg sequence)",
			Help:    "This controls how the observable starts out, does it make all messages available, only the latest, ones after a certain time or time sequence. Settable using --deliver",
		}, &c.startPolicy)
		kingpin.FatalIfError(err, "could not request start policy")
	}

	if c.startPolicy == "all" {
		c.cfg.DeliverAll = true
	} else if c.startPolicy == "last" {
		c.cfg.DeliverLast = true
	} else if ok, _ := regexp.MatchString("^\\d+$", c.startPolicy); ok {
		seq, _ := strconv.Atoi(c.startPolicy)
		c.cfg.MsgSetSeq = uint64(seq)
	} else {
		d, err := parseDurationString(c.startPolicy)
		kingpin.FatalIfError(err, "could not parse starting delta")
		c.cfg.StartTime = time.Now().Add(-d)
	}

	if c.cfg.Delivery != "" {
		if c.ackPolicy == "" {
			err = survey.AskOne(&survey.Select{
				Message: "Acknowledgement policy",
				Options: []string{"none", "all", "explicit"},
				Default: "none",
				Help:    "Messages that are not acknowledged will be redelivered at a later time. 'none' means no acknowledgement is needed only 1 delivery ever, 'all' means acknowledging message 10 will also acknowledge 0-9 and 'explicit' means each has to be acknowledged specifically. Settable using --ack",
			}, &c.ackPolicy)
			kingpin.FatalIfError(err, "could not ask acknowledgement policy")
		}

		switch c.ackPolicy {
		case "none":
			c.cfg.AckPolicy = api.AckNone
		case "all":
			c.cfg.AckPolicy = api.AckAll
		case "explicit":
			c.cfg.AckPolicy = api.AckExplicit
		}
	}

	if c.cfg.Delivery != "" {
		if c.replyPolicy == "" {
			mode := ""
			err = survey.AskOne(&survey.Select{
				Message: "Replay policy",
				Options: []string{"instant", "original"},
				Default: "instant",
				Help:    "Replay policy is the time interval at which messages are delivered to interested parties. 'instant' means deliver all as soon as possible while 'original' will match the time intervals in which messages were received, useful for replaying production traffic in development. Settable using --replay",
			}, &mode)
			kingpin.FatalIfError(err, "could not ask replay policy")

			c.cfg.ReplayPolicy = api.ReplayInstant
			if mode == "original" {
				c.cfg.ReplayPolicy = api.ReplayOriginal
			}
		}
	}

	err = jsm.ObservableCreate(c.messageSet, c.cfg)
	kingpin.FatalIfError(err, "observable creation failed: ")

	c.obs = c.cfg.Durable

	return c.infoAction(pc)
}

func (c *obsCmd) nextAction(pc *kingpin.ParseContext) error {
	jsm := c.connectAndSetup(true, true)

	msg, err := jsm.ObservableNext(c.messageSet, c.obs)
	kingpin.FatalIfError(err, "could not load next message")

	if !c.raw {
		fmt.Printf("--- received on %s\n", msg.Subject)
		fmt.Println(string(msg.Data))
	} else {
		fmt.Println(string(msg.Data))
	}

	if c.ack {
		err = msg.Respond(api.AckAck)
		kingpin.FatalIfError(err, "could not Acknowledge message")
		if !c.raw {
			fmt.Println("\nAcknowledged message")
		}
	}

	return nil
}

func (c *obsCmd) connectAndSetup(askSet bool, askObs bool) (jsm *JetStreamMgmt) {
	nc, err := connect()
	kingpin.FatalIfError(err, "could not connect")

	jsm = NewJSM(nc, timeout)

	if askSet {
		c.messageSet, err = selectMessageSet(jsm, c.messageSet)
		kingpin.FatalIfError(err, "could not select message set")

		if askObs {
			c.obs, err = selectObservable(jsm, c.messageSet, c.obs)
			kingpin.FatalIfError(err, "could not select observable")
		}
	}

	return jsm
}
