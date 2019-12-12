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
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/AlecAivazis/survey/v2"
	"github.com/nats-io/nats.go"
)

func selectObservable(jsm *JetStreamMgmt, set string, obs string) (string, error) {
	if obs != "" {
		known, err := jsm.IsObservableKnown(set, obs)
		if err != nil {
			return "", err
		}

		if known {
			return obs, nil
		}
	}

	observables, err := jsm.Observables(set)
	if err != nil {
		return "", err
	}

	switch len(observables) {
	case 0:
		return "", fmt.Errorf("no observables are defined for message set %s", set)
	default:
		observable := ""

		err = survey.AskOne(&survey.Select{
			Message: "Select an observable",
			Options: observables,
		}, &observable)
		if err != nil {
			return "", err
		}

		return observable, nil
	}
}

func selectMessageSet(jsm *JetStreamMgmt, set string) (string, error) {
	if set != "" {
		known, err := jsm.IsMessageSetKnown(set)
		if err != nil {
			return "", err
		}

		if known {
			return set, nil
		}
	}

	sets, err := jsm.MessageSets()
	if err != nil {
		return "", err
	}

	switch len(sets) {
	case 0:
		return "", errors.New("no message sets are defined")
	default:
		set := ""

		err = survey.AskOne(&survey.Select{
			Message: "Select a message set",
			Options: sets,
		}, &set)
		if err != nil {
			return "", err
		}

		return set, nil
	}
}

func printJSON(d interface{}) error {
	j, err := json.MarshalIndent(d, "", "  ")
	if err != nil {
		return err
	}

	fmt.Println(string(j))

	return nil
}
func parseDurationString(dstr string) (dur time.Duration, err error) {
	dstr = strings.TrimSpace(dstr)

	if len(dstr) <= 0 {
		return dur, nil
	}

	ls := len(dstr)
	di := ls - 1
	unit := dstr[di:]

	switch unit {
	case "d", "D":
		val, err := strconv.ParseFloat(dstr[:di], 32)
		if err != nil {
			return dur, err
		}

		dur = time.Duration(val*24) * time.Hour
	case "M":
		val, err := strconv.ParseFloat(dstr[:di], 32)
		if err != nil {
			return dur, err
		}

		dur = time.Duration(val*24*30) * time.Hour
	case "Y", "y":
		val, err := strconv.ParseFloat(dstr[:di], 32)
		if err != nil {
			return dur, err
		}

		dur = time.Duration(val*24*365) * time.Hour
	case "s", "S", "m", "h", "H":
		dur, err = time.ParseDuration(dstr)
		if err != nil {
			return dur, err
		}

	default:
		return dur, fmt.Errorf("invalid time unit %s", unit)
	}

	return dur, nil
}

func askConfirmation(prompt string, dflt bool) (bool, error) {
	ans := dflt

	err := survey.AskOne(&survey.Confirm{
		Message: prompt,
		Default: dflt,
	}, &ans)

	return ans, err
}

func askOneInt(prompt string, dflt string, help string) (int64, error) {
	val := ""
	err := survey.AskOne(&survey.Input{
		Message: prompt,
		Default: dflt,
		Help:    help,
	}, &val)
	if err != nil {
		return 0, err
	}

	i, err := strconv.Atoi(val)
	if err != nil {
		return 0, err
	}

	return int64(i), nil
}

func splitString(s string) []string {
	return strings.FieldsFunc(s, func(c rune) bool {
		if unicode.IsSpace(c) {
			return true
		}

		if c == ',' {
			return true
		}

		return false
	})
}

func connect() (*nats.Conn, error) {
	opts := []nats.Option{nats.Name("JetStream Management CLI"), nats.NoReconnect()}

	if creds != "" {
		opts = append(opts, nats.UserCredentials(creds))
	}

	if tlsCert != "" && tlsKey != "" {
		opts = append(opts, nats.ClientCert(tlsCert, tlsKey))
	}

	if tlsCA != "" {
		opts = append(opts, nats.RootCAs(tlsCA))
	}

	return nats.Connect(servers, opts...)
}
