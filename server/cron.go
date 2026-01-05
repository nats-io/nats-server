// Copyright 2025 The NATS Authors
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
//
// Based on code from https://github.com/robfig/cron
// Copyright (C) 2012 Rob Figueiredo
// All Rights Reserved.
//
// MIT LICENSE
//
// Permission is hereby granted, free of charge, to any person obtaining a copy of
// this software and associated documentation files (the "Software"), to deal in
// the Software without restriction, including without limitation the rights to
// use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
// the Software, and to permit persons to whom the Software is furnished to do so,
// subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
// FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
// COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
// IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
// CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

package server

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

// parseCron parses the given cron pattern and returns the next time it will fire based on the provided ts.
func parseCron(pattern string, tz string, ts int64) (time.Time, error) {
	fields := strings.Fields(pattern)
	if len(fields) != 6 {
		return time.Time{}, fmt.Errorf("pattern requires 6 fields, got %d", len(fields))
	}

	// Load the time zone.
	loc, err := time.LoadLocation(tz)
	if err != nil {
		return time.Time{}, err
	}

	// Parse each field.
	var second, minute, hour, dayOfMonth, month, dayOfWeek uint64
	if second, err = getField(fields[0], seconds); err != nil {
		return time.Time{}, err
	}
	if minute, err = getField(fields[1], minutes); err != nil {
		return time.Time{}, err
	}
	if hour, err = getField(fields[2], hours); err != nil {
		return time.Time{}, err
	}
	if dayOfMonth, err = getField(fields[3], dom); err != nil {
		return time.Time{}, err
	}
	if month, err = getField(fields[4], months); err != nil {
		return time.Time{}, err
	}
	if dayOfWeek, err = getField(fields[5], dow); err != nil {
		return time.Time{}, err
	}

	// General approach
	//
	// For Month, Day, Hour, Minute, Second:
	// Check if the time value matches. If yes, continue to the next field.
	// If the field doesn't match the schedule, then increment the field until it matches.
	// While incrementing the field, a wrap-around brings it back to the beginning
	// of the field list (since it is necessary to re-verify previous field values)
	next := time.Unix(0, ts).In(loc).Round(time.Second)

	// Start at the earliest possible time (the upcoming second).
	next = next.Add(time.Second - time.Duration(next.Nanosecond())*time.Nanosecond)

	// This flag indicates whether a field has been truncated at one point.
	truncated := false

	// If no time is found within five years, return error.
	yearLimit := next.Year() + 5

WRAP:
	if next.Year() > yearLimit {
		return time.Time{}, errors.New("pattern exceeds maximum range")
	}
	for 1<<uint(next.Month())&month == 0 {
		if !truncated {
			truncated = true
			next = time.Date(next.Year(), next.Month(), 1, 0, 0, 0, 0, loc)
		}
		if next = next.AddDate(0, 1, 0); next.Month() == time.January {
			goto WRAP
		}
	}
	for !dayMatches(dayOfMonth, dayOfWeek, next) {
		if !truncated {
			truncated = true
			next = time.Date(next.Year(), next.Month(), next.Day(), 0, 0, 0, 0, loc)
		}
		if next = next.AddDate(0, 0, 1); next.Day() == 1 {
			goto WRAP
		}
	}
	for 1<<uint(next.Hour())&hour == 0 {
		if !truncated {
			truncated = true
			next = time.Date(next.Year(), next.Month(), next.Day(), next.Hour(), 0, 0, 0, loc)
		}
		if next = next.Add(time.Hour); next.Hour() == 0 {
			goto WRAP
		}
	}
	for 1<<uint(next.Minute())&minute == 0 {
		if !truncated {
			truncated = true
			next = next.Truncate(time.Minute)
		}
		if next = next.Add(time.Minute); next.Minute() == 0 {
			goto WRAP
		}
	}
	for 1<<uint(next.Second())&second == 0 {
		if !truncated {
			truncated = true
			next = next.Truncate(time.Second)
		}
		if next = next.Add(time.Second); next.Second() == 0 {
			goto WRAP
		}
	}
	return next, nil
}

// getField returns an Int with the bits set representing all of the times that
// the field represents or error parsing field value.  A "field" is a comma-separated
// list of "ranges".
func getField(field string, r bounds) (uint64, error) {
	var bits uint64
	ranges := strings.FieldsFuncSeq(field, func(r rune) bool { return r == ',' })
	for expr := range ranges {
		bit, err := getRange(expr, r)
		if err != nil {
			return bits, err
		}
		bits |= bit
	}
	return bits, nil
}

// getRange returns the bits indicated by the given expression: number | number [ "-" number ] [ "/" number ]
// or error parsing range.
func getRange(expr string, r bounds) (uint64, error) {
	var (
		start, end, step uint
		rangeAndStep     = strings.Split(expr, "/")
		lowAndHigh       = strings.Split(rangeAndStep[0], "-")
		singleDigit      = len(lowAndHigh) == 1
		err              error
	)

	var extra uint64
	if lowAndHigh[0] == "*" || lowAndHigh[0] == "?" {
		start = r.min
		end = r.max
		extra = starBit
	} else {
		start, err = parseIntOrName(lowAndHigh[0], r.names)
		if err != nil {
			return 0, err
		}
		switch len(lowAndHigh) {
		case 1:
			end = start
		case 2:
			end, err = parseIntOrName(lowAndHigh[1], r.names)
			if err != nil {
				return 0, err
			}
		default:
			return 0, fmt.Errorf("too many hyphens: %s", expr)
		}
	}

	switch len(rangeAndStep) {
	case 1:
		step = 1
	case 2:
		step, err = mustParseInt(rangeAndStep[1])
		if err != nil {
			return 0, err
		}
		// Special handling: "N/step" means "N-max/step".
		if singleDigit {
			end = r.max
		}
		if step > 1 {
			extra = 0
		}
	default:
		return 0, fmt.Errorf("too many slashes: %s", expr)
	}

	if start < r.min {
		return 0, fmt.Errorf("beginning of range (%d) below minimum (%d): %s", start, r.min, expr)
	}
	if end > r.max {
		return 0, fmt.Errorf("end of range (%d) above maximum (%d): %s", end, r.max, expr)
	}
	if start > end {
		return 0, fmt.Errorf("beginning of range (%d) beyond end of range (%d): %s", start, end, expr)
	}
	if step == 0 {
		return 0, fmt.Errorf("step of range should be a positive number: %s", expr)
	}
	return getBits(start, end, step) | extra, nil
}

// parseIntOrName returns the (possibly-named) integer contained in expr.
func parseIntOrName(expr string, names map[string]uint) (uint, error) {
	if names != nil {
		if namedInt, ok := names[strings.ToLower(expr)]; ok {
			return namedInt, nil
		}
	}
	return mustParseInt(expr)
}

// mustParseInt parses the given expression as an int or returns an error.
func mustParseInt(expr string) (uint, error) {
	num, err := strconv.Atoi(expr)
	if err != nil {
		return 0, fmt.Errorf("failed to parse int from %s: %s", expr, err)
	}
	if num < 0 {
		return 0, fmt.Errorf("negative number (%d) not allowed: %s", num, expr)
	}
	return uint(num), nil
}

// getBits sets all bits in the range [min, max], modulo the given step size.
func getBits(min, max, step uint) uint64 {
	var bits uint64

	// If step is 1, use shifts.
	if step == 1 {
		return ^(math.MaxUint64 << (max + 1)) & (math.MaxUint64 << min)
	}

	// Else, use a simple loop.
	for i := min; i <= max; i += step {
		bits |= 1 << i
	}
	return bits
}

// bounds provides a range of acceptable values (plus a map of name to value).
type bounds struct {
	min, max uint
	names    map[string]uint
}

// The bounds for each field.
var (
	seconds = bounds{0, 59, nil}
	minutes = bounds{0, 59, nil}
	hours   = bounds{0, 23, nil}
	dom     = bounds{1, 31, nil}
	months  = bounds{1, 12, map[string]uint{
		"jan": 1,
		"feb": 2,
		"mar": 3,
		"apr": 4,
		"may": 5,
		"jun": 6,
		"jul": 7,
		"aug": 8,
		"sep": 9,
		"oct": 10,
		"nov": 11,
		"dec": 12,
	}}
	dow = bounds{0, 6, map[string]uint{
		"sun": 0,
		"mon": 1,
		"tue": 2,
		"wed": 3,
		"thu": 4,
		"fri": 5,
		"sat": 6,
	}}
)

const (
	// Set the top bit if a star was included in the expression.
	starBit = 1 << 63
)

// dayMatches returns true if the schedule's day-of-week and day-of-month
// restrictions are satisfied by the given time.
func dayMatches(dayOfMonth, dayOfWeek uint64, t time.Time) bool {
	var (
		domMatch = 1<<uint(t.Day())&dayOfMonth > 0
		dowMatch = 1<<uint(t.Weekday())&dayOfWeek > 0
	)
	if dayOfMonth&starBit > 0 || dayOfWeek&starBit > 0 {
		return domMatch && dowMatch
	}
	return domMatch || dowMatch
}
