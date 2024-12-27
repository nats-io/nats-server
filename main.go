// Copyright 2012-2022 The NATS Authors
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
	"flag"
	"fmt"
	"os"

	"github.com/nats-io/nats-server/v2/taa"
)

// Run without arguments to print the list of maps available
// Pass one argument to find a random solution to that map
func main() {

	flag.Parse()

	if len(flag.Args()) == 1 {
		// Exactly one argument: load map, create and print (random) route

		mapName := flag.Arg(0)
		citiesMap := taa.LoadMap(mapName)

		distance, route := taa.FindRandomSolution(citiesMap)

		fmt.Printf(
			"Random solution for map: %s\nDistance:%d (optimal: %d)\nRoute: %v\n",
			mapName,
			distance,
			citiesMap.OptimalSolution(),
			route,
		)

	} else {
		// Zero or multiple arguments: print usage
		fmt.Printf("Run with <map name> argument to create a random route through the selected map\n")
		fmt.Printf("List of maps:\n")
		for _, mapName := range taa.MapNames() {
			fmt.Printf(" - %s\n", mapName)
		}
		os.Exit(1)
	}
}
