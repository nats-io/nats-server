package taa

import (
	"bufio"
	"bytes"
	"fmt"
	"slices"

	"github.com/antithesishq/antithesis-sdk-go/random"
)

type CitiesMap struct {
	distances       map[string]map[string]uint
	cityNames       []string
	optimalSolution uint
}

func (m CitiesMap) NumCities() int {
	return len(m.cityNames)
}

func (m CitiesMap) CityNames() []string {
	return m.cityNames
}

func (m CitiesMap) Distance(a, b string) uint {
	cityA, ok := m.distances[a]
	if !ok {
		panic(fmt.Sprintf("City not found: %s", a))
	}
	distance, ok := cityA[b]
	if !ok {
		panic(fmt.Sprintf("Distance not found: %s > %s", a, b))
	}
	return distance
}

func (m CitiesMap) OptimalSolution() uint {
	return m.optimalSolution
}

func FindRandomSolution(c CitiesMap) (uint, []string) {

	distanceTraveled := uint(0)
	route := make([]string, 0, c.NumCities())
	toVisit := make([]string, 0, c.NumCities())

	// Initialize toVisit with list of all cities
	toVisit = append(toVisit, c.CityNames()...)

	var previousCity string

	// Iterate until all cities are visited
	for len(toVisit) > 0 {

		// Select a random city
		nextCity := random.RandomChoice(toVisit)

		// Remove city from the list to be visited
		toVisit = slices.DeleteFunc(toVisit, func(s string) bool { return s == nextCity })

		// Add city to route
		route = append(route, nextCity)

		// Add distance (unless it's the first city visited)
		if previousCity != "" {
			distanceTraveled += c.Distance(previousCity, nextCity)
		}

		previousCity = nextCity
	}

	return distanceTraveled, route
}

func NewCitiesMap(rawData []byte, optimalSolution uint) CitiesMap {
	fmt.Printf("Loading map data (%dB)...\n", len(rawData))

	scanner := bufio.NewScanner(bytes.NewReader(rawData))

	for scanner.Scan() {
		line := scanner.Text()
		
	}

	return CitiesMap{
		distances:       make(map[string]map[string]uint),
		optimalSolution: optimalSolution,
		cityNames:       []string{},
	}
}
