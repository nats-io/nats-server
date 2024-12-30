package taa

import (
	"fmt"
	"slices"
	"sort"
	"testing"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/antithesishq/antithesis-sdk-go/lifecycle"
	"github.com/antithesishq/antithesis-sdk-go/random"
)

func TestValidRandomRoute(t *testing.T) {

	for _, mapName := range MapNames() {
		t.Run(mapName, func(t *testing.T) {

			m := LoadMap(mapName)
			distance, route := FindRandomSolution(m)

			t.Logf(
				"Random solution for map: %s\nDistance:%d (optimal: %d)\nRoute: %v",
				mapName,
				distance,
				m.OptimalSolution(),
				route,
			)

			cityNamesSorted := m.CityNames()[:]
			sort.Strings(cityNamesSorted)

			routeSorted := route[:]
			sort.Strings(routeSorted)

			if !slices.Equal(routeSorted, cityNamesSorted) {
				t.Fatalf("Route %v != cities %v", routeSorted, cityNamesSorted)
			}
		})

	}
}

func TestSomeDistances(t *testing.T) {
	testCases := []struct {
		mapName  string
		from     string
		to       string
		distance uint
	}{
		{"bayg29", "C_1", "C_2", 97},
		{"bayg29", "C_1", "C_3", 205},
		{"bayg29", "C_1", "C_28", 34},
		{"bayg29", "C_1", "C_29", 145},
		{"bayg29", "C_28", "C_29", 162},
		{"bayg29", "C_20", "C_21", 79},
		{"bayg29", "C_20", "C_28", 153},

		{"bays29", "C_15", "C_16", 122},
		{"bays29", "C_28", "C_29", 199},
		{"bays29", "C_1", "C_29", 167},

		{"brazil58", "C_1", "C_2", 2635},
		{"brazil58", "C_1", "C_57", 1417},
		{"brazil58", "C_1", "C_58", 739},
		{"brazil58", "C_57", "C_58", 962},

		{"swiss42", "C_15", "C_16", 11},
		{"swiss42", "C_28", "C_29", 18},
		{"swiss42", "C_1", "C_42", 124},
	}

	for _, testCase := range testCases {
		t.Run(
			fmt.Sprintf("%s: %s to %s", testCase.mapName, testCase.from, testCase.to),
			func(t *testing.T) {
				m := LoadMap(testCase.mapName)
				d := m.Distance(testCase.from, testCase.to)
				if d != testCase.distance {
					t.Fatalf("Expected distance: %d, actual: %d", testCase.distance, d)
				}

				// All maps are symmetric, check the other direction too
				d = m.Distance(testCase.to, testCase.from)
				if d != testCase.distance {
					t.Fatalf("Expected distance (reverse): %d, actual: %d", testCase.distance, d)
				}

				// While at it, check that distance to each city to self is zero
				d = m.Distance(testCase.from, testCase.from)
				if d != 0 {
					t.Fatalf("Expected 0 from %s to itself, actual: %d", testCase.from, d)
				}
				d = m.Distance(testCase.to, testCase.to)
				if d != 0 {
					t.Fatalf("Expected 0 from %s to itself, actual: %d", testCase.to, d)
				}
			},
		)
	}
}

// This explores all maps
func TestAntithesisRandomMap(t *testing.T) {

	lifecycle.SetupComplete("Test begins here")

	mapName := random.RandomChoice(MapNames())
	testAntithesisFindRoute(t, mapName)

}

// This is a convenience to only run a single map using the appropriate test filter, i.e. 'TestAntithesisAllMaps/brazil58'
func TestAntithesisAllMaps(t *testing.T) {

	lifecycle.SetupComplete("Test begins here")

	for _, mapName := range MapNames() {
		t.Run(mapName, func(t *testing.T) {
			testAntithesisFindRoute(t, mapName)
		})
	}
}

func testAntithesisFindRoute(t *testing.T, mapName string) {
	t.Helper()

	citiesMap := LoadMap(mapName)

	optimalSolution := citiesMap.OptimalSolution()
	distance, route := FindRandomSolution(citiesMap)

	// This is an actual bug
	if distance < optimalSolution {
		assert.Unreachable("Solution better than optimal!", map[string]any{
			"map":     mapName,
			"optimal": optimalSolution,
			"found":   distance,
			"route":   fmt.Sprintf("%v", route),
		})
	}

	if distance == citiesMap.OptimalSolution() {
		assert.Unreachable(mapName+" - Found optimal solution", map[string]any{
			"route": fmt.Sprintf("%v", route),
		})
	} else if distance <= optimalSolution+uint(float64(optimalSolution)*0.01) {
		assert.Unreachable(mapName+" - Found solution within 1% of the optimal", map[string]any{
			"map":     mapName,
			"optimal": optimalSolution,
			"found":   distance,
			"route":   fmt.Sprintf("%v", route),
		})
	} else if distance <= optimalSolution+uint(float64(optimalSolution)*0.05) {
		assert.Unreachable(mapName+" - Found solution within 5% of the optimal", map[string]any{
			"map":     mapName,
			"optimal": optimalSolution,
			"found":   distance,
			"route":   fmt.Sprintf("%v", route),
		})
	} else if distance <= optimalSolution+uint(float64(optimalSolution)*0.10) {
		assert.Unreachable(mapName+" - Found solution within 10% of the optimal", map[string]any{
			"map":     mapName,
			"optimal": optimalSolution,
			"found":   distance,
			"route":   fmt.Sprintf("%v", route),
		})
	} else if distance <= optimalSolution+uint(float64(optimalSolution)*0.25) {
		assert.Unreachable(mapName+" - Found solution within 25% of the optimal", map[string]any{
			"map":     mapName,
			"optimal": optimalSolution,
			"found":   distance,
			"route":   fmt.Sprintf("%v", route),
		})
	} else if distance < 2*optimalSolution {
		assert.Unreachable(mapName+" - Found solution less than 2x the optimal", map[string]any{
			"map":     mapName,
			"optimal": optimalSolution,
			"found":   distance,
			"route":   fmt.Sprintf("%v", route),
		})
	} else if distance < 10*optimalSolution {
		assert.Unreachable(mapName+" - Found solution less than 10x the optimal", map[string]any{
			"map":     mapName,
			"optimal": optimalSolution,
			"found":   distance,
			"route":   fmt.Sprintf("%v", route),
		})
	}
}
