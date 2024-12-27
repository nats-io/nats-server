package taa

import (
	"embed"
	"fmt"
)

// Embed all cities maps
//
//go:embed data/*.tsp
var mapsFilesFs embed.FS

var optimalSolutions = map[string]uint{
	"bayg29":   1610,
	"bays29":   2020,
	"brazil58": 25395,
	"swiss42":  1273,
}

func LoadMap(name string) CitiesMap {
	optimalSolution, found := optimalSolutions[name]
	if !found {
		panic(fmt.Sprintf("Map '%s' not found", name))
	}

	fileName := fmt.Sprintf("data/%s.tsp", name)
	fileContent, err := mapsFilesFs.ReadFile(fileName)
	if err != nil {
		panic(err)
	}
	citiesMap := NewCitiesMap(fileContent, optimalSolution)
	return citiesMap
}

func MapNames() []string {
	names := make([]string, 0, len(optimalSolutions))
	for name, _ := range optimalSolutions {
		names = append(names, name)
	}
	return names
}
