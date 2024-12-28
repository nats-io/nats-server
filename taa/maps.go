package taa

import (
	"bufio"
	"embed"
	"fmt"
	"io"
	"io/fs"
	"strconv"
	"strings"
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
	file, err := mapsFilesFs.Open(fileName)
	if err != nil {
		panic(err)
	}
	defer func(file fs.File) {
		err := file.Close()
		if err != nil {
			panic(err)
		}
	}(file)
	citiesMap := NewCitiesMap(name, file, optimalSolution)
	return citiesMap
}

func MapNames() []string {
	names := make([]string, 0, len(optimalSolutions))
	for name, _ := range optimalSolutions {
		names = append(names, name)
	}
	return names
}

func NewCitiesMap(mapName string, reader io.Reader, optimalSolution uint) CitiesMap {
	fmt.Printf("Loading map %s...\n", mapName)

	scanner := bufio.NewScanner(reader)

	var numCities int
	var matrixType string

	for scanner.Scan() {
		header := scanner.Text()
		_, err := fmt.Sscanf(header, "%d %s", &numCities, &matrixType)
		if err != nil {
			panic(fmt.Errorf("failed to parse header of file %s: %s", mapName, err))
		}
		break
	}

	fmt.Printf("Parsing %d cities (%s)\n", numCities, matrixType)

	// Create city names vector
	cityNames := make([]string, 0, numCities)
	for i := 1; i <= numCities; i++ {
		cityNames = append(cityNames, fmt.Sprintf("C_%d", i))
	}

	// Pre-initialize distances matrix
	distancesMatrix := make(map[string]map[string]uint, numCities)
	for _, cityFrom := range cityNames {
		distances := make(map[string]uint, numCities)
		for _, cityTo := range cityNames {
			distances[cityTo] = 0
		}
		distancesMatrix[cityFrom] = distances
	}

	switch matrixType {

	case "UPPER_ROW":
		for i := 0; i < numCities-1; i++ {
			ok := scanner.Scan()
			if !ok {
				panic(fmt.Errorf("failed to scan %d", i))
			}

			cityFrom := cityNames[i]
			cityDistancesVector := strings.Fields(scanner.Text())

			expectedDistances := numCities - i - 1
			if len(cityDistancesVector) != expectedDistances {
				panic(fmt.Errorf("expected a vector of %d distances, got %d", expectedDistances, len(cityDistancesVector)))
			}

			for j, distanceString := range cityDistancesVector {

				cityTo := cityNames[i+j+1]

				distance, err := strconv.Atoi(distanceString)
				if err != nil {
					panic(fmt.Errorf("failed to parse distance from %s to %s: %s: %w", cityFrom, cityTo, distanceString, err))
				}

				distancesMatrix[cityFrom][cityTo] = uint(distance)
				distancesMatrix[cityTo][cityFrom] = uint(distance)
			}
		}

	case "FULL_MATRIX":
		for i := 0; i < numCities; i++ {
			ok := scanner.Scan()
			if !ok {
				panic(fmt.Errorf("failed to scan %d", i))
			}

			cityFrom := cityNames[i]

			cityDistancesVector := strings.Fields(scanner.Text())
			if len(cityDistancesVector) != numCities {
				panic(fmt.Errorf("expected %d distances, got %d", numCities, len(cityDistancesVector)))
			}
			for j, distanceString := range cityDistancesVector {
				cityTo := cityNames[j]
				distance, err := strconv.Atoi(distanceString)
				if err != nil {
					panic(fmt.Errorf("failed to parse distance from %s to %s: %s: %w", cityFrom, cityTo, distanceString, err))
				}

				distancesMatrix[cityFrom][cityTo] = uint(distance)
			}
		}

	default:
		panic(fmt.Sprintf("Unknown matrix type: %s", matrixType))
	}

	return CitiesMap{
		distances:       distancesMatrix,
		optimalSolution: optimalSolution,
		cityNames:       cityNames,
	}
}
