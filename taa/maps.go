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

var mapNames []string

func init() {
	err := fs.WalkDir(mapsFilesFs, ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if strings.HasSuffix(path, ".tsp") && strings.HasPrefix(path, "data/") {
			mapName := path
			mapName = strings.TrimPrefix(mapName, "data/")
			mapName = strings.TrimSuffix(mapName, ".tsp")
			mapNames = append(mapNames, mapName)
		}
		return nil
	})
	if err != nil {
		panic(fmt.Sprintf("Failed to load map names: %s", err))
	}
}

func LoadMap(name string) CitiesMap {
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
	citiesMap := NewCitiesMap(name, file)
	return citiesMap
}

func MapNames() []string {
	return mapNames[:]
}

func NewCitiesMap(mapName string, reader io.Reader) CitiesMap {
	fmt.Printf("Loading map %s...\n", mapName)

	scanner := bufio.NewScanner(reader)

	var numCities int
	var matrixType string
	var optimalSolution uint

	for scanner.Scan() {
		header := scanner.Text()
		_, err := fmt.Sscanf(header, "%d %d %s", &numCities, &optimalSolution, &matrixType)
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
