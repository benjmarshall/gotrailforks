// Package gotf provide methods to manipulate and use data from Trailforks
package gotf

import (
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/kniren/gota/dataframe"
	"github.com/kniren/gota/series"
)

//
// Load and Parse Functions
//

// LoadFromCSV populates a TrailData dataframe from a csv file
func (t *TrailData) LoadFromCSV(r io.Reader) error {
	t.df = dataframe.ReadCSV(r)
	if t.df.Err != nil {
		return t.df.Err
	}
	t.normaliseDifficulty()
	if t.Err != nil {
		return t.Err
	}
	t.parseDistances()
	if t.Err != nil {
		return t.Err
	}
	t.parseRating()
	if t.Err != nil {
		return t.Err
	}
	t.addCustomRankCol()
	if t.Err != nil {
		return t.Err
	}
	return nil
}

func (t *TrailData) normaliseDifficulty() {
	// Pull difficulty Column
	difficultyCol := t.df.Col("difficulty")
	if difficultyCol.Err != nil {
		t.Err = difficultyCol.Err
		return
	}
	// Covert to a slice
	difficultySlice := difficultyCol.Records()
	// Populate a new slice with the result of appyling the map down the column
	var difficultyNumCol []int
	for _, name := range difficultySlice {
		if num, ok := difficultyMap[name]; ok == true {
			difficultyNumCol = append(difficultyNumCol, num)
		} else {
			t.Err = errors.New("difficulty string not found in map")
			return
		}
	}
	// Push the new column on to the dataframe to replace the original
	t.df = t.df.Mutate(series.New(difficultyNumCol, series.Int, "difficulty"))
	if t.df.Err != nil {
		t.Err = t.df.Err
		return
	}
	return
}

func (t *TrailData) parseDistances() {
	// Pull distance column
	distanceCol := t.df.Col("distance")
	if distanceCol.Err != nil {
		t.Err = distanceCol.Err
		return
	}
	// Convert to slice
	distanceSlice := distanceCol.Records()
	// Populate a new slice with the result of parsing to int
	var distanceNumCol []int
	var distanceParsed string
	for _, distance := range distanceSlice {
		// Check for blank distances
		if strings.Compare(distance, "") == 0 {
			distanceParsed = "0"
			// Catch km values
		} else if strings.Contains(distance, "km") {
			distanceParsed = strings.TrimSpace(strings.Replace(strings.Replace(distance, "km", "", 1), ".", "", 1)) + "00"
		} else {
			distanceParsed = strings.TrimSpace(strings.Replace(strings.Replace(distance, "m", "", 1), ",", "", 1))
		}
		i, err := strconv.Atoi(distanceParsed)
		if err != nil {
			t.Err = err
			return
		}
		distanceNumCol = append(distanceNumCol, i)
	}
	// Push the new column on to the dataframe to replace the original
	t.df = t.df.Mutate(series.New(distanceNumCol, series.Int, "distance"))
	if t.df.Err != nil {
		t.Err = t.df.Err
		return
	}
	return
}

func (t *TrailData) parseRating() {
	// Pull rating column
	ratingCol := t.df.Col("rating")
	if ratingCol.Err != nil {
		t.Err = ratingCol.Err
		return
	}
	// Convert to slice
	ratingSlice := ratingCol.Records()
	// Populate a new slice with the result of parsing
	var ratingNumCol []int
	var votesNumCol []int
	for _, rawRating := range ratingSlice {
		// Catch case for no ratings
		if strings.Compare(rawRating, "0") == 0 {
			ratingNumCol = append(ratingNumCol, 0)
			votesNumCol = append(votesNumCol, 0)
		} else {
			// Last 2 digits are number of votes
			votesNum, err := strconv.Atoi(rawRating[len(rawRating)-2:])
			if err != nil {
				t.Err = err
				return
			}
			// First 2/3 digits up to the 0 delimeter preceding number of votes represents the score
			ratingNum, err := strconv.Atoi(rawRating[:len(rawRating)-3])
			if err != nil {
				t.Err = err
				return
			}
			ratingNumCol = append(ratingNumCol, ratingNum)
			votesNumCol = append(votesNumCol, votesNum)
		}
	}
	// Push the new columns on to the dataframe to replace the original
	t.df = t.df.Mutate(series.New(ratingNumCol, series.Int, "rating"))
	if t.df.Err != nil {
		t.Err = t.df.Err
		return
	}
	t.df = t.df.Mutate(series.New(votesNumCol, series.Int, "votes"))
	if t.df.Err != nil {
		t.Err = t.df.Err
		return
	}
	return
}

func (t *TrailData) addCustomRankCol() {
	var customRankSlice []float64
	r, _ := t.df.Dims()
	for i := 0; i < r; i++ {
		rank, err := strconv.Atoi(t.df.Col("rating").Subset([]int{i}).Records()[0])
		if err != nil {
			t.Err = err
			return
		}
		rankScore := float64(rank) / 100.0
		vote, err := strconv.Atoi(t.df.Col("votes").Subset([]int{i}).Records()[0])
		if err != nil {
			t.Err = err
			return
		}
		voteScore := float64(vote) / 100.0
		customRankSlice = append(customRankSlice, (rankScore+voteScore)/2)
	}
	// Push the new column on to the dataframe
	t.df = t.df.Mutate(series.New(customRankSlice, series.Float, "customrank"))
	if t.df.Err != nil {
		t.Err = t.df.Err
		return
	}
	return
}

//
// Filtering Functions
//

// ApplyTechFilter returns a new TrailData struct which is filtered to only
// contain the appropriate trails for a specified technical ability
func (t TrailData) ApplyTechFilter(a TechAbilityType) TrailData {
	if t.Err != nil {
		return t
	}
	// Get the grades suitable for given ability
	difficulties := techAbilityMap[a]
	// Copy the input struct
	tFiltered := new(TrailData)
	*tFiltered = t
	// Apply a filter based of the trail grade
	tFiltered.df = tFiltered.df.Filter(dataframe.F{Colname: "difficulty", Comparator: series.GreaterEq, Comparando: difficulties[0]}).
		Filter(dataframe.F{Colname: "difficulty", Comparator: series.LessEq, Comparando: difficulties[len(difficulties)-1]})
	tFiltered.Err = tFiltered.df.Err
	return *tFiltered
}

// GetTopN returns the first N entires from the trailforks struct
func (t TrailData) GetTopN(i int) TrailData {
	if t.Err != nil {
		return t
	}
	// Copy the input struct
	tFiltered := new(TrailData)
	*tFiltered = t
	// Check N against the number of rows and adjust if required
	if i > tFiltered.df.Nrow() {
		i = tFiltered.df.Nrow()
	}
	// Return the first N rows
	indexes, err := genIndexSlice(0, i-1)
	if err != nil {
		tFiltered.Err = err
		return *tFiltered
	}
	tFiltered.df = tFiltered.df.Subset(indexes)
	tFiltered.Err = tFiltered.df.Err
	return *tFiltered
}

// ApplyLocationFilter returns a nww trail data struct with only trail
// within the sleected region
func (t TrailData) ApplyLocationFilter(l string) TrailData {
	if t.Err != nil {
		return t
	}
	// Copy the input struct
	tFiltered := new(TrailData)
	*tFiltered = t
	// Pull locations column
	locCol := t.df.Col("region_title")
	if locCol.Err != nil {
		tFiltered.Err = locCol.Err
		return *tFiltered
	}
	// Convert to slice
	locSlice := locCol.Records()
	// Check that the location is valid
	if !stringSliceContains(locSlice, l) {
		tFiltered.Err = errors.New("specified location not found in trail data set")
		return *tFiltered
	}
	// Apply the filter
	tFiltered.df = tFiltered.df.Filter(dataframe.F{Colname: "region_title", Comparator: series.Eq, Comparando: l})
	tFiltered.Err = tFiltered.df.Err
	return *tFiltered

}

// ApplyColumnFilter filters the trail data struct based on the names of the data columns
func (t TrailData) ApplyColumnFilter(names []string) TrailData {
	if t.Err != nil {
		return t
	}
	// Copy the input struct
	tFiltered := new(TrailData)
	*tFiltered = t
	tFiltered.df = tFiltered.df.Select(names)
	tFiltered.Err = tFiltered.df.Err
	return *tFiltered
}

//
// Sorting Functions
//

// SortBy returns a new TrailData struct which has been sorted in according to the input sort type
func (t TrailData) SortBy(s SortType) TrailData {
	if t.Err != nil {
		return t
	}
	tFiltered := new(TrailData)
	*tFiltered = t
	switch s {
	case RatingSort:
		tFiltered.df = tFiltered.df.Arrange(dataframe.RevSort("rating"))
	case RankSort:
		tFiltered.df = tFiltered.df.Arrange(dataframe.RevSort("rank"))
	case DistanceSort:
		tFiltered.df = tFiltered.df.Arrange(dataframe.RevSort("distance"))
	case DifficultySort:
		tFiltered.df = tFiltered.df.Arrange(dataframe.Sort("difficulty"))
	case NameSort:
		tFiltered.df = tFiltered.df.Arrange(dataframe.RevSort("title"))
	case CustomRankSort:
		tFiltered.df = tFiltered.df.Arrange(dataframe.RevSort("customrank"))
	default:
		tFiltered.Err = errors.New("invalid sort type")
		return *tFiltered
	}
	tFiltered.Err = tFiltered.df.Err
	return *tFiltered
}

//
// Reporting Functions
//

// NumTrails reports the number of trails in the current trailforks struct
func (t TrailData) NumTrails() int {
	return t.df.Nrow()
}

// Locations reports all the locations found within the current trailforks struct
func (t TrailData) Locations() ([]string, error) {
	// Pull locations column
	locCol := t.df.Col("region_title")
	if locCol.Err != nil {
		return []string{}, locCol.Err
	}
	// Convert to slice
	locSlice := locCol.Records()
	// Loop over locations, generating a list of unique entries
	var uniqueLocSlice []string
	for _, l := range locSlice {
		if !stringSliceContains(uniqueLocSlice, l) {
			uniqueLocSlice = append(uniqueLocSlice, l)
		}
	}
	return uniqueLocSlice, nil
}

// Names returns the names of the columns in the trail data struct
func (t TrailData) Names() []string {
	return t.df.Names()
}

// String implements the Stringer interface for TrailData
func (t TrailData) String() string {
	if t.Err != nil {
		return fmt.Sprintf("TrailData error: %v", t.Err)
	}
	return t.df.String()
}
