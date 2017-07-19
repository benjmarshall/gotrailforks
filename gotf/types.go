package gotf

import "github.com/kniren/gota/dataframe"

type difficultyType uint8

var difficultyMap = map[string]int{
	"Access Road/Trail":                       1,
	"Easy / Green Circle":                     2,
	"Intermediate / Blue Square":              3,
	"Advanced: Grade 4":                       4,
	"Very Difficult / Black Diamond":          5,
	"Extremely Difficult / Dbl Black Diamond": 6}

// TechAbilityType supports an enumerated type for technical ability
type TechAbilityType uint8

const (
	// TechBeginner represents a beginner technical ability level i.e. mtb trail grade 1-3
	TechBeginner TechAbilityType = iota + 1
	// TechIntermediate represents an intermediate technical ability level i.e. mtb trail grade 3-5
	TechIntermediate
	// TechAdvanced represents an advanced technical ability level i.e. mtb trail grade 4-6
	TechAdvanced
)

type fitnessAbilityType uint8

const (
	fitnessBeginner fitnessAbilityType = iota + 1
	fitnessIntermediate
	fitnessAdvanced
)

var techAbilityMap = map[TechAbilityType][]int{
	TechBeginner:     {1, 2, 3},
	TechIntermediate: {2, 3, 4, 5},
	TechAdvanced:     {4, 5, 6}}

// SortType supports an enumerated type for sorting trails
type SortType uint8

const (
	// RatingSort represents sorting by trailforks user rating
	RatingSort SortType = iota + 1
	// RankSort represents sorting by trailforks global RankSort
	RankSort
	// DistanceSort represents sorting by trail length
	DistanceSort
	// DifficultySort represents sorting by difficulty
	DifficultySort
	// NameSort represents sorting alphabetically on trail name
	NameSort
	// CustomRankSort represents sorting by an internal popularity scoring system
	CustomRankSort
)

// TrailData holds the trail information dataframe
type TrailData struct {
	df  dataframe.DataFrame
	Err error
}
