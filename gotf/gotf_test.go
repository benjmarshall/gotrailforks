package gotf

import (
	"reflect"
	"strings"
	"testing"

	"github.com/kniren/gota/dataframe"
	"github.com/kniren/gota/series"
)

const (
	inputCSV = `title,difficulty,region_title,rating,ridden,total_checkins,faved,global_rank,distance
titleA,Access Road/Trail,regionA,10010,3,5,0,60001,"3,500 m"
titleB,Very Difficult / Black Diamond,regionB,90046,17,49,4,58071,
titleC,Intermediate / Blue Square,regionC,100001,5,34,1,4732,8.9 km
titleD,Extremely Difficult / Dbl Black Diamond,,0,3,0,1234,0,921 m`
)

func TestLoadFromCSV(t *testing.T) {
	series := []series.Series{
		series.New([]string{"titleA", "titleB", "titleC", "titleD"}, series.String, "title"),
		series.New([]int{1, 5, 3, 6}, series.Int, "difficulty"),
		series.New([]string{"regionA", "regionB", "regionC", ""}, series.String, "region_title"),
		series.New([]int{10, 90, 100, 0}, series.Int, "rating"),
		series.New([]int{3, 17, 5, 3}, series.Int, "ridden"),
		series.New([]int{5, 49, 34, 0}, series.Int, "total_checkins"),
		series.New([]int{0, 4, 1, 1234}, series.Int, "faved"),
		series.New([]int{60001, 58071, 4732, 0}, series.Int, "global_rank"),
		series.New([]int{3500, 0, 8900, 921}, series.Int, "distance"),
		series.New([]int{10, 46, 1, 0}, series.Int, "votes"),
		series.New([]float64{0.1, 0.68, 0.505, 0.0}, series.Float, "customrank"),
	}
	want := TrailData{Err: nil,
		df: dataframe.New(series...)}

	got := new(TrailData)
	err := got.LoadFromCSV(strings.NewReader(inputCSV))
	if err != nil {
		t.Errorf("error whilst loading from CSV string %v", err)
	}

	// Check errors
	if want.Err != nil && got.Err != nil {
		t.Errorf("error found in TrailData struct:\nWant:\n%v\nGot:\n%v", want.Err, got.Err)
	}
	// Check that the types are the same between both DataFrames
	if !reflect.DeepEqual(want.df.Types(), got.df.Types()) {
		t.Errorf("Different types:\nWant:%v\nGot:%v", want.df.Types(), got.df.Types())
	}
	// Check that the values are the same between both DataFrames
	if !reflect.DeepEqual(want.df.Records(), got.df.Records()) {
		t.Errorf("Different values:\nWant:%v\nGot:%v", want.df.Records(), got.df.Records())
	}
}
