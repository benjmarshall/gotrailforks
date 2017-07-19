package gotf

import (
	"errors"
	"fmt"
	"log"
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

func TestTechFilter(t *testing.T) {
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
	df := dataframe.New(series...)

	easydf := df.Subset([]int{0, 2})
	if easydf.Err != nil {
		log.Fatal("Error getting subset of easy trails")
	}
	intdf := df.Subset([]int{1, 2})
	if easydf.Err != nil {
		log.Fatal("Error getting subset of easy trails")
	}
	advdf := df.Subset([]int{1, 3})
	if easydf.Err != nil {
		log.Fatal("Error getting subset of easy trails")
	}
	testSlice := []struct {
		ability TechAbilityType
		df      dataframe.DataFrame
	}{
		{ability: TechBeginner,
			df: easydf},
		{ability: TechIntermediate,
			df: intdf},
		{ability: TechAdvanced,
			df: advdf},
	}

	for testnum, test := range testSlice {
		want := TrailData{Err: nil, df: test.df}

		got := TrailData{Err: nil, df: df}
		got = got.ApplyTechFilter(test.ability)

		// Check errors
		if want.Err != nil && got.Err != nil {
			t.Errorf("error in test number: %d, found in TrailData struct:\nWant:\n%v\nGot:\n%v", testnum, want.Err, got.Err)
		}
		// Check that the types are the same between both DataFrames
		if !reflect.DeepEqual(want.df.Types(), got.df.Types()) {
			t.Errorf("Different types in test number %d:\nWant:%v\nGot:%v", testnum, want.df.Types(), got.df.Types())
		}
		// Check that the values are the same between both DataFrames
		if !reflect.DeepEqual(want.df.Records(), got.df.Records()) {
			t.Errorf("Different values in test number %d:\nWant:%v\nGot:%v", testnum, want.df.Records(), got.df.Records())
		}

	}
}

func TestTopNFilter(t *testing.T) {
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
	df := dataframe.New(series...)

	topdf := df.Subset([]int{0, 1})
	if topdf.Err != nil {
		log.Fatal("Error getting subset of trails")
	}

	testSlice := []struct {
		num int
		df  dataframe.DataFrame
	}{
		{num: 2,
			df: topdf},
		{num: 10,
			df: df},
	}

	for testnum, test := range testSlice {
		want := TrailData{Err: nil, df: test.df}

		got := TrailData{Err: nil, df: df}
		got = got.GetTopN(test.num)

		// Check errors
		if want.Err != nil && got.Err != nil {
			t.Errorf("error in test number: %d, found in TrailData struct:\nWant:\n%v\nGot:\n%v", testnum, want.Err, got.Err)
		}
		// Check that the types are the same between both DataFrames
		if !reflect.DeepEqual(want.df.Types(), got.df.Types()) {
			t.Errorf("Different types in test number %d:\nWant:%v\nGot:%v", testnum, want.df.Types(), got.df.Types())
		}
		// Check that the values are the same between both DataFrames
		if !reflect.DeepEqual(want.df.Records(), got.df.Records()) {
			t.Errorf("Different values in test number %d:\nWant:%v\nGot:%v", testnum, want.df.Records(), got.df.Records())
		}
	}

}

func TestLocationFilter(t *testing.T) {
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
	df := dataframe.New(series...)

	testSlice := series[2].Records()

	for testnum, test := range testSlice {
		testdf := df.Subset([]int{testnum})
		if testdf.Err != nil {
			log.Fatal("Error getting subset of trails")
		}

		want := TrailData{Err: nil, df: testdf}

		got := TrailData{Err: nil, df: df}
		got = got.ApplyLocationFilter(test)

		// Check errors
		if want.Err != nil && got.Err != nil {
			t.Errorf("error in test number: %d, found in TrailData struct:\nWant:\n%v\nGot:\n%v", testnum, want.Err, got.Err)
		}
		// Check that the types are the same between both DataFrames
		if !reflect.DeepEqual(want.df.Types(), got.df.Types()) {
			t.Errorf("Different types in test number %d:\nWant:%v\nGot:%v", testnum, want.df.Types(), got.df.Types())
		}
		// Check that the values are the same between both DataFrames
		if !reflect.DeepEqual(want.df.Records(), got.df.Records()) {
			t.Errorf("Different values in test number %d:\nWant:%v\nGot:%v", testnum, want.df.Records(), got.df.Records())
		}

	}
}

func TestColumnFilter(t *testing.T) {
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
	df := dataframe.New(series...)

	testSlice := []struct {
		columns []string
	}{
		{columns: []string{"title"}},
		{columns: []string{"rating", "faved"}},
		{columns: []string{"customrank", "votes", "difficulty"}},
	}

	for testnum, test := range testSlice {
		testdf := df.Select(test.columns)
		if testdf.Err != nil {
			log.Fatal("Error getting subset of columns")
		}

		want := TrailData{Err: nil, df: testdf}

		got := TrailData{Err: nil, df: df}
		got = got.ApplyColumnFilter(test.columns)

		// Check errors
		if want.Err != nil && got.Err != nil {
			t.Errorf("error in test number: %d, found in TrailData struct:\nWant:\n%v\nGot:\n%v", testnum, want.Err, got.Err)
		}
		// Check that the types are the same between both DataFrames
		if !reflect.DeepEqual(want.df.Types(), got.df.Types()) {
			t.Errorf("Different types in test number %d:\nWant:%v\nGot:%v", testnum, want.df.Types(), got.df.Types())
		}
		// Check that the values are the same between both DataFrames
		if !reflect.DeepEqual(want.df.Records(), got.df.Records()) {
			t.Errorf("Different values in test number %d:\nWant:%v\nGot:%v", testnum, want.df.Records(), got.df.Records())
		}

	}

}

func TestSortBy(t *testing.T) {
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
	df := dataframe.New(series...)

	testSlice := []struct {
		sort SortType
		df   dataframe.DataFrame
	}{
		{sort: RatingSort,
			df: df.Arrange(dataframe.RevSort("rating"))},
		{sort: RankSort,
			df: df.Arrange(dataframe.RevSort("global_rank"))},
		{sort: DistanceSort,
			df: df.Arrange(dataframe.RevSort("distance"))},
		{sort: DifficultySort,
			df: df.Arrange(dataframe.Sort("difficulty"))},
		{sort: NameSort,
			df: df.Arrange(dataframe.Sort("title"))},
		{sort: CustomRankSort,
			df: df.Arrange(dataframe.RevSort("customrank"))},
	}

	for testnum, test := range testSlice {
		want := TrailData{Err: nil, df: test.df}

		got := TrailData{Err: nil, df: df}
		got = got.SortBy(test.sort)

		// Check errors
		if want.Err != nil && got.Err != nil {
			t.Errorf("error in test number: %d, found in TrailData struct:\nWant:\n%v\nGot:\n%v", testnum, want.Err, got.Err)
		}
		// Check that the types are the same between both DataFrames
		if !reflect.DeepEqual(want.df.Types(), got.df.Types()) {
			t.Errorf("Different types in test number %d:\nWant:%v\nGot:%v", testnum, want.df.Types(), got.df.Types())
		}
		// Check that the values are the same between both DataFrames
		if !reflect.DeepEqual(want.df.Records(), got.df.Records()) {
			t.Errorf("Different values in test number %d:\nWant:%v\nGot:%v", testnum, want.df.Records(), got.df.Records())
		}

	}
}

func TestNumTrails(t *testing.T) {
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
	df := dataframe.New(series...)

	want := len(series[0].Records())

	tf := TrailData{Err: nil, df: df}
	got := tf.NumTrails()

	if got != want {
		t.Errorf("number of trails reported is different, want: %v, got: %v", want, got)
	}

}

func TestLocations(t *testing.T) {
	seriesFull := []series.Series{
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

	// reqionA duplicated in this series!
	seriesDup := []series.Series{
		series.New([]string{"titleA", "titleB", "titleC", "titleD"}, series.String, "title"),
		series.New([]int{1, 5, 3, 6}, series.Int, "difficulty"),
		series.New([]string{"regionA", "regionB", "regionA", ""}, series.String, "region_title"),
		series.New([]int{10, 90, 100, 0}, series.Int, "rating"),
		series.New([]int{3, 17, 5, 3}, series.Int, "ridden"),
		series.New([]int{5, 49, 34, 0}, series.Int, "total_checkins"),
		series.New([]int{0, 4, 1, 1234}, series.Int, "faved"),
		series.New([]int{60001, 58071, 4732, 0}, series.Int, "global_rank"),
		series.New([]int{3500, 0, 8900, 921}, series.Int, "distance"),
		series.New([]int{10, 46, 1, 0}, series.Int, "votes"),
		series.New([]float64{0.1, 0.68, 0.505, 0.0}, series.Float, "customrank"),
	}

	testSlice := []struct {
		locs []string
		df   dataframe.DataFrame
	}{
		{locs: []string{"regionA", "regionB", "regionC"},
			df: dataframe.New(seriesFull...)},
		{locs: []string{"regionA", "regionB"},
			df: dataframe.New(seriesDup...)},
	}

	for testnum, test := range testSlice {

		want := test.locs

		tf := TrailData{Err: nil, df: test.df}
		got, err := tf.Locations()

		// Check errors
		if err != nil {
			t.Errorf("error in test number: %d, Got:\n%v", testnum, err)
		}

		if !reflect.DeepEqual(want, got) {
			t.Errorf("locations reported are different in test %d, want: %v, got: %v", testnum, want, got)
		}

	}

}

func TestNames(t *testing.T) {
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
	df := dataframe.New(series...)

	want := df.Names()

	tf := TrailData{Err: nil, df: df}
	got := tf.Names()

	if !reflect.DeepEqual(want, got) {
		t.Errorf("column names reported are different, want: %v, got: %v", want, got)
	}

}

func TestTrails(t *testing.T) {
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
	df := dataframe.New(series...)

	want := series[0].Records()

	tf := TrailData{Err: nil, df: df}
	got, err := tf.Trails()

	// Check errors
	if err != nil {
		t.Errorf("error in trails function, Got:\n%v", err)
	}

	if !reflect.DeepEqual(want, got) {
		t.Errorf("trails names reported are different, want: %v, got: %v", want, got)
	}

}

func TestStringer(t *testing.T) {
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
	df := dataframe.New(series...)

	testSlice := []struct {
		tf TrailData
	}{
		{tf: TrailData{Err: nil, df: df}},
		{tf: TrailData{Err: errors.New("error"), df: df}},
	}

	for testnum, test := range testSlice {

		var want string
		if test.tf.Err != nil {
			want = fmt.Sprintf("TrailData error: %v", test.tf.Err)
		} else {
			want = test.tf.df.String()
		}

		got := test.tf.String()

		if want != got {
			t.Errorf("string returns are different in test %d, want: %v, got: %v", testnum, want, got)
		}

	}

}
