package main

import (
	"fmt"
	"log"
	"os"

	"github.com/benjmarshall/gotrailforks/gotf"
)

func main() {
	// Pick a data file
	f, err := os.Open("/mnt/data/centos_share/trailforks/data/tf.csv")
	defer f.Close()
	if err != nil {
		log.Fatal(err)
	}

	// Create a new trailforks data struct and load the data
	tf := new(gotf.TrailData)
	err = tf.LoadFromCSV(f)
	if err != nil {
		log.Fatal(err)
	}

	// Apply a diificulty filter
	myAbility := gotf.TechAdvanced
	tfFiltered := tf.ApplyTechFilter(myAbility)
	if tfFiltered.Err != nil {
		log.Fatal(tfFiltered.Err)
	}

	// Apply a sort
	mySort := gotf.CustomRankSort
	tfSorted := tfFiltered.SortBy(mySort)
	if tfSorted.Err != nil {
		log.Fatal(tfSorted.Err)
	}

	// Apply a location filter based on the location of the top ranked trail
	locations, _ := tfSorted.Locations()
	topLoc := locations[0]
	tfSpecificLoc := tfSorted.ApplyLocationFilter(topLoc)
	if tfSpecificLoc.Err != nil {
		log.Fatal(tfSpecificLoc.Err)
	}

	// Get the top 10
	tfTop := tfSpecificLoc.GetTopN(10)
	if tfTop.Err != nil {
		log.Fatal(tfTop.Err)
	}

	// Print results
	fmt.Println(tf)
	fmt.Println(tfFiltered)
	fmt.Println(tfSorted)
	fmt.Println(tfSpecificLoc)
	fmt.Println(tfTop)

	// Print some stats
	fmt.Println()
	fmt.Println("Stats for input trails struct:")
	fmt.Printf("Number of trails: %d\n", tf.NumTrails())
	fmt.Println("Locations:")
	locations, _ = tf.Locations()
	fmt.Println(locations)

	// Repeat using chained operations
	tfChained := tf.ApplyTechFilter(myAbility).SortBy(mySort)
	locations, _ = tfChained.Locations()
	topLoc = locations[0]
	tfChained = tfChained.ApplyLocationFilter(topLoc).GetTopN(10)
	if tfChained.Err != nil {
		log.Fatal(tfChained.Err)
	}

	// Print Final Selection
	fmt.Println()
	fmt.Printf("Top %d trails in %s:\n", tfTop.NumTrails(), topLoc)
	fmt.Println(tfChained.ApplyColumnFilter([]string{"title", "difficulty", "rating", "votes", "customrank"}))

}
