package csvparse

import (
	"encoding/csv"
	"os"
)

// LoadCSV loads the passed file name as a record.
func LoadCSV(s string) ([][]string, error) {
	f, err := os.Open(s)
	defer f.Close()

	if err != nil {
		return nil, err
	}

	r := csv.NewReader(f)
	records, err := r.ReadAll()
	if err != nil {
		return nil, err
	}

	return records, nil

}
