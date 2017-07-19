package gotf

import "errors"

func stringSliceContains(sSlice []string, s string) bool {
	for _, value := range sSlice {
		if value == s {
			return true
		}
	}
	return false
}

func genIndexSlice(first int, last int) ([]int, error) {
	if last < first {
		return []int{}, errors.New("second index must be equal or bigger than the first")
	}
	if last < 0 || first < 0 {
		return []int{}, errors.New("indexes must be positive")
	}
	var indexSlice []int
	for i := first; i <= last; i++ {
		indexSlice = append(indexSlice, i)
	}
	return indexSlice, nil
}
