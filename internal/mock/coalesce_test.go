package mock //nolint: testpackage // testing a private function

import (
	"strconv"
	"testing"
)

func Test_coalesce(t *testing.T) {
	// ARRANGE
	testcases := []struct {
		values []int
		result int
	}{
		{values: []int{0, 0, 0}, result: 0},
		{values: []int{0, 0, 1}, result: 1},
		{values: []int{0, 2, 1}, result: 2},
		{values: []int{3, 2, 1}, result: 3},
	}
	for tn, tc := range testcases {
		t.Run(strconv.Itoa(tn), func(t *testing.T) {
			// ACT
			got := coalesce(tc.values...)

			// ASSERT
			wanted := tc.result
			if wanted != got {
				t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
			}
		})
	}
}
