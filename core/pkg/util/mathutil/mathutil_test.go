package mathutil

import (
	"fmt"
	"testing"
)

func TestClosestDivision(t *testing.T) {
	tests := []struct {
		current  int
		into     int
		expected int
	}{
		{0, 60, 0},
		{1, 60, 1},
		{2, 60, 2},
		{8, 60, 10},
		{7, 60, 6},
		{11, 60, 12},
		{41, 60, 30},
		{42, 60, 30},
		{43, 60, 30},
		{44, 60, 30},
		{45, 60, 60},
		{46, 60, 60},
		{47, 60, 60},
		{48, 60, 60},
		{49, 60, 60},
		{50, 60, 60},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("TestClosestDivision[%d_%d]", test.current, test.into), func(t *testing.T) {
			result := FindClosestDivisor(test.current, test.into)
			if result != test.expected {
				t.Errorf("Expected %d, got %d", test.expected, result)
			}
		})
	}
}

func BenchmarkClosestDivision(b *testing.B) {
	for i := 0; i < b.N; i++ {
		for current := 0; current <= 60; current++ {
			FindClosestDivisor(current, 60)
		}
	}
}
