package mathutil

import (
	"math"
	"sync"
)

// intSearch is a mechanism for searching integers in a specific direction
// for a given distance.
type intSearch struct {
	value     int
	distance  int
	increment int
}

func newIntSearch(value int, increment int) *intSearch {
	return &intSearch{
		value:     value,
		distance:  0,
		increment: increment,
	}
}

func (i *intSearch) advance() {
	i.value += i.increment
	i.distance++
}

func Approximately(exp, act float64) bool {
	return ApproximatelyPct(exp, act, 0.0001) // within 0.1%
}

func ApproximatelyPct(exp, act, pct float64) bool {
	delta := exp * pct
	if delta < 0.00001 {
		delta = 0.00001
	}
	return math.Abs(exp-act) < delta
}

// FindClosestDivisor finds the closest divisor into the `into` value starting with the `current` value.
// It runs concurrent searches in both directions and returns the value that travels the least distance.
// If the distances are equivalent, it returns the greater value.
func FindClosestDivisor(current int, into int) int {
	if isDivisibleBy(current, into) {
		return current
	}

	// we run forward and backwards searches
	var wg sync.WaitGroup
	wg.Add(2)

	// find just advances until it finds a number that can divide cleanly into
	// the target int
	find := func(res *intSearch) {
		defer wg.Done()

		for !isDivisibleBy(res.value, into) {
			res.advance()
		}
	}

	rev := newIntSearch(current, -1)
	fwd := newIntSearch(current, 1)

	go find(rev)
	go find(fwd)

	wg.Wait()

	if rev.distance < fwd.distance {
		return rev.value
	}
	return fwd.value
}

// is b divisible by a
func isDivisibleBy(a, b int) bool {
	return (a == 0 || b == 0) || (b%a == 0)
}
