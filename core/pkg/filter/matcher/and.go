package matcher

import (
	"fmt"
	"strings"
)

// And is a set of filters that should be evaluated as a logical
// AND.
type And[T any] struct {
	Matchers []Matcher[T]
}

func (a *And[T]) Add(m Matcher[T]) {
	a.Matchers = append(a.Matchers, m)
}

func (a *And[T]) String() string {
	var s strings.Builder
	s.WriteString("(and")
	for _, f := range a.Matchers {
		fmt.Fprintf(&s, " %s", f)
	}

	s.WriteString(")")
	return s.String()
}

// Matches is the canonical in-Go function for determining if T
// matches a AND match rules.
func (a *And[T]) Matches(that T) bool {
	filters := a.Matchers
	if len(filters) == 0 {
		return true
	}

	for _, filter := range filters {
		if !filter.Matches(that) {
			return false
		}
	}

	return true
}
