package filter

import "sort"

type group[T any] interface {
	Filter[T]
	GetFilters() []Filter[T]
}

// flattened returns a new slice of filters after flattening.
func flattenGroup[T any](g group[T]) []Filter[T] {
	var flattenedFilters []Filter[T]
	for _, innerFilter := range g.GetFilters() {
		if innerFilter == nil {
			continue
		}
		flattenedInner := innerFilter.Flattened()
		if flattenedInner != nil {
			flattenedFilters = append(flattenedFilters, flattenedInner)
		}
	}
	return flattenedFilters

}

func sortGroup[T any](g group[T]) {
	// Run recursively on any internal filter grouping
	for _, inner := range g.GetFilters() {
		if innerGroup, ok := inner.(group[T]); ok {
			sortGroup[T](innerGroup)
		}
	}

	// While a slight hack, we can rely on the string serialization of the
	// inner filters to get a sortable representation.
	sort.SliceStable(g.GetFilters(), func(i, j int) bool {
		return g.GetFilters()[i].String() < g.GetFilters()[j].String()
	})
}
