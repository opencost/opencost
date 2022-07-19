package kubecost

import (
	"fmt"
	"strings"

	"github.com/opencost/opencost/pkg/log"
)

// FilterField is an enum that represents Allocation-specific fields that can be
// filtered on (namespace, label, etc.)
type FilterField string

// If you add a FilterField, MAKE SURE TO UPDATE ALL FILTER IMPLEMENTATIONS! Go
// does not enforce exhaustive pattern matching on "enum" types.
const (
	FilterClusterID      FilterField = "clusterid"
	FilterNode                       = "node"
	FilterNamespace                  = "namespace"
	FilterControllerKind             = "controllerkind"
	FilterControllerName             = "controllername"
	FilterPod                        = "pod"
	FilterContainer                  = "container"

	// Filtering based on label aliases (team, department, etc.) should be a
	// responsibility of the query handler. By the time it reaches this
	// structured representation, we shouldn't have to be aware of what is
	// aliased to what.

	FilterLabel      = "label"
	FilterAnnotation = "annotation"

	FilterServices = "services"
)

// FilterOp is an enum that represents operations that can be performed
// when filtering (equality, inequality, etc.)
type FilterOp string

// If you add a FilterOp, MAKE SURE TO UPDATE ALL FILTER IMPLEMENTATIONS! Go
// does not enforce exhaustive pattern matching on "enum" types.
const (
	// FilterEquals is the equality operator
	// "kube-system" FilterEquals "kube-system" = true
	// "kube-syste" FilterEquals "kube-system" = false
	FilterEquals FilterOp = "equals"

	// FilterNotEquals is the inequality operator
	FilterNotEquals = "notequals"

	// FilterContains is an array/slice membership operator
	// ["a", "b", "c"] FilterContains "a" = true
	FilterContains = "contains"

	// FilterNotContains is an array/slice non-membership operator
	// ["a", "b", "c"] FilterNotContains "d" = true
	FilterNotContains = "notcontains"

	// FilterStartsWith matches strings with the given prefix.
	// "kube-system" StartsWith "kube" = true
	//
	// When comparing with a field represented by an array/slice, this is like
	// applying FilterContains to every element of the slice.
	FilterStartsWith = "startswith"

	// FilterContainsPrefix is like FilterContains, but using StartsWith instead
	// of Equals.
	// ["kube-system", "abc123"] ContainsPrefix ["kube"] = true
	FilterContainsPrefix = "containsprefix"
)

// AllocationFilter represents anything that can be used to filter an
// Allocation.
//
// Implement this interface with caution. While it is generic, it
// is intended to be introspectable so query handlers can perform various
// optimizations. These optimizations include:
// - Routing a query to the most optimal cache
// - Querying backing data stores efficiently (e.g. translation to SQL)
//
// Custom implementations of this interface outside of this package should not
// expect to receive these benefits. Passing a custom implementation to a
// handler may in errors.
type AllocationFilter interface {
	// Matches is the canonical in-Go function for determing if an Allocation
	// matches a filter.
	Matches(a *Allocation) bool

	// Flattened converts a filter into a minimal form, removing unnecessary
	// intermediate objects, like single-element or zero-element AND and OR
	// conditions.
	//
	// It returns nil if the filter is filtering nothing.
	//
	// Example:
	// (and (or (namespaceequals "kubecost")) (or)) ->
	// (namespaceequals "kubecost")
	//
	// (and (or)) -> nil
	Flattened() AllocationFilter

	String() string
}

// AllocationFilterCondition is the lowest-level type of filter. It represents
// the a filter operation (equality, inequality, etc.) on a field (namespace,
// label, etc.).
type AllocationFilterCondition struct {
	Field FilterField
	Op    FilterOp

	// Key is for filters that require key-value pairs, like labels or
	// annotations.
	//
	// A filter of 'label[app]:"foo"' has Key="app" and Value="foo"
	Key string

	// Value is for _all_ filters. A filter of 'namespace:"kubecost"' has
	// Value="kubecost"
	Value string
}

func (afc AllocationFilterCondition) String() string {
	if afc.Key == "" {
		return fmt.Sprintf(`(%s %s "%s")`, afc.Op, afc.Field, afc.Value)
	}

	return fmt.Sprintf(`(%s %s[%s] "%s")`, afc.Op, afc.Field, afc.Key, afc.Value)
}

// Flattened returns itself because you cannot flatten a base condition further
func (filter AllocationFilterCondition) Flattened() AllocationFilter {
	return filter
}

// AllocationFilterOr is a set of filters that should be evaluated as a logical
// OR.
type AllocationFilterOr struct {
	Filters []AllocationFilter
}

func (af AllocationFilterOr) String() string {
	s := "(or"
	for _, f := range af.Filters {
		s += fmt.Sprintf(" %s", f)
	}

	s += ")"
	return s
}

// flattened returns a new slice of filters after flattening.
func flattened(filters []AllocationFilter) []AllocationFilter {
	var flattenedFilters []AllocationFilter
	for _, innerFilter := range filters {
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

// Flattened converts a filter into a minimal form, removing unnecessary
// intermediate objects
//
// Flattened returns:
// - nil if filter contains no filters
// - the inner filter if filter contains one filter
// - an equivalent AllocationFilterOr if filter contains more than one filter
func (filter AllocationFilterOr) Flattened() AllocationFilter {
	flattenedFilters := flattened(filter.Filters)
	if len(flattenedFilters) == 0 {
		return nil
	}

	if len(flattenedFilters) == 1 {
		return flattenedFilters[0]
	}

	return AllocationFilterOr{Filters: flattenedFilters}
}

// AllocationFilterOr is a set of filters that should be evaluated as a logical
// AND.
type AllocationFilterAnd struct {
	Filters []AllocationFilter
}

func (af AllocationFilterAnd) String() string {
	s := "(and"
	for _, f := range af.Filters {
		s += fmt.Sprintf(" %s", f)
	}

	s += ")"
	return s
}

// Flattened converts a filter into a minimal form, removing unnecessary
// intermediate objects
//
// Flattened returns:
// - nil if filter contains no filters
// - the inner filter if filter contains one filter
// - an equivalent AllocationFilterAnd if filter contains more than one filter
func (filter AllocationFilterAnd) Flattened() AllocationFilter {
	flattenedFilters := flattened(filter.Filters)
	if len(flattenedFilters) == 0 {
		return nil
	}

	if len(flattenedFilters) == 1 {
		return flattenedFilters[0]
	}

	return AllocationFilterAnd{Filters: flattenedFilters}
}

func (filter AllocationFilterCondition) Matches(a *Allocation) bool {
	if a == nil {
		return false
	}
	if a.Properties == nil {
		return false
	}

	// The Allocation's value for the field to compare
	// We use an interface{} so this can contain the services []string slice
	var valueToCompare interface{}

	// toCompareMissing will be true if the value to be compared is missing in
	// the Allocation. For example, if we're filtering based on the value of
	// the "app" label, but the Allocation doesn't have an "app" label, this
	// will become true. This lets us deal with != gracefully.
	toCompareMissing := false

	// This switch maps the filter.Field to the field to be compared in
	// a.Properties and sets valueToCompare from the value in a.Properties.
	switch filter.Field {
	case FilterClusterID:
		valueToCompare = a.Properties.Cluster
	case FilterNode:
		valueToCompare = a.Properties.Node
	case FilterNamespace:
		valueToCompare = a.Properties.Namespace
	case FilterControllerKind:
		valueToCompare = a.Properties.ControllerKind
	case FilterControllerName:
		valueToCompare = a.Properties.Controller
	case FilterPod:
		valueToCompare = a.Properties.Pod
	case FilterContainer:
		valueToCompare = a.Properties.Container
	// Comes from GetAnnotation/LabelFilterFunc in KCM
	case FilterLabel:
		val, ok := a.Properties.Labels[filter.Key]

		if !ok {
			toCompareMissing = true
		} else {
			valueToCompare = val
		}
	case FilterAnnotation:
		val, ok := a.Properties.Annotations[filter.Key]

		if !ok {
			toCompareMissing = true
		} else {
			valueToCompare = val
		}
	case FilterServices:
		valueToCompare = a.Properties.Services
	default:
		log.Errorf("Allocation Filter: Unhandled filter field. This is a filter implementation error and requires immediate patching. Field: %s", filter.Field)
		return false
	}

	switch filter.Op {
	case FilterEquals:
		// namespace:"__unallocated__" should match a.Properties.Namespace = ""
		// label[app]:"__unallocated__" should match _, ok := Labels[app]; !ok
		if toCompareMissing || valueToCompare == "" {
			return filter.Value == UnallocatedSuffix
		}

		if valueToCompare == filter.Value {
			return true
		}
	case FilterNotEquals:
		// namespace!:"__unallocated__" should match
		// a.Properties.Namespace != ""
		// label[app]!:"__unallocated__" should match _, ok := Labels[app]; ok
		if filter.Value == UnallocatedSuffix {
			if toCompareMissing {
				return false
			}
			return valueToCompare != ""
		}

		if toCompareMissing {
			return true
		}

		if valueToCompare != filter.Value {
			return true
		}
	case FilterContains:
		if stringSlice, ok := valueToCompare.([]string); ok {
			if len(stringSlice) == 0 {
				return filter.Value == UnallocatedSuffix
			}

			for _, s := range stringSlice {
				if s == filter.Value {
					return true
				}
			}
		} else {
			log.Warnf("Allocation Filter: invalid 'contains' call for non-list filter value")
		}
	case FilterNotContains:
		if stringSlice, ok := valueToCompare.([]string); ok {
			// services!:"__unallocated__" should match
			// len(a.Properties.Services) > 0
			//
			// TODO: is this true?
			if filter.Value == UnallocatedSuffix {
				return len(stringSlice) > 0
			}

			for _, s := range stringSlice {
				if s == filter.Value {
					return false
				}
			}

			return true
		} else {
			log.Warnf("Allocation Filter: invalid 'notcontains' call for non-list filter value")
		}
	case FilterStartsWith:
		if toCompareMissing {
			return false
		}

		// We don't need special __unallocated__ logic here because a query
		// asking for "__unallocated__" won't have a wildcard and unallocated
		// properties are the empty string.

		s, ok := valueToCompare.(string)
		if !ok {
			log.Warnf("Allocation Filter: invalid 'startswith' call for field with unsupported type")
			return false
		}
		return strings.HasPrefix(s, filter.Value)
	case FilterContainsPrefix:
		if toCompareMissing {
			return false
		}

		// We don't need special __unallocated__ logic here because a query
		// asking for "__unallocated__" won't have a wildcard and unallocated
		// properties are the empty string.

		values, ok := valueToCompare.([]string)
		if !ok {
			log.Warnf("Allocation Filter: invalid '%s' call for field with unsupported type", FilterContainsPrefix)
			return false
		}

		for _, s := range values {
			if strings.HasPrefix(s, filter.Value) {
				return true
			}
		}

		return false
	default:
		log.Errorf("Allocation Filter: Unhandled filter op. This is a filter implementation error and requires immediate patching. Op: %s", filter.Op)
		return false
	}

	return false
}

func (and AllocationFilterAnd) Matches(a *Allocation) bool {
	filters := and.Filters
	if len(filters) == 0 {
		return true
	}

	for _, filter := range filters {
		if !filter.Matches(a) {
			return false
		}
	}

	return true
}

func (or AllocationFilterOr) Matches(a *Allocation) bool {
	filters := or.Filters
	if len(filters) == 0 {
		return true
	}

	for _, filter := range filters {
		if filter.Matches(a) {
			return true
		}
	}

	return false
}

// AllocationFilterNone is a filter that matches no allocations. This is useful
// for applications like authorization, where a user/group/role may be disallowed
// from viewing Allocation data entirely.
type AllocationFilterNone struct{}

func (afn AllocationFilterNone) String() string { return "(none)" }

func (afn AllocationFilterNone) Flattened() AllocationFilter { return afn }

func (afn AllocationFilterNone) Matches(a *Allocation) bool { return false }
