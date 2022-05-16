package kubecost

import (
	"strings"

	"github.com/kubecost/cost-model/pkg/log"
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
	// FilterStartsWith matches strings with the given prefix.
	// "kube-system" StartsWith "kube" = true
	//
	// When comparing with a field represented by an array/slice, this is like
	// applying FilterContains to every element of the slice.
	FilterStartsWith = "startswith"
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

// AllocationFilterOr is a set of filters that should be evaluated as a logical
// OR.
type AllocationFilterOr struct {
	Filters []AllocationFilter
}

// AllocationFilterOr is a set of filters that should be evaluated as a logical
// AND.
type AllocationFilterAnd struct {
	Filters []AllocationFilter
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
		if toCompareMissing {
			return false
		}

		// namespace:"__unallocated__" should match a.Properties.Namespace = ""
		if valueToCompare == "" {
			return filter.Value == UnallocatedSuffix
		}

		if valueToCompare == filter.Value {
			return true
		}
	case FilterNotEquals:
		if toCompareMissing {
			return true
		}

		// namespace!:"__unallocated__" should match
		// a.Properties.Namespace != ""
		if filter.Value == UnallocatedSuffix {
			return valueToCompare != ""
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
	case FilterStartsWith:
		if toCompareMissing {
			return false
		}

		// We don't need special __unallocated__ logic here because a query
		// asking for "__unallocated__" won't have a wildcard and unallocated
		// properties are the empty string.

		switch v := valueToCompare.(type) {
		case string:
			return strings.HasPrefix(v, filter.Value)
		case []string:
			for _, s := range v {
				if strings.HasPrefix(s, filter.Value) {
					return true
				}
			}
			return false
		default:
			log.Warnf("Allocation Filter: invalid 'startswith' call for field with unsupported type")
			return false
		}

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
