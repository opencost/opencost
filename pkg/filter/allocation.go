package filter

import (
	"fmt"
	"github.com/opencost/opencost/pkg/kubecost"
	"github.com/opencost/opencost/pkg/log"
	"strings"
)

// AllocationFilterField is an enum that represents Allocation-specific fields that can be
// filtered on (namespace, label, etc.)
type AllocationField string

// If you add a AllocationFilterField, MAKE SURE TO UPDATE ALL FILTER IMPLEMENTATIONS! Go
// does not enforce exhaustive pattern matching on "enum" types.
const (
	FilterClusterID      AllocationField = "clusterid"
	FilterNode                           = "node"
	FilterNamespace                      = "namespace"
	FilterControllerKind                 = "controllerkind"
	FilterControllerName                 = "controllername"
	FilterPod                            = "pod"
	FilterContainer                      = "container"

	// Filtering based on label aliases (team, department, etc.) should be a
	// responsibility of the query handler. By the time it reaches this
	// structured representation, we shouldn't have to be aware of what is
	// aliased to what.

	FilterLabel      = "label"
	FilterAnnotation = "annotation"

	FilterServices = "services"
)

// AllocationCondition is the lowest-level type of filter. It represents
// the a filter operation (equality, inequality, etc.) on a field (namespace,
// label, etc.).
type AllocationCondition struct {
	Field AllocationField
	Op    StringOperation

	// Key is for filters that require key-value pairs, like labels or
	// annotations.
	//
	// A filter of 'label[app]:"foo"' has Key="app" and Value="foo"
	Key string

	// Value is for _all_ filters. A filter of 'namespace:"kubecost"' has
	// Value="kubecost"
	Value string
}

func (ac AllocationCondition) String() string {
	if ac.Key == "" {
		return fmt.Sprintf(`(%s %s "%s")`, ac.Op, ac.Field, ac.Value)
	}

	return fmt.Sprintf(`(%s %s[%s] "%s")`, ac.Op, ac.Field, ac.Key, ac.Value)
}

// Flattened returns itself because you cannot flatten a base condition further
func (ac AllocationCondition) Flattened() Filter[*kubecost.Allocation] {
	return ac
}

func (ac AllocationCondition) equals(that Filter[*kubecost.Allocation]) bool {
	if thatAC, ok := that.(AllocationCondition); ok {
		return ac == thatAC
	}
	return false
}

func (ac AllocationCondition) Matches(alloc *kubecost.Allocation) bool {
	if alloc == nil {
		return false
	}
	if alloc.Properties == nil {
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
	switch ac.Field {
	case FilterClusterID:
		valueToCompare = alloc.Properties.Cluster
	case FilterNode:
		valueToCompare = alloc.Properties.Node
	case FilterNamespace:
		valueToCompare = alloc.Properties.Namespace
	case FilterControllerKind:
		valueToCompare = alloc.Properties.ControllerKind
	case FilterControllerName:
		valueToCompare = alloc.Properties.Controller
	case FilterPod:
		valueToCompare = alloc.Properties.Pod
	case FilterContainer:
		valueToCompare = alloc.Properties.Container
	// Comes from GetAnnotation/LabelFilterFunc in KCM
	case FilterLabel:
		val, ok := alloc.Properties.Labels[ac.Key]

		if !ok {
			toCompareMissing = true
		} else {
			valueToCompare = val
		}
	case FilterAnnotation:
		val, ok := alloc.Properties.Annotations[ac.Key]

		if !ok {
			toCompareMissing = true
		} else {
			valueToCompare = val
		}
	case FilterServices:
		valueToCompare = alloc.Properties.Services
	default:
		log.Errorf("Allocation Filter: Unhandled filter field. This is a filter implementation error and requires immediate patching. Field: %s", ac.Field)
		return false
	}

	switch ac.Op {
	case StringEquals:
		// namespace:"__unallocated__" should match a.Properties.Namespace = ""
		// label[app]:"__unallocated__" should match _, ok := Labels[app]; !ok
		if toCompareMissing || valueToCompare == "" {
			return ac.Value == kubecost.UnallocatedSuffix
		}

		if valueToCompare == ac.Value {
			return true
		}
	case StringSliceContains:
		if stringSlice, ok := valueToCompare.([]string); ok {
			if len(stringSlice) == 0 {
				return ac.Value == kubecost.UnallocatedSuffix
			}

			for _, s := range stringSlice {
				if s == ac.Value {
					return true
				}
			}
		} else {
			log.Warnf("Allocation Filter: invalid 'contains' call for non-list filter value")
		}
	case StringStartsWith:
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
		return strings.HasPrefix(s, ac.Value)
	case StringContainsPrefix:
		if toCompareMissing {
			return false
		}

		// We don't need special __unallocated__ logic here because a query
		// asking for "__unallocated__" won't have a wildcard and unallocated
		// properties are the empty string.

		values, ok := valueToCompare.([]string)
		if !ok {
			log.Warnf("Allocation Filter: invalid '%s' call for field with unsupported type", StringContainsPrefix)
			return false
		}

		for _, s := range values {
			if strings.HasPrefix(s, ac.Value) {
				return true
			}
		}

		return false
	default:
		log.Errorf("Allocation Filter: Unhandled filter op. This is a filter implementation error and requires immediate patching. Op: %s", ac.Op)
		return false
	}

	return false
}
