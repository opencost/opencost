package kubecost

// FilterCondition is an enum that represents Allocation-specific fields
// that can be filtered on (namespace, label, etc.)
type FilterField int

// If you add a FilterField, MAKE SURE TO UPDATE ALL FILTER IMPLEMENTATIONS! Go
// does not enforce exhaustive pattern matching on "enum" types.
const (
	FilterClusterID FilterField = iota
	FilterNode
	FilterNamespace
	FilterControllerKind
	FilterControllerName
	FilterPod
	FilterContainer

	// Filtering based on label aliases (team, department, etc.) should be a
	// responsibility of the query handler. By the time it reaches this
	// structured representation, we shouldn't have to be aware of what is
	// aliased to what.

	FilterLabel
	FilterAnnotation
)

// FilterOp is an enum that represents operations that can be performed
// when filtering (equality, inequality, etc.)
type FilterOp int

// If you add a FilterOp, MAKE SURE TO UPDATE ALL FILTER IMPLEMENTATIONS! Go
// does not enforce exhaustive pattern matching on "enum" types.
const (
	FilterEquals FilterOp = iota

	// TODO: what is the != behavior for __unallocated__?
	FilterNotEquals
)

// AllocationFilter is a mini-DSL for filtering Allocation data by different
// conditions. By specifying a more strict DSL instead of using arbitrary
// functions we gain the ability to take advantage of storage-level filtering
// performance improvements like indexes in databases. We can create a
// transformation from our DSL to the storage's specific query language. We
// also gain the ability to define a more feature-rich query language for
// users, supporting more operators and more complex logic, if desired.
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
	// TODO: For these nil cases, what about != filters?
	if a == nil {
		return false
	}
	if a.Properties == nil {
		return false
	}

	// TODO Controller PARSING should allow controllerkind:controllername
	// syntax, converted to:
	// (AND (ControllerName Equals) (ControllerKind Equals))

	// The Allocation's value for the field to compare
	var valueToCompare string

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
	default:
		// TODO: log an error here? this should never happen
		return false
	}

	switch filter.Op {
	case FilterEquals:
		if toCompareMissing {
			return false
		}

		// namespace="__unallocated__" should match a.Properties.Namespace = ""
		if valueToCompare == "" && filter.Value == UnallocatedSuffix {
			return true
		}

		if valueToCompare == filter.Value {
			return true
		}
	case FilterNotEquals:
		if toCompareMissing {
			return true
		}

		// TODO: __unallocated__ behavior?

		if valueToCompare != filter.Value {
			return true
		}
	default:
		// TODO: log an error here? this should never happen
		return false
	}

	return false
}

func (and AllocationFilterAnd) Matches(a *Allocation) bool {
	filters := and.Filters
	if len(filters) == 0 {
		// TODO: Should an empty set of ANDs be true or false?
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
