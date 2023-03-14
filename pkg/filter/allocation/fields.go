package allocation

// AllocationField is an enum that represents Allocation-specific fields that can be
// filtered on (namespace, label, etc.)
type AllocationField string

// If you add a AllocationFilterField, MAKE SURE TO UPDATE ALL FILTER IMPLEMENTATIONS! Go
// does not enforce exhaustive pattern matching on "enum" types.
const (
	AllocationFieldClusterID      AllocationField = "cluster"
	AllocationFieldNode                           = "node"
	AllocationFieldNamespace                      = "namespace"
	AllocationFieldControllerKind                 = "controllerKind"
	AllocationFieldControllerName                 = "controllerName"
	AllocationFieldPod                            = "pod"
	AllocationFieldContainer                      = "container"
	AllocationFieldProvider                       = "provider"
	AllocationFieldServices                       = "services"
	AllocationFieldLabel                          = "label"
	AllocationFieldAnnotation                     = "annotation"
)

// AllocationAlias represents an alias field type for allocations.
// Filtering based on label aliases (team, department, etc.) should be a
// responsibility of the query handler. By the time it reaches this
// structured representation, we shouldn't have to be aware of what is
// aliased to what.
type AllocationAlias string

const (
	AllocationAliasDepartment  AllocationAlias = "department"
	AllocationAliasEnvironment                 = "environment"
	AllocationAliasOwner                       = "owner"
	AllocationAliasProduct                     = "product"
	AllocationAliasTeam                        = "team"
)
