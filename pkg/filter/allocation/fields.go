package allocation

// AllocationField is an enum that represents Allocation-specific fields that can be
// filtered on (namespace, label, etc.)
type AllocationField string

// If you add a AllocationFilterField, make sure to update field maps to return the correct
// Allocation value
// does not enforce exhaustive pattern matching on "enum" types.
const (
	AllocationFieldClusterID      AllocationField = "cluster"
	AllocationFieldNode           AllocationField = "node"
	AllocationFieldNamespace      AllocationField = "namespace"
	AllocationFieldControllerKind AllocationField = "controllerKind"
	AllocationFieldControllerName AllocationField = "controllerName"
	AllocationFieldPod            AllocationField = "pod"
	AllocationFieldContainer      AllocationField = "container"
	AllocationFieldProvider       AllocationField = "provider"
	AllocationFieldServices       AllocationField = "services"
	AllocationFieldLabel          AllocationField = "label"
	AllocationFieldAnnotation     AllocationField = "annotation"
)

// AllocationAlias represents an alias field type for allocations.
// Filtering based on label aliases (team, department, etc.) should be a
// responsibility of the query handler. By the time it reaches this
// structured representation, we shouldn't have to be aware of what is
// aliased to what.
type AllocationAlias string

const (
	AllocationAliasDepartment  AllocationAlias = "department"
	AllocationAliasEnvironment AllocationAlias = "environment"
	AllocationAliasOwner       AllocationAlias = "owner"
	AllocationAliasProduct     AllocationAlias = "product"
	AllocationAliasTeam        AllocationAlias = "team"
)
