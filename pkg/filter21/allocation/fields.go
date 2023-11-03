package allocation

// AllocationField is an enum that represents Allocation-specific fields that can be
// filtered on (namespace, label, etc.)
type AllocationField string

// If you add a AllocationFilterField, make sure to update field maps to return the correct
// Allocation value
// does not enforce exhaustive pattern matching on "enum" types.
const (
	FieldClusterID      AllocationField = "cluster"
	FieldNode           AllocationField = "node"
	FieldNamespace      AllocationField = "namespace"
	FieldControllerKind AllocationField = "controllerKind"
	FieldControllerName AllocationField = "controllerName"
	FieldPod            AllocationField = "pod"
	FieldContainer      AllocationField = "container"
	FieldProvider       AllocationField = "provider"
	FieldServices       AllocationField = "services"
	FieldLabel          AllocationField = "label"
	FieldAnnotation     AllocationField = "annotation"
)

// AllocationAlias represents an alias field type for allocations.
// Filtering based on label aliases (team, department, etc.) should be a
// responsibility of the query handler. By the time it reaches this
// structured representation, we shouldn't have to be aware of what is
// aliased to what. The aliases correspond to either a label or annotation,
// defined by the user.
type AllocationAlias string

const (
	AliasDepartment  AllocationAlias = "department"
	AliasEnvironment AllocationAlias = "environment"
	AliasOwner       AllocationAlias = "owner"
	AliasProduct     AllocationAlias = "product"
	AliasTeam        AllocationAlias = "team"
)
