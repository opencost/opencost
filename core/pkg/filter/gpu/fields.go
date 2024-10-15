package gpu

import "github.com/opencost/opencost/core/pkg/filter/fieldstrings"

// AllocationField is an enum that represents Allocation-specific fields that can be
// filtered on (namespace, label, etc.)
type AllocationGPUField string

// If you add a AllocationFilterField, make sure to update field maps to return the correct
// Allocation value
// does not enforce exhaustive pattern matching on "enum" types.
const (
	FieldClusterID      AllocationGPUField = AllocationGPUField(fieldstrings.FieldClusterID)
	FieldNamespace      AllocationGPUField = AllocationGPUField(fieldstrings.FieldNamespace)
	FieldControllerKind AllocationGPUField = AllocationGPUField(fieldstrings.FieldControllerKind)
	FieldControllerName AllocationGPUField = AllocationGPUField(fieldstrings.FieldControllerName)
	FieldPod            AllocationGPUField = AllocationGPUField(fieldstrings.FieldPod)
	FieldContainer      AllocationGPUField = AllocationGPUField(fieldstrings.FieldContainer)
)
