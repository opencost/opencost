package node

import (
	"github.com/opencost/opencost/core/pkg/filter/fieldstrings"
)

// NodeField is an enum that represents Asset-specific fields that can be
// filtered on (namespace, label, etc.)
type NodeField string

// If you add a NodeField, make sure to update field maps to return the correct
// Asset value does not enforce exhaustive pattern matching on "enum" types.
const (
	FieldProviderID NodeField = NodeField(fieldstrings.FieldProviderID)
	FieldName       NodeField = NodeField(fieldstrings.FieldName)
	FieldNodeType   NodeField = NodeField(fieldstrings.FieldNodeType)
	FieldClusterID  NodeField = NodeField(fieldstrings.FieldClusterID)
	FieldProvider   NodeField = NodeField(fieldstrings.FieldProvider)
	FieldLabel      NodeField = NodeField(fieldstrings.FieldLabel)
)
