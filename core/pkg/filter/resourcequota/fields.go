package resourcequota

import (
	"github.com/opencost/opencost/core/pkg/filter/fieldstrings"
)

type ResourceQuotaField string

// If you add a ResourceQuotaField, make sure to update field maps to return the correct
// Asset value does not enforce exhaustive pattern matching on "enum" types.
const (
	FieldClusterID      ResourceQuotaField = ResourceQuotaField(fieldstrings.FieldClusterID)
	FieldResourceQuota  ResourceQuotaField = ResourceQuotaField(fieldstrings.FieldResourceQuota)
	FieldNamespace      ResourceQuotaField = ResourceQuotaField(fieldstrings.FieldNamespace)
	FieldNamespaceLabel ResourceQuotaField = ResourceQuotaField(fieldstrings.FieldNamespaceLabel)
	FieldUID            ResourceQuotaField = ResourceQuotaField(fieldstrings.FieldUID)
)
