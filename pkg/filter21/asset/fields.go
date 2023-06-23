package asset

// AssetField is an enum that represents Asset-specific fields that can be
// filtered on (namespace, label, etc.)
type AssetField string

// If you add a AssetField, make sure to update field maps to return the correct
// Asset value does not enforce exhaustive pattern matching on "enum" types.
const (
	FieldName       AssetField = "name"
	FieldType       AssetField = "assetType"
	FieldCategory   AssetField = "category"
	FieldClusterID  AssetField = "cluster"
	FieldProject    AssetField = "project"
	FieldProvider   AssetField = "provider"
	FieldProviderID AssetField = "providerID"
	FieldAccount    AssetField = "account"
	FieldService    AssetField = "service"
	FieldLabel      AssetField = "label"
)

// AssetAlias represents an alias field type for assets.
// Filtering based on label aliases (team, department, etc.) should be a
// responsibility of the query handler. By the time it reaches this
// structured representation, we shouldn't have to be aware of what is
// aliased to what.
type AssetAlias string

const (
	DepartmentProp  AssetAlias = "department"
	EnvironmentProp AssetAlias = "environment"
	OwnerProp       AssetAlias = "owner"
	ProductProp     AssetAlias = "product"
	TeamProp        AssetAlias = "team"
)
