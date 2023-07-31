package cloudcost

// CloudCostField represents CloudCost-specific filterable fields.
type CloudCostField string

// If a field is added, ensure field maps are updated
const (
	FieldProviderID      CloudCostField = "providerID"
	FieldProvider        CloudCostField = "provider"
	FieldAccountID       CloudCostField = "accountID"
	FieldInvoiceEntityID CloudCostField = "invoiceEntityID"
	FieldService         CloudCostField = "service"
	FieldCategory        CloudCostField = "category"
	FieldLabel           CloudCostField = "label"
)

// Label aliases for CloudCosts, may not be needed, given
// the logical purpose of CloudCosts? Included pending review.
type CloudCostAlias string

const (
	DepartmentProp  CloudCostAlias = "department"
	EnvironmentProp CloudCostAlias = "environment"
	OwnerProp       CloudCostAlias = "owner"
	ProductProp     CloudCostAlias = "product"
	TeamProp        CloudCostAlias = "team"
)
