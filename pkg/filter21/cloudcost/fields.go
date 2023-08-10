package cloudcost

// CloudCostField is an enum that represents CloudCost specific fields that can be filtered
type CloudCostField string

const (
	FieldInvoiceEntity CloudCostField = "invoiceEntity"
	FieldAccount       CloudCostField = "account"
	FieldProvider      CloudCostField = "provider"
	FieldProviderID    CloudCostField = "providerID"
	FieldCategory      CloudCostField = "category"
	FieldService       CloudCostField = "service"
	FieldLabel         CloudCostField = "label"
)
