package cloudcost

// CloudCostField is an enum that represents CloudCost specific fields that can be filtered
type CloudCostField string

const (
	FieldInvoiceEntityID CloudCostField = "invoiceEntityID"
	FieldAccountID       CloudCostField = "accountID"
	FieldProvider        CloudCostField = "provider"
	FieldProviderID      CloudCostField = "providerID"
	FieldCategory        CloudCostField = "category"
	FieldService         CloudCostField = "service"
	FieldLabel           CloudCostField = "label"
)
