package cloud

// CloudAggregationField is an enum that represents CloudAggregation specific fields that can be
// filtered
type CloudAggregationField string

// If you add a CloudAggregationField, make sure to update field maps to return the correct
// CloudAggregation* value
const (
	CloudAggregationFieldBilling   CloudAggregationField = "billing"
	CloudAggregationFieldWorkGroup CloudAggregationField = "workGroup"
	CloudAggregationFieldProvider  CloudAggregationField = "provider"
	CloudAggregationFieldService   CloudAggregationField = "service"
	CloudAggregationFieldLabel     CloudAggregationField = "label"
)
