package cloud

import "github.com/opencost/opencost/pkg/filter/ast"

// a slice of all the cloud aggregation field instances the lexer should recognize as
// valid left-hand comparators
var cloudAggregationFilterFields []*ast.Field = []*ast.Field{
	ast.NewField(CloudAggregationFieldBilling),
	ast.NewField(CloudAggregationFieldWorkGroup),
	ast.NewField(CloudAggregationFieldService),
	ast.NewField(CloudAggregationFieldProvider),
	ast.NewField(CloudAggregationFieldLabel),
}

// fieldMap is a lazily loaded mapping from CloudAggregationField to ast.Field
var fieldMap map[CloudAggregationField]*ast.Field

// DefaultFieldByName returns only default cloud aggregation filter fields by name.
func DefaultFieldByName(field CloudAggregationField) *ast.Field {
	if fieldMap == nil {
		fieldMap = make(map[CloudAggregationField]*ast.Field, len(cloudAggregationFilterFields))
		for _, f := range cloudAggregationFilterFields {
			ff := *f
			fieldMap[CloudAggregationField(ff.Name)] = &ff
		}
	}

	if af, ok := fieldMap[field]; ok {
		afcopy := *af
		return &afcopy
	}

	return nil
}

// NewCloudAggregateParser creates a new `ast.FilterParser` implementation
// which uses CloudCostAggregate specific fields
func NewCloudAggregateParser() ast.FilterParser {
	return ast.NewFilterParser(cloudAggregationFilterFields)
}
