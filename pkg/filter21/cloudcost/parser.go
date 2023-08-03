package cloudcost

import "github.com/opencost/opencost/pkg/filter21/ast"

// Slice of valid left comparisons for the lexer
var cloudCostFilterFields []*ast.Field = []*ast.Field{
	ast.NewField(FieldProviderID),
	ast.NewField(FieldProvider),
	ast.NewField(FieldAccountID),
	ast.NewField(FieldInvoiceEntityID),
	ast.NewField(FieldService),
	ast.NewField(FieldCategory),
	ast.NewMapField(FieldLabel),
	ast.NewAliasField(DepartmentProp),
	ast.NewAliasField(EnvironmentProp),
	ast.NewAliasField(OwnerProp),
	ast.NewAliasField(ProductProp),
	ast.NewAliasField(TeamProp),
}

// lazily CloudCostField to ast.Field mapping
var fieldMap map[CloudCostField]*ast.Field

// Returns default CloudCost filter fields
func DefaultFieldByName(field CloudCostField) *ast.Field {
	if fieldMap == nil {
		fieldMap = make(map[CloudCostField]*ast.Field, len(cloudCostFilterFields))
		for _, filt := range cloudCostFilterFields {
			filtCopy := *filt
			fieldMap[CloudCostField(filtCopy.Name)] = &filtCopy
		}
	}

	ccf, ok := fieldMap[field]
	if ok {
		ccfCopy := *ccf
		return &ccfCopy
	}
	return nil
}

func NewCloudCostFilterParser() ast.FilterParser {
	return ast.NewFilterParser(cloudCostFilterFields)
}
