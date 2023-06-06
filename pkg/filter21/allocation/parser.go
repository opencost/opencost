package allocation

import "github.com/opencost/opencost/pkg/filter21/ast"

// a slice of all the allocation field instances the lexer should recognize as
// valid left-hand comparators
var allocationFilterFields []*ast.Field = []*ast.Field{
	ast.NewField(AllocationFieldClusterID),
	ast.NewField(AllocationFieldNode),
	ast.NewField(AllocationFieldNamespace),
	ast.NewField(AllocationFieldControllerName),
	ast.NewField(AllocationFieldControllerKind),
	ast.NewField(AllocationFieldContainer),
	ast.NewField(AllocationFieldPod),
	ast.NewField(AllocationFieldProvider),
	ast.NewAliasField(AllocationAliasDepartment),
	ast.NewAliasField(AllocationAliasEnvironment),
	ast.NewAliasField(AllocationAliasOwner),
	ast.NewAliasField(AllocationAliasProduct),
	ast.NewAliasField(AllocationAliasTeam),
	ast.NewSliceField(AllocationFieldServices),
	ast.NewMapField(AllocationFieldLabel),
	ast.NewMapField(AllocationFieldAnnotation),
}

// fieldMap is a lazily loaded mapping from AllocationField to ast.Field
var fieldMap map[AllocationField]*ast.Field

// DefaultFieldByName returns only default allocation filter fields by name.
func DefaultFieldByName(field AllocationField) *ast.Field {
	if fieldMap == nil {
		fieldMap = make(map[AllocationField]*ast.Field, len(allocationFilterFields))
		for _, f := range allocationFilterFields {
			ff := *f
			fieldMap[AllocationField(ff.Name)] = &ff
		}
	}

	if af, ok := fieldMap[field]; ok {
		afcopy := *af
		return &afcopy
	}

	return nil
}

// NewAllocationParser creates a new `ast.FilterParser` implementation
// which uses allocation specific fields
func NewAllocationParser() ast.FilterParser {
	return ast.NewFilterParser(allocationFilterFields)
}
