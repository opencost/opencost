package gpu

import "github.com/opencost/opencost/core/pkg/filter/ast"

// a slice of all the gpu allocation field instances the lexer should recognize as
// valid left-hand comparators
var allocationGPUFilterFields []*ast.Field = []*ast.Field{
	ast.NewField(FieldClusterID),
	ast.NewField(FieldNamespace),
	ast.NewField(FieldControllerName, ast.FieldAttributeNilable),
	ast.NewField(FieldControllerKind, ast.FieldAttributeNilable),
	ast.NewField(FieldContainer),
	ast.NewField(FieldPod),
}

// fieldMap is a lazily loaded mapping from AllocationGPUField to ast.Field
var fieldMap map[AllocationGPUField]*ast.Field

func init() {
	fieldMap = make(map[AllocationGPUField]*ast.Field, len(allocationGPUFilterFields))
	for _, f := range allocationGPUFilterFields {
		ff := *f
		fieldMap[AllocationGPUField(ff.Name)] = &ff
	}
}

// DefaultFieldByName returns only default gpu allocation filter fields by name.
func DefaultFieldByName(field AllocationGPUField) *ast.Field {
	if af, ok := fieldMap[field]; ok {
		afcopy := *af
		return &afcopy
	}

	return nil
}

// NewAllocationGPUFilterParser creates a new `ast.FilterParser` implementation
// which uses gpu allocation specific fields
func NewAllocationGPUFilterParser() ast.FilterParser {
	return ast.NewFilterParser(allocationGPUFilterFields)
}
