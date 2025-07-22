package node

import "github.com/opencost/opencost/core/pkg/filter/ast"

// a slice of all the node field instances the lexer should recognize as
// valid left-hand comparators
var nodeFilterFields []*ast.Field = []*ast.Field{
	ast.NewField(FieldName),
	ast.NewField(FieldNodeType),
	ast.NewField(FieldClusterID),
	ast.NewField(FieldProvider),
	ast.NewField(FieldProviderID),
	ast.NewMapField(FieldLabel),
}

// fieldMap is a lazily loaded mapping from NodeField to ast.Field
var fieldMap map[NodeField]*ast.Field

func init() {
	fieldMap = make(map[NodeField]*ast.Field, len(nodeFilterFields))
	for _, f := range nodeFilterFields {
		ff := *f
		fieldMap[NodeField(ff.Name)] = &ff
	}
}

// DefaultFieldByName returns only default node filter fields by name.
func DefaultFieldByName(field NodeField) *ast.Field {
	if af, ok := fieldMap[field]; ok {
		afcopy := *af
		return &afcopy
	}

	return nil
}

// NewNodeFilterParser creates a new `ast.FilterParser` implementation
// which uses node specific fields
func NewNodeFilterParser() ast.FilterParser {
	return ast.NewFilterParser(nodeFilterFields)
}
