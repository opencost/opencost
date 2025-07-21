package diagnostics

import "github.com/opencost/opencost/core/pkg/filter/ast"

// a slice of all the diagnostics field instances the lexer should recognize as
// valid left-hand comparators
var diagnosticsFilterFields []*ast.Field = []*ast.Field{
	ast.NewField(FieldClusterID),
}

// fieldMap is a lazily loaded mapping from DiagnosticsField to ast.Field
var fieldMap map[DiagnosticsField]*ast.Field

func init() {
	fieldMap = make(map[DiagnosticsField]*ast.Field, len(diagnosticsFilterFields))
	for _, f := range diagnosticsFilterFields {
		ff := *f
		fieldMap[DiagnosticsField(ff.Name)] = &ff
	}
}

// DefaultFieldByName returns only default diagnostics filter fields by name.
func DefaultFieldByName(field DiagnosticsField) *ast.Field {
	if af, ok := fieldMap[field]; ok {
		afcopy := *af
		return &afcopy
	}

	return nil
}

// NewDiagnosticFilterParser creates a new `ast.FilterParser` implementation
// which uses diagnostics specific fields
func NewDiagnosticsFilterParser() ast.FilterParser {
	return ast.NewFilterParser(diagnosticsFilterFields)
}