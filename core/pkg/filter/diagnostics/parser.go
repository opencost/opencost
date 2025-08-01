package diagnostics

import (
	"github.com/opencost/opencost/core/pkg/filter/ast"
	"github.com/opencost/opencost/core/pkg/filter/ops"
)

// a slice of all the diagnostics field instances the lexer should recognize as
// valid left-hand comparators
var diagnosticsFilterFields []*ast.Field = []*ast.Field{
	ast.NewField(FieldClusterID),
}

// a slice of all the diagnostics summary field instances the lexer should recognize as
// valid left-hand comparators
var diagnosticsSummaryFilterFields []*ast.Field = []*ast.Field{
	ast.NewField(FieldSummaryClusterID),
	ast.NewField(FieldSummaryProvider),
	ast.NewField(FieldSummaryRegion),
	ast.NewField(FieldSummaryVersion),
}

// fieldMap is a lazily loaded mapping from DiagnosticsField to ast.Field
var fieldMap map[DiagnosticsField]*ast.Field
var diagnosticsSummaryFieldMap map[DiagnosticsSummaryField]*ast.Field

func init() {
	fieldMap = make(map[DiagnosticsField]*ast.Field, len(diagnosticsFilterFields))
	for _, f := range diagnosticsFilterFields {
		ff := *f
		fieldMap[DiagnosticsField(ff.Name)] = &ff
	}
	diagnosticsSummaryFieldMap = make(map[DiagnosticsSummaryField]*ast.Field, len(diagnosticsSummaryFilterFields))
	for _, f := range diagnosticsSummaryFilterFields {
		ff := *f
		diagnosticsSummaryFieldMap[DiagnosticsSummaryField(ff.Name)] = &ff
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

func DefaultSummaryFieldByName(field DiagnosticsSummaryField) *ast.Field {
	if af, ok := diagnosticsSummaryFieldMap[field]; ok {
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

func NewDiagnosticsSummaryFilterParser() ast.FilterParser {
	return ast.NewFilterParser(diagnosticsSummaryFilterFields)
}

// use initialization function to assign field types to the ops package helper for programatically
// building v2.1 filters
func init() {
	ops.RegisterDefaultFieldLookup(DefaultFieldByName)
	ops.RegisterDefaultFieldLookup(DefaultSummaryFieldByName)
}
