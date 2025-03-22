package networkinsight

import (
	"github.com/opencost/opencost/core/pkg/filter/ast"
	"github.com/opencost/opencost/core/pkg/filter/ops"
)

// ast filter field map for network insights
var networkInsightFilterFields []*ast.Field = []*ast.Field{
	ast.NewField(FieldClusterID),
	ast.NewField(FieldNamespace),
	ast.NewField(FieldPod),
}

var fieldMap map[NetworkInsightField]*ast.Field

func init() {
	fieldMap = make(map[NetworkInsightField]*ast.Field, len(networkInsightFilterFields))
	for _, f := range networkInsightFilterFields {
		ff := *f
		fieldMap[NetworkInsightField(ff.Name)] = &ff
	}
}

// DefaultFieldByName returns only default network insight filter fields by name.
func DefaultFieldByName(field NetworkInsightField) *ast.Field {
	if af, ok := fieldMap[field]; ok {
		afcopy := *af
		return &afcopy
	}

	return nil
}

// NewNetworkInsightFilterParser creates a new `ast.FilterParser` implementation
// which uses network insight specific fields
func NewNetworkInsightFilterParser() ast.FilterParser {
	return ast.NewFilterParser(networkInsightFilterFields)
}

var networkInsightDetailFilterFields []*ast.Field = []*ast.Field{
	ast.NewField(FieldEndPoint),
}

var networkInsightDetailFieldMap map[NetworkInsightDetailField]*ast.Field

// DefaultFieldByName returns only default network insight filter fields by name.
func DefaultNetworkInsightDetailFieldByName(field NetworkInsightDetailField) *ast.Field {
	if networkInsightDetailFieldMap == nil {
		networkInsightDetailFieldMap = make(map[NetworkInsightDetailField]*ast.Field, len(networkInsightDetailFilterFields))
		for _, f := range networkInsightDetailFilterFields {
			ff := *f
			networkInsightDetailFieldMap[NetworkInsightDetailField(ff.Name)] = &ff
		}
	}

	if af, ok := networkInsightDetailFieldMap[field]; ok {
		afcopy := *af
		return &afcopy
	}

	return nil
}

// NewNetworkInsightDetailFilterParser creates a new `ast.FilterParser` implementation
// which uses network insight details specific fields
func NewNetworkInsightDetailFilterParser() ast.FilterParser {
	return ast.NewFilterParser(networkInsightDetailFilterFields)
}

// use initialization function to assign field types to the ops package helper for programatically
// building v2.1 filters
func init() {
	ops.RegisterDefaultFieldLookup[NetworkInsightField](DefaultFieldByName)
	ops.RegisterDefaultFieldLookup[NetworkInsightDetailField](DefaultNetworkInsightDetailFieldByName)
}
