package resourcequota

import "github.com/opencost/opencost/core/pkg/filter/ast"

var resourceQuotaFilterFields []*ast.Field = []*ast.Field{
	ast.NewField(FieldClusterID),
	ast.NewField(FieldResourceQuota),
	ast.NewField(FieldNamespace),
	ast.NewMapField(FieldNamespaceLabel),
	ast.NewField(FieldUID),
}

// fieldMap is a lazily loaded mapping from ResourceQuotaField to ast.Field
var fieldMap map[ResourceQuotaField]*ast.Field

func init() {
	fieldMap = make(map[ResourceQuotaField]*ast.Field, len(resourceQuotaFilterFields))
	for _, f := range resourceQuotaFilterFields {
		ff := *f
		fieldMap[ResourceQuotaField(ff.Name)] = &ff
	}
}

// DefaultFieldByName returns only default resource quota filter fields by name.
func DefaultFieldByName(field ResourceQuotaField) *ast.Field {
	if af, ok := fieldMap[field]; ok {
		afcopy := *af
		return &afcopy
	}

	return nil
}

// NewResourceQuotaFilterParser creates a new `ast.FilterParser` implementation
// which uses resource quota specific fields
func NewResourceQuotaFilterParser() ast.FilterParser {
	return ast.NewFilterParser(resourceQuotaFilterFields)
}
