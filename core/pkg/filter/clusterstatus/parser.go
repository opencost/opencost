package clusterstatus

import (
	"github.com/opencost/opencost/core/pkg/filter/ast"
	"github.com/opencost/opencost/core/pkg/filter/ops"
)

// ast filter field map for cluster status
var clusterStatusFilterFields []*ast.Field = []*ast.Field{
	ast.NewField(FieldClusterID),
	ast.NewField(FieldAccount),
	ast.NewField(FieldCloudAccountID),
	ast.NewField(FieldProvider),
}

var fieldMap map[ClusterStatusField]*ast.Field

func init() {
	fieldMap = make(map[ClusterStatusField]*ast.Field, len(clusterStatusFilterFields))
	for _, f := range clusterStatusFilterFields {
		ff := *f
		fieldMap[ClusterStatusField(ff.Name)] = &ff
	}
	ops.RegisterDefaultFieldLookup[ClusterStatusField](DefaultFieldByName)
}

// DefaultFieldByName returns only default cluster status filter fields by name.
func DefaultFieldByName(field ClusterStatusField) *ast.Field {
	if af, ok := fieldMap[field]; ok {
		afcopy := *af
		return &afcopy
	}

	return nil
}

// NewClusterStatusFilterParser creates a new `ast.FilterParser` implementation
// which uses cluster status specific fields
func NewClusterStatusFilterParser() ast.FilterParser {
	return ast.NewFilterParser(clusterStatusFilterFields)
}
