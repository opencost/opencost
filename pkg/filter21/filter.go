package filter

import "github.com/opencost/opencost/pkg/filter21/ast"

// Filter is just the root node of an AST. There are various compiler implementations
// available to create data source specific filtering from the AST.
type Filter = ast.FilterNode
