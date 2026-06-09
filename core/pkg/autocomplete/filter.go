package autocomplete

import (
	"github.com/opencost/opencost/core/pkg/filter"
	"github.com/opencost/opencost/core/pkg/filter/ast"
)

// HasFilter reports whether the request carries a user-provided filter.
// Omitted or empty filters normalize to VoidOp and return false.
func HasFilter(f filter.Filter) bool {
	return f != nil && f.Op() != ast.FilterOpVoid
}
