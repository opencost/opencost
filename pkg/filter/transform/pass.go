package transform

import "github.com/opencost/opencost/pkg/filter/ast"

// CompilerPass is an interface which defines an implementation capable of
// accepting an input AST and making optimizations or changes, and returning
// a new (or the existing) AST.
type CompilerPass interface {
	// Exec executes the pass on the provided AST. This method may either return
	// a new AST or the existing modified AST. Note that the parameter to this method
	// may be changed directly.
	Exec(filter ast.FilterNode) ast.FilterNode
}

// ApplyAll applies all the compiler passes serially and returns the resulting tree. This
// method copies the passes AST before executing the compiler passes.
func ApplyAll(filter ast.FilterNode, passes []CompilerPass) ast.FilterNode {
	// return the input filter if there are no passes to run
	if len(passes) == 0 {
		return filter
	}

	// Clone the filter first, then apply the passes
	var f ast.FilterNode = ast.Clone(filter)
	for _, pass := range passes {
		f = pass.Exec(f)
	}
	return f
}
