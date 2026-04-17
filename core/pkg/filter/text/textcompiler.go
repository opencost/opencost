package text

import (
	"fmt"
	"strings"

	"github.com/opencost/opencost/core/pkg/filter/ast"
	"github.com/opencost/opencost/core/pkg/filter/transform"
	"github.com/opencost/opencost/core/pkg/filter/util"
)

// TextCompiler is a filter compiler implementation that will compile the filter AST back
// into the filter query text.
type TextCompiler struct {
	passes []transform.CompilerPass
}

// NewTextCompiler creates a new TextCompiler instance that will compile the filter AST
// back into the filter query text, after running all pre-compile transformations.
func NewTextCompiler(passes ...transform.CompilerPass) *TextCompiler {
	return &TextCompiler{
		passes: passes,
	}
}

// Compile accepts an `ast.FilterNode` tree and builds out the filter text that was used to
// build the tree in the first place.
func (tc *TextCompiler) Compile(filter ast.FilterNode) (string, error) {
	// apply compiler passes on parsed ast
	var err error
	filter, err = transform.ApplyAll(filter, tc.passes)
	if err != nil {
		return "", fmt.Errorf("applying compiler passes: %w", err)
	}

	// if the root node is a void op, empty filter
	if _, ok := filter.(*ast.VoidOp); ok {
		return "", nil
	}

	var isContradictionOp bool
	var result TextOp
	var currentOps *util.Stack[TextGroupOp] = util.NewStack[TextGroupOp]()

	// handle leaf is the ast walker func. group ops get pushed onto a stack on
	// the Enter state, and popped on the Exit state. Any ops between Enter and
	// Exit are added to the group. If there are no more groups on the stack after
	// an Exit state, we set the result to the final group.
	handleLeaf := func(leaf ast.FilterNode, state ast.TraversalState) {
		switch n := leaf.(type) {
		case *ast.AndOp:
			if state == ast.TraversalStateEnter {
				currentOps.Push(newGroupOp("+"))
			} else if state == ast.TraversalStateExit {
				if currentOps.Length() > 1 {
					current := currentOps.Pop()
					currentOps.Top().Add(current)
				} else {
					result = currentOps.Pop()
				}
			}
		case *ast.OrOp:
			if state == ast.TraversalStateEnter {
				currentOps.Push(newGroupOp("|"))
			} else if state == ast.TraversalStateExit {
				if currentOps.Length() > 1 {
					current := currentOps.Pop()
					currentOps.Top().Add(current)
				} else {
					result = currentOps.Pop()
				}
			}

		case *ast.NotOp:
			if state == ast.TraversalStateEnter {
				currentOps.Push(newNotOp())
			} else if state == ast.TraversalStateExit {
				if currentOps.Length() > 1 {
					current := currentOps.Pop()
					currentOps.Top().Add(current)
				} else {
					result = currentOps.Pop()
				}
			}
		// Special case here, these can only be created programmatically and
		// don't have a filter variant, but we will represent it as a special
		// string at the end of the compile action
		case *ast.ContradictionOp:
			isContradictionOp = true
			if currentOps.Length() == 0 {
				result = NoOp
			} else {
				currentOps.Top().Add(NoOp)
			}
		case *ast.EqualOp:
			op := newComparisonOp(":", n.Left, n.Right)
			if currentOps.Length() == 0 {
				result = op
			} else {
				currentOps.Top().Add(op)
			}

		case *ast.ContainsOp:
			op := newComparisonOp("~:", n.Left, n.Right)
			if currentOps.Length() == 0 {
				result = op
			} else {
				currentOps.Top().Add(op)
			}

		case *ast.ContainsPrefixOp:
			op := newComparisonOp("<~:", n.Left, n.Right)
			if currentOps.Length() == 0 {
				result = op
			} else {
				currentOps.Top().Add(op)
			}

		case *ast.ContainsSuffixOp:
			op := newComparisonOp("~>:", n.Left, n.Right)
			if currentOps.Length() == 0 {
				result = op
			} else {
				currentOps.Top().Add(op)
			}
		}
	}

	ast.PreOrderTraversal(filter, handleLeaf)

	// if we discover a contraction op, we reject all inputs
	// this isn't able to be expressed via a filter string
	if isContradictionOp {
		return "[all-fail]", nil
	}

	if result == nil {
		return "", nil
	}

	return result.String(), nil

}

//--------------------------------------------------------------------------
//  TextOp Abstractions
//--------------------------------------------------------------------------

// TextOp is just a basic operation that we will generate a string to represent the recreation of the filter from
// the AST.
type TextOp interface {
	String() string
}

// TextGroupOp is a grouping operation like and, or, or not.
type TextGroupOp interface {
	TextOp

	Add(TextOp)
}

//--------------------------------------------------------------------------
//  Ops
//--------------------------------------------------------------------------

const NoOp ContradictionOp = ContradictionOp("")

// ContradictionOp implementation for a filter all operation
type ContradictionOp string

func (no ContradictionOp) String() string { return "" }

// And/Or
type GroupOp struct {
	Symbol string
	Ops    []TextOp
}

// creates a new grouping operation with the op symbol
func newGroupOp(symbol string) *GroupOp {
	return &GroupOp{
		Symbol: symbol,
	}
}

// Add appends a text op as part of the group
func (a *GroupOp) Add(m TextOp) {
	a.Ops = append(a.Ops, m)
}

// generates the group op using the provided symbol
func (a *GroupOp) String() string {
	return writeGroupOp(a.Symbol, a.Ops...)
}

// ComparisonOp is your standard boolean expression used in the filters we need to
// express as merely a symbol and operands.
type ComparisonOp struct {
	Symbol string
	Left   ast.Identifier
	Right  string
}

func newComparisonOp(symbol string, left ast.Identifier, right string) *ComparisonOp {
	return &ComparisonOp{
		Symbol: symbol,
		Left:   left,
		Right:  right,
	}
}

func (a *ComparisonOp) String() string {
	return writeOp(a.Symbol, a.Left, a.Right)
}

// Not is a negation that contains a single op to negate.
type Not struct {
	Op TextOp
}

func newNotOp() *Not {
	return new(Not)
}

func (a *Not) Add(m TextOp) {
	a.Op = m
}

// Because our tree will treat 'foo !: bar' as '!(foo : bar)' we can easily convert back into the originating negation
// depending on the inner op by prepending a '!'
func (a *Not) String() string {
	if a.Op == nil {
		return ""
	}

	switch innerOp := a.Op.(type) {
	case *GroupOp:
		return "!" + writeGroupOp("", innerOp)
	case *ComparisonOp:
		merged := newComparisonOp("!"+innerOp.Symbol, innerOp.Left, innerOp.Right)
		return merged.String()
	}

	return ""
}

//--------------------------------------------------------------------------
//  Helpers
//--------------------------------------------------------------------------

// helper function that writes all of the provided operands with a joining
// operation symbol
func writeGroupOp(op string, operands ...TextOp) string {
	if len(operands) == 0 {
		return ""
	}
	if len(operands) == 1 {
		return operands[0].String()
	}

	sep := fmt.Sprintf(" %s ", op)

	var sb strings.Builder
	sb.WriteRune('(')
	sb.WriteString(operands[0].String())
	for _, f := range operands[1:] {
		sb.WriteString(sep)
		sb.WriteString(f.String())
	}
	sb.WriteRune(')')

	return sb.String()
}

// helper function to generate a basic comparison operation
func writeOp(op string, left ast.Identifier, right string) string {
	var sb strings.Builder
	sb.WriteString(left.String())
	sb.WriteString(op)
	sb.WriteRune('"')
	sb.WriteString(right)
	sb.WriteRune('"')
	return sb.String()
}
