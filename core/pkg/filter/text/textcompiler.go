package text

import (
	"fmt"
	"slices"
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
					current := currentOps.Pop().Close()
					currentOps.Top().Add(current)
				} else {
					result = currentOps.Pop().Close()
				}
			}
		case *ast.OrOp:
			if state == ast.TraversalStateEnter {
				currentOps.Push(newGroupOp("|"))
			} else if state == ast.TraversalStateExit {
				if currentOps.Length() > 1 {
					current := currentOps.Pop().Close()
					currentOps.Top().Add(current)
				} else {
					result = currentOps.Pop().Close()
				}
			}

		case *ast.NotOp:
			if state == ast.TraversalStateEnter {
				currentOps.Push(newNotOp())
			} else if state == ast.TraversalStateExit {
				if currentOps.Length() > 1 {
					current := currentOps.Pop().Close()
					currentOps.Top().Add(current)
				} else {
					result = currentOps.Pop().Close()
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

	// if we discover a contradiction op, we reject all inputs
	// this isn't able to be expressed via a filter string
	if isContradictionOp {
		return "[all-fail]", nil
	}

	if result == nil {
		return "", nil
	}

	// for group ops, trim the root level parens
	strResult := result.String()
	if rootOp, ok := result.(*GroupOp); ok {
		if rootOp.Symbol == "|" || rootOp.Symbol == "+" {
			strResult = strResult[1 : len(strResult)-1]
		}
	}

	return strResult, nil
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

	// Add appends a new operation to the group
	Add(TextOp)

	// Close collapses any inline reductions to the negation or multi-compare operations
	Close() TextOp
}

//--------------------------------------------------------------------------
//  Ops
//--------------------------------------------------------------------------

const NoOp ContradictionOp = ContradictionOp("")

// ContradictionOp implementation for a filter all operation
type ContradictionOp string

func (no ContradictionOp) String() string { return "" }

// And/Or grouping
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
	// this merges identical comparisons - this is a bit of a pre-optimization for Close()
	// that combines ie: (foo:"bar" + foo:"baz") into (foo:"bar","baz")
	if compOp, ok := m.(*ComparisonOp); ok {
		sym := compOp.Symbol
		left := compOp.Left
		r := compOp.Right

		// look for identical symbol and identifiers, also ensure there isn't a repeat
		// value.
		for _, op := range a.Ops {
			if currOp, ook := op.(*ComparisonOp); ook {
				if currOp.Symbol != sym || !currOp.Left.Equal(left) || currOp.Right == r {
					continue
				}
				if slices.Contains(currOp.Other, r) {
					continue
				}

				// found a match, merge comparison operands
				currOp.Other = append(currOp.Other, r)
				return
			}
		}
	}

	a.Ops = append(a.Ops, m)
}

func (a *GroupOp) Close() TextOp {
	if len(a.Ops) == 1 {
		return a.Ops[0]
	}

	return a
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
	Other  []string
}

// creates a new comparison op with a symbol, identifier, and value.
func newComparisonOp(symbol string, left ast.Identifier, right string) *ComparisonOp {
	return &ComparisonOp{
		Symbol: symbol,
		Left:   left,
		Right:  right,
	}
}

func (a *ComparisonOp) String() string {
	return writeOp(a.Symbol, a.Left, a.Right, a.Other...)
}

// NotOp is a negation that contains a single op to negate.
type NotOp struct {
	Op TextOp
}

func newNotOp() *NotOp {
	return new(NotOp)
}

func (a *NotOp) Add(m TextOp) {
	a.Op = m
}

func (a *NotOp) Close() TextOp {
	if a.Op == nil {
		return a
	}

	switch innerOp := a.Op.(type) {
	case *GroupOp:
		return a
	case *ComparisonOp:
		merged := newComparisonOp("!"+innerOp.Symbol, innerOp.Left, innerOp.Right)
		merged.Other = innerOp.Other
		return merged
	}

	return a
}

// Because our tree will treat 'foo !: bar' as '!(foo : bar)' we can easily convert back into the originating negation
// depending on the inner op by prepending a '!'
func (a *NotOp) String() string {
	if a.Op == nil {
		return ""
	}

	switch innerOp := a.Op.(type) {
	case *GroupOp:
		return "!" + writeGroupOp("", innerOp)
	case *ComparisonOp:
		merged := newComparisonOp("!"+innerOp.Symbol, innerOp.Left, innerOp.Right)
		merged.Other = innerOp.Other
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
func writeOp(op string, left ast.Identifier, right string, additional ...string) string {
	var sb strings.Builder
	sb.WriteString(left.String())
	sb.WriteString(op)
	sb.WriteRune('"')
	sb.WriteString(right)
	sb.WriteRune('"')
	for _, other := range additional {
		sb.WriteRune(',')
		sb.WriteRune('"')
		sb.WriteString(other)
		sb.WriteRune('"')
	}
	return sb.String()
}
