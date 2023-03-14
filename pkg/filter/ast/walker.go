package ast

import (
	"fmt"
	"strings"

	"github.com/opencost/opencost/pkg/filter/util"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"
)

// used to apply a title to pipeline
var titleCaser cases.Caser = cases.Title(language.Und, cases.NoLower)

// TraversalState represents the state of the current leaf node in a traversal
// of the filter  Any grouping ops will include an Enter on their first
// occurence, and an Exit when leaving the op state.
type TraversalState int

const (
	// TraversalStateNone is used whenever a binary op leaf node is traversed.
	TraversalStateNone TraversalState = iota

	// TraversalStateEnter is used when a group op leaf node is traversed (and, or, not)
	TraversalStateEnter

	// TraversalStateExit is used wwhen a group op leaf node is popped (and, or, not).
	TraversalStateExit
)

// PreOrderTraversal accepts a root `FilterNode` and calls the f callback on each leaf node it traverses.
// When entering "group" leaf nodes (leaf nodes which contain other leaf nodes), a TraversalStateEnter/Exit
// will be includes to denote each depth. In short, the callback will be executed twice for each "group" op,
// once before entering, and once bofore exiting.
func PreOrderTraversal(node FilterNode, f func(FilterNode, TraversalState)) {
	if node == nil {
		return
	}

	// For group ops, we need to execute the callback with an Enter,
	// recursively call traverse, then execute the callback with an Exit.
	switch n := node.(type) {
	case *NotOp:
		f(node, TraversalStateEnter)
		PreOrderTraversal(n.Operand, f)
		f(node, TraversalStateExit)

	case *AndOp:
		f(node, TraversalStateEnter)
		for _, o := range n.Operands {
			PreOrderTraversal(o, f)
		}
		f(node, TraversalStateExit)

	case *OrOp:
		f(node, TraversalStateEnter)
		for _, o := range n.Operands {
			PreOrderTraversal(o, f)
		}
		f(node, TraversalStateExit)

	// Otherwise, we just linearly traverse
	default:
		f(node, TraversalStateNone)
	}

}

// ToPreOrderString runs a PreOrderTraversal and generates an indented tree structure string
// format for the provided tree root.
func ToPreOrderString(node FilterNode) string {
	var sb strings.Builder
	indent := 0

	printNode := func(n FilterNode, action TraversalState) {
		if action == TraversalStateEnter {
			sb.WriteString(OpStringFor(n, action, indent))
			indent++
		} else if action == TraversalStateExit {
			indent--
			sb.WriteString(OpStringFor(n, action, indent))
		} else {
			sb.WriteString(OpStringFor(n, action, indent))
		}
	}

	PreOrderTraversal(node, printNode)

	return sb.String()
}

// OpStringFor returns a string for the provided leaf node, traversal state, and current
// depth.
func OpStringFor(node FilterNode, traversalState TraversalState, depth int) string {
	prefix := indent(depth)

	if traversalState == TraversalStateExit {
		return prefix + "}\n"
	}

	if traversalState == TraversalStateEnter {
		return prefix + titleCaser.String(string(node.Op())) + " {\n"
	}

	open := prefix + titleCaser.String(string(node.Op())) + " { "

	switch n := node.(type) {
	case *VoidOp:
		open += "Empty }\n"
	case *EqualOp:
		open += fmt.Sprintf("Left: %s, Right: %s }\n", n.Left.String(), n.Right)
	case *ContainsOp:
		open += fmt.Sprintf("Left: %s, Right: %s }\n", n.Left.String(), n.Right)
	case *ContainsPrefixOp:
		open += fmt.Sprintf("Left: %s, Right: %s }\n", n.Left.String(), n.Right)
	case *ContainsSuffixOp:
		open += fmt.Sprintf("Left: %s, Right: %s }\n", n.Left.String(), n.Right)
	default:
		open += "}\n"
	}

	return open
}

// Clone deep copies and returns the AST parameter.
func Clone(filter FilterNode) FilterNode {
	var result FilterNode = &VoidOp{}
	var currentOps *util.Stack[FilterGroup] = util.NewStack[FilterGroup]()

	PreOrderTraversal(filter, func(fn FilterNode, state TraversalState) {
		switch n := fn.(type) {
		case *AndOp:
			if state == TraversalStateEnter {
				currentOps.Push(&AndOp{})
			} else if state == TraversalStateExit {
				if currentOps.Length() > 1 {
					current := currentOps.Pop()
					currentOps.Top().Add(current)
				} else {
					result = currentOps.Pop()
				}
			}
		case *OrOp:
			if state == TraversalStateEnter {
				currentOps.Push(&OrOp{})
			} else if state == TraversalStateExit {
				if currentOps.Length() > 1 {
					current := currentOps.Pop()
					currentOps.Top().Add(current)
				} else {
					result = currentOps.Pop()
				}
			}

		case *NotOp:
			if state == TraversalStateEnter {
				currentOps.Push(&NotOp{})
			} else if state == TraversalStateExit {
				if currentOps.Length() > 1 {
					current := currentOps.Pop()
					currentOps.Top().Add(current)
				} else {
					result = currentOps.Pop()
				}
			}

		case *EqualOp:
			var field Field = *n.Left.Field
			sm := &EqualOp{
				Left: Identifier{
					Field: &field,
					Key:   n.Left.Key,
				},
				Right: n.Right,
			}

			if currentOps.Length() == 0 {
				result = sm
			} else {
				currentOps.Top().Add(sm)
			}

		case *ContainsOp:
			var field Field = *n.Left.Field
			sm := &ContainsOp{
				Left: Identifier{
					Field: &field,
					Key:   n.Left.Key,
				},
				Right: n.Right,
			}

			if currentOps.Length() == 0 {
				result = sm
			} else {
				currentOps.Top().Add(sm)
			}

		case *ContainsPrefixOp:
			var field Field = *n.Left.Field
			sm := &ContainsPrefixOp{
				Left: Identifier{
					Field: &field,
					Key:   n.Left.Key,
				},
				Right: n.Right,
			}

			if currentOps.Length() == 0 {
				result = sm
			} else {
				currentOps.Top().Add(sm)
			}

		case *ContainsSuffixOp:
			var field Field = *n.Left.Field
			sm := &ContainsSuffixOp{
				Left: Identifier{
					Field: &field,
					Key:   n.Left.Key,
				},
				Right: n.Right,
			}

			if currentOps.Length() == 0 {
				result = sm
			} else {
				currentOps.Top().Add(sm)
			}
		}
	})

	return result
}

// returns an 2-space indention for each depth
func indent(depth int) string {
	if depth <= 0 {
		return ""
	}
	return strings.Repeat("  ", depth)
}
