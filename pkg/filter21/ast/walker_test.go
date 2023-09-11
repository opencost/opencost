package ast

import (
	"fmt"
)

func ExampleTransformLeaves() {
	originalTree := &AndOp{
		Operands: []FilterNode{
			&EqualOp{
				Left: Identifier{
					Field: &Field{
						Name: "field1",
					},
					Key: "foo",
				},
				Right: "bar",
			},

			&EqualOp{
				Left: Identifier{
					Field: &Field{
						Name: "field2",
					},
				},
				Right: "bar",
			},
		},
	}

	// This transformer applies "Not" to all leaves
	transformFunc := func(node FilterNode) FilterNode {
		switch concrete := node.(type) {
		case *AndOp, *OrOp, *NotOp:
			panic("Leaf transformer should not be called on non-leaf nodes")
		default:
			return &NotOp{Operand: concrete}
		}
	}

	newTree := TransformLeaves(originalTree, transformFunc)
	fmt.Println(ToPreOrderString(newTree))
	// Output:
	// And {
	//   Not {
	//     Equals { Left: field1[foo], Right: bar }
	//   }
	//   Not {
	//     Equals { Left: field2, Right: bar }
	//   }
	// }
}
