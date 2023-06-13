package ops_test

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/opencost/opencost/pkg/filter21/allocation"
	"github.com/opencost/opencost/pkg/filter21/ast"
	"github.com/opencost/opencost/pkg/filter21/ops"
)

func TestBasicOpsBuilder(t *testing.T) {
	parser := allocation.NewAllocationFilterParser()

	filterTree := ops.And(
		ops.Or(
			ops.Eq(allocation.AllocationFieldNamespace, "kubecost"),
			ops.Eq(allocation.AllocationFieldClusterID, "cluster-one"),
		),
		ops.NotContains(allocation.AllocationFieldServices, "service-a"),
		ops.NotEq(ops.WithKey(allocation.AllocationFieldLabel, "app"), "cost-analyzer"),
		ops.Contains(allocation.AllocationFieldLabel, "foo"),
	)

	otherTree, err := parser.Parse(`
		(namespace: "kubecost" | cluster: "cluster-one") +
		services!~:"service-a" +
		label[app]!: "cost-analyzer" +
		label~:"foo"
	`)

	if err != nil {
		t.Fatal(err)
	}

	if !cmp.Equal(filterTree, otherTree) {
		t.Fatalf("Filter Trees are not equal: %s", cmp.Diff(filterTree, otherTree))
	}
}

func TestLongFormComparison(t *testing.T) {
	filterTree := ops.And(
		ops.Or(
			ops.Eq(allocation.AllocationFieldNamespace, "kubecost"),
			ops.Eq(allocation.AllocationFieldClusterID, "cluster-one"),
		),
		ops.NotContains(allocation.AllocationFieldServices, "service-a"),
		ops.NotEq(ops.WithKey(allocation.AllocationFieldLabel, "app"), "cost-analyzer"),
		ops.Contains(allocation.AllocationFieldLabel, "foo"),
	)

	comparisonTree := &ast.AndOp{
		Operands: []ast.FilterNode{
			&ast.OrOp{
				Operands: []ast.FilterNode{
					&ast.EqualOp{
						Left: ast.Identifier{
							Field: ast.NewField(allocation.AllocationFieldNamespace),
							Key:   "",
						},
						Right: "kubecost",
					},
					&ast.EqualOp{
						Left: ast.Identifier{
							Field: ast.NewField(allocation.AllocationFieldClusterID),
							Key:   "",
						},
						Right: "cluster-one",
					},
				},
			},
			&ast.NotOp{
				Operand: &ast.ContainsOp{
					Left: ast.Identifier{
						Field: ast.NewSliceField(allocation.AllocationFieldServices),
						Key:   "",
					},
					Right: "service-a",
				},
			},
			&ast.NotOp{
				Operand: &ast.EqualOp{
					Left: ast.Identifier{
						Field: ast.NewMapField(allocation.AllocationFieldLabel),
						Key:   "app",
					},
					Right: "cost-analyzer",
				},
			},
			&ast.ContainsOp{
				Left: ast.Identifier{
					Field: ast.NewMapField(allocation.AllocationFieldLabel),
					Key:   "",
				},
				Right: "foo",
			},
		},
	}

	if !cmp.Equal(filterTree, comparisonTree) {
		t.Fatalf("Filter Trees are not equal: %s", cmp.Diff(filterTree, comparisonTree))
	}
}
