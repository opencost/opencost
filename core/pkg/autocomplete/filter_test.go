package autocomplete

import (
	"testing"

	"github.com/opencost/opencost/core/pkg/filter/ast"
)

func TestHasFilter(t *testing.T) {
	if HasFilter(nil) {
		t.Fatal("expected nil filter to be treated as no filter")
	}
	if HasFilter(&ast.VoidOp{}) {
		t.Fatal("expected void filter to be treated as no filter")
	}
	if !HasFilter(&ast.EqualOp{
		Left:  ast.Identifier{Field: ast.NewField("cluster")},
		Right: "c1",
	}) {
		t.Fatal("expected non-void filter to be treated as active filter")
	}
}
