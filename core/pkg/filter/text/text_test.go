package text

import (
	"testing"

	"github.com/opencost/opencost/core/pkg/filter/allocation"
	"github.com/opencost/opencost/core/pkg/filter/ast"
	"github.com/opencost/opencost/core/pkg/filter/transform"
)

var allocCompiler = NewTextCompiler(
	transform.PrometheusKeySanitizePass(),
	transform.UnallocatedReplacementPass(),
)

// AST parser for allocation syntax
var allocParser ast.FilterParser = allocation.NewAllocationFilterParser()

func TestThings(t *testing.T) {
	filter := `namespace:"kubecost" +
label[app]:"cost_analyzer" +
annotation[a1]:"b2" +
cluster:"cluster-one" +
node!:
  "node-123",
  "node-456" +
controllerName:
  "kubecost-cost-analyzer",
  "kubecost-prometheus-server" +
controllerKind!:
  "daemonset",
  "statefulset",
  "job" +
container!:"123-abc_foo" +
pod!:"aaaaaaaaaaaaaaaaaaaaaaaaa" +
services!:"abc123"
`

	tree, err := allocParser.Parse(filter)
	if err != nil {
		t.Fatalf("Unexpected parse error: %s", err)
	}
	t.Logf("%s", ast.ToPreOrderString(tree))

	origFilter, err := allocCompiler.Compile(tree)
	t.Logf("Result: %s", origFilter)
}
