package opencost

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	afilter "github.com/opencost/opencost/core/pkg/filter/allocation"
	"github.com/opencost/opencost/core/pkg/filter/ast"
	"github.com/opencost/opencost/core/pkg/filter/ops"
)

func TestAliasPass(t *testing.T) {
	labelConfig := &LabelConfig{
		DepartmentLabel:  "keydepartment",
		EnvironmentLabel: "keyenvironment",
		OwnerLabel:       "keyowner",
		ProductLabel:     "keyproduct",
		TeamLabel:        "keyteam",
	}

	cases := []struct {
		name     string
		input    ast.FilterNode
		expected ast.FilterNode
	}{
		{
			name: "department equal",
			input: &ast.EqualOp{
				Left: ast.Identifier{
					Field: ast.NewAliasField(afilter.AliasDepartment),
				},
				Right: "x",
			},
			expected: ops.Or(
				ops.And(
					ops.Contains(afilter.FieldLabel, "keydepartment"),
					ops.Eq(ops.WithKey(afilter.FieldLabel, "keydepartment"), "x"),
				),
				ops.And(
					ops.Not(ops.Contains(afilter.FieldLabel, "keydepartment")),
					ops.And(
						ops.Contains(afilter.FieldAnnotation, "keydepartment"),
						ops.Eq(ops.WithKey(afilter.FieldAnnotation, "keydepartment"), "x"),
					),
				),
			),
		},
	}

	for _, c := range cases {
		pass := NewAllocationAliasPass(*labelConfig)

		t.Run(c.name, func(t *testing.T) {
			result, err := pass.Exec(c.input)
			if err != nil {
				t.Fatalf("unexpected error: %s", err)
			}

			if diff := cmp.Diff(c.expected, result); len(diff) > 0 {
				t.Errorf("diff: %s", diff)
			}
		})
	}
}

// TestAllocationMatchCompiler_FilterStrings runs filter strings through the
// same parser and compiler that the /allocation API uses for its `filter`
// query parameter. The cases cover the syntax and examples documented for
// that parameter in docs/swagger.json.
func TestAllocationMatchCompiler_FilterStrings(t *testing.T) {
	costAnalyzer := &Allocation{
		Name: "cost-analyzer",
		Properties: &AllocationProperties{
			Cluster:        "cluster-one",
			Node:           "node-1",
			Namespace:      "production",
			ControllerKind: "deployment",
			Controller:     "cost-analyzer",
			Pod:            "cost-analyzer-abc",
			Container:      "cost-model",
			ProviderID:     "aws:///us-east-1a/i-0abc123",
			Services:       []string{"cost-analyzer-svc"},
			Labels: AllocationLabels{
				"app":                    "cost-analyzer",
				"team":                   "data",
				"app_kubernetes_io_name": "opencost",
			},
			Annotations: AllocationAnnotations{
				"owner": "finance",
			},
		},
	}
	frontend := &Allocation{
		Name: "frontend",
		Properties: &AllocationProperties{
			Namespace: "staging",
			Labels:    AllocationLabels{"app": "frontend", "team": "data"},
		},
	}
	backend := &Allocation{
		Name: "backend",
		Properties: &AllocationProperties{
			Namespace: "dev",
			Labels:    AllocationLabels{"app": "backend"},
		},
	}
	unlabelled := &Allocation{
		Name: "unlabelled",
		Properties: &AllocationProperties{
			Namespace: "production",
		},
	}

	cases := []struct {
		name           string
		filter         string
		shouldMatch    []*Allocation
		shouldNotMatch []*Allocation
	}{
		{
			name:           "label value",
			filter:         `label[app]:"cost-analyzer"`,
			shouldMatch:    []*Allocation{costAnalyzer},
			shouldNotMatch: []*Allocation{frontend, backend, unlabelled},
		},
		{
			name:           "label value is case-sensitive",
			filter:         `label[app]:"Cost-Analyzer"`,
			shouldNotMatch: []*Allocation{costAnalyzer, frontend, backend, unlabelled},
		},
		{
			name:           "label value any of",
			filter:         `label[app]:"frontend","backend"`,
			shouldMatch:    []*Allocation{frontend, backend},
			shouldNotMatch: []*Allocation{costAnalyzer, unlabelled},
		},
		{
			name:           "negated label value matches missing label",
			filter:         `label[app]!:"cost-analyzer"`,
			shouldMatch:    []*Allocation{frontend, backend, unlabelled},
			shouldNotMatch: []*Allocation{costAnalyzer},
		},
		{
			name:           "negated label value none of",
			filter:         `label[app]!:"frontend","backend"`,
			shouldMatch:    []*Allocation{costAnalyzer, unlabelled},
			shouldNotMatch: []*Allocation{frontend, backend},
		},
		{
			name:           "namespace and label",
			filter:         `namespace:"production"+label[team]:"data"`,
			shouldMatch:    []*Allocation{costAnalyzer},
			shouldNotMatch: []*Allocation{frontend, backend, unlabelled},
		},
		{
			name:           "grouped or with and",
			filter:         `(namespace:"dev"|namespace:"staging")+label[team]:"data"`,
			shouldMatch:    []*Allocation{frontend},
			shouldNotMatch: []*Allocation{costAnalyzer, backend, unlabelled},
		},
		{
			// label keys are compared in their Prometheus-sanitized form
			name:           "dotted label key",
			filter:         `label[app.kubernetes.io/name]:"opencost"`,
			shouldMatch:    []*Allocation{costAnalyzer},
			shouldNotMatch: []*Allocation{frontend, backend, unlabelled},
		},
		{
			name:           "sanitized label key",
			filter:         `label[app_kubernetes_io_name]:"opencost"`,
			shouldMatch:    []*Allocation{costAnalyzer},
			shouldNotMatch: []*Allocation{frontend, backend, unlabelled},
		},
		{
			name:           "has label",
			filter:         `label:"team"`,
			shouldMatch:    []*Allocation{costAnalyzer, frontend},
			shouldNotMatch: []*Allocation{backend, unlabelled},
		},
		{
			name:           "does not have label",
			filter:         `label!:"team"`,
			shouldMatch:    []*Allocation{backend, unlabelled},
			shouldNotMatch: []*Allocation{costAnalyzer, frontend},
		},
		{
			name:           "has label key with prefix",
			filter:         `label<~:"te"`,
			shouldMatch:    []*Allocation{costAnalyzer, frontend},
			shouldNotMatch: []*Allocation{backend, unlabelled},
		},
		{
			name:           "unallocated label",
			filter:         `label[team]:"__unallocated__"`,
			shouldMatch:    []*Allocation{backend, unlabelled},
			shouldNotMatch: []*Allocation{costAnalyzer, frontend},
		},
		{
			name:           "label value contains",
			filter:         `label[app]~:"analy"`,
			shouldMatch:    []*Allocation{costAnalyzer},
			shouldNotMatch: []*Allocation{frontend, backend, unlabelled},
		},
		{
			name:           "label value starts with",
			filter:         `label[app]<~:"front"`,
			shouldMatch:    []*Allocation{frontend},
			shouldNotMatch: []*Allocation{costAnalyzer, backend, unlabelled},
		},
		{
			name:           "label value ends with",
			filter:         `label[app]~>:"end"`,
			shouldMatch:    []*Allocation{frontend, backend},
			shouldNotMatch: []*Allocation{costAnalyzer, unlabelled},
		},
		{
			name:           "annotation value",
			filter:         `annotation[owner]:"finance"`,
			shouldMatch:    []*Allocation{costAnalyzer},
			shouldNotMatch: []*Allocation{frontend, backend, unlabelled},
		},
		{
			name:           "services exact name",
			filter:         `services:"cost-analyzer-svc"`,
			shouldMatch:    []*Allocation{costAnalyzer},
			shouldNotMatch: []*Allocation{frontend, backend, unlabelled},
		},
		{
			// on a slice field ~: also means "has this exact element"
			name:           "services contains is exact",
			filter:         `services~:"cost"`,
			shouldNotMatch: []*Allocation{costAnalyzer, frontend, backend, unlabelled},
		},
		{
			name:           "services name starts with",
			filter:         `services<~:"cost"`,
			shouldMatch:    []*Allocation{costAnalyzer},
			shouldNotMatch: []*Allocation{frontend, backend, unlabelled},
		},
		{
			name:           "provider",
			filter:         `provider:"aws:///us-east-1a/i-0abc123"`,
			shouldMatch:    []*Allocation{costAnalyzer},
			shouldNotMatch: []*Allocation{frontend, backend, unlabelled},
		},
		{
			name:           "every scalar field",
			filter:         `cluster:"cluster-one"+node:"node-1"+controllerKind:"deployment"+controllerName:"cost-analyzer"+pod:"cost-analyzer-abc"+container:"cost-model"`,
			shouldMatch:    []*Allocation{costAnalyzer},
			shouldNotMatch: []*Allocation{frontend, backend, unlabelled},
		},
	}

	parser := afilter.NewAllocationFilterParser()
	compiler := NewAllocationMatchCompiler(nil)

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			node, err := parser.Parse(c.filter)
			if err != nil {
				t.Fatalf("parsing filter %s: %s", c.filter, err)
			}

			matcher, err := compiler.Compile(node)
			if err != nil {
				t.Fatalf("compiling filter %s: %s", c.filter, err)
			}

			for _, a := range c.shouldMatch {
				if !matcher.Matches(a) {
					t.Errorf("filter %s: expected %s to match", c.filter, a.Name)
				}
			}
			for _, a := range c.shouldNotMatch {
				if matcher.Matches(a) {
					t.Errorf("filter %s: expected %s not to match", c.filter, a.Name)
				}
			}
		})
	}
}

// TestAllocationFilterParser_InvalidFilterStrings covers filter strings that
// the docs for the /allocation `filter` query parameter call out as invalid.
func TestAllocationFilterParser_InvalidFilterStrings(t *testing.T) {
	cases := map[string]string{
		"aggregate-style label key": `label:app:"cost-analyzer"`,
		"unquoted value":            `label[app]:cost-analyzer`,
		"mixed AND and OR":          `namespace:"dev"+label[team]:"data"|pod:"abc"`,
	}

	parser := afilter.NewAllocationFilterParser()

	for name, filter := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := parser.Parse(filter); err == nil {
				t.Errorf("expected an error parsing %s", filter)
			}
		})
	}
}
