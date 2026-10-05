package networkinsight

import (
	"fmt"
	"testing"

	"github.com/opencost/opencost/core/pkg/filter/ast"
)

var parser ast.FilterParser = NewNetworkInsightFilterParser()

func TestParse(t *testing.T) {
	cases := []struct {
		name  string
		input string
	}{
		{
			name:  "cluster",
			input: `cluster:"cluster-one"`,
		},
		{
			name:  "namespace",
			input: `namespace:"kubecost"`,
		},
		{
			name:  "pod",
			input: `pod:"my-pod"`,
		},
		{
			name:  "account",
			input: `account:"aws-account-123"`,
		},
		{
			name:  "account negation",
			input: `account!:"aws-account-123"`,
		},
		{
			name:  "account multi-value",
			input: `account:"aws-account-123","gcp-account-456"`,
		},
		{
			name:  "account and namespace",
			input: `account:"aws-account-123" + namespace:"kubecost"`,
		},
		{
			name:  "account or cluster",
			input: `account:"aws-account-123" | cluster:"cluster-one"`,
		},
	}

	for i, c := range cases {
		t.Run(fmt.Sprintf("%d:%s", i, c.name), func(t *testing.T) {
			t.Logf("Query: %s", c.input)
			tree, err := parser.Parse(c.input)
			if err != nil {
				t.Fatalf("Unexpected parse error: %s", err)
			}
			t.Logf("%s", ast.ToPreOrderString(tree))
		})
	}
}

func TestFailingParses(t *testing.T) {
	cases := []struct {
		name  string
		input string
	}{
		{
			name:  "unknown field",
			input: `services:"foo"`,
		},
		{
			name:  "invalid op",
			input: `account.:"aws-account-123"`,
		},
	}

	for i, c := range cases {
		t.Run(fmt.Sprintf("%d:%s", i, c.name), func(t *testing.T) {
			t.Logf("Query: %s", c.input)
			_, err := parser.Parse(c.input)
			if err == nil {
				t.Fatalf("Expected parse error for input: %s", c.input)
			}
			t.Logf("Error (expected): %s", err)
		})
	}
}

func TestDefaultFieldByName(t *testing.T) {
	for _, field := range []NetworkInsightField{FieldClusterID, FieldNamespace, FieldPod, FieldAccount} {
		f := DefaultFieldByName(field)
		if f == nil {
			t.Errorf("DefaultFieldByName(%q) returned nil", field)
		}
		if f != nil && f.Name != string(field) {
			t.Errorf("DefaultFieldByName(%q): got Name %q", field, f.Name)
		}
	}

	if f := DefaultFieldByName("nonexistent"); f != nil {
		t.Errorf("DefaultFieldByName(nonexistent) expected nil, got %+v", f)
	}
}
