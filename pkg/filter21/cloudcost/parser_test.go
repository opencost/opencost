package cloudcost

import (
	"fmt"
	"testing"

	"github.com/opencost/opencost/pkg/filter21/ast"
)

var parser ast.FilterParser = NewCloudCostFilterParser()

func TestParse(t *testing.T) {
	cases := []struct {
		name  string
		input string
	}{
		{
			name: "Empty",
			input: `
			
			`,
		},
		{
			name:  "Single",
			input: `service: "AmazonEC2"`,
		},
		{
			name:  "Single Group",
			input: `(service: "AmazonEC2")`,
		},
		{
			name:  "Double Group",
			input: `((service: "AmazonEC2"))`,
		},
		{
			name:  "Double And",
			input: `(service~: "foo" + provider: "AWS")`,
		},
	}

	for idx, tCase := range cases {
		t.Run(fmt.Sprintf("%d:%s", idx, tCase.name), func(t *testing.T) {
			t.Logf("Query: %s", tCase.input)
			tree, err := parser.Parse(tCase.input)
			if err != nil {
				t.Fatalf("Unexpected parse error: %s", err)
			}
			t.Logf("%s", ast.ToPreOrderString(tree))
		})
	}
}
