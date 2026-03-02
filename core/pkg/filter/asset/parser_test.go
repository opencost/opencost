package asset

import (
	"fmt"
	"testing"

	"github.com/opencost/opencost/core/pkg/filter/ast"
)

var parser ast.FilterParser = NewAssetFilterParser()

func TestAssetFilterParser_AccountID(t *testing.T) {
	cases := []struct {
		name  string
		input string
	}{
		{
			name:  "AccountID Equals",
			input: `accountID: "123456789"`,
		},
		{
			name:  "AccountID Not Equals",
			input: `accountID!: "123456789"`,
		},
		{
			name:  "AccountID Contains",
			input: `accountID~: "12345"`,
		},
		{
			name:  "AccountID Not Contains",
			input: `accountID!~: "12345"`,
		},
		{
			name:  "AccountID Multiple Values",
			input: `accountID: "123456789", "987654321"`,
		},
		{
			name:  "AccountID With Other Fields",
			input: `accountID: "123456789" + cluster: "cluster-one"`,
		},
		{
			name:  "AccountID Or Account",
			input: `accountID: "123456789" | account: "my-account"`,
		},
		{
			name:  "Complex Query With AccountID",
			input: `(accountID: "123456789" + provider: "AWS") | (accountID: "987654321" + provider: "Azure")`,
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

func TestAssetFilterParser_AllFields(t *testing.T) {
	cases := []struct {
		name  string
		input string
	}{
		{
			name:  "Single Field",
			input: `cluster: "cluster-one"`,
		},
		{
			name:  "Provider Field",
			input: `provider: "AWS"`,
		},
		{
			name:  "Account Field",
			input: `account: "my-account"`,
		},
		{
			name:  "AccountID Field",
			input: `accountID: "123456789"`,
		},
		{
			name:  "Service Field",
			input: `service: "AmazonEC2"`,
		},
		{
			name:  "Label Field",
			input: `label[app]: "cost-analyzer"`,
		},
		{
			name:  "Multiple Fields",
			input: `cluster: "cluster-one" + provider: "AWS" + accountID: "123456789"`,
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

// Made with Bob
