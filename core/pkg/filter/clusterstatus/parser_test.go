package clusterstatus

import (
	"testing"

	"github.com/opencost/opencost/core/pkg/filter/ast"
	"github.com/opencost/opencost/core/pkg/filter/ops"
)

func TestClusterStatusFilterParser(t *testing.T) {
	parser := NewClusterStatusFilterParser()

	testCases := []struct {
		name          string
		filter        string
		expectedField string
		expectedValue string
	}{
		{
			name:          "cluster filter",
			filter:        `cluster:"test-cluster"`,
			expectedField: "cluster",
			expectedValue: "test-cluster",
		},
		{
			name:          "account filter",
			filter:        `account:"test-account"`,
			expectedField: "account",
			expectedValue: "test-account",
		},
		{
			name:          "accountID filter (alias for account)",
			filter:        `accountID:"test-cloud-account"`,
			expectedField: "accountID",
			expectedValue: "test-cloud-account",
		},
		{
			name:          "provider filter",
			filter:        `provider:"AWS"`,
			expectedField: "provider",
			expectedValue: "AWS",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := parser.Parse(tc.filter)
			if err != nil {
				t.Fatalf("failed to parse filter %q: %s", tc.filter, err)
			}

			// Verify the parsed filter is an EqualOp
			equalOp, ok := result.(*ast.EqualOp)
			if !ok {
				t.Fatalf("expected *ast.EqualOp, got %T", result)
			}

			// Verify the field name
			if equalOp.Left.Field == nil {
				t.Fatal("expected Field to be non-nil, got nil")
			}
			if equalOp.Left.Field.Name != tc.expectedField {
				t.Fatalf("expected field name %q, got %q", tc.expectedField, equalOp.Left.Field.Name)
			}

			// Verify the value
			if equalOp.Right != tc.expectedValue {
				t.Fatalf("expected value %q, got %q", tc.expectedValue, equalOp.Right)
			}
		})
	}
}

func TestOpsEqWithClusterStatusField(t *testing.T) {
	clusterFilter := ops.Eq(FieldClusterID, "test-cluster")

	equalOp, ok := clusterFilter.(*ast.EqualOp)
	if !ok {
		t.Fatalf("expected *ast.EqualOp, got %T", clusterFilter)
	}

	if equalOp.Left.Field == nil {
		t.Fatal("expected Field to be non-nil, got nil")
	}

	if equalOp.Left.Field.Name == "" {
		t.Fatal("expected Field.Name to be non-empty, got empty string")
	}

	if equalOp.Left.Field.Name != string(FieldClusterID) {
		t.Fatalf("expected Field.Name to be %q, got %q", FieldClusterID, equalOp.Left.Field.Name)
	}
}

func TestOpsAndWithClusterStatusFields(t *testing.T) {
	filter := ops.And(
		ops.Eq(FieldClusterID, "test-cluster"),
		ops.Eq(FieldAccount, "test-account"),
		ops.Eq(FieldProvider, "AWS"),
	)

	andOp, ok := filter.(*ast.AndOp)
	if !ok {
		t.Fatalf("expected *ast.AndOp, got %T", filter)
	}

	if len(andOp.Operands) != 3 {
		t.Fatalf("expected 3 operands, got %d", len(andOp.Operands))
	}

	// Verify each operand is an EqualOp with a valid field
	for i, operand := range andOp.Operands {
		equalOp, ok := operand.(*ast.EqualOp)
		if !ok {
			t.Fatalf("operand %d: expected *ast.EqualOp, got %T", i, operand)
		}

		if equalOp.Left.Field == nil {
			t.Fatalf("operand %d: expected Field to be non-nil, got nil", i)
		}

		if equalOp.Left.Field.Name == "" {
			t.Fatalf("operand %d: expected Field.Name to be non-empty, got empty string", i)
		}
	}
}

func TestDefaultFieldByName(t *testing.T) {
	testCases := []struct {
		name          string
		field         ClusterStatusField
		expectNil     bool
		expectedField string
	}{
		{
			name:          "valid cluster field",
			field:         FieldClusterID,
			expectNil:     false,
			expectedField: "cluster",
		},
		{
			name:          "valid account field",
			field:         FieldAccount,
			expectNil:     false,
			expectedField: "account",
		},
		{
			name:          "valid accountID field",
			field:         FieldCloudAccountID,
			expectNil:     false,
			expectedField: "accountID",
		},
		{
			name:          "valid provider field",
			field:         FieldProvider,
			expectNil:     false,
			expectedField: "provider",
		},
		{
			name:      "invalid field returns nil",
			field:     ClusterStatusField("invalid-field"),
			expectNil: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := DefaultFieldByName(tc.field)

			if tc.expectNil {
				if result != nil {
					t.Fatalf("expected nil for invalid field, got %v", result)
				}
			} else {
				if result == nil {
					t.Fatalf("expected non-nil field, got nil")
				}
				if result.Name != tc.expectedField {
					t.Fatalf("expected field name %q, got %q", tc.expectedField, result.Name)
				}
			}
		})
	}
}
