package resourcequota

import (
	"testing"

	"github.com/opencost/opencost/core/pkg/filter/ast"
	"github.com/opencost/opencost/core/pkg/filter/ops"
)

func TestDefaultFieldByName(t *testing.T) {
	var rqField ResourceQuotaField
	var astf *ast.Field

	rqField = FieldResourceQuota
	astf = DefaultFieldByName(rqField)
	if astf.Name != "resourcequota" {
		t.Errorf("expected %s; received %s", "resourcequota", astf.Name)
	}

	rqField = FieldClusterID
	astf = DefaultFieldByName(rqField)
	if astf.Name != "cluster" {
		t.Errorf("expected %s; received %s", "cluster", astf.Name)
	}

	rqField = FieldNamespace
	astf = DefaultFieldByName(rqField)
	if astf.Name != "namespace" {
		t.Errorf("expected %s; received %s", "namespace", astf.Name)
	}

	rqField = FieldNamespaceLabel
	astf = DefaultFieldByName(rqField)
	if astf.Name != "namespaceLabel" {
		t.Errorf("expected %s; received %s", "namespaceLabel", astf.Name)
	}

	rqField = FieldUID
	astf = DefaultFieldByName(rqField)
	if astf.Name != "uid" {
		t.Errorf("expected %s; received %s", "uid", astf.Name)
	}

}

func TestOpsEqWithResourceQuotaField(t *testing.T) {
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

	if equalOp.Left.Field.Name != "cluster" {
		t.Errorf("expected Field.Name to be 'cluster', got '%s'", equalOp.Left.Field.Name)
	}

	if equalOp.Right != "test-cluster" {
		t.Errorf("expected Right to be 'test-cluster', got '%s'", equalOp.Right)
	}
}
