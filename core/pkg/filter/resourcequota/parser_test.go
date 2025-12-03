package resourcequota

import (
	"testing"

	"github.com/opencost/opencost/core/pkg/filter/ast"
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
