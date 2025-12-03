package resourcequota

import (
	"testing"

	"github.com/opencost/opencost/core/pkg/filter/ast"
)

func TestDefaultFieldByName(t *testing.T) {
	var rqField ResourceQuotaField
	var astf *ast.Field

	rqField = FieldName
	astf = DefaultFieldByName(rqField)
	if astf.Name != "name" {
		t.Errorf("expected %s; received %s", "name", astf.Name)
	}

	rqField = FieldClusterUID
	astf = DefaultFieldByName(rqField)
	if astf.Name != "clusterUID" {
		t.Errorf("expected %s; received %s", "clusterUID", astf.Name)
	}

	rqField = FieldLabel
	astf = DefaultFieldByName(rqField)
	if astf.Name != "label" {
		t.Errorf("expected %s; received %s", "label", astf.Name)
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
