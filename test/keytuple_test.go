package test

import (
	"strings"
	"testing"

	"github.com/opencost/opencost/pkg/costmodel"
)

func TestKeyTupleSplit(t *testing.T) {
	const (
		ns        = "kubecost"
		key       = "my-pod"
		clusterID = "cluster-one"
		fullKey   = "kubecost,my-pod,cluster-one"
	)

	kt, err := costmodel.NewKeyTuple(fullKey)
	if err != nil {
		t.Errorf("Error: %s\n", err)
		return
	}

	t.Logf("Namespace: %s, Key: %s, ClusterID: %s\n", kt.Namespace(), kt.Key(), kt.ClusterID())

	if !strings.EqualFold(kt.Namespace(), ns) {
		t.Errorf("Namespace: \"%s\" != \"%s\"", kt.Namespace(), ns)
		return
	}
	if !strings.EqualFold(kt.Key(), key) {
		t.Errorf("Key: \"%s\" != \"%s\"\n", kt.Key(), key)
		return
	}
	if !strings.EqualFold(kt.ClusterID(), clusterID) {
		t.Errorf("ClusterID: \"%s\" != \"%s\"\n", kt.ClusterID(), clusterID)
		return
	}
}

func TestKeyTupleSingleFail(t *testing.T) {
	_, err := costmodel.NewKeyTuple("foo")
	if err == nil {
		t.Errorf("Error was non-nil for single element!")
		return
	}
}

func TestKeyTupleDoubleFail(t *testing.T) {
	_, err := costmodel.NewKeyTuple("foo,bar")
	if err == nil {
		t.Errorf("Error was non-nil for two elements!")
		return
	}
}

func TestKeyTupleMoreThanThreeFail(t *testing.T) {
	_, err := costmodel.NewKeyTuple("foo,bar,fizz,buzz")
	if err == nil {
		t.Errorf("Error was non-nil for two elements!")
		return
	}
}

func TestOnlyCommas(t *testing.T) {
	kt, err := costmodel.NewKeyTuple(",,")
	if err != nil {
		t.Errorf("Error: %s\n", err)
		return
	}

	t.Logf("Namespace: \"%s\", Key: \"%s\", ClusterID: \"%s\"\n", kt.Namespace(), kt.Key(), kt.ClusterID())

	if !strings.EqualFold(kt.Namespace(), "") {
		t.Errorf("Namespace: \"%s\" != \"%s\"", kt.Namespace(), "")
		return
	}
	if !strings.EqualFold(kt.Key(), "") {
		t.Errorf("Key: \"%s\" != \"%s\"\n", kt.Key(), "")
		return
	}
	if !strings.EqualFold(kt.ClusterID(), "") {
		t.Errorf("ClusterID: \"%s\" != \"%s\"\n", kt.ClusterID(), "")
		return
	}
}

func TestManyEntrys(t *testing.T) {
	_, err := costmodel.NewKeyTuple("a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p")
	if err == nil {
		t.Errorf("Error was non-nil for single element!")
		return
	}
}
