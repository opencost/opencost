package kubemodel

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestKubeModelMarshalBinary(t *testing.T) {
	s := time.Now().UTC().Truncate(time.Hour)
	e := s.Add(time.Hour)

	// Test empty KubeModelSet

	kms := NewKubeModelSet(s, e)

	b, err := kms.MarshalBinary()
	require.NoError(t, err)

	var act = new(KubeModelSet)
	err = act.UnmarshalBinary(b)
	require.NoError(t, err)

	require.Equal(t, kms.Metadata, act.Metadata)
	require.Equal(t, kms.Window, act.Window)
	require.Equal(t, kms.Cluster, act.Cluster)
	require.Equal(t, kms.Namespaces, act.Namespaces)
	require.Equal(t, kms.ResourceQuotas, act.ResourceQuotas)

	// Test non-empty KubeModelSet

	kms = NewKubeModelSet(s, e)
	kms.RegisterCluster("cluster")
	kms.RegisterNamespace("ns1", "ns1")
	kms.RegisterNamespace("ns2", "ns2")
	kms.RegisterResourceQuota("rq1", "rq1", "ns1")
	kms.RegisterResourceQuota("rq2", "rq2", "ns1")
	kms.RegisterResourceQuota("rq3", "rq3", "ns2")
	kms.RegisterResourceQuota("rq4", "rq4", "ns2")

	b, err = kms.MarshalBinary()
	require.NoError(t, err)

	act = new(KubeModelSet)
	err = act.UnmarshalBinary(b)
	require.NoError(t, err)

	require.Equal(t, kms.Metadata, act.Metadata)
	require.Equal(t, kms.Window, act.Window)
	require.Equal(t, kms.Cluster, act.Cluster)
	require.Equal(t, kms.Namespaces, act.Namespaces)
	require.Equal(t, kms.ResourceQuotas, act.ResourceQuotas)
}
