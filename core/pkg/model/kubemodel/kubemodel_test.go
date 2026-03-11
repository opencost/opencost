package kubemodel

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestKubeModel(t *testing.T) {
	start := time.Now().UTC().Truncate(time.Hour)
	end := start.Add(time.Hour)

	t.Run("RegisterError", func(t *testing.T) {
		kms := NewKubeModelSet(start, end)

		require.NotNil(t, kms.Metadata)
		require.Len(t, kms.GetErrors(), 0)

		kms.Error(errors.New("test error"))
		require.Len(t, kms.GetErrors(), 1)
		require.Equal(t, "test error", kms.GetErrors()[0].Message)

		kms.Error(errors.New("test error 2"))
		require.Len(t, kms.GetErrors(), 2)
		require.Equal(t, "test error 2", kms.GetErrors()[1].Message)
	})

	t.Run("RegisterCluster", func(t *testing.T) {
		t.Run("empty cluster UID", func(t *testing.T) {
			var err error

			kms := NewKubeModelSet(start, end)

			err = kms.RegisterCluster(&Cluster{UID: ""})
			require.NotNil(t, err)

			require.Len(t, kms.GetErrors(), 1)
			require.Equal(t, "RegisterCluster: uid is nil", kms.GetErrors()[0].Message)
			require.Nil(t, kms.Cluster)
		})

		t.Run("new cluster UID", func(t *testing.T) {
			var err error
			var clusterUID = "cluster-uid"

			kms := NewKubeModelSet(start, end)

			err = kms.RegisterCluster(&Cluster{UID: clusterUID})
			require.Nil(t, err)

			require.Len(t, kms.GetErrors(), 0)
			require.NotNil(t, kms.Cluster)
			require.Equal(t, clusterUID, kms.Cluster.UID)
		})

		t.Run("multiple Register calls", func(t *testing.T) {
			var err error
			var clusterUID = "cluster-uid"

			kms := NewKubeModelSet(start, end)

			err = kms.RegisterCluster(&Cluster{UID: clusterUID})
			require.Nil(t, err)

			require.Len(t, kms.GetErrors(), 0)
			require.NotNil(t, kms.Cluster)
			require.Equal(t, clusterUID, kms.Cluster.UID)

			// Register cluster with same UID, expect no-op on second try
			err = kms.RegisterCluster(&Cluster{UID: clusterUID})
			require.Nil(t, err)

			require.Len(t, kms.GetErrors(), 0)
			require.NotNil(t, kms.Cluster)
			require.Equal(t, clusterUID, kms.Cluster.UID)

			// Register cluster with another UID (should not happen), expect no-op
			err = kms.RegisterCluster(&Cluster{UID: "another-uid"})
			require.Nil(t, err)

			require.Len(t, kms.GetWarnings(), 1)
			require.Equal(t, "RegisterCluster(another-uid): attempting to change cluster UID from cluster-uid to another-uid", kms.GetWarnings()[0].Message)
			require.NotNil(t, kms.Cluster)
			require.Equal(t, clusterUID, kms.Cluster.UID) // original kms.Cluster is not modified
		})
	})

	t.Run("RegisterNamespace", func(t *testing.T) {
		t.Run("empty namespace UID", func(t *testing.T) {
			var err error

			kms := NewKubeModelSet(start, end)

			err = kms.RegisterNamespace(&Namespace{UID: "", Name: ""})
			require.NotNil(t, err)

			require.Len(t, kms.GetErrors(), 1)
			require.Equal(t, "UID is missing for Namespace with name ''", kms.GetErrors()[0].Message)
			require.Len(t, kms.Namespaces, 0)
		})

		t.Run("register namespace on KMS w/o cluster", func(t *testing.T) {
			var err error

			kms := NewKubeModelSet(start, end)

			testUID := "uid"
			testName := "name"

			err = kms.RegisterNamespace(&Namespace{UID: testUID, Name: testName})
			require.Nil(t, err)

			require.Len(t, kms.GetWarnings(), 1)
			require.Equal(t, "RegisterNamespace: Cluster is nil", kms.GetWarnings()[0].Message)

			testNamespace := &Namespace{UID: testUID, Name: testName}

			require.NotNil(t, kms.Namespaces[testUID])
			require.Equal(t, testNamespace, kms.Namespaces[testUID])
			require.NotNil(t, kms.idx.namespaceByName[testName])
			require.Equal(t, testNamespace, kms.idx.namespaceByName[testName])
			require.Equal(t, 1, kms.Metadata.ObjectCount)
		})

		t.Run("register namespace on KMS w/ cluster", func(t *testing.T) {
			var err error

			kms := NewKubeModelSet(start, end)
			err = kms.RegisterCluster(&Cluster{UID: "cluster-uid"})
			require.Nil(t, err)

			// At this point we have a KMS with a cluster registered

			testUID := "uid"
			testName := "name"

			err = kms.RegisterNamespace(&Namespace{UID: testUID, Name: testName})
			require.Nil(t, err)

			require.Len(t, kms.GetErrors(), 0)
			require.NotNil(t, kms.Namespaces[testUID])

			testNamespace := &Namespace{UID: testUID, Name: testName}

			require.Equal(t, testNamespace, kms.Namespaces[testUID])
			require.Equal(t, testNamespace, kms.idx.namespaceByName[testName])
			require.Equal(t, 1, kms.Metadata.ObjectCount)

			// Register same namespace again, expect no-op on second try
			err = kms.RegisterNamespace(&Namespace{UID: testUID, Name: testName})
			require.Nil(t, err)

			require.Len(t, kms.GetErrors(), 0)
			require.NotNil(t, kms.Namespaces[testUID])
			require.Equal(t, testNamespace, kms.Namespaces[testUID])
			require.Equal(t, testNamespace, kms.idx.namespaceByName[testName])
			require.Equal(t, 1, kms.Metadata.ObjectCount) // remains 1
		})
	})

	t.Run("RegisterResourceQuota", func(t *testing.T) {
		t.Run("empty resourceQuota UID", func(t *testing.T) {
			var err error

			kms := NewKubeModelSet(start, end)

			err = kms.RegisterResourceQuota(&ResourceQuota{UID: "", Name: "test"})
			require.NotNil(t, err)
			require.Len(t, kms.GetErrors(), 1)
			require.Equal(t, "UID is missing for ResourceQuota with name 'test'", kms.GetErrors()[0].Message)
			require.Len(t, kms.ResourceQuotas, 0)
		})

		t.Run("register resource quota with empty NamespaceUID", func(t *testing.T) {
			var err error

			kms := NewKubeModelSet(start, end)

			err = kms.RegisterResourceQuota(&ResourceQuota{UID: "uid", Name: "name", NamespaceUID: ""})
			require.NotNil(t, err)
			require.Len(t, kms.GetErrors(), 1)
			require.Equal(t, "Namespace is missing for ResourceQuota 'uid'", kms.GetErrors()[0].Message)
			require.Len(t, kms.ResourceQuotas, 0)
		})

		t.Run("register resource quota on KMS w/ namespace", func(t *testing.T) {
			kms := NewKubeModelSet(start, end)
			kms.RegisterCluster(&Cluster{UID: "cluster-uid"})
			kms.RegisterNamespace(&Namespace{UID: "namespace-uid", Name: "namespace"})
			// At this point we have a KMS with a cluster and namespace registered

			testUID := "uid"
			testName := "name"

			kms.RegisterResourceQuota(&ResourceQuota{UID: testUID, Name: testName, NamespaceUID: "namespace-uid"})

			testRQ := &ResourceQuota{
				UID:          "uid",
				NamespaceUID: "namespace-uid",
				Name:         "name",
				Spec:         &ResourceQuotaSpec{Hard: &ResourceQuotaSpecHard{}},
				Status:       &ResourceQuotaStatus{Used: &ResourceQuotaStatusUsed{}},
			}

			require.Len(t, kms.GetErrors(), 0)
			require.NotNil(t, kms.ResourceQuotas[testUID])
			require.Equal(t, testRQ, kms.ResourceQuotas[testUID])
			require.Equal(t, 2, kms.Metadata.ObjectCount) // 1 namespace and 1 RQ

			// Register same RQ again, expect no-op on second try
			kms.RegisterResourceQuota(&ResourceQuota{UID: testUID, Name: testName, NamespaceUID: "namespace-uid"})
			require.Len(t, kms.GetErrors(), 0)
			require.NotNil(t, kms.ResourceQuotas[testUID])
			require.Equal(t, testRQ, kms.ResourceQuotas[testUID])
			require.Equal(t, 2, kms.Metadata.ObjectCount) // 1 namespace and 1 RQ
		})

		t.Run("register multiple RQs in multiple namespaces", func(t *testing.T) {
			kms := NewKubeModelSet(start, end)
			kms.RegisterCluster(&Cluster{UID: "cluster-uid"})
			kms.RegisterNamespace(&Namespace{UID: "namespace-1-uid", Name: "namespace-1"})
			kms.RegisterNamespace(&Namespace{UID: "namespace-2-uid", Name: "namespace-2"})

			kms.RegisterResourceQuota(&ResourceQuota{UID: "uid-1", Name: "name-1", NamespaceUID: "namespace-1-uid"})
			kms.RegisterResourceQuota(&ResourceQuota{UID: "uid-2", Name: "name-2", NamespaceUID: "namespace-2-uid"})

			require.Len(t, kms.GetErrors(), 0)
			require.NotNil(t, kms.ResourceQuotas)
			require.Len(t, kms.ResourceQuotas, 2)

			testRQ1 := &ResourceQuota{
				UID:          "uid-1",
				NamespaceUID: "namespace-1-uid",
				Name:         "name-1",
				Spec:         &ResourceQuotaSpec{Hard: &ResourceQuotaSpecHard{}},
				Status:       &ResourceQuotaStatus{Used: &ResourceQuotaStatusUsed{}},
			}
			testRQ2 := &ResourceQuota{
				UID:          "uid-2",
				NamespaceUID: "namespace-2-uid",
				Name:         "name-2",
				Spec:         &ResourceQuotaSpec{Hard: &ResourceQuotaSpecHard{}},
				Status:       &ResourceQuotaStatus{Used: &ResourceQuotaStatusUsed{}},
			}

			require.Equal(t, testRQ1, kms.ResourceQuotas["uid-1"])
			require.Equal(t, testRQ2, kms.ResourceQuotas["uid-2"])
			require.Equal(t, 4, kms.Metadata.ObjectCount) // 2 namespaces and 2 RQs

			// Register a third RQ with empty NamespaceUID — expect error, not registered
			err := kms.RegisterResourceQuota(&ResourceQuota{UID: "uid-3", Name: "name-3", NamespaceUID: ""})
			require.NotNil(t, err)
			require.Len(t, kms.GetErrors(), 1)
			require.Equal(t, "Namespace is missing for ResourceQuota 'uid-3'", kms.GetErrors()[0].Message)
			require.Len(t, kms.ResourceQuotas, 2)          // still 2
			require.Equal(t, 4, kms.Metadata.ObjectCount)  // unchanged
		})
	})
}
