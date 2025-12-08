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

			err = kms.RegisterCluster("")
			require.NotNil(t, err)

			require.Len(t, kms.GetErrors(), 1)
			require.Equal(t, "RegisterCluster: uid is nil", kms.GetErrors()[0].Message)
			require.Nil(t, kms.Cluster)
		})

		t.Run("new cluster UID", func(t *testing.T) {
			var err error
			var clusterUID = "cluster-uid"

			kms := NewKubeModelSet(start, end)

			err = kms.RegisterCluster(clusterUID)
			require.Nil(t, err)

			require.Len(t, kms.GetErrors(), 0)
			require.NotNil(t, kms.Cluster)
			require.Equal(t, clusterUID, kms.Cluster.UID)
		})

		t.Run("multiple Register calls", func(t *testing.T) {
			var err error
			var clusterUID = "cluster-uid"

			kms := NewKubeModelSet(start, end)

			err = kms.RegisterCluster(clusterUID)
			require.Nil(t, err)

			require.Len(t, kms.GetErrors(), 0)
			require.NotNil(t, kms.Cluster)
			require.Equal(t, clusterUID, kms.Cluster.UID)

			// Register cluster with same UID, expect no-op on second try
			err = kms.RegisterCluster(clusterUID)
			require.Nil(t, err)

			require.Len(t, kms.GetErrors(), 0)
			require.NotNil(t, kms.Cluster)
			require.Equal(t, clusterUID, kms.Cluster.UID)

			// Register cluster with another UID (should not happen), expect no-op
			err = kms.RegisterCluster("another-uid")
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

			err = kms.RegisterNamespace("", "")
			require.NotNil(t, err)

			require.Len(t, kms.GetErrors(), 1)
			require.Equal(t, "UID is nil for Namespace ''", kms.GetErrors()[0].Message)
			require.Len(t, kms.Namespaces, 0)
		})

		t.Run("register namespace on KMS w/o cluster", func(t *testing.T) {
			var err error

			kms := NewKubeModelSet(start, end)

			testUID := "uid"
			testName := "name"

			err = kms.RegisterNamespace(testUID, testName)
			require.Nil(t, err)

			require.Len(t, kms.GetWarnings(), 1)
			require.Equal(t, "RegisterNamespace(uid, name): Cluster is nil", kms.GetWarnings()[0].Message)

			testNamespace := &Namespace{UID: testUID, ClusterUID: "", Name: testName}

			require.NotNil(t, kms.Namespaces[testUID])
			require.Equal(t, testNamespace, kms.Namespaces[testUID])
			require.NotNil(t, kms.idx.namespaceByName[testName])
			require.Equal(t, testNamespace, kms.idx.namespaceByName[testName])
			require.Equal(t, 1, kms.Metadata.ObjectCount)
		})

		t.Run("register namespace on KMS w/ cluster", func(t *testing.T) {
			var err error

			kms := NewKubeModelSet(start, end)
			err = kms.RegisterCluster("cluster-uid")
			require.Nil(t, err)

			// At this point we have a KMS with a cluster registered

			testUID := "uid"
			testName := "name"

			err = kms.RegisterNamespace(testUID, testName)
			require.Nil(t, err)

			require.Len(t, kms.GetErrors(), 0)
			require.NotNil(t, kms.Namespaces[testUID])

			testNamespace := &Namespace{UID: testUID, ClusterUID: "cluster-uid", Name: testName}

			require.Equal(t, testNamespace, kms.Namespaces[testUID])
			require.Equal(t, testNamespace, kms.idx.namespaceByName[testName])
			require.Equal(t, 1, kms.Metadata.ObjectCount)

			// Register same namespace again, expect no-op on second try
			err = kms.RegisterNamespace(testUID, testName)
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

			err = kms.RegisterResourceQuota("", "test", "")
			require.NotNil(t, err)
			require.Len(t, kms.GetErrors(), 1)
			require.Equal(t, "UID is nil for ResourceQuota 'test'", kms.GetErrors()[0].Message)
			require.Len(t, kms.ResourceQuotas, 0)
		})

		t.Run("register resource quota on KMS w/o namespace", func(t *testing.T) {
			var err error

			kms := NewKubeModelSet(start, end)

			testUID := "uid"
			testName := "name"

			err = kms.RegisterResourceQuota(testUID, testName, "unregistered-namespace")
			require.Nil(t, err)
			require.Len(t, kms.GetWarnings(), 1)
			require.Equal(t, "RegisterResourceQuota(uid, name, unregistered-namespace): missing namespace", kms.GetWarnings()[0].Message)

			testRQ := &ResourceQuota{
				UID:          "uid",
				NamespaceUID: "",
				Name:         "name",
				Spec:         &ResourceQuotaSpec{Hard: &ResourceQuotaSpecHard{}},
				Status:       &ResourceQuotaStatus{Used: &ResourceQuotaStatusUsed{}},
			}

			require.NotNil(t, kms.ResourceQuotas[testUID])
			require.Equal(t, testRQ, kms.ResourceQuotas[testUID])
			require.Equal(t, 1, kms.Metadata.ObjectCount)
		})

		t.Run("register resource quota on KMS w/ namespace", func(t *testing.T) {
			kms := NewKubeModelSet(start, end)
			kms.RegisterCluster("cluster-uid")
			kms.RegisterNamespace("namespace-uid", "namespace")
			// At this point we have a KMS with a cluster and namespace registered

			testUID := "uid"
			testName := "name"
			testNamespace := "namespace" // Register RQ in namespace that was already registered

			kms.RegisterResourceQuota(testUID, testName, testNamespace)

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
			kms.RegisterResourceQuota(testUID, testName, testNamespace)
			require.Len(t, kms.GetErrors(), 0)
			require.NotNil(t, kms.ResourceQuotas[testUID])
			require.Equal(t, testRQ, kms.ResourceQuotas[testUID])
			require.Equal(t, 2, kms.Metadata.ObjectCount) // 1 namespace and 1 RQ
		})

		t.Run("register multiple RQs in multiple namespaces", func(t *testing.T) {
			kms := NewKubeModelSet(start, end)
			kms.RegisterCluster("cluster-uid")
			kms.RegisterNamespace("namespace-1-uid", "namespace-1")
			kms.RegisterNamespace("namespace-2-uid", "namespace-2")

			kms.RegisterResourceQuota("uid-1", "name-1", "namespace-1")
			kms.RegisterResourceQuota("uid-2", "name-2", "namespace-2")

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

			// Register a third RQ with an invalid namespace
			kms.RegisterResourceQuota("uid-3", "name-3", "namespace-3")

			require.Len(t, kms.GetWarnings(), 1)
			require.Equal(t, "RegisterResourceQuota(uid-3, name-3, namespace-3): missing namespace", kms.GetWarnings()[0].Message)

			testRQ3 := &ResourceQuota{
				UID:          "uid-3",
				NamespaceUID: "",
				Name:         "name-3",
				Spec:         &ResourceQuotaSpec{Hard: &ResourceQuotaSpecHard{}},
				Status:       &ResourceQuotaStatus{Used: &ResourceQuotaStatusUsed{}},
			}

			require.Len(t, kms.ResourceQuotas, 3)
			require.NotNil(t, kms.ResourceQuotas["uid-3"])
			require.Equal(t, testRQ3, kms.ResourceQuotas["uid-3"])
			require.Equal(t, 5, kms.Metadata.ObjectCount) // 2 namespaces and 3 RQs
		})
	})
}
