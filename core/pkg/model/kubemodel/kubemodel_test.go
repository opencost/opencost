package kubemodel

import (
<<<<<<< HEAD
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestKubeModel(t *testing.T) {
	start := time.Now().UTC().Truncate(time.Hour)
	end := start.Add(time.Hour)

	t.Run("RegisterError", func(t *testing.T) {
		kms := NewKubeModelSet(start, end)

		require.NotNil(t, kms.Metadata)
		require.Len(t, kms.Metadata.Errors, 0)

		kms.RegisterError("test error")
		require.Len(t, kms.Metadata.Errors, 1)
		require.Equal(t, "test error", kms.Metadata.Errors[0])

		kms.RegisterError("test error 2")
		require.Len(t, kms.Metadata.Errors, 2)
		require.Equal(t, "test error 2", kms.Metadata.Errors[1])
	})

	t.Run("RegisterCluster", func(t *testing.T) {
		t.Run("empty cluster UID", func(t *testing.T) {
			kms := NewKubeModelSet(start, end)

			kms.RegisterCluster("")
			require.Len(t, kms.Metadata.Errors, 1)
			require.Equal(t, "RegisterCluster: uid is nil for Cluster", kms.Metadata.Errors[0])
			require.Nil(t, kms.Cluster)
		})

		t.Run("new cluster UID", func(t *testing.T) {
			kms := NewKubeModelSet(start, end)

			testUID := "test-uid"
			kms.RegisterCluster(testUID)
			require.Len(t, kms.Metadata.Errors, 0)
			require.NotNil(t, kms.Cluster)
			require.Equal(t, testUID, kms.Cluster.UID)
		})

		t.Run("multiple Register calls", func(t *testing.T) {
			kms := NewKubeModelSet(start, end)

			testUID := "test-uid"
			kms.RegisterCluster(testUID)
			require.Len(t, kms.Metadata.Errors, 0)
			require.NotNil(t, kms.Cluster)
			require.Equal(t, testUID, kms.Cluster.UID)

			// Register cluster with same UID, expect no-op on second try
			kms.RegisterCluster(testUID)
			require.Len(t, kms.Metadata.Errors, 0)
			require.NotNil(t, kms.Cluster)
			require.Equal(t, testUID, kms.Cluster.UID)

			// Register cluster with another UID (should not happen), expect no-op
			kms.RegisterCluster("another-uid")
			require.Len(t, kms.Metadata.Errors, 0)
			require.NotNil(t, kms.Cluster)
			require.Equal(t, testUID, kms.Cluster.UID) // original kms.Cluster is not modified
		})
	})

	t.Run("RegisterNamespace", func(t *testing.T) {
		t.Run("empty namespace UID", func(t *testing.T) {
			kms := NewKubeModelSet(start, end)

			kms.RegisterNamespace("", "")
			require.Len(t, kms.Metadata.Errors, 1)
			require.Equal(t, "RegisterNamespace: uid is nil for Namespace ''", kms.Metadata.Errors[0])
			require.Len(t, kms.Namespaces, 0)
		})

		t.Run("register namespace on KMS w/o cluster", func(t *testing.T) {
			kms := NewKubeModelSet(start, end)

			testUID := "uid"
			testName := "name"

			kms.RegisterNamespace(testUID, testName)
			require.Len(t, kms.Metadata.Errors, 1)
			require.Equal(t, "RegisterNamespace(uid, name): Cluster is nil", kms.Metadata.Errors[0])

			// TODO: Confirm here whether we still want to register a Namespace if cluster is nil
			testNamespace := &Namespace{UID: testUID, ClusterUID: "", Name: testName}

			require.NotNil(t, kms.Namespaces[testUID])
			require.Equal(t, testNamespace, kms.Namespaces[testUID])
			require.NotNil(t, kms.idx.namespaceByName[testName])
			require.Equal(t, testNamespace, kms.idx.namespaceByName[testName])
			require.Equal(t, 1, kms.Metadata.ObjectCount)
		})

		t.Run("register namespace on KMS w/ cluster", func(t *testing.T) {
			kms := NewKubeModelSet(start, end)
			kms.RegisterCluster("cluster-uid")
			// At this point we have a KMS with a cluster registered

			testUID := "uid"
			testName := "name"

			kms.RegisterNamespace(testUID, testName)
			require.Len(t, kms.Metadata.Errors, 0)
			require.NotNil(t, kms.Namespaces[testUID])

			testNamespace := &Namespace{UID: testUID, ClusterUID: "cluster-uid", Name: testName}

			require.Equal(t, testNamespace, kms.Namespaces[testUID])
			require.Equal(t, testNamespace, kms.idx.namespaceByName[testName])
			require.Equal(t, 1, kms.Metadata.ObjectCount)

			// Register same namespace again, expect no-op on second try
			kms.RegisterNamespace(testUID, testName)
			require.Len(t, kms.Metadata.Errors, 0)
			require.NotNil(t, kms.Namespaces[testUID])
			require.Equal(t, testNamespace, kms.Namespaces[testUID])
			require.Equal(t, testNamespace, kms.idx.namespaceByName[testName])
			require.Equal(t, 1, kms.Metadata.ObjectCount) // remains 1
		})
	})

	t.Run("RegisterResourceQuota", func(t *testing.T) {
		t.Run("empty resourceQuota UID", func(t *testing.T) {
			kms := NewKubeModelSet(start, end)

			kms.RegisterResourceQuota("", "test", "")
			require.Len(t, kms.Metadata.Errors, 1)
			require.Equal(t, "RegisterResourceQuota: uid is nil for ResourceQuota 'test'", kms.Metadata.Errors[0])
			require.Len(t, kms.ResourceQuotas, 0)
		})

		t.Run("register resource quota on KMS w/o namespace", func(t *testing.T) {
			kms := NewKubeModelSet(start, end)

			testUID := "uid"
			testName := "name"

			kms.RegisterResourceQuota(testUID, testName, "unregistered-namespace")
			require.Len(t, kms.Metadata.Errors, 1)
			require.Equal(t, "RegisterResourceQuota(uid, name, unregistered-namespace): missing namespace", kms.Metadata.Errors[0])

			// TODO: Confirm here whether we still want to register a RQ if namespace is nil
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

			require.Len(t, kms.Metadata.Errors, 0)
			require.NotNil(t, kms.ResourceQuotas[testUID])
			require.Equal(t, testRQ, kms.ResourceQuotas[testUID])
			require.Equal(t, 2, kms.Metadata.ObjectCount) // 1 namespace and 1 RQ

			// Register same RQ again, expect no-op on second try
			kms.RegisterResourceQuota(testUID, testName, testNamespace)
			require.Len(t, kms.Metadata.Errors, 0)
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

			require.Len(t, kms.Metadata.Errors, 0)
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

			require.Len(t, kms.Metadata.Errors, 1)
			require.Equal(t, "RegisterResourceQuota(uid-3, name-3, namespace-3): missing namespace", kms.Metadata.Errors[0])

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
=======
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestKubeModel(t *testing.T) {
	start := time.Now().Add(-1 * time.Hour)
	end := time.Now()

	t.Run("RegisterNamespace", func(t *testing.T) {
		t.Run("register new namespace", func(t *testing.T) {
			kms := NewKubeModelSet(start, end)
			kms.Cluster = &Cluster{UID: "cluster-1"}

			err := kms.RegisterNamespace("ns-1", "default")
			require.NoError(t, err)

			require.Len(t, kms.Namespaces, 1)
			ns, ok := kms.Namespaces["ns-1"]
			require.True(t, ok)
			require.NotNil(t, ns)
			require.Equal(t, "ns-1", ns.UID)
			require.Equal(t, "default", ns.Name)
			require.Equal(t, 1, kms.Metadata.ObjectCount)
		})

		t.Run("register duplicate namespace", func(t *testing.T) {
			kms := NewKubeModelSet(start, end)
			kms.Cluster = &Cluster{UID: "cluster-1"}

			err := kms.RegisterNamespace("ns-1", "default")
			require.NoError(t, err)
			require.Equal(t, 1, kms.Metadata.ObjectCount)

			err = kms.RegisterNamespace("ns-1", "default")
			require.NoError(t, err)
			require.Len(t, kms.Namespaces, 1)
			require.Equal(t, 1, kms.Metadata.ObjectCount, "ObjectCount should not increment for duplicate")
		})

		t.Run("register multiple namespaces", func(t *testing.T) {
			kms := NewKubeModelSet(start, end)
			kms.Cluster = &Cluster{UID: "cluster-1"}

			err := kms.RegisterNamespace("ns-1", "default")
			require.NoError(t, err)

			err = kms.RegisterNamespace("ns-2", "kube-system")
			require.NoError(t, err)

			require.Len(t, kms.Namespaces, 2)
			require.Equal(t, 2, kms.Metadata.ObjectCount)
		})
	})

	t.Run("RegisterPod", func(t *testing.T) {
		t.Run("register new pod", func(t *testing.T) {
			kms := NewKubeModelSet(start, end)
			kms.Cluster = &Cluster{UID: "cluster-1"}
			kms.RegisterNamespace("ns-1", "default")

			err := kms.RegisterPod("pod-1", "nginx", "default")
			require.NoError(t, err)

			require.Len(t, kms.Pods, 1)
			pod, ok := kms.Pods["pod-1"]
			require.True(t, ok)
			require.NotNil(t, pod)
			require.Equal(t, "pod-1", pod.UID)
			require.Equal(t, "nginx", pod.Name)
			require.Equal(t, "ns-1", pod.NamespaceUID)
		})

		t.Run("register duplicate pod", func(t *testing.T) {
			kms := NewKubeModelSet(start, end)
			kms.Cluster = &Cluster{UID: "cluster-1"}
			kms.RegisterNamespace("ns-1", "default")

			err := kms.RegisterPod("pod-1", "nginx", "default")
			require.NoError(t, err)

			err = kms.RegisterPod("pod-1", "nginx", "default")
			require.NoError(t, err)
			require.Len(t, kms.Pods, 1)
		})
	})

	t.Run("RegisterNode", func(t *testing.T) {
		t.Run("register new node", func(t *testing.T) {
			kms := NewKubeModelSet(start, end)
			kms.Cluster = &Cluster{UID: "cluster-1"}

			err := kms.RegisterNode("node-1", "worker-1")
			require.NoError(t, err)

			require.Len(t, kms.Nodes, 1)
			node, ok := kms.Nodes["node-1"]
			require.True(t, ok)
			require.NotNil(t, node)
			require.Equal(t, "node-1", node.UID)
			require.Equal(t, "worker-1", node.Name)
		})

		t.Run("register duplicate node", func(t *testing.T) {
			kms := NewKubeModelSet(start, end)
			kms.Cluster = &Cluster{UID: "cluster-1"}

			err := kms.RegisterNode("node-1", "worker-1")
			require.NoError(t, err)

			err = kms.RegisterNode("node-1", "worker-1")
			require.NoError(t, err)
			require.Len(t, kms.Nodes, 1)
		})
	})

	t.Run("RegisterOwner", func(t *testing.T) {
		t.Run("register new owner", func(t *testing.T) {
			kms := NewKubeModelSet(start, end)
			kms.Cluster = &Cluster{UID: "cluster-1"}
			kms.RegisterNamespace("ns-1", "default")

			err := kms.RegisterOwner("ctrl-1", "nginx-deployment", "default", "Deployment", true)
			require.NoError(t, err)

			require.Len(t, kms.Owners, 1)
			owner, ok := kms.Owners["ctrl-1"]
			require.True(t, ok)
			require.NotNil(t, owner)
			require.Equal(t, "ctrl-1", owner.UID)
			require.Equal(t, "nginx-deployment", owner.Name)
			require.Equal(t, OwnerKind("Deployment"), owner.Kind)
			require.True(t, owner.Controller)
		})

		t.Run("register duplicate owner", func(t *testing.T) {
			kms := NewKubeModelSet(start, end)
			kms.Cluster = &Cluster{UID: "cluster-1"}
			kms.RegisterNamespace("ns-1", "default")

			err := kms.RegisterOwner("ctrl-1", "nginx-deployment", "default", "Deployment", true)
			require.NoError(t, err)

			err = kms.RegisterOwner("ctrl-1", "nginx-deployment", "default", "Deployment", true)
			require.NoError(t, err)
			require.Len(t, kms.Owners, 1)
		})
	})

	t.Run("RegisterService", func(t *testing.T) {
		t.Run("register new service", func(t *testing.T) {
			kms := NewKubeModelSet(start, end)
			kms.Cluster = &Cluster{UID: "cluster-1"}
			kms.RegisterNamespace("ns-1", "default")

			err := kms.RegisterService("svc-1", "nginx-service", "default")
			require.NoError(t, err)

			require.Len(t, kms.Services, 1)
			svc, ok := kms.Services["svc-1"]
			require.True(t, ok)
			require.NotNil(t, svc)
			require.Equal(t, "svc-1", svc.UID)
			require.Equal(t, "nginx-service", svc.Name)
		})

		t.Run("register duplicate service", func(t *testing.T) {
			kms := NewKubeModelSet(start, end)
			kms.Cluster = &Cluster{UID: "cluster-1"}
			kms.RegisterNamespace("ns-1", "default")

			err := kms.RegisterService("svc-1", "nginx-service", "default")
			require.NoError(t, err)

			err = kms.RegisterService("svc-1", "nginx-service", "default")
			require.NoError(t, err)
			require.Len(t, kms.Services, 1)
		})
	})

	t.Run("RegisterContainer", func(t *testing.T) {
		t.Run("register new container", func(t *testing.T) {
			kms := NewKubeModelSet(start, end)
			kms.Cluster = &Cluster{UID: "cluster-1"}
			kms.RegisterNamespace("ns-1", "default")
			kms.RegisterPod("pod-1", "nginx", "default")

			err := kms.RegisterContainer("container-1", "nginx-container", "pod-1")
			require.NoError(t, err)

			require.Len(t, kms.Containers, 1)
			container, ok := kms.Containers["container-1"]
			require.True(t, ok)
			require.NotNil(t, container)
			require.Equal(t, "nginx-container", container.Name)
			require.Equal(t, "pod-1", container.PodUID)
		})

		t.Run("register duplicate container", func(t *testing.T) {
			kms := NewKubeModelSet(start, end)
			kms.Cluster = &Cluster{UID: "cluster-1"}
			kms.RegisterNamespace("ns-1", "default")
			kms.RegisterPod("pod-1", "nginx", "default")

			err := kms.RegisterContainer("container-1", "nginx-container", "pod-1")
			require.NoError(t, err)

			err = kms.RegisterContainer("container-1", "nginx-container", "pod-1")
			require.NoError(t, err)
			require.Len(t, kms.Containers, 1)
		})
	})

	t.Run("IsEmpty", func(t *testing.T) {
		t.Run("empty KubeModelSet", func(t *testing.T) {
			kms := NewKubeModelSet(start, end)
			kms.Cluster = &Cluster{UID: "cluster-1"}

			isEmpty := kms.IsEmpty()
			require.True(t, isEmpty)
		})

		t.Run("KubeModelSet with namespace", func(t *testing.T) {
			kms := NewKubeModelSet(start, end)
			kms.Cluster = &Cluster{UID: "cluster-1"}
			kms.RegisterNamespace("ns-1", "default")

			isEmpty := kms.IsEmpty()
			require.False(t, isEmpty)
		})

	})
>>>>>>> 92af4761 (Introduce kubemodel with core Kubernetes resources (Cluster, Namespace, Node, Pod, Container, Owner, Service) (#3472))
}
