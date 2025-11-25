package kubemodel

import (
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
}
