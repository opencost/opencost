package kubemodel

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestGPUDevice(t *testing.T) {
	t.Run("Validate", func(t *testing.T) {
		t.Run("valid GPU device", func(t *testing.T) {
			device := &GPUDevice{
				UID:               "gpu-1",
				NodeUID:           "node-1",
				DeviceNumber:      0,
				ModelName:         "NVIDIA A100",
				IsShared:          false,
				SharePercentage:   100.0,
				GpuHours:          1.0,
				GpuRequestAverage: 50.0,
				GpuUsageAverage:   45.0,
				GpuUsageMax:       90.0,
				MemoryBytes:       42949672960,
			}

			err := device.Validate()
			require.NoError(t, err)
		})

		t.Run("shared GPU with MIG", func(t *testing.T) {
			device := &GPUDevice{
				UID:               "gpu-1-mig-0",
				NodeUID:           "node-1",
				DeviceNumber:      0,
				ModelName:         "NVIDIA A100-MIG-1g.5gb",
				IsShared:          true,
				SharePercentage:   14.3,
				GpuHours:          0.5,
				GpuRequestAverage: 100.0,
				GpuUsageAverage:   75.0,
				GpuUsageMax:       92.0,
				MemoryBytes:       5368709120,
			}

			err := device.Validate()
			require.NoError(t, err)
		})

		t.Run("missing ID", func(t *testing.T) {
			device := &GPUDevice{
				NodeUID:         "node-1",
				GpuUsageAverage: 50.0,
			}

			err := device.Validate()
			require.Error(t, err)
			require.Contains(t, err.Error(), "UID is required")
		})

		t.Run("missing NodeID", func(t *testing.T) {
			device := &GPUDevice{
				UID:             "gpu-1",
				GpuUsageAverage: 50.0,
			}

			err := device.Validate()
			require.Error(t, err)
			require.Contains(t, err.Error(), "NodeUID is required")
		})

		t.Run("invalid SharePercentage", func(t *testing.T) {
			t.Run("negative", func(t *testing.T) {
				device := &GPUDevice{
					UID:             "gpu-1",
					NodeUID:         "node-1",
					SharePercentage: -10.0,
				}

				err := device.Validate()
				require.Error(t, err)
				require.Contains(t, err.Error(), "SharePercentage must be 0-100")
			})

			t.Run("over 100", func(t *testing.T) {
				device := &GPUDevice{
					UID:             "gpu-1",
					NodeUID:         "node-1",
					SharePercentage: 150.0,
				}

				err := device.Validate()
				require.Error(t, err)
				require.Contains(t, err.Error(), "SharePercentage must be 0-100")
			})
		})

		t.Run("invalid GpuRequestAverage", func(t *testing.T) {
			device := &GPUDevice{
				UID:               "gpu-1",
				NodeUID:           "node-1",
				SharePercentage:   50.0,
				GpuRequestAverage: -5.0,
			}

			err := device.Validate()
			require.Error(t, err)
			require.Contains(t, err.Error(), "GpuRequestAverage must be 0-100")
		})

		t.Run("invalid GpuUsageAverage", func(t *testing.T) {
			device := &GPUDevice{
				UID:             "gpu-1",
				NodeUID:         "node-1",
				SharePercentage: 50.0,
				GpuUsageAverage: 105.0,
			}

			err := device.Validate()
			require.Error(t, err)
			require.Contains(t, err.Error(), "GpuUsageAverage must be 0-100")
		})

		t.Run("GpuUsageMax less than average", func(t *testing.T) {
			device := &GPUDevice{
				UID:             "gpu-1",
				NodeUID:         "node-1",
				SharePercentage: 50.0,
				GpuUsageAverage: 80.0,
				GpuUsageMax:     70.0,
			}

			err := device.Validate()
			require.Error(t, err)
			require.Contains(t, err.Error(), "GpuUsageMax cannot be less than GpuUsageAverage")
		})

		t.Run("negative GpuHours", func(t *testing.T) {
			device := &GPUDevice{
				UID:             "gpu-1",
				NodeUID:         "node-1",
				SharePercentage: 50.0,
				GpuUsageAverage: 50.0,
				GpuUsageMax:     80.0,
				GpuHours:        -1.0,
			}

			err := device.Validate()
			require.Error(t, err)
			require.Contains(t, err.Error(), "GpuHours cannot be negative")
		})
	})

	t.Run("Clone", func(t *testing.T) {
		t.Run("nil device", func(t *testing.T) {
			var device *GPUDevice
			cloned := device.Clone()
			require.Nil(t, cloned)
		})

		t.Run("basic device", func(t *testing.T) {
			device := &GPUDevice{
				UID:               "gpu-1",
				NodeUID:           "node-1",
				DeviceNumber:      0,
				ModelName:         "NVIDIA A100",
				IsShared:          false,
				SharePercentage:   100.0,
				GpuHours:          2.5,
				GpuRequestAverage: 80.0,
				GpuUsageAverage:   75.0,
				GpuUsageMax:       95.0,
				MemoryBytes:       42949672960,
			}

			cloned := device.Clone()
			require.NotNil(t, cloned)
			require.NotSame(t, device, cloned, "Clone should return a different pointer")
			require.Equal(t, device.UID, cloned.UID)
			require.Equal(t, device.NodeUID, cloned.NodeUID)
			require.Equal(t, device.DeviceNumber, cloned.DeviceNumber)
			require.Equal(t, device.ModelName, cloned.ModelName)
			require.Equal(t, device.IsShared, cloned.IsShared)
			require.Equal(t, device.SharePercentage, cloned.SharePercentage)
			require.Equal(t, device.GpuHours, cloned.GpuHours)
			require.Equal(t, device.GpuRequestAverage, cloned.GpuRequestAverage)
			require.Equal(t, device.GpuUsageAverage, cloned.GpuUsageAverage)
			require.Equal(t, device.GpuUsageMax, cloned.GpuUsageMax)
			require.Equal(t, device.MemoryBytes, cloned.MemoryBytes)
		})

		t.Run("device with diagnostic", func(t *testing.T) {
			device := &GPUDevice{
				UID:               "gpu-2",
				NodeUID:           "node-2",
				DeviceNumber:      1,
				ModelName:         "NVIDIA H100",
				IsShared:          true,
				SharePercentage:   50.0,
				GpuHours:          1.0,
				GpuRequestAverage: 90.0,
				GpuUsageAverage:   85.0,
				GpuUsageMax:       98.0,
				MemoryBytes:       85899345920,
				Diagnostic: &DiagnosticResult{
					UID:         "diag-1",
					Name:        "GPU Error",
					Description: "Test GPU error",
					Category:    "gpu",
					Error:       "Test error",
				},
			}

			cloned := device.Clone()
			require.NotNil(t, cloned)
			require.NotSame(t, device, cloned)
			require.Equal(t, device.UID, cloned.UID)
			require.NotNil(t, cloned.Diagnostic)
		})
	})
}

func TestGPUUsage(t *testing.T) {
	t.Run("Validate", func(t *testing.T) {
		t.Run("valid GPU usage", func(t *testing.T) {
			usage := &GPUUsage{
				ContainerUID:         "container-1",
				GpuDeviceUID:         "gpu-1",
				GpuHours:             1.0,
				GpuRequestPercentage: 100.0,
				GpuUsageAverage:      75.0,
				GpuUsageMax:          95.0,
				MemoryBytesUsed:      34359738368,
			}

			err := usage.Validate()
			require.NoError(t, err)
		})

		t.Run("shared GPU usage", func(t *testing.T) {
			usage := &GPUUsage{
				ContainerUID:         "container-2",
				GpuDeviceUID:         "gpu-1-mig-0",
				GpuHours:             0.5,
				GpuRequestPercentage: 100.0,
				GpuUsageAverage:      75.0,
				GpuUsageMax:          92.0,
				MemoryBytesUsed:      4294967296,
			}

			err := usage.Validate()
			require.NoError(t, err)
		})

		t.Run("missing ContainerID", func(t *testing.T) {
			usage := &GPUUsage{
				GpuDeviceUID:    "gpu-1",
				GpuUsageAverage: 50.0,
			}

			err := usage.Validate()
			require.Error(t, err)
			require.Contains(t, err.Error(), "ContainerUID is required")
		})

		t.Run("missing GpuDeviceID", func(t *testing.T) {
			usage := &GPUUsage{
				ContainerUID:    "container-1",
				GpuUsageAverage: 50.0,
			}

			err := usage.Validate()
			require.Error(t, err)
			require.Contains(t, err.Error(), "GpuDeviceUID is required")
		})

		t.Run("invalid GpuRequestPercentage", func(t *testing.T) {
			t.Run("negative", func(t *testing.T) {
				usage := &GPUUsage{
					ContainerUID:         "container-1",
					GpuDeviceUID:         "gpu-1",
					GpuRequestPercentage: -10.0,
				}

				err := usage.Validate()
				require.Error(t, err)
				require.Contains(t, err.Error(), "GpuRequestPercentage must be 0-100")
			})

			t.Run("over 100", func(t *testing.T) {
				usage := &GPUUsage{
					ContainerUID:         "container-1",
					GpuDeviceUID:         "gpu-1",
					GpuRequestPercentage: 150.0,
				}

				err := usage.Validate()
				require.Error(t, err)
				require.Contains(t, err.Error(), "GpuRequestPercentage must be 0-100")
			})
		})

		t.Run("invalid GpuUsageAverage", func(t *testing.T) {
			usage := &GPUUsage{
				ContainerUID:         "container-1",
				GpuDeviceUID:         "gpu-1",
				GpuRequestPercentage: 50.0,
				GpuUsageAverage:      -5.0,
			}

			err := usage.Validate()
			require.Error(t, err)
			require.Contains(t, err.Error(), "GpuUsageAverage must be 0-100")
		})

		t.Run("invalid GpuUsageMax", func(t *testing.T) {
			usage := &GPUUsage{
				ContainerUID:         "container-1",
				GpuDeviceUID:         "gpu-1",
				GpuRequestPercentage: 50.0,
				GpuUsageAverage:      50.0,
				GpuUsageMax:          105.0,
			}

			err := usage.Validate()
			require.Error(t, err)
			require.Contains(t, err.Error(), "GpuUsageMax must be 0-100")
		})

		t.Run("GpuUsageMax less than average", func(t *testing.T) {
			usage := &GPUUsage{
				ContainerUID:         "container-1",
				GpuDeviceUID:         "gpu-1",
				GpuRequestPercentage: 50.0,
				GpuUsageAverage:      80.0,
				GpuUsageMax:          70.0,
			}

			err := usage.Validate()
			require.Error(t, err)
			require.Contains(t, err.Error(), "GpuUsageMax cannot be less than GpuUsageAverage")
		})

		t.Run("negative GpuHours", func(t *testing.T) {
			usage := &GPUUsage{
				ContainerUID:         "container-1",
				GpuDeviceUID:         "gpu-1",
				GpuRequestPercentage: 50.0,
				GpuUsageAverage:      50.0,
				GpuUsageMax:          80.0,
				GpuHours:             -1.0,
			}

			err := usage.Validate()
			require.Error(t, err)
			require.Contains(t, err.Error(), "GpuHours cannot be negative")
		})
	})

	t.Run("Clone", func(t *testing.T) {
		t.Run("nil usage", func(t *testing.T) {
			var usage *GPUUsage
			cloned := usage.Clone()
			require.Nil(t, cloned)
		})

		t.Run("basic usage", func(t *testing.T) {
			usage := &GPUUsage{
				ContainerUID:         "container-1",
				GpuDeviceUID:         "gpu-1",
				GpuHours:             2.5,
				GpuRequestPercentage: 100.0,
				GpuUsageAverage:      80.0,
				GpuUsageMax:          95.0,
				MemoryBytesUsed:      34359738368,
			}

			cloned := usage.Clone()
			require.NotNil(t, cloned)
			require.NotSame(t, usage, cloned, "Clone should return a different pointer")
			require.Equal(t, usage.ContainerUID, cloned.ContainerUID)
			require.Equal(t, usage.GpuDeviceUID, cloned.GpuDeviceUID)
			require.Equal(t, usage.GpuHours, cloned.GpuHours)
			require.Equal(t, usage.GpuRequestPercentage, cloned.GpuRequestPercentage)
			require.Equal(t, usage.GpuUsageAverage, cloned.GpuUsageAverage)
			require.Equal(t, usage.GpuUsageMax, cloned.GpuUsageMax)
			require.Equal(t, usage.MemoryBytesUsed, cloned.MemoryBytesUsed)
		})

		t.Run("usage with diagnostic", func(t *testing.T) {
			usage := &GPUUsage{
				ContainerUID:         "container-2",
				GpuDeviceUID:         "gpu-2",
				GpuHours:             1.0,
				GpuRequestPercentage: 50.0,
				GpuUsageAverage:      45.0,
				GpuUsageMax:          75.0,
				MemoryBytesUsed:      17179869184,
				Diagnostic: &DiagnosticResult{
					UID:         "diag-2",
					Name:        "GPU Warning",
					Description: "Test GPU warning",
					Category:    "gpu",
					Error:       "Test warning",
				},
			}

			cloned := usage.Clone()
			require.NotNil(t, cloned)
			require.NotSame(t, usage, cloned)
			require.Equal(t, usage.ContainerUID, cloned.ContainerUID)
			require.NotNil(t, cloned.Diagnostic)
		})
	})
}

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

	t.Run("RegisterResourceQuota", func(t *testing.T) {
		t.Run("register new resource quota", func(t *testing.T) {
			kms := NewKubeModelSet(start, end)
			kms.Cluster = &Cluster{UID: "cluster-1"}
			kms.RegisterNamespace("ns-1", "default")

			err := kms.RegisterResourceQuota("rq-1", "quota-1", "default")
			require.NoError(t, err)

			require.Len(t, kms.ResourceQuotas, 1)
			rq, ok := kms.ResourceQuotas["rq-1"]
			require.True(t, ok)
			require.NotNil(t, rq)
			require.Equal(t, "rq-1", rq.UID)
			require.Equal(t, "quota-1", rq.Name)
			require.Equal(t, "ns-1", rq.NamespaceUID)
		})

		t.Run("register duplicate resource quota", func(t *testing.T) {
			kms := NewKubeModelSet(start, end)
			kms.Cluster = &Cluster{UID: "cluster-1"}
			kms.RegisterNamespace("ns-1", "default")

			err := kms.RegisterResourceQuota("rq-1", "quota-1", "default")
			require.NoError(t, err)

			err = kms.RegisterResourceQuota("rq-1", "quota-1", "default")
			require.NoError(t, err)
			require.Len(t, kms.ResourceQuotas, 1)
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

	t.Run("RegisterController", func(t *testing.T) {
		t.Run("register new controller", func(t *testing.T) {
			kms := NewKubeModelSet(start, end)
			kms.Cluster = &Cluster{UID: "cluster-1"}
			kms.RegisterNamespace("ns-1", "default")

			err := kms.RegisterController("ctrl-1", "nginx-deployment", "default", "Deployment")
			require.NoError(t, err)

			require.Len(t, kms.Controllers, 1)
			ctrl, ok := kms.Controllers["ctrl-1"]
			require.True(t, ok)
			require.NotNil(t, ctrl)
			require.Equal(t, "ctrl-1", ctrl.UID)
			require.Equal(t, "nginx-deployment", ctrl.Name)
			require.Equal(t, ControllerKind("Deployment"), ctrl.Kind)
		})

		t.Run("register duplicate controller", func(t *testing.T) {
			kms := NewKubeModelSet(start, end)
			kms.Cluster = &Cluster{UID: "cluster-1"}
			kms.RegisterNamespace("ns-1", "default")

			err := kms.RegisterController("ctrl-1", "nginx-deployment", "default", "Deployment")
			require.NoError(t, err)

			err = kms.RegisterController("ctrl-1", "nginx-deployment", "default", "Deployment")
			require.NoError(t, err)
			require.Len(t, kms.Controllers, 1)
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

	t.Run("RegisterPVC", func(t *testing.T) {
		t.Run("register new PVC", func(t *testing.T) {
			kms := NewKubeModelSet(start, end)
			kms.Cluster = &Cluster{UID: "cluster-1"}
			kms.RegisterNamespace("ns-1", "default")

			err := kms.RegisterPVC("pvc-1", "data-volume", "default")
			require.NoError(t, err)

			require.Len(t, kms.PersistentVolumeClaims, 1)
			pvc, ok := kms.PersistentVolumeClaims["pvc-1"]
			require.True(t, ok)
			require.NotNil(t, pvc)
			require.Equal(t, "pvc-1", pvc.UID)
			require.Equal(t, "data-volume", pvc.Name)
		})

		t.Run("register duplicate PVC", func(t *testing.T) {
			kms := NewKubeModelSet(start, end)
			kms.Cluster = &Cluster{UID: "cluster-1"}
			kms.RegisterNamespace("ns-1", "default")

			err := kms.RegisterPVC("pvc-1", "data-volume", "default")
			require.NoError(t, err)

			err = kms.RegisterPVC("pvc-1", "data-volume", "default")
			require.NoError(t, err)
			require.Len(t, kms.PersistentVolumeClaims, 1)
		})
	})

	t.Run("RegisterVolume", func(t *testing.T) {
		t.Run("register new volume", func(t *testing.T) {
			kms := NewKubeModelSet(start, end)
			kms.Cluster = &Cluster{UID: "cluster-1"}

			err := kms.RegisterVolume("vol-1", "pv-data")
			require.NoError(t, err)

			require.Len(t, kms.Volumes, 1)
			vol, ok := kms.Volumes["vol-1"]
			require.True(t, ok)
			require.NotNil(t, vol)
			require.Equal(t, "vol-1", vol.UID)
			require.Equal(t, "pv-data", vol.Name)
		})

		t.Run("register duplicate volume", func(t *testing.T) {
			kms := NewKubeModelSet(start, end)
			kms.Cluster = &Cluster{UID: "cluster-1"}

			err := kms.RegisterVolume("vol-1", "pv-data")
			require.NoError(t, err)

			err = kms.RegisterVolume("vol-1", "pv-data")
			require.NoError(t, err)
			require.Len(t, kms.Volumes, 1)
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

	t.Run("RegisterGPUDevice", func(t *testing.T) {
		t.Run("register new GPU device", func(t *testing.T) {
			kms := NewKubeModelSet(start, end)

			err := kms.RegisterGPUDevice("gpu-1", "node-1")
			require.NoError(t, err)

			require.Len(t, kms.GPUDevices, 1)
			device, ok := kms.GPUDevices["gpu-1"]
			require.True(t, ok)
			require.NotNil(t, device)
			require.Equal(t, "gpu-1", device.UID)
			require.Equal(t, "node-1", device.NodeUID)
			require.Equal(t, 1, kms.Metadata.ObjectCount)
		})

		t.Run("register duplicate GPU device", func(t *testing.T) {
			kms := NewKubeModelSet(start, end)

			err := kms.RegisterGPUDevice("gpu-1", "node-1")
			require.NoError(t, err)
			require.Equal(t, 1, kms.Metadata.ObjectCount)

			err = kms.RegisterGPUDevice("gpu-1", "node-1")
			require.NoError(t, err)
			require.Len(t, kms.GPUDevices, 1)
			require.Equal(t, 1, kms.Metadata.ObjectCount, "ObjectCount should not increment for duplicate")
		})

		t.Run("register multiple GPU devices", func(t *testing.T) {
			kms := NewKubeModelSet(start, end)

			err := kms.RegisterGPUDevice("gpu-1", "node-1")
			require.NoError(t, err)

			err = kms.RegisterGPUDevice("gpu-2", "node-2")
			require.NoError(t, err)

			require.Len(t, kms.GPUDevices, 2)
			require.Equal(t, 2, kms.Metadata.ObjectCount)

			device1, ok := kms.GPUDevices["gpu-1"]
			require.True(t, ok)
			require.Equal(t, "gpu-1", device1.UID)

			device2, ok := kms.GPUDevices["gpu-2"]
			require.True(t, ok)
			require.Equal(t, "gpu-2", device2.UID)
		})
	})

	t.Run("RegisterGPUUsage", func(t *testing.T) {
		t.Run("register new GPU usage", func(t *testing.T) {
			kms := NewKubeModelSet(start, end)

			err := kms.RegisterGPUUsage("usage-1", "container-1", "gpu-1")
			require.NoError(t, err)

			require.Len(t, kms.GPUUsages, 1)
			usage, ok := kms.GPUUsages["usage-1"]
			require.True(t, ok)
			require.NotNil(t, usage)
			require.Equal(t, "container-1", usage.ContainerUID)
			require.Equal(t, "gpu-1", usage.GpuDeviceUID)
			require.Equal(t, 1, kms.Metadata.ObjectCount)
		})

		t.Run("register duplicate GPU usage", func(t *testing.T) {
			kms := NewKubeModelSet(start, end)

			err := kms.RegisterGPUUsage("usage-1", "container-1", "gpu-1")
			require.NoError(t, err)
			require.Equal(t, 1, kms.Metadata.ObjectCount)

			err = kms.RegisterGPUUsage("usage-1", "container-1", "gpu-1")
			require.NoError(t, err)
			require.Len(t, kms.GPUUsages, 1)
			require.Equal(t, 1, kms.Metadata.ObjectCount, "ObjectCount should not increment for duplicate")
		})

		t.Run("register multiple GPU usages", func(t *testing.T) {
			kms := NewKubeModelSet(start, end)

			err := kms.RegisterGPUUsage("usage-1", "container-1", "gpu-1")
			require.NoError(t, err)

			err = kms.RegisterGPUUsage("usage-2", "container-2", "gpu-2")
			require.NoError(t, err)

			require.Len(t, kms.GPUUsages, 2)
			require.Equal(t, 2, kms.Metadata.ObjectCount)

			usage1, ok := kms.GPUUsages["usage-1"]
			require.True(t, ok)
			require.Equal(t, "container-1", usage1.ContainerUID)

			usage2, ok := kms.GPUUsages["usage-2"]
			require.True(t, ok)
			require.Equal(t, "container-2", usage2.ContainerUID)
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

		t.Run("KubeModelSet with GPU device", func(t *testing.T) {
			kms := NewKubeModelSet(start, end)
			kms.Cluster = &Cluster{UID: "cluster-1"}
			kms.GPUDevices["gpu-1"] = &GPUDevice{
				UID:     "gpu-1",
				NodeUID: "node-1",
			}

			isEmpty := kms.IsEmpty()
			require.False(t, isEmpty)
		})

		t.Run("KubeModelSet with GPU usage", func(t *testing.T) {
			kms := NewKubeModelSet(start, end)
			kms.Cluster = &Cluster{UID: "cluster-1"}
			kms.GPUUsages["usage-1"] = &GPUUsage{
				ContainerUID: "container-1",
				GpuDeviceUID: "gpu-1",
			}

			isEmpty := kms.IsEmpty()
			require.False(t, isEmpty)
		})
	})
}
