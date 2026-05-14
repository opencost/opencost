package kubemodel

import (
	"time"

	"github.com/opencost/opencost/core/pkg/model/shared"
)

// NewMockKubeModelSet returns a KubeModelSet populated with one instance of every
// type. All time values are derived from start/end so the caller controls
// monotonic-clock safety (use Truncate before passing in).
func NewMockKubeModelSet(start, end time.Time) *KubeModelSet {
	kms := NewKubeModelSet(start, end)

	// --- Cluster ---
	kms.RegisterCluster(&Cluster{
		UID:      "cluster-uid",
		Provider: shared.ProviderAWS,
		Account:  "123456789012",
		Name:     "test-cluster",
		Region:   "us-east-1",
		Start:    start,
		End:      end,
	})

	// --- Namespace ---
	kms.RegisterNamespace(&Namespace{
		UID:         "ns-uid",
		Name:        "default",
		Labels:      map[string]string{"env": "test"},
		Annotations: map[string]string{"note": "mock"},
		Start:       start,
		End:         end,
	})

	// --- ResourceQuota ---
	kms.RegisterResourceQuota(&ResourceQuota{
		UID:          "rq-uid",
		NamespaceUID: "ns-uid",
		Name:         "default-quota",
		Spec: &ResourceQuotaSpec{
			Hard: &ResourceQuotaSpecHard{
				Requests: ResourceQuantities{
					ResourceCPU:    {Resource: ResourceCPU, Unit: UnitMillicore, Values: Stats{StatAvg: 4000, StatMax: 4000}},
					ResourceMemory: {Resource: ResourceMemory, Unit: UnitByte, Values: Stats{StatAvg: 8e9, StatMax: 8e9}},
				},
				Limits: ResourceQuantities{
					ResourceCPU:    {Resource: ResourceCPU, Unit: UnitMillicore, Values: Stats{StatAvg: 8000, StatMax: 8000}},
					ResourceMemory: {Resource: ResourceMemory, Unit: UnitByte, Values: Stats{StatAvg: 16e9, StatMax: 16e9}},
				},
			},
		},
		Status: &ResourceQuotaStatus{
			Used: &ResourceQuotaStatusUsed{
				Requests: ResourceQuantities{
					ResourceCPU:    {Resource: ResourceCPU, Unit: UnitMillicore, Values: Stats{StatAvg: 500, StatMax: 800}},
					ResourceMemory: {Resource: ResourceMemory, Unit: UnitByte, Values: Stats{StatAvg: 1e9, StatMax: 2e9}},
				},
			},
		},
		Start: start,
		End:   end,
	})

	// --- Node ---
	kms.RegisterNode(&Node{
		UID:          "node-uid",
		ProviderID:   "aws:///us-east-1a/i-0abc123def456",
		Name:         "node-1",
		Labels:       map[string]string{"node.kubernetes.io/instance-type": "m5.large"},
		InstanceType: "m5.large",
		Preemptible:  false,
		ResourceCapacities: ResourceQuantities{
			ResourceCPU:    {Resource: ResourceCPU, Unit: UnitMillicore, Values: Stats{StatAvg: 2000, StatMax: 2000}},
			ResourceMemory: {Resource: ResourceMemory, Unit: UnitByte, Values: Stats{StatAvg: 8e9, StatMax: 8e9}},
		},
		ResourcesAllocatable: ResourceQuantities{
			ResourceCPU:    {Resource: ResourceCPU, Unit: UnitMillicore, Values: Stats{StatAvg: 1900, StatMax: 1900}},
			ResourceMemory: {Resource: ResourceMemory, Unit: UnitByte, Values: Stats{StatAvg: 7e9, StatMax: 7e9}},
		},
		FileSystem: FileSystem{
			CapacityBytes: 100e9,
			UsageByteAvg:  40e9,
			UsageByteMax:  55e9,
		},
		Start: start,
		End:   end,
	})

	// --- Pod ---
	kms.RegisterPod(&Pod{
		UID:          "pod-uid",
		NamespaceUID: "ns-uid",
		NodeUID:      "node-uid",
		Name:         "my-pod-abc12",
		Owners:       []Owner{{UID: "dep-uid", Kind: OwnerKindDeployment, Controller: true}},
		PVCVolumes:   []PodPVCVolumes{{Name: "data", PersistentVolumeClaimUID: "pvc-uid"}},
		Labels:       map[string]string{"app": "my-app", "version": "v1"},
		Annotations:  map[string]string{"prometheus.io/scrape": "true"},
		NetworkTrafficDetails: []NetworkTrafficDetail{
			{
				PodUID:           "pod-uid",
				Endpoint:         "10.0.0.5:443",
				TrafficDirection: TrafficDirectionEgress,
				TrafficType:      TrafficTypeCrossZone,
				IsNatGateway:     false,
				Bytes:            1024,
			},
		},
		Start: start,
		End:   end,
	})

	// --- Container ---
	kms.RegisterContainer(&Container{
		PodUID: "pod-uid",
		Name:   "app",
		ResourceRequests: ResourceQuantities{
			ResourceCPU:    {Resource: ResourceCPU, Unit: UnitMillicore, Values: Stats{StatAvg: 250, StatMax: 250}},
			ResourceMemory: {Resource: ResourceMemory, Unit: UnitByte, Values: Stats{StatAvg: 512e6, StatMax: 512e6}},
		},
		ResourceLimits: ResourceQuantities{
			ResourceCPU:    {Resource: ResourceCPU, Unit: UnitMillicore, Values: Stats{StatAvg: 500, StatMax: 500}},
			ResourceMemory: {Resource: ResourceMemory, Unit: UnitByte, Values: Stats{StatAvg: 1e9, StatMax: 1e9}},
		},
		CPUCoresAllocated: 0.25,
		CPUCoreUsageAvg:   0.18,
		CPUCoreUsageMax:   0.42,
		RAMBytesAllocated: 512e6,
		RAMBytesUsageAvg:  300e6,
		RAMBytesUsageMax:  480e6,
		Start:             start,
		End:               end,
	})

	// --- Deployment ---
	kms.RegisterDeployment(&Deployment{
		UID:          "dep-uid",
		NamespaceUID: "ns-uid",
		Name:         "my-app",
		Labels:       map[string]string{"app": "my-app"},
		Annotations:  map[string]string{"deployment.kubernetes.io/revision": "3"},
		MatchLabels:  map[string]string{"app": "my-app"},
		Start:        start,
		End:          end,
	})

	// --- StatefulSet ---
	kms.RegisterStatefulSet(&StatefulSet{
		UID:          "sts-uid",
		NamespaceUID: "ns-uid",
		Name:         "my-statefulset",
		Labels:       map[string]string{"app": "my-statefulset"},
		Annotations:  map[string]string{"note": "test"},
		MatchLabels:  map[string]string{"app": "my-statefulset"},
		Start:        start,
		End:          end,
	})

	// --- DaemonSet ---
	kms.RegisterDaemonSet(&DaemonSet{
		UID:          "ds-uid",
		NamespaceUID: "ns-uid",
		Name:         "my-daemonset",
		Labels:       map[string]string{"app": "my-daemonset"},
		Annotations:  map[string]string{"note": "test"},
		Start:        start,
		End:          end,
	})

	// --- Job ---
	kms.RegisterJob(&Job{
		UID:          "job-uid",
		NamespaceUID: "ns-uid",
		Name:         "my-job",
		Labels:       map[string]string{"app": "my-job"},
		Annotations:  map[string]string{"note": "test"},
		Start:        start,
		End:          end,
	})

	// --- CronJob ---
	kms.RegisterCronJob(&CronJob{
		UID:          "cj-uid",
		NamespaceUID: "ns-uid",
		Name:         "my-cronjob",
		Labels:       map[string]string{"app": "my-cronjob"},
		Annotations:  map[string]string{"note": "test"},
		Start:        start,
		End:          end,
	})

	// --- ReplicaSet ---
	kms.RegisterReplicaSet(&ReplicaSet{
		UID:          "rs-uid",
		NamespaceUID: "ns-uid",
		Name:         "my-app-7d9f8b",
		Owners:       []Owner{{UID: "dep-uid", Kind: OwnerKindDeployment, Controller: true}},
		Labels:       map[string]string{"app": "my-app", "pod-template-hash": "7d9f8b"},
		Annotations:  map[string]string{"note": "test"},
		Start:        start,
		End:          end,
	})

	// --- Service ---
	kms.RegisterService(&Service{
		UID:          "svc-uid",
		NamespaceUID: "ns-uid",
		Name:         "my-app",
		Type:         ServiceTypeClusterIP,
		Selector:     map[string]string{"app": "my-app"},
		Start:        start,
		End:          end,
	})

	// --- PersistentVolumeClaim ---
	kms.RegisterPVC(&PersistentVolumeClaim{
		UID:                 "pvc-uid",
		NamespaceUID:        "ns-uid",
		Name:                "data",
		PersistentVolumeUID: "pv-uid",
		StorageClass:        "gp2",
		RequestedBytes:      50e9,
		UsageBytesAvg:       20e9,
		UsageBytesMax:       35e9,
		Start:               start,
		End:                 end,
	})

	// --- PersistentVolume ---
	kms.RegisterPersistentVolume(&PersistentVolume{
		UID:             "pv-uid",
		Name:            "pvc-abc123",
		StorageClass:    "gp2",
		CSIVolumeHandle: "vol-0abc123def456789",
		SizeBytes:       50e9,
		Start:           start,
		End:             end,
	})

	// --- DCGMDevice ---
	kms.RegisterDCGMDevice(&DCGMDevice{
		UUID:      "GPU-abc123def-456-789",
		Device:    "0",
		ModelName: "Tesla T4",
		PodUsage: map[string]DCGMPod{
			"pod-uid": {
				ContainerUsage: map[string]DCGMContainer{
					"app": {UsageAvg: 0.65, UsageMax: 0.92},
				},
			},
		},
		Start: start,
		End:   end,
	})

	// --- Diagnostics ---
	kms.Error(errMock("mock error"))
	kms.Warn("mock warning")
	kms.Info("mock info")

	return kms
}

// errMock is a minimal error implementation that avoids importing errors/fmt in the mock.
type errMock string

func (e errMock) Error() string { return string(e) }
