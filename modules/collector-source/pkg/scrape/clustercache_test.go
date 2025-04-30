package scrape

import (
	"testing"
	"time"

	"github.com/opencost/opencost/core/pkg/clustercache"
	"github.com/opencost/opencost/modules/collector-source/pkg/metric"
	"github.com/opencost/opencost/modules/collector-source/pkg/util"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var Start1Str = "2025-01-01T00:00:00Z00:00"

func Test_kubernetesScraper_scrapeNodes(t *testing.T) {

	start1, _ := time.Parse(time.RFC3339, Start1Str)

	type scrape struct {
		Nodes     []*clustercache.Node
		Timestamp time.Time
	}
	tests := []struct {
		name     string
		scrapes  []scrape
		expected []metric.UpdateArgs
	}{
		{
			name: "simple",
			scrapes: []scrape{
				{
					Nodes: []*clustercache.Node{
						{
							Name:           "node1",
							SpecProviderID: "i-1",
							Status: v1.NodeStatus{
								Capacity: v1.ResourceList{
									v1.ResourceCPU:    resource.MustParse("2"),
									v1.ResourceMemory: resource.MustParse("2048"),
								},
								Allocatable: v1.ResourceList{
									v1.ResourceCPU:    resource.MustParse("1"),
									v1.ResourceMemory: resource.MustParse("1024"),
								},
							},
							Labels: map[string]string{
								"test1": "blah",
								"test2": "blah2",
							},
						},
					},
					Timestamp: start1,
				},
			},
			expected: []metric.UpdateArgs{
				{
					MetricName: KubeNodeStatusCapacityCPUCores,
					Labels: map[string]string{
						"node":        "node1",
						"provider_id": "i-1",
					},
					Value:                 2.0,
					Timestamp:             &start1,
					AdditionalInformation: nil,
				},
				{
					MetricName: KubeNodeStatusCapacityMemoryBytes,
					Labels: map[string]string{
						"node":        "node1",
						"provider_id": "i-1",
					},
					Value:                 2048.0,
					Timestamp:             &start1,
					AdditionalInformation: nil,
				},
				{
					MetricName: KubeNodeStatusAllocatableCPUCores,
					Labels: map[string]string{
						"node":        "node1",
						"provider_id": "i-1",
					},
					Value:                 1.0,
					Timestamp:             &start1,
					AdditionalInformation: nil,
				},
				{
					MetricName: KubeNodeStatusAllocatableMemoryBytes,
					Labels: map[string]string{
						"node":        "node1",
						"provider_id": "i-1",
					},
					Value:                 1024.0,
					Timestamp:             &start1,
					AdditionalInformation: nil,
				},
				{
					MetricName: KubeNodeLabels,
					Labels: map[string]string{
						"node":        "node1",
						"provider_id": "i-1",
					},
					Value:     0,
					Timestamp: &start1,
					AdditionalInformation: map[string]string{
						"label_test1": "blah",
						"label_test2": "blah2",
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			updateRecorder := metric.ArgRecordUpdater{}
			ks := &ClusterCacheScraper{
				updater: &updateRecorder,
			}
			for _, s := range tt.scrapes {
				ks.scrapeNodes(s.Nodes, s.Timestamp)
			}

			if len(updateRecorder.UpdateArgs) != len(tt.expected) {
				t.Errorf("Expected result length of %d, got %d", len(tt.expected), len(updateRecorder.UpdateArgs))
			}

			for i, expected := range tt.expected {
				updateArg := updateRecorder.UpdateArgs[i]
				err := expected.Equals(updateArg)
				if err != nil {
					t.Errorf("Result did not match expected at index %d: %s", i, err.Error())
				}
			}
		})
	}
}

func Test_kubernetesScraper_scrapeDeployments(t *testing.T) {

	start1, _ := time.Parse(time.RFC3339, Start1Str)

	type scrape struct {
		Deployments []*clustercache.Deployment
		Timestamp   time.Time
	}
	tests := []struct {
		name     string
		scrapes  []scrape
		expected []metric.UpdateArgs
	}{
		{
			name: "simple",
			scrapes: []scrape{
				{
					Deployments: []*clustercache.Deployment{
						{
							Name:      "deployment1",
							Namespace: "namespace1",
							MatchLabels: map[string]string{
								"test1": "blah",
								"test2": "blah2",
							},
						},
					},
					Timestamp: start1,
				},
			},
			expected: []metric.UpdateArgs{

				{
					MetricName: DeploymentMatchLabels,
					Labels: map[string]string{
						"deployment": "deployment1",
						"namespace":  "namespace1",
					},
					Value:     0,
					Timestamp: &start1,
					AdditionalInformation: map[string]string{
						"label_test1": "blah",
						"label_test2": "blah2",
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			updateRecorder := metric.ArgRecordUpdater{}
			ks := &ClusterCacheScraper{
				updater: &updateRecorder,
			}
			for _, s := range tt.scrapes {
				ks.scrapeDeployments(s.Deployments, s.Timestamp)
			}

			if len(updateRecorder.UpdateArgs) != len(tt.expected) {
				t.Errorf("Expected result length of %d, got %d", len(tt.expected), len(updateRecorder.UpdateArgs))
			}

			for i, expected := range tt.expected {
				updateArg := updateRecorder.UpdateArgs[i]
				err := expected.Equals(updateArg)
				if err != nil {
					t.Errorf("Result did not match expected at index %d: %s", i, err.Error())
				}
			}
		})
	}
}

func Test_kubernetesScraper_scrapeNamespaces(t *testing.T) {

	start1, _ := time.Parse(time.RFC3339, Start1Str)

	type scrape struct {
		Namespaces []*clustercache.Namespace
		Timestamp  time.Time
	}
	tests := []struct {
		name     string
		scrapes  []scrape
		expected []metric.UpdateArgs
	}{
		{
			name: "simple",
			scrapes: []scrape{
				{
					Namespaces: []*clustercache.Namespace{
						{
							Name: "namespace1",
							Labels: map[string]string{
								"test1": "blah",
								"test2": "blah2",
							},
							Annotations: map[string]string{
								"test3": "blah3",
								"test4": "blah4",
							},
						},
					},
					Timestamp: start1,
				},
			},
			expected: []metric.UpdateArgs{
				{
					MetricName: KubeNamespaceLabels,
					Labels: map[string]string{
						"namespace": "namespace1",
					},
					Value:     0,
					Timestamp: &start1,
					AdditionalInformation: map[string]string{
						"label_test1": "blah",
						"label_test2": "blah2",
					},
				},
				{
					MetricName: KubeNamespaceAnnotations,
					Labels: map[string]string{
						"namespace": "namespace1",
					},
					Value:     0,
					Timestamp: &start1,
					AdditionalInformation: map[string]string{
						"annotation_test3": "blah3",
						"annotation_test4": "blah4",
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			updateRecorder := metric.ArgRecordUpdater{}
			ks := &ClusterCacheScraper{
				updater: &updateRecorder,
			}
			for _, s := range tt.scrapes {
				ks.scrapeNamespaces(s.Namespaces, s.Timestamp)
			}

			if len(updateRecorder.UpdateArgs) != len(tt.expected) {
				t.Errorf("Expected result length of %d, got %d", len(tt.expected), len(updateRecorder.UpdateArgs))
			}

			for i, expected := range tt.expected {
				updateArg := updateRecorder.UpdateArgs[i]
				err := expected.Equals(updateArg)
				if err != nil {
					t.Errorf("Result did not match expected at index %d: %s", i, err.Error())
				}
			}
		})
	}
}

func Test_kubernetesScraper_scrapePods(t *testing.T) {

	start1, _ := time.Parse(time.RFC3339, Start1Str)

	type scrape struct {
		Pods      []*clustercache.Pod
		Timestamp time.Time
	}
	tests := []struct {
		name     string
		scrapes  []scrape
		expected []metric.UpdateArgs
	}{
		{
			name: "simple",
			scrapes: []scrape{
				{
					Pods: []*clustercache.Pod{
						{
							Name:      "pod1",
							Namespace: "namespace1",
							UID:       "uuid1",
							Spec: clustercache.PodSpec{
								NodeName: "node1",
								Containers: []clustercache.Container{
									{
										Name: "container1",
										Resources: v1.ResourceRequirements{
											Requests: map[v1.ResourceName]resource.Quantity{
												v1.ResourceCPU:    resource.MustParse("500m"),
												v1.ResourceMemory: resource.MustParse("512"),
											},
										},
									},
								},
							},
							Labels: map[string]string{
								"test1": "blah",
								"test2": "blah2",
							},
							Annotations: map[string]string{
								"test3": "blah3",
								"test4": "blah4",
							},
							OwnerReferences: []metav1.OwnerReference{
								{
									Kind:       "deployment",
									Name:       "deployment1",
									Controller: nil,
								},
							},
							Status: clustercache.PodStatus{
								ContainerStatuses: []v1.ContainerStatus{
									{
										Name: "container1",
										State: v1.ContainerState{
											Running: &v1.ContainerStateRunning{},
										},
									},
								},
							},
						},
					},
					Timestamp: start1,
				},
			},
			expected: []metric.UpdateArgs{
				{
					MetricName: KubePodLabels,
					Labels: map[string]string{
						"name":      "pod1",
						"namespace": "namespace1",
						"uid":       "uuid1",
						"node":      "node1",
					},
					Value:     0,
					Timestamp: &start1,
					AdditionalInformation: map[string]string{
						"label_test1": "blah",
						"label_test2": "blah2",
					},
				},
				{
					MetricName: KubePodAnnotations,
					Labels: map[string]string{
						"name":      "pod1",
						"namespace": "namespace1",
						"uid":       "uuid1",
						"node":      "node1",
					},
					Value:     0,
					Timestamp: &start1,
					AdditionalInformation: map[string]string{
						"annotation_test3": "blah3",
						"annotation_test4": "blah4",
					},
				},
				{
					MetricName: KubePodOwner,
					Labels: map[string]string{
						"name":                "pod1",
						"namespace":           "namespace1",
						"uid":                 "uuid1",
						"node":                "node1",
						"owner_kind":          "deployment",
						"owner_name":          "deployment1",
						"owner_is_controller": "false",
					},
					Value:                 0,
					Timestamp:             &start1,
					AdditionalInformation: nil,
				},
				{
					MetricName: KubePodContainerStatusRunning,
					Labels: map[string]string{
						"name":      "pod1",
						"namespace": "namespace1",
						"uid":       "uuid1",
						"node":      "node1",
						"container": "container1",
					},
					Value:                 0,
					Timestamp:             &start1,
					AdditionalInformation: nil,
				},
				{
					MetricName: KubePodContainerResourceRequests,
					Labels: map[string]string{
						"name":      "pod1",
						"namespace": "namespace1",
						"uid":       "uuid1",
						"node":      "node1",
						"container": "container1",
						"resource":  "cpu",
						"unit":      "core",
					},
					Value:                 0.5,
					Timestamp:             &start1,
					AdditionalInformation: nil,
				},
				{
					MetricName: KubePodContainerResourceRequests,
					Labels: map[string]string{
						"name":      "pod1",
						"namespace": "namespace1",
						"uid":       "uuid1",
						"node":      "node1",
						"container": "container1",
						"resource":  "memory",
						"unit":      "byte",
					},
					Value:                 512,
					Timestamp:             &start1,
					AdditionalInformation: nil,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			updateRecorder := metric.ArgRecordUpdater{}
			ks := &ClusterCacheScraper{
				updater: &updateRecorder,
			}
			for _, s := range tt.scrapes {
				ks.scrapePods(s.Pods, s.Timestamp)
			}

			if len(updateRecorder.UpdateArgs) != len(tt.expected) {
				t.Errorf("Expected result length of %d, got %d", len(tt.expected), len(updateRecorder.UpdateArgs))
			}

			for i, expected := range tt.expected {
				updateArg := updateRecorder.UpdateArgs[i]
				err := expected.Equals(updateArg)
				if err != nil {
					t.Errorf("Result did not match expected at index %d: %s", i, err.Error())
				}
			}
		})
	}
}

func Test_kubernetesScraper_scrapePVCs(t *testing.T) {

	start1, _ := time.Parse(time.RFC3339, Start1Str)

	type scrape struct {
		PVCs      []*clustercache.PersistentVolumeClaim
		Timestamp time.Time
	}
	tests := []struct {
		name     string
		scrapes  []scrape
		expected []metric.UpdateArgs
	}{
		{
			name: "simple",
			scrapes: []scrape{
				{
					PVCs: []*clustercache.PersistentVolumeClaim{
						{
							Name:      "pvc1",
							Namespace: "namespace1",
							Spec: v1.PersistentVolumeClaimSpec{
								VolumeName:       "vol1",
								StorageClassName: util.Ptr("storageClass1"),
								Resources: v1.VolumeResourceRequirements{
									Requests: v1.ResourceList{
										v1.ResourceStorage: resource.MustParse("4096"),
									},
								},
							},
						},
					},
					Timestamp: start1,
				},
			},
			expected: []metric.UpdateArgs{
				{
					MetricName: KubePersistenVolumeClaimInfo,
					Labels: map[string]string{
						"name":         "pvc1",
						"namespace":    "namespace1",
						"volumename":   "vol1",
						"storageclass": "storageClass1",
					},
					Value:                 0,
					Timestamp:             &start1,
					AdditionalInformation: nil,
				},
				{
					MetricName: KubePersistentVolumeClaimResourceRequestsStorageBytes,
					Labels: map[string]string{
						"name":         "pvc1",
						"namespace":    "namespace1",
						"volumename":   "vol1",
						"storageclass": "storageClass1",
					},
					Value:                 4096,
					Timestamp:             &start1,
					AdditionalInformation: nil,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			updateRecorder := metric.ArgRecordUpdater{}
			ks := &ClusterCacheScraper{
				updater: &updateRecorder,
			}
			for _, s := range tt.scrapes {
				ks.scrapePVCs(s.PVCs, s.Timestamp)
			}

			if len(updateRecorder.UpdateArgs) != len(tt.expected) {
				t.Errorf("Expected result length of %d, got %d", len(tt.expected), len(updateRecorder.UpdateArgs))
			}

			for i, expected := range tt.expected {
				updateArg := updateRecorder.UpdateArgs[i]
				err := expected.Equals(updateArg)
				if err != nil {
					t.Errorf("Result did not match expected at index %d: %s", i, err.Error())
				}
			}
		})
	}
}

func Test_kubernetesScraper_scrapePVs(t *testing.T) {

	start1, _ := time.Parse(time.RFC3339, Start1Str)

	type scrape struct {
		PVs       []*clustercache.PersistentVolume
		Timestamp time.Time
	}
	tests := []struct {
		name     string
		scrapes  []scrape
		expected []metric.UpdateArgs
	}{
		{
			name: "simple",
			scrapes: []scrape{
				{
					PVs: []*clustercache.PersistentVolume{
						{
							Name: "pv1",
							Spec: v1.PersistentVolumeSpec{
								StorageClassName: "storageClass1",
								PersistentVolumeSource: v1.PersistentVolumeSource{
									CSI: &v1.CSIPersistentVolumeSource{
										VolumeHandle: "vol-1",
									},
								},
								Capacity: v1.ResourceList{
									v1.ResourceStorage: resource.MustParse("4096"),
								},
							},
						},
					},
					Timestamp: start1,
				},
			},
			expected: []metric.UpdateArgs{
				{
					MetricName: KubecostPVInfo,
					Labels: map[string]string{
						"name":         "pv1",
						"providerID":   "vol-1",
						"storageClass": "storageClass1",
					},
					Value:                 0,
					Timestamp:             &start1,
					AdditionalInformation: nil,
				},
				{
					MetricName: KubePersistentVolumeCapacityBytes,
					Labels: map[string]string{
						"name":         "pv1",
						"providerID":   "vol-1",
						"storageClass": "storageClass1",
					},
					Value:                 4096,
					Timestamp:             &start1,
					AdditionalInformation: nil,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			updateRecorder := metric.ArgRecordUpdater{}
			ks := &ClusterCacheScraper{
				updater: &updateRecorder,
			}
			for _, s := range tt.scrapes {
				ks.scrapePVs(s.PVs, s.Timestamp)
			}

			if len(updateRecorder.UpdateArgs) != len(tt.expected) {
				t.Errorf("Expected result length of %d, got %d", len(tt.expected), len(updateRecorder.UpdateArgs))
			}

			for i, expected := range tt.expected {
				updateArg := updateRecorder.UpdateArgs[i]
				err := expected.Equals(updateArg)
				if err != nil {
					t.Errorf("Result did not match expected at index %d: %s", i, err.Error())
				}
			}
		})
	}
}

func Test_kubernetesScraper_scrapeServices(t *testing.T) {

	start1, _ := time.Parse(time.RFC3339, Start1Str)

	type scrape struct {
		Services  []*clustercache.Service
		Timestamp time.Time
	}
	tests := []struct {
		name     string
		scrapes  []scrape
		expected []metric.UpdateArgs
	}{
		{
			name: "simple",
			scrapes: []scrape{
				{
					Services: []*clustercache.Service{
						{
							Name:      "service1",
							Namespace: "namespace1",
							SpecSelector: map[string]string{
								"test1": "blah",
								"test2": "blah2",
							},
						},
					},
					Timestamp: start1,
				},
			},
			expected: []metric.UpdateArgs{
				{
					MetricName: ServiceSelectorLabels,
					Labels: map[string]string{
						"service":   "service1",
						"namespace": "namespace1",
					},
					Value:     0,
					Timestamp: &start1,
					AdditionalInformation: map[string]string{
						"label_test1": "blah",
						"label_test2": "blah2",
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			updateRecorder := metric.ArgRecordUpdater{}
			ks := &ClusterCacheScraper{
				updater: &updateRecorder,
			}
			for _, s := range tt.scrapes {
				ks.scrapeServices(s.Services, s.Timestamp)
			}

			if len(updateRecorder.UpdateArgs) != len(tt.expected) {
				t.Errorf("Expected result length of %d, got %d", len(tt.expected), len(updateRecorder.UpdateArgs))
			}

			for i, expected := range tt.expected {
				updateArg := updateRecorder.UpdateArgs[i]
				err := expected.Equals(updateArg)
				if err != nil {
					t.Errorf("Result did not match expected at index %d: %s", i, err.Error())
				}
			}
		})
	}
}

func Test_kubernetesScraper_scrapeStatefulSets(t *testing.T) {

	start1, _ := time.Parse(time.RFC3339, Start1Str)

	type scrape struct {
		StatefulSets []*clustercache.StatefulSet
		Timestamp    time.Time
	}
	tests := []struct {
		name     string
		scrapes  []scrape
		expected []metric.UpdateArgs
	}{
		{
			name: "simple",
			scrapes: []scrape{
				{
					StatefulSets: []*clustercache.StatefulSet{
						{
							Name:      "statefulSet1",
							Namespace: "namespace1",
							SpecSelector: &metav1.LabelSelector{
								MatchLabels: map[string]string{
									"test1": "blah",
									"test2": "blah2",
								},
							},
						},
					},
					Timestamp: start1,
				},
			},
			expected: []metric.UpdateArgs{
				{
					MetricName: StatefulSetMatchLabels,
					Labels: map[string]string{
						"name":      "statefulSet1",
						"namespace": "namespace1",
					},
					Value:     0,
					Timestamp: &start1,
					AdditionalInformation: map[string]string{
						"label_test1": "blah",
						"label_test2": "blah2",
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			updateRecorder := metric.ArgRecordUpdater{}
			ks := &ClusterCacheScraper{
				updater: &updateRecorder,
			}
			for _, s := range tt.scrapes {
				ks.scrapeStatefulSets(s.StatefulSets, s.Timestamp)
			}

			if len(updateRecorder.UpdateArgs) != len(tt.expected) {
				t.Errorf("Expected result length of %d, got %d", len(tt.expected), len(updateRecorder.UpdateArgs))
			}

			for i, expected := range tt.expected {
				updateArg := updateRecorder.UpdateArgs[i]
				err := expected.Equals(updateArg)
				if err != nil {
					t.Errorf("Result did not match expected at index %d: %s", i, err.Error())
				}
			}
		})
	}
}

func Test_kubernetesScraper_scrapeReplicaSets(t *testing.T) {

	start1, _ := time.Parse(time.RFC3339, Start1Str)

	type scrape struct {
		ReplicaSets []*clustercache.ReplicaSet
		Timestamp   time.Time
	}
	tests := []struct {
		name     string
		scrapes  []scrape
		expected []metric.UpdateArgs
	}{
		{
			name: "simple",
			scrapes: []scrape{
				{
					ReplicaSets: []*clustercache.ReplicaSet{
						{
							Name:      "replicaSet1",
							Namespace: "namespace1",
							OwnerReferences: []metav1.OwnerReference{
								{
									Name: "rollout1",
									Kind: "Rollout",
								},
							},
						},
					},
					Timestamp: start1,
				},
			},
			expected: []metric.UpdateArgs{
				{
					MetricName: KubeReplicasetOwner,
					Labels: map[string]string{
						"replicaset": "replicaSet1",
						"namespace":  "namespace1",
						"owner_name": "rollout1",
						"owner_kind": "Rollout",
					},
					Value:     0,
					Timestamp: &start1,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			updateRecorder := metric.ArgRecordUpdater{}
			ks := &ClusterCacheScraper{
				updater: &updateRecorder,
			}
			for _, s := range tt.scrapes {
				ks.scrapeReplicaSets(s.ReplicaSets, s.Timestamp)
			}

			if len(updateRecorder.UpdateArgs) != len(tt.expected) {
				t.Errorf("Expected result length of %d, got %d", len(tt.expected), len(updateRecorder.UpdateArgs))
			}

			for i, expected := range tt.expected {
				updateArg := updateRecorder.UpdateArgs[i]
				err := expected.Equals(updateArg)
				if err != nil {
					t.Errorf("Result did not match expected at index %d: %s", i, err.Error())
				}
			}
		})
	}
}
