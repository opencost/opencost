package collector

import (
	"testing"
	"time"

	"github.com/opencost/opencost/core/pkg/clustercache"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func Test_kubernetesScraper_scrapeNodes(t *testing.T) {

	start1, _ := time.Parse(time.RFC3339, start1Str)

	type scrape struct {
		nodes     []*clustercache.Node
		timestamp time.Time
	}
	tests := []struct {
		name     string
		scrapes  []scrape
		expected []UpdateArgs
	}{
		{
			name: "simple",
			scrapes: []scrape{
				{
					nodes: []*clustercache.Node{
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
					timestamp: start1,
				},
			},
			expected: []UpdateArgs{
				{
					metricName: KubeNodeStatusCapacityCPUCores,
					labels: map[string]string{
						"node":        "node1",
						"provider_id": "i-1",
					},
					value:                 2.0,
					timestamp:             &start1,
					additionalInformation: nil,
				},
				{
					metricName: KubeNodeStatusCapacityMemoryBytes,
					labels: map[string]string{
						"node":        "node1",
						"provider_id": "i-1",
					},
					value:                 2048.0,
					timestamp:             &start1,
					additionalInformation: nil,
				},
				{
					metricName: KubeNodeStatusAllocatableCPUCores,
					labels: map[string]string{
						"node":        "node1",
						"provider_id": "i-1",
					},
					value:                 1.0,
					timestamp:             &start1,
					additionalInformation: nil,
				},
				{
					metricName: KubeNodeStatusAllocatableMemoryBytes,
					labels: map[string]string{
						"node":        "node1",
						"provider_id": "i-1",
					},
					value:                 1024.0,
					timestamp:             &start1,
					additionalInformation: nil,
				},
				{
					metricName: KubeNodeLabels,
					labels: map[string]string{
						"node":        "node1",
						"provider_id": "i-1",
					},
					value:     0,
					timestamp: &start1,
					additionalInformation: map[string]string{
						"label_test1": "blah",
						"label_test2": "blah2",
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			updateRecorder := UpdateRecorderCollector{}
			ks := &kubernetesScraper{
				collector: &updateRecorder,
			}
			for _, s := range tt.scrapes {
				ks.scrapeNodes(s.nodes, s.timestamp)
			}

			if len(updateRecorder.updateArgs) != len(tt.expected) {
				t.Errorf("Expected result length of %d, got %d", len(tt.expected), len(updateRecorder.updateArgs))
			}

			for i, expected := range tt.expected {
				updateArg := updateRecorder.updateArgs[i]
				err := expected.equals(updateArg)
				if err != nil {
					t.Errorf("Result did not match expected at index %d: %s", i, err.Error())
				}
			}
		})
	}
}

func Test_kubernetesScraper_scrapeDeployments(t *testing.T) {

	start1, _ := time.Parse(time.RFC3339, start1Str)

	type scrape struct {
		deployments []*clustercache.Deployment
		timestamp   time.Time
	}
	tests := []struct {
		name     string
		scrapes  []scrape
		expected []UpdateArgs
	}{
		{
			name: "simple",
			scrapes: []scrape{
				{
					deployments: []*clustercache.Deployment{
						{
							Name:      "deployment1",
							Namespace: "namespace1",
							MatchLabels: map[string]string{
								"test1": "blah",
								"test2": "blah2",
							},
						},
					},
					timestamp: start1,
				},
			},
			expected: []UpdateArgs{

				{
					metricName: DeploymentMatchLabels,
					labels: map[string]string{
						"deployment": "deployment1",
						"namespace":  "namespace1",
					},
					value:     0,
					timestamp: &start1,
					additionalInformation: map[string]string{
						"label_test1": "blah",
						"label_test2": "blah2",
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			updateRecorder := UpdateRecorderCollector{}
			ks := &kubernetesScraper{
				collector: &updateRecorder,
			}
			for _, s := range tt.scrapes {
				ks.scrapeDeployments(s.deployments, s.timestamp)
			}

			if len(updateRecorder.updateArgs) != len(tt.expected) {
				t.Errorf("Expected result length of %d, got %d", len(tt.expected), len(updateRecorder.updateArgs))
			}

			for i, expected := range tt.expected {
				updateArg := updateRecorder.updateArgs[i]
				err := expected.equals(updateArg)
				if err != nil {
					t.Errorf("Result did not match expected at index %d: %s", i, err.Error())
				}
			}
		})
	}
}

func Test_kubernetesScraper_scrapeNamespaces(t *testing.T) {

	start1, _ := time.Parse(time.RFC3339, start1Str)

	type scrape struct {
		namespaces []*clustercache.Namespace
		timestamp  time.Time
	}
	tests := []struct {
		name     string
		scrapes  []scrape
		expected []UpdateArgs
	}{
		{
			name: "simple",
			scrapes: []scrape{
				{
					namespaces: []*clustercache.Namespace{
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
					timestamp: start1,
				},
			},
			expected: []UpdateArgs{
				{
					metricName: KubeNamespaceLabels,
					labels: map[string]string{
						"namespace": "namespace1",
					},
					value:     0,
					timestamp: &start1,
					additionalInformation: map[string]string{
						"label_test1": "blah",
						"label_test2": "blah2",
					},
				},
				{
					metricName: KubeNamespaceAnnotations,
					labels: map[string]string{
						"namespace": "namespace1",
					},
					value:     0,
					timestamp: &start1,
					additionalInformation: map[string]string{
						"annotation_test3": "blah3",
						"annotation_test4": "blah4",
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			updateRecorder := UpdateRecorderCollector{}
			ks := &kubernetesScraper{
				collector: &updateRecorder,
			}
			for _, s := range tt.scrapes {
				ks.scrapeNamespaces(s.namespaces, s.timestamp)
			}

			if len(updateRecorder.updateArgs) != len(tt.expected) {
				t.Errorf("Expected result length of %d, got %d", len(tt.expected), len(updateRecorder.updateArgs))
			}

			for i, expected := range tt.expected {
				updateArg := updateRecorder.updateArgs[i]
				err := expected.equals(updateArg)
				if err != nil {
					t.Errorf("Result did not match expected at index %d: %s", i, err.Error())
				}
			}
		})
	}
}

func Test_kubernetesScraper_scrapePods(t *testing.T) {

	start1, _ := time.Parse(time.RFC3339, start1Str)

	type scrape struct {
		pods      []*clustercache.Pod
		timestamp time.Time
	}
	tests := []struct {
		name     string
		scrapes  []scrape
		expected []UpdateArgs
	}{
		{
			name: "simple",
			scrapes: []scrape{
				{
					pods: []*clustercache.Pod{
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
					timestamp: start1,
				},
			},
			expected: []UpdateArgs{
				{
					metricName: KubePodLabels,
					labels: map[string]string{
						"name":      "pod1",
						"namespace": "namespace1",
						"uid":       "uuid1",
						"node":      "node1",
					},
					value:     0,
					timestamp: &start1,
					additionalInformation: map[string]string{
						"label_test1": "blah",
						"label_test2": "blah2",
					},
				},
				{
					metricName: KubePodAnnotations,
					labels: map[string]string{
						"name":      "pod1",
						"namespace": "namespace1",
						"uid":       "uuid1",
						"node":      "node1",
					},
					value:     0,
					timestamp: &start1,
					additionalInformation: map[string]string{
						"annotation_test3": "blah3",
						"annotation_test4": "blah4",
					},
				},
				{
					metricName: KubePodOwner,
					labels: map[string]string{
						"name":                "pod1",
						"namespace":           "namespace1",
						"uid":                 "uuid1",
						"node":                "node1",
						"owner_kind":          "deployment",
						"owner_name":          "deployment1",
						"owner_is_controller": "false",
					},
					value:                 0,
					timestamp:             &start1,
					additionalInformation: nil,
				},
				{
					metricName: KubePodContainerStatusRunning,
					labels: map[string]string{
						"name":      "pod1",
						"namespace": "namespace1",
						"uid":       "uuid1",
						"node":      "node1",
						"container": "container1",
					},
					value:                 0,
					timestamp:             &start1,
					additionalInformation: nil,
				},
				{
					metricName: KubePodContainerResourceRequests,
					labels: map[string]string{
						"name":      "pod1",
						"namespace": "namespace1",
						"uid":       "uuid1",
						"node":      "node1",
						"container": "container1",
						"resource":  "cpu",
						"unit":      "core",
					},
					value:                 0.5,
					timestamp:             &start1,
					additionalInformation: nil,
				},
				{
					metricName: KubePodContainerResourceRequests,
					labels: map[string]string{
						"name":      "pod1",
						"namespace": "namespace1",
						"uid":       "uuid1",
						"node":      "node1",
						"container": "container1",
						"resource":  "memory",
						"unit":      "byte",
					},
					value:                 512,
					timestamp:             &start1,
					additionalInformation: nil,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			updateRecorder := UpdateRecorderCollector{}
			ks := &kubernetesScraper{
				collector: &updateRecorder,
			}
			for _, s := range tt.scrapes {
				ks.scrapePods(s.pods, s.timestamp)
			}

			if len(updateRecorder.updateArgs) != len(tt.expected) {
				t.Errorf("Expected result length of %d, got %d", len(tt.expected), len(updateRecorder.updateArgs))
			}

			for i, expected := range tt.expected {
				updateArg := updateRecorder.updateArgs[i]
				err := expected.equals(updateArg)
				if err != nil {
					t.Errorf("Result did not match expected at index %d: %s", i, err.Error())
				}
			}
		})
	}
}

func Test_kubernetesScraper_scrapePVCs(t *testing.T) {

	start1, _ := time.Parse(time.RFC3339, start1Str)

	type scrape struct {
		pvcs      []*clustercache.PersistentVolumeClaim
		timestamp time.Time
	}
	tests := []struct {
		name     string
		scrapes  []scrape
		expected []UpdateArgs
	}{
		{
			name: "simple",
			scrapes: []scrape{
				{
					pvcs: []*clustercache.PersistentVolumeClaim{
						{
							Name:      "pvc1",
							Namespace: "namespace1",
							Spec: v1.PersistentVolumeClaimSpec{
								VolumeName:       "vol1",
								StorageClassName: ptr("storageClass1"),
								Resources: v1.VolumeResourceRequirements{
									Requests: v1.ResourceList{
										v1.ResourceStorage: resource.MustParse("4096"),
									},
								},
							},
						},
					},
					timestamp: start1,
				},
			},
			expected: []UpdateArgs{
				{
					metricName: KubePersistenVolumeClaimInfo,
					labels: map[string]string{
						"name":         "pvc1",
						"namespace":    "namespace1",
						"volumename":   "vol1",
						"storageclass": "storageClass1",
					},
					value:                 0,
					timestamp:             &start1,
					additionalInformation: nil,
				},
				{
					metricName: KubePersistentVolumeClaimResourceRequestsStorageBytes,
					labels: map[string]string{
						"name":         "pvc1",
						"namespace":    "namespace1",
						"volumename":   "vol1",
						"storageclass": "storageClass1",
					},
					value:                 4096,
					timestamp:             &start1,
					additionalInformation: nil,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			updateRecorder := UpdateRecorderCollector{}
			ks := &kubernetesScraper{
				collector: &updateRecorder,
			}
			for _, s := range tt.scrapes {
				ks.scrapePVCs(s.pvcs, s.timestamp)
			}

			if len(updateRecorder.updateArgs) != len(tt.expected) {
				t.Errorf("Expected result length of %d, got %d", len(tt.expected), len(updateRecorder.updateArgs))
			}

			for i, expected := range tt.expected {
				updateArg := updateRecorder.updateArgs[i]
				err := expected.equals(updateArg)
				if err != nil {
					t.Errorf("Result did not match expected at index %d: %s", i, err.Error())
				}
			}
		})
	}
}

func Test_kubernetesScraper_scrapePVs(t *testing.T) {

	start1, _ := time.Parse(time.RFC3339, start1Str)

	type scrape struct {
		pvs       []*clustercache.PersistentVolume
		timestamp time.Time
	}
	tests := []struct {
		name     string
		scrapes  []scrape
		expected []UpdateArgs
	}{
		{
			name: "simple",
			scrapes: []scrape{
				{
					pvs: []*clustercache.PersistentVolume{
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
					timestamp: start1,
				},
			},
			expected: []UpdateArgs{
				{
					metricName: KubecostPVInfo,
					labels: map[string]string{
						"name":         "pv1",
						"providerID":   "vol-1",
						"storageClass": "storageClass1",
					},
					value:                 0,
					timestamp:             &start1,
					additionalInformation: nil,
				},
				{
					metricName: KubePersistentVolumeCapacityBytes,
					labels: map[string]string{
						"name":         "pv1",
						"providerID":   "vol-1",
						"storageClass": "storageClass1",
					},
					value:                 4096,
					timestamp:             &start1,
					additionalInformation: nil,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			updateRecorder := UpdateRecorderCollector{}
			ks := &kubernetesScraper{
				collector: &updateRecorder,
			}
			for _, s := range tt.scrapes {
				ks.scrapePVs(s.pvs, s.timestamp)
			}

			if len(updateRecorder.updateArgs) != len(tt.expected) {
				t.Errorf("Expected result length of %d, got %d", len(tt.expected), len(updateRecorder.updateArgs))
			}

			for i, expected := range tt.expected {
				updateArg := updateRecorder.updateArgs[i]
				err := expected.equals(updateArg)
				if err != nil {
					t.Errorf("Result did not match expected at index %d: %s", i, err.Error())
				}
			}
		})
	}
}

func Test_kubernetesScraper_scrapeServices(t *testing.T) {

	start1, _ := time.Parse(time.RFC3339, start1Str)

	type scrape struct {
		services  []*clustercache.Service
		timestamp time.Time
	}
	tests := []struct {
		name     string
		scrapes  []scrape
		expected []UpdateArgs
	}{
		{
			name: "simple",
			scrapes: []scrape{
				{
					services: []*clustercache.Service{
						{
							Name:      "service1",
							Namespace: "namespace1",
							SpecSelector: map[string]string{
								"test1": "blah",
								"test2": "blah2",
							},
						},
					},
					timestamp: start1,
				},
			},
			expected: []UpdateArgs{
				{
					metricName: ServiceSelectorLabels,
					labels: map[string]string{
						"service":   "service1",
						"namespace": "namespace1",
					},
					value:     0,
					timestamp: &start1,
					additionalInformation: map[string]string{
						"label_test1": "blah",
						"label_test2": "blah2",
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			updateRecorder := UpdateRecorderCollector{}
			ks := &kubernetesScraper{
				collector: &updateRecorder,
			}
			for _, s := range tt.scrapes {
				ks.scrapeServices(s.services, s.timestamp)
			}

			if len(updateRecorder.updateArgs) != len(tt.expected) {
				t.Errorf("Expected result length of %d, got %d", len(tt.expected), len(updateRecorder.updateArgs))
			}

			for i, expected := range tt.expected {
				updateArg := updateRecorder.updateArgs[i]
				err := expected.equals(updateArg)
				if err != nil {
					t.Errorf("Result did not match expected at index %d: %s", i, err.Error())
				}
			}
		})
	}
}

func Test_kubernetesScraper_scrapeStatefulSets(t *testing.T) {

	start1, _ := time.Parse(time.RFC3339, start1Str)

	type scrape struct {
		statefulSets []*clustercache.StatefulSet
		timestamp    time.Time
	}
	tests := []struct {
		name     string
		scrapes  []scrape
		expected []UpdateArgs
	}{
		{
			name: "simple",
			scrapes: []scrape{
				{
					statefulSets: []*clustercache.StatefulSet{
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
					timestamp: start1,
				},
			},
			expected: []UpdateArgs{
				{
					metricName: StatefulSetMatchLabels,
					labels: map[string]string{
						"name":      "statefulSet1",
						"namespace": "namespace1",
					},
					value:     0,
					timestamp: &start1,
					additionalInformation: map[string]string{
						"label_test1": "blah",
						"label_test2": "blah2",
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			updateRecorder := UpdateRecorderCollector{}
			ks := &kubernetesScraper{
				collector: &updateRecorder,
			}
			for _, s := range tt.scrapes {
				ks.scrapeStatefulSets(s.statefulSets, s.timestamp)
			}

			if len(updateRecorder.updateArgs) != len(tt.expected) {
				t.Errorf("Expected result length of %d, got %d", len(tt.expected), len(updateRecorder.updateArgs))
			}

			for i, expected := range tt.expected {
				updateArg := updateRecorder.updateArgs[i]
				err := expected.equals(updateArg)
				if err != nil {
					t.Errorf("Result did not match expected at index %d: %s", i, err.Error())
				}
			}
		})
	}
}
