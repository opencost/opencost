package scrape

import (
	"reflect"
	"testing"
	"time"

	"github.com/opencost/opencost/core/pkg/clustercache"
	"github.com/opencost/opencost/core/pkg/source"
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
		expected []metric.Update
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
			expected: []metric.Update{
				{
					Name: metric.KubeNodeStatusCapacityCPUCores,
					Labels: map[string]string{
						source.NodeLabel:       "node1",
						source.ProviderIDLabel: "i-1",
					},
					Value:          2.0,
					AdditionalInfo: nil,
				},
				{
					Name: metric.KubeNodeStatusCapacityMemoryBytes,
					Labels: map[string]string{
						source.NodeLabel:       "node1",
						source.ProviderIDLabel: "i-1",
					},
					Value:          2048.0,
					AdditionalInfo: nil,
				},
				{
					Name: metric.KubeNodeStatusAllocatableCPUCores,
					Labels: map[string]string{
						source.NodeLabel:       "node1",
						source.ProviderIDLabel: "i-1",
					},
					Value:          1.0,
					AdditionalInfo: nil,
				},
				{
					Name: metric.KubeNodeStatusAllocatableMemoryBytes,
					Labels: map[string]string{
						source.NodeLabel:       "node1",
						source.ProviderIDLabel: "i-1",
					},
					Value:          1024.0,
					AdditionalInfo: nil,
				},
				{
					Name: metric.KubeNodeLabels,
					Labels: map[string]string{
						source.NodeLabel:       "node1",
						source.ProviderIDLabel: "i-1",
					},
					Value: 0,
					AdditionalInfo: map[string]string{
						"label_test1": "blah",
						"label_test2": "blah2",
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ks := &ClusterCacheScraper{}
			var scrapeResults []metric.Update
			for _, s := range tt.scrapes {
				res := ks.scrapeNodes(s.Nodes)
				scrapeResults = append(scrapeResults, res...)
			}

			if len(scrapeResults) != len(tt.expected) {
				t.Errorf("Expected result length of %d, got %d", len(tt.expected), len(scrapeResults))
			}

			for i, expected := range tt.expected {
				got := scrapeResults[i]
				if !reflect.DeepEqual(expected, got) {
					t.Errorf("Result did not match expected at index %d: got %v, want %v", i, got, expected)
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
		expected []metric.Update
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
			expected: []metric.Update{

				{
					Name: metric.DeploymentMatchLabels,
					Labels: map[string]string{
						source.DeploymentLabel: "deployment1",
						source.NamespaceLabel:  "namespace1",
					},
					Value: 0,
					AdditionalInfo: map[string]string{
						"label_test1": "blah",
						"label_test2": "blah2",
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ks := &ClusterCacheScraper{}
			var scrapeResults []metric.Update
			for _, s := range tt.scrapes {
				res := ks.scrapeDeployments(s.Deployments)
				scrapeResults = append(scrapeResults, res...)
			}

			if len(scrapeResults) != len(tt.expected) {
				t.Errorf("Expected result length of %d, got %d", len(tt.expected), len(scrapeResults))
			}

			for i, expected := range tt.expected {
				got := scrapeResults[i]
				if !reflect.DeepEqual(expected, got) {
					t.Errorf("Result did not match expected at index %d: got %v, want %v", i, got, expected)
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
		expected []metric.Update
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
			expected: []metric.Update{
				{
					Name: metric.KubeNamespaceLabels,
					Labels: map[string]string{
						source.NamespaceLabel: "namespace1",
					},
					Value: 0,
					AdditionalInfo: map[string]string{
						"label_test1": "blah",
						"label_test2": "blah2",
					},
				},
				{
					Name: metric.KubeNamespaceAnnotations,
					Labels: map[string]string{
						source.NamespaceLabel: "namespace1",
					},
					Value: 0,
					AdditionalInfo: map[string]string{
						"annotation_test3": "blah3",
						"annotation_test4": "blah4",
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ks := &ClusterCacheScraper{}
			var scrapeResults []metric.Update
			for _, s := range tt.scrapes {
				res := ks.scrapeNamespaces(s.Namespaces)
				scrapeResults = append(scrapeResults, res...)
			}

			if len(scrapeResults) != len(tt.expected) {
				t.Errorf("Expected result length of %d, got %d", len(tt.expected), len(scrapeResults))
			}

			for i, expected := range tt.expected {
				got := scrapeResults[i]
				if !reflect.DeepEqual(expected, got) {
					t.Errorf("Result did not match expected at index %d: got %v, want %v", i, got, expected)
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
		expected []metric.Update
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
									Kind:       source.DeploymentLabel,
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
			expected: []metric.Update{
				{
					Name: metric.KubePodLabels,
					Labels: map[string]string{
						source.PodLabel:       "pod1",
						source.NamespaceLabel: "namespace1",
						source.UIDLabel:       "uuid1",
						source.NodeLabel:      "node1",
						source.InstanceLabel:  "node1",
					},
					Value: 0,
					AdditionalInfo: map[string]string{
						"label_test1": "blah",
						"label_test2": "blah2",
					},
				},
				{
					Name: metric.KubePodAnnotations,
					Labels: map[string]string{
						source.PodLabel:       "pod1",
						source.NamespaceLabel: "namespace1",
						source.UIDLabel:       "uuid1",
						source.NodeLabel:      "node1",
						source.InstanceLabel:  "node1",
					},
					Value: 0,
					AdditionalInfo: map[string]string{
						"annotation_test3": "blah3",
						"annotation_test4": "blah4",
					},
				},
				{
					Name: metric.KubePodOwner,
					Labels: map[string]string{
						source.PodLabel:       "pod1",
						source.NamespaceLabel: "namespace1",
						source.UIDLabel:       "uuid1",
						source.NodeLabel:      "node1",
						source.InstanceLabel:  "node1",
						source.OwnerKindLabel: "deployment",
						source.OwnerNameLabel: "deployment1",
					},
					Value:          0,
					AdditionalInfo: nil,
				},
				{
					Name: metric.KubePodContainerStatusRunning,
					Labels: map[string]string{
						source.PodLabel:       "pod1",
						source.NamespaceLabel: "namespace1",
						source.UIDLabel:       "uuid1",
						source.NodeLabel:      "node1",
						source.InstanceLabel:  "node1",
						source.ContainerLabel: "container1",
					},
					Value:          0,
					AdditionalInfo: nil,
				},
				{
					Name: metric.KubePodContainerResourceRequests,
					Labels: map[string]string{
						source.PodLabel:       "pod1",
						source.NamespaceLabel: "namespace1",
						source.UIDLabel:       "uuid1",
						source.NodeLabel:      "node1",
						source.InstanceLabel:  "node1",
						source.ContainerLabel: "container1",
						source.ResourceLabel:  "cpu",
						source.UnitLabel:      "core",
					},
					Value:          0.5,
					AdditionalInfo: nil,
				},
				{
					Name: metric.KubePodContainerResourceRequests,
					Labels: map[string]string{
						source.PodLabel:       "pod1",
						source.NamespaceLabel: "namespace1",
						source.UIDLabel:       "uuid1",
						source.NodeLabel:      "node1",
						source.InstanceLabel:  "node1",
						source.ContainerLabel: "container1",
						source.ResourceLabel:  "memory",
						source.UnitLabel:      "byte",
					},
					Value:          512,
					AdditionalInfo: nil,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ks := &ClusterCacheScraper{}
			var scrapeResults []metric.Update
			for _, s := range tt.scrapes {
				res := ks.scrapePods(s.Pods)
				scrapeResults = append(scrapeResults, res...)
			}

			if len(scrapeResults) != len(tt.expected) {
				t.Errorf("Expected result length of %d, got %d", len(tt.expected), len(scrapeResults))
			}

			for i, expected := range tt.expected {
				got := scrapeResults[i]
				if !reflect.DeepEqual(expected, got) {
					t.Errorf("Result did not match expected at index %d: got %v, want %v", i, got, expected)
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
		expected []metric.Update
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
			expected: []metric.Update{
				{
					Name: metric.KubePersistentVolumeClaimInfo,
					Labels: map[string]string{
						source.PVCLabel:          "pvc1",
						source.NamespaceLabel:    "namespace1",
						source.VolumeNameLabel:   "vol1",
						source.StorageClassLabel: "storageClass1",
					},
					Value:          0,
					AdditionalInfo: nil,
				},
				{
					Name: metric.KubePersistentVolumeClaimResourceRequestsStorageBytes,
					Labels: map[string]string{
						source.PVCLabel:          "pvc1",
						source.NamespaceLabel:    "namespace1",
						source.VolumeNameLabel:   "vol1",
						source.StorageClassLabel: "storageClass1",
					},
					Value:          4096,
					AdditionalInfo: nil,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ks := &ClusterCacheScraper{}
			var scrapeResults []metric.Update
			for _, s := range tt.scrapes {
				res := ks.scrapePVCs(s.PVCs)
				scrapeResults = append(scrapeResults, res...)
			}

			if len(scrapeResults) != len(tt.expected) {
				t.Errorf("Expected result length of %d, got %d", len(tt.expected), len(scrapeResults))
			}

			for i, expected := range tt.expected {
				got := scrapeResults[i]
				if !reflect.DeepEqual(expected, got) {
					t.Errorf("Result did not match expected at index %d: got %v, want %v", i, got, expected)
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
		expected []metric.Update
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
			expected: []metric.Update{
				{
					Name: metric.KubecostPVInfo,
					Labels: map[string]string{
						source.PVLabel:           "pv1",
						source.ProviderIDLabel:   "vol-1",
						source.StorageClassLabel: "storageClass1",
					},
					Value:          0,
					AdditionalInfo: nil,
				},
				{
					Name: metric.KubePersistentVolumeCapacityBytes,
					Labels: map[string]string{
						source.PVLabel:           "pv1",
						source.ProviderIDLabel:   "vol-1",
						source.StorageClassLabel: "storageClass1",
					},
					Value:          4096,
					AdditionalInfo: nil,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ks := &ClusterCacheScraper{}
			var scrapeResults []metric.Update
			for _, s := range tt.scrapes {
				res := ks.scrapePVs(s.PVs)
				scrapeResults = append(scrapeResults, res...)
			}

			if len(scrapeResults) != len(tt.expected) {
				t.Errorf("Expected result length of %d, got %d", len(tt.expected), len(scrapeResults))
			}

			for i, expected := range tt.expected {
				got := scrapeResults[i]
				if !reflect.DeepEqual(expected, got) {
					t.Errorf("Result did not match expected at index %d: got %v, want %v", i, got, expected)
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
		expected []metric.Update
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
			expected: []metric.Update{
				{
					Name: metric.ServiceSelectorLabels,
					Labels: map[string]string{
						"service":             "service1",
						source.NamespaceLabel: "namespace1",
					},
					Value: 0,
					AdditionalInfo: map[string]string{
						"label_test1": "blah",
						"label_test2": "blah2",
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ks := &ClusterCacheScraper{}
			var scrapeResults []metric.Update
			for _, s := range tt.scrapes {
				res := ks.scrapeServices(s.Services)
				scrapeResults = append(scrapeResults, res...)
			}

			if len(scrapeResults) != len(tt.expected) {
				t.Errorf("Expected result length of %d, got %d", len(tt.expected), len(scrapeResults))
			}

			for i, expected := range tt.expected {
				got := scrapeResults[i]
				if !reflect.DeepEqual(expected, got) {
					t.Errorf("Result did not match expected at index %d: got %v, want %v", i, got, expected)
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
		expected []metric.Update
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
			expected: []metric.Update{
				{
					Name: metric.StatefulSetMatchLabels,
					Labels: map[string]string{
						source.StatefulSetLabel: "statefulSet1",
						source.NamespaceLabel:   "namespace1",
					},
					Value: 0,
					AdditionalInfo: map[string]string{
						"label_test1": "blah",
						"label_test2": "blah2",
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ks := &ClusterCacheScraper{}
			var scrapeResults []metric.Update
			for _, s := range tt.scrapes {
				res := ks.scrapeStatefulSets(s.StatefulSets)
				scrapeResults = append(scrapeResults, res...)
			}

			if len(scrapeResults) != len(tt.expected) {
				t.Errorf("Expected result length of %d, got %d", len(tt.expected), len(scrapeResults))
			}

			for i, expected := range tt.expected {
				got := scrapeResults[i]
				if !reflect.DeepEqual(expected, got) {
					t.Errorf("Result did not match expected at index %d: got %v, want %v", i, got, expected)
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
		expected []metric.Update
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
						{
							Name:            "pureReplicaSet",
							Namespace:       "namespace1",
							OwnerReferences: []metav1.OwnerReference{},
						},
					},
					Timestamp: start1,
				},
			},
			expected: []metric.Update{
				{
					Name: metric.KubeReplicasetOwner,
					Labels: map[string]string{
						"replicaset":          "replicaSet1",
						source.NamespaceLabel: "namespace1",
						source.OwnerNameLabel: "rollout1",
						source.OwnerKindLabel: "Rollout",
					},
					Value: 0,
				},
				{
					Name: metric.KubeReplicasetOwner,
					Labels: map[string]string{
						"replicaset":          "pureReplicaSet",
						source.NamespaceLabel: "namespace1",
						source.OwnerNameLabel: source.NoneLabelValue,
						source.OwnerKindLabel: source.NoneLabelValue,
					},
					Value: 0,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ks := &ClusterCacheScraper{}
			var scrapeResults []metric.Update
			for _, s := range tt.scrapes {
				res := ks.scrapeReplicaSets(s.ReplicaSets)
				scrapeResults = append(scrapeResults, res...)
			}

			if len(scrapeResults) != len(tt.expected) {
				t.Errorf("Expected result length of %d, got %d", len(tt.expected), len(scrapeResults))
			}

			for i, expected := range tt.expected {
				got := scrapeResults[i]
				if !reflect.DeepEqual(expected, got) {
					t.Errorf("Result did not match expected at index %d: got %v, want %v", i, got, expected)
				}
			}
		})
	}
}
