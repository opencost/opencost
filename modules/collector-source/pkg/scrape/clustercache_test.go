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
	"k8s.io/apimachinery/pkg/types"
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
							UID:            "uuid1",
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
					Name: metric.NodeInfo,
					Labels: map[string]string{
						source.NodeLabel:       "node1",
						source.ProviderIDLabel: "i-1",
						source.UIDLabel:        "uuid1",
					},
					Value: 0,
					AdditionalInfo: map[string]string{
						source.NodeLabel:       "node1",
						source.ProviderIDLabel: "i-1",
						source.UIDLabel:        "uuid1",
					},
				},
				{
					Name: metric.NodeResourceCapacities,
					Labels: map[string]string{
						source.NodeLabel:       "node1",
						source.ProviderIDLabel: "i-1",
						source.UIDLabel:        "uuid1",
						source.ResourceLabel:   "cpu",
						source.UnitLabel:       "core",
					},
					Value:          2.0,
					AdditionalInfo: nil,
				},
				{
					Name: metric.NodeResourceCapacities,
					Labels: map[string]string{
						source.NodeLabel:       "node1",
						source.ProviderIDLabel: "i-1",
						source.UIDLabel:        "uuid1",
						source.ResourceLabel:   "memory",
						source.UnitLabel:       "byte",
					},
					Value:          2048.0,
					AdditionalInfo: nil,
				},
				{
					Name: metric.KubeNodeStatusCapacityCPUCores,
					Labels: map[string]string{
						source.NodeLabel:       "node1",
						source.ProviderIDLabel: "i-1",
						source.UIDLabel:        "uuid1",
					},
					Value:          2.0,
					AdditionalInfo: nil,
				},
				{
					Name: metric.KubeNodeStatusCapacityMemoryBytes,
					Labels: map[string]string{
						source.NodeLabel:       "node1",
						source.ProviderIDLabel: "i-1",
						source.UIDLabel:        "uuid1",
					},
					Value:          2048.0,
					AdditionalInfo: nil,
				},
				{
					Name: metric.NodeResourcesAllocatable,
					Labels: map[string]string{
						source.NodeLabel:       "node1",
						source.ProviderIDLabel: "i-1",
						source.UIDLabel:        "uuid1",
						source.ResourceLabel:   "cpu",
						source.UnitLabel:       "core",
					},
					Value:          1.0,
					AdditionalInfo: nil,
				},
				{
					Name: metric.NodeResourcesAllocatable,
					Labels: map[string]string{
						source.NodeLabel:       "node1",
						source.ProviderIDLabel: "i-1",
						source.UIDLabel:        "uuid1",
						source.ResourceLabel:   "memory",
						source.UnitLabel:       "byte",
					},
					Value:          1024.0,
					AdditionalInfo: nil,
				},
				{
					Name: metric.KubeNodeStatusAllocatableCPUCores,
					Labels: map[string]string{
						source.NodeLabel:       "node1",
						source.ProviderIDLabel: "i-1",
						source.UIDLabel:        "uuid1",
					},
					Value:          1.0,
					AdditionalInfo: nil,
				},
				{
					Name: metric.KubeNodeStatusAllocatableMemoryBytes,
					Labels: map[string]string{
						source.NodeLabel:       "node1",
						source.ProviderIDLabel: "i-1",
						source.UIDLabel:        "uuid1",
					},
					Value:          1024.0,
					AdditionalInfo: nil,
				},
				{
					Name: metric.KubeNodeLabels,
					Labels: map[string]string{
						source.NodeLabel:       "node1",
						source.ProviderIDLabel: "i-1",
						source.UIDLabel:        "uuid1",
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
							UID:       "uuid1",
							MatchLabels: map[string]string{
								"test1": "blah",
								"test2": "blah2",
							},
						},
					},
					Timestamp: start1,
				},
			},
			// deploymentInfo map is shared across all 4 metrics and has namespace
			// added to it before DeploymentMatchLabels is appended, so all 4 metrics
			// reflect the final state: {uid, namespace_uid, deployment, namespace}.
			expected: []metric.Update{
				{
					Name: metric.DeploymentInfo,
					Labels: map[string]string{
						source.UIDLabel:          "uuid1",
						source.NamespaceUIDLabel: "ns-uuid1",
						source.DeploymentLabel:   "deployment1",
						source.NamespaceLabel:    "namespace1",
					},
					Value: 0,
					AdditionalInfo: map[string]string{
						source.UIDLabel:          "uuid1",
						source.NamespaceUIDLabel: "ns-uuid1",
						source.DeploymentLabel:   "deployment1",
						source.NamespaceLabel:    "namespace1",
					},
				},
				{
					Name: metric.DeploymentLabels,
					Labels: map[string]string{
						source.UIDLabel:          "uuid1",
						source.NamespaceUIDLabel: "ns-uuid1",
						source.DeploymentLabel:   "deployment1",
						source.NamespaceLabel:    "namespace1",
					},
					Value:          0,
					AdditionalInfo: map[string]string{},
				},
				{
					Name: metric.DeploymentAnnotations,
					Labels: map[string]string{
						source.UIDLabel:          "uuid1",
						source.NamespaceUIDLabel: "ns-uuid1",
						source.DeploymentLabel:   "deployment1",
						source.NamespaceLabel:    "namespace1",
					},
					Value:          0,
					AdditionalInfo: map[string]string{},
				},
				{
					Name: metric.DeploymentMatchLabels,
					Labels: map[string]string{
						source.UIDLabel:          "uuid1",
						source.NamespaceUIDLabel: "ns-uuid1",
						source.DeploymentLabel:   "deployment1",
						source.NamespaceLabel:    "namespace1",
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
			nsIndex := newSyncMap[string, types.UID](0)
			nsIndex.Set("namespace1", "ns-uuid1")
			var scrapeResults []metric.Update
			for _, s := range tt.scrapes {
				res := ks.scrapeDeployments(s.Deployments, nsIndex)
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
							UID:  "uuid1",
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
					Name: metric.NamespaceInfo,
					Labels: map[string]string{
						source.NamespaceLabel: "namespace1",
						source.UIDLabel:       "uuid1",
					},
					Value: 0,
					AdditionalInfo: map[string]string{
						source.NamespaceLabel: "namespace1",
						source.UIDLabel:       "uuid1",
					},
				},
				{
					Name: metric.KubeNamespaceLabels,
					Labels: map[string]string{
						source.NamespaceLabel: "namespace1",
						source.UIDLabel:       "uuid1",
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
						source.UIDLabel:       "uuid1",
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
		PVCs      []*clustercache.PersistentVolumeClaim
		Pods      []*clustercache.Pod
		Timestamp time.Time
	}
	tests := []struct {
		name     string
		nsSetup  func(*SyncMap[string, types.UID])
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
							UID:       "uuid1",
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
											Limits: map[v1.ResourceName]resource.Quantity{
												v1.ResourceCPU:    resource.MustParse("1"),
												v1.ResourceMemory: resource.MustParse("1024"),
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
			// podInfo is mutated after PodInfo is appended (namespace/node/instance added),
			// so PodInfo.Labels also reflects those fields at assertion time.
			expected: []metric.Update{
				{
					Name: metric.PodInfo,
					Labels: map[string]string{
						source.UIDLabel:          "uuid1",
						source.PodLabel:          "pod1",
						source.NamespaceUIDLabel: "",
						source.NodeUIDLabel:      "",
						source.NamespaceLabel:    "namespace1",
						source.NodeLabel:         "node1",
						source.InstanceLabel:     "node1",
					},
					Value: 0,
					AdditionalInfo: map[string]string{
						source.UIDLabel:          "uuid1",
						source.PodLabel:          "pod1",
						source.NamespaceUIDLabel: "",
						source.NodeUIDLabel:      "",
						source.NamespaceLabel:    "namespace1",
						source.NodeLabel:         "node1",
						source.InstanceLabel:     "node1",
					},
				},
				{
					Name: metric.KubePodLabels,
					Labels: map[string]string{
						source.UIDLabel:          "uuid1",
						source.PodLabel:          "pod1",
						source.NamespaceUIDLabel: "",
						source.NodeUIDLabel:      "",
						source.NamespaceLabel:    "namespace1",
						source.NodeLabel:         "node1",
						source.InstanceLabel:     "node1",
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
						source.UIDLabel:          "uuid1",
						source.PodLabel:          "pod1",
						source.NamespaceUIDLabel: "",
						source.NodeUIDLabel:      "",
						source.NamespaceLabel:    "namespace1",
						source.NodeLabel:         "node1",
						source.InstanceLabel:     "node1",
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
						source.UIDLabel:          "uuid1",
						source.PodLabel:          "pod1",
						source.NamespaceUIDLabel: "",
						source.NodeUIDLabel:      "",
						source.NamespaceLabel:    "namespace1",
						source.NodeLabel:         "node1",
						source.InstanceLabel:     "node1",
						source.OwnerKindLabel:    "deployment",
						source.OwnerNameLabel:    "deployment1",
						source.OwnerUIDLabel:     "",
						source.ContainerLabel:    "false",
					},
					Value:          0,
					AdditionalInfo: nil,
				},
				{
					Name: metric.KubePodContainerStatusRunning,
					Labels: map[string]string{
						source.UIDLabel:          "uuid1",
						source.PodLabel:          "pod1",
						source.NamespaceUIDLabel: "",
						source.NodeUIDLabel:      "",
						source.NamespaceLabel:    "namespace1",
						source.NodeLabel:         "node1",
						source.InstanceLabel:     "node1",
						source.ContainerLabel:    "container1",
					},
					Value: 0,
					AdditionalInfo: map[string]string{
						source.UIDLabel:          "uuid1",
						source.PodLabel:          "pod1",
						source.NamespaceUIDLabel: "",
						source.NodeUIDLabel:      "",
						source.NamespaceLabel:    "namespace1",
						source.NodeLabel:         "node1",
						source.InstanceLabel:     "node1",
						source.ContainerLabel:    "container1",
					},
				},
				{
					Name: metric.KubePodContainerResourceRequests,
					Labels: map[string]string{
						source.UIDLabel:          "uuid1",
						source.PodLabel:          "pod1",
						source.NamespaceUIDLabel: "",
						source.NodeUIDLabel:      "",
						source.NamespaceLabel:    "namespace1",
						source.NodeLabel:         "node1",
						source.InstanceLabel:     "node1",
						source.ContainerLabel:    "container1",
						source.ResourceLabel:     "cpu",
						source.UnitLabel:         "core",
					},
					Value:          0.5,
					AdditionalInfo: nil,
				},
				{
					Name: metric.KubePodContainerResourceRequests,
					Labels: map[string]string{
						source.UIDLabel:          "uuid1",
						source.PodLabel:          "pod1",
						source.NamespaceUIDLabel: "",
						source.NodeUIDLabel:      "",
						source.NamespaceLabel:    "namespace1",
						source.NodeLabel:         "node1",
						source.InstanceLabel:     "node1",
						source.ContainerLabel:    "container1",
						source.ResourceLabel:     "memory",
						source.UnitLabel:         "byte",
					},
					Value:          512,
					AdditionalInfo: nil,
				},
				{
					Name: metric.KubePodContainerResourceLimits,
					Labels: map[string]string{
						source.UIDLabel:          "uuid1",
						source.PodLabel:          "pod1",
						source.NamespaceUIDLabel: "",
						source.NodeUIDLabel:      "",
						source.NamespaceLabel:    "namespace1",
						source.NodeLabel:         "node1",
						source.InstanceLabel:     "node1",
						source.ContainerLabel:    "container1",
						source.ResourceLabel:     "cpu",
						source.UnitLabel:         "core",
					},
					Value:          1,
					AdditionalInfo: nil,
				},
				{
					Name: metric.KubePodContainerResourceLimits,
					Labels: map[string]string{
						source.UIDLabel:          "uuid1",
						source.PodLabel:          "pod1",
						source.NamespaceUIDLabel: "",
						source.NodeUIDLabel:      "",
						source.NamespaceLabel:    "namespace1",
						source.NodeLabel:         "node1",
						source.InstanceLabel:     "node1",
						source.ContainerLabel:    "container1",
						source.ResourceLabel:     "memory",
						source.UnitLabel:         "byte",
					},
					Value:          1024,
					AdditionalInfo: nil,
				},
			},
		},
		{
			name: "with namespace index",
			nsSetup: func(nsIndex *SyncMap[string, types.UID]) {
				nsIndex.Set("namespace1", "ns-uuid1")
			},
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
											Limits: map[v1.ResourceName]resource.Quantity{
												v1.ResourceCPU:    resource.MustParse("1"),
												v1.ResourceMemory: resource.MustParse("1024"),
											},
										},
									},
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
					Name: metric.PodInfo,
					Labels: map[string]string{
						source.UIDLabel:          "uuid1",
						source.PodLabel:          "pod1",
						source.NamespaceUIDLabel: "ns-uuid1",
						source.NodeUIDLabel:      "",
						source.NamespaceLabel:    "namespace1",
						source.NodeLabel:         "node1",
						source.InstanceLabel:     "node1",
					},
					Value: 0,
					AdditionalInfo: map[string]string{
						source.UIDLabel:          "uuid1",
						source.PodLabel:          "pod1",
						source.NamespaceUIDLabel: "ns-uuid1",
						source.NodeUIDLabel:      "",
						source.NamespaceLabel:    "namespace1",
						source.NodeLabel:         "node1",
						source.InstanceLabel:     "node1",
					},
				},
				{
					Name: metric.KubePodLabels,
					Labels: map[string]string{
						source.UIDLabel:          "uuid1",
						source.PodLabel:          "pod1",
						source.NamespaceUIDLabel: "ns-uuid1",
						source.NodeUIDLabel:      "",
						source.NamespaceLabel:    "namespace1",
						source.NodeLabel:         "node1",
						source.InstanceLabel:     "node1",
					},
					Value:          0,
					AdditionalInfo: map[string]string{},
				},
				{
					Name: metric.KubePodAnnotations,
					Labels: map[string]string{
						source.UIDLabel:          "uuid1",
						source.PodLabel:          "pod1",
						source.NamespaceUIDLabel: "ns-uuid1",
						source.NodeUIDLabel:      "",
						source.NamespaceLabel:    "namespace1",
						source.NodeLabel:         "node1",
						source.InstanceLabel:     "node1",
					},
					Value:          0,
					AdditionalInfo: map[string]string{},
				},
				{
					Name: metric.KubePodContainerStatusRunning,
					Labels: map[string]string{
						source.UIDLabel:          "uuid1",
						source.PodLabel:          "pod1",
						source.NamespaceUIDLabel: "ns-uuid1",
						source.NodeUIDLabel:      "",
						source.NamespaceLabel:    "namespace1",
						source.NodeLabel:         "node1",
						source.InstanceLabel:     "node1",
						source.ContainerLabel:    "container1",
					},
					Value: 0,
					AdditionalInfo: map[string]string{
						source.UIDLabel:          "uuid1",
						source.PodLabel:          "pod1",
						source.NamespaceUIDLabel: "ns-uuid1",
						source.NodeUIDLabel:      "",
						source.NamespaceLabel:    "namespace1",
						source.NodeLabel:         "node1",
						source.InstanceLabel:     "node1",
						source.ContainerLabel:    "container1",
					},
				},
				{
					Name: metric.KubePodContainerResourceRequests,
					Labels: map[string]string{
						source.UIDLabel:          "uuid1",
						source.PodLabel:          "pod1",
						source.NamespaceUIDLabel: "ns-uuid1",
						source.NodeUIDLabel:      "",
						source.NamespaceLabel:    "namespace1",
						source.NodeLabel:         "node1",
						source.InstanceLabel:     "node1",
						source.ContainerLabel:    "container1",
						source.ResourceLabel:     "cpu",
						source.UnitLabel:         "core",
					},
					Value:          0.5,
					AdditionalInfo: nil,
				},
				{
					Name: metric.KubePodContainerResourceRequests,
					Labels: map[string]string{
						source.UIDLabel:          "uuid1",
						source.PodLabel:          "pod1",
						source.NamespaceUIDLabel: "ns-uuid1",
						source.NodeUIDLabel:      "",
						source.NamespaceLabel:    "namespace1",
						source.NodeLabel:         "node1",
						source.InstanceLabel:     "node1",
						source.ContainerLabel:    "container1",
						source.ResourceLabel:     "memory",
						source.UnitLabel:         "byte",
					},
					Value:          512,
					AdditionalInfo: nil,
				},
				{
					Name: metric.KubePodContainerResourceLimits,
					Labels: map[string]string{
						source.UIDLabel:          "uuid1",
						source.PodLabel:          "pod1",
						source.NamespaceUIDLabel: "ns-uuid1",
						source.NodeUIDLabel:      "",
						source.NamespaceLabel:    "namespace1",
						source.NodeLabel:         "node1",
						source.InstanceLabel:     "node1",
						source.ContainerLabel:    "container1",
						source.ResourceLabel:     "cpu",
						source.UnitLabel:         "core",
					},
					Value:          1,
					AdditionalInfo: nil,
				},
				{
					Name: metric.KubePodContainerResourceLimits,
					Labels: map[string]string{
						source.UIDLabel:          "uuid1",
						source.PodLabel:          "pod1",
						source.NamespaceUIDLabel: "ns-uuid1",
						source.NodeUIDLabel:      "",
						source.NamespaceLabel:    "namespace1",
						source.NodeLabel:         "node1",
						source.InstanceLabel:     "node1",
						source.ContainerLabel:    "container1",
						source.ResourceLabel:     "memory",
						source.UnitLabel:         "byte",
					},
					Value:          1024,
					AdditionalInfo: nil,
				},
				{
					Name: metric.PodPVCAllocation,
					Labels: map[string]string{
						source.InstanceLabel:  "",
						source.NamespaceLabel: "namespace1",
						source.NodeLabel:      "",
						source.PVLabel:        "vol1",
						source.PVCLabel:       "pvc1",
						source.PodLabel:       "unmounted-pvs",
						source.UIDLabel:       "",
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
			nodeIndex := newSyncMap[string, types.UID](0)
			nsIndex := newSyncMap[string, types.UID](0)
			if tt.nsSetup != nil {
				tt.nsSetup(nsIndex)
			}
			pvcIndex := newSyncMap[pvcKey, types.UID](0)
			var scrapeResults []metric.Update
			for _, s := range tt.scrapes {
				res := ks.scrapePods(s.Pods, s.PVCs, nodeIndex, nsIndex, pvcIndex)
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
		nsSetup  func(*SyncMap[string, types.UID])
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
							UID:       "uuid1",
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
						source.UIDLabel:          "uuid1",
						source.PVCLabel:          "pvc1",
						source.NamespaceUIDLabel: "",
						source.NamespaceLabel:    "namespace1",
						source.VolumeNameLabel:   "vol1",
						source.PVUIDLabel:        "",
						source.StorageClassLabel: "storageClass1",
					},
					Value: 0,
					AdditionalInfo: map[string]string{
						source.UIDLabel:          "uuid1",
						source.PVCLabel:          "pvc1",
						source.NamespaceUIDLabel: "",
						source.NamespaceLabel:    "namespace1",
						source.VolumeNameLabel:   "vol1",
						source.PVUIDLabel:        "",
						source.StorageClassLabel: "storageClass1",
					},
				},
				{
					Name: metric.KubePersistentVolumeClaimResourceRequestsStorageBytes,
					Labels: map[string]string{
						source.UIDLabel:          "uuid1",
						source.PVCLabel:          "pvc1",
						source.NamespaceUIDLabel: "",
						source.NamespaceLabel:    "namespace1",
						source.VolumeNameLabel:   "vol1",
						source.PVUIDLabel:        "",
						source.StorageClassLabel: "storageClass1",
					},
					Value:          4096,
					AdditionalInfo: nil,
				},
			},
		},
		{
			name: "with namespace index",
			nsSetup: func(nsIndex *SyncMap[string, types.UID]) {
				nsIndex.Set("namespace1", "ns-uuid1")
			},
			scrapes: []scrape{
				{
					PVCs: []*clustercache.PersistentVolumeClaim{
						{
							Name:      "pvc1",
							Namespace: "namespace1",
							UID:       "uuid1",
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
						source.UIDLabel:          "uuid1",
						source.PVCLabel:          "pvc1",
						source.NamespaceUIDLabel: "ns-uuid1",
						source.NamespaceLabel:    "namespace1",
						source.VolumeNameLabel:   "vol1",
						source.PVUIDLabel:        "",
						source.StorageClassLabel: "storageClass1",
					},
					Value: 0,
					AdditionalInfo: map[string]string{
						source.UIDLabel:          "uuid1",
						source.PVCLabel:          "pvc1",
						source.NamespaceUIDLabel: "ns-uuid1",
						source.NamespaceLabel:    "namespace1",
						source.VolumeNameLabel:   "vol1",
						source.PVUIDLabel:        "",
						source.StorageClassLabel: "storageClass1",
					},
				},
				{
					Name: metric.KubePersistentVolumeClaimResourceRequestsStorageBytes,
					Labels: map[string]string{
						source.UIDLabel:          "uuid1",
						source.PVCLabel:          "pvc1",
						source.NamespaceUIDLabel: "ns-uuid1",
						source.NamespaceLabel:    "namespace1",
						source.VolumeNameLabel:   "vol1",
						source.PVUIDLabel:        "",
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
			nsIndex := newSyncMap[string, types.UID](0)
			if tt.nsSetup != nil {
				tt.nsSetup(nsIndex)
			}
			pvIndex := newSyncMap[string, types.UID](0)
			var scrapeResults []metric.Update
			for _, s := range tt.scrapes {
				res := ks.scrapePVCs(s.PVCs, nsIndex, pvIndex)
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
							UID:  "uuid1",
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
						source.UIDLabel:             "uuid1",
						source.PVLabel:              "pv1",
						source.StorageClassLabel:    "storageClass1",
						source.ProviderIDLabel:      "vol-1",
						source.CSIVolumeHandleLabel: "vol-1",
					},
					Value: 0,
					AdditionalInfo: map[string]string{
						source.UIDLabel:             "uuid1",
						source.PVLabel:              "pv1",
						source.StorageClassLabel:    "storageClass1",
						source.ProviderIDLabel:      "vol-1",
						source.CSIVolumeHandleLabel: "vol-1",
					},
				},
				{
					Name: metric.KubePersistentVolumeCapacityBytes,
					Labels: map[string]string{
						source.UIDLabel:             "uuid1",
						source.PVLabel:              "pv1",
						source.StorageClassLabel:    "storageClass1",
						source.ProviderIDLabel:      "vol-1",
						source.CSIVolumeHandleLabel: "vol-1",
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
							UID:       "uuid1",
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
					Name: metric.ServiceInfo,
					Labels: map[string]string{
						source.UIDLabel:         "uuid1",
						source.ServiceLabel:     "service1",
						source.NamespaceLabel:   "namespace1",
						source.ServiceTypeLabel: "",
					},
					Value: 0,
					AdditionalInfo: map[string]string{
						source.UIDLabel:         "uuid1",
						source.ServiceLabel:     "service1",
						source.NamespaceLabel:   "namespace1",
						source.ServiceTypeLabel: "",
					},
				},
				{
					Name: metric.ServiceSelectorLabels,
					Labels: map[string]string{
						source.UIDLabel:         "uuid1",
						source.ServiceLabel:     "service1",
						source.NamespaceLabel:   "namespace1",
						source.ServiceTypeLabel: "",
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
		nsSetup  func(*SyncMap[string, types.UID])
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
							UID:       "uuid1",
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
			// statefulSetInfo map is shared across all 4 metrics; namespace is added
			// before StatefulSetMatchLabels is appended, so all 4 reflect the final state.
			expected: []metric.Update{
				{
					Name: metric.StatefulSetInfo,
					Labels: map[string]string{
						source.UIDLabel:          "uuid1",
						source.NamespaceUIDLabel: "",
						source.StatefulSetLabel:  "statefulSet1",
						source.NamespaceLabel:    "namespace1",
					},
					Value: 0,
					AdditionalInfo: map[string]string{
						source.UIDLabel:          "uuid1",
						source.NamespaceUIDLabel: "",
						source.StatefulSetLabel:  "statefulSet1",
						source.NamespaceLabel:    "namespace1",
					},
				},
				{
					Name: metric.StatefulSetLabels,
					Labels: map[string]string{
						source.UIDLabel:          "uuid1",
						source.NamespaceUIDLabel: "",
						source.StatefulSetLabel:  "statefulSet1",
						source.NamespaceLabel:    "namespace1",
					},
					Value:          0,
					AdditionalInfo: map[string]string{},
				},
				{
					Name: metric.StatefulSetAnnotations,
					Labels: map[string]string{
						source.UIDLabel:          "uuid1",
						source.NamespaceUIDLabel: "",
						source.StatefulSetLabel:  "statefulSet1",
						source.NamespaceLabel:    "namespace1",
					},
					Value:          0,
					AdditionalInfo: map[string]string{},
				},
				{
					Name: metric.StatefulSetMatchLabels,
					Labels: map[string]string{
						source.UIDLabel:          "uuid1",
						source.NamespaceUIDLabel: "",
						source.StatefulSetLabel:  "statefulSet1",
						source.NamespaceLabel:    "namespace1",
					},
					Value: 0,
					AdditionalInfo: map[string]string{
						"label_test1": "blah",
						"label_test2": "blah2",
					},
				},
			},
		},
		{
			// statefulSetInfo map is shared; NamespaceLabel is added before MatchLabels,
			// so all 4 metrics reflect the final state including namespace_uid.
			name: "with namespace index",
			nsSetup: func(nsIndex *SyncMap[string, types.UID]) {
				nsIndex.Set("namespace1", "ns-uuid1")
			},
			scrapes: []scrape{
				{
					StatefulSets: []*clustercache.StatefulSet{
						{
							Name:         "statefulSet1",
							Namespace:    "namespace1",
							UID:          "uuid1",
							SpecSelector: &metav1.LabelSelector{},
						},
					},
					Timestamp: start1,
				},
			},
			expected: []metric.Update{
				{
					Name: metric.StatefulSetInfo,
					Labels: map[string]string{
						source.UIDLabel:          "uuid1",
						source.NamespaceUIDLabel: "ns-uuid1",
						source.StatefulSetLabel:  "statefulSet1",
						source.NamespaceLabel:    "namespace1",
					},
					Value: 0,
					AdditionalInfo: map[string]string{
						source.UIDLabel:          "uuid1",
						source.NamespaceUIDLabel: "ns-uuid1",
						source.StatefulSetLabel:  "statefulSet1",
						source.NamespaceLabel:    "namespace1",
					},
				},
				{
					Name: metric.StatefulSetLabels,
					Labels: map[string]string{
						source.UIDLabel:          "uuid1",
						source.NamespaceUIDLabel: "ns-uuid1",
						source.StatefulSetLabel:  "statefulSet1",
						source.NamespaceLabel:    "namespace1",
					},
					Value:          0,
					AdditionalInfo: map[string]string{},
				},
				{
					Name: metric.StatefulSetAnnotations,
					Labels: map[string]string{
						source.UIDLabel:          "uuid1",
						source.NamespaceUIDLabel: "ns-uuid1",
						source.StatefulSetLabel:  "statefulSet1",
						source.NamespaceLabel:    "namespace1",
					},
					Value:          0,
					AdditionalInfo: map[string]string{},
				},
				{
					Name: metric.StatefulSetMatchLabels,
					Labels: map[string]string{
						source.UIDLabel:          "uuid1",
						source.NamespaceUIDLabel: "ns-uuid1",
						source.StatefulSetLabel:  "statefulSet1",
						source.NamespaceLabel:    "namespace1",
					},
					Value:          0,
					AdditionalInfo: map[string]string{},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ks := &ClusterCacheScraper{}
			nsIndex := newSyncMap[string, types.UID](0)
			if tt.nsSetup != nil {
				tt.nsSetup(nsIndex)
			}
			var scrapeResults []metric.Update
			for _, s := range tt.scrapes {
				res := ks.scrapeStatefulSets(s.StatefulSets, nsIndex)
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
		nsSetup  func(*SyncMap[string, types.UID])
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
							UID:       "uuid1",
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
							UID:             "uuid2",
							OwnerReferences: []metav1.OwnerReference{},
						},
					},
					Timestamp: start1,
				},
			},
			expected: []metric.Update{
				// replicaSet1: info/labels/annotations use replicaSetInfo (uid, namespace_uid, replicaset)
				{
					Name: metric.ReplicaSetInfo,
					Labels: map[string]string{
						source.UIDLabel:          "uuid1",
						source.NamespaceUIDLabel: "",
						source.ReplicaSetLabel:   "replicaSet1",
					},
					Value: 0,
					AdditionalInfo: map[string]string{
						source.UIDLabel:          "uuid1",
						source.NamespaceUIDLabel: "",
						source.ReplicaSetLabel:   "replicaSet1",
					},
				},
				{
					Name: metric.ReplicaSetLabels,
					Labels: map[string]string{
						source.UIDLabel:          "uuid1",
						source.NamespaceUIDLabel: "",
						source.ReplicaSetLabel:   "replicaSet1",
					},
					Value:          0,
					AdditionalInfo: map[string]string{},
				},
				{
					Name: metric.ReplicaSetAnnotations,
					Labels: map[string]string{
						source.UIDLabel:          "uuid1",
						source.NamespaceUIDLabel: "",
						source.ReplicaSetLabel:   "replicaSet1",
					},
					Value:          0,
					AdditionalInfo: map[string]string{},
				},
				// replicaSet1 owner: uses replicaSetOwnerInfo (replicaset, namespace, uid) + owner fields
				{
					Name: metric.KubeReplicasetOwner,
					Labels: map[string]string{
						source.ReplicaSetLabel: "replicaSet1",
						source.NamespaceLabel:  "namespace1",
						source.UIDLabel:        "uuid1",
						source.OwnerNameLabel:  "rollout1",
						source.OwnerKindLabel:  "Rollout",
						source.OwnerUIDLabel:   "",
						source.ControllerLabel: "false",
					},
					Value: 0,
				},
				// pureReplicaSet: info/labels/annotations
				{
					Name: metric.ReplicaSetInfo,
					Labels: map[string]string{
						source.UIDLabel:          "uuid2",
						source.NamespaceUIDLabel: "",
						source.ReplicaSetLabel:   "pureReplicaSet",
					},
					Value: 0,
					AdditionalInfo: map[string]string{
						source.UIDLabel:          "uuid2",
						source.NamespaceUIDLabel: "",
						source.ReplicaSetLabel:   "pureReplicaSet",
					},
				},
				{
					Name: metric.ReplicaSetLabels,
					Labels: map[string]string{
						source.UIDLabel:          "uuid2",
						source.NamespaceUIDLabel: "",
						source.ReplicaSetLabel:   "pureReplicaSet",
					},
					Value:          0,
					AdditionalInfo: map[string]string{},
				},
				{
					Name: metric.ReplicaSetAnnotations,
					Labels: map[string]string{
						source.UIDLabel:          "uuid2",
						source.NamespaceUIDLabel: "",
						source.ReplicaSetLabel:   "pureReplicaSet",
					},
					Value:          0,
					AdditionalInfo: map[string]string{},
				},
				// pureReplicaSet owner: no owners path uses <none> sentinel, no owner_uid/controller
				{
					Name: metric.KubeReplicasetOwner,
					Labels: map[string]string{
						source.ReplicaSetLabel: "pureReplicaSet",
						source.NamespaceLabel:  "namespace1",
						source.UIDLabel:        "uuid2",
						source.OwnerNameLabel:  source.NoneLabelValue,
						source.OwnerKindLabel:  source.NoneLabelValue,
					},
					Value: 0,
				},
			},
		},
		{
			name: "with namespace index",
			nsSetup: func(nsIndex *SyncMap[string, types.UID]) {
				nsIndex.Set("namespace1", "ns-uuid1")
			},
			scrapes: []scrape{
				{
					ReplicaSets: []*clustercache.ReplicaSet{
						{
							Name:            "replicaSet1",
							Namespace:       "namespace1",
							UID:             "uuid1",
							OwnerReferences: []metav1.OwnerReference{},
						},
					},
					Timestamp: start1,
				},
			},
			expected: []metric.Update{
				{
					Name: metric.ReplicaSetInfo,
					Labels: map[string]string{
						source.UIDLabel:          "uuid1",
						source.NamespaceUIDLabel: "ns-uuid1",
						source.ReplicaSetLabel:   "replicaSet1",
					},
					Value: 0,
					AdditionalInfo: map[string]string{
						source.UIDLabel:          "uuid1",
						source.NamespaceUIDLabel: "ns-uuid1",
						source.ReplicaSetLabel:   "replicaSet1",
					},
				},
				{
					Name: metric.ReplicaSetLabels,
					Labels: map[string]string{
						source.UIDLabel:          "uuid1",
						source.NamespaceUIDLabel: "ns-uuid1",
						source.ReplicaSetLabel:   "replicaSet1",
					},
					Value:          0,
					AdditionalInfo: map[string]string{},
				},
				{
					Name: metric.ReplicaSetAnnotations,
					Labels: map[string]string{
						source.UIDLabel:          "uuid1",
						source.NamespaceUIDLabel: "ns-uuid1",
						source.ReplicaSetLabel:   "replicaSet1",
					},
					Value:          0,
					AdditionalInfo: map[string]string{},
				},
				{
					Name: metric.KubeReplicasetOwner,
					Labels: map[string]string{
						source.ReplicaSetLabel: "replicaSet1",
						source.NamespaceLabel:  "namespace1",
						source.UIDLabel:        "uuid1",
						source.OwnerNameLabel:  source.NoneLabelValue,
						source.OwnerKindLabel:  source.NoneLabelValue,
					},
					Value: 0,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ks := &ClusterCacheScraper{}
			nsIndex := newSyncMap[string, types.UID](0)
			if tt.nsSetup != nil {
				tt.nsSetup(nsIndex)
			}
			var scrapeResults []metric.Update
			for _, s := range tt.scrapes {
				res := ks.scrapeReplicaSets(s.ReplicaSets, nsIndex)
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

func Test_kubernetesScraper_scrapeResourceQuotas(t *testing.T) {
	start1, _ := time.Parse(time.RFC3339, Start1Str)

	type scrape struct {
		ResourceQuotas []*clustercache.ResourceQuota
		Timestamp      time.Time
	}
	tests := []struct {
		name     string
		nsSetup  func(*SyncMap[string, types.UID])
		scrapes  []scrape
		expected []metric.Update
	}{
		{
			name: "simple",
			scrapes: []scrape{
				{
					ResourceQuotas: []*clustercache.ResourceQuota{
						{
							Name:      "resourceQuota1",
							Namespace: "namespace1",
							UID:       "uuid1",
							Spec: v1.ResourceQuotaSpec{
								Hard: v1.ResourceList{
									v1.ResourceRequestsCPU:    resource.MustParse("1"),
									v1.ResourceRequestsMemory: resource.MustParse("1024"),
									v1.ResourceLimitsCPU:      resource.MustParse("2"),
									v1.ResourceLimitsMemory:   resource.MustParse("2048"),
								},
							},
							Status: v1.ResourceQuotaStatus{
								Used: v1.ResourceList{
									v1.ResourceRequestsCPU:    resource.MustParse("0.5"),
									v1.ResourceRequestsMemory: resource.MustParse("512"),
									v1.ResourceLimitsCPU:      resource.MustParse("1"),
									v1.ResourceLimitsMemory:   resource.MustParse("1024"),
								},
							},
						},
					},
					Timestamp: start1,
				},
			},
			expected: []metric.Update{
				{
					Name: metric.ResourceQuotaInfo,
					Labels: map[string]string{
						source.UIDLabel:           "uuid1",
						source.NamespaceUIDLabel:  "",
						source.ResourceQuotaLabel: "resourceQuota1",
					},
					Value: 0,
					AdditionalInfo: map[string]string{
						source.UIDLabel:           "uuid1",
						source.NamespaceUIDLabel:  "",
						source.ResourceQuotaLabel: "resourceQuota1",
					},
				},
				{
					Name: metric.KubeResourceQuotaSpecResourceRequests,
					Labels: map[string]string{
						source.UIDLabel:           "uuid1",
						source.NamespaceUIDLabel:  "",
						source.ResourceQuotaLabel: "resourceQuota1",
						source.ResourceLabel:      "cpu",
						source.UnitLabel:          "core",
					},
					Value:          1,
					AdditionalInfo: nil,
				},
				{
					Name: metric.KubeResourceQuotaSpecResourceRequests,
					Labels: map[string]string{
						source.UIDLabel:           "uuid1",
						source.NamespaceUIDLabel:  "",
						source.ResourceQuotaLabel: "resourceQuota1",
						source.ResourceLabel:      "memory",
						source.UnitLabel:          "byte",
					},
					Value:          1024,
					AdditionalInfo: nil,
				},
				{
					Name: metric.KubeResourceQuotaSpecResourceLimits,
					Labels: map[string]string{
						source.UIDLabel:           "uuid1",
						source.NamespaceUIDLabel:  "",
						source.ResourceQuotaLabel: "resourceQuota1",
						source.ResourceLabel:      "cpu",
						source.UnitLabel:          "core",
					},
					Value:          2,
					AdditionalInfo: nil,
				},
				{
					Name: metric.KubeResourceQuotaSpecResourceLimits,
					Labels: map[string]string{
						source.UIDLabel:           "uuid1",
						source.NamespaceUIDLabel:  "",
						source.ResourceQuotaLabel: "resourceQuota1",
						source.ResourceLabel:      "memory",
						source.UnitLabel:          "byte",
					},
					Value:          2048,
					AdditionalInfo: nil,
				},
				{
					Name: metric.KubeResourceQuotaStatusUsedResourceRequests,
					Labels: map[string]string{
						source.UIDLabel:           "uuid1",
						source.NamespaceUIDLabel:  "",
						source.ResourceQuotaLabel: "resourceQuota1",
						source.ResourceLabel:      "cpu",
						source.UnitLabel:          "core",
					},
					Value:          0.5,
					AdditionalInfo: nil,
				},
				{
					Name: metric.KubeResourceQuotaStatusUsedResourceRequests,
					Labels: map[string]string{
						source.UIDLabel:           "uuid1",
						source.NamespaceUIDLabel:  "",
						source.ResourceQuotaLabel: "resourceQuota1",
						source.ResourceLabel:      "memory",
						source.UnitLabel:          "byte",
					},
					Value:          512,
					AdditionalInfo: nil,
				},
				{
					Name: metric.KubeResourceQuotaStatusUsedResourceLimits,
					Labels: map[string]string{
						source.UIDLabel:           "uuid1",
						source.NamespaceUIDLabel:  "",
						source.ResourceQuotaLabel: "resourceQuota1",
						source.ResourceLabel:      "cpu",
						source.UnitLabel:          "core",
					},
					Value:          1,
					AdditionalInfo: nil,
				},
				{
					Name: metric.KubeResourceQuotaStatusUsedResourceLimits,
					Labels: map[string]string{
						source.UIDLabel:           "uuid1",
						source.NamespaceUIDLabel:  "",
						source.ResourceQuotaLabel: "resourceQuota1",
						source.ResourceLabel:      "memory",
						source.UnitLabel:          "byte",
					},
					Value:          1024,
					AdditionalInfo: nil,
				},
			},
		},
		{
			name: "with namespace index",
			nsSetup: func(nsIndex *SyncMap[string, types.UID]) {
				nsIndex.Set("namespace1", "ns-uuid1")
			},
			scrapes: []scrape{
				{
					ResourceQuotas: []*clustercache.ResourceQuota{
						{
							Name:      "resourceQuota1",
							Namespace: "namespace1",
							UID:       "uuid1",
							Spec: v1.ResourceQuotaSpec{
								Hard: v1.ResourceList{
									v1.ResourceRequestsCPU: resource.MustParse("1"),
								},
							},
						},
					},
					Timestamp: start1,
				},
			},
			expected: []metric.Update{
				{
					Name: metric.ResourceQuotaInfo,
					Labels: map[string]string{
						source.UIDLabel:           "uuid1",
						source.NamespaceUIDLabel:  "ns-uuid1",
						source.ResourceQuotaLabel: "resourceQuota1",
					},
					Value: 0,
					AdditionalInfo: map[string]string{
						source.UIDLabel:           "uuid1",
						source.NamespaceUIDLabel:  "ns-uuid1",
						source.ResourceQuotaLabel: "resourceQuota1",
					},
				},
				{
					Name: metric.KubeResourceQuotaSpecResourceRequests,
					Labels: map[string]string{
						source.UIDLabel:           "uuid1",
						source.NamespaceUIDLabel:  "ns-uuid1",
						source.ResourceQuotaLabel: "resourceQuota1",
						source.ResourceLabel:      "cpu",
						source.UnitLabel:          "core",
					},
					Value:          1,
					AdditionalInfo: nil,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ks := &ClusterCacheScraper{}
			nsIndex := newSyncMap[string, types.UID](0)
			if tt.nsSetup != nil {
				tt.nsSetup(nsIndex)
			}
			var scrapeResults []metric.Update
			for _, s := range tt.scrapes {
				res := ks.scrapeResourceQuotas(s.ResourceQuotas, nsIndex)
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

func Test_kubernetesScraper_scrapeDaemonSets(t *testing.T) {
	start1, _ := time.Parse(time.RFC3339, Start1Str)

	type scrape struct {
		DaemonSets []*clustercache.DaemonSet
		Timestamp  time.Time
	}
	tests := []struct {
		name     string
		nsSetup  func(*SyncMap[string, types.UID])
		scrapes  []scrape
		expected []metric.Update
	}{
		{
			name: "simple",
			scrapes: []scrape{
				{
					DaemonSets: []*clustercache.DaemonSet{
						{
							Name:      "daemonSet1",
							Namespace: "namespace1",
							UID:       "uuid1",
						},
					},
					Timestamp: start1,
				},
			},
			expected: []metric.Update{
				{
					Name: metric.DaemonSetInfo,
					Labels: map[string]string{
						source.UIDLabel:          "uuid1",
						source.NamespaceUIDLabel: "",
						source.DaemonSetLabel:    "daemonSet1",
					},
					Value: 0,
					AdditionalInfo: map[string]string{
						source.UIDLabel:          "uuid1",
						source.NamespaceUIDLabel: "",
						source.DaemonSetLabel:    "daemonSet1",
					},
				},
				{
					Name: metric.DaemonSetLabels,
					Labels: map[string]string{
						source.UIDLabel:          "uuid1",
						source.NamespaceUIDLabel: "",
						source.DaemonSetLabel:    "daemonSet1",
					},
					Value:          0,
					AdditionalInfo: map[string]string{},
				},
				{
					Name: metric.DaemonSetAnnotations,
					Labels: map[string]string{
						source.UIDLabel:          "uuid1",
						source.NamespaceUIDLabel: "",
						source.DaemonSetLabel:    "daemonSet1",
					},
					Value:          0,
					AdditionalInfo: map[string]string{},
				},
			},
		},
		{
			name: "with namespace index",
			nsSetup: func(nsIndex *SyncMap[string, types.UID]) {
				nsIndex.Set("namespace1", "ns-uuid1")
			},
			scrapes: []scrape{
				{
					DaemonSets: []*clustercache.DaemonSet{
						{
							Name:      "daemonSet1",
							Namespace: "namespace1",
							UID:       "uuid1",
						},
					},
					Timestamp: start1,
				},
			},
			expected: []metric.Update{
				{
					Name: metric.DaemonSetInfo,
					Labels: map[string]string{
						source.UIDLabel:          "uuid1",
						source.NamespaceUIDLabel: "ns-uuid1",
						source.DaemonSetLabel:    "daemonSet1",
					},
					Value: 0,
					AdditionalInfo: map[string]string{
						source.UIDLabel:          "uuid1",
						source.NamespaceUIDLabel: "ns-uuid1",
						source.DaemonSetLabel:    "daemonSet1",
					},
				},
				{
					Name: metric.DaemonSetLabels,
					Labels: map[string]string{
						source.UIDLabel:          "uuid1",
						source.NamespaceUIDLabel: "ns-uuid1",
						source.DaemonSetLabel:    "daemonSet1",
					},
					Value:          0,
					AdditionalInfo: map[string]string{},
				},
				{
					Name: metric.DaemonSetAnnotations,
					Labels: map[string]string{
						source.UIDLabel:          "uuid1",
						source.NamespaceUIDLabel: "ns-uuid1",
						source.DaemonSetLabel:    "daemonSet1",
					},
					Value:          0,
					AdditionalInfo: map[string]string{},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ks := &ClusterCacheScraper{}
			nsIndex := newSyncMap[string, types.UID](0)
			if tt.nsSetup != nil {
				tt.nsSetup(nsIndex)
			}
			var scrapeResults []metric.Update
			for _, s := range tt.scrapes {
				res := ks.scrapeDaemonSets(s.DaemonSets, nsIndex)
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

func Test_kubernetesScraper_scrapeJobs(t *testing.T) {
	start1, _ := time.Parse(time.RFC3339, Start1Str)

	type scrape struct {
		Jobs      []*clustercache.Job
		Timestamp time.Time
	}
	tests := []struct {
		name     string
		nsSetup  func(*SyncMap[string, types.UID])
		scrapes  []scrape
		expected []metric.Update
	}{
		{
			name: "simple",
			scrapes: []scrape{
				{
					Jobs: []*clustercache.Job{
						{
							Name:      "job1",
							Namespace: "namespace1",
							UID:       "uuid1",
						},
					},
					Timestamp: start1,
				},
			},
			expected: []metric.Update{
				{
					Name: metric.JobInfo,
					Labels: map[string]string{
						source.UIDLabel:          "uuid1",
						source.NamespaceUIDLabel: "",
						source.JobLabel:          "job1",
					},
					Value: 0,
					AdditionalInfo: map[string]string{
						source.UIDLabel:          "uuid1",
						source.NamespaceUIDLabel: "",
						source.JobLabel:          "job1",
					},
				},
				{
					Name: metric.JobLabels,
					Labels: map[string]string{
						source.UIDLabel:          "uuid1",
						source.NamespaceUIDLabel: "",
						source.JobLabel:          "job1",
					},
					Value:          0,
					AdditionalInfo: map[string]string{},
				},
				{
					Name: metric.JobAnnotations,
					Labels: map[string]string{
						source.UIDLabel:          "uuid1",
						source.NamespaceUIDLabel: "",
						source.JobLabel:          "job1",
					},
					Value:          0,
					AdditionalInfo: map[string]string{},
				},
			},
		},
		{
			name: "with namespace index",
			nsSetup: func(nsIndex *SyncMap[string, types.UID]) {
				nsIndex.Set("namespace1", "ns-uuid1")
			},
			scrapes: []scrape{
				{
					Jobs: []*clustercache.Job{
						{
							Name:      "job1",
							Namespace: "namespace1",
							UID:       "uuid1",
						},
					},
					Timestamp: start1,
				},
			},
			expected: []metric.Update{
				{
					Name: metric.JobInfo,
					Labels: map[string]string{
						source.UIDLabel:          "uuid1",
						source.NamespaceUIDLabel: "ns-uuid1",
						source.JobLabel:          "job1",
					},
					Value: 0,
					AdditionalInfo: map[string]string{
						source.UIDLabel:          "uuid1",
						source.NamespaceUIDLabel: "ns-uuid1",
						source.JobLabel:          "job1",
					},
				},
				{
					Name: metric.JobLabels,
					Labels: map[string]string{
						source.UIDLabel:          "uuid1",
						source.NamespaceUIDLabel: "ns-uuid1",
						source.JobLabel:          "job1",
					},
					Value:          0,
					AdditionalInfo: map[string]string{},
				},
				{
					Name: metric.JobAnnotations,
					Labels: map[string]string{
						source.UIDLabel:          "uuid1",
						source.NamespaceUIDLabel: "ns-uuid1",
						source.JobLabel:          "job1",
					},
					Value:          0,
					AdditionalInfo: map[string]string{},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ks := &ClusterCacheScraper{}
			nsIndex := newSyncMap[string, types.UID](0)
			if tt.nsSetup != nil {
				tt.nsSetup(nsIndex)
			}
			var scrapeResults []metric.Update
			for _, s := range tt.scrapes {
				res := ks.scrapeJobs(s.Jobs, nsIndex)
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

func Test_kubernetesScraper_scrapeCronJobs(t *testing.T) {
	start1, _ := time.Parse(time.RFC3339, Start1Str)

	type scrape struct {
		CronJobs  []*clustercache.CronJob
		Timestamp time.Time
	}
	tests := []struct {
		name     string
		nsSetup  func(*SyncMap[string, types.UID])
		scrapes  []scrape
		expected []metric.Update
	}{
		{
			name: "simple",
			scrapes: []scrape{
				{
					CronJobs: []*clustercache.CronJob{
						{
							Name:      "cronJob1",
							Namespace: "namespace1",
							UID:       "uuid1",
						},
					},
					Timestamp: start1,
				},
			},
			expected: []metric.Update{
				{
					Name: metric.CronJobInfo,
					Labels: map[string]string{
						source.UIDLabel:          "uuid1",
						source.NamespaceUIDLabel: "",
						source.CronJobLabel:      "cronJob1",
					},
					Value: 0,
					AdditionalInfo: map[string]string{
						source.UIDLabel:          "uuid1",
						source.NamespaceUIDLabel: "",
						source.CronJobLabel:      "cronJob1",
					},
				},
				{
					Name: metric.CronJobLabels,
					Labels: map[string]string{
						source.UIDLabel:          "uuid1",
						source.NamespaceUIDLabel: "",
						source.CronJobLabel:      "cronJob1",
					},
					Value:          0,
					AdditionalInfo: map[string]string{},
				},
				{
					Name: metric.CronJobAnnotations,
					Labels: map[string]string{
						source.UIDLabel:          "uuid1",
						source.NamespaceUIDLabel: "",
						source.CronJobLabel:      "cronJob1",
					},
					Value:          0,
					AdditionalInfo: map[string]string{},
				},
			},
		},
		{
			name: "with namespace index",
			nsSetup: func(nsIndex *SyncMap[string, types.UID]) {
				nsIndex.Set("namespace1", "ns-uuid1")
			},
			scrapes: []scrape{
				{
					CronJobs: []*clustercache.CronJob{
						{
							Name:      "cronJob1",
							Namespace: "namespace1",
							UID:       "uuid1",
						},
					},
					Timestamp: start1,
				},
			},
			expected: []metric.Update{
				{
					Name: metric.CronJobInfo,
					Labels: map[string]string{
						source.UIDLabel:          "uuid1",
						source.NamespaceUIDLabel: "ns-uuid1",
						source.CronJobLabel:      "cronJob1",
					},
					Value: 0,
					AdditionalInfo: map[string]string{
						source.UIDLabel:          "uuid1",
						source.NamespaceUIDLabel: "ns-uuid1",
						source.CronJobLabel:      "cronJob1",
					},
				},
				{
					Name: metric.CronJobLabels,
					Labels: map[string]string{
						source.UIDLabel:          "uuid1",
						source.NamespaceUIDLabel: "ns-uuid1",
						source.CronJobLabel:      "cronJob1",
					},
					Value:          0,
					AdditionalInfo: map[string]string{},
				},
				{
					Name: metric.CronJobAnnotations,
					Labels: map[string]string{
						source.UIDLabel:          "uuid1",
						source.NamespaceUIDLabel: "ns-uuid1",
						source.CronJobLabel:      "cronJob1",
					},
					Value:          0,
					AdditionalInfo: map[string]string{},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ks := &ClusterCacheScraper{}
			nsIndex := newSyncMap[string, types.UID](0)
			if tt.nsSetup != nil {
				tt.nsSetup(nsIndex)
			}
			var scrapeResults []metric.Update
			for _, s := range tt.scrapes {
				res := ks.scrapeCronJobs(s.CronJobs, nsIndex)
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
