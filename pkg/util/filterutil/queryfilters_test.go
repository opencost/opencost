package filterutil

import (
	"testing"

	"github.com/opencost/opencost/pkg/costmodel/clusters"
	"github.com/opencost/opencost/pkg/kubecost"
	"github.com/opencost/opencost/pkg/util/mapper"
)

var allocCompiler = kubecost.NewAllocationMatchCompiler(nil)

type mockClusterMap struct {
	m map[string]*clusters.ClusterInfo
}

func (mcp mockClusterMap) GetClusterIDs() []string {
	panic("unimplemented")
}

func (mcp mockClusterMap) AsMap() map[string]*clusters.ClusterInfo {
	return mcp.m
}

func (mcp mockClusterMap) InfoFor(clusterID string) *clusters.ClusterInfo {
	panic("unimplemented")
}

func (mcp mockClusterMap) NameFor(clusterID string) string {
	panic("unimplemented")
}
func (mcp mockClusterMap) NameIDFor(clusterID string) string {
	panic("unimplemented")
}
func (mcp mockClusterMap) SplitNameID(nameID string) (string, string) {
	panic("unimplemented")
}

func (mcp mockClusterMap) StopRefresh() {}

func allocGenerator(props kubecost.AllocationProperties) kubecost.Allocation {
	a := kubecost.Allocation{
		Properties: &props,
	}

	a.Name = a.Properties.String()
	return a
}

func TestFiltersFromParamsV1(t *testing.T) {
	// TODO: __unallocated__ case?
	cases := []struct {
		name           string
		qp             map[string]string
		shouldMatch    []kubecost.Allocation
		shouldNotMatch []kubecost.Allocation
	}{
		{
			name: "single cluster ID",
			qp: map[string]string{
				"filterClusters": "cluster-one",
			},
			shouldMatch: []kubecost.Allocation{
				allocGenerator(kubecost.AllocationProperties{
					Cluster: "cluster-one",
				}),
			},
			shouldNotMatch: []kubecost.Allocation{
				allocGenerator(kubecost.AllocationProperties{
					Cluster: "foo",
				}),
			},
		},
		{
			name: "wildcard cluster ID",
			qp: map[string]string{
				"filterClusters": "cluster*",
			},
			shouldMatch: []kubecost.Allocation{
				allocGenerator(kubecost.AllocationProperties{
					Cluster: "cluster-one",
				}),
				allocGenerator(kubecost.AllocationProperties{
					Cluster: "cluster-two",
				}),
				allocGenerator(kubecost.AllocationProperties{
					Cluster: "cluster",
				}),
			},
			shouldNotMatch: []kubecost.Allocation{
				allocGenerator(kubecost.AllocationProperties{
					Cluster: "foo",
				}),
				allocGenerator(kubecost.AllocationProperties{
					Cluster: "cluste",
				}),
			},
		},
		{
			name: "single cluster name",
			qp: map[string]string{
				"filterClusters": "cluster ABC",
			},
			shouldMatch: []kubecost.Allocation{
				allocGenerator(kubecost.AllocationProperties{
					Cluster: "mapped-cluster-ID-ABC",
				}),
			},
			shouldNotMatch: []kubecost.Allocation{
				allocGenerator(kubecost.AllocationProperties{
					Cluster: "cluster-one",
				}),
			},
		},
		{
			name: "wildcard cluster name",
			qp: map[string]string{
				"filterClusters": "cluster A*",
			},
			shouldMatch: []kubecost.Allocation{
				allocGenerator(kubecost.AllocationProperties{
					Cluster: "mapped-cluster-ID-ABC",
				}),
			},
			shouldNotMatch: []kubecost.Allocation{
				allocGenerator(kubecost.AllocationProperties{
					Cluster: "cluster-one",
				}),
			},
		},
		{
			name: "single node",
			qp: map[string]string{
				"filterNodes": "node-123-abc",
			},
			shouldMatch: []kubecost.Allocation{
				allocGenerator(kubecost.AllocationProperties{
					Node: "node-123-abc",
				}),
			},
			shouldNotMatch: []kubecost.Allocation{
				allocGenerator(kubecost.AllocationProperties{
					Node: "node-456-def",
				}),
			},
		},
		{
			name: "wildcard node",
			qp: map[string]string{
				"filterNodes": "node-1*",
			},
			shouldMatch: []kubecost.Allocation{
				allocGenerator(kubecost.AllocationProperties{
					Node: "node-123-abc",
				}),
			},
			shouldNotMatch: []kubecost.Allocation{
				allocGenerator(kubecost.AllocationProperties{
					Node: "node-456-def",
				}),
			},
		},
		{
			name: "single namespace",
			qp: map[string]string{
				"filterNamespaces": "kubecost",
			},
			shouldMatch: []kubecost.Allocation{
				allocGenerator(kubecost.AllocationProperties{
					Namespace: "kubecost",
				}),
			},
			shouldNotMatch: []kubecost.Allocation{
				allocGenerator(kubecost.AllocationProperties{
					Namespace: "kubecost2",
				}),
			},
		},
		{
			name: "wildcard namespace",
			qp: map[string]string{
				"filterNamespaces": "kube*",
			},
			shouldMatch: []kubecost.Allocation{
				allocGenerator(kubecost.AllocationProperties{
					Namespace: "kubecost",
				}),
				allocGenerator(kubecost.AllocationProperties{
					Namespace: "kube-system",
				}),
			},
			shouldNotMatch: []kubecost.Allocation{
				allocGenerator(kubecost.AllocationProperties{
					Namespace: "kub",
				}),
			},
		},
		{
			name: "single controller kind",
			qp: map[string]string{
				"filterControllerKinds": "deployment",
			},
			shouldMatch: []kubecost.Allocation{
				allocGenerator(kubecost.AllocationProperties{
					ControllerKind: "deployment",
				}),
			},
			shouldNotMatch: []kubecost.Allocation{
				allocGenerator(kubecost.AllocationProperties{
					ControllerKind: "daemonset",
				}),
			},
		},
		{
			name: "wildcard controller kind",
			qp: map[string]string{
				"filterControllerKinds": "depl*",
			},
			shouldMatch: []kubecost.Allocation{
				allocGenerator(kubecost.AllocationProperties{
					ControllerKind: "deployment",
				}),
			},
			shouldNotMatch: []kubecost.Allocation{
				allocGenerator(kubecost.AllocationProperties{
					ControllerKind: "daemonset",
				}),
			},
		},
		{
			name: "single controller name",
			qp: map[string]string{
				"filterControllers": "kubecost-cost-analyzer",
			},
			shouldMatch: []kubecost.Allocation{
				allocGenerator(kubecost.AllocationProperties{
					Controller: "kubecost-cost-analyzer",
				}),
			},
			shouldNotMatch: []kubecost.Allocation{
				allocGenerator(kubecost.AllocationProperties{
					Controller: "kube-proxy",
				}),
			},
		},
		{
			name: "wildcard controller name",
			qp: map[string]string{
				"filterControllers": "kubecost-*",
			},
			shouldMatch: []kubecost.Allocation{
				allocGenerator(kubecost.AllocationProperties{
					Controller: "kubecost-cost-analyzer",
				}),
				allocGenerator(kubecost.AllocationProperties{
					Controller: "kubecost-frontend",
				}),
			},
			shouldNotMatch: []kubecost.Allocation{
				allocGenerator(kubecost.AllocationProperties{
					Controller: "kube-proxy",
				}),
			},
		},
		{
			name: "single controller kind:name combo",
			qp: map[string]string{
				"filterControllers": "deployment:kubecost-cost-analyzer",
			},
			shouldMatch: []kubecost.Allocation{
				allocGenerator(kubecost.AllocationProperties{
					ControllerKind: "deployment",
					Controller:     "kubecost-cost-analyzer",
				}),
			},
			shouldNotMatch: []kubecost.Allocation{
				allocGenerator(kubecost.AllocationProperties{
					ControllerKind: "daemonset",
					Controller:     "kubecost-cost-analyzer",
				}),
			},
		},
		{
			name: "wildcard controller kind:name combo",
			qp: map[string]string{
				"filterControllers": "deployment:kubecost*",
			},
			shouldMatch: []kubecost.Allocation{
				allocGenerator(kubecost.AllocationProperties{
					ControllerKind: "deployment",
					Controller:     "kubecost-cost-analyzer",
				}),
			},
			shouldNotMatch: []kubecost.Allocation{
				allocGenerator(kubecost.AllocationProperties{
					ControllerKind: "daemonset",
					Controller:     "kubecost-cost-analyzer",
				}),
				allocGenerator(kubecost.AllocationProperties{
					ControllerKind: "deployment",
					Controller:     "kube-system",
				}),
			},
		},
		{
			name: "single pod",
			qp: map[string]string{
				"filterPods": "pod-123-abc",
			},
			shouldMatch: []kubecost.Allocation{
				allocGenerator(kubecost.AllocationProperties{
					Pod: "pod-123-abc",
				}),
			},
			shouldNotMatch: []kubecost.Allocation{
				allocGenerator(kubecost.AllocationProperties{
					Pod: "pod-456-def",
				}),
			},
		},
		{
			name: "wildcard pod",
			qp: map[string]string{
				"filterPods": "pod-1*",
			},
			shouldMatch: []kubecost.Allocation{
				allocGenerator(kubecost.AllocationProperties{
					Pod: "pod-123-abc",
				}),
			},
			shouldNotMatch: []kubecost.Allocation{
				allocGenerator(kubecost.AllocationProperties{
					Pod: "pod-456-def",
				}),
			},
		},
		{
			name: "single container",
			qp: map[string]string{
				"filterContainers": "container-123-abc",
			},
			shouldMatch: []kubecost.Allocation{
				allocGenerator(kubecost.AllocationProperties{
					Container: "container-123-abc",
				}),
			},
			shouldNotMatch: []kubecost.Allocation{
				allocGenerator(kubecost.AllocationProperties{
					Container: "container-456-def",
				}),
			},
		},
		{
			name: "wildcard container",
			qp: map[string]string{
				"filterContainers": "container-1*",
			},
			shouldMatch: []kubecost.Allocation{
				allocGenerator(kubecost.AllocationProperties{
					Container: "container-123-abc",
				}),
			},
			shouldNotMatch: []kubecost.Allocation{
				allocGenerator(kubecost.AllocationProperties{
					Container: "container-456-def",
				}),
			},
		},
		{
			name: "single department, sanitization required",
			qp: map[string]string{
				"filterDepartments": "pa-1",
			},
			shouldMatch: []kubecost.Allocation{
				allocGenerator(kubecost.AllocationProperties{
					Labels: map[string]string{
						"internal_product_umbrella": "pa-1",
					},
				}),
			},
			shouldNotMatch: []kubecost.Allocation{
				allocGenerator(kubecost.AllocationProperties{
					Labels: map[string]string{
						"internal_product_umbrella": "ps-N",
					},
				}),
			},
		},
		{
			name: "single department, no label, annotation",
			qp: map[string]string{
				"filterDepartments": "pa-1",
			},
			shouldMatch: []kubecost.Allocation{
				allocGenerator(kubecost.AllocationProperties{
					Annotations: map[string]string{
						"internal_product_umbrella": "pa-1",
					},
				}),
			},
			// should find labels first and fail
			shouldNotMatch: []kubecost.Allocation{
				allocGenerator(kubecost.AllocationProperties{
					Labels: map[string]string{
						"internal_product_umbrella": "ps-N",
					},
					Annotations: map[string]string{
						"internal_product_umbrella": "pa-1",
					},
				}),
			},
		},
		{
			name: "wildcard department",
			qp: map[string]string{
				"filterDepartments": "pa*",
			},
			shouldMatch: []kubecost.Allocation{
				allocGenerator(kubecost.AllocationProperties{
					Labels: map[string]string{
						"internal_product_umbrella": "pa-1",
					},
				}),
			},
			shouldNotMatch: []kubecost.Allocation{
				allocGenerator(kubecost.AllocationProperties{
					Labels: map[string]string{
						"internal_product_umbrella": "ps-N",
					},
				}),
			},
		},
		{
			name: "single label, sanitization required",
			qp: map[string]string{
				"filterLabels": "app-a:cost-analyzer",
			},
			shouldMatch: []kubecost.Allocation{
				allocGenerator(kubecost.AllocationProperties{
					Labels: map[string]string{
						"app_a": "cost-analyzer",
					},
				}),
			},
			shouldNotMatch: []kubecost.Allocation{
				allocGenerator(kubecost.AllocationProperties{
					Labels: map[string]string{
						"app_a": "foo",
					},
				}),
				allocGenerator(kubecost.AllocationProperties{
					Labels: map[string]string{
						"foo": "bar",
					},
				}),
			},
		},
		{
			name: "wildcard label",
			qp: map[string]string{
				"filterLabels": "app:cost-*",
			},
			shouldMatch: []kubecost.Allocation{
				allocGenerator(kubecost.AllocationProperties{
					Labels: map[string]string{
						"app": "cost-analyzer",
					},
				}),
			},
			shouldNotMatch: []kubecost.Allocation{
				allocGenerator(kubecost.AllocationProperties{
					Labels: map[string]string{
						"app": "foo",
					},
				}),
				allocGenerator(kubecost.AllocationProperties{
					Labels: map[string]string{
						"foo": "bar",
					},
				}),
			},
		},
		{
			name: "single annotation",
			qp: map[string]string{
				"filterAnnotations": "app:cost-analyzer",
			},
			shouldMatch: []kubecost.Allocation{
				allocGenerator(kubecost.AllocationProperties{
					Annotations: map[string]string{
						"app": "cost-analyzer",
					},
				}),
			},
			shouldNotMatch: []kubecost.Allocation{
				allocGenerator(kubecost.AllocationProperties{
					Annotations: map[string]string{
						"app": "foo",
					},
				}),
				allocGenerator(kubecost.AllocationProperties{
					Annotations: map[string]string{
						"foo": "bar",
					},
				}),
			},
		},
		{
			name: "wildcard annotation",
			qp: map[string]string{
				"filterAnnotations": "app:cost-*",
			},
			shouldMatch: []kubecost.Allocation{
				allocGenerator(kubecost.AllocationProperties{
					Annotations: map[string]string{
						"app": "cost-analyzer",
					},
				}),
			},
			shouldNotMatch: []kubecost.Allocation{
				allocGenerator(kubecost.AllocationProperties{
					Annotations: map[string]string{
						"app": "foo",
					},
				}),
				allocGenerator(kubecost.AllocationProperties{
					Annotations: map[string]string{
						"foo": "bar",
					},
				}),
			},
		},
		{
			name: "single service",
			qp: map[string]string{
				"filterServices": "serv1",
			},
			shouldMatch: []kubecost.Allocation{
				allocGenerator(kubecost.AllocationProperties{
					Services: []string{"serv1"},
				}),
			},
			shouldNotMatch: []kubecost.Allocation{
				allocGenerator(kubecost.AllocationProperties{}),
				allocGenerator(kubecost.AllocationProperties{
					Services: []string{"serv2"},
				}),
			},
		},
		{
			name: "multi service",
			qp: map[string]string{
				"filterServices": "serv1,serv3",
			},
			shouldMatch: []kubecost.Allocation{
				allocGenerator(kubecost.AllocationProperties{
					Services: []string{"serv1"},
				}),
				allocGenerator(kubecost.AllocationProperties{
					Services: []string{"serv2", "serv3"},
				}),
			},
			shouldNotMatch: []kubecost.Allocation{
				allocGenerator(kubecost.AllocationProperties{}),
				allocGenerator(kubecost.AllocationProperties{
					Services: []string{"serv2"},
				}),
			},
		},
		{
			name: "wildcard service",
			qp: map[string]string{
				"filterServices": "serv*",
			},
			shouldMatch: []kubecost.Allocation{
				allocGenerator(kubecost.AllocationProperties{
					Services: []string{"serv1"},
				}),
				allocGenerator(kubecost.AllocationProperties{
					Services: []string{"serv2"},
				}),
			},
			shouldNotMatch: []kubecost.Allocation{
				allocGenerator(kubecost.AllocationProperties{}),
				allocGenerator(kubecost.AllocationProperties{
					Services: []string{"foo"},
				}),
			},
		},
		{
			name: "multi: namespaces, labels",
			qp: map[string]string{
				"filterNamespaces": "kube-system,kubecost",
				"filterLabels":     "app:cost-analyzer,app:kube-proxy,foo:bar",
			},
			shouldMatch: []kubecost.Allocation{
				allocGenerator(kubecost.AllocationProperties{
					Namespace: "kubecost",
					Labels: map[string]string{
						"app": "cost-analyzer",
					},
				}),
				allocGenerator(kubecost.AllocationProperties{
					Namespace: "kubecost",
					Labels: map[string]string{
						"foo": "bar",
					},
				}),
				allocGenerator(kubecost.AllocationProperties{
					Namespace: "kube-system",
					Labels: map[string]string{
						"app": "kube-proxy",
					},
				}),
			},
			shouldNotMatch: []kubecost.Allocation{
				allocGenerator(kubecost.AllocationProperties{
					Namespace: "kubecost",
				}),
				allocGenerator(kubecost.AllocationProperties{
					Namespace: "kubecost",
					Labels: map[string]string{
						"app": "something",
					},
				}),
				allocGenerator(kubecost.AllocationProperties{
					Labels: map[string]string{
						"app": "foo",
					},
				}),
				allocGenerator(kubecost.AllocationProperties{
					Labels: map[string]string{
						"foo": "bar",
					},
				}),
			},
		},
		{
			name: "cluster name OR cluster ID",
			qp: map[string]string{
				"filterClusters": "cluster ABC,cluster-one",
			},
			shouldMatch: []kubecost.Allocation{
				allocGenerator(kubecost.AllocationProperties{
					Cluster: "mapped-cluster-ID-ABC",
				}),
				allocGenerator(kubecost.AllocationProperties{
					Cluster: "cluster-one",
				}),
			},
			shouldNotMatch: []kubecost.Allocation{
				allocGenerator(kubecost.AllocationProperties{
					Cluster: "cluster",
				}),
			},
		},
		{
			name: "single owner",
			qp: map[string]string{
				"filterOwners": "nick",
			},
			shouldMatch: []kubecost.Allocation{
				allocGenerator(kubecost.AllocationProperties{
					Labels: kubecost.AllocationLabels{
						"testowner": "nick",
					},
				}),
				allocGenerator(kubecost.AllocationProperties{
					Annotations: kubecost.AllocationAnnotations{
						"testowner": "nick",
					},
				}),
				allocGenerator(kubecost.AllocationProperties{
					Labels: kubecost.AllocationLabels{
						"testowner": "nick",
					},
					Annotations: kubecost.AllocationAnnotations{
						"dontpick": "notnick",
					},
				}),
				allocGenerator(kubecost.AllocationProperties{
					Labels: kubecost.AllocationLabels{
						"dontpick": "notnick",
					},
					Annotations: kubecost.AllocationAnnotations{
						"testowner": "nick",
					},
				}),
			},
			shouldNotMatch: []kubecost.Allocation{
				allocGenerator(kubecost.AllocationProperties{
					Labels: kubecost.AllocationLabels{
						"dontpick": "notnick",
					},
					Annotations: kubecost.AllocationAnnotations{
						"dontpick": "notnick",
					},
				}),
				allocGenerator(kubecost.AllocationProperties{
					Labels: kubecost.AllocationLabels{
						"testowner": "notnick",
					},
					Annotations: kubecost.AllocationAnnotations{
						"testowner": "nick",
					},
				}),
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			// Convert map[string]string representation to the mapper
			// library type
			qpMap := mapper.NewMap()
			for k, v := range c.qp {
				qpMap.Set(k, v)
			}
			qpMapper := mapper.NewMapper(qpMap)

			labelConfig := kubecost.LabelConfig{}
			labelConfig.DepartmentLabel = "internal-product-umbrella"
			labelConfig.OwnerLabel = "testowner"

			clustersMap := mockClusterMap{
				m: map[string]*clusters.ClusterInfo{
					"mapped-cluster-ID-1": {
						ID:   "mapped-cluster-ID-ABC",
						Name: "cluster ABC",
					},
				},
			}

			filterTree := AllocationFilterFromParamsV1(ConvertFilterQueryParams(qpMapper, &labelConfig), &labelConfig, clustersMap)
			filter, err := allocCompiler.Compile(filterTree)
			if err != nil {
				t.Fatalf("compiling filter: %s", err)
			}
			for _, alloc := range c.shouldMatch {
				if !filter.Matches(&alloc) {
					t.Errorf("should have matched: %s", alloc.Name)
				}
			}
			for _, alloc := range c.shouldNotMatch {
				if filter.Matches(&alloc) {
					t.Errorf("incorrectly matched: %s", alloc.Name)
				}
			}
		})
	}
}
