package filterutil

import (
	"testing"

	"github.com/kubecost/cost-model/pkg/kubecost"
	"github.com/kubecost/cost-model/pkg/util/mapper"
)

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
			name: "single label",
			qp: map[string]string{
				"filterLabels": "app:cost-analyzer",
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

			filter := FiltersFromParamsV1(qpMapper)
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
