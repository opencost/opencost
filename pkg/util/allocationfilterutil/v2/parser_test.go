package allocationfilterutil

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/opencost/opencost/pkg/kubecost"
)

func allocGenerator(props kubecost.AllocationProperties) kubecost.Allocation {
	a := kubecost.Allocation{
		Properties: &props,
	}

	a.Name = a.Properties.String()
	return a
}

func TestParse(t *testing.T) {
	cases := []struct {
		input          string
		expected       kubecost.AllocationFilter
		shouldMatch    []kubecost.Allocation
		shouldNotMatch []kubecost.Allocation
	}{
		{
			input: `namespace:"kubecost"`,
			expected: kubecost.AllocationFilterAnd{[]kubecost.AllocationFilter{
				kubecost.AllocationFilterOr{[]kubecost.AllocationFilter{
					kubecost.AllocationFilterCondition{
						Field: kubecost.FilterNamespace,
						Op:    kubecost.FilterEquals,
						Value: "kubecost",
					},
				}},
			}},
			shouldMatch: []kubecost.Allocation{
				allocGenerator(kubecost.AllocationProperties{Namespace: "kubecost"}),
			},
			shouldNotMatch: []kubecost.Allocation{
				allocGenerator(kubecost.AllocationProperties{Namespace: "kube-system"}),
			},
		},
		{
			input: `cluster:"cluster-one"+namespace:"kubecost"+controllerKind:"daemonset"+controllerName:"kubecost-network-costs"+container:"kubecost-network-costs"`,
			expected: kubecost.AllocationFilterAnd{[]kubecost.AllocationFilter{
				kubecost.AllocationFilterOr{[]kubecost.AllocationFilter{
					kubecost.AllocationFilterCondition{
						Field: kubecost.FilterClusterID,
						Op:    kubecost.FilterEquals,
						Value: "cluster-one",
					},
				}},
				kubecost.AllocationFilterOr{[]kubecost.AllocationFilter{
					kubecost.AllocationFilterCondition{
						Field: kubecost.FilterNamespace,
						Op:    kubecost.FilterEquals,
						Value: "kubecost",
					},
				}},
				kubecost.AllocationFilterOr{[]kubecost.AllocationFilter{
					kubecost.AllocationFilterCondition{
						Field: kubecost.FilterControllerKind,
						Op:    kubecost.FilterEquals,
						Value: "daemonset",
					},
				}},
				kubecost.AllocationFilterOr{[]kubecost.AllocationFilter{
					kubecost.AllocationFilterCondition{
						Field: kubecost.FilterControllerName,
						Op:    kubecost.FilterEquals,
						Value: "kubecost-network-costs",
					},
				}},
				kubecost.AllocationFilterOr{[]kubecost.AllocationFilter{
					kubecost.AllocationFilterCondition{
						Field: kubecost.FilterContainer,
						Op:    kubecost.FilterEquals,
						Value: "kubecost-network-costs",
					},
				}},
			}},
			shouldMatch: []kubecost.Allocation{
				allocGenerator(kubecost.AllocationProperties{
					Cluster:        "cluster-one",
					Namespace:      "kubecost",
					ControllerKind: "daemonset",
					Controller:     "kubecost-network-costs",
					Pod:            "kubecost-network-costs-abc123",
					Container:      "kubecost-network-costs",
				}),
			},
			shouldNotMatch: []kubecost.Allocation{
				allocGenerator(kubecost.AllocationProperties{
					Cluster:        "cluster-one",
					Namespace:      "default",
					ControllerKind: "deployment",
					Controller:     "workload-abc",
					Pod:            "workload-abc-123abc",
					Container:      "abc",
				}),
			},
		},
		{
			input: `namespace!:"kubecost","kube-system"`,
			expected: kubecost.AllocationFilterAnd{[]kubecost.AllocationFilter{
				kubecost.AllocationFilterAnd{[]kubecost.AllocationFilter{
					kubecost.AllocationFilterCondition{
						Field: kubecost.FilterNamespace,
						Op:    kubecost.FilterNotEquals,
						Value: "kubecost",
					},
					kubecost.AllocationFilterCondition{
						Field: kubecost.FilterNamespace,
						Op:    kubecost.FilterNotEquals,
						Value: "kube-system",
					},
				}},
			}},
			shouldMatch: []kubecost.Allocation{
				allocGenerator(kubecost.AllocationProperties{Namespace: "abc"}),
			},
			shouldNotMatch: []kubecost.Allocation{
				allocGenerator(kubecost.AllocationProperties{Namespace: "kubecost"}),
				allocGenerator(kubecost.AllocationProperties{Namespace: "kube-system"}),
			},
		},
		{
			input: `namespace:"kubecost","kube-system"`,
			expected: kubecost.AllocationFilterAnd{[]kubecost.AllocationFilter{
				kubecost.AllocationFilterOr{[]kubecost.AllocationFilter{
					kubecost.AllocationFilterCondition{
						Field: kubecost.FilterNamespace,
						Op:    kubecost.FilterEquals,
						Value: "kubecost",
					},
					kubecost.AllocationFilterCondition{
						Field: kubecost.FilterNamespace,
						Op:    kubecost.FilterEquals,
						Value: "kube-system",
					},
				}},
			}},
			shouldMatch: []kubecost.Allocation{
				allocGenerator(kubecost.AllocationProperties{Namespace: "kubecost"}),
				allocGenerator(kubecost.AllocationProperties{Namespace: "kube-system"}),
			},
			shouldNotMatch: []kubecost.Allocation{
				allocGenerator(kubecost.AllocationProperties{Namespace: "abc"}),
			},
		},
		{
			input: `node:"node a b c" , "node 12 3"` + string('\n') + "+" + string('\n') + string('\r') + `namespace : "kubecost"`,
			expected: kubecost.AllocationFilterAnd{[]kubecost.AllocationFilter{
				kubecost.AllocationFilterOr{[]kubecost.AllocationFilter{
					kubecost.AllocationFilterCondition{
						Field: kubecost.FilterNode,
						Op:    kubecost.FilterEquals,
						Value: "node a b c",
					},
					kubecost.AllocationFilterCondition{
						Field: kubecost.FilterNode,
						Op:    kubecost.FilterEquals,
						Value: "node 12 3",
					},
				}},
				kubecost.AllocationFilterOr{[]kubecost.AllocationFilter{
					kubecost.AllocationFilterCondition{
						Field: kubecost.FilterNamespace,
						Op:    kubecost.FilterEquals,
						Value: "kubecost",
					},
				}},
			}},
		},
		{
			input: `label[app_abc]:"cost_analyzer"`,
			expected: kubecost.AllocationFilterAnd{[]kubecost.AllocationFilter{
				kubecost.AllocationFilterOr{[]kubecost.AllocationFilter{
					kubecost.AllocationFilterCondition{
						Field: kubecost.FilterLabel,
						Key:   "app_abc",
						Op:    kubecost.FilterEquals,
						Value: "cost_analyzer",
					},
				}},
			}},
		},
		{
			input: `services:"123","abc"`,
			expected: kubecost.AllocationFilterAnd{[]kubecost.AllocationFilter{
				kubecost.AllocationFilterOr{[]kubecost.AllocationFilter{
					kubecost.AllocationFilterCondition{
						Field: kubecost.FilterServices,
						Op:    kubecost.FilterContains,
						Value: "123",
					},
					kubecost.AllocationFilterCondition{
						Field: kubecost.FilterServices,
						Op:    kubecost.FilterContains,
						Value: "abc",
					},
				}},
			}},
		},
		{
			input: `services!:"123","abc"`,
			expected: kubecost.AllocationFilterAnd{[]kubecost.AllocationFilter{
				kubecost.AllocationFilterAnd{[]kubecost.AllocationFilter{
					kubecost.AllocationFilterCondition{
						Field: kubecost.FilterServices,
						Op:    kubecost.FilterNotContains,
						Value: "123",
					},
					kubecost.AllocationFilterCondition{
						Field: kubecost.FilterServices,
						Op:    kubecost.FilterNotContains,
						Value: "abc",
					},
				}},
			}},
		},
		{
			input: `label[app_abc]:"cost_analyzer"`,
			expected: kubecost.AllocationFilterAnd{[]kubecost.AllocationFilter{
				kubecost.AllocationFilterOr{[]kubecost.AllocationFilter{
					kubecost.AllocationFilterCondition{
						Field: kubecost.FilterLabel,
						Key:   "app_abc",
						Op:    kubecost.FilterEquals,
						Value: "cost_analyzer",
					},
				}},
			}},
		},
		{
			input: `label[app_abc]:"cost_analyzer"+label[foo]:"bar"`,
			expected: kubecost.AllocationFilterAnd{[]kubecost.AllocationFilter{
				kubecost.AllocationFilterOr{[]kubecost.AllocationFilter{
					kubecost.AllocationFilterCondition{
						Field: kubecost.FilterLabel,
						Key:   "app_abc",
						Op:    kubecost.FilterEquals,
						Value: "cost_analyzer",
					},
				}},
				kubecost.AllocationFilterOr{[]kubecost.AllocationFilter{
					kubecost.AllocationFilterCondition{
						Field: kubecost.FilterLabel,
						Key:   "foo",
						Op:    kubecost.FilterEquals,
						Value: "bar",
					},
				}},
			}},
		},
		{
			input: `
namespace:"kubecost" +
label[app]:"cost_analyzer" +
annotation[a1]:"b2" +
cluster:"cluster-one" +
node!:
  "node-123",
  "node-456" +
controllerName:
  "kubecost-cost-analyzer",
  "kubecost-prometheus-server" +
controllerKind!:
  "daemonset",
  "statefulset",
  "job" +
container!:"123-abc_foo" +
pod!:"aaaaaaaaaaaaaaaaaaaaaaaaa" +
services!:"abc123"
`,
			expected: kubecost.AllocationFilterAnd{[]kubecost.AllocationFilter{
				kubecost.AllocationFilterOr{[]kubecost.AllocationFilter{
					kubecost.AllocationFilterCondition{
						Field: kubecost.FilterNamespace,
						Op:    kubecost.FilterEquals,
						Value: "kubecost",
					},
				}},
				kubecost.AllocationFilterOr{[]kubecost.AllocationFilter{
					kubecost.AllocationFilterCondition{
						Field: kubecost.FilterLabel,
						Key:   "app",
						Op:    kubecost.FilterEquals,
						Value: "cost_analyzer",
					},
				}},
				kubecost.AllocationFilterOr{[]kubecost.AllocationFilter{
					kubecost.AllocationFilterCondition{
						Field: kubecost.FilterAnnotation,
						Key:   "a1",
						Op:    kubecost.FilterEquals,
						Value: "b2",
					},
				}},
				kubecost.AllocationFilterOr{[]kubecost.AllocationFilter{
					kubecost.AllocationFilterCondition{
						Field: kubecost.FilterClusterID,
						Op:    kubecost.FilterEquals,
						Value: "cluster-one",
					},
				}},
				kubecost.AllocationFilterAnd{[]kubecost.AllocationFilter{
					kubecost.AllocationFilterCondition{
						Field: kubecost.FilterNode,
						Op:    kubecost.FilterNotEquals,
						Value: "node-123",
					},
					kubecost.AllocationFilterCondition{
						Field: kubecost.FilterNode,
						Op:    kubecost.FilterNotEquals,
						Value: "node-456",
					},
				}},
				kubecost.AllocationFilterOr{[]kubecost.AllocationFilter{
					kubecost.AllocationFilterCondition{
						Field: kubecost.FilterControllerName,
						Op:    kubecost.FilterEquals,
						Value: "kubecost-cost-analyzer",
					},
					kubecost.AllocationFilterCondition{
						Field: kubecost.FilterControllerName,
						Op:    kubecost.FilterEquals,
						Value: "kubecost-prometheus-server",
					},
				}},
				kubecost.AllocationFilterAnd{[]kubecost.AllocationFilter{
					kubecost.AllocationFilterCondition{
						Field: kubecost.FilterControllerKind,
						Op:    kubecost.FilterNotEquals,
						Value: "daemonset",
					},
					kubecost.AllocationFilterCondition{
						Field: kubecost.FilterControllerKind,
						Op:    kubecost.FilterNotEquals,
						Value: "statefulset",
					},
					kubecost.AllocationFilterCondition{
						Field: kubecost.FilterControllerKind,
						Op:    kubecost.FilterNotEquals,
						Value: "job",
					},
				}},
				kubecost.AllocationFilterAnd{[]kubecost.AllocationFilter{
					kubecost.AllocationFilterCondition{
						Field: kubecost.FilterContainer,
						Op:    kubecost.FilterNotEquals,
						Value: "123-abc_foo",
					},
				}},
				kubecost.AllocationFilterAnd{[]kubecost.AllocationFilter{
					kubecost.AllocationFilterCondition{
						Field: kubecost.FilterPod,
						Op:    kubecost.FilterNotEquals,
						Value: "aaaaaaaaaaaaaaaaaaaaaaaaa",
					},
				}},
				kubecost.AllocationFilterAnd{[]kubecost.AllocationFilter{
					kubecost.AllocationFilterCondition{
						Field: kubecost.FilterServices,
						Op:    kubecost.FilterNotContains,
						Value: "abc123",
					},
				}},
			}},
		},
		{
			input: `namespace:"__unallocated__"`,
			expected: kubecost.AllocationFilterAnd{[]kubecost.AllocationFilter{
				kubecost.AllocationFilterOr{[]kubecost.AllocationFilter{
					kubecost.AllocationFilterCondition{
						Field: kubecost.FilterNamespace,
						Op:    kubecost.FilterEquals,
						Value: kubecost.UnallocatedSuffix,
					},
				}},
			}},
			shouldMatch: []kubecost.Allocation{
				allocGenerator(kubecost.AllocationProperties{Namespace: ""}),
			},
			shouldNotMatch: []kubecost.Allocation{
				allocGenerator(kubecost.AllocationProperties{Namespace: "kube-system"}),
			},
		},
		{
			input: `namespace!:"__unallocated__"`,
			expected: kubecost.AllocationFilterAnd{[]kubecost.AllocationFilter{
				kubecost.AllocationFilterAnd{[]kubecost.AllocationFilter{
					kubecost.AllocationFilterCondition{
						Field: kubecost.FilterNamespace,
						Op:    kubecost.FilterNotEquals,
						Value: kubecost.UnallocatedSuffix,
					},
				}},
			}},
			shouldMatch: []kubecost.Allocation{
				allocGenerator(kubecost.AllocationProperties{Namespace: "kubecost"}),
			},
			shouldNotMatch: []kubecost.Allocation{
				allocGenerator(kubecost.AllocationProperties{Namespace: ""}),
			},
		},
		{
			input: `controllerKind:"__unallocated__"`,
			expected: kubecost.AllocationFilterAnd{[]kubecost.AllocationFilter{
				kubecost.AllocationFilterOr{[]kubecost.AllocationFilter{
					kubecost.AllocationFilterCondition{
						Field: kubecost.FilterControllerKind,
						Op:    kubecost.FilterEquals,
						Value: kubecost.UnallocatedSuffix,
					},
				}},
			}},
			shouldMatch: []kubecost.Allocation{
				allocGenerator(kubecost.AllocationProperties{ControllerKind: ""}),
			},
			shouldNotMatch: []kubecost.Allocation{
				allocGenerator(kubecost.AllocationProperties{ControllerKind: "deployment"}),
			},
		},
		{
			input: `controllerKind!:"__unallocated__"`,
			expected: kubecost.AllocationFilterAnd{[]kubecost.AllocationFilter{
				kubecost.AllocationFilterAnd{[]kubecost.AllocationFilter{
					kubecost.AllocationFilterCondition{
						Field: kubecost.FilterControllerKind,
						Op:    kubecost.FilterNotEquals,
						Value: kubecost.UnallocatedSuffix,
					},
				}},
			}},
			shouldMatch: []kubecost.Allocation{
				allocGenerator(kubecost.AllocationProperties{ControllerKind: "deployment"}),
			},
			shouldNotMatch: []kubecost.Allocation{
				allocGenerator(kubecost.AllocationProperties{ControllerKind: ""}),
			},
		},
		{
			input: `label[app]:"__unallocated__"`,
			expected: kubecost.AllocationFilterAnd{[]kubecost.AllocationFilter{
				kubecost.AllocationFilterOr{[]kubecost.AllocationFilter{
					kubecost.AllocationFilterCondition{
						Field: kubecost.FilterLabel,
						Key:   "app",
						Op:    kubecost.FilterEquals,
						Value: kubecost.UnallocatedSuffix,
					},
				}},
			}},
			shouldMatch: []kubecost.Allocation{
				allocGenerator(kubecost.AllocationProperties{Labels: map[string]string{"foo": "bar"}}),
			},
			shouldNotMatch: []kubecost.Allocation{
				allocGenerator(kubecost.AllocationProperties{Labels: map[string]string{"app": "test"}}),
			},
		},
		{
			input: `label[app]!:"__unallocated__"`,
			expected: kubecost.AllocationFilterAnd{[]kubecost.AllocationFilter{
				kubecost.AllocationFilterAnd{[]kubecost.AllocationFilter{
					kubecost.AllocationFilterCondition{
						Field: kubecost.FilterLabel,
						Key:   "app",
						Op:    kubecost.FilterNotEquals,
						Value: kubecost.UnallocatedSuffix,
					},
				}},
			}},
			shouldMatch: []kubecost.Allocation{
				allocGenerator(kubecost.AllocationProperties{Labels: map[string]string{"app": "test"}}),
			},
			shouldNotMatch: []kubecost.Allocation{
				allocGenerator(kubecost.AllocationProperties{Labels: map[string]string{"foo": "bar"}}),
			},
		},
		{
			input: `services:"__unallocated__"`,
			expected: kubecost.AllocationFilterAnd{[]kubecost.AllocationFilter{
				kubecost.AllocationFilterOr{[]kubecost.AllocationFilter{
					kubecost.AllocationFilterCondition{
						Field: kubecost.FilterServices,
						Op:    kubecost.FilterContains,
						Value: kubecost.UnallocatedSuffix,
					},
				}},
			}},
			shouldMatch: []kubecost.Allocation{
				allocGenerator(kubecost.AllocationProperties{Services: []string{}}),
			},
			shouldNotMatch: []kubecost.Allocation{
				allocGenerator(kubecost.AllocationProperties{Services: []string{"svc1", "svc2"}}),
			},
		},
		{
			input: `services!:"__unallocated__"`,
			expected: kubecost.AllocationFilterAnd{[]kubecost.AllocationFilter{
				kubecost.AllocationFilterAnd{[]kubecost.AllocationFilter{
					kubecost.AllocationFilterCondition{
						Field: kubecost.FilterServices,
						Op:    kubecost.FilterNotContains,
						Value: kubecost.UnallocatedSuffix,
					},
				}},
			}},
			shouldMatch: []kubecost.Allocation{
				allocGenerator(kubecost.AllocationProperties{Services: []string{"svc1", "svc2"}}),
			},
			shouldNotMatch: []kubecost.Allocation{
				allocGenerator(kubecost.AllocationProperties{Services: []string{}}),
			},
		},
	}

	for i, c := range cases {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			t.Logf("Query: %s", c.input)
			result, err := ParseAllocationFilter(c.input)
			t.Logf("Result: %s", result)
			if err != nil {
				t.Fatalf("Unexpected parse error: %s", err)
			}
			if !reflect.DeepEqual(result, c.expected) {
				t.Fatalf("Expected:\n%s\nGot:\n%s", c.expected, result)
			}

			for _, shouldMatch := range c.shouldMatch {
				if !result.Matches(&shouldMatch) {
					t.Errorf("Failed to match %s", shouldMatch.Name)
				}
			}
			for _, shouldNotMatch := range c.shouldNotMatch {
				if result.Matches(&shouldNotMatch) {
					t.Errorf("Incorrectly matched %s", shouldNotMatch.Name)
				}
			}
		})
	}
}
