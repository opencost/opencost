package filter

import (
	"fmt"
	"github.com/opencost/opencost/pkg/kubecost"
	"reflect"
	"testing"
)

func Test_AllocationCondition_Matches(t *testing.T) {
	cases := []struct {
		name   string
		a      *kubecost.Allocation
		filter Filter[*kubecost.Allocation]

		expected bool
	}{
		{
			name: "ClusterID Equals -> true",
			a: &kubecost.Allocation{
				Properties: &kubecost.AllocationProperties{
					Cluster: "cluster-one",
				},
			},
			filter: AllocationCondition{
				Field: FilterClusterID,
				Op:    FilterEquals,
				Value: "cluster-one",
			},

			expected: true,
		},
		{
			name: "ClusterID StartsWith -> true",
			a: &kubecost.Allocation{
				Properties: &kubecost.AllocationProperties{
					Cluster: "cluster-one",
				},
			},
			filter: AllocationCondition{
				Field: FilterClusterID,
				Op:    FilterStartsWith,
				Value: "cluster",
			},

			expected: true,
		},
		{
			name: "ClusterID StartsWith -> false",
			a: &kubecost.Allocation{
				Properties: &kubecost.AllocationProperties{
					Cluster: "k8s-one",
				},
			},
			filter: AllocationCondition{
				Field: FilterClusterID,
				Op:    FilterStartsWith,
				Value: "cluster",
			},

			expected: false,
		},
		{
			name: "ClusterID empty StartsWith '' -> true",
			a: &kubecost.Allocation{
				Properties: &kubecost.AllocationProperties{
					Cluster: "",
				},
			},
			filter: AllocationCondition{
				Field: FilterClusterID,
				Op:    FilterStartsWith,
				Value: "",
			},

			expected: true,
		},
		{
			name: "ClusterID nonempty StartsWith '' -> true",
			a: &kubecost.Allocation{
				Properties: &kubecost.AllocationProperties{
					Cluster: "abc",
				},
			},
			filter: AllocationCondition{
				Field: FilterClusterID,
				Op:    FilterStartsWith,
				Value: "",
			},

			expected: true,
		},
		{
			name: "Node Equals -> true",
			a: &kubecost.Allocation{
				Properties: &kubecost.AllocationProperties{
					Node: "node123",
				},
			},
			filter: AllocationCondition{
				Field: FilterNode,
				Op:    FilterEquals,
				Value: "node123",
			},

			expected: true,
		},
		{
			name: "Namespace NotEquals -> false",
			a: &kubecost.Allocation{
				Properties: &kubecost.AllocationProperties{
					Namespace: "kube-system",
				},
			},
			filter: AllocationCondition{
				Field: FilterNamespace,
				Op:    FilterNotEquals,
				Value: "kube-system",
			},

			expected: false,
		},
		{
			name: "Namespace NotEquals Unallocated -> true",
			a: &kubecost.Allocation{
				Properties: &kubecost.AllocationProperties{
					Namespace: "kube-system",
				},
			},
			filter: AllocationCondition{
				Field: FilterNamespace,
				Op:    FilterNotEquals,
				Value: kubecost.UnallocatedSuffix,
			},

			expected: true,
		},
		{
			name: "Namespace NotEquals Unallocated -> false",
			a: &kubecost.Allocation{
				Properties: &kubecost.AllocationProperties{
					Namespace: "",
				},
			},
			filter: AllocationCondition{
				Field: FilterNamespace,
				Op:    FilterNotEquals,
				Value: kubecost.UnallocatedSuffix,
			},

			expected: false,
		},
		{
			name: "Namespace Equals Unallocated -> true",
			a: &kubecost.Allocation{
				Properties: &kubecost.AllocationProperties{
					Namespace: "",
				},
			},
			filter: AllocationCondition{
				Field: FilterNamespace,
				Op:    FilterEquals,
				Value: kubecost.UnallocatedSuffix,
			},

			expected: true,
		},
		{
			name: "ControllerKind Equals -> true",
			a: &kubecost.Allocation{
				Properties: &kubecost.AllocationProperties{
					ControllerKind: "deployment", // We generally store controller kinds as all lowercase
				},
			},
			filter: AllocationCondition{
				Field: FilterControllerKind,
				Op:    FilterEquals,
				Value: "deployment",
			},

			expected: true,
		},
		{
			name: "ControllerName Equals -> true",
			a: &kubecost.Allocation{
				Properties: &kubecost.AllocationProperties{
					Controller: "kc-cost-analyzer",
				},
			},
			filter: AllocationCondition{
				Field: FilterControllerName,
				Op:    FilterEquals,
				Value: "kc-cost-analyzer",
			},

			expected: true,
		},
		{
			name: "Pod (with UID) Equals -> true",
			a: &kubecost.Allocation{
				Properties: &kubecost.AllocationProperties{
					Pod: "pod-123 UID-ABC",
				},
			},
			filter: AllocationCondition{
				Field: FilterPod,
				Op:    FilterEquals,
				Value: "pod-123 UID-ABC",
			},

			expected: true,
		},
		{
			name: "Container Equals -> true",
			a: &kubecost.Allocation{
				Properties: &kubecost.AllocationProperties{
					Container: "cost-model",
				},
			},
			filter: AllocationCondition{
				Field: FilterContainer,
				Op:    FilterEquals,
				Value: "cost-model",
			},

			expected: true,
		},
		{
			name: `label[app]="foo" -> true`,
			a: &kubecost.Allocation{
				Properties: &kubecost.AllocationProperties{
					Labels: map[string]string{
						"app": "foo",
					},
				},
			},
			filter: AllocationCondition{
				Field: FilterLabel,
				Op:    FilterEquals,
				Key:   "app",
				Value: "foo",
			},

			expected: true,
		},
		{
			name: `label[app]="foo" -> different value -> false`,
			a: &kubecost.Allocation{
				Properties: &kubecost.AllocationProperties{
					Labels: map[string]string{
						"app": "bar",
					},
				},
			},
			filter: AllocationCondition{
				Field: FilterLabel,
				Op:    FilterEquals,
				Key:   "app",
				Value: "foo",
			},

			expected: false,
		},
		{
			name: `label[app]="foo" -> label missing -> false`,
			a: &kubecost.Allocation{
				Properties: &kubecost.AllocationProperties{
					Labels: map[string]string{
						"someotherlabel": "someothervalue",
					},
				},
			},
			filter: AllocationCondition{
				Field: FilterLabel,
				Op:    FilterEquals,
				Key:   "app",
				Value: "foo",
			},

			expected: false,
		},
		{
			name: `label[app]=Unallocated -> label missing -> true`,
			a: &kubecost.Allocation{
				Properties: &kubecost.AllocationProperties{
					Labels: map[string]string{
						"someotherlabel": "someothervalue",
					},
				},
			},
			filter: AllocationCondition{
				Field: FilterLabel,
				Op:    FilterEquals,
				Key:   "app",
				Value: kubecost.UnallocatedSuffix,
			},

			expected: true,
		},
		{
			name: `label[app]=Unallocated -> label present -> false`,
			a: &kubecost.Allocation{
				Properties: &kubecost.AllocationProperties{
					Labels: map[string]string{
						"app": "test",
					},
				},
			},
			filter: AllocationCondition{
				Field: FilterLabel,
				Op:    FilterEquals,
				Key:   "app",
				Value: kubecost.UnallocatedSuffix,
			},

			expected: false,
		},
		{
			name: `label[app]!=Unallocated -> label missing -> false`,
			a: &kubecost.Allocation{
				Properties: &kubecost.AllocationProperties{
					Labels: map[string]string{
						"someotherlabel": "someothervalue",
					},
				},
			},
			filter: AllocationCondition{
				Field: FilterLabel,
				Op:    FilterNotEquals,
				Key:   "app",
				Value: kubecost.UnallocatedSuffix,
			},

			expected: false,
		},
		{
			name: `label[app]!=Unallocated -> label present -> true`,
			a: &kubecost.Allocation{
				Properties: &kubecost.AllocationProperties{
					Labels: map[string]string{
						"app": "test",
					},
				},
			},
			filter: AllocationCondition{
				Field: FilterLabel,
				Op:    FilterNotEquals,
				Key:   "app",
				Value: kubecost.UnallocatedSuffix,
			},

			expected: true,
		},
		{
			name: `label[app]!="foo" -> label missing -> true`,
			a: &kubecost.Allocation{
				Properties: &kubecost.AllocationProperties{
					Labels: map[string]string{
						"someotherlabel": "someothervalue",
					},
				},
			},
			filter: AllocationCondition{
				Field: FilterLabel,
				Op:    FilterNotEquals,
				Key:   "app",
				Value: "foo",
			},

			expected: true,
		},
		{
			name: `annotation[prom_modified_name]="testing123" -> true`,
			a: &kubecost.Allocation{
				Properties: &kubecost.AllocationProperties{
					Annotations: map[string]string{
						"prom_modified_name": "testing123",
					},
				},
			},
			filter: AllocationCondition{
				Field: FilterAnnotation,
				Op:    FilterEquals,
				Key:   "prom_modified_name",
				Value: "testing123",
			},

			expected: true,
		},
		{
			name: `annotation[app]="foo" -> different value -> false`,
			a: &kubecost.Allocation{
				Properties: &kubecost.AllocationProperties{
					Annotations: map[string]string{
						"app": "bar",
					},
				},
			},
			filter: AllocationCondition{
				Field: FilterAnnotation,
				Op:    FilterEquals,
				Key:   "app",
				Value: "foo",
			},

			expected: false,
		},
		{
			name: `annotation[app]="foo" -> annotation missing -> false`,
			a: &kubecost.Allocation{
				Properties: &kubecost.AllocationProperties{
					Annotations: map[string]string{
						"someotherannotation": "someothervalue",
					},
				},
			},
			filter: AllocationCondition{
				Field: FilterAnnotation,
				Op:    FilterEquals,
				Key:   "app",
				Value: "foo",
			},

			expected: false,
		},
		{
			name: `annotation[app]!="foo" -> annotation missing -> true`,
			a: &kubecost.Allocation{
				Properties: &kubecost.AllocationProperties{
					Annotations: map[string]string{
						"someotherannotation": "someothervalue",
					},
				},
			},
			filter: AllocationCondition{
				Field: FilterAnnotation,
				Op:    FilterNotEquals,
				Key:   "app",
				Value: "foo",
			},

			expected: true,
		},
		{
			name: `namespace unallocated -> true`,
			a: &kubecost.Allocation{
				Properties: &kubecost.AllocationProperties{
					Namespace: "",
				},
			},
			filter: AllocationCondition{
				Field: FilterNamespace,
				Op:    FilterEquals,
				Value: kubecost.UnallocatedSuffix,
			},

			expected: true,
		},
		{
			name: `services contains -> true`,
			a: &kubecost.Allocation{
				Properties: &kubecost.AllocationProperties{
					Services: []string{"serv1", "serv2"},
				},
			},
			filter: AllocationCondition{
				Field: FilterServices,
				Op:    FilterContains,
				Value: "serv2",
			},

			expected: true,
		},
		{
			name: `services contains -> false`,
			a: &kubecost.Allocation{
				Properties: &kubecost.AllocationProperties{
					Services: []string{"serv1", "serv2"},
				},
			},
			filter: AllocationCondition{
				Field: FilterServices,
				Op:    FilterContains,
				Value: "serv3",
			},

			expected: false,
		},
		{
			name: `services notcontains -> true`,
			a: &kubecost.Allocation{
				Properties: &kubecost.AllocationProperties{
					Services: []string{"serv1", "serv2"},
				},
			},
			filter: AllocationCondition{
				Field: FilterServices,
				Op:    FilterNotContains,
				Value: "serv3",
			},

			expected: true,
		},
		{
			name: `services notcontains -> false`,
			a: &kubecost.Allocation{
				Properties: &kubecost.AllocationProperties{
					Services: []string{"serv1", "serv2"},
				},
			},
			filter: AllocationCondition{
				Field: FilterServices,
				Op:    FilterNotContains,
				Value: "serv2",
			},

			expected: false,
		},
		{
			name: `services notcontains unallocated -> true`,
			a: &kubecost.Allocation{
				Properties: &kubecost.AllocationProperties{
					Services: []string{"serv1", "serv2"},
				},
			},
			filter: AllocationCondition{
				Field: FilterServices,
				Op:    FilterNotContains,
				Value: kubecost.UnallocatedSuffix,
			},

			expected: true,
		},
		{
			name: `services notcontains unallocated -> false`,
			a: &kubecost.Allocation{
				Properties: &kubecost.AllocationProperties{
					Services: []string{},
				},
			},
			filter: AllocationCondition{
				Field: FilterServices,
				Op:    FilterNotContains,
				Value: kubecost.UnallocatedSuffix,
			},

			expected: false,
		},
		{
			name: `services containsprefix -> true`,
			a: &kubecost.Allocation{
				Properties: &kubecost.AllocationProperties{
					Services: []string{"serv1", "serv2"},
				},
			},
			filter: AllocationCondition{
				Field: FilterServices,
				Op:    FilterContainsPrefix,
				Value: "serv",
			},

			expected: true,
		},
		{
			name: `services containsprefix -> false`,
			a: &kubecost.Allocation{
				Properties: &kubecost.AllocationProperties{
					Services: []string{"foo", "bar"},
				},
			},
			filter: AllocationCondition{
				Field: FilterServices,
				Op:    FilterContainsPrefix,
				Value: "serv",
			},

			expected: false,
		},
		{
			name: `services contains unallocated -> false`,
			a: &kubecost.Allocation{
				Properties: &kubecost.AllocationProperties{
					Services: []string{"serv1", "serv2"},
				},
			},
			filter: AllocationCondition{
				Field: FilterServices,
				Op:    FilterContains,
				Value: kubecost.UnallocatedSuffix,
			},

			expected: false,
		},
		{
			name: `services contains unallocated -> true`,
			a: &kubecost.Allocation{
				Properties: &kubecost.AllocationProperties{
					Services: []string{},
				},
			},
			filter: AllocationCondition{
				Field: FilterServices,
				Op:    FilterContains,
				Value: kubecost.UnallocatedSuffix,
			},

			expected: true,
		},
	}

	for _, c := range cases {
		result := c.filter.Matches(c.a)

		if result != c.expected {
			t.Errorf("%s: expected %t, got %t", c.name, c.expected, result)
		}
	}
}

func Test_NoneAllocation_Matches(t *testing.T) {
	cases := []struct {
		name string
		a    *kubecost.Allocation
	}{
		{
			name: "nil",
			a:    nil,
		},
		{
			name: "nil properties",
			a: &kubecost.Allocation{
				Properties: nil,
			},
		},
		{
			name: "empty properties",
			a: &kubecost.Allocation{
				Properties: &kubecost.AllocationProperties{},
			},
		},
		{
			name: "ClusterID",
			a: &kubecost.Allocation{
				Properties: &kubecost.AllocationProperties{
					Cluster: "cluster-one",
				},
			},
		},
		{
			name: "Node",
			a: &kubecost.Allocation{
				Properties: &kubecost.AllocationProperties{
					Node: "node123",
				},
			},
		},
		{
			name: "Namespace",
			a: &kubecost.Allocation{
				Properties: &kubecost.AllocationProperties{
					Namespace: "kube-system",
				},
			},
		},
		{
			name: "ControllerKind",
			a: &kubecost.Allocation{
				Properties: &kubecost.AllocationProperties{
					ControllerKind: "deployment", // We generally store controller kinds as all lowercase
				},
			},
		},
		{
			name: "ControllerName",
			a: &kubecost.Allocation{
				Properties: &kubecost.AllocationProperties{
					Controller: "kc-cost-analyzer",
				},
			},
		},
		{
			name: "Pod",
			a: &kubecost.Allocation{
				Properties: &kubecost.AllocationProperties{
					Pod: "pod-123 UID-ABC",
				},
			},
		},
		{
			name: "Container",
			a: &kubecost.Allocation{
				Properties: &kubecost.AllocationProperties{
					Container: "cost-model",
				},
			},
		},
		{
			name: `label`,
			a: &kubecost.Allocation{
				Properties: &kubecost.AllocationProperties{
					Labels: map[string]string{
						"app": "foo",
					},
				},
			},
		},
		{
			name: `annotation`,
			a: &kubecost.Allocation{
				Properties: &kubecost.AllocationProperties{
					Annotations: map[string]string{
						"prom_modified_name": "testing123",
					},
				},
			},
		},
		{
			name: `services`,
			a: &kubecost.Allocation{
				Properties: &kubecost.AllocationProperties{
					Services: []string{"serv1", "serv2"},
				},
			},
		},
	}

	for _, c := range cases {
		result := None[*kubecost.Allocation]{}.Matches(c.a)

		if result {
			t.Errorf("%s: should have been rejected", c.name)
		}
	}
}
func Test_AndAllocation_Matches(t *testing.T) {
	cases := []struct {
		name   string
		a      *kubecost.Allocation
		filter Filter[*kubecost.Allocation]

		expected bool
	}{
		{
			name: `label[app]="foo" and namespace="kubecost" -> both true`,
			a: &kubecost.Allocation{
				Properties: &kubecost.AllocationProperties{
					Namespace: "kubecost",
					Labels: map[string]string{
						"app": "foo",
					},
				},
			},
			filter: And[*kubecost.Allocation]{[]Filter[*kubecost.Allocation]{
				AllocationCondition{
					Field: FilterLabel,
					Op:    FilterEquals,
					Key:   "app",
					Value: "foo",
				},
				AllocationCondition{
					Field: FilterNamespace,
					Op:    FilterEquals,
					Value: "kubecost",
				},
			}},
			expected: true,
		},
		{
			name: `label[app]="foo" and namespace="kubecost" -> first true`,
			a: &kubecost.Allocation{
				Properties: &kubecost.AllocationProperties{
					Namespace: "kube-system",
					Labels: map[string]string{
						"app": "foo",
					},
				},
			},
			filter: And[*kubecost.Allocation]{[]Filter[*kubecost.Allocation]{
				AllocationCondition{
					Field: FilterLabel,
					Op:    FilterEquals,
					Key:   "app",
					Value: "foo",
				},
				AllocationCondition{
					Field: FilterNamespace,
					Op:    FilterEquals,
					Value: "kubecost",
				},
			}},
			expected: false,
		},
		{
			name: `label[app]="foo" and namespace="kubecost" -> second true`,
			a: &kubecost.Allocation{
				Properties: &kubecost.AllocationProperties{
					Namespace: "kubecost",
					Labels: map[string]string{
						"app": "bar",
					},
				},
			},
			filter: And[*kubecost.Allocation]{[]Filter[*kubecost.Allocation]{
				AllocationCondition{
					Field: FilterLabel,
					Op:    FilterEquals,
					Key:   "app",
					Value: "foo",
				},
				AllocationCondition{
					Field: FilterNamespace,
					Op:    FilterEquals,
					Value: "kubecost",
				},
			}},
			expected: false,
		},
		{
			name: `label[app]="foo" and namespace="kubecost" -> both false`,
			a: &kubecost.Allocation{
				Properties: &kubecost.AllocationProperties{
					Namespace: "kube-system",
					Labels: map[string]string{
						"app": "bar",
					},
				},
			},
			filter: And[*kubecost.Allocation]{[]Filter[*kubecost.Allocation]{
				AllocationCondition{
					Field: FilterLabel,
					Op:    FilterEquals,
					Key:   "app",
					Value: "foo",
				},
				AllocationCondition{
					Field: FilterNamespace,
					Op:    FilterEquals,
					Value: "kubecost",
				},
			}},
			expected: false,
		},
		{
			name: `(and none) matches nothing`,
			a: &kubecost.Allocation{
				Properties: &kubecost.AllocationProperties{
					Namespace: "kube-system",
					Labels: map[string]string{
						"app": "bar",
					},
				},
			},
			filter: And[*kubecost.Allocation]{[]Filter[*kubecost.Allocation]{
				None[*kubecost.Allocation]{},
			}},
			expected: false,
		},
	}

	for _, c := range cases {
		result := c.filter.Matches(c.a)

		if result != c.expected {
			t.Errorf("%s: expected %t, got %t", c.name, c.expected, result)
		}
	}
}

func Test_OrAllocation_Matches(t *testing.T) {
	cases := []struct {
		name   string
		a      *kubecost.Allocation
		filter Filter[*kubecost.Allocation]

		expected bool
	}{
		{
			name: `label[app]="foo" or namespace="kubecost" -> both true`,
			a: &kubecost.Allocation{
				Properties: &kubecost.AllocationProperties{
					Namespace: "kubecost",
					Labels: map[string]string{
						"app": "foo",
					},
				},
			},
			filter: Or[*kubecost.Allocation]{[]Filter[*kubecost.Allocation]{
				AllocationCondition{
					Field: FilterLabel,
					Op:    FilterEquals,
					Key:   "app",
					Value: "foo",
				},
				AllocationCondition{
					Field: FilterNamespace,
					Op:    FilterEquals,
					Value: "kubecost",
				},
			}},
			expected: true,
		},
		{
			name: `label[app]="foo" or namespace="kubecost" -> first true`,
			a: &kubecost.Allocation{
				Properties: &kubecost.AllocationProperties{
					Namespace: "kube-system",
					Labels: map[string]string{
						"app": "foo",
					},
				},
			},
			filter: Or[*kubecost.Allocation]{[]Filter[*kubecost.Allocation]{
				AllocationCondition{
					Field: FilterLabel,
					Op:    FilterEquals,
					Key:   "app",
					Value: "foo",
				},
				AllocationCondition{
					Field: FilterNamespace,
					Op:    FilterEquals,
					Value: "kubecost",
				},
			}},
			expected: true,
		},
		{
			name: `label[app]="foo" or namespace="kubecost" -> second true`,
			a: &kubecost.Allocation{
				Properties: &kubecost.AllocationProperties{
					Namespace: "kubecost",
					Labels: map[string]string{
						"app": "bar",
					},
				},
			},
			filter: Or[*kubecost.Allocation]{[]Filter[*kubecost.Allocation]{
				AllocationCondition{
					Field: FilterLabel,
					Op:    FilterEquals,
					Key:   "app",
					Value: "foo",
				},
				AllocationCondition{
					Field: FilterNamespace,
					Op:    FilterEquals,
					Value: "kubecost",
				},
			}},
			expected: true,
		},
		{
			name: `label[app]="foo" or namespace="kubecost" -> both false`,
			a: &kubecost.Allocation{
				Properties: &kubecost.AllocationProperties{
					Namespace: "kube-system",
					Labels: map[string]string{
						"app": "bar",
					},
				},
			},
			filter: Or[*kubecost.Allocation]{[]Filter[*kubecost.Allocation]{
				AllocationCondition{
					Field: FilterLabel,
					Op:    FilterEquals,
					Key:   "app",
					Value: "foo",
				},
				AllocationCondition{
					Field: FilterNamespace,
					Op:    FilterEquals,
					Value: "kubecost",
				},
			}},
			expected: false,
		},
	}

	for _, c := range cases {
		result := c.filter.Matches(c.a)

		if result != c.expected {
			t.Errorf("%s: expected %t, got %t", c.name, c.expected, result)
		}
	}
}

func Test_Allocation_Flattened(t *testing.T) {
	cases := []struct {
		name string

		input    Filter[*kubecost.Allocation]
		expected Filter[*kubecost.Allocation]
	}{
		{
			name: "AllocationCondition",
			input: AllocationCondition{
				Field: FilterNamespace,
				Op:    FilterEquals,
			},
			expected: AllocationCondition{
				Field: FilterNamespace,
				Op:    FilterEquals,
			},
		},
		{
			name:     "empty And[*kubecost.Allocation] (nil)",
			input:    And[*kubecost.Allocation]{},
			expected: nil,
		},
		{
			name:     "empty And[*kubecost.Allocation] (len 0)",
			input:    And[*kubecost.Allocation]{Filters: []Filter[*kubecost.Allocation]{}},
			expected: nil,
		},
		{
			name:     "empty Or[*kubecost.Allocation] (nil)",
			input:    Or[*kubecost.Allocation]{},
			expected: nil,
		},
		{
			name:     "empty Or[*kubecost.Allocation] (len 0)",
			input:    Or[*kubecost.Allocation]{Filters: []Filter[*kubecost.Allocation]{}},
			expected: nil,
		},
		{
			name: "single-element And[*kubecost.Allocation]",
			input: And[*kubecost.Allocation]{Filters: []Filter[*kubecost.Allocation]{
				AllocationCondition{
					Field: FilterNamespace,
					Op:    FilterEquals,
				},
			}},

			expected: AllocationCondition{
				Field: FilterNamespace,
				Op:    FilterEquals,
			},
		},
		{
			name: "single-element Or[*kubecost.Allocation]",
			input: Or[*kubecost.Allocation]{Filters: []Filter[*kubecost.Allocation]{
				AllocationCondition{
					Field: FilterNamespace,
					Op:    FilterEquals,
				},
			}},

			expected: AllocationCondition{
				Field: FilterNamespace,
				Op:    FilterEquals,
			},
		},
		{
			name: "multi-element And[*kubecost.Allocation]",
			input: And[*kubecost.Allocation]{Filters: []Filter[*kubecost.Allocation]{
				AllocationCondition{
					Field: FilterNamespace,
					Op:    FilterEquals,
				},
				AllocationCondition{
					Field: FilterClusterID,
					Op:    FilterNotEquals,
				},
				AllocationCondition{
					Field: FilterServices,
					Op:    FilterContains,
				},
			}},

			expected: And[*kubecost.Allocation]{Filters: []Filter[*kubecost.Allocation]{
				AllocationCondition{
					Field: FilterNamespace,
					Op:    FilterEquals,
				},
				AllocationCondition{
					Field: FilterClusterID,
					Op:    FilterNotEquals,
				},
				AllocationCondition{
					Field: FilterServices,
					Op:    FilterContains,
				},
			}},
		},
		{
			name: "multi-element Or[*kubecost.Allocation]",
			input: Or[*kubecost.Allocation]{Filters: []Filter[*kubecost.Allocation]{
				AllocationCondition{
					Field: FilterNamespace,
					Op:    FilterEquals,
				},
				AllocationCondition{
					Field: FilterClusterID,
					Op:    FilterNotEquals,
				},
				AllocationCondition{
					Field: FilterServices,
					Op:    FilterContains,
				},
			}},

			expected: Or[*kubecost.Allocation]{Filters: []Filter[*kubecost.Allocation]{
				AllocationCondition{
					Field: FilterNamespace,
					Op:    FilterEquals,
				},
				AllocationCondition{
					Field: FilterClusterID,
					Op:    FilterNotEquals,
				},
				AllocationCondition{
					Field: FilterServices,
					Op:    FilterContains,
				},
			}},
		},
		{
			name:     "None[*kubecost.Allocation]",
			input:    None[*kubecost.Allocation]{},
			expected: None[*kubecost.Allocation]{},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			result := c.input.Flattened()

			if !reflect.DeepEqual(result, c.expected) {
				t.Errorf("Expected: '%s'. Got '%s'.", c.expected, result)
			}
		})
	}
}

func Test_Allocation_Equals(t *testing.T) {
	cases := []struct {
		left     Filter[*kubecost.Allocation]
		right    Filter[*kubecost.Allocation]
		expected bool
	}{
		// AC
		{
			left:     AllocationCondition{},
			right:    AllocationCondition{},
			expected: true,
		},
		{
			left: AllocationCondition{
				Field: FilterNamespace,
				Op:    FilterStartsWith,
				Value: "kubecost-abc",
			},
			right: AllocationCondition{
				Field: FilterNamespace,
				Op:    FilterStartsWith,
				Value: "kubecost-abc",
			},
			expected: true,
		},
		{
			left: AllocationCondition{
				Field: FilterLabel,
				Op:    FilterEquals,
				Key:   "app",
				Value: "kubecost-abc",
			},
			right: AllocationCondition{
				Field: FilterLabel,
				Op:    FilterEquals,
				Key:   "app",
				Value: "kubecost-abc",
			},
			expected: true,
		},
		{
			left: AllocationCondition{
				Field: FilterLabel,
				Op:    FilterEquals,
				Key:   "app",
				Value: "kubecost-abc",
			},
			right: AllocationCondition{
				Field: FilterLabel,
				Op:    FilterEquals,
				Value: "kubecost-abc",
			},
			expected: false,
		},
		{
			left: AllocationCondition{
				Field: FilterLabel,
				Op:    FilterEquals,
				Value: "kubecost-abc",
			},
			right: AllocationCondition{
				Field: FilterLabel,
				Op:    FilterEquals,
				Key:   "app",
				Value: "kubecost-abc",
			},
			expected: false,
		},
		{
			left: AllocationCondition{
				Field: FilterNamespace,
				Op:    FilterStartsWith,
				Value: "kubecost-abc",
			},
			right: AllocationCondition{
				Field: FilterNamespace,
				Op:    FilterStartsWith,
				Value: "kubecost-abcd",
			},
			expected: false,
		},
		// OR
		// EMPTY
		{
			left:     Or[*kubecost.Allocation]{},
			right:    nil,
			expected: false,
		},
		{
			left:     Or[*kubecost.Allocation]{Filters: []Filter[*kubecost.Allocation]{}},
			right:    nil,
			expected: false,
		},

		{
			left:     Or[*kubecost.Allocation]{},
			right:    Or[*kubecost.Allocation]{},
			expected: true,
		},
		{
			left:     Or[*kubecost.Allocation]{},
			right:    Or[*kubecost.Allocation]{Filters: []Filter[*kubecost.Allocation]{}},
			expected: true,
		},

		{
			left:     Or[*kubecost.Allocation]{Filters: []Filter[*kubecost.Allocation]{}},
			right:    Or[*kubecost.Allocation]{},
			expected: true,
		},
		{
			left:     Or[*kubecost.Allocation]{Filters: []Filter[*kubecost.Allocation]{}},
			right:    Or[*kubecost.Allocation]{Filters: []Filter[*kubecost.Allocation]{}},
			expected: true,
		},
		// FILLED
		{
			left: Or[*kubecost.Allocation]{Filters: []Filter[*kubecost.Allocation]{
				None[*kubecost.Allocation]{},
				AllocationCondition{
					Field: FilterLabel,
					Op:    FilterStartsWith,
					Key:   "xyz",
					Value: "kubecost",
				},
				And[*kubecost.Allocation]{
					Filters: []Filter[*kubecost.Allocation]{
						AllocationCondition{
							Field: FilterNamespace,
							Op:    FilterEquals,
							Value: "ns1",
						},
						AllocationCondition{
							Field: FilterNamespace,
							Op:    FilterEquals,
							Value: "ns2",
						},
					},
				},
			}},
			right: Or[*kubecost.Allocation]{Filters: []Filter[*kubecost.Allocation]{
				None[*kubecost.Allocation]{},
				AllocationCondition{
					Field: FilterLabel,
					Op:    FilterStartsWith,
					Key:   "xyz",
					Value: "kubecost",
				},
				And[*kubecost.Allocation]{
					Filters: []Filter[*kubecost.Allocation]{
						AllocationCondition{
							Field: FilterNamespace,
							Op:    FilterEquals,
							Value: "ns1",
						},
						AllocationCondition{
							Field: FilterNamespace,
							Op:    FilterEquals,
							Value: "ns2",
						},
					},
				},
			}},
			expected: true,
		},
		{
			left: Or[*kubecost.Allocation]{Filters: []Filter[*kubecost.Allocation]{
				None[*kubecost.Allocation]{},
				AllocationCondition{
					Field: FilterLabel,
					Op:    FilterStartsWith,
					Key:   "xyz",
					Value: "kubecost",
				},
				And[*kubecost.Allocation]{
					Filters: []Filter[*kubecost.Allocation]{
						AllocationCondition{
							Field: FilterNamespace,
							Op:    FilterEquals,
							Value: "ns1",
						},
						AllocationCondition{
							Field: FilterNamespace,
							Op:    FilterEquals,
							Value: "ns2",
						},
					},
				},
			}},
			right: Or[*kubecost.Allocation]{Filters: []Filter[*kubecost.Allocation]{
				AllocationCondition{
					Field: FilterLabel,
					Op:    FilterStartsWith,
					Key:   "xyz",
					Value: "kubecost",
				},
				None[*kubecost.Allocation]{},
				And[*kubecost.Allocation]{
					Filters: []Filter[*kubecost.Allocation]{
						AllocationCondition{
							Field: FilterNamespace,
							Op:    FilterEquals,
							Value: "ns2",
						},
						AllocationCondition{
							Field: FilterNamespace,
							Op:    FilterEquals,
							Value: "ns1",
						},
					},
				},
			}},
			expected: true,
		},
		{
			left: Or[*kubecost.Allocation]{Filters: []Filter[*kubecost.Allocation]{
				None[*kubecost.Allocation]{},
				AllocationCondition{
					Field: FilterLabel,
					Op:    FilterStartsWith,
					Key:   "xyz",
					Value: "kubecost",
				},
				And[*kubecost.Allocation]{
					Filters: []Filter[*kubecost.Allocation]{
						AllocationCondition{
							Field: FilterNamespace,
							Op:    FilterEquals,
							Value: "ns1",
						},
						AllocationCondition{
							Field: FilterNamespace,
							Op:    FilterEquals,
							Value: "ns2",
						},
					},
				},
			}},
			right: Or[*kubecost.Allocation]{Filters: []Filter[*kubecost.Allocation]{
				And[*kubecost.Allocation]{
					Filters: []Filter[*kubecost.Allocation]{
						AllocationCondition{
							Field: FilterNamespace,
							Op:    FilterEquals,
							Value: "ns1",
						},
						AllocationCondition{
							Field: FilterNamespace,
							Op:    FilterEquals,
							Value: "ns2",
						},
					},
				},
				None[*kubecost.Allocation]{},
				AllocationCondition{
					Field: FilterLabel,
					Op:    FilterStartsWith,
					Key:   "xyz",
					Value: "kubecost",
				},
			}},
			expected: true,
		},
		{
			left: Or[*kubecost.Allocation]{Filters: []Filter[*kubecost.Allocation]{
				None[*kubecost.Allocation]{},
				AllocationCondition{
					Field: FilterLabel,
					Op:    FilterStartsWith,
					Key:   "xyz",
					Value: "kubecost",
				},
				And[*kubecost.Allocation]{
					Filters: []Filter[*kubecost.Allocation]{
						AllocationCondition{
							Field: FilterNamespace,
							Op:    FilterEquals,
							Value: "ns1",
						},
						AllocationCondition{
							Field: FilterNamespace,
							Op:    FilterEquals,
							Value: "ns2",
						},
					},
				},
			}},
			right: Or[*kubecost.Allocation]{Filters: []Filter[*kubecost.Allocation]{
				None[*kubecost.Allocation]{},
				AllocationCondition{
					Field: FilterLabel,
					Op:    FilterStartsWith,
					Key:   "xyz",
					Value: "kubecost",
				},
				And[*kubecost.Allocation]{
					Filters: []Filter[*kubecost.Allocation]{
						AllocationCondition{
							Field: FilterNamespace,
							Op:    FilterEquals,
							Value: "ns3",
						},
						AllocationCondition{
							Field: FilterNamespace,
							Op:    FilterEquals,
							Value: "ns2",
						},
					},
				},
			}},
			expected: false,
		},
		{
			left: Or[*kubecost.Allocation]{Filters: []Filter[*kubecost.Allocation]{
				None[*kubecost.Allocation]{},
				AllocationCondition{
					Field: FilterLabel,
					Op:    FilterStartsWith,
					Key:   "xyz",
					Value: "kubecost",
				},
				And[*kubecost.Allocation]{
					Filters: []Filter[*kubecost.Allocation]{
						AllocationCondition{
							Field: FilterNamespace,
							Op:    FilterEquals,
							Value: "ns1",
						},
						AllocationCondition{
							Field: FilterNamespace,
							Op:    FilterEquals,
							Value: "ns2",
						},
					},
				},
			}},
			right: Or[*kubecost.Allocation]{Filters: []Filter[*kubecost.Allocation]{
				AllocationCondition{
					Field: FilterLabel,
					Op:    FilterStartsWith,
					Key:   "xyz",
					Value: "kubecost",
				},
				And[*kubecost.Allocation]{
					Filters: []Filter[*kubecost.Allocation]{
						AllocationCondition{
							Field: FilterNamespace,
							Op:    FilterEquals,
							Value: "ns1",
						},
						AllocationCondition{
							Field: FilterNamespace,
							Op:    FilterEquals,
							Value: "ns2",
						},
					},
				},
			}},
			expected: false,
		},
		// AND
		// EMPTY
		{
			left:     And[*kubecost.Allocation]{},
			right:    nil,
			expected: false,
		},
		{
			left:     And[*kubecost.Allocation]{Filters: []Filter[*kubecost.Allocation]{}},
			right:    nil,
			expected: false,
		},

		{
			left:     And[*kubecost.Allocation]{},
			right:    And[*kubecost.Allocation]{},
			expected: true,
		},
		{
			left:     And[*kubecost.Allocation]{},
			right:    And[*kubecost.Allocation]{Filters: []Filter[*kubecost.Allocation]{}},
			expected: true,
		},

		{
			left:     And[*kubecost.Allocation]{Filters: []Filter[*kubecost.Allocation]{}},
			right:    And[*kubecost.Allocation]{},
			expected: true,
		},
		{
			left:     And[*kubecost.Allocation]{Filters: []Filter[*kubecost.Allocation]{}},
			right:    And[*kubecost.Allocation]{Filters: []Filter[*kubecost.Allocation]{}},
			expected: true,
		},
		// FILLED
		{
			left: And[*kubecost.Allocation]{Filters: []Filter[*kubecost.Allocation]{
				None[*kubecost.Allocation]{},
				AllocationCondition{
					Field: FilterLabel,
					Op:    FilterStartsWith,
					Key:   "xyz",
					Value: "kubecost",
				},
				Or[*kubecost.Allocation]{
					Filters: []Filter[*kubecost.Allocation]{
						AllocationCondition{
							Field: FilterNamespace,
							Op:    FilterEquals,
							Value: "ns1",
						},
						AllocationCondition{
							Field: FilterNamespace,
							Op:    FilterEquals,
							Value: "ns2",
						},
					},
				},
			}},
			right: And[*kubecost.Allocation]{Filters: []Filter[*kubecost.Allocation]{
				None[*kubecost.Allocation]{},
				AllocationCondition{
					Field: FilterLabel,
					Op:    FilterStartsWith,
					Key:   "xyz",
					Value: "kubecost",
				},
				Or[*kubecost.Allocation]{
					Filters: []Filter[*kubecost.Allocation]{
						AllocationCondition{
							Field: FilterNamespace,
							Op:    FilterEquals,
							Value: "ns1",
						},
						AllocationCondition{
							Field: FilterNamespace,
							Op:    FilterEquals,
							Value: "ns2",
						},
					},
				},
			}},
			expected: true,
		},
		{
			left: And[*kubecost.Allocation]{Filters: []Filter[*kubecost.Allocation]{
				None[*kubecost.Allocation]{},
				AllocationCondition{
					Field: FilterLabel,
					Op:    FilterStartsWith,
					Key:   "xyz",
					Value: "kubecost",
				},
				Or[*kubecost.Allocation]{
					Filters: []Filter[*kubecost.Allocation]{
						AllocationCondition{
							Field: FilterNamespace,
							Op:    FilterEquals,
							Value: "ns1",
						},
						AllocationCondition{
							Field: FilterNamespace,
							Op:    FilterEquals,
							Value: "ns2",
						},
					},
				},
			}},
			right: And[*kubecost.Allocation]{Filters: []Filter[*kubecost.Allocation]{
				AllocationCondition{
					Field: FilterLabel,
					Op:    FilterStartsWith,
					Key:   "xyz",
					Value: "kubecost",
				},
				None[*kubecost.Allocation]{},
				Or[*kubecost.Allocation]{
					Filters: []Filter[*kubecost.Allocation]{
						AllocationCondition{
							Field: FilterNamespace,
							Op:    FilterEquals,
							Value: "ns2",
						},
						AllocationCondition{
							Field: FilterNamespace,
							Op:    FilterEquals,
							Value: "ns1",
						},
					},
				},
			}},
			expected: true,
		},
		{
			left: And[*kubecost.Allocation]{Filters: []Filter[*kubecost.Allocation]{
				None[*kubecost.Allocation]{},
				AllocationCondition{
					Field: FilterLabel,
					Op:    FilterStartsWith,
					Key:   "xyz",
					Value: "kubecost",
				},
				Or[*kubecost.Allocation]{
					Filters: []Filter[*kubecost.Allocation]{
						AllocationCondition{
							Field: FilterNamespace,
							Op:    FilterEquals,
							Value: "ns1",
						},
						AllocationCondition{
							Field: FilterNamespace,
							Op:    FilterEquals,
							Value: "ns2",
						},
					},
				},
			}},
			right: And[*kubecost.Allocation]{Filters: []Filter[*kubecost.Allocation]{
				Or[*kubecost.Allocation]{
					Filters: []Filter[*kubecost.Allocation]{
						AllocationCondition{
							Field: FilterNamespace,
							Op:    FilterEquals,
							Value: "ns1",
						},
						AllocationCondition{
							Field: FilterNamespace,
							Op:    FilterEquals,
							Value: "ns2",
						},
					},
				},
				None[*kubecost.Allocation]{},
				AllocationCondition{
					Field: FilterLabel,
					Op:    FilterStartsWith,
					Key:   "xyz",
					Value: "kubecost",
				},
			}},
			expected: true,
		},
		{
			left: And[*kubecost.Allocation]{Filters: []Filter[*kubecost.Allocation]{
				None[*kubecost.Allocation]{},
				AllocationCondition{
					Field: FilterLabel,
					Op:    FilterStartsWith,
					Key:   "xyz",
					Value: "kubecost",
				},
				Or[*kubecost.Allocation]{
					Filters: []Filter[*kubecost.Allocation]{
						AllocationCondition{
							Field: FilterNamespace,
							Op:    FilterEquals,
							Value: "ns1",
						},
						AllocationCondition{
							Field: FilterNamespace,
							Op:    FilterEquals,
							Value: "ns2",
						},
					},
				},
			}},
			right: And[*kubecost.Allocation]{Filters: []Filter[*kubecost.Allocation]{
				None[*kubecost.Allocation]{},
				AllocationCondition{
					Field: FilterLabel,
					Op:    FilterStartsWith,
					Key:   "xyz",
					Value: "kubecost",
				},
				Or[*kubecost.Allocation]{
					Filters: []Filter[*kubecost.Allocation]{
						AllocationCondition{
							Field: FilterNamespace,
							Op:    FilterEquals,
							Value: "ns3",
						},
						AllocationCondition{
							Field: FilterNamespace,
							Op:    FilterEquals,
							Value: "ns2",
						},
					},
				},
			}},
			expected: false,
		},
		{
			left: And[*kubecost.Allocation]{Filters: []Filter[*kubecost.Allocation]{
				None[*kubecost.Allocation]{},
				AllocationCondition{
					Field: FilterLabel,
					Op:    FilterStartsWith,
					Key:   "xyz",
					Value: "kubecost",
				},
				Or[*kubecost.Allocation]{
					Filters: []Filter[*kubecost.Allocation]{
						AllocationCondition{
							Field: FilterNamespace,
							Op:    FilterEquals,
							Value: "ns1",
						},
						AllocationCondition{
							Field: FilterNamespace,
							Op:    FilterEquals,
							Value: "ns2",
						},
					},
				},
			}},
			right: And[*kubecost.Allocation]{Filters: []Filter[*kubecost.Allocation]{
				AllocationCondition{
					Field: FilterLabel,
					Op:    FilterStartsWith,
					Key:   "xyz",
					Value: "kubecost",
				},
				Or[*kubecost.Allocation]{
					Filters: []Filter[*kubecost.Allocation]{
						AllocationCondition{
							Field: FilterNamespace,
							Op:    FilterEquals,
							Value: "ns1",
						},
						AllocationCondition{
							Field: FilterNamespace,
							Op:    FilterEquals,
							Value: "ns2",
						},
					},
				},
			}},
			expected: false,
		},
	}

	for _, c := range cases {
		t.Run(fmt.Sprintf("'%s' = '%s'", c.left, c.right), func(t *testing.T) {
			if c.left.Equals(c.right) != c.expected {
				t.Fatalf("Expected: %t", c.expected)
			}
		})
	}
}
