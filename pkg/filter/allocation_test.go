package filter

import (
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
				Op:    StringEquals,
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
				Op:    StringStartsWith,
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
				Op:    StringStartsWith,
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
				Op:    StringStartsWith,
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
				Op:    StringStartsWith,
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
				Op:    StringEquals,
				Value: "node123",
			},

			expected: true,
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
				Op:    StringEquals,
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
				Op:    StringEquals,
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
				Op:    StringEquals,
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
				Op:    StringEquals,
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
				Op:    StringEquals,
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
				Op:    StringEquals,
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
				Op:    StringEquals,
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
				Op:    StringEquals,
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
				Op:    StringEquals,
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
				Op:    StringEquals,
				Key:   "app",
				Value: kubecost.UnallocatedSuffix,
			},

			expected: false,
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
				Op:    StringEquals,
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
				Op:    StringEquals,
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
				Op:    StringEquals,
				Key:   "app",
				Value: "foo",
			},

			expected: false,
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
				Op:    StringEquals,
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
				Op:    StringSliceContains,
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
				Op:    StringSliceContains,
				Value: "serv3",
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
				Op:    StringSliceContains,
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
				Op:    StringSliceContains,
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

func Test_NotAllocation_Matches(t *testing.T) {
	cases := []struct {
		name   string
		a      *kubecost.Allocation
		filter Filter[*kubecost.Allocation]

		expected bool
	}{
		{
			name: "Namespace NotEquals -> false",
			a: &kubecost.Allocation{
				Properties: &kubecost.AllocationProperties{
					Namespace: "kube-system",
				},
			},
			filter: Not[*kubecost.Allocation]{
				Filter: AllocationCondition{
					Field: FilterNamespace,
					Op:    StringEquals,
					Value: "kube-system",
				},
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
			filter: Not[*kubecost.Allocation]{
				Filter: AllocationCondition{
					Field: FilterNamespace,
					Op:    StringEquals,
					Value: kubecost.UnallocatedSuffix,
				},
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
			filter: Not[*kubecost.Allocation]{
				Filter: AllocationCondition{
					Field: FilterNamespace,
					Op:    StringEquals,
					Value: kubecost.UnallocatedSuffix,
				},
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
			filter: Not[*kubecost.Allocation]{
				Filter: AllocationCondition{
					Field: FilterLabel,
					Op:    StringEquals,
					Key:   "app",
					Value: kubecost.UnallocatedSuffix,
				},
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
			filter: Not[*kubecost.Allocation]{
				Filter: AllocationCondition{
					Field: FilterLabel,
					Op:    StringEquals,
					Key:   "app",
					Value: kubecost.UnallocatedSuffix,
				},
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
			filter: Not[*kubecost.Allocation]{
				Filter: AllocationCondition{
					Field: FilterLabel,
					Op:    StringEquals,
					Key:   "app",
					Value: "foo",
				},
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
				Op:    StringEquals,
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
				Op:    StringEquals,
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
				Op:    StringEquals,
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
			filter: Not[*kubecost.Allocation]{
				Filter: AllocationCondition{
					Field: FilterAnnotation,
					Op:    StringEquals,
					Key:   "app",
					Value: "foo",
				},
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
				Op:    StringEquals,
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
				Op:    StringSliceContains,
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
				Op:    StringSliceContains,
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
			filter: Not[*kubecost.Allocation]{
				Filter: AllocationCondition{
					Field: FilterServices,
					Op:    StringSliceContains,
					Value: "serv3",
				},
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
			filter: Not[*kubecost.Allocation]{
				Filter: AllocationCondition{
					Field: FilterServices,
					Op:    StringSliceContains,
					Value: "serv2",
				},
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
			filter: Not[*kubecost.Allocation]{
				Filter: AllocationCondition{
					Field: FilterServices,
					Op:    StringSliceContains,
					Value: kubecost.UnallocatedSuffix,
				},
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
			filter: Not[*kubecost.Allocation]{
				Filter: AllocationCondition{
					Field: FilterServices,
					Op:    StringSliceContains,
					Value: kubecost.UnallocatedSuffix,
				},
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
				Op:    StringContainsPrefix,
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
				Op:    StringContainsPrefix,
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
				Op:    StringSliceContains,
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
				Op:    StringSliceContains,
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
		result := AllCut[*kubecost.Allocation]{}.Matches(c.a)

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
					Op:    StringEquals,
					Key:   "app",
					Value: "foo",
				},
				AllocationCondition{
					Field: FilterNamespace,
					Op:    StringEquals,
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
					Op:    StringEquals,
					Key:   "app",
					Value: "foo",
				},
				AllocationCondition{
					Field: FilterNamespace,
					Op:    StringEquals,
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
					Op:    StringEquals,
					Key:   "app",
					Value: "foo",
				},
				AllocationCondition{
					Field: FilterNamespace,
					Op:    StringEquals,
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
					Op:    StringEquals,
					Key:   "app",
					Value: "foo",
				},
				AllocationCondition{
					Field: FilterNamespace,
					Op:    StringEquals,
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
				AllCut[*kubecost.Allocation]{},
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
					Op:    StringEquals,
					Key:   "app",
					Value: "foo",
				},
				AllocationCondition{
					Field: FilterNamespace,
					Op:    StringEquals,
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
					Op:    StringEquals,
					Key:   "app",
					Value: "foo",
				},
				AllocationCondition{
					Field: FilterNamespace,
					Op:    StringEquals,
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
					Op:    StringEquals,
					Key:   "app",
					Value: "foo",
				},
				AllocationCondition{
					Field: FilterNamespace,
					Op:    StringEquals,
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
					Op:    StringEquals,
					Key:   "app",
					Value: "foo",
				},
				AllocationCondition{
					Field: FilterNamespace,
					Op:    StringEquals,
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
				Op:    StringEquals,
			},
			expected: AllocationCondition{
				Field: FilterNamespace,
				Op:    StringEquals,
			},
		},
		{
			name:     "empty And[*kubecost.Allocation] (nil)",
			input:    And[*kubecost.Allocation]{},
			expected: AllPass[*kubecost.Allocation]{},
		},
		{
			name:     "empty And[*kubecost.Allocation] (len 0)",
			input:    And[*kubecost.Allocation]{Filters: []Filter[*kubecost.Allocation]{}},
			expected: AllPass[*kubecost.Allocation]{},
		},
		{
			name:     "empty Or[*kubecost.Allocation] (nil)",
			input:    Or[*kubecost.Allocation]{},
			expected: AllPass[*kubecost.Allocation]{},
		},
		{
			name:     "empty Or[*kubecost.Allocation] (len 0)",
			input:    Or[*kubecost.Allocation]{Filters: []Filter[*kubecost.Allocation]{}},
			expected: AllPass[*kubecost.Allocation]{},
		},
		{
			name: "single-element And[*kubecost.Allocation]",
			input: And[*kubecost.Allocation]{Filters: []Filter[*kubecost.Allocation]{
				AllocationCondition{
					Field: FilterNamespace,
					Op:    StringEquals,
				},
			}},

			expected: AllocationCondition{
				Field: FilterNamespace,
				Op:    StringEquals,
			},
		},
		{
			name: "single-element Or[*kubecost.Allocation]",
			input: Or[*kubecost.Allocation]{Filters: []Filter[*kubecost.Allocation]{
				AllocationCondition{
					Field: FilterNamespace,
					Op:    StringEquals,
				},
			}},

			expected: AllocationCondition{
				Field: FilterNamespace,
				Op:    StringEquals,
			},
		},
		{
			name: "multi-element And[*kubecost.Allocation]",
			input: And[*kubecost.Allocation]{Filters: []Filter[*kubecost.Allocation]{
				AllocationCondition{
					Field: FilterNamespace,
					Op:    StringEquals,
				},
				Not[*kubecost.Allocation]{
					Filter: AllocationCondition{
						Field: FilterClusterID,
						Op:    StringEquals,
					},
				},
				AllocationCondition{
					Field: FilterServices,
					Op:    StringSliceContains,
				},
			}},

			expected: And[*kubecost.Allocation]{Filters: []Filter[*kubecost.Allocation]{
				Not[*kubecost.Allocation]{
					Filter: AllocationCondition{
						Field: FilterClusterID,
						Op:    StringEquals,
					},
				},
				AllocationCondition{
					Field: FilterNamespace,
					Op:    StringEquals,
				},
				AllocationCondition{
					Field: FilterServices,
					Op:    StringSliceContains,
				},
			}},
		},
		{
			name: "multi-element Or[*kubecost.Allocation]",
			input: Or[*kubecost.Allocation]{Filters: []Filter[*kubecost.Allocation]{
				AllocationCondition{
					Field: FilterNamespace,
					Op:    StringEquals,
				},
				Not[*kubecost.Allocation]{
					Filter: AllocationCondition{
						Field: FilterClusterID,
						Op:    StringEquals,
					},
				},
				AllocationCondition{
					Field: FilterServices,
					Op:    StringSliceContains,
				},
			}},

			expected: Or[*kubecost.Allocation]{Filters: []Filter[*kubecost.Allocation]{
				Not[*kubecost.Allocation]{
					Filter: AllocationCondition{
						Field: FilterClusterID,
						Op:    StringEquals,
					},
				},
				AllocationCondition{
					Field: FilterNamespace,
					Op:    StringEquals,
				},
				AllocationCondition{
					Field: FilterServices,
					Op:    StringSliceContains,
				},
			}},
		},
		{
			name:     "AllCut[*kubecost.Allocation]",
			input:    AllCut[*kubecost.Allocation]{},
			expected: AllCut[*kubecost.Allocation]{},
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
	cases := map[string]struct {
		left     Filter[*kubecost.Allocation]
		right    Filter[*kubecost.Allocation]
		expected bool
	}{
		// AC
		"AC1": {
			left:     AllocationCondition{},
			right:    AllocationCondition{},
			expected: true,
		},
		"AC2": {
			left: AllocationCondition{
				Field: FilterNamespace,
				Op:    StringStartsWith,
				Value: "kubecost-abc",
			},
			right: AllocationCondition{
				Field: FilterNamespace,
				Op:    StringStartsWith,
				Value: "kubecost-abc",
			},
			expected: true,
		},
		"AC3": {
			left: AllocationCondition{
				Field: FilterLabel,
				Op:    StringEquals,
				Key:   "app",
				Value: "kubecost-abc",
			},
			right: AllocationCondition{
				Field: FilterLabel,
				Op:    StringEquals,
				Key:   "app",
				Value: "kubecost-abc",
			},
			expected: true,
		},
		"AC4": {
			left: AllocationCondition{
				Field: FilterLabel,
				Op:    StringEquals,
				Key:   "app",
				Value: "kubecost-abc",
			},
			right: AllocationCondition{
				Field: FilterLabel,
				Op:    StringEquals,
				Value: "kubecost-abc",
			},
			expected: false,
		},
		"AC5": {
			left: AllocationCondition{
				Field: FilterLabel,
				Op:    StringEquals,
				Value: "kubecost-abc",
			},
			right: AllocationCondition{
				Field: FilterLabel,
				Op:    StringEquals,
				Key:   "app",
				Value: "kubecost-abc",
			},
			expected: false,
		},
		"AC6": {
			left: AllocationCondition{
				Field: FilterNamespace,
				Op:    StringStartsWith,
				Value: "kubecost-abc",
			},
			right: AllocationCondition{
				Field: FilterNamespace,
				Op:    StringStartsWith,
				Value: "kubecost-abcd",
			},
			expected: false,
		},
		// OR
		// EMPTY
		"OrEmpty1": {
			left:     Or[*kubecost.Allocation]{},
			right:    AllPass[*kubecost.Allocation]{},
			expected: true,
		},
		"OrEmpty2": {
			left:     Or[*kubecost.Allocation]{Filters: []Filter[*kubecost.Allocation]{}},
			right:    AllPass[*kubecost.Allocation]{},
			expected: true,
		},

		"OrEmpty3": {
			left:     Or[*kubecost.Allocation]{},
			right:    Or[*kubecost.Allocation]{},
			expected: true,
		},
		"OrEmpty4": {
			left:     Or[*kubecost.Allocation]{},
			right:    Or[*kubecost.Allocation]{Filters: []Filter[*kubecost.Allocation]{}},
			expected: true,
		},

		"OrEmpty5": {
			left:     Or[*kubecost.Allocation]{Filters: []Filter[*kubecost.Allocation]{}},
			right:    Or[*kubecost.Allocation]{},
			expected: true,
		},
		"OrEmpty6": {
			left:     Or[*kubecost.Allocation]{Filters: []Filter[*kubecost.Allocation]{}},
			right:    Or[*kubecost.Allocation]{Filters: []Filter[*kubecost.Allocation]{}},
			expected: true,
		},
		// FILLED
		"OrFilled1": {
			left: Or[*kubecost.Allocation]{Filters: []Filter[*kubecost.Allocation]{
				AllCut[*kubecost.Allocation]{},
				AllocationCondition{
					Field: FilterLabel,
					Op:    StringStartsWith,
					Key:   "xyz",
					Value: "kubecost",
				},
				And[*kubecost.Allocation]{
					Filters: []Filter[*kubecost.Allocation]{
						AllocationCondition{
							Field: FilterNamespace,
							Op:    StringEquals,
							Value: "ns1",
						},
						AllocationCondition{
							Field: FilterNamespace,
							Op:    StringEquals,
							Value: "ns2",
						},
					},
				},
			}},
			right: Or[*kubecost.Allocation]{Filters: []Filter[*kubecost.Allocation]{
				AllCut[*kubecost.Allocation]{},
				AllocationCondition{
					Field: FilterLabel,
					Op:    StringStartsWith,
					Key:   "xyz",
					Value: "kubecost",
				},
				And[*kubecost.Allocation]{
					Filters: []Filter[*kubecost.Allocation]{
						AllocationCondition{
							Field: FilterNamespace,
							Op:    StringEquals,
							Value: "ns1",
						},
						AllocationCondition{
							Field: FilterNamespace,
							Op:    StringEquals,
							Value: "ns2",
						},
					},
				},
			}},
			expected: true,
		},
		"OrFilled2": {
			left: Or[*kubecost.Allocation]{Filters: []Filter[*kubecost.Allocation]{
				AllCut[*kubecost.Allocation]{},
				AllocationCondition{
					Field: FilterLabel,
					Op:    StringStartsWith,
					Key:   "xyz",
					Value: "kubecost",
				},
				And[*kubecost.Allocation]{
					Filters: []Filter[*kubecost.Allocation]{
						AllocationCondition{
							Field: FilterNamespace,
							Op:    StringEquals,
							Value: "ns1",
						},
						AllocationCondition{
							Field: FilterNamespace,
							Op:    StringEquals,
							Value: "ns2",
						},
					},
				},
			}},
			right: Or[*kubecost.Allocation]{Filters: []Filter[*kubecost.Allocation]{
				AllocationCondition{
					Field: FilterLabel,
					Op:    StringStartsWith,
					Key:   "xyz",
					Value: "kubecost",
				},
				AllCut[*kubecost.Allocation]{},
				And[*kubecost.Allocation]{
					Filters: []Filter[*kubecost.Allocation]{
						AllocationCondition{
							Field: FilterNamespace,
							Op:    StringEquals,
							Value: "ns2",
						},
						AllocationCondition{
							Field: FilterNamespace,
							Op:    StringEquals,
							Value: "ns1",
						},
					},
				},
			}},
			expected: true,
		},
		"OrFilled3": {
			left: Or[*kubecost.Allocation]{Filters: []Filter[*kubecost.Allocation]{
				AllCut[*kubecost.Allocation]{},
				AllocationCondition{
					Field: FilterLabel,
					Op:    StringStartsWith,
					Key:   "xyz",
					Value: "kubecost",
				},
				And[*kubecost.Allocation]{
					Filters: []Filter[*kubecost.Allocation]{
						AllocationCondition{
							Field: FilterNamespace,
							Op:    StringEquals,
							Value: "ns1",
						},
						AllocationCondition{
							Field: FilterNamespace,
							Op:    StringEquals,
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
							Op:    StringEquals,
							Value: "ns1",
						},
						AllocationCondition{
							Field: FilterNamespace,
							Op:    StringEquals,
							Value: "ns2",
						},
					},
				},
				AllCut[*kubecost.Allocation]{},
				AllocationCondition{
					Field: FilterLabel,
					Op:    StringStartsWith,
					Key:   "xyz",
					Value: "kubecost",
				},
			}},
			expected: true,
		},
		"OrFilled4": {
			left: Or[*kubecost.Allocation]{Filters: []Filter[*kubecost.Allocation]{
				AllCut[*kubecost.Allocation]{},
				AllocationCondition{
					Field: FilterLabel,
					Op:    StringStartsWith,
					Key:   "xyz",
					Value: "kubecost",
				},
				And[*kubecost.Allocation]{
					Filters: []Filter[*kubecost.Allocation]{
						AllocationCondition{
							Field: FilterNamespace,
							Op:    StringEquals,
							Value: "ns1",
						},
						AllocationCondition{
							Field: FilterNamespace,
							Op:    StringEquals,
							Value: "ns2",
						},
					},
				},
			}},
			right: Or[*kubecost.Allocation]{Filters: []Filter[*kubecost.Allocation]{
				AllCut[*kubecost.Allocation]{},
				AllocationCondition{
					Field: FilterLabel,
					Op:    StringStartsWith,
					Key:   "xyz",
					Value: "kubecost",
				},
				And[*kubecost.Allocation]{
					Filters: []Filter[*kubecost.Allocation]{
						AllocationCondition{
							Field: FilterNamespace,
							Op:    StringEquals,
							Value: "ns3",
						},
						AllocationCondition{
							Field: FilterNamespace,
							Op:    StringEquals,
							Value: "ns2",
						},
					},
				},
			}},
			expected: false,
		},
		"OrFilled5": {
			left: Or[*kubecost.Allocation]{Filters: []Filter[*kubecost.Allocation]{
				AllocationCondition{
					Field: FilterLabel,
					Op:    StringStartsWith,
					Key:   "xyz",
					Value: "kubecost",
				},
				And[*kubecost.Allocation]{
					Filters: []Filter[*kubecost.Allocation]{
						AllocationCondition{
							Field: FilterNamespace,
							Op:    StringEquals,
							Value: "ns1",
						},
						AllocationCondition{
							Field: FilterNamespace,
							Op:    StringEquals,
							Value: "ns2",
						},
					},
				},
			}},
			right: Or[*kubecost.Allocation]{Filters: []Filter[*kubecost.Allocation]{
				AllocationCondition{
					Field: FilterLabel,
					Op:    StringStartsWith,
					Key:   "xyz",
					Value: "kubecost",
				},
				And[*kubecost.Allocation]{
					Filters: []Filter[*kubecost.Allocation]{
						AllocationCondition{
							Field: FilterNamespace,
							Op:    StringEquals,
							Value: "ns3",
						},
						AllocationCondition{
							Field: FilterNamespace,
							Op:    StringEquals,
							Value: "ns2",
						},
					},
				},
			}},
			expected: false,
		},
		"OrFilled6": {
			left: Or[*kubecost.Allocation]{Filters: []Filter[*kubecost.Allocation]{
				AllCut[*kubecost.Allocation]{},
				AllocationCondition{
					Field: FilterLabel,
					Op:    StringStartsWith,
					Key:   "xyz",
					Value: "kubecost",
				},
				And[*kubecost.Allocation]{
					Filters: []Filter[*kubecost.Allocation]{
						AllocationCondition{
							Field: FilterNamespace,
							Op:    StringEquals,
							Value: "ns1",
						},
						AllocationCondition{
							Field: FilterNamespace,
							Op:    StringEquals,
							Value: "ns2",
						},
					},
				},
			}},
			right: Or[*kubecost.Allocation]{Filters: []Filter[*kubecost.Allocation]{
				AllocationCondition{
					Field: FilterLabel,
					Op:    StringStartsWith,
					Key:   "xyz",
					Value: "kubecost",
				},
				And[*kubecost.Allocation]{
					Filters: []Filter[*kubecost.Allocation]{
						AllocationCondition{
							Field: FilterNamespace,
							Op:    StringEquals,
							Value: "ns1",
						},
						AllocationCondition{
							Field: FilterNamespace,
							Op:    StringEquals,
							Value: "ns2",
						},
					},
				},
			}},
			expected: true,
		},
		// AND
		// EMPTY
		"AndEmpty1": {
			left:     And[*kubecost.Allocation]{},
			right:    AllPass[*kubecost.Allocation]{},
			expected: true,
		},
		"AndEmpty2": {
			left:     And[*kubecost.Allocation]{Filters: []Filter[*kubecost.Allocation]{}},
			right:    AllPass[*kubecost.Allocation]{},
			expected: true,
		},

		"AndEmpty3": {
			left:     And[*kubecost.Allocation]{},
			right:    And[*kubecost.Allocation]{},
			expected: true,
		},
		"AndEmpty4": {
			left:     And[*kubecost.Allocation]{},
			right:    And[*kubecost.Allocation]{Filters: []Filter[*kubecost.Allocation]{}},
			expected: true,
		},

		"AndEmpty5": {
			left:     And[*kubecost.Allocation]{Filters: []Filter[*kubecost.Allocation]{}},
			right:    And[*kubecost.Allocation]{},
			expected: true,
		},
		"AndEmpty6": {
			left:     And[*kubecost.Allocation]{Filters: []Filter[*kubecost.Allocation]{}},
			right:    And[*kubecost.Allocation]{Filters: []Filter[*kubecost.Allocation]{}},
			expected: true,
		},
		// FILLED
		"AndFilled1": {
			left: And[*kubecost.Allocation]{Filters: []Filter[*kubecost.Allocation]{
				AllCut[*kubecost.Allocation]{},
				AllocationCondition{
					Field: FilterLabel,
					Op:    StringStartsWith,
					Key:   "xyz",
					Value: "kubecost",
				},
				Or[*kubecost.Allocation]{
					Filters: []Filter[*kubecost.Allocation]{
						AllocationCondition{
							Field: FilterNamespace,
							Op:    StringEquals,
							Value: "ns1",
						},
						AllocationCondition{
							Field: FilterNamespace,
							Op:    StringEquals,
							Value: "ns2",
						},
					},
				},
			}},
			right: And[*kubecost.Allocation]{Filters: []Filter[*kubecost.Allocation]{
				AllCut[*kubecost.Allocation]{},
				AllocationCondition{
					Field: FilterLabel,
					Op:    StringStartsWith,
					Key:   "xyz",
					Value: "kubecost",
				},
				Or[*kubecost.Allocation]{
					Filters: []Filter[*kubecost.Allocation]{
						AllocationCondition{
							Field: FilterNamespace,
							Op:    StringEquals,
							Value: "ns1",
						},
						AllocationCondition{
							Field: FilterNamespace,
							Op:    StringEquals,
							Value: "ns2",
						},
					},
				},
			}},
			expected: true,
		},
		"AndFilled2": {
			left: And[*kubecost.Allocation]{Filters: []Filter[*kubecost.Allocation]{
				AllCut[*kubecost.Allocation]{},
				AllocationCondition{
					Field: FilterLabel,
					Op:    StringStartsWith,
					Key:   "xyz",
					Value: "kubecost",
				},
				Or[*kubecost.Allocation]{
					Filters: []Filter[*kubecost.Allocation]{
						AllocationCondition{
							Field: FilterNamespace,
							Op:    StringEquals,
							Value: "ns1",
						},
						AllocationCondition{
							Field: FilterNamespace,
							Op:    StringEquals,
							Value: "ns2",
						},
					},
				},
			}},
			right: And[*kubecost.Allocation]{Filters: []Filter[*kubecost.Allocation]{
				AllocationCondition{
					Field: FilterLabel,
					Op:    StringStartsWith,
					Key:   "xyz",
					Value: "kubecost",
				},
				AllCut[*kubecost.Allocation]{},
				Or[*kubecost.Allocation]{
					Filters: []Filter[*kubecost.Allocation]{
						AllocationCondition{
							Field: FilterNamespace,
							Op:    StringEquals,
							Value: "ns2",
						},
						AllocationCondition{
							Field: FilterNamespace,
							Op:    StringEquals,
							Value: "ns1",
						},
					},
				},
			}},
			expected: true,
		},
		"AndFilled3": {
			left: And[*kubecost.Allocation]{Filters: []Filter[*kubecost.Allocation]{
				AllCut[*kubecost.Allocation]{},
				AllocationCondition{
					Field: FilterLabel,
					Op:    StringStartsWith,
					Key:   "xyz",
					Value: "kubecost",
				},
				Or[*kubecost.Allocation]{
					Filters: []Filter[*kubecost.Allocation]{
						AllocationCondition{
							Field: FilterNamespace,
							Op:    StringEquals,
							Value: "ns1",
						},
						AllocationCondition{
							Field: FilterNamespace,
							Op:    StringEquals,
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
							Op:    StringEquals,
							Value: "ns1",
						},
						AllocationCondition{
							Field: FilterNamespace,
							Op:    StringEquals,
							Value: "ns2",
						},
					},
				},
				AllCut[*kubecost.Allocation]{},
				AllocationCondition{
					Field: FilterLabel,
					Op:    StringStartsWith,
					Key:   "xyz",
					Value: "kubecost",
				},
			}},
			expected: true,
		},
		"AndFilled4": {
			left: And[*kubecost.Allocation]{Filters: []Filter[*kubecost.Allocation]{
				AllCut[*kubecost.Allocation]{},
				AllocationCondition{
					Field: FilterLabel,
					Op:    StringStartsWith,
					Key:   "xyz",
					Value: "kubecost",
				},
				Or[*kubecost.Allocation]{
					Filters: []Filter[*kubecost.Allocation]{
						AllocationCondition{
							Field: FilterNamespace,
							Op:    StringEquals,
							Value: "ns1",
						},
						AllocationCondition{
							Field: FilterNamespace,
							Op:    StringEquals,
							Value: "ns2",
						},
					},
				},
			}},
			right: And[*kubecost.Allocation]{Filters: []Filter[*kubecost.Allocation]{
				AllCut[*kubecost.Allocation]{},
				AllocationCondition{
					Field: FilterLabel,
					Op:    StringStartsWith,
					Key:   "xyz",
					Value: "kubecost",
				},
				Or[*kubecost.Allocation]{
					Filters: []Filter[*kubecost.Allocation]{
						AllocationCondition{
							Field: FilterNamespace,
							Op:    StringEquals,
							Value: "ns3",
						},
						AllocationCondition{
							Field: FilterNamespace,
							Op:    StringEquals,
							Value: "ns2",
						},
					},
				},
			}},
			expected: true,
		},
		"AndFilled5": {
			left: And[*kubecost.Allocation]{Filters: []Filter[*kubecost.Allocation]{
				AllocationCondition{
					Field: FilterLabel,
					Op:    StringStartsWith,
					Key:   "xyz",
					Value: "kubecost",
				},
				Or[*kubecost.Allocation]{
					Filters: []Filter[*kubecost.Allocation]{
						AllocationCondition{
							Field: FilterNamespace,
							Op:    StringEquals,
							Value: "ns1",
						},
						AllocationCondition{
							Field: FilterNamespace,
							Op:    StringEquals,
							Value: "ns2",
						},
					},
				},
			}},
			right: And[*kubecost.Allocation]{Filters: []Filter[*kubecost.Allocation]{
				AllocationCondition{
					Field: FilterLabel,
					Op:    StringStartsWith,
					Key:   "xyz",
					Value: "kubecost",
				},
				Or[*kubecost.Allocation]{
					Filters: []Filter[*kubecost.Allocation]{
						AllocationCondition{
							Field: FilterNamespace,
							Op:    StringEquals,
							Value: "ns3",
						},
						AllocationCondition{
							Field: FilterNamespace,
							Op:    StringEquals,
							Value: "ns2",
						},
					},
				},
			}},
			expected: false,
		},
		"AndFilled6": {
			left: And[*kubecost.Allocation]{Filters: []Filter[*kubecost.Allocation]{
				AllCut[*kubecost.Allocation]{},
				AllocationCondition{
					Field: FilterLabel,
					Op:    StringStartsWith,
					Key:   "xyz",
					Value: "kubecost",
				},
				Or[*kubecost.Allocation]{
					Filters: []Filter[*kubecost.Allocation]{
						AllocationCondition{
							Field: FilterNamespace,
							Op:    StringEquals,
							Value: "ns1",
						},
						AllocationCondition{
							Field: FilterNamespace,
							Op:    StringEquals,
							Value: "ns2",
						},
					},
				},
			}},
			right: And[*kubecost.Allocation]{Filters: []Filter[*kubecost.Allocation]{
				AllocationCondition{
					Field: FilterLabel,
					Op:    StringStartsWith,
					Key:   "xyz",
					Value: "kubecost",
				},
				Or[*kubecost.Allocation]{
					Filters: []Filter[*kubecost.Allocation]{
						AllocationCondition{
							Field: FilterNamespace,
							Op:    StringEquals,
							Value: "ns1",
						},
						AllocationCondition{
							Field: FilterNamespace,
							Op:    StringEquals,
							Value: "ns2",
						},
					},
				},
			}},
			expected: false,
		},
	}

	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			if Equals(c.left, c.right) != c.expected {
				t.Fatalf("'%s' = '%s' \nExpected: %t", c.left, c.right, c.expected)
			}
		})
	}
}
