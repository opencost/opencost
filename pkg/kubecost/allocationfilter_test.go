package kubecost

import (
	"reflect"
	"testing"
)

func Test_AllocationFilterCondition_Matches(t *testing.T) {
	cases := []struct {
		name   string
		a      *Allocation
		filter AllocationFilter

		expected bool
	}{
		{
			name: "ClusterID Equals -> true",
			a: &Allocation{
				Properties: &AllocationProperties{
					Cluster: "cluster-one",
				},
			},
			filter: AllocationFilterCondition{
				Field: FilterClusterID,
				Op:    FilterEquals,
				Value: "cluster-one",
			},

			expected: true,
		},
		{
			name: "ClusterID StartsWith -> true",
			a: &Allocation{
				Properties: &AllocationProperties{
					Cluster: "cluster-one",
				},
			},
			filter: AllocationFilterCondition{
				Field: FilterClusterID,
				Op:    FilterStartsWith,
				Value: "cluster",
			},

			expected: true,
		},
		{
			name: "ClusterID StartsWith -> false",
			a: &Allocation{
				Properties: &AllocationProperties{
					Cluster: "k8s-one",
				},
			},
			filter: AllocationFilterCondition{
				Field: FilterClusterID,
				Op:    FilterStartsWith,
				Value: "cluster",
			},

			expected: false,
		},
		{
			name: "ClusterID empty StartsWith '' -> true",
			a: &Allocation{
				Properties: &AllocationProperties{
					Cluster: "",
				},
			},
			filter: AllocationFilterCondition{
				Field: FilterClusterID,
				Op:    FilterStartsWith,
				Value: "",
			},

			expected: true,
		},
		{
			name: "ClusterID nonempty StartsWith '' -> true",
			a: &Allocation{
				Properties: &AllocationProperties{
					Cluster: "abc",
				},
			},
			filter: AllocationFilterCondition{
				Field: FilterClusterID,
				Op:    FilterStartsWith,
				Value: "",
			},

			expected: true,
		},
		{
			name: "Node Equals -> true",
			a: &Allocation{
				Properties: &AllocationProperties{
					Node: "node123",
				},
			},
			filter: AllocationFilterCondition{
				Field: FilterNode,
				Op:    FilterEquals,
				Value: "node123",
			},

			expected: true,
		},
		{
			name: "Namespace NotEquals -> false",
			a: &Allocation{
				Properties: &AllocationProperties{
					Namespace: "kube-system",
				},
			},
			filter: AllocationFilterCondition{
				Field: FilterNamespace,
				Op:    FilterNotEquals,
				Value: "kube-system",
			},

			expected: false,
		},
		{
			name: "Namespace NotEquals Unallocated -> true",
			a: &Allocation{
				Properties: &AllocationProperties{
					Namespace: "kube-system",
				},
			},
			filter: AllocationFilterCondition{
				Field: FilterNamespace,
				Op:    FilterNotEquals,
				Value: UnallocatedSuffix,
			},

			expected: true,
		},
		{
			name: "Namespace NotEquals Unallocated -> false",
			a: &Allocation{
				Properties: &AllocationProperties{
					Namespace: "",
				},
			},
			filter: AllocationFilterCondition{
				Field: FilterNamespace,
				Op:    FilterNotEquals,
				Value: UnallocatedSuffix,
			},

			expected: false,
		},
		{
			name: "Namespace Equals Unallocated -> true",
			a: &Allocation{
				Properties: &AllocationProperties{
					Namespace: "",
				},
			},
			filter: AllocationFilterCondition{
				Field: FilterNamespace,
				Op:    FilterEquals,
				Value: UnallocatedSuffix,
			},

			expected: true,
		},
		{
			name: "ControllerKind Equals -> true",
			a: &Allocation{
				Properties: &AllocationProperties{
					ControllerKind: "deployment", // We generally store controller kinds as all lowercase
				},
			},
			filter: AllocationFilterCondition{
				Field: FilterControllerKind,
				Op:    FilterEquals,
				Value: "deployment",
			},

			expected: true,
		},
		{
			name: "ControllerName Equals -> true",
			a: &Allocation{
				Properties: &AllocationProperties{
					Controller: "kc-cost-analyzer",
				},
			},
			filter: AllocationFilterCondition{
				Field: FilterControllerName,
				Op:    FilterEquals,
				Value: "kc-cost-analyzer",
			},

			expected: true,
		},
		{
			name: "Pod (with UID) Equals -> true",
			a: &Allocation{
				Properties: &AllocationProperties{
					Pod: "pod-123 UID-ABC",
				},
			},
			filter: AllocationFilterCondition{
				Field: FilterPod,
				Op:    FilterEquals,
				Value: "pod-123 UID-ABC",
			},

			expected: true,
		},
		{
			name: "Container Equals -> true",
			a: &Allocation{
				Properties: &AllocationProperties{
					Container: "cost-model",
				},
			},
			filter: AllocationFilterCondition{
				Field: FilterContainer,
				Op:    FilterEquals,
				Value: "cost-model",
			},

			expected: true,
		},
		{
			name: `label[app]="foo" -> true`,
			a: &Allocation{
				Properties: &AllocationProperties{
					Labels: map[string]string{
						"app": "foo",
					},
				},
			},
			filter: AllocationFilterCondition{
				Field: FilterLabel,
				Op:    FilterEquals,
				Key:   "app",
				Value: "foo",
			},

			expected: true,
		},
		{
			name: `label[app]="foo" -> different value -> false`,
			a: &Allocation{
				Properties: &AllocationProperties{
					Labels: map[string]string{
						"app": "bar",
					},
				},
			},
			filter: AllocationFilterCondition{
				Field: FilterLabel,
				Op:    FilterEquals,
				Key:   "app",
				Value: "foo",
			},

			expected: false,
		},
		{
			name: `label[app]="foo" -> label missing -> false`,
			a: &Allocation{
				Properties: &AllocationProperties{
					Labels: map[string]string{
						"someotherlabel": "someothervalue",
					},
				},
			},
			filter: AllocationFilterCondition{
				Field: FilterLabel,
				Op:    FilterEquals,
				Key:   "app",
				Value: "foo",
			},

			expected: false,
		},
		{
			name: `label[app]=Unallocated -> label missing -> true`,
			a: &Allocation{
				Properties: &AllocationProperties{
					Labels: map[string]string{
						"someotherlabel": "someothervalue",
					},
				},
			},
			filter: AllocationFilterCondition{
				Field: FilterLabel,
				Op:    FilterEquals,
				Key:   "app",
				Value: UnallocatedSuffix,
			},

			expected: true,
		},
		{
			name: `label[app]=Unallocated -> label present -> false`,
			a: &Allocation{
				Properties: &AllocationProperties{
					Labels: map[string]string{
						"app": "test",
					},
				},
			},
			filter: AllocationFilterCondition{
				Field: FilterLabel,
				Op:    FilterEquals,
				Key:   "app",
				Value: UnallocatedSuffix,
			},

			expected: false,
		},
		{
			name: `label[app]!=Unallocated -> label missing -> false`,
			a: &Allocation{
				Properties: &AllocationProperties{
					Labels: map[string]string{
						"someotherlabel": "someothervalue",
					},
				},
			},
			filter: AllocationFilterCondition{
				Field: FilterLabel,
				Op:    FilterNotEquals,
				Key:   "app",
				Value: UnallocatedSuffix,
			},

			expected: false,
		},
		{
			name: `label[app]!=Unallocated -> label present -> true`,
			a: &Allocation{
				Properties: &AllocationProperties{
					Labels: map[string]string{
						"app": "test",
					},
				},
			},
			filter: AllocationFilterCondition{
				Field: FilterLabel,
				Op:    FilterNotEquals,
				Key:   "app",
				Value: UnallocatedSuffix,
			},

			expected: true,
		},
		{
			name: `label[app]!="foo" -> label missing -> true`,
			a: &Allocation{
				Properties: &AllocationProperties{
					Labels: map[string]string{
						"someotherlabel": "someothervalue",
					},
				},
			},
			filter: AllocationFilterCondition{
				Field: FilterLabel,
				Op:    FilterNotEquals,
				Key:   "app",
				Value: "foo",
			},

			expected: true,
		},
		{
			name: `annotation[prom_modified_name]="testing123" -> true`,
			a: &Allocation{
				Properties: &AllocationProperties{
					Annotations: map[string]string{
						"prom_modified_name": "testing123",
					},
				},
			},
			filter: AllocationFilterCondition{
				Field: FilterAnnotation,
				Op:    FilterEquals,
				Key:   "prom_modified_name",
				Value: "testing123",
			},

			expected: true,
		},
		{
			name: `annotation[app]="foo" -> different value -> false`,
			a: &Allocation{
				Properties: &AllocationProperties{
					Annotations: map[string]string{
						"app": "bar",
					},
				},
			},
			filter: AllocationFilterCondition{
				Field: FilterAnnotation,
				Op:    FilterEquals,
				Key:   "app",
				Value: "foo",
			},

			expected: false,
		},
		{
			name: `annotation[app]="foo" -> annotation missing -> false`,
			a: &Allocation{
				Properties: &AllocationProperties{
					Annotations: map[string]string{
						"someotherannotation": "someothervalue",
					},
				},
			},
			filter: AllocationFilterCondition{
				Field: FilterAnnotation,
				Op:    FilterEquals,
				Key:   "app",
				Value: "foo",
			},

			expected: false,
		},
		{
			name: `annotation[app]!="foo" -> annotation missing -> true`,
			a: &Allocation{
				Properties: &AllocationProperties{
					Annotations: map[string]string{
						"someotherannotation": "someothervalue",
					},
				},
			},
			filter: AllocationFilterCondition{
				Field: FilterAnnotation,
				Op:    FilterNotEquals,
				Key:   "app",
				Value: "foo",
			},

			expected: true,
		},
		{
			name: `namespace unallocated -> true`,
			a: &Allocation{
				Properties: &AllocationProperties{
					Namespace: "",
				},
			},
			filter: AllocationFilterCondition{
				Field: FilterNamespace,
				Op:    FilterEquals,
				Value: UnallocatedSuffix,
			},

			expected: true,
		},
		{
			name: `services contains -> true`,
			a: &Allocation{
				Properties: &AllocationProperties{
					Services: []string{"serv1", "serv2"},
				},
			},
			filter: AllocationFilterCondition{
				Field: FilterServices,
				Op:    FilterContains,
				Value: "serv2",
			},

			expected: true,
		},
		{
			name: `services contains -> false`,
			a: &Allocation{
				Properties: &AllocationProperties{
					Services: []string{"serv1", "serv2"},
				},
			},
			filter: AllocationFilterCondition{
				Field: FilterServices,
				Op:    FilterContains,
				Value: "serv3",
			},

			expected: false,
		},
		{
			name: `services notcontains -> true`,
			a: &Allocation{
				Properties: &AllocationProperties{
					Services: []string{"serv1", "serv2"},
				},
			},
			filter: AllocationFilterCondition{
				Field: FilterServices,
				Op:    FilterNotContains,
				Value: "serv3",
			},

			expected: true,
		},
		{
			name: `services notcontains -> false`,
			a: &Allocation{
				Properties: &AllocationProperties{
					Services: []string{"serv1", "serv2"},
				},
			},
			filter: AllocationFilterCondition{
				Field: FilterServices,
				Op:    FilterNotContains,
				Value: "serv2",
			},

			expected: false,
		},
		{
			name: `services notcontains unallocated -> true`,
			a: &Allocation{
				Properties: &AllocationProperties{
					Services: []string{"serv1", "serv2"},
				},
			},
			filter: AllocationFilterCondition{
				Field: FilterServices,
				Op:    FilterNotContains,
				Value: UnallocatedSuffix,
			},

			expected: true,
		},
		{
			name: `services notcontains unallocated -> false`,
			a: &Allocation{
				Properties: &AllocationProperties{
					Services: []string{},
				},
			},
			filter: AllocationFilterCondition{
				Field: FilterServices,
				Op:    FilterNotContains,
				Value: UnallocatedSuffix,
			},

			expected: false,
		},
		{
			name: `services containsprefix -> true`,
			a: &Allocation{
				Properties: &AllocationProperties{
					Services: []string{"serv1", "serv2"},
				},
			},
			filter: AllocationFilterCondition{
				Field: FilterServices,
				Op:    FilterContainsPrefix,
				Value: "serv",
			},

			expected: true,
		},
		{
			name: `services containsprefix -> false`,
			a: &Allocation{
				Properties: &AllocationProperties{
					Services: []string{"foo", "bar"},
				},
			},
			filter: AllocationFilterCondition{
				Field: FilterServices,
				Op:    FilterContainsPrefix,
				Value: "serv",
			},

			expected: false,
		},
		{
			name: `services contains unallocated -> false`,
			a: &Allocation{
				Properties: &AllocationProperties{
					Services: []string{"serv1", "serv2"},
				},
			},
			filter: AllocationFilterCondition{
				Field: FilterServices,
				Op:    FilterContains,
				Value: UnallocatedSuffix,
			},

			expected: false,
		},
		{
			name: `services contains unallocated -> true`,
			a: &Allocation{
				Properties: &AllocationProperties{
					Services: []string{},
				},
			},
			filter: AllocationFilterCondition{
				Field: FilterServices,
				Op:    FilterContains,
				Value: UnallocatedSuffix,
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

func Test_AllocationFilterNone_Matches(t *testing.T) {
	cases := []struct {
		name string
		a    *Allocation
	}{
		{
			name: "nil",
			a:    nil,
		},
		{
			name: "nil properties",
			a: &Allocation{
				Properties: nil,
			},
		},
		{
			name: "empty properties",
			a: &Allocation{
				Properties: &AllocationProperties{},
			},
		},
		{
			name: "ClusterID",
			a: &Allocation{
				Properties: &AllocationProperties{
					Cluster: "cluster-one",
				},
			},
		},
		{
			name: "Node",
			a: &Allocation{
				Properties: &AllocationProperties{
					Node: "node123",
				},
			},
		},
		{
			name: "Namespace",
			a: &Allocation{
				Properties: &AllocationProperties{
					Namespace: "kube-system",
				},
			},
		},
		{
			name: "ControllerKind",
			a: &Allocation{
				Properties: &AllocationProperties{
					ControllerKind: "deployment", // We generally store controller kinds as all lowercase
				},
			},
		},
		{
			name: "ControllerName",
			a: &Allocation{
				Properties: &AllocationProperties{
					Controller: "kc-cost-analyzer",
				},
			},
		},
		{
			name: "Pod",
			a: &Allocation{
				Properties: &AllocationProperties{
					Pod: "pod-123 UID-ABC",
				},
			},
		},
		{
			name: "Container",
			a: &Allocation{
				Properties: &AllocationProperties{
					Container: "cost-model",
				},
			},
		},
		{
			name: `label`,
			a: &Allocation{
				Properties: &AllocationProperties{
					Labels: map[string]string{
						"app": "foo",
					},
				},
			},
		},
		{
			name: `annotation`,
			a: &Allocation{
				Properties: &AllocationProperties{
					Annotations: map[string]string{
						"prom_modified_name": "testing123",
					},
				},
			},
		},
		{
			name: `services`,
			a: &Allocation{
				Properties: &AllocationProperties{
					Services: []string{"serv1", "serv2"},
				},
			},
		},
	}

	for _, c := range cases {
		result := AllocationFilterNone{}.Matches(c.a)

		if result {
			t.Errorf("%s: should have been rejected", c.name)
		}
	}
}
func Test_AllocationFilterAnd_Matches(t *testing.T) {
	cases := []struct {
		name   string
		a      *Allocation
		filter AllocationFilter

		expected bool
	}{
		{
			name: `label[app]="foo" and namespace="kubecost" -> both true`,
			a: &Allocation{
				Properties: &AllocationProperties{
					Namespace: "kubecost",
					Labels: map[string]string{
						"app": "foo",
					},
				},
			},
			filter: AllocationFilterAnd{[]AllocationFilter{
				AllocationFilterCondition{
					Field: FilterLabel,
					Op:    FilterEquals,
					Key:   "app",
					Value: "foo",
				},
				AllocationFilterCondition{
					Field: FilterNamespace,
					Op:    FilterEquals,
					Value: "kubecost",
				},
			}},
			expected: true,
		},
		{
			name: `label[app]="foo" and namespace="kubecost" -> first true`,
			a: &Allocation{
				Properties: &AllocationProperties{
					Namespace: "kube-system",
					Labels: map[string]string{
						"app": "foo",
					},
				},
			},
			filter: AllocationFilterAnd{[]AllocationFilter{
				AllocationFilterCondition{
					Field: FilterLabel,
					Op:    FilterEquals,
					Key:   "app",
					Value: "foo",
				},
				AllocationFilterCondition{
					Field: FilterNamespace,
					Op:    FilterEquals,
					Value: "kubecost",
				},
			}},
			expected: false,
		},
		{
			name: `label[app]="foo" and namespace="kubecost" -> second true`,
			a: &Allocation{
				Properties: &AllocationProperties{
					Namespace: "kubecost",
					Labels: map[string]string{
						"app": "bar",
					},
				},
			},
			filter: AllocationFilterAnd{[]AllocationFilter{
				AllocationFilterCondition{
					Field: FilterLabel,
					Op:    FilterEquals,
					Key:   "app",
					Value: "foo",
				},
				AllocationFilterCondition{
					Field: FilterNamespace,
					Op:    FilterEquals,
					Value: "kubecost",
				},
			}},
			expected: false,
		},
		{
			name: `label[app]="foo" and namespace="kubecost" -> both false`,
			a: &Allocation{
				Properties: &AllocationProperties{
					Namespace: "kube-system",
					Labels: map[string]string{
						"app": "bar",
					},
				},
			},
			filter: AllocationFilterAnd{[]AllocationFilter{
				AllocationFilterCondition{
					Field: FilterLabel,
					Op:    FilterEquals,
					Key:   "app",
					Value: "foo",
				},
				AllocationFilterCondition{
					Field: FilterNamespace,
					Op:    FilterEquals,
					Value: "kubecost",
				},
			}},
			expected: false,
		},
		{
			name: `(and none) matches nothing`,
			a: &Allocation{
				Properties: &AllocationProperties{
					Namespace: "kube-system",
					Labels: map[string]string{
						"app": "bar",
					},
				},
			},
			filter: AllocationFilterAnd{[]AllocationFilter{
				AllocationFilterNone{},
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

func Test_AllocationFilterOr_Matches(t *testing.T) {
	cases := []struct {
		name   string
		a      *Allocation
		filter AllocationFilter

		expected bool
	}{
		{
			name: `label[app]="foo" or namespace="kubecost" -> both true`,
			a: &Allocation{
				Properties: &AllocationProperties{
					Namespace: "kubecost",
					Labels: map[string]string{
						"app": "foo",
					},
				},
			},
			filter: AllocationFilterOr{[]AllocationFilter{
				AllocationFilterCondition{
					Field: FilterLabel,
					Op:    FilterEquals,
					Key:   "app",
					Value: "foo",
				},
				AllocationFilterCondition{
					Field: FilterNamespace,
					Op:    FilterEquals,
					Value: "kubecost",
				},
			}},
			expected: true,
		},
		{
			name: `label[app]="foo" or namespace="kubecost" -> first true`,
			a: &Allocation{
				Properties: &AllocationProperties{
					Namespace: "kube-system",
					Labels: map[string]string{
						"app": "foo",
					},
				},
			},
			filter: AllocationFilterOr{[]AllocationFilter{
				AllocationFilterCondition{
					Field: FilterLabel,
					Op:    FilterEquals,
					Key:   "app",
					Value: "foo",
				},
				AllocationFilterCondition{
					Field: FilterNamespace,
					Op:    FilterEquals,
					Value: "kubecost",
				},
			}},
			expected: true,
		},
		{
			name: `label[app]="foo" or namespace="kubecost" -> second true`,
			a: &Allocation{
				Properties: &AllocationProperties{
					Namespace: "kubecost",
					Labels: map[string]string{
						"app": "bar",
					},
				},
			},
			filter: AllocationFilterOr{[]AllocationFilter{
				AllocationFilterCondition{
					Field: FilterLabel,
					Op:    FilterEquals,
					Key:   "app",
					Value: "foo",
				},
				AllocationFilterCondition{
					Field: FilterNamespace,
					Op:    FilterEquals,
					Value: "kubecost",
				},
			}},
			expected: true,
		},
		{
			name: `label[app]="foo" or namespace="kubecost" -> both false`,
			a: &Allocation{
				Properties: &AllocationProperties{
					Namespace: "kube-system",
					Labels: map[string]string{
						"app": "bar",
					},
				},
			},
			filter: AllocationFilterOr{[]AllocationFilter{
				AllocationFilterCondition{
					Field: FilterLabel,
					Op:    FilterEquals,
					Key:   "app",
					Value: "foo",
				},
				AllocationFilterCondition{
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

func Test_AllocationFilter_Flattened(t *testing.T) {
	cases := []struct {
		name string

		input    AllocationFilter
		expected AllocationFilter
	}{
		{
			name: "AllocationFilterCondition",
			input: AllocationFilterCondition{
				Field: FilterNamespace,
				Op:    FilterEquals,
			},
			expected: AllocationFilterCondition{
				Field: FilterNamespace,
				Op:    FilterEquals,
			},
		},
		{
			name:     "empty AllocationFilterAnd (nil)",
			input:    AllocationFilterAnd{},
			expected: nil,
		},
		{
			name:     "empty AllocationFilterAnd (len 0)",
			input:    AllocationFilterAnd{Filters: []AllocationFilter{}},
			expected: nil,
		},
		{
			name:     "empty AllocationFilterOr (nil)",
			input:    AllocationFilterOr{},
			expected: nil,
		},
		{
			name:     "empty AllocationFilterOr (len 0)",
			input:    AllocationFilterOr{Filters: []AllocationFilter{}},
			expected: nil,
		},
		{
			name: "single-element AllocationFilterAnd",
			input: AllocationFilterAnd{Filters: []AllocationFilter{
				AllocationFilterCondition{
					Field: FilterNamespace,
					Op:    FilterEquals,
				},
			}},

			expected: AllocationFilterCondition{
				Field: FilterNamespace,
				Op:    FilterEquals,
			},
		},
		{
			name: "single-element AllocationFilterOr",
			input: AllocationFilterOr{Filters: []AllocationFilter{
				AllocationFilterCondition{
					Field: FilterNamespace,
					Op:    FilterEquals,
				},
			}},

			expected: AllocationFilterCondition{
				Field: FilterNamespace,
				Op:    FilterEquals,
			},
		},
		{
			name: "multi-element AllocationFilterAnd",
			input: AllocationFilterAnd{Filters: []AllocationFilter{
				AllocationFilterCondition{
					Field: FilterNamespace,
					Op:    FilterEquals,
				},
				AllocationFilterCondition{
					Field: FilterClusterID,
					Op:    FilterNotEquals,
				},
				AllocationFilterCondition{
					Field: FilterServices,
					Op:    FilterContains,
				},
			}},

			expected: AllocationFilterAnd{Filters: []AllocationFilter{
				AllocationFilterCondition{
					Field: FilterNamespace,
					Op:    FilterEquals,
				},
				AllocationFilterCondition{
					Field: FilterClusterID,
					Op:    FilterNotEquals,
				},
				AllocationFilterCondition{
					Field: FilterServices,
					Op:    FilterContains,
				},
			}},
		},
		{
			name: "multi-element AllocationFilterOr",
			input: AllocationFilterOr{Filters: []AllocationFilter{
				AllocationFilterCondition{
					Field: FilterNamespace,
					Op:    FilterEquals,
				},
				AllocationFilterCondition{
					Field: FilterClusterID,
					Op:    FilterNotEquals,
				},
				AllocationFilterCondition{
					Field: FilterServices,
					Op:    FilterContains,
				},
			}},

			expected: AllocationFilterOr{Filters: []AllocationFilter{
				AllocationFilterCondition{
					Field: FilterNamespace,
					Op:    FilterEquals,
				},
				AllocationFilterCondition{
					Field: FilterClusterID,
					Op:    FilterNotEquals,
				},
				AllocationFilterCondition{
					Field: FilterServices,
					Op:    FilterContains,
				},
			}},
		},
		{
			name:     "AllocationFilterNone",
			input:    AllocationFilterNone{},
			expected: AllocationFilterNone{},
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
