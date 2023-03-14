package kubecost

import (
	"testing"
)

func Test_AllocationFilterCondition_Matches(t *testing.T) {
	cases := []struct {
		name   string
		a      *Allocation
		filter AllocationMatcher

		expected bool
	}{
		{
			name: "ClusterID Equals -> true",
			a: &Allocation{
				Properties: &AllocationProperties{
					Cluster: "cluster-one",
				},
			},
			filter:   mustCompileFilter(`cluster:"cluster-one"`),
			expected: true,
		},
		{
			name: "ClusterID StartsWith -> true",
			a: &Allocation{
				Properties: &AllocationProperties{
					Cluster: "cluster-one",
				},
			},
			filter:   mustCompileFilter(`cluster<~:"cluster"`),
			expected: true,
		},
		{
			name: "ClusterID StartsWith -> false",
			a: &Allocation{
				Properties: &AllocationProperties{
					Cluster: "k8s-one",
				},
			},
			filter:   mustCompileFilter(`cluster<~:"cluster"`),
			expected: false,
		},
		{
			name: "ClusterID empty StartsWith '' -> true",
			a: &Allocation{
				Properties: &AllocationProperties{
					Cluster: "",
				},
			},
			filter:   mustCompileFilter(`cluster<~:""`),
			expected: true,
		},
		{
			name: "ClusterID nonempty StartsWith '' -> true",
			a: &Allocation{
				Properties: &AllocationProperties{
					Cluster: "abc",
				},
			},
			filter:   mustCompileFilter(`cluster<~:""`),
			expected: true,
		},
		{
			name: "Node Equals -> true",
			a: &Allocation{
				Properties: &AllocationProperties{
					Node: "node123",
				},
			},
			filter:   mustCompileFilter(`node:"node123"`),
			expected: true,
		},
		{
			name: "Namespace NotEquals -> false",
			a: &Allocation{
				Properties: &AllocationProperties{
					Namespace: "kube-system",
				},
			},
			filter:   mustCompileFilter(`namespace!:"kube-system"`),
			expected: false,
		},
		{
			name: "Namespace NotEquals Unallocated -> true",
			a: &Allocation{
				Properties: &AllocationProperties{
					Namespace: "kube-system",
				},
			},
			filter:   mustCompileFilter(`namespace!:"__unallocated__"`),
			expected: true,
		},
		{
			name: "Namespace NotEquals Unallocated -> false",
			a: &Allocation{
				Properties: &AllocationProperties{
					Namespace: "",
				},
			},
			filter:   mustCompileFilter(`namespace!:"__unallocated__"`),
			expected: false,
		},
		{
			name: "Namespace Equals Unallocated -> true",
			a: &Allocation{
				Properties: &AllocationProperties{
					Namespace: "",
				},
			},
			filter:   mustCompileFilter(`namespace:"__unallocated__"`),
			expected: true,
		},
		{
			name: "ControllerKind Equals -> true",
			a: &Allocation{
				Properties: &AllocationProperties{
					ControllerKind: "deployment", // We generally store controller kinds as all lowercase
				},
			},
			filter:   mustCompileFilter(`controllerKind:"deployment"`),
			expected: true,
		},
		{
			name: "ControllerName Equals -> true",
			a: &Allocation{
				Properties: &AllocationProperties{
					Controller: "kc-cost-analyzer",
				},
			},
			filter:   mustCompileFilter(`controllerName:"kc-cost-analyzer"`),
			expected: true,
		},
		{
			name: "Pod (with UID) Equals -> true",
			a: &Allocation{
				Properties: &AllocationProperties{
					Pod: "pod-123 UID-ABC",
				},
			},
			filter:   mustCompileFilter(`pod:"pod-123 UID-ABC"`),
			expected: true,
		},
		{
			name: "Container Equals -> true",
			a: &Allocation{
				Properties: &AllocationProperties{
					Container: "cost-model",
				},
			},
			filter:   mustCompileFilter(`container:"cost-model"`),
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
			filter:   mustCompileFilter(`label[app]:"foo"`),
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
			filter:   mustCompileFilter(`label[app]:"foo"`),
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
			filter:   mustCompileFilter(`label[app]:"foo"`),
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
			filter:   mustCompileFilter(`label!~:"app"`),
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
			filter:   mustCompileFilter(`label!~:"app"`),
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
			filter:   mustCompileFilter(`label~:"app"`),
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
			filter:   mustCompileFilter(`label~:"app"`),
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
			filter:   mustCompileFilter(`label[app]!:"foo"`),
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
			filter:   mustCompileFilter(`annotation[prom_modified_name]:"testing123"`),
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
			filter:   mustCompileFilter(`annotation[app]:"foo"`),
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
			filter:   mustCompileFilter(`annotation[app]:"foo"`),
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
			filter:   mustCompileFilter(`annotation[app]!:"foo"`),
			expected: true,
		},
		{
			name: `namespace unallocated -> true`,
			a: &Allocation{
				Properties: &AllocationProperties{
					Namespace: "",
				},
			},
			filter:   mustCompileFilter(`namespace:"__unallocated__"`),
			expected: true,
		},
		{
			name: `services contains -> true`,
			a: &Allocation{
				Properties: &AllocationProperties{
					Services: []string{"serv1", "serv2"},
				},
			},
			filter:   mustCompileFilter(`services~:"serv2"`),
			expected: true,
		},
		{
			name: `services contains -> false`,
			a: &Allocation{
				Properties: &AllocationProperties{
					Services: []string{"serv1", "serv2"},
				},
			},
			filter:   mustCompileFilter(`services~:"serv3"`),
			expected: false,
		},
		{
			name: `services notcontains -> true`,
			a: &Allocation{
				Properties: &AllocationProperties{
					Services: []string{"serv1", "serv2"},
				},
			},
			filter:   mustCompileFilter(`services!~:"serv3"`),
			expected: true,
		},
		{
			name: `services notcontains -> false`,
			a: &Allocation{
				Properties: &AllocationProperties{
					Services: []string{"serv1", "serv2"},
				},
			},
			filter:   mustCompileFilter(`services!~:"serv2"`),
			expected: false,
		},
		{
			name: `services notcontains unallocated -> true`,
			a: &Allocation{
				Properties: &AllocationProperties{
					Services: []string{"serv1", "serv2"},
				},
			},
			filter:   mustCompileFilter(`services!~:"__unallocated__"`),
			expected: true,
		},
		{
			name: `services notcontains unallocated -> false`,
			a: &Allocation{
				Properties: &AllocationProperties{
					Services: []string{},
				},
			},
			filter:   mustCompileFilter(`services!~:"__unallocated__"`),
			expected: false,
		},
		{
			name: `services containsprefix -> true`,
			a: &Allocation{
				Properties: &AllocationProperties{
					Services: []string{"serv1", "serv2"},
				},
			},
			filter:   mustCompileFilter(`services<~:"serv"`),
			expected: true,
		},
		{
			name: `services containsprefix -> false`,
			a: &Allocation{
				Properties: &AllocationProperties{
					Services: []string{"foo", "bar"},
				},
			},
			filter:   mustCompileFilter(`services<~:"serv"`),
			expected: false,
		},
		{
			name: `services contains unallocated -> false`,
			a: &Allocation{
				Properties: &AllocationProperties{
					Services: []string{"serv1", "serv2"},
				},
			},
			filter:   mustCompileFilter(`services~:"__unallocated__"`),
			expected: false,
		},
		{
			name: `services contains unallocated -> true`,
			a: &Allocation{
				Properties: &AllocationProperties{
					Services: []string{},
				},
			},
			filter:   mustCompileFilter(`services~:"__unallocated__"`),
			expected: true,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			result := c.filter.Matches(c.a)

			if result != c.expected {
				t.Errorf("%s: expected %t, got %t", c.name, c.expected, result)
			}
		})
	}
}

/*
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
*/

func Test_AllocationFilterAnd_Matches(t *testing.T) {
	cases := []struct {
		name   string
		a      *Allocation
		filter AllocationMatcher

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
			filter:   mustCompileFilter(`label[app]:"foo" + namespace:"kubecost"`),
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
			filter:   mustCompileFilter(`label[app]:"foo" + namespace:"kubecost"`),
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
			filter:   mustCompileFilter(`label[app]:"foo" + namespace:"kubecost"`),
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
			filter:   mustCompileFilter(`label[app]:"foo" + namespace:"kubecost"`),
			expected: false,
		},
		/*
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
		*/
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			result := c.filter.Matches(c.a)

			if result != c.expected {
				t.Errorf("%s: expected %t, got %t", c.name, c.expected, result)
			}
		})
	}
}

func Test_AllocationFilterOr_Matches(t *testing.T) {
	cases := []struct {
		name   string
		a      *Allocation
		filter AllocationMatcher

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
			filter:   mustCompileFilter(`label[app]:"foo" | namespace:"kubecost"`),
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
			filter:   mustCompileFilter(`label[app]:"foo" | namespace:"kubecost"`),
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
			filter:   mustCompileFilter(`label[app]:"foo" | namespace:"kubecost"`),
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
			filter:   mustCompileFilter(`label[app]:"foo" | namespace:"kubecost"`),
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

/*
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

func Test_AllocationFilter_Equals(t *testing.T) {
	cases := []struct {
		left     AllocationFilter
		right    AllocationFilter
		expected bool
	}{
		// AFC
		{
			left:     AllocationFilterCondition{},
			right:    AllocationFilterCondition{},
			expected: true,
		},
		{
			left: AllocationFilterCondition{
				Field: FilterNamespace,
				Op:    FilterStartsWith,
				Value: "kubecost-abc",
			},
			right: AllocationFilterCondition{
				Field: FilterNamespace,
				Op:    FilterStartsWith,
				Value: "kubecost-abc",
			},
			expected: true,
		},
		{
			left: AllocationFilterCondition{
				Field: FilterLabel,
				Op:    FilterEquals,
				Key:   "app",
				Value: "kubecost-abc",
			},
			right: AllocationFilterCondition{
				Field: FilterLabel,
				Op:    FilterEquals,
				Key:   "app",
				Value: "kubecost-abc",
			},
			expected: true,
		},
		{
			left: AllocationFilterCondition{
				Field: FilterLabel,
				Op:    FilterEquals,
				Key:   "app",
				Value: "kubecost-abc",
			},
			right: AllocationFilterCondition{
				Field: FilterLabel,
				Op:    FilterEquals,
				Value: "kubecost-abc",
			},
			expected: false,
		},
		{
			left: AllocationFilterCondition{
				Field: FilterLabel,
				Op:    FilterEquals,
				Value: "kubecost-abc",
			},
			right: AllocationFilterCondition{
				Field: FilterLabel,
				Op:    FilterEquals,
				Key:   "app",
				Value: "kubecost-abc",
			},
			expected: false,
		},
		{
			left: AllocationFilterCondition{
				Field: FilterNamespace,
				Op:    FilterStartsWith,
				Value: "kubecost-abc",
			},
			right: AllocationFilterCondition{
				Field: FilterNamespace,
				Op:    FilterStartsWith,
				Value: "kubecost-abcd",
			},
			expected: false,
		},
		// OR
		// EMPTY
		{
			left:     AllocationFilterOr{},
			right:    nil,
			expected: false,
		},
		{
			left:     AllocationFilterOr{Filters: []AllocationFilter{}},
			right:    nil,
			expected: false,
		},

		{
			left:     AllocationFilterOr{},
			right:    AllocationFilterOr{},
			expected: true,
		},
		{
			left:     AllocationFilterOr{},
			right:    AllocationFilterOr{Filters: []AllocationFilter{}},
			expected: true,
		},

		{
			left:     AllocationFilterOr{Filters: []AllocationFilter{}},
			right:    AllocationFilterOr{},
			expected: true,
		},
		{
			left:     AllocationFilterOr{Filters: []AllocationFilter{}},
			right:    AllocationFilterOr{Filters: []AllocationFilter{}},
			expected: true,
		},
		// FILLED
		{
			left: AllocationFilterOr{Filters: []AllocationFilter{
				AllocationFilterNone{},
				AllocationFilterCondition{
					Field: FilterLabel,
					Op:    FilterStartsWith,
					Key:   "xyz",
					Value: "kubecost",
				},
				AllocationFilterAnd{
					Filters: []AllocationFilter{
						AllocationFilterCondition{
							Field: FilterNamespace,
							Op:    FilterEquals,
							Value: "ns1",
						},
						AllocationFilterCondition{
							Field: FilterNamespace,
							Op:    FilterEquals,
							Value: "ns2",
						},
					},
				},
			}},
			right: AllocationFilterOr{Filters: []AllocationFilter{
				AllocationFilterNone{},
				AllocationFilterCondition{
					Field: FilterLabel,
					Op:    FilterStartsWith,
					Key:   "xyz",
					Value: "kubecost",
				},
				AllocationFilterAnd{
					Filters: []AllocationFilter{
						AllocationFilterCondition{
							Field: FilterNamespace,
							Op:    FilterEquals,
							Value: "ns1",
						},
						AllocationFilterCondition{
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
			left: AllocationFilterOr{Filters: []AllocationFilter{
				AllocationFilterNone{},
				AllocationFilterCondition{
					Field: FilterLabel,
					Op:    FilterStartsWith,
					Key:   "xyz",
					Value: "kubecost",
				},
				AllocationFilterAnd{
					Filters: []AllocationFilter{
						AllocationFilterCondition{
							Field: FilterNamespace,
							Op:    FilterEquals,
							Value: "ns1",
						},
						AllocationFilterCondition{
							Field: FilterNamespace,
							Op:    FilterEquals,
							Value: "ns2",
						},
					},
				},
			}},
			right: AllocationFilterOr{Filters: []AllocationFilter{
				AllocationFilterCondition{
					Field: FilterLabel,
					Op:    FilterStartsWith,
					Key:   "xyz",
					Value: "kubecost",
				},
				AllocationFilterNone{},
				AllocationFilterAnd{
					Filters: []AllocationFilter{
						AllocationFilterCondition{
							Field: FilterNamespace,
							Op:    FilterEquals,
							Value: "ns2",
						},
						AllocationFilterCondition{
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
			left: AllocationFilterOr{Filters: []AllocationFilter{
				AllocationFilterNone{},
				AllocationFilterCondition{
					Field: FilterLabel,
					Op:    FilterStartsWith,
					Key:   "xyz",
					Value: "kubecost",
				},
				AllocationFilterAnd{
					Filters: []AllocationFilter{
						AllocationFilterCondition{
							Field: FilterNamespace,
							Op:    FilterEquals,
							Value: "ns1",
						},
						AllocationFilterCondition{
							Field: FilterNamespace,
							Op:    FilterEquals,
							Value: "ns2",
						},
					},
				},
			}},
			right: AllocationFilterOr{Filters: []AllocationFilter{
				AllocationFilterAnd{
					Filters: []AllocationFilter{
						AllocationFilterCondition{
							Field: FilterNamespace,
							Op:    FilterEquals,
							Value: "ns1",
						},
						AllocationFilterCondition{
							Field: FilterNamespace,
							Op:    FilterEquals,
							Value: "ns2",
						},
					},
				},
				AllocationFilterNone{},
				AllocationFilterCondition{
					Field: FilterLabel,
					Op:    FilterStartsWith,
					Key:   "xyz",
					Value: "kubecost",
				},
			}},
			expected: true,
		},
		{
			left: AllocationFilterOr{Filters: []AllocationFilter{
				AllocationFilterNone{},
				AllocationFilterCondition{
					Field: FilterLabel,
					Op:    FilterStartsWith,
					Key:   "xyz",
					Value: "kubecost",
				},
				AllocationFilterAnd{
					Filters: []AllocationFilter{
						AllocationFilterCondition{
							Field: FilterNamespace,
							Op:    FilterEquals,
							Value: "ns1",
						},
						AllocationFilterCondition{
							Field: FilterNamespace,
							Op:    FilterEquals,
							Value: "ns2",
						},
					},
				},
			}},
			right: AllocationFilterOr{Filters: []AllocationFilter{
				AllocationFilterNone{},
				AllocationFilterCondition{
					Field: FilterLabel,
					Op:    FilterStartsWith,
					Key:   "xyz",
					Value: "kubecost",
				},
				AllocationFilterAnd{
					Filters: []AllocationFilter{
						AllocationFilterCondition{
							Field: FilterNamespace,
							Op:    FilterEquals,
							Value: "ns3",
						},
						AllocationFilterCondition{
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
			left: AllocationFilterOr{Filters: []AllocationFilter{
				AllocationFilterNone{},
				AllocationFilterCondition{
					Field: FilterLabel,
					Op:    FilterStartsWith,
					Key:   "xyz",
					Value: "kubecost",
				},
				AllocationFilterAnd{
					Filters: []AllocationFilter{
						AllocationFilterCondition{
							Field: FilterNamespace,
							Op:    FilterEquals,
							Value: "ns1",
						},
						AllocationFilterCondition{
							Field: FilterNamespace,
							Op:    FilterEquals,
							Value: "ns2",
						},
					},
				},
			}},
			right: AllocationFilterOr{Filters: []AllocationFilter{
				AllocationFilterCondition{
					Field: FilterLabel,
					Op:    FilterStartsWith,
					Key:   "xyz",
					Value: "kubecost",
				},
				AllocationFilterAnd{
					Filters: []AllocationFilter{
						AllocationFilterCondition{
							Field: FilterNamespace,
							Op:    FilterEquals,
							Value: "ns1",
						},
						AllocationFilterCondition{
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
			left:     AllocationFilterAnd{},
			right:    nil,
			expected: false,
		},
		{
			left:     AllocationFilterAnd{Filters: []AllocationFilter{}},
			right:    nil,
			expected: false,
		},

		{
			left:     AllocationFilterAnd{},
			right:    AllocationFilterAnd{},
			expected: true,
		},
		{
			left:     AllocationFilterAnd{},
			right:    AllocationFilterAnd{Filters: []AllocationFilter{}},
			expected: true,
		},

		{
			left:     AllocationFilterAnd{Filters: []AllocationFilter{}},
			right:    AllocationFilterAnd{},
			expected: true,
		},
		{
			left:     AllocationFilterAnd{Filters: []AllocationFilter{}},
			right:    AllocationFilterAnd{Filters: []AllocationFilter{}},
			expected: true,
		},
		// FILLED
		{
			left: AllocationFilterAnd{Filters: []AllocationFilter{
				AllocationFilterNone{},
				AllocationFilterCondition{
					Field: FilterLabel,
					Op:    FilterStartsWith,
					Key:   "xyz",
					Value: "kubecost",
				},
				AllocationFilterOr{
					Filters: []AllocationFilter{
						AllocationFilterCondition{
							Field: FilterNamespace,
							Op:    FilterEquals,
							Value: "ns1",
						},
						AllocationFilterCondition{
							Field: FilterNamespace,
							Op:    FilterEquals,
							Value: "ns2",
						},
					},
				},
			}},
			right: AllocationFilterAnd{Filters: []AllocationFilter{
				AllocationFilterNone{},
				AllocationFilterCondition{
					Field: FilterLabel,
					Op:    FilterStartsWith,
					Key:   "xyz",
					Value: "kubecost",
				},
				AllocationFilterOr{
					Filters: []AllocationFilter{
						AllocationFilterCondition{
							Field: FilterNamespace,
							Op:    FilterEquals,
							Value: "ns1",
						},
						AllocationFilterCondition{
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
			left: AllocationFilterAnd{Filters: []AllocationFilter{
				AllocationFilterNone{},
				AllocationFilterCondition{
					Field: FilterLabel,
					Op:    FilterStartsWith,
					Key:   "xyz",
					Value: "kubecost",
				},
				AllocationFilterOr{
					Filters: []AllocationFilter{
						AllocationFilterCondition{
							Field: FilterNamespace,
							Op:    FilterEquals,
							Value: "ns1",
						},
						AllocationFilterCondition{
							Field: FilterNamespace,
							Op:    FilterEquals,
							Value: "ns2",
						},
					},
				},
			}},
			right: AllocationFilterAnd{Filters: []AllocationFilter{
				AllocationFilterCondition{
					Field: FilterLabel,
					Op:    FilterStartsWith,
					Key:   "xyz",
					Value: "kubecost",
				},
				AllocationFilterNone{},
				AllocationFilterOr{
					Filters: []AllocationFilter{
						AllocationFilterCondition{
							Field: FilterNamespace,
							Op:    FilterEquals,
							Value: "ns2",
						},
						AllocationFilterCondition{
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
			left: AllocationFilterAnd{Filters: []AllocationFilter{
				AllocationFilterNone{},
				AllocationFilterCondition{
					Field: FilterLabel,
					Op:    FilterStartsWith,
					Key:   "xyz",
					Value: "kubecost",
				},
				AllocationFilterOr{
					Filters: []AllocationFilter{
						AllocationFilterCondition{
							Field: FilterNamespace,
							Op:    FilterEquals,
							Value: "ns1",
						},
						AllocationFilterCondition{
							Field: FilterNamespace,
							Op:    FilterEquals,
							Value: "ns2",
						},
					},
				},
			}},
			right: AllocationFilterAnd{Filters: []AllocationFilter{
				AllocationFilterOr{
					Filters: []AllocationFilter{
						AllocationFilterCondition{
							Field: FilterNamespace,
							Op:    FilterEquals,
							Value: "ns1",
						},
						AllocationFilterCondition{
							Field: FilterNamespace,
							Op:    FilterEquals,
							Value: "ns2",
						},
					},
				},
				AllocationFilterNone{},
				AllocationFilterCondition{
					Field: FilterLabel,
					Op:    FilterStartsWith,
					Key:   "xyz",
					Value: "kubecost",
				},
			}},
			expected: true,
		},
		{
			left: AllocationFilterAnd{Filters: []AllocationFilter{
				AllocationFilterNone{},
				AllocationFilterCondition{
					Field: FilterLabel,
					Op:    FilterStartsWith,
					Key:   "xyz",
					Value: "kubecost",
				},
				AllocationFilterOr{
					Filters: []AllocationFilter{
						AllocationFilterCondition{
							Field: FilterNamespace,
							Op:    FilterEquals,
							Value: "ns1",
						},
						AllocationFilterCondition{
							Field: FilterNamespace,
							Op:    FilterEquals,
							Value: "ns2",
						},
					},
				},
			}},
			right: AllocationFilterAnd{Filters: []AllocationFilter{
				AllocationFilterNone{},
				AllocationFilterCondition{
					Field: FilterLabel,
					Op:    FilterStartsWith,
					Key:   "xyz",
					Value: "kubecost",
				},
				AllocationFilterOr{
					Filters: []AllocationFilter{
						AllocationFilterCondition{
							Field: FilterNamespace,
							Op:    FilterEquals,
							Value: "ns3",
						},
						AllocationFilterCondition{
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
			left: AllocationFilterAnd{Filters: []AllocationFilter{
				AllocationFilterNone{},
				AllocationFilterCondition{
					Field: FilterLabel,
					Op:    FilterStartsWith,
					Key:   "xyz",
					Value: "kubecost",
				},
				AllocationFilterOr{
					Filters: []AllocationFilter{
						AllocationFilterCondition{
							Field: FilterNamespace,
							Op:    FilterEquals,
							Value: "ns1",
						},
						AllocationFilterCondition{
							Field: FilterNamespace,
							Op:    FilterEquals,
							Value: "ns2",
						},
					},
				},
			}},
			right: AllocationFilterAnd{Filters: []AllocationFilter{
				AllocationFilterCondition{
					Field: FilterLabel,
					Op:    FilterStartsWith,
					Key:   "xyz",
					Value: "kubecost",
				},
				AllocationFilterOr{
					Filters: []AllocationFilter{
						AllocationFilterCondition{
							Field: FilterNamespace,
							Op:    FilterEquals,
							Value: "ns1",
						},
						AllocationFilterCondition{
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
*/
