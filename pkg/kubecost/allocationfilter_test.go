package kubecost

import (
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
	}

	for _, c := range cases {
		result := c.filter.Matches(c.a)

		if result != c.expected {
			t.Errorf("%s: expected %t, got %t", c.name, c.expected, result)
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
			name: `label[app]="foo" and namespace="kubecost" -> true`,
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
			name: `label[app]="foo" and namespace="kubecost" -> false`,
			a: &Allocation{
				Properties: &AllocationProperties{
					Namespace: "kubecost-secondary",
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
			name: `label[app]="foo" or namespace="kubecost" -> false`,
			a: &Allocation{
				Properties: &AllocationProperties{
					Namespace: "kubecost-secondary",
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
