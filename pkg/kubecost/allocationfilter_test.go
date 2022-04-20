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
