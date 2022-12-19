package filter_test

import (
	"testing"

	"github.com/opencost/opencost/pkg/filter"
	"github.com/opencost/opencost/pkg/kubecost"
)

func Test_String_Matches(t *testing.T) {
	cases := []struct {
		name   string
		a      *kubecost.Allocation
		filter filter.Filter[*kubecost.Allocation]

		expected bool
	}{
		{
			name: "ClusterID Equals -> true",
			a: &kubecost.Allocation{
				Properties: &kubecost.AllocationProperties{
					Cluster: "cluster-one",
				},
			},
			filter: filter.StringProperty[*kubecost.Allocation]{
				Field: kubecost.AllocationClusterProp,
				Op:    filter.StringEquals,
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
			filter: filter.StringProperty[*kubecost.Allocation]{
				Field: kubecost.AllocationClusterProp,
				Op:    filter.StringStartsWith,
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
			filter: filter.StringProperty[*kubecost.Allocation]{
				Field: kubecost.AllocationClusterProp,
				Op:    filter.StringStartsWith,
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
			filter: filter.StringProperty[*kubecost.Allocation]{
				Field: kubecost.AllocationClusterProp,
				Op:    filter.StringStartsWith,
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
			filter: filter.StringProperty[*kubecost.Allocation]{
				Field: kubecost.AllocationClusterProp,
				Op:    filter.StringStartsWith,
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
			filter: filter.StringProperty[*kubecost.Allocation]{
				Field: kubecost.AllocationNodeProp,
				Op:    filter.StringEquals,
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
			filter: filter.StringProperty[*kubecost.Allocation]{
				Field: kubecost.AllocationNamespaceProp,
				Op:    filter.StringEquals,
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
			filter: filter.StringProperty[*kubecost.Allocation]{
				Field: kubecost.AllocationControllerKindProp,
				Op:    filter.StringEquals,
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
			filter: filter.StringProperty[*kubecost.Allocation]{
				Field: kubecost.AllocationControllerProp,
				Op:    filter.StringEquals,
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
			filter: filter.StringProperty[*kubecost.Allocation]{
				Field: kubecost.AllocationPodProp,
				Op:    filter.StringEquals,
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
			filter: filter.StringProperty[*kubecost.Allocation]{
				Field: kubecost.AllocationContainerProp,
				Op:    filter.StringEquals,
				Value: "cost-model",
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
			filter: filter.StringProperty[*kubecost.Allocation]{
				Field: kubecost.AllocationNamespaceProp,
				Op:    filter.StringEquals,
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

func Test_StringSlice_Matches(t *testing.T) {
	cases := []struct {
		name   string
		a      *kubecost.Allocation
		filter filter.Filter[*kubecost.Allocation]

		expected bool
	}{
		{
			name: `services contains -> true`,
			a: &kubecost.Allocation{
				Properties: &kubecost.AllocationProperties{
					Services: []string{"serv1", "serv2"},
				},
			},
			filter: filter.StringSliceProperty[*kubecost.Allocation]{
				Field: kubecost.AllocationServiceProp,
				Op:    filter.StringSliceContains,
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
			filter: filter.StringSliceProperty[*kubecost.Allocation]{
				Field: kubecost.AllocationServiceProp,
				Op:    filter.StringSliceContains,
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
			filter: filter.StringSliceProperty[*kubecost.Allocation]{
				Field: kubecost.AllocationServiceProp,
				Op:    filter.StringSliceContains,
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
			filter: filter.StringSliceProperty[*kubecost.Allocation]{
				Field: kubecost.AllocationServiceProp,
				Op:    filter.StringSliceContains,
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

func Test_StringMap_Matches(t *testing.T) {
	cases := []struct {
		name   string
		a      *kubecost.Allocation
		filter filter.Filter[*kubecost.Allocation]

		expected bool
	}{
		{
			name: `label[app]="foo" -> true`,
			a: &kubecost.Allocation{
				Properties: &kubecost.AllocationProperties{
					Labels: map[string]string{
						"app": "foo",
					},
				},
			},
			filter: filter.StringMapProperty[*kubecost.Allocation]{
				Field: kubecost.AllocationLabelProp,
				Op:    filter.StringMapEquals,
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
			filter: filter.StringMapProperty[*kubecost.Allocation]{
				Field: kubecost.AllocationLabelProp,
				Op:    filter.StringMapEquals,
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
			filter: filter.StringMapProperty[*kubecost.Allocation]{
				Field: kubecost.AllocationLabelProp,
				Op:    filter.StringMapEquals,
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
			filter: filter.StringMapProperty[*kubecost.Allocation]{
				Field: kubecost.AllocationLabelProp,
				Op:    filter.StringMapEquals,
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
			filter: filter.StringMapProperty[*kubecost.Allocation]{
				Field: kubecost.AllocationLabelProp,
				Op:    filter.StringMapEquals,
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
			filter: filter.StringMapProperty[*kubecost.Allocation]{
				Field: kubecost.AllocationAnnotationProp,
				Op:    filter.StringMapEquals,
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
			filter: filter.StringMapProperty[*kubecost.Allocation]{
				Field: kubecost.AllocationAnnotationProp,
				Op:    filter.StringMapEquals,
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
			filter: filter.StringMapProperty[*kubecost.Allocation]{
				Field: kubecost.AllocationAnnotationProp,
				Op:    filter.StringMapEquals,
				Key:   "app",
				Value: "foo",
			},

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

func Test_Not_Matches(t *testing.T) {
	cases := []struct {
		name   string
		a      *kubecost.Allocation
		filter filter.Filter[*kubecost.Allocation]

		expected bool
	}{
		{
			name: "Namespace NotEquals -> false",
			a: &kubecost.Allocation{
				Properties: &kubecost.AllocationProperties{
					Namespace: "kube-system",
				},
			},
			filter: filter.Not[*kubecost.Allocation]{
				Filter: filter.StringProperty[*kubecost.Allocation]{
					Field: kubecost.AllocationNamespaceProp,
					Op:    filter.StringEquals,
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
			filter: filter.Not[*kubecost.Allocation]{
				Filter: filter.StringProperty[*kubecost.Allocation]{
					Field: kubecost.AllocationNamespaceProp,
					Op:    filter.StringEquals,
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
			filter: filter.Not[*kubecost.Allocation]{
				Filter: filter.StringProperty[*kubecost.Allocation]{
					Field: kubecost.AllocationNamespaceProp,
					Op:    filter.StringEquals,
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
			filter: filter.Not[*kubecost.Allocation]{
				Filter: filter.StringMapProperty[*kubecost.Allocation]{
					Field: kubecost.AllocationLabelProp,
					Op:    filter.StringMapEquals,
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
			filter: filter.Not[*kubecost.Allocation]{
				Filter: filter.StringMapProperty[*kubecost.Allocation]{
					Field: kubecost.AllocationLabelProp,
					Op:    filter.StringMapEquals,
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
			filter: filter.Not[*kubecost.Allocation]{
				Filter: filter.StringMapProperty[*kubecost.Allocation]{
					Field: kubecost.AllocationLabelProp,
					Op:    filter.StringMapEquals,
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
			filter: filter.StringMapProperty[*kubecost.Allocation]{
				Field: kubecost.AllocationAnnotationProp,
				Op:    filter.StringMapEquals,
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
			filter: filter.StringMapProperty[*kubecost.Allocation]{
				Field: kubecost.AllocationAnnotationProp,
				Op:    filter.StringMapEquals,
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
			filter: filter.StringMapProperty[*kubecost.Allocation]{
				Field: kubecost.AllocationAnnotationProp,
				Op:    filter.StringMapEquals,
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
			filter: filter.Not[*kubecost.Allocation]{
				Filter: filter.StringMapProperty[*kubecost.Allocation]{
					Field: kubecost.AllocationAnnotationProp,
					Op:    filter.StringMapEquals,
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
			filter: filter.StringProperty[*kubecost.Allocation]{
				Field: kubecost.AllocationNamespaceProp,
				Op:    filter.StringEquals,
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
			filter: filter.StringSliceProperty[*kubecost.Allocation]{
				Field: kubecost.AllocationServiceProp,
				Op:    filter.StringSliceContains,
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
			filter: filter.StringSliceProperty[*kubecost.Allocation]{
				Field: kubecost.AllocationServiceProp,
				Op:    filter.StringSliceContains,
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
			filter: filter.Not[*kubecost.Allocation]{
				Filter: filter.StringSliceProperty[*kubecost.Allocation]{
					Field: kubecost.AllocationServiceProp,
					Op:    filter.StringSliceContains,
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
			filter: filter.Not[*kubecost.Allocation]{
				Filter: filter.StringSliceProperty[*kubecost.Allocation]{
					Field: kubecost.AllocationServiceProp,
					Op:    filter.StringSliceContains,
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
			filter: filter.Not[*kubecost.Allocation]{
				Filter: filter.StringSliceProperty[*kubecost.Allocation]{
					Field: kubecost.AllocationServiceProp,
					Op:    filter.StringSliceContains,
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
			filter: filter.Not[*kubecost.Allocation]{
				Filter: filter.StringSliceProperty[*kubecost.Allocation]{
					Field: kubecost.AllocationServiceProp,
					Op:    filter.StringSliceContains,
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
			filter: filter.StringSliceProperty[*kubecost.Allocation]{
				Field: kubecost.AllocationServiceProp,
				Op:    filter.StringSliceContainsPrefix,
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
			filter: filter.StringSliceProperty[*kubecost.Allocation]{
				Field: kubecost.AllocationServiceProp,
				Op:    filter.StringSliceContainsPrefix,
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
			filter: filter.StringSliceProperty[*kubecost.Allocation]{
				Field: kubecost.AllocationServiceProp,
				Op:    filter.StringSliceContains,
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
			filter: filter.StringSliceProperty[*kubecost.Allocation]{
				Field: kubecost.AllocationServiceProp,
				Op:    filter.StringSliceContains,
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

func Test_None_Matches(t *testing.T) {
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
		result := filter.AllCut[*kubecost.Allocation]{}.Matches(c.a)

		if result {
			t.Errorf("%s: should have been rejected", c.name)
		}
	}
}

func Test_And_Matches(t *testing.T) {
	cases := []struct {
		name   string
		a      *kubecost.Allocation
		filter filter.Filter[*kubecost.Allocation]

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
			filter: filter.And[*kubecost.Allocation]{[]filter.Filter[*kubecost.Allocation]{
				filter.StringMapProperty[*kubecost.Allocation]{
					Field: kubecost.AllocationLabelProp,
					Op:    filter.StringMapEquals,
					Key:   "app",
					Value: "foo",
				},
				filter.StringProperty[*kubecost.Allocation]{
					Field: kubecost.AllocationNamespaceProp,
					Op:    filter.StringEquals,
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
			filter: filter.And[*kubecost.Allocation]{[]filter.Filter[*kubecost.Allocation]{
				filter.StringMapProperty[*kubecost.Allocation]{
					Field: kubecost.AllocationLabelProp,
					Op:    filter.StringMapEquals,
					Key:   "app",
					Value: "foo",
				},
				filter.StringProperty[*kubecost.Allocation]{
					Field: kubecost.AllocationNamespaceProp,
					Op:    filter.StringEquals,
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
			filter: filter.And[*kubecost.Allocation]{[]filter.Filter[*kubecost.Allocation]{
				filter.StringMapProperty[*kubecost.Allocation]{
					Field: kubecost.AllocationLabelProp,
					Op:    filter.StringMapEquals,
					Key:   "app",
					Value: "foo",
				},
				filter.StringProperty[*kubecost.Allocation]{
					Field: kubecost.AllocationNamespaceProp,
					Op:    filter.StringEquals,
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
			filter: filter.And[*kubecost.Allocation]{[]filter.Filter[*kubecost.Allocation]{
				filter.StringMapProperty[*kubecost.Allocation]{
					Field: kubecost.AllocationLabelProp,
					Op:    filter.StringMapEquals,
					Key:   "app",
					Value: "foo",
				},
				filter.StringProperty[*kubecost.Allocation]{
					Field: kubecost.AllocationNamespaceProp,
					Op:    filter.StringEquals,
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
			filter: filter.And[*kubecost.Allocation]{[]filter.Filter[*kubecost.Allocation]{
				filter.AllCut[*kubecost.Allocation]{},
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
