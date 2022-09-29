package filter_test

import (
	"reflect"
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

func Test_Or_Matches(t *testing.T) {
	cases := []struct {
		name   string
		a      *kubecost.Allocation
		filter filter.Filter[*kubecost.Allocation]

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
			filter: filter.Or[*kubecost.Allocation]{[]filter.Filter[*kubecost.Allocation]{
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
			name: `label[app]="foo" or namespace="kubecost" -> first true`,
			a: &kubecost.Allocation{
				Properties: &kubecost.AllocationProperties{
					Namespace: "kube-system",
					Labels: map[string]string{
						"app": "foo",
					},
				},
			},
			filter: filter.Or[*kubecost.Allocation]{[]filter.Filter[*kubecost.Allocation]{
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
			name: `label[app]="foo" or namespace="kubecost" -> second true`,
			a: &kubecost.Allocation{
				Properties: &kubecost.AllocationProperties{
					Namespace: "kubecost",
					Labels: map[string]string{
						"app": "bar",
					},
				},
			},
			filter: filter.Or[*kubecost.Allocation]{[]filter.Filter[*kubecost.Allocation]{
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
			name: `label[app]="foo" or namespace="kubecost" -> both false`,
			a: &kubecost.Allocation{
				Properties: &kubecost.AllocationProperties{
					Namespace: "kube-system",
					Labels: map[string]string{
						"app": "bar",
					},
				},
			},
			filter: filter.Or[*kubecost.Allocation]{[]filter.Filter[*kubecost.Allocation]{
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
	}

	for _, c := range cases {
		result := c.filter.Matches(c.a)

		if result != c.expected {
			t.Errorf("%s: expected %t, got %t", c.name, c.expected, result)
		}
	}
}

func Test_Filter_Flattened(t *testing.T) {
	cases := []struct {
		name string

		input    filter.Filter[*kubecost.Allocation]
		expected filter.Filter[*kubecost.Allocation]
	}{
		{
			name: "filter.StringProperty[*kubecost.Allocation]",
			input: filter.StringProperty[*kubecost.Allocation]{
				Field: kubecost.AllocationNamespaceProp,
				Op:    filter.StringEquals,
			},
			expected: filter.StringProperty[*kubecost.Allocation]{
				Field: kubecost.AllocationNamespaceProp,
				Op:    filter.StringEquals,
			},
		},
		{
			name:     "empty filter.And[*kubecost.Allocation] (nil)",
			input:    filter.And[*kubecost.Allocation]{},
			expected: filter.AllPass[*kubecost.Allocation]{},
		},
		{
			name:     "empty filter.And[*kubecost.Allocation] (len 0)",
			input:    filter.And[*kubecost.Allocation]{Filters: []filter.Filter[*kubecost.Allocation]{}},
			expected: filter.AllPass[*kubecost.Allocation]{},
		},
		{
			name:     "empty filter.Or[*kubecost.Allocation] (nil)",
			input:    filter.Or[*kubecost.Allocation]{},
			expected: filter.AllPass[*kubecost.Allocation]{},
		},
		{
			name:     "empty filter.Or[*kubecost.Allocation] (len 0)",
			input:    filter.Or[*kubecost.Allocation]{Filters: []filter.Filter[*kubecost.Allocation]{}},
			expected: filter.AllPass[*kubecost.Allocation]{},
		},
		{
			name: "single-element filter.And[*kubecost.Allocation]",
			input: filter.And[*kubecost.Allocation]{Filters: []filter.Filter[*kubecost.Allocation]{
				filter.StringProperty[*kubecost.Allocation]{
					Field: kubecost.AllocationNamespaceProp,
					Op:    filter.StringEquals,
				},
			}},

			expected: filter.StringProperty[*kubecost.Allocation]{
				Field: kubecost.AllocationNamespaceProp,
				Op:    filter.StringEquals,
			},
		},
		{
			name: "single-element filter.Or[*kubecost.Allocation]",
			input: filter.Or[*kubecost.Allocation]{Filters: []filter.Filter[*kubecost.Allocation]{
				filter.StringProperty[*kubecost.Allocation]{
					Field: kubecost.AllocationNamespaceProp,
					Op:    filter.StringEquals,
				},
			}},

			expected: filter.StringProperty[*kubecost.Allocation]{
				Field: kubecost.AllocationNamespaceProp,
				Op:    filter.StringEquals,
			},
		},
		{
			name: "multi-element filter.And[*kubecost.Allocation]",
			input: filter.And[*kubecost.Allocation]{Filters: []filter.Filter[*kubecost.Allocation]{
				filter.StringProperty[*kubecost.Allocation]{
					Field: kubecost.AllocationNamespaceProp,
					Op:    filter.StringEquals,
				},
				filter.Not[*kubecost.Allocation]{
					Filter: filter.StringProperty[*kubecost.Allocation]{
						Field: kubecost.AllocationClusterProp,
						Op:    filter.StringEquals,
					},
				},
				filter.StringSliceProperty[*kubecost.Allocation]{
					Field: kubecost.AllocationServiceProp,
					Op:    filter.StringSliceContains,
				},
			}},

			expected: filter.And[*kubecost.Allocation]{Filters: []filter.Filter[*kubecost.Allocation]{
				filter.Not[*kubecost.Allocation]{
					Filter: filter.StringProperty[*kubecost.Allocation]{
						Field: kubecost.AllocationClusterProp,
						Op:    filter.StringEquals,
					},
				},
				filter.StringProperty[*kubecost.Allocation]{
					Field: kubecost.AllocationNamespaceProp,
					Op:    filter.StringEquals,
				},
				filter.StringSliceProperty[*kubecost.Allocation]{
					Field: kubecost.AllocationServiceProp,
					Op:    filter.StringSliceContains,
				},
			}},
		},
		{
			name: "multi-element filter.Or[*kubecost.Allocation]",
			input: filter.Or[*kubecost.Allocation]{Filters: []filter.Filter[*kubecost.Allocation]{
				filter.StringProperty[*kubecost.Allocation]{
					Field: kubecost.AllocationNamespaceProp,
					Op:    filter.StringEquals,
				},
				filter.Not[*kubecost.Allocation]{
					Filter: filter.StringProperty[*kubecost.Allocation]{
						Field: kubecost.AllocationClusterProp,
						Op:    filter.StringEquals,
					},
				},
				filter.StringSliceProperty[*kubecost.Allocation]{
					Field: kubecost.AllocationServiceProp,
					Op:    filter.StringSliceContains,
				},
			}},

			expected: filter.Or[*kubecost.Allocation]{Filters: []filter.Filter[*kubecost.Allocation]{
				filter.Not[*kubecost.Allocation]{
					Filter: filter.StringProperty[*kubecost.Allocation]{
						Field: kubecost.AllocationClusterProp,
						Op:    filter.StringEquals,
					},
				},
				filter.StringProperty[*kubecost.Allocation]{
					Field: kubecost.AllocationNamespaceProp,
					Op:    filter.StringEquals,
				},
				filter.StringSliceProperty[*kubecost.Allocation]{
					Field: kubecost.AllocationServiceProp,
					Op:    filter.StringSliceContains,
				},
			}},
		},
		{
			name:     "filter.AllCut[*kubecost.Allocation]",
			input:    filter.AllCut[*kubecost.Allocation]{},
			expected: filter.AllCut[*kubecost.Allocation]{},
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

func Test_Filter_Equals(t *testing.T) {
	cases := map[string]struct {
		left     filter.Filter[*kubecost.Allocation]
		right    filter.Filter[*kubecost.Allocation]
		expected bool
	}{
		// AC
		"AC1": {
			left:     filter.StringProperty[*kubecost.Allocation]{},
			right:    filter.StringProperty[*kubecost.Allocation]{},
			expected: true,
		},
		"AC2": {
			left: filter.StringProperty[*kubecost.Allocation]{
				Field: kubecost.AllocationNamespaceProp,
				Op:    filter.StringStartsWith,
				Value: "kubecost-abc",
			},
			right: filter.StringProperty[*kubecost.Allocation]{
				Field: kubecost.AllocationNamespaceProp,
				Op:    filter.StringStartsWith,
				Value: "kubecost-abc",
			},
			expected: true,
		},
		"AC3": {
			left: filter.StringMapProperty[*kubecost.Allocation]{
				Field: kubecost.AllocationLabelProp,
				Op:    filter.StringMapEquals,
				Key:   "app",
				Value: "kubecost-abc",
			},
			right: filter.StringMapProperty[*kubecost.Allocation]{
				Field: kubecost.AllocationLabelProp,
				Op:    filter.StringMapEquals,
				Key:   "app",
				Value: "kubecost-abc",
			},
			expected: true,
		},
		"AC4": {
			left: filter.StringMapProperty[*kubecost.Allocation]{
				Field: kubecost.AllocationLabelProp,
				Op:    filter.StringMapEquals,
				Key:   "app",
				Value: "kubecost-abc",
			},
			right: filter.StringProperty[*kubecost.Allocation]{
				Field: kubecost.AllocationLabelProp,
				Op:    filter.StringMapEquals,
				Value: "kubecost-abc",
			},
			expected: false,
		},
		"AC5": {
			left: filter.StringMapProperty[*kubecost.Allocation]{
				Field: kubecost.AllocationLabelProp,
				Op:    filter.StringMapEquals,
				Value: "kubecost-abc",
			},
			right: filter.StringMapProperty[*kubecost.Allocation]{
				Field: kubecost.AllocationLabelProp,
				Op:    filter.StringMapEquals,
				Key:   "app",
				Value: "kubecost-abc",
			},
			expected: false,
		},
		"AC6": {
			left: filter.StringProperty[*kubecost.Allocation]{
				Field: kubecost.AllocationNamespaceProp,
				Op:    filter.StringStartsWith,
				Value: "kubecost-abc",
			},
			right: filter.StringProperty[*kubecost.Allocation]{
				Field: kubecost.AllocationNamespaceProp,
				Op:    filter.StringStartsWith,
				Value: "kubecost-abcd",
			},
			expected: false,
		},
		// OR
		// EMPTY
		"OrEmpty1": {
			left:     filter.Or[*kubecost.Allocation]{},
			right:    filter.AllPass[*kubecost.Allocation]{},
			expected: true,
		},
		"OrEmpty2": {
			left:     filter.Or[*kubecost.Allocation]{Filters: []filter.Filter[*kubecost.Allocation]{}},
			right:    filter.AllPass[*kubecost.Allocation]{},
			expected: true,
		},

		"OrEmpty3": {
			left:     filter.Or[*kubecost.Allocation]{},
			right:    filter.Or[*kubecost.Allocation]{},
			expected: true,
		},
		"OrEmpty4": {
			left:     filter.Or[*kubecost.Allocation]{},
			right:    filter.Or[*kubecost.Allocation]{Filters: []filter.Filter[*kubecost.Allocation]{}},
			expected: true,
		},

		"OrEmpty5": {
			left:     filter.Or[*kubecost.Allocation]{Filters: []filter.Filter[*kubecost.Allocation]{}},
			right:    filter.Or[*kubecost.Allocation]{},
			expected: true,
		},
		"OrEmpty6": {
			left:     filter.Or[*kubecost.Allocation]{Filters: []filter.Filter[*kubecost.Allocation]{}},
			right:    filter.Or[*kubecost.Allocation]{Filters: []filter.Filter[*kubecost.Allocation]{}},
			expected: true,
		},
		// FILLED
		"OrFilled1": {
			left: filter.Or[*kubecost.Allocation]{Filters: []filter.Filter[*kubecost.Allocation]{
				filter.AllCut[*kubecost.Allocation]{},
				filter.StringMapProperty[*kubecost.Allocation]{
					Field: kubecost.AllocationLabelProp,
					Op:    filter.StringStartsWith,
					Key:   "xyz",
					Value: "kubecost",
				},
				filter.And[*kubecost.Allocation]{
					Filters: []filter.Filter[*kubecost.Allocation]{
						filter.StringProperty[*kubecost.Allocation]{
							Field: kubecost.AllocationNamespaceProp,
							Op:    filter.StringEquals,
							Value: "ns1",
						},
						filter.StringProperty[*kubecost.Allocation]{
							Field: kubecost.AllocationNamespaceProp,
							Op:    filter.StringEquals,
							Value: "ns2",
						},
					},
				},
			}},
			right: filter.Or[*kubecost.Allocation]{Filters: []filter.Filter[*kubecost.Allocation]{
				filter.AllCut[*kubecost.Allocation]{},
				filter.StringMapProperty[*kubecost.Allocation]{
					Field: kubecost.AllocationLabelProp,
					Op:    filter.StringStartsWith,
					Key:   "xyz",
					Value: "kubecost",
				},
				filter.And[*kubecost.Allocation]{
					Filters: []filter.Filter[*kubecost.Allocation]{
						filter.StringProperty[*kubecost.Allocation]{
							Field: kubecost.AllocationNamespaceProp,
							Op:    filter.StringEquals,
							Value: "ns1",
						},
						filter.StringProperty[*kubecost.Allocation]{
							Field: kubecost.AllocationNamespaceProp,
							Op:    filter.StringEquals,
							Value: "ns2",
						},
					},
				},
			}},
			expected: true,
		},
		"OrFilled2": {
			left: filter.Or[*kubecost.Allocation]{Filters: []filter.Filter[*kubecost.Allocation]{
				filter.AllCut[*kubecost.Allocation]{},
				filter.StringMapProperty[*kubecost.Allocation]{
					Field: kubecost.AllocationLabelProp,
					Op:    filter.StringStartsWith,
					Key:   "xyz",
					Value: "kubecost",
				},
				filter.And[*kubecost.Allocation]{
					Filters: []filter.Filter[*kubecost.Allocation]{
						filter.StringProperty[*kubecost.Allocation]{
							Field: kubecost.AllocationNamespaceProp,
							Op:    filter.StringEquals,
							Value: "ns1",
						},
						filter.StringProperty[*kubecost.Allocation]{
							Field: kubecost.AllocationNamespaceProp,
							Op:    filter.StringEquals,
							Value: "ns2",
						},
					},
				},
			}},
			right: filter.Or[*kubecost.Allocation]{Filters: []filter.Filter[*kubecost.Allocation]{
				filter.StringMapProperty[*kubecost.Allocation]{
					Field: kubecost.AllocationLabelProp,
					Op:    filter.StringStartsWith,
					Key:   "xyz",
					Value: "kubecost",
				},
				filter.AllCut[*kubecost.Allocation]{},
				filter.And[*kubecost.Allocation]{
					Filters: []filter.Filter[*kubecost.Allocation]{
						filter.StringProperty[*kubecost.Allocation]{
							Field: kubecost.AllocationNamespaceProp,
							Op:    filter.StringEquals,
							Value: "ns2",
						},
						filter.StringProperty[*kubecost.Allocation]{
							Field: kubecost.AllocationNamespaceProp,
							Op:    filter.StringEquals,
							Value: "ns1",
						},
					},
				},
			}},
			expected: true,
		},
		"OrFilled3": {
			left: filter.Or[*kubecost.Allocation]{Filters: []filter.Filter[*kubecost.Allocation]{
				filter.AllCut[*kubecost.Allocation]{},
				filter.StringMapProperty[*kubecost.Allocation]{
					Field: kubecost.AllocationLabelProp,
					Op:    filter.StringStartsWith,
					Key:   "xyz",
					Value: "kubecost",
				},
				filter.And[*kubecost.Allocation]{
					Filters: []filter.Filter[*kubecost.Allocation]{
						filter.StringProperty[*kubecost.Allocation]{
							Field: kubecost.AllocationNamespaceProp,
							Op:    filter.StringEquals,
							Value: "ns1",
						},
						filter.StringProperty[*kubecost.Allocation]{
							Field: kubecost.AllocationNamespaceProp,
							Op:    filter.StringEquals,
							Value: "ns2",
						},
					},
				},
			}},
			right: filter.Or[*kubecost.Allocation]{Filters: []filter.Filter[*kubecost.Allocation]{
				filter.And[*kubecost.Allocation]{
					Filters: []filter.Filter[*kubecost.Allocation]{
						filter.StringProperty[*kubecost.Allocation]{
							Field: kubecost.AllocationNamespaceProp,
							Op:    filter.StringEquals,
							Value: "ns1",
						},
						filter.StringProperty[*kubecost.Allocation]{
							Field: kubecost.AllocationNamespaceProp,
							Op:    filter.StringEquals,
							Value: "ns2",
						},
					},
				},
				filter.AllCut[*kubecost.Allocation]{},
				filter.StringMapProperty[*kubecost.Allocation]{
					Field: kubecost.AllocationLabelProp,
					Op:    filter.StringStartsWith,
					Key:   "xyz",
					Value: "kubecost",
				},
			}},
			expected: true,
		},
		"OrFilled4": {
			left: filter.Or[*kubecost.Allocation]{Filters: []filter.Filter[*kubecost.Allocation]{
				filter.AllCut[*kubecost.Allocation]{},
				filter.StringMapProperty[*kubecost.Allocation]{
					Field: kubecost.AllocationLabelProp,
					Op:    filter.StringStartsWith,
					Key:   "xyz",
					Value: "kubecost",
				},
				filter.And[*kubecost.Allocation]{
					Filters: []filter.Filter[*kubecost.Allocation]{
						filter.StringProperty[*kubecost.Allocation]{
							Field: kubecost.AllocationNamespaceProp,
							Op:    filter.StringEquals,
							Value: "ns1",
						},
						filter.StringProperty[*kubecost.Allocation]{
							Field: kubecost.AllocationNamespaceProp,
							Op:    filter.StringEquals,
							Value: "ns2",
						},
					},
				},
			}},
			right: filter.Or[*kubecost.Allocation]{Filters: []filter.Filter[*kubecost.Allocation]{
				filter.AllCut[*kubecost.Allocation]{},
				filter.StringMapProperty[*kubecost.Allocation]{
					Field: kubecost.AllocationLabelProp,
					Op:    filter.StringStartsWith,
					Key:   "xyz",
					Value: "kubecost",
				},
				filter.And[*kubecost.Allocation]{
					Filters: []filter.Filter[*kubecost.Allocation]{
						filter.StringProperty[*kubecost.Allocation]{
							Field: kubecost.AllocationNamespaceProp,
							Op:    filter.StringEquals,
							Value: "ns3",
						},
						filter.StringProperty[*kubecost.Allocation]{
							Field: kubecost.AllocationNamespaceProp,
							Op:    filter.StringEquals,
							Value: "ns2",
						},
					},
				},
			}},
			expected: false,
		},
		"OrFilled5": {
			left: filter.Or[*kubecost.Allocation]{Filters: []filter.Filter[*kubecost.Allocation]{
				filter.StringMapProperty[*kubecost.Allocation]{
					Field: kubecost.AllocationLabelProp,
					Op:    filter.StringStartsWith,
					Key:   "xyz",
					Value: "kubecost",
				},
				filter.And[*kubecost.Allocation]{
					Filters: []filter.Filter[*kubecost.Allocation]{
						filter.StringProperty[*kubecost.Allocation]{
							Field: kubecost.AllocationNamespaceProp,
							Op:    filter.StringEquals,
							Value: "ns1",
						},
						filter.StringProperty[*kubecost.Allocation]{
							Field: kubecost.AllocationNamespaceProp,
							Op:    filter.StringEquals,
							Value: "ns2",
						},
					},
				},
			}},
			right: filter.Or[*kubecost.Allocation]{Filters: []filter.Filter[*kubecost.Allocation]{
				filter.StringMapProperty[*kubecost.Allocation]{
					Field: kubecost.AllocationLabelProp,
					Op:    filter.StringStartsWith,
					Key:   "xyz",
					Value: "kubecost",
				},
				filter.And[*kubecost.Allocation]{
					Filters: []filter.Filter[*kubecost.Allocation]{
						filter.StringProperty[*kubecost.Allocation]{
							Field: kubecost.AllocationNamespaceProp,
							Op:    filter.StringEquals,
							Value: "ns3",
						},
						filter.StringProperty[*kubecost.Allocation]{
							Field: kubecost.AllocationNamespaceProp,
							Op:    filter.StringEquals,
							Value: "ns2",
						},
					},
				},
			}},
			expected: false,
		},
		"OrFilled6": {
			left: filter.Or[*kubecost.Allocation]{Filters: []filter.Filter[*kubecost.Allocation]{
				filter.AllCut[*kubecost.Allocation]{},
				filter.StringMapProperty[*kubecost.Allocation]{
					Field: kubecost.AllocationLabelProp,
					Op:    filter.StringStartsWith,
					Key:   "xyz",
					Value: "kubecost",
				},
				filter.And[*kubecost.Allocation]{
					Filters: []filter.Filter[*kubecost.Allocation]{
						filter.StringProperty[*kubecost.Allocation]{
							Field: kubecost.AllocationNamespaceProp,
							Op:    filter.StringEquals,
							Value: "ns1",
						},
						filter.StringProperty[*kubecost.Allocation]{
							Field: kubecost.AllocationNamespaceProp,
							Op:    filter.StringEquals,
							Value: "ns2",
						},
					},
				},
			}},
			right: filter.Or[*kubecost.Allocation]{Filters: []filter.Filter[*kubecost.Allocation]{
				filter.StringMapProperty[*kubecost.Allocation]{
					Field: kubecost.AllocationLabelProp,
					Op:    filter.StringStartsWith,
					Key:   "xyz",
					Value: "kubecost",
				},
				filter.And[*kubecost.Allocation]{
					Filters: []filter.Filter[*kubecost.Allocation]{
						filter.StringProperty[*kubecost.Allocation]{
							Field: kubecost.AllocationNamespaceProp,
							Op:    filter.StringEquals,
							Value: "ns1",
						},
						filter.StringProperty[*kubecost.Allocation]{
							Field: kubecost.AllocationNamespaceProp,
							Op:    filter.StringEquals,
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
			left:     filter.And[*kubecost.Allocation]{},
			right:    filter.AllPass[*kubecost.Allocation]{},
			expected: true,
		},
		"AndEmpty2": {
			left:     filter.And[*kubecost.Allocation]{Filters: []filter.Filter[*kubecost.Allocation]{}},
			right:    filter.AllPass[*kubecost.Allocation]{},
			expected: true,
		},

		"AndEmpty3": {
			left:     filter.And[*kubecost.Allocation]{},
			right:    filter.And[*kubecost.Allocation]{},
			expected: true,
		},
		"AndEmpty4": {
			left:     filter.And[*kubecost.Allocation]{},
			right:    filter.And[*kubecost.Allocation]{Filters: []filter.Filter[*kubecost.Allocation]{}},
			expected: true,
		},

		"AndEmpty5": {
			left:     filter.And[*kubecost.Allocation]{Filters: []filter.Filter[*kubecost.Allocation]{}},
			right:    filter.And[*kubecost.Allocation]{},
			expected: true,
		},
		"AndEmpty6": {
			left:     filter.And[*kubecost.Allocation]{Filters: []filter.Filter[*kubecost.Allocation]{}},
			right:    filter.And[*kubecost.Allocation]{Filters: []filter.Filter[*kubecost.Allocation]{}},
			expected: true,
		},
		// FILLED
		"AndFilled1": {
			left: filter.And[*kubecost.Allocation]{Filters: []filter.Filter[*kubecost.Allocation]{
				filter.AllCut[*kubecost.Allocation]{},
				filter.StringMapProperty[*kubecost.Allocation]{
					Field: kubecost.AllocationLabelProp,
					Op:    filter.StringStartsWith,
					Key:   "xyz",
					Value: "kubecost",
				},
				filter.Or[*kubecost.Allocation]{
					Filters: []filter.Filter[*kubecost.Allocation]{
						filter.StringProperty[*kubecost.Allocation]{
							Field: kubecost.AllocationNamespaceProp,
							Op:    filter.StringEquals,
							Value: "ns1",
						},
						filter.StringProperty[*kubecost.Allocation]{
							Field: kubecost.AllocationNamespaceProp,
							Op:    filter.StringEquals,
							Value: "ns2",
						},
					},
				},
			}},
			right: filter.And[*kubecost.Allocation]{Filters: []filter.Filter[*kubecost.Allocation]{
				filter.AllCut[*kubecost.Allocation]{},
				filter.StringMapProperty[*kubecost.Allocation]{
					Field: kubecost.AllocationLabelProp,
					Op:    filter.StringStartsWith,
					Key:   "xyz",
					Value: "kubecost",
				},
				filter.Or[*kubecost.Allocation]{
					Filters: []filter.Filter[*kubecost.Allocation]{
						filter.StringProperty[*kubecost.Allocation]{
							Field: kubecost.AllocationNamespaceProp,
							Op:    filter.StringEquals,
							Value: "ns1",
						},
						filter.StringProperty[*kubecost.Allocation]{
							Field: kubecost.AllocationNamespaceProp,
							Op:    filter.StringEquals,
							Value: "ns2",
						},
					},
				},
			}},
			expected: true,
		},
		"AndFilled2": {
			left: filter.And[*kubecost.Allocation]{Filters: []filter.Filter[*kubecost.Allocation]{
				filter.AllCut[*kubecost.Allocation]{},
				filter.StringMapProperty[*kubecost.Allocation]{
					Field: kubecost.AllocationLabelProp,
					Op:    filter.StringStartsWith,
					Key:   "xyz",
					Value: "kubecost",
				},
				filter.Or[*kubecost.Allocation]{
					Filters: []filter.Filter[*kubecost.Allocation]{
						filter.StringProperty[*kubecost.Allocation]{
							Field: kubecost.AllocationNamespaceProp,
							Op:    filter.StringEquals,
							Value: "ns1",
						},
						filter.StringProperty[*kubecost.Allocation]{
							Field: kubecost.AllocationNamespaceProp,
							Op:    filter.StringEquals,
							Value: "ns2",
						},
					},
				},
			}},
			right: filter.And[*kubecost.Allocation]{Filters: []filter.Filter[*kubecost.Allocation]{
				filter.StringMapProperty[*kubecost.Allocation]{
					Field: kubecost.AllocationLabelProp,
					Op:    filter.StringStartsWith,
					Key:   "xyz",
					Value: "kubecost",
				},
				filter.AllCut[*kubecost.Allocation]{},
				filter.Or[*kubecost.Allocation]{
					Filters: []filter.Filter[*kubecost.Allocation]{
						filter.StringProperty[*kubecost.Allocation]{
							Field: kubecost.AllocationNamespaceProp,
							Op:    filter.StringEquals,
							Value: "ns2",
						},
						filter.StringProperty[*kubecost.Allocation]{
							Field: kubecost.AllocationNamespaceProp,
							Op:    filter.StringEquals,
							Value: "ns1",
						},
					},
				},
			}},
			expected: true,
		},
		"AndFilled3": {
			left: filter.And[*kubecost.Allocation]{Filters: []filter.Filter[*kubecost.Allocation]{
				filter.AllCut[*kubecost.Allocation]{},
				filter.StringMapProperty[*kubecost.Allocation]{
					Field: kubecost.AllocationLabelProp,
					Op:    filter.StringStartsWith,
					Key:   "xyz",
					Value: "kubecost",
				},
				filter.Or[*kubecost.Allocation]{
					Filters: []filter.Filter[*kubecost.Allocation]{
						filter.StringProperty[*kubecost.Allocation]{
							Field: kubecost.AllocationNamespaceProp,
							Op:    filter.StringEquals,
							Value: "ns1",
						},
						filter.StringProperty[*kubecost.Allocation]{
							Field: kubecost.AllocationNamespaceProp,
							Op:    filter.StringEquals,
							Value: "ns2",
						},
					},
				},
			}},
			right: filter.And[*kubecost.Allocation]{Filters: []filter.Filter[*kubecost.Allocation]{
				filter.Or[*kubecost.Allocation]{
					Filters: []filter.Filter[*kubecost.Allocation]{
						filter.StringProperty[*kubecost.Allocation]{
							Field: kubecost.AllocationNamespaceProp,
							Op:    filter.StringEquals,
							Value: "ns1",
						},
						filter.StringProperty[*kubecost.Allocation]{
							Field: kubecost.AllocationNamespaceProp,
							Op:    filter.StringEquals,
							Value: "ns2",
						},
					},
				},
				filter.AllCut[*kubecost.Allocation]{},
				filter.StringMapProperty[*kubecost.Allocation]{
					Field: kubecost.AllocationLabelProp,
					Op:    filter.StringStartsWith,
					Key:   "xyz",
					Value: "kubecost",
				},
			}},
			expected: true,
		},
		"AndFilled4": {
			left: filter.And[*kubecost.Allocation]{Filters: []filter.Filter[*kubecost.Allocation]{
				filter.AllCut[*kubecost.Allocation]{},
				filter.StringMapProperty[*kubecost.Allocation]{
					Field: kubecost.AllocationLabelProp,
					Op:    filter.StringStartsWith,
					Key:   "xyz",
					Value: "kubecost",
				},
				filter.Or[*kubecost.Allocation]{
					Filters: []filter.Filter[*kubecost.Allocation]{
						filter.StringProperty[*kubecost.Allocation]{
							Field: kubecost.AllocationNamespaceProp,
							Op:    filter.StringEquals,
							Value: "ns1",
						},
						filter.StringProperty[*kubecost.Allocation]{
							Field: kubecost.AllocationNamespaceProp,
							Op:    filter.StringEquals,
							Value: "ns2",
						},
					},
				},
			}},
			right: filter.And[*kubecost.Allocation]{Filters: []filter.Filter[*kubecost.Allocation]{
				filter.AllCut[*kubecost.Allocation]{},
				filter.StringMapProperty[*kubecost.Allocation]{
					Field: kubecost.AllocationLabelProp,
					Op:    filter.StringStartsWith,
					Key:   "xyz",
					Value: "kubecost",
				},
				filter.Or[*kubecost.Allocation]{
					Filters: []filter.Filter[*kubecost.Allocation]{
						filter.StringProperty[*kubecost.Allocation]{
							Field: kubecost.AllocationNamespaceProp,
							Op:    filter.StringEquals,
							Value: "ns3",
						},
						filter.StringProperty[*kubecost.Allocation]{
							Field: kubecost.AllocationNamespaceProp,
							Op:    filter.StringEquals,
							Value: "ns2",
						},
					},
				},
			}},
			expected: true,
		},
		"AndFilled5": {
			left: filter.And[*kubecost.Allocation]{Filters: []filter.Filter[*kubecost.Allocation]{
				filter.StringMapProperty[*kubecost.Allocation]{
					Field: kubecost.AllocationLabelProp,
					Op:    filter.StringStartsWith,
					Key:   "xyz",
					Value: "kubecost",
				},
				filter.Or[*kubecost.Allocation]{
					Filters: []filter.Filter[*kubecost.Allocation]{
						filter.StringProperty[*kubecost.Allocation]{
							Field: kubecost.AllocationNamespaceProp,
							Op:    filter.StringEquals,
							Value: "ns1",
						},
						filter.StringProperty[*kubecost.Allocation]{
							Field: kubecost.AllocationNamespaceProp,
							Op:    filter.StringEquals,
							Value: "ns2",
						},
					},
				},
			}},
			right: filter.And[*kubecost.Allocation]{Filters: []filter.Filter[*kubecost.Allocation]{
				filter.StringMapProperty[*kubecost.Allocation]{
					Field: kubecost.AllocationLabelProp,
					Op:    filter.StringStartsWith,
					Key:   "xyz",
					Value: "kubecost",
				},
				filter.Or[*kubecost.Allocation]{
					Filters: []filter.Filter[*kubecost.Allocation]{
						filter.StringProperty[*kubecost.Allocation]{
							Field: kubecost.AllocationNamespaceProp,
							Op:    filter.StringEquals,
							Value: "ns3",
						},
						filter.StringProperty[*kubecost.Allocation]{
							Field: kubecost.AllocationNamespaceProp,
							Op:    filter.StringEquals,
							Value: "ns2",
						},
					},
				},
			}},
			expected: false,
		},
		"AndFilled6": {
			left: filter.And[*kubecost.Allocation]{Filters: []filter.Filter[*kubecost.Allocation]{
				filter.AllCut[*kubecost.Allocation]{},
				filter.StringMapProperty[*kubecost.Allocation]{
					Field: kubecost.AllocationLabelProp,
					Op:    filter.StringStartsWith,
					Key:   "xyz",
					Value: "kubecost",
				},
				filter.Or[*kubecost.Allocation]{
					Filters: []filter.Filter[*kubecost.Allocation]{
						filter.StringProperty[*kubecost.Allocation]{
							Field: kubecost.AllocationNamespaceProp,
							Op:    filter.StringEquals,
							Value: "ns1",
						},
						filter.StringProperty[*kubecost.Allocation]{
							Field: kubecost.AllocationNamespaceProp,
							Op:    filter.StringEquals,
							Value: "ns2",
						},
					},
				},
			}},
			right: filter.And[*kubecost.Allocation]{Filters: []filter.Filter[*kubecost.Allocation]{
				filter.StringMapProperty[*kubecost.Allocation]{
					Field: kubecost.AllocationLabelProp,
					Op:    filter.StringStartsWith,
					Key:   "xyz",
					Value: "kubecost",
				},
				filter.Or[*kubecost.Allocation]{
					Filters: []filter.Filter[*kubecost.Allocation]{
						filter.StringProperty[*kubecost.Allocation]{
							Field: kubecost.AllocationNamespaceProp,
							Op:    filter.StringEquals,
							Value: "ns1",
						},
						filter.StringProperty[*kubecost.Allocation]{
							Field: kubecost.AllocationNamespaceProp,
							Op:    filter.StringEquals,
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
			if filter.Equals(c.left, c.right) != c.expected {
				t.Fatalf("'%s' = '%s' \nExpected: %t", c.left, c.right, c.expected)
			}
		})
	}
}
