package clustercache

import (
	"testing"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

func containerWithCPU(name, cpu string, restartPolicy *v1.ContainerRestartPolicy) v1.Container {
	return v1.Container{
		Name:          name,
		RestartPolicy: restartPolicy,
		Resources: v1.ResourceRequirements{
			Requests: v1.ResourceList{
				v1.ResourceCPU: resource.MustParse(cpu),
			},
		},
	}
}

// Native sidecars are init containers with restartPolicy: Always. Kubernetes sums their
// requests into the pod's requests like a regular container, so they must be carried into
// the cache and costed. Ordinary init containers must not be, or every pod that runs one
// would be over-charged for the whole life of the pod.
func TestTransformPodSpecIncludesNativeSidecars(t *testing.T) {
	always := v1.ContainerRestartPolicyAlways
	onFailure := v1.ContainerRestartPolicyOnFailure

	tests := []struct {
		name     string
		spec     v1.PodSpec
		expected []string
	}{
		{
			name: "regular containers only",
			spec: v1.PodSpec{
				Containers: []v1.Container{containerWithCPU("app", "100m", nil)},
			},
			expected: []string{"app"},
		},
		{
			name: "ordinary init container is excluded",
			spec: v1.PodSpec{
				InitContainers: []v1.Container{containerWithCPU("setup", "500m", nil)},
				Containers:     []v1.Container{containerWithCPU("app", "100m", nil)},
			},
			expected: []string{"app"},
		},
		{
			name: "native sidecar is included after the regular containers",
			spec: v1.PodSpec{
				InitContainers: []v1.Container{containerWithCPU("istio-proxy", "200m", &always)},
				Containers:     []v1.Container{containerWithCPU("app", "100m", nil)},
			},
			expected: []string{"app", "istio-proxy"},
		},
		{
			name: "restartPolicy OnFailure is not a sidecar",
			spec: v1.PodSpec{
				InitContainers: []v1.Container{containerWithCPU("setup", "500m", &onFailure)},
				Containers:     []v1.Container{containerWithCPU("app", "100m", nil)},
			},
			expected: []string{"app"},
		},
		{
			name: "sidecars and ordinary init containers mixed",
			spec: v1.PodSpec{
				InitContainers: []v1.Container{
					containerWithCPU("setup", "500m", nil),
					containerWithCPU("istio-proxy", "200m", &always),
					containerWithCPU("log-shipper", "50m", &always),
				},
				Containers: []v1.Container{containerWithCPU("app", "100m", nil)},
			},
			expected: []string{"app", "istio-proxy", "log-shipper"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := TransformPodSpec(tt.spec)
			if len(got.Containers) != len(tt.expected) {
				t.Fatalf("got %d containers %v, want %d %v", len(got.Containers), containerNames(got.Containers), len(tt.expected), tt.expected)
			}
			for i := range tt.expected {
				if got.Containers[i].Name != tt.expected[i] {
					t.Errorf("index %d: got %q, want %q", i, got.Containers[i].Name, tt.expected[i])
				}
			}
		})
	}
}

// The sidecar's resource requests have to survive the transform, otherwise it is carried
// into the cache but still costed at zero.
func TestTransformPodSpecKeepsSidecarRequests(t *testing.T) {
	always := v1.ContainerRestartPolicyAlways

	got := TransformPodSpec(v1.PodSpec{
		InitContainers: []v1.Container{containerWithCPU("istio-proxy", "200m", &always)},
		Containers:     []v1.Container{containerWithCPU("app", "100m", nil)},
	})

	if len(got.Containers) != 2 {
		t.Fatalf("got %d containers, want 2", len(got.Containers))
	}
	sidecar := got.Containers[1]
	want := resource.MustParse("200m")
	if cpu := sidecar.Resources.Requests[v1.ResourceCPU]; cpu.Cmp(want) != 0 {
		t.Errorf("sidecar cpu request: got %s, want %s", cpu.String(), want.String())
	}
}

func containerNames(containers []Container) []string {
	names := make([]string, len(containers))
	for i, c := range containers {
		names[i] = c.Name
	}
	return names
}
