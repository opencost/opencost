package metrics

import (
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"testing"
)



func TestToResourceUnitValue(t *testing.T){
	testCases := []struct {
		name         string
		resourceName v1.ResourceName
		quantity     resource.Quantity
		wantResource string
		wantUnit     string
		wantValue    float64
	}{
		{
			name:         "CPU converts millicores to cores",
			resourceName: v1.ResourceCPU,
			quantity:     resource.MustParse("500m"),
			wantResource: "cpu",
			wantUnit:     "core",
			wantValue:    0.5,
		},
		{
			name:         "Memory converts to bytes",
			resourceName: v1.ResourceMemory,
			quantity:     resource.MustParse("1Ki"),
			wantResource: "memory",
			wantUnit:     "byte",
			wantValue:    1024,
		},
		{
			name:         "NVIDIA GPU sanitizes slash and dot to underscore",
			resourceName: v1.ResourceName("nvidia.com/gpu"),
			quantity:     resource.MustParse("2"),
			wantResource: "nvidia_com_gpu",
			wantUnit:     "integer",
			wantValue:    2,
		},
		{
			name:         "NVIDIA shared GPU sanitizes correctly",
			resourceName: v1.ResourceName("nvidia.com/gpu.shared"),
			quantity:     resource.MustParse("1"),
			wantResource: "nvidia_com_gpu_shared",
			wantUnit:     "integer",
			wantValue:    1,
		},
		{
			name:         "Huge pages converts hyphens to underscores",
			resourceName: v1.ResourceName(v1.ResourceHugePagesPrefix + "2Mi"),
			quantity:     resource.MustParse("4Ki"),
			wantResource: "hugepages_2Mi",
			wantUnit:     "byte",
			wantValue:    4096,
		},
		{
			name:         "Unrecognized kubernetes.io native resource returns empty",
			resourceName: v1.ResourceName("kubernetes.io/unknown"),
			quantity:     resource.MustParse("1"),
			wantResource: "",
			wantUnit:     "",
			wantValue:    0,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			gotResource, gotUnit, gotValue := toResourceUnitValue(tc.resourceName, tc.quantity)
			if gotResource != tc.wantResource {
				t.Errorf("resource: got %q, want %q", gotResource, tc.wantResource)
			}
			if gotUnit != tc.wantUnit {
				t.Errorf("unit: got %q, want %q", gotUnit, tc.wantUnit)
			}
			if gotValue != tc.wantValue {
				t.Errorf("value: got %v, want %v", gotValue, tc.wantValue)
			}
		})
	}
}