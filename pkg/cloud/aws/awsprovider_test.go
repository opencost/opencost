package aws

import (
	"testing"

	"github.com/opencost/opencost/pkg/cloud"
)

func Test_awsKey_getUsageType(t *testing.T) {
	type fields struct {
		Labels     map[string]string
		ProviderID string
	}
	type args struct {
		labels map[string]string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   string
	}{
		{
			// test with no labels should return false
			name: "Label does not have the capacityType label associated with it",
			args: args{
				labels: map[string]string{},
			},
			want: "",
		},
		{
			name: "EKS label with a capacityType set to empty string should return empty string",
			args: args{
				labels: map[string]string{
					EKSCapacityTypeLabel: "",
				},
			},
			want: "",
		},
		{
			name: "EKS label with capacityType set to a random value should return empty string",
			args: args{
				labels: map[string]string{
					EKSCapacityTypeLabel: "TEST_ME",
				},
			},
			want: "",
		},
		{
			name: "EKS label with capacityType set to spot should return spot",
			args: args{
				labels: map[string]string{
					EKSCapacityTypeLabel: EKSCapacitySpotTypeValue,
				},
			},
			want: PreemptibleType,
		},
		{
			name: "Karpenter label with a capacityType set to empty string should return empty string",
			args: args{
				labels: map[string]string{
					cloud.KarpenterCapacityTypeLabel: "",
				},
			},
			want: "",
		},
		{
			name: "Karpenter label with capacityType set to a random value should return empty string",
			args: args{
				labels: map[string]string{
					cloud.KarpenterCapacityTypeLabel: "TEST_ME",
				},
			},
			want: "",
		},
		{
			name: "Karpenter label with capacityType set to spot should return spot",
			args: args{
				labels: map[string]string{
					cloud.KarpenterCapacityTypeLabel: cloud.KarpenterCapacitySpotTypeValue,
				},
			},
			want: PreemptibleType,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			k := &awsKey{
				Labels:     tt.fields.Labels,
				ProviderID: tt.fields.ProviderID,
			}
			if got := k.getUsageType(tt.args.labels); got != tt.want {
				t.Errorf("getUsageType() = %v, want %v", got, tt.want)
			}
		})
	}
}
