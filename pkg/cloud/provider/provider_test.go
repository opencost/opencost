package provider

import (
	"testing"

	"github.com/opencost/opencost/core/pkg/opencost"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestParseLocalDiskID(t *testing.T) {
	tests := map[string]struct {
		input string
		want  string
	}{
		"empty string": {
			input: "",
			want:  "",
		},
		"generic string": {
			input: "test",
			want:  "test",
		},
		"AWS node provider id": {
			input: "aws:///us-east-2a/i-0fea4fd46592d050b",
			want:  "i-0fea4fd46592d050b",
		},
		"GCP node provider id": {
			input: "gce://guestbook-11111/us-central1-a/gke-niko-n1-standard-2-wlkla-8d48e58a-hfy7",
			want:  "gke-niko-n1-standard-2-wlkla-8d48e58a-hfy7",
		},
		"Azure vmss provider id": {
			input: "azure:///subscriptions/ae337b64-e7ba-3387-b043-187289efe4e3/resourceGroups/mc_test_eastus2/providers/Microsoft.Compute/virtualMachineScaleSets/aks-userpool-12345678-vmss/virtualMachines/11",
			want:  "azure:///subscriptions/ae337b64-e7ba-3387-b043-187289efe4e3/resourcegroups/mc_test_eastus2/providers/microsoft.compute/disks/aks-userpool-12345678-vmss00000b_osdisk",
		},
		"Azure vm provider id": {
			input: "azure:///subscriptions/ae337b64-e7ba-3387-b043-187289efe4e3/resourceGroups/mc_test_eastus2/providers/Microsoft.Compute/virtualMachines/master-0",
			want:  "azure:///subscriptions/ae337b64-e7ba-3387-b043-187289efe4e3/resourcegroups/mc_test_eastus2/providers/microsoft.compute/disks/master-0_osdisk",
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			if got := ParseLocalDiskID(tt.input); got != tt.want {
				t.Errorf("ParseLocalDiskID() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_getClusterProperties(t *testing.T) {
	type testCase struct {
		name string
		node *v1.Node
		exp  clusterProperties
	}

	testCases := []testCase{
		{
			name: "empty = default",
			node: &v1.Node{
				Spec: v1.NodeSpec{},
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{},
				},
			},
			exp: clusterProperties{
				provider:       "DEFAULT",
				configFileName: "default.json",
			},
		},
		{
			name: "AWS by provider ID",
			node: &v1.Node{
				Spec: v1.NodeSpec{
					ProviderID: "aws:///us-east-2a/i-0d907d7e1918849fc",
				},
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{},
				},
			},
			exp: clusterProperties{
				provider:       "AWS",
				configFileName: "aws.json",
			},
		},
		{
			name: "Azure by provider ID",
			node: &v1.Node{
				Spec: v1.NodeSpec{
					ProviderID: "azure:///subscriptions/3hd721c9-d305-43fd-8130-a0hfh37jja21/resourceGroups/mc_default-001_aks-tool-prd-useast-001_useast/providers/Microsoft.Compute/virtualMachineScaleSets/aks-default-92337122-vmss/virtualMachines/2",
				},
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{},
				},
			},
			exp: clusterProperties{
				provider:       "Azure",
				configFileName: "azure.json",
				accountID:      "3hd721c9-d305-43fd-8130-a0hfh37jja21",
			},
		},
		{
			name: "Google by provider ID",
			node: &v1.Node{
				Spec: v1.NodeSpec{
					ProviderID: "gce://guestbook-227502/us-central1-a/gke-kc-integration-t-recommended-by-k-58c36f92-4i62",
				},
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{},
				},
			},
			exp: clusterProperties{
				provider:       "GCP",
				configFileName: "gcp.json",
				projectID:      "guestbook-227502",
			},
		},
		{
			name: "Scaleway by provider ID",
			node: &v1.Node{
				Spec: v1.NodeSpec{
					ProviderID: "scaleway://something/something",
				},
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{},
				},
			},
			exp: clusterProperties{
				provider:       opencost.ScalewayProvider,
				configFileName: "scaleway.json",
			},
		},
		{
			name: "Alibaba by kubelet version",
			node: &v1.Node{
				Status: v1.NodeStatus{
					NodeInfo: v1.NodeSystemInfo{
						KubeletVersion: "something-aliyun-version-97",
					},
				},
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{},
				},
			},
			exp: clusterProperties{
				provider:       opencost.AlibabaProvider,
				configFileName: "alibaba.json",
			},
		},
		{
			name: "Oracle by providerId",
			node: &v1.Node{
				Spec: v1.NodeSpec{
					ProviderID: "ocid://something/something",
				},
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{},
				},
			},
			exp: clusterProperties{
				provider:       opencost.OracleProvider,
				configFileName: "oracle.json",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cp := getClusterProperties(tc.node)
			if !cp.Equal(tc.exp) {
				t.Errorf("expected %+v; received %+v", tc.exp, cp)
			}
		})
	}
}
