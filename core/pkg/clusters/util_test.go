package clusters

import "testing"

const (
	testClusterInfoIDKey      = "testClusterID"
	testClusterInfoNameKey    = "testClusterName"
	testClusterProfileKey     = "testProfile"
	testClusterProviderKey    = "testProvider"
	testClusterAccountKey     = "testAccount"
	testClusterProjectKey     = "testProject"
	testClusterRegionKey      = "testRegion"
	testClusterProvisionerKey = "testProvisioner"
	testClusterVersionKey     = "testVersion"
)

func TestMapToClusterInfo(t *testing.T) {
	mapWOVersion := map[string]string{
		ClusterInfoIdKey:          testClusterInfoIDKey,
		ClusterInfoNameKey:        testClusterInfoNameKey,
		ClusterInfoProfileKey:     testClusterProfileKey,
		ClusterInfoProviderKey:    testClusterProviderKey,
		ClusterInfoAccountKey:     testClusterAccountKey,
		ClusterInfoProjectKey:     testClusterProjectKey,
		ClusterInfoRegionKey:      testClusterRegionKey,
		ClusterInfoProvisionerKey: testClusterProvisionerKey,
	}
	expectedCIwoVersion := ClusterInfo{
		ID:          testClusterInfoIDKey,
		Name:        testClusterInfoNameKey,
		Profile:     testClusterProfileKey,
		Provider:    testClusterProviderKey,
		Account:     testClusterAccountKey,
		Project:     testClusterProjectKey,
		Region:      testClusterRegionKey,
		Provisioner: testClusterProvisionerKey,
	}
	mapWVersion := map[string]string{
		ClusterInfoIdKey:          testClusterInfoIDKey,
		ClusterInfoNameKey:        testClusterInfoNameKey,
		ClusterInfoProfileKey:     testClusterProfileKey,
		ClusterInfoProviderKey:    testClusterProviderKey,
		ClusterInfoAccountKey:     testClusterAccountKey,
		ClusterInfoProjectKey:     testClusterProjectKey,
		ClusterInfoRegionKey:      testClusterRegionKey,
		ClusterInfoProvisionerKey: testClusterProvisionerKey,
		ClusterInfoVersionKey:     testClusterVersionKey,
	}
	expectedCIwVersion := ClusterInfo{
		ID:          testClusterInfoIDKey,
		Name:        testClusterInfoNameKey,
		Profile:     testClusterProfileKey,
		Provider:    testClusterProviderKey,
		Account:     testClusterAccountKey,
		Project:     testClusterProjectKey,
		Region:      testClusterRegionKey,
		Provisioner: testClusterProvisionerKey,
		Version:     testClusterVersionKey,
	}
	tests := []struct {
		name     string
		input    map[string]string
		expected ClusterInfo
		wantErr  bool
	}{
		{
			name:     "when version is not in the cluster info map",
			input:    mapWOVersion,
			expected: expectedCIwoVersion,
			wantErr:  false,
		},
		{
			name:     "when version is in the cluster info map",
			input:    mapWVersion,
			expected: expectedCIwVersion,
			wantErr:  false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			returnCI, err := MapToClusterInfo(tc.input)
			if (err != nil) != tc.wantErr {
				t.Errorf("MapToClusterInfo() error = %v, wantErr %v", err, tc.wantErr)
				return
			}
			if *returnCI != tc.expected {
				t.Errorf("MapToClusterInfo() expected = %v, got %v", tc.expected, returnCI)
				return
			}
		})
	}
}
