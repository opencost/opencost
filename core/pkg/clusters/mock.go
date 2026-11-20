package clusters

type MockClusterInfoProvider struct {
	clusterInfo map[string]string
}

func NewMockClusterInfoProvider(clusterInfo map[string]string) *MockClusterInfoProvider {
	return &MockClusterInfoProvider{clusterInfo: clusterInfo}
}

func (m *MockClusterInfoProvider) GetClusterInfo() map[string]string {
	return m.clusterInfo
}
