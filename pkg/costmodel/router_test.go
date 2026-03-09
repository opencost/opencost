package costmodel

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/julienschmidt/httprouter"
	"github.com/opencost/opencost/core/pkg/clusters"
	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/pkg/env"
)

// MockClusterMap implements clusters.ClusterMap for testing
type MockClusterMap struct {
	clusters map[string]*clusters.ClusterInfo
}

func (m *MockClusterMap) GetClusterIDs() []string {
	ids := make([]string, 0, len(m.clusters))
	for id := range m.clusters {
		ids = append(ids, id)
	}
	return ids
}

func (m *MockClusterMap) AsMap() map[string]*clusters.ClusterInfo {
	return m.clusters
}

func (m *MockClusterMap) InfoFor(clusterID string) *clusters.ClusterInfo {
	return m.clusters[clusterID]
}

func (m *MockClusterMap) NameFor(clusterID string) string {
	if info := m.clusters[clusterID]; info != nil {
		return info.Name
	}
	return ""
}

func (m *MockClusterMap) NameIDFor(clusterID string) string {
	if info := m.clusters[clusterID]; info != nil {
		return info.Name + "/" + info.ID
	}
	return ""
}

func (m *MockClusterMap) StopRefresh() {}

// MockCostModel implements the CostModel interface for testing
type MockCostModel struct {
	clusterMap clusters.ClusterMap
}

func (m *MockCostModel) ClusterMap() clusters.ClusterMap {
	return m.clusterMap
}

func TestGetClusterStatus(t *testing.T) {
	// Create mock cluster map with test data
	mockClusters := map[string]*clusters.ClusterInfo{
		"cluster-1": {
			ID:   "cluster-1",
			Name: "production-cluster",
		},
		"cluster-2": {
			ID:   "cluster-2",
			Name: "staging-cluster",
		},
	}

	mockClusterMap := &MockClusterMap{clusters: mockClusters}
	mockModel := &MockCostModel{clusterMap: mockClusterMap}

	// Create router and add handler
	router := httprouter.New()
	accesses := &Accesses{Model: mockModel}
	router.GET("/clusterStatus", accesses.GetClusterStatus)

	// Create test request
	req, err := http.NewRequest("GET", "/clusterStatus", nil)
	if err != nil {
		t.Fatal(err)
	}

	// Create response recorder
	rr := httptest.NewRecorder()

	// Set test environment variables
	env.Set("CURRENT_CLUSTER_ID_FILTER_ENABLED", "false")
	env.Set("PROM_CLUSTER_ID_LABEL", "cluster_id")
	env.Set("CLUSTER_ID", "cluster-1")

	// Serve request
	router.ServeHTTP(rr, req)

	// Check status code
	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got %v want %v", status, http.StatusOK)
	}

	// Parse response
	var response map[string]interface{}
	if err := json.Unmarshal(rr.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}

	// Verify response fields
	expectedFields := []string{
		"currentClusterID",
		"clusterFilterEnabled",
		"clusterLabel",
		"totalClustersDiscovered",
		"availableClusters",
		"clusterDetails",
		"multiClusterCapable",
	}

	for _, field := range expectedFields {
		if _, exists := response[field]; !exists {
			t.Errorf("response missing expected field: %s", field)
		}
	}

	// Verify specific values
	if response["totalClustersDiscovered"] != float64(2) {
		t.Errorf("expected 2 clusters, got %v", response["totalClustersDiscovered"])
	}

	if response["clusterFilterEnabled"] != false {
		t.Errorf("expected cluster filtering to be disabled, got %v", response["clusterFilterEnabled"])
	}

	if response["multiClusterCapable"] != true {
		t.Errorf("expected multi-cluster capable to be true, got %v", response["multiClusterCapable"])
	}

	// Verify available clusters
	availableClusters, ok := response["availableClusters"].([]interface{})
	if !ok {
		t.Fatal("availableClusters is not an array")
	}

	if len(availableClusters) != 2 {
		t.Errorf("expected 2 available clusters, got %d", len(availableClusters))
	}

	// Check that both cluster IDs are present
	clusterIDs := make(map[string]bool)
	for _, cluster := range availableClusters {
		clusterIDs[cluster.(string)] = true
	}

	if !clusterIDs["cluster-1"] || !clusterIDs["cluster-2"] {
		t.Error("expected cluster-1 and cluster-2 to be in available clusters")
	}
} 