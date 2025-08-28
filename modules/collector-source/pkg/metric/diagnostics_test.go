package metric

import (
	"fmt"
	"testing"
	"time"

	"github.com/kubecost/events"
	"github.com/opencost/opencost/modules/collector-source/pkg/event"
)

// MockUpdater implements the Updater interface for testing
type MockUpdater struct {
}

func (m *MockUpdater) Update(updateSet *UpdateSet) {
}

// Test Update func in DiagnosticsModule and check if diagnostics pass
func TestDiagnosticsModule_Update(t *testing.T) {
	mockUpdater := &MockUpdater{}
	module := NewDiagnosticsModule(mockUpdater)

	// Test with valid update set containing node metrics
	timestamp := time.Now()
	updateSet := &UpdateSet{
		Timestamp: timestamp,
		Updates: []Update{
			{
				Name:  KubeNodeStatusCapacityCPUCores,
				Value: 4.0,
			},
			{
				Name:  NodeTotalHourlyCost,
				Value: 0.50,
			},
		},
	}

	module.Update(updateSet)

	// Check both diagnostics
	nodeDetails, err := module.DiagnosticsDetails(NodesDiagnosticMetricID)
	if err != nil {
		t.Error("Expected no error for valid diagnostic ID")
	}
	if nodeDetails["passed"] != true {
		t.Error("Expected node diagnostic to pass")
	}

	opencostDetails, err := module.DiagnosticsDetails(OpencostDiagnosticMetricID)
	if err != nil {
		t.Error("Expected no error for valid diagnostic ID")
	}
	if opencostDetails["passed"] != true {
		t.Error("Expected kubecost diagnostic to pass")
	}
}

func TestDiagnosticsModule_ScrapeDiagnostics(t *testing.T) {
	mockUpdater := &MockUpdater{}
	module := NewDiagnosticsModule(mockUpdater)

	// dispatch some faux scrape events
	events.Dispatch(event.ScrapeEvent{
		ScraperName: event.NetworkCostsScraperName,
		Targets:     10,
		Errors:      []error{},
	})

	events.Dispatch(event.ScrapeEvent{
		ScraperName: event.KubernetesClusterScraperName,
		ScrapeType:  event.NodeScraperType,
		Targets:     8,
		Errors: []error{
			fmt.Errorf("failed to scrape node 'foo'"),
			fmt.Errorf("failed to scrape node 'bar'"),
		},
	})

	time.Sleep(500 * time.Millisecond)

	networkDiagnosticDetails, err := module.DiagnosticsDetails(NetworkCostsScraperDiagnosticID)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
		return
	}

	stats := networkDiagnosticDetails["stats"].(map[string]any)
	errors := networkDiagnosticDetails["errors"].([]string)
	label := networkDiagnosticDetails["label"].(string)

	statsTotal := stats["total"].(int)
	statsSuccess := stats["success"].(int)
	statsFail := stats["fail"].(int)

	if statsTotal != 10 {
		t.Fatalf("expected networkCostsDetails[\"stats\"][\"total\"] to equal 10, got: %d", statsTotal)
		return
	}
	if statsSuccess != 10 {
		t.Fatalf("expected networkCostsDetails[\"stats\"][\"success\"] to equal 10, got: %d", statsSuccess)
		return
	}
	if statsFail != 0 {
		t.Fatalf("expected networkCostsDetails[\"stats\"][\"fail\"] to equal 0, got: %d", statsFail)
		return
	}

	if len(errors) != 0 {
		t.Fatalf("expected len(networkCostsDetails[\"errors\"]) to equal 0, got: %d", len(errors))
		return
	}

	if len(label) == 0 {
		t.Fatalf("expected len(networkCostsDetails[\"label\"]) to be non-zero. Got 0.")
		return
	}

	nodeScrapeDetails, err := module.DiagnosticsDetails(KubernetesNodesScraperDiagnosticID)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
		return
	}

	stats = nodeScrapeDetails["stats"].(map[string]any)
	errors = nodeScrapeDetails["errors"].([]string)
	label = nodeScrapeDetails["label"].(string)

	statsTotal = stats["total"].(int)
	statsSuccess = stats["success"].(int)
	statsFail = stats["fail"].(int)

	if statsTotal != 8 {
		t.Fatalf("expected nodeScrapeDetails[\"stats\"][\"total\"] to equal 8, got: %d", statsTotal)
		return
	}
	if statsSuccess != 6 {
		t.Fatalf("expected nodeScrapeDetails[\"stats\"][\"success\"] to equal 6, got: %d", statsSuccess)
		return
	}
	if statsFail != 2 {
		t.Fatalf("expected nodeScrapeDetails[\"stats\"][\"fail\"] to equal 2, got: %d", statsFail)
		return
	}

	if len(errors) != 2 {
		t.Fatalf("expected len(nodeScrapeDetails[\"errors\"]) to equal 2, got: %d", len(errors))
		return
	}

	if len(label) == 0 {
		t.Fatalf("expected len(nodeScrapeDetails[\"label\"]) to be non-zero. Got 0.")
		return
	}
}

// Test Update func in DiagnosticsModule with missing metrics and test if diagnostics fail
func TestDiagnosticsModule_UpdateWithMissingMetrics(t *testing.T) {
	mockUpdater := &MockUpdater{}
	module := NewDiagnosticsModule(mockUpdater)

	timestamp := time.Now()
	updateSet := &UpdateSet{
		Timestamp: timestamp,
		Updates: []Update{
			{
				Name:  "some_other_metric",
				Value: 1.0,
			},
		},
	}

	module.Update(updateSet)

	// Check that diagnostics fail when their metrics are missing
	nodeDetails, err := module.DiagnosticsDetails(NodesDiagnosticMetricID)
	if err != nil {
		t.Error("Expected no error for valid diagnostic ID")
	}
	if nodeDetails["passed"] != false {
		t.Error("Expected node diagnostic to fail when metric is missing")
	}

	kubecostDetails, err := module.DiagnosticsDetails(OpencostDiagnosticMetricID)
	if err != nil {
		t.Error("Expected no error for valid diagnostic ID")
	}
	if kubecostDetails["passed"] != false {
		t.Error("Expected kubecost diagnostic to fail when metric is missing")
	}
}

// Test DiagnosticsDetails func in DiagnosticsModule with invalid and valid diagnostic IDs
func TestDiagnosticsModule_DiagnosticsDetails(t *testing.T) {
	mockUpdater := &MockUpdater{}
	module := NewDiagnosticsModule(mockUpdater)

	// Test with invalid diagnostic ID
	_, err := module.DiagnosticsDetails("invalid_id")
	if err.Error() != "invalid diagnostic id: invalid_id not found" {
		t.Error("Expected error for invalid diagnostic ID")
	}

	// Test with valid diagnostic ID
	details, err := module.DiagnosticsDetails(NodesDiagnosticMetricID)
	if err != nil {
		t.Error("Expected no error for valid diagnostic ID")
	}
	if details["error"] != nil {
		t.Error("Expected no error for valid diagnostic ID")
	}

	// Check required fields
	requiredFields := []string{"query", "label", "result", "passed", "docLink"}
	for _, field := range requiredFields {
		if details[field] == nil {
			t.Errorf("Expected field %s to be present", field)
		}
	}
}

// Test concurrent access(race condition) to DiagnosticsModule
func TestDiagnosticsModule_ConcurrentAccess(t *testing.T) {
	mockUpdater := &MockUpdater{}
	module := NewDiagnosticsModule(mockUpdater)

	// Test concurrent access to diagnostics
	done := make(chan bool, 2)

	go func() {
		for i := 0; i < 100; i++ {
			module.DiagnosticsDefinitions()
		}
		done <- true
	}()

	go func() {
		for i := 0; i < 100; i++ {
			timestamp := time.Now()
			updateSet := &UpdateSet{
				Timestamp: timestamp,
				Updates: []Update{
					{
						Name:  KubeNodeStatusCapacityCPUCores,
						Value: float64(i),
					},
				},
			}
			module.Update(updateSet)
		}
		done <- true
	}()

	<-done
	<-done
	// If we get here without a race condition, the test passes
}

// Test reset of diagnostics after details are retrieved
func TestDiagnosticsModule_ResetAfterDetails(t *testing.T) {
	mockUpdater := &MockUpdater{}
	module := NewDiagnosticsModule(mockUpdater)

	// Add some data
	timestamp := time.Now()
	updateSet := &UpdateSet{
		Timestamp: timestamp,
		Updates: []Update{
			{
				Name:  KubeNodeStatusCapacityCPUCores,
				Value: 4.0,
			},
		},
	}

	module.Update(updateSet)

	// Get details (this should reset the diagnostic)
	details, err := module.DiagnosticsDetails(NodesDiagnosticMetricID)
	if err != nil {
		t.Error("Expected no error for valid diagnostic ID")
	}
	if details["passed"] != true {
		t.Error("Expected diagnostic to pass before reset")
	}

	// Get details again (should be reset)
	details2, err := module.DiagnosticsDetails(NodesDiagnosticMetricID)
	if err != nil {
		t.Error("Expected no error for valid diagnostic ID")
	}
	if details2["passed"] != false {
		t.Error("Expected diagnostic to be reset after first details call")
	}
}
