package metric

import (
	"testing"
	"time"
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

	kubecostDetails, err := module.DiagnosticsDetails(KubecostDiagnosticMetricID)
	if err != nil {
		t.Error("Expected no error for valid diagnostic ID")
	}
	if kubecostDetails["passed"] != true {
		t.Error("Expected kubecost diagnostic to pass")
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

	kubecostDetails, err := module.DiagnosticsDetails(KubecostDiagnosticMetricID)
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
	if err.Error() != "diagnostic ID: invalid_id not found" {
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
