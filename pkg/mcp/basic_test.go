package mcp

import (
	"encoding/json"
	"testing"
	"time"
)

// test basic struct functionality
func TestBasicStructs(t *testing.T) {
	// test conversation state
	state := ConversationState{
		SessionID:     "test-123",
		CreatedAt:     time.Now(),
		CurrentDomain: "allocations",
		ActiveFilters: map[string]string{
			"namespace": "production",
		},
	}

	if state.SessionID != "test-123" {
		t.Errorf("Expected SessionID 'test-123', got %s", state.SessionID)
	}

	// Test allocation query struct
	query := AllocationQuery{
		Window:         "7d",
		Aggregate:      []string{"namespace"},
		BusinessIntent: "cost_optimization",
	}

	// test json serialization
	data, err := json.Marshal(query)
	if err != nil {
		t.Errorf("Failed to marshal AllocationQuery: %v", err)
	}

	// basic validation that we have some data
	if len(data) == 0 {
		t.Error("JSON marshaling produced empty result")
	}
}

// test tool creation works
func TestToolCreation(t *testing.T) {
	tool := NewAllocationTool()

	if tool.Name != "query_allocations" {
		t.Errorf("Expected tool name 'query_allocations', got %s", tool.Name)
	}

	if tool.Description == "" {
		t.Error("Tool description should not be empty")
	}
}

// verify our mcp response structure
func TestMCPResponse(t *testing.T) {
	response := MCPResponse{
		Data: map[string]interface{}{
			"allocations": []string{"test"},
		},
		QueryType:  "allocations",
		ExecutedAt: time.Now(),
	}

	// test json conversion
	_, err := json.Marshal(response)
	if err != nil {
		t.Errorf("MCPResponse should marshal to JSON: %v", err)
	}
}
