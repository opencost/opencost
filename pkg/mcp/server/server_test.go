package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
	
	"github.com/opencost/opencost/pkg/mcp/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockOpenCostClient is a mock implementation of the OpenCostClient interface for testing.
type mockOpenCostClient struct{}

// GetAllocations mocks the client call, returning an interface{} as required by the client.OpenCostClient interface.
func (c *mockOpenCostClient) GetAllocations(window types.Window, filters map[string]string) (interface{}, error) {
	allocSet := &types.AllocationSet{
		Allocations: map[string]*types.Allocation{
			"test-allocation": {
				Name:      "test-allocation",
				TotalCost: 123.45,
			},
		},
		TotalCost: 123.45,
	}
	return allocSet, nil
}

// GetAssets mocks the client call, returning an interface{} as required by the client.OpenCostClient interface.
func (c *mockOpenCostClient) GetAssets(window types.Window, filters map[string]string) (interface{}, error) {
	assetSet := &types.AssetSet{
		Assets: map[string]*types.Asset{
			"test-asset": {
				TotalCost: 543.21,
			},
		},
		TotalCost: 543.21,
	}
	return assetSet, nil
}

// HealthCheck is the newly added method to satisfy the client.OpenCostClient interface.
func (c *mockOpenCostClient) HealthCheck() error {
	// For a mock, we can assume the client is always healthy.
	return nil
}

// setupTestServer creates an MCPServer and replaces its client with a mock for predictable testing.
func setupTestServer(t *testing.T) *MCPServer {
	server, err := NewMCPServer()
	require.NoError(t, err)

	// Replace the real OpenCost client with our complete mock implementation.
	server.client = &mockOpenCostClient{}
	return server
}

func TestNewMCPServer(t *testing.T) {
	server, err := NewMCPServer()
	assert.NoError(t, err)
	assert.NotNil(t, server)
	assert.NotNil(t, server.router)
	assert.NotNil(t, server.sessions)
	assert.NotNil(t, server.client)
}

func TestSessionHandlers(t *testing.T) {
	server := setupTestServer(t)

	// Test POST to create a new session
	req := httptest.NewRequest("POST", "/mcp/session", nil)
	w := httptest.NewRecorder()
	server.router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var session types.Session
	err := json.NewDecoder(w.Body).Decode(&session)
	assert.NoError(t, err)
	assert.NotEmpty(t, session.ID)
	assert.Equal(t, 1, len(server.sessions))

	// Test GET to retrieve the created session
	sessionID := session.ID
	getReq := httptest.NewRequest("GET", "/mcp/session?sessionId="+sessionID, nil)
	getW := httptest.NewRecorder()
	server.router.ServeHTTP(getW, getReq)

	assert.Equal(t, http.StatusOK, getW.Code)
	var retrievedSession types.Session
	err = json.NewDecoder(getW.Body).Decode(&retrievedSession)
	assert.NoError(t, err)
	assert.Equal(t, sessionID, retrievedSession.ID)
}

func TestParseWindow(t *testing.T) {
	server := setupTestServer(t)

	testCases := []struct {
		name               string
		window             string
		expectedStatusCode int
	}{
		{"Valid Window (7d)", `{"window": "7d"}`, http.StatusOK},
		{"Valid Window (yesterday)", `{"window": "yesterday"}`, http.StatusOK},
		{"Valid Window (1h)", `{"window": "1h"}`, http.StatusOK},
		{"Invalid Window", `{"window": "invalid-window"}`, http.StatusBadRequest},
		{"Empty Body", `{}`, http.StatusOK}, // Uses default window "7d"
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest("POST", "/mcp/allocations", strings.NewReader(tc.window))
			w := httptest.NewRecorder()
			server.router.ServeHTTP(w, req)
			assert.Equal(t, tc.expectedStatusCode, w.Code)
		})
	}
}

func TestNaturalLanguageProcessing(t *testing.T) {
	server := setupTestServer(t)

	query := `{"naturalLanguageQuery": "show me costs for the prod namespace last week"}`

	req := httptest.NewRequest("POST", "/mcp/allocations", strings.NewReader(query))
	w := httptest.NewRecorder()
	server.router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var resp types.MCPResponse
	err := json.NewDecoder(w.Body).Decode(&resp)
	require.NoError(t, err)

	assert.Equal(t, "allocation", resp.QueryType)
	assert.Equal(t, "7d", resp.Summary.Period, "NLP should have parsed 'last week' into '7d'")
}

func TestAllocationRequestStructure(t *testing.T) {
	reqBody := `{
    "window": "7d",
    "filters": {"namespace": "production"},
    "naturalLanguageQuery": "Show me production costs"
  }`

	var allocReq types.AllocationRequest
	err := json.Unmarshal([]byte(reqBody), &allocReq)

	assert.NoError(t, err)
	assert.Equal(t, "7d", allocReq.Window)
	assert.Equal(t, "production", allocReq.Filters["namespace"])
	assert.NotEmpty(t, allocReq.NaturalLanguageQuery)
}

func TestAssetRequestStructure(t *testing.T) {
	reqBody := `{
    "window": "30d",
    "filters": {"type": "node"},
    "naturalLanguageQuery": "Show me node costs"
  }`

	var assetReq types.AssetRequest
	err := json.Unmarshal([]byte(reqBody), &assetReq)

	assert.NoError(t, err)
	assert.Equal(t, "30d", assetReq.Window)
	assert.Equal(t, "node", assetReq.Filters["type"])
}

func TestHealthEndpoint(t *testing.T) {
	server := setupTestServer(t)

	req := httptest.NewRequest("GET", "/health", nil)
	w := httptest.NewRecorder()

	server.router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var health map[string]interface{}
	err := json.NewDecoder(w.Body).Decode(&health)
	assert.NoError(t, err)
	assert.Equal(t, "healthy", health["status"])
	assert.Equal(t, float64(0), health["sessions"])
}

// Integration test for the full flow
func TestMCPServerIntegration(t *testing.T) {
	server := setupTestServer(t)

	// Step 1: Create a session
	sessionReq := httptest.NewRequest("POST", "/mcp/session", nil)
	sessionW := httptest.NewRecorder()
	server.router.ServeHTTP(sessionW, sessionReq)

	assert.Equal(t, http.StatusOK, sessionW.Code)

	var sessionResp types.Session
	err := json.NewDecoder(sessionW.Body).Decode(&sessionResp)
	assert.NoError(t, err)
	sessionID := sessionResp.ID
	require.NotEmpty(t, sessionID)

	// Step 2: Make an allocation query using the session
	allocBody := `{
    "window": "7d",
    "filters": {"namespace": "default"}
  }`

	allocReq := httptest.NewRequest("POST", "/mcp/allocations", strings.NewReader(allocBody))
	allocReq.Header.Set("Content-Type", "application/json")
	allocReq.Header.Set("X-Session-ID", sessionID)
	allocW := httptest.NewRecorder()
	server.router.ServeHTTP(allocW, allocReq)
	assert.Equal(t, http.StatusOK, allocW.Code)

	// Step 3: Get the session and validate it was updated
	sessionGetReq := httptest.NewRequest("GET", "/mcp/session?sessionId="+sessionID, nil)
	sessionGetW := httptest.NewRecorder()
	server.router.ServeHTTP(sessionGetW, sessionGetReq)

	assert.Equal(t, http.StatusOK, sessionGetW.Code)
	var updatedSession types.Session
	err = json.NewDecoder(sessionGetW.Body).Decode(&updatedSession)
	assert.NoError(t, err)
	assert.Equal(t, sessionID, updatedSession.ID)
	require.Len(t, updatedSession.QueryHistory, 1)
	assert.Equal(t, "allocation", updatedSession.QueryHistory[0].QueryType)
	assert.Equal(t, "default", updatedSession.QueryHistory[0].Parameters["namespace"])
}

func TestChatHandler(t *testing.T) {
	server := setupTestServer(t)

	// Create a session first
	session := &types.Session{ID: "chat-session-123", StartTime: time.Now(), LastActivity: time.Now()}
	server.sessions[session.ID] = session

	// Test chat request that should resolve to an allocation query
	chatReqBody := `{"sessionId": "chat-session-123", "message": "What are my allocation costs?"}`
	req := httptest.NewRequest("POST", "/mcp/chat", strings.NewReader(chatReqBody))
	w := httptest.NewRecorder()

	server.router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var resp types.MCPResponse
	err := json.NewDecoder(w.Body).Decode(&resp)
	require.NoError(t, err)
	assert.Equal(t, "allocation", resp.QueryType)
	assert.NotNil(t, resp.Data)
	assert.Equal(t, 123.45, resp.Summary.TotalCost)
}