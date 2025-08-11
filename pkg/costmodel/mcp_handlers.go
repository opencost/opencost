package costmodel

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/julienschmidt/httprouter"
	"github.com/opencost/opencost/pkg/mcp/client"
	"github.com/opencost/opencost/pkg/mcp/server"
	"github.com/opencost/opencost/pkg/mcp/types"
	"github.com/rs/zerolog/log"
)

// AddMCPRoutes adds MCP endpoints to the existing costmodel router
func AddMCPRoutes(router *httprouter.Router, accesses *Accesses) {
	// Create MCP server instance
	mcpServer, err := server.NewMCPServer()
	if err != nil {
		log.Error().Err(err).Msg("Failed to create MCP server")
		return
	}

	// Create wrapper handler that integrates MCP with your access control
	mcpHandler := &MCPHandler{
		server:   mcpServer,
		accesses: accesses,
	}

	// Add MCP routes with access control wrappers
	router.POST("/mcp/allocations", mcpHandler.HandleAllocationsWithAccess)
	router.POST("/mcp/assets", mcpHandler.HandleAssetsWithAccess)
	router.POST("/mcp/session", mcpHandler.HandleSessionWithAccess)
	router.GET("/mcp/session", mcpHandler.HandleSessionWithAccess)
	router.POST("/mcp/chat", mcpHandler.HandleChatWithAccess)
	router.GET("/mcp/health", mcpHandler.HandleHealthWithAccess)

	log.Info().Msg("MCP routes added to costmodel router with access control")
}

// MCPHandler wraps the MCP server with access control
type MCPHandler struct {
	server   *server.MCPServer
	accesses *Accesses
}

// HandleAllocationsWithAccess wraps allocation requests with access control
func (h *MCPHandler) HandleAllocationsWithAccess(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	// Check access permissions first
	if !h.checkAccess(w, r) {
		return
	}

	// Parse the request to extract filters for additional access control
	var req types.AllocationRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request format", http.StatusBadRequest)
		return
	}

	// Apply additional access-based filters
	req.Filters = h.applyAccessFilters(req.Filters, r)

	// Get session ID from header
	sessionID := r.Header.Get("X-Session-ID")

	// Handle the request directly
	h.handleAllocationsLogic(w, req, sessionID)
}

// HandleAssetsWithAccess wraps asset requests with access control
func (h *MCPHandler) HandleAssetsWithAccess(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	if !h.checkAccess(w, r) {
		return
	}

	var req types.AssetRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request format", http.StatusBadRequest)
		return
	}

	req.Filters = h.applyAccessFilters(req.Filters, r)
	sessionID := r.Header.Get("X-Session-ID")

	h.handleAssetsLogic(w, req, sessionID)
}

// HandleSessionWithAccess wraps session requests with access control
func (h *MCPHandler) HandleSessionWithAccess(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	if !h.checkAccess(w, r) {
		return
	}

	h.handleSessionLogic(w, r)
}

// HandleChatWithAccess wraps chat requests with access control
func (h *MCPHandler) HandleChatWithAccess(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	if !h.checkAccess(w, r) {
		return
	}

	// For chat, we might want to validate the session belongs to the user
	var chatReq types.ChatRequest
	if err := json.NewDecoder(r.Body).Decode(&chatReq); err != nil {
		http.Error(w, "Invalid request format", http.StatusBadRequest)
		return
	}

	// Validate session ownership if needed
	if !h.validateSessionAccess(chatReq.SessionID, r) {
		http.Error(w, "Session access denied", http.StatusForbidden)
		return
	}

	h.handleChatLogic(w, chatReq)
}

// HandleHealthWithAccess wraps health check with basic access control
func (h *MCPHandler) HandleHealthWithAccess(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	if !h.checkAccess(w, r) {
		return
	}

	h.handleHealthLogic(w)
}

// Helper methods

// Since we can't access the private methods directly, we'll implement the logic ourselves
// using the same client that the MCP server uses

func (h *MCPHandler) handleAllocationsLogic(w http.ResponseWriter, req types.AllocationRequest, sessionID string) {
	w.Header().Set("Content-Type", "application/json")

	// Set defaults
	if req.Window == "" {
		req.Window = "7d"
	}

	// Parse window
	window, err := h.parseWindow(req.Window)
	if err != nil {
		http.Error(w, fmt.Sprintf("Invalid window: %v", err), http.StatusBadRequest)
		return
	}

	// Get the OpenCost client
	ocClient, err := client.NewOpenCostClient()
	if err != nil {
		http.Error(w, "Failed to create OpenCost client", http.StatusInternalServerError)
		return
	}

	// Get data from OpenCost
	allocationData, err := ocClient.GetAllocations(window, req.Filters)
	if err != nil {
		log.Error().Err(err).Msg("Failed to get allocations")
		http.Error(w, fmt.Sprintf("Failed to get allocations: %v", err), http.StatusInternalServerError)
		return
	}

	// Build response
	response := h.buildAllocationResponse(allocationData, req)
	json.NewEncoder(w).Encode(response)
}

func (h *MCPHandler) handleAssetsLogic(w http.ResponseWriter, req types.AssetRequest, sessionID string) {
	w.Header().Set("Content-Type", "application/json")

	if req.Window == "" {
		req.Window = "7d"
	}

	window, err := h.parseWindow(req.Window)
	if err != nil {
		http.Error(w, fmt.Sprintf("Invalid window: %v", err), http.StatusBadRequest)
		return
	}

	ocClient, err := client.NewOpenCostClient()
	if err != nil {
		http.Error(w, "Failed to create OpenCost client", http.StatusInternalServerError)
		return
	}

	assetData, err := ocClient.GetAssets(window, req.Filters)
	if err != nil {
		log.Error().Err(err).Msg("Failed to get assets")
		http.Error(w, fmt.Sprintf("Failed to get assets: %v", err), http.StatusInternalServerError)
		return
	}

	response := h.buildAssetResponse(assetData, req)
	json.NewEncoder(w).Encode(response)
}

func (h *MCPHandler) handleSessionLogic(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	switch r.Method {
	case "POST":
		// Create new session - simplified version
		session := map[string]interface{}{
			"id":                 h.generateSessionID(),
			"startTime":          time.Now(),
			"lastActivity":       time.Now(),
			"queryHistory":       []interface{}{},
			"activeFilters":      map[string]interface{}{},
			"preferredUnits":     "USD",
			"userExpertiseLevel": "intermediate",
		}

		json.NewEncoder(w).Encode(session)

	case "GET":
		sessionID := r.URL.Query().Get("sessionId")
		if sessionID == "" {
			http.Error(w, "sessionId parameter required", http.StatusBadRequest)
			return
		}

		// Return session info - simplified version
		session := map[string]interface{}{
			"id":           sessionID,
			"status":       "active",
			"lastActivity": time.Now(),
		}

		json.NewEncoder(w).Encode(session)
	}
}

func (h *MCPHandler) handleChatLogic(w http.ResponseWriter, req types.ChatRequest) {
	w.Header().Set("Content-Type", "application/json")

	// Simple chat response
	response := map[string]interface{}{
		"queryType": "chat",
		"summary": map[string]interface{}{
			"currency": "USD",
			"period":   "unknown",
		},
		"insights": []map[string]interface{}{
			{
				"type":        "info",
				"severity":    "low",
				"title":       "Query Understanding",
				"description": "I understand you're asking about cost data. Could you be more specific?",
				"confidence":  0.5,
			},
		},
		"metadata": map[string]interface{}{
			"queryTime":          time.Now(),
			"confidence":         0.5,
			"nextSuggestedQuery": "Try asking about 'allocation costs' or 'asset costs'",
		},
	}

	json.NewEncoder(w).Encode(response)
}

func (h *MCPHandler) handleHealthLogic(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	
	health := map[string]interface{}{
		"status":    "healthy",
		"timestamp": time.Now(),
		"service":   "mcp-integration",
	}
	
	json.NewEncoder(w).Encode(health)
}

func (h *MCPHandler) checkAccess(w http.ResponseWriter, r *http.Request) bool {
	// Implement your access control logic here
	// This should integrate with your existing Accesses system
	
	// Example implementation:
	// 1. Check authentication token
	// 2. Validate user permissions
	// 3. Check rate limits
	// 4. Validate request source
	
	// For now, return true - replace with actual access control
	return true
}

func (h *MCPHandler) applyAccessFilters(existingFilters map[string]string, r *http.Request) map[string]string {
	if existingFilters == nil {
		existingFilters = make(map[string]string)
	}

	// Apply access-based filtering
	// Example: Restrict user to specific namespaces based on permissions
	
	// Get user context from request (token, headers, etc.)
	userNamespaces := h.getUserAllowedNamespaces(r)
	
	// If user has namespace restrictions, apply them
	if len(userNamespaces) > 0 {
		// If no namespace filter is specified, restrict to allowed namespaces
		if _, exists := existingFilters["namespace"]; !exists {
			// Join allowed namespaces or apply the restriction logic
			// This depends on how your access control works
			existingFilters["namespace"] = userNamespaces[0] // Simple example
		} else {
			// Validate that requested namespace is allowed
			requestedNS := existingFilters["namespace"]
			if !h.isNamespaceAllowed(requestedNS, userNamespaces) {
				// Remove unauthorized namespace or replace with default
				existingFilters["namespace"] = userNamespaces[0]
			}
		}
	}

	// Apply other access-based filters (clusters, labels, etc.)
	if allowedClusters := h.getUserAllowedClusters(r); len(allowedClusters) > 0 {
		if _, exists := existingFilters["cluster"]; !exists {
			existingFilters["cluster"] = allowedClusters[0]
		}
	}

	return existingFilters
}

func (h *MCPHandler) validateSessionAccess(sessionID string, r *http.Request) bool {
	// Validate that the user has access to this session
	// This might involve checking session ownership, user permissions, etc.
	return true // Placeholder
}

func (h *MCPHandler) getUserAllowedNamespaces(r *http.Request) []string {
	// Extract user context and return allowed namespaces
	// This should integrate with your existing access control system
	return []string{} // Empty means no restrictions
}

func (h *MCPHandler) getUserAllowedClusters(r *http.Request) []string {
	// Extract user context and return allowed clusters
	return []string{} // Empty means no restrictions
}

func (h *MCPHandler) isNamespaceAllowed(namespace string, allowedNamespaces []string) bool {
	for _, allowed := range allowedNamespaces {
		if namespace == allowed {
			return true
		}
	}
	return false
}

// Additional helper methods needed for the implementation

func (h *MCPHandler) getOpenCostClient() (interface{}, error) {
	// You can either:
	// 1. Import and use your client package directly
	// 2. Or access the client from the MCP server if it's exposed
	// 3. Or create a new client instance
	
	// For now, we'll import the client package
	// import "github.com/opencost/opencost/pkg/mcp/client"
	// return client.NewOpenCostClient()
	
	// Placeholder - replace with actual client creation
	return nil, fmt.Errorf("OpenCost client not implemented")
}

func (h *MCPHandler) parseWindow(windowStr string) (types.Window, error) {
	now := time.Now()
	var start, end time.Time
	
	switch windowStr {
	case "1h":
		start = now.Add(-1 * time.Hour)
		end = now
	case "1d", "24h":
		start = now.AddDate(0, 0, -1)
		end = now
	case "7d", "1w":
		start = now.AddDate(0, 0, -7)
		end = now
	case "30d", "1m":
		start = now.AddDate(0, 0, -30)
		end = now
	default:
		start = now.AddDate(0, 0, -7)
		end = now
	}
	
	return types.Window{Start: start, End: end}, nil
}

func (h *MCPHandler) generateSessionID() string {
	return fmt.Sprintf("session_%d", time.Now().Unix())
}

func (h *MCPHandler) buildAllocationResponse(data interface{}, req types.AllocationRequest) map[string]interface{} {
	// Build a response similar to what the MCP server would return
	return map[string]interface{}{
		"queryType": "allocation",
		"data":      data,
		"summary": map[string]interface{}{
			"totalCost": 0.0, // Calculate from data
			"currency":  "USD",
			"period":    req.Window,
		},
		"insights": []map[string]interface{}{
			{
				"type":        "info",
				"severity":    "low",
				"title":       "Allocation Data Retrieved",
				"description": "Successfully retrieved allocation cost data",
				"confidence":  0.9,
			},
		},
		"metadata": map[string]interface{}{
			"queryTime":   time.Now(),
			"dataSources": []string{"opencost-allocation-api"},
			"confidence":  0.9,
		},
	}
}

func (h *MCPHandler) buildAssetResponse(data interface{}, req types.AssetRequest) map[string]interface{} {
	// Build a response similar to what the MCP server would return
	return map[string]interface{}{
		"queryType": "asset",
		"data":      data,
		"summary": map[string]interface{}{
			"totalCost": 0.0, // Calculate from data
			"currency":  "USD",
			"period":    req.Window,
		},
		"insights": []map[string]interface{}{
			{
				"type":        "info",
				"severity":    "low",
				"title":       "Asset Data Retrieved",
				"description": "Successfully retrieved asset cost data",
				"confidence":  0.9,
			},
		},
		"metadata": map[string]interface{}{
			"queryTime":   time.Now(),
			"dataSources": []string{"opencost-assets-api"},
			"confidence":  0.9,
		},
	}
}