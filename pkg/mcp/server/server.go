package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
	"sync"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"github.com/opencost/opencost/pkg/mcp/client"
	"github.com/opencost/opencost/pkg/mcp/types"
	"github.com/rs/zerolog/log"
)

type MCPServer struct {
	httpServer *http.Server
	router     *mux.Router
	client     client.OpenCostClient
	sessions   map[string]*types.Session
	mu         sync.RWMutex
}

func NewMCPServer() (*MCPServer, error) {
	// Initialize OpenCost client
	ocClient, err := client.NewOpenCostClient()
	if err != nil {
		return nil, err
	}

	// Create router
	router := mux.NewRouter()

	server := &MCPServer{
		router:   router,
		client:   ocClient,
		sessions: make(map[string]*types.Session),
	}

	// Setup routes
	server.setupRoutes()

	// Create HTTP server
	server.httpServer = &http.Server{
		Addr:         ":8080",
		Handler:      router,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
	}

	return server, nil
}

func (s *MCPServer) setupRoutes() {
	// MCP endpoints
	api := s.router.PathPrefix("/mcp").Subrouter()

	// Session management
	api.HandleFunc("/session", s.handleSession).Methods("POST", "GET")

	// Cost queries
	api.HandleFunc("/allocations", s.handleAllocations).Methods("POST")
	api.HandleFunc("/assets", s.handleAssets).Methods("POST")
	api.HandleFunc("/cloud-costs", s.handleCloudCosts).Methods("POST")

	// Conversational interface
	api.HandleFunc("/chat", s.handleChat).Methods("POST")

	// Health check
	s.router.HandleFunc("/health", s.handleHealth).Methods("GET")
}

// Handler implementations

func (s *MCPServer) handleHealth(w http.ResponseWriter, r *http.Request) {
    w.Header().Set("Content-Type", "application/json")

    s.mu.RLock()
    sessionCount := len(s.sessions)
    s.mu.RUnlock()

    health := map[string]interface{}{
        "status":    "healthy",
        "timestamp": time.Now(),
        "sessions":  sessionCount,
    }

    json.NewEncoder(w).Encode(health)
}

func (s *MCPServer) handleSession(w http.ResponseWriter, r *http.Request) {
    w.Header().Set("Content-Type", "application/json")

    switch r.Method {
    case "POST":
        // Create new session
        session := &types.Session{
            ID:                 uuid.New().String(),
            StartTime:          time.Now(),
            LastActivity:       time.Now(),
            QueryHistory:       []types.QueryHistoryItem{},
            ActiveFilters:      make(map[string]interface{}),
            PreferredUnits:     "USD",
            UserExpertiseLevel: "intermediate",
        }

        s.mu.Lock()
        s.sessions[session.ID] = session
        s.mu.Unlock()
        
        log.Info().Str("sessionId", session.ID).Msg("Created new session")
        json.NewEncoder(w).Encode(session)

    case "GET":
        // Get session info
        sessionID := r.URL.Query().Get("sessionId")
        if sessionID == "" {
            http.Error(w, "sessionId parameter required", http.StatusBadRequest)
            return
        }

        session := s.getSession(sessionID)  // This now uses the mutex internally
        if session == nil {
            http.Error(w, "session not found", http.StatusNotFound)
            return
        }

        json.NewEncoder(w).Encode(session)
    }
}

func (s *MCPServer) handleAllocations(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	var req types.AllocationRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("Invalid request: %v", err), http.StatusBadRequest)
		return
	}

	// Get session context
	sessionID := r.Header.Get("X-Session-ID")
	session := s.getSession(sessionID)

	// Process natural language query if provided
	if req.NaturalLanguageQuery != "" {
		processed := s.processNaturalLanguageQuery(req.NaturalLanguageQuery, "allocation")
		// Merge processed parameters into request
		if processed.Window != "" {
			req.Window = processed.Window
		}
		if len(processed.Filters) > 0 {
			if req.Filters == nil {
				req.Filters = make(map[string]string)
			}
			for k, v := range processed.Filters {
				req.Filters[k] = v
			}
		}
	}

	// Set defaults
	if req.Window == "" {
		req.Window = "7d"
	}

	startTime := time.Now()

	// Parse window and get data from OpenCost
	window, err := s.parseWindow(req.Window)
	if err != nil {
		http.Error(w, fmt.Sprintf("Invalid window: %v", err), http.StatusBadRequest)
		return
	}

	allocationSet, err := s.client.GetAllocations(window, req.Filters)
	if err != nil {
		log.Error().Err(err).Msg("Failed to get allocations")
		http.Error(w, fmt.Sprintf("Failed to get allocations: %v", err), http.StatusInternalServerError)
		return
	}

	// Build AI-friendly response
	response := s.buildAllocationResponse(allocationSet, req, session)
	response.Metadata.ProcessingTime = time.Since(startTime)
	response.Metadata.QueryTime = time.Now()

	// Update session
	if session != nil {
		s.updateSession(session, "allocation", req.Filters, s.getResultCount(allocationSet), time.Since(startTime))
	}

	json.NewEncoder(w).Encode(response)
}

func (s *MCPServer) handleAssets(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	var req types.AssetRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("Invalid request: %v", err), http.StatusBadRequest)
		return
	}

	sessionID := r.Header.Get("X-Session-ID")
	session := s.getSession(sessionID)

	// Process natural language query if provided
	if req.NaturalLanguageQuery != "" {
		processed := s.processNaturalLanguageQuery(req.NaturalLanguageQuery, "asset")
		if processed.Window != "" {
			req.Window = processed.Window
		}
		if len(processed.Filters) > 0 {
			if req.Filters == nil {
				req.Filters = make(map[string]string)
			}
			for k, v := range processed.Filters {
				req.Filters[k] = v
			}
		}
	}

	if req.Window == "" {
		req.Window = "7d"
	}

	startTime := time.Now()

	// Parse window and get data from OpenCost
	window, err := s.parseWindow(req.Window)
	if err != nil {
		http.Error(w, fmt.Sprintf("Invalid window: %v", err), http.StatusBadRequest)
		return
	}

	assetSet, err := s.client.GetAssets(window, req.Filters)
	if err != nil {
		log.Error().Err(err).Msg("Failed to get assets")
		http.Error(w, fmt.Sprintf("Failed to get assets: %v", err), http.StatusInternalServerError)
		return
	}

	response := s.buildAssetResponse(assetSet, req, session)
	response.Metadata.ProcessingTime = time.Since(startTime)
	response.Metadata.QueryTime = time.Now()

	if session != nil {
		s.updateSession(session, "asset", req.Filters, s.getResultCount(assetSet), time.Since(startTime))
	}

	json.NewEncoder(w).Encode(response)
}

func (s *MCPServer) handleCloudCosts(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	var req types.CloudCostRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("Invalid request: %v", err), http.StatusBadRequest)
		return
	}

	sessionID := r.Header.Get("X-Session-ID")
	session := s.getSession(sessionID)

	if req.Window == "" {
		req.Window = "7d"
	}

	// For now, return a placeholder response
	response := types.MCPResponse{
		QueryType: "cloud",
		Data:      []interface{}{},
		Summary: types.Summary{
			TotalCost: 0,
			Currency:  s.getCurrency(session),
			Period:    req.Window,
		},
		Insights: []types.Insight{
			{
				Type:        "info",
				Severity:    "low",
				Title:       "Cloud Cost Integration",
				Description: "Cloud cost data integration is being configured",
				Confidence:  0.8,
			},
		},
		Metadata: types.ResponseMetadata{
			QueryTime:   time.Now(),
			DataSources: []string{"cloud-billing-apis"},
			Confidence:  0.8,
		},
	}

	json.NewEncoder(w).Encode(response)
}

func (s *MCPServer) handleChat(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	var chatReq types.ChatRequest
	if err := json.NewDecoder(r.Body).Decode(&chatReq); err != nil {
		http.Error(w, fmt.Sprintf("Invalid request: %v", err), http.StatusBadRequest)
		return
	}

	session := s.getSession(chatReq.SessionID)
	if session == nil {
		http.Error(w, "Session not found", http.StatusNotFound)
		return
	}

	// Analyze intent and route to appropriate handler
	intent := s.analyzeIntent(chatReq.Message)

	var response types.MCPResponse

	switch intent.QueryType {
	case "allocation":
		req := types.AllocationRequest{
			NaturalLanguageQuery: chatReq.Message,
		}
		window, _ := s.parseWindow("7d") // Default window
		allocationSet, err := s.client.GetAllocations(window, nil)
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to get allocations: %v", err), http.StatusInternalServerError)
			return
		}
		response = s.buildAllocationResponse(allocationSet, req, session)

	case "asset":
		req := types.AssetRequest{
			NaturalLanguageQuery: chatReq.Message,
		}
		window, _ := s.parseWindow("7d") // Default window
		assetSet, err := s.client.GetAssets(window, nil)
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to get assets: %v", err), http.StatusInternalServerError)
			return
		}
		response = s.buildAssetResponse(assetSet, req, session)

	default:
		response = types.MCPResponse{
			QueryType: "chat",
			Summary: types.Summary{
				Currency: session.PreferredUnits,
				Period:   "unknown",
			},
			Insights: []types.Insight{
				{
					Type:        "info",
					Severity:    "low",
					Title:       "Query Understanding",
					Description: fmt.Sprintf("I understand you're asking about cost data. Could you be more specific?"),
					Confidence:  intent.Confidence,
				},
			},
			Metadata: types.ResponseMetadata{
				QueryTime:          time.Now(),
				Confidence:         intent.Confidence,
				NextSuggestedQuery: "Try asking about 'allocation costs' or 'asset costs'",
			},
		}
	}

	json.NewEncoder(w).Encode(response)
}

func (s *MCPServer) cleanupSessions() {
    ticker := time.NewTicker(30 * time.Minute)
    go func() {
        for range ticker.C {
            s.mu.Lock()
            now := time.Now()
            for id, session := range s.sessions {
                if now.Sub(session.LastActivity) > 2*time.Hour {
                    delete(s.sessions, id)
                    log.Info().Str("sessionId", id).Msg("Cleaned up inactive session")
                }
            }
            s.mu.Unlock()
        }
    }()
}

func (s *MCPServer) Start() error {
    s.cleanupSessions()  // Start the cleanup goroutine
    log.Info().Str("addr", s.httpServer.Addr).Msg("Starting MCP server")
    return s.httpServer.ListenAndServe()
}

func (s *MCPServer) Shutdown(ctx context.Context) error {
	return s.httpServer.Shutdown(ctx)
}

// Helper methods

func (s *MCPServer) getSession(sessionID string) *types.Session {
    if sessionID == "" {
        return nil
    }
    
    s.mu.RLock()
    session, exists := s.sessions[sessionID]
    s.mu.RUnlock()
    
    if !exists {
        return nil
    }
    
    // Update last activity with write lock
    s.mu.Lock()
    session.LastActivity = time.Now()
    s.mu.Unlock()
    
    return session
}

func (s *MCPServer) getCurrency(session *types.Session) string {
	if session != nil && session.PreferredUnits != "" {
		return session.PreferredUnits
	}
	return "USD"
}

func (s *MCPServer) getResultCount(data interface{}) int {
	switch typed := data.(type) {
	case *types.AllocationSet:
		return len(typed.Allocations)
	case *types.AssetSet:
		return len(typed.Assets)
	case map[string]interface{}:
		if dataField, ok := typed["data"].(map[string]interface{}); ok {
			return len(dataField)
		}
		return 0
	default:
		return 0
	}
}

func (s *MCPServer) updateSession(session *types.Session, queryType string, filters map[string]string, resultCount int, duration time.Duration) {
	parameters := make(map[string]interface{})
	for k, v := range filters {
		parameters[k] = v
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	session.QueryHistory = append(session.QueryHistory, types.QueryHistoryItem{
		Timestamp:   time.Now(),
		QueryType:   queryType,
		Parameters:  parameters,
		ResultCount: resultCount,
		Duration:    duration,
	})

	session.LastActivity = time.Now()

	// Keep only last 10 queries
	if len(session.QueryHistory) > 10 {
		session.QueryHistory = session.QueryHistory[len(session.QueryHistory)-10:]
	}
}

// Enhanced natural language processing
func (s *MCPServer) processNaturalLanguageQuery(query string, queryType string) types.NLQueryResult {
	query = strings.ToLower(strings.TrimSpace(query))

	result := types.NLQueryResult{
		Filters:    make(map[string]string),
		Confidence: 0.5,
	}

	// Extract time window with higher confidence
	if strings.Contains(query, "last hour") || strings.Contains(query, "past hour") {
		result.Window = "1h"
		result.Confidence += 0.2
	} else if strings.Contains(query, "last 6 hours") || strings.Contains(query, "past 6 hours") {
		result.Window = "6h"
		result.Confidence += 0.2
	} else if strings.Contains(query, "last 24 hours") || strings.Contains(query, "past 24 hours") || strings.Contains(query, "last day") {
		result.Window = "1d"
		result.Confidence += 0.2
	} else if strings.Contains(query, "last week") || strings.Contains(query, "past week") {
		result.Window = "7d"
		result.Confidence += 0.2
	} else if strings.Contains(query, "last month") || strings.Contains(query, "past month") {
		result.Window = "30d"
		result.Confidence += 0.2
	} else if strings.Contains(query, "yesterday") {
		result.Window = "yesterday"
		result.Confidence += 0.2
	} else if strings.Contains(query, "today") {
		result.Window = "today"
		result.Confidence += 0.2
	}

	// Extract namespaces with better pattern matching
	namespacePatterns := map[string][]string{
		"production":  {"production", "prod", "prd"},
		"staging":     {"staging", "stage", "stg"},
		"development": {"development", "dev", "develop"},
		"testing":     {"testing", "test", "qa"},
	}

	for namespace, patterns := range namespacePatterns {
		for _, pattern := range patterns {
			if strings.Contains(query, pattern) {
				result.Filters["namespace"] = namespace
				result.Confidence += 0.15
				break
			}
		}
	}

	// Extract cluster information
	clusterKeywords := []string{"cluster", "kube", "k8s"}
	for _, keyword := range clusterKeywords {
		if strings.Contains(query, keyword) {
			result.Confidence += 0.1
		}
	}

	// Extract service/application names
	if strings.Contains(query, "app") || strings.Contains(query, "application") || strings.Contains(query, "service") {
		words := strings.Fields(query)
		for i, word := range words {
			if (word == "app" || word == "application" || word == "service") && i+1 < len(words) {
				result.Filters["app"] = words[i+1]
				result.Confidence += 0.15
				break
			}
		}
	}

	// Extract cost thresholds or comparisons
	if strings.Contains(query, "expensive") || strings.Contains(query, "costly") || strings.Contains(query, "high cost") {
		result.Aggregate = "sum"
		result.Confidence += 0.1
	}

	if strings.Contains(query, "top") || strings.Contains(query, "highest") || strings.Contains(query, "most expensive") {
		result.Aggregate = "sum"
		result.Confidence += 0.15
	}

	return result
}

// Enhanced intent analysis
func (s *MCPServer) analyzeIntent(message string) types.IntentResult {
	message = strings.ToLower(strings.TrimSpace(message))

	result := types.IntentResult{
		QueryType:  "general",
		Confidence: 0.3,
		Entities:   []types.Entity{},
	}

	// Allocation-related keywords
	allocationKeywords := []string{
		"allocation", "allocations", "pod", "pods", "container", "containers",
		"namespace", "namespaces", "deployment", "deployments", "workload", "workloads",
	}

	// Asset-related keywords
	assetKeywords := []string{
		"asset", "assets", "node", "nodes", "cluster", "clusters", "instance", "instances",
		"machine", "machines", "infrastructure", "compute", "storage", "network",
	}

	// Cloud cost keywords
	cloudKeywords := []string{
		"cloud", "aws", "azure", "gcp", "google cloud", "amazon", "microsoft",
		"bill", "billing", "invoice", "cloud cost", "cloud costs",
	}

	// Check for allocation intent
	for _, keyword := range allocationKeywords {
		if strings.Contains(message, keyword) {
			result.QueryType = "allocation"
			result.Confidence = 0.8
			result.Entities = append(result.Entities, types.Entity{
				Type:       "keyword",
				Value:      keyword,
				Confidence: 0.8,
			})
			break
		}
	}

	// Check for asset intent
	if result.QueryType == "general" {
		for _, keyword := range assetKeywords {
			if strings.Contains(message, keyword) {
				result.QueryType = "asset"
				result.Confidence = 0.8
				result.Entities = append(result.Entities, types.Entity{
					Type:       "keyword",
					Value:      keyword,
					Confidence: 0.8,
				})
				break
			}
		}
	}

	// Check for cloud cost intent
	if result.QueryType == "general" {
		for _, keyword := range cloudKeywords {
			if strings.Contains(message, keyword) {
				result.QueryType = "cloud"
				result.Confidence = 0.7
				result.Entities = append(result.Entities, types.Entity{
					Type:       "keyword",
					Value:      keyword,
					Confidence: 0.7,
				})
				break
			}
		}
	}

	// Extract specific entities from the message
	s.extractEntities(message, &result)

	return result
}

// Entity extraction helper
func (s *MCPServer) extractEntities(message string, result *types.IntentResult) {
	words := strings.Fields(message)

	// Look for namespace entities
	for i, word := range words {
		if word == "namespace" && i+1 < len(words) {
			result.Entities = append(result.Entities, types.Entity{
				Type:       "namespace",
				Value:      words[i+1],
				Confidence: 0.7,
			})
		}
	}

	// Look for cluster entities
	for i, word := range words {
		if word == "cluster" && i+1 < len(words) {
			result.Entities = append(result.Entities, types.Entity{
				Type:       "cluster",
				Value:      words[i+1],
				Confidence: 0.7,
			})
		}
	}

	// Look for time entities
	timePatterns := map[string]string{
		"today":     "time",
		"yesterday": "time",
		"week":      "time",
		"month":     "time",
		"day":       "time",
		"hour":      "time",
	}

	for _, word := range words {
		if entityType, exists := timePatterns[word]; exists {
			result.Entities = append(result.Entities, types.Entity{
				Type:       entityType,
				Value:      word,
				Confidence: 0.6,
			})
		}
	}
}

// Enhanced window parsing
func (s *MCPServer) parseWindow(windowStr string) (types.Window, error) {
	now := time.Now()
	var start, end time.Time

	switch windowStr {
	case "1h":
		start = now.Add(-1 * time.Hour)
		end = now
	case "6h":
		start = now.Add(-6 * time.Hour)
		end = now
	case "12h":
		start = now.Add(-12 * time.Hour)
		end = now
	case "1d", "24h":
		start = now.AddDate(0, 0, -1)
		end = now
	case "2d":
		start = now.AddDate(0, 0, -2)
		end = now
	case "3d":
		start = now.AddDate(0, 0, -3)
		end = now
	case "7d", "1w":
		start = now.AddDate(0, 0, -7)
		end = now
	case "14d", "2w":
		start = now.AddDate(0, 0, -14)
		end = now
	case "30d", "1m":
		start = now.AddDate(0, 0, -30)
		end = now
	case "90d", "3m":
		start = now.AddDate(0, 0, -90)
		end = now
	case "today":
		start = time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location())
		end = now
	case "yesterday":
		yesterday := now.AddDate(0, 0, -1)
		start = time.Date(yesterday.Year(), yesterday.Month(), yesterday.Day(), 0, 0, 0, 0, yesterday.Location())
		end = time.Date(yesterday.Year(), yesterday.Month(), yesterday.Day(), 23, 59, 59, 0, yesterday.Location())
	default:
		// Try to parse as duration
		if duration, err := time.ParseDuration(windowStr); err == nil {
			start = now.Add(-duration)
			end = now
		} else {
			return types.Window{}, fmt.Errorf("unsupported window format: %s", windowStr)
		}
	}

	return types.Window{Start: start, End: end}, nil
}

// Response builders with proper type handling
func (s *MCPServer) buildAllocationResponse(allocationData interface{}, req types.AllocationRequest, session *types.Session) types.MCPResponse {
	var totalCost float64
	var itemCount int
	var topItems []types.Item

	// Type assertion and data processing
	switch data := allocationData.(type) {
	case *types.AllocationSet:
		totalCost = data.TotalCost
		itemCount = len(data.Allocations)
		
		// Build top items from allocations
		for name, allocation := range data.Allocations {
			if len(topItems) < 5 { // Top 5 items
				topItems = append(topItems, types.Item{
					Name: name,
					Cost: allocation.TotalCost,
					Type: "allocation",
				})
			}
		}
		
	case map[string]interface{}:
		// Handle generic interface{} data from OpenCost API
		if allocations, ok := data["data"].(map[string]interface{}); ok {
			itemCount = len(allocations)
			
			// Extract costs if available
			for name, alloc := range allocations {
				if allocData, ok := alloc.(map[string]interface{}); ok {
					if cost, ok := allocData["totalCost"].(float64); ok {
						totalCost += cost
						if len(topItems) < 5 {
							topItems = append(topItems, types.Item{
								Name: name,
								Cost: cost,
								Type: "allocation",
							})
						}
					}
				}
			}
		}
		
	default:
		// Fallback for unknown types
		totalCost = 0
		itemCount = 0
	}

	// Calculate percentages for top items
	for i := range topItems {
		if totalCost > 0 {
			topItems[i].Percentage = (topItems[i].Cost / totalCost) * 100
		}
	}

	return types.MCPResponse{
		QueryType: "allocation",
		Data:      allocationData,
		Summary: types.Summary{
			TotalCost: totalCost,
			Currency:  s.getCurrency(session),
			Period:    req.Window,
			ItemCount: itemCount,
			TopItems:  topItems,
		},
		Insights: s.generateAllocationInsights(totalCost, itemCount, topItems),
		Metadata: types.ResponseMetadata{
			DataSources:     []string{"opencost-allocation-api"},
			Confidence:      0.9,
			TotalResults:    itemCount,
			ResultsReturned: itemCount,
		},
	}
}

func (s *MCPServer) buildAssetResponse(assetData interface{}, req types.AssetRequest, session *types.Session) types.MCPResponse {
	var totalCost float64
	var itemCount int
	var topItems []types.Item

	// Type assertion and data processing
	switch data := assetData.(type) {
	case *types.AssetSet:
		totalCost = data.TotalCost
		itemCount = len(data.Assets)
		
		// Build top items from assets
		for name, asset := range data.Assets {
			if len(topItems) < 5 { // Top 5 items
				topItems = append(topItems, types.Item{
					Name: name,
					Cost: asset.TotalCost,
					Type: "asset",
				})
			}
		}
		
	case map[string]interface{}:
		// Handle generic interface{} data from OpenCost API
		if assets, ok := data["data"].(map[string]interface{}); ok {
			itemCount = len(assets)
			
			// Extract costs if available
			for name, asset := range assets {
				if assetData, ok := asset.(map[string]interface{}); ok {
					if cost, ok := assetData["totalCost"].(float64); ok {
						totalCost += cost
						if len(topItems) < 5 {
							topItems = append(topItems, types.Item{
								Name: name,
								Cost: cost,
								Type: "asset",
							})
						}
					}
				}
			}
		}
		
	default:
		// Fallback for unknown types
		totalCost = 0
		itemCount = 0
	}

	// Calculate percentages for top items
	for i := range topItems {
		if totalCost > 0 {
			topItems[i].Percentage = (topItems[i].Cost / totalCost) * 100
		}
	}

	return types.MCPResponse{
		QueryType: "asset",
		Data:      assetData,
		Summary: types.Summary{
			TotalCost: totalCost,
			Currency:  s.getCurrency(session),
			Period:    req.Window,
			ItemCount: itemCount,
			TopItems:  topItems,
		},
		Insights: s.generateAssetInsights(totalCost, itemCount, topItems),
		Metadata: types.ResponseMetadata{
			DataSources:     []string{"opencost-assets-api"},
			Confidence:      0.9,
			TotalResults:    itemCount,
			ResultsReturned: itemCount,
		},
	}
}

func (s *MCPServer) generateAllocationInsights(totalCost float64, itemCount int, topItems []types.Item) []types.Insight {
	insights := []types.Insight{
		{
			Type:        "info",
			Severity:    "low",
			Title:       "Allocation Data Retrieved",
			Description: fmt.Sprintf("Successfully retrieved %d allocation entries with total cost of $%.2f", itemCount, totalCost),
			Confidence:  0.9,
		},
	}

	// Add cost optimization insights
	if totalCost > 1000 {
		insights = append(insights, types.Insight{
			Type:        "optimization",
			Severity:    "medium",
			Title:       "High Cost Detected",
			Description: fmt.Sprintf("Total allocation cost of $%.2f is above $1000. Consider reviewing resource usage.", totalCost),
			Confidence:  0.7,
			ActionItems: []string{
				"Review top cost-driving allocations",
				"Check for unused resources",
				"Consider resource optimization",
			},
		})
	}

	// Add insights about top consumers
	if len(topItems) > 0 && topItems[0].Percentage > 50 {
		insights = append(insights, types.Insight{
			Type:        "warning",
			Severity:    "medium",
			Title:       "Single Allocation Dominates Costs",
			Description: fmt.Sprintf("'%s' accounts for %.1f%% of total allocation costs", topItems[0].Name, topItems[0].Percentage),
			Confidence:  0.8,
			ActionItems: []string{
				"Review resource allocation for " + topItems[0].Name,
				"Consider cost optimization strategies",
				"Investigate if resources are being used efficiently",
			},
		})
	}

	return insights
}

func (s *MCPServer) generateAssetInsights(totalCost float64, itemCount int, topItems []types.Item) []types.Insight {
	insights := []types.Insight{
		{
			Type:        "info",
			Severity:    "low",
			Title:       "Asset Data Retrieved",
			Description: fmt.Sprintf("Successfully retrieved %d asset entries with total cost of $%.2f", itemCount, totalCost),
			Confidence:  0.9,
		},
	}

	// Add asset-specific insights
	if totalCost > 2000 {
		insights = append(insights, types.Insight{
			Type:        "optimization",
			Severity:    "medium",
			Title:       "High Asset Costs",
			Description: fmt.Sprintf("Total asset cost of $%.2f indicates significant infrastructure spending", totalCost),
			Confidence:  0.7,
			ActionItems: []string{
				"Review asset utilization",
				"Consider rightsizing instances",
				"Evaluate reserved instance opportunities",
			},
		})
	}

	// Add insights about asset distribution
	if len(topItems) > 0 && topItems[0].Percentage > 60 {
		insights = append(insights, types.Insight{
			Type:        "warning",
			Severity:    "high",
			Title:       "Asset Cost Concentration Risk",
			Description: fmt.Sprintf("'%s' accounts for %.1f%% of total asset costs, creating concentration risk", topItems[0].Name, topItems[0].Percentage),
			Confidence:  0.8,
			ActionItems: []string{
				"Diversify asset distribution",
				"Review single points of failure",
				"Consider backup resources in different regions",
			},
		})
	}

	return insights
}
