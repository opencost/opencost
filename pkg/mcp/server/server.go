package server

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/opencost/opencost/pkg/mcp/types"
	"github.com/opencost/opencost/pkg/mcp/client"
)

const (
	MCPProtocolVersion = "2024-11-05"
	ServerName         = "opencost-mcp-server"
	ServerVersion      = "1.0.0"
	ServerDescription  = "OpenCost MCP Server - Kubernetes cost analysis and optimization"
	SessionTimeout     = 30 * time.Minute
	MaxRequestSize     = 10 * 1024 * 1024 // 10MB
)

// MCPServer represents the main MCP server
type MCPServer struct {
	sessionManager *types.SessionManager
	openCostClient *client.OpenCostClient
	handlers       map[string]RequestHandler
	initialized    bool
	mu             sync.RWMutex
	logger         *log.Logger
	config         *ServerConfig
}

// ServerConfig contains server configuration
type ServerConfig struct {
	OpenCostURL     string        `json:"opencost_url"`
	SessionTimeout  time.Duration `json:"session_timeout"`
	MaxRequestSize  int64         `json:"max_request_size"`
	EnableDebug     bool          `json:"enable_debug"`
	CorsEnabled     bool          `json:"cors_enabled"`
	AllowedOrigins  []string      `json:"allowed_origins"`
	RateLimitRPS    int           `json:"rate_limit_rps"`
}

// RequestHandler defines the interface for handling MCP requests
type RequestHandler func(ctx context.Context, params json.RawMessage, sessionCtx *types.SessionContext) (*types.MCPResponse, error)

// NewMCPServer creates a new MCP server instance
func NewMCPServer(config *ServerConfig, logger *log.Logger) (*MCPServer, error) {
	if config == nil {
		config = &ServerConfig{
			OpenCostURL:    "http://localhost:9090",
			SessionTimeout: SessionTimeout,
			MaxRequestSize: MaxRequestSize,
			EnableDebug:    false,
			CorsEnabled:    true,
			AllowedOrigins: []string{"*"},
			RateLimitRPS:   100,
		}
	}

	openCostClient, err := client.NewOpenCostClient(config.OpenCostURL)
	if err != nil {
		return nil, fmt.Errorf("failed to create OpenCost client: %w", err)
	}

	server := &MCPServer{
		sessionManager: types.NewSessionManager(config.SessionTimeout),
		openCostClient: openCostClient,
		handlers:       make(map[string]RequestHandler),
		logger:         logger,
		config:         config,
	}

	server.registerHandlers()

	// Start cleanup goroutine
	go server.cleanupExpiredSessions()

	return server, nil
}

// registerHandlers registers all MCP protocol handlers
func (s *MCPServer) registerHandlers() {
	s.handlers["initialize"] = s.handleInitialize
	s.handlers["initialized"] = s.handleInitialized
	s.handlers["tools/list"] = s.handleListTools
	s.handlers["tools/call"] = s.handleCallTool
	s.handlers["notifications/cancelled"] = s.handleCancelled
	s.handlers["ping"] = s.handlePing
}

// ServeHTTP implements the http.Handler interface
func (s *MCPServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Set CORS headers if enabled
	if s.config.CorsEnabled {
		s.setCORSHeaders(w, r)
		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}
	}

	// Only allow POST requests
	if r.Method != "POST" {
		s.writeErrorResponse(w, nil, types.MethodNotFound, "Method not allowed", nil)
		return
	}

	// Check content type
	contentType := r.Header.Get("Content-Type")
	if !strings.Contains(strings.ToLower(contentType), "application/json") {
		s.writeErrorResponse(w, nil, types.InvalidRequest, "Content-Type must be application/json", nil)
		return
	}

	// Limit request size
	r.Body = http.MaxBytesReader(w, r.Body, s.config.MaxRequestSize)

	// Read request body
	body, err := io.ReadAll(r.Body)
	if err != nil {
		s.writeErrorResponse(w, nil, types.InvalidRequest, "Failed to read request body", err)
		return
	}

	// Parse JSON-RPC request
	var request types.MCPRequest
	if err := json.Unmarshal(body, &request); err != nil {
		s.writeErrorResponse(w, nil, types.ParseError, "Invalid JSON", err)
		return
	}

	// Validate JSON-RPC version
	if request.JSONRPC != "2.0" {
		s.writeErrorResponse(w, request.ID, types.InvalidRequest, "Invalid JSON-RPC version", nil)
		return
	}

	// Handle the request
	ctx := context.WithValue(r.Context(), "request_id", request.ID)
	response := s.handleRequest(ctx, &request)

	// Write response
	s.writeJSONResponse(w, response)
}

// handleRequest handles a parsed MCP request
func (s *MCPServer) handleRequest(ctx context.Context, request *types.MCPRequest) *types.MCPResponse {
	// Get or create session
	sessionID := s.getSessionID(ctx)
	clientInfo := s.getClientInfo(request)
	session := s.sessionManager.GetOrCreateSession(sessionID, clientInfo)

	// Find handler
	handler, exists := s.handlers[request.Method]
	if !exists {
		return &types.MCPResponse{
			JSONRPC: "2.0",
			ID:      request.ID,
			Error: &types.MCPError{
				Code:    types.MethodNotFound,
				Message: fmt.Sprintf("Method not found: %s", request.Method),
			},
		}
	}

	// Call handler
	response, err := handler(ctx, request.Params, session)
	if err != nil {
		s.logger.Printf("Handler error for method %s: %v", request.Method, err)
		return &types.MCPResponse{
			JSONRPC: "2.0",
			ID:      request.ID,
			Error: &types.MCPError{
				Code:    types.InternalError,
				Message: err.Error(),
			},
		}
	}

	if response.ID == nil {
		response.ID = request.ID
	}

	return response
}

// Handler implementations

func (s *MCPServer) handleInitialize(ctx context.Context, params json.RawMessage, session *types.SessionContext) (*types.MCPResponse, error) {
	var initParams types.InitializeParams
	if err := json.Unmarshal(params, &initParams); err != nil {
		return nil, fmt.Errorf("invalid initialize parameters: %w", err)
	}

	// Validate protocol version
	if initParams.ProtocolVersion != MCPProtocolVersion {
		return nil, fmt.Errorf("unsupported protocol version: %s", initParams.ProtocolVersion)
	}

	result := types.InitializeResult{
		ProtocolVersion: MCPProtocolVersion,
		ServerInfo: types.ServerInfo{
			Name:        ServerName,
			Version:     ServerVersion,
			Description: ServerDescription,
		},
		Capabilities: types.Capabilities{
			Tools: &types.ToolCapabilities{
				ListChanged: false,
			},
			Logging: &types.LoggingCapabilities{
				Level: "info",
			},
		},
	}

	s.mu.Lock()
	s.initialized = true
	s.mu.Unlock()

	return &types.MCPResponse{
		JSONRPC: "2.0",
		Result:  result,
	}, nil
}

func (s *MCPServer) handleInitialized(ctx context.Context, params json.RawMessage, session *types.SessionContext) (*types.MCPResponse, error) {
	s.logger.Println("Client initialized successfully")
	return &types.MCPResponse{
		JSONRPC: "2.0",
		Result:  map[string]interface{}{},
	}, nil
}

func (s *MCPServer) handleListTools(ctx context.Context, params json.RawMessage, session *types.SessionContext) (*types.MCPResponse, error) {
	tools := []types.Tool{
		{
			Name:        "query_allocations",
			Description: "Query Kubernetes allocation costs with natural language. Supports filtering by namespace, pod, service, cluster, time range, and more. Provides cost breakdowns, efficiency metrics, and optimization recommendations.",
			InputSchema: types.ToolInputSchema{
				Type: "object",
				Properties: map[string]interface{}{
					"query": map[string]interface{}{
						"type":        "string",
						"description": "Natural language query for allocation costs (e.g., 'Show me the most expensive pods in production namespace last week')",
					},
					"window": map[string]interface{}{
						"type":        "string",
						"description": "Time window for the query (e.g., '7d', '1d', '1h', '2023-01-01T00:00:00Z,2023-01-02T00:00:00Z')",
						"default":     "1d",
					},
					"aggregate": map[string]interface{}{
						"type":        "string",
						"description": "Aggregation level: namespace, pod, container, service, deployment, etc.",
						"default":     "namespace",
					},
					"step": map[string]interface{}{
						"type":        "string",
						"description": "Step size for time series data (e.g., '1h', '1d')",
						"default":     "1d",
					},
					"filter": map[string]interface{}{
						"type":        "string",
						"description": "Filter expression for allocations",
					},
				},
				Required: []string{"query"},
			},
		},
		{
			Name:        "query_assets",
			Description: "Query cloud and Kubernetes asset costs with natural language. Includes compute nodes, storage volumes, load balancers, and other infrastructure assets. Provides utilization analysis and right-sizing recommendations.",
			InputSchema: types.ToolInputSchema{
				Type: "object",
				Properties: map[string]interface{}{
					"query": map[string]interface{}{
						"type":        "string",
						"description": "Natural language query for asset costs (e.g., 'Which nodes have low utilization and could be downsized?')",
					},
					"window": map[string]interface{}{
						"type":        "string",
						"description": "Time window for the query",
						"default":     "1d",
					},
					"aggregate": map[string]interface{}{
						"type":        "string",
						"description": "Aggregation level: type, name, cluster, etc.",
						"default":     "type",
					},
					"filter": map[string]interface{}{
						"type":        "string",
						"description": "Filter expression for assets",
					},
				},
				Required: []string{"query"},
			},
		},
		{
			Name:        "query_cloud_costs",
			Description: "Query cloud provider billing data with natural language. Supports AWS, GCP, Azure cost analysis with service breakdowns, usage patterns, and cost optimization opportunities.",
			InputSchema: types.ToolInputSchema{
				Type: "object",
				Properties: map[string]interface{}{
					"query": map[string]interface{}{
						"type":        "string",
						"description": "Natural language query for cloud costs (e.g., 'Show me AWS spend by service for last month with anomalies')",
					},
					"window": map[string]interface{}{
						"type":        "string",
						"description": "Time window for the query",
						"default":     "7d",
					},
					"aggregate": map[string]interface{}{
						"type":        "string",
						"description": "Aggregation level: provider, service, account, region, etc.",
						"default":     "service",
					},
					"filter": map[string]interface{}{
						"type":        "string",
						"description": "Filter expression for cloud costs",
					},
				},
				Required: []string{"query"},
			},
		},
	}

	result := types.ListToolsResult{
		Tools: tools,
	}

	return &types.MCPResponse{
		JSONRPC: "2.0",
		Result:  result,
	}, nil
}

func (s *MCPServer) handleCallTool(ctx context.Context, params json.RawMessage, session *types.SessionContext) (*types.MCPResponse, error) {
	var callParams types.CallToolParams
	if err := json.Unmarshal(params, &callParams); err != nil {
		return nil, fmt.Errorf("invalid call tool parameters: %w", err)
	}

	// Check if server is initialized
	s.mu.RLock()
	initialized := s.initialized
	s.mu.RUnlock()

	if !initialized {
		return nil, fmt.Errorf("server not initialized")
	}

	// Create query context
	queryCtx := types.NewQueryContext(ctx, session, ctx.Value("request_id"), callParams.Name, "", callParams.Arguments)

	// Handle different tool calls
	var result *types.CallToolResult
	var err error

	switch callParams.Name {
	case "query_allocations":
		result, err = s.handleQueryAllocations(queryCtx, callParams.Arguments)
	case "query_assets":
		result, err = s.handleQueryAssets(queryCtx, callParams.Arguments)
	case "query_cloud_costs":
		result, err = s.handleQueryCloudCosts(queryCtx, callParams.Arguments)
	default:
		return nil, fmt.Errorf("unknown tool: %s", callParams.Name)
	}

	if err != nil {
		result = &types.CallToolResult{
			Content: []types.ToolContent{
				{
					Type: "text",
					Text: fmt.Sprintf("Error: %s", err.Error()),
				},
			},
			IsError: true,
		}
	}

	// Record query in session history
	historyEntry := types.QueryHistoryEntry{
		Timestamp:  time.Now(),
		ToolName:   callParams.Name,
		Query:      fmt.Sprintf("%v", callParams.Arguments["query"]),
		Parameters: callParams.Arguments,
		Duration:   queryCtx.GetDuration(),
		Success:    !result.IsError,
	}

	if result.IsError && len(result.Content) > 0 {
		historyEntry.ErrorMsg = result.Content[0].Text
	} else if len(result.Content) > 0 {
		historyEntry.Response = result.Content[0].Text
	}

	session.AddQueryToHistory(historyEntry)

	return &types.MCPResponse{
		JSONRPC: "2.0",
		Result:  result,
	}, nil
}

func (s *MCPServer) handleCancelled(ctx context.Context, params json.RawMessage, session *types.SessionContext) (*types.MCPResponse, error) {
	s.logger.Println("Request cancelled by client")
	return &types.MCPResponse{
		JSONRPC: "2.0",
		Result:  map[string]interface{}{},
	}, nil
}

func (s *MCPServer) handlePing(ctx context.Context, params json.RawMessage, session *types.SessionContext) (*types.MCPResponse, error) {
	return &types.MCPResponse{
		JSONRPC: "2.0",
		Result: map[string]interface{}{
			"status":    "ok",
			"timestamp": time.Now().Unix(),
			"version":   ServerVersion,
		},
	}, nil
}

// Tool-specific handlers are implemented in handlers.go

// Utility methods

func (s *MCPServer) getSessionID(ctx context.Context) string {
	// Extract session ID from context or generate one
	if sessionID := ctx.Value("session_id"); sessionID != nil {
		if id, ok := sessionID.(string); ok {
			return id
		}
	}
	// Generate a simple session ID based on timestamp
	return fmt.Sprintf("session_%d", time.Now().UnixNano())
}

func (s *MCPServer) getClientInfo(request *types.MCPRequest) types.ClientInfo {
	// Extract client info from request or use defaults
	return types.ClientInfo{
		Name:    "unknown",
		Version: "unknown",
	}
}

func (s *MCPServer) setCORSHeaders(w http.ResponseWriter, r *http.Request) {
	origin := r.Header.Get("Origin")
	if origin == "" {
		origin = "*"
	}

	// Check if origin is allowed
	allowed := false
	for _, allowedOrigin := range s.config.AllowedOrigins {
		if allowedOrigin == "*" || allowedOrigin == origin {
			allowed = true
			break
		}
	}

	if allowed {
		w.Header().Set("Access-Control-Allow-Origin", origin)
	}

	w.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
	w.Header().Set("Access-Control-Max-Age", "3600")
}

func (s *MCPServer) writeErrorResponse(w http.ResponseWriter, id interface{}, code int, message string, err error) {
	errorResp := &types.MCPResponse{
		JSONRPC: "2.0",
		ID:      id,
		Error: &types.MCPError{
			Code:    code,
			Message: message,
		},
	}

	if err != nil && s.config.EnableDebug {
		errorResp.Error.Data = err.Error()
	}

	s.writeJSONResponse(w, errorResp)
}

func (s *MCPServer) writeJSONResponse(w http.ResponseWriter, response *types.MCPResponse) {
	w.Header().Set("Content-Type", "application/json")

	if err := json.NewEncoder(w).Encode(response); err != nil {
		s.logger.Printf("Failed to write response: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
	}
}

func (s *MCPServer) cleanupExpiredSessions() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		s.sessionManager.CleanupExpiredSessions()
	}
}

// GetSessionManager returns the session manager (for testing)
func (s *MCPServer) GetSessionManager() *types.SessionManager {
	return s.sessionManager
}

// GetOpenCostClient returns the OpenCost client (for testing)
func (s *MCPServer) GetOpenCostClient() *client.OpenCostClient {
	return s.openCostClient
}

// IsInitialized returns true if the server has been initialized
func (s *MCPServer) IsInitialized() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.initialized
}