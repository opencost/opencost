package types

import (
	"context"
	"encoding/json"
	"sync"
	"time"
)

// JSON-RPC 2.0 Protocol Types

// MCPRequest represents a JSON-RPC 2.0 request
type MCPRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      interface{}     `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

// MCPResponse represents a JSON-RPC 2.0 response
type MCPResponse struct {
	JSONRPC string      `json:"jsonrpc"`
	ID      interface{} `json:"id,omitempty"`
	Result  interface{} `json:"result,omitempty"`
	Error   *MCPError   `json:"error,omitempty"`
}

// MCPError represents a JSON-RPC 2.0 error
type MCPError struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
	Data    interface{} `json:"data,omitempty"`
}

// Standard JSON-RPC 2.0 error codes
const (
	ParseError     = -32700
	InvalidRequest = -32600
	MethodNotFound = -32601
	InvalidParams  = -32602
	InternalError  = -32603
)

// MCP Protocol Specific Types

// ServerInfo represents MCP server information
type ServerInfo struct {
	Name        string `json:"name"`
	Version     string `json:"version"`
	Description string `json:"description"`
}

// ClientInfo represents MCP client information
type ClientInfo struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

// InitializeParams represents initialization parameters
type InitializeParams struct {
	ProtocolVersion string      `json:"protocolVersion"`
	Capabilities    Capabilities `json:"capabilities"`
	ClientInfo      ClientInfo   `json:"clientInfo"`
}

// InitializeResult represents initialization result
type InitializeResult struct {
	ProtocolVersion string       `json:"protocolVersion"`
	Capabilities    Capabilities `json:"capabilities"`
	ServerInfo      ServerInfo   `json:"serverInfo"`
}

// Capabilities represents server/client capabilities
type Capabilities struct {
	Tools     *ToolCapabilities     `json:"tools,omitempty"`
	Resources *ResourceCapabilities `json:"resources,omitempty"`
	Prompts   *PromptCapabilities   `json:"prompts,omitempty"`
	Logging   *LoggingCapabilities  `json:"logging,omitempty"`
}

// ToolCapabilities represents tool-related capabilities
type ToolCapabilities struct {
	ListChanged bool `json:"listChanged,omitempty"`
}

// ResourceCapabilities represents resource-related capabilities
type ResourceCapabilities struct {
	Subscribe   bool `json:"subscribe,omitempty"`
	ListChanged bool `json:"listChanged,omitempty"`
}

// PromptCapabilities represents prompt-related capabilities
type PromptCapabilities struct {
	ListChanged bool `json:"listChanged,omitempty"`
}

// LoggingCapabilities represents logging-related capabilities
type LoggingCapabilities struct {
	Level string `json:"level,omitempty"`
}

// Tool Types

// Tool represents an MCP tool
type Tool struct {
	Name        string         `json:"name"`
	Description string         `json:"description"`
	InputSchema ToolInputSchema `json:"inputSchema"`
}

// ToolInputSchema represents the JSON schema for tool input
type ToolInputSchema struct {
	Type       string                 `json:"type"`
	Properties map[string]interface{} `json:"properties"`
	Required   []string               `json:"required,omitempty"`
}

// ListToolsResult represents the result of listing tools
type ListToolsResult struct {
	Tools []Tool `json:"tools"`
}

// CallToolParams represents parameters for calling a tool
type CallToolParams struct {
	Name      string                 `json:"name"`
	Arguments map[string]interface{} `json:"arguments,omitempty"`
}

// CallToolResult represents the result of calling a tool
type CallToolResult struct {
	Content []ToolContent `json:"content"`
	IsError bool          `json:"isError,omitempty"`
}

// ToolContent represents content returned by a tool
type ToolContent struct {
	Type string `json:"type"`
	Text string `json:"text,omitempty"`
	Data string `json:"data,omitempty"`
}

// Session Management Types

// SessionContext represents a conversation session
type SessionContext struct {
	SessionID    string                 `json:"session_id"`
	ClientInfo   ClientInfo             `json:"client_info"`
	StartTime    time.Time              `json:"start_time"`
	LastActivity time.Time              `json:"last_activity"`
	QueryHistory []QueryHistoryEntry    `json:"query_history"`
	Context      map[string]interface{} `json:"context"`
	mu           sync.RWMutex           `json:"-"`
}

// QueryHistoryEntry represents a single query in the conversation history
type QueryHistoryEntry struct {
	Timestamp   time.Time              `json:"timestamp"`
	ToolName    string                 `json:"tool_name"`
	Query       string                 `json:"query"`
	Parameters  map[string]interface{} `json:"parameters"`
	Response    string                 `json:"response"`
	Duration    time.Duration          `json:"duration"`
	Success     bool                   `json:"success"`
	ErrorMsg    string                 `json:"error_msg,omitempty"`
	Insights    []string               `json:"insights,omitempty"`
	Suggestions []string               `json:"suggestions,omitempty"`
}

// SessionManager manages conversation sessions
type SessionManager struct {
	sessions map[string]*SessionContext
	mu       sync.RWMutex
	timeout  time.Duration
}

// NewSessionManager creates a new session manager
func NewSessionManager(timeout time.Duration) *SessionManager {
	return &SessionManager{
		sessions: make(map[string]*SessionContext),
		timeout:  timeout,
	}
}

// GetOrCreateSession gets an existing session or creates a new one
func (sm *SessionManager) GetOrCreateSession(sessionID string, clientInfo ClientInfo) *SessionContext {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if session, exists := sm.sessions[sessionID]; exists {
		session.mu.Lock()
		session.LastActivity = time.Now()
		session.mu.Unlock()
		return session
	}

	session := &SessionContext{
		SessionID:    sessionID,
		ClientInfo:   clientInfo,
		StartTime:    time.Now(),
		LastActivity: time.Now(),
		QueryHistory: make([]QueryHistoryEntry, 0),
		Context:      make(map[string]interface{}),
	}

	sm.sessions[sessionID] = session
	return session
}

// GetSession retrieves an existing session
func (sm *SessionManager) GetSession(sessionID string) *SessionContext {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.sessions[sessionID]
}

// CleanupExpiredSessions removes expired sessions
func (sm *SessionManager) CleanupExpiredSessions() {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	now := time.Now()
	for sessionID, session := range sm.sessions {
		session.mu.RLock()
		expired := now.Sub(session.LastActivity) > sm.timeout
		session.mu.RUnlock()

		if expired {
			delete(sm.sessions, sessionID)
		}
	}
}

// AddQueryToHistory adds a query to the session history
func (sc *SessionContext) AddQueryToHistory(entry QueryHistoryEntry) {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	sc.QueryHistory = append(sc.QueryHistory, entry)
	sc.LastActivity = time.Now()

	// Keep only the last 50 queries
	if len(sc.QueryHistory) > 50 {
		sc.QueryHistory = sc.QueryHistory[len(sc.QueryHistory)-50:]
	}
}

// GetRecentQueries returns recent queries from the session
func (sc *SessionContext) GetRecentQueries(limit int) []QueryHistoryEntry {
	sc.mu.RLock()
	defer sc.mu.RUnlock()

	if limit <= 0 || limit > len(sc.QueryHistory) {
		limit = len(sc.QueryHistory)
	}

	start := len(sc.QueryHistory) - limit
	if start < 0 {
		start = 0
	}

	result := make([]QueryHistoryEntry, limit)
	copy(result, sc.QueryHistory[start:])
	return result
}

// SetContextValue sets a context value for the session
func (sc *SessionContext) SetContextValue(key string, value interface{}) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.Context[key] = value
}

// GetContextValue gets a context value from the session
func (sc *SessionContext) GetContextValue(key string) (interface{}, bool) {
	sc.mu.RLock()
	defer sc.mu.RUnlock()
	value, exists := sc.Context[key]
	return value, exists
}

// OpenCost Integration Types

// QueryContext represents the context for executing queries
type QueryContext struct {
	Session      *SessionContext
	RequestID    interface{}
	StartTime    time.Time
	QueryType    string
	NLQuery      string
	Parameters   map[string]interface{}
	ClientInfo   ClientInfo
	Context      context.Context
}

// NewQueryContext creates a new query context
func NewQueryContext(ctx context.Context, session *SessionContext, requestID interface{}, queryType, nlQuery string, params map[string]interface{}) *QueryContext {
	return &QueryContext{
		Session:    session,
		RequestID:  requestID,
		StartTime:  time.Now(),
		QueryType:  queryType,
		NLQuery:    nlQuery,
		Parameters: params,
		ClientInfo: session.ClientInfo,
		Context:    ctx,
	}
}

// GetDuration returns the duration since the query context was created
func (qc *QueryContext) GetDuration() time.Duration {
	return time.Since(qc.StartTime)
}

// ValidationError represents a validation error
type ValidationError struct {
	Field   string `json:"field"`
	Message string `json:"message"`
	Code    string `json:"code"`
}

// Error implements the error interface
func (v *ValidationError) Error() string {
	return v.Message
}

// ValidationErrors represents multiple validation errors
type ValidationErrors struct {
	Errors []ValidationError `json:"errors"`
}

// Error implements the error interface
func (v *ValidationErrors) Error() string {
	if len(v.Errors) == 0 {
		return "validation failed"
	}
	return v.Errors[0].Message
}

// HasErrors returns true if there are validation errors
func (v *ValidationErrors) HasErrors() bool {
	return len(v.Errors) > 0
}

// Add adds a validation error
func (v *ValidationErrors) Add(field, message, code string) {
	v.Errors = append(v.Errors, ValidationError{
		Field:   field,
		Message: message,
		Code:    code,
	})
}