package costmodel

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	mcp_sdk "github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/opencost/opencost/pkg/costmodel"
	"github.com/opencost/opencost/pkg/env"
	opencost_mcp "github.com/opencost/opencost/pkg/mcp"
)

func TestMCPServerGracefulShutdown(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	addr := listener.Addr().String()
	done := startMCPServer(ctx, &costmodel.Accesses{}, nil, listener)
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get("http://" + addr + "/")
	if err != nil {
		t.Fatalf("MCP server did not respond: %v", err)
	}
	resp.Body.Close()

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("MCP server did not shut down")
	}

	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err == nil {
		conn.Close()
		t.Error("Server still accepting connections after shutdown")
	}
}

func TestStartMCPServerReturnsBindError(t *testing.T) {
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	port := listener.Addr().(*net.TCPAddr).Port
	t.Setenv(env.MCPHTTPPortEnvVar, fmt.Sprint(port))

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	err = StartMCPServer(ctx, &costmodel.Accesses{}, nil)
	var opErr *net.OpError
	if !errors.As(err, &opErr) || opErr.Op != "listen" {
		t.Fatalf("expected listen error on occupied port, got %v", err)
	}
}

// TestShutdownTimeoutConstant verifies the shutdown timeout constant is set correctly
func TestShutdownTimeoutConstant(t *testing.T) {
	if shutdownTimeout != 30*time.Second {
		t.Errorf("Expected shutdown timeout of 30s, got %v", shutdownTimeout)
	}
}

// TestGracefulShutdownConfiguration verifies graceful shutdown works with the configured timeout
func TestGracefulShutdownConfiguration(t *testing.T) {
	if shutdownTimeout < 5*time.Second {
		t.Error("Shutdown timeout is too short for graceful shutdown")
	}
}

// TestMCPToolsList verifies that all MCP tools are registered with detailed
// descriptions and complete JSON schemas with property descriptions.
func TestMCPToolsList(t *testing.T) {
	ctx := t.Context()
	clientTransport, serverTransport := mcp_sdk.NewInMemoryTransports()

	mcpServer := opencost_mcp.NewMCPServer(nil, nil, nil)
	sdkServer := newMCPSDKServer(mcpServer)

	serverSession, err := sdkServer.Connect(ctx, serverTransport, nil)
	if err != nil {
		t.Fatalf("failed to connect server session: %v", err)
	}
	defer serverSession.Close()

	client := mcp_sdk.NewClient(&mcp_sdk.Implementation{Name: "test-client", Version: "1.0"}, nil)
	clientSession, err := client.Connect(ctx, clientTransport, nil)
	if err != nil {
		t.Fatalf("failed to connect client session: %v", err)
	}
	defer clientSession.Close()

	toolsResult, err := clientSession.ListTools(ctx, nil)
	if err != nil {
		t.Fatalf("ListTools failed: %v", err)
	}

	expectedTools := []string{
		"get_allocation_costs",
		"get_asset_costs",
		"get_cloud_costs",
		"get_efficiency",
	}

	if len(toolsResult.Tools) != len(expectedTools) {
		t.Fatalf("expected %d tools, got %d", len(expectedTools), len(toolsResult.Tools))
	}

	toolMap := make(map[string]*mcp_sdk.Tool)
	for _, tool := range toolsResult.Tools {
		toolMap[tool.Name] = tool
	}

	for _, name := range expectedTools {
		tool, ok := toolMap[name]
		if !ok {
			t.Fatalf("tool %q not found in tools list", name)
		}

		// Verify Description
		if tool.Description == "" {
			t.Errorf("tool %q has empty description", name)
		}
		if len(tool.Description) < 100 {
			t.Errorf("tool %q description is unexpectedly short (%d chars): %s", name, len(tool.Description), tool.Description)
		}
		if !strings.Contains(tool.Description, "window (required)") {
			t.Errorf("tool %q description does not document window (required)", name)
		}

		// Verify InputSchema
		if tool.InputSchema == nil {
			t.Fatalf("tool %q has nil InputSchema", name)
		}

		schemaBytes, err := json.Marshal(tool.InputSchema)
		if err != nil {
			t.Fatalf("tool %q InputSchema failed to marshal to JSON: %v", name, err)
		}

		var rawSchema struct {
			Type       string                 `json:"type"`
			Properties map[string]interface{} `json:"properties"`
			Required   []string               `json:"required"`
		}
		if err := json.Unmarshal(schemaBytes, &rawSchema); err != nil {
			t.Fatalf("tool %q failed to unmarshal schema JSON: %v", name, err)
		}

		if rawSchema.Type != "object" {
			t.Errorf("tool %q schema type expected 'object', got %q", name, rawSchema.Type)
		}

		if len(rawSchema.Properties) == 0 {
			t.Errorf("tool %q has no properties in schema", name)
		}

		// Verify 'window' is in properties and is required
		if _, ok := rawSchema.Properties["window"]; !ok {
			t.Errorf("tool %q schema is missing 'window' property", name)
		}
		if !slices.Contains(rawSchema.Required, "window") {
			t.Errorf("tool %q schema required fields do not contain 'window': %v", name, rawSchema.Required)
		}

		// Verify each property has a non-empty description
		for propName, propVal := range rawSchema.Properties {
			propMap, ok := propVal.(map[string]interface{})
			if !ok {
				t.Errorf("tool %q property %q is not a map", name, propName)
				continue
			}
			desc, ok := propMap["description"].(string)
			if !ok || desc == "" {
				t.Errorf("tool %q property %q has missing or empty description: %v", name, propName, propMap)
			}
		}

		// Verify optional fields are NOT required
		if slices.Contains(rawSchema.Required, "aggregate") {
			t.Errorf("tool %q schema should not require 'aggregate'", name)
		}
		if slices.Contains(rawSchema.Required, "filter") {
			t.Errorf("tool %q schema should not require 'filter'", name)
		}
	}
}
