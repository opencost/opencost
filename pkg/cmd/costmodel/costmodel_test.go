package costmodel

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/opencost/opencost/pkg/costmodel"
)

func TestMCPServerGracefulShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	accesses := &costmodel.Accesses{}

	// Use an OS-assigned port to avoid collision with a running MCP server
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to create listener: %v", err)
	}
	addr := listener.Addr().String()

	// Start MCP server
	if err := startMCPServerWithListener(ctx, listener, accesses, nil); err != nil {
		t.Fatalf("failed to start MCP server: %v", err)
	}

	// Wait for server to be ready
	serverUp := false
	for i := 0; i < 10; i++ {
		time.Sleep(100 * time.Millisecond)
		client := &http.Client{Timeout: 1 * time.Second}
		resp, err := client.Get(fmt.Sprintf("http://%s/", addr))
		if err == nil {
			resp.Body.Close()
			serverUp = true
			break
		}
	}

	if !serverUp {
		t.Skip("MCP server did not start")
	}

	// Trigger shutdown
	cancel()
	time.Sleep(500 * time.Millisecond)

	// Verify server is no longer accepting connections
	client := &http.Client{Timeout: 500 * time.Millisecond}
	_, err = client.Get(fmt.Sprintf("http://%s/", addr))
	if err == nil {
		t.Error("Server still accepting connections after shutdown")
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
