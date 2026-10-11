package costmodel

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/opencost/opencost/pkg/costmodel"
	"github.com/opencost/opencost/pkg/env"
)

func TestMCPServerGracefulShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Use an OS-assigned port to avoid collision with a running MCP server
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to create listener: %v", err)
	}
	addr := listener.Addr().String()

	handler := http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	startMCPHTTPServer(ctx, listener, handler)

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
		t.Fatal("MCP server did not start")
	}

	// Trigger shutdown
	cancel()

	// Verify server is no longer accepting connections
	deadline := time.Now().Add(2 * time.Second)
	for {
		conn, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err != nil {
			break
		}
		conn.Close()

		if time.Now().After(deadline) {
			t.Fatal("Server still accepting connections after shutdown")
		}

		time.Sleep(50 * time.Millisecond)
	}
}

func TestStartMCPServerPortConflict(t *testing.T) {
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("failed to reserve port: %v", err)
	}
	defer listener.Close()

	_, port, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		t.Fatalf("failed to determine reserved port: %v", err)
	}

	t.Setenv(env.MCPHTTPPortEnvVar, port)

	err = StartMCPServer(context.Background(), &costmodel.Accesses{}, nil)
	if err == nil {
		t.Fatal("expected StartMCPServer to return an error when MCP port is already in use")
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
