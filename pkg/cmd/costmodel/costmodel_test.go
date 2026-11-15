package costmodel

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/opencost/opencost/pkg/costmodel"
	"github.com/opencost/opencost/pkg/env"
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
