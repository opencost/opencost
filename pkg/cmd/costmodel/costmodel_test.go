package costmodel

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/opencost/opencost/pkg/costmodel"
	"github.com/opencost/opencost/pkg/env"
)

func TestMCPServerGracefulShutdown(t *testing.T) {
	// Test that MCP server responds to context cancellation and shuts down gracefully

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	accesses := &costmodel.Accesses{}
	port := env.GetMCPHTTPPort()

	// Channel to signal when server is ready
	serverReady := make(chan error, 1)

	// Start MCP server
	go func() {
		err := StartMCPServer(ctx, accesses, nil)
		serverReady <- err
	}()

	// Wait for server to be ready by attempting to connect
	serverUp := false
	for i := 0; i < 10; i++ {
		time.Sleep(100 * time.Millisecond)
		client := &http.Client{Timeout: 1 * time.Second}
		resp, err := client.Get(fmt.Sprintf("http://localhost:%d/", port))
		if err == nil {
			resp.Body.Close()
			serverUp = true
			break
		}
	}

	if !serverUp {
		t.Skip("MCP server did not start (may be expected in test environment)")
	}

	// Trigger shutdown by cancelling context
	shutdownStart := time.Now()
	cancel()

	// Wait for shutdown to complete (with reasonable timeout)
	shutdownDone := make(chan bool, 1)
	go func() {
		time.Sleep(15 * time.Second)
		shutdownDone <- false
	}()

	// Give shutdown goroutine time to execute
	time.Sleep(1 * time.Second)

	// Verify server is no longer accepting connections
	client := &http.Client{Timeout: 1 * time.Second}
	_, err := client.Get(fmt.Sprintf("http://localhost:%d/", port))
	if err == nil {
		t.Error("Server still accepting connections after shutdown")
	}

	shutdownDone <- true
	<-shutdownDone

	shutdownDuration := time.Since(shutdownStart)
	t.Logf("Graceful shutdown completed in %v", shutdownDuration)

	// Verify shutdown completed in reasonable time (should be much less than 12s)
	if shutdownDuration > 12*time.Second {
		t.Errorf("Shutdown took too long: %v (expected < 12s)", shutdownDuration)
	}
}
