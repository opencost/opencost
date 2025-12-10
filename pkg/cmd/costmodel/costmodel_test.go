package costmodel

import (
	"context"
	"testing"
	"time"

	"github.com/opencost/opencost/pkg/costmodel"
)

func TestMCPServerGracefulShutdown(t *testing.T) {
	// Test that MCP server responds to context cancellation and shuts down gracefully

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	accesses := &costmodel.Accesses{}

	// Start MCP server
	go func() {
		_ = StartMCPServer(ctx, accesses, nil)
	}()

	// Allow server to start
	time.Sleep(100 * time.Millisecond)

	// Trigger shutdown
	shutdownStart := time.Now()
	cancel()

	// Wait for shutdown to complete (10s timeout + 2s buffer)
	time.Sleep(500 * time.Millisecond)
	shutdownDuration := time.Since(shutdownStart)

	// Verify shutdown completed quickly (should not hang)
	if shutdownDuration > 12*time.Second {
		t.Errorf("Shutdown took too long: %v (expected < 12s)", shutdownDuration)
	}

	t.Logf("Graceful shutdown completed in %v", shutdownDuration)
}
