package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/opencost/opencost/pkg/mcp/server"
	"github.com/rs/zerolog/log"
)

func main() {
	log.Info().Msg("Starting OpenCost MCP Server")

	// Create MCP server
	mcpServer, err := server.NewMCPServer()
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create MCP server")
	}

	// Start server in goroutine
	go func() {
		if err := mcpServer.Start(); err != nil {
			log.Fatal().Err(err).Msg("MCP server failed")
		}
	}()

	// Wait for interrupt signal
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c

	log.Info().Msg("Shutting down MCP server")
	mcpServer.Shutdown(context.Background())
}