package costmodel

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/opencost/opencost/pkg/cloud/models"
)

type testPricingProvider struct {
	models.Provider
	mu            sync.Mutex
	downloadCount int
	errToReturn   error
}

func (t *testPricingProvider) DownloadPricingData() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.downloadCount++
	return t.errToReturn
}

func (t *testPricingProvider) getDownloadCount() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.downloadCount
}

func TestStartPricingRefreshWorker_Disabled(t *testing.T) {
	// Save and restore original getPricingRefreshInterval
	originalInterval := getPricingRefreshInterval
	defer func() { getPricingRefreshInterval = originalInterval }()

	// Set interval to <= 0 to disable worker
	getPricingRefreshInterval = func() time.Duration {
		return 0
	}

	provider := &testPricingProvider{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := StartPricingRefreshWorker(ctx, provider)
	if err != nil {
		t.Fatalf("unexpected error starting pricing refresh worker: %v", err)
	}

	// Sleep slightly to confirm no goroutine runs/downloads
	time.Sleep(50 * time.Millisecond)

	if provider.getDownloadCount() > 0 {
		t.Errorf("expected 0 downloads when worker is disabled, got %d", provider.getDownloadCount())
	}
}

func TestStartPricingRefreshWorker_PeriodicTicks(t *testing.T) {
	// Save and restore original getPricingRefreshInterval
	originalInterval := getPricingRefreshInterval
	defer func() { getPricingRefreshInterval = originalInterval }()

	// Set a very short interval for testing
	getPricingRefreshInterval = func() time.Duration {
		return 10 * time.Millisecond
	}

	provider := &testPricingProvider{}
	ctx, cancel := context.WithCancel(context.Background())

	err := StartPricingRefreshWorker(ctx, provider)
	if err != nil {
		t.Fatalf("unexpected error starting pricing refresh worker: %v", err)
	}

	// Wait for at least 2 ticks
	time.Sleep(35 * time.Millisecond)
	cancel() // Stop the worker

	// Give the worker a moment to observe cancellation and finish any in-flight tick.
	time.Sleep(20 * time.Millisecond)

	count := provider.getDownloadCount()
	if count < 2 {
		t.Errorf("expected at least 2 downloads, got %d", count)
	}

	// Verify no further downloads occur after cancel
	time.Sleep(20 * time.Millisecond)
	postCancelCount := provider.getDownloadCount()
	if postCancelCount != count {
		t.Errorf("expected download count to remain %d after cancellation, but got %d", count, postCancelCount)
	}
}

func TestStartPricingRefreshWorker_ErrorHandling(t *testing.T) {
	// Save and restore original getPricingRefreshInterval
	originalInterval := getPricingRefreshInterval
	defer func() { getPricingRefreshInterval = originalInterval }()

	getPricingRefreshInterval = func() time.Duration {
		return 10 * time.Millisecond
	}

	provider := &testPricingProvider{
		errToReturn: errors.New("download failed"),
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := StartPricingRefreshWorker(ctx, provider)
	if err != nil {
		t.Fatalf("unexpected error starting pricing refresh worker: %v", err)
	}

	// Wait for at least 1 tick
	time.Sleep(15 * time.Millisecond)

	count := provider.getDownloadCount()
	if count < 1 {
		t.Errorf("expected at least 1 download attempt, got %d", count)
	}
}

func TestStartPricingRefreshWorker_NilArguments(t *testing.T) {
	provider := &testPricingProvider{}
	ctx := context.Background()

	// 1. Nil context
	err := StartPricingRefreshWorker(nil, provider)
	if err == nil {
		t.Errorf("expected error when context is nil, got nil")
	}

	// 2. Nil provider
	err = StartPricingRefreshWorker(ctx, nil)
	if err == nil {
		t.Errorf("expected error when provider is nil, got nil")
	}
}
