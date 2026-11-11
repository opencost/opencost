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

func waitForDownloadCount(t *testing.T, provider *testPricingProvider, targetCount int, timeout time.Duration) int {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		cnt := provider.getDownloadCount()
		if cnt >= targetCount {
			return cnt
		}
		time.Sleep(2 * time.Millisecond)
	}
	return provider.getDownloadCount()
}

func TestStartPricingRefreshWorker_Disabled(t *testing.T) {
	provider := &testPricingProvider{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := startPricingRefreshWorkerWithInterval(ctx, provider, 0)
	if err != nil {
		t.Fatalf("unexpected error starting pricing refresh worker: %v", err)
	}

	time.Sleep(30 * time.Millisecond)

	if provider.getDownloadCount() > 0 {
		t.Errorf("expected 0 downloads when worker is disabled, got %d", provider.getDownloadCount())
	}
}

func TestStartPricingRefreshWorker_PeriodicTicks(t *testing.T) {
	provider := &testPricingProvider{}
	ctx, cancel := context.WithCancel(context.Background())

	err := startPricingRefreshWorkerWithInterval(ctx, provider, 10*time.Millisecond)
	if err != nil {
		t.Fatalf("unexpected error starting pricing refresh worker: %v", err)
	}

	// Wait for at least 2 ticks deterministically
	count := waitForDownloadCount(t, provider, 2, 500*time.Millisecond)
	if count < 2 {
		t.Errorf("expected at least 2 downloads, got %d", count)
	}

	cancel() // Stop the worker

	// Wait briefly to allow cancellation to be processed
	time.Sleep(30 * time.Millisecond)
	postCancelCount := provider.getDownloadCount()

	// Wait further and verify count does not increase after cancellation
	time.Sleep(30 * time.Millisecond)
	if finalCount := provider.getDownloadCount(); finalCount != postCancelCount {
		t.Errorf("expected download count to remain %d after cancellation, but got %d", postCancelCount, finalCount)
	}
}

func TestStartPricingRefreshWorker_ErrorHandling(t *testing.T) {
	provider := &testPricingProvider{
		errToReturn: errors.New("download failed"),
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := startPricingRefreshWorkerWithInterval(ctx, provider, 10*time.Millisecond)
	if err != nil {
		t.Fatalf("unexpected error starting pricing refresh worker: %v", err)
	}

	count := waitForDownloadCount(t, provider, 1, 500*time.Millisecond)
	if count < 1 {
		t.Errorf("expected at least 1 download attempt, got %d", count)
	}
}

func TestStartPricingRefreshWorker_NilArguments(t *testing.T) {
	provider := &testPricingProvider{}
	ctx := context.Background()

	// 1. Nil context
	err := startPricingRefreshWorkerWithInterval(nil, provider, 10*time.Millisecond)
	if err == nil {
		t.Errorf("expected error when context is nil, got nil")
	}

	// 2. Nil provider
	err = startPricingRefreshWorkerWithInterval(ctx, nil, 10*time.Millisecond)
	if err == nil {
		t.Errorf("expected error when provider is nil, got nil")
	}
}
