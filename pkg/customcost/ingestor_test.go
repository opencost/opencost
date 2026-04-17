package customcost

import (
	"fmt"
	"os/exec"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/go-plugin"
	"github.com/opencost/opencost/core/pkg/opencost"
)

func TestIngestor_Stop_KillsPluginProcesses(t *testing.T) {
	cmd := exec.Command("sleep", "60")
	client := plugin.NewClient(&plugin.ClientConfig{
		HandshakeConfig: plugin.HandshakeConfig{
			ProtocolVersion:  1,
			MagicCookieKey:   "test",
			MagicCookieValue: "test",
		},
		Cmd:          cmd,
		StartTimeout: 2 * time.Second,
	})
	// Start the process (handshake will fail but process runs)
	_, _ = client.Client()

	ingestor := &CustomCostIngestor{
		plugins: map[string]pluginConnector{
			"test-plugin": client,
		},
	}
	ingestor.Stop()

	if !client.Exited() {
		t.Error("Expected plugin client process to be terminated after Stop()")
	}
}

func TestIngestor_Stop_MultiplePlugins(t *testing.T) {
	connectors := make(map[string]pluginConnector)
	clients := make(map[string]*plugin.Client)
	for _, name := range []string{"plugin-a", "plugin-b", "plugin-c"} {
		cmd := exec.Command("sleep", "60")
		client := plugin.NewClient(&plugin.ClientConfig{
			HandshakeConfig: plugin.HandshakeConfig{
				ProtocolVersion:  1,
				MagicCookieKey:   "test",
				MagicCookieValue: name,
			},
			Cmd:          cmd,
			StartTimeout: 2 * time.Second,
		})
		_, _ = client.Client()
		connectors[name] = client
		clients[name] = client
	}

	ingestor := &CustomCostIngestor{plugins: connectors}
	ingestor.Stop()

	for name, client := range clients {
		if !client.Exited() {
			t.Errorf("Expected plugin %s to be terminated after Stop()", name)
		}
	}
}

func TestIngestor_Stop_EmptyPluginsMap(t *testing.T) {
	ingestor := &CustomCostIngestor{
		plugins: map[string]pluginConnector{},
	}
	ingestor.Stop() // covers lock path with 0 iterations
}

func TestIngestor_Stop_NilPluginsMap(t *testing.T) {
	ingestor := &CustomCostIngestor{}
	ingestor.Stop() // should not panic
}

func TestIngestor_Stop_AlreadyStopping(t *testing.T) {
	ingestor := &CustomCostIngestor{
		plugins: map[string]pluginConnector{},
	}
	ingestor.isStopping.Store(true) // atomic.Bool must use Store()!
	ingestor.Stop()                 // should return immediately
}

func TestIngestor_Stop_ConcurrentCalls(t *testing.T) {
	ingestor := &CustomCostIngestor{
		plugins: map[string]pluginConnector{},
	}

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ingestor.Stop()
		}()
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// success
	case <-time.After(5 * time.Second):
		t.Fatal("Concurrent Stop() calls deadlocked")
	}
}

func TestIngestor_Stop_WithStartedIngestor(t *testing.T) {
	repo := NewMemoryRepository()
	config := &CustomCostIngestorConfig{
		DailyDuration:     7 * 24 * time.Hour,
		HourlyDuration:    16 * time.Hour,
		DailyQueryWindow:  24 * time.Hour,
		HourlyQueryWindow: time.Hour,
	}

	ingestor, err := NewCustomCostIngestor(config, repo, map[string]*plugin.Client{}, time.Hour)
	if err != nil {
		t.Fatalf("Failed to create ingestor: %v", err)
	}

	ingestor.Start(false)
	time.Sleep(100 * time.Millisecond)

	done := make(chan struct{})
	go func() {
		ingestor.Stop()
		close(done)
	}()

	select {
	case <-done:
		// success
	case <-time.After(5 * time.Second):
		t.Fatal("Stop() on started ingestor timed out")
	}

	if ingestor.isRunning.Load() {
		t.Error("Expected isRunning to be false after Stop()")
	}
	if ingestor.isStopping.Load() {
		t.Error("Expected isStopping to be false after Stop()")
	}
}

// TestIngestor_BuildWindow_WithPlugin covers pluginsLock paths inside buildSingleDomain.
// Using a command that exits immediately causes client.Client() to fail fast, exercising
// the RLock/RUnlock calls and the error-return path without hanging.
func TestIngestor_BuildWindow_WithPlugin(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("requires Unix false command")
	}

	cmd := exec.Command("false") // exits immediately with failure
	client := plugin.NewClient(&plugin.ClientConfig{
		HandshakeConfig: plugin.HandshakeConfig{
			ProtocolVersion:  1,
			MagicCookieKey:   "test",
			MagicCookieValue: "test",
		},
		Cmd:          cmd,
		StartTimeout: 2 * time.Second,
	})
	t.Cleanup(func() { client.Kill() })

	repo := NewMemoryRepository()
	config := &CustomCostIngestorConfig{
		DailyDuration:     24 * time.Hour,
		HourlyDuration:    time.Hour,
		DailyQueryWindow:  24 * time.Hour,
		HourlyQueryWindow: time.Hour,
	}

	ingestor, err := NewCustomCostIngestor(config, repo, map[string]*plugin.Client{"test-plugin": client}, 24*time.Hour)
	if err != nil {
		t.Fatalf("Failed to create ingestor: %v", err)
	}

	now := time.Now().UTC()
	// BuildWindow iterates the plugins map, exercising pluginsLock in both
	// BuildWindow and buildSingleDomain; client.Client() fails fast (false exits)
	ingestor.BuildWindow(now.Add(-time.Hour), now)
}

// mockClientProtocol implements plugin.ClientProtocol for testing.
type mockClientProtocol struct {
	dispenseResult interface{}
	dispenseErr    error
}

func (m *mockClientProtocol) Dispense(string) (interface{}, error) {
	return m.dispenseResult, m.dispenseErr
}
func (m *mockClientProtocol) Ping() error  { return nil }
func (m *mockClientProtocol) Close() error { return nil }

// mockPluginConnector implements pluginConnector for testing.
type mockPluginConnector struct {
	protocol  plugin.ClientProtocol
	clientErr error
	killed    bool
}

func (m *mockPluginConnector) Client() (plugin.ClientProtocol, error) {
	if m.clientErr != nil {
		return nil, m.clientErr
	}
	return m.protocol, nil
}

func (m *mockPluginConnector) Kill() { m.killed = true }

func TestBuildSingleDomain_InvalidPluginType_NoPanic(t *testing.T) {
	mock := &mockPluginConnector{
		protocol: &mockClientProtocol{
			dispenseResult: "not a CustomCostSource", // wrong type
		},
	}

	repo := NewMemoryRepository()
	ingestor := &CustomCostIngestor{
		plugins:    map[string]pluginConnector{"bad-plugin": mock},
		resolution: time.Hour,
		repo:       repo,
		coverage:   map[string]opencost.Window{},
	}

	now := time.Now().UTC()
	// Before the fix this would panic; now it should log an error and return.
	ingestor.BuildWindow(now.Add(-time.Hour), now)
}

func TestBuildSingleDomain_DispenseError(t *testing.T) {
	mock := &mockPluginConnector{
		protocol: &mockClientProtocol{
			dispenseErr: fmt.Errorf("dispense failed"),
		},
	}

	repo := NewMemoryRepository()
	ingestor := &CustomCostIngestor{
		plugins:    map[string]pluginConnector{"err-plugin": mock},
		resolution: time.Hour,
		repo:       repo,
		coverage:   map[string]opencost.Window{},
	}

	now := time.Now().UTC()
	// Should handle the error gracefully without panic.
	ingestor.BuildWindow(now.Add(-time.Hour), now)
}

func TestBuildSingleDomain_ClientError(t *testing.T) {
	mock := &mockPluginConnector{
		clientErr: fmt.Errorf("connection failed"),
	}

	repo := NewMemoryRepository()
	ingestor := &CustomCostIngestor{
		plugins:    map[string]pluginConnector{"fail-plugin": mock},
		resolution: time.Hour,
		repo:       repo,
		coverage:   map[string]opencost.Window{},
	}

	now := time.Now().UTC()
	// Should handle the error gracefully without panic.
	ingestor.BuildWindow(now.Add(-time.Hour), now)
}
