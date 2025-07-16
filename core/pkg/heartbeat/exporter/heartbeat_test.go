package exporter

import (
	"encoding/json"
	"fmt"
	"path"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/opencost/opencost/core/pkg/heartbeat"
	"github.com/opencost/opencost/core/pkg/storage"
	"github.com/opencost/opencost/core/pkg/util/sliceutil"
)

const (
	MockClusterId       = "mock-cluster-1"
	MockApplicationName = "mock-agent"
	MockVersion         = "1.0.0"
)

type MockHeartbeatMetadataProvider struct{}

func NewMockHeartbeatMetadataProvider() *MockHeartbeatMetadataProvider {
	return &MockHeartbeatMetadataProvider{}
}

func (m *MockHeartbeatMetadataProvider) GetMetadata() map[string]any {
	return map[string]any{
		"cluster_id": MockClusterId,
	}
}

func TestHeartbeatExporter(t *testing.T) {
	t.Parallel()

	mdp := NewMockHeartbeatMetadataProvider()
	store := storage.NewMemoryStorage()

	controller := NewHeartbeatExportController(MockClusterId, MockApplicationName, MockVersion, store, mdp)

	if !controller.Start(time.Second) {
		t.Fatal("Failed to start controller")
	}

	time.Sleep(10 * time.Second)
	controller.Stop()

	files, _ := store.List(path.Join(MockApplicationName, MockClusterId, heartbeat.HeartbeatEventName))
	if len(files) == 0 {
		t.Fatal("No files found in storage")
	}

	fileNames := sliceutil.Map(files, func(stat *storage.StorageInfo) string {
		return stat.Name
	})

	slices.Sort(fileNames)

	lastCheck := time.Time{}

	for _, f := range fileNames {
		fpath := filepath.Join(MockApplicationName, MockClusterId, heartbeat.HeartbeatEventName, f)
		data, err := store.Read(fpath)
		if err != nil {
			t.Fatalf("Failed to read file %s: %v", fpath, err)
		}

		hb := new(heartbeat.Heartbeat)
		if err := json.Unmarshal(data, hb); err != nil {
			t.Fatalf("Failed to unmarshal heartbeat data: %v", err)
		}

		fmt.Printf("%s: %d bytes\n%s\n\n", f, len(data), string(data))

		if hb.Metadata["cluster_id"] != MockClusterId {
			t.Fatalf("Expected cluster ID %s, got %s", MockClusterId, hb.Metadata["cluster_id"])
		}

		if hb.Timestamp.Before(lastCheck) {
			t.Fatalf("Expected timestamp %s to be after %s", hb.Timestamp, lastCheck)
		}
		lastCheck = hb.Timestamp

	}
}
