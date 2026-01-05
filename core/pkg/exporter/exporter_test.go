package exporter

import (
	"testing"
	"time"

	"github.com/opencost/opencost/core/pkg/exporter/pathing"
	"github.com/opencost/opencost/core/pkg/exporter/validator"
	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/pipelines"
	"github.com/opencost/opencost/core/pkg/storage"
	"github.com/opencost/opencost/core/pkg/util/json"
)

const (
	TestClusterId = "test-cluster"
	TestEventName = "test-event-path"
)

type TestData struct {
	Message string `json:"message"`
}

func TestStorageExporters(t *testing.T) {
	t.Run("test event storage exporter", func(t *testing.T) {
		store := storage.NewMemoryStorage()
		p, err := pathing.NewEventStoragePathFormatter("root", TestClusterId, TestEventName)
		if err != nil {
			t.Fatalf("failed to create path formatter: %v", err)
		}

		encoder := NewJSONEncoder[TestData]()
		export := NewEventStorageExporter(p, encoder, store)

		ts := time.Now().UTC().Truncate(time.Minute)

		export.Export(ts, &TestData{
			Message: "TestMessage-1",
		})

		expectedPath := p.ToFullPath("", ts, "json")
		t.Logf("expected path: %s", expectedPath)

		data, err := store.Read(expectedPath)
		if err != nil {
			t.Fatalf("failed to read data from store: %v", err)
		}

		if len(data) == 0 {
			t.Fatalf("expected data to be non-empty, got empty")
		}

		t.Logf("Data: %s", string(data))

		var td *TestData = new(TestData)
		if err := json.Unmarshal(data, td); err != nil {
			t.Fatalf("failed to unmarshal data: %v", err)
		}

		if td.Message != "TestMessage-1" {
			t.Fatalf("expected message to be 'TestMessage-1', got '%s'", td.Message)
		}
	})

	t.Run("test compute storage exporter", func(t *testing.T) {
		res := 24 * time.Hour
		store := storage.NewMemoryStorage()
		p, err := pathing.NewDefaultStoragePathFormatter(TestClusterId, pipelines.AllocationPipelineName, &res)
		if err != nil {
			t.Fatalf("failed to create path formatter: %v", err)
		}

		encoder := NewBingenEncoder[opencost.AllocationSet]()
		export := NewComputeStorageExporter[opencost.AllocationSet](
			p,
			encoder,
			store,
			validator.NewSetValidator[opencost.AllocationSet](24*time.Hour),
		)

		start := time.Now().UTC().Truncate(res)
		end := start.Add(res)

		toExport := opencost.GenerateMockAllocationSet(start)
		err = export.Export(opencost.NewClosedWindow(start, end), toExport)
		if err != nil {
			t.Fatalf("failed to export data: %v", err)
		}

		expectedPath := p.ToFullPath("", opencost.NewClosedWindow(start, end), "")

		data, err := store.Read(expectedPath)
		if err != nil {
			t.Fatalf("failed to read data from store: %v", err)
		}

		if len(data) == 0 {
			t.Fatalf("expected data to be non-empty, got empty")
		}

		var as *opencost.AllocationSet = new(opencost.AllocationSet)
		err = as.UnmarshalBinary(data)
		if err != nil {
			t.Fatalf("failed to unmarshal data: %v", err)
		}

		if as.IsEmpty() {
			t.Fatalf("expected allocation set to be non-empty, got empty")
		}
	})
}
