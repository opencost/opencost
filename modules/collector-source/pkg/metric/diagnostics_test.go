package metric

import (
	"fmt"
	"testing"
	"time"

	"github.com/kubecost/events"
	"github.com/opencost/opencost/modules/collector-source/pkg/event"
)

func TestDiagnosticsModule_ScrapeDiagnostics(t *testing.T) {
	module := NewDiagnosticsModule()

	// dispatch some faux scrape events
	events.Dispatch(event.ScrapeEvent{
		ScraperName: event.NetworkCostsScraperName,
		Targets:     10,
		Errors:      []error{},
	})

	events.Dispatch(event.ScrapeEvent{
		ScraperName: event.KubernetesClusterScraperName,
		ScrapeType:  event.NodeScraperType,
		Targets:     8,
		Errors: []error{
			fmt.Errorf("failed to scrape node 'foo'"),
			fmt.Errorf("failed to scrape node 'bar'"),
		},
	})

	time.Sleep(500 * time.Millisecond)

	networkDiagnosticDetails, err := module.DiagnosticsDetails(NetworkCostsScraperDiagnosticID)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
		return
	}

	stats := networkDiagnosticDetails["stats"].(map[string]any)
	errors := networkDiagnosticDetails["errors"].([]string)
	label := networkDiagnosticDetails["label"].(string)

	statsTotal := stats["total"].(int)
	statsSuccess := stats["success"].(int)
	statsFail := stats["fail"].(int)

	if statsTotal != 10 {
		t.Fatalf("expected networkCostsDetails[\"stats\"][\"total\"] to equal 10, got: %d", statsTotal)
		return
	}
	if statsSuccess != 10 {
		t.Fatalf("expected networkCostsDetails[\"stats\"][\"success\"] to equal 10, got: %d", statsSuccess)
		return
	}
	if statsFail != 0 {
		t.Fatalf("expected networkCostsDetails[\"stats\"][\"fail\"] to equal 0, got: %d", statsFail)
		return
	}

	if len(errors) != 0 {
		t.Fatalf("expected len(networkCostsDetails[\"errors\"]) to equal 0, got: %d", len(errors))
		return
	}

	if len(label) == 0 {
		t.Fatalf("expected len(networkCostsDetails[\"label\"]) to be non-zero. Got 0.")
		return
	}

	nodeScrapeDetails, err := module.DiagnosticsDetails(KubernetesNodesScraperDiagnosticID)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
		return
	}

	stats = nodeScrapeDetails["stats"].(map[string]any)
	errors = nodeScrapeDetails["errors"].([]string)
	label = nodeScrapeDetails["label"].(string)

	statsTotal = stats["total"].(int)
	statsSuccess = stats["success"].(int)
	statsFail = stats["fail"].(int)

	if statsTotal != 8 {
		t.Fatalf("expected nodeScrapeDetails[\"stats\"][\"total\"] to equal 8, got: %d", statsTotal)
		return
	}
	if statsSuccess != 6 {
		t.Fatalf("expected nodeScrapeDetails[\"stats\"][\"success\"] to equal 6, got: %d", statsSuccess)
		return
	}
	if statsFail != 2 {
		t.Fatalf("expected nodeScrapeDetails[\"stats\"][\"fail\"] to equal 2, got: %d", statsFail)
		return
	}

	if len(errors) != 2 {
		t.Fatalf("expected len(nodeScrapeDetails[\"errors\"]) to equal 2, got: %d", len(errors))
		return
	}

	if len(label) == 0 {
		t.Fatalf("expected len(nodeScrapeDetails[\"label\"]) to be non-zero. Got 0.")
		return
	}
}

func TestDiagnosticsModule_ScrapeDiagnosticsWithSameScraperName(t *testing.T) {
	module := NewDiagnosticsModule()

	// dispatch some faux scrape events with same scraper name
	events.Dispatch(event.ScrapeEvent{
		ScraperName: event.KubernetesClusterScraperName,
		ScrapeType:  event.NodeScraperType,
		Targets:     8,
		Errors: []error{
			fmt.Errorf("failed to scrape node 'foo'"),
			fmt.Errorf("failed to scrape node 'bar'"),
		},
	})

	events.Dispatch(event.ScrapeEvent{
		ScraperName: event.KubernetesClusterScraperName,
		ScrapeType:  event.PodScraperType,
		Targets:     8,
		Errors: []error{
			fmt.Errorf("failed to scrape node 'foo'"),
			fmt.Errorf("failed to scrape node 'bar'"),
		},
	})

	time.Sleep(500 * time.Millisecond)

	// for both the diagnostics, if they remain unregistered even after an event was dispatched getting the details would raise an error
	_, err := module.DiagnosticsDetails(KubernetesNodesScraperDiagnosticID)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
		return
	}
	_, err = module.DiagnosticsDetails(KubernetesPodsScraperDiagnosticID)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
		return
	}
}
