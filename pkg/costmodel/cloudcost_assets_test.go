package costmodel

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/opencost/opencost/core/pkg/autocomplete"
	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/pkg/cloudcost"
)

// fakeCloudCostQuerier is a minimal cloudcost.Querier stand-in that lets tests
// control exactly what CloudCostSetRange (or error) ClusterCloudCosts sees, without
// needing a full Repository/Ingestor stack.
type fakeCloudCostQuerier struct {
	result *opencost.CloudCostSetRange
	err    error
}

func (f *fakeCloudCostQuerier) Query(_ context.Context, _ cloudcost.QueryRequest) (*opencost.CloudCostSetRange, error) {
	return f.result, f.err
}

func (f *fakeCloudCostQuerier) QueryCloudCostAutocomplete(_ context.Context, _ autocomplete.Request) (*autocomplete.Response, error) {
	return nil, nil
}

func TestClusterCloudCosts_NilQuerier(t *testing.T) {
	cm := &CostModel{}
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	end := start.AddDate(0, 0, 1)

	assets, err := cm.ClusterCloudCosts(start, end)
	if err != nil {
		t.Fatalf("expected no error, got: %s", err)
	}
	if assets != nil {
		t.Fatalf("expected nil assets when CloudCostQuerier is unset, got: %v", assets)
	}
}

func TestClusterCloudCosts_QueryError(t *testing.T) {
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	end := start.AddDate(0, 0, 1)

	cm := &CostModel{
		CloudCostQuerier: &fakeCloudCostQuerier{err: fmt.Errorf("boom")},
	}

	_, err := cm.ClusterCloudCosts(start, end)
	if err == nil {
		t.Fatal("expected an error from a failing CloudCostQuerier, got nil")
	}
}

// TestClusterCloudCosts_ConvertsCategorizedCloudCosts verifies that categorized
// CloudCost data (as produced by, e.g., huawei.CostIntegration.GetCloudCost, which
// classifies RDS/DEW as Storage and DCS/ECS/CCE as Compute) is converted into Cloud
// assets carrying the same category, provider, service and providerID, and that the
// resulting assets insert cleanly into an AssetSet as Cloud-typed entries -- which is
// what the Infra Assets panel reads.
func TestClusterCloudCosts_ConvertsCategorizedCloudCosts(t *testing.T) {
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	end := start.AddDate(0, 0, 1)

	rdsProps := &opencost.CloudCostProperties{
		ProviderID: "rds-instance-1",
		Provider:   opencost.HuaweiProvider,
		AccountID:  "project-1",
		Service:    "Relational Database Service",
		Category:   opencost.StorageCategory,
		Labels:     opencost.CloudCostLabels{},
	}
	dcsProps := &opencost.CloudCostProperties{
		ProviderID: "dcs-instance-1",
		Provider:   opencost.HuaweiProvider,
		AccountID:  "project-1",
		Service:    "Distributed Cache Service",
		Category:   opencost.ComputeCategory,
		Labels:     opencost.CloudCostLabels{},
	}

	ccs := opencost.NewCloudCostSet(start, end,
		&opencost.CloudCost{
			Properties: rdsProps,
			Window:     opencost.NewClosedWindow(start, end),
			NetCost:    opencost.CostMetric{Cost: 12.5},
		},
		&opencost.CloudCost{
			Properties: dcsProps,
			Window:     opencost.NewClosedWindow(start, end),
			NetCost:    opencost.CostMetric{Cost: 3.25},
		},
	)

	cm := &CostModel{
		CloudCostQuerier: &fakeCloudCostQuerier{
			result: &opencost.CloudCostSetRange{
				CloudCostSets: []*opencost.CloudCostSet{ccs},
				Window:        opencost.NewClosedWindow(start, end),
			},
		},
	}

	assets, err := cm.ClusterCloudCosts(start, end)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if len(assets) != 2 {
		t.Fatalf("expected 2 Cloud assets, got %d", len(assets))
	}

	byProviderID := map[string]*opencost.Cloud{}
	for _, a := range assets {
		byProviderID[a.Properties.ProviderID] = a
	}

	rds, ok := byProviderID["rds-instance-1"]
	if !ok {
		t.Fatal("expected a Cloud asset for the RDS resource")
	}
	if rds.Properties.Category != opencost.StorageCategory {
		t.Errorf("expected RDS category %q, got %q", opencost.StorageCategory, rds.Properties.Category)
	}
	if rds.Properties.Service != "Relational Database Service" {
		t.Errorf("expected RDS service to carry through, got %q", rds.Properties.Service)
	}
	if rds.Properties.Provider != opencost.HuaweiProvider {
		t.Errorf("expected RDS provider %q, got %q", opencost.HuaweiProvider, rds.Properties.Provider)
	}
	if rds.Cost != 12.5 {
		t.Errorf("expected RDS cost 12.5, got %f", rds.Cost)
	}

	dcs, ok := byProviderID["dcs-instance-1"]
	if !ok {
		t.Fatal("expected a Cloud asset for the DCS resource")
	}
	if dcs.Properties.Category != opencost.ComputeCategory {
		t.Errorf("expected DCS category %q, got %q", opencost.ComputeCategory, dcs.Properties.Category)
	}
	if dcs.Cost != 3.25 {
		t.Errorf("expected DCS cost 3.25, got %f", dcs.Cost)
	}

	// Insert into an AssetSet the same way ComputeAssets does, and confirm the
	// items land in the Cloud-typed bucket the Infra Assets panel consumes.
	assetSet := opencost.NewAssetSet(start, end)
	for _, a := range assets {
		if err := assetSet.Insert(a, nil); err != nil {
			t.Fatalf("unexpected error inserting Cloud asset: %s", err)
		}
	}
	if len(assetSet.Cloud) != 2 {
		t.Fatalf("expected 2 assets in AssetSet.Cloud, got %d", len(assetSet.Cloud))
	}
}

// TestClusterCloudCosts_DescribesResources verifies that the details a billing
// API reports about a resource -- which service it belongs to, where it runs,
// what it is -- reach the Cloud asset, since an asset without them renders as an
// anonymous row indistinguishable from every other row of the same service.
func TestClusterCloudCosts_DescribesResources(t *testing.T) {
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	end := start.AddDate(0, 0, 1)

	props := &opencost.CloudCostProperties{
		ProviderID: "rds-instance-1",
		Provider:   opencost.HuaweiProvider,
		AccountID:  "project-1",
		RegionID:   "la-south-2",
		Service:    "Relational Database Service",
		Category:   opencost.StorageCategory,
		Labels: opencost.CloudCostLabels{
			opencost.AssetResourceTypeLabel: "RDS DB Instance VM",
			opencost.AssetResourceSpecLabel: "rds.mysql.n1.large.2.ha",
			"owner":                         "mlops",
		},
	}

	ccs := opencost.NewCloudCostSet(start, end,
		&opencost.CloudCost{
			Properties: props,
			Window:     opencost.NewClosedWindow(start, end),
			NetCost:    opencost.CostMetric{Cost: 12.5},
		},
	)

	cm := &CostModel{
		CloudCostQuerier: &fakeCloudCostQuerier{
			result: &opencost.CloudCostSetRange{
				CloudCostSets: []*opencost.CloudCostSet{ccs},
				Window:        opencost.NewClosedWindow(start, end),
			},
		},
	}

	assets, err := cm.ClusterCloudCosts(start, end)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if len(assets) != 1 {
		t.Fatalf("expected 1 Cloud asset, got %d", len(assets))
	}

	asset := assets[0]
	if asset.Type() != opencost.RDSCloudAssetType {
		t.Errorf("expected asset type %s, got %s", opencost.RDSCloudAssetType, asset.Type())
	}
	if asset.Properties.Name != "rds-instance-1" {
		t.Errorf("expected the resource ID as the asset name, got %q", asset.Properties.Name)
	}
	if got := asset.Labels[opencost.AssetRegionLabel]; got != "la-south-2" {
		t.Errorf("expected region label %q, got %q", "la-south-2", got)
	}
	if got := asset.Labels[opencost.AssetResourceTypeLabel]; got != "RDS DB Instance VM" {
		t.Errorf("expected resource type label to carry over, got %q", got)
	}
	if got := asset.Labels[opencost.AssetResourceSpecLabel]; got != "rds.mysql.n1.large.2.ha" {
		t.Errorf("expected resource spec label to carry over, got %q", got)
	}
	if got := asset.Labels["owner"]; got != "mlops" {
		t.Errorf("expected billing labels to carry over, got %q", got)
	}
}

// TestClusterCloudCosts_OtherProvidersStayGeneric verifies that only providers
// with a known service catalogue get sub-typed: another provider's service name
// must not be forced into a Huawei Cloud service's asset type.
func TestClusterCloudCosts_OtherProvidersStayGeneric(t *testing.T) {
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	end := start.AddDate(0, 0, 1)

	ccs := opencost.NewCloudCostSet(start, end,
		&opencost.CloudCost{
			Properties: &opencost.CloudCostProperties{
				ProviderID: "arn:aws:rds:us-east-1:1234:db:db-1",
				Provider:   opencost.AWSProvider,
				Service:    "Amazon Relational Database Service",
				Category:   opencost.StorageCategory,
			},
			Window:  opencost.NewClosedWindow(start, end),
			NetCost: opencost.CostMetric{Cost: 3},
		},
	)

	cm := &CostModel{
		CloudCostQuerier: &fakeCloudCostQuerier{
			result: &opencost.CloudCostSetRange{
				CloudCostSets: []*opencost.CloudCostSet{ccs},
				Window:        opencost.NewClosedWindow(start, end),
			},
		},
	}

	assets, err := cm.ClusterCloudCosts(start, end)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if len(assets) != 1 {
		t.Fatalf("expected 1 Cloud asset, got %d", len(assets))
	}
	if assets[0].Type() != opencost.CloudAssetType {
		t.Errorf("expected a generic Cloud asset, got %s", assets[0].Type())
	}
}

func TestClusterCloudCosts_ClampsWindowToRequestedRange(t *testing.T) {
	start := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)
	end := start.AddDate(0, 0, 1)

	// The CloudCostSet's own window is wider than [start, end) -- as can happen
	// since CloudCost is ingested daily while Assets may be requested for any
	// sub-range -- and should be clamped, mirroring the Disk/Node pattern.
	wideStart := start.AddDate(0, 0, -1)
	wideEnd := end.AddDate(0, 0, 1)

	props := &opencost.CloudCostProperties{
		ProviderID: "obs-bucket-1",
		Provider:   opencost.HuaweiProvider,
		Service:    "Object Storage Service",
		Category:   opencost.StorageCategory,
	}

	ccs := opencost.NewCloudCostSet(wideStart, wideEnd,
		&opencost.CloudCost{
			Properties: props,
			Window:     opencost.NewClosedWindow(wideStart, wideEnd),
			NetCost:    opencost.CostMetric{Cost: 7},
		},
	)

	cm := &CostModel{
		CloudCostQuerier: &fakeCloudCostQuerier{
			result: &opencost.CloudCostSetRange{
				CloudCostSets: []*opencost.CloudCostSet{ccs},
				Window:        opencost.NewClosedWindow(wideStart, wideEnd),
			},
		},
	}

	assets, err := cm.ClusterCloudCosts(start, end)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if len(assets) != 1 {
		t.Fatalf("expected 1 Cloud asset, got %d", len(assets))
	}

	asset := assets[0]
	if !asset.Start.Equal(start) {
		t.Errorf("expected Start clamped to %s, got %s", start, asset.Start)
	}
	if !asset.End.Equal(end) {
		t.Errorf("expected End clamped to %s, got %s", end, asset.End)
	}
}
