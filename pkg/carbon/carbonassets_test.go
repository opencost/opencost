package carbon

import (
	"math"
	"testing"
	"time"

	"github.com/opencost/opencost/core/pkg/opencost"
	v1 "k8s.io/api/core/v1"
)

const (
	// Known-good row from carbonlookupdata.csv:
	//   AWS,us-east-1,t4g.nano,Node,0.012788433076234564,4.84769853777516e-06
	awsT4gNanoUSEast1Coeff = 4.84769853777516e-06

	// AWS,average-region,,Node,0.186739186034359,7.278989705005508e-05
	awsAvgRegionNodeCoeff = 7.278989705005508e-05

	// AWS,us-east-1,,Network,0.001135,4.30243315e-7
	awsUSEast1NetworkCoeff = 4.30243315e-7
)

// floatEqual compares floats at a tolerance appropriate for the lookup table
// values, which are stored with full float64 precision in the CSV.
func floatEqual(a, b float64) bool {
	if a == b {
		return true
	}
	return math.Abs(a-b) <= 1e-18+1e-12*math.Max(math.Abs(a), math.Abs(b))
}

func nodeWithLabels(provider, providerID, region, instanceType string, minutes float64) *opencost.Node {
	start := time.Date(2026, time.April, 1, 0, 0, 0, 0, time.UTC)
	end := start.Add(time.Duration(minutes) * time.Minute)
	window := opencost.NewWindow(&start, &end)

	n := opencost.NewNode("node", "cluster", providerID, start, end, window)
	n.Properties.Provider = provider
	labels := opencost.AssetLabels{}
	if region != "" {
		labels[v1.LabelTopologyRegion] = region
	}
	if instanceType != "" {
		labels[v1.LabelInstanceTypeStable] = instanceType
	}
	n.Labels = labels
	return n
}

func diskWithLabels(provider, providerID, region string, minutes float64) *opencost.Disk {
	start := time.Date(2026, time.April, 1, 0, 0, 0, 0, time.UTC)
	end := start.Add(time.Duration(minutes) * time.Minute)
	window := opencost.NewWindow(&start, &end)

	d := opencost.NewDisk("disk", "cluster", providerID, start, end, window)
	d.Properties.Provider = provider
	if region != "" {
		d.Labels = opencost.AssetLabels{v1.LabelTopologyRegion: region}
	}
	return d
}

func networkWithLabels(provider, providerID, region string, minutes float64) *opencost.Network {
	start := time.Date(2026, time.April, 1, 0, 0, 0, 0, time.UTC)
	end := start.Add(time.Duration(minutes) * time.Minute)
	window := opencost.NewWindow(&start, &end)

	nw := opencost.NewNetwork("network", "cluster", providerID, start, end, window)
	nw.Properties.Provider = provider
	if region != "" {
		nw.Labels = opencost.AssetLabels{v1.LabelTopologyRegion: region}
	}
	return nw
}

func TestInferProviderFromProviderID(t *testing.T) {
	cases := []struct {
		name string
		id   string
		want string
	}{
		{"empty", "", ""},
		{"aws standard", "aws:///us-east-1a/i-0abc123", opencost.AWSProvider},
		{"aws raw instance", "i-0abc123", opencost.AWSProvider},
		{"gce standard", "gce://my-project/us-central1-a/gke-node-1", opencost.GCPProvider},
		{"legacy gke prefix", "gke-node-1", opencost.GCPProvider},
		{"azure standard", "azure:///subscriptions/x/resourceGroups/y/providers/Microsoft.Compute/virtualMachines/z", opencost.AzureProvider},
		{"unknown prefix", "something-else", ""},
		{"whitespace and case", "  AWS:///eu-west-1a/i-xyz  ", opencost.AWSProvider},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := inferProviderFromProviderID(tc.id); got != tc.want {
				t.Fatalf("inferProviderFromProviderID(%q) = %q, want %q", tc.id, got, tc.want)
			}
		})
	}
}

func TestResolveProvider_PrefersCanonicalProperty(t *testing.T) {
	// ProviderID is a GCP-shaped string but the canonical property says AWS.
	// Canonical property wins.
	n := nodeWithLabels(opencost.AWSProvider, "gce://foo/bar/baz", "us-east-1", "t4g.nano", 60)
	if got := resolveProvider(n); got != opencost.AWSProvider {
		t.Fatalf("resolveProvider = %q, want %q", got, opencost.AWSProvider)
	}
}

func TestResolveProvider_FallsBackToProviderID(t *testing.T) {
	// No canonical Provider property — must fall back to parsing ProviderID.
	n := nodeWithLabels("", "gce://my-project/us-central1-a/gke-node-1", "us-central1", "e2-standard-2", 60)
	if got := resolveProvider(n); got != opencost.GCPProvider {
		t.Fatalf("resolveProvider = %q, want %q", got, opencost.GCPProvider)
	}
}

func TestLookupCarbonCoeff_Node_ExactMatch(t *testing.T) {
	n := nodeWithLabels(opencost.AWSProvider, "aws:///us-east-1a/i-1", "us-east-1", "t4g.nano", 60)
	if got := lookupCarbonCoeff(n); !floatEqual(got, awsT4gNanoUSEast1Coeff) {
		t.Fatalf("lookupCarbonCoeff = %g, want %g", got, awsT4gNanoUSEast1Coeff)
	}
}

func TestLookupCarbonCoeff_Node_FallsBackWhenRegionUnknown(t *testing.T) {
	// Region is garbage; instance type is fine. Should fall back to
	// (AWS, average-region, "") instead of returning zero.
	n := nodeWithLabels(opencost.AWSProvider, "aws:///xx/i-1", "not-a-real-region", "t4g.nano", 60)
	if got := lookupCarbonCoeff(n); !floatEqual(got, awsAvgRegionNodeCoeff) {
		t.Fatalf("lookupCarbonCoeff = %g, want %g (average-region fallback)", got, awsAvgRegionNodeCoeff)
	}
}

func TestLookupCarbonCoeff_Node_FallsBackWhenInstanceTypeUnknown(t *testing.T) {
	// Region is real; instance type is unknown. Previously returned 0 because
	// only the region was reset. Must now fall back to average-region.
	n := nodeWithLabels(opencost.AWSProvider, "aws:///us-east-1a/i-1", "us-east-1", "future-xxlarge", 60)
	if got := lookupCarbonCoeff(n); !floatEqual(got, awsAvgRegionNodeCoeff) {
		t.Fatalf("lookupCarbonCoeff = %g, want %g (average-region fallback)", got, awsAvgRegionNodeCoeff)
	}
}

func TestLookupCarbonCoeff_Node_FallsBackWhenBothUnknown(t *testing.T) {
	n := nodeWithLabels(opencost.AWSProvider, "aws:///xx/i-1", "not-a-real-region", "future-xxlarge", 60)
	if got := lookupCarbonCoeff(n); !floatEqual(got, awsAvgRegionNodeCoeff) {
		t.Fatalf("lookupCarbonCoeff = %g, want %g (average-region fallback)", got, awsAvgRegionNodeCoeff)
	}
}

func TestLookupCarbonCoeff_Node_ZeroForUnknownProvider(t *testing.T) {
	n := nodeWithLabels("", "some-unknown-id", "us-east-1", "t4g.nano", 60)
	if got := lookupCarbonCoeff(n); got != 0 {
		t.Fatalf("lookupCarbonCoeff = %g, want 0", got)
	}
}

func TestLookupCarbonCoeff_Disk_ExactMatch(t *testing.T) {
	// The CSV contains several disk rows per (provider, region), one per
	// disk type. They collide under a key of (provider, region), so we
	// check the lookup against whatever value the table actually holds.
	want, ok := carbonLookupDisk[carbonLookupKeyRegion{opencost.AWSProvider, "us-east-1"}]
	if !ok || want == 0 {
		t.Fatalf("expected AWS/us-east-1 disk coefficient to be loaded")
	}
	d := diskWithLabels(opencost.AWSProvider, "aws:///us-east-1a/vol-1", "us-east-1", 60)
	if got := lookupCarbonCoeff(d); !floatEqual(got, want) {
		t.Fatalf("lookupCarbonCoeff disk = %g, want %g", got, want)
	}
}

func TestLookupCarbonCoeff_Disk_FallsBackWhenRegionUnknown(t *testing.T) {
	d := diskWithLabels(opencost.AWSProvider, "aws:///xx/vol-1", "not-a-real-region", 60)
	want, ok := carbonLookupDisk[carbonLookupKeyRegion{opencost.AWSProvider, averageRegionKey}]
	if !ok {
		t.Fatalf("expected AWS average-region disk coefficient to be loaded")
	}
	if got := lookupCarbonCoeff(d); !floatEqual(got, want) {
		t.Fatalf("lookupCarbonCoeff disk fallback = %g, want %g", got, want)
	}
}

func TestLookupCarbonCoeff_Network_Populated(t *testing.T) {
	// Regression: Network rows were loaded but never consulted, so every
	// Network asset produced 0 emissions.
	nw := networkWithLabels(opencost.AWSProvider, "aws:///us-east-1a/net-1", "us-east-1", 60)
	if got := lookupCarbonCoeff(nw); !floatEqual(got, awsUSEast1NetworkCoeff) {
		t.Fatalf("lookupCarbonCoeff network = %g, want %g", got, awsUSEast1NetworkCoeff)
	}
}

func TestRelateCarbonAssets_MinutesToHours(t *testing.T) {
	// Coefficient is tonnes CO2e per hour, so 120 minutes should yield exactly
	// twice the coefficient.
	n := nodeWithLabels(opencost.AWSProvider, "aws:///us-east-1a/i-1", "us-east-1", "t4g.nano", 120)
	as := opencost.NewAssetSet(*n.Window.Start(), *n.Window.End(), n)

	rows, err := RelateCarbonAssets(as)
	if err != nil {
		t.Fatalf("RelateCarbonAssets: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("got %d rows, want 1", len(rows))
	}
	var row CarbonRow
	for _, r := range rows {
		row = r
	}
	want := awsT4gNanoUSEast1Coeff * 2
	if !floatEqual(row.Co2e, want) {
		t.Fatalf("Co2e = %g, want %g", row.Co2e, want)
	}
}

func TestRelateCarbonAssets_ZeroForUnknownProvider(t *testing.T) {
	n := nodeWithLabels("", "totally-unknown", "us-east-1", "t4g.nano", 60)
	as := opencost.NewAssetSet(*n.Window.Start(), *n.Window.End(), n)

	rows, err := RelateCarbonAssets(as)
	if err != nil {
		t.Fatalf("RelateCarbonAssets: %v", err)
	}
	for _, r := range rows {
		if r.Co2e != 0 {
			t.Fatalf("Co2e = %g, want 0 for unknown provider", r.Co2e)
		}
	}
}

func TestLookupCarbonCoeff_NoPanicOnNilProperties(t *testing.T) {
	// A bare Node with nil Properties must not panic — older code would
	// dereference props.ProviderID in the log line after resolveProvider
	// returned "" for nil properties.
	start := time.Date(2026, time.April, 1, 0, 0, 0, 0, time.UTC)
	end := start.Add(60 * time.Minute)
	window := opencost.NewWindow(&start, &end)
	n := opencost.NewNode("node", "cluster", "", start, end, window)
	n.Properties = nil

	if got := lookupCarbonCoeff(n); got != 0 {
		t.Fatalf("lookupCarbonCoeff with nil properties = %g, want 0", got)
	}
}

func TestLookupTables_LoadedAtInit(t *testing.T) {
	if len(carbonLookupNode) == 0 {
		t.Error("carbonLookupNode is empty — init did not populate node lookups")
	}
	if len(carbonLookupDisk) == 0 {
		t.Error("carbonLookupDisk is empty — init did not populate disk lookups")
	}
	if len(carbonLookupNetwork) == 0 {
		t.Error("carbonLookupNetwork is empty — init did not populate network lookups")
	}
}
