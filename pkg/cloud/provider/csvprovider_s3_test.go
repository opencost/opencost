package provider_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/opencost/opencost/core/pkg/clustercache"
	"github.com/opencost/opencost/pkg/cloud/provider"
)

func TestCSVProviderLoadsS3KeyWithDirectories(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/b/prices/nodes.csv" {
			http.Error(w, "unexpected object path", http.StatusNotFound)
			return
		}
		fmt.Fprint(w, "EndTimestamp,InstanceID,Region,AssetClass,InstanceIDField,InstanceType,MarketPriceHourly,Version\n,worker-a,,node,metadata.name,,0.24,\n")
	}))
	t.Cleanup(server.Close)
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	t.Setenv("CSV_REGION", "us-east-1")
	t.Setenv("CSV_ENDPOINT", server.URL)

	// A one-character bucket makes the SDK use path-style requests to the local server.
	c := &provider.CSVProvider{CSVLocation: "s3://b/prices/nodes.csv"}
	if err := c.DownloadPricingData(); err != nil {
		t.Fatalf("load CSV from nested S3 key: %v", err)
	}

	key := c.GetKey(nil, &clustercache.Node{Name: "worker-a"})
	node, _, err := c.NodePricing(key)
	if err != nil {
		t.Fatal(err)
	}
	if node.Cost != "0.24" {
		t.Fatalf("node cost = %q, want 0.24", node.Cost)
	}
}
