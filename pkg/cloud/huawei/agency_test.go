package huawei

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/huaweicloud/huaweicloud-sdk-go-v3/core/auth"
	"github.com/huaweicloud/huaweicloud-sdk-go-v3/core/auth/global"

	"github.com/opencost/opencost/core/pkg/clustercache"
	"github.com/opencost/opencost/pkg/cloud/models"
	"github.com/opencost/opencost/pkg/env"
)

// TestHuawei_DownloadPricingData_IAMAgency covers a node with an IAM agency
// and no static AK/SK: the agency's temporary credentials come from the
// metadata service, the project ID from meta_data.json, and BSS must receive
// the temporary security token with them. The credentials and the project ID
// are cached, and the credentials are fetched again once their cache expires.
func TestHuawei_DownloadPricingData_IAMAgency(t *testing.T) {
	withoutHuaweiCredentials(t)
	// The deployment sets the domain ID, so the SDK has no IAM lookup to make.
	t.Setenv(env.HuaweiDomainIDEnvVar, "agency-domain")

	var agencyLookups atomic.Int32
	agencyCredentials = func() (auth.ICredential, error) {
		agencyLookups.Add(1)
		return global.NewCredentialsBuilder().
			WithAk("agency-ak").
			WithSk("agency-sk").
			WithSecurityToken("agency-token").
			Build(), nil
	}

	var metadataLookups atomic.Int32
	metadata := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != metadataPath {
			http.NotFound(w, r)
			return
		}
		metadataLookups.Add(1)
		w.Write([]byte(`{"project_id": "agency-project"}`))
	}))
	t.Cleanup(metadata.Close)
	metadataEndpoint = metadata.URL

	var mu sync.Mutex
	var tokens, projects []string
	bss := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			ProjectID string `json:"project_id"`
		}
		json.NewDecoder(r.Body).Decode(&body)
		mu.Lock()
		tokens = append(tokens, r.Header.Get("X-Security-Token"))
		projects = append(projects, body.ProjectID)
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"product_rating_results": [{"id": "node-0", "amount": "1.5"}]}`))
	}))
	t.Cleanup(bss.Close)
	bssEndpointOverride = bss.URL

	h := &Huawei{
		Clientset:     &clustercache.MockClusterCache{Nodes: []*clustercache.Node{flavorNode("c7n.2xlarge.4")}},
		Config:        &fakeProviderConfig{customPricing: &models.CustomPricing{CPU: "0.0345", RAM: "0.0046"}},
		ClusterRegion: "la-south-2",
	}

	if err := h.DownloadPricingData(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := h.Pricing["la-south-2,c7n.2xlarge.4,linux"]; got == nil || got.NodeAttributes.Price != "1.5" {
		t.Fatalf("expected a live price through the IAM agency, got %+v", h.Pricing)
	}
	if h.ProjectID != "agency-project" {
		t.Fatalf("expected the project ID from instance metadata, got %q", h.ProjectID)
	}

	// Cached: a second download reuses the credentials and the project ID.
	if err := h.DownloadPricingData(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n := agencyLookups.Load(); n != 1 {
		t.Fatalf("expected the agency credentials to be reused, looked up %d times", n)
	}
	if n := metadataLookups.Load(); n != 1 {
		t.Fatalf("expected the project ID to be reused, looked up %d times", n)
	}

	// Expired: the next download fetches the credentials again.
	agencyCredsCacheMu.Lock()
	agencyCredsCacheAt = time.Now().Add(-agencyCredsCacheTTL - time.Second)
	agencyCredsCacheMu.Unlock()
	if err := h.DownloadPricingData(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n := agencyLookups.Load(); n != 2 {
		t.Fatalf("expected the expired credentials to be fetched again, looked up %d times", n)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(tokens) != 3 {
		t.Fatalf("expected three demandPrice requests, got %d", len(tokens))
	}
	for i := range tokens {
		if tokens[i] != "agency-token" {
			t.Errorf("request %d: X-Security-Token %q, want the agency's temporary token", i, tokens[i])
		}
		if projects[i] != "agency-project" {
			t.Errorf("request %d: project_id %q, want agency-project", i, projects[i])
		}
	}
}
