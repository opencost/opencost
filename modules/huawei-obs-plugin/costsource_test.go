package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/opencost/opencost/core/pkg/model/pb"
)

func TestObsCostSource_GetCustomCosts_MissingCredentials(t *testing.T) {
	t.Setenv(envAccessKeyID, "")
	t.Setenv(envAccessKeySecret, "")
	t.Setenv(envProjectID, "")

	s := NewObsCostSource(&Config{Region: "la-south-2"})
	req := &pb.CustomCostRequest{
		Start: timestamppb.New(time.Now().Add(-time.Hour)),
		End:   timestamppb.New(time.Now()),
	}

	resps := s.GetCustomCosts(req)
	if len(resps) != 1 {
		t.Fatalf("expected 1 response, got %d", len(resps))
	}
	if len(resps[0].Errors) == 0 {
		t.Fatalf("expected an error response for missing credentials")
	}
}

func TestObsGBHourPrice(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v2/bills/ratings/on-demand-resources" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{
			"currency": "USD",
			"product_rating_results": [
				{"id": "obs-0", "amount": "2.300"}
			]
		}`))
	}))
	defer server.Close()

	bssEndpointOverride = server.URL
	defer func() { bssEndpointOverride = "" }()

	t.Setenv(envAccessKeyID, "test-ak")
	t.Setenv(envAccessKeySecret, "test-sk")
	t.Setenv(envDomainID, "test-domain")

	price, currency, err := obsGBHourPrice("test-project", "la-south-2")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if currency != "USD" {
		t.Fatalf("expected currency USD, got %s", currency)
	}
	expected := "0.023" // 2.300 / obsReferenceSizeGB(100)
	if price.String() != expected {
		t.Fatalf("expected price %s, got %s", expected, price.String())
	}
}

func TestObsGBHourPrice_MissingCredentials(t *testing.T) {
	t.Setenv(envAccessKeyID, "")
	t.Setenv(envAccessKeySecret, "")
	t.Setenv(envDomainID, "")

	if _, _, err := obsGBHourPrice("test-project", "la-south-2"); err == nil {
		t.Fatalf("expected error for missing credentials")
	}
}
