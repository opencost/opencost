package aws

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"os"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/pricing"
	"github.com/aws/aws-sdk-go-v2/service/pricing/types"
)

// fakePriceList serves an offer file fixture through GetProducts, one product per page.
type fakePriceList struct {
	serviceCode string
	entries     []string
	attributes  []map[string]string
}

func newFakePriceList(t *testing.T, serviceCode, fixture string) *fakePriceList {
	t.Helper()
	raw, err := os.ReadFile(fixture)
	if err != nil {
		t.Fatal(err)
	}
	var offer struct {
		Products map[string]json.RawMessage `json:"products"`
		Terms    struct {
			OnDemand map[string]json.RawMessage `json:"OnDemand"`
		} `json:"terms"`
	}
	if err := json.Unmarshal(raw, &offer); err != nil {
		t.Fatal(err)
	}

	f := &fakePriceList{serviceCode: serviceCode}
	for sku, product := range offer.Products {
		var attrs struct {
			Attributes map[string]string `json:"attributes"`
		}
		if err := json.Unmarshal(product, &attrs); err != nil {
			t.Fatal(err)
		}
		entry, err := json.Marshal(map[string]any{
			"product": product,
			"terms":   map[string]any{"OnDemand": offer.Terms.OnDemand[sku]},
		})
		if err != nil {
			t.Fatal(err)
		}
		f.entries = append(f.entries, string(entry))
		f.attributes = append(f.attributes, attrs.Attributes)
	}
	return f
}

func (f *fakePriceList) GetProducts(_ context.Context, in *pricing.GetProductsInput, _ ...func(*pricing.Options)) (*pricing.GetProductsOutput, error) {
	var matches []string
	if *in.ServiceCode == f.serviceCode {
		for i, attrs := range f.attributes {
			if matchesFilters(attrs, in.Filters) {
				matches = append(matches, f.entries[i])
			}
		}
	}
	start := 0
	if in.NextToken != nil {
		start, _ = strconv.Atoi(*in.NextToken)
	}
	out := &pricing.GetProductsOutput{}
	if start < len(matches) {
		out.PriceList = matches[start : start+1]
		if start+1 < len(matches) {
			next := strconv.Itoa(start + 1)
			out.NextToken = &next
		}
	}
	return out, nil
}

func matchesFilters(attrs map[string]string, filters []types.Filter) bool {
	for _, f := range filters {
		value, ok := attrs[*f.Field]
		var match bool
		switch f.Type {
		case types.FilterTypeTermMatch, types.FilterTypeEquals:
			match = ok && strings.EqualFold(value, *f.Value)
		case types.FilterTypeContains:
			match = ok && strings.Contains(value, *f.Value)
		case types.FilterTypeAnyOf:
			match = ok && slices.Contains(strings.Split(*f.Value, ","), value)
		case types.FilterTypeNoneOf:
			match = !ok || !slices.Contains(strings.Split(*f.Value, ","), value)
		}
		if !match {
			return false
		}
	}
	return true
}

func TestPopulatePricingFromAPIMatchesOfferFile(t *testing.T) {
	const fixture = "testdata/pricing-us-east-2.json"
	inputkeys := map[string]bool{
		"us-east-2,m5.large,linux":                 true,
		"us-east-2,p3.8xlarge,windows,preemptible": true,
	}

	file, err := os.Open(fixture)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	fromFile := &AWS{ValidPricingKeys: map[string]bool{}}
	err = fromFile.populatePricing(&http.Response{
		Body:    io.NopCloser(file),
		Request: &http.Request{URL: &url.URL{Scheme: "https", Host: "offer-file"}},
	}, inputkeys)
	if err != nil {
		t.Fatal(err)
	}

	fromAPI := &AWS{ValidPricingKeys: map[string]bool{}}
	err = fromAPI.populatePricingFromAPI(context.Background(), newFakePriceList(t, "AmazonEC2", fixture), []string{"us-east-2"}, inputkeys)
	if err != nil {
		t.Fatal(err)
	}

	// The fixture's p3.8xlarge is BYOL Windows, which the API query excludes on purpose.
	for _, key := range []string{"us-east-2,p3.8xlarge,windows", "us-east-2,p3.8xlarge,windows,preemptible"} {
		if _, ok := fromAPI.Pricing[key]; ok {
			t.Errorf("API pricing has BYOL key %q", key)
		}
		delete(fromFile.Pricing, key)
	}
	if len(fromAPI.Pricing) == 0 || !reflect.DeepEqual(fromFile.Pricing, fromAPI.Pricing) {
		got, _ := json.Marshal(fromAPI.Pricing)
		want, _ := json.Marshal(fromFile.Pricing)
		t.Errorf("API pricing differs from offer file pricing\n got: %s\nwant: %s", got, want)
	}
}

func TestFargatePricingFromAPI(t *testing.T) {
	f := NewFargatePricing()
	err := f.populatePricingFromAPI(context.Background(), newFakePriceList(t, "AmazonECS", "testdata/ecs-pricing-us-east-1.json"), []string{"us-east-1"})
	if err != nil {
		t.Fatal(err)
	}
	if err := f.regions["us-east-1"].Validate(); err != nil {
		t.Error(err)
	}
}

func TestPricingRegions(t *testing.T) {
	got := pricingRegions(slices.Values([]string{"us-east-1,m5.large,linux", ",m5.large,linux", "eu-west-1,c5.large,linux"}), slices.Values([]string{"us-east-1,EBS:VolumeUsage.gp3", "ap-south-1,EBS:VolumeUsage.gp2"}))
	want := []string{"us-east-1", "eu-west-1", "ap-south-1"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("pricingRegions() = %v, want %v", got, want)
	}
}
