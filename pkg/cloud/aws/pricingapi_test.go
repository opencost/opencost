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
	v1 "k8s.io/api/core/v1"
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

	// The fixture's p3.8xlarge is BYOL Windows, which the API query excludes on purpose (see TestPricingFromAPINodePrices).
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

func TestPricingFromAPINodePrices(t *testing.T) {
	// Real us-east-1 m5.large SKUs, including the BYOL and "License Included - Infrastructure" Windows variants that
	// share the Windows pricing key.
	inputkeys := map[string]bool{
		"us-east-1,m5.large,linux":   true,
		"us-east-1,m5.large,windows": true,
	}
	a := &AWS{ValidPricingKeys: map[string]bool{}, pricingAPIKeys: inputkeys}
	err := a.populatePricingFromAPI(context.Background(), newFakePriceList(t, "AmazonEC2", "testdata/pricing-us-east-1-m5.json"), []string{"us-east-1"}, inputkeys)
	if err != nil {
		t.Fatal(err)
	}

	for os, want := range map[string]string{"linux": "0.0960000000", "windows": "0.1880000000"} {
		node, _, err := a.NodePricing(&awsKey{
			ProviderID: "aws:///us-east-1a/i-0123456789abcdef0",
			Labels: map[string]string{
				v1.LabelTopologyRegion:     "us-east-1",
				v1.LabelInstanceTypeStable: "m5.large",
				v1.LabelOSStable:           os,
			},
		})
		if err != nil {
			t.Fatalf("%s: %v", os, err)
		}
		if node.Cost != want {
			t.Errorf("%s m5.large cost = %s, want %s", os, node.Cost, want)
		}
	}
}

func TestFargatePricingFromAPIMissingRegion(t *testing.T) {
	f := NewFargatePricing()
	err := f.populatePricingFromAPI(context.Background(), newFakePriceList(t, "AmazonECS", "testdata/ecs-pricing-us-east-1.json"), []string{"us-east-1", "eu-west-1"})
	if err == nil {
		t.Fatal("expected an error for a region without Fargate prices")
	}
	if got := f.missingRegions([]string{"us-east-1", "eu-west-1"}); !reflect.DeepEqual(got, []string{"eu-west-1"}) {
		t.Errorf("missingRegions() = %v, want [eu-west-1]", got)
	}
}

func TestFargatePricingFromAPIFailedRegionIsRetried(t *testing.T) {
	attrs := map[string]string{"regionCode": "us-east-1", "usagetype": "USE1-Fargate-GB-Hours"}
	fake := &fakePriceList{
		serviceCode: "AmazonECS",
		entries:     []string{`{"product":{"sku":"NOTERMS","attributes":{"regionCode":"us-east-1","usagetype":"USE1-Fargate-GB-Hours"}},"terms":{"OnDemand":{}}}`},
		attributes:  []map[string]string{attrs},
	}
	f := NewFargatePricing()
	if err := f.populatePricingFromAPI(context.Background(), fake, []string{"us-east-1"}); err == nil {
		t.Fatal("expected an error for a SKU without an on-demand term")
	}
	if got := f.missingRegions([]string{"us-east-1"}); !reflect.DeepEqual(got, []string{"us-east-1"}) {
		t.Errorf("missingRegions() = %v, want the failed region to be retried", got)
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
