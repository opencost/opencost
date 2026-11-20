package aws

import (
	"context"
	"fmt"
	"testing"
	"time"

	awsSDK "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2Types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
)

// fakeSpotClient implements spotPriceHistoryClient by returning a fixed set of
// pages. Each element in pages is one page of results.
type fakeSpotClient struct {
	pages [][]ec2Types.SpotPrice
	calls int
	err   error // if set, returned on every NextPage call
}

func (f *fakeSpotClient) DescribeSpotPriceHistory(
	_ context.Context,
	_ *ec2.DescribeSpotPriceHistoryInput,
	_ ...func(*ec2.Options),
) (*ec2.DescribeSpotPriceHistoryOutput, error) {
	if f.err != nil {
		return nil, f.err
	}
	if f.calls >= len(f.pages) {
		return &ec2.DescribeSpotPriceHistoryOutput{}, nil
	}
	page := f.pages[f.calls]
	f.calls++
	var nextToken *string
	if f.calls < len(f.pages) {
		nextToken = awsSDK.String("next")
	}
	return &ec2.DescribeSpotPriceHistoryOutput{
		SpotPriceHistory: page,
		NextToken:        nextToken,
	}, nil
}

func spotItem(instanceType, price string, az string) ec2Types.SpotPrice {
	ts := time.Now()
	return ec2Types.SpotPrice{
		InstanceType:     ec2Types.InstanceType(instanceType),
		SpotPrice:        awsSDK.String(price),
		Timestamp:        &ts,
		AvailabilityZone: awsSDK.String(az),
	}
}

func TestQuerySpotPrices_Basic(t *testing.T) {
	client := &fakeSpotClient{
		pages: [][]ec2Types.SpotPrice{
			{
				spotItem("m5.large", "0.05", "us-west-2a"),
				spotItem("c5.xlarge", "0.10", "us-west-2b"),
			},
		},
	}

	results, err := querySpotPrices(context.Background(), "us-west-2", client)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
	byType := make(map[string]float64)
	for _, r := range results {
		byType[r.InstanceType] = r.Price
	}
	if byType["m5.large"] != 0.05 {
		t.Errorf("m5.large price = %f, want 0.05", byType["m5.large"])
	}
	if byType["c5.xlarge"] != 0.10 {
		t.Errorf("c5.xlarge price = %f, want 0.10", byType["c5.xlarge"])
	}
}

func TestQuerySpotPrices_DeduplicatesAcrossAZs(t *testing.T) {
	// Same instance type appears in multiple AZs — only the first should be kept.
	client := &fakeSpotClient{
		pages: [][]ec2Types.SpotPrice{
			{
				spotItem("m5.large", "0.05", "us-west-2a"),
				spotItem("m5.large", "0.09", "us-west-2b"), // duplicate, should be skipped
			},
		},
	}

	results, err := querySpotPrices(context.Background(), "us-west-2", client)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result after dedup, got %d", len(results))
	}
	if results[0].Price != 0.05 {
		t.Errorf("expected first price 0.05, got %f", results[0].Price)
	}
}

func TestQuerySpotPrices_MultiPage(t *testing.T) {
	client := &fakeSpotClient{
		pages: [][]ec2Types.SpotPrice{
			{spotItem("m5.large", "0.05", "us-west-2a")},
			{spotItem("c5.xlarge", "0.10", "us-west-2a")},
			{spotItem("r5.2xlarge", "0.20", "us-west-2a")},
		},
	}

	results, err := querySpotPrices(context.Background(), "us-west-2", client)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 3 {
		t.Fatalf("expected 3 results across pages, got %d", len(results))
	}
}

func TestQuerySpotPrices_APIError(t *testing.T) {
	client := &fakeSpotClient{
		err: fmt.Errorf("api unavailable"),
	}

	_, err := querySpotPrices(context.Background(), "us-west-2", client)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}
