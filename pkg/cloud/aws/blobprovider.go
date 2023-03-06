package aws

import (
	"context"
	"encoding/json"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/pricing"
	"github.com/opencost/opencost/pkg/cloud"
	"strings"
)

type BlobProvider struct {
	client *pricing.Client
}

type ProductListing struct {
	Product     cloud.AWSProduct      `json:"product"`
	Terms       cloud.AWSProductTerms `json:"terms"`
	ServiceCode string                `json:"serviceCode"`
}

func NewBlobProvider(region string, profile string) (*BlobProvider, error) {
	if region == "" {
		region = "us-east-1"
	}
	if profile == "" {
		profile = "sandbox"
	}
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion(region),
		config.WithSharedConfigProfile(profile),
	)

	if err != nil {
		return nil, err
	}
	svc := pricing.NewFromConfig(cfg)
	return &BlobProvider{
		client: svc,
	}, nil
}

func (s3 *BlobProvider) DownloadPricingIndexFile(context context.Context) ([]ProductListing, error) {
	var priceLists []string
	var nextToken *string
	serviceCode := "AmazonS3"

	for {
		resp, err := s3.client.GetProducts(context, &pricing.GetProductsInput{
			NextToken:   nextToken,
			ServiceCode: &serviceCode,
		})
		if err != nil {
			return nil, err
		}
		priceLists = append(
			priceLists,
			resp.PriceList...,
		)
		if resp.NextToken == nil {
			break
		}
		nextToken = resp.NextToken
	}
	return s3.decodePriceLists(priceLists)
}

func (s3 *BlobProvider) decodePriceLists(priceLists []string) ([]ProductListing, error) {

	var prices []ProductListing
	for _, priceList := range priceLists {
		dec := json.NewDecoder(strings.NewReader(priceList))
		var productListing ProductListing
		if err := dec.Decode(&productListing); err != nil {
			return nil, err
		}
		prices = append(prices, productListing)
	}
	return prices, nil
}
