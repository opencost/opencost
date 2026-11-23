package aws

import (
	"context"
	"fmt"
	"iter"
	"slices"
	"strings"
	"time"

	awsSDK "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/pricing"
	"github.com/aws/aws-sdk-go-v2/service/pricing/types"

	"github.com/opencost/opencost/core/pkg/clustercache"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/util/json"
	"github.com/opencost/opencost/pkg/env"
)

const (
	pricingAPITimeout = 2 * time.Minute
	// ANY_OF values are one comma-separated string; chunk to keep each filter short.
	pricingAPIInstanceTypesPerQuery = 25
)

// newPricingAPIClient builds a Price List Query API client. The API is only served from us-east-1, eu-central-1,
// ap-south-1 and cn-northwest-1, so its region is independent of the cluster's.
func (aws *AWS) newPricingAPIClient(regions []string) (*pricing.Client, error) {
	region := env.GetAWSPricingAPIRegion()
	if region == "" {
		region = "us-east-1"
		if slices.ContainsFunc(regions, func(r string) bool { return strings.HasPrefix(r, chinaRegionPrefix) }) {
			region = "cn-northwest-1"
		}
	}
	accessKey, err := aws.GetAWSAccessKey()
	if err != nil {
		return nil, err
	}
	cfg, err := accessKey.CreateConfig(region)
	if err != nil {
		return nil, err
	}
	return pricing.NewFromConfig(cfg), nil
}

// pricingRegions returns the distinct regions of node and PV pricing keys, which all start with "<region>,".
func pricingRegions(keys ...iter.Seq[string]) []string {
	var regions []string
	for _, seq := range keys {
		for key := range seq {
			region, _, _ := strings.Cut(key, ",")
			if region != "" && !slices.Contains(regions, region) {
				regions = append(regions, region)
			}
		}
	}
	return regions
}

func pricingFilter(filterType types.FilterType, field, value string) types.Filter {
	return types.Filter{Type: filterType, Field: awsSDK.String(field), Value: awsSDK.String(value)}
}

// getProducts returns the products matching filters with their on-demand terms, shaped like an offer file.
func getProducts(ctx context.Context, client pricing.GetProductsAPIClient, serviceCode string, filters ...types.Filter) (*AWSPricing, error) {
	result := &AWSPricing{
		Products: make(map[string]*AWSProduct),
		Terms:    AWSPricingTerms{OnDemand: make(map[string]map[string]*AWSOfferTerm)},
	}
	pages := pricing.NewGetProductsPaginator(client, &pricing.GetProductsInput{
		ServiceCode:   awsSDK.String(serviceCode),
		Filters:       filters,
		FormatVersion: awsSDK.String("aws_v1"),
	})
	for pages.HasMorePages() {
		page, err := pages.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("GetProducts %s: %w", serviceCode, err)
		}
		for _, item := range page.PriceList {
			var entry struct {
				Product AWSProduct `json:"product"`
				Terms   struct {
					OnDemand map[string]*AWSOfferTerm `json:"OnDemand"`
				} `json:"terms"`
			}
			if err := json.Unmarshal([]byte(item), &entry); err != nil {
				return nil, fmt.Errorf("decoding %s price list entry: %w", serviceCode, err)
			}
			result.Products[entry.Product.Sku] = &entry.Product
			result.Terms.OnDemand[entry.Product.Sku] = entry.Terms.OnDemand
		}
	}
	return result, nil
}

// populatePricingFromAPI fills aws.Pricing the way populatePricing does from the offer file, but only queries the
// instance types in inputkeys plus each region's EBS and NLB prices.
func (aws *AWS) populatePricingFromAPI(ctx context.Context, client pricing.GetProductsAPIClient, regions []string, inputkeys map[string]bool) error {
	instanceTypes := instanceTypesByRegion(inputkeys)
	aws.Pricing = make(map[string]*AWSProductTerms)
	skuToPricingKeyMap := make(map[string]string)
	for _, region := range regions {
		for _, filters := range ec2PricingQueries(instanceTypes[region]) {
			products, err := getProducts(ctx, client, "AmazonEC2", append(filters, pricingFilter(types.FilterTypeTermMatch, "regionCode", region))...)
			if err != nil {
				return err
			}
			for _, product := range products.Products {
				aws.addProduct(product, inputkeys, skuToPricingKeyMap)
			}
			for sku, terms := range products.Terms.OnDemand {
				for _, term := range terms {
					aws.addOnDemandTerm(sku, term, skuToPricingKeyMap)
				}
			}
		}
	}
	return nil
}

// instanceTypesByRegion groups the instance types of "<region>,<instanceType>,..." node keys by region.
func instanceTypesByRegion(inputkeys map[string]bool) map[string][]string {
	instanceTypes := make(map[string][]string)
	for key := range inputkeys {
		parts := strings.Split(key, ",")
		if len(parts) >= 2 && parts[1] != "" && !slices.Contains(instanceTypes[parts[0]], parts[1]) {
			instanceTypes[parts[0]] = append(instanceTypes[parts[0]], parts[1])
		}
	}
	return instanceTypes
}

// ec2PricingQueries returns the AmazonEC2 filters for a region's EBS and NLB prices and its instance types.
func ec2PricingQueries(instanceTypes []string) [][]types.Filter {
	queries := [][]types.Filter{
		{pricingFilter(types.FilterTypeContains, "usagetype", "EBS:Volume")},
		{
			pricingFilter(types.FilterTypeTermMatch, "operation", "LoadBalancing:Network"),
			pricingFilter(types.FilterTypeContains, "usagetype", "LoadBalancerUsage"),
		},
	}
	for chunk := range slices.Chunk(instanceTypes, pricingAPIInstanceTypesPerQuery) {
		queries = append(queries, []types.Filter{
			pricingFilter(types.FilterTypeAnyOf, "instanceType", strings.Join(chunk, ",")),
			pricingFilter(types.FilterTypeAnyOf, "operatingSystem", "Linux,Windows"),
			// Windows also lists BYOL and "License Included - Infrastructure" SKUs under the same pricing key; the
			// standard license-included Windows SKU is labelled "No License required", like Linux.
			pricingFilter(types.FilterTypeTermMatch, "licenseModel", "No License required"),
			pricingFilter(types.FilterTypeTermMatch, "tenancy", "Shared"),
			pricingFilter(types.FilterTypeTermMatch, "preInstalledSw", "NA"),
			pricingFilter(types.FilterTypeTermMatch, "capacitystatus", "Used"),
			pricingFilter(types.FilterTypeTermMatch, "marketoption", "OnDemand"),
		})
	}
	return queries
}

// refreshFargatePricing loads Fargate prices once, preferring the Price List API over the AmazonECS offer file. With the
// API, regions that appear later are added on each refresh, keeping the prices already loaded.
func (aws *AWS) refreshFargatePricing(ctx context.Context, client *pricing.Client, regions []string, nodeList []*clustercache.Node) {
	useAPI := client != nil && env.GetAWSECSPricingURLOverride() == ""
	if aws.FargatePricing != nil {
		if missing := aws.FargatePricing.missingRegions(regions); useAPI && len(missing) > 0 {
			aws.FargatePricingError = aws.FargatePricing.populatePricingFromAPI(ctx, client, missing)
			if aws.FargatePricingError != nil {
				log.Warnf("Failed to load Fargate pricing for %v: %v", missing, aws.FargatePricingError)
			}
		}
		return
	}

	aws.FargatePricing = NewFargatePricing()
	if useAPI {
		aws.FargatePricingError = aws.FargatePricing.populatePricingFromAPI(ctx, client, regions)
		if aws.FargatePricingError == nil {
			return
		}
		log.Warnf("Falling back to the Fargate offer file: %v", aws.FargatePricingError)
		aws.FargatePricing = NewFargatePricing()
	}
	aws.FargatePricingError = aws.FargatePricing.Initialize(nodeList)
	if aws.FargatePricingError != nil {
		log.Errorf("Failed to initialize fargate pricing: %s", aws.FargatePricingError.Error())
	}
}

func (f *FargatePricing) missingRegions(regions []string) []string {
	return slices.DeleteFunc(slices.Clone(regions), func(region string) bool {
		_, ok := f.regions[region]
		return ok
	})
}

func (f *FargatePricing) populatePricingFromAPI(ctx context.Context, client pricing.GetProductsAPIClient, regions []string) error {
	for _, region := range regions {
		products, err := getProducts(ctx, client, "AmazonECS",
			pricingFilter(types.FilterTypeTermMatch, "regionCode", region),
			pricingFilter(types.FilterTypeContains, "usagetype", "Fargate"),
		)
		if err != nil {
			return err
		}
		if err := f.populatePricing(products); err != nil {
			return err
		}
		if _, ok := f.regions[region]; !ok {
			return fmt.Errorf("no Fargate prices returned for %s", region)
		}
	}
	return nil
}
