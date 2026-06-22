package aws

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/opencost/opencost/core/pkg/env"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/model/pricingmodel"
	"github.com/opencost/opencost/core/pkg/model/shared"
)

const pricingCacheTTL = 24 * time.Hour
const pricingCacheDir = "pricingsource/aws"
const pricingCacheFile = "cached_ec2_pricingmodelset"

const PricingListPricingSourceType pricingmodel.PricingSourceType = "aws_pricing_list_api"

type PricingListPricingSourceConfig struct {
	CurrencyCode string
}

type PricingListPricingSource struct {
	config PricingListPricingSourceConfig
}

func NewPricingListPricingSource(cfg PricingListPricingSourceConfig) *PricingListPricingSource {
	return &PricingListPricingSource{config: cfg}
}

func (p *PricingListPricingSource) cacheFilePath() (string, error) {
	dir := env.GetPathFromConfig(pricingCacheDir)
	if _, e := os.Stat(dir); e != nil && os.IsNotExist(e) {
		err := os.MkdirAll(dir, os.ModePerm)
		if err != nil {
			return "", err
		}
	}
	return filepath.Join(dir, pricingCacheFile), nil
}

func (p *PricingListPricingSource) PricingSourceType() pricingmodel.PricingSourceType {
	return PricingListPricingSourceType
}

// PricingSourceKey returns the PricingSourceType because it is meant to run single instance.
func (p *PricingListPricingSource) PricingSourceKey() string {
	return string(PricingListPricingSourceType)
}

func (p *PricingListPricingSource) GetPricing() (*pricingmodel.PricingModelSet, error) {
	log.Infof("PricingListPricingSource: starting AWS EC2 pricing list download (large file, this may take a while)")
	start := time.Now()

	now := time.Now().UTC()
	pms := pricingmodel.NewPricingModelSet(now, p.PricingSourceType(), p.PricingSourceKey())
	skuToNodeKey := make(map[string]pricingmodel.NodeKey)

	var productCount, termCount int
	const logInterval = 50000

	// When parsing product we create keys based off of product attributes and link those to a SKU.
	handleProduct := func(product *PriceListEC2Product) {
		productCount++
		if productCount%logInterval == 0 {
			log.Infof("PricingListPricingSource: processed %d products...", productCount)
		}
		attr := product.Attributes
		if attr.LocationType != "AWS Region" {
			return
		}

		if !((strings.HasPrefix(attr.UsageType, "BoxUsage") || strings.Contains(attr.UsageType, "-BoxUsage")) &&
			(attr.CapacityStatus == "Used" || attr.CapacityStatus == "") &&
			(attr.MarketOption == "OnDemand" || attr.MarketOption == "")) {
			return
		}

		if attr.OperatingSystem != "" && attr.OperatingSystem != "NA" && attr.OperatingSystem != "Linux" {
			return
		}

		if attr.PreInstalledSw != "" && attr.PreInstalledSw != "NA" {

		}

		if attr.RegionCode == "" || attr.InstanceType == "" {
			return
		}

		skuToNodeKey[product.Sku] = pricingmodel.NodeKey{
			Provider:    shared.ProviderAWS,
			Region:      attr.RegionCode,
			NodeType:    attr.InstanceType,
			UsageType:   shared.UsageTypeOnDemand,
			PricingType: pricingmodel.NodePricingTypeTotal,
		}
	}

	// Terms are used to define pricing and have the sku to look up the appropriate key.
	handleTerm := func(term *PriceListEC2Term) {
		termCount++
		if termCount%logInterval == 0 {
			log.Infof("PricingListPricingSource: processed %d terms, %d pricing entries so far...", termCount, len(pms.NodePricing))
		}
		nodeKey, ok := skuToNodeKey[term.Sku]
		if !ok {
			return
		}
		hourlyRateCode := HourlyRateCode
		if _, ok = OnDemandRateCodes[term.OfferTermCode]; !ok {
			if _, okCN := OnDemandRateCodesCn[term.OfferTermCode]; !okCN {
				// Skip if term is not OnDemand
				return
			}
			hourlyRateCode = HourlyRateCodeCn
		}
		priceDimensionKey := strings.Join([]string{term.Sku, term.OfferTermCode, hourlyRateCode}, ".")

		pricingDimension, ok := term.PriceDimensions[priceDimensionKey]
		if !ok {
			return
		}

		priceStr := pricingDimension.PricePerUnit.ForCurrency(p.config.CurrencyCode)
		price, err := strconv.ParseFloat(priceStr, 64)
		if err != nil {
			log.Errorf("failed to parse str to float '%s': %s", priceStr, err.Error())
			return
		}
		pms.NodePricing[nodeKey] = pricingmodel.NodePricing{
			HourlyRate: price,
		}
	}

	err := QueryEC2PriceList("", handleProduct, handleTerm)
	if err != nil {
		return nil, fmt.Errorf("failed to query list pricing data %w", err)
	}

	log.Infof("PricingListPricingSource: completed in %s — %d products, %d terms, %d pricing entries",
		time.Since(start).Round(time.Second), productCount, termCount, len(pms.NodePricing))

	return pms, nil
}
